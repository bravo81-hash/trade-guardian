import pandas as pd
import numpy as np
import io
from datetime import datetime
import hashlib
import streamlit as st

from db import (
    make_trade_id,
    upsert_trade,
    replace_greeks,
    replace_legs,
    add_note
)

# ---------------------------------------------------------
# CONFIG (same as v35)
# ---------------------------------------------------------
BASE_CONFIG = {
    '130/160': {'yield': 0.13, 'pnl': 500, 'roi': 6.8, 'dit': 36},
    '160/190': {'yield': 0.28, 'pnl': 700, 'roi': 12.7, 'dit': 44},
    'M200':    {'yield': 0.56, 'pnl': 900, 'roi': 11.1, 'dit': 41}
}


def get_strategy(group_name: str) -> str:
    g = str(group_name).upper()
    if "M200" in g:
        return "M200"
    if "160/190" in g:
        return "160/190"
    if "130/160" in g:
        return "130/160"
    return "Other"


def clean_num(x):
    try:
        return float(str(x).replace("$", "").replace(",", "").strip())
    except:
        return 0.0


# ---------------------------------------------------------
# PARSE OPTIONSTRAT FILES
# ---------------------------------------------------------
def _read_file(f):
    """Reads an uploaded OptionStrat file (CSV/XLSX) into a DataFrame."""
    name = f.name.lower()

    if name.endswith(".xlsx") or name.endswith(".xls"):
        df_raw = pd.read_excel(f, header=None, engine='openpyxl')

        header_idx = -1
        for i, row in df_raw.head(20).iterrows():
            if "Name" in " ".join(row.astype(str).values):
                header_idx = i
                break

        if header_idx != -1:
            df = df_raw.iloc[header_idx+1:].copy()
            df.columns = df_raw.iloc[header_idx]
            return df

    # CSV fallback
    content = f.getvalue().decode("utf-8")
    lines = content.split("\n")

    header_idx = 0
    for i, line in enumerate(lines[:20]):
        if "Name" in line:
            header_idx = i
            break

    return pd.read_csv(io.StringIO(content), skiprows=header_idx)


# ---------------------------------------------------------
# MAIN INGEST FUNCTION
# ---------------------------------------------------------
def ingest_files(uploaded_files):
    """
    Reads OptionStrat files, performs the same calculations
    as v35.0, writes trades into SQLite, and returns DataFrame
    for UI display (Active + Expired).
    """

    all_trades = []

    for f in uploaded_files:

        try:
            df = _read_file(f)
            if df is None or df.empty:
                continue

            is_active_file = ("active" in f.name.lower())

            for _, row in df.iterrows():
                created_val = row.get("Created At", "")
                entry_date = None

                # Detect date
                if isinstance(created_val, (pd.Timestamp, datetime)):
                    entry_date = created_val
                elif isinstance(created_val, str) and ":" in created_val:
                    try:
                        entry_date = pd.to_datetime(created_val)
                    except:
                        pass

                if entry_date is None:
                    continue

                name = str(row.get("Name", "Unknown"))
                group = str(row.get("Group", ""))
                strategy = get_strategy(group)

                pnl = clean_num(row.get("Total Return $", 0))
                debit = abs(clean_num(row.get("Net Debit/Credit", 0)))
                theta = clean_num(row.get("Theta", 0))
                delta = clean_num(row.get("Delta", 0))
                gamma = clean_num(row.get("Gamma", 0))
                vega = clean_num(row.get("Vega", 0))

                # Expiration
                expiration_raw = row.get("Expiration", "")
                expiration_date = None
                if isinstance(expiration_raw, (pd.Timestamp, datetime)):
                    expiration_date = expiration_raw
                else:
                    try:
                        if str(expiration_raw).strip():
                            expiration_date = pd.to_datetime(expiration_raw)
                    except:
                        pass

                # Determine status
                status = "Expired"
                if is_active_file:
                    status = "Active"
                if status == "Active" and pnl == 0 and expiration_date is None:
                    status = "Active"

                # If expired but missing expiration, fallback
                if status == "Expired" and expiration_date is None:
                    expiration_date = datetime.now()

                # Days Held
                dh_end = expiration_date if expiration_date else datetime.now()
                days_held = (dh_end - entry_date).days
                if days_held < 1:
                    days_held = 1

                # ROI / Daily Yield
                roi = (pnl / debit * 100) if debit > 0 else 0
                daily_yield = roi / days_held

                # Lot size logic (same as v35)
                lot_size = 1
                if strategy == "130/160":
                    if debit > 10000:
                        lot_size = 3
                    elif debit > 6000:
                        lot_size = 2
                elif strategy == "160/190":
                    if debit > 8000:
                        lot_size = 2
                elif strategy == "M200":
                    if debit > 12000:
                        lot_size = 2

                debit_per_lot = debit / max(1, lot_size)

                # Grade logic (same as v35)
                grade = "C"
                reason = "Standard"

                if strategy == "130/160":
                    if debit_per_lot > 4800:
                        grade = "F"; reason = "Overpriced (> $4.8k)"
                    elif 3500 <= debit_per_lot <= 4500:
                        grade = "A+"; reason = "Sweet Spot"
                    else:
                        grade = "B"; reason = "Acceptable"

                elif strategy == "160/190":
                    if 4800 <= debit_per_lot <= 5500:
                        grade = "A"; reason = "Ideal Pricing"
                    else:
                        grade = "C"; reason = "Check Pricing"

                elif strategy == "M200":
                    if 7500 <= debit_per_lot <= 8500:
                        grade = "A"; reason = "Perfect Entry"
                    else:
                        grade = "B"; reason = "Variance"

                alerts = []
                if strategy == "130/160" and status == "Active" and 25 <= days_held <= 35 and pnl < 100:
                    alerts.append("💀 STALE CAPITAL")

                # Generate unique trade ID
                trade_id = make_trade_id(
                    name=name,
                    created_at=str(entry_date),
                    strategy=strategy
                )

                # Store in DB (idempotent)
                upsert_trade({
                    "trade_id": trade_id,
                    "name": name,
                    "strategy": strategy,
                    "status": status,
                    "pnl": pnl,
                    "debit": debit,
                    "debit_per_lot": debit_per_lot,
                    "grade": grade,
                    "reason": reason,
                    "alerts": " ".join(alerts),
                    "days_held": days_held,
                    "daily_yield": daily_yield,
                    "roi": roi,
                    "entry_date": entry_date.isoformat(),
                    "expiration_date": expiration_date.isoformat() if expiration_date else None,
                    "lot_size": lot_size,
                    "latest_flag": 1
                })

                # Store greeks
                replace_greeks(
                    trade_id,
                    theta=theta,
                    delta=delta,
                    gamma=gamma,
                    vega=vega
                )

                # Store placeholder legs (OptionStrat legs not included in v35 parsing)
                replace_legs(trade_id, [])  # Can be filled later

                # Build UI row
                all_trades.append({
                    "Trade ID": trade_id,
                    "Name": name,
                    "Strategy": strategy,
                    "Status": status,
                    "P&L": pnl,
                    "Debit": debit,
                    "Debit/Lot": debit_per_lot,
                    "Grade": grade,
                    "Reason": reason,
                    "Alerts": " ".join(alerts),
                    "Days Held": days_held,
                    "Daily Yield %": daily_yield,
                    "ROI": roi,
                    "Theta": theta,
                    "Gamma": gamma,
                    "Vega": vega,
                    "Delta": delta,
                    "Entry Date": entry_date,
                    "Expiration Date": expiration_date,
                    "Notes": ""
                })

        except Exception as e:
            st.error(f"Error processing file {f.name}: {e
