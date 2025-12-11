import pandas as pd
import numpy as np
import io
from datetime import datetime
import db  # our local database module

# ---------------------------------------------------------
# BASE CONFIG (same as v35.0)
# ---------------------------------------------------------
BASE_CONFIG = {
    '130/160': {'yield': 0.13, 'pnl': 500, 'roi': 6.8, 'dit': 36},
    '160/190': {'yield': 0.28, 'pnl': 700, 'roi': 12.7, 'dit': 44},
    'M200':    {'yield': 0.56, 'pnl': 900, 'roi': 11.1, 'dit': 41}
}


# ---------------------------------------------------------
# UTILITY FUNCTIONS (from v35.0)
# ---------------------------------------------------------
def get_strategy(group_name):
    g = str(group_name).upper()
    if "M200" in g: return "M200"
    elif "160/190" in g: return "160/190"
    elif "130/160" in g: return "130/160"
    return "Other"


def clean_num(x):
    try:
        return float(str(x).replace('$', '').replace(',', ''))
    except:
        return 0.0


def detect_header_index(lines_or_df):
    """
    Detects where OptionStrat header row begins.
    Works for both CSV text (list of lines) and XLSX dataframe.
    """
    if isinstance(lines_or_df, list):  # CSV
        for i, line in enumerate(lines_or_df[:20]):
            if "Name" in line and "Total Return" in line:
                return i
        return 0

    else:  # Excel DataFrame
        df_raw = lines_or_df
        for i, row in df_raw.head(20).iterrows():
            row_str = " ".join(row.astype(str).values)
            if "Name" in row_str and "Total Return" in row_str:
                return i
        return -1


def safe_date(val):
    """Parse a date safely, return None if invalid."""
    if isinstance(val, (pd.Timestamp, datetime)):
        return val
    try:
        return pd.to_datetime(val)
    except:
        return None


# ---------------------------------------------------------
# GRADE CALCULATOR (same as v35.0)
# ---------------------------------------------------------
def compute_grade(strategy, debit_lot):
    """Returns grade + reason."""
    if strategy == '130/160':
        if debit_lot > 4800: return "F", "Overpriced (> $4.8k)"
        elif 3500 <= debit_lot <= 4500: return "A+", "Sweet Spot"
        else: return "B", "Acceptable"

    elif strategy == '160/190':
        if 4800 <= debit_lot <= 5500: return "A", "Ideal Pricing"
        else: return "C", "Check Pricing"

    elif strategy == 'M200':
        if 7500 <= debit_lot <= 8500: return "A", "Perfect Entry"
        else: return "B", "Variance"

    return "C", "Standard"


# ---------------------------------------------------------
# MAIN INGEST FUNCTION
# This replicates ALL processing from v35.0 while writing to DB
# ---------------------------------------------------------
def ingest_files(files):
    """
    Process uploaded OptionStrat files, write results to DB,
    return a pandas DataFrame exactly like v35.0 for UI consumption.
    """
    all_trades = []

    for f in files:
        fname = f.name.lower()
        is_active_file = "active" in fname
        
        # --------------------------------------------
        # STEP 1 — Load file
        # --------------------------------------------
        if fname.endswith(".xlsx") or fname.endswith(".xls"):
            df_raw = pd.read_excel(f, header=None, engine="openpyxl")
            header_idx = detect_header_index(df_raw)
            if header_idx != -1:
                df = df_raw.iloc[header_idx+1:].copy()
                df.columns = df_raw.iloc[header_idx]
            else:
                continue

        else:  # CSV
            content = f.getvalue().decode("utf-8")
            lines = content.split("\n")
            header_idx = detect_header_index(lines)
            df = pd.read_csv(io.StringIO(content), skiprows=header_idx)

        if df is None or df.empty:
            continue

        # --------------------------------------------
        # STEP 2 — Process each trade row
        # --------------------------------------------
        for _, row in df.iterrows():

            # Validate timestamp
            created_at = safe_date(row.get("Created At"))
            if not created_at:
                continue

            name = row.get("Name", "Unknown")
            group = str(row.get("Group", ""))
            strategy = get_strategy(group)

            # Basic metrics
            pnl = clean_num(row.get("Total Return $", 0))
            debit = abs(clean_num(row.get("Net Debit/Credit", 0)))

            theta = clean_num(row.get("Theta", 0))
            delta = clean_num(row.get("Delta", 0))
            gamma = clean_num(row.get("Gamma", 0))
            vega = clean_num(row.get("Vega", 0))

            # Active vs expired
            status = "Active" if is_active_file else "Expired"

            expiration_val = row.get("Expiration")
            exp_date = safe_date(expiration_val)

            # Fix OptionStrat bug: expired but no expiration → treat as active
            if status == "Expired" and pnl == 0 and exp_date is None:
                status = "Active"

            if exp_date is None:
                end_date = datetime.now()
            else:
                end_date = exp_date

            # Days held
            days_held = (end_date - created_at).days
            if days_held < 1:
                days_held = 1

            # ROI + daily yield
            roi = (pnl / debit * 100) if debit > 0 else 0
            daily_yield = roi / days_held

            # Lot size rules (same as v35)
            lot_size = 1
            if strategy == '130/160' and debit > 10000:
                lot_size = 3
            elif strategy == '130/160' and debit > 6000:
                lot_size = 2
            elif strategy == '160/190' and debit > 8000:
                lot_size = 2
            elif strategy == 'M200' and debit > 12000:
                lot_size = 2

            debit_lot = debit / max(1, lot_size)

            # Grade
            grade, reason = compute_grade(strategy, debit_lot)

            # Alerts
            alerts = []
            if strategy == '130/160' and status == "Active" and 25 <= days_held <= 35 and pnl < 100:
                alerts.append("💀 STALE CAPITAL")

            alerts_str = " ".join(alerts)

            # --------------------------------------------
            # STEP 3 — Create trade ID
            # --------------------------------------------
            trade_id = db.make_trade_id(name, str(created_at), strategy)

            # --------------------------------------------
            # STEP 4 — Save to DB (idempotent)
            # --------------------------------------------
            trade_record = {
                "trade_id": trade_id,
                "name": name,
                "strategy": strategy,
                "status": status,
                "pnl": pnl,
                "debit": debit,
                "debit_per_lot": debit_lot,
                "grade": grade,
                "reason": reason,
                "alerts": alerts_str,
                "days_held": days_held,
                "daily_yield": daily_yield,
                "roi": roi,
                "entry_date": created_at.isoformat(),
                "expiration_date": exp_date.isoformat() if exp_date else None,
                "lot_size": lot_size,
                "latest_flag": 1
            }

            db.upsert_trade(trade_record)

            # Save greeks
            db.replace_greeks(trade_id, theta, delta, gamma, vega)

            # Legs not stored here — OptionStrat exports sometimes split legs across pages.
            # We'll leave leg ingestion optional later.

        # END for each row

    # END for each file

    # ---------------------------------------------------------
    # LOAD ALL TRADES BACK FROM DB FOR UI
    # ---------------------------------------------------------
    rows = db.load_all_trades()
    if not rows:
        return pd.DataFrame()

    df_out = pd.DataFrame(rows)

    # Merge greeks
    greeks_rows = [dict(db.fetch_greeks(r["trade_id"]) or {}) for r in rows]
    greeks_df = pd.DataFrame(greeks_rows)

    if not greeks_df.empty:
        df_out = pd.concat([df_out.reset_index(drop=True),
                            greeks_df.reset_index(drop=True)], axis=1)

    # Add notes indicator
    df_out["Notes"] = ""

    return df_out
