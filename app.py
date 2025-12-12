import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- VERSION CONTROL ---
VER = "v80.0 (Greeks Lab Fixed + IV/PoP Data Added)"
st.sidebar.info(f"✅ RUNNING: {VER}")
st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v5.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # 1. TRADES TABLE (Base Schema)
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    strategy TEXT,
                    status TEXT,
                    entry_date DATE,
                    exit_date DATE,
                    days_held INTEGER,
                    debit REAL,
                    lot_size INTEGER,
                    pnl REAL,
                    theta REAL,
                    delta REAL,
                    gamma REAL,
                    vega REAL,
                    iv REAL,
                    pop REAL,
                    max_loss REAL,
                    max_profit REAL,
                    notes TEXT
                )''')
    
    # 2. SNAPSHOTS TABLE
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT,
                    snapshot_date DATE,
                    pnl REAL,
                    days_held INTEGER,
                    theta REAL,
                    delta REAL,
                    gamma REAL,
                    vega REAL,
                    iv REAL,
                    FOREIGN KEY(trade_id) REFERENCES trades(id)
                )''')
                
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
    conn.commit()
    conn.close()
    
    # Run Schema Migration to fix missing columns in old DBs
    check_schema_updates()

def check_schema_updates():
    """Automatically adds new columns to existing DB if they are missing."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Columns to check and add to 'trades'
    new_cols = {
        'iv': 'REAL', 
        'pop': 'REAL', 
        'max_loss': 'REAL', 
        'max_profit': 'REAL'
    }
    
    # Get existing columns
    cursor = c.execute('select * from trades')
    names = [description[0] for description in cursor.description]
    
    for col, dtype in new_cols.items():
        if col not in names:
            try:
                c.execute(f"ALTER TABLE trades ADD COLUMN {col} {dtype}")
                print(f"Migrated DB: Added {col} to trades")
            except: pass
            
    # Columns to check and add to 'snapshots'
    snap_cols = {'iv': 'REAL'}
    cursor = c.execute('select * from snapshots')
    snap_names = [description[0] for description in cursor.description]
    
    for col, dtype in snap_cols.items():
        if col not in snap_names:
            try:
                c.execute(f"ALTER TABLE snapshots ADD COLUMN {col} {dtype}")
                print(f"Migrated DB: Added {col} to snapshots")
            except: pass
            
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_NAME)

# --- CONFIGURATION ---
BASE_CONFIG = {
    '130/160': {'yield': 0.13, 'pnl': 500, 'roi': 6.8, 'dit': 36},
    '160/190': {'yield': 0.28, 'pnl': 700, 'roi': 12.7, 'dit': 44},
    'M200':    {'yield': 0.56, 'pnl': 900, 'roi': 11.1, 'dit': 41}
}

# --- HELPER FUNCTIONS ---
def get_strategy(group_name, trade_name=""):
    """
    Prioritizes the CSV 'Group' column for accuracy.
    """
    g = str(group_name).upper()
    n = str(trade_name).upper()
    
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    try:
        s = str(x).replace('$', '').replace(',', '').replace('%', '')
        if s.strip() == '' or s.strip() == '-': return 0.0
        return float(s)
    except: return 0.0

def safe_fmt(val, fmt_str):
    try:
        if isinstance(val, (int, float)): return fmt_str.format(val)
        return str(val)
    except: return str(val)

def generate_id(name, strategy, entry_date):
    # Sanitize name to prevent ID breakage on minor renames
    clean_name = name.split('(')[0].strip().replace(" ", "")
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{clean_name}_{strategy}_{d_str}".replace("/", "-")

def extract_ticker(name):
    try:
        parts = str(name).split(' ')
        if parts:
            ticker = parts[0].replace('.', '').upper()
            if ticker in ['M200', '130', '160', 'APR', 'MAY', 'MAR', 'FEB', 'JAN', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']:
                return "SPX" # Default to SPX if named by month
            return ticker
        return "UNKNOWN"
    except: return "UNKNOWN"

# --- SMART FILE READER ---
def read_file_safely(file):
    try:
        if file.name.endswith('.xlsx') or file.name.endswith('.xls'):
            df_raw = pd.read_excel(file, header=None, engine='openpyxl')
        else:
            # Handle CSV
            content = file.getvalue().decode("utf-8")
            # Sniff header line
            lines = content.split('\n')
            header_row = 0
            for i, line in enumerate(lines[:20]):
                if "Name" in line and "Total Return" in line:
                    header_row = i
                    break
            file.seek(0)
            df_raw = pd.read_csv(file, skiprows=header_row)
            return df_raw

        # Excel Logic to find header
        header_idx = -1
        for i, row in df_raw.head(20).iterrows():
            row_str = " ".join(row.astype(str).values)
            if "Name" in row_str and "Total Return" in row_str:
                header_idx = i
                break
        if header_idx != -1:
            df = df_raw.iloc[header_idx+1:].copy()
            df.columns = df_raw.iloc[header_idx]
            return df
        return None
    except Exception as e:
        return None

# --- SYNC ENGINE ---
def sync_data(file_list, file_type, snapshot_date_override=None):
    log = []
    if not isinstance(file_list, list): file_list = [file_list]
    
    conn = get_db_connection()
    c = conn.cursor()
    
    count_new = 0
    count_update = 0
    
    snap_date = snapshot_date_override if snapshot_date_override else datetime.now().date()
    
    for file in file_list:
        try:
            df = read_file_safely(file)
            if df is None or df.empty:
                log.append(f"⚠️ Skipped {file.name} (Empty/Invalid)")
                continue

            for _, row in df.iterrows():
                # 1. Validation
                name = str(row.get('Name', ''))
                if name.startswith('.') or name in ['nan', '', 'Symbol']: continue
                
                created = row.get('Created At', '')
                try: start_dt = pd.to_datetime(created)
                except: continue
                
                # 2. Extract Data (Expanded)
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                
                # Financials
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                
                # Greeks
                theta = clean_num(row.get('Theta', 0))
                delta = clean_num(row.get('Delta', 0))
                gamma = clean_num(row.get('Gamma', 0))
                vega = clean_num(row.get('Vega', 0))
                iv = clean_num(row.get('IV', 0))
                
                # Risk Data
                pop = clean_num(row.get('Chance', 0)) # "Chance" in CSV is PoP
                max_loss = clean_num(row.get('Max Loss', 0))
                max_profit = clean_num(row.get('Max Profit', 0))
                
                # Lot Logic
                lot_size = 1
                if strat == '130/160':
                    if debit > 10000: lot_size = 3
                    elif debit > 6000: lot_size = 2
                elif strat == '160/190' and debit > 8000: lot_size = 2
                elif strat == 'M200' and debit > 12000: lot_size = 2

                trade_id = generate_id(name, strat, start_dt)
                status = "Active" if file_type == "Active" else "Expired"
                
                # 3. Date Logic
                exit_dt = None
                days_held = 1
                
                if file_type == "History":
                    try:
                        exit_dt = pd.to_datetime(row.get('Expiration'))
                        # If Expiration is in future but status is history, it might have closed early
                        # Use file upload date or today as proxy if expiration is far out?
                        # Better: Use current date for calculation if no specific close date in CSV (OptionStrat limitation)
                        # Fallback: Expiration date is usually the best proxy for "Closed" in OptionStrat exports
                        days_held = (exit_dt - start_dt).days
                    except: days_held = 1
                else:
                    # Active: Use Snapshot Date for calc
                    days_held = (pd.to_datetime(snap_date) - start_dt).days
                
                if days_held < 1: days_held = 1
                
                # 4. DB Upsert
                c.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
                if existing is None:
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, 
                         theta, delta, gamma, vega, iv, pop, max_loss, max_profit, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, name, strat, status, start_dt.date(), 
                         exit_dt.date() if exit_dt else None, 
                         days_held, debit, lot_size, pnl, 
                         theta, delta, gamma, vega, iv, pop, max_loss, max_profit, ""))
                    count_new += 1
                else:
                    # Update Logic
                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, iv=?, pop=?
                            WHERE id=?''', 
                            (pnl, status, exit_dt.date() if exit_dt else None, days_held, 
                             theta, delta, gamma, vega, iv, pop, trade_id))
                        count_update += 1
                    elif existing[0] == "Active":
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, iv=?, pop=?, max_loss=?, max_profit=?
                            WHERE id=?''', 
                            (pnl, days_held, theta, delta, gamma, vega, iv, pop, max_loss, max_profit, trade_id))
                        count_update += 1
                        
                # 5. Snapshot (Active Only)
                if file_type == "Active":
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, snap_date))
                    if not c.fetchone():
                        c.execute('''INSERT INTO snapshots 
                            (trade_id, snapshot_date, pnl, days_held, theta, delta, gamma, vega, iv) 
                            VALUES (?,?,?,?,?,?,?,?,?)''',
                            (trade_id, snap_date, pnl, days_held, theta, delta, gamma, vega, iv))

            log.append(f"✅ {file.name}: {count_new} New, {count_update} Updated")
            
        except Exception as e:
            log.append(f"❌ Error {file.name}: {str(e)}")
            
    conn.commit()
    conn.close()
    return log

# --- DATA LOADER ---
def load_data():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
    except Exception as e:
        st.error(f"🚨 DATABASE ERROR: {str(e)}")
        return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        # Standardize Columns
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'iv': 'IV', 'pop': 'PoP', 'max_loss': 'Max Loss', 'max_profit': 'Max Profit',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes'
        })
        
        # Ensure Columns exist (migration fallback)
        for col in ['IV', 'PoP', 'Max Loss', 'Max Profit']:
            if col not in df.columns: df[col] = 0.0

        # Fix Types
        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        for c in ['Debit', 'P&L', 'Theta', 'Delta', 'Gamma', 'Vega', 'IV', 'PoP', 'Max Loss', 'Max Profit']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
        df['Days Held'] = pd.to_numeric(df['Days Held'], errors='coerce').fillna(1)
        
        # Metrics
        df['Debit/Lot'] = df['Debit'] / df['lot_size'].replace(0, 1)
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
        # Risk Reward Ratio
        df['RR Ratio'] = np.where(df['Max Loss'] != 0, df['Max Profit'] / df['Max Loss'].abs(), 0)
        
        # Grading Logic
        def get_grade(row):
            s, d = row['Strategy'], row['Debit/Lot']
            reason = "Standard"
            grade = "C"
            if s == '130/160':
                if d > 4800: grade="F"; reason="Overpriced (> $4.8k)"
                elif 3500 <= d <= 4500: grade="A+"; reason="Sweet Spot"
                else: grade="B"; reason="Acceptable"
            elif s == '160/190':
                if 4800 <= d <= 5500: grade="A"; reason="Ideal Pricing"
                else: grade="C"; reason="Check Pricing"
            elif s == 'M200':
                if 7500 <= d <= 8500: grade="A"; reason="Perfect Entry"
                else: grade="B"; reason="Variance"
            return pd.Series([grade, reason])

        df[['Grade', 'Reason']] = df.apply(get_grade, axis=1)
        
    return df

def load_snapshots():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        # Explicit column selection to avoid ID clashes
        q = """
        SELECT s.snapshot_date, s.pnl, s.days_held, s.theta, s.delta, s.gamma, s.vega, s.iv,
               t.strategy, t.name, t.id as trade_id
        FROM snapshots s
        JOIN trades t ON s.trade_id = t.id
        """
        df = pd.read_sql(q, conn)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        
        for c in ['pnl', 'days_held', 'theta', 'delta', 'gamma', 'vega', 'iv']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- INITIALIZE ---
init_db()

# --- SIDEBAR ---
st.sidebar.markdown("### 🚦 Daily Workflow")

# 1. RESTORE
with st.sidebar.expander("1. 🟢 STARTUP (Restore)", expanded=False):
    restore = st.file_uploader("Upload .db file", type=['db'], key='restore')
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
        st.success("Database restored!")
        check_schema_updates() # Run migration on restored DB
        st.rerun()

# 2. SYNC
with st.sidebar.expander("2. 🔵 WORK (Sync Files)", expanded=True):
    st.markdown("**📅 Set Data Date**")
    snap_date = st.date_input("File Date", datetime.now(), label_visibility="collapsed")
    
    active_up = st.file_uploader("Active Trades", accept_multiple_files=True, key="act")
    history_up = st.file_uploader("History (Closed)", accept_multiple_files=True, key="hist")
    
    if st.button("🔄 Process New Data"):
        logs = []
        if active_up: logs.extend(sync_data(active_up, "Active", snap_date))
        if history_up: logs.extend(sync_data(history_up, "History", snap_date))
        
        if logs:
            for l in logs: st.write(l)
            st.success("Trades Updated!")
            st.rerun()

# 3. BACKUP
with st.sidebar.expander("3. 🔴 SHUTDOWN (Backup)", expanded=True):
    with open(DB_NAME, "rb") as f:
        st.download_button("💾 Save Database", f, "trade_guardian_v80.db", "application/x-sqlite3")

st.sidebar.divider()

# SETTINGS
st.sidebar.header("⚙️ Strategy Settings")
market_regime = st.sidebar.selectbox(
    "Current Market Regime", 
    ["Neutral (Standard)", "Bullish (Aggr. Targets)", "Bearish (Safe Targets)"],
    index=0
)
regime_mult = 1.0
if "Bullish" in market_regime: regime_mult = 1.10
if "Bearish" in market_regime: regime_mult = 0.90

# --- SMART EXIT ENGINE ---
def get_action_signal(strat, status, days_held, pnl, benchmarks_dict):
    action = ""
    signal_type = "NONE" 
    
    if status == "Active":
        benchmark = benchmarks_dict.get(strat, {})
        base_target = benchmark.get('pnl', 0)
        if base_target == 0: base_target = BASE_CONFIG.get(strat, {}).get('pnl', 9999)
        final_target = base_target * regime_mult
            
        if pnl >= final_target:
            return f"TAKE PROFIT (Hit ${final_target:,.0f})", "SUCCESS"

        if strat == '130/160':
            if 25 <= days_held <= 35 and pnl < 100:
                return "KILL (Stale >25d)", "ERROR"
            
        elif strat == '160/190':
            if days_held < 30:
                return "COOKING (Do Not Touch)", "INFO"
            elif 30 <= days_held <= 40:
                return "WATCH (Profit Zone)", "WARNING"

        elif strat == 'M200':
            if 12 <= days_held <= 16:
                if pnl > 200: return "DAY 14 CHECK (Green)", "SUCCESS"
                else: return "DAY 14 CHECK (Red)", "WARNING"
                
    return action, signal_type

# --- MAIN APP ---
df = load_data()

benchmarks = BASE_CONFIG.copy()
if not df.empty:
    expired_df = df[df['Status'] == 'Expired']
    if not expired_df.empty:
        hist_grp = expired_df.groupby('Strategy')
        for strat, grp in hist_grp:
            winners = grp[grp['P&L'] > 0]
            if not winners.empty:
                benchmarks[strat] = {
                    'yield': grp['Daily Yield %'].mean(),
                    'pnl': winners['P&L'].mean(),
                    'roi': winners['ROI'].mean(),
                    'dit': winners['Days Held'].mean()
                }

tab1, tab2, tab3, tab4 = st.tabs(["📊 Active Dashboard", "🧪 Trade Validator", "📈 Analytics", "📖 Rule Book"])

# 1. ACTIVE DASHBOARD
with tab1:
    if not df.empty:
        active_df = df[df['Status'] == 'Active'].copy()
        
        if active_df.empty:
            st.info("📭 No active trades.")
        else:
            # Generate Signals
            act_list = []
            sig_list = []
            for _, row in active_df.iterrows():
                act, sig = get_action_signal(row['Strategy'], row['Status'], row['Days Held'], row['P&L'], benchmarks)
                act_list.append(act)
                sig_list.append(sig)
                
            active_df['Action'] = act_list
            active_df['Signal_Type'] = sig_list
            
            strat_tabs = st.tabs(["📋 Overview", "🔹 130/160", "🔸 160/190", "🐳 M200"])
            
            # Expanded Columns for Display
            cols = ['Name', 'Action', 'Grade', 'Daily Yield %', 'P&L', 'Debit', 'Days Held', 
                    'Theta', 'Delta', 'IV', 'PoP', 'Notes']

            def render_tab(tab, strategy_name):
                with tab:
                    subset = active_df[active_df['Strategy'] == strategy_name].copy()
                    
                    # ACTION CENTER
                    urgent = subset[subset['Action'] != ""]
                    if not urgent.empty:
                        st.markdown(f"**🚨 Action Center ({len(urgent)})**")
                        for _, row in urgent.iterrows():
                            color = {"SUCCESS":"#4caf50", "ERROR":"#f44336", "WARNING":"#ff9800", "INFO":"#2196f3"}.get(row['Signal_Type'], "#9e9e9e")
                            st.markdown(f"* <span style='color: {color}'>**{row['Name']}**: {row['Action']}</span>", unsafe_allow_html=True)
                        st.divider()

                    # METRICS
                    bench = benchmarks.get(strategy_name, BASE_CONFIG.get(strategy_name))
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Hist. Avg Win", f"${bench['pnl']:,.0f}")
                    c2.metric("Target Profit", f"${bench['pnl']*regime_mult:,.0f}")
                    if not subset.empty:
                        avg_pop = subset['PoP'].mean() * 100
                        c3.metric("Avg IV", f"{subset['IV'].mean():.1f}%")
                        c4.metric("Avg Win Chance", f"{avg_pop:.1f}%")

                    # TABLE
                    if not subset.empty:
                        # Format PoP to %
                        disp_sub = subset.copy()
                        disp_sub['PoP'] = disp_sub['PoP'] * 100
                        
                        st.dataframe(
                            disp_sub[cols].style
                            .format({
                                'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%",
                                'Theta': "{:.1f}", 'Delta': "{:.1f}", 'IV': "{:.1f}%", 'PoP': "{:.0f}%",
                                'Days Held': "{:.0f}"
                            })
                            .map(lambda v: 'background-color: #d1e7dd; color: #0f5132' if 'TAKE PROFIT' in str(v) else 'background-color: #f8d7da; color: #842029' if 'KILL' in str(v) else '', subset=['Action']),
                            use_container_width=True
                        )
                    else:
                        st.info(f"No active {strategy_name} trades.")

            # OVERVIEW TAB
            with strat_tabs[0]:
                st.markdown("#### Portfolio Health")
                total_delta = active_df['Delta'].sum()
                total_theta = active_df['Theta'].sum()
                total_iv = active_df['IV'].mean()
                
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Net Delta", f"{total_delta:,.1f}")
                c2.metric("Daily Theta", f"${total_theta:,.0f}")
                c3.metric("Avg Portfolio IV", f"{total_iv:.2f}%")
                c4.metric("Capital at Risk", f"${active_df['Debit'].sum():,.0f}")
                
                st.dataframe(active_df[['Strategy', 'Name', 'P&L', 'Daily Yield %', 'Action']].sort_values('P&L'), use_container_width=True)

            render_tab(strat_tabs[1], '130/160')
            render_tab(strat_tabs[2], '160/190')
            render_tab(strat_tabs[3], 'M200')
    else:
        st.info("Start by syncing data in the sidebar.")

# 2. VALIDATOR (Updated with PoP/Max Loss)
with tab2:
    st.markdown("### 🧪 Pre-Flight Audit")
    model_file = st.file_uploader("Upload Model File (Check potential trade)", key="mod")
    if model_file:
        m_df = pd.DataFrame()
        try:
            m_raw = read_file_safely(model_file)
            if m_raw is not None:
                row = m_raw.iloc[0]
                strat = get_strategy(row.get('Group', ''), row.get('Name', ''))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                max_loss = clean_num(row.get('Max Loss', 0))
                max_profit = clean_num(row.get('Max Profit', 0))
                pop = clean_num(row.get('Chance', 0))
                
                # Grading Logic
                lot_size = 1
                if strat == '130/160' and debit > 6000: lot_size = 2
                debit_lot = debit / max(1, lot_size)
                
                grade = "C"
                if strat == '130/160':
                    if debit_lot > 4800: grade="F"
                    elif 3500 <= debit_lot <= 4500: grade="A+"
                    else: grade="B"
                elif strat == 'M200':
                    if 7500 <= debit_lot <= 8500: grade="A"
                    else: grade="B"

                st.divider()
                st.subheader(f"Audit: {row.get('Name')}")
                
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Strategy", strat)
                c2.metric("Debit/Lot", f"${debit_lot:,.0f}")
                c3.metric("Max Risk", f"${max_loss:,.0f}")
                c4.metric("Prob. of Profit", f"{pop*100:.1f}%")
                
                if max_loss != 0:
                    rr = max_profit / abs(max_loss)
                    st.metric("Risk/Reward Ratio", f"1 : {rr:.2f}")

                if "A" in grade:
                    st.success(f"✅ APPROVED (Grade {grade})")
                elif "F" in grade:
                    st.error(f"⛔ REJECT (Grade {grade}) - Too Expensive")
                else:
                    st.warning(f"⚠️ CAUTION (Grade {grade})")

        except Exception as e:
            st.error(f"Error reading model: {e}")

# 3. ANALYTICS
with tab3:
    if not df.empty:
        # Date Filter
        min_date, max_date = df['Entry Date'].min(), df['Entry Date'].max()
        date_range = st.date_input("Filter Data Range", [min_date, max_date])
        filtered_df = df
        if len(date_range) == 2:
            filtered_df = df[(df['Entry Date'] >= pd.to_datetime(date_range[0])) & (df['Entry Date'] <= pd.to_datetime(date_range[1]))]
            
        expired_sub = filtered_df[filtered_df['Status'] == 'Expired'].copy()
        
        an1, an2, an3, an4, an5, an6 = st.tabs(["🌊 Equity Curve", "🎯 Expectancy", "🔥 Heatmaps", "🎲 Win Probability", "🧬 Lifecycle", "🧮 Greeks Lab"])
        
        # 1. Equity Curve
        with an1:
            if not expired_sub.empty:
                ec_df = expired_sub.sort_values("Exit Date").copy()
                ec_df['Cumulative P&L'] = ec_df['P&L'].cumsum()
                fig = px.line(ec_df, x='Exit Date', y='Cumulative P&L', title="Account Growth", markers=True)
                st.plotly_chart(fig, use_container_width=True)
                
        # 4. Probability Calibration (NEW)
        with an4:
            if not expired_sub.empty and 'PoP' in expired_sub.columns:
                st.markdown("#### Probability Reality Check")
                st.caption("Does a 70% 'Chance' actually win 70% of the time?")
                
                # Bucketing PoP
                expired_sub['PoP_Bucket'] = (expired_sub['PoP'] * 10).astype(int) * 10
                calib = expired_sub.groupby('PoP_Bucket').apply(lambda x: (x['P&L'] > 0).mean() * 100).reset_index(name='Actual Win Rate')
                
                fig = px.bar(calib, x='PoP_Bucket', y='Actual Win Rate', 
                             title="Theoretical 'Chance' vs Actual Win Rate",
                             labels={'PoP_Bucket': 'OptionStrat Probability %', 'Actual Win Rate': 'Realized Win %'})
                fig.add_shape(type="line", x0=0, y0=0, x1=100, y1=100, line=dict(color="Red", dash="dash"))
                st.plotly_chart(fig, use_container_width=True)
        
        # 5. Lifecycle (FIXED)
        with an5:
            snaps = load_snapshots()
            if not snaps.empty:
                sel_strat = st.selectbox("Select Strategy", snaps['strategy'].unique(), key='lc_strat')
                strat_snaps = snaps[snaps['strategy'] == sel_strat]
                
                # Filter to ensure we don't plot empty lines
                if not strat_snaps.empty:
                    fig = px.line(
                        strat_snaps, x='days_held', y='pnl', 
                        color='name', 
                        line_group='trade_id', 
                        markers=True, # Critical for single-day data points
                        title=f"Trade Lifecycle: {sel_strat}",
                        hover_data=['name', 'snapshot_date']
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No data for this strategy.")
            else:
                st.info("Lifecycle builds over time. Sync active trades daily.")

        # 6. Greeks Lab (FIXED & EXPANDED)
        with an6:
            snaps = load_snapshots()
            if not snaps.empty:
                c1, c2 = st.columns(2)
                g_strat = c1.selectbox("Strategy", snaps['strategy'].unique(), key='gp_strat')
                g_metric = c2.selectbox("Select Metric", ['iv', 'theta', 'delta', 'gamma', 'vega', 'pnl'], key='gp_met')
                
                sub_snaps = snaps[snaps['strategy'] == g_strat]
                sub_snaps = sub_snaps.dropna(subset=[g_metric])
                
                if not sub_snaps.empty:
                    fig = px.line(sub_snaps, x='days_held', y=g_metric, color='name', line_group='trade_id',
                                  markers=True, title=f"Evolution of {g_metric.upper()}")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No data found for this metric.")
