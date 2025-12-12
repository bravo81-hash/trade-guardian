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
st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v4.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # TRADES TABLE
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
                    notes TEXT
                )''')
    
    # SNAPSHOTS TABLE
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT,
                    snapshot_date DATE,
                    pnl REAL,
                    days_held INTEGER,
                    FOREIGN KEY(trade_id) REFERENCES trades(id)
                )''')
                
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
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
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    try: return float(str(x).replace('$', '').replace(',', ''))
    except: return 0.0

def safe_fmt(val, fmt_str):
    try:
        if isinstance(val, (int, float)): return fmt_str.format(val)
        return str(val)
    except: return str(val)

def generate_id(name, strategy, entry_date):
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{name}_{strategy}_{d_str}".replace(" ", "").replace("/", "-")

def extract_ticker(name):
    try:
        parts = str(name).split(' ')
        if parts:
            ticker = parts[0].replace('.', '').upper()
            if ticker in ['M200', '130', '160', 'IRON', 'VERTICAL']:
                return "UNKNOWN"
            return ticker
        return "UNKNOWN"
    except: return "UNKNOWN"

# --- SMART FILE READER ---
def read_file_safely(file):
    try:
        if file.name.endswith('.xlsx') or file.name.endswith('.xls'):
            df_raw = pd.read_excel(file, header=None, engine='openpyxl')
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
        else:
            content = file.getvalue().decode("utf-8")
            lines = content.split('\n')
            header_row = 0
            for i, line in enumerate(lines[:20]):
                if "Name" in line and "Total Return" in line:
                    header_row = i
                    break
            file.seek(0)
            return pd.read_csv(file, skiprows=header_row)
    except Exception as e:
        return None

# --- SYNC ENGINE ---
def sync_data(file_list, file_type):
    log = []
    if not isinstance(file_list, list): file_list = [file_list]
    
    conn = get_db_connection()
    c = conn.cursor()
    
    count_new = 0
    count_update = 0
    
    for file in file_list:
        try:
            df = read_file_safely(file)
            if df is None or df.empty:
                log.append(f"⚠️ Skipped {file.name} (Empty/Invalid)")
                continue

            for _, row in df.iterrows():
                name = str(row.get('Name', ''))
                if name.startswith('.') or name in ['nan', '', 'Symbol']: continue
                
                created = row.get('Created At', '')
                try: start_dt = pd.to_datetime(created)
                except: continue
                
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                theta = clean_num(row.get('Theta', 0))
                delta = clean_num(row.get('Delta', 0))
                gamma = clean_num(row.get('Gamma', 0))
                vega = clean_num(row.get('Vega', 0))
                
                lot_size = 1
                if strat == '130/160' and debit > 6000: lot_size = 2
                elif strat == '130/160' and debit > 10000: lot_size = 3
                elif strat == '160/190' and debit > 8000: lot_size = 2
                elif strat == 'M200' and debit > 12000: lot_size = 2

                trade_id = generate_id(name, strat, start_dt)
                status = "Active" if file_type == "Active" else "Expired"
                
                exit_dt = None
                days_held = 1
                
                if file_type == "History":
                    try:
                        exit_dt = pd.to_datetime(row.get('Expiration'))
                        days_held = (exit_dt - start_dt).days
                    except: days_held = 1
                else:
                    days_held = (datetime.now() - start_dt).days
                
                if days_held < 1: days_held = 1
                
                c.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
                if existing is None:
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, name, strat, status, start_dt.date(), 
                         exit_dt.date() if exit_dt else None, 
                         days_held, debit, lot_size, pnl, theta, delta, gamma, vega, ""))
                    count_new += 1
                else:
                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=? 
                            WHERE id=?''', 
                            (pnl, status, exit_dt.date() if exit_dt else None, days_held, theta, delta, gamma, vega, trade_id))
                        count_update += 1
                    elif existing[0] == "Active":
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=? 
                            WHERE id=?''', 
                            (pnl, days_held, theta, delta, gamma, vega, trade_id))
                        count_update += 1
                        
                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    if not c.fetchone():
                        c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held) VALUES (?,?,?,?)",
                                  (trade_id, today, pnl, days_held))

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
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes'
        })
        
        for col in ['Gamma', 'Vega', 'Theta', 'Delta']:
            if col not in df.columns:
                df[col] = 0.0
        
        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        df['Debit'] = df['Debit'].fillna(0)
        df['P&L'] = df['P&L'].fillna(0)
        
        df['Debit/Lot'] = df['Debit'] / df['lot_size'].replace(0, 1)
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = df['ROI'] / df['Days Held'].replace(0, 1)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
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
        df['Latest'] = True 
        
    return df

def load_snapshots():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        # PULLING BOTH NAME AND ID TO ENSURE CHART WORKS
        q = """
        SELECT s.snapshot_date, s.pnl, s.days_held, t.strategy, t.name, t.id
        FROM snapshots s
        JOIN trades t ON s.trade_id = t.id
        """
        df = pd.read_sql(q, conn)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- INITIALIZE DB ---
init_db()

# --- SIDEBAR: WORKFLOW WIZARD ---
st.sidebar.markdown("### 🚦 Daily Workflow")

# STEP 1: RESTORE
with st.sidebar.expander("1. 🟢 STARTUP (Restore)", expanded=True):
    st.caption("Doing this first avoids amnesia!")
    restore = st.file_uploader("Upload .db file", type=['db'], key='restore')
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
        
        # Validation
        conn = get_db_connection()
        count = pd.read_sql("SELECT count(*) as c FROM trades", conn).iloc[0]['c']
        conn.close()
        
        if count == 0:
            st.warning(f"Brain Loaded, but it is EMPTY (0 trades).")
        else:
            st.success(f"Brain Loaded! Found {count} trades.")
        
        if 'restored' not in st.session_state:
            st.session_state['restored'] = True
            st.rerun()

st.sidebar.markdown("⬇️ *then...*")

# STEP 2: SYNC
with st.sidebar.expander("2. 🔵 WORK (Sync Files)", expanded=True):
    st.caption("Feed today's broker exports.")
    active_up = st.file_uploader("Active Trades", accept_multiple_files=True, key="act")
    history_up = st.file_uploader("History (Closed)", accept_multiple_files=True, key="hist")
    
    if st.button("🔄 Process New Data"):
        logs = []
        if active_up: logs.extend(sync_data(active_up, "Active"))
        if history_up: logs.extend(sync_data(history_up, "History"))
        
        if logs:
            for l in logs: st.write(l)
            st.success("Trades Updated!")
            st.rerun()

st.sidebar.markdown("⬇️ *finally...*")

# STEP 3: BACKUP
with st.sidebar.expander("3. 🔴 SHUTDOWN (Backup)", expanded=True):
    st.caption("Save state before leaving.")
    with open(DB_NAME, "rb") as f:
        st.download_button("💾 Save Database File", f, "trade_guardian_v4.db", "application/x-sqlite3")

st.sidebar.divider()

# STRATEGY SETTINGS
st.sidebar.header("⚙️ Strategy Settings")
market_regime = st.sidebar.selectbox(
    "Current Market Regime", 
    ["Neutral (Standard)", "Bullish (Aggr. Targets)", "Bearish (Safe Targets)"],
    index=0,
    help="Bullish: +10% Profit Target | Bearish: -10% Profit Target"
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
        
        if base_target == 0: 
            base_target = BASE_CONFIG.get(strat, {}).get('pnl', 9999)
            
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

# TABS
tab1, tab2, tab3, tab4 = st.tabs(["📊 Active Dashboard", "🧪 Trade Validator", "📈 Analytics", "📖 Rule Book"])

# 1. ACTIVE DASHBOARD
with tab1:
    if not df.empty:
        active_df = df[df['Status'] == 'Active'].copy()
        
        if active_df.empty:
            st.info("📭 No active trades in database. Go to Step 2 (Work) in the sidebar.")
        else:
            port_yield = active_df['Daily Yield %'].mean()
            if port_yield < 0.10:
                st.sidebar.error(f"🚨 Yield Critical: {port_yield:.2f}%")
            elif port_yield < 0.15:
                st.sidebar.warning(f"⚠️ Yield Low: {port_yield:.2f}%")
            else:
                st.sidebar.success(f"✅ Yield Healthy: {port_yield:.2f}%")

            act_list = []
            sig_list = []
            for _, row in active_df.iterrows():
                act, sig = get_action_signal(
                    row['Strategy'], row['Status'], row['Days Held'], row['P&L'], benchmarks
                )
                act_list.append(act)
                sig_list.append(sig)
                
            active_df['Action'] = act_list
            active_df['Signal_Type'] = sig_list
            
            st.markdown("### 🏛️ Active Trades by Strategy")
            
            strat_tabs = st.tabs(["📋 Strategy Overview", "🔹 130/160", "🔸 160/190", "🐳 M200"])
            
            cols = ['Name', 'Action', 'Grade', 'Daily Yield %', 'P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'Notes']

            def render_tab(tab, strategy_name):
                with tab:
                    subset = active_df[active_df['Strategy'] == strategy_name].copy()
                    bench = benchmarks.get(strategy_name, BASE_CONFIG.get(strategy_name))
                    target_disp = bench['pnl'] * regime_mult
                    
                    # --- ACTION CENTER ---
                    urgent = subset[subset['Action'] != ""]
                    if not urgent.empty:
                        st.markdown(f"**🚨 Action Center ({len(urgent)})**")
                        action_lines = []
                        for _, row in urgent.iterrows():
                            sig = row['Signal_Type']
                            color_map = {"SUCCESS":"#4caf50", "ERROR":"#f44336", "WARNING":"#ff9800", "INFO":"#2196f3", "NONE":"#9e9e9e"}
                            color = color_map.get(sig, "#9e9e9e")
                            line = f"* <span style='color: {color}'>**{row['Name']}**: {row['Action']}</span>"
                            action_lines.append(line)
                        st.markdown("\n".join(action_lines), unsafe_allow_html=True)
                        st.divider()

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Hist. Avg Win", f"${bench['pnl']:,.0f}")
                    c2.metric("Target Yield", f"{bench['yield']:.2f}%/d")
                    c3.metric("Target Profit", f"${target_disp:,.0f}")
                    c4.metric("Avg Hold", f"{bench['dit']:.0f}d")
                    
                    if not subset.empty:
                        sum_row = pd.DataFrame({
                            'Name': ['TOTAL'], 'Action': ['-'], 'Grade': ['-'],
                            'Daily Yield %': [subset['Daily Yield %'].mean()],
                            'P&L': [subset['P&L'].sum()], 'Debit': [subset['Debit'].sum()],
                            'Days Held': [subset['Days Held'].mean()],
                            'Theta': [subset['Theta'].sum()], 'Delta': [subset['Delta'].sum()],
                            'Gamma': [subset['Gamma'].sum()], 'Vega': [subset['Vega'].sum()],
                            'Notes': ['']
                        })
                        
                        display_df = pd.concat([subset[cols], sum_row], ignore_index=True)
                        
                        st.dataframe(
                            display_df.style
                            .format({
                                'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%",
                                'Theta': "{:.1f}", 'Delta': "{:.1f}", 'Gamma': "{:.2f}", 'Vega': "{:.0f}",
                                'Days Held': "{:.0f}"
                            })
                            .map(lambda v: 'background-color: #d1e7dd; color: #0f5132; font-weight: bold' if 'TAKE PROFIT
