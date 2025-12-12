import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
import time
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- DEBUG BANNER ---
st.info("✅ RESTORED: v76.0 UI (Action Signals) + v81 Engine (Greeks Data)")

st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE ENGINE (v81 Schema) ---
DB_NAME = "trade_guardian_v81.db"

def get_db_connection():
    """Robust connection with retry."""
    retries = 5
    for i in range(retries):
        try:
            return sqlite3.connect(DB_NAME, timeout=10)
        except sqlite3.OperationalError:
            time.sleep(0.1)
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # TRADES TABLE (Extended for new data)
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
                    rho REAL,
                    iv REAL,
                    pop REAL,
                    notes TEXT
                )''')
    
    # SNAPSHOTS TABLE (Extended for Greeks Lifecycle)
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
                    rho REAL,
                    iv REAL,
                    FOREIGN KEY(trade_id) REFERENCES trades(id)
                )''')
                
    # Auto-Migration (If restoring old DB)
    try:
        c.execute("SELECT rho FROM trades LIMIT 1")
    except:
        cols = ['rho', 'iv', 'pop']
        for col in cols:
            try: c.execute(f"ALTER TABLE trades ADD COLUMN {col} REAL")
            except: pass
            
    try:
        c.execute("SELECT theta FROM snapshots LIMIT 1")
    except:
        cols = ['theta', 'delta', 'gamma', 'vega', 'rho', 'iv']
        for col in cols:
            try: c.execute(f"ALTER TABLE snapshots ADD COLUMN {col} REAL")
            except: pass

    conn.commit()
    conn.close()

# --- CONFIGURATION (v76 Defaults) ---
BASE_CONFIG = {
    '130/160': {'yield': 0.13, 'pnl': 500, 'roi': 6.8, 'dit': 36},
    '160/190': {'yield': 0.28, 'pnl': 700, 'roi': 12.7, 'dit': 44},
    'M200':    {'yield': 0.56, 'pnl': 900, 'roi': 11.1, 'dit': 41}
}

# --- HELPER FUNCTIONS ---
def get_strategy(group_name, trade_name=""):
    """Smarter Strategy Detection"""
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    try: return float(str(x).replace('$', '').replace(',', '').replace('%',''))
    except: return 0.0

def safe_fmt(val, fmt_str):
    try:
        if isinstance(val, (int, float)): return fmt_str.format(val)
        return str(val)
    except: return str(val)

def generate_id(name, strategy, entry_date):
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    # Simple hash to avoid special char issues in IDs
    clean_name = "".join(c for c in str(name) if c.isalnum())
    return f"{strategy}_{d_str}_{clean_name}"[:50]

def extract_ticker(name):
    try:
        parts = str(name).split(' ')
        if parts:
            ticker = parts[0].replace('.', '').upper()
            if ticker in ['M200', '130', '160', 'IRON', 'VERTICAL', 'MAR', 'APR', 'MAY', 'JUN']:
                return "SPX" # Assume SPX for most
            return ticker
        return "UNKNOWN"
    except: return "UNKNOWN"

# --- SMART FILE READER (v80 Engine) ---
def read_file_safely(file):
    try:
        # Try Excel logic
        if file.name.endswith('.xlsx') or file.name.endswith('.xls'):
            df_raw = pd.read_excel(file, header=None, engine='openpyxl')
            header_idx = -1
            for i, row in df_raw.head(25).iterrows():
                row_str = " ".join(row.astype(str).values)
                if "Name" in row_str and "Total Return" in row_str:
                    header_idx = i
                    break
            if header_idx != -1:
                df = df_raw.iloc[header_idx+1:].copy()
                df.columns = df_raw.iloc[header_idx]
                return df
        
        # Try CSV logic
        file.seek(0)
        content = file.getvalue().decode("utf-8", errors='replace')
        lines = content.split('\n')
        header_row = 0
        for i, line in enumerate(lines[:25]):
            if "Name" in line and "Total Return" in line:
                header_row = i
                break
        file.seek(0)
        return pd.read_csv(file, skiprows=header_row)
    except Exception as e:
        return None

# --- SYNC ENGINE (v81 Upgraded) ---
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
                except: start_dt = datetime.now()
                
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                
                # New Metrics
                theta = clean_num(row.get('Theta', 0))
                delta = clean_num(row.get('Delta', 0))
                gamma = clean_num(row.get('Gamma', 0))
                vega = clean_num(row.get('Vega', 0))
                rho = clean_num(row.get('Rho', 0))
                iv = clean_num(row.get('IV', 0)) * 100
                pop = clean_num(row.get('Chance', 0)) * 100
                
                # Auto Lot Size Estimation
                lot_size = 1
                if strat == '130/160':
                    if debit > 9000: lot_size = 3
                    elif debit > 5500: lot_size = 2
                elif strat == '160/190':
                    if debit > 8000: lot_size = 2
                elif strat == 'M200':
                    if debit > 12000: lot_size = 2

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
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, 
                         theta, delta, gamma, vega, rho, iv, pop, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, name, strat, status, start_dt.date(), 
                         exit_dt.date() if exit_dt else None, 
                         days_held, debit, lot_size, pnl, theta, delta, gamma, vega, rho, iv, pop, ""))
                    count_new += 1
                else:
                    # Update Logic
                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, rho=?, iv=?, pop=?
                            WHERE id=?''', 
                            (pnl, status, exit_dt.date() if exit_dt else None, days_held, theta, delta, gamma, vega, rho, iv, pop, trade_id))
                        count_update += 1
                    elif existing[0] == "Active":
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, rho=?, iv=?, pop=?
                            WHERE id=?''', 
                            (pnl, days_held, theta, delta, gamma, vega, rho, iv, pop, trade_id))
                        count_update += 1
                        
                # SNAPSHOTS (Greeks History)
                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    if not c.fetchone():
                        c.execute('''INSERT INTO snapshots 
                            (trade_id, snapshot_date, pnl, days_held, theta, delta, gamma, vega, rho, iv) 
                            VALUES (?,?,?,?,?,?,?,?,?,?)''',
                            (trade_id, today, pnl, days_held, theta, delta, gamma, vega, rho, iv))

            log.append(f"✅ {file.name}: {count_new} New, {count_update} Updated")
            
        except Exception as e:
            log.append(f"❌ Error {file.name}: {str(e)}")
            
    conn.commit()
    conn.close()
    return log

# --- DATA LOADER (v76.0 Compatible) ---
def load_data():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
    except Exception as e:
        return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        # RENAME TO MATCH v76 UI EXPECTATIONS
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes',
            'iv': 'IV', 'pop': 'Chance', 'rho': 'Rho' # New cols
        })
        
        # Fill Missing
        cols = ['Gamma', 'Vega', 'Theta', 'Delta', 'P&L', 'Debit', 'lot_size', 'IV', 'Chance']
        for col in cols:
            if col not in df.columns: df[col] = 0.0
        
        # Fix Types
        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        df['Debit'] = pd.to_numeric(df['Debit'], errors='coerce').fillna(0)
        df['P&L'] = pd.to_numeric(df['P&L'], errors='coerce').fillna(0)
        df['Days Held'] = pd.to_numeric(df['Days Held'], errors='coerce').fillna(1)
        
        # Derived
        df['Debit/Lot'] = df['Debit'] / df['lot_size'].replace(0, 1)
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
        # v76 GRADING LOGIC
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

def load_snapshots_for_chart(trade_id=None):
    conn = get_db_connection()
    query = "SELECT * FROM snapshots"
    if trade_id: query += f" WHERE trade_id = '{trade_id}' ORDER BY snapshot_date"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- INITIALIZE DB ---
init_db()

# --- SIDEBAR: WORKFLOW WIZARD (RESTORED v76) ---
st.sidebar.markdown("### 🚦 Daily Workflow")

# STEP 1: RESTORE
with st.sidebar.expander("1. 🟢 STARTUP (Restore)", expanded=True):
    st.caption("Doing this first avoids amnesia!")
    restore = st.file_uploader("Upload .db file", type=['db'], key='restore')
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
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
        for l in logs: st.write(l)
        if logs: st.success("Trades Updated!"); time.sleep(1); st.rerun()

st.sidebar.markdown("⬇️ *finally...*")

# STEP 3: BACKUP
with st.sidebar.expander("3. 🔴 SHUTDOWN (Backup)", expanded=True):
    st.caption("Save state before leaving.")
    if os.path.exists(DB_NAME):
        with open(DB_NAME, "rb") as f:
            st.download_button("💾 Save Database File", f, "trade_guardian_v81.db", "application/x-sqlite3")

st.sidebar.divider()

# STRATEGY SETTINGS
st.sidebar.header("⚙️ Strategy Settings")
market_regime = st.sidebar.selectbox(
    "Current Market Regime", 
    ["Neutral (Standard)", "Bullish (Aggr. Targets)", "Bearish (Safe Targets)"],
    index=0
)

regime_mult = 1.0
if "Bullish" in market_regime: regime_mult = 1.10
if "Bearish" in market_regime: regime_mult = 0.90

# --- SMART EXIT ENGINE (RESTORED v76) ---
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

# TABS (v76 Structure)
tab1, tab2, tab3, tab4 = st.tabs(["📊 Active Dashboard", "🧪 Trade Validator", "📈 Analytics", "📖 Rule Book"])

# 1. ACTIVE DASHBOARD
with tab1:
    if not df.empty:
        active_df = df[df['Status'] == 'Active'].copy()
        
        if active_df.empty:
            st.info("📭 No active trades in database.")
        else:
            port_yield = active_df['Daily Yield %'].mean()
            if port_yield < 0.10: st.sidebar.error(f"🚨 Yield Critical: {port_yield:.2f}%")
            elif port_yield < 0.15: st.sidebar.warning(f"⚠️ Yield Low: {port_yield:.2f}%")
            else: st.sidebar.success(f"✅ Yield Healthy: {port_yield:.2f}%")

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
            
            # Added IV and Chance to Display Cols
            cols = ['Name', 'Action', 'Grade', 'Daily Yield %', 'P&L', 'Debit', 'Days Held', 'IV', 'Chance', 'Theta', 'Delta', 'Notes']

            def render_tab(tab, strategy_name):
                with tab:
                    subset = active_df[active_df['Strategy'] == strategy_name].copy()
                    bench = benchmarks.get(strategy_name, BASE_CONFIG.get(strategy_name))
                    target_disp = bench['pnl'] * regime_mult
                    
                    # --- ACTION CENTER (v76 RESTORED) ---
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
                        st.dataframe(
                            subset[cols].style
                            .format({
                                'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%",
                                'Theta': "{:.1f}", 'Delta': "{:.1f}", 
                                'IV': "{:.1f}%", 'Chance': "{:.1f}%",
                                'Days Held': "{:.0f}"
                            })
                            .map(lambda v: 'background-color: #d1e7dd; color: #0f5132; font-weight: bold' if 'TAKE PROFIT' in str(v) 
                                           else 'background-color: #f8d7da; color: #842029; font-weight: bold' if 'KILL' in str(v) 
                                           else '', subset=['Action'])
                            .map(lambda v: 'color: #0f5132; font-weight: bold' if 'A' in str(v) 
                                           else 'color: #842029; font-weight: bold' if 'F' in str(v) 
                                           else '', subset=['Grade']),
                            use_container_width=True
                        )
                    else:
                        st.info("No active trades.")

            with strat_tabs[0]:
                with st.expander("📊 Portfolio Risk Metrics", expanded=True):
                    total_delta = active_df['Delta'].sum()
                    total_theta = active_df['Theta'].sum()
                    total_cap = active_df['Debit'].sum()
                    r1, r2, r3 = st.columns(3)
                    r1.metric("Net Delta", f"{total_delta:,.1f}", delta="Bullish" if total_delta > 0 else "Bearish")
                    r2.metric("Daily Theta", f"${total_theta:,.0f}")
                    r3.metric("Capital at Risk", f"${total_cap:,.0f}")

                strat_agg = active_df.groupby('Strategy').agg({
                    'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum', 'Delta': 'sum',
                    'Name': 'count', 'Daily Yield %': 'mean' 
                }).reset_index()
                
                st.dataframe(
                    strat_agg.style.format({'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Theta': "{:,.0f}"}),
                    use_container_width=True
                )

            render_tab(strat_tabs[1], '130/160')
            render_tab(strat_tabs[2], '160/190')
            render_tab(strat_tabs[3], 'M200')
    else:
        st.info("👋 Database is empty. Use the sidebar to Sync your first Active/History file.")

# 2. VALIDATOR (v76 RESTORED)
with tab2:
    st.markdown("### 🧪 Pre-Flight Audit")
    
    with st.expander("ℹ️ Grading System Legend", expanded=True):
        st.markdown("""
        | Strategy | Grade | Debit Range (Per Lot) | Verdict |
        | :--- | :--- | :--- | :--- |
        | **130/160** | **A+** | `$3,500 - $4,500` | ✅ **Sweet Spot** |
        | **130/160** | **F** | `> $4,800` | ⛔ **Overpriced** |
        | **160/190** | **A** | `$4,800 - $5,500` | ✅ **Ideal Pricing** |
        | **M200** | **A** | `$7,500 - $8,500` | ✅ **Perfect Whale** |
        """)
        
    model_file = st.file_uploader("Upload Model File (Check potential trade)", key="mod")
    if model_file:
        try:
            m_raw = read_file_safely(model_file)
            if m_raw is not None and not m_raw.empty:
                row = m_raw.iloc[0]
                name = row.get('Name', 'Unknown')
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                
                # Manual estimation fallback
                lot_size = 1
                if strat == '130/160' and debit > 6000: lot_size = 2
                debit_lot = debit / max(1, lot_size)
                
                st.metric("Debit Per Lot", f"${debit_lot:,.0f}")
                
                # Check Logic
                grade = "C"
                if strat == '130/160':
                    if debit_lot > 4800: grade="F"
                    elif 3500 <= debit_lot <= 4500: grade="A+"
                    else: grade="B"
                elif strat == '160/190':
                    if 4800 <= debit_lot <= 5500: grade="A"
                elif strat == 'M200':
                    if 7500 <= debit_lot <= 8500: grade="A"

                if "A" in grade: st.success(f"✅ APPROVED: Grade {grade}")
                elif "F" in grade: st.error(f"⛔ REJECT: Grade {grade}")
                else: st.warning(f"⚠️ CHECK: Grade {grade}")

        except Exception as e: st.error(f"Error: {e}")

# 3. ANALYTICS (v76 + NEW LIFECYCLE)
with tab3:
    st.subheader("📈 Analytics Suite")
    
    # Sub-tabs within Analytics
    a1, a2, a3 = st.tabs(["Performance", "Seasonality", "🧬 Lifecycle (New)"])
    
    with a1:
        if not df.empty:
            expired_sub = df[df['Status'] == 'Expired'].copy()
            if not expired_sub.empty:
                expired_sub = expired_sub.sort_values("Exit Date")
                expired_sub['Cum PnL'] = expired_sub['P&L'].cumsum()
                fig = px.line(expired_sub, x='Exit Date', y='Cum PnL', title="Equity Curve")
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("No closed trades.")
            
    with a3: # THE REQUESTED FEATURE
        st.markdown("#### Greeks & PnL Lifecycle")
        if not df.empty:
            trades = df['Name'].unique()
            sel_trade = st.selectbox("Select Trade", trades)
            trade_id = df[df['Name'] == sel_trade]['id'].iloc[0]
            
            snaps = load_snapshots_for_chart(trade_id)
            if not snaps.empty:
                # Dual Axis Chart
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(go.Scatter(x=snaps['days_held'], y=snaps['pnl'], name="P&L", line=dict(color='green', width=3)), secondary_y=False)
                fig.add_trace(go.Scatter(x=snaps['days_held'], y=snaps['theta'], name="Theta", line=dict(color='purple', dash='dot')), secondary_y=True)
                fig.add_trace(go.Scatter(x=snaps['days_held'], y=snaps['iv'], name="IV", line=dict(color='orange', dash='dot')), secondary_y=True)
                
                fig.update_layout(title="PnL vs Theta/IV Evolution")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No snapshot history yet. Sync Active trades daily to build this graph.")

# 4. RULE BOOK
with tab4:
    st.markdown("""
    # 📖 Trading Constitution
    ### 1. 130/160 Strategy (Income Engine)
    * **Target Entry:** Monday.
    * **Debit Target:** `$3,500 - $4,500` per lot.
    * **Stop Rule:** Never pay > `$4,800` per lot.
    * **Management:** Kill if trade is **25 days old** and profit is flat/negative.
    
    ### 2. 160/190 Strategy (Compounder)
    * **Target Entry:** Friday.
    * **Debit Target:** `~$5,200` per lot.
    * **Exit:** Hold for **40-50 Days**. Do not touch in first 30 days.
    
    ### 3. M200 Strategy (Whale)
    * **Target Entry:** Wednesday.
    * **Debit Target:** `$7,500 - $8,500` per lot.
    """)
