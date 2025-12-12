import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import re
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- DEBUG BANNER ---
st.info("✅ RUNNING VERSION: v80.6 (Crash-Proof: Auto-Detects Streamlit Version)")

st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v80.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # 1. TRADES
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
                    link TEXT,
                    notes TEXT
                )''')
    
    # 2. SNAPSHOTS
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

    # 3. LEGS
    c.execute('''CREATE TABLE IF NOT EXISTS legs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT,
                    symbol TEXT,
                    quantity INTEGER,
                    strike REAL,
                    type TEXT,
                    FOREIGN KEY(trade_id) REFERENCES trades(id)
                )''')
                
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
    
    # MIGRATIONS
    try: c.execute("ALTER TABLE trades ADD COLUMN link TEXT")
    except: pass
    try: c.execute("ALTER TABLE snapshots ADD COLUMN iv REAL")
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
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    try: return float(str(x).replace('$', '').replace(',', '').replace('%', ''))
    except: return 0.0

def safe_fmt(val, fmt_str):
    try:
        if isinstance(val, (int, float)): return fmt_str.format(val)
        return str(val)
    except: return str(val)

def generate_id(name, strategy, entry_date):
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    safe_name = re.sub(r'[^a-zA-Z0-9]', '', str(name))[:15]
    return f"{safe_name}_{strategy}_{d_str}".replace(" ", "").replace("/", "-")

def extract_ticker(name):
    try:
        parts = str(name).split(' ')
        if parts:
            ticker = parts[0].replace('.', '').upper()
            if ticker in ['M200', '130', '160', 'IRON', 'VERTICAL', 'APR', 'MAR', 'FEB', 'JAN', 'DEC']:
                return "SPX"
            return ticker
        return "UNKNOWN"
    except: return "UNKNOWN"

# --- SMART FILE READER ---
def read_file_safely(file):
    try:
        if file.name.endswith('.xlsx') or file.name.endswith('.xls'):
            try:
                df_raw = pd.read_excel(file, header=None)
            except:
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
def sync_data(file_list, file_type, snapshot_date_override=None):
    log = []
    if not isinstance(file_list, list): file_list = [file_list]
    
    conn = get_db_connection()
    c = conn.cursor()
    
    count_new = 0
    count_update = 0
    count_legs = 0
    
    snap_date = snapshot_date_override if snapshot_date_override else datetime.now().date()
    
    for file in file_list:
        try:
            df = read_file_safely(file)
            if df is None or df.empty:
                log.append(f"⚠️ Skipped {file.name} (Empty)")
                continue

            current_trade_id = None 

            for _, row in df.iterrows():
                name = str(row.get('Name', ''))
                if name in ['nan', '', 'Symbol']: continue
                
                # LEG
                if name.startswith('.'):
                    if current_trade_id:
                        try:
                            qty = clean_num(row.iloc[1]) 
                            match = re.search(r'([CP])(\d+\.?\d*)$', name)
                            if match:
                                l_type = "Call" if match.group(1) == 'C' else "Put"
                                strike = float(match.group(2))
                                
                                c.execute("SELECT id FROM legs WHERE trade_id=? AND strike=? AND type=?", 
                                         (current_trade_id, strike, l_type))
                                if not c.fetchone():
                                    c.execute("INSERT INTO legs (trade_id, symbol, quantity, strike, type) VALUES (?,?,?,?,?)",
                                             (current_trade_id, "SPX", qty, strike, l_type))
                                    count_legs += 1
                        except: pass
                    continue
                
                # TRADE
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
                iv = clean_num(row.get('IV', 0))
                
                raw_link = str(row.get('Link', ''))
                link = raw_link if raw_link.startswith('http') else ''
                
                lot_size = 1
                if strat == '130/160':
                    if debit > 10000: lot_size = 3
                    elif debit > 6000: lot_size = 2
                elif strat == '160/190' and debit > 8000: lot_size = 2
                elif strat == 'M200' and debit > 12000: lot_size = 2

                trade_id = generate_id(name, strat, start_dt)
                current_trade_id = trade_id 
                
                status = "Active" if file_type == "Active" else "Expired"
                
                exit_dt = None
                if file_type == "History":
                    try: exit_dt = pd.to_datetime(row.get('Expiration'))
                    except: pass
                    days_held = (exit_dt - start_dt).days if exit_dt else 1
                else:
                    days_held = (pd.to_datetime(snap_date) - start_dt).days
                
                if days_held < 1: days_held = 1
                
                c.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
                if existing is None:
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, link, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, name, strat, status, start_dt.date(), 
                         exit_dt.date() if exit_dt else None, 
                         days_held, debit, lot_size, pnl, theta, delta, gamma, vega, link, ""))
                    count_new += 1
                else:
                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, link=?
                            WHERE id=?''', 
                            (pnl, status, exit_dt.date() if exit_dt else None, days_held, theta, delta, gamma, vega, link, trade_id))
                        count_update += 1
                    elif existing[0] == "Active":
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, link=?
                            WHERE id=?''', 
                            (pnl, days_held, theta, delta, gamma, vega, link, trade_id))
                        count_update += 1
                        
                if file_type == "Active":
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, snap_date))
                    if not c.fetchone():
                        c.execute('''INSERT INTO snapshots 
                            (trade_id, snapshot_date, pnl, days_held, theta, delta, gamma, vega, iv) 
                            VALUES (?,?,?,?,?,?,?,?,?)''',
                            (trade_id, snap_date, pnl, days_held, theta, delta, gamma, vega, iv))

            log.append(f"✅ {file.name}: {count_new} Trades, {count_legs} Legs")
            
        except Exception as e:
            log.append(f"❌ Error {file.name}: {str(e)}")
            
    conn.commit()
    conn.close()
    return log

# --- DATA LOADERS ---
def load_data():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
    except: return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes', 'link': 'Link'
        })
        
        for col in ['Gamma', 'Vega', 'Theta', 'Delta', 'P&L', 'Debit', 'lot_size', 'Link']:
            if col not in df.columns: df[col] = 0.0 if col != 'Link' else ''
        
        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        df['Debit'] = pd.to_numeric(df['Debit'], errors='coerce').fillna(0)
        df['P&L'] = pd.to_numeric(df['P&L'], errors='coerce').fillna(0)
        df['Days Held'] = pd.to_numeric(df['Days Held'], errors='coerce').fillna(1)
        
        df['Debit/Lot'] = df['Debit'] / df['lot_size'].replace(0, 1)
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
        def get_grade(row):
            s, d = row['Strategy'], row['Debit/Lot']
            grade = "C"
            if s == '130/160':
                if d > 4800: grade="F"
                elif 3500 <= d <= 4500: grade="A+"
                else: grade="B"
            elif s == '160/190':
                if 4800 <= d <= 5500: grade="A"
                else: grade="C"
            elif s == 'M200':
                if 7500 <= d <= 8500: grade="A"
                else: grade="B"
            return grade

        df['Grade'] = df.apply(get_grade, axis=1)
    return df

def load_snapshots():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        q = """
        SELECT s.snapshot_date, s.pnl, s.days_held, s.theta, s.delta, s.gamma, s.vega, s.iv,
               t.strategy, t.name, t.id as trade_id
        FROM snapshots s
        JOIN trades t ON s.trade_id = t.id
        """
        df = pd.read_sql(q, conn)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        
        # Sort so lines draw correctly
        df = df.sort_values(['trade_id', 'snapshot_date'])
        
        for c in ['pnl', 'days_held', 'theta', 'delta', 'gamma', 'vega', 'iv']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

def load_legs(trade_id):
    conn = get_db_connection()
    try:
        return pd.read_sql("SELECT * FROM legs WHERE trade_id=?", conn, params=(trade_id,))
    except: return pd.DataFrame()
    finally: conn.close()

# --- INITIALIZE ---
init_db()

# --- SIDEBAR ---
st.sidebar.markdown("### 🚦 Daily Workflow")

with st.sidebar.expander("1. 🟢 STARTUP (Restore)", expanded=False):
    restore = st.file_uploader("Upload .db file", type=['db'], key='restore')
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
        st.success("Brain Loaded!")
        if 'restored' not in st.session_state:
            st.session_state['restored'] = True
            st.rerun()

st.sidebar.markdown("⬇️ *then...*")

with st.sidebar.expander("2. 🔵 WORK (Sync Files)", expanded=True):
    snap_date = st.date_input("File Date", datetime.now(), label_visibility="collapsed")
    active_up = st.file_uploader("Active Trades", accept_multiple_files=True, key="act")
    history_up = st.file_uploader("History (Closed)", accept_multiple_files=True, key="hist")
    if st.button("🔄 Process New Data"):
        logs = []
        if active_up: logs.extend(sync_data(active_up, "Active", snap_date))
        if history_up: logs.extend(sync_data(history_up, "History", snap_date))
        if logs:
            for l in logs: st.write(l)
            st.success("Updated!")
            st.rerun()

st.sidebar.markdown("⬇️ *finally...*")

with st.sidebar.expander("3. 🔴 SHUTDOWN (Backup)", expanded=True):
    with open(DB_NAME, "rb") as f:
        st.download_button("💾 Save Database File", f, "trade_guardian_v80.db", "application/x-sqlite3")

st.sidebar.divider()
market_regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish", "Bearish"])
regime_mult = 1.10 if "Bullish" in market_regime else 0.90 if "Bearish" in market_regime else 1.0

# --- LOGIC ---
def get_action_signal(strat, status, days_held, pnl, benchmarks_dict):
    action = ""; signal_type = "NONE" 
    if status == "Active":
        benchmark = benchmarks_dict.get(strat, {})
        base_target = benchmark.get('pnl', BASE_CONFIG.get(strat, {}).get('pnl', 9999))
        final_target = base_target * regime_mult
        
        if pnl >= final_target: return f"TAKE PROFIT (Hit ${final_target:,.0f})", "SUCCESS"
        if strat == '130/160':
            if 25 <= days_held <= 35 and pnl < 100: return "KILL (Stale)", "ERROR"
        elif strat == '160/190':
            if days_held < 30: return "COOKING", "INFO"
            elif 30 <= days_held <= 40: return "WATCH", "WARNING"
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

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "⛺ Tents", "🧪 Validator", "📈 Analytics", "📖 Rules"])

# 1. DASHBOARD
with tab1:
    if not df.empty:
        active_df = df[df['Status'] == 'Active'].copy()
        if active_df.empty:
            st.info("No active trades.")
        else:
            # Yield Warning
            port_yield = active_df['Daily Yield %'].mean()
            if port_yield < 0.10: st.sidebar.error(f"🚨 Low Yield: {port_yield:.2f}%")
            
            act_list = []; sig_list = []
            for _, row in active_df.iterrows():
                act, sig = get_action_signal(row['Strategy'], row['Status'], row['Days Held'], row['P&L'], benchmarks)
                act_list.append(act)
                sig_list.append(sig)
            active_df['Action'] = act_list
            active_df['Signal_Type'] = sig_list

            strat_tabs = st.tabs(["Overview", "130/160", "160/190", "M200"])
            cols = ['Name', 'Link', 'Action', 'Grade', 'Daily Yield %', 'P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'IV']

            def render_tab(tab, strategy_name):
                with tab:
                    subset = active_df if strategy_name == "Overview" else active_df[active_df['Strategy'] == strategy_name].copy()
                    if not subset.empty:
                        # Action Center
                        urgent = subset[subset['Action'] != ""]
                        if not urgent.empty:
                            st.markdown(f"**🚨 Action Center**")
                            for _, row in urgent.iterrows():
                                sig = row['Signal_Type']
                                color = {"SUCCESS":"#4caf50", "ERROR":"#f44336", "WARNING":"#ff9800", "INFO":"#2196f3"}.get(sig, "#9e9e9e")
                                st.markdown(f"* <span style='color: {color}'>**{row['Name']}**: {row['Action']}</span>", unsafe_allow_html=True)
                            st.divider()
                        
                        # Metrics
                        if strategy_name != "Overview":
                            bench = benchmarks.get(strategy_name, BASE_CONFIG.get(strategy_name))
                            target_disp = bench['pnl'] * regime_mult
                            c1, c2, c3, c4 = st.columns(4)
                            c1.metric("Hist. Avg Win", f"${bench['pnl']:,.0f}")
                            c2.metric("Target Yield", f"{bench['yield']:.2f}%/d")
                            c3.metric("Target Profit", f"${target_disp:,.0f}")
                            c4.metric("Avg Hold", f"{bench['dit']:.0f}d")

                        # Summary Logic
                        cols_to_sum = ['P&L', 'Debit', 'Theta', 'Delta', 'Gamma', 'Vega']
                        valid_sum_cols = [c for c in cols_to_sum if c in subset.columns]
                        sum_data = {c: [subset[c].sum()] for c in valid_sum_cols}
                        sum_data['Name'] = ['TOTAL']
                        sum_data['Daily Yield %'] = [subset['Daily Yield %'].mean()]
                        sum_data['Days Held'] = [subset['Days Held'].mean()]
                        sum_row = pd.DataFrame(sum_data)
                        
                        for c in cols:
                            if c not in sum_row.columns: sum_row[c] = 0.0 if c not in ['Name', 'Action', 'Grade', 'Link'] else ''
                        disp_sub = subset.copy()
                        for c in cols:
                            if c not in disp_sub.columns: disp_sub[c] = 0.0 if c not in ['Name', 'Action', 'Grade', 'Link'] else ''
                        display_df = pd.concat([disp_sub[cols], sum_row[cols]], ignore_index=True)

                        # Styled Dataframe with VERSION CHECK for LinkColumn
                        st_style = display_df.style.format({'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%", 'Theta': "{:.1f}", 'Delta': "{:.1f}", 'IV': "{:.1f}%"})
                        st_style = st_style.map(lambda v: 'background-color: #d1e7dd; color: #0f5132' if 'TAKE' in str(v) else 'background-color: #f8d7da; color: #842029' if 'KILL' in str(v) else '', subset=['Action'])
                        st_style = st_style.map(lambda v: 'color: #0f5132' if 'A' in str(v) else 'color: #842029' if 'F' in str(v) else '', subset=['Grade'])
                        st_style = st_style.apply(lambda x: ['background-color: #d1d5db; color: black; font-weight: bold' if x.name == len(display_df)-1 else '' for _ in x], axis=1)

                        # Check for st.column_config support (Streamlit >= 1.23)
                        if hasattr(st, "column_config"):
                            st.dataframe(
                                st_style,
                                column_config={
                                    "Link": st.column_config.LinkColumn("Open", display_text="Open")
                                },
                                use_container_width=True
                            )
                        else:
                            st.dataframe(st_style, use_container_width=True)

                    else: st.info("No active trades.")

            with strat_tabs[0]:
                with st.expander("📊 Portfolio Risk Metrics", expanded=True):
                    total_delta = active_df['Delta'].sum()
                    total_theta = active_df['Theta'].sum()
                    total_cap = active_df['Debit'].sum()
                    r1, r2, r3 = st.columns(3)
                    r1.metric("Net Delta", f"{total_delta:,.1f}")
                    r2.metric("Daily Theta", f"${total_theta:,.0f}")
                    r3.metric("Capital at Risk", f"${total_cap:,.0f}")
                
                # Summary Aggregation
                strat_agg = active_df.groupby('Strategy').agg({
                    'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum', 'Delta': 'sum',
                    'Name': 'count', 'Daily Yield %': 'mean' 
                }).reset_index()
                
                if hasattr(st, "dataframe"):
                     st.dataframe(strat_agg.style.format({'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%"}), use_container_width=True)

                render_tab(strat_tabs[0], "Overview")

            render_tab(strat_tabs[1], "130/160")
            render_tab(strat_tabs[2], "160/190")
            render_tab(strat_tabs[3], "M200")
            
            csv = active_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Active CSV", csv, "active.csv", "text/csv")
    else: st.info("Database Empty.")

# 2. TENTS
with tab2:
    st.subheader("⛺ Structure Viz")
    if not df.empty:
        active_df = df[df['Status'] == 'Active']
        if not active_df.empty:
            sel_trade = st.selectbox("Select Trade", active_df['Name'].unique())
            if sel_trade:
                trade_row = active_df[active_df['Name'] == sel_trade].iloc[0]
                legs = load_legs(trade_row['id'])
                if not legs.empty:
                    c1, c2 = st.columns([1, 3])
                    with c1: st.table(legs[['quantity', 'strike', 'type']])
                    with c2:
                        strikes = sorted(legs['strike'].unique())
                        min_s, max_s = min(strikes) * 0.95, max(strikes) * 1.05
                        x = np.linspace(min_s, max_s, 200)
                        y = np.zeros_like(x)
                        for _, l in legs.iterrows():
                            val = np.maximum(x - l['strike'], 0) if l['type'] == 'Call' else np.maximum(l['strike'] - x, 0)
                            y += (val * l['quantity'] * 100)
                        y -= trade_row['Debit']
                        fig = px.line(x=x, y=y, title=f"Profit Tent: {sel_trade}")
                        fig.add_hline(y=0, line_dash="dash", line_color="red")
                        st.plotly_chart(fig, use_container_width=True)
                else: st.warning("No legs found. Re-sync your Active file.")
        else: st.info("No active trades.")
    else: st.info("Database Empty.")

# 3. VALIDATOR
with tab3:
    st.markdown("### 🧪 Audit")
    
    st.markdown("""
    | Strat | Grade | Price |
    | :--- | :--- | :--- |
    | 130/160 | **A+** | $3.5k - $4.5k |
    | 160/190 | **A** | $4.8k - $5.5k |
    | M200 | **A** | $7.5k - $8.5k |
    """)
    
    uploaded = st.file_uploader("Upload OptionStrat File", key="audit")
    if uploaded:
        m_df = read_file_safely(uploaded)
        if m_df is not None and not m_df.empty:
            row = m_df.iloc[0]
            name = row.get('Name', 'Unknown')
            strat = get_strategy(row.get('Group', ''), name)
            debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
            
            lot_size = 1
            if strat == '130/160' and debit > 6000: lot_size = 2
            elif strat == 'M200' and debit > 12000: lot_size = 2
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
            c1, c2, c3 = st.columns(3)
            c1.metric("Strategy", strat)
            c2.metric("Total Debit", f"${debit:,.0f}")
            c3.metric("Debit/Lot", f"${debit_lot:,.0f}")
            
            if "A" in grade: st.success("✅ APPROVED")
            elif "F" in grade: st.error("⛔ OVERPRICED")
            else: st.warning("⚠️ CHECK")
            
            # Historical Search
            if not df.empty:
                expired_df = df[df['Status'] == 'Expired']
                similar = expired_df[
                    (expired_df['Strategy'] == strat) & 
                    (expired_df['Debit/Lot'].between(debit_lot*0.9, debit_lot*1.1))
                ]
                if not similar.empty:
                    avg_win = similar[similar['P&L']>0]['P&L'].mean()
                    st.info(f"📊 Historical Match: Found {len(similar)} similar trades. Avg Win: ${avg_win:,.0f}")

# 4. ANALYTICS
with tab4:
    if not df.empty:
        an1, an2, an3, an4 = st.tabs(["Lifecycle", "Greeks", "Equity", "Heatmaps"])
        
        with an1:
            snaps = load_snapshots()
            if not snaps.empty:
                sel = st.selectbox("Strategy", snaps['strategy'].unique())
                sub = snaps[snaps['strategy'] == sel]
                fig = px.line(sub, x='days_held', y='pnl', color='name', line_group='trade_id', markers=True, title=f"P&L Path: {sel}")
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("Sync daily to see this.")

        with an2:
            snaps = load_snapshots()
            if not snaps.empty:
                c1, c2 = st.columns(2)
                g_strat = c1.selectbox("Strategy", snaps['strategy'].unique(), key='gks')
                g_met = c2.selectbox("Metric", ['theta', 'delta', 'gamma', 'vega', 'iv', 'pnl'], key='gkm')
                sub = snaps[snaps['strategy'] == g_strat].dropna(subset=[g_met])
                fig = px.line(sub, x='days_held', y=g_met, color='name', line_group='trade_id', markers=True, title=f"{g_met} Path")
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("Sync daily to see this.")

        with an3:
            expired = df[df['Status'] == 'Expired'].sort_values('Exit Date')
            if not expired.empty:
                wins = expired[expired['P&L'] > 0]
                win_rate = (len(wins) / len(expired)) * 100
                st.metric("Win Rate", f"{win_rate:.1f}%")
                
                expired['Cum'] = expired['P&L'].cumsum()
                st.plotly_chart(px.line(expired, x='Exit Date', y='Cum', title="Equity Curve"), use_container_width=True)
            else: st.info("No closed trades.")
                
        with an4:
            expired = df[df['Status'] == 'Expired']
            if not expired.empty:
                expired['Month'] = expired['Exit Date'].dt.month_name()
                expired['Year'] = expired['Exit Date'].dt.year
                hm = expired.groupby(['Year', 'Month'])['P&L'].sum().reset_index()
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                st.markdown("##### Monthly Seasonality")
                st.plotly_chart(px.density_heatmap(hm, x="Month", y="Year", z="P&L", text_auto=True, color_continuous_scale="RdBu", category_orders={"Month": months}), use_container_width=True)
                
                st.divider()
                st.markdown("##### Duration Sweet Spot")
                st.plotly_chart(px.density_heatmap(expired, x="Days Held", y="Strategy", z="P&L", histfunc="avg"), use_container_width=True)
                
                st.divider()
                st.markdown("##### Ticker Performance")
                tick = expired.groupby('Ticker')['P&L'].sum().reset_index().sort_values('P&L', ascending=False).head(10)
                st.plotly_chart(px.bar(tick, x='P&L', y='Ticker', orientation='h'), use_container_width=True)

with tab5:
    st.markdown("## 📜 Rules")
    st.markdown("### 1. 130/160 Strategy (Income Engine)")
    st.markdown("* **Target Entry:** Monday.")
    st.markdown("* **Debit Target:** `$3,500 - $4,500` per lot.")
    st.markdown("* **Stop Rule:** Never pay > `$4,800` per lot.")
    st.markdown("* **Management:** Kill if trade is **25 days old** and profit is flat/negative.")
    
    st.markdown("### 2. 160/190 Strategy (Compounder)")
    st.markdown("* **Target Entry:** Friday.")
    st.markdown("* **Debit Target:** `~$5,200` per lot.")
    st.markdown("* **Sizing:** Trade **1 Lot** (Scaling to 2 lots reduces ROI).")
    st.markdown("* **Exit:** Hold for **40-50 Days**. Do not touch in first 30 days.")
    
    st.markdown("### 3. M200 Strategy (Whale)")
    st.markdown("* **Target Entry:** Wednesday.")
    st.markdown("* **Debit Target:** `$7,500 - $8,500` per lot.")
    st.markdown("* **Management:** Check P&L at **Day 14**.")
    st.markdown("    * If Green > $200: Exit or Roll.")
    st.markdown("    * If Red/Flat: HOLD. Do not exit in the 'Dip Valley' (Day 15-50).")
