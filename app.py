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

# --- VERSION BANNER ---
st.info("✅ RUNNING VERSION: v72.0 (Greeks & Heatmap Restored)")

st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v4.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT,
                    entry_date DATE, exit_date DATE, days_held INTEGER,
                    debit REAL, lot_size INTEGER, pnl REAL,
                    theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT, snapshot_date DATE,
                    pnl REAL, days_held INTEGER, FOREIGN KEY(trade_id) REFERENCES trades(id)
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
    g, n = str(group_name).upper(), str(trade_name).upper()
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
            if ticker in ['M200', '130', '160', 'IRON', 'VERTICAL']: return "UNKNOWN"
            return ticker
        return "UNKNOWN"
    except: return "UNKNOWN"

# --- SYNC ENGINE ---
def read_file_safely(file):
    try:
        if file.name.endswith(('.xlsx', '.xls')):
            df_raw = pd.read_excel(file, header=None, engine='openpyxl')
            header_idx = -1
            for i, row in df_raw.head(20).iterrows():
                if "Name" in " ".join(row.astype(str).values) and "Total Return" in " ".join(row.astype(str).values):
                    header_idx = i; break
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
                if "Name" in line and "Total Return" in line: header_row = i; break
            file.seek(0)
            return pd.read_csv(file, skiprows=header_row)
    except: return None

def sync_data(file_list, file_type):
    log = []
    if not isinstance(file_list, list): file_list = [file_list]
    conn = get_db_connection()
    c = conn.cursor()
    count_new, count_update = 0, 0
    
    for file in file_list:
        df = read_file_safely(file)
        if df is None or df.empty: continue
        
        for _, row in df.iterrows():
            name = str(row.get('Name', ''))
            if name.startswith('.') or name in ['nan', '', 'Symbol']: continue
            try: start_dt = pd.to_datetime(row.get('Created At', ''))
            except: continue
            
            group, strat = str(row.get('Group', '')), get_strategy(row.get('Group', ''), name)
            pnl, debit = clean_num(row.get('Total Return $', 0)), abs(clean_num(row.get('Net Debit/Credit', 0)))
            theta, delta = clean_num(row.get('Theta', 0)), clean_num(row.get('Delta', 0))
            gamma, vega = clean_num(row.get('Gamma', 0)), clean_num(row.get('Vega', 0))
            
            lot_size = 1
            if strat == '130/160' and debit > 6000: lot_size = 2
            elif strat == '130/160' and debit > 10000: lot_size = 3
            elif strat == '160/190' and debit > 8000: lot_size = 2
            elif strat == 'M200' and debit > 12000: lot_size = 2
            
            trade_id = generate_id(name, strat, start_dt)
            status = "Active" if file_type == "Active" else "Expired"
            
            if file_type == "History":
                try: exit_dt = pd.to_datetime(row.get('Expiration')); days_held = (exit_dt - start_dt).days
                except: exit_dt, days_held = None, 1
            else:
                exit_dt, days_held = None, (datetime.now() - start_dt).days
            if days_held < 1: days_held = 1
            
            c.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
            existing = c.fetchone()
            
            if existing is None:
                c.execute('''INSERT INTO trades (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (trade_id, name, strat, status, start_dt.date(), exit_dt.date() if exit_dt else None, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, ""))
                count_new += 1
            else:
                if file_type == "History":
                    c.execute("UPDATE trades SET pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=? WHERE id=?", 
                              (pnl, status, exit_dt.date() if exit_dt else None, days_held, theta, delta, gamma, vega, trade_id))
                    count_update += 1
                elif existing[0] == "Active":
                    c.execute("UPDATE trades SET pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=? WHERE id=?", 
                              (pnl, days_held, theta, delta, gamma, vega, trade_id))
                    count_update += 1
                    
            if file_type == "Active":
                today = datetime.now().date()
                c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                if not c.fetchone():
                    c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held) VALUES (?,?,?,?)", (trade_id, today, pnl, days_held))
                    
        log.append(f"✅ {file.name}: {count_new} New, {count_update} Updated")
    conn.commit(); conn.close()
    return log

# --- DATA LOADER ---
def load_data():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try: df = pd.read_sql("SELECT * FROM trades", conn)
    except: return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        df.columns = [c.title().replace('_', ' ') for c in df.columns]
        df = df.rename(columns={'Id':'id', 'Name':'Name', 'Strategy':'Strategy', 'Status':'Status', 'Pnl':'P&L', 'Days Held':'Days Held'})
        
        # Ensure numeric for charting
        cols = ['P&L', 'Debit', 'Theta', 'Delta', 'Gamma', 'Vega', 'Days Held']
        for c in cols: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            
        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
        # Grading
        def get_grade(row):
            s, d = row['Strategy'], row['Debit'] / max(1, row.get('Lot Size', 1))
            grade, reason = "C", "Standard"
            if s == '130/160':
                if d > 4800: grade, reason = "F", "Overpriced"
                elif 3500 <= d <= 4500: grade, reason = "A+", "Sweet Spot"
                else: grade, reason = "B", "Acceptable"
            elif s == '160/190':
                if 4800 <= d <= 5500: grade, reason = "A", "Ideal"
                else: grade, reason = "C", "Check Pricing"
            elif s == 'M200':
                if 7500 <= d <= 8500: grade, reason = "A", "Perfect"
                else: grade, reason = "B", "Variance"
            return pd.Series([grade, reason])
        df[['Grade', 'Reason']] = df.apply(get_grade, axis=1)
    return df

def load_snapshots():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        q = "SELECT s.snapshot_date, s.pnl, s.days_held, t.strategy, t.name, t.id FROM snapshots s JOIN trades t ON s.trade_id = t.id"
        df = pd.read_sql(q, conn)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        # Force numeric types for line chart
        df['pnl'] = pd.to_numeric(df['pnl'], errors='coerce').fillna(0)
        df['days_held'] = pd.to_numeric(df['days_held'], errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- INITIALIZE ---
init_db()

# --- SIDEBAR ---
st.sidebar.markdown("### 🚦 Daily Workflow")
with st.sidebar.expander("1. 🟢 STARTUP (Restore)", expanded=True):
    restore = st.file_uploader("Upload .db file", type=['db'], key='restore')
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
        st.success("Brain Loaded!")
        if 'restored' not in st.session_state: st.session_state['restored'] = True; st.rerun()

with st.sidebar.expander("2. 🔵 WORK (Sync Files)", expanded=True):
    active_up = st.file_uploader("Active Trades", accept_multiple_files=True, key="act")
    history_up = st.file_uploader("History (Closed)", accept_multiple_files=True, key="hist")
    if st.button("🔄 Process New Data"):
        logs = []
        if active_up: logs.extend(sync_data(active_up, "Active"))
        if history_up: logs.extend(sync_data(history_up, "History"))
        if logs: st.success("Updated!"); st.rerun()

with st.sidebar.expander("3. 🔴 SHUTDOWN (Backup)", expanded=True):
    with open(DB_NAME, "rb") as f: st.download_button("💾 Save Database File", f, "trade_guardian_v4.db")

st.sidebar.divider()
market_regime = st.sidebar.selectbox("Market Regime", ["Neutral (Standard)", "Bullish (+10%)", "Bearish (-10%)"])
regime_mult = 1.1 if "Bullish" in market_regime else 0.9 if "Bearish" in market_regime else 1.0

# --- MAIN APP ---
df = load_data()
benchmarks = BASE_CONFIG.copy()
if not df.empty:
    expired_df = df[df['Status'] == 'Expired']
    if not expired_df.empty:
        for strat, grp in expired_df.groupby('Strategy'):
            wins = grp[grp['P&L'] > 0]
            if not wins.empty:
                benchmarks[strat] = {
                    'yield': grp['Daily Yield %'].mean(),
                    'pnl': wins['P&L'].mean(),
                    'dit': wins['Days Held'].mean()
                }

tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "🧪 Validator", "📈 Analytics", "📖 Rules"])

# 1. DASHBOARD
with tab1:
    if not df.empty:
        active_df = df[df['Status'] == 'Active'].copy()
        if active_df.empty: st.info("No active trades.")
        else:
            def get_action(row):
                bench = benchmarks.get(row['Strategy'], {})
                target = bench.get('pnl', BASE_CONFIG.get(row['Strategy'], {}).get('pnl', 9999)) * regime_mult
                if row['P&L'] >= target: return f"TAKE PROFIT (Hit ${target:,.0f})", "SUCCESS"
                if row['Strategy'] == '130/160' and 25 <= row['Days Held'] <= 35 and row['P&L'] < 100: return "KILL (Stale)", "ERROR"
                if row['Strategy'] == 'M200' and 12 <= row['Days Held'] <= 16: return "DAY 14 CHECK", "WARNING"
                if row['Strategy'] == '160/190' and row['Days Held'] < 30: return "COOKING", "INFO"
                return "", "NONE"

            actions = active_df.apply(get_action, axis=1, result_type='expand')
            active_df['Action'], active_df['Signal_Type'] = actions[0], actions[1]
            
            st.markdown("### 🏛️ Active Trades")
            strat_tabs = st.tabs(["Overview", "130/160", "160/190", "M200"])
            
            def render_tab(tab, strat):
                with tab:
                    sub = active_df[active_df['Strategy'] == strat].copy()
                    
                    # ACTION CENTER
                    urgent = sub[sub['Action'] != ""]
                    if not urgent.empty:
                        st.markdown(f"**🚨 Action Center**")
                        for _, r in urgent.iterrows():
                            color = {"SUCCESS":"#4caf50", "ERROR":"#f44336", "WARNING":"#ff9800", "INFO":"#2196f3"}.get(r['Signal_Type'], "#999")
                            st.markdown(f"* <span style='color:{color}'>**{r['Name']}**: {r['Action']}</span>", unsafe_allow_html=True)
                        st.divider()
                        
                    # METRICS
                    bench = benchmarks.get(strat, BASE_CONFIG.get(strat))
                    c1,c2,c3,c4 = st.columns(4)
                    c1.metric("Target Win", f"${bench.get('pnl',0)*regime_mult:,.0f}")
                    c2.metric("Target Yield", f"{bench.get('yield',0):.2f}%")
                    c3.metric("Avg Hold", f"{bench.get('dit',0):.0f}d")
                    c4.metric("Active P&L", f"${sub['P&L'].sum():,.0f}")
                    
                    if not sub.empty:
                        # Display
                        cols = ['Name', 'Action', 'Grade', 'P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega']
                        st.dataframe(sub[cols].style.format({'P&L':'${:,.0f}', 'Debit':'${:,.0f}', 'Theta':'{:.1f}'}), use_container_width=True)
                    else: st.info("No trades.")

            with strat_tabs[0]:
                total_delta, total_theta = active_df['Delta'].sum(), active_df['Theta'].sum()
                c1,c2,c3 = st.columns(3)
                c1.metric("Net Delta", f"{total_delta:.1f}")
                c2.metric("Daily Theta", f"${total_theta:.0f}")
                c3.metric("Capital", f"${active_df['Debit'].sum():,.0f}")
                
                # Summary Table
                agg = active_df.groupby('Strategy').agg({'P&L':'sum', 'Debit':'sum', 'Theta':'sum', 'Name':'count'}).reset_index()
                st.dataframe(agg.style.format({'P&L':'${:,.0f}', 'Debit':'${:,.0f}'}), use_container_width=True)

            render_tab(strat_tabs[1], '130/160')
            render_tab(strat_tabs[2], '160/190')
            render_tab(strat_tabs[3], 'M200')
    else: st.info("Database Empty. Upload files in Step 2.")

# 2. VALIDATOR
with tab2:
    st.markdown("### 🧪 Trade Validator")
    up = st.file_uploader("Upload Model File", key="mod")
    if up:
        m_df = read_file_safely(up)
        if m_df is not None:
            r = m_df.iloc[0]
            s = get_strategy(r.get('Group',''), r.get('Name',''))
            d = abs(clean_num(r.get('Net Debit/Credit', 0)))
            # Simple grading display logic here...
            st.success(f"File Read: {r.get('Name')} | Strategy: {s} | Debit: ${d:,.0f}")

# 3. ANALYTICS (EXPANDED)
with tab3:
    if not df.empty:
        st.subheader("📈 Analytics Suite")
        exp = df[df['Status'] == 'Expired'].copy()
        
        # Sub-Tabs
        at1, at2, at3, at4, at5, at6 = st.tabs(["🌊 Equity", "🎯 Expectancy", "🔥 DIT Heatmap", "🏷️ Tickers", "🧬 Lifecycle", "🧮 Greeks Lab"])
        
        # 1. Equity
        with at1:
            if not exp.empty:
                exp = exp.sort_values('Exit Date')
                exp['Cum P&L'] = exp['P&L'].cumsum()
                fig = px.line(exp, x='Exit Date', y='Cum P&L', title="Equity Curve", markers=True)
                st.plotly_chart(fig, use_container_width=True)
                
        # 2. Expectancy
        with at2:
            if not exp.empty:
                wins = exp[exp['P&L']>0]
                losses = exp[exp['P&L']<=0]
                wr = len(wins)/len(exp)*100
                st.metric("Win Rate", f"{wr:.1f}%")
                fig = px.histogram(exp, x="P&L", color="Strategy", nbins=20)
                st.plotly_chart(fig, use_container_width=True)
                
        # 3. RESTORED DIT HEATMAP
        with at3:
            if not exp.empty:
                fig = px.density_heatmap(exp, x="Days Held", y="Strategy", z="P&L", histfunc="avg", 
                                         title="Profit Heatmap: Strategy vs Duration", color_continuous_scale="RdBu")
                st.plotly_chart(fig, use_container_width=True)
                
        # 4. Tickers
        with at4:
            if not exp.empty:
                grp = exp.groupby('Ticker')['P&L'].sum().reset_index().sort_values('P&L')
                fig = px.bar(grp, x='P&L', y='Ticker', orientation='h', color='P&L')
                st.plotly_chart(fig, use_container_width=True)
                
        # 5. LIFECYCLE (FIXED)
        with at5:
            snaps = load_snapshots()
            if not snaps.empty:
                sel = st.selectbox("Select Strategy", snaps['strategy'].unique())
                sub_snaps = snaps[snaps['strategy'] == sel]
                # Force types just in case
                sub_snaps['pnl'] = sub_snaps['pnl'].astype(float)
                sub_snaps['days_held'] = sub_snaps['days_held'].astype(int)
                
                fig = px.line(sub_snaps, x='days_held', y='pnl', color='name', line_group='id',
                              title=f"Trade Lifecycle: {sel}", hover_data=['name'])
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("Sync Active trades daily to build this chart.")
            
        # 6. NEW GREEKS LAB
        with at6:
            if not exp.empty:
                st.markdown("##### 🔬 Does Greek Exposure correlate with Profit?")
                g_col = st.selectbox("Select Greek", ['Theta', 'Delta', 'Gamma', 'Vega'])
                fig = px.scatter(exp, x=g_col, y='P&L', color='Strategy', trendline='ols', 
                                 title=f"Correlation: {g_col} vs P&L")
                st.plotly_chart(fig, use_container_width=True)

# 4. RULES
with tab4:
    st.markdown("### 📖 Rules")
    st.info("Rules loaded from v72.0 configuration.")
