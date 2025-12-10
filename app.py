import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import shutil
import os
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")
st.title("🛡️ Allantis Trade Guardian: Enterprise DB")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v2.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    strategy TEXT,
                    status TEXT,
                    entry_date TIMESTAMP,
                    exit_date TIMESTAMP,
                    days_held INTEGER,
                    debit REAL,
                    lot_size INTEGER,
                    pnl REAL,
                    notes TEXT
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT,
                    snapshot_date DATE,
                    pnl REAL,
                    theta REAL,
                    delta REAL,
                    gamma REAL,
                    vega REAL,
                    days_held INTEGER,
                    FOREIGN KEY(trade_id) REFERENCES trades(id)
                )''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trade_snap ON snapshots(trade_id)")
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES)

# --- HELPER FUNCTIONS ---
def get_strategy(group_name, trade_name):
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    try: return float(str(x).replace('$','').replace(',',''))
    except: return 0.0

def generate_trade_id(name, strategy, entry_date):
    date_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{name}_{strategy}_{date_str}".replace(" ", "").replace("/", "-")

# --- REPAIR ENGINE (NEW) ---
def repair_database():
    """Recalculates Days Held for all trades to fix '1 Day' bugs."""
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM trades", conn)
    
    updates = []
    for _, row in df.iterrows():
        try:
            start = pd.to_datetime(row['entry_date'])
            status = row['status']
            
            if status == "Expired" and row['exit_date']:
                end = pd.to_datetime(row['exit_date'])
                days = (end - start).days
            else:
                days = (datetime.now() - start).days
            
            if days < 1: days = 1
            updates.append((days, row['id']))
        except: continue
        
    c = conn.cursor()
    c.executemany("UPDATE trades SET days_held = ? WHERE id = ?", updates)
    conn.commit()
    conn.close()
    return len(updates)

# --- SYNC ENGINE ---
def sync_data(file_list, file_type):
    log = []
    if not isinstance(file_list, list): file_list = [file_list]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    count_new, count_update = 0, 0
    
    for file in file_list:
        try:
            if file.name.endswith('.xlsx'): df = pd.read_excel(file)
            else:
                try:
                    df = pd.read_csv(file)
                    if 'Name' not in df.columns: 
                        file.seek(0)
                        df = pd.read_csv(file, skiprows=1)
                except: continue
                
            for _, row in df.iterrows():
                name = str(row.get('Name', ''))
                if name.startswith('.') or name == 'nan' or name == '' or name == 'Symbol': continue
                
                created_val = row.get('Created At', '')
                try: start_dt = pd.to_datetime(created_val)
                except: continue
                
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                
                lot_size = 1
                if strat == '130/160' and debit > 6000: lot_size = 2
                elif strat == '130/160' and debit > 10000: lot_size = 3
                elif strat == '160/190' and debit > 8000: lot_size = 2
                elif strat == 'M200' and debit > 12000: lot_size = 2
                
                trade_id = generate_trade_id(name, strat, start_dt)
                status = "Active" if file_type == "Active" else "Expired"
                
                # DATE LOGIC (Improved)
                exit_dt = None
                days_held = 0
                
                if file_type == "History":
                    try:
                        # For history files, Expiration IS the exit date per user confirmation
                        exit_dt = pd.to_datetime(row.get('Expiration', ''))
                        days_held = (exit_dt - start_dt).days
                    except: pass
                else:
                    days_held = (datetime.now() - start_dt).days

                if days_held < 1: days_held = 1

                # UPSERT
                cursor.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
                data = cursor.fetchone()
                
                if data is None:
                    cursor.execute('''INSERT INTO trades 
                                      (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, notes) 
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                   (trade_id, name, strat, status, start_dt, 
                                    exit_dt if exit_dt else None, 
                                    days_held, debit, lot_size, pnl, ""))
                    count_new += 1
                else:
                    # Update Existing
                    if file_type == "History":
                        cursor.execute('''UPDATE trades SET pnl=?, status=?, exit_date=?, days_held=? WHERE id=?''', 
                                       (pnl, status, exit_dt, days_held, trade_id))
                        count_update += 1
                    elif data[0] == "Active":
                        cursor.execute('''UPDATE trades SET pnl=?, days_held=? WHERE id=?''', 
                                       (pnl, days_held, trade_id))
                        count_update += 1

                # SNAPSHOT (Active Only)
                if file_type == "Active":
                    theta = clean_num(row.get('Theta', 0))
                    delta = clean_num(row.get('Delta', 0))
                    gamma = clean_num(row.get('Gamma', 0))
                    vega = clean_num(row.get('Vega', 0))
                    
                    today_str = datetime.now().date()
                    cursor.execute("SELECT id FROM snapshots WHERE trade_id = ? AND snapshot_date = ?", (trade_id, today_str))
                    if not cursor.fetchone():
                        cursor.execute('''INSERT INTO snapshots (trade_id, snapshot_date, pnl, theta, delta, gamma, vega, days_held)
                                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                       (trade_id, today_str, pnl, theta, delta, gamma, vega, days_held))

            log.append(f"✅ {file.name}: {count_new} New / {count_update} Updated")
        except Exception as e:
            log.append(f"❌ Error {file.name}: {str(e)}")
            
    conn.commit()
    conn.close()
    return log

# --- DATA LOADER ---
def load_data_from_db():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    
    query = """
    SELECT t.id, t.name, t.strategy, t.status, t.entry_date, t.days_held, t.debit, t.lot_size, t.pnl, t.notes,
           s.theta, s.delta, s.gamma, s.vega
    FROM trades t
    LEFT JOIN (SELECT * FROM snapshots WHERE id IN (SELECT MAX(id) FROM snapshots GROUP BY trade_id)) s ON t.id = s.trade_id
    """
    try:
        df = pd.read_sql_query(query, conn)
    except: return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status', 
            'pnl': 'P&L', 'debit': 'Debit', 'notes': 'Notes',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'days_held': 'Days Held'
        }, inplace=True)
        
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['Debit/Lot'] = df['Debit'] / df['lot_size']
        # Recalculate Yield on the fly to ensure accuracy
        df['Daily Yield %'] = (df['P&L'] / df['Debit'] * 100) / df['Days Held']
        
        def get_grade(row):
            strat, debit = row['Strategy'], row['Debit/Lot']
            if strat == '130/160': return "F" if debit > 4800 else "A+" if 3500 <= debit <= 4500 else "B"
            if strat == '160/190': return "A" if 4800 <= debit <= 5500 else "C"
            if strat == 'M200': return "A" if 7500 <= debit <= 8500 else "B"
            return "C"
        df['Grade'] = df.apply(get_grade, axis=1)
        
    return df

# --- INITIALIZE ---
init_db()

# --- SIDEBAR ---
with st.sidebar.expander("📂 Data Sync", expanded=True):
    active_files = st.file_uploader("1. ACTIVE Trades", type=['csv','xlsx'], accept_multiple_files=True, key='act')
    history_files = st.file_uploader("2. HISTORY (Closed)", type=['csv','xlsx'], accept_multiple_files=True, key='hist')
    
    c1, c2 = st.columns(2)
    if c1.button("🔄 Sync"):
        logs = []
        if active_files: logs.extend(sync_data(active_files, "Active"))
        if history_files: logs.extend(sync_data(history_files, "History"))
        if logs:
            for l in logs: 
                if "❌" in l: st.error(l)
                else: st.success(l)
            st.rerun()
            
    if c2.button("🛠️ Repair DB"):
        count = repair_database()
        st.success(f"Repaired {count} trades. Dates corrected.")
        st.rerun()

    if st.button("💾 Backup DB"):
        try:
            with open(DB_NAME, "rb") as f:
                st.download_button("⬇️ Download DB File", f, file_name=f"backup_{datetime.now().strftime('%Y%m%d')}.db", mime="application/x-sqlite3")
        except Exception as e: st.error(f"Error: {e}")

    uploaded_db = st.file_uploader("📥 Restore DB", type=['db', 'sqlite'], key='restore')
    if uploaded_db:
        with open(DB_NAME, "wb") as f: f.write(uploaded_db.getbuffer())
        st.success("Restored!")
        st.rerun()

st.sidebar.divider()
st.sidebar.header("⚙️ Settings")
acct_size = st.sidebar.number_input("Account Size ($)", value=150000, step=5000)
market_regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish (+10%)", "Bearish (-10%)"], index=0)
regime_mult = 1.1 if "Bullish" in market_regime else 0.9 if "Bearish" in market_regime else 1.0

# --- CONFIG (DYNAMIC YIELD) ---
# We no longer hardcode yield/days. We let the data speak.
BASE_CONFIG = {
    '130/160': {'pnl': 600}, 
    '160/190': {'pnl': 420}, 
    'M200':    {'pnl': 910}
}

def get_action_signal(strat, status, days_held, pnl, benchmarks_dict):
    if status != "Active": return "", "NONE"
    benchmark = benchmarks_dict.get(strat, {})
    target = benchmark.get('pnl', 0)
    if target == 0: target = BASE_CONFIG.get(strat, {}).get('pnl', 9999)
    final_target = target * regime_mult
    
    if pnl >= final_target: return f"TAKE PROFIT (Hit ${final_target:,.0f})", "SUCCESS"
    if strat == '130/160' and 25 <= days_held <= 35 and pnl < 100: return "KILL (Stale >25d)", "ERROR"
    if strat == '160/190' and days_held < 30: return "COOKING (Hold)", "INFO"
    if strat == 'M200' and 12 <= days_held <= 16: return "DAY 14 CHECK", "WARNING"
    return "", "NONE"

# --- LOAD DATA ---
df = load_data_from_db()

if df.empty:
    st.info("👋 Database empty. Upload files to initialize.")
else:
    # --- BENCHMARKS ---
    expired_df = df[df['Status'] == 'Expired'].copy()
    benchmarks = BASE_CONFIG.copy()
    if not expired_df.empty:
        hist_grp = expired_df.groupby('Strategy')
        for strat, grp in hist_grp:
            winners = grp[grp['P&L'] > 0]
            if not winners.empty:
                real_pnl = winners['P&L'].mean()
                real_days = winners['Days Held'].mean()
                avg_debit = grp['Debit'].mean()
                
                # True Yield Calculation
                benchmarks[strat] = {
                    'pnl': real_pnl,
                    'dit': real_days,
                    'yield': (real_pnl / avg_debit * 100) / real_days if real_days > 0 else 0
                }

    # --- TABS ---
    tabs = st.tabs(["📊 Dashboard", "🧪 Validator", "📈 Analytics", "📜 Timeline", "💰 Allocation", "📓 Journal", "📖 Rules"])

    # 1. DASHBOARD
    with tabs[0]:
        active_df = df[df['Status'] == 'Active'].copy()
        
        if not active_df.empty:
            port_yield = active_df['Daily Yield %'].mean()
            if port_yield < 0.10: st.sidebar.error(f"🚨 Critical Yield: {port_yield:.2f}%")
            elif port_yield < 0.15: st.sidebar.warning(f"⚠️ Low Yield: {port_yield:.2f}%")
            else: st.sidebar.success(f"✅ Healthy: {port_yield:.2f}%")
            
            act_list, sig_list = [], []
            for _, row in active_df.iterrows():
                act, sig = get_action_signal(row['Strategy'], row['Status'], row['Days Held'], row['P&L'], benchmarks)
                act_list.append(act)
                sig_list.append(sig)
            active_df['Action'] = act_list
            active_df['Signal_Type'] = sig_list
            
            with st.expander("📊 Risk Command Center", expanded=True):
                c1, c2, c3 = st.columns(3)
                delta_net = active_df['Delta'].sum()
                c1.metric("Net Delta", f"{delta_net:,.1f}", delta="Bullish" if delta_net > 0 else "Bearish")
                c2.metric("Daily Theta", f"${active_df['Theta'].sum():,.0f}")
                c3.metric("Capital at Risk", f"${active_df['Debit'].sum():,.0f}")

            strat_tabs = st.tabs(["📋 Overview", "🔹 130/160", "🔸 160/190", "🐳 M200"])
            
            with strat_tabs[0]:
                agg = active_df.groupby('Strategy').agg({
                    'P&L':'sum', 'Debit':'sum', 'Theta':'sum', 'Name':'count', 'Daily Yield %':'mean'
                }).reset_index()
                agg.rename(columns={'Name': 'Trade Count'}, inplace=True)
                agg['Trend'] = agg.apply(lambda r: "🟢" if r['Daily Yield %'] >= benchmarks.get(r['Strategy'], {}).get('yield', 0) else "🔴", axis=1)
                
                total = pd.DataFrame({'Strategy':['TOTAL'], 'P&L':[agg['P&L'].sum()], 'Debit':[agg['Debit'].sum()], 
                                      'Theta':[agg['Theta'].sum()], 'Trade Count':[agg['Trade Count'].sum()], 'Trend':['-']})
                
                st.dataframe(
                    pd.concat([agg, total], ignore_index=True)
                    .style.format({'P&L':"${:,.0f}", 'Debit':"${:,.0f}", 'Theta':"{:.0f}", 'Daily Yield %':"{:.2f}%"})
                    .apply(lambda x: ['background-color: #f0f2f6; color: black; font-weight: bold' if x.name == len(agg) else '' for _ in x], axis=1),
                    use_container_width=True
                )

            cols = ['Name', 'Action', 'Grade', 'P&L', 'Debit', 'Days Held', 'Daily Yield %', 'Theta', 'Delta']
            
            def render_strat(tab, strat):
                with tab:
                    sub = active_df[active_df['Strategy'] == strat].copy()
                    bench = benchmarks.get(strat, BASE_CONFIG.get(strat))
                    
                    urgent = sub[sub['Action'] != ""]
                    if not urgent.empty:
                        st.markdown("**🚨 Action Center**")
                        for _, r in urgent.iterrows():
                            color = "green" if "TAKE" in r['Action'] else "red" if "KILL" in r['Action'] else "orange"
                            st.markdown(f"<span style='color:{color}; font-weight:bold'>● {r['Name']}</span>: {r['Action']}", unsafe_allow_html=True)
                        st.divider()

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Hist. Win", f"${bench.get('pnl',0):,.0f}")
                    c2.metric("Target Yield", f"{bench.get('yield',0):.2f}%")
                    c3.metric("Target Profit", f"${bench.get('pnl',0)*regime_mult:,.0f}")
                    c4.metric("Avg Days Held", f"{bench.get('dit',0):.0f}d")
                    
                    if not sub.empty:
                        sum_row = pd.DataFrame({'Name':['TOTAL'], 'Action':['-'], 'Grade':['-'], 'P&L':[sub['P&L'].sum()], 
                                                'Debit':[sub['Debit'].sum()], 'Days Held':[sub['Days Held'].mean()], 
                                                'Daily Yield %':[sub['Daily Yield %'].mean()], 'Theta':[sub['Theta'].sum()], 'Delta':[sub['Delta'].sum()]})
                        display = pd.concat([sub[cols], sum_row], ignore_index=True)
                        
                        st.dataframe(
                            display.style.format({'P&L':"${:,.0f}", 'Debit':"${:,.0f}", 'Daily Yield %':"{:.2f}%", 'Theta':"{:.1f}", 'Delta':"{:.1f}", 'Days Held':"{:.0f}"})
                            .map(lambda v: 'background-color: #d1e7dd; color: #0f5132' if 'TAKE' in str(v) else ('background-color: #f8d7da; color: #842029' if 'KILL' in str(v) else ''), subset=['Action'])
                            .map(lambda v: 'color: green' if 'A' in str(v) else ('color: red' if 'F' in str(v) else ''), subset=['Grade'])
                            .apply(lambda x: ['background-color: #f0f2f6; color: black; font-weight: bold' if x.name == len(display)-1 else '' for _ in x], axis=1),
                            use_container_width=True
                        )
                    else: st.info("No active trades.")

            render_strat(strat_tabs[1], '130/160')
            render_strat(strat_tabs[2], '160/190')
            render_strat(strat_tabs[3], 'M200')
            
            csv = active_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Export Active CSV", csv, "active_snapshot.csv", "text/csv")
        else:
            st.info("Empty Database. Upload 'Active' file.")

    # 2. VALIDATOR
    with tabs[1]:
        st.markdown("### 🧪 Pre-Flight Audit")
        with st.expander("ℹ️ Grading System Legend", expanded=True):
            st.markdown("""
            | Strategy | Grade | Debit Range (Per Lot) | Verdict |
            | :--- | :--- | :--- | :--- |
            | **130/160** | **A+** | `$3,500 - $4,500` | ✅ **Sweet Spot** |
            | **130/160** | **B** | `< $3,500` or `$4,500-$4,800` | ⚠️ **Acceptable** |
            | **130/160** | **F** | `> $4,800` | ⛔ **Overpriced** |
            | **160/190** | **A** | `$4,800 - $5,500` | ✅ **Ideal** |
            | **M200** | **A** | `$7,500 - $8,500` | ✅ **Perfect** |
            """)
        
        model_file = st.file_uploader("Upload Model File", key="mod")
        if model_file:
            try:
                if model_file.name.endswith('.xlsx'): m_df = pd.read_excel(model_file)
                else: 
                    m_df = pd.read_csv(model_file)
                    if 'Name' not in m_df.columns:
                        model_file.seek(0)
                        m_df = pd.read_csv(model_file, skiprows=1)
                
                if not m_df.empty:
                    row = m_df.iloc[0]
                    name = str(row.get('Name', ''))
                    debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                    strat = get_strategy(str(row.get('Group', '')), name)
                    
                    lot_size = 1
                    if strat == '130/160' and debit > 6000: lot_size = 2
                    elif strat == '130/160' and debit > 10000: lot_size = 3
                    elif strat == '160/190' and debit > 8000: lot_size = 2
                    elif strat == 'M200' and debit > 12000: lot_size = 2
                    
                    debit_lot = debit / lot_size
                    grade = "C"
                    if strat == '130/160': grade = "F" if debit_lot > 4800 else "A+" if 3500 <= debit_lot <= 4500 else "B"
                    if strat == '160/190': grade = "A" if 4800 <= debit_lot <= 5500 else "C"
                    if strat == 'M200': grade = "A" if 7500 <= debit_lot <= 8500 else "B"

                    st.divider()
                    st.subheader(f"Audit: {name}")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Strategy", strat)
                    c2.metric("Debit Total", f"${debit:,.0f}")
                    c3.metric("Debit Per Lot", f"${debit_lot:,.0f}")
                    
                    if not expired_df.empty:
                        similar = expired_df[
                            (expired_df['Strategy'] == strat) & 
                            (expired_df['Debit/Lot'].between(debit_lot*0.9, debit_lot*1.1))
                        ]
                        if not similar.empty:
                            avg_win = similar[similar['P&L']>0]['P&L'].mean()
                            st.info(f"📊 **Historical Context:** Found {len(similar)} similar trades. Average Win: **${avg_win:,.0f}**")
                    
                    if "A" in grade: st.success("✅ **APPROVED:** Great Entry")
                    elif "F" in grade: st.error("⛔ **REJECT:** Overpriced")
                    else: st.warning("⚠️ **CHECK:** Acceptable Variance")
            except Exception as e: st.error(f"Error: {e}")

    # 3. ANALYTICS
    with tabs[2]:
        if not df.empty:
            filt_df = df.copy()
            if 'entry_date' in df.columns:
                min_d, max_d = df['entry_date'].min(), df['entry_date'].max()
                rng = st.date_input("Filter Date", [min_d, max_d])
                if len(rng)==2: 
                    mask = (df['entry_date'] >= pd.to_datetime(rng[0])) & (df['entry_date'] <= pd.to_datetime(rng[1]))
                    filt_df = df[mask]
            
            an_tabs = st.tabs(["🚀 Efficiency", "⏳ Time vs Money", "⚔️ Head-to-Head", "🔥 Heatmap"])
            
            with an_tabs[0]:
                act_sub = filt_df[filt_df['Status']=='Active']
                if not act_sub.empty:
                    fig = px.scatter(act_sub, x='Days Held', y='Daily Yield %', color='Strategy', size='Debit', hover_data=['Name'])
                    st.plotly_chart(fig, use_container_width=True)
            
            with an_tabs[1]:
                exp_sub = filt_df[filt_df['Status']=='Expired']
                if not exp_sub.empty:
                    fig = px.scatter(exp_sub, x='Days Held', y='P&L', color='Strategy', size='Debit')
                    st.plotly_chart(fig, use_container_width=True)
            
            with an_tabs[2]: 
                exp_sub = filt_df[filt_df['Status']=='Expired']
                if not exp_sub.empty:
                    perf = exp_sub.groupby('Strategy').agg({'P&L':lambda x: (x>0).sum()/len(x)*100, 'Days Held':'mean', 'Daily Yield %':'mean'}).reset_index()
                    perf.columns = ['Strategy', 'Win Rate %', 'Avg Days', 'Avg Yield']
                    st.dataframe(perf.style.format({'Win Rate %':'{:.1f}%', 'Avg Yield':'{:.2f}%', 'Avg Days':'{:.0f}'}), use_container_width=True)
            
            with an_tabs[3]: 
                exp_sub = filt_df[filt_df['Status']=='Expired']
                if not exp_sub.empty:
                    fig = px.density_heatmap(exp_sub, x="Days Held", y="Strategy", z="P&L", histfunc="avg", color_continuous_scale="RdBu")
                    st.plotly_chart(fig, use_container_width=True)

    # 4. TIMELINE
    with tabs[3]:
        st.markdown("### 📜 Trade Timeline")
        trade_options = df['id'].unique()
        if len(trade_options) > 0:
            sel_trade_id = st.selectbox("Select Trade", trade_options, format_func=lambda x: df[df['id']==x]['Name'].iloc[0])
            conn = get_db_connection()
            history = pd.read_sql_query("SELECT * FROM snapshots WHERE trade_id = ? ORDER BY snapshot_date", conn, params=(sel_trade_id,))
            conn.close()
            if not history.empty:
                fig = px.line(history, x='snapshot_date', y='pnl', title=f"P&L History", markers=True)
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("No history snapshots available.")
        else: st.info("No trades.")

    # 5. ALLOCATION
    with tabs[4]:
        st.markdown(f"### 💰 Portfolio Allocation (Based on ${acct_size:,.0f})")
        st.info("💡 **Barbell Approach:** Balance high-growth M200 with steady 130/160 cash flow.")
        
        reserve = acct_size * 0.20
        deployable = acct_size - reserve
        
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 🐳 M200 (40%)")
            st.metric("Allocation", f"${deployable * 0.40:,.0f}")
            st.caption("Growth Engine. Enter Wed. Max 6 Trades.")
        with c2:
            st.markdown("#### 🔸 160/190 (30%)")
            st.metric("Allocation", f"${deployable * 0.30:,.0f}")
            st.caption("Stabilizer. Enter Fri. Max 7 Trades.")
        with c3:
            st.markdown("#### 🔹 130/160 (30%)")
            st.metric("Allocation", f"${deployable * 0.30:,.0f}")
            st.caption("Income Engine. Enter Mon. Max 9 Trades.")
            
        st.progress(0.8)
        st.caption(f"Cash Reserve: 20% (${reserve:,.0f}) for repairs/opportunities.")

    # 6. JOURNAL
    with tabs[5]:
        st.markdown("### 📓 Trade Journal")
        all_strats = list(df['Strategy'].unique())
        sel_strat = st.selectbox("Filter by Strategy", ["All"] + all_strats)
        j_df = df if sel_strat == "All" else df[df['Strategy'] == sel_strat]
        
        edited = st.data_editor(
            j_df[['id','Name','Strategy','P&L','Days Held','Notes']], 
            key="journal", hide_index=True, use_container_width=True,
            column_config={"id": st.column_config.TextColumn(disabled=True)}
        )
        if st.button("💾 Save Changes"):
            try:
                conn = get_db_connection()
                for i, r in edited.iterrows():
                    conn.execute("UPDATE trades SET notes = ?, days_held = ? WHERE id = ?", (r['Notes'], r['Days Held'], r['id']))
                conn.commit()
                st.success("Saved!")
                st.rerun()
            except Exception as e: st.error(f"Save failed: {e}")

    # 7. RULES
    with tabs[6]:
        st.markdown("""
        ### 1. 130/160 Strategy (Income Engine)
        * **Target Entry:** Monday.
        * **Debit Target:** `$3,500 - $4,500` per lot.
        * **Stop Rule:** Never pay > `$4,800` per lot.
        * **Management:** * **Kill Rule:** If trade is **>25 days old** AND profit is **flat/negative (<$100)**, EXIT immediately. Dead money.
            * **Take Profit:** Target **~$600** (Historical Avg).
        
        ### 2. 160/190 Strategy (Compounder)
        * **Target Entry:** Friday.
        * **Debit Target:** `~$5,200` per lot.
        * **Sizing:** Trade **1 Lot** (Scaling to 2 lots reduces ROI efficiency).
        * **Exit:** Hold for **40-50 Days**. Do not touch in first 30 days (Cooking Phase).
        
        ### 3. M200 Strategy (Whale)
        * **Target Entry:** Wednesday.
        * **Debit Target:** `$7,500 - $8,500` per lot.
        * **Management:** Check P&L at **Day 14**.
            * If **Green (>$200):** Exit or Roll.
            * If **Red/Flat:** HOLD. Do not exit in the "Dip Valley" (Day 15-50).
        """)

    st.sidebar.divider()
    st.sidebar.markdown("---")
    st.sidebar.caption("Allantis Trade Guardian v53.0 | Auto-Repair Edition | Dec 2024")
    st.sidebar.markdown("### 🎯 Quick Start\n1. Upload 'Active' File\n2. Check Action Center\n3. Review Health\n4. Export Records")
