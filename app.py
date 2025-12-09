import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
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
                    entry_date DATE,
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
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_NAME)

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

def safe_fmt(val, fmt_str):
    try:
        if isinstance(val, (int, float)): return fmt_str.format(val)
        return str(val)
    except: return str(val)

def generate_trade_id(name, strategy, entry_date):
    date_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{name}_{strategy}_{date_str}".replace(" ", "").replace("/", "-")

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
                if name.startswith('.') or name == 'nan' or name == '': continue
                
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
                
                cursor.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
                data = cursor.fetchone()
                
                if data is None:
                    cursor.execute('''INSERT INTO trades (id, name, strategy, status, entry_date, debit, lot_size, pnl, notes) 
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                   (trade_id, name, strat, status, start_dt.date(), debit, lot_size, pnl, ""))
                    count_new += 1
                else:
                    if file_type == "History" or data[0] == "Active":
                        cursor.execute('''UPDATE trades SET pnl = ?, status = ? WHERE id = ?''', (pnl, status, trade_id))
                        count_update += 1

                if file_type == "Active":
                    theta = clean_num(row.get('Theta', 0))
                    delta = clean_num(row.get('Delta', 0))
                    gamma = clean_num(row.get('Gamma', 0))
                    vega = clean_num(row.get('Vega', 0))
                    days = (datetime.now() - start_dt).days
                    if days < 1: days = 1
                    
                    cursor.execute('''INSERT INTO snapshots (trade_id, snapshot_date, pnl, theta, delta, gamma, vega, days_held)
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                   (trade_id, datetime.now().date(), pnl, theta, delta, gamma, vega, days))

            log.append(f"✅ {file.name}: {count_new} New / {count_update} Updated")
        except Exception as e:
            log.append(f"❌ Error {file.name}: {str(e)}")
            
    conn.commit()
    conn.close()
    return log

# --- DATA LOADER ---
def load_data_from_db():
    conn = get_db_connection()
    query = """
    SELECT t.id, t.name, t.strategy, t.status, t.entry_date, t.debit, t.lot_size, t.pnl, t.notes,
           s.theta, s.delta, s.gamma, s.vega, s.days_held
    FROM trades t
    LEFT JOIN (SELECT * FROM snapshots WHERE id IN (SELECT MAX(id) FROM snapshots GROUP BY trade_id)) s ON t.id = s.trade_id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        # Standardize Columns
        df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status', 
            'pnl': 'P&L', 'debit': 'Debit', 'notes': 'Notes',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'days_held': 'Days Held'
        }, inplace=True)
        
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['calc_days'] = (datetime.now() - df['entry_date']).dt.days
        df['Days Held'] = df['Days Held'].fillna(df['calc_days']) 
        df.loc[df['Days Held'] < 1, 'Days Held'] = 1
        
        df['Debit/Lot'] = df['Debit'] / df['lot_size']
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
    active_files = st.file_uploader("1. ACTIVE Trades", type=['csv','xlsx'], accept_multiple_files=True, key='active')
    history_files = st.file_uploader("2. HISTORY (Closed)", type=['csv','xlsx'], accept_multiple_files=True, key='history')
    if st.button("🔄 Sync Database"):
        logs = []
        if active_files: logs.extend(sync_data(active_files, "Active"))
        if history_files: logs.extend(sync_data(history_files, "History"))
        if logs:
            for l in logs: st.write(l)
            st.success("Sync Complete!")
            st.rerun()

st.sidebar.divider()
st.sidebar.header("⚙️ Settings")
acct_size = st.sidebar.number_input("Account Size ($)", value=150000, step=5000)
market_regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish (+10%)", "Bearish (-10%)"], index=0)
regime_mult = 1.1 if "Bullish" in market_regime else 0.9 if "Bearish" in market_regime else 1.0

# --- CONFIG ---
BASE_CONFIG = {
    '130/160': {'yield': 0.13, 'pnl': 600}, 
    '160/190': {'yield': 0.28, 'pnl': 420}, 
    'M200':    {'yield': 0.56, 'pnl': 900}
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
                benchmarks[strat] = {
                    'yield': grp['Daily Yield %'].mean(),
                    'pnl': winners['P&L'].mean(),
                    'dit': winners['Days Held'].mean()
                }

    # --- TABS ---
    tabs = st.tabs(["📊 Dashboard", "🧪 Validator", "📈 Analytics", "💰 Allocation", "📓 Journal", "📖 Rules"])

    # 1. DASHBOARD
    with tabs[0]:
        active_df = df[df['Status'] == 'Active'].copy()
        
        if not active_df.empty:
            # Portfolio Health
            port_yield = active_df['Daily Yield %'].mean()
            if port_yield < 0.10: st.sidebar.error(f"🚨 Critical Yield: {port_yield:.2f}%")
            elif port_yield < 0.15: st.sidebar.warning(f"⚠️ Low Yield: {port_yield:.2f}%")
            else: st.sidebar.success(f"✅ Healthy: {port_yield:.2f}%")
            
            # Action Logic
            act_list, sig_list = [], []
            for _, row in active_df.iterrows():
                act, sig = get_action_signal(row['Strategy'], row['Status'], row['Days Held'], row['P&L'], benchmarks)
                act_list.append(act)
                sig_list.append(sig)
            active_df['Action'] = act_list
            active_df['Signal_Type'] = sig_list
            
            # Risk Center
            with st.expander("📊 Risk Command Center", expanded=True):
                c1, c2, c3 = st.columns(3)
                delta_net = active_df['Delta'].sum()
                c1.metric("Net Delta", f"{delta_net:,.1f}", delta="Bullish" if delta_net > 0 else "Bearish")
                c2.metric("Daily Theta", f"${active_df['Theta'].sum():,.0f}")
                c3.metric("Capital at Risk", f"${active_df['Debit'].sum():,.0f}")

            # Sub-Tabs
            strat_tabs = st.tabs(["📋 Overview", "🔹 130/160", "🔸 160/190", "🐳 M200"])
            
            # Overview Tab
            with strat_tabs[0]:
                agg = active_df.groupby('Strategy').agg({
                    'P&L':'sum', 'Debit':'sum', 'Theta':'sum', 'Name':'count', 'Daily Yield %':'mean'
                }).reset_index()
                agg.rename(columns={'Name': 'Trade Count'}, inplace=True) # Renamed per request
                agg['Trend'] = agg.apply(lambda r: "🟢" if r['Daily Yield %'] >= benchmarks.get(r['Strategy'], {}).get('yield', 0) else "🔴", axis=1)
                
                total = pd.DataFrame({'Strategy':['TOTAL'], 'P&L':[agg['P&L'].sum()], 'Debit':[agg['Debit'].sum()], 
                                      'Theta':[agg['Theta'].sum()], 'Trade Count':[agg['Trade Count'].sum()], 'Trend':['-']})
                
                st.dataframe(
                    pd.concat([agg, total], ignore_index=True)
                    .style.format({'P&L':"${:,.0f}", 'Debit':"${:,.0f}", 'Theta':"{:.0f}", 'Daily Yield %':"{:.2f}%"})
                    .apply(lambda x: ['background-color: #f0f2f6; color: black; font-weight: bold' if x.name == len(agg) else '' for _ in x], axis=1),
                    use_container_width=True
                )

            # Strategy Tabs
            cols = ['Name', 'Action', 'Grade', 'P&L', 'Debit', 'Days Held', 'Daily Yield %', 'Theta', 'Delta']
            
            def render_strat(tab, strat):
                with tab:
                    sub = active_df[active_df['Strategy'] == strat].copy()
                    bench = benchmarks.get(strat, BASE_CONFIG.get(strat))
                    
                    # COMPACT ALERT TILES
                    urgent = sub[sub['Action'] != ""]
                    if not urgent.empty:
                        st.markdown("**🚨 Action Center**")
                        for _, r in urgent.iterrows():
                            # Thin styled alerts
                            color = "green" if "TAKE" in r['Action'] else "red" if "KILL" in r['Action'] else "orange"
                            st.markdown(f"<span style='color:{color}; font-weight:bold'>● {r['Name']}</span>: {r['Action']}", unsafe_allow_html=True)
                        st.divider()

                    # Benchmarks
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Hist. Win", f"${bench.get('pnl',0):,.0f}")
                    c2.metric("Target Yield", f"{bench.get('yield',0):.2f}%")
                    c3.metric("Target (Adj)", f"${bench.get('pnl',0)*regime_mult:,.0f}")
                    c4.metric("Avg Hold", f"{bench.get('dit',0):.0f}d")
                    
                    if not sub.empty:
                        # Total Row
                        sum_row = pd.DataFrame({'Name':['TOTAL'], 'Action':['-'], 'Grade':['-'], 'P&L':[sub['P&L'].sum()], 
                                                'Debit':[sub['Debit'].sum()], 'Days Held':[sub['Days Held'].mean()], 
                                                'Daily Yield %':[sub['Daily Yield %'].mean()], 'Theta':[sub['Theta'].sum()], 'Delta':[sub['Delta'].sum()]})
                        display = pd.concat([sub[cols], sum_row], ignore_index=True)
                        
                        st.dataframe(
                            display.style.format({'P&L':"${:,.0f}", 'Debit':"${:,.0f}", 'Daily Yield %':"{:.2f}%", 'Theta':"{:.1f}", 'Delta':"{:.1f}", 'Days Held':"{:.0f}"})
                            .applymap(lambda v: 'background-color: #d1e7dd; color: #0f5132' if 'TAKE' in str(v) else 'background-color: #f8d7da; color: #842029' if 'KILL' in str(v) else '', subset=['Action'])
                            .applymap(lambda v: 'color: green' if 'A' in str(v) else 'color: red' if 'F' in str(v) else '', subset=['Grade'])
                            # LIGHTER GREY TOTAL ROW
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
            | **130/160** | **A+** | `$3,500 - $4,500` | ✅ **Sweet Spot** (Highest statistical win rate) |
            | **130/160** | **B** | `< $3,500` or `$4,500-$4,800` | ⚠️ **Acceptable** (Watch volatility) |
            | **130/160** | **F** | `> $4,800` | ⛔ **Overpriced** (Historical failure rate 100%) |
            | **160/190** | **A** | `$4,800 - $5,500` | ✅ **Ideal** Pricing |
            | **160/190** | **C** | `> $5,500` | ⚠️ **Expensive** (Reduces ROI efficiency) |
            | **M200** | **A** | `$7,500 - $8,500` | ✅ **Perfect** "Whale" sizing |
            | **M200** | **B** | Any other price | ⚠️ **Variance** from mean |
            """)
            
        model_file = st.file_uploader("Upload Model File", key="mod")
        if model_file:
            m_df = process_data([model_file])
            if not m_df.empty:
                row = m_df.iloc[0]
                st.divider()
                st.subheader(f"Audit: {row['Name']}")
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Strategy", row['Strategy'])
                c2.metric("Debit Total", f"${row['Debit']:,.0f}")
                c3.metric("Debit Per Lot", f"${row['Debit/Lot']:,.0f}")
                
                # COMPARISON
                if not expired_df.empty:
                    similar = expired_df[
                        (expired_df['Strategy'] == row['Strategy']) & 
                        (expired_df['Debit/Lot'].between(row['Debit/Lot']*0.9, row['Debit/Lot']*1.1))
                    ]
                    if not similar.empty:
                        avg_win = similar[similar['P&L']>0]['P&L'].mean()
                        st.info(f"📊 **Historical Context:** Found {len(similar)} similar trades. Average Win: **${avg_win:,.0f}**")
                
                if "A" in row['Grade']:
                    st.success(f"✅ **APPROVED:** {row['Reason']}")
                elif "F" in row['Grade']:
                    st.error(f"⛔ **REJECT:** {row['Reason']}")
                else:
                    st.warning(f"⚠️ **CHECK:** {row['Reason']}")

    # 3. ANALYTICS
    with tabs[2]:
        if not df.empty:
            if 'entry_date' in df.columns:
                min_d, max_d = df['entry_date'].min(), df['entry_date'].max()
                rng = st.date_input("Filter Date", [min_d, max_d])
                if len(rng)==2: 
                    mask = (df['entry_date'] >= pd.to_datetime(rng[0])) & (df['entry_date'] <= pd.to_datetime(rng[1]))
                    filt_df = df[mask]
                else: filt_df = df
            else: filt_df = df
            
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
                if not exp_sub.empty:
                    perf = exp_sub.groupby('Strategy').agg({'P&L':['count','mean','sum'], 'Days Held':'mean', 'Daily Yield %':'mean'}).reset_index()
                    st.dataframe(perf, use_container_width=True)
            
            with an_tabs[3]: 
                if not exp_sub.empty:
                    fig = px.density_heatmap(exp_sub, x="Days Held", y="Strategy", z="P&L", histfunc="avg", color_continuous_scale="RdBu")
                    st.plotly_chart(fig, use_container_width=True)

    # 4. ALLOCATION (DYNAMIC)
    with tabs[3]:
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

    # 5. JOURNAL
    with tabs[4]:
        st.markdown("### 📓 Trade Journal")
        
        # Strategy Filter
        all_strats = list(df['Strategy'].unique())
        sel_strat = st.selectbox("Filter by Strategy", ["All"] + all_strats)
        
        j_df = df if sel_strat == "All" else df[df['Strategy'] == sel_strat]
        
        edited = st.data_editor(j_df[['id','Name','Strategy','P&L','Notes']], key="journal", hide_index=True, use_container_width=True)
        if st.button("💾 Save Notes"):
            conn = get_db_connection()
            for i, r in edited.iterrows():
                conn.execute("UPDATE trades SET notes = ? WHERE id = ?", (r['Notes'], r['id']))
            conn.commit()
            st.success("Saved!")
            st.rerun()

    # 6. RULES (DETAILED)
    with tabs[5]:
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

    # QUICK START
    st.sidebar.divider()
    st.sidebar.markdown("### 🎯 Quick Start\n1. Upload 'Active' File\n2. Check Action Center\n3. Review Health\n4. Export Records")
