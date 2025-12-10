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
st.title("🛡️ Allantis Trade Guardian: Certified DB v56.0")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v2.db"

def init_db():
    if not os.path.exists(DB_NAME):
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
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
    return sqlite3.connect(DB_NAME)

# --- HELPER FUNCTIONS ---
def get_strategy(group_name, trade_name):
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

# FIX: Improved numeric robustness
def clean_num(x):
    try:
        if pd.isna(x): return 0.0
        return float(str(x).replace('$', '').replace(',', ''))
    except:
        return 0.0

def generate_trade_id(name, strategy, entry_date):
    date_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{name}_{strategy}_{date_str}".replace(" ", "").replace("/", "-")

# --- REPAIR ENGINE ---
def repair_database():
    """Recalculates Days Held for ALL trades AND snapshots."""
    conn = get_db_connection()
    
    # 1. FIX TRADES
    df = pd.read_sql("SELECT * FROM trades", conn)
    trade_updates = []
    
    for _, row in df.iterrows():
        try:
            start = pd.to_datetime(row['entry_date'])
            exit_date_val = row['exit_date']
            
            # Priority: 1. Explicit Exit Date, 2. Today
            if row['status'] == "Expired" and pd.notnull(exit_date_val): # FIX: Use pd.notnull
                try:
                    end = pd.to_datetime(exit_date_val)
                    days = (end - start).days
                except: days = (datetime.now() - start).days
            else:
                days = (datetime.now() - start).days
            
            if days < 1: days = 1
            trade_updates.append((int(days), row['id'])) # FIX: Ensure days is int
        except: continue
        
    # 2. FIX SNAPSHOTS
    # Optimized query to avoid slow lookups inside the loop
    snap_df = pd.read_sql("SELECT s.id, s.trade_id, s.snapshot_date, t.entry_date FROM snapshots s JOIN trades t ON s.trade_id = t.id", conn)
    snap_updates = []
    
    for _, snap in snap_df.iterrows():
        try:
            start = pd.to_datetime(snap['entry_date'])
            snap_date = pd.to_datetime(snap['snapshot_date'])
            days = (snap_date - start).days
            if days < 1: days = 1
            snap_updates.append((int(days), snap['id'])) # FIX: Ensure days is int
        except: continue
        
    c = conn.cursor()
    c.executemany("UPDATE trades SET days_held = ? WHERE id = ?", trade_updates)
    c.executemany("UPDATE snapshots SET days_held = ? WHERE id = ?", snap_updates)
    conn.commit()
    conn.close()
    
    return len(trade_updates), len(snap_updates)

# --- BONUS: VALIDATION ---
def validate_row_data(row):
    name = str(row.get('Name', '')).strip()
    if name.startswith('.') or name in ['nan', '', 'Symbol', 'Name']: return False
    
    created = row.get('Created At', '')
    if not created or str(created).strip() == '' or str(created) == 'nan': return False
    
    # Basic check for essential numeric fields
    if clean_num(row.get('Net Debit/Credit', 0)) <= 0: return False
    if clean_num(row.get('Total Return $', 0)) == 0 and row.get('Status', 'Active') == 'Expired': 
        # An expired trade with 0 P&L is sometimes valid, but this flag is just defensive.
        pass
    
    return True

# --- SYNC ENGINE ---
def sync_data(file_list, file_type):
    log = []
    if not isinstance(file_list, list): file_list = [file_list]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for file in file_list:
        count_new, count_update = 0, 0 # FIX: Reset counters per file
        
        try:
            if file.name.endswith('.xlsx'): df = pd.read_excel(file)
            else:
                try:
                    df = pd.read_csv(file)
                    if 'Name' not in df.columns: 
                        file.seek(0)
                        df = pd.read_csv(file, skiprows=1)
                except: 
                    log.append(f"❌ Error {file.name}: Could not read file.")
                    continue
                
            for _, row in df.iterrows():
                if not validate_row_data(row): continue
                
                name = str(row.get('Name', ''))
                created_val = row.get('Created At', '')
                try: start_dt = pd.to_datetime(created_val)
                except: continue
                
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                
                # FIX: Lot size calculation - Check larger thresholds first
                lot_size = 1
                if strat == '130/160':
                    if debit > 10000: lot_size = 3
                    elif debit > 6000: lot_size = 2
                elif strat == '160/190':
                    if debit > 8000: lot_size = 2
                elif strat == 'M200':
                    if debit > 12000: lot_size = 2
                
                trade_id = generate_trade_id(name, strat, start_dt)
                status = "Active" if file_type == "Active" else "Expired"
                
                # FIX: Days Held Calculation
                exit_dt = None
                days_held = 1
                
                if file_type == "History":
                    exit_val = row.get('Expiration', '')
                    try:
                        if exit_val and str(exit_val).strip() not in ['', 'nan', 'NaT']:
                            exit_dt = pd.to_datetime(exit_val)
                            days_held = (exit_dt - start_dt).days
                        else:
                            # Fallback if expiration is missing in history file
                            days_held = (datetime.now() - start_dt).days
                    except: 
                        days_held = (datetime.now() - start_dt).days
                else:
                    days_held = (datetime.now() - start_dt).days

                days_held = int(days_held)
                if days_held < 1: days_held = 1

                # UPSERT
                cursor.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
                data = cursor.fetchone()
                
                if data is None:
                    cursor.execute('''INSERT INTO trades 
                                      (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, notes) 
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                   (trade_id, name, strat, status, start_dt.date(), 
                                    exit_dt.date() if exit_dt else None, 
                                    days_held, debit, lot_size, pnl, ""))
                    count_new += 1
                else:
                    if file_type == "History" or data[0] == "Active":
                        exit_date_update = exit_dt.date() if exit_dt else None
                        
                        cursor.execute('''UPDATE trades SET pnl=?, status=?, exit_date=?, days_held=?, lot_size=? WHERE id=?''', 
                                       (pnl, status, exit_date_update, days_held, lot_size, trade_id))
                        count_update += 1

                # SNAPSHOT (Only for Active trades)
                if file_type == "Active":
                    theta = clean_num(row.get('Theta', 0))
                    delta = clean_num(row.get('Delta', 0))
                    gamma = clean_num(row.get('Gamma', 0))
                    vega = clean_num(row.get('Vega', 0))
                    
                    today_str = datetime.now().date().isoformat() # FIX: Consistent string format
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
    SELECT t.id, t.name, t.strategy, t.status, t.entry_date, t.days_held, t.debit, t.lot_size, t.pnl, t.notes, t.exit_date,
           s.theta, s.delta, s.gamma, s.vega
    FROM trades t
    LEFT JOIN (SELECT * FROM snapshots WHERE id IN (SELECT MAX(id) FROM snapshots GROUP BY trade_id)) s ON t.id = s.trade_id
    """
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Database query error: {e}")
        return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status', 
            'pnl': 'P&L', 'debit': 'Debit', 'notes': 'Notes',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'days_held': 'Days Held'
        }, inplace=True)
        
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        
        # Calculate Active Days Live
        mask_active = df['Status'] == 'Active'
        df.loc[mask_active, 'calc_days'] = (datetime.now() - df.loc[mask_active, 'entry_date']).dt.days
        df.loc[mask_active, 'Days Held'] = df.loc[mask_active, 'calc_days'].apply(lambda x: max(1, int(x)))
        
        # Recalculate Days Held for Expired trades using exit_date if possible (repair_db should have handled this, but for robustness)
        mask_expired = df['Status'] == 'Expired'
        try:
             df.loc[mask_expired & pd.notnull(df['exit_date']), 'Days Held'] = (pd.to_datetime(df.loc[mask_expired & pd.notnull(df['exit_date']), 'exit_date']) - df.loc[mask_expired & pd.notnull(df['exit_date']), 'entry_date']).dt.days.apply(lambda x: max(1, int(x)))
        except:
            pass # Keep existing days_held if calculation fails

        df['Days Held'] = df['Days Held'].fillna(1).astype(int)
        df['Debit/Lot'] = df['Debit'] / df['lot_size']
        
        # Safe Daily Yield Calculation
        def safe_daily_yield(row):
            if row['Debit'] <= 0 or row['Days Held'] <= 0: return 0.0
            return (row['P&L'] / row['Debit'] * 100) / row['Days Held']
        df['Daily Yield %'] = df.apply(safe_daily_yield, axis=1)
        
        def get_grade(row):
            strat, debit_lot = row['Strategy'], row['Debit/Lot']
            if strat == '130/160': return "F" if debit_lot > 4800 else "A+" if 3500 <= debit_lot <= 4500 else "B"
            if strat == '160/190': return "A" if 4800 <= debit_lot <= 5500 else "C"
            if strat == 'M200': return "A" if 7500 <= debit_lot <= 8500 else "B"
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
            
    # Repair Engine
    if c2.button("🛠️ Repair DB"):
        t_count, s_count = repair_database()
        st.success(f"Fixed {t_count} Trades + {s_count} Snapshots.")
        st.rerun()

    if st.button("💾 Backup DB"):
        try:
            with open(DB_NAME, "rb") as f:
                st.download_button("⬇️ Download DB", f, file_name=f"trade_guardian_backup_{datetime.now().strftime('%Y%m%d')}.db", mime="application/x-sqlite3")
        except Exception as e: st.error(f"Error: {e}")

    uploaded_db = st.file_uploader("📥 Restore DB", type=['db', 'sqlite'], key='restore')
    if uploaded_db:
        if st.button("Confirm Restore"):
            with open(DB_NAME, "wb") as f: f.write(uploaded_db.getbuffer())
            st.success("Restored! Please re-run the app.")
            st.rerun()

st.sidebar.divider()
st.sidebar.header("⚙️ Settings")
acct_size = st.sidebar.number_input("Account Size ($)", value=150000, step=5000)
market_regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish (+10%)", "Bearish (-10%)"], index=0)
regime_mult = 1.1 if "Bullish" in market_regime else 0.9 if "Bearish" in market_regime else 1.0

# Base configuration for minimum acceptable pnl (if no history exists)
BASE_CONFIG = {
    '130/160': {'pnl': 600, 'yield': 0.15}, 
    '160/190': {'pnl': 420, 'yield': 0.10}, 
    'M200':    {'pnl': 910, 'yield': 0.20}
}

def get_action_signal(strat, status, days_held, pnl, benchmarks_dict):
    if status != "Active": return "", "NONE"
    
    benchmark = benchmarks_dict.get(strat, {})
    target = benchmark.get('pnl', 0)
    
    if target == 0: target = BASE_CONFIG.get(strat, {}).get('pnl', 9999)
    
    final_target = target * regime_mult
    
    if pnl >= final_target: return f"TAKE PROFIT (Hit ${final_target:,.0f})", "SUCCESS"
    if strat == '130/160' and 25 <= days_held <= 35 and pnl < 100: return "KILL (Stale >25d)", "ERROR"
    if strat == '160/190' and days_held < 30 and pnl > 0: return "COOKING (Hold)", "INFO"
    if strat == 'M200' and 12 <= days_held <= 16: return "DAY 14 CHECK", "WARNING"
    return "", "NONE"

# --- LOAD DATA ---
df = load_data_from_db()

if df.empty:
    st.info("👋 Database empty. Upload files to initialize.")
else:
    # --- BENCHMARKS (FIX: USE DEBIT/LOT FOR YIELD) ---
    expired_df = df[df['Status'] == 'Expired'].copy()
    benchmarks = BASE_CONFIG.copy()
    
    if not expired_df.empty:
        hist_grp = expired_df.groupby('Strategy')
        for strat, grp in hist_grp:
            winners = grp[grp['P&L'] > 0]
            if not winners.empty:
                real_pnl = winners['P&L'].mean()
                real_days = winners['Days Held'].mean()
                # CRITICAL FIX: Use winners' Debit/Lot for yield calculation
                avg_win_debit_lot = winners['Debit/Lot'].mean() 
                
                yield_val = 0
                if real_days > 0 and avg_win_debit_lot > 0:
                    # Yield based on P&L / (Debit/Lot)
                    yield_val = (real_pnl / avg_win_debit_lot * 100) / real_days
                
                benchmarks[strat] = {
                    'pnl': real_pnl,
                    'dit': real_days,
                    'yield': yield_val
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
                
                # Trend Calculation with proper fallback
                def get_trend(row):
                    cy = row['Daily Yield %']
                    ty = benchmarks.get(row['Strategy'], {}).get('yield', 0)
                    if ty == 0: 
                        # Use '-' if no historical benchmark exists for the strategy
                        return "-"
                    return "🟢" if cy >= ty else "🔴"
                
                agg['Trend'] = agg.apply(get_trend, axis=1)
                
                total = pd.DataFrame({'Strategy':['TOTAL'], 'P&L':[agg['P&L'].sum()], 'Debit':[agg['Debit'].sum()], 
                                      'Theta':[agg['Theta'].sum()], 'Trade Count':[agg['Trade Count'].sum()], 'Trend':['-'], 'Daily Yield %':[agg['Daily Yield %'].mean()]})
                
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
                    c1.metric("Hist. Win (P&L)", f"${bench.get('pnl',0):,.0f}")
                    c2.metric("Target Yield/Lot", f"{bench.get('yield',0):.2f}%")
                    c3.metric("Target (Adj)", f"${bench.get('pnl',0)*regime_mult:,.0f}")
                    c4.metric("Avg Duration", f"{bench.get('dit',0):.0f}d")
                    
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
        with st.expander("ℹ️ Grading System Legend (Debit Per Lot)", expanded=True):
            st.markdown("""
            | Strategy | Grade | Debit Range (Per Lot) | Verdict |
            | :--- | :--- | :--- | :--- |
            | **130/160** | **A+** | `$3,500 - $4,500` | ✅ **Sweet Spot** |
            | **130/160** | **B** | `< $3,500` or `$4,500-$4,800` | ⚠️ **Acceptable** |
            | **130/160** | **F** | `> $4,800` | ⛔ **Overpriced/High Risk** |
            | **160/190** | **A** | `$4,800 - $5,500` | ✅ **Ideal** |
            | **160/190** | **C** | `Other` | ⚠️ **Check Variance** |
            | **M200** | **A** | `$7,500 - $8,500` | ✅ **Perfect** |
            | **M200** | **B** | `Other` | ⚠️ **Check Variance** |
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
                    
                    # FIX: Recalculate lot size for validation consistency
                    lot_size = 1
                    if strat == '130/160':
                        if debit > 10000: lot_size = 3
                        elif debit > 6000: lot_size = 2
                    elif strat == '160/190':
                        if debit > 8000: lot_size = 2
                    elif strat == 'M200':
                        if debit > 12000: lot_size = 2

                    debit_lot = debit / lot_size
                    grade = "C"
                    if strat == '130/160': grade = "F" if debit_lot > 4800 else "A+" if 3500 <= debit_lot <= 4500 else "B"
                    if strat == '160/190': grade = "A" if 4800 <= debit_lot <= 5500 else "C"
                    if strat == 'M200': grade = "A" if 7500 <= debit_lot <= 8500 else "B"

                    st.divider()
                    st.subheader(f"Audit: {name}")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Strategy", strat)
                    c2.metric("Lot Size", lot_size)
                    c3.metric("Debit Total", f"${debit:,.0f}")
                    c4.metric("Debit Per Lot", f"${debit_lot:,.0f}")
                    
                    if not expired_df.empty:
                        # Find similar trades by strategy and debit/lot
                        similar = expired_df[
                            (expired_df['Strategy'] == strat) & 
                            (expired_df['Debit/Lot'].between(debit_lot*0.9, debit_lot*1.1))
                        ]
                        if not similar.empty:
                            avg_win_pnl = similar[similar['P&L']>0]['P&L'].mean()
                            avg_win_days = similar[similar['P&L']>0]['Days Held'].mean()
                            st.info(f"📊 **Historical Context:** Found {len(similar)} similar trades (±10% Debit/Lot). Average Winner P&L: **${avg_win_pnl:,.0f}** | Avg Days Held: **{avg_win_days:.0f}**")
                    
                    if "A" in grade: st.success("✅ **APPROVED:** Great Entry Price/Risk Profile.")
                    elif "F" in grade: st.error("⛔ **REJECT:** Overpriced. Exceeds max debit tolerance.")
                    else: st.warning("⚠️ **CHECK:** Acceptable Variance. Review historical context.")
            except Exception as e: st.error(f"Error processing model file: {e}")

    # 3. ANALYTICS (No changes, logic is sound)
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
                else: st.info("No active trades in this date range.")
            
            with an_tabs[1]:
                exp_sub = filt_df[filt_df['Status']=='Expired']
                if not exp_sub.empty:
                    fig = px.scatter(exp_sub, x='Days Held', y='P&L', color='Strategy', size='Debit', hover_data=['Name', 'Debit/Lot'])
                    st.plotly_chart(fig, use_container_width=True)
                else: st.info("No expired trades in this date range.")
            
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
                else: st.info("No expired trades for heatmap.")


    # 4. TIMELINE (Ensured date conversion for plotting)
    with tabs[3]:
        st.markdown("### 📜 Trade Timeline")
        trade_options = df['id'].unique()
        if len(trade_options) > 0:
            sel_trade_id = st.selectbox("Select Trade", trade_options, format_func=lambda x: df[df['id']==x]['Name'].iloc[0])
            conn = get_db_connection()
            history = pd.read_sql_query("SELECT * FROM snapshots WHERE trade_id = ? ORDER BY snapshot_date", conn, params=(sel_trade_id,))
            conn.close()
            if not history.empty:
                history['snapshot_date'] = pd.to_datetime(history['snapshot_date']) # FIX: Ensure datetime for plotting
                fig = px.line(history, x='snapshot_date', y='pnl', title=f"P&L History: {df[df['id']==sel_trade_id]['Name'].iloc[0]}", markers=True)
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("No history snapshots available.")
        else: st.info("No trades.")

    # 5. ALLOCATION (No changes)
    with tabs[4]:
        st.markdown(f"### 💰 Portfolio Allocation (Based on ${acct_size:,.0f})")
        st.info("💡 **Barbell Approach:** The strategy balances high-growth potential (M200) with steady income (130/160 and 160/190).")
        
        reserve = acct_size * 0.20
        deployable = acct_size - reserve
        
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("#### 🔹 130/160 (30%)")
            st.metric("Allocation", f"${deployable * 0.30:,.0f}")
            st.caption("Income Engine. Target low-risk, high-probability trades.")
        with c2:
            st.markdown("#### 🔸 160/190 (30%)")
            st.metric("Allocation", f"${deployable * 0.30:,.0f}")
            st.caption("Stabilizer. Consistent monthly returns.")
        with c3:
            st.markdown("#### 🐳 M200 (40%)")
            st.metric("Allocation", f"${deployable * 0.40:,.0f}")
            st.caption("Growth Engine. Maximize gains through higher risk/reward.")
            
        st.progress(0.8)
        st.caption(f"Cash Reserve: 20% (${reserve:,.0f}) kept for repairs/opportunistic entries.")

    # 6. JOURNAL (No changes)
    with tabs[5]:
        st.markdown("### 📓 Trade Journal")
        all_strats = list(df['Strategy'].unique())
        sel_strat = st.selectbox("Filter by Strategy", ["All"] + all_strats)
        j_df = df if sel_strat == "All" else df[df['Strategy'] == sel_strat]
        
        edited = st.data_editor(
            j_df[['id','Name','Strategy','P&L','Days Held','Notes']], 
            key="journal", hide_index=True, use_container_width=True,
            column_config={
                "id": st.column_config.TextColumn(disabled=True),
                "Name": st.column_config.TextColumn(disabled=True),
                "Strategy": st.column_config.TextColumn(disabled=True),
                "P&L": st.column_config.NumberColumn(disabled=True, format="$%.2f"),
                "Days Held": st.column_config.NumberColumn(min_value=1, max_value=365, step=1, help="Manually correct hold time"),
                "Notes": st.column_config.TextColumn()
            }
        )
        if st.button("💾 Save Changes"):
            try:
                conn = get_db_connection()
                for i, r in edited.iterrows():
                    # The P&L and other metrics are recalculated by the data loader, so only Notes and Days Held need updating
                    conn.execute("UPDATE trades SET notes = ?, days_held = ? WHERE id = ?", (r['Notes'], r['Days Held'], r['id']))
                conn.commit()
                st.success("Saved!")
                st.rerun()
            except Exception as e: st.error(f"Save failed: {e}")

    # 7. RULES (FIX: Restore detailed information)
    with tabs[6]:
        st.markdown("### 📖 Strategy and Rules Detailed Guide")
        
        st.markdown("#### 1. 130/160 Strategy (Income Engine)")
        st.markdown("* **Primary Goal:** Consistent, high-probability income generation with defined risk.")
        st.markdown("* **Target Entry:** **Monday**, ideally for weekly or monthly expiry cycles.")
        st.markdown("* **Ideal Debit/Lot:** **\$3,500 - \$4,500** (A+ Grade). Max acceptable is \$4,800.")
        st.markdown("* **Lot Sizing:** Automatically adjusts: **2 Lots** >\$6k, **3 Lots** >\$10k.")
        st.markdown("* **Target P&L:** Achieve **~$600 P&L** per lot.")
        st.markdown("* **Management (Kill Rule):** **Kill** the trade if it is **Stale** (Days Held between 25-35) AND **P&L is < \$100** to free up capital.")
        
        st.markdown("#### 2. 160/190 Strategy (Compounder)")
        st.markdown("* **Primary Goal:** Compound capital growth through slightly longer duration trades.")
        st.markdown("* **Target Entry:** **Friday**, to capture weekly time decay over the weekend.")
        st.markdown("* **Ideal Debit/Lot:** **\$4,800 - \$5,500** (A Grade).")
        st.markdown("* **Lot Sizing:** Typically **1 Lot** at entry. Max 2 Lots >\$8k.")
        st.markdown("* **Target Duration:** Hold for **40-50 days** or until target P&L is hit.")
        st.markdown("* **Management:** Allow the trade to **Cook** for the first 30 days unless there is a critical breach.")
        
        st.markdown("#### 3. M200 Strategy (Whale)")
        st.markdown("* **Primary Goal:** Maximize large, infrequent profits with higher risk/reward profile.")
        st.markdown("* **Target Entry:** **Wednesday**, focusing on long-dated options (45-60+ DTE).")
        st.markdown("* **Ideal Debit/Lot:** **\$7,500 - \$8,500** (A Grade).")
        st.markdown("* **Lot Sizing:** Typically **1 Lot**. Max 2 Lots >\$12k.")
        st.markdown("* **Target P&L:** Achieve **~$910 P&L** per lot.")
        st.markdown("* **Management (Day 14 Check):** **Day 14** is the key check point. If the trade is highly profitable (Green), look to **Roll** the position. If Red, **Hold** and monitor for next action point.")

    st.sidebar.divider()
    st.sidebar.markdown("---")
    st.sidebar.caption("Allantis Trade Guardian v56.0 | Benchmarks Re-Certified | Dec 2025")
    st.sidebar.markdown("### 🎯 Quick Start\n1. Upload 'Active' File\n2. Check Action Center\n3. Review Health\n4. Export Records")
