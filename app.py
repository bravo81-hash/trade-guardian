import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import sqlite3
import os
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")
st.title("🛡️ Allantis Trade Guardian: Fresh Start (v3)")

# --- DATABASE ENGINE ---
# CHANGED NAME TO FORCE A FRESH START
DB_NAME = "trade_guardian_v3.db"

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

# --- HELPER FUNCTIONS ---
def get_strategy(group_name, trade_name):
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    try: return float(str(x).replace('$', '').replace(',', ''))
    except: return 0.0

def generate_id(name, strategy, entry_date):
    # Unique ID Generation
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{name}_{strategy}_{d_str}".replace(" ", "").replace("/", "-")

# --- SMART FILE READER ---
def read_file_safely(file):
    """Handles CSVs with potential garbage rows at the top."""
    try:
        if file.name.endswith('.xlsx'):
            return pd.read_excel(file)
        else:
            # It's a CSV
            content = file.getvalue().decode("utf-8")
            # Try finding the header
            lines = content.split('\n')
            header_row = 0
            for i, line in enumerate(lines[:10]):
                if "Name" in line and "Total Return" in line:
                    header_row = i
                    break
            
            # Reset pointer and read
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
                # 1. Validation
                name = str(row.get('Name', ''))
                if name.startswith('.') or name in ['nan', '', 'Symbol']: continue
                
                created = row.get('Created At', '')
                try: start_dt = pd.to_datetime(created)
                except: continue
                
                # 2. Extract Data
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                theta = clean_num(row.get('Theta', 0))
                delta = clean_num(row.get('Delta', 0))
                
                # Lot Size
                lot_size = 1
                if strat == '130/160' and debit > 6000: lot_size = 2
                elif strat == '130/160' and debit > 10000: lot_size = 3
                elif strat == '160/190' and debit > 8000: lot_size = 2
                elif strat == 'M200' and debit > 12000: lot_size = 2

                trade_id = generate_id(name, strat, start_dt)
                status = "Active" if file_type == "Active" else "Expired"
                
                # 3. Date Logic (The Fix)
                exit_dt = None
                days_held = 1
                
                if file_type == "History":
                    try:
                        # History file: Expiration column = Exit Date
                        exit_dt = pd.to_datetime(row.get('Expiration'))
                        days_held = (exit_dt - start_dt).days
                    except: days_held = 1
                else:
                    # Active file: Days = Today - Start
                    days_held = (datetime.now() - start_dt).days
                
                if days_held < 1: days_held = 1
                
                # 4. DB Upsert
                c.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
                if existing is None:
                    # Insert New
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, name, strat, status, start_dt.date(), 
                         exit_dt.date() if exit_dt else None, 
                         days_held, debit, lot_size, pnl, theta, delta, ""))
                    count_new += 1
                else:
                    # Update Existing
                    # Only update if source is History (Truth) or currently Active
                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=? 
                            WHERE id=?''', 
                            (pnl, status, exit_dt.date() if exit_dt else None, days_held, theta, delta, trade_id))
                        count_update += 1
                    elif existing[0] == "Active":
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=? 
                            WHERE id=?''', 
                            (pnl, days_held, theta, delta, trade_id))
                        count_update += 1
                        
                # 5. Snapshot (Active Only)
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
    except: return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        # Standardize Columns
        df.columns = [x.lower() for x in df.columns]
        
        # Types
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['debit'] = df['debit'].fillna(0)
        df['pnl'] = df['pnl'].fillna(0)
        
        # Calc
        df['debit_lot'] = df['debit'] / df['lot_size'].replace(0, 1)
        df['daily_yield'] = (df['pnl'] / df['debit'].replace(0, 1) * 100) / df['days_held'].replace(0, 1)
        
        # Grading
        def get_grade(row):
            s, d = row['strategy'], row['debit_lot']
            if s == '130/160': return "A+" if 3500<=d<=4500 else "F" if d>4800 else "B"
            if s == '160/190': return "A" if 4800<=d<=5500 else "C"
            if s == 'M200': return "A" if 7500<=d<=8500 else "B"
            return "C"
        df['grade'] = df.apply(get_grade, axis=1)
        
    return df

# --- INITIALIZE ---
init_db()

# --- SIDEBAR ---
with st.sidebar.expander("📂 Data Sync", expanded=True):
    active_up = st.file_uploader("1. ACTIVE Trades", accept_multiple_files=True, key="act")
    history_up = st.file_uploader("2. HISTORY (Closed)", accept_multiple_files=True, key="hist")
    
    if st.button("🔄 Sync New Data"):
        logs = []
        if active_up: logs.extend(sync_data(active_up, "Active"))
        if history_up: logs.extend(sync_data(history_up, "History"))
        
        if logs:
            for l in logs: st.write(l)
            st.success("Synced!")
            st.rerun()

    if st.button("💾 Backup DB"):
        with open(DB_NAME, "rb") as f:
            st.download_button("⬇️ Download DB", f, "trade_guardian_v3.db", "application/x-sqlite3")
            
    restore = st.file_uploader("📥 Restore DB", type=['db'])
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
        st.success("Restored!")
        st.rerun()

st.sidebar.divider()
acct_size = st.sidebar.number_input("Account Size", value=150000, step=5000)
regime = st.sidebar.selectbox("Regime", ["Neutral", "Bullish (+10%)", "Bearish (-10%)"])
mult = 1.1 if "Bullish" in regime else 0.9 if "Bearish" in regime else 1.0

# --- CONFIG ---
BASE_CONFIG = {
    '130/160': {'pnl': 600}, 
    '160/190': {'pnl': 420}, 
    'M200':    {'pnl': 910}
}

# --- MAIN ---
df = load_data()

if df.empty:
    st.info("👋 System Ready. Please upload files to begin.")
else:
    # BENCHMARKS (Recalculate from DB)
    expired = df[df['status'] == 'Expired']
    benchmarks = BASE_CONFIG.copy()
    if not expired.empty:
        for strat, grp in expired.groupby('strategy'):
            wins = grp[grp['pnl'] > 0]
            if not wins.empty:
                avg_win = wins['pnl'].mean()
                avg_debit = grp['debit'].mean() # Use total average
                avg_days = grp['days_held'].mean()
                
                # True Yield = (Avg Win / Avg Debit) / Avg Days
                y = (avg_win / avg_debit * 100) / avg_days if avg_days > 0 else 0
                benchmarks[strat] = {'pnl': avg_win, 'yield': y, 'dit': avg_days}

    # TABS
    t1, t2, t3, t4, t5 = st.tabs(["Dashboard", "Analysis", "Allocation", "Journal", "Rules"])

    # 1. DASHBOARD
    with t1:
        active = df[df['status'] == 'Active'].copy()
        
        if not active.empty:
            # Action Logic
            def get_action(row):
                bench = benchmarks.get(row['strategy'], {'pnl':9999})
                target = bench['pnl'] * mult
                if row['pnl'] >= target: return "TAKE PROFIT"
                if row['strategy'] == '130/160' and row['days_held'] > 25 and row['pnl'] < 100: return "KILL (Stale)"
                if row['strategy'] == 'M200' and 12 <= row['days_held'] <= 16: return "DAY 14 CHECK"
                return ""
            active['Action'] = active.apply(get_action, axis=1)
            
            # Alerts
            urgent = active[active['Action'] != ""]
            if not urgent.empty:
                st.markdown("### 🚨 Action Center")
                for _, r in urgent.iterrows():
                    color = "green" if "TAKE" in r['Action'] else "red"
                    st.markdown(f"<span style='color:{color}'>**{r['name']}**: {r['Action']}</span>", unsafe_allow_html=True)
                st.divider()
                
            # Overview Tab
            st_tabs = st.tabs(["Overview", "130/160", "160/190", "M200"])
            
            # Helper
            def style_df(d):
                return (d.style.format({'pnl':'${:,.0f}', 'debit':'${:,.0f}', 'theta':'{:.1f}', 'daily_yield':'{:.2f}%', 'days_held':'{:.0f}'})
                        .map(lambda v: 'background-color:#d1e7dd' if 'TAKE' in str(v) else 'background-color:#f8d7da' if 'KILL' in str(v) else '', subset=['Action']))

            with st_tabs[0]:
                agg = active.groupby('strategy').agg({'pnl':'sum','debit':'sum','name':'count'}).reset_index()
                st.markdown("#### Portfolio Risk")
                c1,c2,c3 = st.columns(3)
                c1.metric("Net Delta", f"{active['delta'].sum():.1f}")
                c2.metric("Daily Theta", f"${active['theta'].sum():.0f}")
                c3.metric("Capital at Risk", f"${active['debit'].sum():,.0f}")
                st.dataframe(style_df(active[['name','strategy','pnl','debit','days_held','Action']]), use_container_width=True)

            for i, s in enumerate(['130/160', '160/190', 'M200'], 1):
                with st_tabs[i]:
                    sub = active[active['strategy'] == s]
                    b = benchmarks.get(s, {})
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Target Profit", f"${b.get('pnl',0)*mult:,.0f}")
                    c2.metric("Target Yield", f"{b.get('yield',0)*100:.2f}%")
                    c3.metric("Avg Duration", f"{b.get('dit',0):.0f}d")
                    
                    if not sub.empty: st.dataframe(style_df(sub[['name','Action','pnl','days_held','daily_yield','theta']]), use_container_width=True)
                    else: st.info("No trades.")
        else:
            st.info("No active trades.")

    # 2. ANALYSIS
    with t2:
        if not expired.empty:
            st.markdown("### 🏆 Performance")
            c1, c2 = st.columns(2)
            c1.metric("Realized P&L", f"${expired['pnl'].sum():,.0f}")
            c2.metric("Win Rate", f"{(len(expired[expired['pnl']>0])/len(expired)*100):.1f}%")
            
            an_tabs = st.tabs(["Efficiency", "Head-to-Head", "Timeline"])
            with an_tabs[0]:
                fig = px.scatter(expired, x='days_held', y='pnl', color='strategy', title="P&L vs Duration")
                st.plotly_chart(fig, use_container_width=True)
            with an_tabs[1]:
                perf = expired.groupby('strategy').agg({'pnl':['count','mean','sum'], 'days_held':'mean'}).reset_index()
                st.dataframe(perf, use_container_width=True)
            with an_tabs[2]:
                # Timeline requires snapshots, showing static view for now
                fig = px.bar(expired, x='exit_date', y='pnl', color='strategy', title="Realized P&L Over Time")
                st.plotly_chart(fig, use_container_width=True)

    # 3. ALLOCATION
    with t3:
        st.markdown(f"### 💰 Target Allocation (${acct_size:,.0f})")
        deploy = acct_size * 0.8
        c1, c2, c3 = st.columns(3)
        c1.metric("M200 (40%)", f"${deploy*0.4:,.0f}")
        c2.metric("160/190 (30%)", f"${deploy*0.3:,.0f}")
        c3.metric("130/160 (30%)", f"${deploy*0.3:,.0f}")

    # 4. JOURNAL
    with t4:
        st.markdown("### 📓 Journal")
        edited = st.data_editor(df[['id', 'name', 'strategy', 'pnl', 'days_held', 'notes']], key="journal", hide_index=True)
        if st.button("💾 Save Changes"):
            conn = get_db_connection()
            for i, r in edited.iterrows():
                conn.execute("UPDATE trades SET notes = ?, days_held = ? WHERE id = ?", (r['notes'], r['days_held'], r['id']))
            conn.commit()
            conn.close()
            st.success("Saved!")
            st.rerun()
            
    # 5. RULES
    with t5:
        st.markdown("""
        ### 1. 130/160
        * Mon Entry. $3.5k-$4.5k Debit. Kill >25d & Flat.
        ### 2. 160/190
        * Fri Entry. ~$5.2k Debit. Hold 40-50d.
        ### 3. M200
        * Wed Entry. $7.5k-$8.5k Debit. Day 14 Check.
        """)

    # DEBUGGER
    with st.expander("🕵️‍♂️ Debugger (Raw DB)"):
        st.write(df)
