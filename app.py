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
st.title("🛡️ Allantis Trade Guardian: Auto-Fix DB")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian.db"

def init_and_migrate_db():
    """Creates DB if missing, and ADDS missing columns if they don't exist."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # 1. Create Table (if not exists)
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    strategy TEXT,
                    status TEXT,
                    entry_date DATE,
                    days_held INTEGER,
                    debit REAL,
                    pnl REAL,
                    theta REAL,
                    delta REAL,
                    notes TEXT
                )''')
    
    # 2. AUTO-MIGRATE: Check for missing columns (The Crash Fix)
    # Get current columns
    c.execute("PRAGMA table_info(trades)")
    existing_cols = [info[1] for info in c.fetchall()]
    
    # Add 'days_held' if missing
    if 'days_held' not in existing_cols:
        try:
            c.execute("ALTER TABLE trades ADD COLUMN days_held INTEGER")
            print("Migrated: Added days_held")
        except: pass

    # Add 'theta' if missing
    if 'theta' not in existing_cols:
        try:
            c.execute("ALTER TABLE trades ADD COLUMN theta REAL")
            print("Migrated: Added theta")
        except: pass

    # Add 'delta' if missing
    if 'delta' not in existing_cols:
        try:
            c.execute("ALTER TABLE trades ADD COLUMN delta REAL")
            print("Migrated: Added delta")
        except: pass
        
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_NAME)

# --- v35 LOGIC: STRATEGY & CLEANING ---
def get_strategy(group_name, trade_name):
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    try:
        if pd.isna(x): return 0.0
        return float(str(x).replace('$', '').replace(',', ''))
    except: return 0.0

def generate_id(name, strategy, entry_date):
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{name}_{strategy}_{d_str}".replace(" ", "").replace("/", "-")

# --- v35 LOGIC: SYNC ENGINE ---
def sync_file(file, file_type):
    conn = get_db_connection()
    c = conn.cursor()
    count = 0
    
    try:
        if file.name.endswith('.xlsx'):
            df = pd.read_excel(file)
        else:
            df = pd.read_csv(file)
            if 'Name' not in df.columns:
                file.seek(0)
                df = pd.read_csv(file, skiprows=1)
        
        for _, row in df.iterrows():
            name = str(row.get('Name', ''))
            if name.startswith('.') or name == 'nan' or name == '' or name == 'Symbol': continue
            
            created = row.get('Created At', '')
            try: start_dt = pd.to_datetime(created)
            except: continue
            
            strat = get_strategy(str(row.get('Group', '')), name)
            pnl = clean_num(row.get('Total Return $', 0))
            debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
            theta = clean_num(row.get('Theta', 0))
            delta = clean_num(row.get('Delta', 0))
            
            tid = generate_id(name, strat, start_dt)
            status = "Active" if file_type == "Active" else "Expired"
            
            # Simple Date Math
            if status == "Expired":
                try: 
                    # Try to use Expiration column as Exit Date proxy for history files
                    end_dt = pd.to_datetime(row.get('Expiration'))
                    # Safety check: If Expiration is way in future (e.g. >30 days from now), 
                    # it implies trade is NOT held till expiration.
                    # v35 logic accepted this, but let's cap it at Today to avoid "130 days held" bugs
                    if end_dt > datetime.now():
                        days = (datetime.now() - start_dt).days
                    else:
                        days = (end_dt - start_dt).days
                except: days = 1
            else:
                days = (datetime.now() - start_dt).days
            
            if days < 1: days = 1
            
            c.execute("SELECT id FROM trades WHERE id = ?", (tid,))
            exists = c.fetchone()
            
            if exists:
                if status == "Active" or file_type == "History":
                    c.execute("""UPDATE trades SET 
                                 pnl=?, status=?, days_held=?, theta=?, delta=? 
                                 WHERE id=?""", 
                              (pnl, status, days, theta, delta, tid))
            else:
                c.execute("""INSERT INTO trades 
                             (id, name, strategy, status, entry_date, days_held, debit, pnl, theta, delta, notes)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                          (tid, name, strat, status, start_dt.date(), days, debit, pnl, theta, delta, ""))
            count += 1
            
        conn.commit()
        return f"✅ Parsed {count} trades from {file.name}"
        
    except Exception as e:
        return f"❌ Error {file.name}: {str(e)}"
    finally:
        conn.close()

# --- INITIALIZE ---
init_and_migrate_db() # Run Auto-Healer

# --- STATIC BENCHMARKS (Hardcoded Safety) ---
STATIC_BENCHMARKS = {
    '130/160': {'yield': 0.33, 'pnl': 600, 'dit': 30},
    '160/190': {'yield': 0.16, 'pnl': 420, 'dit': 45},
    'M200':    {'yield': 0.37, 'pnl': 910, 'dit': 30}
}

# --- SIDEBAR ---
with st.sidebar.expander("📂 Data Sync", expanded=True):
    active_up = st.file_uploader("1. Active Files", accept_multiple_files=True, key="act")
    history_up = st.file_uploader("2. History Files", accept_multiple_files=True, key="hist")
    
    if st.button("🔄 Sync Data"):
        if active_up:
            for f in active_up: st.write(sync_file(f, "Active"))
        if history_up:
            for f in history_up: st.write(sync_file(f, "History"))
        st.success("Done!")
        st.rerun()

    conn = get_db_connection()
    count = pd.read_sql("SELECT count(*) as c FROM trades", conn).iloc[0]['c']
    st.caption(f"Trades in DB: {count}")
    conn.close()
    
    with open(DB_NAME, "rb") as f:
        st.download_button("⬇️ Download DB", f, "trade_guardian.db", "application/x-sqlite3")
    
    restore = st.file_uploader("📥 Restore DB", type=['db'])
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
        st.success("Restored!")
        st.rerun()

st.sidebar.divider()
acct_size = st.sidebar.number_input("Account Size", value=150000, step=1000)
regime = st.sidebar.selectbox("Regime", ["Neutral", "Bullish (+10%)", "Bearish (-10%)"])
mult = 1.1 if "Bullish" in regime else 0.9 if "Bearish" in regime else 1.0

# --- MAIN LOGIC ---
conn = get_db_connection()
df = pd.read_sql("SELECT * FROM trades", conn)
conn.close()

if df.empty:
    st.info("👋 System Ready. Please upload files to begin.")
else:
    # Normalize columns to prevent KeyErrors
    df.columns = df.columns.str.lower()
    
    # Fill NaN values for calculation safety
    df['debit'] = df['debit'].fillna(0)
    df['pnl'] = df['pnl'].fillna(0)
    df['days_held'] = df['days_held'].fillna(1).replace(0, 1)
    
    # Calculations
    df['Daily Yield %'] = (df['pnl'] / df['debit'].replace(0, 1) * 100) / df['days_held']
    
    def get_grade(row):
        strat, debit = row['strategy'], row['debit']
        if strat == '130/160': return "A+" if 3500 <= debit <= 4500 else "B"
        if strat == '160/190': return "A" if 4800 <= debit <= 5500 else "C"
        if strat == 'M200': return "A" if 7500 <= debit <= 8500 else "B"
        return "C"
    
    df['Grade'] = df.apply(get_grade, axis=1)

    t1, t2, t3, t4, t5 = st.tabs(["Dashboard", "Analysis", "Allocation", "Journal", "Rules"])

    # 1. DASHBOARD
    with t1:
        # Filter Case-Insensitively
        active = df[df['status'].str.lower() == 'active'].copy()
        
        if not active.empty:
            def get_action(row):
                bench = STATIC_BENCHMARKS.get(row['strategy'], {'pnl':9999})
                target = bench['pnl'] * mult
                if row['pnl'] >= target: return "TAKE PROFIT"
                if row['strategy'] == '130/160' and row['days_held'] > 25 and row['pnl'] < 100: return "KILL (Stale)"
                if row['strategy'] == 'M200' and 12 <= row['days_held'] <= 16: return "DAY 14 CHECK"
                return ""
            
            active['Action'] = active.apply(get_action, axis=1)
            
            urgent = active[active['Action'] != ""]
            if not urgent.empty:
                st.markdown("### 🚨 Action Center")
                for _, r in urgent.iterrows():
                    color = "green" if "TAKE" in r['Action'] else "red"
                    st.markdown(f"<span style='color:{color}'>**{r['name']}**: {r['Action']}</span>", unsafe_allow_html=True)
                st.divider()

            st.markdown("### 📋 Active Overview")
            
            # Helper for displaying columns safely
            disp_cols = ['name', 'strategy', 'Action', 'Grade', 'pnl', 'debit', 'days_held']
            # Add theta/delta if they exist
            if 'theta' in active.columns: disp_cols.append('theta')
            
            st.dataframe(
                active[disp_cols].style
                .format({'pnl': '${:.0f}', 'debit': '${:.0f}', 'theta': '{:.1f}'})
                .map(lambda x: 'background-color: #d1e7dd' if 'TAKE' in str(x) else 'background-color: #f8d7da' if 'KILL' in str(x) else '', subset=['Action']),
                use_container_width=True
            )
        else:
            st.info("No active trades.")

    # 2. ANALYSIS
    with t2:
        expired = df[df['status'].str.lower() == 'expired']
        if not expired.empty:
            st.markdown("### 🏆 Performance")
            c1, c2 = st.columns(2)
            c1.metric("Realized P&L", f"${expired['pnl'].sum():,.0f}")
            c2.metric("Win Rate", f"{(len(expired[expired['pnl']>0])/len(expired)*100):.1f}%")
            
            st.markdown("#### Profit by Strategy")
            st.bar_chart(expired.groupby('strategy')['pnl'].sum())
            
            st.markdown("#### P&L vs Duration")
            fig = px.scatter(expired, x='days_held', y='pnl', color='strategy', title="Sweet Spots")
            st.plotly_chart(fig, use_container_width=True)

    # 3. ALLOCATION
    with t3:
        st.markdown(f"### 💰 Target Allocation (${acct_size:,.0f})")
        deploy = acct_size * 0.8
        c1, c2, c3 = st.columns(3)
        c1.metric("M200 (40%)", f"${deploy*0.4:,.0f}")
        c2.metric("160/190 (30%)", f"${deploy*0.3:,.0f}")
        c3.metric("130/160 (30%)", f"${deploy*0.3:,.0f}")
        st.caption("Cash Reserve: 20%")

    # 4. JOURNAL
    with t4:
        st.markdown("### 📓 Journal")
        edited = st.data_editor(df[['id', 'name', 'strategy', 'pnl', 'notes']], key="journal", hide_index=True)
        if st.button("💾 Save Notes"):
            conn = get_db_connection()
            for i, r in edited.iterrows():
                conn.execute("UPDATE trades SET notes = ? WHERE id = ?", (r['notes'], r['id']))
            conn.commit()
            conn.close()
            st.success("Saved!")
            st.rerun()

    # 5. RULES
    with t5:
        st.markdown("""
        ### 1. 130/160 (Income)
        * Target: Mon. Debit: $3.5k-$4.5k. Manage: Kill >25d & Flat.
        ### 2. 160/190 (Compound)
        * Target: Fri. Debit: ~$5.2k. Hold 40-50d.
        ### 3. M200 (Growth)
        * Target: Wed. Debit: $7.5k-$8.5k. Day 14 Check.
        """)
