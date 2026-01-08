import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import re
from datetime import datetime, timedelta

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- CONSTANTS & CONFIG ---
VERSION = "v1.3.1 (Schema Auto-Fix & Logic Upgrade)"
DB_NAME = "trade_guardian_v4.db"

# Strategy Specific Thresholds
STRATEGY_CONFIG = {
    "M200": {
        "profit_target": 7500,  # Per Lot
        "valley_start": 15,
        "valley_end": 40,
        "max_delta_tolerance": 5.0,
    },
    "130/160": {
        "profit_target": 3000,
        "valley_start": 0,
        "valley_end": 0,
        "max_delta_tolerance": 3.0,
    }
}

# --- DATABASE ENGINE & MIGRATION ---
def get_db_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def run_migration(conn):
    """
    Inspects existing DB and adds missing columns to match v131 schema.
    This prevents 'no such column' errors on existing v130 databases.
    """
    c = conn.cursor()
    
    # 1. Check TRADES table columns
    try:
        c.execute("PRAGMA table_info(trades)")
        columns = [info[1] for info in c.fetchall()]
        
        # Add 'current_price' if missing (New in v131)
        if 'current_price' not in columns:
            try:
                c.execute("ALTER TABLE trades ADD COLUMN current_price REAL")
                print("Migrated: Added current_price to trades")
            except Exception as e:
                print(f"Migration Note: {e}")

        # Add 'strategy' if missing
        if 'strategy' not in columns:
            try:
                c.execute("ALTER TABLE trades ADD COLUMN strategy TEXT")
                print("Migrated: Added strategy to trades")
            except:
                pass
                
    except Exception as e:
        print(f"Migration Check Failed: {e}")

    # 2. Check SNAPSHOTS table columns
    try:
        c.execute("PRAGMA table_info(snapshots)")
        snap_cols = [info[1] for info in c.fetchall()]
        
        # List of required columns for v131
        req_cols = ['delta', 'theta', 'vega', 'gamma', 'iv', 'pnl']
        for col in req_cols:
            if col not in snap_cols:
                try:
                    c.execute(f"ALTER TABLE snapshots ADD COLUMN {col} REAL")
                    print(f"Migrated: Added {col} to snapshots")
                except:
                    pass
    except:
        pass
    
    conn.commit()

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Create tables if they don't exist at all
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    strategy TEXT,
                    status TEXT,
                    entry_date DATE,
                    expiration_date DATE,
                    lot_size INTEGER,
                    debit REAL,
                    pnl REAL,
                    current_price REAL,
                    notes TEXT
                )''')

    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT,
                    date DATE,
                    delta REAL,
                    theta REAL,
                    vega REAL,
                    gamma REAL,
                    pnl REAL,
                    iv REAL,
                    FOREIGN KEY(trade_id) REFERENCES trades(id)
                )''')
    
    # Run Migration to fix missing columns in existing DBs
    run_migration(conn)
    
    conn.commit()

# --- UTILITY: ROBUST MATH ---
def safe_div(n, d, default=0.0):
    """Prevents division by zero errors in ratios."""
    if d == 0 or pd.isna(d) or pd.isna(n):
        return default
    return n / d

def calculate_stability_ratio(theta, delta):
    """
    Calculates Stability Ratio: Theta / Delta.
    Handles Delta Neutral singularities (div by zero).
    """
    denominator = abs(delta) + 0.001 
    ratio = theta / denominator
    return min(ratio, 50.0)

# --- DATA PARSING ENGINE ---
def parse_optionstrat_csv(file_obj):
    try:
        df_raw = pd.read_csv(file_obj)
        trades = []
        
        for _, row in df_raw.iterrows():
            name = str(row.get('Name', ''))
            
            # Identify Parent Trade
            if name and not name.startswith('.') and name != 'nan':
                strategy = "Unknown"
                if "M200" in name: strategy = "M200"
                elif "130" in name or "160" in name: strategy = "130/160"
                elif "SMSF" in name: strategy = "SMSF"

                try:
                    entry_date = pd.to_datetime(row.get('Created At', datetime.now())).date()
                    exp_date = pd.to_datetime(row.get('Expiration', datetime.now())).date()
                except:
                    entry_date = datetime.now().date()
                    exp_date = datetime.now().date()

                # Map CSV columns to DB columns
                current_trade = {
                    'id': name,
                    'name': name,
                    'strategy': strategy,
                    'entry_date': entry_date,
                    'expiration_date': exp_date,
                    'debit': float(row.get('Net Debit/Credit', 0)),
                    'pnl': float(row.get('Total Return $', 0)),
                    'current_price': float(row.get('Current Price', 0)),
                    'delta': float(row.get('Delta', 0)),
                    'theta': float(row.get('Theta', 0)),
                    'vega': float(row.get('Vega', 0)),
                    'gamma': float(row.get('Gamma', 0)),
                    'iv': float(row.get('IV', 0)),
                    'status': 'Open'
                }
                trades.append(current_trade)
                
        return pd.DataFrame(trades)

    except Exception as e:
        st.error(f"Error parsing CSV: {e}")
        return pd.DataFrame()

# --- DATABASE SYNC ---
def sync_data_to_db(df_trades):
    conn = get_db_connection()
    c = conn.cursor()
    snapshot_date = datetime.now().date()
    
    count_new = 0
    count_updated = 0
    
    for _, row in df_trades.iterrows():
        # 1. Update/Insert Trade
        c.execute("SELECT id FROM trades WHERE id = ?", (row['id'],))
        data = c.fetchone()
        
        if data is None:
            c.execute('''INSERT INTO trades (id, name, strategy, status, entry_date, expiration_date, 
                         debit, pnl, current_price) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (row['id'], row['name'], row['strategy'], row['status'], row['entry_date'], 
                       row['expiration_date'], row['debit'], row['pnl'], row['current_price']))
            count_new += 1
        else:
            c.execute('''UPDATE trades SET pnl = ?, current_price = ? WHERE id = ?''',
                      (row['pnl'], row['current_price'], row['id']))
            count_updated += 1
            
        # 2. Insert Snapshot
        c.execute("SELECT snapshot_id FROM snapshots WHERE trade_id = ? AND date = ?", (row['id'], snapshot_date))
        snap = c.fetchone()
        
        if snap is None:
            c.execute('''INSERT INTO snapshots (trade_id, date, delta, theta, vega, gamma, pnl, iv)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                      (row['id'], snapshot_date, row['delta'], row['theta'], 
                       row['vega'], row['gamma'], row['pnl'], row['iv']))
    
    conn.commit()
    conn.close()
    return count_new, count_updated

# --- CACHED DATA LOADER ---
@st.cache_data(ttl=300)
def load_dashboard_data():
    conn = get_db_connection()
    
    # Check if 'entry_debit' or 'debit' column exists to build correct query
    try:
        c = conn.cursor()
        c.execute("PRAGMA table_info(trades)")
        cols = [r[1] for r in c.fetchall()]
        debit_col = 'debit' if 'debit' in cols else 'entry_debit'
        pnl_col = 'pnl' if 'pnl' in cols else 'current_pnl'
    except:
        debit_col = 'debit'
        pnl_col = 'pnl'

    # Safe Query with dynamic column names
    query = f'''
    SELECT 
        t.id, t.name, t.strategy, t.entry_date, t.expiration_date, 
        t.{debit_col} as entry_debit, 
        s.pnl, s.delta, s.theta, s.vega, s.gamma, s.iv, s.date as snapshot_date
    FROM trades t
    JOIN snapshots s ON t.id = s.trade_id
    WHERE s.date = (SELECT MAX(date) FROM snapshots WHERE trade_id = t.id)
    AND t.status = 'Open'
    '''
    
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        # Fallback if DB is empty or very broken
        st.warning(f"Could not load data. DB Error: {e}")
        conn.close()
        return pd.DataFrame()

    conn.close()
    
    if not df.empty:
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['expiration_date'] = pd.to_datetime(df['expiration_date'])
        df['days_held'] = (datetime.now() - df['entry_date']).dt.days
        df['dte'] = (df['expiration_date'] - datetime.now()).dt.days
        
        # Metrics
        df['stability_ratio'] = df.apply(lambda x: calculate_stability_ratio(x['theta'], x['delta']), axis=1)
        df['theta_efficiency'] = df.apply(lambda x: safe_div(x['theta'], abs(x['entry_debit'])) * 100, axis=1)
    
    return df

# --- DECISION LADDER ---
def calculate_decision_ladder(row):
    score = 0
    reasons = []
    
    strategy = row['strategy']
    days_held = row['days_held']
    pnl = row['pnl']
    delta = row['delta']
    stability = row['stability_ratio']
    
    # 1. THE VALLEY
    in_valley = False
    if strategy == "M200" and 15 <= days_held <= 40:
        in_valley = True
        reasons.append("🛡️ In the Valley")
    
    # 2. PROFIT
    if pnl > 1000: 
        score += 60
        reasons.append("💰 Profit Target")
    
    # 3. DEFENSE
    if not in_valley:
        if abs(delta) > 5.0:
            score += 50
            reasons.append("⚠️ High Delta")
        if stability < 0.25:
            score += 30
            reasons.append("⚠️ Low Stability")
    else:
        if abs(delta) > 10.0:
            score += 80
            reasons.append("🚨 Valley Breakout")

    # 4. TIME
    if row['dte'] < 20:
        score += 40
        reasons.append("⏳ Low DTE")

    score = min(score, 100)
    
    status = "🟢 HOLD"
    if score >= 80: status = "🔴 URGENT"
    elif score >= 50: status = "Ql WARNING"
    elif score >= 20: status = "🟡 MONITOR"
    
    return status, score, ", ".join(reasons)

# --- MAIN UI ---
init_db() # Run migration checks on startup

st.sidebar.title("🛡️ Trade Guardian")
st.sidebar.caption(f"{VERSION}")

with st.sidebar.expander("📂 Upload OptionStrat Data", expanded=True):
    uploaded_file = st.file_uploader("Upload Active/Expired CSV", type=['csv', 'xlsx'])
    if uploaded_file:
        if st.button("Sync Data"):
            with st.spinner("Parsing & Syncing..."):
                if uploaded_file.name.endswith('.csv'):
                    df_parsed = parse_optionstrat_csv(uploaded_file)
                else:
                    df_parsed = pd.read_excel(uploaded_file)
                
                if not df_parsed.empty:
                    new_cnt, upd_cnt = sync_data_to_db(df_parsed)
                    st.success(f"Synced! New: {new_cnt}, Updated: {upd_cnt}")
                    st.cache_data.clear()
                else:
                    st.error("Could not parse file.")

# MAIN DASHBOARD
st.title("🛡️ Allantis Trade Guardian")

# Load Data
df = load_dashboard_data()

if df.empty:
    st.info("No active trades found. Sync data to begin.")
else:
    # 1. METRICS
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Net Liquidity P&L", f"${df['pnl'].sum():,.2f}")
    col2.metric("Portfolio Delta", f"{df['delta'].sum():.2f}")
    col3.metric("Daily Theta", f"${df['theta'].sum():.2f}")
    col4.metric("Avg Stability", f"{df['stability_ratio'].mean():.2f}")
    st.markdown("---")

    # 2. DECISIONS
    st.subheader("📋 Decision Ladder")
    decision_data = df.apply(calculate_decision_ladder, axis=1, result_type='expand')
    df[['Action', 'Urgency_Score', 'Reason']] = decision_data
    df_sorted = df.sort_values(by='Urgency_Score', ascending=False)
    
    # Urgent Cards
    urgent_trades = df_sorted[df_sorted['Urgency_Score'] >= 50]
    if not urgent_trades.empty:
        st.error(f"🚨 ACTION REQUIRED: {len(urgent_trades)} Trades flagged")
        for i, row in urgent_trades.iterrows():
            with st.container():
                c1, c2, c3 = st.columns([1, 4, 2])
                c1.markdown(f"### {row['Action']}")
                c2.markdown(f"**{row['name']}**")
                c2.progress(row['Urgency_Score'] / 100, text=f"{row['Urgency_Score']}/100")
                c3.write(row['Reason'])
                st.divider()

    # Table
    st.subheader("📊 Active Positions")
    st.dataframe(
        df_sorted[['name', 'strategy', 'days_held', 'pnl', 'delta', 'theta', 'stability_ratio', 'Action', 'Reason']],
        column_config={
            "pnl": st.column_config.NumberColumn("P&L", format="$%.2f"),
            "Urgency_Score": st.column_config.ProgressColumn("Urgency", min_value=0, max_value=100)
        },
        use_container_width=True,
        hide_index=True
    )

    # 3. CHARTS
    st.subheader("📈 Trade Health")
    tab1, tab2 = st.tabs(["Stability Matrix", "Trade History"])
    
    with tab1:
        fig = px.scatter(df, x="stability_ratio", y="pnl", color="strategy", size="theta", 
                         title="Stability (X) vs PnL (Y)", hover_data=['name'])
        fig.add_vline(x=1.0, line_dash="dash", line_color="green")
        fig.add_vline(x=0.25, line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        sel_trade = st.selectbox("Select Trade", df['id'].unique())
        if sel_trade:
            conn = get_db_connection()
            h_df = pd.read_sql("SELECT * FROM snapshots WHERE trade_id = ? ORDER BY date", conn, params=(sel_trade,))
            conn.close()
            if not h_df.empty:
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=h_df['date'], y=h_df['pnl'], name="P&L", line=dict(color='green')))
                fig2.add_trace(go.Scatter(x=h_df['date'], y=h_df['delta'], name="Delta", line=dict(color='blue'), yaxis='y2'))
                fig2.update_layout(title=f"History: {sel_trade}", yaxis2=dict(overlaying='y', side='right'))
                st.plotly_chart(fig2, use_container_width=True)

# DEBUG
with st.expander("🛠️ Database Tools"):
    if st.button("Reset Database (Clear All Data)"):
        try:
            os.remove(DB_NAME)
            st.success("Database deleted. Please refresh page.")
        except:
            st.error("Error deleting DB.")
