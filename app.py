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
VERSION = "v1.3.1 (Refined Logic & Optimized Parsing)"
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

# --- DATABASE ENGINE ---
def get_db_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Core Trades Table
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    strategy TEXT,
                    status TEXT,
                    entry_date DATE,
                    expiration_date DATE,
                    lot_size INTEGER,
                    entry_debit REAL,
                    current_pnl REAL,
                    current_price REAL,
                    notes TEXT
                )''')

    # Daily Snapshots (The Greeks History)
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
    # Add epsilon to delta to prevent explosion, use abs(delta) to keep magnitude
    denominator = abs(delta) + 0.001 
    ratio = theta / denominator
    
    # Cap the ratio for visualization sanity (e.g., >50 is effectively 'Infinite Stability')
    return min(ratio, 50.0)

# --- DATA PARSING ENGINE (OPTIMIZED) ---
def parse_optionstrat_csv(file_obj):
    """
    Parses OptionStrat CSVs handling Parent (Trade) and Child (Leg) rows.
    """
    try:
        # Load full CSV
        df_raw = pd.read_csv(file_obj)
        
        trades = []
        current_trade = {}
        
        # Iterate through rows
        for _, row in df_raw.iterrows():
            name = str(row.get('Name', ''))
            
            # 1. IDENTIFY PARENT TRADE ROW
            # Logic: Has a valid Name that DOES NOT start with '.' or is empty
            if name and not name.startswith('.') and name != 'nan':
                
                # Extract Strategy from Name (heuristic)
                strategy = "Unknown"
                if "M200" in name: strategy = "M200"
                elif "130" in name or "160" in name: strategy = "130/160"
                elif "SMSF" in name: strategy = "SMSF"

                # Parse Dates
                try:
                    entry_date = pd.to_datetime(row.get('Created At', datetime.now())).date()
                    exp_date = pd.to_datetime(row.get('Expiration', datetime.now())).date()
                except:
                    entry_date = datetime.now().date()
                    exp_date = datetime.now().date()

                current_trade = {
                    'id': name, # Using Name as ID for simplicity, ideally use a UUID
                    'name': name,
                    'strategy': strategy,
                    'entry_date': entry_date,
                    'expiration_date': exp_date,
                    'entry_debit': float(row.get('Net Debit/Credit', 0)),
                    'current_pnl': float(row.get('Total Return $', 0)),
                    'current_price': float(row.get('Current Price', 0)),
                    'delta': float(row.get('Delta', 0)),
                    'theta': float(row.get('Theta', 0)),
                    'vega': float(row.get('Vega', 0)),
                    'gamma': float(row.get('Gamma', 0)),
                    'iv': float(row.get('IV', 0)),
                    'status': 'Open'
                }
                trades.append(current_trade)
            
            # 2. IDENTIFY LEG ROW (Starts with '.')
            elif name.startswith('.'):
                # This is a leg for the 'current_trade'. 
                # Currently we only track the parent aggregate Greeks, 
                # but this is where you'd capture leg-specific data if needed.
                pass
                
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
        # 1. Update/Insert Trade Info
        # Check if exists
        c.execute("SELECT id FROM trades WHERE id = ?", (row['id'],))
        data = c.fetchone()
        
        if data is None:
            c.execute('''INSERT INTO trades (id, name, strategy, status, entry_date, expiration_date, 
                         entry_debit, current_pnl, current_price) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (row['id'], row['name'], row['strategy'], row['status'], row['entry_date'], 
                       row['expiration_date'], row['entry_debit'], row['current_pnl'], row['current_price']))
            count_new += 1
        else:
            # Update dynamic fields
            c.execute('''UPDATE trades SET current_pnl = ?, current_price = ? WHERE id = ?''',
                      (row['current_pnl'], row['current_price'], row['id']))
            count_updated += 1
            
        # 2. Insert Daily Snapshot (History)
        # Check if snapshot already exists for today to prevent duplicates on page refresh
        c.execute("SELECT snapshot_id FROM snapshots WHERE trade_id = ? AND date = ?", (row['id'], snapshot_date))
        snap = c.fetchone()
        
        if snap is None:
            c.execute('''INSERT INTO snapshots (trade_id, date, delta, theta, vega, gamma, pnl, iv)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                      (row['id'], snapshot_date, row['delta'], row['theta'], 
                       row['vega'], row['gamma'], row['current_pnl'], row['iv']))
    
    conn.commit()
    conn.close()
    return count_new, count_updated

# --- CACHED DATA LOADER ---
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_dashboard_data():
    conn = get_db_connection()
    
    # Optimized Query: Join Trades with the LATEST snapshot
    query = '''
    SELECT 
        t.id, t.name, t.strategy, t.entry_date, t.expiration_date, t.entry_debit,
        s.pnl, s.delta, s.theta, s.vega, s.gamma, s.iv, s.date as snapshot_date
    FROM trades t
    JOIN snapshots s ON t.id = s.trade_id
    WHERE s.date = (SELECT MAX(date) FROM snapshots WHERE trade_id = t.id)
    AND t.status = 'Open'
    '''
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if not df.empty:
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['expiration_date'] = pd.to_datetime(df['expiration_date'])
        df['days_held'] = (datetime.now() - df['entry_date']).dt.days
        df['dte'] = (df['expiration_date'] - datetime.now()).dt.days
        
        # Derived Metrics
        df['stability_ratio'] = df.apply(lambda x: calculate_stability_ratio(x['theta'], x['delta']), axis=1)
        df['theta_efficiency'] = df.apply(lambda x: safe_div(x['theta'], abs(x['entry_debit'])) * 100, axis=1) # % return per day via theta
    
    return df

# --- DECISION LADDER ALGORITHM ---
def calculate_decision_ladder(row):
    """
    Returns a score (0-100) and a recommendation based on Strategy rules.
    """
    score = 0
    reasons = []
    
    strategy = row['strategy']
    days_held = row['days_held']
    pnl = row['pnl']
    delta = row['delta']
    stability = row['stability_ratio']
    
    # config = STRATEGY_CONFIG.get(strategy, {}) # Use default if not found
    
    # 1. THE VALLEY PROTECTION (M200 Specific)
    in_valley = False
    if strategy == "M200" and 15 <= days_held <= 40:
        in_valley = True
        reasons.append("🛡️ In the Valley (Variance Tolerant)")
    
    # 2. PROFIT TAKING (Upside Logic)
    # Simple example logic
    if pnl > 1000: 
        score += 60
        reasons.append("💰 Profit Target Approaching")
    
    # 3. DEFENSE (Downside Logic)
    if not in_valley:
        # If we are NOT in the valley, we are sensitive to Delta
        if abs(delta) > 5.0:
            score += 50
            reasons.append("⚠️ High Delta Exposure")
        if stability < 0.25:
            score += 30
            reasons.append("⚠️ Low Stability (Directional Risk)")
    else:
        # In Valley, we tolerate more, but check extremes
        if abs(delta) > 10.0:
            score += 80
            reasons.append("🚨 Extreme Valley Delta Breakdown")

    # 4. TIME GATES
    if row['dte'] < 20:
        score += 40
        reasons.append("⏳ Low DTE (Gamma Risk)")

    # Cap Score
    score = min(score, 100)
    
    # Determine Status
    status = "🟢 HOLD"
    if score >= 80: status = "🔴 URGENT ACTION"
    elif score >= 50: status = "Ql WARNING"
    elif score >= 20: status = "🟡 MONITOR"
    
    return status, score, ", ".join(reasons)

# --- MAIN UI ---
init_db()

# SIDEBAR
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
                    # Basic Excel handler if needed, usually OptionStrat is CSV
                    df_parsed = pd.read_excel(uploaded_file)
                
                if not df_parsed.empty:
                    new_cnt, upd_cnt = sync_data_to_db(df_parsed)
                    st.success(f"Synced! New: {new_cnt}, Updated: {upd_cnt}")
                    st.cache_data.clear() # Clear cache to show new data
                else:
                    st.error("Could not parse file.")

# MAIN DASHBOARD
st.title("🛡️ Allantis Trade Guardian")

# Load Data
df = load_dashboard_data()

if df.empty:
    st.info("No active trades found. Upload an OptionStrat export to begin.")
else:
    # 1. HIGH LEVEL METRICS
    col1, col2, col3, col4 = st.columns(4)
    total_pnl = df['pnl'].sum()
    total_delta = df['delta'].sum()
    daily_theta = df['theta'].sum()
    avg_stability = df['stability_ratio'].mean()

    col1.metric("Net Liquidity P&L", f"${total_pnl:,.2f}", delta_color="normal")
    col2.metric("Portfolio Delta", f"{total_delta:.2f}", delta_color="inverse")
    col3.metric("Daily Theta Income", f"${daily_theta:.2f}")
    col4.metric("Avg Stability Ratio", f"{avg_stability:.2f}", help="Target > 1.0")

    st.markdown("---")

    # 2. DECISION LADDER & TABLE
    st.subheader("📋 Decision Ladder")
    
    # Calculate decisions
    decision_data = df.apply(calculate_decision_ladder, axis=1, result_type='expand')
    df[['Action', 'Urgency_Score', 'Reason']] = decision_data
    
    # Sorting: Urgency descending
    df_sorted = df.sort_values(by='Urgency_Score', ascending=False)
    
    # Display Card View for High Urgency
    urgent_trades = df_sorted[df_sorted['Urgency_Score'] >= 50]
    if not urgent_trades.empty:
        st.error(f"🚨 ACTION REQUIRED: {len(urgent_trades)} Trades flagged")
        for i, row in urgent_trades.iterrows():
            with st.container():
                c1, c2, c3 = st.columns([1, 4, 2])
                c1.markdown(f"### {row['Action']}")
                c2.markdown(f"**{row['name']}** ({row['strategy']})")
                c2.progress(row['Urgency_Score'] / 100, text=f"Urgency: {row['Urgency_Score']}/100")
                c3.write(f"Reason: {row['Reason']}")
                st.divider()

    # Detailed Dataframe
    st.subheader("📊 Active Positions")
    
    # UI Color Styling for Dataframe
    def color_pnl(val):
        color = 'green' if val > 0 else 'red'
        return f'color: {color}'

    def highlight_stability(val):
        color = 'red' if val < 0.25 else 'green' if val > 1.0 else 'orange'
        return f'color: {color}'

    st.dataframe(
        df_sorted[['name', 'strategy', 'days_held', 'pnl', 'delta', 'theta', 'stability_ratio', 'Action', 'Reason']],
        column_config={
            "pnl": st.column_config.NumberColumn("P&L", format="$%.2f"),
            "stability_ratio": st.column_config.NumberColumn("Stability", format="%.2f"),
            "delta": st.column_config.NumberColumn("Delta", format="%.2f"),
            "theta": st.column_config.NumberColumn("Theta", format="%.2f"),
            "Urgency_Score": st.column_config.ProgressColumn("Urgency", min_value=0, max_value=100)
        },
        use_container_width=True,
        hide_index=True
    )

    # 3. VISUALIZATION (Plotly)
    st.subheader("📈 Trade Health Visualization")
    
    tab1, tab2 = st.tabs(["Stability vs PnL", "Greeks History"])
    
    with tab1:
        # Scatter Plot: PnL vs Stability
        # Ideally, we want high stability regardless of PnL
        fig = px.scatter(df, x="stability_ratio", y="pnl", 
                         color="strategy", size="theta", hover_data=['name', 'days_held'],
                         title="Stability (X) vs PnL (Y) - Size = Theta")
        fig.add_vline(x=1.0, line_dash="dash", line_color="green", annotation_text="Safe Zone")
        fig.add_vline(x=0.25, line_dash="dash", line_color="red", annotation_text="Danger Zone")
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        # Drill down into a specific trade to see history
        selected_trade = st.selectbox("Select Trade for History", df['id'].unique())
        
        if selected_trade:
            # Fetch history only for selected trade
            conn = get_db_connection()
            hist_df = pd.read_sql("SELECT * FROM snapshots WHERE trade_id = ? ORDER BY date", 
                                  conn, params=(selected_trade,))
            conn.close()
            
            if not hist_df.empty:
                # Dual Axis Chart: PnL vs Delta
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=hist_df['date'], y=hist_df['pnl'], name="P&L", line=dict(color='green')))
                fig2.add_trace(go.Scatter(x=hist_df['date'], y=hist_df['delta'], name="Delta", line=dict(color='blue'), yaxis='y2'))
                
                fig2.update_layout(
                    title=f"History: {selected_trade}",
                    yaxis=dict(title="P&L ($)"),
                    yaxis2=dict(title="Delta", overlaying='y', side='right')
                )
                st.plotly_chart(fig2, use_container_width=True)

# DEBUG SECTION
with st.expander("🛠️ Developer Options"):
    if st.button("Reset Database"):
        try:
            os.remove(DB_NAME)
            st.success("Database deleted. Please refresh.")
        except:
            st.error("No DB found to delete.")
