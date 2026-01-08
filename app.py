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
from openpyxl import load_workbook
from scipy import stats 
from scipy.spatial.distance import cdist 

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Allantis Trade Guardian", 
    layout="wide", 
    page_icon="🛡️",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS FOR MODERN UI ---
st.markdown("""
<style>
    /* Global Font & Spacing */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Card-like styling for metrics */
    div[data-testid="stMetric"] {
        background-color: #262730;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #363945;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #0E1117;
        border-radius: 5px 5px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #262730;
        border-bottom: 2px solid #FF4B4B;
    }

    /* DataFrame Headers */
    [data-testid="stDataFrame"] th {
        background-color: #262730 !important;
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v4.db"

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    # 1. CREATE TABLES (Preserving original schema)
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
                    notes TEXT,
                    group_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
    
    # 2. CHECK FOR MISSING COLUMNS & MIGRATE
    c.execute("PRAGMA table_info(trades)")
    columns = [info[1] for info in c.fetchall()]
    
    if 'lot_size' not in columns:
        c.execute('ALTER TABLE trades ADD COLUMN lot_size INTEGER DEFAULT 1')
    if 'pnl' not in columns:
        c.execute('ALTER TABLE trades ADD COLUMN pnl REAL DEFAULT 0.0')
    if 'theta' not in columns:
        c.execute('ALTER TABLE trades ADD COLUMN theta REAL DEFAULT 0.0')
    if 'delta' not in columns:
        c.execute('ALTER TABLE trades ADD COLUMN delta REAL DEFAULT 0.0')
    if 'gamma' not in columns:
        c.execute('ALTER TABLE trades ADD COLUMN gamma REAL DEFAULT 0.0')
    if 'vega' not in columns:
        c.execute('ALTER TABLE trades ADD COLUMN vega REAL DEFAULT 0.0')
    if 'notes' not in columns:
        c.execute('ALTER TABLE trades ADD COLUMN notes TEXT')
    if 'group_id' not in columns:
        c.execute('ALTER TABLE trades ADD COLUMN group_id TEXT')

    conn.commit()
    conn.close()

# --- LOGIC & CALCULATIONS (PRESERVED) ---

def parse_optionstrat_excel(uploaded_file):
    try:
        # Load Workbook to get 'Trades' sheet
        wb = load_workbook(uploaded_file, data_only=True)
        if 'Trades' not in wb.sheetnames:
            return None, "❌ Error: 'Trades' sheet not found in Excel file."
            
        # Read Data
        df = pd.read_excel(uploaded_file, sheet_name='Trades')
        
        # BASIC CLEANING
        df = df.dropna(subset=['Symbol'])  # Drop empty rows
        
        # Standardize Columns
        df.columns = df.columns.str.strip()
        
        # Helper: Safe Float Conversion
        def safe_float(x):
            try:
                return float(str(x).replace('$', '').replace(',', '').strip())
            except:
                return 0.0

        # Helper: Safe Date Parsing
        def parse_date(x):
            if pd.isnull(x) or str(x).strip() == '':
                return None
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y'):
                try:
                    return pd.to_datetime(x).strftime('%Y-%m-%d')
                except:
                    continue
            return datetime.today().strftime('%Y-%m-%d')

        processed_trades = []
        
        # Iterate and Extract
        for index, row in df.iterrows():
            # Identify Main Strategy Row (e.g., "JUNE.1.M200") vs Legs
            # Main rows usually have 'Name' or 'Strategy' filled, or we infer from structure
            # Based on your CSV, the main row has the Name in 'Name' column usually
            
            # Logic: If row has a complex name and aggregate stats, it's a parent
            # If it starts with '.', it's a leg. We only want Parent Trades for the DB.
            
            name = str(row.get('Name', '')).strip()
            
            # SKIP LEGS (rows starting with .SPX...)
            if name.startswith('.'):
                continue
                
            # SKIP EMPTY NAMES
            if not name or name.lower() == 'nan':
                continue

            # STRATEGY DETECTION
            strategy = "Other"
            if "M200" in name.upper(): strategy = "M200"
            elif "ALLANTIS" in name.upper(): strategy = "Allantis"
            elif "SMSF" in name.upper(): strategy = "SMSF"
            elif "112" in name.upper(): strategy = "112"
            
            # DATA MAPPING
            entry_date = parse_date(row.get('Created At'))
            expiration = parse_date(row.get('Expiration'))
            
            # Net Debit/Credit is often the 'Entry Price' or 'Net Debit/Credit' column
            # In your CSV snippet, 'Entry Price' exists.
            entry_price = safe_float(row.get('Entry Price', 0))
            current_price = safe_float(row.get('Current Price', 0))
            
            # PnL Calculation (Total Return $)
            pnl = safe_float(row.get('Total Return $', 0))
            
            # Greeks (If available in CSV, map them)
            delta = safe_float(row.get('Delta', 0))
            theta = safe_float(row.get('Theta', 0))
            gamma = safe_float(row.get('Gamma', 0))
            vega = safe_float(row.get('Vega', 0))
            
            # Status Logic
            status = "OPEN"
            if abs(current_price) < 0.01: # Heuristic for closed/expired
                 status = "CLOSED"
            
            # Construct Trade Object
            trade = {
                'id': f"{name}_{strategy}_{entry_date}", # Unique ID generation
                'name': name,
                'strategy': strategy,
                'status': status,
                'entry_date': entry_date,
                'exit_date': expiration, # Defaulting to expiration, can be updated manually
                'days_held': 0, # Recalculated later
                'debit': entry_price,
                'lot_size': 1, # Default
                'pnl': pnl,
                'theta': theta,
                'delta': delta,
                'gamma': gamma,
                'vega': vega,
                'group_id': "Imported"
            }
            processed_trades.append(trade)
            
        return pd.DataFrame(processed_trades), None

    except Exception as e:
        return None, str(e)

def add_trade_to_db(trade_data):
    conn = get_db_connection()
    c = conn.cursor()
    try:
        # Check if exists
        c.execute("SELECT id FROM trades WHERE id = ?", (trade_data['id'],))
        if c.fetchone():
            # Update existing
            c.execute('''UPDATE trades SET 
                        pnl=?, theta=?, delta=?, gamma=?, vega=?, status=?
                        WHERE id=?''',
                      (trade_data['pnl'], trade_data['theta'], trade_data['delta'], 
                       trade_data['gamma'], trade_data['vega'], trade_data['status'],
                       trade_data['id']))
            action = "Updated"
        else:
            # Insert new
            c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, debit, lot_size, pnl, theta, delta, gamma, vega, group_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (trade_data['id'], trade_data['name'], trade_data['strategy'], trade_data['status'],
                       trade_data['entry_date'], trade_data['exit_date'], trade_data['debit'], 
                       trade_data['lot_size'], trade_data['pnl'], trade_data['theta'],
                       trade_data['delta'], trade_data['gamma'], trade_data['vega'], trade_data['group_id']))
            action = "Added"
        conn.commit()
        return True, action
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def delete_trade(trade_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
    conn.commit()
    conn.close()

def update_trade_field(trade_id, field, value):
    conn = get_db_connection()
    c = conn.cursor()
    query = f"UPDATE trades SET {field} = ? WHERE id = ?"
    c.execute(query, (value, trade_id))
    conn.commit()
    conn.close()

# --- INITIALIZATION ---
init_db()

# --- MAIN APP LAYOUT ---

# 1. SIDEBAR (Global Filters & Context)
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/shield.png", width=60) # Placeholder Icon
    st.title("Guardian")
    st.markdown("---")
    
    # Global Filters
    st.subheader("🔍 Filters")
    
    # Load Data for Filters
    conn = get_db_connection()
    df_all = pd.read_sql("SELECT * FROM trades", conn)
    conn.close()
    
    filter_strategy = st.multiselect(
        "Filter by Strategy",
        options=df_all['strategy'].unique() if not df_all.empty else [],
        default=df_all['strategy'].unique() if not df_all.empty else []
    )
    
    filter_status = st.multiselect(
        "Filter by Status",
        options=["OPEN", "CLOSED"],
        default=["OPEN", "CLOSED"]
    )
    
    # Apply Filters
    if not df_all.empty:
        df_filtered = df_all[
            (df_all['strategy'].isin(filter_strategy)) & 
            (df_all['status'].isin(filter_status))
        ]
    else:
        df_filtered = pd.DataFrame()

    st.markdown("---")
    st.info(f"Loaded **{len(df_filtered)}** trades matching criteria.")

# 2. TABS ARCHITECTURE
tab_dashboard, tab_trades, tab_import, tab_strategy = st.tabs([
    "📊 Command Center", 
    "🗂️ Trade Manager", 
    "📥 Operations", 
    "🧠 Strategy Hub"
])

# ==============================================================================
# TAB 1: DASHBOARD (VISUALS)
# ==============================================================================
with tab_dashboard:
    if df_filtered.empty:
        st.warning("No trades found. Please go to the 'Operations' tab to add or import trades.")
    else:
        # KPIS
        total_pnl = df_filtered['pnl'].sum()
        win_rate = (len(df_filtered[df_filtered['pnl'] > 0]) / len(df_filtered) * 100) if len(df_filtered) > 0 else 0
        active_cnt = len(df_filtered[df_filtered['status'] == 'OPEN'])
        total_theta = df_filtered[df_filtered['status'] == 'OPEN']['theta'].sum()
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total P&L", f"${total_pnl:,.2f}", delta_color="normal")
        c2.metric("Win Rate", f"{win_rate:.1f}%")
        c3.metric("Active Trades", active_cnt)
        c4.metric("Portfolio Theta", f"{total_theta:.2f}")

        st.markdown("---")
        
        # ROW 2: CHARTS
        col_left, col_right = st.columns([2, 1])
        
        with col_left:
            st.subheader("📈 Performance Curve")
            # Calculate Cumulative PnL over time based on Entry Date
            df_chart = df_filtered.sort_values('entry_date')
            df_chart['cumulative_pnl'] = df_chart['pnl'].cumsum()
            
            fig_equity = px.line(
                df_chart, 
                x='entry_date', 
                y='cumulative_pnl',
                markers=True,
                title="Equity Curve",
                labels={'entry_date': 'Date', 'cumulative_pnl': 'Cumulative P&L ($)'}
            )
            fig_equity.update_layout(template="plotly_dark", height=350)
            fig_equity.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.3)
            st.plotly_chart(fig_equity, use_container_width=True)
            
        with col_right:
            st.subheader("🎯 Allocation")
            strat_counts = df_filtered['strategy'].value_counts().reset_index()
            strat_counts.columns = ['Strategy', 'Count']
            
            fig_donut = px.pie(
                strat_counts, 
                values='Count', 
                names='Strategy', 
                hole=0.4,
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig_donut.update_layout(template="plotly_dark", height=350, showlegend=False)
            fig_donut.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_donut, use_container_width=True)

        # ROW 3: DETAILED ANALYSIS
        st.subheader("📊 Strategy Performance Matrix")
        
        # Group by Strategy
        strat_perf = df_filtered.groupby('strategy').agg({
            'pnl': 'sum',
            'id': 'count',
            'debit': 'mean' # Average debit
        }).reset_index()
        strat_perf['Avg PnL'] = strat_perf['pnl'] / strat_perf['id']
        
        fig_bar = px.bar(
            strat_perf, 
            x='strategy', 
            y='pnl', 
            color='pnl',
            color_continuous_scale='RdYlGn',
            text_auto='.2s',
            title="Net P&L by Strategy"
        )
        fig_bar.update_layout(template="plotly_dark", height=300)
        st.plotly_chart(fig_bar, use_container_width=True)

# ==============================================================================
# TAB 2: TRADE MANAGER (DATA)
# ==============================================================================
with tab_trades:
    subtab_active, subtab_history = st.tabs(["⚡ Active Trades", "📜 History Archive"])
    
    # --- ACTIVE TRADES ---
    with subtab_active:
        df_active = df_filtered[df_filtered['status'] == 'OPEN'].copy()
        
        if df_active.empty:
            st.info("No active trades currently open.")
        else:
            # Display Options for DataFrame
            st.dataframe(
                df_active[[
                    'name', 'strategy', 'entry_date', 'pnl', 'theta', 'delta', 'vega', 'notes'
                ]],
                column_config={
                    "name": "Trade Name",
                    "pnl": st.column_config.ProgressColumn(
                        "Current P&L",
                        format="$%.2f",
                        min_value=-5000,
                        max_value=5000,
                    ),
                    "entry_date": st.column_config.DateColumn("Entry"),
                    "theta": st.column_config.NumberColumn("Theta", format="%.2f"),
                    "delta": st.column_config.NumberColumn("Delta", format="%.2f"),
                },
                use_container_width=True,
                hide_index=True
            )
            
            # Quick Actions Section
            st.markdown("### 🛠️ Manage Selected Trade")
            selected_trade_id = st.selectbox("Select Trade to Edit/Close", df_active['id'].unique(), format_func=lambda x: f"{x} ({df_active[df_active['id']==x]['name'].values[0]})")
            
            if selected_trade_id:
                trade_row = df_active[df_active['id'] == selected_trade_id].iloc[0]
                
                with st.expander("Show Trade Details & Actions", expanded=True):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        new_status = st.selectbox("Status", ["OPEN", "CLOSED"], index=0)
                        new_pnl = st.number_input("Update P&L ($)", value=float(trade_row['pnl']))
                    with col2:
                        new_notes = st.text_area("Notes", value=str(trade_row['notes']) if trade_row['notes'] else "")
                    with col3:
                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button("💾 Save Changes", type="primary"):
                            update_trade_field(selected_trade_id, "status", new_status)
                            update_trade_field(selected_trade_id, "pnl", new_pnl)
                            update_trade_field(selected_trade_id, "notes", new_notes)
                            st.success("Trade updated!")
                            st.rerun()
                            
                        if st.button("🗑️ Delete Trade", type="secondary"):
                            delete_trade(selected_trade_id)
                            st.warning("Trade deleted.")
                            st.rerun()

    # --- HISTORY ---
    with subtab_history:
        df_closed = df_filtered[df_filtered['status'] == 'CLOSED'].copy()
        if df_closed.empty:
            st.info("No closed trades in history.")
        else:
            st.dataframe(
                df_closed,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "pnl": st.column_config.NumberColumn("Realized P&L", format="$%.2f"),
                    "entry_date": "Entry",
                    "exit_date": "Exit"
                }
            )

# ==============================================================================
# TAB 3: OPERATIONS (IMPORT & MANUAL ADD)
# ==============================================================================
with tab_import:
    col_imp1, col_imp2 = st.columns([1, 1], gap="large")
    
    # --- SECTION: FILE UPLOAD ---
    with col_imp1:
        st.header("📂 Import Data")
        st.markdown("Upload **OptionStrat** Excel exports here.")
        
        uploaded_file = st.file_uploader("Drop Excel File (m200-....xlsx)", type=['xlsx'])
        
        if uploaded_file:
            if st.button("Process File", type="primary"):
                with st.spinner("Processing trades..."):
                    df_result, error = parse_optionstrat_excel(uploaded_file)
                    
                    if error:
                        st.error(error)
                    elif df_result is not None and not df_result.empty:
                        success_count = 0
                        updated_count = 0
                        
                        progress_bar = st.progress(0)
                        
                        for i, row in df_result.iterrows():
                            # Add to DB
                            ok, action = add_trade_to_db(row)
                            if ok:
                                if action == "Added": success_count += 1
                                else: updated_count += 1
                            progress_bar.progress((i + 1) / len(df_result))
                        
                        st.success(f"✅ Processing Complete! Added: {success_count}, Updated: {updated_count}")
                        st.balloons()
                    else:
                        st.warning("No valid trades found in file.")

    # --- SECTION: MANUAL ENTRY ---
    with col_imp2:
        st.header("✍️ Manual Entry")
        with st.form("manual_trade_form"):
            m_name = st.text_input("Trade Name (e.g., M200 SPX Put)")
            c1, c2 = st.columns(2)
            m_strat = c1.selectbox("Strategy", ["M200", "Allantis", "SMSF", "112", "Other"])
            m_status = c2.selectbox("Status", ["OPEN", "CLOSED"])
            
            c3, c4 = st.columns(2)
            m_entry = c3.date_input("Entry Date")
            m_exit = c4.date_input("Target Exit / Expiration")
            
            m_pnl = st.number_input("Current P&L", value=0.0)
            m_debit = st.number_input("Debit/Credit", value=0.0)
            
            submitted = st.form_submit_button("Add Trade")
            
            if submitted:
                # Generate simple ID
                t_id = f"{m_name}_{m_strat}_{m_entry}"
                
                trade_data = {
                    'id': t_id,
                    'name': m_name,
                    'strategy': m_strat,
                    'status': m_status,
                    'entry_date': m_entry,
                    'exit_date': m_exit,
                    'debit': m_debit,
                    'lot_size': 1,
                    'pnl': m_pnl,
                    'theta': 0, 'delta': 0, 'gamma': 0, 'vega': 0,
                    'group_id': 'Manual',
                    'notes': ''
                }
                ok, msg = add_trade_to_db(trade_data)
                if ok:
                    st.success(f"Trade {msg} successfully!")
                else:
                    st.error(f"Error: {msg}")

# ==============================================================================
# TAB 4: STRATEGY HUB
# ==============================================================================
with tab_strategy:
    st.markdown("## 📜 Strategy Playbooks")
    
    with st.expander("📘 Allantis Strategy", expanded=False):
        st.markdown("""
        ### Core Principles
        * **Target:** Positive Theta, Manageable Delta.
        * **Entry:** usually 45-60 DTE.
        * **Management:**
            * Adjust if Delta doubles.
            * Roll untested side if necessary.
        """)
        
    with st.expander("📗 M200 Strategy (The Whale)", expanded=False):
        st.markdown("""
        ### M200 Strategy (Emotional Mastery)
        * **Role:** Whale. Variance-tolerant capital deployment.
        * **Entry:** Wednesday.
        * **Debit Target:** `$7,500 - $8,500` per lot.
        * **The "Dip Valley":**
            * P&L often looks worst between Day 15–40. This is structural.
            * **Management:** Check at **Day 14**.
                * Check **Greeks & VIX**, not just P&L.
                * If Red/Flat: **HOLD.** Do not panic exit in the Valley. Wait for volatility to revert.
        """)

    with st.expander("🛡️ Universal Execution Gates", expanded=True):
        st.markdown("""
        1.  **Stability Check:** Monitor **Stability** Ratio.
            * **> 1.0 (Green):** Fortress. Trade is safe.
            * **< 0.25 (Red):** Coin Flip. Trade is directional gambling.
        2.  **Volatility Gate:** Check VIX before entry. Ideal: 14–22. Skip if VIX exploded >10% in last 48h.
        """)

# --- FOOTER ---
st.markdown("---")
st.caption(f"Trade Guardian Pro v2.0 | DB: `{DB_NAME}` | Connected: ✅")
