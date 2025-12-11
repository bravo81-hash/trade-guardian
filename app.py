import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import hashlib
from datetime import datetime

# ---------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")
st.title("🛡️ Allantis Trade Guardian: Database Edition")

# ---------------------------------------------------------
# 1. DATABASE ENGINE (The Backend)
# ---------------------------------------------------------
DB_NAME = "trade_guardian.db"

def get_connection():
    """Returns a SQLite connection."""
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    """Create tables if they don't exist."""
    conn = get_connection()
    c = conn.cursor()
    
    # TRADES TABLE
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
        trade_id TEXT PRIMARY KEY,
        name TEXT,
        strategy TEXT,
        status TEXT,
        pnl REAL,
        debit REAL,
        entry_date DATE,
        exit_date DATE,
        days_held INTEGER,
        daily_yield REAL,
        lot_size INTEGER,
        notes TEXT
    )''')
    
    # GREEKS TABLE
    c.execute('''CREATE TABLE IF NOT EXISTS greeks (
        trade_id TEXT PRIMARY KEY,
        theta REAL,
        delta REAL,
        gamma REAL,
        vega REAL,
        FOREIGN KEY(trade_id) REFERENCES trades(trade_id)
    )''')
    
    conn.commit()
    conn.close()

# Initialize on load
init_db()

# ---------------------------------------------------------
# 2. HELPER FUNCTIONS
# ---------------------------------------------------------
# --- STATIC BENCHMARKS (Your "Truth") ---
BASE_CONFIG = {
    '130/160': {'yield': 0.13, 'pnl': 500, 'roi': 6.8, 'dit': 36},
    '160/190': {'yield': 0.28, 'pnl': 700, 'roi': 12.7, 'dit': 44},
    'M200':    {'yield': 0.56, 'pnl': 900, 'roi': 11.1, 'dit': 41}
}

def get_strategy(group_name):
    g = str(group_name).upper()
    if "M200" in g: return "M200"
    elif "160/190" in g: return "160/190"
    elif "130/160" in g: return "130/160"
    return "Other"

def clean_num(x):
    try: return float(str(x).replace('$', '').replace(',', ''))
    except: return 0.0

def make_trade_id(name, strategy, entry_date):
    """Creates a unique ID."""
    base = f"{name}_{strategy}_{entry_date}".encode()
    return hashlib.md5(base).hexdigest()

# ---------------------------------------------------------
# 3. INGESTION ENGINE (The Processor)
# ---------------------------------------------------------
def ingest_files(files, file_type_override=None):
    """Reads files and saves to DB."""
    conn = get_connection()
    c = conn.cursor()
    count_new = 0
    count_update = 0
    
    for f in files:
        try:
            # Determine File Type based on filename if not provided
            fname = f.name.lower()
            if file_type_override:
                is_active = (file_type_override == "Active")
            else:
                is_active = "active" in fname

            # Read File
            if fname.endswith('.xlsx'): 
                df = pd.read_excel(f)
            else:
                # Handle CSV header offset
                content = f.getvalue().decode("utf-8")
                lines = content.split('\n')
                header_idx = 0
                for i, line in enumerate(lines[:20]):
                    if "Name" in line and "Total Return" in line:
                        header_idx = i
                        break
                df = pd.read_csv(io.StringIO(content), skiprows=header_idx)

            for _, row in df.iterrows():
                # Basic Validation
                name = str(row.get('Name', ''))
                if name.startswith('.') or name in ['nan', '', 'Symbol']: continue
                
                created_str = str(row.get('Created At', ''))
                try: start_dt = pd.to_datetime(created_str)
                except: continue

                # Metrics
                group = str(row.get('Group', ''))
                strategy = get_strategy(group)
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                
                # Status
                status = "Active" if is_active else "Expired"
                
                # --- FIX: Days Held Logic ---
                exit_dt = None
                if status == "Expired":
                    try:
                        # For History, Expiration = Exit Date
                        exit_dt = pd.to_datetime(row.get('Expiration'))
                        days_held = (exit_dt - start_dt).days
                    except: days_held = 1
                else:
                    # For Active, Days = Today - Entry
                    days_held = (datetime.now() - start_dt).days
                
                if days_held < 1: days_held = 1
                
                # Yield
                roi = (pnl / debit * 100) if debit > 0 else 0
                daily_yield = roi / days_held

                # Lot Size
                lot_size = 1
                if strategy == '130/160' and debit > 6000: lot_size = 2
                elif strategy == '130/160' and debit > 10000: lot_size = 3
                elif strategy == '160/190' and debit > 8000: lot_size = 2
                elif strategy == 'M200' and debit > 12000: lot_size = 2

                trade_id = make_trade_id(name, strategy, start_dt.date())

                # DB: Upsert Trade
                c.execute("""
                    INSERT INTO trades (trade_id, name, strategy, status, pnl, debit, entry_date, exit_date, days_held, daily_yield, lot_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(trade_id) DO UPDATE SET
                    pnl=excluded.pnl,
                    status=excluded.status,
                    days_held=excluded.days_held,
                    daily_yield=excluded.daily_yield,
                    exit_date=excluded.exit_date
                """, (trade_id, name, strategy, status, pnl, debit, start_dt.date(), 
                      exit_dt.date() if exit_dt else None, days_held, daily_yield, lot_size))
                
                # DB: Update Greeks (Active only)
                if is_active:
                    theta = clean_num(row.get('Theta', 0))
                    delta = clean_num(row.get('Delta', 0))
                    gamma = clean_num(row.get('Gamma', 0))
                    vega = clean_num(row.get('Vega', 0))
                    
                    c.execute("""
                        INSERT OR REPLACE INTO greeks (trade_id, theta, delta, gamma, vega)
                        VALUES (?, ?, ?, ?, ?)
                    """, (trade_id, theta, delta, gamma, vega))

            conn.commit()
            count_update += 1
            
        except Exception as e:
            st.error(f"Error processing {f.name}: {e}")
            
    conn.close()
    return count_update

# ---------------------------------------------------------
# 4. LOAD DATA FOR UI
# ---------------------------------------------------------
def load_data():
    conn = get_connection()
    query = """
    SELECT t.*, g.theta, g.delta, g.gamma, g.vega 
    FROM trades t 
    LEFT JOIN greeks g ON t.trade_id = g.trade_id
    """
    try:
        df = pd.read_sql(query, conn)
    except:
        df = pd.DataFrame()
    conn.close()
    
    if not df.empty:
        # Calculate Debit/Lot for Grading
        df['debit_per_lot'] = df['debit'] / df['lot_size'].replace(0, 1)
        
        # Grading Logic
        def get_grade(row):
            strat, debit = row['strategy'], row['debit_per_lot']
            if strat == '130/160': return "F" if debit > 4800 else "A+" if 3500 <= debit <= 4500 else "B"
            if strat == '160/190': return "A" if 4800 <= debit <= 5500 else "C"
            if strat == 'M200': return "A" if 7500 <= debit <= 8500 else "B"
            return "C"
        df['Grade'] = df.apply(get_grade, axis=1)
        
    return df

# ---------------------------------------------------------
# 5. UI & LOGIC
# ---------------------------------------------------------

# --- SIDEBAR ---
st.sidebar.header("Daily Workflow")
active_up = st.sidebar.file_uploader("1. Upload Active Trades", accept_multiple_files=True, key="act")
history_up = st.sidebar.file_uploader("2. Upload History", accept_multiple_files=True, key="hist")

if st.sidebar.button("🔄 Sync Database"):
    if active_up: ingest_files(active_up, "Active")
    if history_up: ingest_files(history_up, "History")
    st.sidebar.success("Database Updated!")
    st.rerun()

st.sidebar.divider()
market_regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish (+10%)", "Bearish (-10%)"], index=0)
regime_mult = 1.1 if "Bullish" in market_regime else 0.9 if "Bearish" in market_regime else 1.0

# --- MAIN DATA LOAD ---
df = load_data()

if df.empty:
    st.info("👋 Database is empty. Please upload Active/History files in the sidebar.")
else:
    # --- TABS ---
    # FIXED: Added "Rules" explicitly to the list so index 6 exists
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📊 Active Dashboard", 
        "🧪 Validator", 
        "📈 Analytics", 
        "📜 Timeline", 
        "💰 Allocation", 
        "📓 Journal", 
        "📖 Rules"
    ])

    # --- TAB 1: DASHBOARD ---
    with tab1:
        active_df = df[df['status'] == 'Active'].copy()
        
        if active_df.empty:
            st.info("No active trades found in database.")
        else:
            # Action Logic
            act_list = []
            for _, row in active_df.iterrows():
                strat = row['strategy']
                pnl = row['pnl']
                days = row['days_held']
                
                # Get Static Benchmark
                bench = BASE_CONFIG.get(strat, {'pnl': 9999})
                target = bench['pnl'] * regime_mult
                
                action = ""
                if pnl >= target: action = "TAKE PROFIT"
                elif strat == '130/160' and days > 25 and pnl < 100: action = "KILL (Stale)"
                elif strat == 'M200' and 12 <= days <= 16: action = "DAY 14 CHECK"
                
                act_list.append(action)
            
            active_df['Action'] = act_list
            
            # Sub Tabs
            strat_tabs = st.tabs(["Overview", "130/160", "160/190", "M200"])
            
            # Helper to render table
            def render_table(sub_df):
                cols = ['name', 'Action', 'Grade', 'daily_yield', 'pnl', 'debit', 'days_held', 'theta', 'delta']
                st.dataframe(
                    sub_df[cols].style
                    .format({'pnl': '${:,.0f}', 'debit': '${:,.0f}', 'daily_yield': '{:.2f}%', 'theta': '{:.1f}', 'delta': '{:.1f}'})
                    .map(lambda x: 'background-color: #d1e7dd; color: green' if 'TAKE' in str(x) else 'background-color: #f8d7da; color: red' if 'KILL' in str(x) else '', subset=['Action']),
                    use_container_width=True
                )

            with strat_tabs[0]:
                st.markdown("#### Portfolio Overview")
                render_table(active_df)
                
            for i, s in enumerate(['130/160', '160/190', 'M200'], 1):
                with strat_tabs[i]:
                    sub = active_df[active_df['strategy'] == s]
                    # Show Static Benchmarks
                    b = BASE_CONFIG.get(s, {})
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Target Profit", f"${b.get('pnl',0)*regime_mult:,.0f}")
                    c2.metric("Target Yield", f"{b.get('yield',0)*100:.2f}%")
                    c3.metric("Avg Days", f"{b.get('dit',0)}d")
                    
                    if not sub.empty: render_table(sub)
                    else: st.info("No trades.")

    # --- TAB 2: VALIDATOR ---
    with tab2:
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
            # Re-use ingestion logic for single file check (without saving to DB)
            try:
                fname = model_file.name.lower()
                if fname.endswith('.xlsx'): df_mod = pd.read_excel(model_file)
                else: df_mod = pd.read_csv(model_file)
                
                if not df_mod.empty:
                    row = df_mod.iloc[0]
                    name = str(row.get('Name', ''))
                    debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                    strat = get_strategy(str(row.get('Group', '')))
                    
                    # Recalculate Lot Size for Grading
                    lot_size = 1
                    if strat == '130/160' and debit > 6000: lot_size = 2
                    elif strat == '130/160' and debit > 10000: lot_size = 3
                    elif strat == '160/190' and debit > 8000: lot_size = 2
                    elif strat == 'M200' and debit > 12000: lot_size = 2
                    
                    debit_lot = debit / lot_size
                    
                    # Grade
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
                    
                    if "A" in grade: st.success("✅ **APPROVED:** Great Entry")
                    elif "F" in grade: st.error("⛔ **REJECT:** Overpriced")
                    else: st.warning("⚠️ **CHECK:** Acceptable Variance")
            except Exception as e:
                st.error(f"Could not parse file: {e}")

    # --- TAB 3: ANALYTICS ---
    with tab3:
        expired = df[df['status'] == 'Expired']
        if not expired.empty:
            st.markdown("### 🏆 Performance Analytics")
            
            an_tabs = st.tabs(["🚀 Efficiency", "⚔️ Head-to-Head", "🔥 Heatmap"])
            
            with an_tabs[0]:
                fig = px.scatter(expired, x='days_held', y='pnl', color='strategy', size='debit', title="P&L vs Duration")
                st.plotly_chart(fig, use_container_width=True)
                
            with an_tabs[1]:
                perf = expired.groupby('strategy').agg({
                    'pnl': ['count', 'sum', 'mean'],
                    'days_held': 'mean',
                    'daily_yield': 'mean'
                }).reset_index()
                perf.columns = ['Strategy', 'Count', 'Total P&L', 'Avg P&L', 'Avg Days', 'Avg Daily Yield']
                st.dataframe(perf.style.format({'Total P&L': "${:,.0f}", 'Avg P&L': "${:,.0f}", 'Avg Days': "{:.0f}", 'Avg Daily Yield': "{:.2f}%"}), use_container_width=True)
                
            with an_tabs[2]:
                fig = px.density_heatmap(expired, x="days_held", y="strategy", z="pnl", histfunc="avg", color_continuous_scale="RdBu")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No expired trades yet.")

    # --- TAB 4: TIMELINE (Placeholder for DB History) ---
    with tab4:
        st.info("⚠️ Timeline feature requires 'Snapshots' table which was removed for v35 simplicity. To enable history tracking, we need the advanced schema from v56.")

    # --- TAB 5: ALLOCATION ---
    with tab5:
        acct = st.number_input("Account Size ($)", value=150000, step=5000)
        reserve = acct * 0.20
        deploy = acct - reserve
        
        c1, c2, c3 = st.columns(3)
        c1.metric("M200 (40%)", f"${deploy*0.4:,.0f}")
        c2.metric("160/190 (30%)", f"${deploy*0.3:,.0f}")
        c3.metric("130/160 (30%)", f"${deploy*0.3:,.0f}")
        st.caption(f"Cash Reserve: ${reserve:,.0f}")

    # --- TAB 6: JOURNAL ---
    with tab6:
        st.markdown("### 📓 Trade Journal")
        # Filter for easier viewing
        f_strat = st.selectbox("Filter Strategy", ["All"] + list(df['strategy'].unique()))
        j_df = df if f_strat == "All" else df[df['strategy'] == f_strat]
        
        edited = st.data_editor(
            j_df[['trade_id', 'name', 'strategy', 'pnl', 'notes']],
            key="journal",
            hide_index=True,
            use_container_width=True,
            column_config={"trade_id": st.column_config.TextColumn(disabled=True)}
        )
        
        if st.button("💾 Save Notes"):
            conn = get_connection()
            for i, r in edited.iterrows():
                conn.execute("UPDATE trades SET notes = ? WHERE trade_id = ?", (r['notes'], r['trade_id']))
            conn.commit()
            conn.close()
            st.success("Notes Saved!")
            st.rerun()

    # --- TAB 7: RULES ---
    with tab7:
        st.markdown("""
        ### 1. 130/160 Strategy
        * **Target:** Monday. **Debit:** $3.5k-$4.5k.
        * **Manage:** Kill >25d & Flat.
        ### 2. 160/190 Strategy
        * **Target:** Friday. **Debit:** ~$5.2k.
        * **Exit:** Hold 40-50d.
        ### 3. M200 Strategy
        * **Target:** Wednesday. **Debit:** $7.5k-$8.5k.
        * **Manage:** Day 14 Check.
        """)
