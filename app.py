import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
import time
from datetime import datetime
import io

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- APP CONSTANTS ---
VER = "v80.2 (Fixed Excel Reader)"
DB_NAME = "trade_guardian_v80.db"

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        margin: 5px;
        border-left: 5px solid #4caf50;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #f0f2f6;
        border-radius: 4px 4px 0px 0px;
        font-weight: 600;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background-color: #ffffff;
        border-top: 2px solid #4caf50;
    }
</style>
""", unsafe_allow_html=True)

# --- DATABASE MANAGEMENT ---

def get_db_connection():
    """Robust connection with retry logic for locked DBs."""
    retries = 5
    for i in range(retries):
        try:
            return sqlite3.connect(DB_NAME, timeout=10)
        except sqlite3.OperationalError:
            if i == retries - 1: raise
            time.sleep(0.1)

def init_db():
    """Initialize DB and perform auto-migration if needed."""
    conn = get_db_connection()
    c = conn.cursor()
    
    # 1. TRADES TABLE
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    ticker TEXT,
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
                    rho REAL,
                    iv REAL,
                    pop REAL,
                    notes TEXT
                )''')
    
    # 2. SNAPSHOTS TABLE (Now with Greeks!)
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT,
                    snapshot_date DATE,
                    pnl REAL,
                    days_held INTEGER,
                    theta REAL,
                    delta REAL,
                    gamma REAL,
                    vega REAL,
                    rho REAL,
                    iv REAL,
                    FOREIGN KEY(trade_id) REFERENCES trades(id)
                )''')

    # 3. SCHEMA MIGRATION (Auto-Healing)
    try:
        c.execute("SELECT rho FROM trades LIMIT 1")
    except sqlite3.OperationalError:
        st.toast("⚙️ Upgrading DB Schema: Adding Greek columns...", icon="🛠️")
        for col in ['rho', 'iv', 'pop', 'ticker']:
            try: c.execute(f"ALTER TABLE trades ADD COLUMN {col} REAL")
            except: pass
            
        # Fix ticker column type if it was added as REAL by mistake in loop above (it's TEXT)
        # SQLite types are dynamic, so it largely doesn't matter, but good to be clean.

    try:
        c.execute("SELECT theta FROM snapshots LIMIT 1")
    except sqlite3.OperationalError:
        st.toast("⚙️ Upgrading Snapshots: Adding History columns...", icon="🛠️")
        cols = ['theta', 'delta', 'gamma', 'vega', 'rho', 'iv']
        for col in cols:
            try: c.execute(f"ALTER TABLE snapshots ADD COLUMN {col} REAL")
            except: pass
            
    conn.commit()
    conn.close()

# --- CONFIGURATION ---
BASE_CONFIG = {
    '130/160': {'yield': 0.13, 'pnl': 500, 'roi': 6.8, 'dit': 36, 'lot_cost_est': 4000},
    '160/190': {'yield': 0.28, 'pnl': 700, 'roi': 12.7, 'dit': 44, 'lot_cost_est': 5200},
    'M200':    {'yield': 0.56, 'pnl': 900, 'roi': 11.1, 'dit': 41, 'lot_cost_est': 8000}
}

# --- HELPER FUNCTIONS ---

def clean_num(x):
    if pd.isna(x) or str(x).strip() == '': return 0.0
    try:
        s = str(x).replace('$', '').replace(',', '').replace('%', '')
        return float(s)
    except: return 0.0

def get_strategy_from_row(row):
    """Smarter strategy detection using Group column."""
    grp = str(row.get('Group', '')).upper()
    name = str(row.get('Name', '')).upper()
    
    if 'M200' in grp or 'M200' in name: return 'M200'
    if '160/190' in grp or '160/190' in name: return '160/190'
    if '130/160' in grp or '130/160' in name: return '130/160'
    return 'Other'

def estimate_lot_size(strategy, debit):
    """Estimate lots based on debit and strategy baselines."""
    cost_map = {'130/160': 4000, '160/190': 5200, 'M200': 8000}
    base_cost = cost_map.get(strategy, 5000)
    if debit == 0: return 1
    lots = round(debit / base_cost)
    return int(max(1, lots))

def generate_id(name, strategy, entry_date):
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    clean_name = "".join(e for e in str(name) if e.isalnum())
    return f"{strategy}_{d_str}_{clean_name}"[:50]

# --- FILE PROCESSOR (RESTORED EXCEL SUPPORT) ---

def read_file_safely(file):
    """Reads both CSV and Excel files robustly."""
    try:
        # A. Try Excel First (Best for .xlsx)
        if file.name.endswith('.xlsx') or file.name.endswith('.xls'):
            try:
                # Load headerless first to find the "Name" row
                df_raw = pd.read_excel(file, header=None, engine='openpyxl')
                header_idx = -1
                for i, row in df_raw.head(25).iterrows():
                    row_str = " ".join(row.astype(str).values)
                    if "Name" in row_str and "Total Return" in row_str:
                        header_idx = i
                        break
                
                if header_idx != -1:
                    file.seek(0)
                    df = pd.read_excel(file, header=header_idx, engine='openpyxl')
                    # Filter out leg rows (start with dot)
                    return df[~df['Name'].astype(str).str.startswith('.')]
            except Exception as e:
                # If it wasn't a real Excel file, fall through to CSV
                pass

        # B. CSV Fallback
        file.seek(0)
        content = file.getvalue().decode("utf-8", errors='replace')
        lines = content.split('\n')
        
        header_row_idx = 0
        for i, line in enumerate(lines[:25]):
            if "Name" in line and "Total Return" in line:
                header_row_idx = i
                break
        
        file.seek(0)
        df = pd.read_csv(file, skiprows=header_row_idx)
        return df[~df['Name'].astype(str).str.startswith('.')]

    except Exception as e:
        st.error(f"Failed to read {file.name}: {e}")
        return None

def sync_data(file_list, file_type):
    log = []
    if not isinstance(file_list, list): file_list = [file_list]
    
    conn = get_db_connection()
    c = conn.cursor()
    
    new_cnt, upd_cnt = 0, 0
    
    for file in file_list:
        try:
            df = read_file_safely(file)
            if df is None or df.empty:
                log.append(f"⚠️ Skipped {file.name} (Empty/Invalid)")
                continue

            for _, row in df.iterrows():
                name = str(row.get('Name', ''))
                if not name or name.lower() == 'nan': continue
                
                # Parse Dates (Handle NaT/Errors)
                created = row.get('Created At', datetime.now())
                try: 
                    start_dt = pd.to_datetime(created)
                    if pd.isna(start_dt): start_dt = datetime.now()
                except: start_dt = datetime.now()
                
                strat = get_strategy_from_row(row)
                
                # Parse Metrics
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                
                # Greeks & Stats
                theta = clean_num(row.get('Theta', 0))
                delta = clean_num(row.get('Delta', 0))
                gamma = clean_num(row.get('Gamma', 0))
                vega = clean_num(row.get('Vega', 0))
                rho = clean_num(row.get('Rho', 0))
                iv = clean_num(row.get('IV', 0)) * 100 
                pop = clean_num(row.get('Chance', 0)) * 100 
                
                lot_size = estimate_lot_size(strat, debit)
                
                # ID Generation
                trade_id = generate_id(name, strat, start_dt)
                status = "Active" if file_type == "Active" else "Expired"
                
                # Duration
                exit_dt = None
                if file_type == "History":
                    try: 
                        exit_dt = pd.to_datetime(row.get('Expiration'))
                        if pd.isna(exit_dt): exit_dt = datetime.now()
                    except: exit_dt = datetime.now()
                    days_held = (exit_dt - start_dt).days
                else:
                    days_held = (datetime.now() - start_dt).days
                
                days_held = max(1, days_held)
                
                # DB Operations
                c.execute("SELECT id FROM trades WHERE id = ?", (trade_id,))
                exists = c.fetchone()
                
                if not exists:
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, 
                         debit, lot_size, pnl, theta, delta, gamma, vega, rho, iv, pop, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, name, strat, status, start_dt.date(), 
                         exit_dt.date() if exit_dt else None, days_held,
                         debit, lot_size, pnl, theta, delta, gamma, vega, rho, iv, pop, ""))
                    new_cnt += 1
                else:
                    # Update existing
                    c.execute('''UPDATE trades SET 
                        pnl=?, status=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, rho=?, iv=?, pop=?
                        WHERE id=?''', 
                        (pnl, status, days_held, theta, delta, gamma, vega, rho, iv, pop, trade_id))
                    upd_cnt += 1
                
                # SNAPSHOT LOGIC
                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    if not c.fetchone():
                        c.execute('''INSERT INTO snapshots 
                            (trade_id, snapshot_date, pnl, days_held, theta, delta, gamma, vega, rho, iv)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (trade_id, today, pnl, days_held, theta, delta, gamma, vega, rho, iv))
            
            log.append(f"✅ {file.name}: {new_cnt} New, {upd_cnt} Updated")

        except Exception as e:
            log.append(f"❌ Error processing {file.name}: {str(e)}")
            
    conn.commit()
    conn.close()
    return log

# --- DATA LOADING ---
def load_data():
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['exit_date'] = pd.to_datetime(df['exit_date'])
        
        df['ROI'] = (df['pnl'] / df['debit'].replace(0, 1)) * 100
        df['Daily Yield'] = df['ROI'] / df['days_held'].replace(0, 1)
        
        return df
    except: return pd.DataFrame()
    finally: conn.close()

def load_snapshots(trade_id=None):
    conn = get_db_connection()
    query = "SELECT * FROM snapshots"
    params = ()
    if trade_id:
        query += " WHERE trade_id = ? ORDER BY snapshot_date ASC"
        params = (trade_id,)
    
    try:
        df = pd.read_sql(query, conn, params=params)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- INITIALIZATION ---
init_db()

# ==============================================================================
# UI LAYOUT
# ==============================================================================

st.title(f"🛡️ Allantis Trade Guardian {VER}")

# --- SIDEBAR ---
with st.sidebar:
    st.header("🗄️ Data Management")
    
    with st.expander("1. Restore / Backup", expanded=True):
        uploaded_db = st.file_uploader("Restore .db File", type=['db'])
        if uploaded_db:
            with open(DB_NAME, "wb") as f: f.write(uploaded_db.getbuffer())
            st.success("Database restored!")
            time.sleep(1)
            st.rerun()
            
        if os.path.exists(DB_NAME):
            with open(DB_NAME, "rb") as f:
                st.download_button("💾 Download Backup", f, DB_NAME, "application/x-sqlite3")
        
        # Panic Button
        if st.button("⚠️ Reset Database"):
            if os.path.exists(DB_NAME): os.remove(DB_NAME)
            st.warning("Database wiped. Please reload page.")
            st.rerun()

    with st.expander("2. Sync OptionStrat Files", expanded=True):
        active_files = st.file_uploader("Active Trades (CSV/Excel)", accept_multiple_files=True, key="act")
        hist_files = st.file_uploader("History Trades (CSV/Excel)", accept_multiple_files=True, key="hist")
        
        if st.button("🔄 Sync Data"):
            logs = []
            if active_files: logs.extend(sync_data(active_files, "Active"))
            if hist_files: logs.extend(sync_data(hist_files, "History"))
            for l in logs: st.write(l)
            if logs: 
                st.success("Sync Complete!")
                time.sleep(1)
                st.rerun()

    st.divider()
    
    # Global Filters
    df = load_data()
    market_regime = st.selectbox("Market Regime", ["Neutral", "Bullish", "Bearish"])
    regime_mult = 1.1 if market_regime == "Bullish" else 0.9 if market_regime == "Bearish" else 1.0

# --- MAIN TABS ---
tab_dash, tab_valid, tab_lab, tab_analytics = st.tabs(["📊 Dashboard", "🧪 Validator", "🔬 Greeks Lab", "📈 Performance"])

# -----------------------------------------------------------------------------
# TAB 1: DASHBOARD (Active Trades)
# -----------------------------------------------------------------------------
with tab_dash:
    if df.empty:
        st.info("👋 Welcome! Upload your DB or Sync files to get started.")
    else:
        active = df[df['status'] == 'Active'].copy()
        
        # KPI Row
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Active Trades", len(active))
        k2.metric("Total P&L (Unrealized)", f"${active['pnl'].sum():,.0f}")
        k3.metric("Capital Deployed", f"${active['debit'].sum():,.0f}")
        k4.metric("Net Portfolio Delta", f"{active['delta'].sum():,.1f}")
        k5.metric("Net Portfolio Theta", f"{active['theta'].sum():,.0f}")

        st.divider()

        # Strategy Breakdowns
        strategies = active['strategy'].unique()
        if len(strategies) == 0: st.warning("No active trades found.")
        
        for strat in strategies:
            s_df = active[active['strategy'] == strat]
            
            with st.expander(f"🔹 {strat} ({len(s_df)} Trades) - Total P&L: ${s_df['pnl'].sum():,.0f}", expanded=True):
                # Custom Display Columns
                display_cols = ['name', 'entry_date', 'days_held', 'pnl', 'pop', 'iv', 'delta', 'theta', 'debit']
                
                # Apply Color Formatting
                def color_pnl(val):
                    color = 'green' if val > 0 else 'red' if val < 0 else 'black'
                    return f'color: {color}; font-weight: bold'
                
                def alert_days(val):
                    return 'background-color: #ffcccc' if val > 45 else ''

                st.dataframe(
                    s_df[display_cols].style
                    .format({
                        'pnl': "${:,.0f}", 'debit': "${:,.0f}", 
                        'delta': "{:.1f}", 'theta': "{:.1f}", 
                        'iv': "{:.1f}%", 'pop': "{:.1f}%"
                    })
                    .map(color_pnl, subset=['pnl'])
                    .map(alert_days, subset=['days_held']),
                    use_container_width=True
                )
                
                # Mini Action Logic
                config = BASE_CONFIG.get(strat, {})
                target = config.get('pnl', 1000) * regime_mult
                
                # Identify actionable trades
                winners = s_df[s_df['pnl'] >= target]
                stales = s_df[(s_df['days_held'] > config.get('dit', 45)) & (s_df['pnl'] < 0)]
                
                if not winners.empty:
                    st.success(f"💰 Take Profit Targets Hit: {', '.join(winners['name'].tolist())}")
                if not stales.empty:
                    st.error(f"🕰️ Stale/Overdue Trades: {', '.join(stales['name'].tolist())}")

# -----------------------------------------------------------------------------
# TAB 2: VALIDATOR (Pre-Trade Check)
# -----------------------------------------------------------------------------
with tab_valid:
    c1, c2 = st.columns([1, 2])
    
    with c1:
        st.markdown("### 🚦 Trade Auditor")
        v_strat = st.selectbox("Strategy", ["130/160", "160/190", "M200"])
        v_debit = st.number_input("Total Debit ($)", min_value=0.0, step=100.0)
        v_lots = st.number_input("Lot Size", min_value=1, step=1)
        
        if v_lots > 0:
            debit_per_lot = v_debit / v_lots
        else: debit_per_lot = 0
        
        st.metric("Debit Per Lot", f"${debit_per_lot:,.0f}")
        
    with c2:
        st.markdown("### Analysis")
        # Validation Logic
        grade = "B"
        msg = "Standard Entry"
        color = "orange"
        
        if v_strat == "130/160":
            if 3500 <= debit_per_lot <= 4500: grade="A+"; msg="Perfect Pricing"; color="green"
            elif debit_per_lot > 4800: grade="F"; msg="Overpriced! Do not enter."; color="red"
        elif v_strat == "160/190":
            if 4800 <= debit_per_lot <= 5500: grade="A"; msg="Ideal Zone"; color="green"
            elif debit_per_lot > 5600: grade="C-"; msg="Expensive"; color="red"
        elif v_strat == "M200":
            if 7500 <= debit_per_lot <= 8500: grade="A"; msg="Whale Zone"; color="green"
            
        st.markdown(f"""
        <div style='background-color: {color}; padding: 20px; border-radius: 10px; color: white; text-align: center'>
            <h1 style='margin:0'>{grade}</h1>
            <p style='margin:0'>{msg}</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Historical Comparison
        if not df.empty:
            similar = df[
                (df['strategy'] == v_strat) & 
                (df['status'] == 'Expired') &
                ((df['debit']/df['lot_size']).between(debit_per_lot*0.9, debit_per_lot*1.1))
            ]
            if not similar.empty:
                avg_win = similar['pnl'].mean()
                win_rate = (len(similar[similar['pnl']>0]) / len(similar)) * 100
                st.info(f"📊 Historical Context: Found {len(similar)} similar trades. \n\n Win Rate: **{win_rate:.0f}%** | Avg P&L: **${avg_win:,.0f}**")
            else:
                st.write("No historical matches found for this price point.")

# -----------------------------------------------------------------------------
# TAB 3: GREEKS LAB (Lifecycle Analysis)
# -----------------------------------------------------------------------------
with tab_lab:
    st.markdown("### 🧬 Trade Lifecycle & Greeks Impact")
    
    if df.empty:
        st.warning("No data available.")
    else:
        # Selector
        trades_list = df['name'].unique().tolist()
        sel_trade_name = st.selectbox("Select Trade to Analyze", trades_list)
        
        if sel_trade_name:
            # Get ID
            sel_trade_id = df[df['name'] == sel_trade_name]['id'].iloc[0]
            
            # Load Snapshots
            snaps = load_snapshots(sel_trade_id)
            
            if snaps.empty:
                st.warning("No daily snapshots found for this trade yet. (Sync Active trades daily to build this view).")
            else:
                # Layout
                g1, g2 = st.columns([3, 1])
                
                with g1:
                    # MAIN CHART: Dual Axis (PnL vs Delta/Theta)
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    
                    # PnL Line
                    fig.add_trace(
                        go.Scatter(x=snaps['days_held'], y=snaps['pnl'], name="P&L ($)", 
                                   line=dict(color='green', width=4), mode='lines+markers'),
                        secondary_y=False
                    )
                    
                    # Greek Line (User selectable)
                    greek_view = st.radio("Overlay Greek:", ["Delta", "Theta", "IV", "Vega"], horizontal=True)
                    
                    color_map = {'Delta': 'blue', 'Theta': 'purple', 'IV': 'orange', 'Vega': 'red'}
                    col_name = greek_view.lower()
                    
                    if col_name in snaps.columns:
                        fig.add_trace(
                            go.Scatter(x=snaps['days_held'], y=snaps[col_name], name=greek_view,
                                       line=dict(color=color_map[greek_view], dash='dot'), mode='lines'),
                            secondary_y=True
                        )
                    
                    fig.update_layout(title=f"PnL vs {greek_view} Evolution", xaxis_title="Days Held", height=500)
                    fig.update_yaxes(title_text="P&L ($)", secondary_y=False)
                    fig.update_yaxes(title_text=greek_view, secondary_y=True)
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                with g2:
                    # Stats Card
                    curr = snaps.iloc[-1]
                    start = snaps.iloc[0]
                    
                    st.markdown("#### Evolution")
                    st.metric("Current P&L", f"${curr['pnl']:,.0f}", delta=f"{curr['pnl'] - start['pnl']:,.0f}")
                    
                    if 'iv' in snaps.columns:
                        st.metric("IV Change", f"{curr['iv']:.1f}%", delta=f"{curr['iv'] - start['iv']:.1f}%", delta_color="inverse")
                        
                    st.metric("Theta Decay", f"{curr['theta']:.1f}", delta=f"{curr['theta'] - start['theta']:.1f}")

                # Correlation Matrix
                if len(snaps) > 5:
                    st.markdown("#### 🔗 Correlation Matrix (What drives PnL?)")
                    corr_cols = ['pnl', 'delta', 'theta', 'vega', 'iv']
                    avail_cols = [c for c in corr_cols if c in snaps.columns]
                    corr = snaps[avail_cols].corr()
                    
                    fig_corr = px.imshow(corr, text_auto=True, color_continuous_scale='RdBu', aspect="auto")
                    st.plotly_chart(fig_corr, use_container_width=True)

# -----------------------------------------------------------------------------
# TAB 4: ANALYTICS (Performance)
# -----------------------------------------------------------------------------
with tab_analytics:
    if df.empty:
        st.write("No data.")
    else:
        expired = df[df['status'] == 'Expired'].copy()
        
        if expired.empty:
            st.info("No closed trades to analyze yet.")
        else:
            # 1. Equity Curve
            expired = expired.sort_values('exit_date')
            expired['cum_pnl'] = expired['pnl'].cumsum()
            
            fig_eq = px.line(expired, x='exit_date', y='cum_pnl', markers=True, title="Portfolio Growth",
                             labels={'cum_pnl': 'Cumulative Profit ($)', 'exit_date': 'Date'})
            fig_eq.add_hline(y=0, line_dash="dash", line_color="gray")
            st.plotly_chart(fig_eq, use_container_width=True)
            
            # 2. Win/Loss Stats
            col1, col2, col3 = st.columns(3)
            wins = expired[expired['pnl'] > 0]
            losses = expired[expired['pnl'] <= 0]
            
            win_rate = len(wins) / len(expired) if len(expired) > 0 else 0
            avg_win = wins['pnl'].mean() if not wins.empty else 0
            avg_loss = losses['pnl'].mean() if not losses.empty else 0
            pf = abs(wins['pnl'].sum() / losses['pnl'].sum()) if losses['pnl'].sum() != 0 else 0
            
            col1.metric("Win Rate", f"{win_rate:.1%}")
            col2.metric("Profit Factor", f"{pf:.2f}")
            col3.metric("Expectancy", f"${(avg_win*win_rate) - (abs(avg_loss)*(1-win_rate)):,.0f}")
            
            # 3. Strategy Comparison
            st.subheader("Performance by Strategy")
            strat_perf = expired.groupby('strategy').agg({
                'pnl': 'sum',
                'id': 'count',
                'ROI': 'mean',
                'days_held': 'mean'
            }).reset_index()
            
            st.dataframe(strat_perf.style.format({'pnl': "${:,.0f}", 'ROI': "{:.1f}%", 'days_held': "{:.0f}"}), use_container_width=True)

# Footer
st.caption(f"System: {VER} | DB: {DB_NAME}")
