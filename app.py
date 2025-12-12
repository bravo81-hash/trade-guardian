import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- DEBUG BANNER ---
st.info("✅ RUNNING VERSION: v78.0 (Journaling + Integrity)")

st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v4.db"

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
                    gamma REAL,
                    vega REAL,
                    notes TEXT,
                    tags TEXT,
                    parent_id TEXT
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
    
    # Run migrations to ensure old DBs get new columns
    migrate_db()

def migrate_db():
    """Safely adds new columns to existing databases."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("ALTER TABLE trades ADD COLUMN tags TEXT")
    except: pass # Column likely exists
    
    try:
        c.execute("ALTER TABLE trades ADD COLUMN parent_id TEXT")
    except: pass
    
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_NAME)

# --- CONFIGURATION ---
BASE_CONFIG = {
    '130/160': {'yield': 0.13, 'pnl': 500, 'roi': 6.8, 'dit': 36},
    '160/190': {'yield': 0.28, 'pnl': 700, 'roi': 12.7, 'dit': 44},
    'M200':    {'yield': 0.56, 'pnl': 900, 'roi': 11.1, 'dit': 41}
}

# --- HELPER FUNCTIONS ---
def get_strategy(group_name, trade_name=""):
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    """Robust number cleaner that handles currency strings and NaNs."""
    try:
        val = float(str(x).replace('$', '').replace(',', ''))
        if np.isnan(val): return 0.0
        return val
    except: return 0.0

def safe_fmt(val, fmt_str):
    try:
        if isinstance(val, (int, float)): return fmt_str.format(val)
        return str(val)
    except: return str(val)

def generate_id(name, strategy, entry_date):
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{name}_{strategy}_{d_str}".replace(" ", "").replace("/", "-")

def extract_ticker(name):
    try:
        parts = str(name).split(' ')
        if parts:
            ticker = parts[0].replace('.', '').upper()
            if ticker in ['M200', '130', '160', 'IRON', 'VERTICAL']:
                return "UNKNOWN"
            return ticker
        return "UNKNOWN"
    except: return "UNKNOWN"

# --- SMART FILE READER ---
def read_file_safely(file):
    try:
        if file.name.endswith('.xlsx'):
            df_raw = pd.read_excel(file, header=None, engine='openpyxl')
        elif file.name.endswith('.xls'):
            df_raw = pd.read_excel(file, header=None)
        else:
            # CSV Handling
            content = file.getvalue().decode("utf-8")
            lines = content.split('\n')
            header_row = 0
            for i, line in enumerate(lines[:20]):
                if "Name" in line and "Total Return" in line:
                    header_row = i
                    break
            file.seek(0)
            return pd.read_csv(file, skiprows=header_row)

        header_idx = -1
        for i, row in df_raw.head(20).iterrows():
            row_str = " ".join(row.astype(str).values)
            if "Name" in row_str and "Total Return" in row_str:
                header_idx = i
                break
        
        if header_idx != -1:
            df = df_raw.iloc[header_idx+1:].copy()
            df.columns = df_raw.iloc[header_idx]
            return df
        return None

    except Exception as e:
        return None

# --- SYNC ENGINE (v2.0 Integrity) ---
def sync_data(file_list, file_type):
    log = []
    if not isinstance(file_list, list): file_list = [file_list]
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # 1. Integrity Check Prep: Get all currently active IDs from DB
    db_active_ids = set()
    if file_type == "Active":
        try:
            current_active = pd.read_sql("SELECT id FROM trades WHERE status = 'Active'", conn)
            db_active_ids = set(current_active['id'].tolist())
        except: pass
    
    file_found_ids = set()

    for file in file_list:
        count_new = 0
        count_update = 0
        count_missing_cols = 0
        
        try:
            df = read_file_safely(file)
            if df is None or df.empty:
                log.append(f"⚠️ {file.name}: Skipped (Empty/Invalid Format)")
                continue

            # QA: Check critical columns
            required_cols = ['Name', 'Total Return $', 'Net Debit/Credit']
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                log.append(f"⚠️ {file.name}: Missing columns {missing}. Check broker export format.")
                continue

            for _, row in df.iterrows():
                name = str(row.get('Name', ''))
                if name.startswith('.') or name in ['nan', '', 'Symbol']: continue
                
                created = row.get('Created At', '')
                try: start_dt = pd.to_datetime(created)
                except: continue
                
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                theta = clean_num(row.get('Theta', 0))
                delta = clean_num(row.get('Delta', 0))
                gamma = clean_num(row.get('Gamma', 0))
                vega = clean_num(row.get('Vega', 0))
                
                # Lot Sizing
                lot_size = 1
                if strat == '130/160':
                    if debit > 10000: lot_size = 3
                    elif debit > 6000: lot_size = 2
                elif strat == '160/190':
                    if debit > 8000: lot_size = 2
                elif strat == 'M200':
                    if debit > 12000: lot_size = 2

                trade_id = generate_id(name, strat, start_dt)
                status = "Active" if file_type == "Active" else "Expired"
                
                if file_type == "Active":
                    file_found_ids.add(trade_id)
                
                exit_dt = None
                days_held = 1
                
                if file_type == "History":
                    try:
                        exit_dt = pd.to_datetime(row.get('Expiration'))
                        days_held = (exit_dt - start_dt).days
                    except: days_held = 1
                else:
                    days_held = (datetime.now() - start_dt).days
                
                if days_held < 1: days_held = 1
                
                c.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
                if existing is None:
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, notes, tags)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, name, strat, status, start_dt.date(), 
                         exit_dt.date() if exit_dt else None, 
                         days_held, debit, lot_size, pnl, theta, delta, gamma, vega, "", ""))
                    count_new += 1
                else:
                    # Don't overwrite notes/tags on update
                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=? 
                            WHERE id=?''', 
                            (pnl, status, exit_dt.date() if exit_dt else None, days_held, theta, delta, gamma, vega, trade_id))
                        count_update += 1
                    elif existing[0] in ["Active", "Missing"]: # Reactivate if it was missing
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, status='Active'
                            WHERE id=?''', 
                            (pnl, days_held, theta, delta, gamma, vega, trade_id))
                        count_update += 1
                        
                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    if not c.fetchone():
                        c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held) VALUES (?,?,?,?)",
                                  (trade_id, today, pnl, days_held))

            log.append(f"✅ {file.name}: {count_new} New, {count_update} Updated")
            
        except Exception as e:
            log.append(f"❌ {file.name}: Error - {str(e)}")
            
    # 2. Integrity Check: Flag Missing Trades
    if file_type == "Active" and file_found_ids:
        missing_ids = db_active_ids - file_found_ids
        if missing_ids:
            # Mark as Missing instead of auto-deleting
            placeholders = ','.join('?' for _ in missing_ids)
            c.execute(f"UPDATE trades SET status = 'Missing' WHERE id IN ({placeholders})", list(missing_ids))
            log.append(f"⚠️ Integrity Check: Marked {len(missing_ids)} trades as 'Missing' (in DB but not in file).")

    conn.commit()
    conn.close()
    return log

def update_journal(edited_df):
    """Writes edited notes/tags back to the database."""
    conn = get_db_connection()
    c = conn.cursor()
    count = 0
    try:
        # Iterate over edited rows and update DB
        for index, row in edited_df.iterrows():
            # We assume 'id' is in the index or column. 
            # Note: st.data_editor returns a df with same index as input.
            t_id = row['id'] 
            notes = str(row['Notes'])
            tags = str(row['Tags'])
            
            c.execute("UPDATE trades SET notes=?, tags=? WHERE id=?", (notes, tags, t_id))
            count += 1
        conn.commit()
        return count
    except Exception as e:
        return 0
    finally:
        conn.close()

# --- DATA LOADER (Cached) ---
@st.cache_data(ttl=60) # Cache for 60 seconds for performance
def load_data():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
    except Exception as e:
        st.error(f"🚨 DATABASE ERROR: {str(e)}")
        return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes',
            'tags': 'Tags', 'parent_id': 'Parent ID'
        })
        
        # Ensure cols
        for col in ['Gamma', 'Vega', 'Theta', 'Delta', 'P&L', 'Debit', 'lot_size', 'Notes', 'Tags']:
            if col not in df.columns:
                df[col] = "" if col in ['Notes', 'Tags'] else 0.0
        
        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        df['Debit'] = pd.to_numeric(df['Debit'], errors='coerce').fillna(0)
        df['P&L'] = pd.to_numeric(df['P&L'], errors='coerce').fillna(0)
        df['Days Held'] = pd.to_numeric(df['Days Held'], errors='coerce').fillna(1)
        
        df['Debit/Lot'] = df['Debit'] / df['lot_size'].replace(0, 1)
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
        # Helper for grading
        def get_grade(row):
            s, d = row['Strategy'], row['Debit/Lot']
            reason = "Standard"
            grade = "C"
            if s == '130/160':
                if d > 4800: grade="F"; reason="Overpriced (> $4.8k)"
                elif 3500 <= d <= 4500: grade="A+"; reason="Sweet Spot"
                else: grade="B"; reason="Acceptable"
            elif s == '160/190':
                if 4800 <= d <= 5500: grade="A"; reason="Ideal Pricing"
                else: grade="C"; reason="Check Pricing"
            elif s == 'M200':
                if 7500 <= d <= 8500: grade="A"; reason="Perfect Entry"
                else: grade="B"; reason="Variance"
            return pd.Series([grade, reason])

        df[['Grade', 'Reason']] = df.apply(get_grade, axis=1)
        
    return df

@st.cache_data(ttl=300)
def load_snapshots():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        q = """
        SELECT s.snapshot_date, s.pnl, s.days_held, t.strategy, t.name, t.id
        FROM snapshots s
        JOIN trades t ON s.trade_id = t.id
        """
        df = pd.read_sql(q, conn)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        df['pnl'] = pd.to_numeric(df['pnl'], errors='coerce').fillna(0)
        df['days_held'] = pd.to_numeric(df['days_held'], errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- INITIALIZE DB ---
init_db()

# --- SIDEBAR: WORKFLOW WIZARD ---
st.sidebar.markdown("### 🚦 Daily Workflow")

# STEP 1: RESTORE
with st.sidebar.expander("1. 🟢 STARTUP (Restore)", expanded=True):
    restore = st.file_uploader("Upload .db file", type=['db'], key='restore')
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
        st.cache_data.clear() # Clear cache on new DB load
        st.success("Database Restored & Cache Cleared.")
        if 'restored' not in st.session_state:
            st.session_state['restored'] = True
            st.rerun()

st.sidebar.markdown("⬇️ *then...*")

# STEP 2: SYNC
with st.sidebar.expander("2. 🔵 WORK (Sync Files)", expanded=True):
    active_up = st.file_uploader("Active Trades", accept_multiple_files=True, key="act")
    history_up = st.file_uploader("History (Closed)", accept_multiple_files=True, key="hist")
    
    if st.button("🔄 Process & Reconcile"):
        logs = []
        if active_up: logs.extend(sync_data(active_up, "Active"))
        if history_up: logs.extend(sync_data(history_up, "History"))
        
        if logs:
            for l in logs: st.write(l)
            st.cache_data.clear() # Refresh data
            st.success("Sync Complete!")
            # st.rerun() # Optional: auto-refresh

st.sidebar.markdown("⬇️ *finally...*")

# STEP 3: BACKUP
with st.sidebar.expander("3. 🔴 SHUTDOWN (Backup)", expanded=True):
    with open(DB_NAME, "rb") as f:
        st.download_button("💾 Save Database File", f, "trade_guardian_v4.db", "application/x-sqlite3")

# MAINTENANCE
with st.sidebar.expander("🛠️ Maintenance"):
    if st.button("🧹 Vacuum DB"):
        conn = get_db_connection()
        conn.execute("VACUUM")
        conn.close()
        st.success("Database optimized.")
    
    if st.button("🔥 Purge Old Snapshots (>90d)"):
        conn = get_db_connection()
        cutoff = (datetime.now() - pd.Timedelta(days=90)).date()
        conn.execute("DELETE FROM snapshots WHERE snapshot_date < ?", (cutoff,))
        conn.commit()
        conn.close()
        st.success("Old snapshots purged.")

st.sidebar.divider()

# STRATEGY SETTINGS
st.sidebar.header("⚙️ Strategy Settings")
market_regime = st.sidebar.selectbox(
    "Current Market Regime", 
    ["Neutral (Standard)", "Bullish (Aggr. Targets)", "Bearish (Safe Targets)"],
    index=0
)
regime_mult = 1.10 if "Bullish" in market_regime else 0.90 if "Bearish" in market_regime else 1.0

# --- SMART EXIT ENGINE ---
def get_action_signal(strat, status, days_held, pnl, benchmarks_dict):
    if status == "Missing":
        return "MISSING (Review)", "ERROR"
        
    if status == "Active":
        benchmark = benchmarks_dict.get(strat, {})
        base_target = benchmark.get('pnl', 0) or BASE_CONFIG.get(strat, {}).get('pnl', 9999)
        final_target = base_target * regime_mult
            
        if pnl >= final_target:
            return f"TAKE PROFIT (Hit ${final_target:,.0f})", "SUCCESS"

        if strat == '130/160':
            if 25 <= days_held <= 35 and pnl < 100:
                return "KILL (Stale >25d)", "ERROR"
        elif strat == '160/190':
            if days_held < 30: return "COOKING (Do Not Touch)", "INFO"
            elif 30 <= days_held <= 40: return "WATCH (Profit Zone)", "WARNING"
        elif strat == 'M200':
            if 12 <= days_held <= 16:
                return ("DAY 14 CHECK (Green)", "SUCCESS") if pnl > 200 else ("DAY 14 CHECK (Red)", "WARNING")
                
    return "", "NONE"

# --- MAIN APP ---
df = load_data()

benchmarks = BASE_CONFIG.copy()
if not df.empty:
    expired_df = df[df['Status'] == 'Expired']
    if not expired_df.empty:
        hist_grp = expired_df.groupby('Strategy')
        for strat, grp in hist_grp:
            winners = grp[grp['P&L'] > 0]
            if not winners.empty:
                benchmarks[strat] = {
                    'yield': grp['Daily Yield %'].mean(),
                    'pnl': winners['P&L'].mean(),
                    'roi': winners['ROI'].mean(),
                    'dit': winners['Days Held'].mean()
                }

# TABS
tab1, tab2, tab3, tab4 = st.tabs(["📊 Active Dashboard", "🧪 Trade Validator", "📈 Analytics", "📖 Rule Book"])

# 1. ACTIVE DASHBOARD
with tab1:
    if not df.empty:
        # Filter active + missing for review
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        
        if active_df.empty:
            st.info("📭 No active trades in database. Go to Step 2 (Work) in the sidebar.")
        else:
            # --- ACTION LOGIC ---
            act_list, sig_list = [], []
            for _, row in active_df.iterrows():
                act, sig = get_action_signal(row['Strategy'], row['Status'], row['Days Held'], row['P&L'], benchmarks)
                act_list.append(act)
                sig_list.append(sig)
            active_df['Action'] = act_list
            active_df['Signal_Type'] = sig_list
            
            # --- ACTION QUEUE (TO-DO LIST) ---
            todo_df = active_df[active_df['Action'] != ""]
            if not todo_df.empty:
                st.markdown("### ✅ Action Queue (To-Do)")
                t1, t2 = st.columns([3, 1])
                with t1:
                    for _, row in todo_df.iterrows():
                        sig = row['Signal_Type']
                        color = {"SUCCESS":"green", "ERROR":"red", "WARNING":"orange", "INFO":"blue"}.get(sig, "grey")
                        st.markdown(f"**{row['Name']}**: :{color}[{row['Action']}] | *{row['Strategy']}*")
                with t2:
                    csv_todo = todo_df[['Name', 'Action', 'P&L', 'Days Held']].to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Download To-Do List", csv_todo, "todo_list.csv", "text/csv")
                st.divider()

            # --- JOURNALING INTERFACE ---
            st.markdown("### 🏛️ Trade Journal")
            st.caption("Edit 'Notes' and 'Tags' directly below, then click Save.")
            
            # Columns to display/edit
            display_cols = ['id', 'Name', 'Strategy', 'Status', 'P&L', 'Debit', 'Days Held', 'Notes', 'Tags', 'Action']
            
            # Configure column settings for data editor
            column_config = {
                "id": None, # Hide ID
                "Name": st.column_config.TextColumn("Trade Name", disabled=True),
                "Strategy": st.column_config.TextColumn("Strat", disabled=True, width="small"),
                "Status": st.column_config.TextColumn("Status", disabled=True, width="small"),
                "P&L": st.column_config.NumberColumn("P&L", format="$%d", disabled=True),
                "Debit": st.column_config.NumberColumn("Debit", format="$%d", disabled=True),
                "Notes": st.column_config.TextColumn("📝 Notes", width="large"),
                "Tags": st.column_config.SelectboxColumn("🏷️ Tags", options=["Rolled", "Hedged", "Earnings", "High Risk", "Watch"], width="medium"),
                "Action": st.column_config.TextColumn("Signal", disabled=True),
            }
            
            # THE EDITABLE DATAFRAME
            edited_df = st.data_editor(
                active_df[display_cols],
                column_config=column_config,
                hide_index=True,
                use_container_width=True,
                key="journal_editor",
                num_rows="fixed"
            )
            
            # SAVE BUTTON
            if st.button("💾 Save Journal Changes"):
                changes = update_journal(edited_df)
                if changes > 0:
                    st.success(f"Saved notes for {changes} trades!")
                    st.cache_data.clear()
                    # st.rerun() # Optional refresh
                else:
                    st.info("No changes detected or save failed.")

            # Summary Metrics
            st.divider()
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Active P&L", f"${active_df['P&L'].sum():,.0f}")
            col2.metric("Total Capital", f"${active_df['Debit'].sum():,.0f}")
            col3.metric("Avg Daily Yield", f"{active_df['Daily Yield %'].mean():.2f}%")

    else:
        st.info("👋 Database is empty. Sync your first file.")

# 2. VALIDATOR
with tab2:
    st.markdown("### 🧪 Pre-Flight Audit")
    model_file = st.file_uploader("Upload Model File", key="mod")
    if model_file:
        try:
            m_raw = read_file_safely(model_file)
            if m_raw is not None:
                row = m_raw.iloc[0]
                name = row.get('Name', 'Unknown')
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                
                # Lot Sizing (Same logic as Sync)
                lot_size = 1
                if strat == '130/160':
                    if debit > 10000: lot_size = 3
                    elif debit > 6000: lot_size = 2
                elif strat == '160/190':
                    if debit > 8000: lot_size = 2
                elif strat == 'M200':
                    if debit > 12000: lot_size = 2
                
                debit_lot = debit / max(1, lot_size)
                
                grade, reason = "C", "Standard"
                if strat == '130/160':
                    if debit_lot > 4800: grade, reason = "F", "Overpriced (> $4.8k)"
                    elif 3500 <= debit_lot <= 4500: grade, reason = "A+", "Sweet Spot"
                    else: grade, reason = "B", "Acceptable"
                elif strat == '160/190':
                    if 4800 <= debit_lot <= 5500: grade, reason = "A", "Ideal Pricing"
                    else: grade, reason = "C", "Check Pricing"
                elif strat == 'M200':
                    if 7500 <= debit_lot <= 8500: grade, reason = "A", "Perfect Entry"
                    else: grade, reason = "B", "Variance"

                st.divider()
                st.subheader(f"Audit: {name}")
                c1, c2, c3 = st.columns(3)
                c1.metric("Strategy", strat)
                c2.metric("Total Debit", f"${debit:,.0f}")
                c3.metric("Per Lot", f"${debit_lot:,.0f}")
                
                if "A" in grade: st.success(f"✅ **APPROVED:** {reason}")
                elif "F" in grade: st.error(f"⛔ **REJECT:** {reason}")
                else: st.warning(f"⚠️ **CHECK:** {reason}")
        except Exception as e:
            st.error(f"Error: {e}")

# 3. ANALYTICS
with tab3:
    if not df.empty and 'Entry Date' in df.columns:
        valid_dates = df['Entry Date'].dropna()
        if not valid_dates.empty:
            min_date, max_date = valid_dates.min().date(), valid_dates.max().date()
            date_range = st.date_input("Filter Date Range", [min_date, max_date])
            if len(date_range) == 2:
                filtered_df = df[(df['Entry Date'] >= pd.to_datetime(date_range[0])) & (df['Entry Date'] <= pd.to_datetime(date_range[1]) + pd.Timedelta(days=1))]
            else: filtered_df = df
        else: filtered_df = df

        expired_sub = filtered_df[filtered_df['Status'] == 'Expired'].copy()
        
        an1, an2, an3, an4, an5 = st.tabs(["🌊 Equity", "🎯 Expectancy", "🔥 Heatmaps", "⚠️ Risk", "🧬 Lifecycle"])

        with an1:
            if not expired_sub.empty:
                ec_df = expired_sub.dropna(subset=["Exit Date"]).sort_values("Exit Date").copy()
                ec_df['Cumulative P&L'] = ec_df['P&L'].cumsum()
                fig = px.line(ec_df, x='Exit Date', y='Cumulative P&L', title="Realized Equity Curve", markers=True)
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("No closed trades.")

        with an2:
            if not expired_sub.empty:
                wins = expired_sub[expired_sub['P&L'] > 0]
                losses = expired_sub[expired_sub['P&L'] <= 0]
                win_rate = len(wins) / len(expired_sub) * 100 if len(expired_sub) > 0 else 0
                st.metric("Win Rate", f"{win_rate:.1f}%")
                fig = px.histogram(expired_sub, x="P&L", color="Strategy", nbins=20, title="P&L Distribution")
                st.plotly_chart(fig, use_container_width=True)

        with an3:
            if not expired_sub.empty:
                exp_hm = expired_sub.dropna(subset=['Exit Date']).copy()
                exp_hm['Month'] = exp_hm['Exit Date'].dt.month_name()
                exp_hm['Year'] = exp_hm['Exit Date'].dt.year
                hm_data = exp_hm.groupby(['Year', 'Month'])['P&L'].sum().reset_index()
                fig = px.density_heatmap(hm_data, x="Month", y="Year", z="P&L", title="Monthly Seasonality", text_auto=True, color_continuous_scale="RdBu")
                st.plotly_chart(fig, use_container_width=True)

        # NEW: Risk Concentration
        with an4:
            active_only = df[df['Status'] == 'Active']
            if not active_only.empty:
                st.subheader("Capital Concentration")
                c1, c2 = st.columns(2)
                with c1:
                    fig_strat = px.pie(active_only, values='Debit', names='Strategy', title="Risk by Strategy", hole=0.4)
                    st.plotly_chart(fig_strat, use_container_width=True)
                with c2:
                    fig_tick = px.pie(active_only, values='Debit', names='Ticker', title="Risk by Ticker", hole=0.4)
                    st.plotly_chart(fig_tick, use_container_width=True)
            else: st.info("No active trades to analyze risk.")

        with an5:
            snaps = load_snapshots()
            if not snaps.empty:
                sel_strat = st.selectbox("Select Strategy", snaps['strategy'].unique())
                strat_snaps = snaps[snaps['strategy'] == sel_strat]
                fig = px.line(strat_snaps, x='days_held', y='pnl', color='name', line_group='id', title=f"Trade Lifecycle: {sel_strat}")
                st.plotly_chart(fig, use_container_width=True)

# 4. RULE BOOK
with tab4:
    st.markdown("### 📖 Trading Rules")
    st.markdown("""
    * **130/160:** Enter Mon. Target $3.5k-$4.5k debit. Kill >25 days if flat.
    * **160/190:** Enter Fri. Target ~$5.2k debit. Hold 40-50 days.
    * **M200:** Enter Wed. Target $7.5k-$8.5k debit. Check Day 14.
    """)
    st.divider()
    st.caption("Allantis Trade Guardian v78.0")
