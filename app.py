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

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- DEBUG BANNER ---
st.info("✅ RUNNING VERSION: v91.2 (Fix: Explicit Active vs History P&L Logic)")

st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v4.db"

def init_db():
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
                    call_pnl REAL,
                    put_pnl REAL,
                    theta REAL,
                    delta REAL,
                    gamma REAL,
                    vega REAL,
                    notes TEXT,
                    tags TEXT,
                    parent_id TEXT
                )''')
    
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
    migrate_db()

def migrate_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try: c.execute("ALTER TABLE trades ADD COLUMN tags TEXT")
    except: pass 
    try: c.execute("ALTER TABLE trades ADD COLUMN parent_id TEXT")
    except: pass
    try: c.execute("ALTER TABLE trades ADD COLUMN call_pnl REAL")
    except: pass
    try: c.execute("ALTER TABLE trades ADD COLUMN put_pnl REAL")
    except: pass
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_NAME)

# --- CONFIGURATION ---
BASE_CONFIG = {
    '130/160': {'yield': 0.13, 'pnl': 500, 'roi': 6.8, 'dit': 36, 'target_debit_min': 3500, 'target_debit_max': 4500, 'target_days': [0, 1]}, 
    '160/190': {'yield': 0.28, 'pnl': 700, 'roi': 12.7, 'dit': 44, 'target_debit_min': 4800, 'target_debit_max': 5500, 'target_days': [4]}, 
    'M200':    {'yield': 0.56, 'pnl': 900, 'roi': 11.1, 'dit': 41, 'target_debit_min': 7500, 'target_debit_max': 8500, 'target_days': [2]}, 
    'SMSF':    {'yield': 0.20, 'pnl': 600, 'roi': 8.0, 'dit': 40, 'target_debit_min': 2000, 'target_debit_max': 15000, 'target_days': [0, 1, 2, 3, 4]} 
}

# --- HELPER FUNCTIONS ---
def get_strategy(group_name, trade_name=""):
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "SMSF" in g or "SMSF" in n: return "SMSF"
    elif "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    try:
        s = str(x).strip()
        if not s or s.lower() == 'nan' or s == '-': return 0.0
        val = float(s.replace('$', '').replace(',', '').replace('%', ''))
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
            if ticker in ['M200', '130', '160', 'IRON', 'VERTICAL', 'SMSF']:
                return "UNKNOWN"
            return ticker
        return "UNKNOWN"
    except: return "UNKNOWN"

# --- EXPLICIT PARSER ---
def read_and_parse_optionstrat(file, file_type):
    """
    Parses OptionStrat files with STRICT separation of logic for Active vs History files.
    """
    try:
        # 1. Read Raw Data
        if file.name.endswith('.xlsx'):
            df_raw = pd.read_excel(file, header=None, engine='openpyxl')
        elif file.name.endswith('.xls'):
            df_raw = pd.read_excel(file, header=None)
        else:
            try: df_raw = pd.read_csv(file, header=None)
            except: 
                file.seek(0)
                df_raw = pd.read_csv(file, header=None)
        
        # 2. Find Header Row
        header_idx = -1
        for i, row in df_raw.head(20).iterrows():
            row_str = " ".join(row.astype(str).values)
            if "Name" in row_str and "Total Return" in row_str:
                header_idx = i
                break
        
        if header_idx == -1: return []

        # 3. Process Data Frame
        df = df_raw.iloc[header_idx+1:].copy()
        df.columns = df_raw.iloc[header_idx]
        df.reset_index(drop=True, inplace=True)
        
        parsed_trades = []
        current_trade = None
        current_legs = [] 
        
        def get_val(r, col_name, default=0):
            if col_name in df.columns:
                return clean_num(r[col_name])
            return default

        # 4. Iterate and Buffer
        for i, row in df.iterrows():
            col0 = str(row.iloc[0]).strip()
            if col0 in ['nan', '', 'Symbol', 'Name']: continue

            # PARENT ROW
            if not col0.startswith('.'):
                if current_trade: 
                    finalize_trade_pnl(current_trade, current_legs, file_type)
                    parsed_trades.append(current_trade)
                
                created_at = row.get('Created At')
                try: 
                    start_dt = pd.to_datetime(created_at)
                    if pd.isna(start_dt): continue
                except: continue
                
                name = col0
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                
                pnl = get_val(row, 'Total Return $')
                debit = abs(get_val(row, 'Net Debit/Credit'))
                
                lot_size = 1
                if strat == '130/160':
                    if debit > 11000: lot_size = 3
                    elif debit > 6000: lot_size = 2
                elif strat == '160/190':
                    if debit > 8000: lot_size = 2
                elif strat == 'M200':
                    if debit > 12000: lot_size = 2
                elif strat == 'SMSF':
                    if debit > 12000: lot_size = 2

                theta = get_val(row, 'Theta')
                delta = get_val(row, 'Delta')
                gamma = get_val(row, 'Gamma')
                vega = get_val(row, 'Vega')

                exit_dt = None
                try:
                    raw_exp = row.get('Expiration')
                    if pd.notnull(raw_exp) and str(raw_exp).strip() != '':
                        exit_dt = pd.to_datetime(raw_exp)
                except: pass

                current_trade = {
                    'name': name, 'strategy': strat, 'start_dt': start_dt, 'exit_dt': exit_dt,
                    'debit': debit, 'lot_size': lot_size, 'pnl': pnl,
                    'theta': theta, 'delta': delta, 'gamma': gamma, 'vega': vega,
                    'call_pnl': 0.0, 'put_pnl': 0.0 
                }
                current_legs = []

            # LEG ROW
            elif col0.startswith('.') and current_trade is not None:
                try:
                    symbol = col0
                    qty = clean_num(row.iloc[1])
                    entry = clean_num(row.iloc[2])
                    current = clean_num(row.iloc[3])
                    close = clean_num(row.iloc[4])
                    
                    # Store raw values for processing in finalize_trade_pnl
                    current_legs.append({
                        'symbol': symbol,
                        'qty': qty,
                        'entry': entry,
                        'current': current,
                        'close': close
                    })
                except: continue
        
        if current_trade:
            finalize_trade_pnl(current_trade, current_legs, file_type)
            parsed_trades.append(current_trade)
        
        return parsed_trades

    except Exception as e:
        st.error(f"Error Parsing File {file.name}: {e}")
        return []

def finalize_trade_pnl(trade, legs, file_type):
    """
    Calculates split P&L based on file type logic.
    """
    if not legs: return

    calc_call_pnl = 0.0
    calc_put_pnl = 0.0
    
    for leg in legs:
        qty = leg['qty']
        entry = leg['entry']
        
        # --- PRICE LOGIC ---
        if file_type == "Active":
            # For Active: Use Current Price.
            # Formula: (Current - Entry) * Qty * 100
            # If current is missing (rare for active), assume 0 change or entry price? 
            # Safest is current.
            exit_price = leg['current']
        else:
            # For History: Use Close Price.
            # If Close is 0/Empty, assume expired worthless (0.0).
            # If Close is missing but Current exists (rare), use Current.
            if leg['close'] != 0: exit_price = leg['close']
            else: exit_price = 0.0 # Expired Worthless
            
        # PnL Calculation (Standard Equity/Index Option Model)
        # Multiplier is key. SPX/Equity options = 100.
        multiplier = 100 
        leg_pnl = (exit_price - entry) * qty * multiplier
        
        # --- TYPE LOGIC (Call vs Put) ---
        match = re.search(r'\d{6}([CP])', leg['symbol'])
        is_call = False
        if match: is_call = (match.group(1) == 'C')
        else: is_call = ('C' in leg['symbol'] and 'P' not in leg['symbol'])
        
        if is_call: calc_call_pnl += leg_pnl
        else: calc_put_pnl += leg_pnl

    # --- NORMALIZATION ---
    # The sum of calculated leg P&Ls (calc_total) might not exactly match the 
    # official trade['pnl'] from the parent row due to fees, mid-market pricing, etc.
    # To be precise, we Pro-Rate the official P&L based on the calculated split.
    
    calc_total = calc_call_pnl + calc_put_pnl
    
    # If we have a calculated total, distribute the official P&L proportionally
    # This ensures the sum of parts equals the whole.
    if abs(calc_total) > 0.01:
        # Avoid division by zero
        ratio_call = calc_call_pnl / abs(calc_total) # Use abs to handle sign correctly in pro-rating?
        # Actually, pro-rating P&L is tricky with mixed signs.
        # Simple Difference Adjustment is safer:
        # diff = Official - Calculated
        # Distribute diff equally or proportionally?
        # Let's trust the RAW Calculated values for the breakdown visuals, 
        # but scale them if they are wildly off.
        
        # For this version, let's just use the raw calculated values for the split columns.
        # They don't have to sum perfectly to the 'Official P&L' column in the database,
        # but they should be close.
        trade['call_pnl'] = calc_call_pnl
        trade['put_pnl'] = calc_put_pnl
    else:
        # If calc_total is 0, we can't determine split. 
        trade['call_pnl'] = 0
        trade['put_pnl'] = 0

# --- SYNC ENGINE ---
def sync_data(file_list, file_type):
    log = []
    if not isinstance(file_list, list): file_list = [file_list]
    
    conn = get_db_connection()
    c = conn.cursor()
    
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
        
        try:
            trades_data = read_and_parse_optionstrat(file, file_type)
            
            if not trades_data:
                log.append(f"⚠️ {file.name}: Skipped (No valid trades found)")
                continue

            for t in trades_data:
                trade_id = generate_id(t['name'], t['strategy'], t['start_dt'])
                status = "Active" if file_type == "Active" else "Expired"
                
                if t['exit_dt'] and file_type == "History":
                      days_held = (t['exit_dt'] - t['start_dt']).days
                else:
                      days_held = (datetime.now() - t['start_dt']).days
                if days_held < 1: days_held = 1
                
                if file_type == "Active":
                    file_found_ids.add(trade_id)
                
                c.execute("SELECT status, theta, delta, gamma, vega FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
                if existing is None:
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, call_pnl, put_pnl, theta, delta, gamma, vega, notes, tags, parent_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, t['name'], t['strategy'], status, t['start_dt'].date(), 
                         t['exit_dt'].date() if t['exit_dt'] else None, 
                         days_held, t['debit'], t['lot_size'], t['pnl'], t['call_pnl'], t['put_pnl'],
                         t['theta'], t['delta'], t['gamma'], t['vega'], "", "", ""))
                    count_new += 1
                else:
                    old_status, old_theta, old_delta, old_gamma, old_vega = existing
                    final_theta = t['theta'] if t['theta'] != 0 else old_theta
                    final_delta = t['delta'] if t['delta'] != 0 else old_delta
                    final_gamma = t['gamma'] if t['gamma'] != 0 else old_gamma
                    final_vega = t['vega'] if t['vega'] != 0 else old_vega

                    c.execute('''UPDATE trades SET 
                        pnl=?, call_pnl=?, put_pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, status=?, exit_date=?
                        WHERE id=?''', 
                        (t['pnl'], t['call_pnl'], t['put_pnl'], days_held, 
                         final_theta, final_delta, final_gamma, final_vega, status,
                         t['exit_dt'].date() if t['exit_dt'] else None, trade_id))
                    count_update += 1
                    
                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    if not c.fetchone():
                        c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held) VALUES (?,?,?,?)",
                                  (trade_id, today, t['pnl'], days_held))

            log.append(f"✅ {file.name}: {count_new} New, {count_update} Updated")
            
        except Exception as e:
            log.append(f"❌ {file.name}: Error - {str(e)}")
            
    if file_type == "Active" and file_found_ids:
        missing_ids = db_active_ids - file_found_ids
        if missing_ids:
            placeholders = ','.join('?' for _ in missing_ids)
            c.execute(f"UPDATE trades SET status = 'Missing' WHERE id IN ({placeholders})", list(missing_ids))
            log.append(f"⚠️ Integrity: Marked {len(missing_ids)} trades as 'Missing'.")

    conn.commit()
    conn.close()
    return log

def update_journal(edited_df):
    conn = get_db_connection()
    c = conn.cursor()
    count = 0
    try:
        for index, row in edited_df.iterrows():
            t_id = row['id'] 
            notes = str(row['Notes'])
            tags = str(row['Tags'])
            pid = str(row['Parent ID'])
            c.execute("UPDATE trades SET notes=?, tags=?, parent_id=? WHERE id=?", (notes, tags, pid, t_id))
            count += 1
        conn.commit()
        return count
    except Exception as e: return 0
    finally: conn.close()

def get_pnl_drivers(df):
    if df.empty: return None
    cols = ['P&L', 'Delta', 'Theta', 'Vega', 'Gamma', 'Days Held']
    valid_cols = [c for c in cols if c in df.columns]
    if len(df) < 3: return None
    try:
        corr_matrix = df[valid_cols].corr()
        if 'P&L' in corr_matrix.columns:
            pnl_drivers = corr_matrix['P&L'].drop('P&L').sort_values(ascending=False).to_frame(name='Correlation')
            return pnl_drivers
        return None
    except: return None

# --- DATA LOADER ---
@st.cache_data(ttl=60)
def load_data():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        snaps = pd.read_sql("SELECT trade_id, pnl FROM snapshots", conn)
        if not snaps.empty:
            vol_df = snaps.groupby('trade_id')['pnl'].std().reset_index()
            vol_df.rename(columns={'pnl': 'P&L Vol'}, inplace=True)
            df = df.merge(vol_df, left_on='id', right_on='trade_id', how='left')
            df['P&L Vol'] = df['P&L Vol'].fillna(0)
        else:
            df['P&L Vol'] = 0.0
    except Exception as e: return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes',
            'tags': 'Tags', 'parent_id': 'Parent ID', 'call_pnl': 'Call P&L', 'put_pnl': 'Put P&L'
        })
        
        required_cols = ['Gamma', 'Vega', 'Theta', 'Delta', 'P&L', 'Call P&L', 'Put P&L', 'Debit', 'lot_size', 'Notes', 'Tags', 'Parent ID']
        for col in required_cols:
            if col not in df.columns:
                df[col] = "" if col in ['Notes', 'Tags', 'Parent ID'] else 0.0
        
        df['Entry Date'] = pd.to_datetime(df['Entry Date'], errors='coerce')
        df['Exit Date'] = pd.to_datetime(df['Exit Date'], errors='coerce')
        df['Debit'] = pd.to_numeric(df['Debit'], errors='coerce').fillna(0)
        df['P&L'] = pd.to_numeric(df['P&L'], errors='coerce').fillna(0)
        df['Call P&L'] = pd.to_numeric(df['Call P&L'], errors='coerce').fillna(0)
        df['Put P&L'] = pd.to_numeric(df['Put P&L'], errors='coerce').fillna(0)
        df['Days Held'] = pd.to_numeric(df['Days Held'], errors='coerce').fillna(1)
        
        df['lot_size'] = pd.to_numeric(df['lot_size'], errors='coerce').fillna(1).replace(0, 1)
        df['Debit/Lot'] = df['Debit'] / df['lot_size']
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = np.where(df['Debit'] > 0, (df['Theta'] / df['Debit']) * 100, 0)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
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
            elif s == 'SMSF':
                if d > 15000: grade="B"; reason="High Debit" 
                else: grade="A"; reason="Standard"
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

# --- SIDEBAR ---
st.sidebar.markdown("### 🚦 Daily Workflow")
with st.sidebar.expander("1. 🟢 STARTUP (Restore)", expanded=True):
    restore = st.file_uploader("Upload .db file", type=['db'], key='restore')
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
        st.cache_data.clear()
        st.success("Restored.")
        st.rerun()

st.sidebar.markdown("⬇️ *then...*")
with st.sidebar.expander("2. 🔵 WORK (Sync Files)", expanded=True):
    active_up = st.file_uploader("Active Trades", accept_multiple_files=True, key="act")
    history_up = st.file_uploader("History (Closed)", accept_multiple_files=True, key="hist")
    if st.button("🔄 Process & Reconcile"):
        logs = []
        if active_up: logs.extend(sync_data(active_up, "Active"))
        if history_up: logs.extend(sync_data(history_up, "History"))
        if logs:
            for l in logs: st.write(l)
            st.cache_data.clear()
            st.success("Sync Complete!")

st.sidebar.markdown("⬇️ *finally...*")
with st.sidebar.expander("3. 🔴 SHUTDOWN (Backup)", expanded=True):
    with open(DB_NAME, "rb") as f:
        st.download_button("💾 Save Database File", f, DB_NAME, "application/x-sqlite3")

with st.sidebar.expander("🛠️ Maintenance"):
    if st.button("🧹 Vacuum DB"):
        conn = get_db_connection()
        conn.execute("VACUUM")
        conn.close()
        st.success("Optimized.")
    if st.button("🔥 Purge Old Snapshots (>90d)"):
        conn = get_db_connection()
        cutoff = (datetime.now() - pd.Timedelta(days=90)).date()
        conn.execute("DELETE FROM snapshots WHERE snapshot_date < ?", (cutoff,))
        conn.commit()
        conn.close()
        st.success("Purged.")
    if st.button("🧨 Hard Reset (Delete All Data)"):
        conn = get_db_connection()
        conn.execute("DROP TABLE IF EXISTS trades")
        conn.execute("DROP TABLE IF EXISTS snapshots")
        conn.commit()
        conn.close()
        init_db()
        st.cache_data.clear()
        st.success("Wiped & Reset.")
        st.rerun()

st.sidebar.divider()
st.sidebar.header("⚙️ Strategy Settings")
market_regime = st.sidebar.selectbox("Current Market Regime", ["Neutral (Standard)", "Bullish (Aggr. Targets)", "Bearish (Safe Targets)"], index=0)
regime_mult = 1.10 if "Bullish" in market_regime else 0.90 if "Bearish" in market_regime else 1.0

# --- SMART EXIT ENGINE ---
def get_action_signal(strat, status, days_held, pnl, benchmarks_dict):
    if status == "Missing": return "MISSING (Review)", "ERROR"
    if status == "Active":
        benchmark = benchmarks_dict.get(strat, {})
        base_target = benchmark.get('pnl', 0) or BASE_CONFIG.get(strat, {}).get('pnl', 9999)
        final_target = base_target * regime_mult
        
        if pnl >= final_target: return f"TAKE PROFIT (Hit ${final_target:,.0f})", "SUCCESS"
        if strat == '130/160':
            if 25 <= days_held <= 35 and pnl < 100: return "KILL (Stale >25d)", "ERROR"
        elif strat == '160/190':
            if days_held < 30: return "COOKING (Do Not Touch)", "INFO"
            elif 30 <= days_held <= 40: return "WATCH (Profit Zone)", "WARNING"
        elif strat == 'M200':
            if 12 <= days_held <= 16:
                return ("DAY 14 CHECK (Green)", "SUCCESS") if pnl > 200 else ("DAY 14 CHECK (Red)", "WARNING")
        elif strat == 'SMSF':
            if pnl > 1000: return "PROFIT CHECK", "SUCCESS"
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
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        
        if active_df.empty:
            st.info("📭 No active trades.")
        else:
            port_yield = active_df['Daily Yield %'].mean()
            if port_yield < 0.10: st.sidebar.error(f"🚨 Yield Critical: {port_yield:.2f}%")
            elif port_yield < 0.15: st.sidebar.warning(f"⚠️ Yield Low: {port_yield:.2f}%")
            else: st.sidebar.success(f"✅ Yield Healthy: {port_yield:.2f}%")

            act_list, sig_list = [], []
            for _, row in active_df.iterrows():
                act, sig = get_action_signal(row['Strategy'], row['Status'], row['Days Held'], row['P&L'], benchmarks)
                act_list.append(act)
                sig_list.append(sig)
            active_df['Action'] = act_list
            active_df['Signal_Type'] = sig_list

            # --- ACTION QUEUE ---
            todo_df = active_df[active_df['Action'] != ""]
            if not todo_df.empty:
                st.markdown("### ✅ Action Queue")
                t1, t2 = st.columns([3, 1])
                with t1:
                    for _, row in todo_df.iterrows():
                        sig = row['Signal_Type']
                        color = {"SUCCESS":"green", "ERROR":"red", "WARNING":"orange", "INFO":"blue"}.get(sig, "grey")
                        st.markdown(f"**{row['Name']}**: :{color}[{row['Action']}] | *{row['Strategy']}*")
                with t2:
                    csv_todo = todo_df[['Name', 'Action', 'P&L', 'Days Held']].to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Download Queue", csv_todo, "todo_list.csv", "text/csv")
                st.divider()

            # --- MASTER JOURNAL ---
            with st.expander("📝 Master Trade Journal (Editable)", expanded=False):
                st.caption("Edit 'Notes', 'Tags' or 'Parent ID'.")
                display_cols = ['id', 'Name', 'Strategy', 'Status', 'Theta/Cap %', 'Theta Eff.', 'P&L', 'Call P&L', 'Put P&L', 'P&L Vol', 'Debit', 'Days Held', 'Notes', 'Tags', 'Parent ID', 'Action']
                column_config = {
                    "id": None, 
                    "Name": st.column_config.TextColumn("Trade Name", disabled=True),
                    "Strategy": st.column_config.TextColumn("Strat", disabled=True, width="small"),
                    "Status": st.column_config.TextColumn("Status", disabled=True, width="small"),
                    "Theta/Cap %": st.column_config.NumberColumn("Θ/Cap", format="%.2f%%", disabled=True),
                    "Theta Eff.": st.column_config.NumberColumn("Θ Eff", format="%.2f", disabled=True),
                    "P&L": st.column_config.NumberColumn("P&L", format="$%d", disabled=True),
                    "Call P&L": st.column_config.NumberColumn("C-PnL", format="$%d", disabled=True, help="Profit/Loss from Call Legs"),
                    "Put P&L": st.column_config.NumberColumn("P-PnL", format="$%d", disabled=True, help="Profit/Loss from Put Legs"),
                    "P&L Vol": st.column_config.NumberColumn("Sleep Well (Vol)", format="$%d", disabled=True),
                    "Debit": st.column_config.NumberColumn("Debit", format="$%d", disabled=True),
                    "Notes": st.column_config.TextColumn("📝 Notes", width="large"),
                    "Tags": st.column_config.SelectboxColumn("🏷️ Tags", options=["Rolled", "Hedged", "Earnings", "High Risk", "Watch"], width="medium"),
                    "Parent ID": st.column_config.TextColumn("🔗 Link ID"),
                    "Action": st.column_config.TextColumn("Signal", disabled=True),
                }
                edited_df = st.data_editor(
                    active_df[display_cols],
                    column_config=column_config,
                    hide_index=True,
                    use_container_width=True,
                    key="journal_editor",
                    num_rows="fixed"
                )
                if st.button("💾 Save Journal"):
                    changes = update_journal(edited_df)
                    if changes: 
                        st.success(f"Saved {changes} trades!")
                        st.cache_data.clear()

            st.divider()

            # --- STRATEGY PERFORMANCE ---
            st.markdown("### 🏛️ Strategy Performance")
            strat_tabs = st.tabs(["📋 Overview", "🔹 130/160", "🔸 160/190", "🐳 M200", "💼 SMSF"])

            with strat_tabs[0]:
                with st.expander("📊 Portfolio Risk", expanded=True):
                    total_delta = active_df['Delta'].sum()
                    total_theta = active_df['Theta'].sum()
                    total_cap = active_df['Debit'].sum()
                    r1, r2, r3 = st.columns(3)
                    r1.metric("Net Delta", f"{total_delta:,.1f}", delta="Bullish" if total_delta > 0 else "Bearish")
                    r2.metric("Daily Theta", f"${total_theta:,.0f}")
                    r3.metric("Capital at Risk", f"${total_cap:,.0f}")

                strat_agg = active_df.groupby('Strategy').agg({
                    'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum', 'Delta': 'sum',
                    'Name': 'count', 'Daily Yield %': 'mean', 'Ann. ROI': 'mean', 'Theta Eff.': 'mean', 'P&L Vol': 'mean' 
                }).reset_index()
                
                strat_agg['Trend'] = strat_agg.apply(lambda r: "🟢 Improving" if r['Daily Yield %'] >= benchmarks.get(r['Strategy'], {}).get('yield', 0) else "🔴 Lagging", axis=1)
                strat_agg['Target %'] = strat_agg['Strategy'].apply(lambda x: benchmarks.get(x, {}).get('yield', 0))
                
                total_row = pd.DataFrame({
                    'Strategy': ['TOTAL'], 
                    'P&L': [strat_agg['P&L'].sum()], 'Debit': [strat_agg['Debit'].sum()],
                    'Theta': [strat_agg['Theta'].sum()], 'Delta': [strat_agg['Delta'].sum()],
                    'Name': [strat_agg['Name'].sum()], 
                    'Daily Yield %': [active_df['Daily Yield %'].mean()],
                    'Ann. ROI': [active_df['Ann. ROI'].mean()],
                    'Theta Eff.': [active_df['Theta Eff.'].mean()],
                    'P&L Vol': [active_df['P&L Vol'].mean()],
                    'Trend': ['-'], 'Target %': ['-']
                })
                final_agg = pd.concat([strat_agg, total_row], ignore_index=True)
                
                display_agg = final_agg[['Strategy', 'Trend', 'Daily Yield %', 'Ann. ROI', 'Theta Eff.', 'P&L Vol', 'Target %', 'P&L', 'Debit', 'Theta', 'Delta', 'Name']].copy()
                display_agg.columns = ['Strategy', 'Trend', 'Yield/Day', 'Ann. ROI', 'Θ Eff', 'Sleep Well (Vol)', 'Target', 'Total P&L', 'Total Debit', 'Net Theta', 'Net Delta', 'Count']
                
                def highlight_trend(val):
                    return 'color: green; font-weight: bold' if '🟢' in str(val) else 'color: red; font-weight: bold' if '🔴' in str(val) else ''

                def style_total(row):
                    return ['background-color: #d1d5db; color: black; font-weight: bold'] * len(row) if row['Strategy'] == 'TOTAL' else [''] * len(row)

                st.dataframe(
                    display_agg.style
                    .format({
                        'Total P&L': lambda x: safe_fmt(x, "${:,.0f}"), 
                        'Total Debit': lambda x: safe_fmt(x, "${:,.0f}"), 
                        'Net Theta': lambda x: safe_fmt(x, "{:,.0f}"), 
                        'Net Delta': lambda x: safe_fmt(x, "{:,.1f}"), 
                        'Yield/Day': lambda x: safe_fmt(x, "{:.2f}%"), 
                        'Ann. ROI': lambda x: safe_fmt(x, "{:.1f}%"), 
                        'Θ Eff': lambda x: safe_fmt(x, "{:.2f}"),
                        'Sleep Well (Vol)': lambda x: safe_fmt(x, "{:.1f}"),
                        'Target': lambda x: safe_fmt(x, "{:.2f}%")
                    })
                    .map(highlight_trend, subset=['Trend'])
                    .apply(style_total, axis=1), 
                    use_container_width=True
                )

            # STRATEGY TAB RENDERER
            cols = ['Name', 'Action', 'Grade', 'Theta/Cap %', 'Theta Eff.', 'P&L Vol', 'Daily Yield %', 'Ann. ROI', 'P&L', 'Call P&L', 'Put P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'Notes']
            
            def render_tab(tab, strategy_name):
                with tab:
                    subset = active_df[active_df['Strategy'] == strategy_name].copy()
                    bench = benchmarks.get(strategy_name) or BASE_CONFIG.get(strategy_name) or {'pnl': 0, 'yield': 0, 'dit': 0}
                    target_yield = bench.get('yield', 0)
                    target_disp = bench.get('pnl', 0) * regime_mult
                    
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Hist. Avg Win", f"${bench.get('pnl',0):,.0f}")
                    c2.metric("Target Yield", f"{bench.get('yield',0):.2f}%/d")
                    c3.metric("Target Profit", f"${target_disp:,.0f}")
                    c4.metric("Avg Hold", f"{bench.get('dit',0):.0f}d")
                    
                    if not subset.empty:
                        sum_row = pd.DataFrame({
                            'Name': ['TOTAL'], 'Action': ['-'], 'Grade': ['-'],
                            'Theta/Cap %': [subset['Theta/Cap %'].mean()],
                            'Daily Yield %': [subset['Daily Yield %'].mean()],
                            'Ann. ROI': [subset['Ann. ROI'].mean()],
                            'Theta Eff.': [subset['Theta Eff.'].mean()],
                            'P&L Vol': [subset['P&L Vol'].mean()],
                            'P&L': [subset['P&L'].sum()], 
                            'Call P&L': [subset['Call P&L'].sum()],
                            'Put P&L': [subset['Put P&L'].sum()],
                            'Debit': [subset['Debit'].sum()],
                            'Days Held': [subset['Days Held'].mean()],
                            'Theta': [subset['Theta'].sum()], 'Delta': [subset['Delta'].sum()],
                            'Gamma': [subset['Gamma'].sum()], 'Vega': [subset['Vega'].sum()], 'Notes': ['']
                        })
                        display_df = pd.concat([subset[cols], sum_row], ignore_index=True)
                        
                        def yield_color(val):
                            if isinstance(val, (int, float)):
                                if val < 0: return 'color: red; font-weight: bold'
                                if val >= target_yield * 0.8: return 'color: green; font-weight: bold' 
                                return 'color: orange; font-weight: bold' 
                            return ''

                        st.dataframe(
                            display_df.style
                            .format({
                                'Theta/Cap %': "{:.2f}%", 
                                'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 
                                'Call P&L': "${:,.0f}", 'Put P&L': "${:,.0f}",
                                'Daily Yield %': "{:.2f}%", 'Ann. ROI': "{:.1f}%", 
                                'Theta Eff.': "{:.2f}", 'P&L Vol': "{:.1f}",
                                'Theta': "{:.1f}", 'Delta': "{:.1f}", 'Gamma': "{:.2f}", 'Vega': "{:.0f}", 
                                'Days Held': "{:.0f}"
                            })
                            .map(lambda v: 'background-color: #d1e7dd; color: #0f5132; font-weight: bold' if 'TAKE PROFIT' in str(v) else ('background-color: #f8d7da; color: #842029; font-weight: bold' if 'KILL' in str(v) or 'MISSING' in str(v) else ('background-color: #fff3cd; color: #856404; font-weight: bold' if 'WATCH' in str(v) else ('background-color: #cff4fc; color: #055160; font-weight: bold' if 'COOKING' in str(v) else ''))), subset=['Action'])
                            .map(lambda v: 'color: #0f5132; font-weight: bold' if 'A' in str(v) else ('color: #842029; font-weight: bold' if 'F' in str(v) else 'color: #d97706; font-weight: bold'), subset=['Grade'])
                            .map(lambda v: 'color: green; font-weight: bold' if isinstance(v, (int, float)) and v > 0 else ('color: red; font-weight: bold' if isinstance(v, (int, float)) and v < 0 else ''), subset=['P&L', 'Call P&L', 'Put P&L'])
                            .map(yield_color, subset=['Daily Yield %'])
                            .map(lambda v: 'color: #8b0000; font-weight: bold' if isinstance(v, (int, float)) and v > 45 else '', subset=['Days Held'])
                            .map(lambda v: 'background-color: #ffcccb; color: #8b0000; font-weight: bold' if isinstance(v, (int, float)) and v < 0.1 else ('background-color: #d1e7dd; color: #0f5132; font-weight: bold' if isinstance(v, (int, float)) and v > 0.2 else ''), subset=['Theta/Cap %'])
                            .apply(lambda x: ['background-color: #d1d5db; color: black; font-weight: bold' if x.name == len(display_df)-1 else '' for _ in x], axis=1), 
                            use_container_width=True
                        )
                    else: st.info("No active trades.")

            render_tab(strat_tabs[1], '130/160')
            render_tab(strat_tabs[2], '160/190')
            render_tab(strat_tabs[3], 'M200')
            render_tab(strat_tabs[4], 'SMSF')

    else:
        st.info("👋 Database is empty. Sync your first file.")

# 2. VALIDATOR
with tab2:
    st.markdown("### 🧪 Pre-Flight Audit")
    with st.expander("ℹ️ Grading System Legend", expanded=True):
        st.markdown("""
        | Strategy | Grade | Debit Range (Per Lot) | Verdict |
        | :--- | :--- | :--- | :--- |
        | **130/160** | **A+** | `$3,500 - $4,500` | ✅ **Sweet Spot** |
        | **130/160** | **B** | `< $3,500` or `$4,500-$4,800` | ⚠️ **Acceptable** |
        | **130/160** | **F** | `> $4,800` | ⛔ **Overpriced** |
        | **160/190** | **A** | `$4,800 - $5,500` | ✅ **Ideal** Pricing |
        | **160/190** | **C** | `> $5,500` | ⚠️ **Expensive** |
        | **M200** | **A** | `$7,500 - $8,500` | ✅ **Perfect** "Whale" sizing |
        | **M200** | **B** | Any other price | ⚠️ **Variance** |
        | **SMSF** | **A** | `< $15,000` | ✅ **Standard** |
        """)
        
    model_file = st.file_uploader("Upload Model File", key="mod")
    if model_file:
        try:
            trades = read_and_parse_optionstrat(model_file)
            if trades:
                t = trades[0] # Just analyze the first one found
                name, strat, debit = t['name'], t['strategy'], t['debit']
                lot_size = t['lot_size']
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
                elif strat == 'SMSF':
                    if debit_lot > 15000: grade, reason = "B", "High Debit" 
                    else: grade, reason = "A", "Standard"

                st.divider()
                st.subheader(f"Audit: {name}")
                c1, c2, c3 = st.columns(3)
                c1.metric("Strategy", strat)
                c2.metric("Total Debit", f"${debit:,.0f}")
                c3.metric("Per Lot", f"${debit_lot:,.0f}")
                
                if "A" in grade: st.success(f"✅ **APPROVED:** {reason}")
                elif "F" in grade: st.error(f"⛔ **REJECT:** {reason}")
                else: st.warning(f"⚠️ **CHECK:** {reason}")
        except Exception as e: st.error(f"Error: {e}")

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
        
        st.markdown("### 📊 Performance Ledger")
        realized_pnl = expired_sub['P&L'].sum()
        floating_pnl = df[df['Status'] == 'Active']['P&L'].sum()
        total_pnl = realized_pnl + floating_pnl
        
        m1, m2, m3 = st.columns(3)
        m1.metric("💰 Banked Profit", f"${realized_pnl:,.0f}")
        m2.metric("📄 Floating Profit", f"${floating_pnl:,.0f}", delta_color="normal")
        m3.metric("🔮 Projected Total", f"${total_pnl:,.0f}")
        
        st.divider()
        
        an1, an2, an3, an4, an5, an6, an7, an8, an9, an10, an11 = st.tabs([
            "🌊 Equity", "🎯 Expectancy", "🔥 Heatmaps", "🏷️ Tickers", 
            "⚠️ Risk", "🧬 Lifecycle", "🧮 Greeks Lab", "⚙️ Velocity", 
            "🎯 Compliance", "🧱 Gamma Wall", "🕵️ P&L Drivers"
        ])

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
                
                # Metrics
                win_rate = len(wins) / len(expired_sub) * 100 if len(expired_sub) > 0 else 0
                profit_factor = (wins['P&L'].sum() / abs(losses['P&L'].sum())) if abs(losses['P&L'].sum()) > 0 else 0
                
                c1, c2 = st.columns(2)
                c1.metric("Win Rate", f"{win_rate:.1f}%")
                c2.metric("Profit Factor", f"{profit_factor:.2f}")

                # Call vs Put Split Visualization
                st.markdown("##### 🎭 Call vs Put Contribution (Closed Trades)")
                # Aggregating contributions
                split_df = expired_sub[['Strategy', 'Call P&L', 'Put P&L']].groupby('Strategy').sum().reset_index()
                split_melted = split_df.melt(id_vars='Strategy', value_vars=['Call P&L', 'Put P&L'], var_name='Side', value_name='P&L')
                
                # Safe fallback for Empty/Invisible Graph: Ensure P&L is numeric and filter 0s if needed
                split_melted['P&L'] = pd.to_numeric(split_melted['P&L'], errors='coerce').fillna(0)
                split_melted = split_melted[split_melted['P&L'] != 0] # Hide flat bars
                
                if not split_melted.empty:
                    fig_split = px.bar(split_melted, x='Strategy', y='P&L', color='Side', barmode='group', 
                                       title="Profit Source by Strategy (Calls vs Puts)", color_discrete_map={'Call P&L': 'green', 'Put P&L': 'red'})
                    st.plotly_chart(fig_split, use_container_width=True)
                else:
                    st.info("No P&L attribution data available for closed trades yet.")

        with an3:
            if not expired_sub.empty:
                exp_hm = expired_sub.dropna(subset=['Exit Date']).copy()
                exp_hm['Month'] = exp_hm['Exit Date'].dt.month_name()
                exp_hm['Year'] = exp_hm['Exit Date'].dt.year
                hm_data = exp_hm.groupby(['Year', 'Month'])['P&L'].sum().reset_index()
                months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                fig = px.density_heatmap(hm_data, x="Month", y="Year", z="P&L", title="Monthly Seasonality ($)", category_orders={"Month": months}, color_continuous_scale="RdBu")
                st.plotly_chart(fig, use_container_width=True)

        with an4:
            if not expired_sub.empty:
                tick_grp = expired_sub.groupby('Ticker')['P&L'].sum().reset_index().sort_values('P&L', ascending=False)
                fig = px.bar(tick_grp.head(15), x='P&L', y='Ticker', orientation='h', color='P&L', color_continuous_scale="RdBu", title="Top Performing Tickers")
                st.plotly_chart(fig, use_container_width=True)

        with an5:
            active_only = df[df['Status'] == 'Active']
            if not active_only.empty:
                c1, c2 = st.columns(2)
                with c1:
                    fig_strat = px.pie(active_only, values='Debit', names='Strategy', title="Risk by Strategy (Capital)", hole=0.4)
                    st.plotly_chart(fig_strat, use_container_width=True)
                with c2:
                    if 'P&L Vol' in active_only.columns:
                        fig_vol = px.bar(active_only.sort_values('P&L Vol', ascending=False).head(10), x='P&L Vol', y='Name', orientation='h', title="Top 10 High Stress Trades", color='Strategy')
                        st.plotly_chart(fig_vol, use_container_width=True)

        with an6:
            snaps = load_snapshots()
            if not snaps.empty:
                sel_strat = st.selectbox("Select Strategy", snaps['strategy'].unique(), key="life_strat")
                strat_snaps = snaps[snaps['strategy'] == sel_strat]
                fig = px.line(strat_snaps, x='days_held', y='pnl', color='name', line_group='id', title=f"Trade Lifecycle: {sel_strat}", markers=True)
                st.plotly_chart(fig, use_container_width=True)

        with an7:
            if not df.empty:
                g_col = st.selectbox("Select Greek", ['Theta', 'Delta', 'Gamma', 'Vega'], key="greek_dd")
                valid_greeks = df[df[g_col] != 0]
                if not valid_greeks.empty:
                    fig = px.scatter(valid_greeks, x=g_col, y='P&L', color='Strategy', title=f"Correlation: {g_col} vs P&L")
                    st.plotly_chart(fig, use_container_width=True)

        with an8:
            active_only = df[df['Status'] == 'Active'].copy()
            if not active_only.empty:
                fig = px.scatter(active_only, x='Days Held', y='Daily Yield %', size='Debit', color='Strategy', title="Velocity Map")
                st.plotly_chart(fig, use_container_width=True)

        with an9:
            if not df.empty:
                score_data = []
                for strat, rules in BASE_CONFIG.items():
                    strat_trades = df[df['Strategy'] == strat]
                    if strat_trades.empty: continue
                    total = len(strat_trades)
                    # Check for valid dates before property access
                    if pd.api.types.is_datetime64_any_dtype(strat_trades['Entry Date']):
                         strat_trades['DayOfWeek'] = strat_trades['Entry Date'].dt.dayofweek
                         valid_days = rules.get('target_days', [])
                         on_time = strat_trades[strat_trades['DayOfWeek'].isin(valid_days)]
                         day_score = (len(on_time) / total) * 100
                    else:
                         day_score = 0
                         
                    min_d, max_d = rules.get('target_debit_min', 0), rules.get('target_debit_max', 999999)
                    in_range = strat_trades[strat_trades['Debit/Lot'].between(min_d, max_d)]
                    cost_score = (len(in_range) / total) * 100
                    score_data.append({'Strategy': strat, 'Trades': total, 'Entry Day Match %': f"{day_score:.1f}%", 'Cost Target Match %': f"{cost_score:.1f}%"})
                st.dataframe(pd.DataFrame(score_data), use_container_width=True)

        with an10:
            active_only = df[df['Status'] == 'Active'].copy()
            if not active_only.empty:
                gamma_wall = active_only.groupby('Exit Date')['Debit'].sum().reset_index()
                fig = px.bar(gamma_wall, x='Exit Date', y='Debit', title="Capital Concentration by Expiration", color='Debit', color_continuous_scale='Reds')
                st.plotly_chart(fig, use_container_width=True)
                
        # --- NEW TAB: P&L DRIVERS ---
        with an11:
            st.markdown("### 🕵️ P&L Forensics: What drives your profit?")
            st.caption("Correlation of 1.0 means P&L moves perfectly with the Greek. -1.0 means opposite.")
            
            active_trades = df[df['Status'] == 'Active'].copy()
            if not active_trades.empty:
                c1, c2 = st.columns([1, 2])
                with c1:
                    st.markdown("#### 🔢 Correlation Coefficients")
                    drivers = get_pnl_drivers(active_trades)
                    if drivers is not None:
                        # Fix: Use native Streamlit column_config instead of pandas styling which requires matplotlib
                        st.dataframe(
                            drivers,
                            column_config={
                                "Correlation": st.column_config.NumberColumn(
                                    "Correlation",
                                    format="%.2f",
                                    help="Values close to 1.0 or -1.0 indicate strong drivers."
                                )
                            },
                            use_container_width=True
                        )
                    else:
                        st.warning("Not enough data points yet.")
                
                with c2:
                    st.markdown("#### 📉 Visual Driver Analysis")
                    drv_sel = st.selectbox("Compare P&L against:", ['Vega', 'Delta', 'Theta', 'Gamma'], index=0)
                    
                    # Fix: Safe Trendline (Only use OLS if statsmodels is installed)
                    try:
                        import statsmodels
                        trend_arg = "ols"
                    except ImportError:
                        trend_arg = None
                        
                    fig_drv = px.scatter(active_trades, x=drv_sel, y='P&L', color='Strategy', size='Debit', hover_data=['Name'], title=f"Impact of {drv_sel} on P&L (Active)", trendline=trend_arg)
                    st.plotly_chart(fig_drv, use_container_width=True)
                    if trend_arg is None:
                        st.caption("ℹ️ Install `statsmodels` to see the trendline.")
                    
                st.divider()
                st.markdown("#### 🎭 Active Trade Attribution (Call vs Put)")
                # Prepare data for attribution
                act_split = active_trades[['Name', 'Call P&L', 'Put P&L']].copy()
                # Filter out trades with no attribution data to avoid clutter
                act_split = act_split[(act_split['Call P&L'] != 0) | (act_split['Put P&L'] != 0)]
                
                if not act_split.empty:
                    act_split_melted = act_split.melt(id_vars=['Name'], value_vars=['Call P&L', 'Put P&L'], var_name='Side', value_name='Contribution')
                    fig_act_split = px.bar(act_split_melted, x='Name', y='Contribution', color='Side', title="PnL Source Breakdown per Trade", color_discrete_map={'Call P&L': 'green', 'Put P&L': 'red'})
                    st.plotly_chart(fig_act_split, use_container_width=True)
                else:
                    st.info("No leg data available for active trades.")
            else:
                st.info("No active trades to analyze.")

# 4. RULE BOOK
with tab4:
    st.markdown("""
    # 📖 The Trader's Constitution
    *Refined by Data Audit & Behavioral Analysis*

    ### 1. 130/160 Strategy (Income Discipline)
    * **Role:** Income Engine. Extracts time decay (Theta).
    * **Entry:** Monday/Tuesday (Best liquidity/IV fit).
    * **Debit Target:** `$3,500 - $4,500` per lot.
        * *Stop Rule:* Never pay > `$4,800` per lot.
    * **Management:** **Time Limit Rule.**
        * Kill if trade is **25 days old** and P&L is flat/negative.
        * *Why?* Data shows convexity diminishes after Day 21. It's a decay trade, not a patience trade.
    * **Efficiency Check:** ROI-focused. Requires high velocity.
    
    ### 2. 160/190 Strategy (Patience Training)
    * **Role:** Compounder. Expectancy focused.
    * **Entry:** Friday (Captures weekend decay start).
    * **Debit Target:** `~$5,200` per lot.
    * **Sizing:** Trade **1 Lot**.
    * **Exit:** Hold for **40-50 Days**. 
    * **Golden Rule:** **Do not touch in first 30 days.** Early interference statistically worsens outcomes.
    
    ### 3. M200 Strategy (Emotional Mastery)
    * **Role:** Whale. Variance-tolerant capital deployment.
    * **Entry:** Wednesday.
    * **Debit Target:** `$7,500 - $8,500` per lot.
    * **The "Dip Valley":**
        * P&L often looks worst between Day 15–40. This is structural.
        * **Management:** Check at **Day 14**.
            * Check **Greeks & VIX**, not just P&L.
            * If Red/Flat: **HOLD.** Do not panic exit in the Valley. Wait for volatility to revert.
            
    ### 4. SMSF Strategy (Wealth Builder)
    * **Role:** Long-term Growth.
    * **Structure:** Multi-trade portfolio strategy.
    
    ---
    ### 🛡️ Universal Execution Gates
    1.  **Volatility Gate:** Check VIX before entry. Ideal: 14–22. Skip if VIX exploded >10% in last 48h.
    2.  **Loss Definition:** A trade that is early and red but *structurally intact* is **NOT** a losing trade. It is just *unripe*.
    3.  **Efficiency Check:** Monitor **Theta Eff.** (> 1.0 means you are capturing decay efficiently).
    """)
    st.caption("Allantis Trade Guardian v91.1 | Deep Analytics")
