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
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- DEBUG BANNER ---
st.info("✅ RUNNING VERSION: v109.0 (PnL Life Cycle Graph Restored)")

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
                    parent_id TEXT,
                    put_pnl REAL,
                    call_pnl REAL,
                    iv REAL,
                    link TEXT
                )''')
    
    # SNAPSHOTS TABLE
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT,
                    snapshot_date DATE,
                    pnl REAL,
                    days_held INTEGER,
                    theta REAL,
                    delta REAL,
                    vega REAL,
                    FOREIGN KEY(trade_id) REFERENCES trades(id)
                )''')
    
    try: c.execute("ALTER TABLE snapshots ADD COLUMN theta REAL")
    except: pass
    try: c.execute("ALTER TABLE snapshots ADD COLUMN delta REAL")
    except: pass
    try: c.execute("ALTER TABLE snapshots ADD COLUMN vega REAL")
    except: pass
                
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
    conn.commit()
    conn.close()
    migrate_db()

def migrate_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    cols = [
        ('tags', 'TEXT'), 
        ('parent_id', 'TEXT'), 
        ('put_pnl', 'REAL'), 
        ('call_pnl', 'REAL'), 
        ('iv', 'REAL'),
        ('link', 'TEXT')
    ]
    for col_name, col_type in cols:
        try: c.execute(f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}")
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

# VIX Context
VIX_CONTEXT = {
    '2024-Q3': 14.2,
    '2024-Q4': 16.8,
    '2025-Q1': 15.3,
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
        if pd.isna(x) or str(x).strip() == "": return 0.0
        val_str = str(x).replace('$', '').replace(',', '').replace('%', '').strip()
        val = float(val_str)
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
    safe_name = re.sub(r'\W+', '', str(name))
    return f"{safe_name}_{strategy}_{d_str}"

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

# --- SMART FILE PARSER ---
def parse_optionstrat_file(file, file_type):
    try:
        df_raw = None
        if file.name.endswith(('.xlsx', '.xls')):
            try:
                df_temp = pd.read_excel(file, header=None)
                header_row = 0
                for i, row in df_temp.head(30).iterrows():
                    row_vals = [str(v).strip() for v in row.values]
                    if "Name" in row_vals and "Total Return $" in row_vals:
                        header_row = i
                        break
                file.seek(0)
                df_raw = pd.read_excel(file, header=header_row)
                
                if 'Link' in df_raw.columns:
                    try:
                        file.seek(0)
                        wb = load_workbook(file, data_only=False)
                        sheet = wb.active
                        excel_header_row = header_row + 1
                        link_col_idx = None
                        for cell in sheet[excel_header_row]:
                            if str(cell.value).strip() == "Link":
                                link_col_idx = cell.col_idx
                                break
                        if link_col_idx:
                            links = []
                            for i in range(len(df_raw)):
                                excel_row_idx = excel_header_row + 1 + i
                                cell = sheet.cell(row=excel_row_idx, column=link_col_idx)
                                url = ""
                                if cell.hyperlink: url = cell.hyperlink.target
                                elif cell.value and str(cell.value).startswith('=HYPERLINK'):
                                    try:
                                        parts = str(cell.value).split('"')
                                        if len(parts) > 1: url = parts[1]
                                    except: pass
                                links.append(url if url else "")
                            df_raw['Link'] = links
                    except: pass
            except: pass

        if df_raw is None:
            file.seek(0)
            content = file.getvalue().decode("utf-8", errors='ignore')
            lines = content.split('\n')
            header_row = 0
            for i, line in enumerate(lines[:30]):
                if "Name" in line and "Total Return" in line:
                    header_row = i
                    break
            file.seek(0)
            df_raw = pd.read_csv(file, skiprows=header_row)

        parsed_trades = []
        current_trade = None
        current_legs = []

        def finalize_trade(trade_data, legs, f_type):
            if not trade_data.any(): return None
            
            name = str(trade_data.get('Name', ''))
            created = trade_data.get('Created At', '')
            try: start_dt = pd.to_datetime(created)
            except: return None 

            group = str(trade_data.get('Group', ''))
            strat = get_strategy(group, name)
            link = str(trade_data.get('Link', ''))
            if link == 'nan' or link == 'Open': link = "" 
            
            pnl = clean_num(trade_data.get('Total Return $', 0))
            debit = abs(clean_num(trade_data.get('Net Debit/Credit', 0)))
            theta = clean_num(trade_data.get('Theta', 0))
            delta = clean_num(trade_data.get('Delta', 0))
            gamma = clean_num(trade_data.get('Gamma', 0))
            vega = clean_num(trade_data.get('Vega', 0))
            iv = clean_num(trade_data.get('IV', 0))

            exit_dt = None
            try:
                raw_exp = trade_data.get('Expiration')
                if pd.notnull(raw_exp) and str(raw_exp).strip() != '':
                    exit_dt = pd.to_datetime(raw_exp)
            except: pass

            days_held = 1
            if exit_dt and f_type == "History":
                  days_held = (exit_dt - start_dt).days
            else:
                  days_held = (datetime.now() - start_dt).days
            if days_held < 1: days_held = 1

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

            put_pnl = 0.0
            call_pnl = 0.0
            
            if f_type == "History":
                for leg in legs:
                    if len(leg) < 5: continue
                    sym = str(leg.iloc[0]) 
                    if not sym.startswith('.'): continue
                    try:
                        qty = clean_num(leg.iloc[1])
                        entry = clean_num(leg.iloc[2])
                        close_price = clean_num(leg.iloc[4])
                        leg_pnl = (close_price - entry) * qty * 100
                        if 'P' in sym and 'C' not in sym: put_pnl += leg_pnl
                        elif 'C' in sym and 'P' not in sym: call_pnl += leg_pnl
                        elif re.search(r'[0-9]P[0-9]', sym): put_pnl += leg_pnl
                        elif re.search(r'[0-9]C[0-9]', sym): call_pnl += leg_pnl
                    except: pass
            
            t_id = generate_id(name, strat, start_dt)
            return {
                'id': t_id, 'name': name, 'strategy': strat, 'start_dt': start_dt,
                'exit_dt': exit_dt, 'days_held': days_held, 'debit': debit,
                'lot_size': lot_size, 'pnl': pnl, 
                'theta': theta, 'delta': delta, 'gamma': gamma, 'vega': vega,
                'iv': iv, 'put_pnl': put_pnl, 'call_pnl': call_pnl, 'link': link
            }

        cols = df_raw.columns
        col_names = [str(c) for c in cols]
        if 'Name' not in col_names or 'Total Return $' not in col_names:
            return []

        for index, row in df_raw.iterrows():
            name_val = str(row['Name'])
            if name_val and not name_val.startswith('.') and name_val != 'Symbol' and name_val != 'nan':
                if current_trade is not None:
                    res = finalize_trade(current_trade, current_legs, file_type)
                    if res: parsed_trades.append(res)
                current_trade = row
                current_legs = []
            elif name_val.startswith('.'):
                current_legs.append(row)
        
        if current_trade is not None:
             res = finalize_trade(current_trade, current_legs, file_type)
             if res: parsed_trades.append(res)
        return parsed_trades
    except Exception as e:
        print(f"Parser Error: {e}")
        return []

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
            trades_data = parse_optionstrat_file(file, file_type)
            if not trades_data:
                log.append(f"⚠️ {file.name}: Skipped (No valid trades found)")
                continue

            for t in trades_data:
                trade_id = t['id']
                if file_type == "Active":
                    file_found_ids.add(trade_id)
                
                c.execute("SELECT status, theta, delta, gamma, vega, put_pnl, call_pnl, iv, link FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                status = "Active" if file_type == "Active" else "Expired"
                
                if existing is None:
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, notes, tags, parent_id, put_pnl, call_pnl, iv, link)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, t['name'], t['strategy'], status, t['start_dt'].date(), 
                         t['exit_dt'].date() if t['exit_dt'] else None, 
                         t['days_held'], t['debit'], t['lot_size'], t['pnl'], 
                         t['theta'], t['delta'], t['gamma'], t['vega'], "", "", "", t['put_pnl'], t['call_pnl'], t['iv'], t['link']))
                    count_new += 1
                else:
                    old_status, old_theta, old_delta, old_gamma, old_vega, old_put, old_call, old_iv, old_link = existing
                    old_put = old_put if old_put else 0.0
                    old_call = old_call if old_call else 0.0
                    old_iv = old_iv if old_iv else 0.0
                    old_link = old_link if old_link else ""

                    final_theta = t['theta'] if t['theta'] != 0 else old_theta
                    final_delta = t['delta'] if t['delta'] != 0 else old_delta
                    final_gamma = t['gamma'] if t['gamma'] != 0 else old_gamma
                    final_vega = t['vega'] if t['vega'] != 0 else old_vega
                    final_iv = t['iv'] if t['iv'] != 0 else old_iv
                    final_put = t['put_pnl'] if t['put_pnl'] != 0 else old_put
                    final_call = t['call_pnl'] if t['call_pnl'] != 0 else old_call
                    final_link = t['link'] if t['link'] != "" else old_link

                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, put_pnl=?, call_pnl=?, iv=?, link=?
                            WHERE id=?''', 
                            (t['pnl'], status, t['exit_dt'].date() if t['exit_dt'] else None, t['days_held'], 
                             final_theta, final_delta, final_gamma, final_vega, final_put, final_call, final_iv, final_link, trade_id))
                        count_update += 1
                    elif old_status in ["Active", "Missing"]: 
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, iv=?, link=?, status='Active', exit_date=?
                            WHERE id=?''', 
                            (t['pnl'], t['days_held'], final_theta, final_delta, final_gamma, final_vega, final_iv, final_link, 
                             t['exit_dt'].date() if t['exit_dt'] else None, trade_id))
                        count_update += 1
                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    
                    theta_val = t['theta'] if t['theta'] else 0.0
                    delta_val = t['delta'] if t['delta'] else 0.0
                    vega_val = t['vega'] if t['vega'] else 0.0
                    
                    if not c.fetchone():
                        c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega) VALUES (?,?,?,?,?,?,?)",
                                  (trade_id, today, t['pnl'], t['days_held'], theta_val, delta_val, vega_val))
                    else:
                        c.execute("UPDATE snapshots SET theta=?, delta=?, vega=? WHERE trade_id=? AND snapshot_date=?",
                                  (theta_val, delta_val, vega_val, trade_id, today))
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
        else: df['P&L Vol'] = 0.0
    except Exception as e: return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes',
            'tags': 'Tags', 'parent_id': 'Parent ID', 
            'put_pnl': 'Put P&L', 'call_pnl': 'Call P&L', 'iv': 'IV', 'link': 'Link'
        })
        
        required_cols = ['Gamma', 'Vega', 'Theta', 'Delta', 'P&L', 'Debit', 'lot_size', 'Notes', 'Tags', 'Parent ID', 'Put P&L', 'Call P&L', 'IV', 'Link']
        for col in required_cols:
            if col not in df.columns: df[col] = "" if col in ['Notes', 'Tags', 'Parent ID', 'Link'] else 0.0
        
        numeric_cols = ['Debit', 'P&L', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'IV', 'Put P&L', 'Call P&L']
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)

        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        df['Debit/Lot'] = df['Debit'] / df['lot_size'].replace(0, 1)
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
                if 7500 <= d <= 8500: grade, reason = "A", "Perfect Entry"
                else: grade, reason = "B", "Variance"
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
        # UPDATED QUERY: Join to get initial trade theta for expected curve calculation
        q = """
        SELECT s.snapshot_date, s.pnl, s.days_held, s.theta, s.delta, s.vega, 
               t.strategy, t.name, t.id, t.theta as initial_theta
        FROM snapshots s
        JOIN trades t ON s.trade_id = t.id
        """
        df = pd.read_sql(q, conn)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        df['pnl'] = pd.to_numeric(df['pnl'], errors='coerce').fillna(0)
        df['days_held'] = pd.to_numeric(df['days_held'], errors='coerce').fillna(0)
        df['theta'] = pd.to_numeric(df['theta'], errors='coerce').fillna(0)
        df['delta'] = pd.to_numeric(df['delta'], errors='coerce').fillna(0)
        df['vega'] = pd.to_numeric(df['vega'], errors='coerce').fillna(0)
        df['initial_theta'] = pd.to_numeric(df['initial_theta'], errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- HELPER: FIND SIMILAR TRADES ---
def find_similar_trades(current_trade, historical_df, top_n=3):
    if historical_df.empty:
        return pd.DataFrame()
    features = ['Theta/Cap %', 'Delta', 'Debit/Lot']
    for f in features:
        if f not in current_trade or f not in historical_df.columns:
            return pd.DataFrame()
    curr_vec = np.nan_to_num(current_trade[features].values.astype(float)).reshape(1, -1)
    hist_vecs = np.nan_to_num(historical_df[features].values.astype(float))
    distances = cdist(curr_vec, hist_vecs, metric='euclidean')[0]
    similar_idx = np.argsort(distances)[:top_n]
    similar = historical_df.iloc[similar_idx].copy()
    max_dist = distances.max() if distances.max() > 0 else 1
    similar['Similarity %'] = 100 * (1 - distances[similar_idx] / max_dist)
    return similar[['Name', 'P&L', 'Days Held', 'ROI', 'Similarity %']]

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
        if 'restored' not in st.session_state:
            st.session_state['restored'] = True
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
        st.download_button("💾 Save Database File", f, "trade_guardian_v4.db", "application/x-sqlite3")

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

def calc_exit_score(row, benchmarks):
    score = 0
    strat = row['Strategy']
    bench = benchmarks.get(strat, BASE_CONFIG.get(strat, {}))
    target = bench.get('pnl', 1000) * regime_mult
    
    # Profit progress (0-50 points)
    if row['P&L'] >= target: 
        score += 50
    elif row['P&L'] >= target * 0.8: 
        score += 30 + (row['P&L'] / target) * 20
    
    # Time factor (0-30 points)
    avg_days = bench.get('dit', 40)
    if row['Days Held'] > avg_days * 1.5: 
        score += 30
    elif row['Days Held'] > avg_days * 1.2: 
        score += 15
    if strat == '130/160' and row['Days Held'] > 25: score += 30
    
    # Efficiency (0-20 points)
    if row['Theta Eff.'] < 0.5: score += 20
    elif row['Theta Eff.'] < 0.8: score += 10
    
    return min(100, max(0, score))

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

# --- TABS ---
tab_dash, tab_analytics, tab_rules = st.tabs(["📊 Dashboard", "📈 Analytics", "📖 Rules"])

# 1. ACTIVE DASHBOARD
with tab_dash:
    if not df.empty:
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        
        if active_df.empty:
            st.info("📭 No active trades.")
        else:
            tot_theta = active_df['Theta'].sum()
            tot_debit = active_df['Debit'].sum()
            eff_score = (tot_theta / tot_debit * 100) if tot_debit > 0 else 0
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Daily Theta Income", f"${tot_theta:,.0f}")
            c2.metric("Portfolio Yield (Theta/Cap)", f"{eff_score:.2f}%", help="How hard is your capital working? Higher is better.")
            c3.metric("Floating PnL", f"${active_df['P&L'].sum():,.0f}")
            
            # Capital Velocity
            target_days = benchmarks.get('130/160', {}).get('dit', 36)
            c4.metric("Capital Velocity", f"{active_df['Days Held'].mean():.0f} days avg", help="Lower = faster capital recycling", delta=f"Target: {target_days:.0f}d")
            
            # Stale Capital Warning
            stale_capital = active_df[active_df['Days Held'] > 40]['Debit'].sum()
            if stale_capital > tot_debit * 0.3:
                 st.warning(f"⚠️ ${stale_capital:,.0f} stuck in trades >40 days old. Consider exits.")

            st.divider()

            act_list, sig_list = [], []
            score_list = []
            for _, row in active_df.iterrows():
                act, sig = get_action_signal(row['Strategy'], row['Status'], row['Days Held'], row['P&L'], benchmarks)
                score = calc_exit_score(row, benchmarks)
                act_list.append(act)
                sig_list.append(sig)
                score_list.append(score)
                
            active_df['Action'] = act_list
            active_df['Signal_Type'] = sig_list
            active_df['Exit Score'] = score_list

            todo_df = active_df[active_df['Action'] != ""]
            with st.expander(f"✅ Action Queue ({len(todo_df)})", expanded=False):
                if not todo_df.empty:
                    t1, t2 = st.columns([3, 1])
                    with t1:
                        for _, row in todo_df.iterrows():
                            sig = row['Signal_Type']
                            color = {"SUCCESS":"green", "ERROR":"red", "WARNING":"orange", "INFO":"blue"}.get(sig, "grey")
                            is_valid_link = str(row['Link']).startswith('http')
                            name_display = f"[{row['Name']}]({row['Link']})" if is_valid_link else row['Name']
                            st.markdown(f"**{name_display}**: :{color}[{row['Action']}] (Score: {row['Exit Score']}) | *{row['Strategy']}*")
                    with t2:
                        csv_todo = todo_df[['Name', 'Action', 'P&L', 'Days Held']].to_csv(index=False).encode('utf-8')
                        st.download_button("📥 Download Queue", csv_todo, "todo_list.csv", "text/csv")
                else:
                    st.info("No actions required.")

            st.divider()

            sub_journal, sub_strat = st.tabs(["📝 Journal & Overview", "🏛️ Strategy Detail"])

            with sub_journal:
                st.caption("Edit 'Notes', 'Tags' or 'Parent ID' (for Linking).")
                display_cols = ['id', 'Name', 'Link', 'Strategy', 'Exit Score', 'Status', 'Theta/Cap %', 'Theta Eff.', 'P&L', 'P&L Vol', 'Debit', 'Days Held', 'Notes', 'Tags', 'Parent ID', 'Action']
                column_config = {
                    "id": None, 
                    "Name": st.column_config.TextColumn("Trade Name", disabled=True),
                    "Link": st.column_config.LinkColumn("OS Link", display_text="Open 🔗"),
                    "Strategy": st.column_config.TextColumn("Strat", disabled=True, width="small"),
                    "Status": st.column_config.TextColumn("Status", disabled=True, width="small"),
                    "Exit Score": st.column_config.ProgressColumn("Exit Urgency", min_value=0, max_value=100, format="%d"),
                    "Theta/Cap %": st.column_config.NumberColumn("Θ/Cap", format="%.2f%%", disabled=True),
                    "Theta Eff.": st.column_config.NumberColumn("Θ Eff", format="%.2f", disabled=True, help="Ratio of P&L to Total Theta Potential. >1.0 is excellent."),
                    "P&L": st.column_config.NumberColumn("P&L", format="$%d", disabled=True),
                    "P&L Vol": st.column_config.NumberColumn("Sleep Well (Vol)", format="$%d", disabled=True, help="Standard Deviation of Daily P&L. Lower is better."),
                    "Debit": st.column_config.NumberColumn("Debit", format="$%d", disabled=True),
                    "Notes": st.column_config.TextColumn("📝 Notes", width="large"),
                    "Tags": st.column_config.SelectboxColumn("🏷️ Tags", options=["Rolled", "Hedged", "Earnings", "High Risk", "Watch"], width="medium"),
                    "Parent ID": st.column_config.TextColumn("🔗 Link ID", help="Paste ID of previous leg to link campaigns."),
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
                
                with st.expander("🧬 Trade DNA Fingerprinting (Find Similar)", expanded=False):
                    if not expired_df.empty:
                        selected_dna_trade = st.selectbox("Select Active Trade to Analyze", active_df['Name'].unique())
                        curr_row = active_df[active_df['Name'] == selected_dna_trade].iloc[0]
                        similar = find_similar_trades(curr_row, expired_df)
                        if not similar.empty:
                            best_match = similar.iloc[0]
                            st.info(f"🎯 **Best Match:** {best_match['Name']} ({best_match['Similarity %']:.0f}% similar) → Made ${best_match['P&L']:,.0f} in {best_match['Days Held']:.0f} days")
                            st.write(f"Most similar historical trades to **{selected_dna_trade}**:")
                            st.dataframe(similar.style.format({'P&L': '${:,.0f}', 'ROI': '{:.1f}%', 'Similarity %': '{:.0f}%'}))
                            avg_outcome = similar['P&L'].mean()
                            st.metric("Expected Outcome (Based on Similar)", f"${avg_outcome:,.0f}")
                        else:
                            st.info("No similar historical trades found.")
                    else:
                        st.info("Need closed trade history for DNA analysis.")

            with sub_strat:
                st.markdown("### 🏛️ Strategy Performance")
                strat_tabs_inner = st.tabs(["📋 Overview", "🔹 130/160", "🔸 160/190", "🐳 M200", "💼 SMSF"])

                with strat_tabs_inner[0]:
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

                cols = ['Name', 'Link', 'Action', 'Exit Score', 'Grade', 'Theta/Cap %', 'Theta Eff.', 'P&L Vol', 'Daily Yield %', 'Ann. ROI', 'P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'Notes']
                
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
                                'Name': ['TOTAL'], 'Link': [''], 'Action': ['-'], 'Exit Score': [0], 'Grade': ['-'],
                                'Theta/Cap %': [subset['Theta/Cap %'].mean()],
                                'Daily Yield %': [subset['Daily Yield %'].mean()],
                                'Ann. ROI': [subset['Ann. ROI'].mean()],
                                'Theta Eff.': [subset['Theta Eff.'].mean()],
                                'P&L Vol': [subset['P&L Vol'].mean()],
                                'P&L': [subset['P&L'].sum()], 'Debit': [subset['Debit'].sum()],
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
                                    'Daily Yield %': "{:.2f}%", 'Ann. ROI': "{:.1f}%", 
                                    'Theta Eff.': "{:.2f}", 'P&L Vol': "{:.1f}",
                                    'Theta': "{:.1f}", 'Delta': "{:.1f}", 'Gamma': "{:.2f}", 'Vega': "{:.0f}", 
                                    'Days Held': "{:.0f}"
                                })
                                .map(lambda v: 'background-color: #d1e7dd; color: #0f5132; font-weight: bold' if 'TAKE PROFIT' in str(v) else ('background-color: #f8d7da; color: #842029; font-weight: bold' if 'KILL' in str(v) or 'MISSING' in str(v) else ('background-color: #fff3cd; color: #856404; font-weight: bold' if 'WATCH' in str(v) else ('background-color: #cff4fc; color: #055160; font-weight: bold' if 'COOKING' in str(v) else ''))), subset=['Action'])
                                .map(lambda v: 'color: #0f5132; font-weight: bold' if 'A' in str(v) else ('color: #842029; font-weight: bold' if 'F' in str(v) else 'color: #d97706; font-weight: bold'), subset=['Grade'])
                                .map(lambda v: 'color: green; font-weight: bold' if isinstance(v, (int, float)) and v > 0 else ('color: red; font-weight: bold' if isinstance(v, (int, float)) and v < 0 else ''), subset=['P&L'])
                                .map(yield_color, subset=['Daily Yield %'])
                                .map(lambda v: 'color: #8b0000; font-weight: bold' if isinstance(v, (int, float)) and v > 45 else '', subset=['Days Held'])
                                .map(lambda v: 'background-color: #ffcccb; color: #8b0000; font-weight: bold' if isinstance(v, (int, float)) and v < 0.1 else ('background-color: #d1e7dd; color: #0f5132; font-weight: bold' if isinstance(v, (int, float)) and v > 0.2 else ''), subset=['Theta/Cap %'])
                                .apply(lambda x: ['background-color: #d1d5db; color: black; font-weight: bold' if x.name == len(display_df)-1 else '' for _ in x], axis=1), 
                                use_container_width=True,
                                column_config={
                                    "Link": st.column_config.LinkColumn("OS Link", display_text="Open ↗️"),
                                    "Exit Score": st.column_config.ProgressColumn("Urgency", min_value=0, max_value=100, format="%d")
                                }
                            )
                        else: st.info("No active trades.")

                render_tab(strat_tabs_inner[1], '130/160')
                render_tab(strat_tabs_inner[2], '160/190')
                render_tab(strat_tabs_inner[3], 'M200')
                render_tab(strat_tabs_inner[4], 'SMSF')

    else:
        st.info("👋 Database is empty. Sync your first file.")

# 3. ANALYTICS
with tab_analytics:
    if not df.empty:
        # Re-establish active context for the health check
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        
        # --- NEW PORTFOLIO HEALTH CHECK (Added v108.0) ---
        if not active_df.empty:
            st.markdown("### 🏥 Portfolio Health Check")
            health_col1, health_col2, health_col3 = st.columns(3)
            
            tot_debit = active_df['Debit'].sum()
            if tot_debit == 0: tot_debit = 1
            
            # 1. Capital Allocation Health
            target_allocation = {'130/160': 0.30, '160/190': 0.40, 'M200': 0.20, 'SMSF': 0.10}
            actual = active_df.groupby('Strategy')['Debit'].sum() / tot_debit
            allocation_score = 100 - sum(abs(actual.get(s, 0) - target_allocation.get(s, 0)) * 100 for s in target_allocation)
            health_col1.metric("🎯 Allocation Score", f"{allocation_score:.0f}/100", 
                                delta="Optimal" if allocation_score > 80 else "Review")
            
            # 2. Greek Exposure Health
            total_delta_pct = abs(active_df['Delta'].sum() / tot_debit * 100)
            greek_health = "🟢 Safe" if total_delta_pct < 2 else "🟡 Warning" if total_delta_pct < 5 else "🔴 Danger"
            health_col2.metric("🧬 Greek Exposure", greek_health, 
                                delta=f"{total_delta_pct:.2f}% Delta/Capital", delta_color="inverse")
            
            # 3. Age Health
            avg_age = active_df['Days Held'].mean()
            age_health = "🟢 Fresh" if avg_age < 25 else "🟡 Aging" if avg_age < 35 else "🔴 Stale"
            health_col3.metric("⏰ Portfolio Age", age_health, 
                                delta=f"{avg_age:.0f} days avg", delta_color="inverse")
            
            st.divider()
        # -----------------------------------------------

        st.markdown("### 📊 Performance Deep Dive")
        
        realized_pnl = df[df['Status']=='Expired']['P&L'].sum()
        floating_pnl = df[df['Status']=='Active']['P&L'].sum()
        
        # Expectancy Metrics
        if not expired_df.empty:
            total_trades = len(expired_df)
            win_rate = (expired_df['P&L'] > 0).sum() / total_trades if total_trades > 0 else 0
            avg_win = expired_df[expired_df['P&L'] > 0]['P&L'].mean() if (expired_df['P&L'] > 0).any() else 0
            avg_loss = abs(expired_df[expired_df['P&L'] <= 0]['P&L'].mean()) if (expired_df['P&L'] <= 0).any() else 0
            expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
            
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("💰 Banked Profit", f"${realized_pnl:,.0f}")
            m2.metric("📊 Win Rate", f"{win_rate:.1%}")
            m3.metric("🎲 Expectancy", f"${expectancy:,.0f}", help="Expected $ per trade")
            m4.metric("🔮 Total Projected", f"${realized_pnl+floating_pnl:,.0f}")
        else:
            m1, m2, m3 = st.columns(3)
            m1.metric("💰 Banked Profit", f"${realized_pnl:,.0f}")
            m2.metric("📄 Floating PnL", f"${floating_pnl:,.0f}", delta_color="normal")
            m3.metric("🔮 Total Projected", f"${realized_pnl+floating_pnl:,.0f}")
        
        st.divider()
        
        an1, an2, an3, an4 = st.tabs(["🔍 Diagnostics", "📈 Trends", "⚠️ Risk & Optimization", "🔄 Rolls"])
        
        with an1:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("🕵️ Root Cause Analysis")
                expired_wins = df[(df['Status'] == 'Expired') & (df['P&L'] > 0)]
                active_trades = df[df['Status'] == 'Active']
                
                if not expired_wins.empty and not active_trades.empty:
                    avg_win_debit = expired_wins.groupby('Strategy')['Debit/Lot'].mean().reset_index()
                    avg_act_debit = active_trades.groupby('Strategy')['Debit/Lot'].mean().reset_index()
                    avg_win_debit['Type'] = 'Winning History'
                    avg_act_debit['Type'] = 'Active (Current)'
                    comp_df = pd.concat([avg_win_debit, avg_act_debit])
                    
                    fig_price = px.bar(comp_df, x='Strategy', y='Debit/Lot', color='Type', barmode='group',
                                    title="Entry Price per Lot Comparison",
                                    color_discrete_map={'Winning History': 'green', 'Active (Current)': 'orange'})
                    st.plotly_chart(fig_price, use_container_width=True)
                else: st.info("Need more data.")

            with col2:
                st.subheader("⚖️ Profit Drivers (Puts vs Calls)")
                expired = df[df['Status'] == 'Expired'].copy()
                if not expired.empty:
                    leg_agg = expired.groupby('Strategy')[['Put P&L', 'Call P&L']].sum().reset_index()
                    fig_legs = px.bar(leg_agg, x='Strategy', y=['Put P&L', 'Call P&L'], 
                                        title="Profit Source Split",
                                        color_discrete_map={'Put P&L': '#EF553B', 'Call P&L': '#00CC96'})
                    st.plotly_chart(fig_legs, use_container_width=True)
                else: st.info("No closed trades.")

            st.markdown("##### 🔬 Trade-by-Trade Split")
            if not expired.empty:
                split_df = expired[['Name', 'Strategy', 'Put P&L', 'Call P&L', 'P&L']].copy()
                split_df['Calc Sum'] = split_df['Put P&L'] + split_df['Call P&L']
                split_df['Diff'] = split_df['P&L'] - split_df['Calc Sum']
                st.dataframe(split_df.style.format({'Put P&L': "${:,.0f}", 'Call P&L': "${:,.0f}", 'P&L': "${:,.0f}", 'Calc Sum': "${:,.0f}", 'Diff': "${:,.0f}"}).map(lambda x: 'color: green' if isinstance(x, (int, float)) and x > 0 else ('color: red' if isinstance(x, (int, float)) and x < 0 else ''), subset=['Put P&L', 'Call P&L', 'P&L']), use_container_width=True)

        with an2:
            if not expired_df.empty:
                ec_df = expired_df.dropna(subset=["Exit Date"]).sort_values("Exit Date").copy()
                ec_df['Cumulative P&L'] = ec_df['P&L'].cumsum()
                fig = px.line(ec_df, x='Exit Date', y='Cumulative P&L', title="Realized Equity Curve", markers=True)
                st.plotly_chart(fig, use_container_width=True)
            
            st.divider()
            hm1, hm2, hm3 = st.tabs(["🗓️ Seasonality", "⏳ Duration", "📅 Entry Day"])
            if not expired_df.empty:
                exp_hm = expired_df.dropna(subset=['Exit Date']).copy()
                exp_hm['Month'] = exp_hm['Exit Date'].dt.month_name()
                exp_hm['Year'] = exp_hm['Exit Date'].dt.year
                
                with hm1:
                    hm_data = exp_hm.groupby(['Year', 'Month']).agg({'P&L': 'sum'}).reset_index()
                    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                    fig = px.density_heatmap(hm_data, x="Month", y="Year", z="P&L", title="Monthly Seasonality ($)", category_orders={"Month": months}, text_auto=True, color_continuous_scale="RdBu")
                    st.plotly_chart(fig, use_container_width=True)
                with hm2:
                    fig2 = px.density_heatmap(exp_hm, x="Days Held", y="Strategy", z="P&L", histfunc="avg", title="Duration Sweet Spot (Avg P&L)", color_continuous_scale="RdBu")
                    st.plotly_chart(fig2, use_container_width=True)
                with hm3:
                    if 'Entry Date' in exp_hm.columns:
                        exp_hm['Day'] = exp_hm['Entry Date'].dt.day_name()
                        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                        fig3 = px.density_heatmap(exp_hm, x="Day", y="Strategy", z="P&L", histfunc="avg", title="Best Entry Day (Avg P&L)", category_orders={"Day": days}, color_continuous_scale="RdBu")
                        st.plotly_chart(fig3, use_container_width=True)

        with an3: 
            st.subheader("🧬 Trade Life Cycle & Decay")
            snaps = load_snapshots()
            if not snaps.empty:
                decay_strat = st.selectbox("Select Strategy for Decay", snaps['strategy'].unique(), key="decay_strat")
                strat_snaps = snaps[snaps['strategy'] == decay_strat].copy()
                if not strat_snaps.empty:
                    # --- NEW FEATURE: PnL LIFE CYCLE GRAPH ---
                    fig_pnl = px.line(strat_snaps, x='days_held', y='pnl', color='name', 
                                      title=f"Trade Life Cycle: PnL Trajectory ({decay_strat})",
                                      labels={'days_held': 'Days Held', 'pnl': 'P&L ($)'},
                                      markers=True)
                    st.plotly_chart(fig_pnl, use_container_width=True)
                    # ----------------------------------------
                    
                    # FIXED: Use EARLIEST snapshot theta as anchor (Day 0 baseline)
                    def get_theta_anchor(group):
                        earliest = group.sort_values('days_held').iloc[0]
                        return earliest['theta'] if earliest['theta'] > 0 else group['theta'].max()
                    
                    anchor_map = strat_snaps.groupby('id').apply(get_theta_anchor)
                    strat_snaps['Theta_Anchor'] = strat_snaps['id'].map(anchor_map)
                    
                    strat_snaps['Theta_Expected'] = strat_snaps['Theta_Anchor'] * (1 - strat_snaps['days_held'] / 45)
                    
                    strat_snaps = strat_snaps[
                        (strat_snaps['Theta_Anchor'] > 0) & 
                        (strat_snaps['theta'] != 0) & 
                        (strat_snaps['days_held'] < 60) 
                    ]
                    
                    if not strat_snaps.empty:
                        d1, d2 = st.columns(2)
                        with d1:
                            fig_theta = go.Figure()
                            for trade_id in strat_snaps['id'].unique():
                                trade_data = strat_snaps[strat_snaps['id'] == trade_id].sort_values('days_held')
                                fig_theta.add_trace(go.Scatter(
                                    x=trade_data['days_held'],
                                    y=trade_data['theta'],
                                    mode='lines+markers',
                                    name=f"{trade_data['name'].iloc[0][:15]} (Actual)",
                                    line=dict(width=2),
                                    showlegend=True
                                ))
                                fig_theta.add_trace(go.Scatter(
                                    x=trade_data['days_held'],
                                    y=trade_data['Theta_Expected'],
                                    mode='lines',
                                    name=f"{trade_data['name'].iloc[0][:15]} (Expected)",
                                    line=dict(dash='dash', width=1),
                                    opacity=0.5,
                                    showlegend=False
                                ))
                            fig_theta.update_layout(title=f"Theta: Actual vs Expected ({decay_strat})", xaxis_title="Days Held", yaxis_title="Theta ($)", hovermode='x unified')
                            st.plotly_chart(fig_theta, use_container_width=True)
                            
                            avg_deviation = (strat_snaps['theta'] - strat_snaps['Theta_Expected']).mean()
                            if avg_deviation > 10:
                                st.success(f"✅ Theta holding better than expected (+${avg_deviation:.0f}/day avg)")
                                st.info("💡 **Action:** Current positions have resilient Greeks. Consider letting winners run longer.")
                            elif avg_deviation < -10:
                                st.warning(f"⚠️ Theta decaying faster than expected (${avg_deviation:.0f}/day avg)")
                                st.error("🚨 **Action:** Greeks deteriorating. Review for early exit or structural issues.")
                            else:
                                st.info("📊 Theta decay tracking as expected")
                        
                        with d2:
                            fig_delta = px.scatter(strat_snaps, x='days_held', y='delta', color='name', title=f"Delta Drift: {decay_strat}", labels={'days_held': 'Days', 'delta': 'Delta'}, trendline="lowess")
                            st.plotly_chart(fig_delta, use_container_width=True)
                    else:
                        st.warning("Insufficient data after filtering. Upload more daily snapshots.")
            else:
                st.info("Upload daily active files to build decay history.")

            st.divider()
            st.subheader("⏳ Exit Timing Optimizer")
            if not expired_df.empty:
                opt_strat = st.selectbox("Optimize Strategy", expired_df['Strategy'].unique(), key="opt_strat")
                strat_hist = expired_df[expired_df['Strategy'] == opt_strat].copy()
                if not strat_hist.empty:
                    strat_hist['Day_Bin'] = pd.cut(strat_hist['Days Held'], bins=[0, 15, 30, 45, 60, 90, 120], labels=['0-15d', '15-30d', '30-45d', '45-60d', '60-90d', '90d+'])
                    pnl_bins = [-99999, -1, 500, 1000, 99999]
                    pnl_labels = ['Loss', 'Small Win', 'Target Win', 'Home Run']
                    strat_hist['PnL_Bin'] = pd.cut(strat_hist['P&L'], bins=pnl_bins, labels=pnl_labels)
                    heatmap_pnl = strat_hist.groupby(['PnL_Bin', 'Day_Bin'])['P&L'].mean().reset_index()
                    pnl_matrix = heatmap_pnl.pivot(index='PnL_Bin', columns='Day_Bin', values='P&L')
                    fig_opt = px.imshow(pnl_matrix, text_auto=".0f", aspect="auto", color_continuous_scale="RdBu", title=f"Avg Exit PnL by Duration & Zone ({opt_strat})")
                    st.plotly_chart(fig_opt, use_container_width=True)
            else: st.info("Need closed trade history.")

        with an4: 
            st.subheader("🔄 Roll Campaign Analysis")
            rolled_trades = df[df['Parent ID'].notna() & (df['Parent ID'] != "")].copy()
            if not rolled_trades.empty:
                campaign_summary = []
                for parent in rolled_trades['Parent ID'].unique():
                    if not parent: continue
                    campaign = df[(df['id'] == parent) | (df['Parent ID'] == parent)]
                    if campaign.empty: continue
                    
                    campaign_summary.append({
                        'Campaign': parent[:15],
                        'Total P&L': campaign['P&L'].sum(),
                        'Total Days': campaign['Days Held'].sum(),
                        'Legs': len(campaign),
                        'Avg P&L/Leg': campaign['P&L'].mean()
                    })
                
                if campaign_summary:
                    camp_df = pd.DataFrame(campaign_summary)
                    st.dataframe(camp_df.style.format({'Total P&L': '${:,.0f}', 'Avg P&L/Leg': '${:,.0f}'}), use_container_width=True)
                    
                    avg_single = expired_df[expired_df['Parent ID'].isna() | (expired_df['Parent ID'] == "")]['P&L'].mean()
                    avg_rolled = camp_df['Total P&L'].mean()
                    
                    c1, c2 = st.columns(2)
                    c1.metric("Avg Single Trade P&L", f"${avg_single:,.0f}")
                    c2.metric("Avg Roll Campaign P&L", f"${avg_rolled:,.0f}", delta=f"{avg_rolled-avg_single:,.0f}")
                    
                    if avg_rolled > avg_single:
                        st.success(f"✅ Rolling WORKS: Rolled trades outperform single trades on average.")
                    else:
                        st.warning(f"⚠️ Rolling HURTS: Consider taking losses earlier.")
            else:
                st.info("No rolled trades linked via Parent ID yet. Use the 'Journal' tab to link trades.")

# 4. RULE BOOK
with tab_rules:
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
    st.divider()
    st.caption("Allantis Trade Guardian v109.0 (PnL Life Cycle Restored)")
