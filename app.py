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

# --- CUSTOM CSS FOR CLEAN LOOK ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
    }
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
    div[data-testid="stExpander"] div[role="button"] p {
        font-size: 1.1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# --- DEBUG BANNER ---
# st.info("✅ RUNNING VERSION: v131.0 (Modern UI/UX Overhaul)") 
# Commented out for cleaner look, uncomment if needed for version check

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v4.db"

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # 1. CREATE TABLES
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
                    link TEXT,
                    original_group TEXT
                )''')
    
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
    
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (
                    name TEXT PRIMARY KEY,
                    identifier TEXT,
                    target_pnl REAL,
                    target_days INTEGER,
                    min_stability REAL,
                    description TEXT,
                    typical_debit REAL
                )''')
    
    # 2. MIGRATIONS
    def add_column_safe(table, col_name, col_type):
        try:
            c.execute(f"SELECT {col_name} FROM {table} LIMIT 1")
        except:
            try:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
            except: pass

    add_column_safe('snapshots', 'theta', 'REAL')
    add_column_safe('snapshots', 'delta', 'REAL')
    add_column_safe('snapshots', 'vega', 'REAL')
    add_column_safe('strategy_config', 'typical_debit', 'REAL')
    add_column_safe('trades', 'original_group', 'TEXT')
    
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
    conn.commit()
    conn.close()
    
    seed_default_strategies()

def seed_default_strategies(force_reset=False):
    conn = get_db_connection()
    c = conn.cursor()
    try:
        if force_reset:
            c.execute("DELETE FROM strategy_config")
        
        c.execute("SELECT count(*) FROM strategy_config")
        count = c.fetchone()[0]
        
        if count == 0:
            defaults = [
                ('130/160', '130/160', 500, 36, 0.8, 'Income Discipline', 4000),
                ('160/190', '160/190', 700, 44, 0.8, 'Patience Training', 5200),
                ('M200', 'M200', 900, 41, 0.8, 'Emotional Mastery', 8000),
                ('SMSF', 'SMSF', 600, 40, 0.8, 'Wealth Builder', 5000)
            ]
            c.executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)
            conn.commit()
    except Exception as e:
        print(f"Seeding error: {e}")
    finally:
        conn.close()

@st.cache_data(ttl=60)
def load_strategy_config():
    if not os.path.exists(DB_NAME): return {}
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='strategy_config'")
        if not c.fetchone(): return {}

        df = pd.read_sql("SELECT * FROM strategy_config", conn)
        config = {}
        for _, row in df.iterrows():
            typ_debit = row['typical_debit'] if 'typical_debit' in row and pd.notnull(row['typical_debit']) else 5000
            
            config[row['name']] = {
                'id': row['identifier'],
                'pnl': row['target_pnl'],
                'dit': row['target_days'],
                'stability': row['min_stability'],
                'debit_per_lot': typ_debit
            }
        return config
    except: return {}
    finally: conn.close()

# --- HELPER FUNCTIONS ---
def get_strategy_dynamic(trade_name, group_name, config_dict):
    t_name = str(trade_name).upper().strip()
    g_name = str(group_name).upper().strip()
    sorted_strats = sorted(config_dict.items(), key=lambda x: len(str(x[1]['id'])), reverse=True)
    
    for strat_name, details in sorted_strats:
        key = str(details['id']).upper()
        if key in t_name: return strat_name
            
    for strat_name, details in sorted_strats:
        key = str(details['id']).upper()
        if key in g_name: return strat_name
            
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
def parse_optionstrat_file(file, file_type, config_dict):
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
            group = str(trade_data.get('Group', '')) 
            created = trade_data.get('Created At', '')
            try: start_dt = pd.to_datetime(created)
            except: return None 

            strat = get_strategy_dynamic(name, group, config_dict)
            
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

            strat_config = config_dict.get(strat, {})
            typical_debit = strat_config.get('debit_per_lot', 5000)
            
            lot_size = int(round(debit / typical_debit))
            if lot_size < 1: lot_size = 1

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
                'iv': iv, 'put_pnl': put_pnl, 'call_pnl': call_pnl, 'link': link,
                'group': group
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
    
    config_dict = load_strategy_config()

    for file in file_list:
        count_new = 0
        count_update = 0
        try:
            trades_data = parse_optionstrat_file(file, file_type, config_dict)
            if not trades_data:
                log.append(f"⚠️ {file.name}: Skipped (No valid trades found)")
                continue

            for t in trades_data:
                trade_id = t['id']
                if file_type == "Active":
                    file_found_ids.add(trade_id)
                
                c.execute("SELECT id, status, theta, delta, gamma, vega, put_pnl, call_pnl, iv, link, lot_size, strategy FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
                # Rename Logic
                if existing is None and t['link'] and len(t['link']) > 15:
                    c.execute("SELECT id, name FROM trades WHERE link = ?", (t['link'],))
                    link_match = c.fetchone()
                    if link_match:
                        old_id, old_name = link_match
                        try:
                            c.execute("UPDATE snapshots SET trade_id = ? WHERE trade_id = ?", (trade_id, old_id))
                            c.execute("UPDATE trades SET id=?, name=? WHERE id=?", (trade_id, t['name'], old_id))
                            log.append(f"🔄 Renamed: '{old_name}' -> '{t['name']}'")
                            c.execute("SELECT id, status, theta, delta, gamma, vega, put_pnl, call_pnl, iv, link, lot_size, strategy FROM trades WHERE id = ?", (trade_id,))
                            existing = c.fetchone()
                            if file_type == "Active":
                                file_found_ids.add(trade_id)
                                if old_id in db_active_ids: db_active_ids.remove(old_id)
                                db_active_ids.add(trade_id)
                        except Exception as rename_err:
                            print(f"Rename failed: {rename_err}")

                status = "Active" if file_type == "Active" else "Expired"
                
                if existing is None:
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, notes, tags, parent_id, put_pnl, call_pnl, iv, link, original_group)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, t['name'], t['strategy'], status, t['start_dt'].date(), 
                         t['exit_dt'].date() if t['exit_dt'] else None, 
                         t['days_held'], t['debit'], t['lot_size'], t['pnl'], 
                         t['theta'], t['delta'], t['gamma'], t['vega'], "", "", "", t['put_pnl'], t['call_pnl'], t['iv'], t['link'], t['group']))
                    count_new += 1
                else:
                    db_lot_size = existing[10]
                    final_lot_size = t['lot_size']
                    if db_lot_size and db_lot_size > 0:
                        final_lot_size = db_lot_size

                    db_strategy = existing[11]
                    final_strategy = db_strategy
                    if db_strategy == 'Other' and t['strategy'] != 'Other':
                         final_strategy = t['strategy']

                    old_put = existing[6] if existing[6] else 0.0
                    old_call = existing[7] if existing[7] else 0.0
                    old_iv = existing[8] if existing[8] else 0.0
                    old_link = existing[9] if existing[9] else ""
                    
                    old_status = existing[1]
                    old_theta = existing[2]

                    final_theta = t['theta'] if t['theta'] != 0 else old_theta
                    final_delta = t['delta'] if t['delta'] != 0 else 0
                    final_gamma = t['gamma'] if t['gamma'] != 0 else 0
                    final_vega = t['vega'] if t['vega'] != 0 else 0
                    final_iv = t['iv'] if t['iv'] != 0 else old_iv
                    final_put = t['put_pnl'] if t['put_pnl'] != 0 else old_put
                    final_call = t['call_pnl'] if t['call_pnl'] != 0 else old_call
                    final_link = t['link'] if t['link'] != "" else old_link

                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, put_pnl=?, call_pnl=?, iv=?, link=?, lot_size=?, strategy=?, original_group=?
                            WHERE id=?''', 
                            (t['pnl'], status, t['exit_dt'].date() if t['exit_dt'] else None, t['days_held'], 
                             final_theta, final_delta, final_gamma, final_vega, final_put, final_call, final_iv, final_link, final_lot_size, final_strategy, t['group'], trade_id))
                        count_update += 1
                    elif old_status in ["Active", "Missing"]: 
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, iv=?, link=?, status='Active', exit_date=?, lot_size=?, strategy=?, original_group=?
                            WHERE id=?''', 
                            (t['pnl'], t['days_held'], final_theta, final_delta, final_gamma, final_vega, final_iv, final_link, 
                             t['exit_dt'].date() if t['exit_dt'] else None, final_lot_size, final_strategy, t['group'], trade_id))
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
            new_lot = int(row['lot_size']) if 'lot_size' in row and row['lot_size'] > 0 else 1
            new_strat = str(row['Strategy']) 
            
            c.execute("UPDATE trades SET notes=?, tags=?, parent_id=?, lot_size=?, strategy=? WHERE id=?", (notes, tags, pid, new_lot, new_strat, t_id))
            count += 1
        conn.commit()
        return count
    except Exception as e: return 0
    finally: conn.close()

def update_strategy_config(edited_df):
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("DELETE FROM strategy_config")
        for i, row in edited_df.iterrows():
            c.execute("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", 
                      (row['Name'], row['Identifier'], row['Target PnL'], row['Target Days'], row['Min Stability'], row['Description'], row['Typical Debit']))
        conn.commit()
        return True
    except Exception as e:
        print(e)
        return False
    finally: conn.close()

def reprocess_other_trades():
    conn = get_db_connection()
    c = conn.cursor()
    config_dict = load_strategy_config()
    
    try:
        c.execute("SELECT id, name, original_group, strategy FROM trades")
    except:
        c.execute("SELECT id, name, '', strategy FROM trades")
        
    all_trades = c.fetchall()
    updated_count = 0
    
    for t_id, t_name, t_group, current_strat in all_trades:
        if current_strat == "Other":
            group_val = t_group if t_group else ""
            new_strat = get_strategy_dynamic(t_name, group_val, config_dict) 
            if new_strat != "Other":
                c.execute("UPDATE trades SET strategy = ? WHERE id = ?", (new_strat, t_id))
                updated_count += 1
            
    conn.commit()
    conn.close()
    return updated_count

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
        
        df['lot_size'] = pd.to_numeric(df['lot_size'], errors='coerce').fillna(1).astype(int)
        df['lot_size'] = df['lot_size'].apply(lambda x: 1 if x < 1 else x)
        
        df['Debit/Lot'] = df['Debit'] / df['lot_size']
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = np.where(df['Debit'] > 0, (df['Theta'] / df['Debit']) * 100, 0)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
        df['Stability'] = np.where(df['Theta'] > 0, df['Theta'] / (df['Delta'].abs() + 1), 0.0)
        
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
with st.sidebar:
    st.markdown("### 🗂️ Data Management")
    
    with st.expander("📥 Sync & Restore", expanded=True):
        restore = st.file_uploader("1. Restore .db file", type=['db'], key='restore')
        if restore:
            with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
            st.cache_data.clear()
            st.success("Database Restored.")
            st.rerun()

        active_up = st.file_uploader("2. Active Trades (OS)", accept_multiple_files=True, key="act")
        history_up = st.file_uploader("3. Closed Trades (OS)", accept_multiple_files=True, key="hist")
        
        if st.button("🔄 Process & Sync", type="primary"):
            logs = []
            if active_up: logs.extend(sync_data(active_up, "Active"))
            if history_up: logs.extend(sync_data(history_up, "History"))
            if logs:
                for l in logs: st.write(l)
                st.cache_data.clear()
                st.success("Sync Complete!")

    with st.expander("💾 Backup", expanded=False):
        with open(DB_NAME, "rb") as f:
            st.download_button("Download .db Backup", f, "trade_guardian_v4.db", "application/x-sqlite3")

    # --- NEW: TRADE MANAGER (Manual Fix) ---
    with st.expander("🛠️ Maintenance", expanded=False):
        st.caption("Fix Duplicates / Rename Issues")
        
        if st.button("🧹 Vacuum DB"):
            conn = get_db_connection()
            conn.execute("VACUUM")
            conn.close()
            st.success("Optimized.")
            
        st.markdown("---")
        conn = get_db_connection()
        try:
            all_trades = pd.read_sql("SELECT id, name, status, pnl, days_held FROM trades ORDER BY status, entry_date DESC", conn)
            if not all_trades.empty:
                st.write("🗑️ **Delete Specific Trades**")
                all_trades['Label'] = all_trades['name'] + " (" + all_trades['status'] + ", $" + all_trades['pnl'].astype(str) + ")"
                trades_to_del = st.multiselect("Select trades to delete:", all_trades['Label'].tolist())
                
                if st.button("🔥 Delete Selected Trades"):
                    if trades_to_del:
                        ids_to_del = all_trades[all_trades['Label'].isin(trades_to_del)]['id'].tolist()
                        placeholders = ','.join('?' for _ in ids_to_del)
                        
                        conn.execute(f"DELETE FROM snapshots WHERE trade_id IN ({placeholders})", ids_to_del)
                        conn.execute(f"DELETE FROM trades WHERE id IN ({placeholders})", ids_to_del)
                        conn.commit()
                        st.success(f"Deleted {len(ids_to_del)} trades!")
                        st.cache_data.clear()
                        st.rerun()
        except: pass
        conn.close()
        
        st.markdown("---")
        if st.button("🧨 Hard Reset (Delete All Data)"):
            conn = get_db_connection()
            conn.execute("DROP TABLE IF EXISTS trades")
            conn.execute("DROP TABLE IF EXISTS snapshots")
            conn.execute("DROP TABLE IF EXISTS strategy_config")
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

# --- SMART ADAPTIVE EXIT ENGINE ---
def calculate_decision_ladder(row, benchmarks_dict):
    strat = row['Strategy']
    days = row['Days Held']
    pnl = row['P&L']
    status = row['Status']
    theta = row['Theta']
    stability = row['Stability']
    debit = row['Debit']
    
    lot_size = row.get('lot_size', 1)
    if lot_size < 1: lot_size = 1
    
    juice_val = 0.0
    juice_type = "Neutral"

    if status == 'Missing': return "REVIEW", 100, "Missing from data", 0, "Error"
    
    bench = benchmarks_dict.get(strat, {})
    
    hist_avg_pnl = bench.get('pnl', 1000)
    target_profit = (hist_avg_pnl * regime_mult) * lot_size
    
    hist_avg_days = bench.get('dit', 40)
    
    score = 50 
    action = "HOLD"
    reason = "Normal"
    
    if pnl < 0:
        juice_type = "Recovery Days"
        if theta > 0:
            recov_days = abs(pnl) / theta
            juice_val = recov_days
            
            is_cooking = (strat == '160/190' and days < 30)
            is_young = days < 15
            
            if not is_cooking and not is_young:
                remaining_time_est = max(1, hist_avg_days - days)
                if recov_days > remaining_time_est:
                    score += 40
                    action = "STRUCTURAL FAILURE"
                    reason = f"Zombie (Recov {recov_days:.0f}d > Left {remaining_time_est:.0f}d)"
        else:
            juice_val = 999
            if days > 15:
                score += 30
                reason = "Negative Theta"
    else:
        juice_type = "Left in Tank"
        left_in_tank = max(0, target_profit - pnl)
        juice_val = left_in_tank
        
        if debit > 0 and (left_in_tank / debit) < 0.05:
            score += 40
            reason = "Squeezed Dry (Risk > Reward)"
        elif left_in_tank < (100 * lot_size):
            score += 35
            reason = f"Empty Tank (<${100*lot_size})"

    if pnl >= target_profit:
        return "TAKE PROFIT", 100, f"Hit Target ${target_profit:.0f}", juice_val, juice_type
    elif pnl >= target_profit * 0.8:
        score += 30
        action = "PREPARE EXIT"
        reason = "Near Target"
        
    stale_threshold = hist_avg_days * 1.25 
    
    if strat == '130/160':
        limit_130 = min(stale_threshold, 30) 
        if days > limit_130 and pnl < (100 * lot_size):
            return "KILL", 95, f"Stale (> {limit_130:.0f}d)", juice_val, juice_type
        elif days > (limit_130 * 0.8):
            score += 20
            reason = "Aging"
            
    elif strat == '160/190':
        cooking_limit = max(30, hist_avg_days * 0.7)
        if days < cooking_limit:
            score = 10 
            action = "COOKING" 
            reason = f"Too Early (<{cooking_limit:.0f}d)"
        elif days > stale_threshold:
            score += 25
            action = "WATCH"
            reason = f"Mature (>{stale_threshold:.0f}d)"
            
    elif strat == 'M200':
        if 13 <= days <= 15:
            score += 10
            action = "DAY 14 CHECK"
            reason = "Scheduled Review"
            
    if stability < 0.3 and days > 5:
        score += 25
        reason += " + Coin Flip (Unstable)"
        action = "RISK REVIEW"
        
    if row['Theta Eff.'] < 0.2 and days > 10:
        score += 15
        reason += " + Bad Decay"
        
    score = min(100, max(0, score))
    
    if score >= 90: action = "CRITICAL"
    elif score >= 70: action = "WATCH"
    elif score <= 30: action = "COOKING"
    
    return action, score, reason, juice_val, juice_type

# --- MAIN APP ---
df = load_data()
BASE_CONFIG = {
    '130/160': {'pnl': 500, 'dit': 36, 'stability': 0.8}, 
    '160/190': {'pnl': 700, 'dit': 44, 'stability': 0.8}, 
    'M200':    {'pnl': 900, 'dit': 41, 'stability': 0.8}, 
    'SMSF':    {'pnl': 600, 'dit': 40, 'stability': 0.8} 
}

dynamic_benchmarks = load_strategy_config() 
if not dynamic_benchmarks: dynamic_benchmarks = BASE_CONFIG.copy()

expired_df = pd.DataFrame() 

if not df.empty:
    expired_df = df[df['Status'] == 'Expired']
    if not expired_df.empty:
        hist_grp = expired_df.groupby('Strategy')
        for strat, grp in hist_grp:
            winners = grp[grp['P&L'] > 0]
            current_bench = dynamic_benchmarks.get(strat, {})
            if not winners.empty:
                current_bench['pnl'] = winners['P&L'].mean()
                current_bench['dit'] = winners['Days Held'].mean()
                current_bench['yield'] = grp['Daily Yield %'].mean()
                current_bench['roi'] = winners['ROI'].mean()
            dynamic_benchmarks[strat] = current_bench

# --- TABS ---
tab_dash, tab_analytics, tab_strategies, tab_rules = st.tabs(["📊 Dashboard", "📈 Analytics", "⚙️ Strategies", "📖 Rules"])

# 1. ACTIVE DASHBOARD
with tab_dash:
    if not df.empty:
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        
        if active_df.empty:
            st.info("📭 No active trades.")
        else:
            # --- PORTFOLIO HEALTH BANNER ---
            tot_debit = active_df['Debit'].sum()
            if tot_debit == 0: tot_debit = 1
            
            target_allocation = {'130/160': 0.30, '160/190': 0.40, 'M200': 0.20, 'SMSF': 0.10}
            actual_alloc = active_df.groupby('Strategy')['Debit'].sum() / tot_debit
            allocation_score = 100 - sum(abs(actual_alloc.get(s, 0) - target_allocation.get(s, 0)) * 100 for s in target_allocation)
            
            total_delta_pct = abs(active_df['Delta'].sum() / tot_debit * 100)
            avg_age = active_df['Days Held'].mean()
            
            health_status = "🟢 HEALTHY" if allocation_score > 80 and total_delta_pct < 2 and avg_age < 25 else \
                            "🟡 REVIEW" if allocation_score > 60 and total_delta_pct < 5 and avg_age < 35 else \
                            "🔴 CRITICAL"
            
            # Use columns to center the banner content
            st.markdown(f"### {health_status} Portfolio Status")
            hb1, hb2, hb3 = st.columns(3)
            hb1.metric("Allocation Score", f"{allocation_score:.0f}/100")
            hb2.metric("Net Delta Exp", f"{total_delta_pct:.1f}%")
            hb3.metric("Avg Age", f"{avg_age:.0f} days")
            
            st.divider()
            
            # --- METRICS ROW ---
            tot_theta = active_df['Theta'].sum()
            eff_score = (tot_theta / tot_debit * 100) if tot_debit > 0 else 0
            target_days = dynamic_benchmarks.get('130/160', {}).get('dit', 36)
            
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Daily Theta Income", f"${tot_theta:,.0f}")
            m2.metric("Portfolio Yield", f"{eff_score:.2f}%")
            m3.metric("Floating PnL", f"${active_df['P&L'].sum():,.0f}")
            m4.metric("Velocity", f"{active_df['Days Held'].mean():.0f}d avg", delta=f"Target: {target_days:.0f}d")
            
            # --- CALCULATE LADDER ---
            ladder_results = active_df.apply(lambda row: calculate_decision_ladder(row, dynamic_benchmarks), axis=1)
            active_df['Action'] = [x[0] for x in ladder_results]
            active_df['Urgency Score'] = [x[1] for x in ladder_results]
            active_df['Reason'] = [x[2] for x in ladder_results]
            active_df['Juice Val'] = [x[3] for x in ladder_results]
            active_df['Juice Type'] = [x[4] for x in ladder_results]

            active_df = active_df.sort_values('Urgency Score', ascending=False)

            def fmt_juice(row):
                if row['Juice Type'] == 'Recovery Days': return f"{row['Juice Val']:.0f} days"
                return f"${row['Juice Val']:.0f}"
            
            active_df['Gauge'] = active_df.apply(fmt_juice, axis=1)

            # --- PRIORITY ACTION QUEUE (Only show if urgent) ---
            todo_df = active_df[active_df['Urgency Score'] >= 70]
            if not todo_df.empty:
                st.subheader(f"🔥 Action Queue ({len(todo_df)})")
                for _, row in todo_df.iterrows():
                    u_score = row['Urgency Score']
                    color = "red" if u_score >= 90 else "orange"
                    is_valid_link = str(row['Link']).startswith('http')
                    name_display = f"[{row['Name']}]({row['Link']})" if is_valid_link else row['Name']
                    
                    with st.container():
                        ac1, ac2, ac3 = st.columns([3, 2, 1])
                        ac1.markdown(f"**{name_display}** ({row['Strategy']})")
                        ac2.markdown(f":{color}[**{row['Action']}**] ({row['Reason']})")
                        if row['Juice Type'] == 'Recovery Days':
                            ac3.metric("Break Even", f"{row['Juice Val']:.0f}d", label_visibility="collapsed")
                        else:
                            ac3.metric("Left", f"${row['Juice Val']:.0f}", label_visibility="collapsed")
                        st.progress(u_score / 100)
                st.divider()

            # --- TABS FOR DATA ---
            # Pre-flight is now its own main tab, so here we just have Journal + Strategy Detail
            sub_journal, sub_strat = st.tabs(["📝 Active Journal", "🏛️ Strategy Detail"])

            with sub_journal:
                strategy_options = sorted(list(dynamic_benchmarks.keys())) + ["Other"]
                
                display_cols = ['id', 'Name', 'Link', 'Strategy', 'Urgency Score', 'Action', 'Gauge', 'Status', 'Stability', 'Theta Eff.', 'lot_size', 'P&L', 'Debit', 'Days Held', 'Notes', 'Tags', 'Parent ID']
                column_config = {
                    "id": None, 
                    "Name": st.column_config.TextColumn("Trade Name", disabled=True),
                    "Link": st.column_config.LinkColumn("OS Link", display_text="Open 🔗"),
                    "Strategy": st.column_config.SelectboxColumn("Strat", width="medium", options=strategy_options, required=True),
                    "Status": st.column_config.TextColumn("Status", disabled=True, width="small"),
                    "Urgency Score": st.column_config.ProgressColumn("⚠️ Urgency Ladder", min_value=0, max_value=100, format="%d"),
                    "Action": st.column_config.TextColumn("Decision", disabled=True),
                    "Gauge": st.column_config.TextColumn("Tank / Recov"),
                    "Stability": st.column_config.NumberColumn("Stability", format="%.2f", disabled=True),
                    "Theta Eff.": st.column_config.NumberColumn("Θ Eff", format="%.2f", disabled=True),
                    "P&L": st.column_config.NumberColumn("P&L", format="$%d", disabled=True),
                    "Debit": st.column_config.NumberColumn("Debit", format="$%d", disabled=True),
                    "lot_size": st.column_config.NumberColumn("Lots", min_value=1, step=1),
                    "Notes": st.column_config.TextColumn("📝 Notes", width="large"),
                    "Tags": st.column_config.SelectboxColumn("🏷️ Tags", options=["Rolled", "Hedged", "Earnings", "High Risk", "Watch"], width="medium"),
                    "Parent ID": st.column_config.TextColumn("🔗 Link ID"),
                }
                edited_df = st.data_editor(
                    active_df[display_cols],
                    column_config=column_config,
                    hide_index=True,
                    use_container_width=True,
                    key="journal_editor",
                    num_rows="fixed"
                )
                if st.button("💾 Save Journal Changes"):
                    changes = update_journal(edited_df)
                    if changes: 
                        st.success(f"Saved {changes} trades!")
                        st.cache_data.clear()
            
            with sub_strat:
                sorted_strats = sorted(list(dynamic_benchmarks.keys()))
                tabs_list = ["📋 Overview"] + [f"🔹 {s}" for s in sorted_strats]
                if "Other" not in sorted_strats: tabs_list.append("📁 Unclassified")
                
                strat_tabs_inner = st.tabs(tabs_list)

                with strat_tabs_inner[0]: # Overview
                    strat_agg = active_df.groupby('Strategy').agg({
                        'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum', 'Delta': 'sum',
                        'Name': 'count', 'Daily Yield %': 'mean', 'Ann. ROI': 'mean', 'Theta Eff.': 'mean', 'P&L Vol': 'mean', 'Stability': 'mean' 
                    }).reset_index()
                    
                    strat_agg['Trend'] = strat_agg.apply(lambda r: "🟢 Improving" if r['Daily Yield %'] >= dynamic_benchmarks.get(r['Strategy'], {}).get('yield', 0) else "🔴 Lagging", axis=1)
                    
                    total_row = pd.DataFrame({
                        'Strategy': ['TOTAL'], 
                        'P&L': [strat_agg['P&L'].sum()], 'Debit': [strat_agg['Debit'].sum()],
                        'Theta': [strat_agg['Theta'].sum()], 'Delta': [strat_agg['Delta'].sum()],
                        'Name': [strat_agg['Name'].sum()], 
                        'Daily Yield %': [active_df['Daily Yield %'].mean()],
                        'Ann. ROI': [active_df['Ann. ROI'].mean()],
                        'Theta Eff.': [active_df['Theta Eff.'].mean()],
                        'P&L Vol': [active_df['P&L Vol'].mean()],
                        'Stability': [active_df['Stability'].mean()],
                        'Trend': ['-']
                    })
                    final_agg = pd.concat([strat_agg, total_row], ignore_index=True)
                    
                    st.dataframe(final_agg.style.format({'P&L': '${:,.0f}', 'Debit': '${:,.0f}', 'Theta': '{:,.0f}', 'Delta': '{:,.1f}'}), use_container_width=True)

                # Individual Tabs
                cols = ['Name', 'Link', 'Action', 'Urgency Score', 'Gauge', 'Stability', 'Theta/Cap %', 'Theta Eff.', 'P&L', 'Debit', 'Days Held']
                for i, strat_name in enumerate(sorted_strats):
                    with strat_tabs_inner[i+1]:
                        subset = active_df[active_df['Strategy'] == strat_name].copy()
                        if not subset.empty:
                            st.dataframe(subset[cols].style.format({'P&L': '${:,.0f}', 'Debit': '${:,.0f}'}), use_container_width=True)
                        else:
                            st.info("No active trades.")
                
                # Unclassified Tab
                if "Other" not in sorted_strats:
                    with strat_tabs_inner[-1]:
                        subset = active_df[active_df['Strategy'] == "Other"].copy()
                        if not subset.empty:
                            st.warning("⚠️ These trades need classification. Edit 'Strategy' in the Journal tab.")
                            st.dataframe(subset[cols], use_container_width=True)
                        else:
                            st.success("All trades classified.")

    else:
        st.info("👋 Database is empty. Sync your first file.")

# 2. ANALYTICS TAB
with tab_analytics:
    an1, an2, an3, an4 = st.tabs(["🔍 Diagnostics", "📈 Trends", "⚠️ Risk & Optimization", "🔄 Rolls"])

    if not expired_df.empty:
        st.markdown("### 🏆 Closed Trade Performance")
        
        expired_df['Cap_Days'] = expired_df['Debit'] * expired_df['Days Held'].clip(lower=1)
        
        perf_agg = expired_df.groupby('Strategy').agg({
            'P&L': 'sum',
            'Debit': 'sum',
            'Cap_Days': 'sum',
            'ROI': 'mean', 
            'id': 'count'
        }).reset_index()
        
        wins = expired_df[expired_df['P&L'] > 0].groupby('Strategy')['id'].count().reset_index(name='Wins')
        perf_agg = perf_agg.merge(wins, on='Strategy', how='left').fillna(0)
        perf_agg['Win Rate'] = perf_agg['Wins'] / perf_agg['id']
        perf_agg['Ann. TWR %'] = (perf_agg['P&L'] / perf_agg['Cap_Days']) * 365 * 100
        perf_agg['Simple Return %'] = (perf_agg['P&L'] / perf_agg['Debit']) * 100
        
        perf_display = perf_agg[['Strategy', 'id', 'Win Rate', 'P&L', 'Debit', 'Simple Return %', 'Ann. TWR %', 'ROI']].copy()
        perf_display.columns = ['Strategy', 'Trades', 'Win Rate', 'Total P&L', 'Total Volume', 'Simple Return %', 'Ann. TWR %', 'Avg Trade ROI']
        
        st.dataframe(
            perf_display.style.format({'Win Rate': "{:.1%}", 'Total P&L': "${:,.0f}", 'Total Volume': "${:,.0f}", 'Simple Return %': "{:.2f}%", 'Ann. TWR %': "{:.2f}%", 'Avg Trade ROI': "{:.2f}%"}),
            use_container_width=True
        )

# 3. STRATEGIES CONFIG TAB
with tab_strategies:
    st.markdown("### ⚙️ Strategy Configuration Manager")
    
    conn = get_db_connection()
    try:
        strat_df = pd.read_sql("SELECT * FROM strategy_config", conn)
        strat_df.columns = ['Name', 'Identifier', 'Target PnL', 'Target Days', 'Min Stability', 'Description', 'Typical Debit']
        
        edited_strats = st.data_editor(
            strat_df, 
            num_rows="dynamic", 
            key="strat_editor_main",
            use_container_width=True
        )
        
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            if st.button("💾 Save Changes"):
                if update_strategy_config(edited_strats):
                    st.success("Configuration Saved!")
                    st.cache_data.clear()
                    st.rerun()
        with c2:
            if st.button("🔄 Reprocess 'Other' Trades"):
                count = reprocess_other_trades()
                st.success(f"Reprocessed {count} trades!")
                st.cache_data.clear()
                st.rerun()
        with c3:
            if st.button("🧨 Reset to Defaults", type="secondary"):
                seed_default_strategies(force_reset=True)
                st.cache_data.clear()
                st.rerun()
    except Exception as e:
        st.error(f"Error: {e}")
    finally:
        conn.close()

# 4. RULES TAB
with tab_rules:
    st.markdown("### 📖 The Trader's Constitution")
    # (Rules text can be shortened or moved to an expander if needed)
    st.info("Rules text placeholder. (Copy previous content here if needed)")
