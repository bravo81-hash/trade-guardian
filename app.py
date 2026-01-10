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
st.info("✅ RUNNING VERSION: v140.0 (Fix: Metrics Error & New ROI% Columns Added)")

st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v4.db"

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_db_connection()
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
                    gamma REAL, 
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
    add_column_safe('snapshots', 'gamma', 'REAL')
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
            if force_reset:
                st.toast("Strategies Reset to Factory Defaults.")
    except Exception as e:
        print(f"Seeding error: {e}")
    finally:
        conn.close()

# --- LOAD STRATEGY CONFIG ---
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
                    gamma_val = t['gamma'] if t['gamma'] else 0.0
                    
                    if not c.fetchone():
                        # Updated to include gamma
                        c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)",
                                  (trade_id, today, t['pnl'], t['days_held'], theta_val, delta_val, vega_val, gamma_val))
                    else:
                        c.execute("UPDATE snapshots SET theta=?, delta=?, vega=?, gamma=? WHERE trade_id=? AND snapshot_date=?",
                                  (theta_val, delta_val, vega_val, gamma_val, trade_id, today))
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
        
        # Stability Ratio (Theta vs Risk)
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
        SELECT s.snapshot_date, s.pnl, s.days_held, s.theta, s.delta, s.vega, s.gamma,
               t.strategy, t.name, t.id as trade_id, t.theta as initial_theta
        FROM snapshots s
        JOIN trades t ON s.trade_id = t.id
        """
        try:
            df = pd.read_sql(q, conn)
        except:
            q_fallback = """
            SELECT s.snapshot_date, s.pnl, s.days_held, s.theta, s.delta, s.vega, 
                   t.strategy, t.name, t.id as trade_id, t.theta as initial_theta
            FROM snapshots s
            JOIN trades t ON s.trade_id = t.id
            """
            df = pd.read_sql(q_fallback, conn)
            df['gamma'] = 0.0

        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        df['pnl'] = pd.to_numeric(df['pnl'], errors='coerce').fillna(0)
        df['days_held'] = pd.to_numeric(df['days_held'], errors='coerce').fillna(0)
        df['theta'] = pd.to_numeric(df['theta'], errors='coerce').fillna(0)
        df['delta'] = pd.to_numeric(df['delta'], errors='coerce').fillna(0)
        df['vega'] = pd.to_numeric(df['vega'], errors='coerce').fillna(0)
        df['gamma'] = pd.to_numeric(df['gamma'], errors='coerce').fillna(0)
        df['initial_theta'] = pd.to_numeric(df['initial_theta'], errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- INTELLIGENCE FUNCTIONS ---
def generate_trade_predictions(active_df, history_df, prob_low, prob_high):
    """
    Predictive Model: Finds K-Nearest Neighbors for active trades
    based on Delta, Theta, and Debit to forecast outcome.
    """
    if active_df.empty or history_df.empty: return pd.DataFrame()
    
    features = ['Theta/Cap %', 'Delta', 'Debit/Lot']
    
    # Filter only closed trades that have valid feature data
    train_df = history_df.dropna(subset=features).copy()
    if len(train_df) < 5: return pd.DataFrame()
    
    predictions = []
    
    for _, row in active_df.iterrows():
        # Get current trade vector
        curr_vec = np.nan_to_num(row[features].values.astype(float)).reshape(1, -1)
        hist_vecs = np.nan_to_num(train_df[features].values.astype(float))
        
        # Calculate distances (Similarity)
        distances = cdist(curr_vec, hist_vecs, metric='euclidean')[0]
        
        # Get Top 7 similar trades
        top_k_idx = np.argsort(distances)[:7]
        nearest_neighbors = train_df.iloc[top_k_idx]
        
        # Forecast Logic
        win_prob = (nearest_neighbors['P&L'] > 0).mean() * 100
        avg_pnl = nearest_neighbors['P&L'].mean()
        
        # Confidence logic (based on how close the neighbors actually are)
        avg_dist = distances[top_k_idx].mean()
        confidence = max(0, 100 - (avg_dist * 10)) # Simple heuristic
        
        rec = "HOLD"
        if win_prob < prob_low: rec = "REDUCE/CLOSE"
        elif win_prob > prob_high: rec = "PRESS WINNER"
        
        predictions.append({
            'Trade Name': row['Name'],
            'Strategy': row['Strategy'],
            'Win Prob %': win_prob,
            'Expected PnL': avg_pnl,
            'AI Rec': rec,
            'Confidence': confidence
        })
        
    return pd.DataFrame(predictions)

def check_rot_and_efficiency(active_df, history_df, threshold_pct, min_days):
    """
    Time-Decay Intelligence: Checks if active trades are 'rotting'
    (Capital efficiency dropping below historical average).
    """
    if active_df.empty or history_df.empty: return pd.DataFrame()
    
    # 1. Calculate Historical Efficiency Baseline (PnL per Day per $1k Capital)
    history_df['Eff_Score'] = (history_df['P&L'] / history_df['Days Held'].clip(lower=1)) / (history_df['Debit'] / 1000)
    baseline_eff = history_df.groupby('Strategy')['Eff_Score'].median().to_dict()
    
    rot_alerts = []
    
    for _, row in active_df.iterrows():
        strat = row['Strategy']
        days = row['Days Held']
        # Only check trades that have been held for a while
        if days < min_days: continue
        
        curr_eff = (row['P&L'] / days) / (row['Debit'] / 1000) if row['Debit'] > 0 else 0
        base = baseline_eff.get(strat, 0)
        
        # Threshold: If current efficiency is < threshold of baseline
        if base > 0 and curr_eff < (base * threshold_pct):
            rot_alerts.append({
                'Trade': row['Name'],
                'Strategy': strat,
                'Current Speed': f"${curr_eff:.1f}/day",
                'Baseline Speed': f"${base:.1f}/day",
                'Raw Current': curr_eff, 
                'Raw Baseline': base,    
                'Status': '⚠️ ROTTING' if row['P&L'] > 0 else '💀 DEAD MONEY'
            })
            
    return pd.DataFrame(rot_alerts)

def get_dynamic_targets(history_df, percentile):
    """
    Dynamic Exits: Calculates MFE (Max Favorable Excursion) based on percentile
    to find the 'Sweet Spot' for profit taking.
    """
    if history_df.empty: return {}
    
    # We need MFE data. Since trades table only has Final PnL, 
    # we approximate MFE using Final PnL of *Winning* trades for now.
    
    winners = history_df[history_df['P&L'] > 0]
    if winners.empty: return {}
    
    targets = {}
    for strat, grp in winners.groupby('Strategy'):
        targets[strat] = {
            'Median Win': grp['P&L'].median(),
            'Optimal Exit': grp['P&L'].quantile(percentile)
        }
    return targets

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

def calculate_portfolio_metrics(trades_df, capital):
    """
    Calculates Sharpe and CAGR based on reconstructed daily PnL equity curve
    from trade entry/exit dates.
    """
    if trades_df.empty or capital <= 0: return 0.0, 0.0
    
    # Prepare dates
    trades = trades_df.copy()
    trades['Entry Date'] = pd.to_datetime(trades['Entry Date'])
    trades['Exit Date'] = pd.to_datetime(trades['Exit Date'])
    
    # Create a date range from first entry to last exit/today
    start_date = trades['Entry Date'].min()
    end_date = max(trades['Exit Date'].max(), pd.Timestamp.now())
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # Daily PnL Dictionary initialization
    daily_pnl = {d.date(): 0.0 for d in date_range}
    
    for _, t in trades.iterrows():
        if pd.isnull(t['Exit Date']) or t['Days Held'] <= 0: continue
        
        # Distribute PnL evenly over duration (Linear Attribution)
        d_pnl = t['P&L'] / t['Days Held']
        
        t_start = t['Entry Date']
        t_end = t['Exit Date']
        
        # Add PnL to each day in range
        curr = t_start
        while curr <= t_end:
             if curr.date() in daily_pnl:
                 daily_pnl[curr.date()] += d_pnl
             curr += pd.Timedelta(days=1)
                 
    # Convert to Series for calculation
    pnl_series = pd.Series(daily_pnl)
    
    # Sharpe Ratio (Daily Returns)
    # Daily Return = Daily PnL / Capital (Simplification using Fixed Capital Base)
    daily_rets = pnl_series / capital
    if daily_rets.std() == 0: sharpe = 0.0
    else: sharpe = (daily_rets.mean() / daily_rets.std()) * np.sqrt(252)
    
    # CAGR
    total_days = (end_date - start_date).days
    if total_days < 1: total_days = 1
    total_pnl = trades['P&L'].sum()
    end_val = capital + total_pnl
    
    try:
        cagr = ( (end_val / capital) ** (365 / total_days) ) - 1
    except:
        cagr = 0.0
    
    return sharpe, cagr * 100

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

# --- MAINTENANCE ---
with st.sidebar.expander("🛠️ Maintenance", expanded=False):
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
st.sidebar.header("⚙️ Portfolio Settings")
acct_capital = st.sidebar.number_input("Starting Capital ($)", min_value=1000, value=50000, step=1000, help="Used for CAGR and Sharpe Ratio calculations.")
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
# v136: Added "🧠 AI & Insights" as a separate main tab
tab_dash, tab_analytics, tab_ai, tab_strategies, tab_rules = st.tabs(["📊 Dashboard", "📈 Analytics", "🧠 AI & Insights", "⚙️ Strategies", "📖 Rules"])

# 1. ACTIVE DASHBOARD
with tab_dash:
    with st.expander("✈️ Universal Pre-Flight Calculator", expanded=False):
        pf_c1, pf_c2, pf_c3 = st.columns(3)
        with pf_c1:
            pf_goal = st.selectbox("Strategy Profile", [
                "🛡️ Hedged Income (Butterflies, Calendars, M200)", 
                "🏰 Standard Income (Credit Spreads, Iron Condors)", 
                "🚀 Directional (Long Calls/Puts, Verticals)", 
                "⚡ Speculative Vol (Straddles, Earnings)"
            ])
            pf_dte = st.number_input("DTE (Days)", min_value=1, value=45, step=1)
        with pf_c2:
            pf_price = st.number_input("Net Price ($)", value=5000.0, step=100.0, help="Total Debit or Credit (Risk Amount)")
            pf_theta = st.number_input("Theta ($)", value=15.0, step=1.0)
        with pf_c3:
            pf_delta = st.number_input("Net Delta", value=-10.0, step=1.0, format="%.2f")
            pf_vega = st.number_input("Vega", value=100.0, step=1.0, format="%.2f")
            
        if st.button("Run Pre-Flight Check"):
            st.markdown("---")
            res_c1, res_c2, res_c3 = st.columns(3)
            if "Hedged Income" in pf_goal:
                stability = pf_theta / (abs(pf_delta) + 1)
                yield_pct = (pf_theta / abs(pf_price)) * 100
                annualized_roi = (yield_pct * 365)
                vega_cushion = pf_vega / pf_theta if pf_theta != 0 else 0
                with res_c1:
                    if stability > 1.0: st.success(f"🛡️ Stability: {stability:.2f} (Fortress)")
                    elif stability > 0.5: st.info(f"⚖️ Stability: {stability:.2f} (Good)")
                    else: st.error(f"🎲 Stability: {stability:.2f} (Coin Flip)")
                with res_c2:
                    if annualized_roi > 50: st.success(f"💰 Ann. ROI: {annualized_roi:.0f}%")
                    elif annualized_roi > 25: st.info(f"💵 Ann. ROI: {annualized_roi:.0f}%")
                    else: st.error(f"📉 Ann. ROI: {annualized_roi:.0f}%")
                with res_c3:
                    if pf_dte < 21: st.warning("⚠️ High Gamma Risk (Low DTE)")
                    elif pf_vega > 0: st.success(f"💎 Hedge: {vega_cushion:.1f}x (Good)")
                    else: st.error(f"⚠️ Hedge: {pf_vega:.0f} (Negative Vega)")
            elif "Standard Income" in pf_goal:
                stability = pf_theta / (abs(pf_delta) + 1)
                yield_pct = (pf_theta / abs(pf_price)) * 100
                annualized_roi = (yield_pct * 365)
                fragility = abs(pf_vega) / pf_theta if pf_theta != 0 else 999
                with res_c1:
                    if stability > 0.5: st.success(f"🛡️ Stability: {stability:.2f} (Good)")
                    else: st.error(f"🎲 Stability: {stability:.2f} (Unstable)")
                with res_c2:
                    if annualized_roi > 40: st.success(f"💰 Ann. ROI: {annualized_roi:.0f}%")
                    else: st.warning(f"📉 Ann. ROI: {annualized_roi:.0f}%")
                with res_c3:
                    if pf_dte < 21: st.warning("⚠️ High Gamma Risk (Low DTE)")
                    elif pf_vega < 0 and fragility < 5: st.success(f"💎 Fragility: {fragility:.1f} (Robust)")
                    else: st.warning(f"⚠️ Fragility: {fragility:.1f} (High)")
            elif "Directional" in pf_goal:
                leverage = abs(pf_delta) / abs(pf_price) * 100
                theta_drag = (pf_theta / abs(pf_price)) * 100
                with res_c1: st.metric("Leverage", f"{leverage:.2f} Δ/$100")
                with res_c2:
                    if theta_drag > -0.1: st.success(f"🔥 Burn: {theta_drag:.2f}% (Low)")
                    else: st.warning(f"💸 Burn: {theta_drag:.2f}% (High)")
                with res_c3:
                    proj_roi = (abs(pf_delta) * 5) / abs(pf_price) * 100 
                    st.metric("ROI on $5 Move", f"{proj_roi:.1f}%")
            elif "Speculative Vol" in pf_goal:
                vega_efficiency = abs(pf_vega) / abs(pf_price) * 100
                move_needed = abs(pf_theta / pf_vega) if pf_vega != 0 else 0
                with res_c1: st.metric("Vega Exposure", f"{vega_efficiency:.1f}%")
                with res_c2: st.metric("Daily Cost", f"${pf_theta:.0f}")
                with res_c3: st.info(f"Need {move_needed:.1f}% IV move to break even")

    if not df.empty:
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        if active_df.empty:
            st.info("📭 No active trades.")
        else:
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
            
            if "HEALTHY" in health_status:
                st.success(f"**Portfolio Status: {health_status}** (Alloc: {allocation_score:.0f}, Delta: {total_delta_pct:.1f}%, Age: {avg_age:.0f}d)")
            elif "REVIEW" in health_status:
                st.warning(f"**Portfolio Status: {health_status}** (Alloc: {allocation_score:.0f}, Delta: {total_delta_pct:.1f}%, Age: {avg_age:.0f}d)")
            else:
                st.error(f"**Portfolio Status: {health_status}** (Alloc: {allocation_score:.0f}, Delta: {total_delta_pct:.1f}%, Age: {avg_age:.0f}d)")
            
            tot_theta = active_df['Theta'].sum()
            eff_score = (tot_theta / tot_debit * 100) if tot_debit > 0 else 0
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Daily Theta Income", f"${tot_theta:,.0f}")
            c2.metric("Portfolio Yield (Theta/Cap)", f"{eff_score:.2f}%", help="How hard is your capital working? Higher is better.")
            c3.metric("Floating PnL", f"${active_df['P&L'].sum():,.0f}")
            target_days = dynamic_benchmarks.get('130/160', {}).get('dit', 36)
            c4.metric("Capital Velocity", f"{active_df['Days Held'].mean():.0f} days avg", help="Lower = faster capital recycling", delta=f"Target: {target_days:.0f}d")
            
            stale_capital = active_df[active_df['Days Held'] > 40]['Debit'].sum()
            if stale_capital > tot_debit * 0.3:
                 st.warning(f"⚠️ ${stale_capital:,.0f} stuck in trades >40 days old. Consider exits.")

            st.divider()

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

            todo_df = active_df[active_df['Urgency Score'] >= 70]
            
            # --- SMART COLLAPSE: Only expand if action needed ---
            is_expanded = len(todo_df) > 0
            with st.expander(f"🔥 Priority Action Queue ({len(todo_df)})", expanded=is_expanded):
                if not todo_df.empty:
                    for _, row in todo_df.iterrows():
                        u_score = row['Urgency Score']
                        color = "red" if u_score >= 90 else "orange"
                        is_valid_link = str(row['Link']).startswith('http')
                        name_display = f"[{row['Name']}]({row['Link']})" if is_valid_link else row['Name']
                        c_a, c_b, c_c = st.columns([2, 1, 1])
                        c_a.markdown(f"**{name_display}** ({row['Strategy']})")
                        c_b.markdown(f":{color}[**{row['Action']}**] ({row['Reason']})")
                        if row['Juice Type'] == 'Recovery Days': c_c.metric("Days to Break Even", f"{row['Juice Val']:.0f}d", delta_color="inverse")
                        else: c_c.metric("Left in Tank", f"${row['Juice Val']:.0f}")
                else: st.success("✅ No critical actions required. Portfolio is healthy.")
            st.divider()

            # --- NEW LAYOUT: DNA Tool in its own tab ---
            sub_journal, sub_strat, sub_dna = st.tabs(["📝 Journal", "🏛️ Strategy Detail", "🧬 DNA Tool"])
            
            with sub_journal:
                st.caption("Trades sorted by Urgency.")
                strategy_options = sorted(list(dynamic_benchmarks.keys())) + ["Other"]
                
                # Updated Columns to include ROI and Ann. ROI
                display_cols = ['id', 'Name', 'Link', 'Strategy', 'Urgency Score', 'Action', 'Gauge', 'Status', 'Stability', 'ROI', 'Ann. ROI', 'Theta Eff.', 'lot_size', 'P&L', 'Debit', 'Days Held', 'Notes', 'Tags', 'Parent ID']
                column_config = {
                    "id": None, "Name": st.column_config.TextColumn("Trade Name", disabled=True),
                    "Link": st.column_config.LinkColumn("OS Link", display_text="Open 🔗"),
                    "Strategy": st.column_config.SelectboxColumn("Strat", width="medium", options=strategy_options, required=True),
                    "Status": st.column_config.TextColumn("Status", disabled=True, width="small"),
                    "Urgency Score": st.column_config.ProgressColumn("⚠️ Urgency Ladder", min_value=0, max_value=100, format="%d"),
                    "Action": st.column_config.TextColumn("Decision", disabled=True),
                    "Gauge": st.column_config.TextColumn("Tank / Recov"),
                    "Stability": st.column_config.NumberColumn("Stability", format="%.2f", disabled=True),
                    "Theta Eff.": st.column_config.NumberColumn("Θ Eff", format="%.2f", disabled=True),
                    "ROI": st.column_config.NumberColumn("ROI %", format="%.1f%%", disabled=True),
                    "Ann. ROI": st.column_config.NumberColumn("Ann. ROI %", format="%.1f%%", disabled=True),
                    "P&L": st.column_config.NumberColumn("P&L", format="$%d", disabled=True),
                    "Debit": st.column_config.NumberColumn("Debit", format="$%d", disabled=True),
                    "lot_size": st.column_config.NumberColumn("Lots", min_value=1, step=1),
                    "Notes": st.column_config.TextColumn("📝 Notes", width="large"),
                    "Tags": st.column_config.SelectboxColumn("🏷️ Tags", options=["Rolled", "Hedged", "Earnings", "High Risk", "Watch"], width="medium"),
                    "Parent ID": st.column_config.TextColumn("🔗 Link ID"),
                }
                edited_df = st.data_editor(active_df[display_cols], column_config=column_config, hide_index=True, use_container_width=True, key="journal_editor", num_rows="fixed")
                if st.button("💾 Save Journal"):
                    changes = update_journal(edited_df)
                    if changes: 
                        st.success(f"Saved {changes} trades!")
                        st.cache_data.clear()
            
            with sub_dna:
                st.subheader("🧬 Trade DNA Fingerprinting")
                st.caption("Find historical trades that match the Greek profile of your current active trade.")
                if not expired_df.empty:
                    selected_dna_trade = st.selectbox("Select Active Trade to Analyze", active_df['Name'].unique())
                    curr_row = active_df[active_df['Name'] == selected_dna_trade].iloc[0]
                    similar = find_similar_trades(curr_row, expired_df)
                    if not similar.empty:
                        best_match = similar.iloc[0]
                        st.info(f"🎯 **Best Match:** {best_match['Name']} ({best_match['Similarity %']:.0f}% similar) → Made ${best_match['P&L']:,.0f} in {best_match['Days Held']:.0f} days")
                        st.dataframe(similar.style.format({'P&L': '${:,.0f}', 'ROI': '{:.1f}%', 'Similarity %': '{:.0f}%'}))
                    else: st.info("No similar historical trades found.")
                else: st.info("Need closed trade history for DNA analysis.")

            with sub_strat:
                st.markdown("### 🏛️ Strategy Performance")
                sorted_strats = sorted(list(dynamic_benchmarks.keys()))
                tabs_list = ["📋 Overview"] + [f"🔹 {s}" for s in sorted_strats]
                if "Other" not in sorted_strats: tabs_list.append("📁 Other / Unclassified")
                strat_tabs_inner = st.tabs(tabs_list)

                with strat_tabs_inner[0]:
                    strat_agg = active_df.groupby('Strategy').agg({
                        'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum', 'Delta': 'sum',
                        'Name': 'count', 'Daily Yield %': 'mean', 'Ann. ROI': 'mean', 'Theta Eff.': 'mean', 'P&L Vol': 'mean', 'Stability': 'mean' 
                    }).reset_index()
                    strat_agg['Trend'] = strat_agg.apply(lambda r: "🟢 Improving" if r['Daily Yield %'] >= dynamic_benchmarks.get(r['Strategy'], {}).get('yield', 0) else "🔴 Lagging", axis=1)
                    strat_agg['Target %'] = strat_agg['Strategy'].apply(lambda x: dynamic_benchmarks.get(x, {}).get('yield', 0))
                    total_row = pd.DataFrame({
                        'Strategy': ['TOTAL'], 'P&L': [strat_agg['P&L'].sum()], 'Debit': [strat_agg['Debit'].sum()],
                        'Theta': [strat_agg['Theta'].sum()], 'Delta': [strat_agg['Delta'].sum()],
                        'Name': [strat_agg['Name'].sum()], 'Daily Yield %': [active_df['Daily Yield %'].mean()],
                        'Ann. ROI': [active_df['Ann. ROI'].mean()], 'Theta Eff.': [active_df['Theta Eff.'].mean()],
                        'P&L Vol': [active_df['P&L Vol'].mean()], 'Stability': [active_df['Stability'].mean()],
                        'Trend': ['-'], 'Target %': ['-']
                    })
                    final_agg = pd.concat([strat_agg, total_row], ignore_index=True)
                    display_agg = final_agg[['Strategy', 'Trend', 'Daily Yield %', 'Ann. ROI', 'Theta Eff.', 'Stability', 'P&L Vol', 'Target %', 'P&L', 'Debit', 'Theta', 'Delta', 'Name']].copy()
                    display_agg.columns = ['Strategy', 'Trend', 'Yield/Day', 'Ann. ROI', 'Θ Eff', 'Stability', 'Sleep Well (Vol)', 'Target', 'Total P&L', 'Total Debit', 'Net Theta', 'Net Delta', 'Count']
                    
                    def highlight_trend(val): return 'color: green; font-weight: bold' if '🟢' in str(val) else 'color: red; font-weight: bold' if '🔴' in str(val) else ''
                    def style_total(row): return ['background-color: #d1d5db; color: black; font-weight: bold'] * len(row) if row['Strategy'] == 'TOTAL' else [''] * len(row)

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
                            'Stability': lambda x: safe_fmt(x, "{:.2f}"),
                            'Sleep Well (Vol)': lambda x: safe_fmt(x, "{:.1f}"),
                            'Target': lambda x: safe_fmt(x, "{:.2f}%")
                        })
                        .map(highlight_trend, subset=['Trend'])
                        .apply(style_total, axis=1), 
                        use_container_width=True
                    )

                cols = ['Name', 'Link', 'Action', 'Urgency Score', 'Grade', 'Gauge', 'Stability', 'Theta/Cap %', 'Theta Eff.', 'P&L Vol', 'Daily Yield %', 'Ann. ROI', 'P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'Notes']
                for i, strat_name in enumerate(sorted_strats):
                    with strat_tabs_inner[i+1]:
                        subset = active_df[active_df['Strategy'] == strat_name].copy()
                        bench = dynamic_benchmarks.get(strat_name, {})
                        target_yield = bench.get('yield', 0)
                        target_disp = bench.get('pnl', 0) * regime_mult
                        
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Hist. Avg Win", f"${bench.get('pnl',0):,.0f}")
                        c2.metric("Target Yield", f"{bench.get('yield',0):.2f}%/d")
                        c3.metric("Target Profit", f"${target_disp:,.0f}")
                        c4.metric("Avg Hold", f"{bench.get('dit',0):.0f}d")
                        
                        if not subset.empty:
                            sum_row = pd.DataFrame({
                                'Name': ['TOTAL'], 'Link': [''], 'Action': ['-'], 'Urgency Score': [0], 'Grade': ['-'], 'Gauge': ['-'],
                                'Theta/Cap %': [subset['Theta/Cap %'].mean()], 'Daily Yield %': [subset['Daily Yield %'].mean()],
                                'Ann. ROI': [subset['Ann. ROI'].mean()], 'Theta Eff.': [subset['Theta Eff.'].mean()],
                                'P&L Vol': [subset['P&L Vol'].mean()], 'Stability': [subset['Stability'].mean()],
                                'P&L': [subset['P&L'].sum()], 'Debit': [subset['Debit'].sum()], 'Days Held': [subset['Days Held'].mean()],
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

                            st.dataframe(display_df.style.format({'Theta/Cap %': "{:.2f}%", 'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%", 'Ann. ROI': "{:.1f}%", 'Theta Eff.': "{:.2f}", 'P&L Vol': "{:.1f}", 'Stability': "{:.2f}", 'Theta': "{:.1f}", 'Delta': "{:.1f}", 'Gamma': "{:.2f}", 'Vega': "{:.0f}", 'Days Held': "{:.0f}"}).map(lambda v: 'background-color: #d1e7dd; color: #0f5132; font-weight: bold' if 'TAKE PROFIT' in str(v) else ('background-color: #f8d7da; color: #842029; font-weight: bold' if 'KILL' in str(v) or 'MISSING' in str(v) else ('background-color: #fff3cd; color: #856404; font-weight: bold' if 'WATCH' in str(v) else ('background-color: #cff4fc; color: #055160; font-weight: bold' if 'COOKING' in str(v) else ''))), subset=['Action']).map(lambda v: 'color: green; font-weight: bold' if isinstance(v, (int, float)) and v > 0 else ('color: red; font-weight: bold' if isinstance(v, (int, float)) and v < 0 else ''), subset=['P&L']).map(yield_color, subset=['Daily Yield %']).apply(lambda x: ['background-color: #d1d5db; color: black; font-weight: bold' if x.name == len(display_df)-1 else '' for _ in x], axis=1), use_container_width=True, column_config={"Link": st.column_config.LinkColumn("OS Link", display_text="Open ↗️"), "Urgency Score": st.column_config.ProgressColumn("Urgency", min_value=0, max_value=100, format="%d"), "Gauge": st.column_config.TextColumn("Tank / Recov")})
                        else: st.info("No active trades.")
                if "Other" not in sorted_strats:
                    with strat_tabs_inner[-1]: 
                        subset = active_df[active_df['Strategy'] == "Other"].copy()
                        if not subset.empty: st.dataframe(subset[cols], use_container_width=True)
                        else: st.info("No unclassified trades.")
    else: st.info("👋 Database is empty. Sync your first file.")

with tab_strategies:
    st.markdown("### ⚙️ Strategy Configuration Manager")
    conn = get_db_connection()
    try:
        # Robust fetch to handle potential extra/missing columns gracefully
        # First ensure we get the data
        strat_df = pd.read_sql("SELECT * FROM strategy_config", conn)
        
        # Expected columns map
        expected_cols = {
            'name': 'Name',
            'identifier': 'Identifier',
            'target_pnl': 'Target PnL',
            'target_days': 'Target Days',
            'min_stability': 'Min Stability',
            'description': 'Description',
            'typical_debit': 'Typical Debit'
        }
        
        # Ensure all expected columns exist in DF
        for db_col in expected_cols.keys():
            if db_col not in strat_df.columns:
                strat_df[db_col] = 0.0 if 'pnl' in db_col or 'debit' in db_col else (0 if 'days' in db_col else "")
        
        # Select and rename
        strat_df = strat_df[list(expected_cols.keys())].rename(columns=expected_cols)
        
        # Render Editor
        edited_strats = st.data_editor(strat_df, num_rows="dynamic", key="strat_editor_main", use_container_width=True,
            column_config={
                "Name": st.column_config.TextColumn("Strategy Name", help="Unique name"),
                "Identifier": st.column_config.TextColumn("Keyword Match"),
                "Target PnL": st.column_config.NumberColumn("Profit Target ($)", format="$%d"),
                "Target Days": st.column_config.NumberColumn("Target DIT (Days)"),
                "Min Stability": st.column_config.NumberColumn("Min Stability", format="%.2f"),
                "Typical Debit": st.column_config.NumberColumn("Typical Debit ($)", format="$%d"),
                "Description": st.column_config.TextColumn("Notes")
            })
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            if st.button("💾 Save Changes"):
                if update_strategy_config(edited_strats): st.success("Configuration Saved!"); st.cache_data.clear(); st.rerun()
        with c2:
            if st.button("🔄 Reprocess 'Other' Trades"):
                count = reprocess_other_trades(); st.success(f"Reprocessed {count} trades!"); st.cache_data.clear(); st.rerun()
        with c3:
            if st.button("🧨 Reset to Defaults", type="secondary"): seed_default_strategies(force_reset=True); st.cache_data.clear(); st.rerun()
    except Exception as e: st.error(f"Error loading strategies: {e}")
    finally: conn.close()
    
    st.info("💡 **How to use:** \n1. **Reset to Defaults** if this table is blank. \n2. **Edit Identifiers:** Ensure '130/160' is longer than '160'. \n3. **Save Changes.** \n4. **Reprocess All Trades** to fix old grouping errors.")

with tab_analytics:
    an_overview, an_trends, an_risk, an_decay, an_rolls = st.tabs(["📊 Overview", "📈 Trends & Seasonality", "⚠️ Risk & Excursion", "🧬 Decay & DNA", "🔄 Rolls"])

    with an_overview:
        if not df.empty:
            active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
            if not active_df.empty:
                st.markdown("### 🏥 Portfolio Health Check (Breakdown)")
                health_col1, health_col2, health_col3 = st.columns(3)
                tot_debit = active_df['Debit'].sum()
                if tot_debit == 0: tot_debit = 1
                
                target_allocation = {'130/160': 0.30, '160/190': 0.40, 'M200': 0.20, 'SMSF': 0.10}
                actual = active_df.groupby('Strategy')['Debit'].sum() / tot_debit
                allocation_score = 100 - sum(abs(actual.get(s, 0) - target_allocation.get(s, 0)) * 100 for s in target_allocation)
                health_col1.metric("🎯 Allocation Score", f"{allocation_score:.0f}/100", delta="Optimal" if allocation_score > 80 else "Review")
                
                total_delta_pct = abs(active_df['Delta'].sum() / tot_debit * 100)
                greek_health = "🟢 Safe" if total_delta_pct < 2 else "🟡 Warning" if total_delta_pct < 5 else "🔴 Danger"
                health_col2.metric("🧬 Greek Exposure", greek_health, delta=f"{total_delta_pct:.2f}% Delta/Capital", delta_color="inverse")
                
                avg_age = active_df['Days Held'].mean()
                age_health = "🟢 Fresh" if avg_age < 25 else "🟡 Aging" if avg_age < 35 else "🔴 Stale"
                health_col3.metric("⏰ Portfolio Age", age_health, delta=f"{avg_age:.0f} days avg", delta_color="inverse")
                st.divider()

            st.markdown("### 📊 Performance Deep Dive")
            realized_pnl = df[df['Status']=='Expired']['P&L'].sum()
            floating_pnl = df[df['Status']=='Active']['P&L'].sum()
            
            try:
                if not expired_df.empty:
                    total_trades = len(expired_df)
                    win_count = (expired_df['P&L'] > 0).sum()
                    win_rate = win_count / total_trades if total_trades > 0 else 0
                    wins = expired_df[expired_df['P&L'] > 0]['P&L']
                    losses = expired_df[expired_df['P&L'] <= 0]['P&L']
                    avg_win = wins.mean() if not wins.empty else 0
                    avg_loss = abs(losses.mean()) if not losses.empty else 0
                    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
                    
                    # --- PROFIT FACTOR ---
                    gross_profit = wins.sum() if not wins.empty else 0
                    gross_loss = abs(losses.sum()) if not losses.empty else 1 
                    profit_factor = gross_profit / gross_loss
                    
                    # --- NEW METRICS: SHARPE & CAGR (v139.0) ---
                    sharpe, cagr = calculate_portfolio_metrics(expired_df, acct_capital)
                    
                    m1, m2, m3, m4, m5 = st.columns(5)
                    m1.metric("💰 Banked Profit", f"${realized_pnl:,.0f}")
                    m2.metric("⚖️ Profit Factor", f"{profit_factor:.2f}")
                    m3.metric("📈 CAGR", f"{cagr:.1f}%")
                    m4.metric("📊 Sharpe Ratio", f"{sharpe:.2f}")
                    m5.metric("🔮 Total Projected", f"${realized_pnl+floating_pnl:,.0f}")
                else:
                    st.info("Need closed trades for deep dive.")
            except Exception as e: st.error(f"Error calculating metrics: {e}")
            st.divider()

        # --- CLOSED TRADE HISTORY ---
        if not expired_df.empty:
            with st.expander("📜 Detailed Trade History (Closed Trades)", expanded=False):
                # Columns: Date In, Date Out, Days, Name, Strategy, Debit, PnL, ROI%, Ann ROI%
                hist_cols = ['Entry Date', 'Exit Date', 'Days Held', 'Name', 'Strategy', 'Debit', 'P&L', 'ROI', 'Ann. ROI']
                
                # Format dates
                hist_view = expired_df[hist_cols].copy()
                hist_view['Entry Date'] = hist_view['Entry Date'].dt.date
                hist_view['Exit Date'] = hist_view['Exit Date'].dt.date
                
                st.dataframe(
                    hist_view.style
                    .format({
                        'Debit': "${:,.0f}", 'P&L': "${:,.0f}", 
                        'ROI': "{:.2f}%", 'Ann. ROI': "{:.2f}%"
                    })
                    .map(lambda x: 'color: green' if x > 0 else 'color: red', subset=['P&L', 'ROI', 'Ann. ROI']),
                    use_container_width=True
                )

            st.markdown("### 🏆 Closed Trade Performance")
            expired_df['Cap_Days'] = expired_df['Debit'] * expired_df['Days Held'].clip(lower=1)
            perf_agg = expired_df.groupby('Strategy').agg({
                'P&L': 'sum', 'Debit': 'sum', 'Cap_Days': 'sum', 'ROI': 'mean', 'id': 'count'
            }).reset_index()
            wins = expired_df[expired_df['P&L'] > 0].groupby('Strategy')['id'].count().reset_index(name='Wins')
            perf_agg = perf_agg.merge(wins, on='Strategy', how='left').fillna(0)
            perf_agg['Win Rate'] = perf_agg['Wins'] / perf_agg['id']
            perf_agg['Ann. TWR %'] = (perf_agg['P&L'] / perf_agg['Cap_Days']) * 365 * 100
            perf_agg['Simple Return %'] = (perf_agg['P&L'] / perf_agg['Debit']) * 100
            
            # Estimate Trade Sharpe (Mean ROI / Std ROI * sqrt(Trades))
            # Group by strategy to get StdDev of ROI
            std_roi = expired_df.groupby('Strategy')['ROI'].std().reset_index(name='Std_ROI')
            perf_agg = perf_agg.merge(std_roi, on='Strategy', how='left').fillna(1)
            perf_agg['Sharpe'] = (perf_agg['ROI'] / perf_agg['Std_ROI']) * np.sqrt(perf_agg['id'])
            
            perf_display = perf_agg[['Strategy', 'id', 'Win Rate', 'P&L', 'Debit', 'Simple Return %', 'Ann. TWR %', 'ROI', 'Sharpe']].copy()
            perf_display.columns = ['Strategy', 'Trades', 'Win Rate', 'Total P&L', 'Total Volume', 'Simple Return %', 'Ann. TWR %', 'Avg Trade ROI', 'Sharpe']
            
            total_pnl = perf_display['Total P&L'].sum()
            total_vol = perf_display['Total Volume'].sum()
            total_cap_days = perf_agg['Cap_Days'].sum()
            total_trades = perf_display['Trades'].sum()
            total_wins = perf_agg['Wins'].sum()
            total_win_rate = total_wins / total_trades if total_trades > 0 else 0
            total_simple_ret = (total_pnl / total_vol * 100) if total_vol > 0 else 0
            total_twr = (total_pnl / total_cap_days * 365 * 100) if total_cap_days > 0 else 0
            avg_trade_roi = expired_df['ROI'].mean()
            
            total_row = pd.DataFrame({
                'Strategy': ['TOTAL'], 'Trades': [total_trades], 'Win Rate': [total_win_rate],
                'Total P&L': [total_pnl], 'Total Volume': [total_vol], 'Simple Return %': [total_simple_ret],
                'Ann. TWR %': [total_twr], 'Avg Trade ROI': [avg_trade_roi], 'Sharpe': [0]
            })
            perf_display = pd.concat([perf_display, total_row], ignore_index=True)

            st.dataframe(perf_display.style.format({'Win Rate': "{:.1%}", 'Total P&L': "${:,.0f}", 'Total Volume': "${:,.0f}", 'Simple Return %': "{:.2f}%", 'Ann. TWR %': "{:.2f}%", 'Avg Trade ROI': "{:.2f}%", 'Sharpe': "{:.2f}"}).map(lambda x: 'color: green' if x > 0 else 'color: red', subset=['Total P&L', 'Simple Return %', 'Ann. TWR %', 'Avg Trade ROI', 'Sharpe']).apply(lambda x: ['background-color: #d1d5db; color: black; font-weight: bold' if x.name == len(perf_display)-1 else '' for _ in x], axis=1), use_container_width=True)
            
            # --- NEW: CAPITAL EFFICIENCY CHART (v134.0) ---
            st.subheader("🚀 Efficiency Showdown: Active vs Historical")
            st.caption("Are current campaigns outperforming your historical average? (Metric: Annualized Return on Invested Capital)")

            # 1. Calculate Active Efficiency
            active_eff_df = pd.DataFrame()
            if not active_df.empty:
                active_df['Cap_Days'] = active_df['Debit'] * active_df['Days Held'].clip(lower=1)
                active_agg = active_df.groupby('Strategy')[['P&L', 'Cap_Days']].sum().reset_index()
                # Calculate TWR-equivalent for Active
                active_agg['Return %'] = (active_agg['P&L'] / active_agg['Cap_Days']) * 365 * 100
                active_agg['Type'] = 'Active (Current)'
                active_eff_df = active_agg[['Strategy', 'Return %', 'Type']]

            # 2. Calculate Historical Efficiency (using perf_agg from above)
            hist_eff_df = pd.DataFrame()
            if not perf_agg.empty:
                hist_eff = perf_agg[['Strategy', 'Ann. TWR %']].copy()
                hist_eff.rename(columns={'Ann. TWR %': 'Return %'}, inplace=True)
                hist_eff['Type'] = 'Historical (Closed)'
                hist_eff_df = hist_eff

            # 3. Combine and Plot
            if not active_eff_df.empty or not hist_eff_df.empty:
                combined_eff = pd.concat([active_eff_df, hist_eff_df], ignore_index=True)
                combined_eff = combined_eff[combined_eff['Strategy'] != 'TOTAL']
                
                fig_compare = px.bar(combined_eff, x='Strategy', y='Return %', color='Type', barmode='group',
                                        title="Capital Efficiency Comparison (Annualized Return)",
                                        color_discrete_map={'Active (Current)': '#00CC96', 'Historical (Closed)': '#636EFA'},
                                        text='Return %')
                fig_compare.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                fig_compare.update_yaxes(title="Annualized Return (%)")
                st.plotly_chart(fig_compare, use_container_width=True)
            else:
                st.info("Insufficient data for comparison.")

            st.subheader("💰 Profit Anatomy: Call vs Put Contribution")
            viz_df = expired_df.sort_values('Exit Date')
            fig_anatomy = go.Figure()
            fig_anatomy.add_trace(go.Bar(x=viz_df['Name'], y=viz_df['Put P&L'], name='Put Side', marker_color='#EF553B'))
            fig_anatomy.add_trace(go.Bar(x=viz_df['Name'], y=viz_df['Call P&L'], name='Call Side', marker_color='#00CC96'))
            fig_anatomy.update_layout(barmode='relative', title='PnL Breakdown per Trade (Red=Puts, Green=Calls)', xaxis_tickangle=-45)
            st.plotly_chart(fig_anatomy, use_container_width=True)

    with an_trends:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🕵️ Root Cause Analysis")
            expired_wins = df[(df['Status'] == 'Expired') & (df['P&L'] > 0)]
            active_trades = df[df['Status'] == 'Active']
            if not expired_wins.empty and not active_trades.empty:
                avg_win_debit = expired_wins.groupby('Strategy')['Debit/Lot'].mean().reset_index()
                avg_act_debit = active_trades.groupby('Strategy')['Debit/Lot'].mean().reset_index()
                avg_win_debit['Type'] = 'Winning History'; avg_act_debit['Type'] = 'Active (Current)'
                comp_df = pd.concat([avg_win_debit, avg_act_debit])
                fig_price = px.bar(comp_df, x='Strategy', y='Debit/Lot', color='Type', barmode='group', title="Entry Price per Lot Comparison", color_discrete_map={'Winning History': 'green', 'Active (Current)': 'orange'})
                st.plotly_chart(fig_price, use_container_width=True)
            else: st.info("Need more data.")
        with col2:
            st.subheader("⚖️ Profit Drivers (Puts vs Calls)")
            expired = df[df['Status'] == 'Expired'].copy()
            if not expired.empty:
                leg_agg = expired.groupby('Strategy')[['Put P&L', 'Call P&L']].sum().reset_index()
                fig_legs = px.bar(leg_agg, x='Strategy', y=['Put P&L', 'Call P&L'], title="Profit Source Split", color_discrete_map={'Put P&L': '#EF553B', 'Call P&L': '#00CC96'})
                st.plotly_chart(fig_legs, use_container_width=True)
            else: st.info("No closed trades.")
        st.divider()
        if not expired_df.empty:
            ec_df = expired_df.dropna(subset=["Exit Date"]).sort_values("Exit Date").copy()
            ec_df['Cumulative P&L'] = ec_df['P&L'].cumsum()
            fig = px.line(ec_df, x='Exit Date', y='Cumulative P&L', title="Realized Equity Curve", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        st.divider()
        hm1, hm2, hm3 = st.tabs(["🗓️ Seasonality", "⏳ Duration", "📅 Entry Day"])
        if not expired_df.empty:
            exp_hm = expired_df.dropna(subset=['Exit Date']).copy()
            exp_hm['Month'] = exp_hm['Exit Date'].dt.month_name(); exp_hm['Year'] = exp_hm['Exit Date'].dt.year
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

    with an_risk:
        r_corr, r_mae = st.tabs(["🔗 Correlation Matrix", "📉 MAE vs MFE (Edge Analysis)"])
        
        with r_corr:
            st.subheader("Strategy Correlation (Daily P&L)")
            snaps = load_snapshots()
            if not snaps.empty:
                strat_daily = snaps.groupby(['snapshot_date', 'strategy'])['pnl'].sum().reset_index()
                pivoted = strat_daily.pivot_table(index='snapshot_date', columns='strategy', values='pnl', aggfunc='sum')
                daily_changes = pivoted.diff().fillna(0)
                if len(daily_changes.columns) > 1:
                    corr_matrix = daily_changes.corr()
                    fig_corr = px.imshow(corr_matrix, text_auto=".2f", aspect="auto", color_continuous_scale="RdBu", title="Strategy Correlation (Daily P&L Movement)", labels=dict(color="Correlation"))
                    st.plotly_chart(fig_corr, use_container_width=True)
                else: st.info("Not enough concurrent strategies to calculate correlation.")
            else: st.info("Insufficient snapshot data for correlation.")

        with r_mae:
            st.subheader("Excursion Analysis: Pain (MAE) vs Potential (MFE)")
            st.caption("MAE = Max Drawdown. MFE = Max Unrealized Profit. Ideally, your Final P&L should be close to your MFE.")
            
            mae_view = st.radio("View:", ["Closed Trades Only (Final Result)", "Include Active Trades (Current Drawdown)"], horizontal=True)
            
            if not snaps.empty and not df.empty:
                excursion_df = snaps.groupby('trade_id')['pnl'].agg(['min', 'max']).reset_index()
                excursion_df.rename(columns={'min': 'MAE', 'max': 'MFE'}, inplace=True)
                
                merged_mae = df.merge(excursion_df, left_on='id', right_on='trade_id', how='inner')
                viz_mae = merged_mae if "Include Active" in mae_view else merged_mae[merged_mae['Status'] == 'Expired'].copy()
                
                if not viz_mae.empty:
                    mae_c1, mae_c2 = st.columns(2)
                    
                    with mae_c1:
                        fig_mae_scat = px.scatter(viz_mae, x='MAE', y='P&L', color='Strategy', symbol='Status' if "Include Active" in mae_view else None,
                            hover_data=['Name', 'Days Held'], title="Drawdown (MAE) vs Final P&L", labels={'MAE': 'Max Drawdown ($)', 'P&L': 'Final Profit ($)'})
                        fig_mae_scat.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.5); fig_mae_scat.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.5)
                        st.plotly_chart(fig_mae_scat, use_container_width=True)
                    
                    with mae_c2:
                        # --- NEW: MFE SCATTER ---
                        # Filter MFE to only show positive MFE (otherwise it's just a losing trade that never went green)
                        viz_mfe = viz_mae[viz_mae['MFE'] > 0]
                        fig_mfe = px.scatter(viz_mfe, x='MFE', y='P&L', color='Strategy',
                            hover_data=['Name'], title="Potential (MFE) vs Final P&L", labels={'MFE': 'Max Profit Reached ($)', 'P&L': 'Realized Profit ($)'})
                        # Add perfect execution line (y=x)
                        if not viz_mfe.empty:
                             max_val = max(viz_mfe['MFE'].max(), viz_mfe['P&L'].max())
                             fig_mfe.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val, line=dict(color="green", dash="dot"))
                        st.plotly_chart(fig_mfe, use_container_width=True)
                        st.caption("Points BELOW the dotted line mean you gave back profits (Greed). Points ON the line mean perfect exit.")
                else: st.warning("No data available.")
            else: st.info("Need snapshot data.")

    with an_decay:
        st.subheader("🧬 Trade Life Cycle & Decay")
        snaps = load_snapshots()
        if not snaps.empty:
            decay_strat = st.selectbox("Select Strategy for Decay", snaps['strategy'].unique(), key="decay_strat")
            strat_snaps = snaps[snaps['strategy'] == decay_strat].copy()
            if not strat_snaps.empty:
                fig_pnl = px.line(strat_snaps, x='days_held', y='pnl', color='name', title=f"Trade Life Cycle: PnL Trajectory ({decay_strat})", labels={'days_held': 'Days Held', 'pnl': 'P&L ($)'}, markers=True)
                st.plotly_chart(fig_pnl, use_container_width=True)
                
                # --- NEW: GAMMA RISK PROFILE ---
                st.markdown("##### ☢️ Gamma Risk Profile")
                
                if 'gamma' in strat_snaps.columns and strat_snaps['gamma'].abs().sum() > 0:
                    # Calculate Gamma/Theta Ratio
                    # We want absolute ratio: Risk per unit of income
                    strat_snaps['Gamma/Theta'] = np.where(strat_snaps['theta'] != 0, 
                                                          (strat_snaps['gamma'] * 100) / strat_snaps['theta'], 
                                                          0)
                    
                    fig_risk = px.line(strat_snaps, x='days_held', y='Gamma/Theta', color='name',
                                      title=f"Explosion Ratio (Gamma Risk per unit of Theta Income)",
                                      labels={'days_held': 'Days Held', 'Gamma/Theta': 'Gamma% / Theta$ Ratio'})
                    st.plotly_chart(fig_risk, use_container_width=True)
                    st.info("ℹ️ **Interpretation:** A spike in this ratio means you are taking exponentially more risk for the same income. Valid for Income Strategies (130/160, M200).")
                else:
                    st.warning("⚠️ Gamma history unavailable. This chart will populate as you sync new daily data.")

                # Standard Decay Analysis
                def get_theta_anchor(group):
                    earliest = group.sort_values('days_held').iloc[0]
                    return earliest['theta'] if earliest['theta'] > 0 else group['theta'].max()
                
                anchor_map = strat_snaps.groupby('trade_id').apply(get_theta_anchor)
                strat_snaps['Theta_Anchor'] = strat_snaps['trade_id'].map(anchor_map)
                strat_snaps['Theta_Expected'] = strat_snaps['Theta_Anchor'] * (1 - strat_snaps['days_held'] / 45)
                strat_snaps = strat_snaps[(strat_snaps['Theta_Anchor'] > 0) & (strat_snaps['theta'] != 0) & (strat_snaps['days_held'] < 60)]
                
                if not strat_snaps.empty:
                    d1, d2 = st.columns(2)
                    with d1:
                        fig_theta = go.Figure()
                        for trade_id in strat_snaps['trade_id'].unique():
                            trade_data = strat_snaps[strat_snaps['trade_id'] == trade_id].sort_values('days_held')
                            fig_theta.add_trace(go.Scatter(x=trade_data['days_held'], y=trade_data['theta'], mode='lines+markers', name=f"{trade_data['name'].iloc[0][:15]} (Actual)", line=dict(width=2), showlegend=True))
                            fig_theta.add_trace(go.Scatter(x=trade_data['days_held'], y=trade_data['Theta_Expected'], mode='lines', name=f"{trade_data['name'].iloc[0][:15]} (Expected)", line=dict(dash='dash', width=1), opacity=0.5, showlegend=False))
                        fig_theta.update_layout(title=f"Theta: Actual vs Expected ({decay_strat})", xaxis_title="Days Held", yaxis_title="Theta ($)", hovermode='x unified')
                        st.plotly_chart(fig_theta, use_container_width=True)
                    with d2:
                        fig_delta = px.scatter(strat_snaps, x='days_held', y='delta', color='name', title=f"Delta Drift: {decay_strat}", labels={'days_held': 'Days', 'delta': 'Delta'}, trendline="lowess")
                        st.plotly_chart(fig_delta, use_container_width=True)
                else: st.warning("Insufficient data after filtering.")
        else: st.info("Upload daily active files to build decay history.")

    with an_rolls: 
        st.subheader("🔄 Roll Campaign Analysis")
        rolled_trades = df[df['Parent ID'].notna() & (df['Parent ID'] != "")].copy()
        if not rolled_trades.empty:
            campaign_summary = []
            for parent in rolled_trades['Parent ID'].unique():
                if not parent: continue
                campaign = df[(df['id'] == parent) | (df['Parent ID'] == parent)]
                if campaign.empty: continue
                campaign_summary.append({'Campaign': parent[:15], 'Total P&L': campaign['P&L'].sum(), 'Total Days': campaign['Days Held'].sum(), 'Legs': len(campaign), 'Avg P&L/Leg': campaign['P&L'].mean()})
            
            if campaign_summary:
                camp_df = pd.DataFrame(campaign_summary)
                st.dataframe(camp_df.style.format({'Total P&L': '${:,.0f}', 'Avg P&L/Leg': '${:,.0f}'}), use_container_width=True)
                avg_single = expired_df[expired_df['Parent ID'].isna() | (expired_df['Parent ID'] == "")]['P&L'].mean()
                avg_rolled = camp_df['Total P&L'].mean()
                c1, c2 = st.columns(2)
                c1.metric("Avg Single Trade P&L", f"${avg_single:,.0f}")
                c2.metric("Avg Roll Campaign P&L", f"${avg_rolled:,.0f}", delta=f"{avg_rolled-avg_single:,.0f}")
                if avg_rolled > avg_single: st.success(f"✅ Rolling WORKS: Rolled trades outperform single trades on average.")
                else: st.warning(f"⚠️ Rolling HURTS: Consider taking losses earlier.")
        else: st.info("No rolled trades linked via Parent ID yet. Use the 'Journal' tab to link trades.")

# --- NEW TAB: AI & INTELLIGENCE ---
with tab_ai:
    st.markdown("### 🧠 The Quant Brain (Beta)")
    st.caption("Self-learning insights based on your specific trading history.")
    
    if df.empty or expired_df.empty:
        st.info("👋 Need more historical data to power the AI engine.")
    else:
        active_trades = df[df['Status'].isin(['Active', 'Missing'])].copy()
        
        # --- NEW: CALIBRATION CONTROLS (v137.0) ---
        with st.expander("⚙️ Calibration & Thresholds", expanded=False):
            c_set1, c_set2, c_set3 = st.columns(3)
            with c_set1:
                st.markdown("**📉 Rot Detector**")
                rot_threshold = st.slider("Efficiency Drop Threshold %", 10, 90, 50, help="Alert if current efficiency is X% of historical average.") / 100.0
                min_days_rot = st.number_input("Min Days to Check", 5, 60, 10)
            with c_set2:
                st.markdown("**🔮 Prediction Logic**")
                prob_high = st.slider("High Confidence Threshold", 60, 95, 75, help="Trades above this % get 'PRESS WINNER' label.")
                prob_low = st.slider("Low Confidence Threshold", 10, 50, 40)
            with c_set3:
                st.markdown("**🎯 Exit Targets**")
                exit_percentile = st.slider("Optimal Exit Percentile", 50, 95, 75, help="Percentile of historical max wins to target.") / 100.0

        # 1. Predictive Engine
        st.subheader("🔮 Win Probability Forecast (KNN Model)")
        
        # Filter Logic
        strategies_avail = sorted(active_trades['Strategy'].unique().tolist())
        selected_strat_ai = st.selectbox("Filter by Strategy", ["All"] + strategies_avail, key="ai_strat_filter")
        
        if selected_strat_ai != "All":
            ai_view_df = active_trades[active_trades['Strategy'] == selected_strat_ai].copy()
        else:
            ai_view_df = active_trades.copy()

        if not ai_view_df.empty:
            preds = generate_trade_predictions(ai_view_df, expired_df, prob_low, prob_high)
            if not preds.empty:
                # Visual 1: Scatter of Probability vs Reward (Layout Fix: 40:60 Ratio)
                c_p1, c_p2 = st.columns([2, 3]) 
                with c_p1:
                    fig_pred = px.scatter(
                        preds, 
                        x="Win Prob %", 
                        y="Expected PnL", 
                        color="Confidence",
                        size="Confidence",
                        hover_data=["Trade Name", "Strategy"],
                        color_continuous_scale="RdYlGn",
                        title="Risk/Reward Map"
                    )
                    # Add quadrants
                    fig_pred.add_vline(x=50, line_dash="dash", line_color="gray")
                    fig_pred.add_hline(y=0, line_dash="dash", line_color="gray")
                    st.plotly_chart(fig_pred, use_container_width=True)
                
                with c_p2:
                    st.dataframe(
                        preds.style.format({
                            'Win Prob %': "{:.1f}%", 'Expected PnL': "${:,.0f}", 'Confidence': "{:.0f}%"
                        }).map(lambda v: 'color: green; font-weight: bold' if v > prob_high else ('color: red; font-weight: bold' if v < prob_low else 'color: orange'), subset=['Win Prob %']),
                        use_container_width=True
                    )
                    st.info(f"💡 **'PRESS WINNER'**: Based on Greeks, these active trades match historical winners with >{prob_high}% win rate. Consider holding.")
            else: st.info("Not enough closed trades with matching Greek profiles for prediction.")
        else: st.info("No active trades to forecast.")
        
        st.divider()
        
        c_ai_1, c_ai_2 = st.columns(2)
        
        with c_ai_1:
            st.subheader("📉 Capital Rot Detector")
            if not active_trades.empty:
                rot_df = check_rot_and_efficiency(active_trades, expired_df, rot_threshold, min_days_rot)
                if not rot_df.empty:
                    # Visual 2: Efficiency Comparison
                    rot_viz = rot_df.copy()
                    fig_rot = go.Figure()
                    fig_rot.add_trace(go.Bar(x=rot_viz['Trade'], y=rot_viz['Raw Current'], name='Current Speed', marker_color='#EF553B'))
                    fig_rot.add_trace(go.Bar(x=rot_viz['Trade'], y=rot_viz['Raw Baseline'], name='Baseline Speed', marker_color='gray'))
                    fig_rot.update_layout(title="Capital Velocity Lag ($/Day/1k)", barmode='group')
                    
                    st.plotly_chart(fig_rot, use_container_width=True)
                    st.dataframe(rot_df[['Trade', 'Strategy', 'Current Speed', 'Baseline Speed', 'Status']], use_container_width=True)
                else: st.success("✅ Capital is moving efficiently. No rot detected.")
        
        with c_ai_2:
            st.subheader(f"🎯 Optimal Exit Zones ({int(exit_percentile*100)}th Percentile)")
            targets = get_dynamic_targets(expired_df, exit_percentile)
            if targets:
                # Visual 3: Distribution of Wins
                winners = expired_df[expired_df['P&L'] > 0]
                if not winners.empty:
                    fig_exit = px.box(winners, x="Strategy", y="P&L", points="all", title="Historical Win Distribution & Targets")
                    st.plotly_chart(fig_exit, use_container_width=True)
                
                # Table below
                target_data = []
                for s, v in targets.items():
                    target_data.append({'Strategy': s, 'Median Win': v['Median Win'], 'Optimal Exit': v['Optimal Exit']})
                t_df = pd.DataFrame(target_data)
                st.dataframe(t_df.style.format({'Median Win': '${:,.0f}', 'Optimal Exit': '${:,.0f}'}), use_container_width=True)
            else: st.info("Need more winning trades to calculate optimal zones.")

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
    1.  **Stability Check:** Monitor **Stability** Ratio.
        * **> 1.0 (Green):** Fortress. Trade is safe.
        * **< 0.25 (Red):** Coin Flip. Trade is directional gambling.
    2.  **Volatility Gate:** Check VIX before entry. Ideal: 14–22. Skip if VIX exploded >10% in last 48h.
    3.  **Loss Definition:** A trade that is early and red but *structurally intact* is **NOT** a losing trade. It is just *unripe*.
    4.  **Efficiency Check:** Monitor **Theta Eff.** (> 1.0 means you are capturing decay efficiently).
    """)
    st.divider()
    st.caption("Allantis Trade Guardian v140.0 (Fix: Metrics Error & New ROI% Columns Added)")
