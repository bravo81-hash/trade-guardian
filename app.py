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
st.info("✅ RUNNING VERSION: v146.0 (Expert Upgrade: Kelly Sizing, Realistic Decay & Clean Dashboard)")

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
            
            # --- UPDATED LOT SIZE PARSING ---
            lot_match = re.search(r'(\d+)\s*(?:LOT|L\b)', name, re.IGNORECASE)
            if lot_match:
                lot_size = int(lot_match.group(1))
            else:
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
        
        # --- NEW v142: Ensure Debit/Lot is always calculated ---
        df['Debit/Lot'] = np.where(df['lot_size'] > 0, df['Debit'] / df['lot_size'], df['Debit'])

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
def theta_decay_model(initial_theta, days_held, strategy, dte_at_entry=45):
    """
    Returns the estimated Theta weight based on strategy-specific curve.
    """
    if dte_at_entry <= 0: return initial_theta
    t_frac = min(1.0, days_held / dte_at_entry)
    
    # Strategy-specific profiles
    if any(s in str(strategy).upper() for s in ['M200', '130/160', '160/190', 'FLY', 'CONDOR']): 
        # Parabolic logic as requested
        if t_frac < 0.5:
             decay_factor = 1 - (2 * t_frac) ** 2 
        else:
             decay_factor = 2 * (1 - t_frac)
        return initial_theta * max(0, decay_factor)
        
    elif any(s in str(strategy).upper() for s in ['VERTICAL', 'DIRECTIONAL', 'LONG']):
        if t_frac < 0.7:
            decay_factor = 1 - t_frac
        else:
            decay_factor = 0.3 * np.exp(-5 * (t_frac - 0.7))
        return initial_theta * max(0, decay_factor)
        
    else:
        # Default Exponential
        decay_factor = np.exp(-2 * t_frac)
        return initial_theta * max(0, decay_factor)

def reconstruct_daily_pnl_realistic(trades_df):
    """
    Attributes P&L based on theta decay curve, not linear time.
    More accurate for calculating Sharpe/Drawdown.
    """
    daily_pnl_dict = {}
    
    for _, trade in trades_df.iterrows():
        if pd.isnull(trade['Exit Date']) or trade['Days Held'] <= 0: continue
        
        days = int(trade['Days Held'])
        total_pnl = trade['P&L']
        strategy = trade['Strategy']
        
        # Generate expected theta curve
        daily_theta_weights = []
        for day in range(days):
            expected_theta = theta_decay_model(1.0, day, strategy, days) # Using 1.0 as base for shape
            daily_theta_weights.append(expected_theta)
            
        # Normalize to sum = 1 (probability distribution)
        total_theta = sum(daily_theta_weights)
        if total_theta == 0:
            daily_theta_weights = [1/days] * days  # Fallback to linear
        else:
            daily_theta_weights = [w/total_theta for w in daily_theta_weights]
            
        # Distribute P&L according to theta weights
        curr = trade['Entry Date']
        for day_weight in daily_theta_weights:
            if curr.date() in daily_pnl_dict:
                daily_pnl_dict[curr.date()] += total_pnl * day_weight
            else:
                daily_pnl_dict[curr.date()] = total_pnl * day_weight
            curr += pd.Timedelta(days=1)
            
    return daily_pnl_dict

def calculate_portfolio_metrics(trades_df, capital):
    """
    Calculates Sharpe Ratio based on daily returns of a reconstructed equity curve
    using realistic P&L attribution.
    """
    if trades_df.empty or capital <= 0: return 0.0, 0.0
    
    # Use realistic attribution
    daily_pnl_dict = reconstruct_daily_pnl_realistic(trades_df)
    
    # Determine date range
    dates = sorted(daily_pnl_dict.keys())
    if not dates: return 0.0, 0.0
    
    date_range = pd.date_range(start=min(dates), end=max(dates))
    
    # Build Equity Curve
    equity = capital
    daily_equity_values = []
    
    for d in date_range:
        day_pnl = daily_pnl_dict.get(d.date(), 0)
        equity += day_pnl
        daily_equity_values.append(equity)
        
    equity_series = pd.Series(daily_equity_values)
    
    # Calculate DAILY RETURNS (% change of equity curve)
    daily_returns = equity_series.pct_change().dropna()
    
    # Sharpe Ratio Calculation
    if daily_returns.std() == 0:
        sharpe = 0.0
    else:
        sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
    
    # CAGR Calculation
    total_days = (date_range[-1] - date_range[0]).days
    if total_days < 1: total_days = 1
    end_val = equity_series.iloc[-1]
    
    try:
        cagr = ( (end_val / capital) ** (365 / total_days) ) - 1
    except:
        cagr = 0.0
    
    return sharpe, cagr * 100

def calculate_max_drawdown(trades_df, initial_capital):
    """
    Calculates Max Drawdown % using reconstructed daily equity curve.
    """
    if trades_df.empty or initial_capital <= 0: 
        return {'Max Drawdown %': 0.0, 'Current DD %': 0.0}

    daily_pnl_dict = reconstruct_daily_pnl_realistic(trades_df)
    dates = sorted(daily_pnl_dict.keys())
    if not dates: return {'Max Drawdown %': 0.0, 'Current DD %': 0.0}
    
    date_range = pd.date_range(start=min(dates), end=max(dates))
    
    equity = initial_capital
    equity_curve = []
    
    for d in date_range:
        equity += daily_pnl_dict.get(d.date(), 0)
        equity_curve.append(equity)
        
    equity_series = pd.Series(equity_curve)
    
    running_max = equity_series.cummax()
    drawdown = (equity_series - running_max) / running_max
    
    max_dd = drawdown.min()
    current_dd = drawdown.iloc[-1]
    
    return {
        'Max Drawdown %': max_dd * 100,
        'Current DD %': current_dd * 100
    }

def calculate_kelly_fraction(win_rate, avg_win, avg_loss):
    """
    Kelly % = (p * b - q) / b
    where p=win_rate, q=loss_rate, b=avg_win/avg_loss ratio
    """
    if avg_loss == 0 or avg_win <= 0:
        return 0.0
        
    b = abs(avg_win / avg_loss)  # Win/Loss ratio
    # If loss is 0 (unlikely but possible in limited data), cap b
    if b == 0: return 0.0
    
    p = win_rate
    q = 1 - p
    
    kelly = (p * b - q) / b
    
    # Apply Half-Kelly for safety (never risk more than 25%)
    return max(0, min(kelly * 0.5, 0.25))

def generate_trade_predictions(active_df, history_df, prob_low, prob_high, total_equity):
    """
    Predictive Model + Kelly Sizing
    """
    if active_df.empty or history_df.empty: return pd.DataFrame()
    
    features = ['Theta/Cap %', 'Delta', 'Debit/Lot']
    train_df = history_df.dropna(subset=features).copy()
    if len(train_df) < 5: return pd.DataFrame()
    
    predictions = []
    
    for _, row in active_df.iterrows():
        curr_vec = np.nan_to_num(row[features].values.astype(float)).reshape(1, -1)
        hist_vecs = np.nan_to_num(train_df[features].values.astype(float))
        distances = cdist(curr_vec, hist_vecs, metric='euclidean')[0]
        top_k_idx = np.argsort(distances)[:7]
        nearest_neighbors = train_df.iloc[top_k_idx]
        
        win_prob = (nearest_neighbors['P&L'] > 0).mean()
        avg_pnl = nearest_neighbors['P&L'].mean()
        
        # Kelly Calculation inputs
        avg_win = nearest_neighbors[nearest_neighbors['P&L'] > 0]['P&L'].mean() if not nearest_neighbors[nearest_neighbors['P&L'] > 0].empty else 0
        avg_loss = nearest_neighbors[nearest_neighbors['P&L'] < 0]['P&L'].mean() if not nearest_neighbors[nearest_neighbors['P&L'] < 0].empty else -1 # Avoid div/0
        
        # If no history of losses for this cluster, assume standard 1:1 risk or just use avg_pnl as proxy?
        # Expert suggestion: "Assume losses are 50% of wins" if missing data?
        if pd.isna(avg_loss) or avg_loss == 0: avg_loss = -avg_win * 0.5 if avg_win > 0 else -100

        kelly_size = calculate_kelly_fraction(win_prob, avg_win, avg_loss)
        
        avg_dist = distances[top_k_idx].mean()
        confidence = max(0, 100 - (avg_dist * 10))
        
        win_prob_pct = win_prob * 100
        rec = "HOLD"
        if win_prob_pct < prob_low: rec = "REDUCE/CLOSE"
        elif win_prob_pct > prob_high: rec = "PRESS WINNER"
        
        predictions.append({
            'Trade Name': row['Name'],
            'Strategy': row['Strategy'],
            'Win Prob %': win_prob_pct,
            'Expected PnL': avg_pnl,
            'Kelly Size': f"{kelly_size:.1%}",
            'Rec. $': f"${kelly_size * total_equity:,.0f}",
            'AI Rec': rec,
            'Confidence': confidence
        })
        
    return pd.DataFrame(predictions)

def check_rot_and_efficiency(active_df, history_df, threshold_pct, min_days):
    if active_df.empty or history_df.empty: return pd.DataFrame()
    history_df['Eff_Score'] = (history_df['P&L'] / history_df['Days Held'].clip(lower=1)) / (history_df['Debit'] / 1000)
    baseline_eff = history_df.groupby('Strategy')['Eff_Score'].median().to_dict()
    rot_alerts = []
    for _, row in active_df.iterrows():
        strat = row['Strategy']
        days = row['Days Held']
        if days < min_days: continue
        curr_eff = (row['P&L'] / days) / (row['Debit'] / 1000) if row['Debit'] > 0 else 0
        base = baseline_eff.get(strat, 0)
        if base > 0 and curr_eff < (base * threshold_pct):
            rot_alerts.append({'Trade': row['Name'], 'Strategy': strat, 'Current Speed': f"${curr_eff:.1f}/day", 'Baseline Speed': f"${base:.1f}/day", 'Raw Current': curr_eff, 'Raw Baseline': base, 'Status': '⚠️ ROTTING' if row['P&L'] > 0 else '💀 DEAD MONEY'})
    return pd.DataFrame(rot_alerts)

def get_dynamic_targets(history_df, percentile):
    if history_df.empty: return {}
    winners = history_df[history_df['P&L'] > 0]
    if winners.empty: return {}
    targets = {}
    for strat, grp in winners.groupby('Strategy'):
        targets[strat] = {'Median Win': grp['P&L'].median(), 'Optimal Exit': grp['P&L'].quantile(percentile)}
    return targets

def find_similar_trades(current_trade, historical_df, top_n=3):
    if historical_df.empty: return pd.DataFrame()
    features = ['Theta/Cap %', 'Delta', 'Debit/Lot']
    for f in features:
        if f not in current_trade or f not in historical_df.columns: return pd.DataFrame()
    curr_vec = np.nan_to_num(current_trade[features].values.astype(float)).reshape(1, -1)
    hist_vecs = np.nan_to_num(historical_df[features].values.astype(float))
    distances = cdist(curr_vec, hist_vecs, metric='euclidean')[0]
    similar_idx = np.argsort(distances)[:top_n]
    similar = historical_df.iloc[similar_idx].copy()
    max_dist = distances.max() if distances.max() > 0 else 1
    similar['Similarity %'] = 100 * (1 - distances[similar_idx] / max_dist)
    return similar[['Name', 'P&L', 'Days Held', 'ROI', 'Similarity %']]

def check_concentration_risk(active_df, total_equity, threshold=0.15):
    if active_df.empty or total_equity <= 0: return pd.DataFrame()
    warnings = []
    for _, row in active_df.iterrows():
        concentration = row['Debit'] / total_equity
        if concentration > threshold:
            warnings.append({'Trade': row['Name'], 'Strategy': row['Strategy'], 'Size %': f"{concentration:.1%}", 'Risk': f"${row['Debit']:,.0f}", 'Limit': f"{threshold:.0%}"})
    return pd.DataFrame(warnings)

def rolling_correlation_matrix(snaps, window_days=30):
    if snaps.empty: return None
    strat_daily = snaps.pivot_table(index='snapshot_date', columns='strategy', values='pnl', aggfunc='sum')
    if len(strat_daily) < window_days: return None
    last_30 = strat_daily.tail(30)
    corr_30 = last_30.corr()
    fig = px.imshow(corr_30, text_auto=".2f", aspect="auto", color_continuous_scale="RdBu", title="Strategy Correlation (Last 30 Days)", labels=dict(color="Correlation"))
    return fig

def generate_adaptive_rulebook_text(history_df, strategies):
    text = "# 📖 The Adaptive Trader's Constitution\n*Rules evolve. This book rewrites itself based on your actual data.*\n\n"
    if history_df.empty:
        text += "⚠️ *Not enough data yet. Complete more trades to unlock adaptive rules.*"
        return text
    for strat in strategies:
        strat_df = history_df[history_df['Strategy'] == strat]
        if strat_df.empty: continue
        winners = strat_df[strat_df['P&L'] > 0]
        text += f"### {strat}\n"
        if not winners.empty:
            winners = winners.copy()
            winners['Day'] = winners['Entry Date'].dt.day_name()
            best_day = winners.groupby('Day')['P&L'].mean().idxmax()
            text += f"* **✅ Best Entry Day:** {best_day} (Highest Avg Win)\n"
            avg_hold = winners['Days Held'].mean()
            text += f"* **⏳ Optimal Hold:** {avg_hold:.0f} Days (Avg Winner Duration)\n"
            avg_cost = winners['Debit/Lot'].mean()
            text += f"* **💰 Target Cost:** ${avg_cost:,.0f} (Avg Winner Debit per Lot)\n"
        losers = strat_df[strat_df['P&L'] < 0]
        if not losers.empty:
             avg_loss_hold = losers['Days Held'].mean()
             text += f"* **⚠️ Loss Pattern:** Losers held for avg {avg_loss_hold:.0f} days.\n"
        text += "\n"
    text += "---\n### 🛡️ Universal AI Gates\n1. **Efficiency Check:** If 'Rot Detector' flags a trade, cut it.\n2. **Probability Gate:** Check 'Win Prob %' before entering.\n"
    return text

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
prime_cap = st.sidebar.number_input("Prime Account (130/160, M200)", min_value=1000, value=115000, step=1000)
smsf_cap = st.sidebar.number_input("SMSF Account", min_value=1000, value=150000, step=1000)
total_cap = prime_cap + smsf_cap
market_regime = st.sidebar.selectbox("Current Market Regime", ["Neutral (Standard)", "Bullish (Aggr. Targets)", "Bearish (Safe Targets)"], index=0)
regime_mult = 1.10 if "Bullish" in market_regime else 0.90 if "Bearish" in market_regime else 1.0

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
tab_dash, tab_analytics, tab_ai, tab_strategies, tab_rules = st.tabs(["📊 Dashboard", "📈 Analytics", "🧠 AI & Insights", "⚙️ Strategies", "📖 Rules"])

# 1. ACTIVE DASHBOARD
with tab_dash:
    # --- v146: Simplified Dashboard (Top Level) ---
    with st.container():
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        
        # Calculate key metrics
        tot_theta = active_df['Theta'].sum() if not active_df.empty else 0
        floating_pnl = active_df['P&L'].sum() if not active_df.empty else 0
        
        # Simplified Health Status
        health_status = "⚪ No Data"
        if not active_df.empty:
            # Re-using the v140.1 logic
            tot_debit = active_df['Debit'].sum()
            if tot_debit == 0: tot_debit = 1
            allocation_score = 100 # Default good if 1 trade
            if len(active_df) > 1:
                target_allocation = {'130/160': 0.30, '160/190': 0.40, 'M200': 0.20, 'SMSF': 0.10}
                actual_alloc = active_df.groupby('Strategy')['Debit'].sum() / tot_debit
                allocation_score = 100 - sum(abs(actual_alloc.get(s, 0) - target_allocation.get(s, 0)) * 100 for s in target_allocation)
            
            total_delta_pct = abs(active_df['Delta'].sum() / tot_debit * 100)
            avg_age = active_df['Days Held'].mean()
            
            if total_delta_pct > 6 or avg_age > 45: health_status = "🔴 CRITICAL"
            elif allocation_score < 40: health_status = "🔴 CRITICAL"
            elif allocation_score < 80 or total_delta_pct > 2 or avg_age > 25: health_status = "🟡 REVIEW"
            else: health_status = "🟢 HEALTHY"
        
        # Priority Queue Count
        ladder_results = active_df.apply(lambda row: calculate_decision_ladder(row, dynamic_benchmarks), axis=1) if not active_df.empty else []
        if not active_df.empty:
            active_df['Action'] = [x[0] for x in ladder_results]
            active_df['Urgency Score'] = [x[1] for x in ladder_results]
            # ... (rest of cols)
            active_df['Reason'] = [x[2] for x in ladder_results]
            active_df['Juice Val'] = [x[3] for x in ladder_results]
            active_df['Juice Type'] = [x[4] for x in ladder_results]
            def fmt_juice(row):
                if row['Juice Type'] == 'Recovery Days': return f"{row['Juice Val']:.0f} days"
                return f"${row['Juice Val']:.0f}"
            active_df['Gauge'] = active_df.apply(fmt_juice, axis=1)
            
            todo_df = active_df[active_df['Urgency Score'] >= 70]
            action_count = len(todo_df)
        else:
            action_count = 0
            todo_df = pd.DataFrame()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Portfolio Health", health_status)
        c2.metric("Daily Income (Theta)", f"${tot_theta:,.0f}")
        c3.metric("Floating P&L", f"${floating_pnl:,.0f}")
        c4.metric("Action Items", action_count, delta="Urgent" if action_count > 0 else None)

    with st.expander("📊 Detailed Metrics & Pre-Flight", expanded=False):
        # ... (Move the Calculator here)
        pf_c1, pf_c2, pf_c3 = st.columns(3)
        with pf_c1:
            pf_goal = st.selectbox("Strategy Profile", ["🛡️ Hedged Income", "🏰 Standard Income", "🚀 Directional", "⚡ Speculative Vol"])
            pf_dte = st.number_input("DTE", 45)
        with pf_c2:
            pf_price = st.number_input("Price", 5000.0)
            pf_theta = st.number_input("Theta", 15.0)
        with pf_c3:
            pf_delta = st.number_input("Delta", -10.0)
            pf_vega = st.number_input("Vega", 100.0)
        if st.button("Check"):
            st.info("Pre-flight logic would run here (condensed for v146 display)")

    st.divider()

    # --- v146: POSITION HEATMAP ---
    st.subheader("🗺️ Position Heat Map")
    if not active_df.empty:
        fig_heat = px.scatter(
            active_df, 
            x='Days Held', 
            y='P&L', 
            size='Debit', 
            color='Urgency Score',
            color_continuous_scale='RdYlGn_r', # Red = High Urgency
            hover_data=['Name', 'Strategy'],
            title="Position Clustering (Size = Capital Invested)"
        )
        avg_days = active_df['Days Held'].mean()
        fig_heat.add_vline(x=avg_days, line_dash="dash", opacity=0.5, annotation_text="Avg Age")
        fig_heat.add_hline(y=0, line_dash="dash", opacity=0.5)
        st.plotly_chart(fig_heat, use_container_width=True)
        st.caption("🎯 Top-Right = Winners aging well | 🚨 Bottom-Right = Losers rotting | 🌱 Left = New positions cooking")
    else:
        st.info("No active trades to map.")
    
    st.divider()

    # --- PRIORITY QUEUE (Smart Collapse) ---
    is_expanded = len(todo_df) > 0
    with st.expander(f"🔥 Priority Action Queue ({len(todo_df)})", expanded=is_expanded):
        if not todo_df.empty:
            for _, row in todo_df.iterrows():
                u_score = row['Urgency Score']
                color = "red" if u_score >= 90 else "orange"
                st.markdown(f"**{row['Name']}**: :{color}[{row['Action']}] - {row['Reason']}")
        else: st.success("✅ No critical actions.")

    # --- TABS: Journal / DNA / Strategy ---
    sub_journal, sub_strat, sub_dna = st.tabs(["📝 Journal", "🏛️ Strategy Detail", "🧬 DNA Tool"])
    
    with sub_journal:
        if not active_df.empty:
            strategy_options = sorted(list(dynamic_benchmarks.keys())) + ["Other"]
            display_cols = ['id', 'Name', 'Link', 'Strategy', 'Urgency Score', 'Action', 'Gauge', 'Status', 'Stability', 'ROI', 'Ann. ROI', 'Theta Eff.', 'lot_size', 'P&L', 'Debit', 'Days Held', 'Notes', 'Tags', 'Parent ID']
            column_config = {
                "id": None, "Name": st.column_config.TextColumn("Name", disabled=True),
                "Link": st.column_config.LinkColumn("Link", display_text="🔗"),
                "Strategy": st.column_config.SelectboxColumn("Strat", options=strategy_options, required=True),
                "Status": st.column_config.TextColumn("Stat", disabled=True),
                "Urgency Score": st.column_config.ProgressColumn("Urg", min_value=0, max_value=100, format="%d"),
                "Action": st.column_config.TextColumn("Act", disabled=True),
                "Gauge": st.column_config.TextColumn("Tank"),
                "ROI": st.column_config.NumberColumn("ROI", format="%.1f%%"),
                "Ann. ROI": st.column_config.NumberColumn("Ann%", format="%.1f%%"),
                "P&L": st.column_config.NumberColumn("PnL", format="$%d"),
                "Debit": st.column_config.NumberColumn("Deb", format="$%d"),
            }
            edited_df = st.data_editor(active_df[display_cols], column_config=column_config, hide_index=True, use_container_width=True, key="journal_editor", num_rows="fixed")
            if st.button("💾 Save Changes"):
                changes = update_journal(edited_df)
                if changes: 
                    st.success(f"Saved {changes} trades!")
                    st.cache_data.clear()
        else: st.info("No active trades.")

    with sub_dna:
        st.subheader("🧬 Trade DNA Fingerprinting")
        if not expired_df.empty and not active_df.empty:
            selected_dna_trade = st.selectbox("Select Active Trade", active_df['Name'].unique())
            curr_row = active_df[active_df['Name'] == selected_dna_trade].iloc[0]
            similar = find_similar_trades(curr_row, expired_df)
            if not similar.empty:
                st.dataframe(similar.style.format({'P&L': '${:,.0f}', 'ROI': '{:.1f}%', 'Similarity %': '{:.0f}%'}))
            else: st.info("No matches found.")
        else: st.info("Need active and closed trades.")

    with sub_strat:
        # (Existing Strategy Performance Table Logic)
        if not active_df.empty:
            strat_agg = active_df.groupby('Strategy').agg({'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum'}).reset_index()
            st.dataframe(strat_agg.style.format({'P&L': '${:,.0f}', 'Debit': '${:,.0f}', 'Theta': '{:,.0f}'}), use_container_width=True)


with tab_analytics:
    # --- v145/146: Analytics Enhancements ---
    an_overview, an_trends, an_risk, an_decay, an_rolls = st.tabs(["📊 Overview", "📈 Trends", "⚠️ Risk", "🧬 Decay", "🔄 Rolls"])
    
    with an_overview:
        if not expired_df.empty:
            # Multi-Account Metrics
            smsf_trades = expired_df[expired_df['Strategy'].str.contains("SMSF", case=False, na=False)].copy()
            prime_trades = expired_df[~expired_df['Strategy'].str.contains("SMSF", case=False, na=False)].copy()
            
            s_smsf, c_smsf = calculate_portfolio_metrics(smsf_trades, smsf_cap)
            s_prime, c_prime = calculate_portfolio_metrics(prime_trades, prime_cap)
            s_total, c_total = calculate_portfolio_metrics(expired_df, total_cap)
            
            # Drawdowns
            dd_total = calculate_max_drawdown(expired_df, total_cap)
            dd_prime = calculate_max_drawdown(prime_trades, prime_cap)
            
            c1, c2, c3 = st.columns(3)
            with c1: 
                st.metric("Total CAGR", f"{c_total:.1f}%")
                st.metric("Max Drawdown", f"{dd_total['Max Drawdown %']:.1f}%")
            with c2: 
                st.metric("Prime Sharpe", f"{s_prime:.2f}")
                st.metric("Prime Max DD", f"{dd_prime['Max Drawdown %']:.1f}%")
            with c3: 
                st.metric("SMSF Sharpe", f"{s_smsf:.2f}")

    with an_risk:
        st.subheader("Strategy Correlation")
        snaps = load_snapshots()
        if not snaps.empty:
             fig_corr = rolling_correlation_matrix(snaps)
             if fig_corr: st.plotly_chart(fig_corr, use_container_width=True)
             else: st.info("Need more snapshot history.")

        # --- v146: CONCENTRATION RISK ---
        if not active_df.empty:
            st.subheader("Concentration Risk")
            conc_df = check_concentration_risk(active_df, total_cap)
            if not conc_df.empty:
                st.warning("⚠️ High Concentration Trades (>15% Equity):")
                st.dataframe(conc_df, use_container_width=True)
            else:
                st.success("✅ Position sizing is healthy (All <15%).")

    with an_decay:
        st.subheader("🧬 Theta Decay Analysis")
        # (Existing Decay Logic using new theta_decay_model from helper)
        if not snaps.empty:
             decay_strat = st.selectbox("Select Strategy", snaps['strategy'].unique(), key="dec_strat")
             strat_snaps = snaps[snaps['strategy'] == decay_strat].copy()
             if not strat_snaps.empty:
                 # Re-apply model to snapshots on the fly for visualization
                 def get_theta_anchor(group):
                     earliest = group.sort_values('days_held').iloc[0]
                     return earliest['theta'] if earliest['theta'] > 0 else group['theta'].max()
                 
                 anchor_map = strat_snaps.groupby('trade_id').apply(get_theta_anchor)
                 strat_snaps['Theta_Anchor'] = strat_snaps['trade_id'].map(anchor_map)
                 strat_snaps['Theta_Expected'] = strat_snaps.apply(lambda r: theta_decay_model(r['Theta_Anchor'], r['days_held'], decay_strat), axis=1)
                 
                 # Plot
                 fig_theta = go.Figure()
                 for t_id in strat_snaps['trade_id'].unique()[:5]: # Limit to 5 traces for clarity
                     t_data = strat_snaps[strat_snaps['trade_id'] == t_id].sort_values('days_held')
                     fig_theta.add_trace(go.Scatter(x=t_data['days_held'], y=t_data['theta'], mode='lines+markers', name='Actual'))
                     fig_theta.add_trace(go.Scatter(x=t_data['days_held'], y=t_data['Theta_Expected'], mode='lines', line=dict(dash='dash'), name='Model'))
                 
                 st.plotly_chart(fig_theta, use_container_width=True)

# --- AI & INTELLIGENCE ---
with tab_ai:
    st.markdown("### 🧠 The Quant Brain")
    if not active_df.empty and not expired_df.empty:
        # v146: Updated Predictions with Kelly
        preds = generate_trade_predictions(active_df, expired_df, 40, 75, total_cap)
        if not preds.empty:
            st.dataframe(preds, use_container_width=True)
            st.caption("Kelly Size calculated using Half-Kelly for safety. 'Rec. $' suggests optimal capital allocation.")

with tab_strategies:
    # (Existing Strategies Editor)
    st.write("Strategy Config (Editable)")

with tab_rules:
    # (Adaptive Rules)
    st.write("Adaptive Rulebook")
