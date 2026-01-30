import streamlit as st

# --- GLOBAL STATE INIT ---
if 'velocity_stats' not in globals(): velocity_stats = {}
if 'mae_stats' not in globals(): mae_stats = {}
# -------------------------

import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import re
import json
import time
from datetime import datetime, timezone, timedelta
from openpyxl import load_workbook
from scipy import stats 
from scipy.spatial.distance import cdist 

# --- GOOGLE DRIVE IMPORTS ---
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
    GOOGLE_DEPS_INSTALLED = True
except ImportError:
    GOOGLE_DEPS_INSTALLED = False

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian (Mission Control)", layout="wide", page_icon="🛡️")
st.info("🚀 RUNNING VERSION: v148.1 (Mission Control + Full Analytics Restoration)")
st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE CONSTANTS ---
DB_NAME = "trade_guardian_v4.db"
SCOPES = ['https://www.googleapis.com/auth/drive']

# --- CLOUD SYNC ENGINE ---
class DriveManager:
    def __init__(self):
        self.creds = None
        self.service = None
        self.is_connected = False
        self.cached_file_id = None 
        if 'gcp_service_account' in st.secrets:
            try:
                service_account_info = st.secrets["gcp_service_account"]
                self.creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
                self.service = build('drive', 'v3', credentials=self.creds)
                self.is_connected = True
            except Exception as e: st.error(f"Cloud Config Error: {e}")

    def list_files_debug(self):
        if not self.is_connected: return []
        try:
            results = self.service.files().list(pageSize=10, fields="files(id, name, modifiedTime)").execute()
            return results.get('files', [])
        except: return []

    def find_db_file(self):
        if not self.is_connected: return None, None
        if self.cached_file_id:
            try:
                file = self.service.files().get(fileId=self.cached_file_id, fields='id,name').execute()
                return file['id'], file['name']
            except: self.cached_file_id = None 

        try:
            query_exact = f"name='{DB_NAME}' and trashed=false"
            results = self.service.files().list(q=query_exact, pageSize=1, fields="files(id, name)").execute()
            items = results.get('files', [])
            if items: 
                self.cached_file_id = items[0]['id']; return items[0]['id'], items[0]['name']

            query_fuzzy = "name contains 'trade_guardian' and name contains '.db' and trashed=false"
            results = self.service.files().list(q=query_fuzzy, pageSize=5, fields="files(id, name)").execute()
            items = results.get('files', [])
            if items:
                selected = items[0]
                for item in items:
                    if item['name'].startswith("trade_guardian_v4"): selected = item; break
                self.cached_file_id = selected['id']; return selected['id'], selected['name']
            return None, None
        except Exception as e: st.error(f"Drive Search Error: {e}"); return None, None

    def get_cloud_modified_time(self, file_id):
        try:
            file = self.service.files().get(fileId=file_id, fields='modifiedTime').execute()
            return datetime.strptime(file['modifiedTime'].replace('Z', '+0000'), '%Y-%m-%dT%H:%M:%S.%f%z')
        except: return None

    def create_backup(self, file_id, file_name):
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"BACKUP_{timestamp}_{file_name}"
            orig_file = self.service.files().get(fileId=file_id, fields='parents').execute()
            parents = orig_file.get('parents', [])
            metadata = {'name': backup_name}
            if parents: metadata['parents'] = parents
            self.service.files().copy(fileId=file_id, body=metadata).execute()
            self.cleanup_backups(file_name)
            return True
        except: return False

    def cleanup_backups(self, original_name):
        try:
            query = f"name contains 'BACKUP_' and name contains '{original_name}' and trashed=false"
            results = self.service.files().list(q=query, pageSize=20, fields="files(id, name, createdTime)", orderBy="createdTime desc").execute()
            items = results.get('files', [])
            if len(items) > 5:
                for item in items[5:]:
                    try: self.service.files().delete(fileId=item['id']).execute()
                    except: pass
        except: pass

    def download_db(self, force=False):
        file_id, file_name = self.find_db_file()
        if not file_id: return False, "Database not found in Cloud."
        if os.path.exists(DB_NAME) and not force:
            try:
                local_ts = os.path.getmtime(DB_NAME)
                local_mod = datetime.fromtimestamp(local_ts, tz=timezone.utc)
                cloud_time = self.get_cloud_modified_time(file_id)
                if cloud_time and (local_mod > cloud_time + timedelta(minutes=2)):
                    return False, f"CONFLICT: Your local database is NEWER ({local_mod.strftime('%H:%M')}) than cloud."
            except Exception as e: print(f"Pull check warning: {e}")

        try:
            try: sqlite3.connect(DB_NAME).close()
            except: pass
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False: status, done = downloader.next_chunk()
            with open(DB_NAME, "wb") as f: f.write(fh.getbuffer())
            st.session_state['last_cloud_sync'] = datetime.now()
            return True, f"Downloaded '{file_name}' successfully."
        except Exception as e: return False, str(e)

    def upload_db(self, force=False, retries=2):
        if not os.path.exists(DB_NAME): return False, "No local database found."
        file_id, file_name = self.find_db_file()
        if file_id and not force:
            cloud_time = self.get_cloud_modified_time(file_id)
            local_ts = os.path.getmtime(DB_NAME)
            local_time = datetime.fromtimestamp(local_ts, tz=timezone.utc) 
            if cloud_time and (cloud_time > local_time + timedelta(seconds=2)):
                 return False, f"CONFLICT: Cloud file is newer. Please Pull first."

        try:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            conn.close()
            if result[0] != "ok": return False, "❌ Local Database Corrupt."
        except Exception as e: return False, f"❌ DB Check Failed: {e}"

        media = MediaFileUpload(DB_NAME, mimetype='application/x-sqlite3', resumable=True)
        for attempt in range(retries + 1):
            try:
                if file_id:
                    if attempt == 0: self.create_backup(file_id, file_name)
                    self.service.files().update(fileId=file_id, media_body=media).execute()
                    action = f"Updated '{file_name}'"
                else:
                    file_metadata = {'name': DB_NAME}
                    self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                    action = "Created New File"
                st.session_state['last_cloud_sync'] = datetime.now()
                return True, f"Sync Successful: {action}"
            except Exception as e:
                if attempt < retries: time.sleep(1); continue
                return False, f"Upload failed: {str(e)}"

drive_mgr = DriveManager()

def auto_sync_if_connected():
    if not drive_mgr.is_connected: return
    with st.spinner("☁️ Auto-syncing..."):
        success, msg = drive_mgr.upload_db()
        if success: st.toast(f"✅ Cloud Saved: {datetime.now().strftime('%H:%M')}")
        elif "CONFLICT" in msg: st.error(f"⚠️ Auto-sync BLOCKED: Conflict.")
        else: st.warning(f"⚠️ Auto-sync failed: {msg}")

# --- DATABASE ENGINE ---
def get_db_connection(): return sqlite3.connect(DB_NAME)

def init_db():
    if not os.path.exists(DB_NAME) and drive_mgr.is_connected:
        success, msg = drive_mgr.download_db()
        if success: st.toast(f"☁️ Cloud Data Loaded: {msg}")
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT, parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT, snapshot_date DATE, pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL, FOREIGN KEY(trade_id) REFERENCES trades(id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER, min_stability REAL, description TEXT, typical_debit REAL)''')
    
    def add_col(table, col, dtype):
        try: c.execute(f"SELECT {col} FROM {table} LIMIT 1")
        except: 
            try: c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
            except: pass

    add_col('snapshots', 'theta', 'REAL'); add_col('snapshots', 'delta', 'REAL')
    add_col('snapshots', 'vega', 'REAL'); add_col('snapshots', 'gamma', 'REAL')
    add_col('strategy_config', 'typical_debit', 'REAL'); add_col('trades', 'original_group', 'TEXT')
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
    conn.commit(); conn.close()
    seed_default_strategies()

def seed_default_strategies(force_reset=False):
    conn = get_db_connection(); c = conn.cursor()
    try:
        if force_reset: c.execute("DELETE FROM strategy_config")
        c.execute("SELECT count(*) FROM strategy_config")
        if c.fetchone()[0] == 0:
            defaults = [
                ('130/160', '130/160', 500, 36, 0.8, 'Income Discipline', 4000),
                ('160/190', '160/190', 700, 44, 0.8, 'Patience Training', 5200),
                ('M200', 'M200', 900, 41, 0.8, 'Emotional Mastery', 8000),
                ('SMSF', 'SMSF', 600, 40, 0.8, 'Wealth Builder', 5000)
            ]
            c.executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)
            conn.commit()
    finally: conn.close()

@st.cache_data(ttl=60)
def load_strategy_config():
    if not os.path.exists(DB_NAME): return {}
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM strategy_config", conn)
        config = {}
        for _, row in df.iterrows():
            config[row['name']] = {
                'id': row['identifier'], 'pnl': row['target_pnl'],
                'dit': row['target_days'], 'stability': row['min_stability'],
                'debit_per_lot': row['typical_debit'] if pd.notnull(row['typical_debit']) else 5000
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
        if str(details['id']).upper() in t_name: return strat_name
    for strat_name, details in sorted_strats:
        if str(details['id']).upper() in g_name: return strat_name
    return "Other"

def clean_num(x):
    try:
        if pd.isna(x) or str(x).strip() == "": return 0.0
        return float(str(x).replace('$', '').replace(',', '').replace('%', '').strip())
    except: return 0.0

def safe_fmt(val, fmt_str):
    try: return fmt_str.format(val) if isinstance(val, (int, float)) else str(val)
    except: return str(val)

def generate_id(name, strategy, entry_date):
    return f"{re.sub(r'\W+', '', str(name))}_{strategy}_{pd.to_datetime(entry_date).strftime('%Y%m%d')}"

def extract_ticker(name):
    try:
        parts = str(name).split(' ')
        if parts:
            ticker = parts[0].replace('.', '').upper()
            if ticker in ['M200', '130', '160', 'IRON', 'VERTICAL', 'SMSF']: return "UNKNOWN"
            return ticker
    except: pass
    return "UNKNOWN"

def theta_decay_model(initial_theta, days_held, strategy, dte_at_entry=45):
    t_frac = min(1.0, days_held / dte_at_entry) if dte_at_entry > 0 else 1.0
    if strategy in ['M200', '130/160', '160/190', 'SMSF']:
        decay_factor = 1 - (2 * t_frac) ** 2 if t_frac < 0.5 else 2 * (1 - t_frac)
        return initial_theta * max(0, decay_factor)
    elif 'VERTICAL' in str(strategy).upper() or 'DIRECTIONAL' in str(strategy).upper():
        decay_factor = 1 - t_frac if t_frac < 0.7 else 0.3 * np.exp(-5 * (t_frac - 0.7))
        return initial_theta * decay_factor
    else:
        return initial_theta * (1 - np.exp(-2 * t_frac))

def get_trade_lifecycle_data(row, snapshots_df):
    days = max(1, int(row['Days Held']))
    total_pnl = row['P&L']
    
    if snapshots_df is not None and not snapshots_df.empty:
        trade_snaps = snapshots_df[snapshots_df['trade_id'] == row['id']].sort_values('days_held').copy()
        if len(trade_snaps) >= 2:
            trade_snaps['Cumulative_PnL'] = trade_snaps['pnl']
            trade_snaps['Day'] = trade_snaps['days_held']
            if trade_snaps['Day'].min() > 0:
                trade_snaps = pd.concat([pd.DataFrame({'Day': [0], 'Cumulative_PnL': [0]}), trade_snaps], ignore_index=True)
            if row['Status'] == 'Expired' and trade_snaps['Day'].max() < days:
                trade_snaps = pd.concat([trade_snaps, pd.DataFrame({'Day': [days], 'Cumulative_PnL': [total_pnl]})], ignore_index=True)
            
            trade_snaps['Pct_Duration'] = (trade_snaps['Day'] / days) * 100
            denom = abs(total_pnl) if abs(total_pnl) > 0 else 1
            trade_snaps['Pct_PnL'] = (trade_snaps['Cumulative_PnL'] / denom) * 100
            return trade_snaps[['Day', 'Cumulative_PnL', 'Pct_Duration', 'Pct_PnL']]

    daily_data = [{'Day': 0, 'Cumulative_PnL': 0, 'Pct_Duration': 0, 'Pct_PnL': 0}]
    initial_theta = row['Theta'] if row['Theta'] != 0 else 1.0
    weights = [abs(theta_decay_model(initial_theta, d, row['Strategy'], max(45, days))) for d in range(1, days + 1)]
    total_w = sum(weights)
    weights = [w/total_w for w in weights] if total_w > 0 else [1/days] * days
    
    cum_pnl = 0
    for i, w in enumerate(weights):
        cum_pnl += total_pnl * w
        denom = abs(total_pnl) if abs(total_pnl) > 0 else 1
        daily_data.append({
            'Day': i + 1, 'Cumulative_PnL': cum_pnl,
            'Pct_Duration': ((i + 1) / days) * 100, 'Pct_PnL': (cum_pnl / denom) * 100
        })
    return pd.DataFrame(daily_data)

def reconstruct_daily_pnl(trades_df):
    trades = trades_df.copy()
    trades['Entry Date'] = pd.to_datetime(trades['Entry Date'])
    trades['Exit Date'] = pd.to_datetime(trades['Exit Date'])
    start_date = trades['Entry Date'].min()
    end_date = max(trades['Exit Date'].max(), pd.Timestamp.now())
    date_range = pd.date_range(start=start_date, end=end_date)
    daily_pnl_dict = {d.date(): 0.0 for d in date_range}

    for _, trade in trades.iterrows():
        if pd.isnull(trade['Exit Date']): continue
        days = max(1, trade['Days Held'])
        total_pnl = trade['P&L']
        initial_theta = trade['Theta'] if trade['Theta'] != 0 else 1.0
        
        weights = [abs(theta_decay_model(initial_theta, d, trade['Strategy'], max(45, days))) for d in range(days)]
        tot_w = sum(weights)
        weights = [w/tot_w for w in weights] if tot_w > 0 else [1/days] * days
        
        curr = trade['Entry Date']
        for w in weights:
            if curr.date() in daily_pnl_dict: daily_pnl_dict[curr.date()] += total_pnl * w
            else: daily_pnl_dict[curr.date()] = total_pnl * w
            curr += pd.Timedelta(days=1)
    return daily_pnl_dict

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
                        header_row = i; break
                file.seek(0); df_raw = pd.read_excel(file, header=header_row)
                
                if 'Link' in df_raw.columns:
                    try:
                        file.seek(0); wb = load_workbook(file, data_only=False); sheet = wb.active
                        excel_header_row = header_row + 1
                        link_col_idx = None
                        for cell in sheet[excel_header_row]:
                            if str(cell.value).strip() == "Link": link_col_idx = cell.col_idx; break
                        if link_col_idx:
                            links = []
                            for i in range(len(df_raw)):
                                cell = sheet.cell(row=excel_header_row + 1 + i, column=link_col_idx)
                                url = cell.hyperlink.target if cell.hyperlink else ("" if not cell.value or not str(cell.value).startswith('=HYPERLINK') else str(cell.value).split('"')[1] if len(str(cell.value).split('"')) > 1 else "")
                                links.append(url)
                            df_raw['Link'] = links
                    except: pass
            except: pass

        if df_raw is None:
            file.seek(0); content = file.getvalue().decode("utf-8", errors='ignore')
            lines = content.split('\n')
            header_row = 0
            for i, line in enumerate(lines[:30]):
                if "Name" in line and "Total Return" in line: header_row = i; break
            file.seek(0); df_raw = pd.read_csv(file, skiprows=header_row)

        parsed_trades = []; current_trade = None; current_legs = []

        def finalize_trade(trade_data, legs, f_type):
            if not trade_data.any(): return None
            name = str(trade_data.get('Name', ''))
            group = str(trade_data.get('Group', ''))
            try: start_dt = pd.to_datetime(trade_data.get('Created At', ''))
            except: return None 
            strat = get_strategy_dynamic(name, group, config_dict)
            link = str(trade_data.get('Link', ''))
            if link == 'nan' or link == 'Open': link = "" 
            
            pnl = clean_num(trade_data.get('Total Return $', 0))
            debit = abs(clean_num(trade_data.get('Net Debit/Credit', 0)))
            theta = clean_num(trade_data.get('Theta', 0)); delta = clean_num(trade_data.get('Delta', 0))
            gamma = clean_num(trade_data.get('Gamma', 0)); vega = clean_num(trade_data.get('Vega', 0))
            iv = clean_num(trade_data.get('IV', 0))

            exit_dt = None
            try:
                raw_exp = trade_data.get('Expiration')
                if pd.notnull(raw_exp) and str(raw_exp).strip() != '': exit_dt = pd.to_datetime(raw_exp)
            except: pass

            days_held = (exit_dt - start_dt).days if (exit_dt and f_type == "History") else (datetime.now() - start_dt).days
            if days_held < 1: days_held = 1

            typical_debit = config_dict.get(strat, {}).get('debit_per_lot', 5000)
            lot_match = re.search(r'(\d+)\s*(?:LOT|L\b)', name, re.IGNORECASE)
            lot_size = int(lot_match.group(1)) if lot_match else max(1, int(round(debit / typical_debit)))

            put_pnl = 0.0; call_pnl = 0.0
            if f_type == "History":
                for leg in legs:
                    if len(leg) < 5: continue
                    sym = str(leg.iloc[0]); 
                    if not sym.startswith('.'): continue
                    try:
                        qty = clean_num(leg.iloc[1]); entry = clean_num(leg.iloc[2]); close_price = clean_num(leg.iloc[4])
                        leg_pnl = (close_price - entry) * qty * 100
                        if 'P' in sym and 'C' not in sym: put_pnl += leg_pnl
                        elif 'C' in sym and 'P' not in sym: call_pnl += leg_pnl
                        elif re.search(r'[0-9]P[0-9]', sym): put_pnl += leg_pnl
                        elif re.search(r'[0-9]C[0-9]', sym): call_pnl += leg_pnl
                    except: pass
            
            return {
                'id': generate_id(name, strat, start_dt), 'name': name, 'strategy': strat, 'start_dt': start_dt,
                'exit_dt': exit_dt, 'days_held': days_held, 'debit': debit, 'lot_size': lot_size, 'pnl': pnl, 
                'theta': theta, 'delta': delta, 'gamma': gamma, 'vega': vega, 'iv': iv, 
                'put_pnl': put_pnl, 'call_pnl': call_pnl, 'link': link, 'group': group
            }

        cols = [str(c) for c in df_raw.columns]
        if 'Name' not in cols or 'Total Return $' not in cols: return []

        for index, row in df_raw.iterrows():
            name_val = str(row['Name'])
            if name_val and not name_val.startswith('.') and name_val != 'Symbol' and name_val != 'nan':
                if current_trade is not None:
                    res = finalize_trade(current_trade, current_legs, file_type)
                    if res: parsed_trades.append(res)
                current_trade = row; current_legs = []
            elif name_val.startswith('.'): current_legs.append(row)
        
        if current_trade is not None:
             res = finalize_trade(current_trade, current_legs, file_type)
             if res: parsed_trades.append(res)
        return parsed_trades
    except Exception as e: print(f"Parser Error: {e}"); return []

def sync_data(file_list, file_type):
    log = []; conn = get_db_connection(); c = conn.cursor()
    config_dict = load_strategy_config()
    db_active_ids = set()
    if file_type == "Active":
        try: db_active_ids = set(pd.read_sql("SELECT id FROM trades WHERE status = 'Active'", conn)['id'].tolist())
        except: pass
    file_found_ids = set()

    for file in file_list:
        count_new = 0; count_update = 0
        try:
            trades_data = parse_optionstrat_file(file, file_type, config_dict)
            if not trades_data: log.append(f" {file.name}: Skipped"); continue

            for t in trades_data:
                trade_id = t['id']
                if file_type == "Active": file_found_ids.add(trade_id)
                
                c.execute("SELECT id, status, theta, delta, gamma, vega, put_pnl, call_pnl, iv, link, lot_size, strategy FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
                # Handle Renames via Link Match
                if existing is None and t['link'] and len(t['link']) > 15:
                    c.execute("SELECT id, name FROM trades WHERE link = ?", (t['link'],))
                    link_match = c.fetchone()
                    if link_match:
                        old_id, old_name = link_match
                        try:
                            c.execute("UPDATE snapshots SET trade_id = ? WHERE trade_id = ?", (trade_id, old_id))
                            c.execute("UPDATE trades SET id=?, name=? WHERE id=?", (trade_id, t['name'], old_id))
                            log.append(f" Renamed: '{old_name}' -> '{t['name']}'")
                            c.execute("SELECT id, status, theta, delta, gamma, vega, put_pnl, call_pnl, iv, link, lot_size, strategy FROM trades WHERE id = ?", (trade_id,))
                            existing = c.fetchone()
                            if file_type == "Active":
                                file_found_ids.add(trade_id)
                                if old_id in db_active_ids: db_active_ids.remove(old_id)
                                db_active_ids.add(trade_id)
                        except: pass

                status = "Active" if file_type == "Active" else "Expired"
                
                if existing is None:
                    c.execute('''INSERT INTO trades (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, notes, tags, parent_id, put_pnl, call_pnl, iv, link, original_group) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, t['name'], t['strategy'], status, t['start_dt'].date(), t['exit_dt'].date() if t['exit_dt'] else None, t['days_held'], t['debit'], t['lot_size'], t['pnl'], t['theta'], t['delta'], t['gamma'], t['vega'], "", "", "", t['put_pnl'], t['call_pnl'], t['iv'], t['link'], t['group']))
                    count_new += 1
                else:
                    final_lot = t['lot_size'] if not (existing[10] and existing[10] > 0) else existing[10]
                    final_strat = existing[11] if existing[11] != 'Other' else t['strategy']
                    old_vals = {'put': existing[6] or 0, 'call': existing[7] or 0, 'iv': existing[8] or 0, 'link': existing[9] or "", 'theta': existing[2]}
                    
                    # Merge Logic
                    vals = {
                        'theta': t['theta'] if t['theta'] != 0 else old_vals['theta'],
                        'put': t['put_pnl'] if t['put_pnl'] != 0 else old_vals['put'],
                        'call': t['call_pnl'] if t['call_pnl'] != 0 else old_vals['call'],
                        'iv': t['iv'] if t['iv'] != 0 else old_vals['iv'],
                        'link': t['link'] if t['link'] != "" else old_vals['link'],
                        'delta': t['delta'] if t['delta'] != 0 else 0,
                        'gamma': t['gamma'] if t['gamma'] != 0 else 0,
                        'vega': t['vega'] if t['vega'] != 0 else 0
                    }

                    if file_type == "History":
                        c.execute('''UPDATE trades SET pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, put_pnl=?, call_pnl=?, iv=?, link=?, lot_size=?, strategy=?, original_group=? WHERE id=?''', 
                            (t['pnl'], status, t['exit_dt'].date() if t['exit_dt'] else None, t['days_held'], vals['theta'], vals['delta'], vals['gamma'], vals['vega'], vals['put'], vals['call'], vals['iv'], vals['link'], final_lot, final_strat, t['group'], trade_id))
                        count_update += 1
                    elif existing[1] in ["Active", "Missing"]: 
                        c.execute('''UPDATE trades SET pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, iv=?, link=?, status='Active', exit_date=?, lot_size=?, strategy=?, original_group=? WHERE id=?''', 
                            (t['pnl'], t['days_held'], vals['theta'], vals['delta'], vals['gamma'], vals['vega'], vals['iv'], vals['link'], t['exit_dt'].date() if t['exit_dt'] else None, final_lot, final_strat, t['group'], trade_id))
                        count_update += 1
                
                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    if not c.fetchone():
                        c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)", (trade_id, today, t['pnl'], t['days_held'], t['theta'] or 0, t['delta'] or 0, t['vega'] or 0, t['gamma'] or 0))
                    else:
                        c.execute("UPDATE snapshots SET theta=?, delta=?, vega=?, gamma=? WHERE trade_id=? AND snapshot_date=?", (t['theta'] or 0, t['delta'] or 0, t['vega'] or 0, t['gamma'] or 0, trade_id, today))
            log.append(f" {file.name}: {count_new} New, {count_update} Updated")
        except Exception as e: log.append(f" {file.name}: Error - {str(e)}")
            
    if file_type == "Active" and file_found_ids:
        missing_ids = db_active_ids - file_found_ids
        if missing_ids:
            placeholders = ','.join('?' for _ in missing_ids)
            c.execute(f"UPDATE trades SET status = 'Missing' WHERE id IN ({placeholders})", list(missing_ids))
            log.append(f" Integrity: Marked {len(missing_ids)} trades as 'Missing'.")
    conn.commit(); conn.close()
    return log

def update_journal(edited_df):
    conn = get_db_connection(); c = conn.cursor(); count = 0
    try:
        for _, row in edited_df.iterrows():
            c.execute("UPDATE trades SET notes=?, tags=?, parent_id=?, lot_size=?, strategy=? WHERE id=?", 
                      (str(row['Notes']), str(row['Tags']), str(row['Parent ID']), int(row['lot_size']) if row['lot_size'] > 0 else 1, str(row['Strategy']), row['id']))
            count += 1
        conn.commit(); return count
    except: return 0
    finally: conn.close()

def update_strategy_config(edited_df):
    conn = get_db_connection(); c = conn.cursor()
    try:
        c.execute("DELETE FROM strategy_config")
        for i, row in edited_df.iterrows():
            c.execute("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", 
                      (row['Name'], row['Identifier'], row['Target PnL'], row['Target Days'], row['Min Stability'], row['Description'], row['Typical Debit']))
        conn.commit(); return True
    except: return False
    finally: conn.close()

def reprocess_other_trades():
    conn = get_db_connection(); c = conn.cursor(); config = load_strategy_config()
    try: c.execute("SELECT id, name, original_group, strategy FROM trades")
    except: c.execute("SELECT id, name, '', strategy FROM trades")
    updated = 0
    for t_id, name, group, strat in c.fetchall():
        if strat == "Other":
            new = get_strategy_dynamic(name, group or "", config)
            if new != "Other":
                c.execute("UPDATE trades SET strategy = ? WHERE id = ?", (new, t_id)); updated += 1
    conn.commit(); conn.close(); return updated

@st.cache_data(ttl=60)
def load_data():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        if df.empty: return pd.DataFrame()
        snaps = pd.read_sql("SELECT trade_id, pnl FROM snapshots", conn)
        if not snaps.empty:
            vol_df = snaps.groupby('trade_id')['pnl'].std().reset_index().rename(columns={'pnl': 'P&L Vol'})
            df = df.merge(vol_df, left_on='id', right_on='trade_id', how='left')
            df['P&L Vol'] = df['P&L Vol'].fillna(0)
        else: df['P&L Vol'] = 0.0
    except: return pd.DataFrame()
    finally: conn.close()
    
    if not df.empty:
        df = df.rename(columns={'name': 'Name', 'strategy': 'Strategy', 'status': 'Status', 'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held', 'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega', 'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes', 'tags': 'Tags', 'parent_id': 'Parent ID', 'put_pnl': 'Put P&L', 'call_pnl': 'Call P&L', 'iv': 'IV', 'link': 'Link'})
        for c in ['Gamma', 'Vega', 'Theta', 'Delta', 'P&L', 'Debit', 'lot_size', 'Put P&L', 'Call P&L', 'IV']: 
            if c not in df: df[c] = 0.0
            else: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
        for c in ['Notes', 'Tags', 'Parent ID', 'Link']: 
            if c not in df: df[c] = ""
            
        df['Entry Date'] = pd.to_datetime(df['Entry Date']); df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        df['lot_size'] = df['lot_size'].apply(lambda x: 1 if x < 1 else int(x))
        df['Debit/Lot'] = np.where(df['lot_size'] > 0, df['Debit'] / df['lot_size'], df['Debit'])
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = np.where(df['Debit'] > 0, (df['Theta'] / df['Debit']) * 100, 0)
        df['Parent ID'] = df['Parent ID'].astype(str).str.strip().replace('nan', '').replace('None', '')
        df['Stability'] = np.where(df['Theta'] > 0, df['Theta'] / (df['Delta'].abs() + 1), 0.0)
        
        def get_grade(row):
            s, d = row['Strategy'], row['Debit/Lot']
            if s == '130/160': return ("F", "Overpriced") if d > 4800 else ("A+", "Sweet Spot") if 3500 <= d <= 4500 else ("B", "Acceptable")
            if s == '160/190': return ("A", "Ideal") if 4800 <= d <= 5500 else ("C", "Check Pricing")
            if s == 'M200': return ("A", "Perfect") if 7500 <= d <= 8500 else ("B", "Variance")
            if s == 'SMSF': return ("B", "High Debit") if d > 15000 else ("A", "Standard")
            return ("C", "Standard")
        
        grades = df.apply(get_grade, axis=1)
        df['Grade'] = [g[0] for g in grades]
        df['Reason'] = [g[1] for g in grades]
    return df

@st.cache_data(ttl=300)
def load_snapshots():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        q = "SELECT s.snapshot_date, s.pnl, s.days_held, s.theta, s.delta, s.vega, s.gamma, t.strategy, t.name, t.id as trade_id, t.theta as initial_theta FROM snapshots s JOIN trades t ON s.trade_id = t.id"
        df = pd.read_sql(q, conn)
    except: return pd.DataFrame()
    finally: conn.close()
    for c in ['pnl', 'days_held', 'theta', 'delta', 'vega', 'gamma', 'initial_theta']: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
    return df

# --- ANALYTICS FUNCS ---
def calculate_kelly_fraction(win_rate, avg_win, avg_loss):
    if avg_loss == 0 or avg_win <= 0: return 0.0
    b = abs(avg_win / avg_loss); kelly = (win_rate * b - (1 - win_rate)) / b
    return max(0, min(kelly * 0.5, 0.25))

def generate_trade_predictions(active_df, history_df, prob_low, prob_high, total_capital=100000):
    if active_df.empty or history_df.empty: return pd.DataFrame()
    train_df = history_df.dropna(subset=['Theta/Cap %', 'Delta', 'Debit/Lot']).copy()
    if len(train_df) < 5: return pd.DataFrame()
    preds = []
    features = ['Theta/Cap %', 'Delta', 'Debit/Lot']
    hist_vecs = np.nan_to_num(train_df[features].values.astype(float))
    
    for _, row in active_df.iterrows():
        curr_vec = np.nan_to_num(row[features].values.astype(float)).reshape(1, -1)
        dists = cdist(curr_vec, hist_vecs, metric='euclidean')[0]
        neighbors = train_df.iloc[np.argsort(dists)[:7]]
        win_prob = (neighbors['P&L'] > 0).mean()
        avg_pnl = neighbors['P&L'].mean()
        avg_win = neighbors[neighbors['P&L'] > 0]['P&L'].mean(); avg_win = 0 if pd.isna(avg_win) else avg_win
        avg_loss = neighbors[neighbors['P&L'] < 0]['P&L'].mean(); avg_loss = -avg_pnl * 0.5 if pd.isna(avg_loss) else avg_loss
        kelly = calculate_kelly_fraction(win_prob, avg_win, avg_loss)
        
        rec = "HOLD"
        if win_prob * 100 < prob_low: rec = "REDUCE/CLOSE"
        elif win_prob * 100 > prob_high: rec = "PRESS WINNER"
        
        preds.append({
            'Trade Name': row['Name'], 'Strategy': row['Strategy'], 'Win Prob %': win_prob * 100,
            'Expected PnL': avg_pnl, 'Kelly Size %': kelly * 100, 'Rec. Size ($)': kelly * total_capital,
            'AI Rec': rec, 'Confidence': max(0, 100 - (dists[:7].mean() * 10))
        })
    return pd.DataFrame(preds)

def get_mae_stats(conn):
    try:
        df = pd.read_sql("SELECT t.strategy, MIN(s.pnl) as worst_drawdown FROM snapshots s JOIN trades t ON s.trade_id = t.id WHERE t.status = 'Expired' AND t.pnl > 0 GROUP BY t.id, t.strategy", conn)
        return {s: df[df['strategy'] == s]['worst_drawdown'].quantile(0.05) for s in df['strategy'].unique()} if not df.empty else {}
    except: return {}

def get_velocity_stats(expired_df):
    stats = {}
    if expired_df.empty: return stats
    winners = expired_df[expired_df['P&L'] > 0].copy()
    if winners.empty: return stats
    winners['velocity'] = winners['P&L'] / winners['Days Held'].replace(0, 1)
    for strat in winners['Strategy'].unique():
        s_df = winners[winners['Strategy'] == strat]
        if len(s_df) > 2:
            stats[strat] = {'threshold': s_df['velocity'].mean() + (2 * s_df['velocity'].std()), 'mean': s_df['velocity'].mean()}
    return stats

def check_rot_and_efficiency(row, hist_avg_days):
    try:
        curr_spd = row['P&L'] / max(1, row.get('Days Held', 1))
        theta = row.get('Theta', 0)
        eff = (curr_spd / theta) if theta != 0 else 0
        status = "Healthy"
        if row.get('Days Held', 1) > hist_avg_days * 1.2 and row['P&L'] < 0: status = "Rotting (Time > Avg & Red)"
        elif eff < 0.2 and row.get('Days Held', 1) > 10 and row['P&L'] > 0: status = "Inefficient (Theta Stuck)"
        elif eff < 0 and row.get('Days Held', 1) > 5: status = "Bleeding (Negative Efficiency)"
        return curr_spd, eff, status
    except: return 0, 0, "Error"

def get_dynamic_targets(history_df, percentile):
    if history_df.empty: return {}
    winners = history_df[history_df['P&L'] > 0]
    return {s: {'Median Win': g['P&L'].median(), 'Optimal Exit': g['P&L'].quantile(percentile)} for s, g in winners.groupby('Strategy')}

def find_similar_trades(current, history, top_n=3):
    if history.empty: return pd.DataFrame()
    cols = ['Theta/Cap %', 'Delta', 'Debit/Lot']
    if not all(c in current and c in history.columns for c in cols): return pd.DataFrame()
    
    vec = np.nan_to_num(current[cols].values.astype(float)).reshape(1, -1)
    hist_vecs = np.nan_to_num(history[cols].values.astype(float))
    dists = cdist(vec, hist_vecs, metric='euclidean')[0]
    idx = np.argsort(dists)[:top_n]
    sim = history.iloc[idx].copy()
    sim['Similarity %'] = 100 * (1 - dists[idx] / (dists.max() if dists.max() > 0 else 1))
    return sim[['Name', 'P&L', 'Days Held', 'ROI', 'Similarity %']]

def calculate_portfolio_metrics(trades_df, capital):
    if trades_df.empty: return 0.0, 0.0
    daily = reconstruct_daily_pnl(trades_df)
    dates = pd.date_range(trades_df['Entry Date'].min(), max(trades_df['Exit Date'].max(), pd.Timestamp.now()))
    equity = [capital + sum(daily.get(d.date(), 0) for d in dates[:i+1]) for i, _ in enumerate(dates)]
    returns = pd.Series(equity).pct_change().dropna()
    sharpe = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() != 0 else 0
    days = (dates[-1] - dates[0]).days or 1
    cagr = ((equity[-1] / capital) ** (365 / days) - 1) * 100 if equity[-1] > 0 else 0
    return sharpe, cagr

def check_concentration_risk(active_df, equity, limit=0.15):
    if active_df.empty or equity <= 0: return pd.DataFrame()
    risks = []
    for _, r in active_df.iterrows():
        conc = r['Debit'] / equity
        if conc > limit: risks.append({'Trade': r['Name'], 'Size %': f"{conc:.1%}", 'Risk': f"${r['Debit']:,.0f}"})
    return pd.DataFrame(risks)

def calculate_max_drawdown(trades_df, capital):
    if trades_df.empty: return {'Max Drawdown %': 0.0}
    daily = reconstruct_daily_pnl(trades_df)
    dates = pd.date_range(trades_df['Entry Date'].min(), max(trades_df['Exit Date'].max(), pd.Timestamp.now()))
    equity = pd.Series([capital + sum(daily.get(d.date(), 0) for d in dates[:i+1]) for i, _ in enumerate(dates)])
    dd = (equity - equity.cummax()) / equity.cummax()
    return {'Max Drawdown %': dd.min() * 100}

def rolling_correlation_matrix(snaps):
    if snaps.empty: return None
    piv = snaps.pivot_table(index='snapshot_date', columns='strategy', values='pnl', aggfunc='sum').tail(30).corr()
    return px.imshow(piv, text_auto=".2f", color_continuous_scale="RdBu", title="Strategy Correlation (30 Days)")

def generate_adaptive_rulebook_text(history, strats):
    txt = "# The Adaptive Trader's Constitution\n"
    if history.empty: return txt + " *Not enough data.*"
    for s in strats:
        sdf = history[history['Strategy'] == s]
        if sdf.empty: continue
        wins = sdf[sdf['P&L'] > 0]; loss = sdf[sdf['P&L'] < 0]
        txt += f"### {s}\n"
        if not wins.empty:
            txt += f"* **Best Day:** {wins.groupby(wins['Entry Date'].dt.day_name())['P&L'].mean().idxmax()}\n"
            txt += f"* **Avg Hold:** {wins['Days Held'].mean():.0f} Days\n"
        if not loss.empty: txt += f"* **Loss Pattern:** Avg hold {loss['Days Held'].mean():.0f} days.\n"
    return txt

# --- INIT ---
init_db()

# --- SIDEBAR ---
st.sidebar.markdown("###  Daily Workflow")
if GOOGLE_DEPS_INSTALLED:
    with st.sidebar.expander("☁️ Cloud Sync (Google Drive)", expanded=True):
        if not drive_mgr.is_connected: st.error("Missing GCP Secrets")
        else:
            c1, c2 = st.columns(2)
            if c1.button("⬆️ Sync"):
                s, m = drive_mgr.upload_db()
                if s: st.success(m)
                elif "CONFLICT" in m: st.error(m); st.session_state['conf'] = True
                else: st.error(m)
            if c2.button("⬇️ Pull"):
                s, m = drive_mgr.download_db()
                if s: st.success(m); st.rerun()
                elif "CONFLICT" in m: st.error(m); st.session_state['p_conf'] = True
                else: st.error(m)
            
            if st.session_state.get('conf'):
                st.warning("Resolve Upload Conflict:")
                if st.button("⬇️ Pull (Safe)", key="r1"):
                    if drive_mgr.download_db()[0]: st.session_state['conf']=False; st.rerun()
                if st.button("⚠️ Force Push", key="r2"):
                    if drive_mgr.upload_db(force=True)[0]: st.session_state['conf']=False; st.rerun()
            
            if st.session_state.get('p_conf'):
                st.warning("Resolve Pull Conflict:")
                if st.button("⬇️ Force Pull", key="p1"):
                    if drive_mgr.download_db(force=True)[0]: st.session_state['p_conf']=False; st.rerun()
            
            st.sidebar.divider()
            ls = st.session_state.get('last_cloud_sync')
            if ls: st.sidebar.caption(f"Last Sync: {ls.strftime('%H:%M')}")
            else: st.sidebar.warning("Unsaved Session")

with st.sidebar.expander("1. Restore Local"):
    res = st.file_uploader("Upload .db", type=['db'])
    if res:
        with open(DB_NAME, "wb") as f: f.write(res.getbuffer())
        st.success("Restored"); st.rerun()

with st.sidebar.expander("2. Sync Files"):
    act_up = st.file_uploader("Active", accept_multiple_files=True, key="a")
    his_up = st.file_uploader("History", accept_multiple_files=True, key="h")
    if st.button("Process"):
        l = []
        if act_up: l.extend(sync_data(act_up, "Active"))
        if his_up: l.extend(sync_data(his_up, "History"))
        for i in l: st.write(i)
        if l: st.success("Done"); auto_sync_if_connected()

with st.sidebar.expander("3. Backup"):
    with open(DB_NAME, "rb") as f: st.download_button("Save DB", f, "backup.db")

with st.sidebar.expander("Maintenance"):
    if st.button("Vacuum"): conn = get_db_connection(); conn.execute("VACUUM"); conn.close(); st.success("Done")
    if st.button("Hard Reset"): 
        conn=get_db_connection(); 
        for t in ['trades','snapshots','strategy_config']: conn.execute(f"DROP TABLE IF EXISTS {t}")
        conn.commit(); conn.close(); init_db(); st.rerun()

st.sidebar.divider()
prime_cap = st.sidebar.number_input("Prime Cap", 1000, value=115000, step=1000)
smsf_cap = st.sidebar.number_input("SMSF Cap", 1000, value=150000, step=1000)
total_cap = prime_cap + smsf_cap
regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish", "Bearish"])
regime_mult = 1.1 if "Bullish" in regime else 0.9 if "Bearish" in regime else 1.0

# --- LADDER LOGIC ---
def calculate_decision_ladder(row, benchmarks):
    strat = row['Strategy']; days = row['Days Held']; pnl = row['P&L']; theta = row['Theta']; debit = row['Debit']
    bench = benchmarks.get(strat, {})
    target = (bench.get('pnl', 1000) * regime_mult) * max(1, row.get('lot_size', 1))
    avg_days = bench.get('dit', 40)
    score = 50; action = "HOLD"; reason = "Normal"; juice = 0; j_type = "Neutral"
    
    if row['Status'] == 'Missing': return "REVIEW", 100, "Missing", 0, "Error"
    
    if pnl < 0:
        j_type = "Recovery Days"
        juice = abs(pnl)/theta if theta > 0 else 999
        if theta > 0 and juice > max(1, avg_days - days) and days > 15: score += 40; action = "STRUCTURAL FAILURE"; reason = "Zombie"
        if theta <= 0 and days > 15: score += 30; reason = "Negative Theta"
    else:
        j_type = "Left in Tank"; juice = max(0, target - pnl)
        if debit > 0 and (juice/debit) < 0.05: score += 40; reason = "Squeezed Dry"
    
    if pnl >= target: return "TAKE PROFIT", 100, "Hit Target", juice, j_type
    if pnl >= target * 0.8: score += 30; action = "PREPARE EXIT"
    
    if strat == '130/160' and days > 30 and pnl < 100: return "KILL", 95, "Stale", juice, j_type
    if strat == '160/190' and days < 30: score = 10; action = "COOKING"; reason = "Early"
    if row['Stability'] < 0.3 and days > 5: score += 25; reason = "Unstable"; action = "RISK REVIEW"
    
    # Capital Efficiency Check
    if pnl > 0 and days > 5:
        hist_vel = bench.get('pnl', 1000) / max(1, avg_days)
        if (pnl/days) < (hist_vel * 0.6) and pnl > (target * 0.2):
            score += 15; action = "REDEPLOY?" if action == "HOLD" else action; reason = "Slow Capital"

    score = min(100, max(0, score))
    if score >= 90: action = "CRITICAL"
    elif score >= 70: action = "WATCH"
    elif score <= 30: action = "COOKING"
    return action, score, reason, juice, j_type

# --- MAIN APP LOGIC ---
df = load_data()
BASE_CONFIG = {
    '130/160': {'pnl': 500, 'dit': 36, 'stability': 0.8}, 
    '160/190': {'pnl': 700, 'dit': 44, 'stability': 0.8}, 
    'M200':    {'pnl': 900, 'dit': 41, 'stability': 0.8}, 
    'SMSF':    {'pnl': 600, 'dit': 40, 'stability': 0.8} 
}
config = load_strategy_config() or BASE_CONFIG
active_df = df[df['Status'].isin(['Active', 'Missing'])].copy() if not df.empty else pd.DataFrame()
expired_df = df[df['Status'] == 'Expired'].copy() if not df.empty else pd.DataFrame()

# Pre-calc stats
if not expired_df.empty:
    velocity_stats = get_velocity_stats(expired_df)
    try: 
        t_conn = get_db_connection()
        mae_stats = get_mae_stats(t_conn)
        t_conn.close()
    except: pass
    
    # Update config with history
    for s, g in expired_df.groupby('Strategy'):
        if s in config:
            wins = g[g['P&L'] > 0]
            if not wins.empty:
                config[s].update({'pnl': wins['P&L'].mean(), 'dit': wins['Days Held'].mean(), 'yield': g['Daily Yield %'].mean()})

# Calculate Ladder for Active
if not active_df.empty:
    lres = active_df.apply(lambda r: calculate_decision_ladder(r, config), axis=1)
    active_df['Action'], active_df['Urgency'], active_df['Reason'], active_df['Juice'], active_df['JType'] = zip(*lres)
    active_df = active_df.sort_values('Urgency', ascending=False)

# --- TABS LAYOUT ---
t_cockpit, t_active, t_ai, t_analytics, t_config = st.tabs(["🚀 Cockpit", "⚡ Active Mgmt", "🧠 AI Command", "🔬 Analytics Lab", "⚙️ Rules & Config"])

# 1. COCKPIT (ACTION CENTER)
with t_cockpit:
    # A. Urgent Queue
    urgent = active_df[active_df['Urgency'] >= 70] if not active_df.empty else pd.DataFrame()
    with st.expander(f"🔥 Priority Action Queue ({len(urgent)})", expanded=len(urgent)>0):
        if not urgent.empty:
            for _, r in urgent.iterrows():
                col = "red" if r['Urgency'] >= 90 else "orange"
                c1, c2, c3 = st.columns([2, 1, 1])
                c1.markdown(f"**{r['Name']}** ({r['Strategy']})")
                c2.markdown(f":{col}[**{r['Action']}**] ({r['Reason']})")
                c3.metric("Juice/Recov", f"{r['Juice']:.0f}")
        else: st.success("All Quiet. No critical actions.")
    
    st.divider()
    
    # B. Portfolio Pulse
    if not active_df.empty:
        c1, c2, c3, c4 = st.columns(4)
        tot_deb = active_df['Debit'].sum() or 1
        c1.metric("Daily Income", f"${active_df['Theta'].sum():,.0f}")
        c2.metric("Floating P&L", f"${active_df['P&L'].sum():,.0f}", delta_color="normal")
        alloc_score = 100 - sum(abs(active_df[active_df['Strategy']==s]['Debit'].sum()/tot_deb - 0.25)*100 for s in config) # Rough alloc logic
        c3.metric("Alloc Score", f"{alloc_score:.0f}", delta="Good" if alloc_score > 70 else "Fix")
        c4.metric("Avg Age", f"{active_df['Days Held'].mean():.0f}d")
        
        with st.expander("Heatmap"):
            fig = px.scatter(active_df, x='Days Held', y='P&L', size='Debit', color='Urgency', color_continuous_scale='RdYlGn_r', hover_data=['Name', 'Action'])
            fig.add_vline(x=active_df['Days Held'].mean(), line_dash="dash")
            st.plotly_chart(fig, use_container_width=True)
            
        # C. Efficiency Alerts
        alerts = []
        for s in active_df['Strategy'].unique():
            target_y = config.get(s, {}).get('yield', 0.1)
            for _, r in active_df[(active_df['Strategy']==s) & (active_df['P&L']>0)].iterrows():
                if r['Daily Yield %'] < target_y * 0.5:
                    alerts.append({'Trade': r['Name'], 'Yield': f"{r['Daily Yield %']:.2f}%", 'Target': f"{target_y:.2f}%"})
        
        if alerts:
            st.warning(f"💡 {len(alerts)} trades dragging efficiency. Consider Redeployment.")
            st.dataframe(pd.DataFrame(alerts), use_container_width=True)

    # D. Pre-Flight
    with st.expander("✈️ Pre-Flight Calculator"):
        c1, c2, c3 = st.columns(3)
        p_th = c1.number_input("Theta", 15.0); p_del = c2.number_input("Delta", -10.0); p_pr = c3.number_input("Price", 5000.0)
        st.info(f"Stability: {p_th/(abs(p_del)+1):.2f} | Yield: {(p_th/p_pr)*36500:.0f}% Ann.")

# 2. ACTIVE MGMT (THE WORKBENCH)
with t_active:
    if not active_df.empty:
        # A. The Journal
        st.caption("📝 Live Journal (Edits Auto-Save)")
        cols = ['id','Name','Link','Strategy','Urgency','Action','P&L','Debit','Days Held','lot_size','Notes','Tags','Parent ID']
        cfg = {
            'id': None, 'Name': st.column_config.TextColumn(disabled=True),
            'Link': st.column_config.LinkColumn(display_text="Open"),
            'Urgency': st.column_config.ProgressColumn(min_value=0, max_value=100, format="%d"),
            'P&L': st.column_config.NumberColumn(format="$%d"),
            'lot_size': st.column_config.NumberColumn("Lots", min_value=1)
        }
        edited = st.data_editor(active_df[cols], column_config=cfg, use_container_width=True, hide_index=True)
        if st.button("Save Journal Changes"):
            if update_journal(edited): st.success("Saved!"); st.rerun()
        
        st.divider()
        
        # B. Strategy Decks
        st.markdown("##### Strategy Breakdown")
        s_tabs = st.tabs(sorted(active_df['Strategy'].unique()))
        for i, s in enumerate(sorted(active_df['Strategy'].unique())):
            with s_tabs[i]:
                sub = active_df[active_df['Strategy'] == s]
                c1, c2, c3 = st.columns(3)
                c1.metric("Total P&L", f"${sub['P&L'].sum():,.0f}")
                c2.metric("Total Delta", f"{sub['Delta'].sum():.1f}")
                c3.metric("Avg Yield", f"{sub['Daily Yield %'].mean():.2f}%")
                st.dataframe(sub[['Name','P&L','Days Held','Action']], use_container_width=True)

        # C. Rolls
        rolled = df[df['Parent ID'] != ""]
        if not rolled.empty:
            with st.expander("🔄 Roll Campaigns"):
                camp = []
                for p in rolled['Parent ID'].unique():
                    subset = df[(df['id']==p)|(df['Parent ID']==p)]
                    camp.append({'ID': p, 'Total P&L': subset['P&L'].sum(), 'Legs': len(subset)})
                st.dataframe(pd.DataFrame(camp), use_container_width=True)
    else: st.info("Sync active trades to manage them.")

# 3. AI COMMAND
with t_ai:
    if not active_df.empty and not expired_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 🔮 Win Prob Forecast")
            preds = generate_trade_predictions(active_df, expired_df, 40, 75, total_cap)
            if not preds.empty:
                st.dataframe(preds[['Trade Name','Win Prob %','AI Rec']].style.map(lambda x: 'color: green' if x=='PRESS WINNER' else 'color: red', subset=['AI Rec']), use_container_width=True)
        with c2:
            st.markdown("##### 🧟 Rot Detector")
            rot = []
            for _, r in active_df.iterrows():
                _, _, status = check_rot_and_efficiency(r, config.get(r['Strategy'], {}).get('dit', 30))
                rot.append({'Trade': r['Name'], 'Status': status})
            st.dataframe(pd.DataFrame(rot), use_container_width=True)
        
        with st.expander("🧬 DNA Fingerprinting (Find Similar)"):
            sel = st.selectbox("Select Trade", active_df['Name'].unique())
            row = active_df[active_df['Name']==sel].iloc[0]
            sim = find_similar_trades(row, expired_df)
            st.dataframe(sim, use_container_width=True)

# 4. ANALYTICS LAB
with t_analytics:
    if not expired_df.empty:
        at1, at2, at3, at4 = st.tabs(["Performance", "Lifecycle (Timing)", "Risk", "Trends"])
        
        with at1:
            s_tot, c_tot = calculate_portfolio_metrics(expired_df, total_cap)
            c1, c2, c3 = st.columns(3)
            c1.metric("Realized P&L", f"${expired_df['P&L'].sum():,.0f}")
            c2.metric("CAGR", f"{c_tot:.1f}%")
            c3.metric("Sharpe", f"{s_tot:.2f}")
            
            daily = reconstruct_daily_pnl(expired_df)
            dates = sorted(daily.keys())
            eq = np.cumsum([daily[d] for d in dates])
            st.line_chart(pd.Series(eq, index=dates))

        with at2: # The NEW v147 Feature
            st.subheader("Harvest Curves")
            snaps = load_snapshots()
            sel_s = st.multiselect("Filter Strat", df['Strategy'].unique(), default=df['Strategy'].unique()[:1])
            subset = df[df['Strategy'].isin(sel_s)]
            
            if not subset.empty:
                lc_data = []
                total_items = len(subset)
                prog_bar = st.progress(0)
                
                for i, (idx, r) in enumerate(subset.iterrows()):
                    d = get_trade_lifecycle_data(r, snaps)
                    if not d.empty:
                        d['Name'] = r['Name']; d['Strat'] = r['Strategy']
                        lc_data.append(d)
                    prog_bar.progress(min((i+1)/total_items, 1.0))
                prog_bar.empty()
                
                if lc_data:
                    full = pd.concat(lc_data)
                    fig = px.line(full, x='Pct_Duration', y='Pct_PnL', color='Strat', line_group='Name', color_discrete_sequence=px.colors.qualitative.Vivid)
                    fig.update_traces(opacity=0.4, line=dict(width=1.5))
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.markdown("**Profit Phasing**")
                    full['Phase'] = full['Pct_Duration'].apply(lambda x: 'Early' if x<30 else 'Mid' if x<70 else 'Late')
                    full = full.sort_values(['Name','Pct_Duration'])
                    full['Marginal'] = full.groupby('Name')['Cumulative_PnL'].diff().fillna(0)
                    phasing = full.groupby(['Strat','Phase'])['Marginal'].sum().reset_index()
                    st.plotly_chart(px.bar(phasing, x='Strat', y='Marginal', color='Phase', barmode='group'), use_container_width=True)
                    
                    # --- RESTORED: CAPITAL STAGNATION (ZOMBIE ZONE) ---
                    st.markdown("**Capital Stagnation (The Zombie Zone)**")
                    zombie_data = []
                    closed_wins = subset[subset['Status'] == 'Expired'][subset['P&L'] > 0]['Name'].unique()
                    
                    for t_name in closed_wins:
                        t_curve = full[full['Name'] == t_name]
                        if t_curve.empty: continue
                        final_pnl = t_curve['Cumulative_PnL'].max()
                        days_80 = t_curve[t_curve['Cumulative_PnL'] >= final_pnl * 0.8]['Day'].min()
                        total_days = t_curve['Day'].max()
                        
                        if days_80 and total_days > days_80:
                            zombie_data.append({'Name': t_name, 'Days to 80%': days_80, 'Total Days': total_days, 'Zombie Days': total_days - days_80})
                            
                    if zombie_data:
                        z_df = pd.DataFrame(zombie_data)
                        fig_z = px.scatter(z_df, x='Days to 80%', y='Total Days', size='Zombie Days', hover_data=['Name'], title="Efficiency: Time to 80% Gain vs Total Hold")
                        fig_z.add_shape(type="line", x0=0, y0=0, x1=z_df['Total Days'].max(), y1=z_df['Total Days'].max(), line=dict(dash='dash', color='gray'))
                        st.plotly_chart(fig_z, use_container_width=True)

        with at3:
            st.subheader("Risk Analysis")
            snaps = load_snapshots()
            if not snaps.empty:
                st.plotly_chart(rolling_correlation_matrix(snaps), use_container_width=True)
            
            mae = df.merge(snaps.groupby('trade_id')['pnl'].min().rename('MAE'), left_on='id', right_index=True)
            st.plotly_chart(px.scatter(mae, x='MAE', y='P&L', color='Strategy', title="MAE vs P&L"), use_container_width=True)

        with at4:
            st.subheader("Trends & Seasonality")
            col1, col2 = st.columns(2)
            
            # --- RESTORED: PROFIT ANATOMY ---
            with col1:
                st.markdown("**Profit Anatomy (Call vs Put)**")
                anatomy = expired_df.groupby('Strategy')[['Put P&L', 'Call P&L']].sum().reset_index()
                st.plotly_chart(px.bar(anatomy, x='Strategy', y=['Put P&L', 'Call P&L'], barmode='group'), use_container_width=True)
                
            # --- RESTORED: ROOT CAUSE ---
            with col2:
                st.markdown("**Root Cause (Entry Price)**")
                wins = expired_df[expired_df['P&L'] > 0].groupby('Strategy')['Debit/Lot'].mean().reset_index()
                active = active_df.groupby('Strategy')['Debit/Lot'].mean().reset_index()
                wins['Type'] = 'Hist Wins'; active['Type'] = 'Active'
                st.plotly_chart(px.bar(pd.concat([wins, active]), x='Strategy', y='Debit/Lot', color='Type', barmode='group'), use_container_width=True)
            
            st.divider()
            
            # --- RESTORED: EFFICIENCY SHOWDOWN ---
            st.markdown("**Efficiency Showdown (ROI)**")
            hist_eff = expired_df.groupby('Strategy')['Ann. ROI'].mean().reset_index()
            act_eff = active_df.groupby('Strategy')['Ann. ROI'].mean().reset_index()
            hist_eff['Type'] = 'History'; act_eff['Type'] = 'Active'
            st.plotly_chart(px.bar(pd.concat([hist_eff, act_eff]), x='Strategy', y='Ann. ROI', color='Type', barmode='group'), use_container_width=True)
            
            st.divider()
            expired_df['Month'] = expired_df['Exit Date'].dt.month_name()
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
            season_data = expired_df.groupby('Month')['P&L'].sum().reindex(months).reset_index()
            st.plotly_chart(px.bar(season_data, x='Month', y='P&L', title="Seasonality"), use_container_width=True)

# 5. CONFIG
with t_config:
    st.markdown("### Strategy Configuration")
    cfg_df = pd.DataFrame(dynamic_benchmarks).T.reset_index().rename(columns={'index':'Name','id':'Identifier','pnl':'Target PnL','dit':'Target Days','stability':'Min Stability','debit_per_lot':'Typical Debit'})
    edited_cfg = st.data_editor(cfg_df, num_rows="dynamic", use_container_width=True)
    if st.button("Save Config"):
        if update_strategy_config(edited_cfg): st.success("Saved"); st.cache_data.clear(); st.rerun()
    
    st.divider()
    st.markdown("### Adaptive Rulebook")
    st.markdown(generate_adaptive_rulebook_text(expired_df, sorted(dynamic_benchmarks.keys())))
