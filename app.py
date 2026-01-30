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
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- CUSTOM CSS FOR COMPACT UI ---
st.markdown("""
<style>
    .block-container {padding-top: 1rem; padding-bottom: 2rem;}
    div[data-testid="stMetricValue"] {font-size: 1.4rem;}
    .stTabs [data-baseweb="tab-list"] {gap: 10px;}
    .stTabs [data-baseweb="tab"] {height: 50px; white-space: pre-wrap; background-color: #f0f2f6; border-radius: 4px 4px 0px 0px; padding: 10px 20px;}
    .stTabs [aria-selected="true"] {background-color: #ffffff; border-top: 2px solid #ff4b4b;}
</style>
""", unsafe_allow_html=True)

# --- DB CONSTANTS ---
DB_NAME = "trade_guardian_v4.db"
SCOPES = ['https://www.googleapis.com/auth/drive']

# ==========================================
#      BACKEND LOGIC (Classes & Funcs)
# ==========================================

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
                self.creds = service_account.Credentials.from_service_account_info(
                    service_account_info, scopes=SCOPES)
                self.service = build('drive', 'v3', credentials=self.creds)
                self.is_connected = True
            except Exception as e:
                st.error(f"Cloud Config Error: {e}")

    def list_files_debug(self):
        if not self.is_connected: return []
        try:
            results = self.service.files().list(
                pageSize=10, fields="files(id, name, modifiedTime)").execute()
            return results.get('files', [])
        except Exception as e:
            return []

    def find_db_file(self):
        if not self.is_connected: return None, None
        if self.cached_file_id:
            try:
                file = self.service.files().get(fileId=self.cached_file_id, fields='id,name').execute()
                return file['id'], file['name']
            except:
                self.cached_file_id = None

        try:
            query_exact = f"name='{DB_NAME}' and trashed=false"
            results = self.service.files().list(q=query_exact, pageSize=1, fields="files(id, name)").execute()
            items = results.get('files', [])
            if items: 
                self.cached_file_id = items[0]['id']
                return items[0]['id'], items[0]['name']

            query_fuzzy = "name contains 'trade_guardian' and name contains '.db' and trashed=false"
            results = self.service.files().list(q=query_fuzzy, pageSize=5, fields="files(id, name)").execute()
            items = results.get('files', [])
            
            if items:
                selected = items[0]
                for item in items:
                    if item['name'].startswith("trade_guardian_v4"):
                        selected = item
                        break
                self.cached_file_id = selected['id']
                return selected['id'], selected['name']
            return None, None
        except Exception as e:
            return None, None

    def get_cloud_modified_time(self, file_id):
        try:
            file = self.service.files().get(fileId=file_id, fields='modifiedTime').execute()
            dt = datetime.strptime(file['modifiedTime'].replace('Z', '+0000'), '%Y-%m-%dT%H:%M:%S.%f%z')
            return dt
        except:
            return None

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
        except Exception as e:
            print(f"Backup failed: {e}")
            return False

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
                 return False, f"CONFLICT: Cloud file is newer. Pull first."

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

# --- DATABASE INIT ---
def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    if not os.path.exists(DB_NAME) and drive_mgr.is_connected:
        success, msg = drive_mgr.download_db()
        if success: st.toast(f"☁️ Cloud Data Loaded: {msg}")
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT, parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT, snapshot_date DATE, pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL, FOREIGN KEY(trade_id) REFERENCES trades(id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER, min_stability REAL, description TEXT, typical_debit REAL)''')
    
    def add_column_safe(table, col_name, col_type):
        try: c.execute(f"SELECT {col_name} FROM {table} LIMIT 1")
        except: 
            try: c.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
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
    except Exception as e: print(f"Seeding error: {e}")
    finally: conn.close()

@st.cache_data(ttl=60)
def load_strategy_config():
    if not os.path.exists(DB_NAME): return {}
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM strategy_config", conn)
        config = {}
        for _, row in df.iterrows():
            typ_debit = row['typical_debit'] if 'typical_debit' in row and pd.notnull(row['typical_debit']) else 5000
            config[row['name']] = {
                'id': row['identifier'], 'pnl': row['target_pnl'], 'dit': row['target_days'],
                'stability': row['min_stability'], 'debit_per_lot': typ_debit
            }
        return config
    except: return {}
    finally: conn.close()

# --- HELPER FUNCTIONS ---
def get_strategy_dynamic(trade_name, group_name, config_dict):
    t_name, g_name = str(trade_name).upper().strip(), str(group_name).upper().strip()
    sorted_strats = sorted(config_dict.items(), key=lambda x: len(str(x[1]['id'])), reverse=True)
    for strat_name, details in sorted_strats:
        if str(details['id']).upper() in t_name: return strat_name
    for strat_name, details in sorted_strats:
        if str(details['id']).upper() in g_name: return strat_name
    return "Other"

def clean_num(x):
    try:
        if pd.isna(x) or str(x).strip() == "": return 0.0
        val = float(str(x).replace('$', '').replace(',', '').replace('%', '').strip())
        return 0.0 if np.isnan(val) else val
    except: return 0.0

def safe_fmt(val, fmt_str):
    try: return fmt_str.format(val) if isinstance(val, (int, float)) else str(val)
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
            if ticker in ['M200', '130', '160', 'IRON', 'VERTICAL', 'SMSF']: return "UNKNOWN"
            return ticker
        return "UNKNOWN"
    except: return "UNKNOWN"

def theta_decay_model(initial_theta, days_held, strategy, dte_at_entry=45):
    t_frac = min(1.0, days_held / dte_at_entry) if dte_at_entry > 0 else 1.0
    if strategy in ['M200', '130/160', '160/190', 'SMSF']:
        decay_factor = 1 - (2 * t_frac) ** 2 if t_frac < 0.5 else 2 * (1 - t_frac)
        return initial_theta * max(0, decay_factor)
    elif 'VERTICAL' in str(strategy).upper() or 'DIRECTIONAL' in str(strategy).upper():
        decay_factor = 1 - t_frac if t_frac < 0.7 else 0.3 * np.exp(-5 * (t_frac - 0.7))
        return initial_theta * decay_factor
    else:
        decay_factor = np.exp(-2 * t_frac)
        return initial_theta * (1 - decay_factor)

def get_trade_lifecycle_data(row, snapshots_df):
    days = int(row['Days Held'])
    days = 1 if days < 1 else days
    total_pnl = row['P&L']
    
    # 1. Try Real Snapshots
    if snapshots_df is not None and not snapshots_df.empty:
        trade_snaps = snapshots_df[snapshots_df['trade_id'] == row['id']].sort_values('days_held').copy()
        if len(trade_snaps) >= 2:
            trade_snaps['Cumulative_PnL'] = trade_snaps['pnl']
            trade_snaps['Day'] = trade_snaps['days_held']
            if trade_snaps['Day'].min() > 0:
                trade_snaps = pd.concat([pd.DataFrame({'Day': [0], 'Cumulative_PnL': [0]}), trade_snaps], ignore_index=True)
            if row['Status'] == 'Expired':
                last_snap_day = trade_snaps['Day'].max()
                if last_snap_day < days:
                    trade_snaps = pd.concat([trade_snaps, pd.DataFrame({'Day': [days], 'Cumulative_PnL': [total_pnl]})], ignore_index=True)
            trade_snaps['Pct_Duration'] = (trade_snaps['Day'] / days) * 100
            denom = abs(total_pnl) if abs(total_pnl) > 0 else 1
            trade_snaps['Pct_PnL'] = (trade_snaps['Cumulative_PnL'] / denom) * 100
            return trade_snaps[['Day', 'Cumulative_PnL', 'Pct_Duration', 'Pct_PnL']]

    # 2. Reconstruction (Theoretical Model)
    daily_data = []
    initial_theta = row['Theta'] if row['Theta'] != 0 else 1.0
    weights = [abs(theta_decay_model(initial_theta, d, row['Strategy'], max(45, days))) for d in range(1, days + 1)]
    total_w = sum(weights)
    if total_w == 0: weights = [1/days] * days
    else: weights = [w/total_w for w in weights]
    
    cum_pnl = 0
    daily_data.append({'Day': 0, 'Cumulative_PnL': 0, 'Pct_Duration': 0, 'Pct_PnL': 0})
    for i, w in enumerate(weights):
        day_num = i + 1
        cum_pnl += total_pnl * w
        denom = abs(total_pnl) if abs(total_pnl) > 0 else 1
        daily_data.append({
            'Day': day_num, 'Cumulative_PnL': cum_pnl,
            'Pct_Duration': (day_num / days) * 100, 'Pct_PnL': (cum_pnl / denom) * 100
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
        days = trade['Days Held']
        days = 1 if days <= 0 else days
        total_pnl = trade['P&L']
        initial_theta = trade['Theta'] if trade['Theta'] != 0 else 1.0
        
        daily_theta_weights = [abs(theta_decay_model(initial_theta, day, trade['Strategy'], max(45, days))) for day in range(days)]
        total_theta_sum = sum(daily_theta_weights)
        if total_theta_sum == 0: daily_theta_weights = [1/days] * days
        else: daily_theta_weights = [w/total_theta_sum for w in daily_theta_weights]
        
        curr = trade['Entry Date']
        for day_weight in daily_theta_weights:
            if curr.date() in daily_pnl_dict:
                daily_pnl_dict[curr.date()] += total_pnl * day_weight
            else:
                daily_pnl_dict[curr.date()] = total_pnl * day_weight
            curr += pd.Timedelta(days=1)
    return daily_pnl_dict

# --- FILE PARSING & SYNC ---
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
                file.seek(0)
                df_raw = pd.read_excel(file, header=header_row)
                if 'Link' in df_raw.columns:
                     # Hyperlink extraction logic omitted for brevity, assumes column exists or similar structure
                     pass 
            except: pass

        if df_raw is None:
            file.seek(0)
            content = file.getvalue().decode("utf-8", errors='ignore')
            lines = content.split('\n')
            header_row = 0
            for i, line in enumerate(lines[:30]):
                if "Name" in line and "Total Return" in line:
                    header_row = i; break
            file.seek(0)
            df_raw = pd.read_csv(file, skiprows=header_row)

        parsed_trades = []
        current_trade = None
        current_legs = []

        def finalize_trade(trade_data, legs, f_type):
            if not trade_data.any(): return None
            name, group = str(trade_data.get('Name', '')), str(trade_data.get('Group', ''))
            created = trade_data.get('Created At', '')
            try: start_dt = pd.to_datetime(created)
            except: return None 
            strat = get_strategy_dynamic(name, group, config_dict)
            link = str(trade_data.get('Link', ''))
            if link == 'nan' or link == 'Open': link = "" 
            
            pnl, debit = clean_num(trade_data.get('Total Return $', 0)), abs(clean_num(trade_data.get('Net Debit/Credit', 0)))
            theta, delta = clean_num(trade_data.get('Theta', 0)), clean_num(trade_data.get('Delta', 0))
            gamma, vega, iv = clean_num(trade_data.get('Gamma', 0)), clean_num(trade_data.get('Vega', 0)), clean_num(trade_data.get('IV', 0))

            exit_dt = None
            try:
                raw_exp = trade_data.get('Expiration')
                if pd.notnull(raw_exp) and str(raw_exp).strip() != '': exit_dt = pd.to_datetime(raw_exp)
            except: pass

            days_held = (exit_dt - start_dt).days if exit_dt and f_type == "History" else (datetime.now() - start_dt).days
            if days_held < 1: days_held = 1
            
            strat_config = config_dict.get(strat, {})
            typical_debit = strat_config.get('debit_per_lot', 5000)
            lot_match = re.search(r'(\d+)\s*(?:LOT|L\b)', name, re.IGNORECASE)
            lot_size = int(lot_match.group(1)) if lot_match else int(round(debit / typical_debit))
            if lot_size < 1: lot_size = 1

            put_pnl, call_pnl = 0.0, 0.0
            if f_type == "History":
                for leg in legs:
                    if len(leg) < 5: continue
                    sym = str(leg.iloc[0]) 
                    if not sym.startswith('.'): continue
                    try:
                        leg_pnl = (clean_num(leg.iloc[4]) - clean_num(leg.iloc[2])) * clean_num(leg.iloc[1]) * 100
                        if 'P' in sym and 'C' not in sym: put_pnl += leg_pnl
                        elif 'C' in sym and 'P' not in sym: call_pnl += leg_pnl
                        elif re.search(r'[0-9]P[0-9]', sym): put_pnl += leg_pnl
                        elif re.search(r'[0-9]C[0-9]', sym): call_pnl += leg_pnl
                    except: pass
            
            t_id = generate_id(name, strat, start_dt)
            return {
                'id': t_id, 'name': name, 'strategy': strat, 'start_dt': start_dt, 'exit_dt': exit_dt,
                'days_held': days_held, 'debit': debit, 'lot_size': lot_size, 'pnl': pnl, 
                'theta': theta, 'delta': delta, 'gamma': gamma, 'vega': vega, 'iv': iv,
                'put_pnl': put_pnl, 'call_pnl': call_pnl, 'link': link, 'group': group
            }

        cols = df_raw.columns
        if 'Name' not in [str(c) for c in cols] or 'Total Return $' not in [str(c) for c in cols]: return []

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
        count_new, count_update = 0, 0
        try:
            trades_data = parse_optionstrat_file(file, file_type, config_dict)
            if not trades_data:
                log.append(f" {file.name}: Skipped (No trades)")
                continue

            for t in trades_data:
                trade_id = t['id']
                if file_type == "Active": file_found_ids.add(trade_id)
                
                c.execute("SELECT id, status, theta, delta, gamma, vega, put_pnl, call_pnl, iv, link, lot_size, strategy FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
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
                    db_lot_size = existing[10]
                    final_lot = db_lot_size if db_lot_size and db_lot_size > 0 else t['lot_size']
                    final_strat = t['strategy'] if existing[11] == 'Other' and t['strategy'] != 'Other' else existing[11]
                    
                    old_status = existing[1]
                    final_theta = t['theta'] if t['theta'] != 0 else existing[2]
                    final_delta = t['delta'] if t['delta'] != 0 else 0
                    final_gamma = t['gamma'] if t['gamma'] != 0 else 0
                    final_vega = t['vega'] if t['vega'] != 0 else 0
                    final_iv = t['iv'] if t['iv'] != 0 else (existing[8] if existing[8] else 0)
                    final_put = t['put_pnl'] if t['put_pnl'] != 0 else (existing[6] if existing[6] else 0)
                    final_call = t['call_pnl'] if t['call_pnl'] != 0 else (existing[7] if existing[7] else 0)
                    final_link = t['link'] if t['link'] != "" else (existing[9] if existing[9] else "")

                    if file_type == "History":
                        c.execute('''UPDATE trades SET pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, put_pnl=?, call_pnl=?, iv=?, link=?, lot_size=?, strategy=?, original_group=? WHERE id=?''', 
                            (t['pnl'], status, t['exit_dt'].date() if t['exit_dt'] else None, t['days_held'], final_theta, final_delta, final_gamma, final_vega, final_put, final_call, final_iv, final_link, final_lot, final_strat, t['group'], trade_id))
                        count_update += 1
                    elif old_status in ["Active", "Missing"]: 
                        c.execute('''UPDATE trades SET pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, iv=?, link=?, status='Active', exit_date=?, lot_size=?, strategy=?, original_group=? WHERE id=?''', 
                            (t['pnl'], t['days_held'], final_theta, final_delta, final_gamma, final_vega, final_iv, final_link, t['exit_dt'].date() if t['exit_dt'] else None, final_lot, final_strat, t['group'], trade_id))
                        count_update += 1
                
                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    if not c.fetchone():
                        c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)",
                                  (trade_id, today, t['pnl'], t['days_held'], t['theta'] or 0, t['delta'] or 0, t['vega'] or 0, t['gamma'] or 0))
                    else:
                        c.execute("UPDATE snapshots SET theta=?, delta=?, vega=?, gamma=? WHERE trade_id=? AND snapshot_date=?",
                                  (t['theta'] or 0, t['delta'] or 0, t['vega'] or 0, t['gamma'] or 0, trade_id, today))
            log.append(f" {file.name}: {count_new} New, {count_update} Updated")
        except Exception as e:
            log.append(f" {file.name}: Error - {str(e)}")
            
    if file_type == "Active" and file_found_ids:
        missing_ids = db_active_ids - file_found_ids
        if missing_ids:
            placeholders = ','.join('?' for _ in missing_ids)
            c.execute(f"UPDATE trades SET status = 'Missing' WHERE id IN ({placeholders})", list(missing_ids))
            log.append(f" Integrity: Marked {len(missing_ids)} trades as 'Missing'.")
    conn.commit()
    conn.close()
    return log

def update_journal(edited_df):
    conn = get_db_connection()
    c = conn.cursor()
    count = 0
    try:
        for index, row in edited_df.iterrows():
            new_lot = int(row['lot_size']) if 'lot_size' in row and row['lot_size'] > 0 else 1
            c.execute("UPDATE trades SET notes=?, tags=?, parent_id=?, lot_size=?, strategy=? WHERE id=?", 
                      (str(row['Notes']), str(row['Tags']), str(row['Parent ID']), new_lot, str(row['Strategy']), row['id']))
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
    except Exception as e: return False
    finally: conn.close()

def reprocess_other_trades():
    conn = get_db_connection()
    c = conn.cursor()
    config_dict = load_strategy_config()
    try: c.execute("SELECT id, name, original_group, strategy FROM trades")
    except: c.execute("SELECT id, name, '', strategy FROM trades")
    all_trades = c.fetchall()
    updated_count = 0
    for t_id, t_name, t_group, current_strat in all_trades:
        if current_strat == "Other":
            new_strat = get_strategy_dynamic(t_name, t_group if t_group else "", config_dict) 
            if new_strat != "Other":
                c.execute("UPDATE trades SET strategy = ? WHERE id = ?", (new_strat, t_id))
                updated_count += 1
    conn.commit()
    conn.close()
    return updated_count

# --- DATA LOADER ---
@st.cache_data(ttl=60)
def load_data():
    empty_schema = pd.DataFrame(columns=['id', 'Name', 'Strategy', 'Status', 'P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'Entry Date', 'Exit Date', 'Notes', 'Tags', 'Parent ID', 'Put P&L', 'Call P&L', 'IV', 'Link', 'lot_size', 'Debit/Lot', 'ROI', 'Daily Yield %', 'Ann. ROI', 'Theta Pot.', 'Theta Eff.', 'Theta/Cap %', 'Ticker', 'Stability', 'Grade', 'Reason', 'P&L Vol'])
    
    if not os.path.exists(DB_NAME): return empty_schema
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        if df.empty: return empty_schema
        
        snaps = pd.read_sql("SELECT trade_id, pnl FROM snapshots", conn)
        if not snaps.empty:
            vol_df = snaps.groupby('trade_id')['pnl'].std().reset_index().rename(columns={'pnl': 'P&L Vol'})
            df = df.merge(vol_df, left_on='id', right_on='trade_id', how='left')
            df['P&L Vol'] = df['P&L Vol'].fillna(0)
        else: df['P&L Vol'] = 0.0
    except Exception as e: return empty_schema
    finally: conn.close()
    
    if not df.empty:
        df = df.rename(columns={'name': 'Name', 'strategy': 'Strategy', 'status': 'Status', 'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held', 'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega', 'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes', 'tags': 'Tags', 'parent_id': 'Parent ID', 'put_pnl': 'Put P&L', 'call_pnl': 'Call P&L', 'iv': 'IV', 'link': 'Link'})
        
        for col in ['Gamma', 'Vega', 'Theta', 'Delta', 'P&L', 'Debit', 'lot_size', 'Notes', 'Tags', 'Parent ID', 'Put P&L', 'Call P&L', 'IV', 'Link']:
            if col not in df.columns: df[col] = "" if col in ['Notes', 'Tags', 'Parent ID', 'Link'] else 0.0
        
        numeric_cols = ['Debit', 'P&L', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'IV', 'Put P&L', 'Call P&L']
        for c in numeric_cols: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)

        df['Entry Date'] = pd.to_datetime(df['Entry Date']); df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        df['lot_size'] = pd.to_numeric(df['lot_size'], errors='coerce').fillna(1).astype(int).apply(lambda x: 1 if x < 1 else x)
        df['Debit/Lot'] = np.where(df['lot_size'] > 0, df['Debit'] / df['lot_size'], df['Debit'])
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = np.where(df['Debit'] > 0, (df['Theta'] / df['Debit']) * 100, 0)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        df['Parent ID'] = df['Parent ID'].astype(str).str.strip().replace('nan', '').replace('None', '')
        df['Stability'] = np.where(df['Theta'] > 0, df['Theta'] / (df['Delta'].abs() + 1), 0.0)
        
        def get_grade(row):
            s, d = row['Strategy'], row['Debit/Lot']
            grade, reason = "C", "Standard"
            if s == '130/160':
                if d > 4800: grade, reason="F", "Overpriced (> $4.8k)"
                elif 3500 <= d <= 4500: grade, reason="A+", "Sweet Spot"
                else: grade, reason="B", "Acceptable"
            elif s == '160/190':
                if 4800 <= d <= 5500: grade, reason="A", "Ideal Pricing"
                else: grade, reason="C", "Check Pricing"
            elif s == 'M200':
                if 7500 <= d <= 8500: grade, reason = "A", "Perfect Entry"
                else: grade, reason = "B", "Variance"
            elif s == 'SMSF':
                if d > 15000: grade, reason="B", "High Debit" 
                else: grade, reason="A", "Standard"
            return pd.Series([grade, reason])

        df[['Grade', 'Reason']] = df.apply(get_grade, axis=1)
    return df

@st.cache_data(ttl=300)
def load_snapshots():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        q = "SELECT s.snapshot_date, s.pnl, s.days_held, s.theta, s.delta, s.vega, s.gamma, t.strategy, t.name, t.id as trade_id, t.theta as initial_theta FROM snapshots s JOIN trades t ON s.trade_id = t.id"
        df = pd.read_sql(q, conn)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        for c in ['pnl', 'days_held', 'theta', 'delta', 'vega', 'gamma', 'initial_theta']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- ANALYTICS ---
def calculate_kelly_fraction(win_rate, avg_win, avg_loss):
    if avg_loss == 0 or avg_win <= 0: return 0.0
    b = abs(avg_win / avg_loss)
    kelly = (win_rate * b - (1 - win_rate)) / b
    return max(0, min(kelly * 0.5, 0.25))

def generate_trade_predictions(active_df, history_df, prob_low, prob_high, total_capital=100000):
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
        avg_win = nearest_neighbors[nearest_neighbors['P&L'] > 0]['P&L'].mean()
        avg_loss = nearest_neighbors[nearest_neighbors['P&L'] < 0]['P&L'].mean()
        if pd.isna(avg_win): avg_win = 0
        if pd.isna(avg_loss): avg_loss = -avg_pnl * 0.5
        kelly_size = calculate_kelly_fraction(win_prob, avg_win, avg_loss)
        rec = "HOLD"
        if win_prob * 100 < prob_low: rec = "REDUCE/CLOSE"
        elif win_prob * 100 > prob_high: rec = "PRESS WINNER"
        predictions.append({
            'Trade Name': row['Name'], 'Strategy': row['Strategy'], 'Win Prob %': win_prob * 100,
            'Expected PnL': avg_pnl, 'Kelly Size %': kelly_size * 100, 'Rec. Size ($)': kelly_size * total_capital,
            'AI Rec': rec, 'Confidence': max(0, 100 - (distances[top_k_idx].mean() * 10))
        })
    return pd.DataFrame(predictions)

def get_mae_stats(conn):
    query = "SELECT t.strategy, MIN(s.pnl) as worst_drawdown FROM snapshots s JOIN trades t ON s.trade_id = t.trade_id WHERE t.status = 'CLOSED' AND t.pnl > 0 GROUP BY t.trade_id, t.strategy"
    try:
        df = pd.read_sql(query, conn)
        mae_stats = {}
        if not df.empty:
            for strategy in df['strategy'].unique():
                strat_df = df[df['strategy'] == strategy]
                if not strat_df.empty: mae_stats[strategy] = strat_df['worst_drawdown'].quantile(0.05)
        return mae_stats
    except Exception as e: return {}

def get_velocity_stats(expired_df):
    velocity_stats = {}
    if expired_df.empty: return velocity_stats
    winners = expired_df[expired_df['P&L'] > 0].copy()
    if winners.empty: return velocity_stats
    winners['days_held'] = winners['days_held'].replace(0, 1)
    winners['velocity'] = winners['P&L'] / winners['days_held']
    for strategy in winners['Strategy'].unique():
        s_df = winners[winners['Strategy'] == strategy]
        if len(s_df) > 2:
            mean_v = s_df['velocity'].mean()
            std_v = s_df['velocity'].std()
            velocity_stats[strategy] = {'threshold': mean_v + (2 * std_v), 'mean': mean_v}
    return velocity_stats

def check_rot_and_efficiency(row, hist_avg_days):
    try:
        current_pnl, days = row['P&L'], max(1, row.get('Days', 1))
        theta, current_speed = row.get('Net Theta', 0), current_pnl / days
        theta_efficiency = (current_speed / theta) if theta != 0 else 0
        status = "Healthy"
        if days > hist_avg_days * 1.2 and current_pnl < 0: status = "Rotting (Time > Avg & Red)"
        elif theta_efficiency < 0.2 and days > 10 and current_pnl > 0: status = "Inefficient (Theta Stuck)"
        elif theta_efficiency < 0 and days > 5: status = "Bleeding (Negative Efficiency)"
        return current_speed, theta_efficiency, status
    except: return 0, 0, "Error"

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

def calculate_portfolio_metrics(trades_df, capital):
    if trades_df.empty or capital <= 0: return 0.0, 0.0
    daily_pnl_dict = reconstruct_daily_pnl(trades_df)
    equity = capital
    daily_returns = pd.Series([equity + daily_pnl_dict.get(d.date(), 0) for d in pd.date_range(start=trades_df['Entry Date'].min(), end=max(trades_df['Exit Date'].max(), pd.Timestamp.now()))]).pct_change().dropna()
    sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252) if daily_returns.std() != 0 else 0.0
    total_days = max(1, (max(trades_df['Exit Date'].max(), pd.Timestamp.now()) - trades_df['Entry Date'].min()).days)
    end_val = capital + trades_df['P&L'].sum()
    cagr = ((end_val / capital) ** (365 / total_days)) - 1 if total_days > 0 else 0.0
    return sharpe, cagr * 100

def check_concentration_risk(active_df, total_equity, threshold=0.15):
    if active_df.empty or total_equity <= 0: return pd.DataFrame()
    warnings = []
    for _, row in active_df.iterrows():
        concentration = row['Debit'] / total_equity
        if concentration > threshold:
            warnings.append({'Trade': row['Name'], 'Strategy': row['Strategy'], 'Size %': f"{concentration:.1%}", 'Risk': f"${row['Debit']:,.0f}", 'Limit': f"{threshold:.0%}"})
    return pd.DataFrame(warnings)

def calculate_max_drawdown(trades_df, initial_capital):
    if trades_df.empty or initial_capital <= 0: return {'Max Drawdown %': 0.0, 'Current DD %': 0.0}
    daily_pnl_dict = reconstruct_daily_pnl(trades_df)
    equity = initial_capital
    equity_curve = []
    for d in pd.date_range(start=trades_df['Entry Date'].min(), end=max(trades_df['Exit Date'].max(), pd.Timestamp.now())):
        equity += daily_pnl_dict.get(d.date(), 0)
        equity_curve.append(equity)
    equity_series = pd.Series(equity_curve)
    running_max = equity_series.cummax()
    drawdown = (equity_series - running_max) / running_max
    return {'Max Drawdown %': drawdown.min() * 100, 'Current DD %': drawdown.iloc[-1] * 100}

def rolling_correlation_matrix(snaps, window_days=30):
    if snaps.empty: return None
    strat_daily = snaps.pivot_table(index='snapshot_date', columns='strategy', values='pnl', aggfunc='sum')
    if len(strat_daily) < window_days: return None
    return px.imshow(strat_daily.tail(30).corr(), text_auto=".2f", aspect="auto", color_continuous_scale="RdBu", title="Strategy Correlation (30 Days)")

def generate_adaptive_rulebook_text(history_df, strategies):
    text = "## 📜 The Adaptive Trader's Constitution\n*Rules evolve based on data.*\n\n"
    if history_df.empty: return text + " *Not enough data yet.*"
    for strat in strategies:
        strat_df = history_df[history_df['Strategy'] == strat]
        if strat_df.empty: continue
        winners = strat_df[strat_df['P&L'] > 0]
        text += f"### {strat}\n"
        if not winners.empty:
            best_day = winners.copy(); best_day['Day'] = best_day['Entry Date'].dt.day_name()
            text += f"* **Best Entry:** {best_day.groupby('Day')['P&L'].mean().idxmax()} | **Hold:** {winners['Days Held'].mean():.0f}d | **Cost:** ${winners['Debit/Lot'].mean():,.0f}\n"
        losers = strat_df[strat_df['P&L'] < 0]
        if not losers.empty: text += f"* **Losses:** Held avg {losers['Days Held'].mean():.0f} days.\n"
    return text

def calculate_decision_ladder(row, benchmarks_dict, regime_mult):
    strat, days, pnl, status = row['Strategy'], row['Days Held'], row['P&L'], row['Status']
    theta, stability, debit, lot_size = row['Theta'], row['Stability'], row['Debit'], max(1, row.get('lot_size', 1))
    
    if status == 'Missing': return "REVIEW", 100, "Missing data", 0, "Error"
    
    bench = benchmarks_dict.get(strat, {})
    hist_avg_pnl, hist_avg_days = bench.get('pnl', 1000), bench.get('dit', 40)
    target_profit = (hist_avg_pnl * regime_mult) * lot_size
    
    score, action, reason, juice_val, juice_type = 50, "HOLD", "Normal", 0.0, "Neutral"

    if pnl < 0:
        juice_type = "Recovery Days"
        if theta > 0:
            recov_days = abs(pnl) / theta
            juice_val = recov_days
            is_cooking = (strat == '160/190' and days < 30)
            remaining_time_est = max(1, hist_avg_days - days)
            if not is_cooking and days >= 15 and recov_days > remaining_time_est:
                score += 40; action = "STRUCTURAL FAILURE"; reason = f"Zombie (Recov {recov_days:.0f}d > Left {remaining_time_est:.0f}d)"
        else:
            juice_val = 999; score += 30; reason = "Negative Theta"
    else:
        juice_type = "Left in Tank"
        juice_val = max(0, target_profit - pnl)
        if debit > 0 and (juice_val / debit) < 0.05: score += 40; reason = "Squeezed Dry"
        elif juice_val < (100 * lot_size): score += 35; reason = "Empty Tank"

    if pnl >= target_profit: return "TAKE PROFIT", 100, f"Hit Target ${target_profit:.0f}", juice_val, juice_type
    elif pnl >= target_profit * 0.8: score += 30; action = "PREPARE EXIT"; reason = "Near Target"
    
    stale_threshold = hist_avg_days * 1.25 
    if strat == '130/160':
        limit_130 = min(stale_threshold, 30) 
        if days > limit_130 and pnl < (100 * lot_size): return "KILL", 95, f"Stale (> {limit_130:.0f}d)", juice_val, juice_type
        elif days > (limit_130 * 0.8): score += 20; reason = "Aging"
    elif strat == '160/190':
        cooking_limit = max(30, hist_avg_days * 0.7)
        if days < cooking_limit: score = 10; action = "COOKING"; reason = f"Early (<{cooking_limit:.0f}d)"
        elif days > stale_threshold: score += 25; action = "WATCH"; reason = f"Mature (>{stale_threshold:.0f}d)"
    
    if stability < 0.3 and days > 5: score += 25; reason += " + Unstable"; action = "RISK REVIEW"
    
    if pnl > 0 and days > 5:
        hist_vel = hist_avg_pnl / max(1, hist_avg_days)
        curr_vel = pnl / days
        if curr_vel < (hist_vel * 0.6) and pnl > (target_profit * 0.2):
             score += 15; action = "REDEPLOY?" if action == "HOLD" else action; reason = f"Slow Capital (${curr_vel:.0f}/d)"

    score = min(100, max(0, score))
    if score >= 90: action = "CRITICAL"
    elif score >= 70: action = "WATCH"
    elif score <= 30: action = "COOKING"
    return action, score, reason, juice_val, juice_type

# ==========================================
#              INITIALIZATION
# ==========================================
init_db()

# --- LOAD DATA ---
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
active_df = pd.DataFrame()

if not df.empty and 'Status' in df.columns:
    expired_df = df[df['Status'] == 'Expired']
    active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
    
    try:
        if not expired_df.empty:
            velocity_stats = get_velocity_stats(expired_df)
            try:
                temp_conn = get_db_connection()
                mae_stats = get_mae_stats(temp_conn)
                temp_conn.close()
            except: pass

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
    except Exception as e: print(f"Stats Error: {e}")

st.title("🛡️ Allantis Trade Guardian")

# ==========================================
#               SIDEBAR
# ==========================================
st.sidebar.markdown("### Settings")
prime_cap = st.sidebar.number_input("Prime Cap", value=115000, step=1000)
smsf_cap = st.sidebar.number_input("SMSF Cap", value=150000, step=1000)
total_cap = prime_cap + smsf_cap
market_regime = st.sidebar.selectbox("Regime", ["Neutral", "Bullish (Aggr)", "Bearish (Safe)"], index=0)
regime_mult = 1.10 if "Bullish" in market_regime else 0.90 if "Bearish" in market_regime else 1.0

# Pre-calc Ladder for UI
if not active_df.empty:
    ladder_results = active_df.apply(lambda row: calculate_decision_ladder(row, dynamic_benchmarks, regime_mult), axis=1)
    active_df['Action'] = [x[0] for x in ladder_results]
    active_df['Urgency Score'] = [x[1] for x in ladder_results]
    active_df['Reason'] = [x[2] for x in ladder_results]
    active_df['Juice Val'] = [x[3] for x in ladder_results]
    active_df['Juice Type'] = [x[4] for x in ladder_results]
    active_df = active_df.sort_values('Urgency Score', ascending=False)
    todo_df = active_df[active_df['Urgency Score'] >= 70]
else:
    todo_df = pd.DataFrame()

# --- CLOUD SYNC SIDEBAR ---
if GOOGLE_DEPS_INSTALLED:
    st.sidebar.divider()
    with st.sidebar.expander("☁️ Cloud Sync", expanded=True):
        if not drive_mgr.is_connected: st.error("No secrets found.")
        else:
            c1, c2 = st.columns(2)
            if c1.button("⬆️ Push"):
                success, msg = drive_mgr.upload_db()
                if success: st.success("Saved!"); st.session_state['show_conflict'] = False
                elif "CONFLICT" in msg: st.error(msg); st.session_state['show_conflict'] = True
                else: st.error(msg)
            if c2.button("⬇️ Pull"):
                success, msg = drive_mgr.download_db()
                if success: st.success("Loaded!"); st.cache_data.clear(); st.rerun()
                elif "CONFLICT" in msg: st.error(msg); st.session_state['show_pull_conflict'] = True
                else: st.error(msg)
            
            # Conflict UI
            if st.session_state.get('show_conflict'):
                if st.button("⬇️ Pull (Safe)"):
                    drive_mgr.download_db(); st.session_state['show_conflict'] = False; st.cache_data.clear(); st.rerun()
                if st.button("⚠️ Force Push"):
                    drive_mgr.upload_db(force=True); st.session_state['show_conflict'] = False
            
            # Sync Status
            last_sync = st.session_state.get('last_cloud_sync')
            if last_sync: st.caption(f"Last Sync: {last_sync.strftime('%H:%M')}")
            else: st.caption("Status: Unsaved")

    with st.sidebar.expander("📂 File Operations", expanded=False):
        active_up = st.file_uploader("Active Trades", accept_multiple_files=True, key="act")
        history_up = st.file_uploader("History (Closed)", accept_multiple_files=True, key="hist")
        if st.button("Process Files"):
            logs = []
            if active_up: logs.extend(sync_data(active_up, "Active"))
            if history_up: logs.extend(sync_data(history_up, "History"))
            if logs: 
                for l in logs: st.write(l)
                st.cache_data.clear(); auto_sync_if_connected(); st.rerun()
        
        st.markdown("---")
        if st.button("💾 Backup Local DB"):
            with open(DB_NAME, "rb") as f:
                st.download_button("Download", f, "trade_guardian_backup.db", "application/x-sqlite3")

# ==========================================
#            MAIN TABS ARCHITECTURE
# ==========================================

# Redesigned for Workflow Priority
tab_command, tab_ops, tab_intel, tab_config = st.tabs([
    "🛡️ Command Center", 
    "⚡ Active Ops", 
    "🧠 Intelligence", 
    "⚙️ Strategy & Rules"
])

# -----------------------------------------------------------------------------
# TAB 1: COMMAND CENTER (Priority Actions & Health)
# -----------------------------------------------------------------------------
with tab_command:
    # 1. PRE-FLIGHT (Collapsible Top)
    with st.expander("✈️ Pre-Flight Calculator (Check before you trade)", expanded=False):
        pf_c1, pf_c2, pf_c3 = st.columns(3)
        pf_goal = pf_c1.selectbox("Profile", ["Hedged Income", "Standard Income", "Directional", "Speculative Vol"])
        pf_dte = pf_c1.number_input("DTE", 1, 100, 45)
        pf_price = pf_c2.number_input("Price ($)", 0.0, 50000.0, 5000.0, step=100.0)
        pf_theta = pf_c2.number_input("Theta ($)", 0.0, 500.0, 15.0)
        pf_delta = pf_c3.number_input("Delta", -500.0, 500.0, -10.0)
        pf_vega = pf_c3.number_input("Vega", -500.0, 500.0, 100.0)
        
        if st.button("Run Check"):
            res_c1, res_c2, res_c3 = st.columns(3)
            stability = pf_theta / (abs(pf_delta) + 1) if pf_delta != -1 else 0
            yield_pct = (pf_theta / abs(pf_price)) * 100 if pf_price > 0 else 0
            ann_roi = yield_pct * 365
            
            res_c1.metric("Stability", f"{stability:.2f}", delta="Good > 0.5")
            res_c2.metric("Ann. ROI", f"{ann_roi:.0f}%", delta="Target > 40%")
            if pf_dte < 21: res_c3.warning("⚠️ High Gamma Risk (Low DTE)")
            else: res_c3.success("✅ DTE Safe")

    # 2. PRIORITY ACTION QUEUE (Critical Items)
    st.subheader("🚨 Priority Action Queue")
    
    if not todo_df.empty:
        for _, row in todo_df.iterrows():
            with st.container():
                cols = st.columns([3, 2, 2, 2])
                color = "red" if row['Urgency Score'] >= 90 else "orange"
                is_link = str(row['Link']).startswith('http')
                name_disp = f"[{row['Name']}]({row['Link']})" if is_link else row['Name']
                
                cols[0].markdown(f"#### {name_disp}")
                cols[0].caption(f"{row['Strategy']} | {row['Days Held']} days")
                
                cols[1].markdown(f":{color}[**{row['Action']}**]")
                cols[1].caption(f"Reason: {row['Reason']}")
                
                gauge_color = "inverse" if row['Juice Type'] == 'Recovery Days' else "normal"
                val_fmt = f"{row['Juice Val']:.0f}d" if row['Juice Type'] == 'Recovery Days' else f"${row['Juice Val']:.0f}"
                cols[2].metric(row['Juice Type'], val_fmt, delta_color=gauge_color)
                
                cols[3].metric("P&L", f"${row['P&L']:,.0f}", delta=f"{row['ROI']:.1f}%")
                st.divider()
    else:
        st.success("🎉 All Quiet! No critical actions required.")

    # 3. PORTFOLIO VITAL SIGNS
    st.markdown("### 🏥 Portfolio Vitals")
    if not active_df.empty:
        tot_debit = active_df['Debit'].sum()
        if tot_debit == 0: tot_debit = 1
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Daily Theta Income", f"${active_df['Theta'].sum():,.0f}")
        col2.metric("Net Delta", f"{active_df['Delta'].sum():.0f}", help="Total directional risk")
        col3.metric("Avg Trade Age", f"{active_df['Days Held'].mean():.0f} days")
        
        target_allocation = {'130/160': 0.30, '160/190': 0.40, 'M200': 0.20, 'SMSF': 0.10}
        actual = active_df.groupby('Strategy')['Debit'].sum() / tot_debit
        alloc_score = 100 - sum(abs(actual.get(s, 0) - target_allocation.get(s, 0)) * 100 for s in target_allocation)
        col4.metric("Allocation Score", f"{alloc_score:.0f}/100")

        # HEATMAP
        fig_heat = px.scatter(
            active_df, x='Days Held', y='P&L', size='Debit',
            color='Urgency Score', color_continuous_scale='RdYlGn_r',
            hover_data=['Name', 'Strategy', 'Action'],
            title="Position Status Map (Size = Capital)"
        )
        fig_heat.add_hline(y=0, line_dash="dash", opacity=0.3)
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("No active trades found. Sync data in Sidebar.")

# -----------------------------------------------------------------------------
# TAB 2: ACTIVE OPS (The Workbench)
# -----------------------------------------------------------------------------
with tab_ops:
    op_tabs = st.tabs(["📒 Journal & Edit", "🧬 Trade DNA", "🔄 Rolls"])
    
    with op_tabs[0]:
        st.caption("Edit active trades here. Changes save to database.")
        if not active_df.empty:
            # Format DataFrame for Editor
            edit_view = active_df.copy()
            edit_view['Tank/Recov'] = edit_view.apply(lambda r: f"{r['Juice Val']:.0f}d" if r['Juice Type'] == 'Recovery Days' else f"${r['Juice Val']:.0f}", axis=1)
            
            column_config = {
                "id": None, 
                "Name": st.column_config.TextColumn("Name", disabled=True),
                "Strategy": st.column_config.SelectboxColumn("Strategy", options=sorted(list(dynamic_benchmarks.keys())) + ["Other"], required=True),
                "Urgency Score": st.column_config.ProgressColumn("Urgency", min_value=0, max_value=100, format="%d"),
                "Action": st.column_config.TextColumn("Action", disabled=True),
                "P&L": st.column_config.NumberColumn("P&L", format="$%d", disabled=True),
                "Debit": st.column_config.NumberColumn("Debit", format="$%d", disabled=True),
                "lot_size": st.column_config.NumberColumn("Lots", min_value=1, step=1),
                "Notes": st.column_config.TextColumn("Notes", width="large"),
                "Tags": st.column_config.SelectboxColumn("Tags", options=["Rolled", "Hedged", "Earnings", "High Risk", "Watch"]),
                "Link": st.column_config.LinkColumn("Link", display_text="Open")
            }
            
            cols_to_show = ['id', 'Name', 'Link', 'Strategy', 'Urgency Score', 'Action', 'Tank/Recov', 'P&L', 'Debit', 'Days Held', 'lot_size', 'Notes', 'Tags', 'Parent ID']
            edited_df = st.data_editor(edit_view[cols_to_show], column_config=column_config, hide_index=True, use_container_width=True, key="journal_edit", num_rows="fixed")
            
            if st.button("💾 Save Journal Changes"):
                changes = update_journal(edited_df)
                if changes: 
                    st.success(f"Saved {changes} trades!")
                    st.cache_data.clear(); auto_sync_if_connected(); time.sleep(1); st.rerun()
        else:
            st.info("No active trades to edit.")

    with op_tabs[1]:
        st.subheader("Trade DNA Fingerprinting")
        st.caption("Find historical trades that match the Greek profile of a current active trade.")
        if not active_df.empty and not expired_df.empty:
            selected_trade = st.selectbox("Select Active Trade", active_df['Name'].unique())
            curr_row = active_df[active_df['Name'] == selected_trade].iloc[0]
            similar = find_similar_trades(curr_row, expired_df)
            if not similar.empty:
                best = similar.iloc[0]
                st.info(f"**Best Match:** {best['Name']} ({best['Similarity %']:.0f}% match) -> Result: ${best['P&L']:,.0f} in {best['Days Held']:.0f} days")
                st.dataframe(similar, use_container_width=True)
            else: st.warning("No matches found.")
        else: st.info("Need both active and historical data.")

    with op_tabs[2]:
        st.subheader("Roll Campaigns")
        rolled_trades = df[df['Parent ID'] != ""].copy()
        if not rolled_trades.empty:
            summary = []
            for parent in rolled_trades['Parent ID'].unique():
                if not parent: continue
                camp = df[(df['id'] == parent) | (df['Parent ID'] == parent)]
                if camp.empty: continue
                summary.append({'Campaign': parent[:15], 'Total P&L': camp['P&L'].sum(), 'Legs': len(camp)})
            st.dataframe(pd.DataFrame(summary).style.format({'Total P&L': '${:,.0f}'}), use_container_width=True)
        else: st.info("No linked roll campaigns found (Use 'Parent ID' column in Journal).")

# -----------------------------------------------------------------------------
# TAB 3: INTELLIGENCE (Analytics & AI)
# -----------------------------------------------------------------------------
with tab_intel:
    intel_tabs = st.tabs(["📊 Performance", "🔮 AI Insights", "🔄 Lifecycle & Trends"])
    
    with intel_tabs[0]:
        if not expired_df.empty:
            st.markdown("### Closed Trade Performance")
            # Aggregation Logic
            expired_df['Cap_Days'] = expired_df['Debit'] * expired_df['Days Held'].clip(lower=1)
            perf = expired_df.groupby('Strategy').agg({'P&L': 'sum', 'Debit': 'sum', 'Cap_Days': 'sum', 'id': 'count'}).reset_index()
            wins = expired_df[expired_df['P&L'] > 0].groupby('Strategy')['id'].count().reset_index(name='Wins')
            perf = perf.merge(wins, on='Strategy', how='left').fillna(0)
            perf['Win Rate'] = perf['Wins'] / perf['id']
            perf['Ann. TWR %'] = (perf['P&L'] / perf['Cap_Days']) * 365 * 100
            
            # Display Table
            st.dataframe(perf[['Strategy', 'id', 'Win Rate', 'P&L', 'Ann. TWR %']].style.format({'Win Rate': '{:.1%}', 'P&L': '${:,.0f}', 'Ann. TWR %': '{:.1f}%'}), use_container_width=True)
            
            # Charts
            c1, c2 = st.columns(2)
            c1.plotly_chart(px.bar(perf, x='Strategy', y='P&L', title="Total P&L by Strategy", color='P&L', color_continuous_scale='RdYlGn'), use_container_width=True)
            
            # Drawdown
            mdd = calculate_max_drawdown(expired_df, total_cap)
            c2.metric("Max Portfolio Drawdown", f"{mdd['Max Drawdown %']:.2f}%", delta=f"Current: {mdd['Current DD %']:.2f}%")
        else: st.info("No closed trade history available.")

    with intel_tabs[1]:
        st.subheader("The Quant Brain")
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("**Win Probabilities (KNN Model)**")
            if not active_df.empty and not expired_df.empty:
                preds = generate_trade_predictions(active_df, expired_df, 40, 75, total_cap)
                if not preds.empty:
                    st.dataframe(preds[['Trade Name', 'Win Prob %', 'AI Rec']].style.map(lambda x: 'color: green' if x=='PRESS WINNER' else ('color: red' if x=='REDUCE/CLOSE' else ''), subset=['AI Rec']), use_container_width=True)
                else: st.caption("Not enough data for predictions.")
        
        with c2:
            st.markdown("**Capital Rot Detector**")
            # Quick rot check
            rot_list = []
            for idx, row in active_df.iterrows():
                spd, eff, stat = check_rot_and_efficiency(row, 30)
                if "Rotting" in stat or "Bleeding" in stat:
                    rot_list.append({'Trade': row['Name'], 'Status': stat, 'Efficiency': f"{eff:.2f}"})
            if rot_list:
                st.dataframe(pd.DataFrame(rot_list), use_container_width=True)
            else: st.success("Capital is moving efficiently.")

    with intel_tabs[2]:
        st.subheader("Market Physics")
        if not expired_df.empty:
            exp_clean = expired_df.dropna(subset=['Exit Date', 'Entry Date'])
            if not exp_clean.empty:
                exp_clean['Month'] = exp_clean['Exit Date'].dt.month_name()
                fig_season = px.density_heatmap(exp_clean, x='Month', y='Strategy', z='P&L', histfunc='sum', title="Seasonality Heatmap", color_continuous_scale='RdBu')
                st.plotly_chart(fig_season, use_container_width=True)
                
                exp_clean['Day'] = exp_clean['Entry Date'].dt.day_name()
                days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                fig_day = px.bar(exp_clean.groupby('Day')['P&L'].mean().reindex(days).reset_index(), x='Day', y='P&L', title="Avg P&L by Entry Day")
                st.plotly_chart(fig_day, use_container_width=True)

# -----------------------------------------------------------------------------
# TAB 4: STRATEGY & RULES (Configuration)
# -----------------------------------------------------------------------------
with tab_config:
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.subheader("Strategy Configuration")
        st.caption("Define targets and behavior for your strategies.")
        conn = get_db_connection()
        try:
            strat_df = pd.read_sql("SELECT * FROM strategy_config", conn)
            edited_strats = st.data_editor(strat_df, num_rows="dynamic", key="strat_cfg", use_container_width=True)
            if st.button("Save Config Changes"):
                update_strategy_config(edited_strats)
                st.success("Configuration Updated!")
                st.cache_data.clear(); st.rerun()
        except: st.error("Could not load config.")
        finally: conn.close()
        
        if st.button("Reprocess 'Other' Trades"):
            n = reprocess_other_trades()
            st.success(f"Reprocessed {n} trades.")

    with c2:
        st.subheader("Adaptive Rulebook")
        st.markdown(generate_adaptive_rulebook_text(expired_df, sorted(list(dynamic_benchmarks.keys()))))
        
        st.divider()
        st.markdown("**Risk Limits**")
        for strat, stats in velocity_stats.items():
            st.caption(f"**{strat}**: Max Velocity ${stats['threshold']:.0f}/day")

st.caption("v148.0 Refactor | Allantis Trade Guardian")
