import streamlit as st

# --- GLOBAL STATE INIT (Critical Fix) ---
# Initializes variables globally to prevent NameError in Tabs
if 'velocity_stats' not in globals(): velocity_stats = {}
if 'mae_stats' not in globals(): mae_stats = {}
# ----------------------------------------

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
st.set_page_config(page_title="Allantis Trade Guardian (Cloud)", layout="wide", page_icon="🛡️")

# --- DEBUG BANNER ---
st.info("🚀 RUNNING VERSION: v147.1 (Enhanced Visuals + Capital Efficiency Logic)")

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
        self.cached_file_id = None # Cache to reduce API calls
        
        # Check for secrets in Streamlit Cloud
        if 'gcp_service_account' in st.secrets:
            try:
                # Load credentials from Streamlit secrets
                service_account_info = st.secrets["gcp_service_account"]
                self.creds = service_account.Credentials.from_service_account_info(
                    service_account_info, scopes=SCOPES)
                self.service = build('drive', 'v3', credentials=self.creds)
                self.is_connected = True
            except Exception as e:
                st.error(f"Cloud Config Error: {e}")

    def list_files_debug(self):
        """Helper to show user what files the bot sees"""
        if not self.is_connected: return []
        try:
            results = self.service.files().list(
                pageSize=10, fields="files(id, name, modifiedTime)").execute()
            return results.get('files', [])
        except Exception as e:
            return []

    def find_db_file(self):
        if not self.is_connected: return None, None
        
        # Try cache first
        if self.cached_file_id:
            try:
                file = self.service.files().get(
                    fileId=self.cached_file_id, 
                    fields='id,name'
                ).execute()
                return file['id'], file['name']
            except:
                self.cached_file_id = None  # Invalidate cache if stale

        try:
            # 1. Try Exact Match First
            query_exact = f"name='{DB_NAME}' and trashed=false"
            results = self.service.files().list(
                q=query_exact, pageSize=1, fields="files(id, name)").execute()
            items = results.get('files', [])
            if items: 
                self.cached_file_id = items[0]['id']
                return items[0]['id'], items[0]['name']

            # 2. Try Fuzzy Match (e.g. "trade_guardian_v4 (9).db")
            query_fuzzy = "name contains 'trade_guardian' and name contains '.db' and trashed=false"
            results = self.service.files().list(
                q=query_fuzzy, pageSize=5, fields="files(id, name)").execute()
            items = results.get('files', [])
            
            if items:
                # Prefer one starting with correct prefix
                selected = items[0]
                for item in items:
                    if item['name'].startswith("trade_guardian_v4"):
                        selected = item
                        break
                self.cached_file_id = selected['id']
                return selected['id'], selected['name']
            
            return None, None
        except Exception as e:
            st.error(f"Drive Search Error: {e}")
            return None, None

    def get_cloud_modified_time(self, file_id):
        try:
            file = self.service.files().get(fileId=file_id, fields='modifiedTime').execute()
            # Parse RFC 3339 timestamp (e.g., 2023-10-25T14:00:00.000Z)
            dt = datetime.strptime(file['modifiedTime'].replace('Z', '+0000'), '%Y-%m-%dT%H:%M:%S.%f%z')
            return dt
        except:
            return None

    def create_backup(self, file_id, file_name):
        """Creates a timestamped copy in the cloud before overwriting"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"BACKUP_{timestamp}_{file_name}"
            
            # Get parent folder of original file to put backup in same place
            orig_file = self.service.files().get(fileId=file_id, fields='parents').execute()
            parents = orig_file.get('parents', [])
            
            metadata = {'name': backup_name}
            if parents: metadata['parents'] = parents
            
            self.service.files().copy(fileId=file_id, body=metadata).execute()
            
            # Cleanup old backups (Keep last 5)
            self.cleanup_backups(file_name)
            return True
        except Exception as e:
            print(f"Backup failed: {e}")
            return False

    def cleanup_backups(self, original_name):
        try:
            # Find files starting with BACKUP_ and ending with original name
            query = f"name contains 'BACKUP_' and name contains '{original_name}' and trashed=false"
            results = self.service.files().list(
                q=query, pageSize=20, fields="files(id, name, createdTime)", orderBy="createdTime desc").execute()
            items = results.get('files', [])
            
            # Keep top 5, delete rest
            if len(items) > 5:
                for item in items[5:]:
                    try:
                        self.service.files().delete(fileId=item['id']).execute()
                    except: pass
        except: pass

    def download_db(self, force=False):
        file_id, file_name = self.find_db_file()
        if not file_id:
            return False, "Database not found in Cloud."
        
        # --- PULL SAFETY CHECK ---
        if os.path.exists(DB_NAME) and not force:
            try:
                local_ts = os.path.getmtime(DB_NAME)
                local_mod = datetime.fromtimestamp(local_ts, tz=timezone.utc)
                cloud_time = self.get_cloud_modified_time(file_id)
                
                # If local is significantly newer (> 2 mins) than cloud
                if cloud_time and (local_mod > cloud_time + timedelta(minutes=2)):
                    return False, f"CONFLICT: Your local database is NEWER ({local_mod.strftime('%H:%M')}) than the cloud file ({cloud_time.strftime('%H:%M')}).\n\nIf you pull now, you will lose your recent local changes."
            except Exception as e:
                print(f"Pull check warning: {e}")

        try:
            # Close any local connections first
            try:
                sqlite3.connect(DB_NAME).close()
            except: pass

            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            with open(DB_NAME, "wb") as f:
                f.write(fh.getbuffer())
            
            # Update session state timestamp
            st.session_state['last_cloud_sync'] = datetime.now()
            return True, f"Downloaded '{file_name}' successfully."
        except Exception as e:
            return False, str(e)

    def upload_db(self, force=False, retries=2):
        if not os.path.exists(DB_NAME):
            return False, "No local database found to upload."
            
        file_id, file_name = self.find_db_file()
        
        # --- ROBUST CONFLICT RESOLUTION ---
        if file_id and not force:
            cloud_time = self.get_cloud_modified_time(file_id)
            
            # Get local time in UTC for fair comparison
            local_ts = os.path.getmtime(DB_NAME)
            local_time = datetime.fromtimestamp(local_ts, tz=timezone.utc) 
            
            # If cloud is newer (allowing for slight clock skew of 2 seconds)
            if cloud_time and (cloud_time > local_time + timedelta(seconds=2)):
                 return False, f"CONFLICT: Cloud file is newer ({cloud_time.strftime('%H:%M')}) than local ({local_time.strftime('%H:%M')}). Please Pull first."

        # --- SAFETY LOCK ---
        # Verify integrity and ensure closed
        try:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            conn.close()
            if result[0] != "ok":
                return False, "❌ Local Database Corrupt. Do not upload."
        except Exception as e:
            return False, f"❌ DB Check Failed: {e}"

        media = MediaFileUpload(DB_NAME, mimetype='application/x-sqlite3', resumable=True)
        
        # --- RETRY LOGIC ---
        for attempt in range(retries + 1):
            try:
                if file_id:
                    # 1. Create Backup first (only on first attempt)
                    if attempt == 0: 
                        self.create_backup(file_id, file_name)
                    
                    # 2. Update existing file
                    self.service.files().update(
                        fileId=file_id,
                        media_body=media).execute()
                    action = f"Updated '{file_name}' (Backup created)"
                else:
                    # Create new file
                    file_metadata = {'name': DB_NAME}
                    self.service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id').execute()
                    action = "Created New File"
                
                st.session_state['last_cloud_sync'] = datetime.now()
                return True, f"Sync Successful: {action}"
            except Exception as e:
                if attempt < retries:
                    time.sleep(1) # Wait 1 sec before retry
                    continue
                return False, f"Upload failed after {retries} retries: {str(e)}"

# Initialize Drive Manager
drive_mgr = DriveManager()

# --- HELPER: AUTO-SYNC WRAPPER ---
def auto_sync_if_connected():
    if not drive_mgr.is_connected: return
    with st.spinner("☁️ Auto-syncing to cloud..."):
        success, msg = drive_mgr.upload_db()
        if success:
            st.toast(f"✅ Cloud Saved: {datetime.now().strftime('%H:%M')}")
        elif "CONFLICT" in msg:
            st.error(f"⚠️ Auto-sync BLOCKED: Conflict detected. Please resolve in sidebar.")
        else:
            st.warning(f"⚠️ Auto-sync failed: {msg}")

# --- DATABASE ENGINE ---
def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    if not os.path.exists(DB_NAME) and drive_mgr.is_connected:
        success, msg = drive_mgr.download_db()
        if success:
            st.toast(f"☁️ Cloud Data Loaded: {msg}")
    
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

def theta_decay_model(initial_theta, days_held, strategy, dte_at_entry=45):
    t_frac = min(1.0, days_held / dte_at_entry) if dte_at_entry > 0 else 1.0
    if strategy in ['M200', '130/160', '160/190', 'SMSF']:
        if t_frac < 0.5:
            decay_factor = 1 - (2 * t_frac) ** 2
        else:
            decay_factor = 2 * (1 - t_frac)
        return initial_theta * max(0, decay_factor)
    elif 'VERTICAL' in str(strategy).upper() or 'DIRECTIONAL' in str(strategy).upper():
        if t_frac < 0.7:
            decay_factor = 1 - t_frac
        else:
            decay_factor = 0.3 * np.exp(-5 * (t_frac - 0.7))
        return initial_theta * decay_factor
    else:
        decay_factor = np.exp(-2 * t_frac)
        return initial_theta * (1 - decay_factor)

def get_trade_lifecycle_data(row, snapshots_df):
    """
    Generates a daily PnL curve for a trade.
    Prioritizes real snapshots. Falls back to reconstruction if snaps missing.
    Returns DataFrame: [Day, Cumulative_PnL, Pct_Duration, Pct_PnL]
    """
    days = int(row['Days Held'])
    if days < 1: days = 1
    total_pnl = row['P&L']
    
    # 1. Try Real Snapshots
    if snapshots_df is not None and not snapshots_df.empty:
        trade_snaps = snapshots_df[snapshots_df['trade_id'] == row['id']].sort_values('days_held').copy()
        if len(trade_snaps) >= 2:
            # Clean data
            trade_snaps['Cumulative_PnL'] = trade_snaps['pnl']
            trade_snaps['Day'] = trade_snaps['days_held']
            
            # Ensure start at 0
            if trade_snaps['Day'].min() > 0:
                start_row = pd.DataFrame({'Day': [0], 'Cumulative_PnL': [0]})
                trade_snaps = pd.concat([start_row, trade_snaps], ignore_index=True)
            
            # Ensure end matches final result if closed
            if row['Status'] == 'Expired':
                last_snap_day = trade_snaps['Day'].max()
                if last_snap_day < days:
                    end_row = pd.DataFrame({'Day': [days], 'Cumulative_PnL': [total_pnl]})
                    trade_snaps = pd.concat([trade_snaps, end_row], ignore_index=True)
            
            trade_snaps['Pct_Duration'] = (trade_snaps['Day'] / days) * 100
            # Avoid div by zero for PnL
            denom = abs(total_pnl) if abs(total_pnl) > 0 else 1
            trade_snaps['Pct_PnL'] = (trade_snaps['Cumulative_PnL'] / denom) * 100
            return trade_snaps[['Day', 'Cumulative_PnL', 'Pct_Duration', 'Pct_PnL']]

    # 2. Reconstruction (Theoretical Model)
    # Generate daily points
    daily_data = []
    initial_theta = row['Theta'] if row['Theta'] != 0 else 1.0
    
    # Calculate weights for distribution
    weights = []
    for d in range(1, days + 1):
        w = theta_decay_model(initial_theta, d, row['Strategy'], max(45, days))
        weights.append(abs(w))
    
    total_w = sum(weights)
    if total_w == 0: weights = [1/days] * days
    else: weights = [w/total_w for w in weights]
    
    cum_pnl = 0
    daily_data.append({'Day': 0, 'Cumulative_PnL': 0, 'Pct_Duration': 0, 'Pct_PnL': 0})
    
    for i, w in enumerate(weights):
        day_num = i + 1
        day_gain = total_pnl * w
        cum_pnl += day_gain
        
        pct_dur = (day_num / days) * 100
        denom = abs(total_pnl) if abs(total_pnl) > 0 else 1
        pct_pnl = (cum_pnl / denom) * 100
        
        daily_data.append({
            'Day': day_num,
            'Cumulative_PnL': cum_pnl,
            'Pct_Duration': pct_dur,
            'Pct_PnL': pct_pnl
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
        if days <= 0: days = 1
        
        total_pnl = trade['P&L']
        strategy = trade['Strategy']
        initial_theta = trade['Theta'] if trade['Theta'] != 0 else 1.0
        
        daily_theta_weights = []
        for day in range(days):
            expected_theta = theta_decay_model(
                initial_theta, day, strategy, max(45, days)
            )
            daily_theta_weights.append(abs(expected_theta))

        total_theta_sum = sum(daily_theta_weights)
        if total_theta_sum == 0:
            daily_theta_weights = [1/days] * days
        else:
            daily_theta_weights = [w/total_theta_sum for w in daily_theta_weights]
            
        curr = trade['Entry Date']
        for day_weight in daily_theta_weights:
            if curr.date() in daily_pnl_dict:
                daily_pnl_dict[curr.date()] += total_pnl * day_weight
            else:
                daily_pnl_dict[curr.date()] = total_pnl * day_weight
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
                log.append(f" {file.name}: Skipped (No valid trades found)")
                continue

            for t in trades_data:
                trade_id = t['id']
                if file_type == "Active":
                    file_found_ids.add(trade_id)
                
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
    empty_schema = pd.DataFrame(columns=[
        'id', 'Name', 'Strategy', 'Status', 'P&L', 'Debit', 
        'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 
        'Entry Date', 'Exit Date', 'Notes', 'Tags', 
        'Parent ID', 'Put P&L', 'Call P&L', 'IV', 'Link',
        'lot_size', 'Debit/Lot', 'ROI', 'Daily Yield %', 
        'Ann. ROI', 'Theta Pot.', 'Theta Eff.', 
        'Theta/Cap %', 'Ticker', 'Stability', 'Grade', 'Reason', 'P&L Vol'
    ])
    
    if not os.path.exists(DB_NAME): return empty_schema
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        if df.empty: return empty_schema
        
        snaps = pd.read_sql("SELECT trade_id, pnl FROM snapshots", conn)
        if not snaps.empty:
            vol_df = snaps.groupby('trade_id')['pnl'].std().reset_index()
            vol_df.rename(columns={'pnl': 'P&L Vol'}, inplace=True)
            df = df.merge(vol_df, left_on='id', right_on='trade_id', how='left')
            df['P&L Vol'] = df['P&L Vol'].fillna(0)
        else: df['P&L Vol'] = 0.0
    except Exception as e: return empty_schema
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
        
        df['Debit/Lot'] = np.where(df['lot_size'] > 0, df['Debit'] / df['lot_size'], df['Debit'])

        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = np.where(df['Debit'] > 0, (df['Theta'] / df['Debit']) * 100, 0)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
        # Clean up Parent ID to ensure strings
        df['Parent ID'] = df['Parent ID'].astype(str).str.strip().replace('nan', '').replace('None', '')
        
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
def calculate_kelly_fraction(win_rate, avg_win, avg_loss):
    if avg_loss == 0 or avg_win <= 0:
        return 0.0
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
        rec_dollars = kelly_size * total_capital
        avg_dist = distances[top_k_idx].mean()
        confidence = max(0, 100 - (avg_dist * 10)) 
        rec = "HOLD"
        if win_prob * 100 < prob_low: rec = "REDUCE/CLOSE"
        elif win_prob * 100 > prob_high: rec = "PRESS WINNER"
        predictions.append({
            'Trade Name': row['Name'], 'Strategy': row['Strategy'], 'Win Prob %': win_prob * 100,
            'Expected PnL': avg_pnl, 'Kelly Size %': kelly_size * 100, 'Rec. Size ($)': rec_dollars,
            'AI Rec': rec, 'Confidence': confidence
        })
    return pd.DataFrame(predictions)


def get_mae_stats(conn):
    """
    FEATURE A: SMART STOP (MAE)
    Calculates the Maximum Adverse Excursion (Lowest PnL) for historical WINNING trades.
    Returns the 5th percentile (95% of winners never went below this).
    """
    query = """
    SELECT t.strategy, MIN(s.pnl) as worst_drawdown
    FROM snapshots s
    JOIN trades t ON s.trade_id = t.trade_id
    WHERE t.status = 'CLOSED' AND t.pnl > 0
    GROUP BY t.trade_id, t.strategy
    """
    try:
        df = pd.read_sql(query, conn)
        mae_stats = {}
        if not df.empty:
            for strategy in df['strategy'].unique():
                strat_df = df[df['strategy'] == strategy]
                if not strat_df.empty:
                    limit = strat_df['worst_drawdown'].quantile(0.05) 
                    mae_stats[strategy] = limit
        return mae_stats
    except Exception as e:
        return {}

def get_velocity_stats(expired_df):
    """
    FEATURE C: PROFIT VELOCITY
    Calculates Mean and StdDev of $/Day velocity for winning trades.
    """
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
            velocity_stats[strategy] = {
                'threshold': mean_v + (2 * std_v),
                'mean': mean_v
            }
    return velocity_stats

def check_rot_and_efficiency(row, hist_avg_days):
    """
    UPDATED: Checks for Theta Decay efficiency, Time Rot, AND Velocity.
    """
    try:
        current_pnl = row['P&L']
        days = row.get('Days', 1)
        if days == 0: days = 1
        
        theta = row.get('Net Theta', 0)
        current_speed = current_pnl / days
        
        if theta != 0:
            theta_efficiency = (current_speed / theta) 
        else:
            theta_efficiency = 0
            
        status = "Healthy"
        if days > hist_avg_days * 1.2 and current_pnl < 0:
            status = "Rotting (Time > Avg & Red)"
        elif theta_efficiency < 0.2 and days > 10 and current_pnl > 0:
             status = "Inefficient (Theta Stuck)"
        elif theta_efficiency < 0 and days > 5:
             status = "Bleeding (Negative Efficiency)"
             
        return current_speed, theta_efficiency, status
    except:
        return 0, 0, "Error"

def get_dynamic_targets(history_df, percentile):
    if history_df.empty: return {}
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
    if historical_df.empty: return pd.DataFrame()
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
    if trades_df.empty or capital <= 0: return 0.0, 0.0
    daily_pnl_dict = reconstruct_daily_pnl(trades_df)
    trades_df['Entry Date'] = pd.to_datetime(trades_df['Entry Date'])
    trades_df['Exit Date'] = pd.to_datetime(trades_df['Exit Date'])
    start_date = trades_df['Entry Date'].min()
    end_date = max(trades_df['Exit Date'].max(), pd.Timestamp.now())
    date_range = pd.date_range(start=start_date, end=end_date)
    equity = capital
    daily_equity_values = []
    for d in date_range:
        day_pnl = daily_pnl_dict.get(d.date(), 0)
        equity += day_pnl
        daily_equity_values.append(equity)
    equity_series = pd.Series(daily_equity_values)
    daily_returns = equity_series.pct_change().dropna()
    if daily_returns.std() == 0:
        sharpe = 0.0
    else:
        sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
    total_days = (end_date - start_date).days
    if total_days < 1: total_days = 1
    total_pnl = trades_df['P&L'].sum()
    end_val = capital + total_pnl
    try:
        cagr = ( (end_val / capital) ** (365 / total_days) ) - 1
    except:
        cagr = 0.0
    return sharpe, cagr * 100

def check_concentration_risk(active_df, total_equity, threshold=0.15):
    if active_df.empty or total_equity <= 0: return pd.DataFrame()
    warnings = []
    for _, row in active_df.iterrows():
        concentration = row['Debit'] / total_equity
        if concentration > threshold:
            warnings.append({
                'Trade': row['Name'], 'Strategy': row['Strategy'], 'Size %': f"{concentration:.1%}",
                'Risk': f"${row['Debit']:,.0f}", 'Limit': f"{threshold:.0%}"
            })
    return pd.DataFrame(warnings)

def calculate_max_drawdown(trades_df, initial_capital):
    if trades_df.empty or initial_capital <= 0: 
        return {'Max Drawdown %': 0.0, 'Current DD %': 0.0}
    daily_pnl_dict = reconstruct_daily_pnl(trades_df)
    trades_df['Entry Date'] = pd.to_datetime(trades_df['Entry Date'])
    trades_df['Exit Date'] = pd.to_datetime(trades_df['Exit Date'])
    start_date = trades_df['Entry Date'].min()
    end_date = max(trades_df['Exit Date'].max(), pd.Timestamp.now())
    date_range = pd.date_range(start=start_date, end=end_date)
    equity = initial_capital
    equity_curve = []
    dates = []
    for d in date_range:
        day_pnl = daily_pnl_dict.get(d.date(), 0)
        equity += day_pnl
        equity_curve.append(equity)
        dates.append(d.date())
    equity_series = pd.Series(equity_curve, index=pd.to_datetime(dates))
    running_max = equity_series.cummax()
    drawdown = (equity_series - running_max) / running_max
    max_dd = drawdown.min()
    current_dd = drawdown.iloc[-1]
    return {'Max Drawdown %': max_dd * 100, 'Current DD %': current_dd * 100}

def rolling_correlation_matrix(snaps, window_days=30):
    if snaps.empty: return None
    strat_daily = snaps.pivot_table(index='snapshot_date', columns='strategy', values='pnl', aggfunc='sum')
    if len(strat_daily) < window_days: return None
    last_30 = strat_daily.tail(30)
    corr_30 = last_30.corr()
    fig = px.imshow(corr_30, text_auto=".2f", aspect="auto", color_continuous_scale="RdBu", 
                    title="Strategy Correlation (Last 30 Days)", labels=dict(color="Correlation"))
    return fig

def generate_adaptive_rulebook_text(history_df, strategies):
    text = "#  The Adaptive Trader's Constitution\n*Rules evolve. This book rewrites itself based on your actual data.*\n\n"
    if history_df.empty:
        text += " *Not enough data yet. Complete more trades to unlock adaptive rules.*"
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
            text += f"* ** Best Entry Day:** {best_day} (Highest Avg Win)\n"
            avg_hold = winners['Days Held'].mean()
            text += f"* ** Optimal Hold:** {avg_hold:.0f} Days (Avg Winner Duration)\n"
            avg_cost = winners['Debit/Lot'].mean()
            text += f"* ** Target Cost:** ${avg_cost:,.0f} (Avg Winner Debit per Lot)\n"
        losers = strat_df[strat_df['P&L'] < 0]
        if not losers.empty:
             avg_loss_hold = losers['Days Held'].mean()
             text += f"* ** Loss Pattern:** Losers held for avg {avg_loss_hold:.0f} days.\n"
        text += "\n"
    text += "---\n###  Universal AI Gates\n"
    text += "1. **Efficiency Check:** If 'Rot Detector' flags a trade, cut it. Your capital is stuck.\n"
    text += "2. **Probability Gate:** Check 'Win Prob %' before entering. If < 40%, skip even if the chart looks good.\n"
    return text

# --- INITIALIZE DB ---
init_db()

# --- SIDEBAR ---
st.sidebar.markdown("###  Daily Workflow")

# --- CLOUD SYNC SECTION (NEW) ---
if GOOGLE_DEPS_INSTALLED:
    with st.sidebar.expander("☁️ Cloud Sync (Google Drive)", expanded=True):
        if not drive_mgr.is_connected:
            st.error("No secrets found. Add 'gcp_service_account' to .streamlit/secrets.toml")
        else:
            c1, c2 = st.columns(2)
            with c1:
                # Main Sync Button
                if st.button("⬆️ Sync to Cloud"):
                    success, msg = drive_mgr.upload_db()
                    if not success and "CONFLICT" in msg:
                        st.error(msg)
                        st.session_state['show_conflict'] = True
                    elif success: 
                        st.success(msg)
                        st.session_state['show_conflict'] = False
                    else: st.error(msg)
            
            with c2:
                if st.button("⬇️ Pull from Cloud"):
                    success, msg = drive_mgr.download_db()
                    if not success and "CONFLICT" in msg:
                        st.error(msg)
                        st.session_state['show_pull_conflict'] = True
                    elif success: 
                        st.cache_data.clear()
                        st.success(msg)
                        st.rerun()
                    else: st.error(msg)
            
            # Conflict Resolution UI (Upload)
            if st.session_state.get('show_conflict'):
                st.warning("Upload Conflict:")
                cc1, cc2 = st.columns(2)
                with cc1:
                    if st.button("⬇️ Pull (Safe)", key="res_pull"):
                        success, msg = drive_mgr.download_db()
                        if success:
                            st.success("Resolved: Cloud version pulled.")
                            st.session_state['show_conflict'] = False
                            st.cache_data.clear()
                            st.rerun()
                with cc2:
                    if st.button("⚠️ Force Push", key="res_force"):
                        success, msg = drive_mgr.upload_db(force=True)
                        if success:
                            st.success("Resolved: Cloud overwritten.")
                            st.session_state['show_conflict'] = False

            # Conflict Resolution UI (Pull)
            if st.session_state.get('show_pull_conflict'):
                st.warning("Pull Warning: Local file is newer!")
                cp1, cp2 = st.columns(2)
                with cp1:
                    if st.button("⬇️ Force Pull (Lose Local)", key="force_pull"):
                        success, msg = drive_mgr.download_db(force=True)
                        if success:
                            st.session_state['show_pull_conflict'] = False
                            st.cache_data.clear()
                            st.rerun()
                with cp2:
                    if st.button("❌ Cancel", key="cancel_pull"):
                        st.session_state['show_pull_conflict'] = False
                        st.rerun()
            
            # Persistent Status Indicator
            st.sidebar.divider()
            last_sync_time = st.session_state.get('last_cloud_sync')
            
            # Get real cloud status
            cloud_file_id, cloud_file_name = drive_mgr.find_db_file()
            if cloud_file_id:
                cloud_ts = drive_mgr.get_cloud_modified_time(cloud_file_id)
                if cloud_ts:
                    # Convert to local/browser time approximation or just show UTC
                    st.sidebar.caption(f"☁️ Cloud File: {cloud_ts.strftime('%b %d, %H:%M')} (UTC)")
            
            if last_sync_time:
                mins_ago = (datetime.now() - last_sync_time).total_seconds() / 60
                if mins_ago < 1:
                    st.sidebar.success(f"✅ Synced just now")
                elif mins_ago < 60:
                    st.sidebar.info(f"Last Sync: {int(mins_ago)}m ago")
                else:
                    st.sidebar.warning(f"Last Sync: {last_sync_time.strftime('%H:%M')}")
            else:
                st.sidebar.warning("⚠️ Session Status: Unsaved")

            with st.expander("🕵️ Debug: What can I see?"):
                st.write("Files visible to Bot:")
                files = drive_mgr.list_files_debug()
                if files:
                    for f in files:
                        mod_time = f['modifiedTime'] if 'modifiedTime' in f else 'Unknown'
                        st.code(f"{f['name']}\nID: {f['id']}\nMod: {mod_time}")
                else:
                    st.warning("No files found. Ensure you shared the DB with the service account email.")

with st.sidebar.expander("1.  STARTUP (Restore Local)", expanded=False):
    restore = st.file_uploader("Upload .db file", type=['db'], key='restore')
    if restore:
        with open(DB_NAME, "wb") as f: f.write(restore.getbuffer())
        st.cache_data.clear()
        st.success("Restored.")
        if 'restored' not in st.session_state:
            st.session_state['restored'] = True
            st.rerun()

st.sidebar.markdown(" *then...*")
with st.sidebar.expander("2.  WORK (Sync Files)", expanded=True):
    active_up = st.file_uploader("Active Trades", accept_multiple_files=True, key="act")
    history_up = st.file_uploader("History (Closed)", accept_multiple_files=True, key="hist")
    if st.button(" Process & Reconcile"):
        logs = []
        if active_up: logs.extend(sync_data(active_up, "Active"))
        if history_up: logs.extend(sync_data(history_up, "History"))
        if logs:
            for l in logs: st.write(l)
            st.cache_data.clear()
            st.success("Sync Complete!")
            auto_sync_if_connected()

st.sidebar.markdown(" *finally...*")
with st.sidebar.expander("3.  SHUTDOWN (Local Backup)", expanded=False):
    with open(DB_NAME, "rb") as f:
        st.download_button(" Save Database File", f, "trade_guardian_v4.db", "application/x-sqlite3")

with st.sidebar.expander(" Maintenance", expanded=False):
    st.caption("Fix Duplicates / Rename Issues")
    if st.button(" Vacuum DB"):
        conn = get_db_connection()
        conn.execute("VACUUM")
        conn.close()
        st.success("Optimized.")
    st.markdown("---")
    conn = get_db_connection()
    try:
        all_trades = pd.read_sql("SELECT id, name, status, pnl, days_held FROM trades ORDER BY status, entry_date DESC", conn)
        if not all_trades.empty:
            st.write(" **Delete Specific Trades**")
            all_trades['Label'] = all_trades['name'] + " (" + all_trades['status'] + ", $" + all_trades['pnl'].astype(str) + ")"
            trades_to_del = st.multiselect("Select trades to delete:", all_trades['Label'].tolist())
            if st.button(" Delete Selected Trades"):
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
    if st.button(" Hard Reset (Delete All Data)"):
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
st.sidebar.header(" Portfolio Settings")

prime_cap = st.sidebar.number_input("Prime Account (130/160, M200)", min_value=1000, value=115000, step=1000)
smsf_cap = st.sidebar.number_input("SMSF Account", min_value=1000, value=150000, step=1000)
total_cap = prime_cap + smsf_cap

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
    
    # NEW: Capital Efficiency Check (Redeployment Logic)
    if pnl > 0 and days > 5:
        # Calculate historical baseline velocity ($/day)
        hist_vel = hist_avg_pnl / max(1, hist_avg_days)
        # Calculate current velocity ($/day)
        curr_vel = pnl / days
        
        # If current trade is dragging the average down significantly
        if curr_vel < (hist_vel * 0.6): # Earning less than 60% of average speed
             # Check if it's because it's super old (already handled by stale check) 
             # or just slow performance.
             # Only trigger if we have decent profit to take (e.g. > 20% of target)
             if pnl > (target_profit * 0.2):
                 score += 15
                 if action == "HOLD": # Only override if currently neutral
                     action = "REDEPLOY?"
                     reason = f"Slow Capital (Vel ${curr_vel:.0f}/d vs Avg ${hist_vel:.0f}/d)"

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
if not df.empty and 'Status' in df.columns:
    expired_df = df[df['Status'] == 'Expired']
    
    # --- v147.0 STATS CALC ---
    try:
        if 'expired_df' in locals() and not expired_df.empty:
            velocity_stats = get_velocity_stats(expired_df)
        if 'conn' in locals():
            mae_stats = get_mae_stats(conn) # Note: conn needs to be open, get_db_connection() creates new
        else:
            # Create temp connection for stats
            try:
                temp_conn = get_db_connection()
                mae_stats = get_mae_stats(temp_conn)
                temp_conn.close()
            except: pass
    except Exception as e: print(f"Stats Error: {e}")
    # -------------------------

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
tab_dash, tab_active, tab_analytics, tab_ai, tab_strategies, tab_rules = st.tabs([" Dashboard", " ⚡ Active Management", " Analytics", " AI & Insights", " Strategies", " Rules"])

with tab_dash:
    with st.expander(" Universal Pre-Flight Calculator", expanded=False):
        pf_c1, pf_c2, pf_c3 = st.columns(3)
        with pf_c1:
            pf_goal = st.selectbox("Strategy Profile", [
                " Hedged Income (Butterflies, Calendars, M200)", 
                " Standard Income (Credit Spreads, Iron Condors)", 
                " Directional (Long Calls/Puts, Verticals)", 
                " Speculative Vol (Straddles, Earnings)"
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
                    if stability > 1.0: st.success(f" Stability: {stability:.2f} (Fortress)")
                    elif stability > 0.5: st.info(f" Stability: {stability:.2f} (Good)")
                    else: st.error(f" Stability: {stability:.2f} (Coin Flip)")
                with res_c2:
                    if annualized_roi > 50: st.success(f" Ann. ROI: {annualized_roi:.0f}%")
                    elif annualized_roi > 25: st.info(f" Ann. ROI: {annualized_roi:.0f}%")
                    else: st.error(f" Ann. ROI: {annualized_roi:.0f}%")
                with res_c3:
                    if pf_dte < 21: st.warning(" High Gamma Risk (Low DTE)")
                    elif pf_vega > 0: st.success(f" Hedge: {vega_cushion:.1f}x (Good)")
                    else: st.error(f" Hedge: {pf_vega:.0f} (Negative Vega)")
            elif "Standard Income" in pf_goal:
                stability = pf_theta / (abs(pf_delta) + 1)
                yield_pct = (pf_theta / abs(pf_price)) * 100
                annualized_roi = (yield_pct * 365)
                fragility = abs(pf_vega) / pf_theta if pf_theta != 0 else 999
                with res_c1:
                    if stability > 0.5: st.success(f" Stability: {stability:.2f} (Good)")
                    else: st.error(f" Stability: {stability:.2f} (Unstable)")
                with res_c2:
                    if annualized_roi > 40: st.success(f" Ann. ROI: {annualized_roi:.0f}%")
                    else: st.warning(f" Ann. ROI: {annualized_roi:.0f}%")
                with res_c3:
                    if pf_dte < 21: st.warning(" High Gamma Risk (Low DTE)")
                    elif pf_vega < 0 and fragility < 5: st.success(f" Fragility: {fragility:.1f} (Robust)")
                    else: st.warning(f" Fragility: {fragility:.1f} (High)")
            elif "Directional" in pf_goal:
                leverage = abs(pf_delta) / abs(pf_price) * 100
                theta_drag = (pf_theta / abs(pf_price)) * 100
                with res_c1: st.metric("Leverage", f"{leverage:.2f} /$100")
                with res_c2:
                    if theta_drag > -0.1: st.success(f" Burn: {theta_drag:.2f}% (Low)")
                    else: st.warning(f" Burn: {theta_drag:.2f}% (High)")
                with res_c3:
                    proj_roi = (abs(pf_delta) * 5) / abs(pf_price) * 100 
                    st.metric("ROI on $5 Move", f"{proj_roi:.1f}%")
            elif "Speculative Vol" in pf_goal:
                vega_efficiency = abs(pf_vega) / abs(pf_price) * 100
                move_needed = abs(pf_theta / pf_vega) if pf_vega != 0 else 0
                with res_c1: st.metric("Vega Exposure", f"{vega_efficiency:.1f}%")
                with res_c2: st.metric("Daily Cost", f"${pf_theta:.0f}")
                with res_c3: st.info(f"Need {move_needed:.1f}% IV move to break even")

    if not df.empty and 'Status' in df.columns:
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        if active_df.empty:
            st.info(" No active trades.")
        else:
            tot_debit = active_df['Debit'].sum()
            if tot_debit == 0: tot_debit = 1
            target_allocation = {'130/160': 0.30, '160/190': 0.40, 'M200': 0.20, 'SMSF': 0.10}
            actual_alloc = active_df.groupby('Strategy')['Debit'].sum() / tot_debit
            allocation_score = 100 - sum(abs(actual_alloc.get(s, 0) - target_allocation.get(s, 0)) * 100 for s in target_allocation)
            total_delta_pct = abs(active_df['Delta'].sum() / tot_debit * 100)
            avg_age = active_df['Days Held'].mean()
            
            if total_delta_pct > 6 or avg_age > 45:
                health_status = " CRITICAL" 
            elif allocation_score < 40:
                health_status = " CRITICAL" 
            elif allocation_score < 80 or total_delta_pct > 2 or avg_age > 25:
                health_status = " REVIEW"    
            else:
                health_status = " HEALTHY"   
            
            with st.container():
                tot_theta = active_df['Theta'].sum()
                c1, c2, c3, c4 = st.columns(4)
                h_icon = "🟢" if "HEALTHY" in health_status else ("🔴" if "CRITICAL" in health_status else "🟡")
                c1.metric("Portfolio Health", f"{h_icon} {health_status}")
                c2.metric("Daily Income", f"${tot_theta:,.0f}")
                curr_pnl = active_df['P&L'].sum()
                c3.metric("Floating P&L", f"${curr_pnl:,.0f}", delta_color="normal" if curr_pnl > 0 else "inverse")
                ladder_results = active_df.apply(lambda row: calculate_decision_ladder(row, dynamic_benchmarks), axis=1)
                active_df['Action'] = [x[0] for x in ladder_results]
                active_df['Urgency Score'] = [x[1] for x in ladder_results]
                active_df['Reason'] = [x[2] for x in ladder_results]
                active_df['Juice Val'] = [x[3] for x in ladder_results]
                active_df['Juice Type'] = [x[4] for x in ladder_results]
                active_df = active_df.sort_values('Urgency Score', ascending=False)
                todo_df = active_df[active_df['Urgency Score'] >= 70]
                c4.metric("Action Items", len(todo_df), delta="Urgent" if len(todo_df) > 0 else None)
            
            with st.expander("📊 Detailed Metrics (Allocation, Greeks, Age)", expanded=False):
                d1, d2, d3, d4 = st.columns(4)
                eff_score = (tot_theta / tot_debit * 100)
                d1.metric("Allocation Score", f"{allocation_score:.0f}/100")
                d2.metric("Yield/Cap", f"{eff_score:.2f}%")
                d3.metric("Net Delta", f"{total_delta_pct:.2f}%")
                d4.metric("Avg Age", f"{avg_age:.0f} days")
                stale_capital = active_df[active_df['Days Held'] > 40]['Debit'].sum()
                if stale_capital > tot_debit * 0.3:
                     st.warning(f" ${stale_capital:,.0f} stuck in trades >40 days old. Consider exits.")

            st.divider()
            st.subheader("🗺️ Position Heat Map")
            fig_heat = px.scatter(
                active_df, x='Days Held', y='P&L', size='Debit',
                color='Urgency Score', color_continuous_scale='RdYlGn_r',
                hover_data=['Name', 'Strategy', 'Action'],
                title="Position Clustering (Size = Capital)"
            )
            avg_days_current = active_df['Days Held'].mean()
            fig_heat.add_vline(x=avg_days_current, line_dash="dash", opacity=0.5, annotation_text="Avg Age")
            fig_heat.add_hline(y=0, line_dash="dash", opacity=0.5)
            st.plotly_chart(fig_heat, use_container_width=True)
            st.caption("🎯 Top-Right = Winners aging well | 🚨 Bottom-Right = Losers rotting | 🌱 Left = New positions cooking")

    else: st.info(" Database is empty. Sync your first file.")

with tab_active:
    if not df.empty and 'Status' in df.columns:
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        if not active_df.empty:
            ladder_results = active_df.apply(lambda row: calculate_decision_ladder(row, dynamic_benchmarks), axis=1)
            active_df['Action'] = [x[0] for x in ladder_results]
            active_df['Urgency Score'] = [x[1] for x in ladder_results]
            active_df['Reason'] = [x[2] for x in ladder_results]
            active_df['Juice Val'] = [x[3] for x in ladder_results]
            active_df['Juice Type'] = [x[4] for x in ladder_results]
            active_df = active_df.sort_values('Urgency Score', ascending=False)
            todo_df = active_df[active_df['Urgency Score'] >= 70]

            is_expanded = len(todo_df) > 0
            with st.expander(f" Priority Action Queue ({len(todo_df)})", expanded=is_expanded):
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
                else: st.success(" No critical actions required. Portfolio is healthy.")
            st.divider()

            sub_strat, sub_journal, sub_dna = st.tabs([" Strategy Detail", " Journal", " DNA Tool"])
            
            with sub_journal:
                st.caption("Trades sorted by Urgency.")
                strategy_options = sorted(list(dynamic_benchmarks.keys())) + ["Other"]
                def fmt_juice(row):
                    if row['Juice Type'] == 'Recovery Days': return f"{row['Juice Val']:.0f} days"
                    return f"${row['Juice Val']:.0f}"
                active_df['Gauge'] = active_df.apply(fmt_juice, axis=1)

                display_cols = ['id', 'Name', 'Link', 'Strategy', 'Urgency Score', 'Action', 'Gauge', 'Status', 'Stability', 'ROI', 'Ann. ROI', 'Theta Eff.', 'lot_size', 'P&L', 'Debit', 'Days Held', 'Notes', 'Tags', 'Parent ID']
                column_config = {
                    "id": None, "Name": st.column_config.TextColumn("Trade Name", disabled=True),
                    "Link": st.column_config.LinkColumn("OS Link", display_text="Open "),
                    "Strategy": st.column_config.SelectboxColumn("Strat", width="medium", options=strategy_options, required=True),
                    "Status": st.column_config.TextColumn("Status", disabled=True, width="small"),
                    "Urgency Score": st.column_config.ProgressColumn(" Urgency Ladder", min_value=0, max_value=100, format="%d"),
                    "Action": st.column_config.TextColumn("Decision", disabled=True),
                    "Gauge": st.column_config.TextColumn("Tank / Recov"),
                    "Stability": st.column_config.ProgressColumn(
                        "Stability",
                        help="Theta / (Delta + 1). Full bar (3.0) = Excellent Health.",
                        format="%.2f",
                        min_value=0,
                        max_value=3,
                    ),
                    "Theta Eff.": st.column_config.NumberColumn(" Eff", format="%.2f", disabled=True),
                    "ROI": st.column_config.NumberColumn("ROI %", format="%.1f%%", disabled=True),
                    "Ann. ROI": st.column_config.NumberColumn("Ann. ROI %", format="%.1f%%", disabled=True),
                    "P&L": st.column_config.NumberColumn("P&L", format="$%d", disabled=True),
                    "Debit": st.column_config.NumberColumn("Debit", format="$%d", disabled=True),
                    "lot_size": st.column_config.NumberColumn("Lots", min_value=1, step=1),
                    "Notes": st.column_config.TextColumn(" Notes", width="large"),
                    "Tags": st.column_config.SelectboxColumn(" Tags", options=["Rolled", "Hedged", "Earnings", "High Risk", "Watch"], width="medium"),
                    "Parent ID": st.column_config.TextColumn(" Link ID"),
                }
                edited_df = st.data_editor(active_df[display_cols], column_config=column_config, hide_index=True, use_container_width=True, key="journal_editor", num_rows="fixed")
                if st.button(" Save Journal"):
                    changes = update_journal(edited_df)
                    if changes: 
                        st.success(f"Saved {changes} trades!")
                        st.cache_data.clear()
                        auto_sync_if_connected()
                        time.sleep(1) 
                        st.rerun()
            
            with sub_dna:
                st.subheader(" Trade DNA Fingerprinting")
                st.caption("Find historical trades that match the Greek profile of your current active trade.")
                if not expired_df.empty:
                    selected_dna_trade = st.selectbox("Select Active Trade to Analyze", active_df['Name'].unique())
                    curr_row = active_df[active_df['Name'] == selected_dna_trade].iloc[0]
                    similar = find_similar_trades(curr_row, expired_df)
                    if not similar.empty:
                        best_match = similar.iloc[0]
                        st.info(f" **Best Match:** {best_match['Name']} ({best_match['Similarity %']:.0f}% similar)  Made ${best_match['P&L']:,.0f} in {best_match['Days Held']:.0f} days")
                        st.dataframe(similar.style.format({'P&L': '${:,.0f}', 'ROI': '{:.1f}%', 'Similarity %': '{:.0f}%'}))
                    else: st.info("No similar historical trades found.")
                else: st.info("Need closed trade history for DNA analysis.")

            with sub_strat:
                st.markdown("###  Strategy Performance")
                sorted_strats = sorted(list(dynamic_benchmarks.keys()))
                tabs_list = [" Overview"] + [f" {s}" for s in sorted_strats]
                if "Other" not in sorted_strats: tabs_list.append(" Other / Unclassified")
                strat_tabs_inner = st.tabs(tabs_list)

                with strat_tabs_inner[0]:
                    strat_agg = active_df.groupby('Strategy').agg({
                        'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum', 'Delta': 'sum',
                        'Name': 'count', 'Daily Yield %': 'mean', 'Ann. ROI': 'mean', 'Theta Eff.': 'mean', 'P&L Vol': 'mean', 'Stability': 'mean' 
                    }).reset_index()
                    strat_agg['Trend'] = strat_agg.apply(lambda r: " Improving" if r['Daily Yield %'] >= dynamic_benchmarks.get(r['Strategy'], {}).get('yield', 0) else " Lagging", axis=1)
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
                    display_agg.columns = ['Strategy', 'Trend', 'Yield/Day', 'Ann. ROI', ' Eff', 'Stability', 'Sleep Well (Vol)', 'Target', 'Total P&L', 'Total Debit', 'Net Theta', 'Net Delta', 'Count']
                    
                    def highlight_trend(val): 
                        val_str = str(val)
                        if 'Improving' in val_str: return 'color: green; font-weight: bold'
                        if 'Lagging' in val_str: return 'color: red; font-weight: bold'
                        return ''
                    
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
                            ' Eff': lambda x: safe_fmt(x, "{:.2f}"),
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

                            st.dataframe(
                                display_df.style.format({'Theta/Cap %': "{:.2f}%", 'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%", 'Ann. ROI': "{:.1f}%", 'Theta Eff.': "{:.2f}", 'P&L Vol': "{:.1f}", 'Stability': "{:.2f}", 'Theta': "{:.1f}", 'Delta': "{:.1f}", 'Gamma': "{:.2f}", 'Vega': "{:.0f}", 'Days Held': "{:.0f}"})
                                .map(lambda v: 'background-color: #d1e7dd; color: #0f5132; font-weight: bold' if 'TAKE PROFIT' in str(v) else ('background-color: #f8d7da; color: #842029; font-weight: bold' if 'KILL' in str(v) or 'MISSING' in str(v) else ('background-color: #fff3cd; color: #856404; font-weight: bold' if 'WATCH' in str(v) else ('background-color: #cff4fc; color: #055160; font-weight: bold' if 'COOKING' in str(v) else ''))), subset=['Action'])
                                .map(lambda v: 'color: green; font-weight: bold' if isinstance(v, (int, float)) and v > 0 else ('color: red; font-weight: bold' if isinstance(v, (int, float)) and v < 0 else ''), subset=['P&L'])
                                .map(yield_color, subset=['Daily Yield %'])
                                .apply(lambda x: ['background-color: #d1d5db; color: black; font-weight: bold' if x.name == len(display_df)-1 else '' for _ in x], axis=1), 
                                use_container_width=True, 
                                column_config={
                                    "Link": st.column_config.LinkColumn("OS Link", display_text="Open "), 
                                    "Urgency Score": st.column_config.ProgressColumn("Urgency", min_value=0, max_value=100, format="%d"), 
                                    "Gauge": st.column_config.TextColumn("Tank / Recov"),
                                    "Stability": st.column_config.ProgressColumn("Stability", format="%.2f", min_value=0, max_value=3)
                                }
                            )
                        else: st.info("No active trades.")
                if "Other" not in sorted_strats:
                    with strat_tabs_inner[-1]: 
                        subset = active_df[active_df['Strategy'] == "Other"].copy()
                        if not subset.empty: st.dataframe(subset[cols], use_container_width=True)
                        else: st.info("No unclassified trades.")
    else: st.info(" Database is empty. Sync your first file.")

with tab_strategies:
    st.markdown("###  Strategy Configuration Manager")
    conn = get_db_connection()
    try:
        strat_df = pd.read_sql("SELECT * FROM strategy_config", conn)
        expected_cols = {
            'name': 'Name',
            'identifier': 'Identifier',
            'target_pnl': 'Target PnL',
            'target_days': 'Target Days',
            'min_stability': 'Min Stability',
            'description': 'Description',
            'typical_debit': 'Typical Debit'
        }
        for db_col in expected_cols.keys():
            if db_col not in strat_df.columns:
                strat_df[db_col] = 0.0 if 'pnl' in db_col or 'debit' in db_col else (0 if 'days' in db_col else "")
        strat_df = strat_df[list(expected_cols.keys())].rename(columns=expected_cols)
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
            if st.button(" Save Changes"):
                if update_strategy_config(edited_strats): st.success("Configuration Saved!"); st.cache_data.clear(); st.rerun()
        with c2:
            if st.button(" Reprocess 'Other' Trades"):
                count = reprocess_other_trades(); st.success(f"Reprocessed {count} trades!"); st.cache_data.clear(); st.rerun()
        with c3:
            if st.button(" Reset to Defaults", type="secondary"): seed_default_strategies(force_reset=True); st.cache_data.clear(); st.rerun()
    except Exception as e: st.error(f"Error loading strategies: {e}")
    finally: conn.close()
    
    st.info(" **How to use:** \n1. **Reset to Defaults** if this table is blank. \n2. **Edit Identifiers:** Ensure '130/160' is longer than '160'. \n3. **Save Changes.** \n4. **Reprocess All Trades** to fix old grouping errors.")

with tab_analytics:
    an_overview, an_trends, an_risk, an_lifecycle, an_rolls = st.tabs([" Overview", " Trends & Seasonality", " Risk & Excursion", " Lifecycle (Timing)", " Rolls"])

    with an_overview:
        if not df.empty and 'Status' in df.columns:
            active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
            if not active_df.empty:
                st.markdown("###  Portfolio Health Check (Breakdown)")
                health_col1, health_col2, health_col3 = st.columns(3)
                tot_debit = active_df['Debit'].sum()
                if tot_debit == 0: tot_debit = 1
                target_allocation = {'130/160': 0.30, '160/190': 0.40, 'M200': 0.20, 'SMSF': 0.10}
                actual = active_df.groupby('Strategy')['Debit'].sum() / tot_debit
                allocation_score = 100 - sum(abs(actual.get(s, 0) - target_allocation.get(s, 0)) * 100 for s in target_allocation)
                health_col1.metric(" Allocation Score", f"{allocation_score:.0f}/100", delta="Optimal" if allocation_score > 80 else "Review")
                total_delta_pct = abs(active_df['Delta'].sum() / tot_debit * 100)
                greek_health = " Safe" if total_delta_pct < 2 else " Warning" if total_delta_pct < 5 else " Danger"
                health_col2.metric(" Greek Exposure", greek_health, delta=f"{total_delta_pct:.2f}% Delta/Capital", delta_color="inverse")
                avg_age = active_df['Days Held'].mean()
                age_health = " Fresh" if avg_age < 25 else " Aging" if avg_age < 35 else " Stale"
                health_col3.metric(" Portfolio Age", age_health, delta=f"{avg_age:.0f} days avg", delta_color="inverse")
                conc_warnings = check_concentration_risk(active_df, total_cap) 
                if not conc_warnings.empty:
                    st.warning(" **Position Sizing Alert:** The following trades exceed 15% concentration.")
                    st.dataframe(conc_warnings, use_container_width=True)
                st.divider()

            st.markdown("###  Performance Deep Dive")
            realized_pnl = df[df['Status']=='Expired']['P&L'].sum()
            try:
                if not expired_df.empty:
                    smsf_trades = expired_df[expired_df['Strategy'].str.contains("SMSF", case=False, na=False)].copy()
                    prime_trades = expired_df[~expired_df['Strategy'].str.contains("SMSF", case=False, na=False)].copy()
                    s_smsf, c_smsf = calculate_portfolio_metrics(smsf_trades, smsf_cap)
                    s_prime, c_prime = calculate_portfolio_metrics(prime_trades, prime_cap)
                    s_total, c_total = calculate_portfolio_metrics(expired_df, total_cap)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.markdown("** TOTAL PORTFOLIO**")
                        st.metric("Banked Profit", f"${realized_pnl:,.0f}")
                        st.metric("CAGR", f"{c_total:.1f}%")
                        st.metric("Sharpe", f"{s_total:.2f}")

                    with col2:
                        st.markdown("** PRIME (Income)**")
                        st.metric("Profit", f"${prime_trades['P&L'].sum():,.0f}")
                        st.metric("CAGR", f"{c_prime:.1f}%")
                        st.metric("Sharpe", f"{s_prime:.2f}", help="Daily Sharpe Ratio (Annualized). >1.0 is Good, >2.0 is Excellent.")

                    with col3:
                        st.markdown("** SMSF (Wealth)**")
                        st.metric("Profit", f"${smsf_trades['P&L'].sum():,.0f}")
                        st.metric("CAGR", f"{c_smsf:.1f}%")
                        st.metric("Sharpe", f"{s_smsf:.2f}")

                    st.divider()
                    st.markdown("** Risk Metrics (Max Drawdown)**")
                    mdd_total = calculate_max_drawdown(expired_df, total_cap)
                    mdd_prime = calculate_max_drawdown(prime_trades, prime_cap)
                    mdd_smsf = calculate_max_drawdown(smsf_trades, smsf_cap)
                    r1, r2, r3 = st.columns(3)
                    with r1: st.metric("Total Max DD", f"{mdd_total['Max Drawdown %']:.1f}%", help="Largest peak-to-trough decline in total equity.")
                    with r2: st.metric("Prime Max DD", f"{mdd_prime['Max Drawdown %']:.1f}%")
                    with r3: st.metric("SMSF Max DD", f"{mdd_smsf['Max Drawdown %']:.1f}%")
                else:
                    st.info("Need closed trades for deep dive.")
            except Exception as e: st.error(f"Error calculating metrics: {e}")
            st.divider()

        if not expired_df.empty:
            with st.expander(" Detailed Trade History (Closed Trades)", expanded=False):
                hist_cols = ['Entry Date', 'Exit Date', 'Days Held', 'Name', 'Strategy', 'Debit', 'P&L', 'ROI', 'Ann. ROI']
                hist_view = expired_df[hist_cols].copy()
                hist_view['Entry Date'] = hist_view['Entry Date'].dt.date
                hist_view['Exit Date'] = hist_view['Exit Date'].dt.date
                st.dataframe(hist_view.style.format({'Debit': "${:,.0f}", 'P&L': "${:,.0f}", 'ROI': "{:.2f}%", 'Ann. ROI': "{:.2f}%"}).map(lambda x: 'color: green' if x > 0 else 'color: red', subset=['P&L', 'ROI', 'Ann. ROI']), use_container_width=True)

            st.markdown("###  Closed Trade Performance")
            expired_df['Cap_Days'] = expired_df['Debit'] * expired_df['Days Held'].clip(lower=1)
            perf_agg = expired_df.groupby('Strategy').agg({'P&L': 'sum', 'Debit': 'sum', 'Cap_Days': 'sum', 'ROI': 'mean', 'id': 'count'}).reset_index()
            wins = expired_df[expired_df['P&L'] > 0].groupby('Strategy')['id'].count().reset_index(name='Wins')
            perf_agg = perf_agg.merge(wins, on='Strategy', how='left').fillna(0)
            perf_agg['Win Rate'] = perf_agg['Wins'] / perf_agg['id']
            perf_agg['Ann. TWR %'] = (perf_agg['P&L'] / perf_agg['Cap_Days']) * 365 * 100
            perf_agg['Simple Return %'] = (perf_agg['P&L'] / perf_agg['Debit']) * 100
            
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
            
            total_row = pd.DataFrame({'Strategy': ['TOTAL'], 'Trades': [total_trades], 'Win Rate': [total_win_rate], 'Total P&L': [total_pnl], 'Total Volume': [total_vol], 'Simple Return %': [total_simple_ret], 'Ann. TWR %': [total_twr], 'Avg Trade ROI': [avg_trade_roi], 'Sharpe': [0]})
            perf_display = pd.concat([perf_display, total_row], ignore_index=True)

            st.dataframe(perf_display.style.format({'Win Rate': "{:.1%}", 'Total P&L': "${:,.0f}", 'Total Volume': "${:,.0f}", 'Simple Return %': "{:.2f}%", 'Ann. TWR %': "{:.2f}%", 'Avg Trade ROI': "{:.2f}%", 'Sharpe': "{:.2f}"}).map(lambda x: 'color: green' if x > 0 else 'color: red', subset=['Total P&L', 'Simple Return %', 'Ann. TWR %', 'Avg Trade ROI', 'Sharpe']).apply(lambda x: ['background-color: #d1d5db; color: black; font-weight: bold' if x.name == len(perf_display)-1 else '' for _ in x], axis=1), use_container_width=True)
            
            st.subheader(" Efficiency Showdown: Active vs Historical")
            st.caption("Are current campaigns outperforming your historical average? (Metric: Annualized Return on Invested Capital)")
            active_eff_df = pd.DataFrame()
            if not active_df.empty:
                active_df['Cap_Days'] = active_df['Debit'] * active_df['Days Held'].clip(lower=1)
                active_agg = active_df.groupby('Strategy')[['P&L', 'Cap_Days']].sum().reset_index()
                active_agg['Return %'] = (active_agg['P&L'] / active_agg['Cap_Days']) * 365 * 100
                active_agg['Type'] = 'Active (Current)'
                active_eff_df = active_agg[['Strategy', 'Return %', 'Type']]
            hist_eff_df = pd.DataFrame()
            if not perf_agg.empty:
                hist_eff = perf_agg[['Strategy', 'Ann. TWR %']].copy()
                hist_eff.rename(columns={'Ann. TWR %': 'Return %'}, inplace=True)
                hist_eff['Type'] = 'Historical (Closed)'
                hist_eff_df = hist_eff
            if not active_eff_df.empty or not hist_eff_df.empty:
                combined_eff = pd.concat([active_eff_df, hist_eff_df], ignore_index=True)
                combined_eff = combined_eff[combined_eff['Strategy'] != 'TOTAL']
                fig_compare = px.bar(combined_eff, x='Strategy', y='Return %', color='Type', barmode='group', title="Capital Efficiency Comparison (Annualized Return)", color_discrete_map={'Active (Current)': '#00CC96', 'Historical (Closed)': '#636EFA'}, text='Return %')
                fig_compare.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                st.plotly_chart(fig_compare, use_container_width=True)

            st.subheader(" Profit Anatomy: Call vs Put Contribution")
            strat_anatomy = expired_df.groupby('Strategy')[['Put P&L', 'Call P&L']].mean().reset_index()
            fig_strat_ana = go.Figure()
            fig_strat_ana.add_trace(go.Bar(y=strat_anatomy['Strategy'], x=strat_anatomy['Put P&L'], name='Avg Put Profit', orientation='h', marker_color='#EF553B'))
            fig_strat_ana.add_trace(go.Bar(y=strat_anatomy['Strategy'], x=strat_anatomy['Call P&L'], name='Avg Call Profit', orientation='h', marker_color='#00CC96'))
            fig_strat_ana.update_layout(barmode='relative', title="Average Profit Sources per Strategy (Stacked)", xaxis_title="Average P&L ($)")
            st.plotly_chart(fig_strat_ana, use_container_width=True)

            st.markdown("#####  Trade-by-Trade Attribution")
            strat_list = sorted(expired_df['Strategy'].unique())
            sel_strat_ana = st.selectbox("Select Strategy to Analyze:", strat_list, key="ana_strat_sel")
            trade_subset = expired_df[expired_df['Strategy'] == sel_strat_ana].sort_values('Exit Date')
            if not trade_subset.empty:
                fig_trade_ana = go.Figure()
                fig_trade_ana.add_trace(go.Bar(x=trade_subset['Name'], y=trade_subset['Put P&L'], name='Put PnL', marker_color='#EF553B'))
                fig_trade_ana.add_trace(go.Bar(x=trade_subset['Name'], y=trade_subset['Call P&L'], name='Call PnL', marker_color='#00CC96'))
                fig_trade_ana.update_layout(barmode='relative', title=f"Profit Attribution: {sel_strat_ana}", xaxis_title="Trade", yaxis_title="PnL ($)", xaxis_tickangle=-45)
                st.plotly_chart(fig_trade_ana, use_container_width=True)

    with an_trends:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader(" Root Cause Analysis")
            if not df.empty and 'Status' in df.columns:
                expired_wins = df[(df['Status'] == 'Expired') & (df['P&L'] > 0)]
                active_trades = df[df['Status'] == 'Active']
                if not expired_wins.empty and not active_trades.empty:
                    avg_win_debit = expired_wins.groupby('Strategy')['Debit/Lot'].mean().reset_index()
                    avg_act_debit = active_trades.groupby('Strategy')['Debit/Lot'].mean().reset_index()
                    avg_win_debit['Type'] = 'Winning History'; avg_act_debit['Type'] = 'Active (Current)'
                    comp_df = pd.concat([avg_win_debit, avg_act_debit])
                    fig_price = px.bar(comp_df, x='Strategy', y='Debit/Lot', color='Type', barmode='group', title="Entry Price per Lot Comparison", color_discrete_map={'Winning History': 'green', 'Active (Current)': 'orange'})
                    st.plotly_chart(fig_price, use_container_width=True)
        with col2:
            st.subheader(" Profit Drivers (Puts vs Calls)")
            if not df.empty and 'Status' in df.columns:
                expired = df[df['Status'] == 'Expired'].copy()
                if not expired.empty:
                    leg_agg = expired.groupby('Strategy')[['Put P&L', 'Call P&L']].sum().reset_index()
                    fig_legs = px.bar(leg_agg, x='Strategy', y=['Put P&L', 'Call P&L'], title="Profit Source Split", color_discrete_map={'Put P&L': '#EF553B', 'Call P&L': '#00CC96'})
                    st.plotly_chart(fig_legs, use_container_width=True)
        st.divider()
        if not expired_df.empty:
            ec_df = expired_df.dropna(subset=["Exit Date"]).sort_values("Exit Date").copy()
            ec_df['Cumulative P&L'] = ec_df['P&L'].cumsum()
            fig = px.line(ec_df, x='Exit Date', y='Cumulative P&L', title="Realized Equity Curve", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        st.divider()
        hm1, hm2, hm3 = st.tabs([" Seasonality", " Duration", " Entry Day"])
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
        r_corr, r_mae = st.tabs([" Correlation Matrix", " MAE vs MFE (Edge Analysis)"])
        with r_corr:
            st.subheader("Strategy Correlation (Daily P&L)")
            snaps = load_snapshots()
            if not snaps.empty:
                fig_rolling_corr = rolling_correlation_matrix(snaps)
                if fig_rolling_corr:
                    st.plotly_chart(fig_rolling_corr, use_container_width=True)
                    st.caption("Heatmap shows correlations of strategy P&L over the last 30 days. Red = Strategies moving together (Risk). Blue = Diversified.")
                else: st.info("Insufficient snapshot history.")
        with r_mae:
            st.subheader("Excursion Analysis: Pain (MAE) vs Potential (MFE)")
            mae_view = st.radio("View:", ["Closed Trades Only (Final Result)", "Include Active Trades (Current Drawdown)"], horizontal=True)
            if not snaps.empty and not df.empty:
                excursion_df = snaps.groupby('trade_id')['pnl'].agg(['min', 'max']).reset_index()
                excursion_df.rename(columns={'min': 'MAE', 'max': 'MFE'}, inplace=True)
                merged_mae = df.merge(excursion_df, left_on='id', right_on='trade_id', how='inner')
                viz_mae = merged_mae if "Include Active" in mae_view else merged_mae[merged_mae['Status'] == 'Expired'].copy()
                if not viz_mae.empty:
                    mae_c1, mae_c2 = st.columns(2)
                    with mae_c1:
                        fig_mae_scat = px.scatter(viz_mae, x='MAE', y='P&L', color='Strategy', symbol='Status' if "Include Active" in mae_view else None, hover_data=['Name', 'Days Held'], title="Drawdown (MAE) vs Final P&L")
                        fig_mae_scat.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.5); fig_mae_scat.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.5)
                        st.plotly_chart(fig_mae_scat, use_container_width=True)
                    with mae_c2:
                        viz_mfe = viz_mae[viz_mae['MFE'] > 0]
                        fig_mfe = px.scatter(viz_mfe, x='MFE', y='P&L', color='Strategy', hover_data=['Name'], title="Potential (MFE) vs Final P&L")
                        if not viz_mfe.empty:
                             max_val = max(viz_mfe['MFE'].max(), viz_mfe['P&L'].max())
                             fig_mfe.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val, line=dict(color="green", dash="dot"))
                        st.plotly_chart(fig_mfe, use_container_width=True)

    with an_lifecycle:
        st.subheader("⏳ Profit Timing & Capital Efficiency")
        st.caption("Analyze WHERE in the trade duration the profit is actually made to optimize churn.")
        
        snaps = load_snapshots()
        all_lifecycle_data = []
        
        # Select strategies to analyze
        strategies_avail = df['Strategy'].unique()
        selected_lifecycle_strat = st.multiselect("Select Strategies", strategies_avail, default=strategies_avail[:2] if len(strategies_avail) > 0 else strategies_avail)
        
        subset_df = df[df['Strategy'].isin(selected_lifecycle_strat)].copy()
        
        if not subset_df.empty:
            progress_text = "Reconstructing Lifecycle Curves..."
            my_bar = st.progress(0, text=progress_text)
            
            total_items = len(subset_df)
            for i, (idx, row) in enumerate(subset_df.iterrows()):
                curve_df = get_trade_lifecycle_data(row, snaps)
                if not curve_df.empty:
                    curve_df['Strategy'] = row['Strategy']
                    curve_df['Trade Name'] = row['Name']
                    curve_df['Status'] = row['Status']
                    all_lifecycle_data.append(curve_df)
                
                # Safe progress calculation
                prog_val = min((i + 1) / total_items, 1.0)
                my_bar.progress(prog_val, text=progress_text)
            my_bar.empty()
            
            if all_lifecycle_data:
                full_lifecycle_df = pd.concat(all_lifecycle_data, ignore_index=True)
                
                # --- VISUAL 1: THE HARVEST CURVE ---
                st.markdown("##### 1. The Harvest Curve (Profit vs Duration %)")
                st.caption("How fast do these strategies yield profit? Convex (bulging up) = Fast/Front-Loaded. Concave (dipping) = Slow/Back-Loaded.")
                
                fig_harvest = px.line(
                    full_lifecycle_df, 
                    x='Pct_Duration', 
                    y='Pct_PnL', 
                    color='Strategy', 
                    line_group='Trade Name', 
                    hover_data=['Trade Name'],
                    color_discrete_sequence=px.colors.qualitative.Vivid # BRIGHTER COLORS
                )
                # Make lines semi-transparent but visible
                fig_harvest.update_traces(opacity=0.4, line=dict(width=1.5)) # INCREASED VISIBILITY
                
                # Add Average Lines
                avg_curves = full_lifecycle_df.groupby(['Strategy', 'Pct_Duration'])['Pct_PnL'].mean().reset_index()
                # Smooth the average line slightly for visual clarity if needed, but grouping by rounded duration helps
                full_lifecycle_df['Pct_Duration_Bin'] = full_lifecycle_df['Pct_Duration'].round(-1) # Bin to nearest 10%
                avg_binned = full_lifecycle_df.groupby(['Strategy', 'Pct_Duration_Bin'])['Pct_PnL'].mean().reset_index()
                
                for strat in selected_lifecycle_strat:
                    strat_avg = avg_binned[avg_binned['Strategy'] == strat]
                    if not strat_avg.empty:
                        fig_harvest.add_trace(go.Scatter(
                            x=strat_avg['Pct_Duration_Bin'], 
                            y=strat_avg['Pct_PnL'], 
                            mode='lines', 
                            name=f"AVG: {strat}", 
                            line=dict(width=4)
                        ))
                        
                fig_harvest.update_layout(xaxis_title="% of Trade Duration", yaxis_title="% of Total Profit", showlegend=True)
                fig_harvest.add_shape(type="line", x0=0, y0=0, x1=100, y1=100, line=dict(color="white", dash="dot", width=1), opacity=0.5)
                st.plotly_chart(fig_harvest, use_container_width=True)
                
                # --- VISUAL 2: PHASING HISTOGRAM ---
                st.markdown("##### 2. Profit Phasing (Where is the money made?)")
                c_p1, c_p2 = st.columns(2)
                
                with c_p1:
                    # Categorize PnL into phases: Early (0-30%), Mid (30-70%), Late (70-100%)
                    def classify_phase(pct):
                        if pct <= 30: return "1. Early (0-30%)"
                        elif pct <= 70: return "2. Mid (30-70%)"
                        return "3. Late (70-100%)"
                    
                    full_lifecycle_df['Phase'] = full_lifecycle_df['Pct_Duration'].apply(classify_phase)
                    
                    # Calculate PnL Delta (Marginal PnL per step) to verify where it was EARNED, not just cumulative
                    # Sort first
                    full_lifecycle_df = full_lifecycle_df.sort_values(['Trade Name', 'Pct_Duration'])
                    full_lifecycle_df['Prev_PnL'] = full_lifecycle_df.groupby('Trade Name')['Cumulative_PnL'].shift(1).fillna(0)
                    full_lifecycle_df['Marginal_PnL'] = full_lifecycle_df['Cumulative_PnL'] - full_lifecycle_df['Prev_PnL']
                    
                    # Group by Strategy and Phase, Sum Marginal PnL
                    phase_attribution = full_lifecycle_df.groupby(['Strategy', 'Phase'])['Marginal_PnL'].sum().reset_index()
                    
                    fig_phase = px.bar(
                        phase_attribution, 
                        x='Strategy', 
                        y='Marginal_PnL', 
                        color='Phase', 
                        barmode='group',
                        title="Net Profit Generated by Phase ($)",
                        color_discrete_map={"1. Early (0-30%)": "#636EFA", "2. Mid (30-70%)": "#00CC96", "3. Late (70-100%)": "#EF553B"}
                    )
                    st.plotly_chart(fig_phase, use_container_width=True)
                    
                with c_p2:
                    st.markdown("**Capital Stagnation Analysis**")
                    # Calculate "Time to 80% Profit" for closed winning trades
                    closed_winners = subset_df[(subset_df['Status'] == 'Expired') & (subset_df['P&L'] > 0)]['Name'].unique()
                    winner_curves = full_lifecycle_df[full_lifecycle_df['Trade Name'].isin(closed_winners)].copy()
                    
                    stagnation_data = []
                    for t_name in closed_winners:
                        t_data = winner_curves[winner_curves['Trade Name'] == t_name]
                        if t_data.empty: continue
                        
                        total_days = t_data['Day'].max()
                        final_pnl = t_data['Cumulative_PnL'].max()
                        if final_pnl <= 0: continue
                        
                        # Find day where we crossed 80% of final PnL
                        threshold = final_pnl * 0.80
                        cross_row = t_data[t_data['Cumulative_PnL'] >= threshold].head(1)
                        
                        if not cross_row.empty:
                            day_reached = cross_row['Day'].values[0]
                            wasted_days = total_days - day_reached
                            stagnation_data.append({
                                'Trade': t_name,
                                'Strategy': t_data['Strategy'].iloc[0],
                                'Total Days': total_days,
                                'Days to 80%': day_reached,
                                'Zombie Days': wasted_days,
                                'Zombie %': (wasted_days / total_days) * 100
                            })
                            
                    if stagnation_data:
                        stag_df = pd.DataFrame(stagnation_data)
                        fig_stag = px.scatter(
                            stag_df, 
                            x='Days to 80%', 
                            y='Total Days', 
                            color='Strategy', 
                            size='Zombie Days',
                            hover_data=['Trade', 'Zombie %'],
                            title="Efficiency: Days to 80% Gain vs Total Hold"
                        )
                        # Add diagonal line (Perfect Efficiency)
                        max_d = stag_df['Total Days'].max()
                        fig_stag.add_shape(type="line", x0=0, y0=0, x1=max_d, y1=max_d, line=dict(color="grey", dash="dash"))
                        fig_stag.add_annotation(x=max_d*0.8, y=max_d*0.2, text="Efficient Zone", showarrow=False)
                        fig_stag.add_annotation(x=max_d*0.2, y=max_d*0.8, text="Zombie Zone (Wasted Time)", showarrow=False)
                        
                        st.plotly_chart(fig_stag, use_container_width=True)
                        st.caption("Trades significantly above the diagonal line achieved their bulk profit early but were held too long.")
                    else:
                        st.info("Not enough closed winning trades for Stagnation Analysis.")
                
                # --- NEW FEATURE: CAPITAL REDEPLOYMENT SUGGESTIONS ---
                st.markdown("### 💡 Capital Efficiency Engine")
                st.caption("AI Suggestions: Trades where capital is earning less than your historical average.")
                
                redeploy_list = []
                for strat in selected_lifecycle_strat:
                    # Get baseline for this strat
                    bench = dynamic_benchmarks.get(strat, {})
                    avg_daily_yield_pct = bench.get('yield', 0.1) # Default 0.1% if missing
                    
                    # Check active trades for this strat
                    strat_active = df[(df['Status'] == 'Active') & (df['Strategy'] == strat)]
                    
                    for idx, row in strat_active.iterrows():
                        curr_yield = row['Daily Yield %']
                        # If earning less than 50% of strategy average and profitable
                        if row['P&L'] > 0 and curr_yield < (avg_daily_yield_pct * 0.5):
                             opportunity_cost = (avg_daily_yield_pct - curr_yield) * row['Days Held'] # Roughly points lost
                             redeploy_list.append({
                                 'Trade': row['Name'],
                                 'Strategy': strat,
                                 'Current Yield': f"{curr_yield:.2f}%/day",
                                 'Target Yield': f"{avg_daily_yield_pct:.2f}%/day",
                                 'Action': "Harvest & Redeploy",
                                 'Reason': "Capital Stagnation"
                             })
                
                if redeploy_list:
                    st.dataframe(pd.DataFrame(redeploy_list), use_container_width=True)
                else:
                    st.success("All active capital is performing near or above historical baselines.")

    with an_rolls: 
        st.subheader(" Roll Campaign Analysis")
        rolled_trades = df[df['Parent ID'] != ""].copy()
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
        else:
            st.info("No roll campaigns detected. Link trades using the 'Parent ID' column in the Journal.")

with tab_ai:
    st.markdown("###  The Quant Brain (Beta)")
    st.caption("Self-learning insights based on your specific trading history.")
    if df.empty or expired_df.empty:
        st.info(" Need more historical data to power the AI engine.")
    else:
        active_trades = df[df['Status'].isin(['Active', 'Missing'])].copy()
        with st.expander(" Calibration & Thresholds", expanded=False):
            c_set1, c_set2, c_set3 = st.columns(3)
            with c_set1:
                st.markdown("** Rot Detector**")
                rot_threshold = st.slider("Efficiency Drop Threshold %", 10, 90, 50) / 100.0
                min_days_rot = st.number_input("Min Days to Check", 5, 60, 10)
            with c_set2:
                st.markdown("** Prediction Logic**")
                prob_high = st.slider("High Confidence Threshold", 60, 95, 75)
                prob_low = st.slider("Low Confidence Threshold", 10, 50, 40)
            with c_set3:
                st.markdown("** Exit Targets**")
                exit_percentile = st.slider("Optimal Exit Percentile", 50, 95, 75) / 100.0

        st.subheader(" Win Probability Forecast (KNN Model + Kelly Size)")
        strategies_avail = sorted(active_trades['Strategy'].unique().tolist())
        selected_strat_ai = st.selectbox("Filter by Strategy", ["All"] + strategies_avail, key="ai_strat_filter")
        if selected_strat_ai != "All": ai_view_df = active_trades[active_trades['Strategy'] == selected_strat_ai].copy()
        else: ai_view_df = active_trades.copy()

        if not ai_view_df.empty:
            preds = generate_trade_predictions(ai_view_df, expired_df, prob_low, prob_high, total_cap)
            if not preds.empty:
                c_p1, c_p2 = st.columns([2, 3]) 
                with c_p1:
                    fig_pred = px.scatter(preds, x="Win Prob %", y="Expected PnL", color="Confidence", size="Rec. Size ($)", hover_data=["Trade Name", "Strategy", "Kelly Size %"], color_continuous_scale="RdYlGn", title="Risk/Reward Map (Size = Kelly Rec)")
                    fig_pred.add_vline(x=50, line_dash="dash", line_color="gray"); fig_pred.add_hline(y=0, line_dash="dash", line_color="gray")
                    st.plotly_chart(fig_pred, use_container_width=True)
                with c_p2:
                    st.dataframe(preds.style.format({'Win Prob %': "{:.1f}%", 'Expected PnL': "${:,.0f}", 'Confidence': "{:.0f}%", 'Kelly Size %': "{:.1f}%", 'Rec. Size ($)': "${:,.0f}"}).map(lambda v: 'color: green; font-weight: bold' if v > prob_high else ('color: red; font-weight: bold' if v < prob_low else 'color: orange'), subset=['Win Prob %']), use_container_width=True)
            else: st.info("Not enough closed trades with matching Greek profiles for prediction.")
        
        st.divider()
        c_ai_1, c_ai_2 = st.columns(2)
        with c_ai_1:
            st.subheader(" Capital Rot Detector")
            if not active_trades.empty:
                # --- v147.0 FIX: Reconstruct rot_df using new row-based function ---
                rot_rows = []
                for idx, row in active_trades.iterrows():
                    s_name = row.get('Strategy', 'Unknown')
                    h_days = dynamic_benchmarks.get(s_name, {}).get('avg_days', 30)
                    h_win = dynamic_benchmarks.get(s_name, {}).get('avg_win', 100)
                    
                    curr_spd, t_eff, status = check_rot_and_efficiency(row, h_days)
                    base_spd = h_win / max(1, h_days)
                    
                    rot_rows.append({
                        'Trade': row.get('Symbol', f"Trade {idx}"), 
                        'Strategy': s_name,
                        'Current Speed': curr_spd,
                        'Baseline Speed': base_spd,
                        'Status': status,
                        'Raw Current': curr_spd,  # Fix for plotting key
                        'Raw Baseline': base_spd  # Fix for plotting key
                    })
                rot_df = pd.DataFrame(rot_rows)
                rot_viz = rot_df.copy() # Ensure viz copy exists
                if not rot_df.empty:
                    rot_viz = rot_df.copy()
                    fig_rot = go.Figure()
                    fig_rot.add_trace(go.Bar(x=rot_viz['Trade'], y=rot_viz['Raw Current'], name='Current Speed', marker_color='#EF553B'))
                    fig_rot.add_trace(go.Bar(x=rot_viz['Trade'], y=rot_viz['Raw Baseline'], name='Baseline Speed', marker_color='gray'))
                    fig_rot.update_layout(title="Capital Velocity Lag ($/Day/1k)", barmode='group')
                    st.plotly_chart(fig_rot, use_container_width=True)
                    st.dataframe(rot_df[['Trade', 'Strategy', 'Current Speed', 'Baseline Speed', 'Status']], use_container_width=True)
                else: st.success(" Capital is moving efficiently. No rot detected.")
        with c_ai_2:

            st.subheader("Diagnostics")
            # --- v147.0 UI: Velocity Speedometer ---
            if 'velocity_stats' in locals() and velocity_stats: # Use velocity_stats (global) not strat_vel_stats
                # Just show the first active trade or aggregate for demo if multiple
                # Ideally this should be trade-specific selection, but for dashboard summary:
                if not active_trades.empty:
                     # Pick the fastest moving trade as the example
                     active_trades['daily_pnl'] = active_trades['P&L'] / active_trades['Days Held'].clip(lower=1)
                     fastest_trade = active_trades.loc[active_trades['daily_pnl'].idxmax()]
                     strat = fastest_trade['Strategy']
                     
                     if strat in velocity_stats:
                        v_thresh = velocity_stats[strat]['threshold']
                        d_pnl = fastest_trade['daily_pnl']
                        
                        fig_gauge = go.Figure(go.Indicator(
                            mode = "gauge+number",
                            value = d_pnl,
                            title = {'text': f"🚀 Top Velocity: {fastest_trade['Name']}"},
                            gauge = {
                                'axis': {'range': [None, v_thresh * 1.5]},
                                'bar': {'color': "#00cc96" if d_pnl < v_thresh else "#EF553B"},
                                'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': v_thresh},
                                'steps': [{'range': [0, v_thresh], 'color': "lightgray"}]
                            }
                        ))
                        fig_gauge.update_layout(height=200, margin=dict(l=20,r=20,t=30,b=20))
                        st.plotly_chart(fig_gauge, use_container_width=True)
            # ---------------------------------------
            st.subheader(f" Optimal Exit Zones ({int(exit_percentile*100)}th Percentile)")
            targets = get_dynamic_targets(expired_df, exit_percentile)
            if targets:
                winners = expired_df[expired_df['P&L'] > 0]
                if not winners.empty:
                    fig_exit = px.box(winners, x="Strategy", y="P&L", points="all", title="Historical Win Distribution & Targets")
                    st.plotly_chart(fig_exit, use_container_width=True)
                target_data = []
                for s, v in targets.items(): target_data.append({'Strategy': s, 'Median Win': v['Median Win'], 'Optimal Exit': v['Optimal Exit']})
                t_df = pd.DataFrame(target_data)
                st.dataframe(t_df.style.format({'Median Win': '${:,.0f}', 'Optimal Exit': '${:,.0f}'}), use_container_width=True)

with tab_rules:
    strategies_for_rules = sorted(list(dynamic_benchmarks.keys()))
    adaptive_content = generate_adaptive_rulebook_text(expired_df, strategies_for_rules)

    # --- v147.0 UI: Risk Floor Visuals ---
    st.markdown("### 🛡️ Smart Stop & Risk Analysis")
    if 'dynamic_benchmarks' in globals():
        for strat, data in dynamic_benchmarks.items():
            mae = mae_stats.get(strat, "N/A") if 'mae_stats' in globals() else "N/A"
            vel = velocity_stats.get(strat) if 'velocity_stats' in globals() else None
            
            with st.expander(f"📊 {strat} Risk Profile"):
                c_r1, c_r2 = st.columns(2)
                with c_r1:
                    st.write(f"**Target Win:** ${data.get('pnl', 0):.0f}")
                    if mae != "N/A":
                        st.error(f"**Smart Stop (MAE):** ${mae:.2f}")
                        safe_range = abs(mae)
                        fig_mae = go.Figure()
                        fig_mae.add_trace(go.Bar(x=[safe_range], y=["Risk Room"], orientation='h', marker_color='lightgreen', name="Safe Zone"))
                        fig_mae.update_layout(xaxis_title="Max Drawdown ($)", xaxis=dict(range=[0, safe_range*1.2]), height=100, margin=dict(l=0,r=0,t=0,b=0), showlegend=False)
                        st.plotly_chart(fig_mae, use_container_width=True)
                    else: st.write("No MAE data yet.")
                
                with c_r2:
                    if vel:
                        st.success(f"**Velocity Limit:** ${vel['threshold']:.2f}/day")
                        st.caption("Profit speed exceeding this is an anomaly.")
                    else: st.write("No Velocity data yet.")
    st.markdown(adaptive_content)
    st.divider()
    st.caption("Allantis Trade Guardian v147.0 (Cloud Edition)")
