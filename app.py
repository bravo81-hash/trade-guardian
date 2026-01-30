import streamlit as st

# --- GLOBAL STATE INIT ---
if 'velocity_stats' not in globals(): velocity_stats = {}
if 'mae_stats' not in globals(): mae_stats = {}

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

# --- UI OVERHAUL: CSS INJECTION ---
def inject_custom_css():
    st.markdown("""
        <style>
        /* FONTS */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;800&family=JetBrains+Mono:wght@400;700&display=swap');

        /* APP BASE */
        .stApp {
            background-color: #0f172a; /* Slate 900 */
            color: #e2e8f0; /* Slate 200 */
            font-family: 'Inter', sans-serif;
        }

        /* SIDEBAR */
        [data-testid="stSidebar"] {
            background-color: #020617; /* Slate 950 */
            border-right: 1px solid #1e293b;
        }

        /* HEADERS */
        h1, h2, h3, h4, h5 {
            color: #f1f5f9 !important;
            font-weight: 800 !important;
            letter-spacing: -0.025em;
        }
        .gradient-text {
            background: linear-gradient(to right, #38bdf8, #818cf8);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800;
        }

        /* COMPACT METRICS */
        div[data-testid="stMetric"] {
            background-color: rgba(30, 41, 59, 0.4);
            padding: 12px 16px !important;
            border-radius: 8px;
            border: 1px solid rgba(148, 163, 184, 0.1);
            backdrop-filter: blur(10px);
            min-height: 85px;
        }
        [data-testid="stMetricLabel"] {
            font-size: 0.75rem !important;
            color: #94a3b8 !important;
        }
        [data-testid="stMetricValue"] {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.6rem !important;
            color: #f8fafc !important;
        }

        /* TABS (BOLD & COMPACT) */
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            margin-bottom: 1rem;
        }
        .stTabs [data-baseweb="tab"] {
            height: 45px;
            background-color: rgba(30, 41, 59, 0.5);
            border-radius: 6px;
            color: #94a3b8;
            font-weight: 700 !important;
            font-size: 0.9rem;
            padding: 0 20px;
            border: 1px solid transparent;
        }
        .stTabs [aria-selected="true"] {
            background-color: rgba(56, 189, 248, 0.1) !important;
            color: #38bdf8 !important;
            border: 1px solid rgba(56, 189, 248, 0.3) !important;
        }

        /* DATAFRAMES */
        [data-testid="stDataFrame"] {
            border: 1px solid #1e293b;
            border-radius: 8px;
        }

        /* EXPANDERS */
        div[data-testid="stExpander"] {
            border: 1px solid #334155;
            border-radius: 8px;
            background-color: rgba(15, 23, 42, 0.5);
        }
        
        /* CUSTOM CARDS */
        .custom-card {
            background-color: rgba(30, 41, 59, 0.3);
            border: 1px solid rgba(148, 163, 184, 0.1);
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
        }
        </style>
    """, unsafe_allow_html=True)

inject_custom_css()

# --- PLOTLY THEME CONFIG ---
def apply_chart_theme(fig):
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Inter, sans-serif", color="#94a3b8"),
        title_font=dict(family="Inter, sans-serif", size=16, color="#f1f5f9"),
        hoverlabel=dict(bgcolor="#1e293b", font_size=12, font_family="JetBrains Mono"),
        colorway=['#38bdf8', '#34d399', '#818cf8', '#f472b6', '#fbbf24'],
        xaxis=dict(showgrid=True, gridcolor='rgba(148, 163, 184, 0.1)', zerolinecolor='rgba(148, 163, 184, 0.2)'),
        yaxis=dict(showgrid=True, gridcolor='rgba(148, 163, 184, 0.1)', zerolinecolor='rgba(148, 163, 184, 0.2)'),
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig

# --- HEADER ---
c_head_1, c_head_2 = st.columns([3, 1])
with c_head_1:
    st.markdown("""
        <div style="margin-bottom: 20px;">
            <span style="font-size: 0.7rem; font-weight: 800; color: #38bdf8; letter-spacing: 0.1em; background: rgba(56,189,248,0.1); padding: 4px 8px; border-radius: 4px;">v150.0 PRODUCTION</span>
            <h1 style="font-size: 2.5rem; margin-top: 5px; margin-bottom: 0;">Allantis <span class="gradient-text">Trade Guardian</span></h1>
        </div>
    """, unsafe_allow_html=True)

# --- DB & CLOUD LOGIC ---
DB_NAME = "trade_guardian_v4.db"
SCOPES = ['https://www.googleapis.com/auth/drive']

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
            except: pass

    def find_db_file(self):
        if not self.is_connected: return None, None
        if self.cached_file_id:
            try:
                f = self.service.files().get(fileId=self.cached_file_id, fields='id,name').execute()
                return f['id'], f['name']
            except: self.cached_file_id = None
        try:
            q_exact = f"name='{DB_NAME}' and trashed=false"
            res = self.service.files().list(q=q_exact, pageSize=1, fields="files(id, name)").execute()
            items = res.get('files', [])
            if items: 
                self.cached_file_id = items[0]['id']
                return items[0]['id'], items[0]['name']
            return None, None
        except: return None, None

    def get_cloud_modified_time(self, file_id):
        try:
            f = self.service.files().get(fileId=file_id, fields='modifiedTime').execute()
            return datetime.strptime(f['modifiedTime'].replace('Z', '+0000'), '%Y-%m-%dT%H:%M:%S.%f%z')
        except: return None

    def create_backup(self, file_id, file_name):
        try:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.service.files().copy(fileId=file_id, body={'name': f"BACKUP_{ts}_{file_name}"}).execute()
            return True
        except: return False

    def download_db(self, force=False):
        fid, fname = self.find_db_file()
        if not fid: return False, "Cloud DB not found."
        if os.path.exists(DB_NAME) and not force:
            try:
                l_ts = datetime.fromtimestamp(os.path.getmtime(DB_NAME), tz=timezone.utc)
                c_ts = self.get_cloud_modified_time(fid)
                if c_ts and (l_ts > c_ts + timedelta(minutes=2)):
                    return False, "CONFLICT: Local is newer."
            except: pass
        try:
            req = self.service.files().get_media(fileId=fid)
            fh = io.BytesIO()
            dl = MediaIoBaseDownload(fh, req)
            done = False
            while not done: _, done = dl.next_chunk()
            with open(DB_NAME, "wb") as f: f.write(fh.getbuffer())
            st.session_state['last_cloud_sync'] = datetime.now()
            return True, f"Downloaded '{fname}'"
        except Exception as e: return False, str(e)

    def upload_db(self, force=False):
        if not os.path.exists(DB_NAME): return False, "No local DB."
        fid, fname = self.find_db_file()
        if fid and not force:
            c_ts = self.get_cloud_modified_time(fid)
            l_ts = datetime.fromtimestamp(os.path.getmtime(DB_NAME), tz=timezone.utc)
            if c_ts and (c_ts > l_ts + timedelta(seconds=2)): return False, "CONFLICT: Cloud is newer."
        
        media = MediaFileUpload(DB_NAME, mimetype='application/x-sqlite3', resumable=True)
        try:
            if fid:
                self.create_backup(fid, fname)
                self.service.files().update(fileId=fid, media_body=media).execute()
            else:
                self.service.files().create(body={'name': DB_NAME}, media_body=media).execute()
            st.session_state['last_cloud_sync'] = datetime.now()
            return True, "Upload Successful"
        except Exception as e: return False, str(e)

drive_mgr = DriveManager()

def auto_sync_if_connected():
    if drive_mgr.is_connected:
        s, m = drive_mgr.upload_db()
        if s: st.toast(f"✅ Auto-Saved to Cloud")

def get_db_connection(): return sqlite3.connect(DB_NAME)

# --- CORE FUNCTIONS (RESTORED FULL LOGIC) ---
def init_db():
    if not os.path.exists(DB_NAME) and drive_mgr.is_connected: drive_mgr.download_db()
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT, parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY, trade_id TEXT, snapshot_date DATE, pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER, min_stability REAL, description TEXT, typical_debit REAL)''')
    
    # Schema migration checks
    try: c.execute("ALTER TABLE snapshots ADD COLUMN theta REAL"); 
    except: pass
    try: c.execute("ALTER TABLE snapshots ADD COLUMN delta REAL"); 
    except: pass
    try: c.execute("ALTER TABLE snapshots ADD COLUMN vega REAL"); 
    except: pass
    try: c.execute("ALTER TABLE snapshots ADD COLUMN gamma REAL"); 
    except: pass
    try: c.execute("ALTER TABLE strategy_config ADD COLUMN typical_debit REAL"); 
    except: pass
    try: c.execute("ALTER TABLE trades ADD COLUMN original_group TEXT"); 
    except: pass
    
    conn.commit()
    conn.close()
    
    # Seed defaults if empty
    conn = get_db_connection()
    if conn.cursor().execute("SELECT count(*) FROM strategy_config").fetchone()[0] == 0:
        defaults = [('130/160','130/160',500,36,0.8,'Income',4000), ('160/190','160/190',700,44,0.8,'Patience',5200), ('M200','M200',900,41,0.8,'Mastery',8000), ('SMSF','SMSF',600,40,0.8,'Wealth',5000)]
        conn.cursor().executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)
        conn.commit()
    conn.close()

@st.cache_data(ttl=60)
def load_strategy_config():
    if not os.path.exists(DB_NAME): return {}
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM strategy_config", conn)
        return {r['name']: {'id':r['identifier'], 'pnl':r['target_pnl'], 'dit':r['target_days'], 'stability':r['min_stability'], 'debit_per_lot':r.get('typical_debit', 5000)} for _,r in df.iterrows()}
    except: return {}
    finally: conn.close()

def generate_id(name, strategy, entry_date):
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    safe_name = re.sub(r'\W+', '', str(name))
    return f"{safe_name}_{strategy}_{d_str}"

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

# --- ROBUST OPTIONSTRAT PARSER (Restored from v147) ---
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
                
                # Robust Link Extraction
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

def parse_and_sync(file_list, file_type):
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
                
                # Check for Link based renaming
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
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, notes, tags, parent_id, put_pnl, call_pnl, iv, link, original_group)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, t['name'], t['strategy'], status, t['start_dt'].date(), 
                         t['exit_dt'].date() if t['exit_dt'] else None, 
                         t['days_held'], t['debit'], t['lot_size'], t['pnl'], 
                         t['theta'], t['delta'], t['gamma'], t['vega'], "", "", "", t['put_pnl'], t['call_pnl'], t['iv'], t['link'], t['group']))
                    count_new += 1
                else:
                    # Logic to preserve manual overrides (lot_size, strategy) if set
                    db_lot_size = existing[10]
                    final_lot_size = t['lot_size']
                    if db_lot_size and db_lot_size > 0: final_lot_size = db_lot_size

                    db_strategy = existing[11]
                    final_strategy = db_strategy
                    if db_strategy == 'Other' and t['strategy'] != 'Other': final_strategy = t['strategy']

                    # Update logic
                    final_theta = t['theta'] if t['theta'] != 0 else existing[2]
                    final_link = t['link'] if t['link'] != "" else existing[9]
                    
                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, put_pnl=?, call_pnl=?, iv=?, link=?, lot_size=?, strategy=?, original_group=?
                            WHERE id=?''', 
                            (t['pnl'], status, t['exit_dt'].date() if t['exit_dt'] else None, t['days_held'], 
                             final_theta, t['delta'], t['gamma'], t['vega'], t['put_pnl'], t['call_pnl'], t['iv'], final_link, final_lot_size, final_strategy, t['group'], trade_id))
                        count_update += 1
                    elif existing[1] in ["Active", "Missing"]: 
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, iv=?, link=?, status='Active', exit_date=?, lot_size=?, strategy=?, original_group=?
                            WHERE id=?''', 
                            (t['pnl'], t['days_held'], final_theta, t['delta'], t['gamma'], t['vega'], t['iv'], final_link, 
                             t['exit_dt'].date() if t['exit_dt'] else None, final_lot_size, final_strategy, t['group'], trade_id))
                        count_update += 1

                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    if not c.fetchone():
                        c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)",
                                  (trade_id, today, t['pnl'], t['days_held'], t['theta'], t['delta'], t['vega'], t['gamma']))
                    else:
                        c.execute("UPDATE snapshots SET theta=?, delta=?, vega=?, gamma=?, pnl=?, days_held=? WHERE trade_id=? AND snapshot_date=?",
                                  (t['theta'], t['delta'], t['vega'], t['gamma'], t['pnl'], t['days_held'], trade_id, today))
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

@st.cache_data(ttl=60)
def load_data():
    if not os.path.exists(DB_NAME): return pd.DataFrame()
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        if df.empty: return pd.DataFrame()
        
        # Types
        cols = ['pnl','debit','theta','delta','vega','gamma','days_held']
        for col in cols: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        df['Entry Date'] = pd.to_datetime(df['entry_date'])
        df['lot_size'] = pd.to_numeric(df['lot_size'], errors='coerce').fillna(1).astype(int)
        df.loc[df['lot_size'] < 1, 'lot_size'] = 1
        
        # Calc Metrics
        df['Stability'] = np.where(df['theta']>0, df['theta']/(df['delta'].abs()+1), 0)
        df['ROI'] = np.where(df['debit']>0, (df['pnl']/df['debit'])*100, 0)
        df['Daily Yield %'] = np.where(df['days_held']>0, df['ROI']/df['days_held'], 0)
        
        # Rename for UI
        df = df.rename(columns={'name':'Name', 'strategy':'Strategy', 'status':'Status', 'pnl':'P&L', 'debit':'Debit', 'theta':'Theta', 'delta':'Delta', 'vega':'Vega', 'days_held':'Days Held', 'link':'Link', 'notes':'Notes', 'tags':'Tags', 'parent_id':'Parent ID'})
        return df
    except: return pd.DataFrame()
    finally: conn.close()

@st.cache_data(ttl=300)
def load_snapshots():
    conn = get_db_connection()
    try: return pd.read_sql("SELECT * FROM snapshots", conn)
    except: return pd.DataFrame()
    finally: conn.close()

# --- FULL INTELLIGENCE LOGIC (Restored from v147) ---
def calculate_decision_ladder(row, config, regime_mult=1.0):
    strat = row['Strategy']
    days = row['Days Held']
    pnl = row['P&L']
    theta = row['Theta']
    status = row['Status']
    debit = row['Debit']
    
    if status == 'Missing': return "REVIEW", 100, "Missing from data"
    
    bench = config.get(strat, {})
    target_profit = (bench.get('pnl', 1000) * regime_mult) * row['lot_size']
    hist_avg_days = bench.get('dit', 45)
    
    score = 50 
    action = "HOLD"
    reason = "Normal"
    
    # 1. Profit Taking
    if pnl >= target_profit: return "TAKE PROFIT", 100, f"Hit Target ${target_profit:.0f}"
    elif pnl >= target_profit * 0.8: score += 30; action = "PREPARE EXIT"; reason = "Near Target"
    
    # 2. Defense / Zombies
    if pnl < 0:
        if theta > 0:
            recov_days = abs(pnl) / theta
            is_cooking = (strat == '160/190' and days < 30)
            is_young = days < 15
            if not is_cooking and not is_young:
                remaining_time_est = max(1, hist_avg_days - days)
                if recov_days > remaining_time_est:
                    score += 40
                    action = "CRITICAL"
                    reason = f"Zombie (Recov {recov_days:.0f}d > Left {remaining_time_est:.0f}d)"
        else:
            if days > 15: score += 30; reason = "Negative Theta"
    else:
        # 3. Efficiency
        left_in_tank = max(0, target_profit - pnl)
        if debit > 0 and (left_in_tank / debit) < 0.05:
            score += 40
            reason = "Squeezed Dry (Risk > Reward)"

    # 4. Strategy Specifics
    stale_threshold = hist_avg_days * 1.25 
    if strat == '130/160':
        limit_130 = min(stale_threshold, 30) 
        if days > limit_130 and pnl < (100 * row['lot_size']): return "KILL", 95, f"Stale (> {limit_130:.0f}d)"
    elif strat == '160/190':
        cooking_limit = max(30, hist_avg_days * 0.7)
        if days < cooking_limit: score = 10; action = "COOKING"; reason = f"Too Early (<{cooking_limit:.0f}d)"
    
    # 5. Stability
    if row['Stability'] < 0.3 and days > 5: score += 25; reason += " + Unstable"

    score = min(100, max(0, score))
    if score >= 90: action = "CRITICAL"
    elif score >= 70: action = "WATCH"
    elif score <= 30: action = "COOKING"
    return action, score, reason

def get_trade_lifecycle_data(row, snapshots_df):
    days = int(row['Days Held'])
    if days < 1: days = 1
    total_pnl = row['P&L']
    
    # Try Real Snapshots
    if not snapshots_df.empty:
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

    # Fallback Reconstruction
    daily_data = []
    initial_theta = row['Theta'] if row['Theta'] != 0 else 1.0
    weights = [abs(theta_decay_model(initial_theta, d, row['Strategy'], max(45, days))) for d in range(1, days + 1)]
    total_w = sum(weights)
    if total_w == 0: weights = [1/days] * days
    else: weights = [w/total_w for w in weights]
    
    cum_pnl = 0
    daily_data.append({'Day': 0, 'Cumulative_PnL': 0, 'Pct_Duration': 0, 'Pct_PnL': 0})
    for i, w in enumerate(weights):
        cum_pnl += total_pnl * w
        daily_data.append({'Day': i + 1, 'Cumulative_PnL': cum_pnl, 'Pct_Duration': ((i+1)/days)*100, 'Pct_PnL': (cum_pnl/(abs(total_pnl) if total_pnl!=0 else 1))*100})
    return pd.DataFrame(daily_data)

# --- MAIN APP LOGIC ---
init_db()
config = load_strategy_config()
df = load_data()

# --- SIDEBAR (Files) ---
with st.sidebar:
    st.markdown("### ☁️ Sync & Data")
    if GOOGLE_DEPS_INSTALLED:
        c1, c2 = st.columns(2)
        if c1.button("⬆️ Push"): 
            s, m = drive_mgr.upload_db()
            if s: st.success("Saved"); time.sleep(1); st.rerun()
            else: st.error(m)
        if c2.button("⬇️ Pull"):
            s, m = drive_mgr.download_db()
            if s: st.success("Loaded"); time.sleep(1); st.rerun()
            else: st.error(m)
    
    with st.expander("📂 File Upload", expanded=False):
        f_act = st.file_uploader("Active Trades", accept_multiple_files=True)
        f_hist = st.file_uploader("History", accept_multiple_files=True)
        if st.button("Process Files"):
            logs = []
            if f_act: logs.extend(parse_and_sync(f_act, "Active"))
            if f_hist: logs.extend(parse_and_sync(f_hist, "History"))
            st.write(logs)
            st.cache_data.clear()
            auto_sync_if_connected()
            time.sleep(1)
            st.rerun()
    
    st.divider()
    market_regime = st.selectbox("Market Regime", ["Neutral", "Bullish", "Bearish"])
    regime_mult = 1.1 if market_regime == "Bullish" else (0.9 if market_regime == "Bearish" else 1.0)

# --- DATA PREP FOR UI ---
active_df = pd.DataFrame()
if not df.empty:
    active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
    if not active_df.empty:
        res = active_df.apply(lambda r: calculate_decision_ladder(r, config, regime_mult), axis=1)
        active_df['Action'] = [x[0] for x in res]
        active_df['Urgency'] = [x[1] for x in res]
        active_df['Reason'] = [x[2] for x in res]

# --- LAYOUT TABS ---
tab_cmd, tab_desk, tab_quant, tab_sys = st.tabs([
    "🛡️ COMMAND CENTER", "⚡ TRADE DESK", "🧠 QUANT LAB", "⚙️ SYSTEM"
])

# ==========================================
# TAB 1: COMMAND CENTER
# ==========================================
with tab_cmd:
    if active_df.empty:
        st.info("👋 Welcome! Upload your OptionStrat files in the sidebar to begin.")
    else:
        # 1. KPI CARDS
        kpi_c1, kpi_c2, kpi_c3, kpi_c4 = st.columns(4)
        
        tot_theta = active_df['Theta'].sum()
        tot_pnl = active_df['P&L'].sum()
        tot_debit = active_df['Debit'].sum()
        urgent_count = len(active_df[active_df['Urgency'] >= 70])
        
        # Health Score Logic
        delta_exp = abs(active_df['Delta'].sum() / tot_debit * 100) if tot_debit else 0
        health_txt = "HEALTHY" if delta_exp < 2 else "WARNING"
        health_color = "normal" if delta_exp < 2 else "inverse"
        
        kpi_c1.metric("System Status", f"🟢 {health_txt}", delta=f"{delta_exp:.1f}% Net Delta", delta_color=health_color)
        kpi_c2.metric("Floating P&L", f"${tot_pnl:,.0f}", delta=f"{(tot_pnl/tot_debit)*100:.1f}% ROI" if tot_debit else "0%")
        kpi_c3.metric("Daily Income (Theta)", f"${tot_theta:,.0f}", delta=f"{(tot_theta/tot_debit)*100:.2f}% Yield/Day" if tot_debit else "0%")
        kpi_c4.metric("Attention Required", f"{urgent_count} Trades", delta="Action Items", delta_color="inverse" if urgent_count > 0 else "off")

        # 2. MAIN DASHBOARD AREA
        col_left, col_right = st.columns([1.8, 1.2])
        
        with col_left:
            st.markdown("##### 🚨 Priority Action Queue")
            queue = active_df[active_df['Urgency'] >= 50].sort_values('Urgency', ascending=False)
            
            if not queue.empty:
                for _, row in queue.iterrows():
                    color = "#ef4444" if row['Urgency'] >= 90 else "#f59e0b"
                    st.markdown(f"""
                        <div style="background: rgba(30,41,59,0.5); border-left: 4px solid {color}; padding: 10px 15px; margin-bottom: 8px; border-radius: 4px; display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <div style="font-weight: 700; color: #f8fafc;">{row['Name']} <span style="font-size:0.8em; color: #94a3b8; font-weight:400;">({row['Strategy']})</span></div>
                                <div style="font-size: 0.85em; color: {color};">{row['Action']} • {row['Reason']}</div>
                            </div>
                            <div style="text-align: right;">
                                <div style="font-family: monospace; color: #f8fafc;">{row['P&L']:+,.0f}</div>
                                <div style="font-size: 0.7em; color: #94a3b8;">{row['Days Held']}d old</div>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
            else:
                st.success("✅ All systems nominal. No urgent actions.")

        with col_right:
            st.markdown("##### 🧭 Portfolio Radar")
            # Calculate Radar Scores
            score_stab = min(100, (active_df['Stability'].mean() / 1.5) * 100)
            score_yield = min(100, ((tot_theta/tot_debit) / 0.001) * 50) if tot_debit else 0
            score_fresh = max(0, 100 - (active_df['Days Held'].mean() * 2))
            
            fig = go.Figure(data=go.Scatterpolar(
                r=[score_stab, score_yield, score_fresh, 80, 90],
                theta=['Stability', 'Yield', 'Freshness', 'Diversification', 'Neutrality'],
                fill='toself', line_color='#38bdf8', fillcolor='rgba(56, 189, 248, 0.2)'
            ))
            fig = apply_chart_theme(fig)
            fig.update_layout(height=250, margin=dict(l=30,r=30,t=20,b=20), polar=dict(radialaxis=dict(visible=True, range=[0,100], showticklabels=False)))
            st.plotly_chart(fig, use_container_width=True)
            
            # Mini Heatmap
            fig_h = px.scatter(active_df, x='Days Held', y='P&L', size='Debit', color='Urgency', color_continuous_scale='RdYlGn_r')
            fig_h = apply_chart_theme(fig_h)
            fig_h.update_layout(height=200, margin=dict(l=0,r=0,t=0,b=0), xaxis_title="Days", yaxis_title="P&L")
            st.plotly_chart(fig_h, use_container_width=True)

# ==========================================
# TAB 2: TRADE DESK
# ==========================================
with tab_desk:
    st.markdown("##### 🛠️ Active Position Management")
    
    # Pre-Flight Calculator (Collapsible)
    with st.expander("🚀 New Trade Architect (Pre-Flight)", expanded=False):
        c1, c2, c3 = st.columns(3)
        p_theta = c1.number_input("Theta", value=10.0)
        p_debit = c2.number_input("Debit/Risk", value=5000.0)
        p_delta = c3.number_input("Net Delta", value=-5.0)
        
        if p_debit > 0:
            stab = p_theta / (abs(p_delta)+1)
            yld = (p_theta / p_debit) * 100
            st.info(f"**Analysis:** Yield {yld:.2f}%/day | Stability {stab:.2f} | Ann. ROI {yld*365:.0f}%")
            if stab < 0.5: st.warning("⚠️ Low Stability (High Delta relative to Theta)")
            else: st.success("✅ Trade structure looks solid.")

    # The Journal
    if not active_df.empty:
        st.markdown("#### 📖 Live Journal")
        
        column_cfg = {
            "Link": st.column_config.LinkColumn("Link", display_text="Open"),
            "Urgency": st.column_config.ProgressColumn("Urgency", min_value=0, max_value=100, format="%d"),
            "Stability": st.column_config.ProgressColumn("Stability", min_value=0, max_value=3, format="%.2f"),
            "P&L": st.column_config.NumberColumn("P&L", format="$%d"),
            "Debit": st.column_config.NumberColumn("Debit", format="$%d"),
            "ROI": st.column_config.NumberColumn("ROI", format="%.1f%%"),
            "Name": st.column_config.TextColumn("Trade Name", width="medium"),
            "Notes": st.column_config.TextColumn("Journal Notes", width="large")
        }
        
        view_cols = ['id','Name','Link','Strategy','P&L','ROI','Urgency','Action','Stability','Days Held','Notes','Tags','Parent ID']
        edited = st.data_editor(
            active_df[view_cols], 
            column_config=column_cfg, 
            hide_index=True, 
            use_container_width=True,
            num_rows="fixed",
            height=400
        )
        
        if st.button("💾 Save Journal Changes"):
            conn = get_db_connection()
            cnt = 0
            for i, r in edited.iterrows():
                conn.execute("UPDATE trades SET notes=?, tags=?, parent_id=? WHERE id=?", (r['Notes'], r['Tags'], r['Parent ID'], r['id']))
                cnt += 1
            conn.commit()
            conn.close()
            st.success(f"Updated {cnt} records.")
            auto_sync_if_connected()
    else:
        st.info("No active trades to manage.")

# ==========================================
# TAB 3: QUANT LAB
# ==========================================
with tab_quant:
    q_tabs = st.tabs(["Performance", "Lifecycle & Efficiency", "Seasonality & Trends", "Roll Campaigns", "AI DNA"])
    
    expired_df = df[df['Status'] == 'Expired'].copy()
    
    with q_tabs[0]: # Performance
        if not expired_df.empty:
            c1, c2 = st.columns(2)
            strat_perf = expired_df.groupby('Strategy')['P&L'].sum().reset_index()
            fig_p = px.bar(strat_perf, x='Strategy', y='P&L', color='P&L', color_continuous_scale='RdYlGn', title="Net Profit by Strategy")
            fig_p = apply_chart_theme(fig_p)
            c1.plotly_chart(fig_p, use_container_width=True)
            
            wins = expired_df[expired_df['P&L']>0].groupby('Strategy')['id'].count()
            total = expired_df.groupby('Strategy')['id'].count()
            wr = (wins/total * 100).fillna(0).reset_index(name='Win Rate')
            fig_w = px.bar(wr, x='Strategy', y='Win Rate', title="Win Rate %", range_y=[0,100])
            fig_w.update_traces(marker_color='#34d399')
            fig_w = apply_chart_theme(fig_w)
            c2.plotly_chart(fig_w, use_container_width=True)

            # Attribution (Calls vs Puts)
            if 'Put P&L' in expired_df.columns:
                strat_anatomy = expired_df.groupby('Strategy')[['Put P&L', 'Call P&L']].mean().reset_index()
                fig_strat_ana = go.Figure()
                fig_strat_ana.add_trace(go.Bar(y=strat_anatomy['Strategy'], x=strat_anatomy['Put P&L'], name='Avg Put Profit', orientation='h', marker_color='#EF553B'))
                fig_strat_ana.add_trace(go.Bar(y=strat_anatomy['Strategy'], x=strat_anatomy['Call P&L'], name='Avg Call Profit', orientation='h', marker_color='#00CC96'))
                fig_strat_ana.update_layout(barmode='relative', title="Profit Attribution (Calls vs Puts)", xaxis_title="Avg P&L ($)")
                fig_strat_ana = apply_chart_theme(fig_strat_ana)
                st.plotly_chart(fig_strat_ana, use_container_width=True)
        else: st.warning("No closed trade history available.")

    with q_tabs[1]: # Lifecycle
        st.markdown("##### ⏳ The Harvest Curve")
        if not expired_df.empty:
            snaps = load_snapshots()
            # CRITICAL FIX: Trendline 'lowess' removed to fix statsmodels crash
            fig_lc = px.scatter(expired_df, x='Days Held', y='ROI', color='Strategy', title="ROI vs Duration")
            fig_lc = apply_chart_theme(fig_lc)
            st.plotly_chart(fig_lc, use_container_width=True)
            
            # Harvest Curve (Detailed)
            if not snaps.empty:
                st.markdown("##### Detailed Profit Path")
                lc_data = []
                for _, r in expired_df.head(20).iterrows(): # Sample top 20 for speed
                    c_df = get_trade_lifecycle_data(r, snaps)
                    if not c_df.empty:
                        c_df['Trade'] = r['Name']
                        c_df['Strategy'] = r['Strategy']
                        lc_data.append(c_df)
                if lc_data:
                    full_lc = pd.concat(lc_data)
                    fig_harvest = px.line(full_lc, x='Pct_Duration', y='Pct_PnL', color='Strategy', line_group='Trade', title="Profit Accumulation Path")
                    fig_harvest = apply_chart_theme(fig_harvest)
                    st.plotly_chart(fig_harvest, use_container_width=True)

    with q_tabs[2]: # Seasonality
        if not expired_df.empty:
            exp_hm = expired_df.dropna(subset=['Exit Date']).copy()
            exp_hm['Month'] = exp_hm['Exit Date'].dt.month_name()
            exp_hm['Year'] = exp_hm['Exit Date'].dt.year
            hm_data = exp_hm.groupby(['Year', 'Month']).agg({'P&L': 'sum'}).reset_index()
            months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
            fig = px.density_heatmap(hm_data, x="Month", y="Year", z="P&L", title="Monthly Seasonality ($)", category_orders={"Month": months}, text_auto=True, color_continuous_scale="RdBu")
            fig = apply_chart_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    with q_tabs[3]: # Rolls
        st.markdown("##### 🔄 Roll Campaign Analysis")
        rolled_trades = df[df['Parent ID'] != ""].copy()
        if not rolled_trades.empty:
            campaign_summary = []
            for parent in rolled_trades['Parent ID'].unique():
                if not parent: continue
                campaign = df[(df['id'] == parent) | (df['Parent ID'] == parent)]
                if campaign.empty: continue
                campaign_summary.append({'Campaign': parent[:15], 'Total P&L': campaign['P&L'].sum(), 'Legs': len(campaign)})
            if campaign_summary:
                st.dataframe(pd.DataFrame(campaign_summary), use_container_width=True)
        else: st.info("Link trades using 'Parent ID' in Journal to track rolls.")

    with q_tabs[4]: # AI
        st.markdown("##### 🧬 DNA Match")
        if not active_df.empty and not expired_df.empty:
            sel_trade = st.selectbox("Select Active Trade", active_df['Name'].unique())
            curr_row = active_df[active_df['Name'] == sel_trade].iloc[0]
            
            # Simple Euclidean distance on Greeks
            feats = ['Theta', 'Delta', 'Debit']
            hist_feats = expired_df[feats].dropna()
            curr_vec = curr_row[feats].to_numpy().reshape(1, -1)
            dists = cdist(curr_vec, hist_feats, metric='euclidean')
            
            # Find closest
            closest_idx = dists.argsort()[0][:3]
            similar = expired_df.iloc[closest_idx]
            
            st.write("**Most Similar Historical Trades:**")
            st.dataframe(similar[['Name', 'P&L', 'Days Held', 'Strategy']], use_container_width=True)

            # Rot Detector
            st.markdown("##### 🧟 Capital Rot Detector")
            rot_list = []
            for _, r in active_df.iterrows():
                bench = config.get(r['Strategy'], {})
                avg_yld = (bench.get('pnl', 1000)/bench.get('dit', 45)) / bench.get('typical_debit', 5000)
                curr_yld = r['Daily Yield %'] / 100
                if r['P&L'] > 0 and curr_yld < avg_yld * 0.5 and r['Days Held'] > 10:
                    rot_list.append({'Trade': r['Name'], 'Current Yield': f"{curr_yld*100:.2f}%", 'Target': f"{avg_yld*100:.2f}%"})
            
            if rot_list:
                st.warning(f"Detected {len(rot_list)} efficient trades slowing down:")
                st.dataframe(pd.DataFrame(rot_list), use_container_width=True)
            else:
                st.success("Capital efficiency nominal.")

# ==========================================
# TAB 4: SYSTEM
# ==========================================
with tab_sys:
    st.markdown("### ⚙️ Configuration")
    
    # Strategy Config Editor
    conn = get_db_connection()
    strat_df = pd.read_sql("SELECT * FROM strategy_config", conn)
    conn.close()
    
    edited_conf = st.data_editor(strat_df, num_rows="dynamic", use_container_width=True)
    if st.button("Update Configuration"):
        conn = get_db_connection()
        conn.execute("DELETE FROM strategy_config")
        for i, r in edited_conf.iterrows():
            conn.execute("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", tuple(r))
        conn.commit()
        conn.close()
        st.success("Config Saved")
        st.rerun()
    
    st.divider()
    if st.button("🗑️ Hard Reset Database (Danger)", type="primary"):
        if os.path.exists(DB_NAME): os.remove(DB_NAME)
        st.warning("Database Wiped.")
        time.sleep(1)
        st.rerun()

st.markdown("---")
st.caption("Allantis Trade Guardian v150.0 | Institutional Grade")
