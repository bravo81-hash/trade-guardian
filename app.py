"""
Allantis Trade Guardian - ELITE EDITION
Complete preservation of all features + superior architecture and UI
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import sqlite3
import io
import os
import re
import json
import time
from datetime import datetime, timezone, timedelta
from scipy import stats
from scipy.spatial.distance import cdist

# Excel/Google Drive imports
try:
    from openpyxl import load_workbook
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False

# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="Trade Guardian Elite",
    layout="wide",
    page_icon="🛡️",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CONSTANTS
# ============================================================================

DB_NAME = "trade_guardian_v4.db"
SCOPES = ['https://www.googleapis.com/auth/drive']

COLORS = {
    'primary': '#6366f1',
    'success': '#10b981',
    'warning': '#f59e0b',
    'danger': '#ef4444',
    'info': '#3b82f6',
    'purple': '#a855f7',
    'teal': '#14b8a6',
}

DEFAULT_STRATEGIES = {
    '130/160': {'pnl': 500, 'dit': 36, 'stability': 0.8, 'debit': 4000},
    '160/190': {'pnl': 700, 'dit': 44, 'stability': 0.8, 'debit': 5200},
    'M200': {'pnl': 900, 'dit': 41, 'stability': 0.8, 'debit': 8000},
    'SMSF': {'pnl': 600, 'dit': 40, 'stability': 0.8, 'debit': 5000}
}

# ============================================================================
# ENHANCED STYLING
# ============================================================================

def inject_css():
    """Modern, professional CSS with all original features"""
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Fira+Code:wght@400;500;600&display=swap');
        
        * { font-family: 'Inter', sans-serif; }
        
        .stApp {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: #f8fafc;
        }
        
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #020617 0%, #0f172a 100%);
            border-right: 1px solid rgba(99, 102, 241, 0.2);
        }
        
        h1, h2, h3, h4, h5, h6 {
            font-weight: 700 !important;
            background: linear-gradient(135deg, #6366f1 0%, #a855f7 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        /* Glassmorphism Cards */
        div[data-testid="stMetric"] {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(20px);
            border: 1px solid rgba(99, 102, 241, 0.2);
            border-radius: 16px;
            padding: 20px;
            transition: all 0.3s ease;
        }
        
        div[data-testid="stMetric"]:hover {
            transform: translateY(-4px);
            border-color: rgba(99, 102, 241, 0.4);
            box-shadow: 0 12px 48px rgba(99, 102, 241, 0.2);
        }
        
        [data-testid="stMetricLabel"] {
            color: #94a3b8 !important;
            font-size: 0.875rem !important;
            font-weight: 600 !important;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        [data-testid="stMetricValue"] {
            color: #f8fafc !important;
            font-family: 'Fira Code', monospace !important;
            font-size: 2rem !important;
            font-weight: 700 !important;
        }
        
        /* Buttons */
        .stButton button {
            background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
            color: white;
            border: none;
            border-radius: 12px;
            padding: 10px 20px;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        
        .stButton button:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 24px rgba(99, 102, 241, 0.4);
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            background: rgba(30, 41, 59, 0.4);
            border-radius: 12px;
            padding: 4px;
        }
        
        .stTabs [data-baseweb="tab"] {
            background: transparent;
            border-radius: 8px;
            color: #94a3b8;
            font-weight: 600;
            padding: 12px 24px;
        }
        
        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%) !important;
            color: white !important;
        }
        
        /* Expanders */
        div[data-testid="stExpander"] {
            background: rgba(30, 41, 59, 0.4);
            border: 1px solid rgba(99, 102, 241, 0.2);
            border-radius: 12px;
        }
        
        /* Data Editor/DataFrame */
        [data-testid="stDataFrame"], .stDataFrame {
            border-radius: 12px;
            border: 1px solid rgba(99, 102, 241, 0.2);
        }
        
        /* Custom badges */
        .status-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }
        
        .status-healthy {
            background: rgba(16, 185, 129, 0.2);
            color: #10b981;
            border: 1px solid rgba(16, 185, 129, 0.3);
        }
        
        .status-warning {
            background: rgba(245, 158, 11, 0.2);
            color: #f59e0b;
            border: 1px solid rgba(245, 158, 11, 0.3);
        }
        
        .status-danger {
            background: rgba(239, 68, 68, 0.2);
            color: #ef4444;
            border: 1px solid rgba(239, 68, 68, 0.3);
        }
        </style>
    """, unsafe_allow_html=True)

def apply_chart_theme(fig):
    """Apply consistent theme to charts"""
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'family': 'Inter', 'color': '#cbd5e1', 'size': 12},
        title_font={'size': 18, 'color': '#f8fafc'},
        xaxis={'showgrid': True, 'gridcolor': 'rgba(99, 102, 241, 0.1)', 'color': '#94a3b8'},
        yaxis={'showgrid': True, 'gridcolor': 'rgba(99, 102, 241, 0.1)', 'color': '#94a3b8'},
        colorway=['#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#14b8a6'],
        hoverlabel={'bgcolor': '#1e293b', 'font': {'family': 'Fira Code', 'color': '#f8fafc'}}
    )
    return fig

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    """Initialize database with all tables"""
    if not os.path.exists(DB_NAME):
        # Try to download from cloud first
        if 'drive_mgr' in globals() and drive_mgr.is_connected:
            success, msg = drive_mgr.download_db()
            if success:
                st.toast(f"☁️ {msg}")
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # Trades table
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
    
    # Snapshots table
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
    
    # Strategy config table
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (
        name TEXT PRIMARY KEY,
        identifier TEXT,
        target_pnl REAL,
        target_days INTEGER,
        min_stability REAL,
        description TEXT,
        typical_debit REAL
    )''')
    
    # Indexes
    c.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_strategy ON trades(strategy)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_date ON snapshots(snapshot_date)")
    
    conn.commit()
    conn.close()
    
    seed_default_strategies()

def seed_default_strategies(force_reset=False):
    """Seed default strategy configurations"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        if force_reset:
            c.execute("DELETE FROM strategy_config")
        
        c.execute("SELECT COUNT(*) FROM strategy_config")
        if c.fetchone()[0] == 0:
            defaults = [
                ('130/160', '130/160', 500, 36, 0.8, 'Income Discipline', 4000),
                ('160/190', '160/190', 700, 44, 0.8, 'Patience Training', 5200),
                ('M200', 'M200', 900, 41, 0.8, 'Emotional Mastery', 8000),
                ('SMSF', 'SMSF', 600, 40, 0.8, 'Wealth Builder', 5000)
            ]
            c.executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)
            conn.commit()
            if force_reset:
                st.toast("Strategies reset to defaults")
    except Exception as e:
        print(f"Seed error: {e}")
    finally:
        conn.close()

@st.cache_data(ttl=60)
def load_strategy_config():
    """Load strategy configurations"""
    if not os.path.exists(DB_NAME):
        return {}
    
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM strategy_config", conn)
        config = {}
        for _, row in df.iterrows():
            config[row['name']] = {
                'id': row['identifier'],
                'pnl': row['target_pnl'],
                'dit': row['target_days'],
                'stability': row['min_stability'],
                'debit_per_lot': row.get('typical_debit', 5000),
                'description': row.get('description', '')
            }
        return config
    except:
        return {}
    finally:
        conn.close()

@st.cache_data(ttl=60)
def load_data():
    """Load all trades with calculated metrics"""
    if not os.path.exists(DB_NAME):
        return pd.DataFrame()
    
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        
        if df.empty:
            return pd.DataFrame()
        
        # Rename columns for display
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date',
            'notes': 'Notes', 'tags': 'Tags', 'parent_id': 'Parent ID',
            'put_pnl': 'Put P&L', 'call_pnl': 'Call P&L', 'iv': 'IV', 'link': 'Link'
        })
        
        # Ensure required columns exist
        for col in ['Gamma', 'Vega', 'Theta', 'Delta', 'P&L', 'Debit', 'lot_size', 'Notes', 'Tags', 'Parent ID', 'Put P&L', 'Call P&L', 'IV', 'Link']:
            if col not in df.columns:
                df[col] = "" if col in ['Notes', 'Tags', 'Parent ID', 'Link'] else 0.0
        
        # Convert numeric columns
        numeric_cols = ['Debit', 'P&L', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'IV', 'Put P&L', 'Call P&L']
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
        
        # Convert dates
        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        
        # Fix lot size
        df['lot_size'] = pd.to_numeric(df['lot_size'], errors='coerce').fillna(1).astype(int)
        df['lot_size'] = df['lot_size'].apply(lambda x: max(1, x))
        
        # Calculate metrics
        df['Debit/Lot'] = np.where(df['lot_size'] > 0, df['Debit'] / df['lot_size'], df['Debit'])
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1)) * 100
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = np.where(df['Debit'] > 0, (df['Theta'] / df['Debit']) * 100, 0)
        df['Ticker'] = df['Name'].str.split().str[0].str.replace('.', '').str.upper()
        df['Stability'] = df['Theta'] / (df['Delta'].abs() + 1)
        
        # Clean Parent ID
        df['Parent ID'] = df['Parent ID'].astype(str).str.strip().replace('nan', '').replace('None', '')
        
        # Add P&L volatility from snapshots
        snaps = pd.read_sql("SELECT trade_id, pnl FROM snapshots", conn)
        if not snaps.empty:
            vol_df = snaps.groupby('trade_id')['pnl'].std().reset_index()
            vol_df.columns = ['id', 'P&L Vol']
            df = df.merge(vol_df, on='id', how='left')
            df['P&L Vol'] = df['P&L Vol'].fillna(0)
        else:
            df['P&L Vol'] = 0.0
        
        # Add grades
        def get_grade(row):
            s, d = row['Strategy'], row['Debit/Lot']
            if s == '130/160':
                if d > 4800: return "F", "Overpriced"
                elif 3500 <= d <= 4500: return "A+", "Sweet Spot"
                else: return "B", "Acceptable"
            elif s == '160/190':
                if 4800 <= d <= 5500: return "A", "Ideal"
                else: return "C", "Check Pricing"
            elif s == 'M200':
                if 7500 <= d <= 8500: return "A", "Perfect"
                else: return "B", "Variance"
            elif s == 'SMSF':
                if d > 15000: return "B", "High Debit"
                else: return "A", "Standard"
            return "C", "Standard"
        
        df[['Grade', 'Reason']] = df.apply(lambda r: pd.Series(get_grade(r)), axis=1)
        
        return df
        
    finally:
        conn.close()

@st.cache_data(ttl=300)
def load_snapshots():
    """Load snapshot data"""
    if not os.path.exists(DB_NAME):
        return pd.DataFrame()
    
    conn = get_db_connection()
    try:
        query = """
            SELECT s.*, t.strategy, t.name, t.id as trade_id, t.theta as initial_theta
            FROM snapshots s
            JOIN trades t ON s.trade_id = t.id
        """
        df = pd.read_sql(query, conn)
        
        if not df.empty:
            df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
            numeric_cols = ['pnl', 'days_held', 'theta', 'delta', 'vega', 'gamma', 'initial_theta']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
    except:
        return pd.DataFrame()
    finally:
        conn.close()

# ============================================================================
# CLOUD SYNC MANAGER
# ============================================================================

class DriveManager:
    """Google Drive sync manager"""
    
    def __init__(self):
        self.service = None
        self.is_connected = False
        self.cached_file_id = None
        
        if GOOGLE_AVAILABLE and 'gcp_service_account' in st.secrets:
            try:
                creds = service_account.Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"],
                    scopes=SCOPES
                )
                self.service = build('drive', 'v3', credentials=creds)
                self.is_connected = True
            except Exception as e:
                st.error(f"Cloud connection error: {e}")
    
    def find_db_file(self):
        """Find database file in Drive"""
        if not self.is_connected:
            return None, None
        
        if self.cached_file_id:
            try:
                file = self.service.files().get(
                    fileId=self.cached_file_id,
                    fields='id,name'
                ).execute()
                return file['id'], file['name']
            except:
                self.cached_file_id = None
        
        try:
            # Exact match
            query = f"name='{DB_NAME}' and trashed=false"
            results = self.service.files().list(
                q=query, pageSize=1, fields="files(id, name)"
            ).execute()
            
            items = results.get('files', [])
            if items:
                self.cached_file_id = items[0]['id']
                return items[0]['id'], items[0]['name']
            
            # Fuzzy match
            query = "name contains 'trade_guardian' and name contains '.db' and trashed=false"
            results = self.service.files().list(
                q=query, pageSize=5, fields="files(id, name)"
            ).execute()
            
            items = results.get('files', [])
            if items:
                for item in items:
                    if item['name'].startswith('trade_guardian_v4'):
                        self.cached_file_id = item['id']
                        return item['id'], item['name']
                
                self.cached_file_id = items[0]['id']
                return items[0]['id'], items[0]['name']
            
            return None, None
            
        except Exception as e:
            st.error(f"Search error: {e}")
            return None, None
    
    def get_cloud_modified_time(self, file_id):
        """Get file modification time"""
        try:
            file = self.service.files().get(
                fileId=file_id, fields='modifiedTime'
            ).execute()
            dt = datetime.strptime(
                file['modifiedTime'].replace('Z', '+0000'),
                '%Y-%m-%dT%H:%M:%S.%f%z'
            )
            return dt
        except:
            return None
    
    def download_db(self, force=False):
        """Download database from cloud"""
        file_id, file_name = self.find_db_file()
        if not file_id:
            return False, "Database not found in cloud"
        
        # Conflict check
        if os.path.exists(DB_NAME) and not force:
            try:
                local_ts = os.path.getmtime(DB_NAME)
                local_mod = datetime.fromtimestamp(local_ts, tz=timezone.utc)
                cloud_time = self.get_cloud_modified_time(file_id)
                
                if cloud_time and (local_mod > cloud_time + timedelta(minutes=2)):
                    return False, f"CONFLICT: Local file is newer ({local_mod.strftime('%H:%M')})"
            except:
                pass
        
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            with open(DB_NAME, "wb") as f:
                f.write(fh.getbuffer())
            
            st.session_state['last_cloud_sync'] = datetime.now()
            return True, f"Downloaded '{file_name}'"
            
        except Exception as e:
            return False, f"Download error: {str(e)}"
    
    def upload_db(self, force=False):
        """Upload database to cloud"""
        if not os.path.exists(DB_NAME):
            return False, "No local database found"
        
        file_id, file_name = self.find_db_file()
        
        # Conflict check
        if file_id and not force:
            cloud_time = self.get_cloud_modified_time(file_id)
            local_ts = os.path.getmtime(DB_NAME)
            local_time = datetime.fromtimestamp(local_ts, tz=timezone.utc)
            
            if cloud_time and (cloud_time > local_time + timedelta(seconds=2)):
                return False, f"CONFLICT: Cloud file is newer ({cloud_time.strftime('%H:%M')})"
        
        # Integrity check
        try:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            conn.close()
            
            if result[0] != "ok":
                return False, "❌ Database corrupt"
        except Exception as e:
            return False, f"❌ Integrity check failed: {e}"
        
        try:
            media = MediaFileUpload(DB_NAME, mimetype='application/x-sqlite3', resumable=True)
            
            if file_id:
                self.service.files().update(
                    fileId=file_id,
                    media_body=media
                ).execute()
                action = f"Updated '{file_name}'"
            else:
                file_metadata = {'name': DB_NAME}
                self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                action = "Created new file"
            
            st.session_state['last_cloud_sync'] = datetime.now()
            return True, f"✅ {action}"
            
        except Exception as e:
            return False, f"Upload error: {str(e)}"

# ============================================================================
# FILE PARSER (PRESERVING ALL ORIGINAL FUNCTIONALITY)
# ============================================================================

def clean_num(x):
    """Clean numeric values"""
    try:
        if pd.isna(x) or str(x).strip() == "":
            return 0.0
        val_str = str(x).replace('$', '').replace(',', '').replace('%', '').strip()
        val = float(val_str)
        return 0.0 if np.isnan(val) else val
    except:
        return 0.0

def get_strategy_dynamic(trade_name, group_name, config_dict):
    """Determine strategy from name/group"""
    t_name = str(trade_name).upper().strip()
    g_name = str(group_name).upper().strip()
    
    sorted_strats = sorted(
        config_dict.items(),
        key=lambda x: len(str(x[1]['id'])),
        reverse=True
    )
    
    for strat_name, details in sorted_strats:
        key = str(details['id']).upper()
        if key in t_name:
            return strat_name
    
    for strat_name, details in sorted_strats:
        key = str(details['id']).upper()
        if key in g_name:
            return strat_name
    
    return "Other"

def generate_id(name, strategy, entry_date):
    """Generate unique trade ID"""
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    safe_name = re.sub(r'\W+', '', str(name))
    return f"{safe_name}_{strategy}_{d_str}"

def parse_optionstrat_file(file, file_type, config_dict):
    """Parse OptionStrat export file (COMPLETE ORIGINAL LOGIC)"""
    try:
        df_raw = None
        
        # Try Excel first
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
                
                # Extract hyperlinks
                if EXCEL_AVAILABLE and 'Link' in df_raw.columns:
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
                                
                                if cell.hyperlink:
                                    url = cell.hyperlink.target
                                elif cell.value and str(cell.value).startswith('=HYPERLINK'):
                                    try:
                                        parts = str(cell.value).split('"')
                                        if len(parts) > 1:
                                            url = parts[1]
                                    except:
                                        pass
                                
                                links.append(url if url else "")
                            
                            df_raw['Link'] = links
                    except:
                        pass
                
            except:
                pass
        
        # Try CSV if Excel failed
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
        
        # Parse trades
        parsed_trades = []
        current_trade = None
        current_legs = []
        
        def finalize_trade(trade_data, legs, f_type):
            if trade_data is None or trade_data.empty:
                return None
            
            name = str(trade_data.get('Name', ''))
            group = str(trade_data.get('Group', ''))
            created = trade_data.get('Created At', '')
            
            try:
                start_dt = pd.to_datetime(created)
            except:
                return None
            
            strat = get_strategy_dynamic(name, group, config_dict)
            
            link = str(trade_data.get('Link', ''))
            if link == 'nan' or link == 'Open':
                link = ""
            
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
            except:
                pass
            
            days_held = 1
            if exit_dt and f_type == "History":
                days_held = (exit_dt - start_dt).days
            else:
                days_held = (datetime.now() - start_dt).days
            
            days_held = max(1, days_held)
            
            strat_config = config_dict.get(strat, {})
            typical_debit = strat_config.get('debit_per_lot', 5000)
            
            lot_match = re.search(r'(\d+)\s*(?:LOT|L\b)', name, re.IGNORECASE)
            if lot_match:
                lot_size = int(lot_match.group(1))
            else:
                lot_size = int(round(debit / typical_debit))
            
            if lot_size < 1:
                lot_size = 1
            
            put_pnl = 0.0
            call_pnl = 0.0
            
            if f_type == "History":
                for leg in legs:
                    if len(leg) < 5:
                        continue
                    sym = str(leg.iloc[0])
                    if not sym.startswith('.'):
                        continue
                    try:
                        qty = clean_num(leg.iloc[1])
                        entry = clean_num(leg.iloc[2])
                        close_price = clean_num(leg.iloc[4])
                        leg_pnl = (close_price - entry) * qty * 100
                        
                        if 'P' in sym and 'C' not in sym:
                            put_pnl += leg_pnl
                        elif 'C' in sym and 'P' not in sym:
                            call_pnl += leg_pnl
                    except:
                        pass
            
            t_id = generate_id(name, strat, start_dt)
            
            return {
                'id': t_id,
                'name': name,
                'strategy': strat,
                'start_dt': start_dt,
                'exit_dt': exit_dt,
                'days_held': days_held,
                'debit': debit,
                'lot_size': lot_size,
                'pnl': pnl,
                'theta': theta,
                'delta': delta,
                'gamma': gamma,
                'vega': vega,
                'iv': iv,
                'put_pnl': put_pnl,
                'call_pnl': call_pnl,
                'link': link,
                'group': group
            }
        
        # Process rows
        for index, row in df_raw.iterrows():
            name_val = str(row['Name'])
            
            if name_val and not name_val.startswith('.') and name_val != 'Symbol' and name_val != 'nan':
                if current_trade is not None:
                    res = finalize_trade(current_trade, current_legs, file_type)
                    if res:
                        parsed_trades.append(res)
                
                current_trade = row
                current_legs = []
            elif name_val.startswith('.'):
                current_legs.append(row)
        
        if current_trade is not None:
            res = finalize_trade(current_trade, current_legs, file_type)
            if res:
                parsed_trades.append(res)
        
        return parsed_trades
        
    except Exception as e:
        print(f"Parser error: {e}")
        return []

def sync_data(file_list, file_type):
    """Sync uploaded files to database (COMPLETE ORIGINAL LOGIC)"""
    log = []
    if not isinstance(file_list, list):
        file_list = [file_list]
    
    conn = get_db_connection()
    c = conn.cursor()
    
    db_active_ids = set()
    if file_type == "Active":
        try:
            current_active = pd.read_sql("SELECT id FROM trades WHERE status = 'Active'", conn)
            db_active_ids = set(current_active['id'].tolist())
        except:
            pass
    
    file_found_ids = set()
    config_dict = load_strategy_config()
    
    for file in file_list:
        count_new = 0
        count_update = 0
        
        try:
            trades_data = parse_optionstrat_file(file, file_type, config_dict)
            
            if not trades_data:
                log.append(f"⚠️ {file.name}: Skipped (No valid trades)")
                continue
            
            for t in trades_data:
                trade_id = t['id']
                
                if file_type == "Active":
                    file_found_ids.add(trade_id)
                
                c.execute("SELECT id, status FROM trades WHERE id = ?", (trade_id,))
                existing = c.fetchone()
                
                # Handle renames via link matching
                if existing is None and t['link'] and len(t['link']) > 15:
                    c.execute("SELECT id, name FROM trades WHERE link = ?", (t['link'],))
                    link_match = c.fetchone()
                    
                    if link_match:
                        old_id, old_name = link_match
                        try:
                            c.execute("UPDATE snapshots SET trade_id = ? WHERE trade_id = ?", (trade_id, old_id))
                            c.execute("UPDATE trades SET id=?, name=? WHERE id=?", (trade_id, t['name'], old_id))
                            log.append(f"🔄 Renamed: '{old_name}' → '{t['name']}'")
                            
                            c.execute("SELECT id, status FROM trades WHERE id = ?", (trade_id,))
                            existing = c.fetchone()
                            
                            if file_type == "Active":
                                file_found_ids.add(trade_id)
                                if old_id in db_active_ids:
                                    db_active_ids.remove(old_id)
                                db_active_ids.add(trade_id)
                        except Exception as rename_err:
                            print(f"Rename error: {rename_err}")
                
                status = "Active" if file_type == "Active" else "Expired"
                
                if existing is None:
                    # Insert new trade
                    c.execute('''INSERT INTO trades 
                        (id, name, strategy, status, entry_date, exit_date, days_held, debit, 
                         lot_size, pnl, theta, delta, gamma, vega, notes, tags, parent_id, 
                         put_pnl, call_pnl, iv, link, original_group)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, t['name'], t['strategy'], status, t['start_dt'].date(),
                         t['exit_dt'].date() if t['exit_dt'] else None,
                         t['days_held'], t['debit'], t['lot_size'], t['pnl'],
                         t['theta'], t['delta'], t['gamma'], t['vega'], "", "", "",
                         t['put_pnl'], t['call_pnl'], t['iv'], t['link'], t['group']))
                    count_new += 1
                else:
                    # Update existing trade
                    if file_type == "History":
                        c.execute('''UPDATE trades SET 
                            pnl=?, status=?, exit_date=?, days_held=?, theta=?, delta=?, 
                            gamma=?, vega=?, put_pnl=?, call_pnl=?, iv=?, link=?, original_group=?
                            WHERE id=?''',
                            (t['pnl'], status, t['exit_dt'].date() if t['exit_dt'] else None,
                             t['days_held'], t['theta'], t['delta'], t['gamma'], t['vega'],
                             t['put_pnl'], t['call_pnl'], t['iv'], t['link'], t['group'], trade_id))
                        count_update += 1
                    elif existing[1] in ["Active", "Missing"]:
                        c.execute('''UPDATE trades SET 
                            pnl=?, days_held=?, theta=?, delta=?, gamma=?, vega=?, iv=?, 
                            link=?, status='Active', exit_date=?, original_group=?
                            WHERE id=?''',
                            (t['pnl'], t['days_held'], t['theta'], t['delta'], t['gamma'],
                             t['vega'], t['iv'], t['link'],
                             t['exit_dt'].date() if t['exit_dt'] else None, t['group'], trade_id))
                        count_update += 1
                
                # Create snapshots for active trades
                if file_type == "Active":
                    today = datetime.now().date()
                    c.execute("SELECT id FROM snapshots WHERE trade_id=? AND snapshot_date=?", (trade_id, today))
                    
                    if not c.fetchone():
                        c.execute("""INSERT INTO snapshots 
                            (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) 
                            VALUES (?,?,?,?,?,?,?,?)""",
                            (trade_id, today, t['pnl'], t['days_held'],
                             t['theta'], t['delta'], t['vega'], t['gamma']))
                    else:
                        c.execute("""UPDATE snapshots SET 
                            pnl=?, days_held=?, theta=?, delta=?, vega=?, gamma=? 
                            WHERE trade_id=? AND snapshot_date=?""",
                            (t['pnl'], t['days_held'], t['theta'], t['delta'],
                             t['vega'], t['gamma'], trade_id, today))
            
            log.append(f"✅ {file.name}: {count_new} new, {count_update} updated")
            
        except Exception as e:
            log.append(f"❌ {file.name}: {str(e)}")
    
    # Mark missing trades
    if file_type == "Active" and file_found_ids:
        missing_ids = db_active_ids - file_found_ids
        if missing_ids:
            placeholders = ','.join('?' for _ in missing_ids)
            c.execute(f"UPDATE trades SET status = 'Missing' WHERE id IN ({placeholders})", list(missing_ids))
            log.append(f"⚠️ Marked {len(missing_ids)} trades as 'Missing'")
    
    conn.commit()
    conn.close()
    
    return log

# ============================================================================
# ANALYTICS FUNCTIONS (ALL ORIGINAL)
# ============================================================================

def calculate_kelly_fraction(win_rate, avg_win, avg_loss):
    """Kelly Criterion for position sizing"""
    if avg_loss == 0 or avg_win <= 0:
        return 0.0
    b = abs(avg_win / avg_loss)
    kelly = (win_rate * b - (1 - win_rate)) / b
    return max(0, min(kelly * 0.5, 0.25))

def generate_trade_predictions(active_df, history_df, prob_low, prob_high, total_capital=100000):
    """KNN-based trade predictions"""
    if active_df.empty or history_df.empty:
        return pd.DataFrame()
    
    features = ['Theta/Cap %', 'Delta', 'Debit/Lot']
    train_df = history_df.dropna(subset=features).copy()
    
    if len(train_df) < 5:
        return pd.DataFrame()
    
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
        
        if pd.isna(avg_win):
            avg_win = 0
        if pd.isna(avg_loss):
            avg_loss = -avg_pnl * 0.5
        
        kelly_size = calculate_kelly_fraction(win_prob, avg_win, avg_loss)
        rec_dollars = kelly_size * total_capital
        
        avg_dist = distances[top_k_idx].mean()
        confidence = max(0, 100 - (avg_dist * 10))
        
        rec = "HOLD"
        if win_prob * 100 < prob_low:
            rec = "REDUCE/CLOSE"
        elif win_prob * 100 > prob_high:
            rec = "PRESS WINNER"
        
        predictions.append({
            'Trade Name': row['Name'],
            'Strategy': row['Strategy'],
            'Win Prob %': win_prob * 100,
            'Expected PnL': avg_pnl,
            'Kelly Size %': kelly_size * 100,
            'Rec. Size ($)': rec_dollars,
            'AI Rec': rec,
            'Confidence': confidence
        })
    
    return pd.DataFrame(predictions)

def calculate_portfolio_metrics(trades_df, capital):
    """Calculate Sharpe, CAGR, etc."""
    if trades_df.empty or capital <= 0:
        return 0.0, 0.0, 0.0
    
    # Reconstruct daily P&L
    daily_pnl_dict = {}
    
    for _, trade in trades_df.iterrows():
        if pd.isnull(trade['Exit Date']):
            continue
        
        days = trade['Days Held']
        if days <= 0:
            days = 1
        
        total_pnl = trade['P&L']
        pnl_per_day = total_pnl / days
        
        current = trade['Entry Date']
        for _ in range(days):
            day_key = current.date()
            if day_key not in daily_pnl_dict:
                daily_pnl_dict[day_key] = 0.0
            daily_pnl_dict[day_key] += pnl_per_day
            current += timedelta(days=1)
    
    if not daily_pnl_dict:
        return 0.0, 0.0, 0.0
    
    # Calculate equity curve
    dates = sorted(daily_pnl_dict.keys())
    equity = capital
    equity_values = []
    
    for date in dates:
        equity += daily_pnl_dict[date]
        equity_values.append(equity)
    
    # Sharpe ratio
    equity_series = pd.Series(equity_values)
    daily_returns = equity_series.pct_change().dropna()
    
    if daily_returns.std() == 0:
        sharpe = 0.0
    else:
        sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
    
    # CAGR
    total_days = (dates[-1] - dates[0]).days
    if total_days < 1:
        total_days = 1
    
    total_pnl = trades_df['P&L'].sum()
    end_val = capital + total_pnl
    
    try:
        cagr = ((end_val / capital) ** (365 / total_days) - 1) * 100
    except:
        cagr = 0.0
    
    # Max Drawdown
    running_max = equity_series.cummax()
    drawdown = (equity_series - running_max) / running_max
    max_dd = abs(drawdown.min() * 100) if len(drawdown) > 0 else 0.0
    
    return sharpe, cagr, max_dd

def calculate_decision_ladder(row, benchmarks_dict, regime_mult=1.0):
    """Decision engine for active trades"""
    strat = row['Strategy']
    days = row['Days Held']
    pnl = row['P&L']
    status = row['Status']
    theta = row['Theta']
    debit = row['Debit']
    lot_size = row.get('lot_size', 1)
    
    if lot_size < 1:
        lot_size = 1
    
    if status == 'Missing':
        return "REVIEW", 100, "Missing from data", 0, "Error"
    
    bench = benchmarks_dict.get(strat, {})
    hist_avg_pnl = bench.get('pnl', 1000)
    target_profit = (hist_avg_pnl * regime_mult) * lot_size
    hist_avg_days = bench.get('dit', 40)
    
    score = 50
    action = "HOLD"
    reason = "Normal"
    juice_val = 0.0
    juice_type = "Neutral"
    
    # Negative P&L logic
    if pnl < 0:
        juice_type = "Recovery Days"
        if theta > 0:
            recov_days = abs(pnl) / theta
            juice_val = recov_days
            
            remaining_time = max(1, hist_avg_days - days)
            if recov_days > remaining_time and days > 15:
                score += 40
                action = "STRUCTURAL FAILURE"
                reason = f"Zombie (Recov {recov_days:.0f}d > Left {remaining_time:.0f}d)"
        else:
            juice_val = 999
            if days > 15:
                score += 30
                reason = "Negative Theta"
    else:
        # Positive P&L logic
        juice_type = "Left in Tank"
        left_in_tank = max(0, target_profit - pnl)
        juice_val = left_in_tank
        
        if debit > 0 and (left_in_tank / debit) < 0.05:
            score += 40
            reason = "Squeezed Dry"
        elif left_in_tank < (100 * lot_size):
            score += 35
            reason = f"Empty Tank (<${100*lot_size})"
    
    # Target hit
    if pnl >= target_profit:
        return "TAKE PROFIT", 100, f"Hit Target ${target_profit:.0f}", juice_val, juice_type
    elif pnl >= target_profit * 0.8:
        score += 30
        action = "PREPARE EXIT"
        reason = "Near Target"
    
    # Staleness check
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
    
    score = min(100, max(0, score))
    
    if score >= 90:
        action = "CRITICAL"
    elif score >= 70:
        action = "WATCH"
    elif score <= 30:
        action = "COOKING"
    
    return action, score, reason, juice_val, juice_type

# ============================================================================
# VISUALIZATION COMPONENTS
# ============================================================================

def create_portfolio_radar(active_df, benchmarks):
    """Portfolio health radar chart"""
    if active_df.empty:
        fig = go.Figure()
        fig.update_layout(title="Portfolio Radar (No Data)")
        return apply_chart_theme(fig)
    
    total_debit = active_df['Debit'].sum()
    if total_debit == 0:
        total_debit = 1
    
    # Calculate scores
    stability_score = min(100, (active_df['Stability'].mean() / 1.5) * 100)
    yield_score = min(100, (active_df['Theta'].sum() / total_debit * 100) * 500)
    hedge_score = min(100, abs(active_df['Vega'].sum() / (active_df['Theta'].sum() or 1)) * 20)
    freshness_score = max(0, 100 - (active_df['Days Held'].mean() / 45 * 100))
    neutrality_score = max(0, 100 - (abs(active_df['Delta'].sum() / total_debit * 100) * 20))
    
    target_allocation = {'130/160': 0.30, '160/190': 0.40, 'M200': 0.20, 'SMSF': 0.10}
    actual_alloc = active_df.groupby('Strategy')['Debit'].sum() / total_debit
    div_score = 100 - sum(abs(actual_alloc.get(s, 0) - target_allocation.get(s, 0)) * 100 for s in target_allocation)
    
    categories = ['Stability', 'Yield', 'Hedge', 'Freshness', 'Neutrality', 'Diversification']
    values = [stability_score, yield_score, hedge_score, freshness_score, neutrality_score, div_score]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name='Current',
        fillcolor='rgba(99, 102, 241, 0.3)',
        line={'color': COLORS['primary'], 'width': 2}
    ))
    
    fig.add_trace(go.Scatterpolar(
        r=[80] * len(categories),
        theta=categories,
        mode='lines',
        name='Target',
        line={'color': COLORS['success'], 'width': 1, 'dash': 'dash'}
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100]),
            angularaxis=dict(gridcolor='rgba(99, 102, 241, 0.1)')
        ),
        showlegend=True,
        title="Portfolio Health Radar",
        height=400
    )
    
    return apply_chart_theme(fig)

def create_heatmap(active_df):
    """Position heat map"""
    if active_df.empty:
        fig = go.Figure()
        fig.update_layout(title="Position Heat Map")
        return apply_chart_theme(fig)
    
    fig = px.scatter(
        active_df,
        x='Days Held',
        y='P&L',
        size='Debit',
        color='Urgency Score',
        hover_data=['Name', 'Strategy'],
        color_continuous_scale='RdYlGn_r',
        title="Position Heat Map"
    )
    
    avg_days = active_df['Days Held'].mean()
    fig.add_vline(x=avg_days, line_dash="dash", opacity=0.5, annotation_text=f"Avg: {avg_days:.0f}d")
    fig.add_hline(y=0, line_dash="dash", opacity=0.5)
    
    fig.update_layout(height=500)
    
    return apply_chart_theme(fig)

# ============================================================================
# MAIN APP
# ============================================================================

def main():
    # Inject CSS
    inject_css()
    
    # Initialize database
    init_db()
    
    # Initialize Drive Manager
    global drive_mgr
    drive_mgr = DriveManager()
    
    # Header
    st.markdown("""
        <div style="margin-bottom: 30px;">
            <div style="display: inline-block; padding: 6px 16px; background: rgba(99, 102, 241, 0.2); 
                        border: 1px solid rgba(99, 102, 241, 0.3); border-radius: 24px; margin-bottom: 12px;">
                <span style="color: #a5b4fc; font-size: 0.75rem; font-weight: 700; letter-spacing: 0.1em;">
                    INSTITUTIONAL GRADE
                </span>
            </div>
            <h1 style="font-size: 3rem; margin: 0;">
                Allantis Trade Guardian <span style="background: linear-gradient(135deg, #6366f1 0%, #a855f7 100%); 
                -webkit-background-clip: text; -webkit-text-fill-color: transparent;">ELITE</span>
            </h1>
            <p style="color: #94a3b8; margin-top: 8px;">All Features Preserved + Superior Architecture</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### ⚡ Quick Actions")
        
        # Cloud Sync
        if GOOGLE_AVAILABLE and drive_mgr.is_connected:
            with st.expander("☁️ Cloud Sync", expanded=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📤 Push", use_container_width=True):
                        with st.spinner("Uploading..."):
                            success, msg = drive_mgr.upload_db()
                            if success:
                                st.success(msg)
                                st.rerun()
                            else:
                                if "CONFLICT" in msg:
                                    st.error(msg)
                                    if st.button("⚠️ Force Push"):
                                        success, msg = drive_mgr.upload_db(force=True)
                                        if success:
                                            st.success(msg)
                                            st.rerun()
                                else:
                                    st.error(msg)
                
                with col2:
                    if st.button("📥 Pull", use_container_width=True):
                        with st.spinner("Downloading..."):
                            success, msg = drive_mgr.download_db()
                            if success:
                                st.cache_data.clear()
                                st.success(msg)
                                st.rerun()
                            else:
                                if "CONFLICT" in msg:
                                    st.error(msg)
                                    if st.button("⚠️ Force Pull"):
                                        success, msg = drive_mgr.download_db(force=True)
                                        if success:
                                            st.cache_data.clear()
                                            st.success(msg)
                                            st.rerun()
                                else:
                                    st.error(msg)
                
                # Sync status
                last_sync = st.session_state.get('last_cloud_sync')
                if last_sync:
                    mins_ago = (datetime.now() - last_sync).total_seconds() / 60
                    if mins_ago < 1:
                        st.success("✅ Synced just now")
                    elif mins_ago < 60:
                        st.info(f"Last sync: {int(mins_ago)}m ago")
                    else:
                        st.warning(f"Last sync: {last_sync.strftime('%H:%M')}")
        
        st.markdown("---")
        
        # File Upload
        with st.expander("📂 Import Trades", expanded=True):
            active_files = st.file_uploader(
                "Active Trades",
                accept_multiple_files=True,
                type=['xlsx', 'xls', 'csv'],
                key="active"
            )
            
            history_files = st.file_uploader(
                "Closed Trades",
                accept_multiple_files=True,
                type=['xlsx', 'xls', 'csv'],
                key="history"
            )
            
            if st.button("🔄 Sync All", use_container_width=True):
                logs = []
                if active_files:
                    logs.extend(sync_data(active_files, "Active"))
                if history_files:
                    logs.extend(sync_data(history_files, "History"))
                
                if logs:
                    for l in logs:
                        st.write(l)
                    st.cache_data.clear()
                    st.success("Sync complete!")
                    
                    # Auto cloud sync
                    if drive_mgr.is_connected:
                        with st.spinner("☁️ Syncing to cloud..."):
                            success, msg = drive_mgr.upload_db()
                            if success:
                                st.toast(f"✅ {msg}")
        
        st.markdown("---")
        
        # Settings
        with st.expander("⚙️ Settings"):
            prime_cap = st.number_input("Prime Account ($)", min_value=1000, value=115000, step=1000)
            smsf_cap = st.number_input("SMSF Account ($)", min_value=1000, value=150000, step=1000)
            total_cap = prime_cap + smsf_cap
            
            st.metric("Total Capital", f"${total_cap:,.0f}")
            
            market_regime = st.selectbox(
                "Market Regime",
                ["Neutral", "Bullish", "Bearish"],
                index=0
            )
            
            regime_mult = 1.10 if "Bullish" in market_regime else 0.90 if "Bearish" in market_regime else 1.0
        
        st.markdown("---")
        
        # Backup
        with st.expander("💾 Backup"):
            with open(DB_NAME, "rb") as f:
                st.download_button(
                    "📥 Download DB",
                    f,
                    "trade_guardian_v4.db",
                    "application/x-sqlite3",
                    use_container_width=True
                )
    
    # Load data
    df = load_data()
    snapshots = load_snapshots()
    config = load_strategy_config()
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Dashboard",
        "⚡ Active Trades",
        "📈 Analytics",
        "🤖 AI Insights",
        "⚙️ Configuration"
    ])
    
    # ========================================================================
    # DASHBOARD TAB
    # ========================================================================
    
    with tab1:
        if df.empty:
            st.info("No trades loaded. Upload your first file to get started.")
        else:
            active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
            
            if not active_df.empty:
                # Calculate decision ladder
                ladder_results = active_df.apply(
                    lambda row: calculate_decision_ladder(row, config, regime_mult),
                    axis=1
                )
                
                active_df['Action'] = [x[0] for x in ladder_results]
                active_df['Urgency Score'] = [x[1] for x in ladder_results]
                active_df['Reason'] = [x[2] for x in ladder_results]
                active_df['Juice Val'] = [x[3] for x in ladder_results]
                active_df['Juice Type'] = [x[4] for x in ladder_results]
                
                # Top metrics
                col1, col2, col3, col4 = st.columns(4)
                
                total_pnl = active_df['P&L'].sum()
                total_theta = active_df['Theta'].sum()
                avg_stability = active_df['Stability'].mean()
                
                with col1:
                    st.metric("Floating P&L", f"${total_pnl:,.0f}")
                
                with col2:
                    st.metric("Daily Theta", f"${total_theta:,.0f}")
                
                with col3:
                    st.metric("Avg Stability", f"{avg_stability:.2f}")
                
                with col4:
                    critical_count = len(active_df[active_df['Urgency Score'] >= 70])
                    st.metric("Action Items", critical_count)
                
                st.markdown("---")
                
                # Charts
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_radar = create_portfolio_radar(active_df, config)
                    st.plotly_chart(fig_radar, use_container_width=True)
                
                with col2:
                    fig_heat = create_heatmap(active_df)
                    st.plotly_chart(fig_heat, use_container_width=True)
            
            # Historical performance
            expired_df = df[df['Status'] == 'Expired'].copy()
            
            if not expired_df.empty:
                st.markdown("### 📈 Historical Performance")
                
                sharpe, cagr, max_dd = calculate_portfolio_metrics(expired_df, total_cap)
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Realized", f"${expired_df['P&L'].sum():,.0f}")
                
                with col2:
                    wins = len(expired_df[expired_df['P&L'] > 0])
                    win_rate = wins / len(expired_df) * 100
                    st.metric("Win Rate", f"{win_rate:.1f}%")
                
                with col3:
                    st.metric("Sharpe Ratio", f"{sharpe:.2f}")
                
                with col4:
                    st.metric("CAGR", f"{cagr:.1f}%")
    
    # ========================================================================
    # ACTIVE TRADES TAB (WITH HYPERLINKS!)
    # ========================================================================
    
    with tab2:
        if df.empty:
            st.info("No trades loaded.")
        else:
            active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
            
            if active_df.empty:
                st.info("No active trades.")
            else:
                # Calculate decision ladder
                ladder_results = active_df.apply(
                    lambda row: calculate_decision_ladder(row, config, regime_mult),
                    axis=1
                )
                
                active_df['Action'] = [x[0] for x in ladder_results]
                active_df['Urgency Score'] = [x[1] for x in ladder_results]
                active_df['Reason'] = [x[2] for x in ladder_results]
                active_df['Juice Val'] = [x[3] for x in ladder_results]
                active_df['Juice Type'] = [x[4] for x in ladder_results]
                
                # Sort by urgency
                active_df = active_df.sort_values('Urgency Score', ascending=False)
                
                # Priority queue
                todo_df = active_df[active_df['Urgency Score'] >= 70]
                
                with st.expander(f"🚨 Priority Action Queue ({len(todo_df)})", expanded=len(todo_df) > 0):
                    if not todo_df.empty:
                        for _, row in todo_df.iterrows():
                            u_score = row['Urgency Score']
                            color = COLORS['danger'] if u_score >= 90 else COLORS['warning']
                            
                            is_valid_link = str(row['Link']).startswith('http')
                            name_display = f"[{row['Name']}]({row['Link']})" if is_valid_link else row['Name']
                            
                            col_a, col_b, col_c = st.columns([2, 1, 1])
                            
                            col_a.markdown(f"**{name_display}** ({row['Strategy']})")
                            col_b.markdown(f":{color}[**{row['Action']}**] ({row['Reason']})")
                            
                            if row['Juice Type'] == 'Recovery Days':
                                col_c.metric("Break Even", f"{row['Juice Val']:.0f}d")
                            else:
                                col_c.metric("Left in Tank", f"${row['Juice Val']:.0f}")
                    else:
                        st.success("✅ No critical actions required")
                
                st.markdown("---")
                
                # Trade table with editable columns
                st.markdown("### 📋 Active Positions")
                
                # Filter strategy options
                strategy_options = sorted(list(config.keys())) + ["Other"]
                
                # Prepare display columns
                display_cols = [
                    'id', 'Name', 'Link', 'Strategy', 'Urgency Score', 'Action',
                    'Status', 'Stability', 'ROI', 'Ann. ROI', 'Theta Eff.',
                    'lot_size', 'P&L', 'Debit', 'Days Held',
                    'Notes', 'Tags', 'Parent ID'
                ]
                
                column_config = {
                    "id": None,
                    "Name": st.column_config.TextColumn("Trade Name", disabled=True),
                    "Link": st.column_config.LinkColumn("OptionStrat", display_text="Open 🔗"),
                    "Strategy": st.column_config.SelectboxColumn(
                        "Strategy",
                        options=strategy_options,
                        required=True
                    ),
                    "Urgency Score": st.column_config.ProgressColumn(
                        "Urgency",
                        min_value=0,
                        max_value=100,
                        format="%d"
                    ),
                    "Action": st.column_config.TextColumn("Decision", disabled=True),
                    "Status": st.column_config.TextColumn("Status", disabled=True),
                    "Stability": st.column_config.ProgressColumn(
                        "Stability",
                        min_value=0,
                        max_value=3,
                        format="%.2f"
                    ),
                    "ROI": st.column_config.NumberColumn("ROI %", format="%.1f%%", disabled=True),
                    "Ann. ROI": st.column_config.NumberColumn("Ann. ROI %", format="%.1f%%", disabled=True),
                    "Theta Eff.": st.column_config.NumberColumn("Eff", format="%.2f", disabled=True),
                    "lot_size": st.column_config.NumberColumn("Lots", min_value=1, step=1),
                    "P&L": st.column_config.NumberColumn("P&L", format="$%d", disabled=True),
                    "Debit": st.column_config.NumberColumn("Debit", format="$%d", disabled=True),
                    "Days Held": st.column_config.NumberColumn("Days", disabled=True),
                    "Notes": st.column_config.TextColumn("Notes", width="large"),
                    "Tags": st.column_config.SelectboxColumn(
                        "Tags",
                        options=["Rolled", "Hedged", "Earnings", "High Risk", "Watch"],
                        width="medium"
                    ),
                    "Parent ID": st.column_config.TextColumn("Link ID")
                }
                
                edited_df = st.data_editor(
                    active_df[display_cols],
                    column_config=column_config,
                    hide_index=True,
                    use_container_width=True,
                    key="active_trades_editor",
                    num_rows="fixed"
                )
                
                # Save button
                if st.button("💾 Save Changes"):
                    conn = get_db_connection()
                    c = conn.cursor()
                    
                    try:
                        for index, row in edited_df.iterrows():
                            t_id = row['id']
                            notes = str(row['Notes'])
                            tags = str(row['Tags'])
                            pid = str(row['Parent ID'])
                            new_lot = int(row['lot_size']) if row['lot_size'] > 0 else 1
                            new_strat = str(row['Strategy'])
                            
                            c.execute("""UPDATE trades SET 
                                notes=?, tags=?, parent_id=?, lot_size=?, strategy=? 
                                WHERE id=?""",
                                (notes, tags, pid, new_lot, new_strat, t_id))
                        
                        conn.commit()
                        st.cache_data.clear()
                        st.success("✅ Changes saved!")
                        
                        # Auto cloud sync
                        if drive_mgr.is_connected:
                            with st.spinner("☁️ Syncing..."):
                                drive_mgr.upload_db()
                        
                        time.sleep(1)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Save error: {e}")
                    finally:
                        conn.close()
    
    # ========================================================================
    # ANALYTICS TAB
    # ========================================================================
    
    with tab3:
        if df.empty:
            st.info("No data loaded.")
        else:
            expired_df = df[df['Status'] == 'Expired'].copy()
            
            if expired_df.empty:
                st.info("No closed trades for analysis.")
            else:
                st.markdown("### 📊 Performance Analysis")
                
                # Strategy performance
                strategy_stats = expired_df.groupby('Strategy').agg({
                    'P&L': ['sum', 'mean'],
                    'ROI': 'mean',
                    'Daily Yield %': 'mean',
                    'Days Held': 'mean',
                    'id': 'count'
                }).round(2)
                
                strategy_stats.columns = ['Total P&L', 'Avg P&L', 'Avg ROI', 'Daily Yield', 'Avg Days', 'Count']
                
                # Win rate
                wins = expired_df[expired_df['P&L'] > 0].groupby('Strategy')['id'].count()
                strategy_stats['Win Rate'] = (wins / strategy_stats['Count']) * 100
                strategy_stats['Win Rate'] = strategy_stats['Win Rate'].fillna(0)
                
                st.dataframe(
                    strategy_stats.style.format({
                        'Total P&L': '${:,.0f}',
                        'Avg P&L': '${:,.0f}',
                        'Avg ROI': '{:.2f}%',
                        'Daily Yield': '{:.3f}%',
                        'Avg Days': '{:.0f}',
                        'Win Rate': '{:.1f}%'
                    }),
                    use_container_width=True
                )
                
                st.markdown("---")
                
                # Monthly performance
                st.markdown("#### Monthly Performance")
                
                expired_df['Month'] = pd.to_datetime(expired_df['Exit Date']).dt.to_period('M')
                monthly = expired_df.groupby('Month')['P&L'].sum().reset_index()
                monthly['Month'] = monthly['Month'].astype(str)
                
                fig_monthly = px.bar(
                    monthly,
                    x='Month',
                    y='P&L',
                    title="Monthly P&L",
                    color='P&L',
                    color_continuous_scale=['#ef4444', '#10b981']
                )
                
                st.plotly_chart(apply_chart_theme(fig_monthly), use_container_width=True)
    
    # ========================================================================
    # AI INSIGHTS TAB
    # ========================================================================
    
    with tab4:
        if df.empty:
            st.info("No data loaded.")
        else:
            active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
            expired_df = df[df['Status'] == 'Expired'].copy()
            
            if active_df.empty or expired_df.empty:
                st.info("Need both active and historical trades for predictions.")
            else:
                st.markdown("### 🤖 AI Trade Predictions")
                
                # Generate predictions
                predictions = generate_trade_predictions(active_df, expired_df, 40, 75, total_cap)
                
                if not predictions.empty:
                    st.dataframe(
                        predictions.style.format({
                            'Win Prob %': '{:.1f}%',
                            'Expected PnL': '${:,.0f}',
                            'Kelly Size %': '{:.1f}%',
                            'Rec. Size ($)': '${:,.0f}',
                            'Confidence': '{:.0f}%'
                        }),
                        use_container_width=True
                    )
                    
                    # Scatter plot
                    fig_pred = px.scatter(
                        predictions,
                        x='Win Prob %',
                        y='Expected PnL',
                        size='Rec. Size ($)',
                        color='Confidence',
                        hover_data=['Trade Name', 'Strategy'],
                        color_continuous_scale='RdYlGn',
                        title="AI Prediction Map"
                    )
                    
                    fig_pred.add_vline(x=50, line_dash="dash")
                    fig_pred.add_hline(y=0, line_dash="dash")
                    
                    st.plotly_chart(apply_chart_theme(fig_pred), use_container_width=True)
                else:
                    st.info("Not enough historical data for predictions.")
    
    # ========================================================================
    # CONFIGURATION TAB
    # ========================================================================
    
    with tab5:
        st.markdown("### ⚙️ Strategy Configuration")
        
        conn = get_db_connection()
        config_df = pd.read_sql("SELECT * FROM strategy_config", conn)
        conn.close()
        
        edited_config = st.data_editor(
            config_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "name": "Strategy Name",
                "identifier": "Keyword",
                "target_pnl": st.column_config.NumberColumn("Target P&L", format="$%d"),
                "target_days": "Target Days",
                "min_stability": st.column_config.NumberColumn("Min Stability", format="%.2f"),
                "typical_debit": st.column_config.NumberColumn("Typical Debit", format="$%d"),
                "description": "Description"
            }
        )
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            if st.button("💾 Save Config", use_container_width=True):
                conn = get_db_connection()
                c = conn.cursor()
                
                try:
                    c.execute("DELETE FROM strategy_config")
                    
                    for _, row in edited_config.iterrows():
                        c.execute(
                            "INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)",
                            tuple(row)
                        )
                    
                    conn.commit()
                    st.cache_data.clear()
                    st.success("✅ Configuration saved!")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Save error: {e}")
                finally:
                    conn.close()
    
    # Footer
    st.markdown("---")
    st.markdown("""
        <div style="text-align: center; color: #64748b; padding: 20px;">
            <p>Trade Guardian ELITE Edition | All Original Features + Superior Architecture</p>
        </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
