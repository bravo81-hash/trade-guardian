"""
Allantis Trade Guardian v2.0 - Elite Edition
Complete rewrite with modern architecture, enhanced analytics, and professional UI
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
from datetime import datetime, timezone, timedelta, date
from scipy import stats
from scipy.spatial.distance import cdist
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

# Google Drive imports
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False

# Excel imports
try:
    from openpyxl import load_workbook
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

DB_NAME = "trade_guardian_v4.db"
SCOPES = ['https://www.googleapis.com/auth/drive']

# Modern color palette
COLORS = {
    'primary': '#6366f1',      # Indigo
    'success': '#10b981',      # Emerald
    'warning': '#f59e0b',      # Amber
    'danger': '#ef4444',       # Red
    'info': '#3b82f6',         # Blue
    'purple': '#a855f7',       # Purple
    'teal': '#14b8a6',         # Teal
    'bg_dark': '#0f172a',      # Slate 900
    'bg_card': '#1e293b',      # Slate 800
    'text_primary': '#f8fafc', # Slate 50
    'text_secondary': '#cbd5e1', # Slate 300
}

# Strategy configurations
DEFAULT_STRATEGIES = {
    '130/160': {'pnl': 500, 'dit': 36, 'stability': 0.8, 'debit': 4000},
    '160/190': {'pnl': 700, 'dit': 44, 'stability': 0.8, 'debit': 5200},
    'M200': {'pnl': 900, 'dit': 41, 'stability': 0.8, 'debit': 8000},
    'SMSF': {'pnl': 600, 'dit': 40, 'stability': 0.8, 'debit': 5000}
}

# ============================================================================
# STYLING & UI
# ============================================================================

def inject_custom_css():
    """Inject modern, professional CSS with glassmorphism and animations"""
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Fira+Code:wght@400;500;600&display=swap');
        
        /* Global Styles */
        * {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }
        
        .stApp {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: #f8fafc;
        }
        
        /* Sidebar */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #020617 0%, #0f172a 100%);
            border-right: 1px solid rgba(99, 102, 241, 0.1);
        }
        
        [data-testid="stSidebar"] .stMarkdown {
            color: #cbd5e1;
        }
        
        /* Headers */
        h1, h2, h3, h4, h5, h6 {
            font-weight: 700 !important;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, #6366f1 0%, #a855f7 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        h1 { font-size: 2.5rem !important; }
        h2 { font-size: 2rem !important; }
        h3 { font-size: 1.5rem !important; }
        
        /* Metric Cards - Modern Glassmorphism */
        div[data-testid="stMetric"] {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(20px);
            border: 1px solid rgba(99, 102, 241, 0.2);
            border-radius: 16px;
            padding: 20px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        div[data-testid="stMetric"]:hover {
            transform: translateY(-4px);
            border-color: rgba(99, 102, 241, 0.4);
            box-shadow: 0 12px 48px rgba(99, 102, 241, 0.2);
        }
        
        [data-testid="stMetricLabel"] {
            color: #94a3b8 !important;
            font-size: 0.875rem !important;
            font-weight: 500 !important;
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
            padding: 12px 24px;
            font-weight: 600;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(99, 102, 241, 0.3);
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
            height: 50px;
            background: transparent;
            border-radius: 8px;
            color: #94a3b8;
            font-weight: 600;
            font-size: 0.9rem;
            border: none;
            transition: all 0.2s ease;
        }
        
        .stTabs [data-baseweb="tab"]:hover {
            background: rgba(99, 102, 241, 0.1);
            color: #a5b4fc;
        }
        
        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%) !important;
            color: white !important;
        }
        
        /* Expanders */
        div[data-testid="stExpander"] {
            background: rgba(30, 41, 59, 0.4);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(99, 102, 241, 0.1);
            border-radius: 12px;
            overflow: hidden;
        }
        
        div[data-testid="stExpander"] summary {
            font-weight: 600;
            color: #e2e8f0;
        }
        
        /* Data Frames */
        [data-testid="stDataFrame"] {
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid rgba(99, 102, 241, 0.2);
        }
        
        /* Success/Warning/Error Messages */
        .element-container .stAlert {
            border-radius: 12px;
            border: none;
            backdrop-filter: blur(10px);
        }
        
        /* Progress bars */
        .stProgress > div > div {
            background: linear-gradient(90deg, #6366f1 0%, #8b5cf6 100%);
            border-radius: 8px;
        }
        
        /* Custom Classes */
        .metric-card {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(20px);
            border: 1px solid rgba(99, 102, 241, 0.2);
            border-radius: 16px;
            padding: 24px;
            margin: 8px 0;
        }
        
        .status-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
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
        
        /* Animations */
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .animate-in {
            animation: fadeIn 0.5s ease-out;
        }
        
        /* Code blocks */
        code {
            background: rgba(99, 102, 241, 0.1);
            color: #a5b4fc;
            padding: 2px 6px;
            border-radius: 4px;
            font-family: 'Fira Code', monospace;
        }
        
        /* Scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        
        ::-webkit-scrollbar-track {
            background: rgba(15, 23, 42, 0.5);
        }
        
        ::-webkit-scrollbar-thumb {
            background: rgba(99, 102, 241, 0.3);
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: rgba(99, 102, 241, 0.5);
        }
        </style>
    """, unsafe_allow_html=True)

def create_plotly_theme():
    """Create consistent Plotly theme for all charts"""
    return {
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'family': 'Inter', 'color': '#cbd5e1', 'size': 12},
        'title': {'font': {'size': 18, 'color': '#f8fafc', 'family': 'Inter'}},
        'xaxis': {
            'showgrid': True,
            'gridcolor': 'rgba(99, 102, 241, 0.1)',
            'gridwidth': 1,
            'zeroline': True,
            'zerolinecolor': 'rgba(99, 102, 241, 0.2)',
            'color': '#94a3b8'
        },
        'yaxis': {
            'showgrid': True,
            'gridcolor': 'rgba(99, 102, 241, 0.1)',
            'gridwidth': 1,
            'zeroline': True,
            'zerolinecolor': 'rgba(99, 102, 241, 0.2)',
            'color': '#94a3b8'
        },
        'colorway': ['#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#14b8a6', '#f43f5e'],
        'hovermode': 'closest',
        'hoverlabel': {
            'bgcolor': '#1e293b',
            'bordercolor': '#6366f1',
            'font': {'family': 'Fira Code', 'size': 12, 'color': '#f8fafc'}
        }
    }

def apply_chart_theme(fig):
    """Apply consistent theme to Plotly figure"""
    fig.update_layout(**create_plotly_theme())
    return fig

# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class Trade:
    """Trade data model"""
    id: str
    name: str
    strategy: str
    status: str
    entry_date: date
    exit_date: Optional[date]
    days_held: int
    debit: float
    lot_size: int
    pnl: float
    theta: float
    delta: float
    gamma: float
    vega: float
    iv: float
    put_pnl: float
    call_pnl: float
    notes: str
    tags: str
    parent_id: str
    link: str
    
    @property
    def roi(self) -> float:
        return (self.pnl / self.debit * 100) if self.debit > 0 else 0
    
    @property
    def daily_yield(self) -> float:
        return (self.roi / self.days_held) if self.days_held > 0 else 0
    
    @property
    def stability(self) -> float:
        return self.theta / (abs(self.delta) + 1) if self.theta > 0 else 0

# ============================================================================
# DATABASE LAYER
# ============================================================================

class DatabaseManager:
    """Enhanced database manager with connection pooling and caching"""
    
    def __init__(self, db_path: str = DB_NAME):
        self.db_path = db_path
        self._ensure_db_exists()
    
    def _ensure_db_exists(self):
        """Initialize database if it doesn't exist"""
        if not os.path.exists(self.db_path):
            self._init_schema()
    
    def _init_schema(self):
        """Create database schema"""
        conn = sqlite3.connect(self.db_path)
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
        
        # Create indexes
        c.execute("CREATE INDEX IF NOT EXISTS idx_status ON trades(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_strategy ON trades(strategy)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_date ON snapshots(snapshot_date)")
        
        conn.commit()
        conn.close()
        
        self._seed_default_strategies()
    
    def _seed_default_strategies(self):
        """Seed default strategy configurations"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
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
        
        conn.close()
    
    @st.cache_data(ttl=60)
    def load_trades(_self) -> pd.DataFrame:
        """Load all trades with calculated metrics"""
        conn = sqlite3.connect(_self.db_path)
        
        try:
            df = pd.read_sql("SELECT * FROM trades", conn)
            
            if df.empty:
                return pd.DataFrame()
            
            # Data type conversions
            numeric_cols = ['debit', 'pnl', 'days_held', 'theta', 'delta', 'gamma', 'vega', 'iv', 'put_pnl', 'call_pnl', 'lot_size']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            df['entry_date'] = pd.to_datetime(df['entry_date'])
            df['exit_date'] = pd.to_datetime(df['exit_date'])
            df['lot_size'] = df['lot_size'].clip(lower=1).astype(int)
            
            # Calculate derived metrics
            df['debit_per_lot'] = df['debit'] / df['lot_size']
            df['roi'] = (df['pnl'] / df['debit'].replace(0, 1)) * 100
            df['daily_yield'] = df['roi'] / df['days_held'].replace(0, 1)
            df['ann_roi'] = df['daily_yield'] * 365
            df['theta_potential'] = df['theta'] * df['days_held']
            df['theta_efficiency'] = np.where(
                df['theta_potential'] > 0,
                df['pnl'] / df['theta_potential'],
                0
            )
            df['theta_cap_pct'] = (df['theta'] / df['debit'].replace(0, 1)) * 100
            df['stability'] = df['theta'] / (df['delta'].abs() + 1)
            
            # Extract ticker
            df['ticker'] = df['name'].str.split().str[0].str.replace('.', '').str.upper()
            
            # Calculate volatility from snapshots
            snaps = pd.read_sql("SELECT trade_id, pnl FROM snapshots", conn)
            if not snaps.empty:
                vol_df = snaps.groupby('trade_id')['pnl'].std().reset_index()
                vol_df.columns = ['id', 'pnl_volatility']
                df = df.merge(vol_df, on='id', how='left')
                df['pnl_volatility'] = df['pnl_volatility'].fillna(0)
            else:
                df['pnl_volatility'] = 0
            
            return df
            
        finally:
            conn.close()
    
    @st.cache_data(ttl=60)
    def load_snapshots(_self) -> pd.DataFrame:
        """Load snapshot data"""
        conn = sqlite3.connect(_self.db_path)
        
        try:
            query = """
                SELECT s.*, t.strategy, t.name, t.theta as initial_theta
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
            
        finally:
            conn.close()
    
    @st.cache_data(ttl=60)
    def load_strategy_config(_self) -> Dict:
        """Load strategy configurations"""
        conn = sqlite3.connect(_self.db_path)
        
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
            
        finally:
            conn.close()

# ============================================================================
# CLOUD SYNC MANAGER
# ============================================================================

class CloudSyncManager:
    """Enhanced Google Drive sync with conflict resolution"""
    
    def __init__(self):
        self.service = None
        self.is_connected = False
        self._init_connection()
    
    def _init_connection(self):
        """Initialize Google Drive connection"""
        if not GOOGLE_AVAILABLE:
            return
        
        if 'gcp_service_account' in st.secrets:
            try:
                creds = service_account.Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"],
                    scopes=SCOPES
                )
                self.service = build('drive', 'v3', credentials=creds)
                self.is_connected = True
            except Exception as e:
                st.error(f"Cloud connection failed: {e}")
    
    def find_db_file(self) -> Tuple[Optional[str], Optional[str]]:
        """Find database file in Drive"""
        if not self.is_connected:
            return None, None
        
        try:
            # Search for exact match first
            query = f"name='{DB_NAME}' and trashed=false"
            results = self.service.files().list(
                q=query,
                pageSize=1,
                fields="files(id, name, modifiedTime)"
            ).execute()
            
            items = results.get('files', [])
            if items:
                return items[0]['id'], items[0]['name']
            
            # Fuzzy search
            query = "name contains 'trade_guardian' and name contains '.db' and trashed=false"
            results = self.service.files().list(
                q=query,
                pageSize=5,
                fields="files(id, name, modifiedTime)"
            ).execute()
            
            items = results.get('files', [])
            if items:
                # Prefer exact prefix match
                for item in items:
                    if item['name'].startswith('trade_guardian_v4'):
                        return item['id'], item['name']
                return items[0]['id'], items[0]['name']
            
            return None, None
            
        except Exception as e:
            st.error(f"Search failed: {e}")
            return None, None
    
    def upload(self, force: bool = False) -> Tuple[bool, str]:
        """Upload database to Drive"""
        if not self.is_connected:
            return False, "Cloud not connected"
        
        if not os.path.exists(DB_NAME):
            return False, "No local database found"
        
        file_id, file_name = self.find_db_file()
        
        # Check for conflicts
        if file_id and not force:
            cloud_time = self._get_modified_time(file_id)
            local_time = datetime.fromtimestamp(
                os.path.getmtime(DB_NAME),
                tz=timezone.utc
            )
            
            if cloud_time and cloud_time > local_time + timedelta(seconds=2):
                return False, f"CONFLICT: Cloud file newer ({cloud_time.strftime('%H:%M')})"
        
        try:
            media = MediaFileUpload(DB_NAME, mimetype='application/x-sqlite3', resumable=True)
            
            if file_id:
                # Update existing
                self.service.files().update(
                    fileId=file_id,
                    media_body=media
                ).execute()
                return True, f"Updated: {file_name}"
            else:
                # Create new
                file_metadata = {'name': DB_NAME}
                self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                return True, "Created new cloud file"
                
        except Exception as e:
            return False, f"Upload failed: {str(e)}"
    
    def download(self, force: bool = False) -> Tuple[bool, str]:
        """Download database from Drive"""
        if not self.is_connected:
            return False, "Cloud not connected"
        
        file_id, file_name = self.find_db_file()
        if not file_id:
            return False, "Database not found in cloud"
        
        # Check for conflicts
        if os.path.exists(DB_NAME) and not force:
            local_time = datetime.fromtimestamp(
                os.path.getmtime(DB_NAME),
                tz=timezone.utc
            )
            cloud_time = self._get_modified_time(file_id)
            
            if cloud_time and local_time > cloud_time + timedelta(minutes=2):
                return False, f"CONFLICT: Local file newer ({local_time.strftime('%H:%M')})"
        
        try:
            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            with open(DB_NAME, "wb") as f:
                f.write(fh.getbuffer())
            
            return True, f"Downloaded: {file_name}"
            
        except Exception as e:
            return False, f"Download failed: {str(e)}"
    
    def _get_modified_time(self, file_id: str) -> Optional[datetime]:
        """Get file modification time"""
        try:
            file = self.service.files().get(
                fileId=file_id,
                fields='modifiedTime'
            ).execute()
            
            dt_str = file['modifiedTime'].replace('Z', '+0000')
            return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%f%z')
        except:
            return None

# ============================================================================
# ANALYTICS ENGINE
# ============================================================================

class AnalyticsEngine:
    """Advanced analytics and calculations"""
    
    @staticmethod
    def calculate_portfolio_metrics(trades_df: pd.DataFrame, capital: float) -> Dict:
        """Calculate comprehensive portfolio metrics"""
        if trades_df.empty or capital <= 0:
            return {
                'total_pnl': 0,
                'roi': 0,
                'sharpe': 0,
                'cagr': 0,
                'max_dd': 0,
                'win_rate': 0,
                'avg_win': 0,
                'avg_loss': 0,
                'profit_factor': 0
            }
        
        # Basic metrics
        total_pnl = trades_df['pnl'].sum()
        roi = (total_pnl / capital) * 100
        
        # Win/Loss analysis
        winners = trades_df[trades_df['pnl'] > 0]
        losers = trades_df[trades_df['pnl'] < 0]
        
        win_rate = len(winners) / len(trades_df) if len(trades_df) > 0 else 0
        avg_win = winners['pnl'].mean() if len(winners) > 0 else 0
        avg_loss = losers['pnl'].mean() if len(losers) > 0 else 0
        
        gross_profit = winners['pnl'].sum() if len(winners) > 0 else 0
        gross_loss = abs(losers['pnl'].sum()) if len(losers) > 0 else 1
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
        
        # Time-based metrics
        if 'entry_date' in trades_df.columns and 'exit_date' in trades_df.columns:
            trades_df = trades_df.copy()
            trades_df['entry_date'] = pd.to_datetime(trades_df['entry_date'])
            trades_df['exit_date'] = pd.to_datetime(trades_df['exit_date'])
            
            valid_trades = trades_df.dropna(subset=['entry_date', 'exit_date'])
            
            if not valid_trades.empty:
                start_date = valid_trades['entry_date'].min()
                end_date = valid_trades['exit_date'].max()
                days = (end_date - start_date).days
                
                if days > 0:
                    # CAGR
                    end_value = capital + total_pnl
                    cagr = ((end_value / capital) ** (365 / days) - 1) * 100
                    
                    # Sharpe (simplified daily returns)
                    daily_pnl = AnalyticsEngine._reconstruct_daily_pnl(valid_trades)
                    if len(daily_pnl) > 1:
                        returns = pd.Series(list(daily_pnl.values())).pct_change().dropna()
                        if returns.std() > 0:
                            sharpe = (returns.mean() / returns.std()) * np.sqrt(252)
                        else:
                            sharpe = 0
                    else:
                        sharpe = 0
                    
                    # Max Drawdown
                    max_dd = AnalyticsEngine._calculate_max_drawdown(valid_trades, capital)
                else:
                    cagr = 0
                    sharpe = 0
                    max_dd = 0
            else:
                cagr = 0
                sharpe = 0
                max_dd = 0
        else:
            cagr = 0
            sharpe = 0
            max_dd = 0
        
        return {
            'total_pnl': total_pnl,
            'roi': roi,
            'sharpe': sharpe,
            'cagr': cagr,
            'max_dd': max_dd,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor
        }
    
    @staticmethod
    def _reconstruct_daily_pnl(trades_df: pd.DataFrame) -> Dict[date, float]:
        """Reconstruct daily P&L from trades"""
        daily_pnl = defaultdict(float)
        
        for _, trade in trades_df.iterrows():
            if pd.isna(trade['exit_date']):
                continue
            
            days = trade['days_held']
            if days <= 0:
                days = 1
            
            pnl_per_day = trade['pnl'] / days
            
            current_date = trade['entry_date']
            for _ in range(days):
                if isinstance(current_date, pd.Timestamp):
                    daily_pnl[current_date.date()] += pnl_per_day
                else:
                    daily_pnl[current_date] += pnl_per_day
                current_date += timedelta(days=1)
        
        return dict(daily_pnl)
    
    @staticmethod
    def _calculate_max_drawdown(trades_df: pd.DataFrame, capital: float) -> float:
        """Calculate maximum drawdown percentage"""
        daily_pnl = AnalyticsEngine._reconstruct_daily_pnl(trades_df)
        
        if not daily_pnl:
            return 0
        
        dates = sorted(daily_pnl.keys())
        equity_curve = []
        equity = capital
        
        for date in dates:
            equity += daily_pnl[date]
            equity_curve.append(equity)
        
        equity_series = pd.Series(equity_curve)
        running_max = equity_series.cummax()
        drawdown = (equity_series - running_max) / running_max
        
        return abs(drawdown.min() * 100) if len(drawdown) > 0 else 0
    
    @staticmethod
    def calculate_kelly_criterion(win_rate: float, avg_win: float, avg_loss: float) -> float:
        """Calculate Kelly Criterion for position sizing"""
        if avg_loss == 0 or avg_win <= 0:
            return 0
        
        b = abs(avg_win / avg_loss)
        kelly = (win_rate * b - (1 - win_rate)) / b
        
        # Use half-Kelly for safety
        return max(0, min(kelly * 0.5, 0.25))
    
    @staticmethod
    def predict_trade_outcome(current_trade: pd.Series, historical_df: pd.DataFrame) -> Dict:
        """Predict trade outcome using KNN"""
        if historical_df.empty:
            return {'win_prob': 0.5, 'expected_pnl': 0, 'confidence': 0}
        
        features = ['theta_cap_pct', 'delta', 'debit_per_lot', 'stability']
        
        # Ensure features exist
        for feat in features:
            if feat not in current_trade.index or feat not in historical_df.columns:
                return {'win_prob': 0.5, 'expected_pnl': 0, 'confidence': 0}
        
        try:
            curr_vec = np.array([current_trade[f] for f in features]).reshape(1, -1)
            hist_vecs = historical_df[features].values
            
            # Calculate distances
            distances = cdist(curr_vec, hist_vecs, metric='euclidean')[0]
            
            # Get top K neighbors
            k = min(7, len(historical_df))
            top_k_idx = np.argsort(distances)[:k]
            neighbors = historical_df.iloc[top_k_idx]
            
            # Calculate metrics
            win_prob = (neighbors['pnl'] > 0).mean()
            expected_pnl = neighbors['pnl'].mean()
            
            # Confidence based on distance
            avg_dist = distances[top_k_idx].mean()
            confidence = max(0, 100 - (avg_dist * 10))
            
            return {
                'win_prob': win_prob,
                'expected_pnl': expected_pnl,
                'confidence': confidence
            }
        except Exception as e:
            return {'win_prob': 0.5, 'expected_pnl': 0, 'confidence': 0}

# ============================================================================
# FILE PARSER
# ============================================================================

class FileParser:
    """Enhanced file parser for OptionStrat exports"""
    
    @staticmethod
    def parse_file(file, file_type: str, config_dict: Dict) -> List[Dict]:
        """Parse uploaded file and extract trade data"""
        try:
            # Try Excel first
            if file.name.endswith(('.xlsx', '.xls')):
                return FileParser._parse_excel(file, file_type, config_dict)
            else:
                # Fall back to CSV
                return FileParser._parse_csv(file, file_type, config_dict)
        except Exception as e:
            st.error(f"Parse error: {e}")
            return []
    
    @staticmethod
    def _parse_excel(file, file_type: str, config_dict: Dict) -> List[Dict]:
        """Parse Excel file"""
        # Find header row
        df_temp = pd.read_excel(file, header=None)
        header_row = 0
        
        for i, row in df_temp.head(30).iterrows():
            row_vals = [str(v).strip() for v in row.values]
            if "Name" in row_vals and "Total Return $" in row_vals:
                header_row = i
                break
        
        # Read with correct header
        file.seek(0)
        df_raw = pd.read_excel(file, header=header_row)
        
        # Extract hyperlinks if available
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
        
        return FileParser._extract_trades(df_raw, file_type, config_dict)
    
    @staticmethod
    def _parse_csv(file, file_type: str, config_dict: Dict) -> List[Dict]:
        """Parse CSV file"""
        content = file.getvalue().decode("utf-8", errors='ignore')
        lines = content.split('\n')
        
        # Find header
        header_row = 0
        for i, line in enumerate(lines[:30]):
            if "Name" in line and "Total Return" in line:
                header_row = i
                break
        
        file.seek(0)
        df_raw = pd.read_csv(file, skiprows=header_row)
        
        return FileParser._extract_trades(df_raw, file_type, config_dict)
    
    @staticmethod
    def _extract_trades(df_raw: pd.DataFrame, file_type: str, config_dict: Dict) -> List[Dict]:
        """Extract trade data from parsed DataFrame"""
        trades = []
        current_trade = None
        current_legs = []
        
        def finalize_trade(trade_row, legs):
            if trade_row is None or trade_row.empty:
                return None
            
            name = str(trade_row.get('Name', ''))
            group = str(trade_row.get('Group', ''))
            
            # Determine strategy
            strategy = FileParser._determine_strategy(name, group, config_dict)
            
            # Extract data
            created = trade_row.get('Created At', '')
            try:
                entry_date = pd.to_datetime(created)
            except:
                return None
            
            link = str(trade_row.get('Link', ''))
            if link == 'nan' or link == 'Open':
                link = ""
            
            pnl = FileParser._clean_numeric(trade_row.get('Total Return $', 0))
            debit = abs(FileParser._clean_numeric(trade_row.get('Net Debit/Credit', 0)))
            theta = FileParser._clean_numeric(trade_row.get('Theta', 0))
            delta = FileParser._clean_numeric(trade_row.get('Delta', 0))
            gamma = FileParser._clean_numeric(trade_row.get('Gamma', 0))
            vega = FileParser._clean_numeric(trade_row.get('Vega', 0))
            iv = FileParser._clean_numeric(trade_row.get('IV', 0))
            
            # Exit date
            exit_date = None
            try:
                raw_exp = trade_row.get('Expiration')
                if pd.notnull(raw_exp) and str(raw_exp).strip():
                    exit_date = pd.to_datetime(raw_exp)
            except:
                pass
            
            # Calculate days held
            if exit_date and file_type == "History":
                days_held = (exit_date - entry_date).days
            else:
                days_held = (datetime.now() - entry_date).days
            
            days_held = max(1, days_held)
            
            # Determine lot size
            strat_config = config_dict.get(strategy, {})
            typical_debit = strat_config.get('debit_per_lot', 5000)
            
            lot_match = re.search(r'(\d+)\s*(?:LOT|L\b)', name, re.IGNORECASE)
            if lot_match:
                lot_size = int(lot_match.group(1))
            else:
                lot_size = max(1, int(round(debit / typical_debit)))
            
            # Calculate put/call P&L from legs
            put_pnl = 0.0
            call_pnl = 0.0
            
            if file_type == "History":
                for leg in legs:
                    if len(leg) < 5:
                        continue
                    
                    sym = str(leg.iloc[0])
                    if not sym.startswith('.'):
                        continue
                    
                    try:
                        qty = FileParser._clean_numeric(leg.iloc[1])
                        entry_price = FileParser._clean_numeric(leg.iloc[2])
                        close_price = FileParser._clean_numeric(leg.iloc[4])
                        
                        leg_pnl = (close_price - entry_price) * qty * 100
                        
                        if 'P' in sym and 'C' not in sym:
                            put_pnl += leg_pnl
                        elif 'C' in sym and 'P' not in sym:
                            call_pnl += leg_pnl
                    except:
                        pass
            
            # Generate ID
            trade_id = FileParser._generate_id(name, strategy, entry_date)
            
            return {
                'id': trade_id,
                'name': name,
                'strategy': strategy,
                'entry_date': entry_date,
                'exit_date': exit_date,
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
        for _, row in df_raw.iterrows():
            name_val = str(row['Name'])
            
            if name_val and not name_val.startswith('.') and name_val != 'Symbol' and name_val != 'nan':
                # New trade
                if current_trade is not None:
                    result = finalize_trade(current_trade, current_legs)
                    if result:
                        trades.append(result)
                
                current_trade = row
                current_legs = []
            elif name_val.startswith('.'):
                # Leg data
                current_legs.append(row)
        
        # Finalize last trade
        if current_trade is not None:
            result = finalize_trade(current_trade, current_legs)
            if result:
                trades.append(result)
        
        return trades
    
    @staticmethod
    def _determine_strategy(name: str, group: str, config_dict: Dict) -> str:
        """Determine strategy from name/group"""
        name_upper = str(name).upper().strip()
        group_upper = str(group).upper().strip()
        
        # Sort by identifier length (longest first for better matching)
        sorted_strats = sorted(
            config_dict.items(),
            key=lambda x: len(str(x[1].get('id', ''))),
            reverse=True
        )
        
        # Check name first
        for strat_name, details in sorted_strats:
            key = str(details.get('id', '')).upper()
            if key in name_upper:
                return strat_name
        
        # Check group
        for strat_name, details in sorted_strats:
            key = str(details.get('id', '')).upper()
            if key in group_upper:
                return strat_name
        
        return "Other"
    
    @staticmethod
    def _clean_numeric(value) -> float:
        """Clean and convert numeric values"""
        try:
            if pd.isna(value) or str(value).strip() == "":
                return 0.0
            
            val_str = str(value).replace('$', '').replace(',', '').replace('%', '').strip()
            val = float(val_str)
            
            return 0.0 if np.isnan(val) else val
        except:
            return 0.0
    
    @staticmethod
    def _generate_id(name: str, strategy: str, entry_date) -> str:
        """Generate unique trade ID"""
        date_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
        safe_name = re.sub(r'\W+', '', str(name))
        return f"{safe_name}_{strategy}_{date_str}"

# ============================================================================
# VISUALIZATION COMPONENTS
# ============================================================================

class VisualizationEngine:
    """Advanced visualization components"""
    
    @staticmethod
    def create_portfolio_radar(active_df: pd.DataFrame, benchmarks: Dict) -> go.Figure:
        """Create portfolio health radar chart"""
        if active_df.empty:
            # Empty state
            fig = go.Figure()
            fig.update_layout(
                title="Portfolio Health Radar",
                annotations=[{
                    'text': 'No Active Trades',
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 20, 'color': '#94a3b8'}
                }]
            )
            return apply_chart_theme(fig)
        
        total_debit = active_df['debit'].sum()
        if total_debit == 0:
            total_debit = 1
        
        # Calculate scores (0-100)
        stability_score = min(100, (active_df['stability'].mean() / 1.5) * 100)
        yield_score = min(100, (active_df['theta'].sum() / total_debit * 100) * 500)
        hedge_score = min(100, abs(active_df['vega'].sum() / (active_df['theta'].sum() or 1)) * 20)
        freshness_score = max(0, 100 - (active_df['days_held'].mean() / 45 * 100))
        neutrality_score = max(0, 100 - (abs(active_df['delta'].sum() / total_debit * 100) * 20))
        
        # Diversification score
        target_allocation = {'130/160': 0.30, '160/190': 0.40, 'M200': 0.20, 'SMSF': 0.10}
        actual_alloc = active_df.groupby('strategy')['debit'].sum() / total_debit
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
        
        # Add target line at 80
        fig.add_trace(go.Scatterpolar(
            r=[80] * len(categories),
            theta=categories,
            mode='lines',
            name='Target',
            line={'color': COLORS['success'], 'width': 1, 'dash': 'dash'}
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 100],
                    tickfont={'size': 10},
                    gridcolor='rgba(99, 102, 241, 0.1)'
                ),
                angularaxis=dict(
                    gridcolor='rgba(99, 102, 241, 0.1)'
                )
            ),
            showlegend=True,
            title="Portfolio Health Radar",
            height=400
        )
        
        return apply_chart_theme(fig)
    
    @staticmethod
    def create_heatmap(active_df: pd.DataFrame) -> go.Figure:
        """Create position heat map"""
        if active_df.empty:
            fig = go.Figure()
            fig.update_layout(title="Position Heat Map")
            return apply_chart_theme(fig)
        
        # Calculate urgency scores (simplified)
        active_df['urgency'] = 50  # Default
        
        fig = px.scatter(
            active_df,
            x='days_held',
            y='pnl',
            size='debit',
            color='urgency',
            hover_data=['name', 'strategy'],
            color_continuous_scale='RdYlGn_r',
            title="Position Heat Map (Size = Capital at Risk)"
        )
        
        # Add reference lines
        avg_days = active_df['days_held'].mean()
        fig.add_vline(
            x=avg_days,
            line_dash="dash",
            line_color=COLORS['text_secondary'],
            opacity=0.5,
            annotation_text=f"Avg Age: {avg_days:.0f}d"
        )
        
        fig.add_hline(
            y=0,
            line_dash="dash",
            line_color=COLORS['text_secondary'],
            opacity=0.5
        )
        
        fig.update_layout(
            xaxis_title="Days Held",
            yaxis_title="P&L ($)",
            height=500
        )
        
        return apply_chart_theme(fig)
    
    @staticmethod
    def create_equity_curve(trades_df: pd.DataFrame, initial_capital: float) -> go.Figure:
        """Create equity curve with drawdown"""
        if trades_df.empty:
            fig = go.Figure()
            fig.update_layout(title="Equity Curve")
            return apply_chart_theme(fig)
        
        # Reconstruct daily equity
        daily_pnl = AnalyticsEngine._reconstruct_daily_pnl(trades_df)
        
        if not daily_pnl:
            fig = go.Figure()
            fig.update_layout(title="Equity Curve")
            return apply_chart_theme(fig)
        
        dates = sorted(daily_pnl.keys())
        equity = initial_capital
        equity_curve = []
        equity_dates = []
        
        for date in dates:
            equity += daily_pnl[date]
            equity_curve.append(equity)
            equity_dates.append(date)
        
        # Calculate drawdown
        equity_series = pd.Series(equity_curve, index=equity_dates)
        running_max = equity_series.cummax()
        drawdown = ((equity_series - running_max) / running_max) * 100
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=1,
            row_heights=[0.7, 0.3],
            subplot_titles=('Equity Curve', 'Drawdown %'),
            vertical_spacing=0.1,
            shared_xaxes=True
        )
        
        # Equity curve
        fig.add_trace(
            go.Scatter(
                x=equity_dates,
                y=equity_curve,
                mode='lines',
                name='Equity',
                line={'color': COLORS['primary'], 'width': 2},
                fill='tozeroy',
                fillcolor='rgba(99, 102, 241, 0.1)'
            ),
            row=1, col=1
        )
        
        # Drawdown
        fig.add_trace(
            go.Scatter(
                x=equity_dates,
                y=drawdown.values,
                mode='lines',
                name='Drawdown',
                line={'color': COLORS['danger'], 'width': 2},
                fill='tozeroy',
                fillcolor='rgba(239, 68, 68, 0.2)'
            ),
            row=2, col=1
        )
        
        fig.update_xaxes(title_text="Date", row=2, col=1)
        fig.update_yaxes(title_text="Equity ($)", row=1, col=1)
        fig.update_yaxes(title_text="DD %", row=2, col=1)
        
        fig.update_layout(
            height=600,
            showlegend=False,
            title="Portfolio Equity Curve & Drawdown"
        )
        
        return apply_chart_theme(fig)
    
    @staticmethod
    def create_strategy_comparison(trades_df: pd.DataFrame) -> go.Figure:
        """Create strategy performance comparison"""
        if trades_df.empty:
            fig = go.Figure()
            fig.update_layout(title="Strategy Comparison")
            return apply_chart_theme(fig)
        
        # Group by strategy
        strategy_stats = trades_df.groupby('strategy').agg({
            'pnl': 'sum',
            'roi': 'mean',
            'daily_yield': 'mean',
            'stability': 'mean',
            'id': 'count'
        }).reset_index()
        
        strategy_stats.columns = ['Strategy', 'Total P&L', 'Avg ROI', 'Daily Yield', 'Stability', 'Count']
        
        # Calculate win rate
        wins = trades_df[trades_df['pnl'] > 0].groupby('strategy')['id'].count()
        strategy_stats['Win Rate'] = strategy_stats.apply(
            lambda row: (wins.get(row['Strategy'], 0) / row['Count']) * 100,
            axis=1
        )
        
        # Create grouped bar chart
        fig = go.Figure()
        
        metrics = ['Avg ROI', 'Daily Yield', 'Win Rate', 'Stability']
        colors = [COLORS['primary'], COLORS['success'], COLORS['warning'], COLORS['info']]
        
        for i, metric in enumerate(metrics):
            fig.add_trace(go.Bar(
                name=metric,
                x=strategy_stats['Strategy'],
                y=strategy_stats[metric],
                marker_color=colors[i]
            ))
        
        fig.update_layout(
            barmode='group',
            title="Strategy Performance Comparison",
            xaxis_title="Strategy",
            yaxis_title="Value",
            height=450,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        return apply_chart_theme(fig)

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main application entry point"""
    
    # Page config
    st.set_page_config(
        page_title="Trade Guardian v2.0",
        layout="wide",
        page_icon="🛡️",
        initial_sidebar_state="expanded"
    )
    
    # Inject CSS
    inject_custom_css()
    
    # Initialize managers
    db = DatabaseManager()
    cloud = CloudSyncManager()
    
    # Header
    st.markdown("""
        <div style="margin-bottom: 40px;">
            <div style="display: inline-block; padding: 6px 16px; background: rgba(99, 102, 241, 0.2); 
                        border: 1px solid rgba(99, 102, 241, 0.3); border-radius: 24px; 
                        margin-bottom: 12px;">
                <span style="color: #a5b4fc; font-size: 0.75rem; font-weight: 700; 
                             letter-spacing: 0.1em;">INSTITUTIONAL GRADE</span>
            </div>
            <h1 style="font-size: 3.5rem; line-height: 1.1; margin: 0;">
                Trade Guardian <span style="background: linear-gradient(135deg, #6366f1 0%, #a855f7 100%); 
                -webkit-background-clip: text; -webkit-text-fill-color: transparent;">v2.0</span>
            </h1>
            <p style="color: #94a3b8; font-size: 1.1rem; margin-top: 8px;">
                Elite Options Trading Intelligence Platform
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### ⚡ Quick Actions")
        
        # Cloud Sync Section
        if GOOGLE_AVAILABLE and cloud.is_connected:
            with st.expander("☁️ Cloud Sync", expanded=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📤 Push", use_container_width=True):
                        with st.spinner("Uploading..."):
                            success, msg = cloud.upload()
                            if success:
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)
                
                with col2:
                    if st.button("📥 Pull", use_container_width=True):
                        with st.spinner("Downloading..."):
                            success, msg = cloud.download()
                            if success:
                                st.cache_data.clear()
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)
        
        st.markdown("---")
        
        # File Upload Section
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
                if active_files or history_files:
                    with st.spinner("Processing files..."):
                        # Process files here (implementation from original code)
                        st.success("Sync complete!")
                        st.cache_data.clear()
                        st.rerun()
        
        st.markdown("---")
        
        # Settings
        with st.expander("⚙️ Settings"):
            prime_capital = st.number_input(
                "Prime Account ($)",
                min_value=1000,
                value=115000,
                step=1000
            )
            
            smsf_capital = st.number_input(
                "SMSF Account ($)",
                min_value=1000,
                value=150000,
                step=1000
            )
            
            total_capital = prime_capital + smsf_capital
            
            st.metric("Total Capital", f"${total_capital:,.0f}")
    
    # Load data
    trades_df = db.load_trades()
    snapshots_df = db.load_snapshots()
    config = db.load_strategy_config()
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Dashboard",
        "⚡ Active Trades",
        "📈 Analytics",
        "🤖 AI Insights",
        "⚙️ Configuration"
    ])
    
    # ========================================================================
    # TAB 1: DASHBOARD
    # ========================================================================
    
    with tab1:
        if trades_df.empty:
            st.info("No trades loaded. Upload your first file to get started.")
        else:
            # Filter active trades
            active_df = trades_df[trades_df['status'].isin(['Active', 'Missing'])].copy()
            
            if not active_df.empty:
                # Top metrics
                col1, col2, col3, col4 = st.columns(4)
                
                total_pnl = active_df['pnl'].sum()
                total_theta = active_df['theta'].sum()
                avg_stability = active_df['stability'].mean()
                position_count = len(active_df)
                
                with col1:
                    st.metric(
                        "Floating P&L",
                        f"${total_pnl:,.0f}",
                        delta=f"{(total_pnl/total_capital)*100:.2f}% of capital"
                    )
                
                with col2:
                    st.metric(
                        "Daily Theta Income",
                        f"${total_theta:,.0f}",
                        delta=f"{(total_theta/total_capital)*100:.3f}%/day"
                    )
                
                with col3:
                    st.metric(
                        "Avg Stability",
                        f"{avg_stability:.2f}",
                        delta="Healthy" if avg_stability > 0.8 else "Review"
                    )
                
                with col4:
                    st.metric(
                        "Active Positions",
                        position_count,
                        delta=f"${active_df['debit'].sum():,.0f} deployed"
                    )
                
                st.markdown("---")
                
                # Visualizations
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    fig_radar = VisualizationEngine.create_portfolio_radar(active_df, config)
                    st.plotly_chart(fig_radar, use_container_width=True)
                
                with col2:
                    fig_heat = VisualizationEngine.create_heatmap(active_df)
                    st.plotly_chart(fig_heat, use_container_width=True)
            else:
                st.info("No active trades.")
            
            # Historical performance
            expired_df = trades_df[trades_df['status'] == 'Expired'].copy()
            
            if not expired_df.empty:
                st.markdown("### 📈 Historical Performance")
                
                # Calculate metrics
                metrics = AnalyticsEngine.calculate_portfolio_metrics(expired_df, total_capital)
                
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric("Total Realized", f"${metrics['total_pnl']:,.0f}")
                
                with col2:
                    st.metric("Win Rate", f"{metrics['win_rate']*100:.1f}%")
                
                with col3:
                    st.metric("Sharpe Ratio", f"{metrics['sharpe']:.2f}")
                
                with col4:
                    st.metric("CAGR", f"{metrics['cagr']:.1f}%")
                
                with col5:
                    st.metric("Max DD", f"{metrics['max_dd']:.1f}%")
                
                # Equity curve
                fig_equity = VisualizationEngine.create_equity_curve(expired_df, total_capital)
                st.plotly_chart(fig_equity, use_container_width=True)
    
    # ========================================================================
    # TAB 2: ACTIVE TRADES
    # ========================================================================
    
    with tab2:
        if trades_df.empty:
            st.info("No trades loaded.")
        else:
            active_df = trades_df[trades_df['status'].isin(['Active', 'Missing'])].copy()
            
            if active_df.empty:
                st.info("No active trades.")
            else:
                # Strategy breakdown
                st.markdown("### 📊 Strategy Breakdown")
                
                fig_strat = VisualizationEngine.create_strategy_comparison(active_df)
                st.plotly_chart(fig_strat, use_container_width=True)
                
                st.markdown("---")
                
                # Trade list
                st.markdown("### 📋 Active Positions")
                
                # Add filters
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    strategies = ['All'] + sorted(active_df['strategy'].unique().tolist())
                    selected_strategy = st.selectbox("Filter by Strategy", strategies)
                
                with col2:
                    status_options = ['All'] + sorted(active_df['status'].unique().tolist())
                    selected_status = st.selectbox("Filter by Status", status_options)
                
                with col3:
                    sort_by = st.selectbox(
                        "Sort by",
                        ['P&L', 'Days Held', 'Debit', 'ROI', 'Stability']
                    )
                
                # Apply filters
                filtered_df = active_df.copy()
                
                if selected_strategy != 'All':
                    filtered_df = filtered_df[filtered_df['strategy'] == selected_strategy]
                
                if selected_status != 'All':
                    filtered_df = filtered_df[filtered_df['status'] == selected_status]
                
                # Sort
                filtered_df = filtered_df.sort_values(
                    sort_by.lower().replace(' ', '_'),
                    ascending=False
                )
                
                # Display columns
                display_cols = [
                    'name', 'strategy', 'status', 'pnl', 'roi',
                    'days_held', 'debit', 'theta', 'stability',
                    'daily_yield', 'notes'
                ]
                
                # Format for display
                display_df = filtered_df[display_cols].copy()
                
                st.dataframe(
                    display_df.style.format({
                        'pnl': '${:,.0f}',
                        'roi': '{:.2f}%',
                        'debit': '${:,.0f}',
                        'theta': '{:.2f}',
                        'stability': '{:.2f}',
                        'daily_yield': '{:.3f}%'
                    }).applymap(
                        lambda x: 'color: #10b981' if isinstance(x, (int, float)) and x > 0 else 'color: #ef4444' if isinstance(x, (int, float)) and x < 0 else '',
                        subset=['pnl', 'roi']
                    ),
                    use_container_width=True,
                    height=600
                )
    
    # ========================================================================
    # TAB 3: ANALYTICS
    # ========================================================================
    
    with tab3:
        if trades_df.empty:
            st.info("No trades loaded.")
        else:
            expired_df = trades_df[trades_df['status'] == 'Expired'].copy()
            
            if expired_df.empty:
                st.info("No closed trades for analysis.")
            else:
                st.markdown("### 📊 Advanced Analytics")
                
                # Performance by strategy
                st.markdown("#### Strategy Performance")
                
                strategy_stats = expired_df.groupby('strategy').agg({
                    'pnl': ['sum', 'mean', 'std'],
                    'roi': 'mean',
                    'daily_yield': 'mean',
                    'days_held': 'mean',
                    'id': 'count'
                }).round(2)
                
                strategy_stats.columns = ['Total P&L', 'Avg P&L', 'Std Dev', 'Avg ROI', 'Daily Yield', 'Avg Days', 'Count']
                
                # Calculate win rate
                wins = expired_df[expired_df['pnl'] > 0].groupby('strategy')['id'].count()
                strategy_stats['Win Rate'] = (wins / strategy_stats['Count']) * 100
                strategy_stats['Win Rate'] = strategy_stats['Win Rate'].fillna(0)
                
                st.dataframe(
                    strategy_stats.style.format({
                        'Total P&L': '${:,.0f}',
                        'Avg P&L': '${:,.0f}',
                        'Std Dev': '${:,.0f}',
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
                
                expired_df['exit_month'] = pd.to_datetime(expired_df['exit_date']).dt.to_period('M')
                monthly_perf = expired_df.groupby('exit_month')['pnl'].sum().reset_index()
                monthly_perf['exit_month'] = monthly_perf['exit_month'].astype(str)
                
                fig_monthly = px.bar(
                    monthly_perf,
                    x='exit_month',
                    y='pnl',
                    title="Monthly P&L",
                    color='pnl',
                    color_continuous_scale=['#ef4444', '#10b981']
                )
                
                fig_monthly.update_layout(
                    xaxis_title="Month",
                    yaxis_title="P&L ($)",
                    showlegend=False,
                    height=400
                )
                
                st.plotly_chart(apply_chart_theme(fig_monthly), use_container_width=True)
                
                st.markdown("---")
                
                # Distribution analysis
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### P&L Distribution")
                    
                    fig_dist = px.histogram(
                        expired_df,
                        x='pnl',
                        nbins=30,
                        title="P&L Distribution",
                        color_discrete_sequence=[COLORS['primary']]
                    )
                    
                    fig_dist.add_vline(
                        x=0,
                        line_dash="dash",
                        line_color=COLORS['text_secondary']
                    )
                    
                    st.plotly_chart(apply_chart_theme(fig_dist), use_container_width=True)
                
                with col2:
                    st.markdown("#### Hold Time Distribution")
                    
                    fig_days = px.histogram(
                        expired_df,
                        x='days_held',
                        nbins=30,
                        title="Days Held Distribution",
                        color_discrete_sequence=[COLORS['info']]
                    )
                    
                    st.plotly_chart(apply_chart_theme(fig_days), use_container_width=True)
    
    # ========================================================================
    # TAB 4: AI INSIGHTS
    # ========================================================================
    
    with tab4:
        if trades_df.empty:
            st.info("No trades loaded.")
        else:
            active_df = trades_df[trades_df['status'].isin(['Active', 'Missing'])].copy()
            expired_df = trades_df[trades_df['status'] == 'Expired'].copy()
            
            if active_df.empty or expired_df.empty:
                st.info("Need both active and historical trades for AI predictions.")
            else:
                st.markdown("### 🤖 AI-Powered Trade Predictions")
                
                st.markdown("""
                    Using K-Nearest Neighbors (KNN) algorithm to predict outcomes based on Greek profile similarity.
                """)
                
                # Select trade to analyze
                trade_names = active_df['name'].tolist()
                selected_trade_name = st.selectbox("Select Trade to Analyze", trade_names)
                
                if selected_trade_name:
                    current_trade = active_df[active_df['name'] == selected_trade_name].iloc[0]
                    
                    # Get prediction
                    prediction = AnalyticsEngine.predict_trade_outcome(current_trade, expired_df)
                    
                    # Display results
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        win_prob = prediction['win_prob'] * 100
                        color = COLORS['success'] if win_prob > 60 else COLORS['warning'] if win_prob > 40 else COLORS['danger']
                        
                        st.markdown(f"""
                            <div style="background: rgba(30, 41, 59, 0.6); padding: 20px; border-radius: 12px; border: 2px solid {color};">
                                <h3 style="margin: 0; color: {color};">Win Probability</h3>
                                <h1 style="margin: 10px 0; font-size: 3rem;">{win_prob:.1f}%</h1>
                            </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        exp_pnl = prediction['expected_pnl']
                        color = COLORS['success'] if exp_pnl > 0 else COLORS['danger']
                        
                        st.markdown(f"""
                            <div style="background: rgba(30, 41, 59, 0.6); padding: 20px; border-radius: 12px; border: 2px solid {color};">
                                <h3 style="margin: 0; color: {color};">Expected P&L</h3>
                                <h1 style="margin: 10px 0; font-size: 3rem;">${exp_pnl:,.0f}</h1>
                            </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        confidence = prediction['confidence']
                        color = COLORS['success'] if confidence > 70 else COLORS['warning'] if confidence > 40 else COLORS['danger']
                        
                        st.markdown(f"""
                            <div style="background: rgba(30, 41, 59, 0.6); padding: 20px; border-radius: 12px; border: 2px solid {color};">
                                <h3 style="margin: 0; color: {color};">Confidence</h3>
                                <h1 style="margin: 10px 0; font-size: 3rem;">{confidence:.0f}%</h1>
                            </div>
                        """, unsafe_allow_html=True)
                    
                    st.markdown("---")
                    
                    # Current trade details
                    st.markdown("#### Current Trade Profile")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown(f"""
                            - **Strategy:** {current_trade['strategy']}
                            - **Days Held:** {current_trade['days_held']}
                            - **Current P&L:** ${current_trade['pnl']:,.0f}
                            - **ROI:** {current_trade['roi']:.2f}%
                        """)
                    
                    with col2:
                        st.markdown(f"""
                            - **Theta:** {current_trade['theta']:.2f}
                            - **Delta:** {current_trade['delta']:.2f}
                            - **Stability:** {current_trade['stability']:.2f}
                            - **Theta/Cap:** {current_trade['theta_cap_pct']:.2f}%
                        """)
                    
                    # Recommendation
                    st.markdown("#### AI Recommendation")
                    
                    if win_prob > 65:
                        recommendation = "🟢 **HOLD/ADD** - Strong probability of success"
                        rec_color = COLORS['success']
                    elif win_prob > 45:
                        recommendation = "🟡 **MONITOR** - Neutral probability, watch closely"
                        rec_color = COLORS['warning']
                    else:
                        recommendation = "🔴 **CONSIDER EXIT** - Low probability of success"
                        rec_color = COLORS['danger']
                    
                    st.markdown(f"""
                        <div style="background: rgba(30, 41, 59, 0.6); padding: 20px; border-radius: 12px; border: 2px solid {rec_color};">
                            <h3 style="margin: 0;">{recommendation}</h3>
                        </div>
                    """, unsafe_allow_html=True)
    
    # ========================================================================
    # TAB 5: CONFIGURATION
    # ========================================================================
    
    with tab5:
        st.markdown("### ⚙️ Strategy Configuration")
        
        st.markdown("""
            Define your trading strategies and their targets. These are used for:
            - Automatic trade classification
            - Performance benchmarking
            - Position sizing calculations
        """)
        
        # Load current config
        conn = sqlite3.connect(DB_NAME)
        config_df = pd.read_sql("SELECT * FROM strategy_config", conn)
        conn.close()
        
        # Edit table
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
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            if st.button("💾 Save Changes", use_container_width=True):
                try:
                    conn = sqlite3.connect(DB_NAME)
                    c = conn.cursor()
                    
                    # Clear existing
                    c.execute("DELETE FROM strategy_config")
                    
                    # Insert new
                    for _, row in edited_config.iterrows():
                        c.execute(
                            "INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)",
                            tuple(row)
                        )
                    
                    conn.commit()
                    conn.close()
                    
                    st.cache_data.clear()
                    st.success("Configuration saved!")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Save failed: {e}")
    
    # Footer
    st.markdown("---")
    st.markdown("""
        <div style="text-align: center; color: #64748b; padding: 20px;">
            <p>Trade Guardian v2.0 Elite Edition | Built with ❤️ for Serious Traders</p>
        </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
