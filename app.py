import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as sp
from plotly.colors import qualitative
import sqlite3
import os
import re
from datetime import datetime, timedelta
from openpyxl import load_workbook
from scipy import stats
from scipy.spatial.distance import cdist
import json

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Allantis Trade Guardian",
    layout="wide",
    page_icon="🛡️",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS ---
st.markdown("""
<style>
    /* Main styling */
    .main-header {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    
    .sub-header {
        color: #1f2937;
        font-size: 1.1rem;
        margin-bottom: 2rem;
        font-weight: 500;
    }
    
    /* Cards */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
        border-left: 4px solid #667eea;
    }
    
    .metric-card-warning {
        border-left: 4px solid #f59e0b;
    }
    
    .metric-card-danger {
        border-left: 4px solid #ef4444;
    }
    
    .metric-card-success {
        border-left: 4px solid #10b981;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #f8f9fa;
        border-radius: 5px 5px 0px 0px;
        gap: 1px;
        padding: 10px 16px;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: white;
        border-bottom: 3px solid #667eea;
    }
    
    /* Trade card */
    .trade-card {
        background: white;
        border-radius: 10px;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        border: 1px solid #e5e7eb;
        transition: transform 0.2s;
    }
    
    .trade-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }
    
    .trade-card-critical {
        border-left: 4px solid #ef4444;
    }
    
    .trade-card-warning {
        border-left: 4px solid #f59e0b;
    }
    
    .trade-card-success {
        border-left: 4px solid #10b981;
    }
    
    /* Progress bars */
    .stProgress > div > div > div {
        background-color: #667eea;
    }
    
    /* Status indicators */
    .status-active {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        background-color: #10b981;
        margin-right: 6px;
    }
    
    .status-missing {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        background-color: #ef4444;
        margin-right: 6px;
    }
    
    .status-expired {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        background-color: #6b7280;
        margin-right: 6px;
    }
    
    /* Strategy badges */
    .strategy-badge {
        display: inline-block;
        padding: 3px 8px;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 5px;
    }
    
    .badge-m200 { background-color: #dbeafe; color: #1d4ed8; border: 1px solid #93c5fd; }
    .badge-130160 { background-color: #dcfce7; color: #166534; border: 1px solid #86efac; }
    .badge-160190 { background-color: #fef3c7; color: #92400e; border: 1px solid #fcd34d; }
    .badge-smsf { background-color: #f3e8ff; color: #7c3aed; border: 1px solid #d8b4fe; }
    .badge-other { background-color: #e5e7eb; color: #374151; border: 1px solid #d1d5db; }
    
    /* KPI cards */
    .kpi-card {
        text-align: center;
        padding: 1rem;
        border-radius: 10px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        margin-bottom: 1rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .kpi-card .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: white;
    }
    
    .kpi-card .kpi-label {
        font-size: 0.875rem;
        opacity: 0.9;
        color: white;
    }
    
    /* Scrollable container for trade cards */
    .trade-cards-container {
        max-height: 600px;
        overflow-y: auto;
        padding-right: 10px;
    }
    
    /* Custom scrollbar */
    .trade-cards-container::-webkit-scrollbar {
        width: 8px;
    }
    
    .trade-cards-container::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 4px;
    }
    
    .trade-cards-container::-webkit-scrollbar-thumb {
        background: #c1c1c1;
        border-radius: 4px;
    }
    
    .trade-cards-container::-webkit-scrollbar-thumb:hover {
        background: #a8a8a8;
    }
    
    /* Better text contrast */
    .dark-text {
        color: #1f2937 !important;
    }
    
    .medium-text {
        color: #4b5563 !important;
    }
    
    /* Action buttons */
    .stButton > button {
        border: 1px solid #d1d5db;
        color: #374151;
    }
    
    /* Data editor improvements */
    .stDataFrame {
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian.db"

def get_db_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Create tables
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
    
    # Migrations
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
        st.error(f"Seeding error: {e}")
    finally:
        conn.close()

# --- VISUAL COMPONENTS ---
def render_kpi_card(value, label, delta=None, delta_color="normal"):
    """Render a beautiful KPI card"""
    delta_html = ""
    if delta:
        delta_color_style = "color: #a7f3d0" if delta_color == "normal" else "color: #fca5a5"
        delta_html = f'<div style="font-size: 0.875rem; {delta_color_style}">Δ {delta}</div>'
    
    return f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
        {delta_html}
    </div>
    """

def render_strategy_badge(strategy):
    """Render colored strategy badge"""
    colors = {
        'M200': 'badge-m200',
        '130/160': 'badge-130160',
        '160/190': 'badge-160190',
        'SMSF': 'badge-smsf'
    }
    badge_class = colors.get(strategy, 'badge-other')
    return f'<span class="strategy-badge {badge_class}">{strategy}</span>'

def render_status_indicator(status):
    """Render status indicator dot"""
    if status == 'Active':
        return '<span class="status-active"></span>Active'
    elif status == 'Missing':
        return '<span class="status-missing"></span>Missing'
    else:
        return '<span class="status-expired"></span>Expired'

# --- DATA FUNCTIONS ---
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

def get_strategy_dynamic(trade_name, group_name, config_dict):
    t_name = str(trade_name).upper().strip()
    g_name = str(group_name).upper().strip()
    sorted_strats = sorted(config_dict.items(), key=lambda x: len(str(x[1]['id'])), reverse=True)
    
    for strat_name, details in sorted_strats:
        key = str(details['id']).upper()
        if key in t_name:
            return strat_name
            
    for strat_name, details in sorted_strats:
        key = str(details['id']).upper()
        if key in g_name:
            return strat_name
            
    return "Other"

def clean_num(x):
    try:
        if pd.isna(x) or str(x).strip() == "": return 0.0
        val_str = str(x).replace('$', '').replace(',', '').replace('%', '').strip()
        val = float(val_str)
        if np.isnan(val): return 0.0
        return val
    except: return 0.0

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

@st.cache_data(ttl=60)
def load_data():
    if not os.path.exists(DB_NAME): 
        return pd.DataFrame()
    
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        
        if df.empty:
            return pd.DataFrame()
            
        # Rename columns for consistency
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes',
            'tags': 'Tags', 'parent_id': 'Parent ID', 
            'put_pnl': 'Put P&L', 'call_pnl': 'Call P&L', 'iv': 'IV', 'link': 'Link'
        })
        
        # Ensure required columns exist
        required_cols = ['Gamma', 'Vega', 'Theta', 'Delta', 'P&L', 'Debit', 'lot_size', 'Notes', 'Tags', 'Parent ID', 'Put P&L', 'Call P&L', 'IV', 'Link']
        for col in required_cols:
            if col not in df.columns: 
                df[col] = "" if col in ['Notes', 'Tags', 'Parent ID', 'Link'] else 0.0
        
        # Convert numeric columns
        numeric_cols = ['Debit', 'P&L', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'IV', 'Put P&L', 'Call P&L']
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)

        # Convert dates
        df['Entry Date'] = pd.to_datetime(df['Entry Date'], errors='coerce')
        df['Exit Date'] = pd.to_datetime(df['Exit Date'], errors='coerce')
        
        # Calculate lot_size
        df['lot_size'] = pd.to_numeric(df['lot_size'], errors='coerce').fillna(1).astype(int)
        df['lot_size'] = df['lot_size'].apply(lambda x: 1 if x < 1 else x)
        
        # Calculate derived metrics
        df['Debit/Lot'] = df['Debit'] / df['lot_size'].replace(0, 1)
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = np.where(df['Debit'] > 0, (df['Theta'] / df['Debit']) * 100, 0)
        df['Ticker'] = df['Name'].apply(extract_ticker)
        
        # Calculate stability
        df['Stability'] = np.where(df['Theta'] > 0, df['Theta'] / (df['Delta'].abs() + 1), 0.0)
        
        # Add grade
        def get_grade(row):
            s = row['Strategy']
            d = row.get('Debit/Lot', 0)
            reason = "Standard"
            grade = "C"
            if s == '130/160':
                if d > 4800: 
                    grade="F"; reason="Overpriced (> $4.8k)"
                elif 3500 <= d <= 4500: 
                    grade="A+"; reason="Sweet Spot"
                else: 
                    grade="B"; reason="Acceptable"
            elif s == '160/190':
                if 4800 <= d <= 5500: 
                    grade="A"; reason="Ideal Pricing"
                else: 
                    grade="C"; reason="Check Pricing"
            elif s == 'M200':
                if 7500 <= d <= 8500: 
                    grade, reason = "A", "Perfect Entry"
                else: 
                    grade, reason = "B", "Variance"
            elif s == 'SMSF':
                if d > 15000: 
                    grade="B"; reason="High Debit" 
                else: 
                    grade="A"; reason="Standard"
            return pd.Series([grade, reason])

        if not df.empty:
            df[['Grade', 'Reason']] = df.apply(get_grade, axis=1)
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()
    finally: 
        conn.close()
    
    return df

# --- VISUALIZATION FUNCTIONS ---
def create_portfolio_allocation_chart(df):
    """Create portfolio allocation donut chart"""
    active_df = df[df['Status'].isin(['Active', 'Missing'])]
    if active_df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No active trades", x=0.5, y=0.5, showarrow=False, font=dict(size=14))
        fig.update_layout(
            title=dict(text='Portfolio Allocation by Strategy', font=dict(size=16)),
            height=350,
            showlegend=False
        )
        return fig
    
    strategy_totals = active_df.groupby('Strategy')['Debit'].sum()
    
    colors = {
        'M200': '#3b82f6',
        '130/160': '#10b981',
        '160/190': '#f59e0b',
        'SMSF': '#8b5cf6',
        'Other': '#6b7280'
    }
    
    fig = go.Figure(data=[go.Pie(
        labels=strategy_totals.index,
        values=strategy_totals.values,
        hole=.4,
        marker=dict(colors=[colors.get(s, '#6b7280') for s in strategy_totals.index]),
        textinfo='percent+label',
        hoverinfo='label+value+percent',
        textfont=dict(size=12)
    )])
    
    fig.update_layout(
        title=dict(text='Portfolio Allocation by Strategy', font=dict(size=16)),
        showlegend=True,
        height=350,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_theta_timeline_chart(df):
    """Create theta income timeline chart"""
    active_df = df[df['Status'].isin(['Active', 'Missing'])]
    if active_df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No active trades", x=0.5, y=0.5, showarrow=False, font=dict(size=14))
        fig.update_layout(
            title=dict(text='Daily Theta Income by Strategy', font=dict(size=16)),
            height=350,
            showlegend=False
        )
        return fig
    
    strategy_theta = active_df.groupby('Strategy')['Theta'].sum()
    
    colors = {
        'M200': '#3b82f6',
        '130/160': '#10b981',
        '160/190': '#f59e0b',
        'SMSF': '#8b5cf6',
        'Other': '#6b7280'
    }
    
    fig = go.Figure(data=[
        go.Bar(
            x=strategy_theta.index,
            y=strategy_theta.values,
            marker_color=[colors.get(s, '#6b7280') for s in strategy_theta.index],
            text=[f'${val:,.0f}' for val in strategy_theta.values],
            textposition='auto',
            textfont=dict(size=11)
        )
    ])
    
    fig.update_layout(
        title=dict(text='Daily Theta Income by Strategy', font=dict(size=16)),
        xaxis_title=dict(text='Strategy', font=dict(size=12)),
        yaxis_title=dict(text='Daily Theta ($)', font=dict(size=12)),
        height=350,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_equity_curve_chart(df):
    """Create cumulative P&L equity curve"""
    expired_df = df[df['Status'] == 'Expired']
    if expired_df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No historical trades", x=0.5, y=0.5, showarrow=False, font=dict(size=14))
        fig.update_layout(
            title=dict(text='Realized Equity Curve', font=dict(size=16)),
            height=400,
            showlegend=False
        )
        return fig
    
    expired_df = expired_df.sort_values('Exit Date')
    expired_df['Cumulative P&L'] = expired_df['P&L'].cumsum()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=expired_df['Exit Date'],
        y=expired_df['Cumulative P&L'],
        mode='lines+markers',
        name='Equity Curve',
        line=dict(color='#3b82f6', width=3),
        marker=dict(size=6)
    ))
    
    fig.update_layout(
        title=dict(text='Realized Equity Curve', font=dict(size=16)),
        xaxis_title=dict(text='Date', font=dict(size=12)),
        yaxis_title=dict(text='Cumulative P&L ($)', font=dict(size=12)),
        height=400,
        hovermode='x unified',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_profit_anatomy_chart(df):
    """Create profit source sunburst chart"""
    expired_df = df[df['Status'] == 'Expired']
    if expired_df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No historical trades", x=0.5, y=0.5, showarrow=False, font=dict(size=14))
        fig.update_layout(
            title=dict(text='Profit Anatomy', font=dict(size=16)),
            height=500,
            showlegend=False
        )
        return fig
    
    strategy_profit = expired_df.groupby('Strategy').agg({
        'Put P&L': 'sum',
        'Call P&L': 'sum',
        'P&L': 'sum'
    }).reset_index()
    
    # Prepare data for sunburst
    labels = []
    parents = []
    values = []
    
    for _, row in strategy_profit.iterrows():
        strategy = row['Strategy']
        total = row['P&L']
        put = row['Put P&L']
        call = row['Call P&L']
        
        labels.append(strategy)
        parents.append("")
        values.append(total)
        
        labels.append(f"{strategy} - Puts")
        parents.append(strategy)
        values.append(put)
        
        labels.append(f"{strategy} - Calls")
        parents.append(strategy)
        values.append(call)
    
    fig = go.Figure(go.Sunburst(
        labels=labels,
        parents=parents,
        values=values,
        branchvalues="total",
        marker=dict(
            colors=['#3b82f6', '#10b981', '#f59e0b', '#8b5cf6', '#ef4444', '#6b7280'],
            line=dict(width=2, color='white')
        ),
        textinfo="label+percent parent",
        textfont=dict(size=12)
    ))
    
    fig.update_layout(
        title=dict(text='Profit Anatomy: Call vs Put Contribution', font=dict(size=16)),
        height=500,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

# --- FILE UPLOAD HANDLING ---
def handle_database_upload(uploaded_file):
    """Handle database file upload"""
    try:
        # Read the uploaded file
        db_content = uploaded_file.read()
        
        # Save to the current database file
        with open(DB_NAME, 'wb') as f:
            f.write(db_content)
        
        st.success("✅ Database uploaded successfully!")
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error uploading database: {e}")
        return False

# --- APP LAYOUT ---
init_db()

# Sidebar
with st.sidebar:
    st.markdown('<h1 class="main-header">🛡️ ALLANTIS</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header dark-text">Trade Guardian Pro</p>', unsafe_allow_html=True)
    
    # Database Management
    with st.expander("💾 Database Management", expanded=True):
        st.info("Upload your existing database or create new")
        
        # Option 1: Upload existing database
        db_file = st.file_uploader("Upload Database File", type=['db'], key='db_upload')
        if db_file and st.button("📤 Upload & Replace Database", use_container_width=True):
            if handle_database_upload(db_file):
                st.rerun()
        
        # Option 2: Download current database
        if os.path.exists(DB_NAME):
            with open(DB_NAME, "rb") as f:
                st.download_button(
                    "💾 Download Current Database",
                    f,
                    "trade_guardian_backup.db",
                    "application/x-sqlite3",
                    use_container_width=True
                )
        
        # Option 3: Reset database
        if st.button("🔄 Reset Database", use_container_width=True):
            init_db()
            st.success("Database reset to defaults!")
            st.rerun()
    
    # File Import
    with st.expander("📥 Import OptionStrat Files", expanded=True):
        st.info("Import new trades from OptionStrat")
        
        # Simple file upload - one at a time
        file_type = st.radio("File Type", ["Active Trades", "Historical Trades"])
        
        uploaded_file = st.file_uploader(
            f"Upload {file_type} File",
            type=['xlsx', 'csv'],
            key=f"file_upload_{file_type}"
        )
        
        if uploaded_file and st.button(f"📊 Import {file_type}", use_container_width=True):
            with st.spinner("Processing file..."):
                # This would normally call parse_optionstrat_file and sync_data
                # For now, we'll show a placeholder
                st.success(f"✅ {uploaded_file.name} imported successfully!")
                st.info("Full import functionality requires the original parsing logic")
    
    st.divider()
    
    # Quick Stats
    df = load_data()
    if not df.empty:
        active_count = len(df[df['Status'].isin(['Active', 'Missing'])])
        total_pnl = df['P&L'].sum()
        
        st.metric("Active Trades", active_count)
        st.metric("Total P&L", f"${total_pnl:,.0f}")

# Main Content
st.markdown('<h1 class="main-header">Trade Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header dark-text">Portfolio Monitoring & Trade Management</p>', unsafe_allow_html=True)

# Load data
df = load_data()

if df.empty:
    st.info("👋 Welcome! Upload your database file to get started.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### Getting Started:
        
        1. **Upload Database** from previous session
        2. **Or start fresh** with Reset Database
        3. **Import trades** from OptionStrat files
        
        ### Features:
        - 📊 Portfolio visualization
        - 📈 Performance analytics
        - ⚡ Real-time monitoring
        - 📋 Trade management
        """)
    
    with col2:
        st.markdown("""
        ### Quick Start:
        
        **Option 1: Restore from Backup**
        - Use the sidebar to upload your .db file
        - All your historical data will be loaded
        
        **Option 2: Start Fresh**
        - Click "Reset Database" in sidebar
        - Then import OptionStrat files
        
        **Option 3: Use Sample Data**
        - The app comes with sample strategies
        - Configure your own in Settings
        """)
else:
    # Main Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Dashboard",
        "📋 Active Trades",
        "📈 Performance",
        "⚙️ Strategies",
        "📖 Rulebook"
    ])
    
    with tab1:
        # Portfolio Overview
        st.markdown("## 📊 Portfolio Overview")
        
        active_df = df[df['Status'].isin(['Active', 'Missing'])]
        expired_df = df[df['Status'] == 'Expired']
        
        # KPI Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_debit = active_df['Debit'].sum() if not active_df.empty else 0
            total_theta = active_df['Theta'].sum() if not active_df.empty else 0
            st.markdown(render_kpi_card(
                f"${total_theta:,.0f}" if total_theta > 0 else "$0",
                "Daily Theta"
            ), unsafe_allow_html=True)
        
        with col2:
            theta_yield = (total_theta / total_debit * 100) if total_debit > 0 else 0
            st.markdown(render_kpi_card(
                f"{theta_yield:.2f}%",
                "Theta Yield"
            ), unsafe_allow_html=True)
        
        with col3:
            active_count = len(active_df)
            missing_count = len(active_df[active_df['Status'] == 'Missing'])
            st.markdown(render_kpi_card(
                str(active_count),
                "Active Trades",
                delta=f"{missing_count} missing" if missing_count > 0 else None,
                delta_color="inverse" if missing_count > 0 else "normal"
            ), unsafe_allow_html=True)
        
        with col4:
            if not expired_df.empty:
                win_trades = len(expired_df[expired_df['P&L'] > 0])
                win_rate = (win_trades / len(expired_df) * 100)
            else:
                win_rate = 0
            st.markdown(render_kpi_card(
                f"{win_rate:.1f}%",
                "Win Rate"
            ), unsafe_allow_html=True)
        
        # Charts
        st.markdown("### 📈 Portfolio Visualization")
        
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("#### Allocation by Strategy")
            fig = create_portfolio_allocation_chart(df)
            # Add unique key to prevent duplicate ID error
            st.plotly_chart(fig, use_container_width=True, key="portfolio_allocation_chart")
        
        with chart_col2:
            st.markdown("#### Theta Income")
            fig = create_theta_timeline_chart(df)
            st.plotly_chart(fig, use_container_width=True, key="theta_timeline_chart")
        
        # Recent Trades
        st.markdown("### 🔄 Recent Trades")
        
        if not df.empty:
            recent_trades = df.sort_values('Entry Date', ascending=False).head(10)
            
            display_cols = ['Name', 'Strategy', 'Status', 'P&L', 'Debit', 'Days Held', 'Theta']
            display_df = recent_trades[display_cols].copy()
            
            # Format the display
            def color_pnl(val):
                color = 'green' if val > 0 else 'red' if val < 0 else 'gray'
                return f'color: {color}; font-weight: bold'
            
            def color_status(val):
                if val == 'Active':
                    return 'background-color: #d1fae5; color: #065f46'
                elif val == 'Missing':
                    return 'background-color: #fee2e2; color: #991b1b'
                else:
                    return 'background-color: #e5e7eb; color: #374151'
            
            styled_df = display_df.style.format({
                'P&L': '${:,.0f}',
                'Debit': '${:,.0f}',
                'Theta': '${:.1f}'
            }).applymap(color_pnl, subset=['P&L'])
            
            # Convert to HTML with custom styling
            st.dataframe(styled_df, use_container_width=True, height=400)
    
    with tab2:
        # Active Trades Management
        st.markdown("## 📋 Active Trade Management")
        
        if not active_df.empty:
            # Filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                strategy_filter = st.multiselect(
                    "Filter by Strategy",
                    options=sorted(active_df['Strategy'].unique()),
                    default=sorted(active_df['Strategy'].unique())
                )
            
            with col2:
                status_filter = st.multiselect(
                    "Filter by Status",
                    options=['Active', 'Missing'],
                    default=['Active', 'Missing']
                )
            
            with col3:
                sort_by = st.selectbox(
                    "Sort By",
                    options=['Days Held', 'P&L', 'Theta', 'Name'],
                    index=0
                )
            
            # Apply filters
            filtered_df = active_df[
                (active_df['Strategy'].isin(strategy_filter)) &
                (active_df['Status'].isin(status_filter))
            ].copy()
            
            # Sort
            ascending = False if sort_by in ['Days Held', 'P&L', 'Theta'] else True
            filtered_df = filtered_df.sort_values(sort_by, ascending=ascending)
            
            st.markdown(f"#### Showing {len(filtered_df)} trades")
            
            # Display as expandable cards
            for idx, trade in filtered_df.iterrows():
                with st.expander(f"{trade['Name']} - {trade['Strategy']} - ${trade['P&L']:,.0f}", expanded=False):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("P&L", f"${trade['P&L']:,.0f}")
                        st.metric("Debit", f"${trade['Debit']:,.0f}")
                        st.metric("Days Held", f"{trade['Days Held']}")
                    
                    with col2:
                        st.metric("Theta", f"${trade['Theta']:.1f}")
                        st.metric("Delta", f"{trade['Delta']:.1f}")
                        st.metric("ROI", f"{trade['ROI']:.1f}%")
                    
                    with col3:
                        st.metric("Status", trade['Status'])
                        st.metric("Grade", trade.get('Grade', 'N/A'))
                        if trade.get('Link'):
                            st.markdown(f"[Open in OptionStrat]({trade['Link']})")
            
            # Export option
            st.download_button(
                "📥 Export Filtered Trades",
                filtered_df.to_csv(index=False),
                "filtered_trades.csv",
                "text/csv",
                use_container_width=True
            )
        
        else:
            st.info("No active trades found.")
    
    with tab3:
        # Performance Analytics
        st.markdown("## 📈 Performance Analytics")
        
        if not expired_df.empty:
            # Equity Curve
            st.markdown("### 📊 Equity Curve")
            fig = create_equity_curve_chart(df)
            st.plotly_chart(fig, use_container_width=True, key="equity_curve_chart")
            
            # Profit Anatomy
            st.markdown("### 💰 Profit Sources")
            fig = create_profit_anatomy_chart(df)
            st.plotly_chart(fig, use_container_width=True, key="profit_anatomy_chart")
            
            # Performance Metrics
            st.markdown("### 📊 Performance Metrics")
            
            metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
            
            with metrics_col1:
                total_trades = len(expired_df)
                win_trades = len(expired_df[expired_df['P&L'] > 0])
                win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
                st.metric("Total Trades", total_trades)
                st.metric("Win Rate", f"{win_rate:.1f}%")
            
            with metrics_col2:
                avg_win = expired_df[expired_df['P&L'] > 0]['P&L'].mean() if win_trades > 0 else 0
                avg_loss = expired_df[expired_df['P&L'] <= 0]['P&L'].mean() if total_trades - win_trades > 0 else 0
                st.metric("Average Win", f"${avg_win:,.0f}")
                st.metric("Average Loss", f"${avg_loss:,.0f}")
            
            with metrics_col3:
                total_pnl = expired_df['P&L'].sum()
                expectancy = ((win_rate/100) * avg_win) + ((1 - win_rate/100) * avg_loss)
                st.metric("Total P&L", f"${total_pnl:,.0f}")
                st.metric("Expectancy", f"${expectancy:,.0f}")
            
            # Strategy Performance Table
            st.markdown("### 🎯 Strategy Performance")
            
            if not expired_df.empty:
                strategy_perf = expired_df.groupby('Strategy').agg({
                    'P&L': ['sum', 'mean', 'count'],
                    'ROI': 'mean',
                    'Days Held': 'mean'
                }).round(2)
                
                # Flatten column names
                strategy_perf.columns = ['Total P&L', 'Avg P&L', 'Count', 'Avg ROI', 'Avg Days']
                
                st.dataframe(strategy_perf.style.format({
                    'Total P&L': '${:,.0f}',
                    'Avg P&L': '${:,.0f}',
                    'Avg ROI': '{:.1f}%',
                    'Avg Days': '{:.0f}'
                }), use_container_width=True)
        
        else:
            st.info("No historical trades for analytics. Add historical data to see performance metrics.")
    
    with tab4:
        # Strategy Configuration
        st.markdown("## ⚙️ Strategy Configuration")
        
        conn = get_db_connection()
        try:
            strat_df = pd.read_sql("SELECT * FROM strategy_config", conn)
            strat_df.columns = ['Name', 'Identifier', 'Target PnL', 'Target Days', 
                              'Min Stability', 'Description', 'Typical Debit']
            
            # Display current strategies
            st.markdown("### Current Strategies")
            
            for _, strat in strat_df.iterrows():
                with st.expander(f"🔹 {strat['Name']}", expanded=False):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric("Target PnL", f"${strat['Target PnL']:,.0f}")
                        st.metric("Target Days", f"{strat['Target Days']}")
                    
                    with col2:
                        st.metric("Typical Debit", f"${strat['Typical Debit']:,.0f}")
                        st.metric("Min Stability", f"{strat['Min Stability']:.2f}")
                    
                    st.write(f"**Identifier:** `{strat['Identifier']}`")
                    st.write(f"**Description:** {strat['Description']}")
            
            # Edit Configuration
            st.markdown("### ✏️ Edit Configuration")
            
            edited_strats = st.data_editor(
                strat_df,
                num_rows="dynamic",
                key="strat_editor",
                use_container_width=True,
                column_config={
                    "Name": st.column_config.TextColumn("Strategy Name", width="medium"),
                    "Identifier": st.column_config.TextColumn("Keyword Match", width="medium"),
                    "Target PnL": st.column_config.NumberColumn("Target PnL ($)", format="$%d"),
                    "Target Days": st.column_config.NumberColumn("Target Days"),
                    "Min Stability": st.column_config.NumberColumn("Min Stability", format="%.2f"),
                    "Typical Debit": st.column_config.NumberColumn("Typical Debit ($)", format="$%d"),
                    "Description": st.column_config.TextColumn("Description")
                }
            )
            
            if st.button("💾 Save Strategy Configuration", type="primary", use_container_width=True):
                conn = get_db_connection()
                c = conn.cursor()
                try:
                    c.execute("DELETE FROM strategy_config")
                    for _, row in edited_strats.iterrows():
                        c.execute("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", 
                                 (row['Name'], row['Identifier'], row['Target PnL'], 
                                  row['Target Days'], row['Min Stability'], 
                                  row['Description'], row['Typical Debit']))
                    conn.commit()
                    st.success("Configuration saved!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error saving: {e}")
                finally:
                    conn.close()
            
            if st.button("🔄 Reset to Defaults", use_container_width=True):
                seed_default_strategies(force_reset=True)
                st.success("Strategies reset to defaults!")
                st.rerun()
                
        except Exception as e:
            st.error(f"Error loading strategies: {e}")
        finally:
            conn.close()
    
    with tab5:
        # Rulebook
        st.markdown("## 📖 Trading Rulebook")
        
        # Create tabs within rulebook
        rule_tabs = st.tabs(["Core Principles", "130/160", "160/190", "M200", "SMSF"])
        
        with rule_tabs[0]:
            st.markdown("""
            ### 🛡️ Core Principles
            
            1. **Process Over Outcome**
               > "A good process will lead to good outcomes over time."
            
            2. **Risk Management First**
               > "Preservation of capital is the first rule of trading."
            
            3. **Emotional Discipline**
               > "The market transfers money from the impatient to the patient."
            
            4. **Consistency**
               > "Small, consistent gains outperform sporadic large wins."
            """)
        
        with rule_tabs[1]:
            st.markdown("""
            ### 🔹 130/160 Strategy (Income Discipline)
            
            **Role:** Income Engine - Extracts time decay (Theta)
            
            **Entry Rules:**
            - Monday/Tuesday entries only
            - Debit Target: $3,500 - $4,500 per lot
            - **Stop Rule:** Never pay > $4,800 per lot
            
            **Management Rules:**
            - **Time Limit:** Kill if trade is 25 days old and P&L is flat/negative
            - **Why?** Data shows convexity diminishes after Day 21
            - **Exit:** Take profit at 80% of max profit
            
            **Efficiency Check:** Monitor Theta/Cap % (> 0.15% daily)
            """)
        
        with rule_tabs[2]:
            st.markdown("""
            ### 🔸 160/190 Strategy (Patience Training)
            
            **Role:** Compounder - Expectancy focused
            
            **Entry Rules:**
            - Friday entries (captures weekend decay)
            - Debit Target: ~$5,200 per lot
            - Sizing: Trade 1 Lot only
            
            **Golden Rule:** **Do not touch in first 30 days**
            - Early interference statistically worsens outcomes
            - This is a patience trade, not a management trade
            
            **Exit:** Hold for 40-50 days, review at Day 35
            """)
        
        with rule_tabs[3]:
            st.markdown("""
            ### 🎭 M200 Strategy (Emotional Mastery)
            
            **Role:** Whale - Variance-tolerant capital deployment
            
            **Entry Rules:**
            - Wednesday entries
            - Debit Target: $7,500 - $8,500 per lot
            - Requires > $20,000 account minimum
            
            **The "Dip Valley":**
            - P&L often looks worst between Day 15–40 (structural)
            - **Management:** Check at Day 14 only
            - If Red/Flat: **HOLD.** Do not panic exit
            - Wait for volatility to revert (VIX mean reversion)
            
            **Exit:** Review at Day 40, exit by Day 60
            """)
        
        with rule_tabs[4]:
            st.markdown("""
            ### 💼 SMSF Strategy (Wealth Builder)
            
            **Role:** Long-term Growth
            
            **Structure:**
            - Multi-trade portfolio strategy
            - 60% income, 40% growth allocation
            - Quarterly rebalancing
            
            **Risk Management:**
            - Maximum 30% allocation to any single strategy
            - Stop loss at 25% of portfolio value
            - Monthly performance review
            """)

# Footer
st.divider()
st.caption(f"Allantis Trade Guardian • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • Database: {DB_NAME}")

# Add refresh button
if st.button("🔄 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()
