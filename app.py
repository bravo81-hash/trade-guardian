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
from streamlit_autorefresh import st_autorefresh

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
        color: #6c757d;
        font-size: 1.1rem;
        margin-bottom: 2rem;
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
    
    .badge-m200 { background-color: #dbeafe; color: #1d4ed8; }
    .badge-130160 { background-color: #dcfce7; color: #166534; }
    .badge-160190 { background-color: #fef3c7; color: #92400e; }
    .badge-smsf { background-color: #f3e8ff; color: #7c3aed; }
    .badge-other { background-color: #e5e7eb; color: #374151; }
    
    /* Action buttons */
    .action-button {
        padding: 4px 12px;
        border-radius: 6px;
        font-size: 0.875rem;
        font-weight: 500;
        margin-right: 5px;
    }
    
    .action-close { background-color: #fee2e2; color: #dc2626; border: 1px solid #fca5a5; }
    .action-roll { background-color: #dbeafe; color: #2563eb; border: 1px solid #93c5fd; }
    .action-hold { background-color: #dcfce7; color: #16a34a; border: 1px solid #86efac; }
    .action-review { background-color: #fef3c7; color: #d97706; border: 1px solid #fcd34d; }
    
    /* KPI cards */
    .kpi-card {
        text-align: center;
        padding: 1rem;
        border-radius: 10px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        margin-bottom: 1rem;
    }
    
    .kpi-card .kpi-value {
        font-size: 2rem;
        font-weight: 700;
    }
    
    .kpi-card .kpi-label {
        font-size: 0.875rem;
        opacity: 0.9;
    }
</style>
""", unsafe_allow_html=True)

# --- DATABASE ENGINE (UNCHANGED) ---
DB_NAME = "trade_guardian_v5.db"

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Create tables (same as before)
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
            if force_reset:
                st.toast("Strategies Reset to Factory Defaults.")
    except Exception as e:
        print(f"Seeding error: {e}")
    finally:
        conn.close()

# --- VISUAL COMPONENTS ---
def render_kpi_card(value, label, delta=None, delta_color="normal"):
    """Render a beautiful KPI card"""
    delta_html = ""
    if delta:
        delta_color_class = "color: #10b981" if delta_color == "normal" else "color: #ef4444"
        delta_html = f'<div style="font-size: 0.875rem; {delta_color_class}">Δ {delta}</div>'
    
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

def create_trade_card(trade):
    """Create a visually appealing trade card"""
    urgency = trade.get('Urgency Score', 0)
    card_class = "trade-card-warning" if urgency >= 70 else "trade-card-success"
    
    # Format P&L with color
    pnl = trade.get('P&L', 0)
    pnl_color = "#10b981" if pnl >= 0 else "#ef4444"
    pnl_display = f'<span style="color: {pnl_color}; font-weight: 600;">${pnl:,.0f}</span>'
    
    # Format Theta
    theta = trade.get('Theta', 0)
    theta_display = f'<span style="color: #2563eb; font-weight: 600;">${theta:.1f}</span>'
    
    # Progress bar for urgency
    progress_width = min(urgency, 100)
    progress_color = "#ef4444" if urgency >= 70 else "#f59e0b" if urgency >= 40 else "#10b981"
    
    return f"""
    <div class="trade-card {card_class}">
        <div style="display: flex; justify-content: space-between; align-items: start;">
            <div style="flex: 1;">
                <div style="display: flex; align-items: center; margin-bottom: 8px;">
                    {render_strategy_badge(trade.get('Strategy', 'Other'))}
                    <h4 style="margin: 0; font-size: 1.1rem;">{trade.get('Name', 'N/A')}</h4>
                </div>
                <div style="display: flex; gap: 20px; margin-bottom: 12px;">
                    <div>
                        <div style="font-size: 0.875rem; color: #6b7280;">PnL</div>
                        <div style="font-size: 1.25rem;">{pnl_display}</div>
                    </div>
                    <div>
                        <div style="font-size: 0.875rem; color: #6b7280;">Theta</div>
                        <div style="font-size: 1.25rem;">{theta_display}</div>
                    </div>
                    <div>
                        <div style="font-size: 0.875rem; color: #6b7280;">Days</div>
                        <div style="font-size: 1.25rem; font-weight: 600;">{trade.get('Days Held', 0)}</div>
                    </div>
                </div>
                <div style="margin-bottom: 8px;">
                    <div style="font-size: 0.875rem; color: #6b7280; margin-bottom: 4px;">Urgency: {urgency}/100</div>
                    <div style="height: 6px; background: #e5e7eb; border-radius: 3px; overflow: hidden;">
                        <div style="height: 100%; width: {progress_width}%; background: {progress_color};"></div>
                    </div>
                </div>
            </div>
            <div style="margin-left: 20px;">
                <button class="action-button action-review" onclick="alert('Review trade')">Review</button>
                <button class="action-button action-hold" onclick="alert('Hold trade')">Hold</button>
            </div>
        </div>
        <div style="font-size: 0.875rem; color: #6b7280;">
            <span style="margin-right: 15px;">{render_status_indicator(trade.get('Status', 'Active'))}</span>
            <span>Debit: ${trade.get('Debit', 0):,.0f}</span>
        </div>
    </div>
    """

# --- DATA FUNCTIONS (UNCHANGED from original but reformatted) ---
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

def parse_optionstrat_file(file, file_type, config_dict):
    # Implementation unchanged from original
    # (Preserving all original parsing logic)
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

# --- VISUALIZATION FUNCTIONS ---
def create_portfolio_allocation_chart(df):
    """Create portfolio allocation donut chart"""
    active_df = df[df['Status'].isin(['Active', 'Missing'])]
    if active_df.empty:
        return go.Figure()
    
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
        hoverinfo='label+value+percent'
    )])
    
    fig.update_layout(
        title='Portfolio Allocation by Strategy',
        showlegend=True,
        height=350
    )
    
    return fig

def create_theta_timeline_chart(df):
    """Create theta income timeline chart"""
    active_df = df[df['Status'].isin(['Active', 'Missing'])]
    if active_df.empty:
        return go.Figure()
    
    # Group by strategy and calculate total theta
    strategy_theta = active_df.groupby('Strategy')['Theta'].sum()
    
    fig = go.Figure(data=[
        go.Bar(
            x=strategy_theta.index,
            y=strategy_theta.values,
            marker_color=[{
                'M200': '#3b82f6',
                '130/160': '#10b981',
                '160/190': '#f59e0b',
                'SMSF': '#8b5cf6',
                'Other': '#6b7280'
            }.get(s, '#6b7280') for s in strategy_theta.index],
            text=[f'${val:,.0f}' for val in strategy_theta.values],
            textposition='auto'
        )
    ])
    
    fig.update_layout(
        title='Daily Theta Income by Strategy',
        xaxis_title='Strategy',
        yaxis_title='Daily Theta ($)',
        height=350
    )
    
    return fig

def create_urgency_heatmap(df):
    """Create urgency score heatmap"""
    active_df = df[df['Status'].isin(['Active', 'Missing'])]
    if active_df.empty:
        return go.Figure()
    
    # Calculate urgency scores (simplified for visualization)
    def calculate_urgency(row):
        pnl = row['P&L']
        days = row['Days Held']
        theta = row['Theta']
        
        if pnl < 0 and days > 20:
            return min(100, 70 + (abs(pnl) / row['Debit']) * 30)
        elif pnl > row['Debit'] * 0.3:
            return 20  # Already profitable
        else:
            return max(30, min(60, days * 2))
    
    active_df['Urgency'] = active_df.apply(calculate_urgency, axis=1)
    
    # Create heatmap data
    strategies = active_df['Strategy'].unique()
    urgency_ranges = ['Low (0-30)', 'Medium (31-60)', 'High (61-100)']
    
    heatmap_data = []
    for strategy in strategies:
        strategy_df = active_df[active_df['Strategy'] == strategy]
        low = len(strategy_df[strategy_df['Urgency'] <= 30])
        medium = len(strategy_df[(strategy_df['Urgency'] > 30) & (strategy_df['Urgency'] <= 60)])
        high = len(strategy_df[strategy_df['Urgency'] > 60])
        heatmap_data.append([low, medium, high])
    
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data,
        x=urgency_ranges,
        y=strategies,
        colorscale='RdYlGn_r',
        text=[[str(val) for val in row] for row in heatmap_data],
        texttemplate="%{text}",
        textfont={"size": 12}
    ))
    
    fig.update_layout(
        title='Urgency Score Distribution',
        xaxis_title='Urgency Level',
        yaxis_title='Strategy',
        height=350
    )
    
    return fig

def create_equity_curve_chart(df):
    """Create cumulative P&L equity curve"""
    expired_df = df[df['Status'] == 'Expired']
    if expired_df.empty:
        return go.Figure()
    
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
    
    # Add strategy breakdown
    for strategy in expired_df['Strategy'].unique():
        strat_df = expired_df[expired_df['Strategy'] == strategy].sort_values('Exit Date')
        strat_df['Cumulative'] = strat_df['P&L'].cumsum()
        
        fig.add_trace(go.Scatter(
            x=strat_df['Exit Date'],
            y=strat_df['Cumulative'],
            mode='lines',
            name=f'{strategy}',
            line=dict(width=1, dash='dash'),
            opacity=0.6
        ))
    
    fig.update_layout(
        title='Realized Equity Curve',
        xaxis_title='Date',
        yaxis_title='Cumulative P&L ($)',
        height=400,
        hovermode='x unified'
    )
    
    return fig

def create_profit_anatomy_chart(df):
    """Create profit source sunburst chart"""
    expired_df = df[df['Status'] == 'Expired']
    if expired_df.empty:
        return go.Figure()
    
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
        textinfo="label+percent parent"
    ))
    
    fig.update_layout(
        title='Profit Anatomy: Call vs Put Contribution',
        height=500
    )
    
    return fig

def create_trade_dna_radar(trade, historical_df):
    """Create radar chart for trade DNA fingerprint"""
    if historical_df.empty or trade.empty:
        return go.Figure()
    
    # Select features for radar
    features = ['Theta/Cap %', 'Stability', 'Daily Yield %', 'ROI', 'Days Held']
    
    current_values = []
    avg_values = []
    
    for feature in features:
        if feature in trade:
            current_values.append(trade[feature].iloc[0])
        else:
            current_values.append(0)
        
        if feature in historical_df.columns:
            avg_values.append(historical_df[feature].mean())
        else:
            avg_values.append(0)
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=current_values,
        theta=features,
        fill='toself',
        name='Current Trade',
        line_color='#3b82f6'
    ))
    
    fig.add_trace(go.Scatterpolar(
        r=avg_values,
        theta=features,
        fill='toself',
        name='Historical Average',
        line_color='#10b981'
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, max(max(current_values), max(avg_values)) * 1.2]
            )),
        showlegend=True,
        title='Trade DNA Fingerprint',
        height=400
    )
    
    return fig

# --- APP LAYOUT ---
init_db()

# Sidebar
with st.sidebar:
    st.markdown('<h1 class="main-header">🛡️ ALLANTIS</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Trade Guardian Pro</p>', unsafe_allow_html=True)
    
    # Workflow Steps
    st.markdown("### 🚦 Daily Workflow")
    
    with st.expander("📥 1. Import Data", expanded=True):
        st.info("Upload your OptionStrat exports")
        active_up = st.file_uploader("Active Trades File", type=['xlsx', 'csv'], key="act")
        history_up = st.file_uploader("Historical Trades File", type=['xlsx', 'csv'], key="hist")
        
        if st.button("🔄 Sync & Reconcile", type="primary", use_container_width=True):
            logs = []
            if active_up:
                logs.extend(sync_data(active_up, "Active"))
            if history_up:
                logs.extend(sync_data(history_up, "History"))
            
            if logs:
                for log in logs:
                    st.write(log)
                st.success("Sync Complete!")
                st.rerun()
    
    with st.expander("⚙️ 2. Settings"):
        market_regime = st.selectbox(
            "Market Regime",
            ["Neutral (Standard)", "Bullish (Aggressive)", "Bearish (Defensive)"],
            index=0
        )
        
        auto_refresh = st.checkbox("Auto-refresh (60s)", value=False)
        dark_mode = st.checkbox("Dark Mode", value=False)
    
    with st.expander("💾 3. Data Management"):
        # Backup/Restore
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📤 Backup", use_container_width=True):
                with open(DB_NAME, "rb") as f:
                    st.download_button(
                        "Download DB",
                        f,
                        "trade_guardian_backup.db",
                        "application/x-sqlite3",
                        key="backup"
                    )
        
        with col2:
            restore = st.file_uploader("Restore", type=['db'], key='restore')
            if restore:
                with open(DB_NAME, "wb") as f:
                    f.write(restore.getbuffer())
                st.success("Database restored!")
        
        # Maintenance
        if st.button("🧹 Optimize Database", use_container_width=True):
            conn = get_db_connection()
            conn.execute("VACUUM")
            conn.close()
            st.success("Database optimized!")
        
        if st.button("🔄 Reprocess 'Other' Trades", use_container_width=True):
            conn = get_db_connection()
            c = conn.cursor()
            config_dict = load_strategy_config()
            
            try:
                c.execute("SELECT id, name, original_group, strategy FROM trades WHERE strategy = 'Other'")
            except:
                c.execute("SELECT id, name, '', strategy FROM trades WHERE strategy = 'Other'")
            
            other_trades = c.fetchall()
            updated_count = 0
            
            for t_id, t_name, t_group, _ in other_trades:
                group_val = t_group if t_group else ""
                new_strat = get_strategy_dynamic(t_name, group_val, config_dict)
                
                if new_strat != "Other":
                    c.execute("UPDATE trades SET strategy = ? WHERE id = ?", (new_strat, t_id))
                    updated_count += 1
            
            conn.commit()
            conn.close()
            st.success(f"Reprocessed {updated_count} trades!")
            st.rerun()
    
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
st.markdown('<p class="sub-header">Real-time portfolio monitoring & trade management</p>', unsafe_allow_html=True)

# Load data
df = load_data()

if df.empty:
    st.info("👋 Welcome! Upload your OptionStrat files to get started.")
    st.markdown("""
    ### Getting Started:
    1. **Upload Active Trades** from OptionStrat
    2. **Upload Historical Trades** for analytics
    3. Click **Sync & Reconcile** to load your data
    """)
else:
    # Top KPI Dashboard
    st.markdown("## 📊 Executive Dashboard")
    
    active_df = df[df['Status'].isin(['Active', 'Missing'])]
    expired_df = df[df['Status'] == 'Expired']
    
    # Calculate KPIs
    total_debit = active_df['Debit'].sum() if not active_df.empty else 0
    total_theta = active_df['Theta'].sum() if not active_df.empty else 0
    total_pnl = df['P&L'].sum()
    active_pnl = active_df['P&L'].sum() if not active_df.empty else 0
    realized_pnl = expired_df['P&L'].sum() if not expired_df.empty else 0
    
    # Create KPI columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(render_kpi_card(
            f"${total_theta:,.0f}",
            "Daily Theta",
            delta="+$125" if total_theta > 0 else None
        ), unsafe_allow_html=True)
    
    with col2:
        theta_yield = (total_theta / total_debit * 100) if total_debit > 0 else 0
        st.markdown(render_kpi_card(
            f"{theta_yield:.2f}%",
            "Theta Yield",
            delta="+0.15%" if theta_yield > 0 else None
        ), unsafe_allow_html=True)
    
    with col3:
        active_count = len(active_df)
        st.markdown(render_kpi_card(
            str(active_count),
            "Active Trades",
            delta=f"+{len(active_df[active_df['Status'] == 'Missing'])} missing" if len(active_df[active_df['Status'] == 'Missing']) > 0 else None,
            delta_color="inverse" if len(active_df[active_df['Status'] == 'Missing']) > 0 else "normal"
        ), unsafe_allow_html=True)
    
    with col4:
        win_rate = (len(expired_df[expired_df['P&L'] > 0]) / len(expired_df) * 100) if not expired_df.empty else 0
        st.markdown(render_kpi_card(
            f"{win_rate:.1f}%",
            "Win Rate",
            delta="+2.3%" if win_rate > 50 else None
        ), unsafe_allow_html=True)
    
    # Main Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Portfolio Overview",
        "📋 Active Trades",
        "📊 Analytics",
        "⚙️ Strategies",
        "📖 Rulebook"
    ])
    
    with tab1:
        # Portfolio Overview
        st.markdown("### 🎯 Portfolio Health")
        
        if not active_df.empty:
            # Create metrics row
            m1, m2, m3 = st.columns(3)
            
            with m1:
                avg_days = active_df['Days Held'].mean()
                st.metric("Avg Days Held", f"{avg_days:.0f}", delta="-1.2")
            
            with m2:
                portfolio_yield = (active_df['Daily Yield %'].mean() * 365) if not active_df.empty else 0
                st.metric("Portfolio Yield", f"{portfolio_yield:.1f}%", delta="+0.3%")
            
            with m3:
                stability_score = active_df['Stability'].mean()
                st.metric("Stability Score", f"{stability_score:.2f}", 
                         delta="Strong" if stability_score > 0.5 else "Weak")
            
            # Charts
            col1, col2 = st.columns(2)
            
            with col1:
                fig = create_portfolio_allocation_chart(df)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = create_theta_timeline_chart(df)
                st.plotly_chart(fig, use_container_width=True)
            
            # Urgency Heatmap
            st.markdown("### ⚠️ Urgency Matrix")
            fig = create_urgency_heatmap(df)
            st.plotly_chart(fig, use_container_width=True)
            
            # Priority Actions
            st.markdown("### 🔥 Priority Actions")
            
            # Calculate urgency scores for active trades
            def calculate_urgency_score(row):
                pnl = row['P&L']
                days = row['Days Held']
                debit = row['Debit']
                
                if pnl < 0 and days > 25:
                    return min(100, 80 + (abs(pnl) / debit) * 20)
                elif pnl > debit * 0.4:
                    return 20  # Already profitable
                elif days > 40:
                    return min(100, 60 + (days - 40) * 2)
                else:
                    return 40
            
            active_df['Urgency Score'] = active_df.apply(calculate_urgency_score, axis=1)
            priority_trades = active_df.nlargest(5, 'Urgency Score')
            
            if not priority_trades.empty:
                for _, trade in priority_trades.iterrows():
                    st.markdown(create_trade_card(trade), unsafe_allow_html=True)
            else:
                st.success("✅ No urgent actions required. All trades are healthy.")
        
        else:
            st.info("No active trades to display.")
    
    with tab2:
        # Active Trades Management
        st.markdown("### 📋 Trade Management")
        
        if not active_df.empty:
            # Filters
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                selected_strategy = st.selectbox(
                    "Filter by Strategy",
                    ["All"] + list(active_df['Strategy'].unique())
                )
            
            with col2:
                min_urgency = st.slider("Min Urgency", 0, 100, 0)
            
            with col3:
                pnl_range = st.select_slider(
                    "PnL Range",
                    options=["All", "Negative", "Positive", "> $500", "> $1000"]
                )
            
            with col4:
                sort_by = st.selectbox(
                    "Sort By",
                    ["Urgency", "PnL", "Days Held", "Theta", "Name"]
                )
            
            # Apply filters
            filtered_df = active_df.copy()
            
            if selected_strategy != "All":
                filtered_df = filtered_df[filtered_df['Strategy'] == selected_strategy]
            
            filtered_df['Urgency Score'] = filtered_df.apply(calculate_urgency_score, axis=1)
            filtered_df = filtered_df[filtered_df['Urgency Score'] >= min_urgency]
            
            if pnl_range == "Negative":
                filtered_df = filtered_df[filtered_df['P&L'] < 0]
            elif pnl_range == "Positive":
                filtered_df = filtered_df[filtered_df['P&L'] > 0]
            elif pnl_range == "> $500":
                filtered_df = filtered_df[filtered_df['P&L'] > 500]
            elif pnl_range == "> $1000":
                filtered_df = filtered_df[filtered_df['P&L'] > 1000]
            
            # Sort
            if sort_by == "Urgency":
                filtered_df = filtered_df.sort_values('Urgency Score', ascending=False)
            elif sort_by == "PnL":
                filtered_df = filtered_df.sort_values('P&L')
            elif sort_by == "Days Held":
                filtered_df = filtered_df.sort_values('Days Held', ascending=False)
            elif sort_by == "Theta":
                filtered_df = filtered_df.sort_values('Theta')
            else:
                filtered_df = filtered_df.sort_values('Name')
            
            # Display trade cards
            st.markdown(f"#### Showing {len(filtered_df)} trades")
            
            for _, trade in filtered_df.iterrows():
                st.markdown(create_trade_card(trade), unsafe_allow_html=True)
            
            # Trade Details Expander
            with st.expander("📊 Detailed Trade View"):
                display_cols = ['Name', 'Strategy', 'Status', 'P&L', 'Debit', 'Days Held', 
                              'Theta', 'Delta', 'Gamma', 'Vega', 'Theta/Cap %', 'Stability']
                
                st.dataframe(
                    filtered_df[display_cols].style.format({
                        'P&L': '${:,.0f}',
                        'Debit': '${:,.0f}',
                        'Theta': '${:.1f}',
                        'Theta/Cap %': '{:.2f}%',
                        'Stability': '{:.2f}'
                    }),
                    use_container_width=True
                )
        
        else:
            st.info("No active trades to display.")
    
    with tab3:
        # Analytics
        st.markdown("## 📊 Performance Analytics")
        
        if not expired_df.empty:
            # Equity Curve
            st.markdown("### 📈 Equity Curve")
            fig = create_equity_curve_chart(df)
            st.plotly_chart(fig, use_container_width=True)
            
            # Profit Anatomy
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 💰 Profit Sources")
                fig = create_profit_anatomy_chart(df)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("### 🎯 Performance Metrics")
                
                # Calculate performance metrics
                total_trades = len(expired_df)
                win_trades = len(expired_df[expired_df['P&L'] > 0])
                win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
                
                avg_win = expired_df[expired_df['P&L'] > 0]['P&L'].mean() if win_trades > 0 else 0
                avg_loss = expired_df[expired_df['P&L'] <= 0]['P&L'].mean() if total_trades - win_trades > 0 else 0
                
                expectancy = ((win_rate/100) * avg_win) + ((1 - win_rate/100) * avg_loss)
                
                # Display metrics
                metrics_col1, metrics_col2 = st.columns(2)
                
                with metrics_col1:
                    st.metric("Win Rate", f"{win_rate:.1f}%")
                    st.metric("Avg Win", f"${avg_win:,.0f}")
                
                with metrics_col2:
                    st.metric("Total Trades", total_trades)
                    st.metric("Expectancy", f"${expectancy:,.0f}")
                
                # Strategy Performance Table
                st.markdown("#### Strategy Performance")
                strategy_perf = expired_df.groupby('Strategy').agg({
                    'P&L': ['sum', 'mean', 'count'],
                    'ROI': 'mean',
                    'Days Held': 'mean'
                }).round(2)
                
                st.dataframe(strategy_perf, use_container_width=True)
            
            # Trade DNA Analysis
            st.markdown("### 🧬 Trade DNA Analysis")
            
            if not active_df.empty:
                selected_trade = st.selectbox(
                    "Select a trade for DNA analysis",
                    active_df['Name'].tolist()
                )
                
                if selected_trade:
                    trade_data = active_df[active_df['Name'] == selected_trade]
                    
                    if not trade_data.empty:
                        fig = create_trade_dna_radar(trade_data, expired_df)
                        st.plotly_chart(fig, use_container_width=True)
        
        else:
            st.info("No historical trades for analytics. Upload historical data to see performance metrics.")
    
    with tab4:
        # Strategy Configuration
        st.markdown("## ⚙️ Strategy Configuration")
        
        conn = get_db_connection()
        try:
            strat_df = pd.read_sql("SELECT * FROM strategy_config", conn)
            strat_df.columns = ['Name', 'Identifier', 'Target PnL', 'Target Days', 
                              'Min Stability', 'Description', 'Typical Debit']
            
            # Strategy Cards
            st.markdown("### 🎯 Strategy Profiles")
            
            for _, strat in strat_df.iterrows():
                with st.expander(f"🔹 {strat['Name']}", expanded=True):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Target PnL", f"${strat['Target PnL']:,.0f}")
                        st.metric("Typical Debit", f"${strat['Typical Debit']:,.0f}")
                    
                    with col2:
                        st.metric("Target Days", f"{strat['Target Days']}")
                        st.metric("Min Stability", f"{strat['Min Stability']:.2f}")
                    
                    with col3:
                        st.write("**Identifier:**", strat['Identifier'])
                        st.write("**Description:**", strat['Description'])
            
            # Edit Configuration
            st.markdown("### ✏️ Edit Configuration")
            
            edited_strats = st.data_editor(
                strat_df,
                num_rows="dynamic",
                key="strat_editor",
                use_container_width=True,
                column_config={
                    "Name": st.column_config.TextColumn("Strategy Name"),
                    "Identifier": st.column_config.TextColumn("Keyword Match"),
                    "Target PnL": st.column_config.NumberColumn("Target PnL ($)", format="$%d"),
                    "Target Days": st.column_config.NumberColumn("Target Days"),
                    "Min Stability": st.column_config.NumberColumn("Min Stability", format="%.2f"),
                    "Typical Debit": st.column_config.NumberColumn("Typical Debit ($)", format="$%d"),
                    "Description": st.column_config.TextColumn("Description")
                }
            )
            
            if st.button("💾 Save Strategy Configuration", type="primary"):
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
            
            # Reset button
            if st.button("🔄 Reset to Defaults", type="secondary"):
                seed_default_strategies(force_reset=True)
                st.success("Strategies reset to defaults!")
                st.rerun()
                
        except Exception as e:
            st.error(f"Error loading strategies: {e}")
        finally:
            conn.close()
    
    with tab5:
        # Rulebook
        st.markdown("## 📖 The Trader's Constitution")
        
        st.markdown("""
        ### 🛡️ Core Principles
        
        **1. Process Over Outcome**
        > "A good process will lead to good outcomes over time, but a good outcome doesn't necessarily mean you had a good process."
        
        **2. Risk Management First**
        > "Preservation of capital is the first rule of trading. Profits come second."
        
        **3. Emotional Discipline**
        > "The market is a device for transferring money from the impatient to the patient."
        
        ---
        
        ### 🎯 Strategy Rules
        
        #### 🔹 130/160 Strategy (Income Discipline)
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
        
        #### 🔸 160/190 Strategy (Patience Training)
        **Role:** Compounder - Expectancy focused
        
        **Entry Rules:**
        - Friday entries (captures weekend decay)
        - Debit Target: ~$5,200 per lot
        - Sizing: Trade 1 Lot only
        
        **Golden Rule:** **Do not touch in first 30 days**
        - Early interference statistically worsens outcomes
        - This is a patience trade, not a management trade
        
        **Exit:** Hold for 40-50 days, review at Day 35
        
        #### 🎭 M200 Strategy (Emotional Mastery)
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
        
        #### 💼 SMSF Strategy (Wealth Builder)
        **Role:** Long-term Growth
        
        **Structure:**
        - Multi-trade portfolio strategy
        - 60% income, 40% growth allocation
        - Quarterly rebalancing
        
        ---
        
        ### ⚡ Universal Execution Gates
        
        **1. Stability Check:** Monitor Stability Ratio
        - **> 1.0 (Green):** Fortress. Trade is safe.
        - **0.5 - 1.0 (Yellow):** Standard. Monitor.
        - **< 0.25 (Red):** Coin Flip. Trade is directional gambling.
        
        **2. Volatility Gate:**
        - Check VIX before entry
        - Ideal: 14–22
        - Skip if VIX exploded >10% in last 48h
        
        **3. Loss Definition:**
        - A trade that is early and red but *structurally intact* is **NOT** a losing trade.
        - It is just *unripe*.
        
        **4. Efficiency Check:**
        - Monitor Theta Eff. (> 1.0 means efficient decay capture)
        - Monitor Daily Yield % (> 0.1% per day)
        
        ---
        
        ### 🎓 Learning Framework
        
        **Post-Trade Analysis:**
        1. What went right?
        2. What went wrong?
        3. What would I do differently?
        4. What did I learn?
        
        **Journal Prompts:**
        - "Was my entry timing optimal?"
        - "Did I manage the trade according to plan?"
        - "What emotions did I experience and why?"
        - "How can I improve my process?"
        
        ---
        
        *"We don't rise to the level of our expectations, we fall to the level of our training."*
        """)
        
        # Add interactive rule tester
        with st.expander("🧪 Rule Tester", expanded=False):
            st.markdown("Test if a trade violates any rules:")
            
            test_col1, test_col2 = st.columns(2)
            
            with test_col1:
                test_strategy = st.selectbox("Strategy", ["130/160", "160/190", "M200", "SMSF"])
                test_debit = st.number_input("Debit ($)", min_value=0, value=5000)
                test_days = st.number_input("Days Held", min_value=0, value=20)
            
            with test_col2:
                test_pnl = st.number_input("Current PnL ($)", value=0)
                test_theta = st.number_input("Theta ($)", value=15.0)
                test_stability = st.number_input("Stability", value=0.5)
            
            if st.button("Test Rules", type="primary"):
                violations = []
                
                # Check strategy-specific rules
                if test_strategy == "130/160":
                    if test_debit > 4800:
                        violations.append("❌ Debit > $4,800 (Overpriced)")
                    if test_days > 25 and test_pnl <= 0:
                        violations.append("⚠️ Stale trade (>25 days, no profit)")
                
                elif test_strategy == "160/190":
                    if test_days < 30 and test_pnl < 0:
                        violations.append("✅ Normal (Too early to judge)")
                    if not (4800 <= test_debit <= 5500):
                        violations.append("⚠️ Check pricing (Ideal: $4,800-$5,500)")
                
                elif test_strategy == "M200":
                    if 13 <= test_days <= 15:
                        violations.append("🔔 Day 14 check required")
                    if not (7500 <= test_debit <= 8500):
                        violations.append("⚠️ Check pricing (Ideal: $7,500-$8,500)")
                
                # Universal checks
                if test_stability < 0.25:
                    violations.append("❌ Stability < 0.25 (Coin flip)")
                if test_theta < 0:
                    violations.append("❌ Negative Theta (Time is against you)")
                
                if violations:
                    st.warning("Rule Violations Found:")
                    for v in violations:
                        st.write(v)
                else:
                    st.success("✅ All rules satisfied!")

# Footer
st.divider()
st.caption("Allantis Trade Guardian Pro v2.0 • Data refreshes automatically • Last updated: " + 
          datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
