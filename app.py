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

# --- UI CSS ---
def inject_custom_css():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;800&family=JetBrains+Mono:wght@400;700&display=swap');
        .stApp { background-color: #0f172a; color: #e2e8f0; font-family: 'Inter', sans-serif; }
        [data-testid="stSidebar"] { background-color: #020617; border-right: 1px solid #1e293b; }
        h1, h2, h3, h4, h5 { color: #f1f5f9 !important; font-weight: 800 !important; letter-spacing: -0.025em; }
        .gradient-text { background: linear-gradient(to right, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-weight: 800; }
        div[data-testid="stMetric"] { background-color: rgba(30, 41, 59, 0.4); padding: 12px 16px !important; border-radius: 8px; border: 1px solid rgba(148, 163, 184, 0.1); backdrop-filter: blur(10px); min-height: 85px; }
        [data-testid="stMetricLabel"] { font-size: 0.75rem !important; color: #94a3b8 !important; }
        [data-testid="stMetricValue"] { font-family: 'JetBrains Mono', monospace; font-size: 1.6rem !important; color: #f8fafc !important; }
        .stTabs [data-baseweb="tab-list"] { gap: 8px; margin-bottom: 1rem; }
        .stTabs [data-baseweb="tab"] { height: 45px; background-color: rgba(30, 41, 59, 0.5); border-radius: 6px; color: #94a3b8; font-weight: 700 !important; font-size: 0.9rem; padding: 0 20px; border: 1px solid transparent; }
        .stTabs [aria-selected="true"] { background-color: rgba(56, 189, 248, 0.1) !important; color: #38bdf8 !important; border: 1px solid rgba(56, 189, 248, 0.3) !important; }
        div[data-testid="stExpander"] { border: 1px solid #334155; border-radius: 8px; background-color: rgba(15, 23, 42, 0.5); }
        </style>
    """, unsafe_allow_html=True)

inject_custom_css()

# --- PLOTLY THEME ---
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
c_h1, c_h2 = st.columns([3, 1])
with c_h1:
    st.markdown("""<div style="margin-bottom: 20px;"><span style="font-size: 0.7rem; font-weight: 800; color: #38bdf8; letter-spacing: 0.1em; background: rgba(56,189,248,0.1); padding: 4px 8px; border-radius: 4px;">v151.0 INSTITUTIONAL</span><h1 style="font-size: 2.5rem; margin-top: 5px; margin-bottom: 0;">Allantis <span class="gradient-text">Trade Guardian</span></h1></div>""", unsafe_allow_html=True)

# --- DRIVE SYNC ---
DB_NAME = "trade_guardian_v4.db"
SCOPES = ['https://www.googleapis.com/auth/drive']

class DriveManager:
    def __init__(self):
        self.creds = None; self.service = None; self.is_connected = False
        if 'gcp_service_account' in st.secrets:
            try:
                self.creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
                self.service = build('drive', 'v3', credentials=self.creds)
                self.is_connected = True
            except: pass
    def find_db_file(self):
        if not self.is_connected: return None, None
        try:
            res = self.service.files().list(q=f"name='{DB_NAME}' and trashed=false", pageSize=1, fields="files(id, name)").execute()
            items = res.get('files', [])
            return (items[0]['id'], items[0]['name']) if items else (None, None)
        except: return None, None
    def upload_db(self):
        if not os.path.exists(DB_NAME): return False, "No local DB"
        fid, fname = self.find_db_file()
        media = MediaFileUpload(DB_NAME, mimetype='application/x-sqlite3', resumable=True)
        try:
            if fid: self.service.files().update(fileId=fid, media_body=media).execute()
            else: self.service.files().create(body={'name': DB_NAME}, media_body=media).execute()
            return True, "Synced"
        except Exception as e: return False, str(e)
    def download_db(self):
        fid, fname = self.find_db_file()
        if not fid: return False, "No Cloud DB"
        try:
            req = self.service.files().get_media(fileId=fid)
            fh = io.BytesIO(); dl = MediaIoBaseDownload(fh, req); done=False
            while not done: _, done = dl.next_chunk()
            with open(DB_NAME, "wb") as f: f.write(fh.getbuffer())
            return True, "Downloaded"
        except Exception as e: return False, str(e)

drive_mgr = DriveManager()
def auto_sync(): 
    if drive_mgr.is_connected: drive_mgr.upload_db()

def get_db_connection(): return sqlite3.connect(DB_NAME)

# --- CORE LOGIC ---
def init_db():
    if not os.path.exists(DB_NAME) and drive_mgr.is_connected: drive_mgr.download_db()
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT, parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY, trade_id TEXT, snapshot_date DATE, pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER, min_stability REAL, description TEXT, typical_debit REAL)''')
    # Migrations
    for col in ['theta','delta','vega','gamma']: 
        try: c.execute(f"ALTER TABLE snapshots ADD COLUMN {col} REAL")
        except: pass
    try: c.execute("ALTER TABLE strategy_config ADD COLUMN typical_debit REAL"); 
    except: pass
    try: c.execute("ALTER TABLE trades ADD COLUMN original_group TEXT"); 
    except: pass
    
    if c.execute("SELECT count(*) FROM strategy_config").fetchone()[0] == 0:
        defaults = [('130/160','130/160',500,36,0.8,'Income',4000), ('160/190','160/190',700,44,0.8,'Patience',5200), ('M200','M200',900,41,0.8,'Mastery',8000), ('SMSF','SMSF',600,40,0.8,'Wealth',5000)]
        c.executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)
        conn.commit()
    conn.close()

@st.cache_data(ttl=60)
def load_strategy_config():
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM strategy_config", conn)
        return {r['name']: {'id':r['identifier'], 'pnl':r['target_pnl'], 'dit':r['target_days'], 'stability':r['min_stability'], 'debit_per_lot':r.get('typical_debit', 5000)} for _,r in df.iterrows()}
    except: return {}
    finally: conn.close()

def clean_num(x):
    try: return float(str(x).replace('$','').replace(',','').replace('%','').strip())
    except: return 0.0

def generate_id(name, strategy, entry_date):
    return f"{re.sub(r'[^a-zA-Z0-9]', '', str(name))}_{strategy}_{pd.to_datetime(entry_date).strftime('%Y%m%d')}"

def get_strategy_dynamic(name, group, config):
    t, g = str(name).upper(), str(group).upper()
    for s, d in sorted(config.items(), key=lambda x: len(str(x[1]['id'])), reverse=True):
        if str(d['id']).upper() in t or str(d['id']).upper() in g: return s
    return "Other"

# --- ANALYTICS CALCULATIONS ---
def theta_decay_model(initial, days, strategy, dte=45):
    t_frac = min(1.0, days / dte) if dte > 0 else 1.0
    if strategy in ['M200', '130/160', '160/190', 'SMSF']:
        decay = 1 - (2 * t_frac) ** 2 if t_frac < 0.5 else 2 * (1 - t_frac)
        return initial * max(0, decay)
    elif 'VERTICAL' in str(strategy).upper():
        return initial * (1 - t_frac)
    return initial * (1 - np.exp(-2 * t_frac))

def calculate_portfolio_metrics(trades_df, capital):
    if trades_df.empty or capital <= 0: return 0.0, 0.0
    # Simplified daily reconstruction for speed
    total_pnl = trades_df['P&L'].sum()
    days = (trades_df['Exit Date'].max() - trades_df['Entry Date'].min()).days
    if days < 1: days = 1
    cagr = ((capital + total_pnl) / capital) ** (365 / days) - 1
    # Sharpe approximation
    wins = trades_df[trades_df['P&L']>0]['P&L']
    losses = trades_df[trades_df['P&L']<0]['P&L']
    if len(trades_df) > 5 and trades_df['P&L'].std() != 0:
        sharpe = (trades_df['P&L'].mean() / trades_df['P&L'].std()) * np.sqrt(252/10) # rough annualized
    else: sharpe = 0.0
    return sharpe, cagr * 100

def calculate_max_drawdown(trades_df, capital):
    if trades_df.empty: return 0.0
    sorted_df = trades_df.sort_values('Exit Date')
    equity = [capital]
    for p in sorted_df['P&L']: equity.append(equity[-1] + p)
    eq_series = pd.Series(equity)
    dd = (eq_series - eq_series.cummax()) / eq_series.cummax()
    return dd.min() * 100

# --- PARSING & LOADING ---
def parse_and_sync(files, mode):
    log = []
    conn = get_db_connection(); c = conn.cursor()
    config = load_strategy_config()
    existing_ids = set()
    if mode == "Active":
        try: existing_ids = set(pd.read_sql("SELECT id FROM trades WHERE status='Active'", conn)['id'])
        except: pass
    found_ids = set()

    for f in files:
        try:
            if f.name.endswith(('.xls', '.xlsx')):
                df_raw = pd.read_excel(f)
            else:
                content = f.getvalue().decode('utf-8').split('\n')
                h_row = 0
                for i, l in enumerate(content[:30]):
                    if "Name" in l and "Total Return" in l: h_row = i; break
                f.seek(0)
                df_raw = pd.read_csv(f, skiprows=h_row)
            
            for _, r in df_raw.iterrows():
                name = str(r.get('Name',''))
                if not name or name == 'nan' or name.startswith('.'): continue
                
                try: start_dt = pd.to_datetime(r.get('Created At'))
                except: continue
                
                strat = get_strategy_dynamic(name, r.get('Group',''), config)
                tid = generate_id(name, strat, start_dt)
                if mode == "Active": found_ids.add(tid)
                
                pnl = clean_num(r.get('Total Return $', 0))
                debit = abs(clean_num(r.get('Net Debit/Credit', 0)))
                
                # Check exist
                c.execute("SELECT id FROM trades WHERE id=?", (tid,))
                exists = c.fetchone()
                
                status = "Active" if mode == "Active" else "Expired"
                days = (datetime.now() - start_dt).days if mode == "Active" else 1
                
                if exists:
                    c.execute("UPDATE trades SET pnl=?, theta=?, delta=?, vega=?, gamma=?, days_held=?, status=?, put_pnl=?, call_pnl=?, iv=? WHERE id=?",
                              (pnl, clean_num(r.get('Theta',0)), clean_num(r.get('Delta',0)), clean_num(r.get('Vega',0)), clean_num(r.get('Gamma',0)), days, status, 0, 0, clean_num(r.get('IV',0)), tid))
                else:
                    c.execute("INSERT INTO trades (id, name, strategy, status, entry_date, debit, pnl, theta, delta, vega, gamma, days_held, iv) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                              (tid, name, strat, status, start_dt.date(), debit, pnl, clean_num(r.get('Theta',0)), clean_num(r.get('Delta',0)), clean_num(r.get('Vega',0)), clean_num(r.get('Gamma',0)), days, clean_num(r.get('IV',0))))
                
                if mode == "Active":
                    today = datetime.now().date()
                    c.execute("DELETE FROM snapshots WHERE trade_id=? AND snapshot_date=?", (tid, today))
                    c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)",
                              (tid, today, pnl, days, clean_num(r.get('Theta',0)), clean_num(r.get('Delta',0)), clean_num(r.get('Vega',0)), clean_num(r.get('Gamma',0))))
            
            log.append(f"✅ {f.name} Processed")
        except Exception as e: log.append(f"❌ Error {f.name}: {str(e)}")
    
    if mode == "Active" and found_ids:
        missing = existing_ids - found_ids
        if missing:
            placeholders = ','.join('?' * len(missing))
            c.execute(f"UPDATE trades SET status='Missing' WHERE id IN ({placeholders})", list(missing))
            log.append(f"⚠️ {len(missing)} trades marked missing")
    
    conn.commit(); conn.close()
    return log

@st.cache_data(ttl=60)
def load_data():
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        if df.empty: return pd.DataFrame()
        
        # KEY FIX: Rename DB columns to UI expected names
        df = df.rename(columns={
            'name':'Name', 'strategy':'Strategy', 'status':'Status', 'pnl':'P&L', 'debit':'Debit', 
            'theta':'Theta', 'delta':'Delta', 'vega':'Vega', 'gamma':'Gamma', 'iv':'IV',
            'days_held':'Days Held', 'link':'Link', 'notes':'Notes', 'tags':'Tags', 'parent_id':'Parent ID',
            'put_pnl': 'Put P&L', 'call_pnl': 'Call P&L',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date'
        })
        
        # Convert Types
        cols = ['P&L','Debit','Theta','Delta','Vega','Gamma','Days Held','Put P&L','Call P&L']
        for c in cols: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
        
        # DATE FIX: Handle NaT properly
        df['Entry Date'] = pd.to_datetime(df['Entry Date'], errors='coerce')
        df['Exit Date'] = pd.to_datetime(df['Exit Date'], errors='coerce')
        
        df['lot_size'] = pd.to_numeric(df['lot_size'], errors='coerce').fillna(1).astype(int)
        df.loc[df['lot_size'] < 1, 'lot_size'] = 1
        
        df['Stability'] = np.where(df['Theta']>0, df['Theta']/(df['Delta'].abs()+1), 0)
        df['ROI'] = np.where(df['Debit']>0, (df['P&L']/df['Debit'])*100, 0)
        df['Daily Yield %'] = np.where(df['Days Held']>0, df['ROI']/df['Days Held'], 0)
        
        return df
    except: return pd.DataFrame()
    finally: conn.close()

@st.cache_data(ttl=300)
def load_snapshots():
    conn = get_db_connection()
    try: return pd.read_sql("SELECT * FROM snapshots", conn)
    except: return pd.DataFrame()
    finally: conn.close()

# --- APP LOGIC ---
init_db()
config = load_strategy_config()
df = load_data()

# --- SIDEBAR ---
with st.sidebar:
    st.header("☁️ Data Sync")
    if GOOGLE_DEPS_INSTALLED:
        c1, c2 = st.columns(2)
        if c1.button("⬆️ Push"): s, m = drive_mgr.upload_db(); st.toast(m)
        if c2.button("⬇️ Pull"): s, m = drive_mgr.download_db(); st.toast(m); st.rerun()
    
    with st.expander("📂 Upload Files", expanded=False):
        fa = st.file_uploader("Active Trades", accept_multiple_files=True)
        fh = st.file_uploader("History", accept_multiple_files=True)
        if st.button("Process"):
            l = []
            if fa: l.extend(parse_and_sync(fa, "Active"))
            if fh: l.extend(parse_and_sync(fh, "History"))
            st.write(l); st.cache_data.clear(); auto_sync(); time.sleep(1); st.rerun()
    
    st.divider()
    prime_cap = st.number_input("Prime Cap", value=115000, step=5000)
    smsf_cap = st.number_input("SMSF Cap", value=150000, step=5000)
    market_regime = st.selectbox("Regime", ["Neutral", "Bullish", "Bearish"])
    regime_mult = 1.1 if market_regime == "Bullish" else (0.9 if market_regime == "Bearish" else 1.0)

# --- PROCESSING ---
active_df = pd.DataFrame()
expired_df = pd.DataFrame()
if not df.empty:
    active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
    expired_df = df[df['Status'] == 'Expired'].copy()

    # Decision Ladder
    def decision(row):
        strat = row['Strategy']; days = row['Days Held']; pnl = row['P&L']
        bench = config.get(strat, {})
        target = bench.get('pnl', 1000) * regime_mult * row['lot_size']
        max_days = bench.get('dit', 45)
        
        score = 50; action = "HOLD"; reason = "Normal"
        
        if pnl >= target: return "TAKE PROFIT", 100, f"Target ${target:.0f} Hit"
        if pnl >= target*0.8: score += 30; action = "PREPARE EXIT"; reason = "Near Target"
        
        if pnl < 0:
            recov = abs(pnl)/(row['Theta'] if row['Theta']>0 else 1)
            left = max(1, max_days - days)
            if recov > left and days > 15: score += 40; action = "CRITICAL"; reason = f"Zombie (Recov {recov:.0f}d)"
        
        if days > max_days * 1.25: score += 25; reason = "Overdue"
        if row['Stability'] < 0.3 and days > 5: score += 15; reason += " Unstable"
        
        return action, min(100, score), reason

    if not active_df.empty:
        res = active_df.apply(decision, axis=1)
        active_df['Action'] = [x[0] for x in res]
        active_df['Urgency'] = [x[1] for x in res]
        active_df['Reason'] = [x[2] for x in res]

# --- TABS ---
t1, t2, t3, t4 = st.tabs(["🛡️ COMMAND CENTER", "⚡ TRADE DESK", "🧠 QUANT LAB", "⚙️ SYSTEM"])

# TAB 1: COMMAND CENTER
with t1:
    if active_df.empty: st.info("Welcome! Please upload data in the sidebar.")
    else:
        # KPI
        k1, k2, k3, k4 = st.columns(4)
        tot_theta = active_df['Theta'].sum()
        tot_pnl = active_df['P&L'].sum()
        tot_debit = active_df['Debit'].sum() if active_df['Debit'].sum() > 0 else 1
        
        delta_exp = abs(active_df['Delta'].sum() / tot_debit * 100)
        h_color = "normal" if delta_exp < 2 else "inverse"
        
        k1.metric("System Health", "HEALTHY" if delta_exp < 2 else "WARNING", f"{delta_exp:.1f}% Net Delta", delta_color=h_color)
        k2.metric("Floating P&L", f"${tot_pnl:,.0f}", f"{(tot_pnl/tot_debit)*100:.1f}% ROI")
        k3.metric("Daily Income", f"${tot_theta:,.0f}", f"{(tot_theta/tot_debit)*100:.2f}% Yield/Day")
        urgent = active_df[active_df['Urgency'] >= 70]
        k4.metric("Action Items", len(urgent), delta="CRITICAL" if not urgent.empty else "None", delta_color="inverse")
        
        c_l, c_r = st.columns([1.8, 1.2])
        with c_l:
            st.markdown("##### 🚨 Priority Queue")
            queue = active_df[active_df['Urgency'] >= 50].sort_values('Urgency', ascending=False)
            if not queue.empty:
                for _, r in queue.iterrows():
                    clr = "#ef4444" if r['Urgency'] >= 90 else "#f59e0b"
                    st.markdown(f"""<div style="background:rgba(30,41,59,0.5); border-left:4px solid {clr}; padding:10px; margin-bottom:5px; border-radius:4px; display:flex; justify-content:space-between;">
                        <div><b>{r['Name']}</b> <span style="color:#94a3b8">({r['Strategy']})</span><br><span style="color:{clr}">{r['Action']} • {r['Reason']}</span></div>
                        <div style="text-align:right"><b>{r['P&L']:+,.0f}</b><br><span style="color:#94a3b8">{r['Days Held']}d</span></div>
                    </div>""", unsafe_allow_html=True)
            else: st.success("All Quiet.")
            
        with c_r:
            st.markdown("##### 🧭 Radar")
            s_stab = min(100, (active_df['Stability'].mean()/1.5)*100)
            s_yld = min(100, ((tot_theta/tot_debit)/0.001)*50)
            s_fresh = max(0, 100 - (active_df['Days Held'].mean()*2))
            fig = go.Figure(go.Scatterpolar(r=[s_stab, s_yld, s_fresh, 80, 90], theta=['Stability','Yield','Freshness','Div','Neutral'], fill='toself', line_color='#38bdf8'))
            fig = apply_chart_theme(fig); fig.update_layout(height=250, margin=dict(l=30,r=30,t=20,b=20), polar=dict(radialaxis=dict(visible=True, range=[0,100], showticklabels=False)))
            st.plotly_chart(fig, use_container_width=True)

# TAB 2: TRADE DESK
with t2:
    with st.expander("🚀 Advanced Pre-Flight Calculator", expanded=False):
        pf_type = st.selectbox("Strategy Type", ["Hedged Income (M200, 130/160)", "Directional", "Standard Income"])
        c1, c2, c3 = st.columns(3)
        p_theta = c1.number_input("Theta", 10.0)
        p_debit = c2.number_input("Debit", 5000.0)
        p_delta = c3.number_input("Net Delta", -5.0)
        
        if p_debit > 0:
            stab = p_theta / (abs(p_delta)+1)
            yld = (p_theta / p_debit) * 100
            st.markdown(f"**Analysis:** Yield **{yld:.2f}%/day** | Stability **{stab:.2f}**")
            
            if pf_type == "Hedged Income":
                if stab > 0.8: st.success("✅ Excellent Stability")
                elif stab < 0.5: st.error("❌ Too Unstable (High Delta)")
                else: st.warning("⚠️ Borderline")
            elif pf_type == "Directional":
                lev = abs(p_delta)/p_debit * 100
                st.info(f"Leverage: {lev:.2f} delta per $100")

    if not active_df.empty:
        st.markdown("#### 📖 Live Journal")
        edit_cols = ['id','Name','Strategy','P&L','Urgency','Action','Stability','Days Held','Notes','Tags']
        edited = st.data_editor(active_df[edit_cols], hide_index=True, use_container_width=True, height=400,
            column_config={"Urgency": st.column_config.ProgressColumn("Urg", min_value=0, max_value=100, format="%d"),
                           "Stability": st.column_config.ProgressColumn("Stab", min_value=0, max_value=3, format="%.2f"),
                           "P&L": st.column_config.NumberColumn(format="$%d")})
        
        if st.button("Save Journal"):
            conn = get_db_connection()
            for i, r in edited.iterrows():
                conn.execute("UPDATE trades SET notes=?, tags=? WHERE id=?", (r['Notes'], r['Tags'], r['id']))
            conn.commit(); conn.close(); st.success("Saved!"); auto_sync()
        
        st.markdown("#### 📊 Strategy Breakdown")
        strats = sorted(active_df['Strategy'].unique())
        subtabs = st.tabs(strats)
        for i, s in enumerate(strats):
            with subtabs[i]:
                sub = active_df[active_df['Strategy'] == s]
                sc1, sc2, sc3 = st.columns(3)
                sc1.metric("Allocated", f"${sub['Debit'].sum():,.0f}")
                sc2.metric("P&L", f"${sub['P&L'].sum():,.0f}")
                sc3.metric("Avg Yield", f"{sub['Daily Yield %'].mean():.2f}%")
                st.dataframe(sub[['Name','P&L','Days Held','Theta','Delta','Action']], use_container_width=True)

# TAB 3: QUANT LAB
with t3:
    if expired_df.empty: st.warning("Need closed trade history.")
    else:
        # DEEP DIVE
        st.markdown("#### 🔬 Performance Deep Dive")
        dd_c1, dd_c2, dd_c3 = st.columns(3)
        sharpe, cagr = calculate_portfolio_metrics(expired_df, prime_cap + smsf_cap)
        mdd = calculate_max_drawdown(expired_df, prime_cap + smsf_cap)
        
        dd_c1.metric("Sharpe Ratio", f"{sharpe:.2f}", help=">1 Good, >2 Excellent")
        dd_c2.metric("CAGR", f"{cagr:.1f}%")
        dd_c3.metric("Max Drawdown", f"{mdd:.1f}%")
        
        q1, q2 = st.tabs(["⏳ Lifecycle & Phasing", "🧠 AI & Risk"])
        
        with q1:
            st.markdown("##### Profit Phasing (Where is money made?)")
            # Create Phasing Data
            snaps = load_snapshots()
            if not snaps.empty:
                # Phasing Histogram
                phasing_data = []
                for _, t in expired_df.iterrows():
                    t_snaps = snaps[snaps['trade_id'] == t['id']].sort_values('days_held')
                    if len(t_snaps) > 1:
                        total_days = t['Days Held']
                        for _, s in t_snaps.iterrows():
                            phase = "Early (0-30%)" if s['days_held']/total_days <= 0.3 else ("Mid (30-70%)" if s['days_held']/total_days <= 0.7 else "Late (70%+)")
                            phasing_data.append({'Strategy': t['Strategy'], 'Phase': phase, 'PnL': s['pnl']}) # Approximation using total PnL at snap
                
                if phasing_data:
                    pdf = pd.DataFrame(phasing_data)
                    # Use last snapshot PnL as approximation for bucket
                    fig_ph = px.histogram(pdf, x='Strategy', y='PnL', color='Phase', barmode='group', title="Profit Generation by Phase")
                    fig_ph = apply_chart_theme(fig_ph)
                    st.plotly_chart(fig_ph, use_container_width=True)
            
            st.markdown("##### Harvest Curve (ROI vs Time)")
            # Trendline removed to fix statsmodels crash, using standard scatter
            fig_lc = px.scatter(expired_df, x='Days Held', y='ROI', color='Strategy', title="ROI Decay")
            fig_lc = apply_chart_theme(fig_lc)
            st.plotly_chart(fig_lc, use_container_width=True)

        with q2:
            st.markdown("##### 🧬 DNA Fingerprinting (KNN)")
            if not active_df.empty:
                sel = st.selectbox("Analyze Trade", active_df['Name'].unique())
                row = active_df[active_df['Name']==sel].iloc[0]
                feats = ['Theta','Delta','Debit']
                
                # KNN
                hist_clean = expired_df.dropna(subset=feats)
                if not hist_clean.empty:
                    dists = cdist([row[feats].fillna(0)], hist_clean[feats], 'euclidean')
                    indices = dists.argsort()[0][:5]
                    sim = hist_clean.iloc[indices]
                    
                    win_prob = (sim[sim['P&L']>0].shape[0] / 5) * 100
                    st.info(f"**Win Probability:** {win_prob:.0f}% (based on 5 most similar past trades)")
                    st.dataframe(sim[['Name','P&L','Days Held']], use_container_width=True)
                    
                    # Kelly
                    wins = sim[sim['P&L']>0]['P&L']
                    loss = sim[sim['P&L']<0]['P&L']
                    if not wins.empty and not loss.empty:
                        W = win_prob/100; R = wins.mean() / abs(loss.mean())
                        kelly = W - (1-W)/R
                        st.write(f"**Kelly Suggestion:** {max(0, kelly*100):.1f}% Size")

            st.markdown("##### ⚠️ Concentration Risk")
            conc = active_df[active_df['Debit'] > (prime_cap+smsf_cap)*0.15]
            if not conc.empty:
                st.warning("Trades exceeding 15% of capital:")
                st.dataframe(conc[['Name','Debit','Strategy']], use_container_width=True)
            else: st.success("Concentration limits respected.")

            st.markdown("##### 🔁 Rolling Correlation")
            if not snaps.empty:
                pvt = snaps.pivot_table(index='snapshot_date', columns='trade_id', values='pnl').fillna(0)
                if not pvt.empty:
                    corr = pvt.corr().iloc[:10,:10] # Show first 10 for performance
                    fig_c = px.imshow(corr, title="Trade Correlation Matrix (Sample)")
                    fig_c = apply_chart_theme(fig_c)
                    st.plotly_chart(fig_c, use_container_width=True)

# TAB 4: SYSTEM
with t4:
    st.markdown("### ⚙️ Configuration & Data")
    conn = get_db_connection()
    sdf = pd.read_sql("SELECT * FROM strategy_config", conn)
    conn.close()
    
    edited_conf = st.data_editor(sdf, num_rows="dynamic", use_container_width=True)
    if st.button("Update Config"):
        conn = get_db_connection()
        conn.execute("DELETE FROM strategy_config")
        for i, r in edited_conf.iterrows(): conn.execute("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", tuple(r))
        conn.commit(); conn.close(); st.success("Updated")
    
    st.divider()
    if st.button("🗑️ Reset Database"):
        if os.path.exists(DB_NAME): os.remove(DB_NAME)
        st.warning("Reset."); time.sleep(1); st.rerun()

st.markdown("---"); st.caption("Allantis Trade Guardian v151.0")
