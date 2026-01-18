import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import re
import random
from datetime import datetime, timedelta
from scipy.spatial.distance import cdist 

# --- PAGE CONFIG ---
st.set_page_config(page_title="Trade Guardian (DEMO)", layout="wide", page_icon="🧪")

# --- DEMO BANNER ---
st.warning("🧪 **DEMO MODE**: This version uses randomly generated data. No real trades are connected.")
st.title("🛡️ Allantis Trade Guardian (Demo)")

# --- DATABASE CONSTANTS ---
DB_NAME = "demo_trade_data.db"

# --- FAKE DATA GENERATOR (Built-in) ---
def generate_fake_data(conn):
    c = conn.cursor()
    strategies = ['130/160', '160/190', 'M200', 'SMSF']
    
    # 1. Generate 50 Expired (History) Trades
    for i in range(50):
        strat = random.choice(strategies)
        status = "Expired"
        start_date = datetime.now() - timedelta(days=random.randint(60, 365))
        days = random.randint(20, 50)
        end_date = start_date + timedelta(days=days)
        
        # Random P&L based on fake win rate
        if random.random() > 0.3: # 70% win rate
            pnl = random.randint(200, 1500)
        else:
            pnl = random.randint(-1000, -100)
            
        debit = random.randint(3000, 8000)
        t_id = f"MOCK_HIST_{i}_{strat}"
        
        c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (t_id, f"Mock Trade {i}", strat, status, start_date.date(), end_date.date(),
                   days, debit, 1, pnl, 0, 0, 0, 0, "Generated history", "Demo", "", 
                   pnl*0.6, pnl*0.4, 15.0, "", strat))

    # 2. Generate 15 Active Trades with Snapshots (for graphs)
    for i in range(15):
        strat = random.choice(strategies)
        status = "Active"
        days_active = random.randint(1, 45)
        start_date = datetime.now() - timedelta(days=days_active)
        
        debit = random.randint(4000, 6000)
        pnl = random.randint(-500, 800)
        theta = random.randint(10, 30)
        delta = random.uniform(-15, 15)
        
        t_id = f"MOCK_ACTIVE_{i}_{strat}"
        
        # Insert Trade
        c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (t_id, f"Active Demo {i}", strat, status, start_date.date(), None,
                   days_active, debit, 1, pnl, theta, delta, 0, 100, "Active demo position", "Demo", "", 
                   0, 0, 20.0, "", strat))
        
        # Generate Snapshots for this trade (Trade History)
        curr_pnl = 0
        for d in range(days_active):
            snap_date = start_date + timedelta(days=d)
            curr_pnl += random.randint(-50, 100) # Random daily fluctuation
            c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)",
                      (t_id, snap_date.date(), curr_pnl, d, theta, delta, 100, 0))

    conn.commit()

# --- DATABASE ENGINE ---
def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    # If DB doesn't exist, create it and FILL IT WITH FAKE DATA
    if not os.path.exists(DB_NAME):
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS trades (
                        id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT, parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT, snapshot_date DATE, pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL, FOREIGN KEY(trade_id) REFERENCES trades(id))''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (
                        name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER, min_stability REAL, description TEXT, typical_debit REAL)''')
        
        defaults = [
            ('130/160', '130/160', 500, 36, 0.8, 'Income Discipline', 4000),
            ('160/190', '160/190', 700, 44, 0.8, 'Patience Training', 5200),
            ('M200', 'M200', 900, 41, 0.8, 'Emotional Mastery', 8000),
            ('SMSF', 'SMSF', 600, 40, 0.8, 'Wealth Builder', 5000)
        ]
        c.executemany("INSERT OR IGNORE INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)
        conn.commit()
        
        # --- GENERATE FAKE DATA ---
        generate_fake_data(conn)
        conn.close()

# --- HELPER FUNCTIONS (Simplified for Demo) ---
def extract_ticker(name):
    try:
        return str(name).split(' ')[0]
    except: return "UNKNOWN"

def theta_decay_model(initial_theta, days_held, strategy, dte_at_entry=45):
    t_frac = min(1.0, days_held / dte_at_entry) if dte_at_entry > 0 else 1.0
    if strategy in ['M200', '130/160', '160/190', 'SMSF']:
        if t_frac < 0.5: decay_factor = 1 - (2 * t_frac) ** 2
        else: decay_factor = 2 * (1 - t_frac)
        return initial_theta * max(0, decay_factor)
    return initial_theta * (1 - t_frac) # Simple linear for others

def reconstruct_daily_pnl(trades_df):
    trades = trades_df.copy()
    trades['Entry Date'] = pd.to_datetime(trades['Entry Date'])
    start_date = trades['Entry Date'].min()
    end_date = pd.Timestamp.now()
    date_range = pd.date_range(start=start_date, end=end_date)
    daily_pnl_dict = {d.date(): 0.0 for d in date_range}
    for _, trade in trades.iterrows():
        days = trade['Days Held']
        if days <= 0: days = 1
        total_pnl = trade['P&L']
        curr = trade['Entry Date']
        for day in range(days):
            if curr.date() in daily_pnl_dict: daily_pnl_dict[curr.date()] += (total_pnl / days)
            curr += pd.Timedelta(days=1)
    return daily_pnl_dict

# --- DATA LOADER ---
@st.cache_data(ttl=60)
def load_data():
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        if df.empty: return pd.DataFrame()
        
        # --- CRITICAL FIX: Ensure correct capitalization ---
        # SQLite columns are lowercase by default in results
        # We must manually map them to match the app's expectations
        
        # 1. Rename columns from DB (lowercase) to App format (Capitalized)
        df = df.rename(columns={
            'name': 'Name',
            'strategy': 'Strategy',
            'status': 'Status',
            'pnl': 'P&L',
            'debit': 'Debit',
            'days_held': 'Days Held',
            'theta': 'Theta',
            'delta': 'Delta',
            'gamma': 'Gamma',
            'vega': 'Vega',
            'entry_date': 'Entry Date',
            'exit_date': 'Exit Date',
            'notes': 'Notes',
            'tags': 'Tags',
            'parent_id': 'Parent ID',
            'put_pnl': 'Put P&L',
            'call_pnl': 'Call P&L',
            'iv': 'IV',
            'link': 'Link',
            'lot_size': 'lot_size' # Keep lowercase for compatibility
        })

        # 2. Type conversion
        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        numeric_cols = ['P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'lot_size', 'Put P&L', 'Call P&L']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # 3. Derived Metrics
        df['Debit/Lot'] = np.where(df['lot_size'] > 0, df['Debit'] / df['lot_size'], df['Debit'])
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = np.where(df['Debit'] > 0, (df['Theta'] / df['Debit']) * 100, 0)
        
        df['Stability'] = np.where(df['Theta'] > 0, df['Theta'] / (df['Delta'].abs() + 1), 0.0)
        df['P&L Vol'] = 0.0 
        
        def get_grade(row):
            d = row['Debit/Lot']
            if d > 6000: return "A"
            return "B"
        df['Grade'] = df.apply(get_grade, axis=1)
        df['Reason'] = "Demo"
        
        return df
    except Exception as e:
        print(f"Data Load Error: {e}")
        return pd.DataFrame()
    finally: conn.close()

# --- INTELLIGENCE FUNCTIONS (Simplified) ---
def generate_trade_predictions(active_df, history_df):
    if active_df.empty: return pd.DataFrame()
    predictions = []
    for _, row in active_df.iterrows():
        predictions.append({
            'Trade Name': row['Name'], 'Strategy': row['Strategy'], 
            'Win Prob %': random.randint(40, 90),
            'Expected PnL': random.randint(100, 1000), 
            'Kelly Size %': random.uniform(5, 15), 
            'Rec. Size ($)': random.randint(5000, 15000),
            'AI Rec': random.choice(["HOLD", "TAKE PROFIT", "WATCH"]), 
            'Confidence': random.randint(60, 99)
        })
    return pd.DataFrame(predictions)

# --- INIT ---
init_db()
df = load_data()

# --- SIDEBAR ---
st.sidebar.header("🧪 Demo Controls")
if st.sidebar.button("🔄 Regenerate Random Data"):
    if os.path.exists(DB_NAME): os.remove(DB_NAME)
    st.cache_data.clear()
    st.rerun()

st.sidebar.info("This is a demo version. All data is randomly generated on the fly.")

# --- TABS ---
tab_dash, tab_active, tab_analytics, tab_ai = st.tabs([" Dashboard", " ⚡ Active Management", " Analytics", " AI & Insights"])

# CHECK IF DATA EXISTS
if df.empty or 'Status' not in df.columns:
    st.error("⚠️ Error generating demo data. Please click 'Regenerate Random Data' in the sidebar.")
    st.stop()

with tab_dash:
    active_df = df[df['Status'] == 'Active']
    if not active_df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Portfolio Health", "🟢 HEALTHY")
        c2.metric("Daily Income", f"${active_df['Theta'].sum():,.0f}")
        c3.metric("Floating P&L", f"${active_df['P&L'].sum():,.0f}")
        c4.metric("Active Trades", len(active_df))
        
        st.subheader("🗺️ Position Heat Map (Demo Data)")
        fig_heat = px.scatter(
            active_df, x='Days Held', y='P&L', size='Debit',
            color='ROI', color_continuous_scale='RdYlGn',
            hover_data=['Name', 'Strategy'], title="Position Clustering"
        )
        st.plotly_chart(fig_heat, use_container_width=True)

with tab_active:
    st.dataframe(df[df['Status'] == 'Active'][['Name', 'Strategy', 'P&L', 'Theta', 'Delta', 'Days Held']], use_container_width=True)

with tab_analytics:
    expired_df = df[df['Status'] == 'Expired']
    if not expired_df.empty:
        st.subheader("Realized Equity Curve (Demo)")
        expired_df = expired_df.sort_values("Exit Date")
        expired_df['Cumulative P&L'] = expired_df['P&L'].cumsum()
        fig = px.line(expired_df, x='Exit Date', y='Cumulative P&L', markers=True)
        st.plotly_chart(fig, use_container_width=True)

with tab_ai:
    st.subheader("AI Predictions (Simulated)")
    active_df = df[df['Status'] == 'Active']
    hist_df = df[df['Status'] == 'Expired']
    preds = generate_trade_predictions(active_df, hist_df)
    st.dataframe(preds, use_container_width=True)
