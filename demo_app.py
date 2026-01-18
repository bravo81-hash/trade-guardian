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
st.set_page_config(page_title="Trade Guardian (Demo)", layout="wide", page_icon="🛡️")

# --- SIMULATION CONFIG ---
# This replaces your specific strategies with generic ones for the demo
STRATEGY_MAP = {
    'Alpha_Inc': {'pnl': 500, 'dit': 36, 'stability': 0.8, 'desc': 'High Probability Income'},
    'Beta_Growth': {'pnl': 700, 'dit': 44, 'stability': 0.8, 'desc': 'Directional Growth'},
    'Gamma_Hedge': {'pnl': 900, 'dit': 41, 'stability': 0.8, 'desc': 'Portfolio Protection'},
    'Delta_Flow': {'pnl': 600, 'dit': 40, 'stability': 0.8, 'desc': 'Cash Flow Gen'}
}

# --- DATABASE CONSTANTS ---
DB_NAME = "simulation_v1.db"

# --- MOCK DATA GENERATOR (THE SIMULATION ENGINE) ---
def generate_mock_data(conn):
    """
    Generates realistic looking trade history and active positions 
    so the programmer can see the app's logic in action without real data.
    """
    c = conn.cursor()
    
    # 1. Clear existing
    c.execute("DELETE FROM trades")
    c.execute("DELETE FROM snapshots")
    c.execute("DELETE FROM strategy_config")
    
    # 2. Setup Strategies
    for name, cfg in STRATEGY_MAP.items():
        c.execute("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", 
                  (name, name.upper(), cfg['pnl'], cfg['dit'], cfg['stability'], cfg['desc'], 5000))

    # 3. Generate History (Closed Trades)
    strategies = list(STRATEGY_MAP.keys())
    statuses = ['Expired'] * 80 + ['Active'] * 15 # 80 closed, 15 active
    
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(100):
        strat = random.choice(strategies)
        status = "Expired" if i < 80 else "Active"
        
        # Randomized parameters based on strategy "personality"
        if strat == 'Alpha_Inc':
            win_rate = 0.85
            avg_win = 600
            avg_loss = -2000
            avg_days = 35
        elif strat == 'Gamma_Hedge':
            win_rate = 0.40
            avg_win = 2500
            avg_loss = -400
            avg_days = 60
        else:
            win_rate = 0.65
            avg_win = 800
            avg_loss = -800
            avg_days = 40
            
        is_win = random.random() < win_rate
        pnl = float(np.random.normal(avg_win, avg_win*0.2) if is_win else np.random.normal(avg_loss, abs(avg_loss)*0.2))
        
        # If active, PnL is floating (unrealized)
        if status == 'Active':
            days_held = random.randint(5, 50)
            entry_date = datetime.now() - timedelta(days=days_held)
            exit_date = None
            # Active trades usually have smaller PnL swings unless old
            pnl = pnl * (days_held / avg_days) 
        else:
            days_held = int(abs(np.random.normal(avg_days, 10)))
            if days_held < 1: days_held = 1
            # Spread entry dates out over the year
            entry_offset = random.randint(0, 300)
            entry_date = start_date + timedelta(days=entry_offset)
            exit_date = entry_date + timedelta(days=days_held)

        trade_id = f"SIM_{i}_{strat}_{entry_date.strftime('%Y%m%d')}"
        name = f"ASSET_{random.randint(100,999)}_{strat.upper()}"
        
        debit = random.randint(3000, 8000)
        lot_size = max(1, int(debit / 4000))
        
        # Greeks Simulation
        theta = (debit * 0.002) * (1 if status == 'Active' else 0) # Simple theta model
        delta = (debit * 0.001) * random.choice([-1, 1])
        gamma = debit * 0.00005
        vega = debit * 0.01
        
        c.execute('''INSERT INTO trades 
            (id, name, strategy, status, entry_date, exit_date, days_held, debit, lot_size, pnl, theta, delta, gamma, vega, notes, tags, parent_id, put_pnl, call_pnl, iv, link, original_group)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (trade_id, name, strat, status, entry_date.date(), 
             exit_date.date() if exit_date else None, 
             days_held, debit, lot_size, pnl, 
             theta, delta, gamma, vega, "Simulated Data", "Demo", "", 0, 0, 15.5, "", "SIM_GROUP"))

        # Generate Snapshots for Active trades (for charts)
        if status == 'Active':
            for d in range(days_held):
                snap_date = entry_date + timedelta(days=d)
                snap_pnl = pnl * (d / days_held) + np.random.normal(0, 50)
                c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)",
                          (trade_id, snap_date.date(), snap_pnl, d, theta, delta, vega, gamma))
    
    conn.commit()

# --- DATABASE ENGINE ---
def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    """Initializes the DB. If it's missing, it runs the Simulation Generator."""
    new_db = not os.path.exists(DB_NAME)
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
    
    # Check if we need to seed mock data
    c.execute("SELECT count(*) FROM trades")
    if c.fetchone()[0] == 0:
        print("Seeding Simulation Data...")
        generate_mock_data(conn)
        
    conn.commit()
    conn.close()

# --- LOAD STRATEGY CONFIG ---
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

def extract_ticker(name):
    try:
        parts = str(name).split('_')
        if len(parts) > 1: return parts[1] # Return the random asset ID
        return "UNKNOWN"
    except: return "UNKNOWN"

def theta_decay_model(initial_theta, days_held, strategy, dte_at_entry=45):
    # Same physics engine, just genericized strategy names
    t_frac = min(1.0, days_held / dte_at_entry) if dte_at_entry > 0 else 1.0
    if strategy in ['Alpha_Inc', 'Gamma_Hedge']:
        if t_frac < 0.5:
            decay_factor = 1 - (2 * t_frac) ** 2
        else:
            decay_factor = 2 * (1 - t_frac)
        return initial_theta * max(0, decay_factor)
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
        
        # Grading Logic (Adapted for generic strategies)
        def get_grade(row):
            s, d = row['Strategy'], row['Debit/Lot']
            reason = "Standard"
            grade = "C"
            if s == 'Alpha_Inc':
                if d > 4800: grade="F"; reason="Overpriced"
                elif 3500 <= d <= 4500: grade="A+"; reason="Sweet Spot"
                else: grade="B"; reason="Acceptable"
            elif s == 'Beta_Growth':
                if 4800 <= d <= 5500: grade="A"; reason="Ideal Pricing"
                else: grade="C"; reason="Check Pricing"
            elif s == 'Gamma_Hedge':
                if 7500 <= d <= 8500: grade, reason = "A", "Perfect Entry"
                else: grade, reason = "B", "Variance"
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
        df = pd.read_sql(q, conn)
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

def check_rot_and_efficiency(active_df, history_df, threshold_pct, min_days):
    if active_df.empty or history_df.empty: return pd.DataFrame()
    history_df['Eff_Score'] = (history_df['P&L'] / history_df['Days Held'].clip(lower=1)) / (history_df['Debit'] / 1000)
    baseline_eff = history_df.groupby('Strategy')['Eff_Score'].median().to_dict()
    rot_alerts = []
    for _, row in active_df.iterrows():
        strat = row['Strategy']
        days = row['Days Held']
        if days < min_days: continue
        curr_eff = (row['P&L'] / days) / (row['Debit'] / 1000) if row['Debit'] > 0 else 0
        base = baseline_eff.get(strat, 0)
        if base > 0 and curr_eff < (base * threshold_pct):
            rot_alerts.append({
                'Trade': row['Name'], 'Strategy': strat, 'Current Speed': f"${curr_eff:.1f}/day",
                'Baseline Speed': f"${base:.1f}/day", 'Raw Current': curr_eff, 'Raw Baseline': base,    
                'Status': ' ROTTING' if row['P&L'] > 0 else ' DEAD MONEY'
            })
    return pd.DataFrame(rot_alerts)

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
st.sidebar.markdown("###  Demo Controls")
if st.sidebar.button("♻️ Regenerate Simulation Data"):
    conn = get_db_connection()
    generate_mock_data(conn)
    conn.close()
    st.cache_data.clear()
    st.success("Simulation Data Reset!")
    st.rerun()

st.sidebar.divider()
st.sidebar.header(" Portfolio Settings")

prime_cap = st.sidebar.number_input("Prime Account", min_value=1000, value=115000, step=1000)
smsf_cap = st.sidebar.number_input("Secondary Account", min_value=1000, value=150000, step=1000)
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
            is_cooking = (strat == 'Beta_Growth' and days < 30)
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
    if strat == 'Alpha_Inc':
        limit_130 = min(stale_threshold, 30) 
        if days > limit_130 and pnl < (100 * lot_size):
            return "KILL", 95, f"Stale (> {limit_130:.0f}d)", juice_val, juice_type
        elif days > (limit_130 * 0.8):
            score += 20
            reason = "Aging"
    
    if stability < 0.3 and days > 5:
        score += 25
        reason += " + Coin Flip (Unstable)"
        action = "RISK REVIEW"
    if row['Theta Eff.'] < 0.2 and days > 10:
        score += 15
        reason += " + Bad Decay"
    
    score = min(100, max(0, score))
    if score >= 90: action = "CRITICAL"
    elif score >= 70: action = "WATCH"
    elif score <= 30: action = "COOKING"
    return action, score, reason, juice_val, juice_type

# --- MAIN APP ---
st.title("🛡️ Trade Guardian: Logic Demo")
st.caption("Running in Simulation Mode. All data is computer-generated to demonstrate logic flow.")

df = load_data()
dynamic_benchmarks = load_strategy_config() 

expired_df = pd.DataFrame() 
if not df.empty and 'Status' in df.columns:
    expired_df = df[df['Status'] == 'Expired']
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
tab_dash, tab_active, tab_analytics, tab_ai, tab_strategies, tab_rules = st.tabs([" Dashboard", "  Active Management", " Analytics", " AI & Insights", " Strategies", " Rules"])

with tab_dash:
    with st.expander(" Universal Pre-Flight Calculator", expanded=False):
        pf_c1, pf_c2, pf_c3 = st.columns(3)
        with pf_c1:
            pf_goal = st.selectbox("Strategy Profile", [
                " Hedged Income (Butterflies, Calendars)", 
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
            target_allocation = {k: 0.25 for k in dynamic_benchmarks.keys()}
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
                h_icon = "" if "HEALTHY" in health_status else ("" if "CRITICAL" in health_status else "")
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
            
            with st.expander(" Detailed Metrics (Allocation, Greeks, Age)", expanded=False):
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
            st.subheader(" Position Heat Map")
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
            st.caption(" Top-Right = Winners aging well |  Bottom-Right = Losers rotting |  Left = New positions cooking")

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
                    "Stability": st.column_config.NumberColumn("Stability", format="%.2f", disabled=True),
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

                            st.dataframe(display_df.style.format({'Theta/Cap %': "{:.2f}%", 'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%", 'Ann. ROI': "{:.1f}%", 'Theta Eff.': "{:.2f}", 'P&L Vol': "{:.1f}", 'Stability': "{:.2f}", 'Theta': "{:.1f}", 'Delta': "{:.1f}", 'Gamma': "{:.2f}", 'Vega': "{:.0f}", 'Days Held': "{:.0f}"}).map(lambda v: 'background-color: #d1e7dd; color: #0f5132; font-weight: bold' if 'TAKE PROFIT' in str(v) else ('background-color: #f8d7da; color: #842029; font-weight: bold' if 'KILL' in str(v) or 'MISSING' in str(v) else ('background-color: #fff3cd; color: #856404; font-weight: bold' if 'WATCH' in str(v) else ('background-color: #cff4fc; color: #055160; font-weight: bold' if 'COOKING' in str(v) else ''))), subset=['Action']).map(lambda v: 'color: green; font-weight: bold' if isinstance(v, (int, float)) and v > 0 else ('color: red; font-weight: bold' if isinstance(v, (int, float)) and v < 0 else ''), subset=['P&L']).map(yield_color, subset=['Daily Yield %']).apply(lambda x: ['background-color: #d1d5db; color: black; font-weight: bold' if x.name == len(display_df)-1 else '' for _ in x], axis=1), use_container_width=True, column_config={"Link": st.column_config.LinkColumn("OS Link", display_text="Open "), "Urgency Score": st.column_config.ProgressColumn("Urgency", min_value=0, max_value=100, format="%d"), "Gauge": st.column_config.TextColumn("Tank / Recov")})
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
    except Exception as e: st.error(f"Error loading strategies: {e}")
    finally: conn.close()
    
    st.info(" **How to use:** \n1. **Reset to Defaults** if this table is blank. \n2. **Edit Identifiers:** Ensure '130/160' is longer than '160'. \n3. **Save Changes.** \n4. **Reprocess All Trades** to fix old grouping errors.")

with tab_analytics:
    an_overview, an_trends, an_risk, an_decay, an_rolls = st.tabs([" Overview", " Trends & Seasonality", " Risk & Excursion", " Decay & DNA", " Rolls"])

    with an_overview:
        if not df.empty and 'Status' in df.columns:
            active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
            if not active_df.empty:
                st.markdown("###  Portfolio Health Check (Breakdown)")
                health_col1, health_col2, health_col3 = st.columns(3)
                tot_debit = active_df['Debit'].sum()
                if tot_debit == 0: tot_debit = 1
                target_allocation = {k: 0.25 for k in dynamic_benchmarks.keys()}
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
                    s_total, c_total = calculate_portfolio_metrics(expired_df, total_cap)
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("** TOTAL PORTFOLIO**")
                        st.metric("Banked Profit", f"${realized_pnl:,.0f}")
                        st.metric("CAGR", f"{c_total:.1f}%")
                        st.metric("Sharpe", f"{s_total:.2f}")

                    with col2:
                         st.markdown("** Risk Metrics (Max Drawdown)**")
                         mdd_total = calculate_max_drawdown(expired_df, total_cap)
                         st.metric("Total Max DD", f"{mdd_total['Max Drawdown %']:.1f}%", help="Largest peak-to-trough decline in total equity.")
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

    with an_decay:
        st.subheader(" Trade Life Cycle & Decay")
        snaps = load_snapshots()
        if not snaps.empty:
            decay_strat = st.selectbox("Select Strategy for Decay", snaps['strategy'].unique(), key="decay_strat")
            strat_snaps = snaps[snaps['strategy'] == decay_strat].copy()
            if not strat_snaps.empty:
                def get_theta_anchor(group):
                    earliest = group.sort_values('days_held').iloc[0]
                    return earliest['theta'] if earliest['theta'] > 0 else group['theta'].max()
                anchor_map = strat_snaps.groupby('trade_id').apply(get_theta_anchor)
                strat_snaps['Theta_Anchor'] = strat_snaps['trade_id'].map(anchor_map)
                strat_snaps['Theta_Expected'] = strat_snaps.apply(lambda r: theta_decay_model(r['Theta_Anchor'], r['days_held'], r['strategy']), axis=1)
                strat_snaps = strat_snaps[(strat_snaps['Theta_Anchor'] > 0) & (strat_snaps['theta'] != 0) & (strat_snaps['days_held'] < 60)]
                
                fig_pnl = px.line(strat_snaps, x='days_held', y='pnl', color='name', title=f"Trade Life Cycle: PnL Trajectory ({decay_strat})", markers=True)
                st.plotly_chart(fig_pnl, use_container_width=True)

                if not strat_snaps.empty:
                    d1, d2 = st.columns(2)
                    with d1:
                        fig_theta = go.Figure()
                        for trade_id in strat_snaps['trade_id'].unique():
                            trade_data = strat_snaps[strat_snaps['trade_id'] == trade_id].sort_values('days_held')
                            fig_theta.add_trace(go.Scatter(x=trade_data['days_held'], y=trade_data['theta'], mode='lines+markers', name=f"{trade_data['name'].iloc[0][:15]} (Actual)", line=dict(width=2), showlegend=True))
                            fig_theta.add_trace(go.Scatter(x=trade_data['days_held'], y=trade_data['Theta_Expected'], mode='lines', name=f"{trade_data['name'].iloc[0][:15]} (Expected)", line=dict(dash='dash', width=1), opacity=0.5, showlegend=False))
                        fig_theta.update_layout(title=f"Theta: Actual vs Expected (Realistic Model)", xaxis_title="Days Held", yaxis_title="Theta ($)", hovermode='x unified')
                        st.plotly_chart(fig_theta, use_container_width=True)
                    with d2:
                        if 'gamma' in strat_snaps.columns and strat_snaps['gamma'].abs().sum() > 0:
                            strat_snaps['Gamma/Theta'] = np.where(strat_snaps['theta'] != 0, (strat_snaps['gamma'] * 100) / strat_snaps['theta'], 0)
                            fig_risk = px.line(strat_snaps, x='days_held', y='Gamma/Theta', color='name', title=f"Explosion Ratio (Gamma Risk)", labels={'days_held': 'Days Held', 'Gamma/Theta': 'Gamma% / Theta$ Ratio'})
                            st.plotly_chart(fig_risk, use_container_width=True)
                        else:
                            fig_delta = px.scatter(strat_snaps, x='days_held', y='delta', color='name', title=f"Delta Drift: {decay_strat}", labels={'days_held': 'Days', 'delta': 'Delta'}, trendline="lowess")
                            st.plotly_chart(fig_delta, use_container_width=True)

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
    st.markdown("###  The Quant Brain")
    st.caption("Self-learning insights based on trading history.")
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
                rot_df = check_rot_and_efficiency(active_trades, expired_df, rot_threshold, min_days_rot)
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
    st.markdown(adaptive_content)
    st.divider()
