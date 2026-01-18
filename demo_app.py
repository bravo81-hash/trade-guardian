import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import re
import random
import time
from datetime import datetime, timezone, timedelta
from scipy.spatial.distance import cdist 

# --- PAGE CONFIG ---
st.set_page_config(page_title="Trade Guardian (Showcase)", layout="wide", page_icon="🛡️")

# --- DEMO BANNER ---
st.warning("🧪 **SHOWCASE MODE**: Using simulated market data. No real account connection.")
st.title("🛡️ Quant Trade Guardian")

# --- DATABASE CONSTANTS ---
DB_NAME = "demo_showcase_final.db"

# ==========================================
# 1. HELPER FUNCTIONS & MATH (Exact from Real App v146.7)
# ==========================================

def clean_num(x):
    try:
        if pd.isna(x) or str(x).strip() == "": return 0.0
        val_str = str(x).replace('$', '').replace(',', '').replace('%', '').strip()
        return float(val_str)
    except: return 0.0

def safe_fmt(val, fmt_str):
    try:
        if isinstance(val, (int, float)): return fmt_str.format(val)
        return str(val)
    except: return str(val)

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
        if t_frac < 0.5: decay_factor = 1 - (2 * t_frac) ** 2
        else: decay_factor = 2 * (1 - t_frac)
        return initial_theta * max(0, decay_factor)
    elif 'VERTICAL' in str(strategy).upper() or 'DIRECTIONAL' in str(strategy).upper():
        if t_frac < 0.7: decay_factor = 1 - t_frac
        else: decay_factor = 0.3 * np.exp(-5 * (t_frac - 0.7))
        return initial_theta * decay_factor
    return initial_theta * (1 - t_frac)

def reconstruct_daily_pnl(trades_df):
    trades = trades_df.copy()
    trades['Entry Date'] = pd.to_datetime(trades['Entry Date'])
    start_date = trades['Entry Date'].min()
    end_date = pd.Timestamp.now()
    date_range = pd.date_range(start=start_date, end=end_date)
    daily_pnl_dict = {d.date(): 0.0 for d in date_range}
    
    for _, trade in trades.iterrows():
        if trade['Status'] == 'Expired' and pd.isnull(trade['Exit Date']): continue
        days = trade['Days Held']
        if days <= 0: days = 1
        total_pnl = trade['P&L']
        strategy = trade['Strategy']
        initial_theta = trade['Theta'] if trade['Theta'] != 0 else 1.0
        
        daily_theta_weights = []
        for day in range(int(days)):
            expected_theta = theta_decay_model(initial_theta, day, strategy, max(45, days))
            daily_theta_weights.append(abs(expected_theta))
            
        total_theta_sum = sum(daily_theta_weights)
        if total_theta_sum == 0: daily_theta_weights = [1/days] * int(days)
        else: daily_theta_weights = [w/total_theta_sum for w in daily_theta_weights]
            
        curr = trade['Entry Date']
        for day_weight in daily_theta_weights:
            if curr.date() in daily_pnl_dict:
                daily_pnl_dict[curr.date()] += total_pnl * day_weight
            else:
                daily_pnl_dict[curr.date()] = total_pnl * day_weight
            curr += pd.Timedelta(days=1)
    return daily_pnl_dict

def calculate_kelly_fraction(win_rate, avg_win, avg_loss):
    if avg_loss == 0 or avg_win <= 0: return 0.0
    b = abs(avg_win / avg_loss)
    kelly = (win_rate * b - (1 - win_rate)) / b
    return max(0, min(kelly * 0.5, 0.25))

def calculate_portfolio_metrics(trades_df, capital):
    if trades_df.empty or capital <= 0: return 0.0, 0.0
    daily_pnl_dict = reconstruct_daily_pnl(trades_df)
    dates = sorted(daily_pnl_dict.keys())
    equity = [capital]
    for d in dates: equity.append(equity[-1] + daily_pnl_dict[d])
    equity_series = pd.Series(equity)
    daily_returns = equity_series.pct_change().dropna()
    if daily_returns.std() == 0: sharpe = 0.0
    else: sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
    total_pnl = trades_df['P&L'].sum()
    try: cagr = ((capital + total_pnl) / capital) ** (365 / max(1, len(dates))) - 1
    except: cagr = 0.0
    return sharpe, cagr * 100

def calculate_max_drawdown(trades_df, initial_capital):
    if trades_df.empty or initial_capital <= 0: return {'Max Drawdown %': 0.0, 'Current DD %': 0.0}
    daily_pnl_dict = reconstruct_daily_pnl(trades_df)
    dates = sorted(daily_pnl_dict.keys())
    equity = [initial_capital]
    for d in dates: equity.append(equity[-1] + daily_pnl_dict[d])
    equity_series = pd.Series(equity, index=pd.to_datetime(dates + [dates[-1] + timedelta(days=1)])) # Fix index length mismatch
    running_max = equity_series.cummax()
    drawdown = (equity_series - running_max) / running_max
    if drawdown.empty: return {'Max Drawdown %': 0.0, 'Current DD %': 0.0}
    return {'Max Drawdown %': drawdown.min() * 100, 'Current DD %': drawdown.iloc[-1] * 100}

def rolling_correlation_matrix(snaps, window_days=30):
    if snaps.empty: return None
    strat_daily = snaps.pivot_table(index='snapshot_date', columns='strategy', values='pnl', aggfunc='sum')
    if len(strat_daily) < 5: return None 
    corr = strat_daily.corr()
    fig = px.imshow(corr, text_auto=".2f", aspect="auto", color_continuous_scale="RdBu", 
                    title="Strategy Correlation (Simulated)", labels=dict(color="Correlation"))
    return fig

# --- INTELLIGENCE FUNCTIONS ---
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
        base = baseline_eff.get(strat, 0.5) 
        if base > 0 and curr_eff < (base * threshold_pct):
            rot_alerts.append({
                'Trade': row['Name'], 'Strategy': strat, 'Current Speed': f"${curr_eff:.1f}/day",
                'Baseline Speed': f"${base:.1f}/day", 'Raw Current': curr_eff, 'Raw Baseline': base,    
                'Status': ' ROTTING' if row['P&L'] > 0 else ' DEAD MONEY'
            })
    return pd.DataFrame(rot_alerts)

def generate_trade_predictions(active_df, history_df, prob_low, prob_high, total_capital=100000):
    if active_df.empty: return pd.DataFrame()
    predictions = []
    for _, row in active_df.iterrows():
        # Simulation Logic for Demo
        win_prob = random.randint(35, 95)
        rec = "HOLD"
        if win_prob < prob_low: rec = "REDUCE/CLOSE"
        elif win_prob > prob_high: rec = "PRESS WINNER"
        
        avg_pnl = random.randint(200, 1500)
        kelly = calculate_kelly_fraction(win_prob/100, avg_pnl, avg_pnl*0.5)
        
        predictions.append({
            'Trade Name': row['Name'], 'Strategy': row['Strategy'], 'Win Prob %': win_prob,
            'Expected PnL': avg_pnl, 'Kelly Size %': kelly * 100,
            'Rec. Size ($)': kelly * total_capital, 'AI Rec': rec, 'Confidence': random.randint(60, 99)
        })
    return pd.DataFrame(predictions)

def check_concentration_risk(active_df, total_equity, threshold=0.15):
    if active_df.empty: return pd.DataFrame()
    warnings = []
    for _, row in active_df.iterrows():
        concentration = row['Debit'] / total_equity
        if concentration > threshold:
            warnings.append({
                'Trade': row['Name'], 'Strategy': row['Strategy'], 'Size %': f"{concentration:.1%}",
                'Risk': f"${row['Debit']:,.0f}", 'Limit': f"{threshold:.0%}"
            })
    return pd.DataFrame(warnings)

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
    similar = historical_df.sample(n=min(len(historical_df), top_n)).copy()
    similar['Similarity %'] = [random.randint(70, 99) for _ in range(len(similar))]
    return similar[['Name', 'P&L', 'Days Held', 'ROI', 'Similarity %']]

def generate_adaptive_rulebook_text(history_df, strategies):
    text = "#  The Adaptive Trader's Constitution\n*Rules evolve based on simulated data.*\n\n"
    if history_df.empty: return text
    for strat in strategies:
        strat_df = history_df[history_df['Strategy'] == strat]
        if strat_df.empty: continue
        winners = strat_df[strat_df['P&L'] > 0]
        text += f"### {strat}\n"
        if not winners.empty:
            avg_win = winners['P&L'].mean()
            avg_hold = winners['Days Held'].mean()
            text += f"* ** Target Profit:** ${avg_win:,.0f}\n"
            text += f"* ** Optimal Hold:** {avg_hold:.0f} Days\n"
    return text

# ==========================================
# 2. FAKE DATA GENERATOR (Schema Matches Real App)
# ==========================================
def generate_fake_data():
    if os.path.exists(DB_NAME): os.remove(DB_NAME)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # 1. Create Schema
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT, parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT, snapshot_date DATE, pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL, FOREIGN KEY(trade_id) REFERENCES trades(id))''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (
                    name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER, min_stability REAL, description TEXT, typical_debit REAL)''')
    
    # 2. Config
    defaults = [
        ('130/160', '130/160', 500, 36, 0.8, 'Income', 4000), ('160/190', '160/190', 700, 44, 0.8, 'Patience', 5200),
        ('M200', 'M200', 900, 41, 0.8, 'Mastery', 8000), ('SMSF', 'SMSF', 600, 40, 0.8, 'Wealth', 5000)
    ]
    c.executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)

    strategies = ['130/160', '160/190', 'M200', 'SMSF']
    
    # 3. Generate History
    print("Generating history...")
    for i in range(150):
        strat = random.choice(strategies)
        status = "Expired"
        start_date = datetime.now() - timedelta(days=random.randint(60, 400))
        days = random.randint(20, 60)
        end_date = start_date + timedelta(days=days)
        
        if random.random() > 0.35: pnl = random.randint(300, 1500)
        else: pnl = random.randint(-1500, -200)
            
        debit = random.randint(4000, 9000)
        t_id = f"HIST_{i}_{strat}"
        put_pnl = pnl * random.uniform(0.1, 0.9)
        call_pnl = pnl - put_pnl
        
        c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (t_id, f"Closed {strat} #{i}", strat, status, start_date.date(), end_date.date(),
                   days, debit, 1, pnl, 0, 0, 0, 0, "Simulation", "Demo", "", 
                   put_pnl, call_pnl, 18.0, "http://example.com", strat))

    # 4. Generate Active Trades
    print("Generating active...")
    for i in range(20):
        strat = random.choice(strategies)
        status = "Active"
        days_active = random.randint(2, 50)
        start_date = datetime.now() - timedelta(days=days_active)
        debit = random.randint(4000, 7000)
        theta = random.randint(15, 40)
        delta = random.uniform(-10, 10)
        t_id = f"ACTIVE_{i}_{strat}"
        
        curr_pnl = -100
        for d in range(days_active):
            snap_date = start_date + timedelta(days=d)
            decay = theta * (1 if d < 30 else 1.5) 
            noise = random.randint(-150, 120)
            curr_pnl += (decay/5) + noise
            c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)",
                      (t_id, snap_date.date(), curr_pnl, d, theta, delta, 100, 0))
        
        c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (t_id, f"Active {strat} #{i}", strat, status, start_date.date(), None,
                   days_active, debit, 1, curr_pnl, theta, delta, 0, 100, "Active sim", "Demo", "", 
                   0, 0, 20.0, "http://example.com", strat))

    # 5. Roll Campaign
    parent_id = "ROLL_P1"
    c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (parent_id, "Rolled Leg (Loss)", "M200", "Expired", (datetime.now()-timedelta(days=100)).date(), (datetime.now()-timedelta(days=60)).date(),
               40, 5000, 1, -2000, 0, 0, 0, 0, "Rolled", "Rolled", "", -1000, -1000, 20, "", "M200"))
    child_id = "ROLL_C1"
    c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (child_id, "Recovery Leg (Win)", "M200", "Active", (datetime.now()-timedelta(days=59)).date(), None,
               59, 6000, 1, 1500, 25, -5, 0, 80, "Recovering", "Hedged", parent_id, 0, 0, 18, "", "M200"))

    conn.commit()
    conn.close()

# --- INIT DATABASE ---
def init_db():
    if not os.path.exists(DB_NAME): generate_fake_data()

# --- LOADERS ---
@st.cache_data(ttl=60)
def load_strategy_config():
    conn = sqlite3.connect(DB_NAME)
    try:
        df = pd.read_sql("SELECT * FROM strategy_config", conn)
        config = {}
        for _, row in df.iterrows():
            config[row['name']] = {
                'id': row['identifier'], 'pnl': row['target_pnl'], 'dit': row['target_days'],
                'stability': row['min_stability'], 'debit_per_lot': row['typical_debit']
            }
        return config
    except: return {}
    finally: conn.close()

# --- LOAD ALL DATA (Full Columns) ---
@st.cache_data(ttl=60)
def load_data():
    conn = sqlite3.connect(DB_NAME)
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        if df.empty: return pd.DataFrame()
        
        # --- Capitalize Columns ---
        df = df.rename(columns={
            'name': 'Name', 'strategy': 'Strategy', 'status': 'Status',
            'pnl': 'P&L', 'debit': 'Debit', 'days_held': 'Days Held',
            'theta': 'Theta', 'delta': 'Delta', 'gamma': 'Gamma', 'vega': 'Vega',
            'entry_date': 'Entry Date', 'exit_date': 'Exit Date', 'notes': 'Notes',
            'tags': 'Tags', 'parent_id': 'Parent ID', 'put_pnl': 'Put P&L',
            'call_pnl': 'Call P&L', 'iv': 'IV', 'link': 'Link', 'lot_size': 'lot_size'
        })

        df['Entry Date'] = pd.to_datetime(df['Entry Date'])
        df['Exit Date'] = pd.to_datetime(df['Exit Date'])
        for col in ['P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'lot_size', 'Put P&L', 'Call P&L']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        # --- DERIVED METRICS (Matched from Real App) ---
        df['lot_size'] = df['lot_size'].replace(0, 1)
        df['Debit/Lot'] = df['Debit'] / df['lot_size']
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = np.where(df['Debit'] > 0, (df['Theta'] / df['Debit']) * 100, 0)
        df['Stability'] = np.where(df['Theta'] > 0, df['Theta'] / (df['Delta'].abs() + 1), 0.0)
        df['P&L Vol'] = 0.0
        df['Parent ID'] = df['Parent ID'].fillna('').astype(str)
        df['Link'] = df['Link'].fillna('')

        def get_grade(row):
            s, d = row['Strategy'], row['Debit/Lot']
            if s == '130/160' and 3500 <= d <= 4500: return "A", "Sweet Spot"
            if s == 'M200' and 7500 <= d <= 8500: return "A", "Perfect Entry"
            return "B", "Acceptable"
            
        grades = df.apply(get_grade, axis=1, result_type='expand')
        df['Grade'] = grades[0]
        df['Reason'] = grades[1]
        
        return df
    except Exception as e:
        st.error(f"Data Load Error: {e}")
        return pd.DataFrame()
    finally: conn.close()

@st.cache_data(ttl=60)
def load_snaps():
    conn = sqlite3.connect(DB_NAME)
    try:
        q = """SELECT s.snapshot_date, s.pnl, s.days_held, s.theta, s.delta, s.vega, s.gamma, t.strategy, t.name, t.id as trade_id, t.theta as initial_theta FROM snapshots s JOIN trades t ON s.trade_id = t.id"""
        df = pd.read_sql(q, conn)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        for col in ['pnl', 'days_held', 'theta', 'delta', 'vega', 'gamma', 'initial_theta']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- MAIN EXECUTION START ---
init_db()
df = load_data()
dynamic_benchmarks = load_strategy_config()

# --- SIDEBAR ---
st.sidebar.header("🧪 Demo Controls")
if st.sidebar.button("🔄 Regenerate Random Data"):
    generate_fake_data()
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.header("Portfolio Settings")
prime_cap = st.sidebar.number_input("Prime Account", value=115000)
smsf_cap = st.sidebar.number_input("SMSF Account", value=150000)
total_cap = prime_cap + smsf_cap
market_regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish", "Bearish"])
regime_mult = 1.10 if "Bullish" in market_regime else 0.90 if "Bearish" in market_regime else 1.0

# --- TABS ---
tab_dash, tab_active, tab_analytics, tab_ai, tab_strategies, tab_rules = st.tabs([" Dashboard", " ⚡ Active Management", " Analytics", " AI & Insights", " Strategies", " Rules"])

expired_df = df[df['Status'] == 'Expired'].copy()
active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()

# --- LADDER LOGIC (Decision Brain) ---
def calculate_decision_ladder(row, benchmarks_dict):
    strat = row['Strategy']
    days = row['Days Held']
    pnl = row['P&L']
    theta = row['Theta']
    debit = row['Debit']
    lot_size = row['lot_size']
    bench = benchmarks_dict.get(strat, {'pnl': 1000, 'dit': 40})
    target_profit = bench['pnl'] * lot_size * regime_mult
    
    score = 50 
    action = "HOLD"
    reason = "Normal"
    juice_type = "Left in Tank"
    juice_val = 0.0

    if pnl < 0:
        juice_type = "Recovery Days"
        if theta > 0:
            recov_days = abs(pnl) / theta
            juice_val = recov_days
            if recov_days > 45:
                score = 95
                action = "STRUCTURAL FAILURE"
                reason = f"Zombie (Recov {recov_days:.0f}d)"
        else:
            juice_val = 999
    else:
        juice_type = "Left in Tank"
        juice_val = max(0, target_profit - pnl)
        if juice_val < 100:
            score = 80
            action = "PREPARE EXIT"
            reason = "Tank Empty"

    if pnl >= target_profit: return "TAKE PROFIT", 100, "Hit Target", juice_val, juice_type
    
    return action, score, reason, juice_val, juice_type

if not active_df.empty:
    ladder_results = active_df.apply(lambda row: calculate_decision_ladder(row, dynamic_benchmarks), axis=1)
    active_df['Action'] = [x[0] for x in ladder_results]
    active_df['Urgency Score'] = [x[1] for x in ladder_results]
    active_df['Reason'] = [x[2] for x in ladder_results]
    active_df['Juice Val'] = [x[3] for x in ladder_results]
    active_df['Juice Type'] = [x[4] for x in ladder_results]
    
    # Gauge Formatting
    def fmt_juice(row):
        if row['Juice Type'] == 'Recovery Days': return f"{row['Juice Val']:.0f} days"
        return f"${row['Juice Val']:.0f}"
    active_df['Gauge'] = active_df.apply(fmt_juice, axis=1)
    
    active_df = active_df.sort_values('Urgency Score', ascending=False)

# --- TAB 1: DASHBOARD ---
with tab_dash:
    with st.expander(" Universal Pre-Flight Calculator", expanded=False):
        pf_c1, pf_c2, pf_c3 = st.columns(3)
        with pf_c1:
            pf_goal = st.selectbox("Strategy Profile", [" Hedged Income", " Standard Income", " Directional", " Speculative"])
            pf_dte = st.number_input("DTE", 45)
        with pf_c2:
            pf_price = st.number_input("Price", 5000.0)
            pf_theta = st.number_input("Theta", 15.0)
        with pf_c3:
            pf_delta = st.number_input("Delta", -10.0)
            pf_vega = st.number_input("Vega", 100.0)
        if st.button("Run Pre-Flight Check"):
            st.success(f" Stability: {(pf_theta/(abs(pf_delta)+1)):.2f}")

    if not active_df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Portfolio Health", "🟢 HEALTHY", "Simulated")
        c2.metric("Daily Income", f"${active_df['Theta'].sum():,.0f}")
        c3.metric("Floating P&L", f"${active_df['P&L'].sum():,.0f}")
        todo = active_df[active_df['Urgency Score'] >= 70]
        c4.metric("Action Items", len(todo), delta="Urgent" if len(todo)>0 else None, delta_color="inverse")
        
        st.divider()
        st.subheader("🗺️ Position Heat Map")
        fig_heat = px.scatter(
            active_df, x='Days Held', y='P&L', size='Debit',
            color='Urgency Score', color_continuous_scale='RdYlGn_r',
            hover_data=['Name', 'Strategy', 'Action'],
            title="Position Clustering (Size = Capital)"
        )
        st.plotly_chart(fig_heat, use_container_width=True)
        
        st.divider()
        if not todo.empty:
            with st.expander(f" Priority Action Queue ({len(todo)})", expanded=True):
                for _, row in todo.iterrows():
                    color = "red" if row['Urgency Score'] >= 90 else "orange"
                    st.markdown(f"**{row['Name']}**: :{color}[{row['Action']}] - {row['Reason']}")

# --- TAB 2: ACTIVE ---
with tab_active:
    sub_strat, sub_journal, sub_dna = st.tabs([" Strategy Detail", " Journal", " DNA Tool"])
    with sub_strat:
        # Full Strategy Detail View (Restored Tables)
        st.markdown("### Strategy Performance (Live)")
        strategies = sorted(active_df['Strategy'].unique())
        if strategies:
            cols = ['Name', 'Link', 'Action', 'Urgency Score', 'Grade', 'Gauge', 'Stability', 'Theta/Cap %', 'Theta Eff.', 'P&L Vol', 'Daily Yield %', 'Ann. ROI', 'P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega']
            
            sel_strat = st.selectbox("Select Strategy", strategies)
            subset = active_df[active_df['Strategy'] == sel_strat]
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Target Profit", "$1,000")
            c2.metric("Avg Hold", "40d")
            c3.metric("Current P&L", f"${subset['P&L'].sum():,.0f}")
            
            # Full Table with conditional formatting
            st.dataframe(subset[cols].style.format({
                'Theta/Cap %': "{:.2f}%", 'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 
                'Daily Yield %': "{:.2f}%", 'Ann. ROI': "{:.1f}%", 'Theta Eff.': "{:.2f}", 
                'P&L Vol': "{:.1f}", 'Stability': "{:.2f}", 'Theta': "{:.1f}", 
                'Delta': "{:.1f}", 'Gamma': "{:.2f}", 'Vega': "{:.0f}", 'Days Held': "{:.0f}"
            }), use_container_width=True)
            
    with sub_journal:
        st.dataframe(active_df[['Name', 'Strategy', 'Notes', 'Tags']], use_container_width=True)
    with sub_dna:
        st.subheader("Trade DNA")
        if not expired_df.empty:
            st.dataframe(find_similar_trades(active_df.iloc[0], expired_df), use_container_width=True)

# --- TAB 3: ANALYTICS ---
with tab_analytics:
    an_overview, an_trends, an_risk, an_decay, an_rolls = st.tabs(["Overview", " Trends & Seasonality", " Risk & Excursion", " Decay & DNA", " Rolls"])
    
    with an_overview:
        if not expired_df.empty:
            st.subheader("Realized Equity Curve")
            expired_df = expired_df.sort_values("Exit Date")
            expired_df['Cumulative P&L'] = expired_df['P&L'].cumsum()
            fig = px.line(expired_df, x='Exit Date', y='Cumulative P&L', markers=True)
            st.plotly_chart(fig, use_container_width=True)
            
            c1, c2, c3 = st.columns(3)
            s_tot, c_tot = calculate_portfolio_metrics(expired_df, total_cap)
            c1.metric("Banked Profit", f"${expired_df['P&L'].sum():,.0f}")
            c2.metric("CAGR", f"{c_tot:.1f}%")
            c3.metric("Sharpe", f"{s_tot:.2f}")
            
            # Profit Anatomy
            st.subheader(" Profit Anatomy")
            strat_anatomy = expired_df.groupby('Strategy')[['Put P&L', 'Call P&L']].mean().reset_index()
            fig_strat_ana = go.Figure()
            fig_strat_ana.add_trace(go.Bar(y=strat_anatomy['Strategy'], x=strat_anatomy['Put P&L'], name='Avg Put Profit', orientation='h', marker_color='#EF553B'))
            fig_strat_ana.add_trace(go.Bar(y=strat_anatomy['Strategy'], x=strat_anatomy['Call P&L'], name='Avg Call Profit', orientation='h', marker_color='#00CC96'))
            fig_strat_ana.update_layout(barmode='relative', title="Average Profit Sources per Strategy")
            st.plotly_chart(fig_strat_ana, use_container_width=True)

    with an_trends:
        st.subheader(" Seasonality")
        exp_hm = expired_df.dropna(subset=['Exit Date']).copy()
        exp_hm['Month'] = exp_hm['Exit Date'].dt.month_name()
        exp_hm['Year'] = exp_hm['Exit Date'].dt.year
        hm_data = exp_hm.groupby(['Year', 'Month']).agg({'P&L': 'sum'}).reset_index()
        fig = px.density_heatmap(hm_data, x="Month", y="Year", z="P&L", title="Monthly Seasonality ($)", text_auto=True, color_continuous_scale="RdBu")
        st.plotly_chart(fig, use_container_width=True)

    with an_risk:
        st.subheader("Drawdown")
        mdd = calculate_max_drawdown(expired_df, total_cap)
        st.metric("Max Drawdown", f"{mdd['Max Drawdown %']:.1f}%")
        st.subheader("Strategy Correlation")
        snaps = load_snaps()
        if not snaps.empty:
            fig_corr = rolling_correlation_matrix(snaps)
            if fig_corr: st.plotly_chart(fig_corr, use_container_width=True)

    with an_decay:
        st.subheader("Trade Life Cycle")
        snaps = load_snaps()
        if not snaps.empty:
            trade_ids = snaps['trade_id'].unique()[:5]
            subset = snaps[snaps['trade_id'].isin(trade_ids)]
            fig_pnl = px.line(subset, x='days_held', y='pnl', color='trade_id')
            st.plotly_chart(fig_pnl, use_container_width=True)

    with an_rolls:
        st.subheader("Roll Campaign Analysis")
        rolled = df[df['Parent ID'] != ""]
        if not rolled.empty:
            st.success(f"Found {len(rolled)} linked trades.")
            st.dataframe(rolled[['Name', 'Parent ID', 'P&L', 'Strategy']], use_container_width=True)
        else: st.info("No rolled trades in this simulation seed.")

# --- TAB 4: AI ---
with tab_ai:
    st.subheader("🧠 The Quant Brain")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Rot Detector**")
        rot = check_rot_and_efficiency(active_df, expired_df, 0.5, 10)
        if not rot.empty: st.dataframe(rot, use_container_width=True)
        else: st.success("No capital rot detected.")
    with c2:
        st.markdown("**Win Probability Forecast**")
        preds = generate_trade_predictions(active_df, expired_df, 40, 80, total_cap)
        st.dataframe(preds, use_container_width=True)

# --- TAB 5 & 6 ---
with tab_strategies:
    st.subheader("Strategy Config")
    st.dataframe(pd.DataFrame.from_dict(dynamic_benchmarks, orient='index'), use_container_width=True)
with tab_rules:
    st.markdown(generate_adaptive_rulebook_text(expired_df, sorted(list(dynamic_benchmarks.keys()))))
