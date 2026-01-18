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
st.set_page_config(page_title="Trade Guardian (FULL DEMO)", layout="wide", page_icon="🛡️")

# --- DEMO BANNER ---
st.warning("🧪 **DEMO SHOWCASE MODE**: This is a simulation. All financial data is randomly generated to demonstrate features.")
st.title("🛡️ Allantis Trade Guardian (Showcase)")

# --- DATABASE CONSTANTS ---
DB_NAME = "demo_showcase_v4.db"

# ==========================================
# 1. HELPER FUNCTIONS (Exact Math from Real App)
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
        
        # Distribute PnL (Simplified for demo speed)
        daily_val = total_pnl / days
        curr = trade['Entry Date']
        for _ in range(days):
            if curr.date() in daily_pnl_dict:
                daily_pnl_dict[curr.date()] += daily_val
            curr += pd.Timedelta(days=1)
    return daily_pnl_dict

def calculate_kelly_fraction(win_rate, avg_win, avg_loss):
    if avg_loss == 0 or avg_win <= 0: return 0.0
    b = abs(avg_win / avg_loss)
    kelly = (win_rate * b - (1 - win_rate)) / b
    return max(0, min(kelly * 0.5, 0.25))

def generate_trade_predictions(active_df, history_df, prob_low, prob_high, total_capital=100000):
    # Simulated prediction logic since we have fake history
    if active_df.empty: return pd.DataFrame()
    predictions = []
    for _, row in active_df.iterrows():
        win_prob = random.randint(40, 95)
        rec = "HOLD"
        if win_prob < prob_low: rec = "REDUCE/CLOSE"
        elif win_prob > prob_high: rec = "PRESS WINNER"
        
        predictions.append({
            'Trade Name': row['Name'], 'Strategy': row['Strategy'], 'Win Prob %': win_prob,
            'Expected PnL': random.randint(200, 1500), 'Kelly Size %': random.uniform(5, 15),
            'Rec. Size ($)': random.randint(8000, 15000), 'AI Rec': rec, 'Confidence': random.randint(60, 99)
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
    # Fake distance calculation for demo
    similar = historical_df.sample(n=min(len(historical_df), top_n)).copy()
    similar['Similarity %'] = [random.randint(70, 98) for _ in range(len(similar))]
    return similar[['Name', 'P&L', 'Days Held', 'ROI', 'Similarity %']]

def rolling_correlation_matrix(snaps, window_days=30):
    if snaps.empty: return None
    strat_daily = snaps.pivot_table(index='snapshot_date', columns='strategy', values='pnl', aggfunc='sum')
    if len(strat_daily) < window_days: return None
    last_30 = strat_daily.tail(30)
    corr_30 = last_30.corr()
    fig = px.imshow(corr_30, text_auto=".2f", aspect="auto", color_continuous_scale="RdBu", 
                    title="Strategy Correlation (Last 30 Days)", labels=dict(color="Correlation"))
    return fig

def calculate_max_drawdown(trades_df, initial_capital):
    if trades_df.empty: return {'Max Drawdown %': 0.0, 'Current DD %': 0.0}
    daily_pnl = reconstruct_daily_pnl(trades_df)
    dates = sorted(daily_pnl.keys())
    equity = [initial_capital]
    for d in dates: equity.append(equity[-1] + daily_pnl[d])
    eq_series = pd.Series(equity)
    running_max = eq_series.cummax()
    dd = (eq_series - running_max) / running_max
    return {'Max Drawdown %': dd.min() * 100, 'Current DD %': dd.iloc[-1] * 100}

# ==========================================
# 2. FAKE DATA GENERATOR
# ==========================================
def generate_fake_data():
    if os.path.exists(DB_NAME): os.remove(DB_NAME)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Create Schema (Exact Copy of Real App)
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT, parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT, snapshot_date DATE, pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL, FOREIGN KEY(trade_id) REFERENCES trades(id))''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (
                    name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER, min_stability REAL, description TEXT, typical_debit REAL)''')
    
    # Seed Config
    defaults = [
        ('130/160', '130/160', 500, 36, 0.8, 'Income Discipline', 4000),
        ('160/190', '160/190', 700, 44, 0.8, 'Patience Training', 5200),
        ('M200', 'M200', 900, 41, 0.8, 'Emotional Mastery', 8000),
        ('SMSF', 'SMSF', 600, 40, 0.8, 'Wealth Builder', 5000)
    ]
    c.executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)

    strategies = ['130/160', '160/190', 'M200', 'SMSF']
    
    # 1. Generate History (150 Trades)
    for i in range(150):
        strat = random.choice(strategies)
        status = "Expired"
        start_date = datetime.now() - timedelta(days=random.randint(60, 365))
        days = random.randint(20, 60)
        end_date = start_date + timedelta(days=days)
        
        # Win Rate Simulation
        if random.random() > 0.3: pnl = random.randint(400, 1500)
        else: pnl = random.randint(-1500, -100)
            
        debit = random.randint(4000, 9000)
        t_id = f"HIST_{i}_{strat}"
        
        # Add random Put/Call split for "Profit Anatomy" chart
        put_pnl = pnl * random.uniform(0.2, 0.8)
        call_pnl = pnl - put_pnl
        
        c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (t_id, f"Closed {strat} #{i}", strat, status, start_date.date(), end_date.date(),
                   days, debit, 1, pnl, 0, 0, 0, 0, "Simulation history", "Demo", "", 
                   put_pnl, call_pnl, 18.0, "http://optionstrat.com/demo", strat))

    # 2. Generate Active Trades (20 Trades with Snapshots)
    for i in range(20):
        strat = random.choice(strategies)
        status = "Active"
        days_active = random.randint(2, 50)
        start_date = datetime.now() - timedelta(days=days_active)
        debit = random.randint(4000, 7000)
        theta = random.randint(15, 40)
        delta = random.uniform(-8, 8)
        t_id = f"ACTIVE_{i}_{strat}"
        
        curr_pnl = -50
        for d in range(days_active):
            snap_date = start_date + timedelta(days=d)
            daily_decay = theta * (1 if d < 30 else 1.2) 
            noise = random.randint(-150, 100)
            curr_pnl += (daily_decay/4) + noise
            c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)",
                      (t_id, snap_date.date(), curr_pnl, d, theta, delta, 100, 0))
        
        c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (t_id, f"Active {strat} #{i}", strat, status, start_date.date(), None,
                   days_active, debit, 1, curr_pnl, theta, delta, 0, 100, "Active demo", "Demo", "", 
                   0, 0, 20.0, "http://optionstrat.com/demo", strat))

    # 3. Generate Roll Campaign (Linked Trades)
    parent_id = "ROLL_PARENT_001"
    c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (parent_id, "Bad Trade (Rolled)", "M200", "Expired", (datetime.now()-timedelta(days=100)).date(), (datetime.now()-timedelta(days=60)).date(),
               40, 5000, 1, -2000, 0, 0, 0, 0, "Rolled for defense", "Rolled", "", -1000, -1000, 20, "", "M200"))
    child_id = "ROLL_CHILD_001"
    c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (child_id, "Recovery Trade", "M200", "Active", (datetime.now()-timedelta(days=59)).date(), None,
               59, 6000, 1, 1200, 25, -5, 0, 80, "Recovering", "Hedged", parent_id, 0, 0, 18, "", "M200"))

    conn.commit()
    conn.close()

# --- DATABASE MANAGEMENT ---
def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    if not os.path.exists(DB_NAME):
        generate_fake_data()

# --- DATA LOADERS ---
@st.cache_data(ttl=60)
def load_strategy_config():
    conn = get_db_connection()
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

@st.cache_data(ttl=60)
def load_data():
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM trades", conn)
        if df.empty: return pd.DataFrame()
        
        # Capitalize Columns to match real app logic
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
            
        df['lot_size'] = df['lot_size'].replace(0, 1)
        df['Debit/Lot'] = df['Debit'] / df['lot_size']
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = (df['Theta'] / df['Debit'].replace(0,1)) * 100
        df['Stability'] = np.where(df['Theta'] > 0, df['Theta'] / (df['Delta'].abs() + 1), 0.0)
        df['P&L Vol'] = 0.0
        df['Parent ID'] = df['Parent ID'].fillna('').astype(str)
        df['Link'] = df['Link'].fillna('')

        def get_grade(row):
            d = row['Debit/Lot']
            if d > 6000: return "A", "Good Price"
            return "B", "Average"
        grades = df.apply(get_grade, axis=1, result_type='expand')
        df['Grade'] = grades[0]
        df['Reason'] = grades[1]
        
        return df
    except Exception as e:
        st.error(f"Data Load Error: {e}")
        return pd.DataFrame()
    finally: conn.close()

@st.cache_data(ttl=60)
def load_snapshots():
    conn = get_db_connection()
    try:
        q = """SELECT s.snapshot_date, s.pnl, s.days_held, s.theta, s.delta, s.vega, s.gamma, t.strategy, t.name, t.id as trade_id, t.theta as initial_theta FROM snapshots s JOIN trades t ON s.trade_id = t.id"""
        df = pd.read_sql(q, conn)
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
        for col in ['pnl', 'days_held', 'theta', 'delta', 'vega', 'gamma', 'initial_theta']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- MAIN EXECUTION ---
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

if df.empty or 'Status' not in df.columns:
    st.error("Data load failed. Click Regenerate Random Data.")
    st.stop()

expired_df = df[df['Status'] == 'Expired'].copy()
active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()

# --- TAB 1: DASHBOARD (with Pre-Flight) ---
with tab_dash:
    # 1. Pre-Flight Calculator (RESTORED)
    with st.expander(" Universal Pre-Flight Calculator", expanded=False):
        pf_c1, pf_c2, pf_c3 = st.columns(3)
        with pf_c1:
            pf_goal = st.selectbox("Strategy Profile", [" Hedged Income (Butterflies, Calendars)", " Standard Income (Credit Spreads)", " Directional", " Speculative Vol"])
            pf_dte = st.number_input("DTE (Days)", min_value=1, value=45)
        with pf_c2:
            pf_price = st.number_input("Net Price ($)", value=5000.0)
            pf_theta = st.number_input("Theta ($)", value=15.0)
        with pf_c3:
            pf_delta = st.number_input("Net Delta", value=-10.0)
            pf_vega = st.number_input("Vega", value=100.0)
            
        if st.button("Run Pre-Flight Check"):
            st.markdown("---")
            res_c1, res_c2, res_c3 = st.columns(3)
            if "Hedged Income" in pf_goal:
                stability = pf_theta / (abs(pf_delta) + 1)
                yield_pct = (pf_theta / abs(pf_price)) * 100
                with res_c1: st.success(f" Stability: {stability:.2f} (Fortress)" if stability > 1.0 else f" Stability: {stability:.2f}")
                with res_c2: st.info(f" Ann. ROI: {(yield_pct * 365):.0f}%")
                with res_c3: st.warning(" High Gamma Risk") if pf_dte < 21 else st.success(" DTE Safe")

    # 2. Ladder Logic (Exact Match)
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
                remaining_time_est = max(1, bench['dit'] - days)
                if recov_days > remaining_time_est:
                    score = 95
                    action = "STRUCTURAL FAILURE"
                    reason = f"Zombie (Recov {recov_days:.0f}d > Left {remaining_time_est:.0f}d)"
        else:
            juice_type = "Left in Tank"
            juice_val = max(0, target_profit - pnl)
            if juice_val < 100:
                score = 80
                action = "PREPARE EXIT"
                reason = "Tank Empty"

        if pnl >= target_profit:
            return "TAKE PROFIT", 100, f"Hit Target ${target_profit:.0f}", juice_val, juice_type
        
        if days > bench['dit'] * 1.25:
            score = max(score, 75)
            reason = "Stale"
            
        return action, score, reason, juice_val, juice_type

    if not active_df.empty:
        ladder_results = active_df.apply(lambda row: calculate_decision_ladder(row, dynamic_benchmarks), axis=1)
        active_df['Action'] = [x[0] for x in ladder_results]
        active_df['Urgency Score'] = [x[1] for x in ladder_results]
        active_df['Reason'] = [x[2] for x in ladder_results]
        active_df['Juice Val'] = [x[3] for x in ladder_results]
        active_df['Juice Type'] = [x[4] for x in ladder_results]
        active_df = active_df.sort_values('Urgency Score', ascending=False)
        todo_df = active_df[active_df['Urgency Score'] >= 70]

        # Top Metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Portfolio Health", "🟢 HEALTHY", "Simulated")
        c2.metric("Daily Income", f"${active_df['Theta'].sum():,.0f}")
        c3.metric("Floating P&L", f"${active_df['P&L'].sum():,.0f}")
        c4.metric("Action Items", len(todo_df), delta="Urgent" if len(todo_df) > 0 else "None", delta_color="inverse")

        # Heatmap
        st.divider()
        st.subheader("🗺️ Position Heat Map")
        fig_heat = px.scatter(
            active_df, x='Days Held', y='P&L', size='Debit',
            color='Urgency Score', color_continuous_scale='RdYlGn_r',
            hover_data=['Name', 'Strategy', 'Action'],
            title="Position Clustering (Size = Capital)"
        )
        st.plotly_chart(fig_heat, use_container_width=True)
        
        # Queue
        st.divider()
        if not todo_df.empty:
            with st.expander(f" Priority Action Queue ({len(todo_df)})", expanded=True):
                for _, row in todo_df.iterrows():
                    color = "red" if row['Urgency Score'] >= 90 else "orange"
                    st.markdown(f"**{row['Name']}**: :{color}[{row['Action']}] - {row['Reason']}")

# --- TAB 2: ACTIVE MANAGEMENT ---
with tab_active:
    sub_strat, sub_journal, sub_dna = st.tabs([" Strategy Detail", " Journal", " DNA Tool"])
    with sub_strat:
        st.markdown("### Strategy Performance (Live)")
        strategies = sorted(active_df['Strategy'].unique())
        if strategies:
            sel_strat = st.selectbox("Select Strategy", strategies)
            subset = active_df[active_df['Strategy'] == sel_strat]
            # Metrics Row
            c1, c2, c3 = st.columns(3)
            c1.metric("Target Profit", "$1,000")
            c2.metric("Avg Hold", "40d")
            c3.metric("Current P&L", f"${subset['P&L'].sum():,.0f}")
            # DataFrame
            cols = ['Name', 'Action', 'Urgency Score', 'P&L', 'Theta', 'Days Held', 'Juice Type', 'Juice Val']
            st.dataframe(subset[cols].style.format({'P&L': '${:,.0f}', 'Theta': '{:.1f}', 'Juice Val': '{:.0f}'}), use_container_width=True)
    with sub_journal:
        st.info("📝 Journal editing is disabled in Demo Mode.")
        st.dataframe(active_df[['Name', 'Strategy', 'Notes', 'Tags']], use_container_width=True)
    with sub_dna:
        st.subheader("Trade DNA (Similarity Search)")
        if not expired_df.empty and not active_df.empty:
            st.dataframe(find_similar_trades(active_df.iloc[0], expired_df), use_container_width=True)

# --- TAB 3: ANALYTICS (Full Suite) ---
with tab_analytics:
    an_overview, an_trends, an_risk, an_decay, an_rolls = st.tabs(["Overview", " Trends & Seasonality", " Risk & Excursion", " Decay & DNA", " Rolls"])
    
    with an_overview:
        if not expired_df.empty:
            st.subheader("Realized Equity Curve")
            expired_df = expired_df.sort_values("Exit Date")
            expired_df['Cumulative P&L'] = expired_df['P&L'].cumsum()
            fig = px.line(expired_df, x='Exit Date', y='Cumulative P&L', markers=True)
            st.plotly_chart(fig, use_container_width=True)
            
            # Profit Anatomy
            st.subheader(" Profit Anatomy: Call vs Put Contribution")
            strat_anatomy = expired_df.groupby('Strategy')[['Put P&L', 'Call P&L']].mean().reset_index()
            fig_strat_ana = go.Figure()
            fig_strat_ana.add_trace(go.Bar(y=strat_anatomy['Strategy'], x=strat_anatomy['Put P&L'], name='Avg Put Profit', orientation='h', marker_color='#EF553B'))
            fig_strat_ana.add_trace(go.Bar(y=strat_anatomy['Strategy'], x=strat_anatomy['Call P&L'], name='Avg Call Profit', orientation='h', marker_color='#00CC96'))
            fig_strat_ana.update_layout(barmode='relative', title="Average Profit Sources per Strategy (Stacked)")
            st.plotly_chart(fig_strat_ana, use_container_width=True)

    with an_trends:
        st.subheader(" Seasonality & Trends")
        # Seasonality Map
        exp_hm = expired_df.dropna(subset=['Exit Date']).copy()
        exp_hm['Month'] = exp_hm['Exit Date'].dt.month_name()
        exp_hm['Year'] = exp_hm['Exit Date'].dt.year
        hm_data = exp_hm.groupby(['Year', 'Month']).agg({'P&L': 'sum'}).reset_index()
        fig = px.density_heatmap(hm_data, x="Month", y="Year", z="P&L", title="Monthly Seasonality ($)", text_auto=True, color_continuous_scale="RdBu")
        st.plotly_chart(fig, use_container_width=True)
        # Entry Day
        exp_hm['Day'] = exp_hm['Entry Date'].dt.day_name()
        fig3 = px.density_heatmap(exp_hm, x="Day", y="Strategy", z="P&L", histfunc="avg", title="Best Entry Day (Avg P&L)", color_continuous_scale="RdBu")
        st.plotly_chart(fig3, use_container_width=True)

    with an_risk:
        st.subheader("Drawdown Analysis")
        mdd = calculate_max_drawdown(expired_df, total_cap)
        st.metric("Max Drawdown", f"{mdd['Max Drawdown %']:.1f}%")
        st.subheader("Strategy Correlation")
        snaps = load_snapshots()
        if not snaps.empty:
            fig_corr = rolling_correlation_matrix(snaps)
            if fig_corr: st.plotly_chart(fig_corr, use_container_width=True)

    with an_decay:
        st.subheader("Trade Life Cycle & Decay")
        snaps = load_snapshots()
        if not snaps.empty:
            trade_ids = snaps['trade_id'].unique()[:5] # Show first 5
            subset_snaps = snaps[snaps['trade_id'].isin(trade_ids)]
            fig_pnl = px.line(subset_snaps, x='days_held', y='pnl', color='trade_id', title="P&L Trajectory (Active Trades)")
            st.plotly_chart(fig_pnl, use_container_width=True)

    with an_rolls:
        st.subheader("Roll Campaign Analysis")
        rolled = df[df['Parent ID'] != ""]
        if not rolled.empty:
            st.success(f"Found {len(rolled)} linked trades in roll campaigns.")
            st.dataframe(rolled[['Name', 'Parent ID', 'P&L', 'Strategy']], use_container_width=True)
        else: st.info("No roll campaigns generated in this seed.")

# --- TAB 4: AI & INSIGHTS ---
with tab_ai:
    st.subheader("🧠 The Quant Brain")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Rot Detector** (Simulated)")
        rot = check_rot_and_efficiency(active_df, expired_df, 0.5, 10)
        if not rot.empty: st.dataframe(rot, use_container_width=True)
        else: st.success("No capital rot detected.")
    with c2:
        st.markdown("**Win Probability Forecast** (Simulated)")
        preds = generate_trade_predictions(active_df, expired_df, 40, 80)
        st.dataframe(preds, use_container_width=True)

# --- TAB 5: STRATEGIES ---
with tab_strategies:
    st.subheader("Strategy Configuration")
    st.dataframe(pd.DataFrame.from_dict(dynamic_benchmarks, orient='index'), use_container_width=True)

# --- TAB 6: RULES ---
with tab_rules:
    st.markdown("### Adaptive Rulebook")
    st.markdown("Based on the (simulated) historical data, here are the optimal parameters:")
    for strat in ['130/160', 'M200']:
        strat_df = expired_df[expired_df['Strategy'] == strat]
        if not strat_df.empty:
            avg_win = strat_df[strat_df['P&L'] > 0]['P&L'].mean()
            st.markdown(f"**{strat}**: Target Profit **${avg_win:,.0f}**")
