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
st.warning("🧪 **DEMO SHOWCASE MODE**: This is a full-feature simulation. All financial data is randomly generated.")
st.title("🛡️ Allantis Trade Guardian (Showcase)")

# --- DATABASE CONSTANTS ---
DB_NAME = "demo_showcase.db"

# --- HELPER FUNCTIONS ---
def extract_ticker(name):
    try:
        parts = str(name).split(' ')
        if parts: return parts[0]
    except: return "UNKNOWN"

def theta_decay_model(initial_theta, days_held, strategy, dte_at_entry=45):
    t_frac = min(1.0, days_held / dte_at_entry) if dte_at_entry > 0 else 1.0
    if strategy in ['M200', '130/160', '160/190', 'SMSF']:
        if t_frac < 0.5: decay_factor = 1 - (2 * t_frac) ** 2
        else: decay_factor = 2 * (1 - t_frac)
        return initial_theta * max(0, decay_factor)
    return initial_theta * (1 - t_frac)

def reconstruct_daily_pnl(trades_df):
    trades = trades_df.copy()
    trades['Entry Date'] = pd.to_datetime(trades['Entry Date'])
    start_date = trades['Entry Date'].min()
    end_date = pd.Timestamp.now()
    date_range = pd.date_range(start=start_date, end=end_date)
    daily_pnl_dict = {d.date(): 0.0 for d in date_range}
    for _, trade in trades.iterrows():
        if pd.isnull(trade['Exit Date']) and trade['Status'] == 'Expired': continue 
        days = trade['Days Held']
        if days <= 0: days = 1
        total_pnl = trade['P&L']
        curr = trade['Entry Date']
        # Simple linear attribution for demo speed
        daily_val = total_pnl / days
        for day in range(days):
            if curr.date() in daily_pnl_dict: daily_pnl_dict[curr.date()] += daily_val
            curr += pd.Timedelta(days=1)
    return daily_pnl_dict

# --- FAKE DATA GENERATOR (Advanced) ---
def generate_fake_data():
    if os.path.exists(DB_NAME): os.remove(DB_NAME)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # 1. Create Schema (Exact Copy of Real App)
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT, parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT, snapshot_date DATE, pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL, FOREIGN KEY(trade_id) REFERENCES trades(id))''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (
                    name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER, min_stability REAL, description TEXT, typical_debit REAL)''')
    
    # 2. Seed Strategy Config
    defaults = [
        ('130/160', '130/160', 500, 36, 0.8, 'Income Discipline', 4000),
        ('160/190', '160/190', 700, 44, 0.8, 'Patience Training', 5200),
        ('M200', 'M200', 900, 41, 0.8, 'Emotional Mastery', 8000),
        ('SMSF', 'SMSF', 600, 40, 0.8, 'Wealth Builder', 5000)
    ]
    c.executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)

    # 3. Generate History (Closed Trades) - 100 random trades
    strategies = ['130/160', '160/190', 'M200', 'SMSF']
    print("Generating history...")
    for i in range(100):
        strat = random.choice(strategies)
        status = "Expired"
        start_date = datetime.now() - timedelta(days=random.randint(60, 400))
        days = random.randint(20, 50)
        end_date = start_date + timedelta(days=days)
        
        # Realistic P&L logic
        if random.random() > 0.25: # 75% Win Rate
            pnl = random.randint(300, 1200)
        else:
            pnl = random.randint(-2000, -200)
            
        debit = random.randint(3500, 8500)
        t_id = f"HIST_{i}_{strat}"
        
        c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (t_id, f"Closed {strat} #{i}", strat, status, start_date.date(), end_date.date(),
                   days, debit, 1, pnl, 0, 0, 0, 0, "Simulation history", "Demo", "", 
                   pnl*0.7, pnl*0.3, 18.0, "", strat))

    # 4. Generate Active Trades with Snapshots
    print("Generating active...")
    for i in range(12):
        strat = random.choice(strategies)
        status = "Active"
        days_active = random.randint(1, 45)
        start_date = datetime.now() - timedelta(days=days_active)
        
        debit = random.randint(4000, 6000)
        
        # P&L Curve simulation
        pnl = 0
        theta = random.randint(15, 35)
        delta = random.uniform(-10, 10)
        
        t_id = f"ACTIVE_{i}_{strat}"
        
        # Create Snapshots
        curr_pnl = -50 # Start slightly red due to spread
        for d in range(days_active):
            snap_date = start_date + timedelta(days=d)
            daily_decay = theta * (1 if d < 30 else 1.5) # Acceleration
            noise = random.randint(-100, 80)
            curr_pnl += (daily_decay/5) + noise # Slow drift up
            
            c.execute("INSERT INTO snapshots (trade_id, snapshot_date, pnl, days_held, theta, delta, vega, gamma) VALUES (?,?,?,?,?,?,?,?)",
                      (t_id, snap_date.date(), curr_pnl, d, theta, delta, 100, 0))
        
        final_pnl = curr_pnl
        
        # Insert Active Trade
        c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (t_id, f"Active {strat} #{i}", strat, status, start_date.date(), None,
                   days_active, debit, 1, final_pnl, theta, delta, 0, 100, "Active simulation", "Demo", "", 
                   0, 0, 20.0, "", strat))

    # 5. Generate a Roll Campaign (Complex Data)
    parent_id = "ROLL_PARENT_001"
    c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (parent_id, "Bad Trade (Rolled)", "M200", "Expired", (datetime.now()-timedelta(days=100)).date(), (datetime.now()-timedelta(days=60)).date(),
               40, 5000, 1, -1500, 0, 0, 0, 0, "Rolled for defense", "Rolled", "", -1000, -500, 20, "", "M200"))
    
    child_id = "ROLL_CHILD_001"
    c.execute('''INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (child_id, "Recovery Trade", "M200", "Active", (datetime.now()-timedelta(days=59)).date(), None,
               59, 6000, 1, 800, 25, -5, 0, 80, "Recovering nicely", "Hedged", parent_id, 0, 0, 18, "", "M200"))

    conn.commit()
    conn.close()

# --- DATABASE ENGINE ---
def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    if not os.path.exists(DB_NAME):
        generate_fake_data()

# --- LOAD CONFIG ---
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

# --- DATA LOADER ---
@st.cache_data(ttl=60)
def load_data():
    conn = get_db_connection()
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
        
        # Numeric clean
        cols = ['P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega', 'lot_size', 'Put P&L', 'Call P&L']
        for c in cols: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
        df['lot_size'] = df['lot_size'].replace(0, 1)
        df['Debit/Lot'] = df['Debit'] / df['lot_size']
        df['ROI'] = (df['P&L'] / df['Debit'].replace(0, 1) * 100)
        df['Daily Yield %'] = np.where(df['Days Held'] > 0, df['ROI'] / df['Days Held'], 0)
        df['Ann. ROI'] = df['Daily Yield %'] * 365
        df['Theta Pot.'] = df['Theta'] * df['Days Held']
        df['Theta Eff.'] = np.where(df['Theta Pot.'] > 0, df['P&L'] / df['Theta Pot.'], 0.0)
        df['Theta/Cap %'] = (df['Theta'] / df['Debit'].replace(0,1)) * 100
        df['Stability'] = np.where(df['Theta'] > 0, df['Theta'] / (df['Delta'].abs() + 1), 0.0)
        df['P&L Vol'] = 0.0 # Sim
        
        # Fix Parent ID
        df['Parent ID'] = df['Parent ID'].fillna('').astype(str)

        def get_grade(row):
            d = row['Debit/Lot']
            if d > 6000: return "A", "Good Price"
            return "B", "Average"
        
        grades = df.apply(get_grade, axis=1, result_type='expand')
        df['Grade'] = grades[0]
        df['Reason'] = grades[1]
        
        return df
    except Exception as e:
        st.error(f"Load Error: {e}")
        return pd.DataFrame()
    finally: conn.close()

@st.cache_data(ttl=60)
def load_snapshots():
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
        return df
    except: return pd.DataFrame()
    finally: conn.close()

# --- INTELLIGENCE FUNCTIONS ---
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

def generate_trade_predictions(active_df, history_df):
    if active_df.empty: return pd.DataFrame()
    predictions = []
    for _, row in active_df.iterrows():
        # Simulation of KNN logic
        win_prob = random.randint(45, 95)
        rec = "HOLD"
        if win_prob < 50: rec = "REDUCE/CLOSE"
        elif win_prob > 80: rec = "PRESS WINNER"
        
        predictions.append({
            'Trade Name': row['Name'], 'Strategy': row['Strategy'], 
            'Win Prob %': win_prob,
            'Expected PnL': random.randint(200, 1500), 
            'Kelly Size %': random.uniform(5, 12), 
            'Rec. Size ($)': random.randint(8000, 12000),
            'AI Rec': rec, 
            'Confidence': random.randint(70, 99)
        })
    return pd.DataFrame(predictions)

def calculate_decision_ladder(row, benchmarks_dict):
    # Simplified ladder for demo speed
    strat = row['Strategy']
    pnl = row['P&L']
    days = row['Days Held']
    bench = benchmarks_dict.get(strat, {'pnl': 1000, 'dit': 40})
    target = bench['pnl']
    
    score = 50
    action = "HOLD"
    reason = "Normal"
    juice_type = "Left in Tank"
    juice_val = max(0, target - pnl)
    
    if pnl > target: return "TAKE PROFIT", 100, "Hit Target", juice_val, juice_type
    if days > bench['dit'] * 1.2: return "KILL", 90, "Stale", juice_val, juice_type
    
    if pnl < -1000:
        score = 80
        action = "REVIEW"
        reason = "Drawdown"
        juice_type = "Recovery Days"
        juice_val = 25 # Fake recovery
        
    return action, score, reason, juice_val, juice_type

def calculate_max_drawdown(trades_df, initial_capital):
    if trades_df.empty: return {'Max Drawdown %': 0.0}
    daily = reconstruct_daily_pnl(trades_df)
    dates = sorted(daily.keys())
    equity = [initial_capital]
    for d in dates: equity.append(equity[-1] + daily[d])
    
    eq_series = pd.Series(equity)
    running_max = eq_series.cummax()
    dd = (eq_series - running_max) / running_max
    return {'Max Drawdown %': dd.min() * 100}

def rolling_correlation_matrix(snaps, window_days=30):
    if snaps.empty: return None
    strat_daily = snaps.pivot_table(index='snapshot_date', columns='strategy', values='pnl', aggfunc='sum')
    if len(strat_daily) < window_days: return None
    last_30 = strat_daily.tail(30)
    corr_30 = last_30.corr()
    fig = px.imshow(corr_30, text_auto=".2f", aspect="auto", color_continuous_scale="RdBu", 
                    title="Strategy Correlation (Last 30 Days)", labels=dict(color="Correlation"))
    return fig

# --- INIT ---
init_db()
df = load_data()
dynamic_benchmarks = load_strategy_config()

# --- SIDEBAR ---
st.sidebar.header("🧪 Simulation Controls")
if st.sidebar.button("🎲 Generate New Scenario"):
    generate_fake_data()
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
prime_cap = st.sidebar.number_input("Prime Account", value=115000)
smsf_cap = st.sidebar.number_input("SMSF Account", value=150000)
total_cap = prime_cap + smsf_cap
market_regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish", "Bearish"])
regime_mult = 1.10 if "Bullish" in market_regime else 0.90 if "Bearish" in market_regime else 1.0

# --- TABS ---
tab_dash, tab_active, tab_analytics, tab_ai, tab_strategies, tab_rules = st.tabs([" Dashboard", " ⚡ Active Management", " Analytics", " AI & Insights", " Strategies", " Rules"])

if df.empty or 'Status' not in df.columns:
    st.error("No data generated.")
    st.stop()

# --- DASHBOARD TAB ---
with tab_dash:
    active_df = df[df['Status'] == 'Active'].copy()
    if not active_df.empty:
        # Metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Portfolio Health", "🟢 HEALTHY", "Simulated")
        c2.metric("Daily Income", f"${active_df['Theta'].sum():,.0f}")
        c3.metric("Floating P&L", f"${active_df['P&L'].sum():,.0f}")
        
        # Calculate Ladder
        ladder = active_df.apply(lambda r: calculate_decision_ladder(r, dynamic_benchmarks), axis=1)
        active_df['Action'] = [x[0] for x in ladder]
        active_df['Urgency Score'] = [x[1] for x in ladder]
        
        urgent_count = len(active_df[active_df['Urgency Score'] >= 70])
        c4.metric("Action Items", urgent_count, delta="Urgent" if urgent_count > 0 else "None", delta_color="inverse")
        
        st.divider()
        st.subheader("🗺️ Position Heat Map")
        fig_heat = px.scatter(
            active_df, x='Days Held', y='P&L', size='Debit',
            color='Urgency Score', color_continuous_scale='RdYlGn_r',
            hover_data=['Name', 'Strategy', 'Action'],
            title="Position Clustering (Size = Capital)"
        )
        st.plotly_chart(fig_heat, use_container_width=True)

# --- ACTIVE MANAGEMENT TAB ---
with tab_active:
    if not active_df.empty:
        # Re-apply ladder for detail
        ladder = active_df.apply(lambda r: calculate_decision_ladder(r, dynamic_benchmarks), axis=1)
        active_df['Action'] = [x[0] for x in ladder]
        active_df['Reason'] = [x[2] for x in ladder]
        
        sub_strat, sub_journal, sub_dna = st.tabs([" Strategy Detail", " Journal", " DNA Tool"])
        
        with sub_strat:
            st.markdown("### Strategy Performance (Live)")
            cols = ['Name', 'Strategy', 'Action', 'P&L', 'Theta', 'Days Held']
            st.dataframe(active_df[cols].style.format({'P&L': '${:,.0f}', 'Theta': '{:.1f}'}), use_container_width=True)
            
        with sub_journal:
            st.info("📝 Journal editing is disabled in Demo Mode.")
            st.dataframe(active_df[['Name', 'Strategy', 'Notes', 'Tags']], use_container_width=True)
            
        with sub_dna:
            st.subheader("Trade DNA (Similarity Search)")
            st.caption("Matches active trades to historical winners.")
            st.dataframe(active_df.head(3)[['Name', 'Theta/Cap %', 'Delta']], use_container_width=True)

# --- ANALYTICS TAB ---
with tab_analytics:
    an_overview, an_risk, an_rolls = st.tabs(["Overview", "Risk & Drawdown", "Rolls"])
    
    expired_df = df[df['Status'] == 'Expired'].copy()
    
    with an_overview:
        if not expired_df.empty:
            st.subheader("Realized Equity Curve")
            expired_df = expired_df.sort_values("Exit Date")
            expired_df['Cumulative P&L'] = expired_df['P&L'].cumsum()
            fig = px.line(expired_df, x='Exit Date', y='Cumulative P&L', markers=True)
            st.plotly_chart(fig, use_container_width=True)
            
            c1, c2 = st.columns(2)
            c1.metric("Total Banked Profit", f"${expired_df['P&L'].sum():,.0f}")
            c2.metric("Win Rate", f"{(len(expired_df[expired_df['P&L']>0])/len(expired_df)*100):.1f}%")

    with an_risk:
        st.subheader("Drawdown Analysis")
        mdd = calculate_max_drawdown(expired_df, total_cap)
        st.metric("Max Drawdown", f"{mdd['Max Drawdown %']:.1f}%")
        
        # Correlation Matrix
        st.subheader("Strategy Correlation")
        snaps = load_snapshots()
        if not snaps.empty:
            fig_corr = rolling_correlation_matrix(snaps)
            if fig_corr: st.plotly_chart(fig_corr, use_container_width=True)
            else: st.info("Not enough snapshot history generated.")

    with an_rolls:
        st.subheader("Roll Campaign Analysis")
        rolled = df[df['Parent ID'] != ""]
        if not rolled.empty:
            st.success(f"Found {len(rolled)} linked trades in roll campaigns.")
            st.dataframe(rolled[['Name', 'Parent ID', 'P&L']], use_container_width=True)
        else:
            st.info("No roll campaigns generated in this seed.")

# --- AI TAB ---
with tab_ai:
    st.subheader("🧠 The Quant Brain")
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Rot Detector**")
        rot = check_rot_and_efficiency(active_df, expired_df, 0.5, 10)
        if not rot.empty:
            st.dataframe(rot, use_container_width=True)
        else:
            st.success("No capital rot detected.")
            
    with c2:
        st.markdown("**Win Probability Forecast**")
        preds = generate_trade_predictions(active_df, expired_df)
        st.dataframe(preds, use_container_width=True)

# --- STRATEGIES TAB ---
with tab_strategies:
    st.subheader("Strategy Configuration")
    st.dataframe(pd.DataFrame.from_dict(dynamic_benchmarks, orient='index'), use_container_width=True)

# --- RULES TAB ---
with tab_rules:
    st.markdown("### Adaptive Rulebook")
    st.markdown("Based on the (simulated) historical data, here are the optimal parameters:")
    
    for strat in ['130/160', 'M200']:
        strat_df = expired_df[expired_df['Strategy'] == strat]
        if not strat_df.empty:
            avg_win = strat_df[strat_df['P&L'] > 0]['P&L'].mean()
            st.markdown(f"**{strat}**: Target Profit **${avg_win:,.0f}**")
