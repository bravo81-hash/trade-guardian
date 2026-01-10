import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import re
from datetime import datetime
from openpyxl import load_workbook
from scipy import stats 
from scipy.spatial.distance import cdist 

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

# --- DEBUG BANNER ---
st.info("✅ RUNNING VERSION: v149.0 (Full Restoration: Trends Tab, All AI Features, & Stability Fixes)")

st.title("🛡️ Allantis Trade Guardian")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian_v4.db"

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE, days_held INTEGER, debit REAL, lot_size INTEGER, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL, notes TEXT, tags TEXT, parent_id TEXT, put_pnl REAL, call_pnl REAL, iv REAL, link TEXT, original_group TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT, snapshot_date DATE, pnl REAL, days_held INTEGER, theta REAL, delta REAL, vega REAL, gamma REAL, FOREIGN KEY(trade_id) REFERENCES trades(id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS strategy_config (name TEXT PRIMARY KEY, identifier TEXT, target_pnl REAL, target_days INTEGER, min_stability REAL, description TEXT, typical_debit REAL)''')
    
    def add_column_safe(table, col_name, col_type):
        try: c.execute(f"SELECT {col_name} FROM {table} LIMIT 1")
        except: 
            try: c.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
            except: pass

    add_column_safe('snapshots', 'theta', 'REAL')
    add_column_safe('snapshots', 'delta', 'REAL')
    add_column_safe('snapshots', 'vega', 'REAL')
    add_column_safe('snapshots', 'gamma', 'REAL')
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
        if force_reset: c.execute("DELETE FROM strategy_config")
        c.execute("SELECT count(*) FROM strategy_config")
        if c.fetchone()[0] == 0:
            defaults = [
                ('130/160', '130/160', 500, 36, 0.8, 'Income Discipline', 4000),
                ('160/190', '160/190', 700, 44, 0.8, 'Patience Training', 5200),
                ('M200', 'M200', 900, 41, 0.8, 'Emotional Mastery', 8000),
                ('SMSF', 'SMSF', 600, 40, 0.8, 'Wealth Builder', 5000)
            ]
            c.executemany("INSERT INTO strategy_config VALUES (?,?,?,?,?,?,?)", defaults)
            conn.commit()
    finally: conn.close()

# --- UTILS ---
def clean_num(x):
    try:
        if pd.isna(x) or str(x).strip() == "": return 0.0
        val = float(str(x).replace('$', '').replace(',', '').replace('%', '').strip())
        return 0.0 if np.isnan(val) else val
    except: return 0.0

def safe_fmt(val, fmt_str):
    try: return fmt_str.format(val) if isinstance(val, (int, float)) else str(val)
    except: return str(val)

def generate_id(name, strategy, entry_date):
    d_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    safe_name = re.sub(r'\W+', '', str(name))
    return f"{safe_name}_{strategy}_{d_str}"

def extract_ticker(name):
    try:
        parts = str(name).split(' ')
        if parts:
            ticker = parts[0].replace('.', '').upper()
            return "UNKNOWN" if ticker in ['M200', '130', '160', 'IRON', 'VERTICAL', 'SMSF'] else ticker
        return "UNKNOWN"
    except: return "UNKNOWN"

# --- CORE LOGIC FUNCTIONS ---
def get_strategy_dynamic(trade_name, group_name, config_dict):
    t_name = str(trade_name).upper().strip()
    g_name = str(group_name).upper().strip()
    sorted_strats = sorted(config_dict.items(), key=lambda x: len(str(x[1]['id'])), reverse=True)
    for strat_name, details in sorted_strats:
        if str(details['id']).upper() in t_name: return strat_name
    for strat_name, details in sorted_strats:
        if str(details['id']).upper() in g_name: return strat_name
    return "Other"

def theta_decay_model(initial_theta, days_held, strategy, dte_at_entry=45):
    if dte_at_entry <= 0: return initial_theta
    t_frac = min(1.0, days_held / dte_at_entry)
    if any(s in str(strategy).upper() for s in ['M200', '130/160', '160/190', 'FLY', 'CONDOR']): 
        if t_frac < 0.5: decay_factor = 1 - (2 * t_frac) ** 2 
        else: decay_factor = 2 * (1 - t_frac)
        return initial_theta * max(0, decay_factor)
    elif any(s in str(strategy).upper() for s in ['VERTICAL', 'DIRECTIONAL', 'LONG']):
        if t_frac < 0.7: decay_factor = 1 - t_frac
        else: decay_factor = 0.3 * np.exp(-5 * (t_frac - 0.7))
        return initial_theta * max(0, decay_factor)
    else:
        decay_factor = np.exp(-2 * t_frac)
        return initial_theta * max(0, decay_factor)

def reconstruct_daily_pnl_realistic(trades_df):
    daily_pnl_dict = {}
    for _, trade in trades_df.iterrows():
        if pd.isnull(trade['Exit Date']) or trade['Days Held'] <= 0: continue
        days = int(trade['Days Held'])
        total_pnl = trade['P&L']
        strategy = trade['Strategy']
        
        daily_theta_weights = []
        for day in range(days):
            expected_theta = theta_decay_model(1.0, day, strategy, days)
            daily_theta_weights.append(expected_theta)
            
        total_theta = sum(daily_theta_weights)
        if total_theta == 0: daily_theta_weights = [1/days] * days
        else: daily_theta_weights = [w/total_theta for w in daily_theta_weights]
            
        curr = trade['Entry Date']
        for day_weight in daily_theta_weights:
            if curr.date() in daily_pnl_dict: daily_pnl_dict[curr.date()] += total_pnl * day_weight
            else: daily_pnl_dict[curr.date()] = total_pnl * day_weight
            curr += pd.Timedelta(days=1)
    return daily_pnl_dict

def calculate_portfolio_metrics(trades_df, capital):
    if trades_df.empty or capital <= 0: return 0.0, 0.0
    daily_pnl_dict = reconstruct_daily_pnl_realistic(trades_df)
    dates = sorted(daily_pnl_dict.keys())
    if not dates: return 0.0, 0.0
    
    date_range = pd.date_range(start=min(dates), end=max(dates))
    equity = capital
    daily_equity_values = []
    
    for d in date_range:
        day_pnl = daily_pnl_dict.get(d.date(), 0)
        equity += day_pnl
        daily_equity_values.append(equity)
        
    equity_series = pd.Series(daily_equity_values)
    daily_returns = equity_series.pct_change().dropna()
    
    sharpe = 0.0 if daily_returns.std() == 0 else (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
    
    total_days = (date_range[-1] - date_range[0]).days
    if total_days < 1: total_days = 1
    end_val = equity_series.iloc[-1]
    
    try: cagr = ( (end_val / capital) ** (365 / total_days) ) - 1
    except: cagr = 0.0
    
    return sharpe, cagr * 100

def calculate_max_drawdown(trades_df, initial_capital):
    if trades_df.empty or initial_capital <= 0: return {'Max Drawdown %': 0.0, 'Current DD %': 0.0}
    daily_pnl_dict = reconstruct_daily_pnl_realistic(trades_df)
    dates = sorted(daily_pnl_dict.keys())
    if not dates: return {'Max Drawdown %': 0.0, 'Current DD %': 0.0}
    
    date_range = pd.date_range(start=min(dates), end=max(dates))
    equity = initial_capital
    equity_curve = []
    for d in date_range:
        equity += daily_pnl_dict.get(d.date(), 0)
        equity_curve.append(equity)
        
    equity_series = pd.Series(equity_curve)
    running_max = equity_series.cummax()
    drawdown = (equity_series - running_max) / running_max
    return {'Max Drawdown %': drawdown.min() * 100, 'Current DD %': drawdown.iloc[-1] * 100}

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
    
    if status == 'Missing': return "REVIEW", 100, "Missing from data", 0, "Error"
    
    regime_mult = 1.0 
    bench = benchmarks_dict.get(strat, {})
    hist_avg_pnl = bench.get('pnl', 1000)
    target_profit = (hist_avg_pnl * regime_mult) * lot_size
    hist_avg_days = bench.get('dit', 40)
    
    score = 50; action = "HOLD"; reason = "Normal"; juice_val = 0.0; juice_type = "Neutral"
    
    if pnl < 0:
        juice_type = "Recovery Days"
        if theta > 0:
            recov_days = abs(pnl) / theta
            juice_val = recov_days
            is_cooking = (strat == '160/190' and days < 30)
            is_young = days < 15
            if not is_cooking and not is_young:
                remaining_time_est = max(1, hist_avg_days - days)
                if recov_days > remaining_time_est:
                    score += 40
                    action = "STRUCTURAL FAILURE"
                    reason = f"Zombie (Recov {recov_days:.0f}d > Left {remaining_time_est:.0f}d)"
        else:
            juice_val = 999
            if days > 15: score += 30; reason = "Negative Theta"
    else:
        juice_type = "Left in Tank"
        left_in_tank = max(0, target_profit - pnl)
        juice_val = left_in_tank
        if debit > 0 and (left_in_tank / debit) < 0.05: score += 40; reason = "Squeezed Dry (Risk > Reward)"
        elif left_in_tank < (100 * lot_size): score += 35; reason = f"Empty Tank (<${100*lot_size})"

    if pnl >= target_profit: return "TAKE PROFIT", 100, f"Hit Target ${target_profit:.0f}", juice_val, juice_type
    elif pnl >= target_profit * 0.8: score += 30; action = "PREPARE EXIT"; reason = "Near Target"
        
    stale_threshold = hist_avg_days * 1.25 
    if strat == '130/160':
        limit_130 = min(stale_threshold, 30) 
        if days > limit_130 and pnl < (100 * lot_size): return "KILL", 95, f"Stale (> {limit_130:.0f}d)", juice_val, juice_type
        elif days > (limit_130 * 0.8): score += 20; reason = "Aging"
    elif strat == '160/190':
        cooking_limit = max(30, hist_avg_days * 0.7)
        if days < cooking_limit: score = 10; action = "COOKING"; reason = f"Too Early (<{cooking_limit:.0f}d)"
        elif days > stale_threshold: score += 25; action = "WATCH"; reason = f"Mature (>{stale_threshold:.0f}d)"
    elif strat == 'M200':
        if 13 <= days <= 15: score += 10; action = "DAY 14 CHECK"; reason = "Scheduled Review"
            
    if stability < 0.3 and days > 5: score += 25; reason += " + Coin Flip (Unstable)"; action = "RISK REVIEW"
    if row['Theta Eff.'] < 0.2 and days > 10: score += 15; reason += " + Bad Decay"
    
    score = min(100, max(0, score))
    if score >= 90: action = "CRITICAL"
    elif score >= 70: action = "WATCH"
    elif score <= 30: action = "COOKING"
    return action, score, reason, juice_val, juice_type

def find_similar_trades(current_trade, historical_df, top_n=3):
    if historical_df.empty: return pd.DataFrame()
    features = ['Theta/Cap %', 'Delta', 'Debit/Lot']
    for f in features:
        if f not in current_trade or f not in historical_df.columns: return pd.DataFrame()
    curr_vec = np.nan_to_num(current_trade[features].values.astype(float)).reshape(1, -1)
    hist_vecs = np.nan_to_num(historical_df[features].values.astype(float))
    distances = cdist(curr_vec, hist_vecs, metric='euclidean')[0]
    similar_idx = np.argsort(distances)[:top_n]
    similar = historical_df.iloc[similar_idx].copy()
    max_dist = distances.max() if distances.max() > 0 else 1
    similar['Similarity %'] = 100 * (1 - distances[similar_idx] / max_dist)
    return similar[['Name', 'P&L', 'Days Held', 'ROI', 'Similarity %']]

def calculate_kelly_fraction(win_rate, avg_win, avg_loss):
    if avg_loss == 0 or avg_win <= 0: return 0.0
    b = abs(avg_win / avg_loss)
    if b == 0: return 0.0
    p = win_rate
    q = 1 - p
    kelly = (p * b - q) / b
    return max(0, min(kelly * 0.5, 0.25))

def generate_trade_predictions(active_df, history_df, prob_low, prob_high, total_equity):
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
        
        avg_win = nearest_neighbors[nearest_neighbors['P&L'] > 0]['P&L'].mean() if not nearest_neighbors[nearest_neighbors['P&L'] > 0].empty else 0
        avg_loss = nearest_neighbors[nearest_neighbors['P&L'] < 0]['P&L'].mean() if not nearest_neighbors[nearest_neighbors['P&L'] < 0].empty else -1
        if pd.isna(avg_loss) or avg_loss == 0: avg_loss = -avg_win * 0.5 if avg_win > 0 else -100

        kelly_size = calculate_kelly_fraction(win_prob, avg_win, avg_loss)
        avg_dist = distances[top_k_idx].mean()
        confidence = max(0, 100 - (avg_dist * 10))
        
        win_prob_pct = win_prob * 100
        rec = "HOLD"
        if win_prob_pct < prob_low: rec = "REDUCE/CLOSE"
        elif win_prob_pct > prob_high: rec = "PRESS WINNER"
        
        predictions.append({
            'Trade Name': row['Name'], 'Strategy': row['Strategy'], 'Win Prob %': win_prob_pct,
            'Expected PnL': avg_pnl, 'Kelly Size': f"{kelly_size:.1%}",
            'Rec. $': f"${kelly_size * total_equity:,.0f}", 'AI Rec': rec, 'Confidence': confidence
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
            rot_alerts.append({'Trade': row['Name'], 'Strategy': strat, 'Current Speed': f"${curr_eff:.1f}/day", 'Baseline Speed': f"${base:.1f}/day", 'Raw Current': curr_eff, 'Raw Baseline': base, 'Status': '⚠️ ROTTING' if row['P&L'] > 0 else '💀 DEAD MONEY'})
    return pd.DataFrame(rot_alerts)

def get_dynamic_targets(history_df, percentile):
    if history_df.empty: return {}
    winners = history_df[history_df['P&L'] > 0]
    if winners.empty: return {}
    targets = {}
    for strat, grp in winners.groupby('Strategy'):
        targets[strat] = {'Median Win': grp['P&L'].median(), 'Optimal Exit': grp['P&L'].quantile(percentile)}
    return targets

def check_concentration_risk(active_df, total_equity, threshold=0.15):
    if active_df.empty or total_equity <= 0: return pd.DataFrame()
    warnings = []
    for _, row in active_df.iterrows():
        concentration = row['Debit'] / total_equity
        if concentration > threshold:
            warnings.append({'Trade': row['Name'], 'Strategy': row['Strategy'], 'Size %': f"{concentration:.1%}", 'Risk': f"${row['Debit']:,.0f}", 'Limit': f"{threshold:.0%}"})
    return pd.DataFrame(warnings)

def rolling_correlation_matrix(snaps, window_days=30):
    if snaps.empty: return None
    strat_daily = snaps.pivot_table(index='snapshot_date', columns='strategy', values='pnl', aggfunc='sum')
    if len(strat_daily) < window_days: return None
    last_30 = strat_daily.tail(30)
    corr_30 = last_30.corr()
    fig = px.imshow(corr_30, text_auto=".2f", aspect="auto", color_continuous_scale="RdBu", title="Strategy Correlation (Last 30 Days)", labels=dict(color="Correlation"))
    return fig

def generate_adaptive_rulebook_text(history_df, strategies):
    text = "# 📖 The Adaptive Trader's Constitution\n*Rules evolve. This book rewrites itself based on your actual data.*\n\n"
    if history_df.empty: return text + "⚠️ *Not enough data yet. Complete more trades to unlock adaptive rules.*"
    for strat in strategies:
        strat_df = history_df[history_df['Strategy'] == strat]
        if strat_df.empty: continue
        winners = strat_df[strat_df['P&L'] > 0]
        text += f"### {strat}\n"
        if not winners.empty:
            winners = winners.copy()
            winners['Day'] = winners['Entry Date'].dt.day_name()
            best_day = winners.groupby('Day')['P&L'].mean().idxmax()
            text += f"* **✅ Best Entry Day:** {best_day} (Highest Avg Win)\n"
            avg_hold = winners['Days Held'].mean()
            text += f"* **⏳ Optimal Hold:** {avg_hold:.0f} Days (Avg Winner Duration)\n"
            avg_cost = winners['Debit/Lot'].mean()
            text += f"* **💰 Target Cost:** ${avg_cost:,.0f} (Avg Winner Debit per Lot)\n"
        losers = strat_df[strat_df['P&L'] < 0]
        if not losers.empty:
             avg_loss_hold = losers['Days Held'].mean()
             text += f"* **⚠️ Loss Pattern:** Losers held for avg {avg_loss_hold:.0f} days.\n"
        text += "\n"
    text += "---\n### 🛡️ Universal AI Gates\n1. **Efficiency Check:** If 'Rot Detector' flags a trade, cut it.\n2. **Probability Gate:** Check 'Win Prob %' before entering.\n"
    return text

# --- LOAD STRATEGY CONFIG ---
@st.cache_data(ttl=60)
def load_strategy_config():
    if not os.path.exists(DB_NAME): return {}
    conn = get_db_connection()
    try:
        # Robust fetch
        strat_df = pd.read_sql("SELECT * FROM strategy_config", conn)
        expected = {'name': 'Name', 'identifier': 'Identifier', 'target_pnl': 'Target PnL', 'target_days': 'Target Days', 'min_stability': 'Min Stability', 'description': 'Description', 'typical_debit': 'Typical Debit'}
        for k in expected.keys(): 
            if k not in strat_df.columns: strat_df[k] = 0
        strat_df = strat_df[list(expected.keys())].rename(columns=expected)
        config = {}
        for _, row in strat_df.iterrows():
            config[row['Name']] = {'id': row['Identifier'], 'pnl': row['Target PnL'], 'dit': row['Target Days'], 'stability': row['Min Stability'], 'debit_per_lot': row['Typical Debit']}
        return config
    except: return {}
    finally: conn.close()

def parse_optionstrat_file(file, file_type, config_dict):
    try:
        df_raw = None
        if file.name.endswith(('.xlsx', '.xls')):
            try:
                df_temp = pd.read_excel(file, header=None)
                header_row = 0
                for i, row in df_temp.head(30).iterrows():
                    row_vals = [str(v).strip() for v in row.values]
                    if "Name" in row_vals and "Total Return $" in row_vals: header_row = i; break
                file.seek(0); df_raw = pd.read_excel(file, header=header_row)
                if 'Link' in df_raw.columns:
                    try:
                        file.seek(0); wb = load_workbook(file, data_only=False); sheet = wb.active
                        excel_header_row = header_row + 1; link_col_idx = None
                        for cell in sheet[excel_header_row]:
                            if str(cell.value).strip() == "Link": link_col_idx = cell.col_idx; break
                        if link_col_idx:
                            links = []
                            for i in range(len(df_raw)):
                                excel_row_idx = excel_header_row + 1 + i; cell = sheet.cell(row=excel_row_idx, column=link_col_idx); url = ""
                                if cell.hyperlink: url = cell.hyperlink.target
                                elif cell.value and str(cell.value).startswith('=HYPERLINK'):
                                    try: parts = str(cell.value).split('"'); url = parts[1] if len(parts) > 1 else ""
                                    except: pass
                                links.append(url if url else "")
                            df_raw['Link'] = links
                    except: pass
            except: pass
        if df_raw is None:
            file.seek(0); content = file.getvalue().decode("utf-8", errors='ignore'); lines = content.split('\n'); header_row = 0
            for i, line in enumerate(lines[:30]):
                if "Name" in line and "Total Return" in line: header_row = i; break
            file.seek(0); df_raw = pd.read_csv(file, skiprows=header_row)

        parsed_trades = []
        current_trade = None
        current_legs = []

        def finalize_trade(trade_data, legs, f_type):
            if not trade_data.any(): return None
            name = str(trade_data.get('Name', '')); group = str(trade_data.get('Group', '')); created = trade_data.get('Created At', '')
            try: start_dt = pd.to_datetime(created)
            except: return None 
            strat = get_strategy_dynamic(name, group, config_dict)
            link = str(trade_data.get('Link', ''))
            if link == 'nan' or link == 'Open': link = "" 
            pnl = clean_num(trade_data.get('Total Return $', 0))
            debit = abs(clean_num(trade_data.get('Net Debit/Credit', 0)))
            theta = clean_num(trade_data.get('Theta', 0)); delta = clean_num(trade_data.get('Delta', 0))
            gamma = clean_num(trade_data.get('Gamma', 0)); vega = clean_num(trade_data.get('Vega', 0)); iv = clean_num(trade_data.get('IV', 0))
            exit_dt = None
            try:
                raw_exp = trade_data.get('Expiration')
                if pd.notnull(raw_exp) and str(raw_exp).strip() != '': exit_dt = pd.to_datetime(raw_exp)
            except: pass
            days_held = 1
            if exit_dt and f_type == "History": days_held = (exit_dt - start_dt).days
            else: days_held = (datetime.now() - start_dt).days
            if days_held < 1: days_held = 1
            strat_config = config_dict.get(strat, {}); typical_debit = strat_config.get('debit_per_lot', 5000)
            
            lot_match = re.search(r'(\d+)\s*(?:LOT|L\b)', name, re.IGNORECASE)
            lot_size = int(lot_match.group(1)) if lot_match else int(round(debit / typical_debit))
            if lot_size < 1: lot_size = 1

            put_pnl = 0.0; call_pnl = 0.0
            if f_type == "History":
                for leg in legs:
                    if len(leg) < 5: continue
                    sym = str(leg.iloc[0]) 
                    if not sym.startswith('.'): continue
                    try:
                        qty = clean_num(leg.iloc[1]); entry = clean_num(leg.iloc[2]); close_price = clean_num(leg.iloc[4])
                        leg_pnl = (close_price - entry) * qty * 100
                        if 'P' in sym and 'C' not in sym: put_pnl += leg_pnl
                        elif 'C' in sym and 'P' not in sym: call_pnl += leg_pnl
                        elif re.search(r'[0-9]P[0-9]', sym): put_pnl += leg_pnl
                        elif re.search(r'[0-9]C[0-9]', sym): call_pnl += leg_pnl
                    except: pass
            t_id = generate_id(name, strat, start_dt)
            return {'id': t_id, 'name': name, 'strategy': strat, 'start_dt': start_dt, 'exit_dt': exit_dt, 'days_held': days_held, 'debit': debit, 'lot_size': lot_size, 'pnl': pnl, 'theta': theta, 'delta': delta, 'gamma': gamma, 'vega': vega, 'iv': iv, 'put_pnl': put_pnl, 'call_pnl': call_pnl, 'link': link, 'group': group}

        for index, row in df_raw.iterrows():
            name_val = str(row['Name'])
            if name_val and not name_val.startswith('.') and name_val != 'Symbol' and name_val != 'nan':
                if current_trade is not None:
                    res = finalize_trade(current_trade, current_legs, file_type)
                    if res: parsed_trades.append(res)
                current_trade = row; current_legs = []
            elif name_val.startswith('.'): current_legs.append(row)
        
        if current_trade is not None:
             res = finalize_trade(current_trade, current_legs, file_type)
             if res: parsed_trades.append(res)
        return parsed_trades
    except Exception as e: print(f"Parser Error: {e}"); return []

# --- MAIN APP ---
df = load_data()
dynamic_benchmarks = load_strategy_config()

# --- v141: MULTI-ACCOUNT CAPITAL ---
prime_cap = st.sidebar.number_input("Prime Account (130/160, M200)", min_value=1000, value=115000, step=1000)
smsf_cap = st.sidebar.number_input("SMSF Account", min_value=1000, value=150000, step=1000)
total_cap = prime_cap + smsf_cap
market_regime = st.sidebar.selectbox("Current Market Regime", ["Neutral", "Bullish", "Bearish"], index=0)
regime_mult = 1.10 if "Bullish" in market_regime else 0.90 if "Bearish" in market_regime else 1.0

expired_df = pd.DataFrame() 
if not df.empty:
    expired_df = df[df['Status'] == 'Expired']

# --- TABS ---
tab_dash, tab_analytics, tab_ai, tab_strategies, tab_rules = st.tabs(["📊 Dashboard", "📈 Analytics", "🧠 AI & Insights", "⚙️ Strategies", "📖 Rules"])

with tab_dash:
    with st.container():
        active_df = df[df['Status'].isin(['Active', 'Missing'])].copy()
        tot_theta = active_df['Theta'].sum() if not active_df.empty else 0
        floating_pnl = active_df['P&L'].sum() if not active_df.empty else 0
        health_status = "⚪ No Data"
        
        if not active_df.empty:
             ladder_results = active_df.apply(lambda row: calculate_decision_ladder(row, dynamic_benchmarks), axis=1)
             active_df['Action'] = [x[0] for x in ladder_results]
             active_df['Urgency Score'] = [x[1] for x in ladder_results]
             active_df['Reason'] = [x[2] for x in ladder_results]
             active_df['Juice Val'] = [x[3] for x in ladder_results]
             active_df['Juice Type'] = [x[4] for x in ladder_results]
             def fmt_juice(row): return f"{row['Juice Val']:.0f} days" if row['Juice Type'] == 'Recovery Days' else f"${row['Juice Val']:.0f}"
             active_df['Gauge'] = active_df.apply(fmt_juice, axis=1)
             todo_df = active_df[active_df['Urgency Score'] >= 70]
             
             tot_debit = active_df['Debit'].sum() if active_df['Debit'].sum() > 0 else 1
             total_delta_pct = abs(active_df['Delta'].sum() / tot_debit * 100)
             avg_age = active_df['Days Held'].mean()
             if total_delta_pct > 6 or avg_age > 45: health_status = "🔴 CRITICAL"
             elif total_delta_pct > 2 or avg_age > 25: health_status = "🟡 REVIEW"
             else: health_status = "🟢 HEALTHY"
        else: todo_df = pd.DataFrame()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Portfolio Health", health_status)
        c2.metric("Daily Income (Theta)", f"${tot_theta:,.0f}")
        c3.metric("Floating P&L", f"${floating_pnl:,.0f}")
        c4.metric("Action Items", len(todo_df), delta="Urgent" if not todo_df.empty else None)
    
    st.divider()

    st.subheader("🗺️ Position Heat Map")
    if not active_df.empty:
        fig_heat = px.scatter(active_df, x='Days Held', y='P&L', size='Debit', color='Urgency Score', color_continuous_scale='RdYlGn_r', hover_data=['Name', 'Strategy'], title="Position Clustering (Size = Capital Invested)")
        avg_days = active_df['Days Held'].mean()
        fig_heat.add_vline(x=avg_days, line_dash="dash", opacity=0.5, annotation_text="Avg Age")
        fig_heat.add_hline(y=0, line_dash="dash", opacity=0.5)
        st.plotly_chart(fig_heat, use_container_width=True)
        st.caption("🎯 Top-Right = Winners aging well | 🚨 Bottom-Right = Losers rotting | 🌱 Left = New positions cooking")
    else: st.info("No active trades to map.")
    
    st.divider()

    with st.expander(f"🔥 Priority Action Queue ({len(todo_df)})", expanded=len(todo_df) > 0):
        if not todo_df.empty:
            for _, row in todo_df.iterrows():
                u_score = row['Urgency Score']; color = "red" if u_score >= 90 else "orange"
                st.markdown(f"**{row['Name']}**: :{color}[{row['Action']}] - {row['Reason']}")
        else: st.success("✅ No critical actions.")

    sub_journal, sub_strat = st.tabs(["📝 Journal", "🏛️ Strategy Detail"])
    with sub_journal:
        if not active_df.empty:
            strategy_options = sorted(list(dynamic_benchmarks.keys())) + ["Other"]
            display_cols = ['id', 'Name', 'Link', 'Strategy', 'Urgency Score', 'Action', 'Gauge', 'Status', 'Stability', 'ROI', 'Ann. ROI', 'Theta Eff.', 'lot_size', 'P&L', 'Debit', 'Days Held', 'Notes', 'Tags', 'Parent ID']
            column_config = {
                "id": None, "Name": st.column_config.TextColumn("Name", disabled=True), "Link": st.column_config.LinkColumn("Link", display_text="🔗"),
                "Strategy": st.column_config.SelectboxColumn("Strat", options=strategy_options, required=True), "Status": st.column_config.TextColumn("Stat", disabled=True),
                "Urgency Score": st.column_config.ProgressColumn("Urg", min_value=0, max_value=100, format="%d"), "Action": st.column_config.TextColumn("Act", disabled=True),
                "Gauge": st.column_config.TextColumn("Tank"), "ROI": st.column_config.NumberColumn("ROI", format="%.1f%%"), "Ann. ROI": st.column_config.NumberColumn("Ann%", format="%.1f%%"),
                "P&L": st.column_config.NumberColumn("PnL", format="$%d"), "Debit": st.column_config.NumberColumn("Deb", format="$%d"),
            }
            edited_df = st.data_editor(active_df[display_cols], column_config=column_config, hide_index=True, use_container_width=True, key="journal_editor", num_rows="fixed")
            if st.button("💾 Save Changes"):
                changes = update_journal(edited_df)
                if changes: st.success(f"Saved {changes} trades!"); st.cache_data.clear()
        else: st.info("No active trades.")

    with sub_strat:
        if not active_df.empty:
            strat_agg = active_df.groupby('Strategy').agg({'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum'}).reset_index()
            st.dataframe(strat_agg.style.format({'P&L': '${:,.0f}', 'Debit': '${:,.0f}', 'Theta': '{:,.0f}'}), use_container_width=True)

with tab_analytics:
    an_overview, an_trends, an_risk, an_decay, an_rolls = st.tabs(["📊 Overview", "📈 Trends", "⚠️ Risk", "🧬 Decay", "🔄 Rolls"])
    
    with an_overview:
        if not expired_df.empty:
            smsf_trades = expired_df[expired_df['Strategy'].str.contains("SMSF", case=False, na=False)].copy()
            prime_trades = expired_df[~expired_df['Strategy'].str.contains("SMSF", case=False, na=False)].copy()
            s_smsf, c_smsf = calculate_portfolio_metrics(smsf_trades, smsf_cap)
            s_prime, c_prime = calculate_portfolio_metrics(prime_trades, prime_cap)
            s_total, c_total = calculate_portfolio_metrics(expired_df, total_cap)
            dd_total = calculate_max_drawdown(expired_df, total_cap)
            dd_prime = calculate_max_drawdown(prime_trades, prime_cap)
            c1, c2, c3 = st.columns(3)
            with c1: st.metric("Total CAGR", f"{c_total:.1f}%"); st.metric("Max Drawdown", f"{dd_total['Max Drawdown %']:.1f}%")
            with c2: st.metric("Prime Sharpe", f"{s_prime:.2f}"); st.metric("Prime Max DD", f"{dd_prime['Max Drawdown %']:.1f}%")
            with c3: st.metric("SMSF Sharpe", f"{s_smsf:.2f}")

            st.subheader("💰 Profit Anatomy: Call vs Put Contribution")
            strat_list = sorted(expired_df['Strategy'].unique())
            sel_strat_ana = st.selectbox("Select Strategy to Analyze:", strat_list, key="ana_strat_sel")
            trade_subset = expired_df[expired_df['Strategy'] == sel_strat_ana].sort_values('Exit Date')
            if not trade_subset.empty:
                fig_trade_ana = go.Figure()
                fig_trade_ana.add_trace(go.Bar(x=trade_subset['Name'], y=trade_subset['Put P&L'], name='Put PnL', marker_color='#EF553B'))
                if 'Call P&L' in trade_subset.columns:
                    fig_trade_ana.add_trace(go.Bar(x=trade_subset['Name'], y=trade_subset['Call P&L'], name='Call PnL', marker_color='#00CC96'))
                fig_trade_ana.update_layout(barmode='relative', title=f"Profit Attribution: {sel_strat_ana}", xaxis_title="Trade", yaxis_title="PnL ($)", xaxis_tickangle=-45)
                st.plotly_chart(fig_trade_ana, use_container_width=True)

    with an_trends:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🕵️ Root Cause Analysis")
            expired_wins = df[(df['Status'] == 'Expired') & (df['P&L'] > 0)]
            active_trades = df[df['Status'] == 'Active']
            if not expired_wins.empty and not active_trades.empty:
                avg_win_debit = expired_wins.groupby('Strategy')['Debit/Lot'].mean().reset_index()
                avg_act_debit = active_trades.groupby('Strategy')['Debit/Lot'].mean().reset_index()
                avg_win_debit['Type'] = 'Winning History'; avg_act_debit['Type'] = 'Active (Current)'
                comp_df = pd.concat([avg_win_debit, avg_act_debit])
                fig_price = px.bar(comp_df, x='Strategy', y='Debit/Lot', color='Type', barmode='group', title="Entry Price per Lot Comparison", color_discrete_map={'Winning History': 'green', 'Active (Current)': 'orange'})
                st.plotly_chart(fig_price, use_container_width=True)
            else: st.info("Need more data.")
        with col2:
            st.subheader("⚖️ Profit Drivers (Puts vs Calls)")
            expired = df[df['Status'] == 'Expired'].copy()
            if not expired.empty:
                leg_agg = expired.groupby('Strategy')[['Put P&L', 'Call P&L']].sum().reset_index()
                fig_legs = px.bar(leg_agg, x='Strategy', y=['Put P&L', 'Call P&L'], title="Profit Source Split", color_discrete_map={'Put P&L': '#EF553B', 'Call P&L': '#00CC96'})
                st.plotly_chart(fig_legs, use_container_width=True)
            else: st.info("No closed trades.")
        st.divider()
        if not expired_df.empty:
            ec_df = expired_df.dropna(subset=["Exit Date"]).sort_values("Exit Date").copy()
            ec_df['Cumulative P&L'] = ec_df['P&L'].cumsum()
            fig = px.line(ec_df, x='Exit Date', y='Cumulative P&L', title="Realized Equity Curve", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        st.divider()
        hm1, hm2, hm3 = st.tabs(["🗓️ Seasonality", "⏳ Duration", "📅 Entry Day"])
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
        st.subheader("Strategy Correlation")
        snaps = load_snapshots()
        if not snaps.empty:
             fig_corr = rolling_correlation_matrix(snaps)
             if fig_corr: st.plotly_chart(fig_corr, use_container_width=True)
             else: st.info("Need more snapshot history.")
        if not active_df.empty:
            st.subheader("Concentration Risk")
            conc_df = check_concentration_risk(active_df, total_cap)
            if not conc_df.empty: st.warning("⚠️ High Concentration Trades (>15% Equity):"); st.dataframe(conc_df, use_container_width=True)
            else: st.success("✅ Position sizing is healthy (All <15%).")

    with an_decay:
        st.subheader("🧬 Theta Decay Analysis")
        if not snaps.empty:
             decay_strat = st.selectbox("Select Strategy", snaps['strategy'].unique(), key="dec_strat")
             strat_snaps = snaps[snaps['strategy'] == decay_strat].copy()
             if not strat_snaps.empty:
                 def get_theta_anchor(group): return group.sort_values('days_held').iloc[0]['theta'] if group.sort_values('days_held').iloc[0]['theta'] > 0 else group['theta'].max()
                 anchor_map = strat_snaps.groupby('trade_id').apply(get_theta_anchor)
                 strat_snaps['Theta_Anchor'] = strat_snaps['trade_id'].map(anchor_map)
                 strat_snaps['Theta_Expected'] = strat_snaps.apply(lambda r: theta_decay_model(r['Theta_Anchor'], r['days_held'], decay_strat), axis=1)
                 fig_theta = go.Figure()
                 for t_id in strat_snaps['trade_id'].unique()[:5]: 
                     t_data = strat_snaps[strat_snaps['trade_id'] == t_id].sort_values('days_held')
                     fig_theta.add_trace(go.Scatter(x=t_data['days_held'], y=t_data['theta'], mode='lines+markers', name='Actual'))
                     fig_theta.add_trace(go.Scatter(x=t_data['days_held'], y=t_data['Theta_Expected'], mode='lines', line=dict(dash='dash'), name='Model'))
                 st.plotly_chart(fig_theta, use_container_width=True)

with tab_ai:
    st.markdown("### 🧠 The Quant Brain")
    with st.expander("⚙️ Calibration", expanded=False):
        c1, c2 = st.columns(2)
        with c1: prob_high = st.slider("High Conf %", 60, 95, 75); prob_low = st.slider("Low Conf %", 10, 50, 40)
        with c2: exit_pct = st.slider("Exit Target %", 50, 95, 75) / 100.0

    if not active_df.empty and not expired_df.empty:
        preds = generate_trade_predictions(active_df, expired_df, prob_low, prob_high, total_cap)
        if not preds.empty:
            c_p1, c_p2 = st.columns([2, 3])
            with c_p1:
                fig_pred = px.scatter(preds, x="Win Prob %", y="Expected PnL", color="Confidence", size="Confidence", hover_data=["Trade Name", "Strategy"], color_continuous_scale="RdYlGn", title="Risk/Reward Map")
                st.plotly_chart(fig_pred, use_container_width=True)
            with c_p2:
                st.dataframe(preds.style.format({'Win Prob %': "{:.1f}%", 'Expected PnL': "${:,.0f}", 'Confidence': "{:.0f}%"}).map(lambda v: 'color: green; font-weight: bold' if v > prob_high else ('color: red; font-weight: bold' if v < prob_low else 'color: orange'), subset=['Win Prob %']), use_container_width=True)
            
            c_ai_1, c_ai_2 = st.columns(2)
            with c_ai_1:
                st.subheader("📉 Rot Detector")
                rot_df = check_rot_and_efficiency(active_df, expired_df, 0.5, 10)
                if not rot_df.empty: st.dataframe(rot_df, use_container_width=True)
                else: st.success("✅ No rot detected.")
            with c_ai_2:
                st.subheader(f"🎯 Optimal Exits ({int(exit_pct*100)}%)")
                targets = get_dynamic_targets(expired_df, exit_pct)
                if targets:
                    target_data = [{'Strategy': s, 'Median Win': v['Median Win'], 'Optimal Exit': v['Optimal Exit']} for s, v in targets.items()]
                    st.dataframe(pd.DataFrame(target_data).style.format({'Median Win': '${:,.0f}', 'Optimal Exit': '${:,.0f}'}), use_container_width=True)

    with st.expander("🧬 DNA Tool", expanded=False):
         st.subheader("🧬 Trade DNA Fingerprinting")
         st.caption("Find historical trades that match the Greek profile of your current active trade.")
         if not expired_df.empty and not active_df.empty:
             selected_dna_trade = st.selectbox("Select Active Trade", active_df['Name'].unique())
             curr_row = active_df[active_df['Name'] == selected_dna_trade].iloc[0]
             similar = find_similar_trades(curr_row, expired_df)
             if not similar.empty:
                 st.dataframe(similar.style.format({'P&L': '${:,.0f}', 'ROI': '{:.1f}%', 'Similarity %': '{:.0f}%'}))
             else: st.info("No matches found.")
         else: st.info("Need active and closed trades.")

with tab_strategies:
    st.markdown("### ⚙️ Strategy Config")
    conn = get_db_connection()
    try:
        strat_df = pd.read_sql("SELECT * FROM strategy_config", conn)
        expected = {'name': 'Name', 'identifier': 'Identifier', 'target_pnl': 'Target PnL', 'target_days': 'Target Days', 'min_stability': 'Min Stability', 'description': 'Description', 'typical_debit': 'Typical Debit'}
        for k in expected.keys(): 
            if k not in strat_df.columns: strat_df[k] = 0
        strat_df = strat_df[list(expected.keys())].rename(columns=expected)
        edited_strats = st.data_editor(strat_df, num_rows="dynamic", use_container_width=True)
        if st.button("💾 Save Config"):
            if update_strategy_config(edited_strats): st.success("Saved!"); st.cache_data.clear()
    except Exception as e: st.error(f"Error: {e}")
    finally: conn.close()

with tab_rules:
    strategies_for_rules = sorted(list(dynamic_benchmarks.keys()))
    adaptive_content = generate_adaptive_rulebook_text(expired_df, strategies_for_rules)
    st.markdown(adaptive_content)
    st.divider(); st.caption("Allantis Trade Guardian v149.0 (Full Restoration: Trends Tab, All AI Features, & Stability Fixes)")
