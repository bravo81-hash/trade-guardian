import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")
st.title("🛡️ Allantis Trade Guardian: Database Edition")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    strategy TEXT,
                    status TEXT,
                    entry_date DATE,
                    debit REAL,
                    lot_size INTEGER,
                    pnl REAL,
                    notes TEXT
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT,
                    snapshot_date DATE,
                    pnl REAL,
                    theta REAL,
                    delta REAL,
                    gamma REAL,
                    vega REAL,
                    days_held INTEGER,
                    FOREIGN KEY(trade_id) REFERENCES trades(id)
                )''')
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(DB_NAME)

# --- HELPER FUNCTIONS ---
def get_strategy(group_name, trade_name):
    g = str(group_name).upper()
    n = str(trade_name).upper()
    if "M200" in g or "M200" in n: return "M200"
    elif "160/190" in g or "160/190" in n: return "160/190"
    elif "130/160" in g or "130/160" in n: return "130/160"
    return "Other"

def clean_num(x):
    try: return float(str(x).replace('$','').replace(',',''))
    except: return 0.0

def generate_trade_id(name, strategy, entry_date):
    date_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{name}_{strategy}_{date_str}".replace(" ", "").replace("/", "-")

# --- SYNC ENGINE ---
def sync_data(file_list, file_type):
    """Reads a list of files and updates the Database."""
    log = []
    
    # Handle single file or list
    if not isinstance(file_list, list):
        file_list = [file_list]

    conn = get_db_connection()
    cursor = conn.cursor()
    
    count_new = 0
    count_update = 0
    
    for file in file_list:
        try:
            # Read File
            if file.name.endswith('.xlsx'):
                df = pd.read_excel(file)
            else:
                # OptionStrat CSVs often have a metadata row at top
                try:
                    df = pd.read_csv(file)
                    if 'Name' not in df.columns: # Heuristic check
                        file.seek(0)
                        df = pd.read_csv(file, skiprows=1)
                except:
                    log.append(f"⚠️ Could not read {file.name}")
                    continue
                
            for _, row in df.iterrows():
                # 1. Filter Garbage
                name = str(row.get('Name', ''))
                # Skip legs (.SPX...) or empty names
                if name.startswith('.') or name == 'nan' or name == '':
                    continue
                
                # Parse Date
                created_val = row.get('Created At', '')
                try: 
                    start_dt = pd.to_datetime(created_val)
                except: 
                    continue # Skip summary rows without dates
                
                group = str(row.get('Group', ''))
                strat = get_strategy(group, name)
                
                pnl = clean_num(row.get('Total Return $', 0))
                debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                
                # Lot Size Logic
                lot_size = 1
                if strat == '130/160' and debit > 6000: lot_size = 2
                elif strat == '130/160' and debit > 10000: lot_size = 3
                elif strat == '160/190' and debit > 8000: lot_size = 2
                elif strat == 'M200' and debit > 12000: lot_size = 2
                
                trade_id = generate_trade_id(name, strat, start_dt)
                status = "Active" if file_type == "Active" else "Expired"
                
                # 2. Upsert TRADES
                cursor.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
                data = cursor.fetchone()
                
                if data is None:
                    cursor.execute('''INSERT INTO trades 
                                      (id, name, strategy, status, entry_date, debit, lot_size, pnl, notes) 
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                   (trade_id, name, strat, status, start_dt.date(), debit, lot_size, pnl, ""))
                    count_new += 1
                else:
                    # Only update if we have new data
                    if file_type == "History" or data[0] == "Active":
                        cursor.execute('''UPDATE trades SET pnl = ?, status = ? WHERE id = ?''', (pnl, status, trade_id))
                        count_update += 1

                # 3. Insert SNAPSHOT (Only for Active files)
                if file_type == "Active":
                    theta = clean_num(row.get('Theta', 0))
                    delta = clean_num(row.get('Delta', 0))
                    gamma = clean_num(row.get('Gamma', 0))
                    vega = clean_num(row.get('Vega', 0))
                    days = (datetime.now() - start_dt).days
                    if days < 1: days = 1
                    
                    cursor.execute('''INSERT INTO snapshots 
                                      (trade_id, snapshot_date, pnl, theta, delta, gamma, vega, days_held)
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                   (trade_id, datetime.now().date(), pnl, theta, delta, gamma, vega, days))

            log.append(f"✅ {file.name}: {count_new} New / {count_update} Updated")
            
        except Exception as e:
            log.append(f"❌ Error {file.name}: {str(e)}")
            
    conn.commit()
    conn.close()
    return log

# --- DATA LOADER ---
def load_data_from_db():
    conn = get_db_connection()
    # Get Trades with latest snapshot info
    query = """
    SELECT 
        t.id, t.name, t.strategy, t.status, t.entry_date, t.debit, t.lot_size, t.pnl, t.notes,
        s.theta, s.delta, s.gamma, s.vega, s.days_held
    FROM trades t
    LEFT JOIN (
        SELECT * FROM snapshots 
        WHERE id IN (SELECT MAX(id) FROM snapshots GROUP BY trade_id)
    ) s ON t.id = s.trade_id
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['calc_days'] = (datetime.now() - df['entry_date']).dt.days
        df['days_held'] = df['days_held'].fillna(df['calc_days']) 
        df.loc[df['days_held'] < 1, 'days_held'] = 1
        
        df['Debit/Lot'] = df['debit'] / df['lot_size']
        df['Daily Yield %'] = (df['pnl'] / df['debit'] * 100) / df['days_held']
        
        def get_grade(row):
            strat, debit = row['strategy'], row['Debit/Lot']
            if strat == '130/160': return "F" if debit > 4800 else "A+" if 3500 <= debit <= 4500 else "B"
            if strat == '160/190': return "A" if 4800 <= debit <= 5500 else "C"
            if strat == 'M200': return "A" if 7500 <= debit <= 8500 else "B"
            return "C"
        df['Grade'] = df.apply(get_grade, axis=1)
        
    return df

# --- INITIALIZE ---
init_db()

# --- SIDEBAR: SYNC ---
with st.sidebar.expander("📂 Data Sync", expanded=True):
    # FIXED: accept_multiple_files=True
    active_files = st.file_uploader("1. ACTIVE Trades", type=['csv','xlsx'], accept_multiple_files=True, key='active')
    history_files = st.file_uploader("2. HISTORY (Closed)", type=['csv','xlsx'], accept_multiple_files=True, key='history')
    
    if st.button("🔄 Sync Database"):
        logs = []
        if active_files: logs.extend(sync_data(active_files, "Active"))
        if history_files: logs.extend(sync_data(history_files, "History"))
        
        if logs:
            for l in logs: st.write(l)
            st.success("Sync Complete!")
            st.rerun()
        else:
            st.warning("Upload files first.")

# --- SETTINGS ---
st.sidebar.divider()
market_regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish (+10%)", "Bearish (-10%)"], index=0)

# --- LOAD DATA ---
df = load_data_from_db()

if df.empty:
    st.info("👋 Welcome! Database is empty. Upload your Active & History files to start.")
else:
    # --- UPDATED BASELINES (From User Data) ---
    BASE_CONFIG = {
        '130/160': {'yield': 0.13, 'pnl': 600}, # Updated from 500
        '160/190': {'yield': 0.28, 'pnl': 420}, # Updated from 700
        'M200':    {'yield': 0.56, 'pnl': 900}
    }
    
    regime_mult = 1.1 if "Bullish" in market_regime else 0.9 if "Bearish" in market_regime else 1.0

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 Active Dashboard", "📈 Analytics", "📓 Journal"])

    # 1. DASHBOARD
    with tab1:
        active_df = df[df['status'] == 'Active'].copy()
        
        if not active_df.empty:
            st.markdown("### 🏛️ Active Portfolio")
            
            def get_action(row):
                target = BASE_CONFIG.get(row['strategy'], {}).get('pnl', 9999) * regime_mult
                if row['pnl'] >= target: return "TAKE PROFIT"
                if row['strategy'] == '130/160' and row['days_held'] > 25 and row['pnl'] < 100: return "KILL (Stale)"
                if row['strategy'] == 'M200' and 12 <= row['days_held'] <= 16: return "DAY 14 CHECK"
                return ""

            active_df['Action'] = active_df.apply(get_action, axis=1)
            
            urgent = active_df[active_df['Action'] != ""]
            if not urgent.empty:
                st.error(f"🚨 **Action Required ({len(urgent)})**")
                for _, r in urgent.iterrows():
                    st.write(f"• **{r['name']}**: {r['Action']}")
            
            cols = ['name', 'strategy', 'Action', 'Grade', 'pnl', 'debit', 'days_held', 'Daily Yield %', 'theta', 'delta']
            
            def style_table(styler):
                styler.applymap(lambda v: 'background-color: #d1e7dd; color: #0f5132; font-weight: bold' if 'TAKE PROFIT' in str(v) 
                                       else 'background-color: #f8d7da; color: #842029; font-weight: bold' if 'KILL' in str(v) 
                                       else '', subset=['Action'])
                return styler

            st.dataframe(
                style_table(active_df[cols].style).format({
                    'pnl': "${:,.0f}", 'debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%", 
                    'theta': "{:.1f}", 'delta': "{:.1f}", 'days_held': "{:.0f}"
                }),
                use_container_width=True,
                height=500
            )
        else:
            st.info("No active trades.")

    # 2. ANALYTICS
    with tab2:
        expired_df = df[df['status'] == 'Expired'].copy()
        if not expired_df.empty:
            st.markdown("#### 🏆 Historical Performance")
            
            total_pnl = expired_df['pnl'].sum()
            win_rate = (len(expired_df[expired_df['pnl'] > 0]) / len(expired_df)) * 100
            
            c1, c2 = st.columns(2)
            c1.metric("Total Realized P&L", f"${total_pnl:,.0f}")
            c2.metric("Win Rate", f"{win_rate:.1f}%")
            
            st.plotly_chart(px.bar(expired_df, x='strategy', y='pnl', color='strategy', title="P&L by Strategy"), use_container_width=True)
            st.plotly_chart(px.scatter(expired_df, x='days_held', y='pnl', color='strategy', title="P&L vs Duration"), use_container_width=True)

    # 3. JOURNAL
    with tab3:
        st.markdown("### 📓 Trade Journal")
        edited_df = st.data_editor(
            df[['id', 'name', 'strategy', 'status', 'pnl', 'notes']],
            column_config={
                "id": st.column_config.TextColumn(disabled=True),
                "notes": st.column_config.TextColumn("Notes (Editable)", width="large"),
            },
            hide_index=True,
            use_container_width=True,
            key="journal_editor"
        )
        
        if st.button("💾 Save Journal Changes"):
            conn = get_db_connection()
            c = conn.cursor()
            for index, row in edited_df.iterrows():
                c.execute("UPDATE trades SET notes = ? WHERE id = ?", (row['notes'], row['id']))
            conn.commit()
            conn.close()
            st.success("Notes Saved!")
            st.rerun()
