import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3
from datetime import datetime
import io

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian (DB)", layout="wide", page_icon="🛡️")
st.title("🛡️ Allantis Trade Guardian: Database Edition")

# --- DATABASE ENGINE ---
DB_NAME = "trade_guardian.db"

def init_db():
    """Initialize the SQLite database structure if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Table: Trades (Master list of all trades ever seen)
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    strategy TEXT,
                    status TEXT,
                    entry_date DATE,
                    exit_date DATE,
                    debit REAL,
                    lot_size INTEGER,
                    pnl REAL,
                    notes TEXT
                )''')
    
    # Table: Snapshots (Daily record of active trades for trend analysis)
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
def get_strategy(group_name):
    g = str(group_name).upper()
    if "M200" in g: return "M200"
    elif "160/190" in g: return "160/190"
    elif "130/160" in g: return "130/160"
    return "Other"

def clean_num(x):
    try: return float(str(x).replace('$','').replace(',',''))
    except: return 0.0

def generate_trade_id(name, strategy, entry_date):
    """Generate a unique ID for a trade to prevent duplicates."""
    # Simple hash-like ID: Name + Strategy + EntryDate (YYYYMMDD)
    date_str = pd.to_datetime(entry_date).strftime('%Y%m%d')
    return f"{name}_{strategy}_{date_str}".replace(" ", "").replace("/", "-")

# --- SYNC ENGINE ---
def sync_data(file, file_type):
    """Reads a CSV/Excel and updates the Database."""
    log = []
    try:
        # Read File
        if file.name.endswith('.xlsx'):
            df = pd.read_excel(file)
        else:
            df = pd.read_csv(file)
            
        conn = get_db_connection()
        cursor = conn.cursor()
        
        count_new = 0
        count_update = 0
        
        for _, row in df.iterrows():
            # 1. Parse Key Data
            created_val = row.get('Created At', '')
            try: start_dt = pd.to_datetime(created_val)
            except: continue # Skip bad rows
            
            name = row.get('Name', 'Unknown')
            group = str(row.get('Group', ''))
            strat = get_strategy(group)
            
            pnl = clean_num(row.get('Total Return $', 0))
            debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
            
            # Lot Size Logic
            lot_size = 1
            if strat == '130/160' and debit > 6000: lot_size = 2
            elif strat == '130/160' and debit > 10000: lot_size = 3
            elif strat == '160/190' and debit > 8000: lot_size = 2
            elif strat == 'M200' and debit > 12000: lot_size = 2
            
            trade_id = generate_trade_id(name, strat, start_dt)
            
            # 2. Determine Status from File Type
            # If coming from "Active" file, it's Active.
            # If coming from "History" file, it's Expired.
            status = "Active" if file_type == "Active" else "Expired"
            
            # 3. Upsert into TRADES table
            # Check if exists
            cursor.execute("SELECT status FROM trades WHERE id = ?", (trade_id,))
            data = cursor.fetchone()
            
            if data is None:
                # Insert New
                cursor.execute('''INSERT INTO trades 
                                  (id, name, strategy, status, entry_date, debit, lot_size, pnl, notes) 
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                               (trade_id, name, strat, status, start_dt.date(), debit, lot_size, pnl, ""))
                count_new += 1
            else:
                # Update Existing (Update P&L and Status)
                # Note: We only update status if the new file is "History" (confirming close)
                # or if we are actively tracking it.
                if file_type == "History" or data[0] == "Active":
                    cursor.execute('''UPDATE trades SET pnl = ?, status = ? WHERE id = ?''', (pnl, status, trade_id))
                    count_update += 1

            # 4. Insert SNAPSHOT (Only for Active files)
            if file_type == "Active":
                # Calculate Greeks/Metrics
                theta = clean_num(row.get('Theta', 0))
                delta = clean_num(row.get('Delta', 0))
                gamma = clean_num(row.get('Gamma', 0))
                vega = clean_num(row.get('Vega', 0))
                days = (datetime.now() - start_dt).days
                if days < 1: days = 1
                
                # Record daily snapshot
                cursor.execute('''INSERT INTO snapshots 
                                  (trade_id, snapshot_date, pnl, theta, delta, gamma, vega, days_held)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                               (trade_id, datetime.now().date(), pnl, theta, delta, gamma, vega, days))

        conn.commit()
        conn.close()
        log.append(f"✅ Processed {file.name}: {count_new} New, {count_update} Updated.")
        
    except Exception as e:
        log.append(f"❌ Error {file.name}: {str(e)}")
        
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
    
    # Calculate Calculated Columns
    if not df.empty:
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        
        # Recalculate Days Held for display (Live Calc)
        df['calc_days'] = (datetime.now() - df['entry_date']).dt.days
        df['days_held'] = df['days_held'].fillna(df['calc_days']) # Fill missing snapshots with live calc
        df.loc[df['days_held'] < 1, 'days_held'] = 1
        
        # Per Lot
        df['Debit/Lot'] = df['debit'] / df['lot_size']
        df['Daily Yield %'] = (df['pnl'] / df['debit'] * 100) / df['days_held']
        
        # Grading
        def get_grade(row):
            strat, debit = row['strategy'], row['Debit/Lot']
            if strat == '130/160': return "F" if debit > 4800 else "A+" if 3500 <= debit <= 4500 else "B"
            if strat == '160/190': return "A" if 4800 <= debit <= 5500 else "C"
            if strat == 'M200': return "A" if 7500 <= debit <= 8500 else "B"
            return "C"
        
        df['Grade'] = df.apply(get_grade, axis=1)
        
    return df

# --- INITIALIZE APP ---
init_db()

# --- SIDEBAR: SYNC ---
with st.sidebar.expander("📂 Data Sync", expanded=True):
    active_file = st.file_uploader("1. Update ACTIVE Trades", type=['csv','xlsx'], key='active')
    history_file = st.file_uploader("2. Update HISTORY (Closed)", type=['csv','xlsx'], key='history')
    
    if st.button("🔄 Sync Database"):
        logs = []
        if active_file: logs.extend(sync_data(active_file, "Active"))
        if history_file: logs.extend(sync_data(history_file, "History"))
        
        if logs:
            for l in logs: st.write(l)
            st.success("Database Updated!")
            st.rerun()
        else:
            st.warning("Upload a file first.")

# --- LOAD DATA ---
df = load_data_from_db()

if df.empty:
    st.info("👋 Welcome! The database is empty. Please upload your Active & History files in the sidebar to initialize.")
else:
    # --- CONSTANTS ---
    BASE_CONFIG = {
        '130/160': {'yield': 0.13, 'pnl': 500},
        '160/190': {'yield': 0.28, 'pnl': 700},
        'M200':    {'yield': 0.56, 'pnl': 900}
    }
    
    # Market Regime
    regime = st.sidebar.selectbox("Market Regime", ["Neutral", "Bullish (+10%)", "Bearish (-10%)"])
    mult = 1.1 if "Bullish" in regime else 0.9 if "Bearish" in regime else 1.0

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 Active Dashboard", "📈 Analytics", "📓 Journal"])

    # 1. DASHBOARD
    with tab1:
        active_df = df[df['status'] == 'Active'].copy()
        
        if not active_df.empty:
            st.markdown("### 🏛️ Active Portfolio")
            
            # Action Logic
            def get_action(row):
                target = BASE_CONFIG.get(row['strategy'], {}).get('pnl', 9999) * mult
                if row['pnl'] >= target: return "TAKE PROFIT"
                if row['strategy'] == '130/160' and row['days_held'] > 25 and row['pnl'] < 100: return "KILL (Stale)"
                if row['strategy'] == 'M200' and 12 <= row['days_held'] <= 16: return "DAY 14 CHECK"
                return ""

            active_df['Action'] = active_df.apply(get_action, axis=1)
            
            # Alerts
            urgent = active_df[active_df['Action'] != ""]
            if not urgent.empty:
                st.error(f"🚨 **Action Required ({len(urgent)})**")
                for _, r in urgent.iterrows():
                    st.write(f"• **{r['name']}**: {r['Action']}")
            
            # Table
            cols = ['name', 'strategy', 'Action', 'Grade', 'pnl', 'debit', 'days_held', 'Daily Yield %', 'theta', 'delta']
            
            def style_row(row):
                bg = ''
                if "TAKE PROFIT" in str(row['Action']): bg = 'background-color: #d1e7dd'
                elif "KILL" in str(row['Action']): bg = 'background-color: #f8d7da'
                return [bg] * len(row)

            st.dataframe(
                active_df[cols].style.format({
                    'pnl': "${:,.0f}", 'debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%", 
                    'theta': "{:.1f}", 'delta': "{:.1f}", 'days_held': "{:.0f}"
                }).apply(style_row, axis=1),
                use_container_width=True,
                height=500
            )
        else:
            st.info("No active trades in Database. Sync an Active file.")

    # 2. ANALYTICS
    with tab2:
        expired_df = df[df['status'] == 'Expired'].copy()
        if not expired_df.empty:
            st.markdown("#### 🏆 Historical Performance")
            
            # Metrics
            total_pnl = expired_df['pnl'].sum()
            win_rate = (len(expired_df[expired_df['pnl'] > 0]) / len(expired_df)) * 100
            
            c1, c2 = st.columns(2)
            c1.metric("Total Realized P&L", f"${total_pnl:,.0f}")
            c2.metric("Win Rate", f"{win_rate:.1f}%")
            
            st.plotly_chart(px.bar(expired_df, x='strategy', y='pnl', color='strategy', title="P&L by Strategy"), use_container_width=True)
            st.plotly_chart(px.scatter(expired_df, x='days_held', y='pnl', color='strategy', title="P&L vs Duration"), use_container_width=True)

    # 3. JOURNAL (EDITABLE)
    with tab3:
        st.markdown("### 📓 Trade Journal")
        st.caption("Edit notes here. They are saved to the database automatically.")
        
        # Editable Data Editor
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
        
        # Save Button
        if st.button("💾 Save Journal Changes"):
            conn = get_db_connection()
            c = conn.cursor()
            for index, row in edited_df.iterrows():
                c.execute("UPDATE trades SET notes = ? WHERE id = ?", (row['notes'], row['id']))
            conn.commit()
            conn.close()
            st.success("Notes Saved!")
            st.rerun()
