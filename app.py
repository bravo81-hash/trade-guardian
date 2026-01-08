import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import shutil
from datetime import datetime

# --- CONFIG & CSS ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️", initial_sidebar_state="expanded")
st.markdown("""
<style>
    .block-container { padding-top: 1rem; padding-bottom: 5rem; }
    .trade-card { background-color: #1E1E1E; border: 1px solid #333; border-radius: 10px; padding: 15px; margin-bottom: 10px; }
    div[data-testid="stMetricValue"] { font-size: 1.6rem !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: transparent; border-radius: 4px 4px 0px 0px; gap: 1px; }
    .stTabs [aria-selected="true"] { background-color: #262730; border-bottom: 2px solid #FF4B4B; }
    .status-badge { padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 0.8em; }
</style>
""", unsafe_allow_html=True)

# --- DEBUG BANNER ---
st.info("✅ RUNNING VERSION: v2.0 (Restored Features & Robust Parsing)")

# --- DATABASE ---
DB_NAME = "trade_guardian_v4.db"

def get_db():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    # Schema matches your original structure with lot_size included
    conn.execute('''CREATE TABLE IF NOT EXISTS trades (
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
        stability_ratio REAL, 
        roi REAL, 
        max_loss REAL, 
        updated_at TIMESTAMP)''')
    conn.commit()
    conn.close()

# --- LOGIC & PARSING ---
def get_strategy(name):
    n = str(name).upper()
    if "112" in n: return "112 Strategy"
    if "M200" in n: return "M200"
    if "SMSF" in n: return "SMSF"
    return "Earnings" if "EARNINGS" in n else "Other"

def process_file(file):
    try:
        df = None
        # Handle Excel vs CSV
        if file.name.endswith('.xlsx') and "csv" not in file.type:
            df = pd.read_excel(file)
        else:
            # Robust CSV Parsing for OptionStrat
            content = file.getvalue().decode('utf-8')
            lines = content.split('\n')
            data_lines = []
            headers = None
            
            for line in lines:
                # SKIP Sub-headers commonly found in OptionStrat exports
                if "Symbol,Quantity" in line or "Entry Price,Current Price" in line:
                    continue
                
                # Capture Main Headers
                if "Name,Total Return" in line and headers is None:
                    headers = line.strip().split(',')
                    continue
                    
                # Capture Data
                if headers and line.strip():
                    data_lines.append(line)
            
            if not headers:
                st.error("Could not find valid headers (Name, Total Return...) in file.")
                return None
                
            # Reconstruct CSV
            csv_str = "\n".join([",".join(headers)] + data_lines)
            df = pd.read_csv(io.StringIO(csv_str))

        if df is None or 'Name' not in df.columns:
            st.error("Column 'Name' not found. Check file format.")
            return None

        # Post-Processing Cleaning (Double check for garbage rows)
        if 'Created At' in df.columns:
             # Remove rows where 'Created At' is not a date (e.g. if a subheader sneaked in)
             df = df[pd.to_datetime(df['Created At'], errors='coerce').notna()]

        # Filter out leg rows (starting with .)
        df = df[~df['Name'].astype(str).str.startswith('.')].copy()
        
        # Date Parsing
        df['entry_date'] = pd.to_datetime(df['Created At']).dt.date
        df['expiration'] = pd.to_datetime(df['Expiration']).dt.date
        
        # Numeric Cleanup
        cols_to_clean = ['Total Return $', 'Max Loss', 'Total Return %', 'Net Debit/Credit', 'Theta', 'Delta', 'Gamma', 'Vega']
        for c in cols_to_clean:
            if c in df.columns: 
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            
        df['id'] = df['Name'] + "_" + df['entry_date'].astype(str)
        df['strategy'] = df['Name'].apply(get_strategy)
        
        # Stability Ratio
        df['abs_delta'] = df['Delta'].abs()
        df['stability_ratio'] = np.where(df['abs_delta'] > 0.01, df['Theta'] / df['abs_delta'], 0.0)
        
        # Lot Size Estimation (Primitive logic based on name or debit if needed, defaulting to 1 for now)
        df['lot_size'] = 1 
        
        return df
    except Exception as e:
        st.error(f"Error processing file: {e}")
        return None

def sync_db(df, status):
    conn = get_db()
    c = conn.cursor()
    count = 0
    for _, r in df.iterrows():
        c.execute("SELECT id FROM trades WHERE id=?", (r['id'],))
        exists = c.fetchone()
        
        # Prepare values
        vals = (
            r['Name'], r['strategy'], status, r['entry_date'], 
            r.get('Net Debit/Credit',0), r.get('lot_size', 1),
            r.get('Total Return $',0), r.get('Theta',0), r.get('Delta',0), 
            r.get('Gamma',0), r.get('Vega',0), r.get('stability_ratio',0), 
            r.get('Total Return %',0), r.get('Max Loss',0), datetime.now()
        )
        
        if exists:
            c.execute('''UPDATE trades SET 
                name=?, strategy=?, status=?, entry_date=?, debit=?, lot_size=?,
                pnl=?, theta=?, delta=?, gamma=?, vega=?, stability_ratio=?, 
                roi=?, max_loss=?, updated_at=? WHERE id=?''', vals + (r['id'],))
        else:
            c.execute('''INSERT INTO trades (
                name, strategy, status, entry_date, debit, lot_size,
                pnl, theta, delta, gamma, vega, stability_ratio, 
                roi, max_loss, updated_at, id) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', vals + (r['id'],))
        count += 1
    conn.commit()
    conn.close()
    return count

@st.cache_data
def load_data():
    conn = get_db()
    try: df = pd.read_sql("SELECT * FROM trades", conn)
    except: df = pd.DataFrame()
    conn.close()
    if not df.empty:
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['exit_date'] = pd.to_datetime(df['exit_date'])
        now = pd.Timestamp.now()
        df['days_held'] = df.apply(lambda x: (now - x['entry_date']).days if pd.isna(x['exit_date']) 
            else (x['exit_date'] - x['entry_date']).days, axis=1)
    return df

# --- VISUALS ---
def plot_equity(df):
    closed = df[df['status'] == 'Closed'].sort_values('exit_date')
    if closed.empty: return st.info("No closed trades.")
    closed['cum_pnl'] = closed['pnl'].cumsum()
    fig = px.area(closed, x='exit_date', y='cum_pnl', title="Realized Equity Curve", color_discrete_sequence=['#00CC96'])
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", margin=dict(t=30,b=0,l=0,r=0))
    st.plotly_chart(fig, use_container_width=True)

def plot_gauge(val):
    fig = go.Figure(go.Indicator(mode="gauge+number", value=val, title={'text':"Stability"},
        gauge={'axis':{'range':[None,5]}, 'bar':{'color':"white"}, 'steps':[
            {'range':[0,0.25], 'color':'#FF4B4B'}, {'range':[0.25,1], 'color':'#FFAA00'}, {'range':[1,5], 'color':'#00CC96'}]}))
    fig.update_layout(height=140, margin=dict(t=30,b=20,l=20,r=20), paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

def plot_calendar(df):
    closed = df[df['status']=='Closed'].copy()
    if closed.empty: return
    pnl = closed.groupby('exit_date')['pnl'].sum().reset_index()
    pnl['date'] = pd.to_datetime(pnl['exit_date'])
    pnl['week'] = pnl['date'].dt.isocalendar().week
    pnl['day'] = pnl['date'].dt.day_name()
    hm = pnl.pivot_table(index='day', columns='week', values='pnl', aggfunc='sum').fillna(0)
    days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    hm = hm.reindex(days)
    fig = px.imshow(hm, labels=dict(x="Week", y="Day", color="PnL"), color_continuous_scale="RdBu", title="PnL Heatmap")
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

# --- SIDEBAR CONTENT (RESTORED) ---
def render_sidebar():
    with st.sidebar:
        st.header("Strategy Playbook")
        
        with st.expander("112 Strategy Rules", expanded=False):
            st.markdown("""
            * **Exit:** Hold for **40-50 Days**.
            * **Golden Rule:** **Do not touch in first 30 days.** Early interference statistically worsens outcomes.
            """)
            
        with st.expander("M200 Strategy (Whale)", expanded=False):
            st.markdown("""
            * **Role:** Variance-tolerant capital.
            * **Entry:** Wednesday.
            * **Debit Target:** `$7,500 - $8,500` per lot.
            * **The "Dip Valley":**
                * P&L often looks worst between Day 15–40.
                * **Management:** Check at **Day 14**.
                * If Red/Flat: **HOLD.** Do not panic exit. Wait for volatility to revert.
            """)
            
        with st.expander("🛡️ Universal Gates", expanded=True):
            st.markdown("""
            1.  **Stability Check:**
                * **> 1.0 (Green):** Fortress. Safe.
                * **< 0.25 (Red):** Coin Flip. Gambling.
            2.  **Volatility:** Ideal VIX 14–22.
            """)

# --- MAIN APP ---
def main():
    init_db()
    render_sidebar()
    
    st.title("🛡️ Allantis Trade Guardian")
    
    t1, t2, t3, t4, t5 = st.tabs(["🏠 Command", "⚡ Active Fleet", "📜 Trade Journal", "📊 Analytics", "⚙️ Data & Config"])
    df = load_data()

    # --- TAB 1: COMMAND ---
    with t1:
        if df.empty: st.info("Welcome! Please go to 'Data & Config' to Restore Database or Upload Trades.")
        else:
            # KPIS
            closed = df[df['status']=='Closed']
            active = df[df['status']=='Open']
            
            # Simple aggregations
            total_realized = closed['pnl'].sum()
            open_pnl = active['pnl'].sum()
            win_rate = (len(closed[closed['pnl']>0]) / len(closed) * 100) if len(closed) > 0 else 0
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Realized PnL", f"${total_realized:,.0f}")
            k2.metric("Open PnL", f"${open_pnl:,.0f}", delta=f"{len(active)} Trades")
            k3.metric("Win Rate", f"{win_rate:.1f}%")
            k4.metric("Profit Factor", "1.5" if total_realized > 0 else "0.0") # Placeholder logic

            st.divider()
            
            c1, c2 = st.columns([2, 1])
            with c1: plot_equity(df)
            with c2:
                st.subheader("🔔 Action Items")
                alerts = []
                for _, r in active.iterrows():
                    # M200 Logic (Restored)
                    if "M200" in r['strategy'] and 13 <= r['days_held'] <= 15:
                        alerts.append(f"⚠️ **{r['name']}**: Day 14 Valley Check")
                    # Max Loss
                    if r['max_loss'] < 0 and r['pnl'] < (r['max_loss'] * 0.8):
                        alerts.append(f"🚨 **{r['name']}**: Near Max Loss")
                    # Stability
                    if r['stability_ratio'] < 0.25 and r['days_held'] > 5:
                         alerts.append(f"📉 **{r['name']}**: Unstable ({r['stability_ratio']:.2f})")
                
                if alerts:
                    for a in alerts: st.markdown(a)
                else:
                    st.success("Fleet is Stable. No critical actions.")

    # --- TAB 2: ACTIVE ---
    with t2:
        active = df[df['status']=='Open']
        if active.empty: st.info("No active trades.")
        else:
            sel = st.multiselect("Filter Strategy", active['strategy'].unique(), active['strategy'].unique())
            filt = active[active['strategy'].isin(sel)]
            
            # Greeks Header
            g1, g2, g3 = st.columns(3)
            g1.progress(min(1.0, max(0.0, (filt['delta'].sum()+100)/200)), f"Portfolio Delta: {filt['delta'].sum():.1f}")
            g2.progress(min(1.0, max(0.0, filt['theta'].sum()/500)), f"Portfolio Theta: {filt['theta'].sum():.1f}")
            g3.caption("Keep Stability Ratio > 1.0")
            
            st.divider()
            
            # Cards
            cols = st.columns(3)
            for i, r in filt.reset_index().iterrows():
                with cols[i%3]:
                    st.markdown(f"""
                    <div class="trade-card">
                        <h4>{r["name"]}</h4>
                        <div style="display:flex; justify-content:space-between; color:#aaa; font-size:0.8em;">
                            <span>{r["strategy"]}</span>
                            <span>Day {r["days_held"]}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    m1, m2 = st.columns(2)
                    m1.metric("PnL", f"${r['pnl']:.0f}")
                    m2.metric("Theta", f"{r['theta']:.1f}")
                    
                    plot_gauge(r['stability_ratio'])
                    
                    with st.expander("Anatomy"):
                        st.write(f"**Entry:** {r['entry_date']}")
                        st.write(f"**Max Loss:** ${r['max_loss']:.0f}")
                        st.write(f"**Delta:** {r['delta']:.2f}")

    # --- TAB 3: JOURNAL ---
    with t3:
        closed = df[df['status']=='Closed']
        if closed.empty: st.info("No history.")
        else:
            st.dataframe(
                closed[['entry_date','exit_date','name','strategy','days_held','pnl','roi']], 
                column_config={
                    "pnl": st.column_config.ProgressColumn("PnL", format="$%.2f", min_value=-5000, max_value=5000),
                    "roi": st.column_config.NumberColumn("ROI", format="%.2f%%"),
                    "entry_date": "Entry",
                    "exit_date": "Exit"
                },
                use_container_width=True, hide_index=True
            )

    # --- TAB 4: ANALYTICS ---
    with t4:
        if not df.empty:
            plot_calendar(df)
            st.divider()
            c1, c2 = st.columns(2)
            strat = df.groupby('strategy')['pnl'].sum().reset_index()
            c1.plotly_chart(px.bar(strat, x='strategy', y='pnl', color='pnl', title="PnL by Strategy"), use_container_width=True)
            c2.plotly_chart(px.histogram(df[df['status']=='Closed'], x='pnl', title="Win/Loss Dist"), use_container_width=True)
            
            st.subheader("🧪 What-If Simulator")
            sc1, sc2, sc3 = st.columns(3)
            s_strat = sc1.selectbox("Strat", ["M200", "112"])
            qty = sc2.number_input("Qty", 1, 10)
            if sc3.button("Simulate"): 
                val = 150 * qty if "M200" in s_strat else 50 * qty
                st.success(f"+{qty}x {s_strat} adds ~${val} Theta exposure.")

    # --- TAB 5: CONFIG ---
    with t5:
        st.subheader("💾 Database Management")
        
        # 1. Restore DB
        st.markdown("##### 1. Restore Database")
        db_file = st.file_uploader("Upload .db file", type=['db'], key="db_restore")
        if db_file:
            if st.button("Restore Database"):
                with open(DB_NAME, "wb") as f:
                    f.write(db_file.getbuffer())
                st.success("Database restored! Refresh page.")
                st.cache_data.clear()

        st.divider()

        # 2. Upload Trades
        st.markdown("##### 2. Import Trades (OptionStrat Export)")
        c1, c2 = st.columns(2)
        
        act = c1.file_uploader("Active Trades (.csv/.xlsx)", key="act")
        if act and c1.button("Sync Active"):
            d = process_file(act)
            if d is not None: 
                cnt = sync_db(d, 'Open')
                st.success(f"Synced {cnt} active trades")
                st.cache_data.clear()

        cls = c2.file_uploader("Closed Trades (.csv/.xlsx)", key="cls")
        if cls and c2.button("Sync Closed"):
            d = process_file(cls)
            if d is not None: 
                cnt = sync_db(d, 'Closed')
                st.success(f"Synced {cnt} closed trades")
                st.cache_data.clear()

        st.divider()
        if st.expander("danger zone").button("WIPE ALL DATA"):
            conn = get_db()
            conn.execute("DELETE FROM trades")
            conn.commit()
            conn.close()
            st.cache_data.clear()
            st.error("Data Wiped.")

if __name__ == "__main__":
    main()
