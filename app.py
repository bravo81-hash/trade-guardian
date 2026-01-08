import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
from datetime import datetime

# --- CONFIG & CSS ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️", initial_sidebar_state="collapsed")
st.markdown("""
<style>
    .block-container { padding-top: 1rem; padding-bottom: 5rem; }
    .trade-card { background-color: #1E1E1E; border: 1px solid #333; border-radius: 10px; padding: 15px; margin-bottom: 10px; }
    div[data-testid="stMetricValue"] { font-size: 1.6rem !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: transparent; border-radius: 4px 4px 0px 0px; gap: 1px; }
    .stTabs [aria-selected="true"] { background-color: #262730; border-bottom: 2px solid #FF4B4B; }
</style>
""", unsafe_allow_html=True)

# --- DATABASE ---
DB_NAME = "trade_guardian_v4.db"

def get_db():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute('''CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY, name TEXT, strategy TEXT, status TEXT, entry_date DATE, exit_date DATE,
        days_held INTEGER, debit REAL, pnl REAL, theta REAL, delta REAL, gamma REAL, vega REAL,
        notes TEXT, stability_ratio REAL, roi REAL, max_loss REAL, updated_at TIMESTAMP)''')
    conn.commit()
    conn.close()

# --- LOGIC ---
def get_strategy(name):
    n = str(name).upper()
    if "112" in n: return "112 Strategy"
    if "M200" in n: return "M200"
    if "SMSF" in n: return "SMSF"
    return "Earnings" if "EARNINGS" in n else "Other"

def process_file(file):
    try:
        if file.name.endswith('.xlsx'):
            df = pd.read_excel(file)
        else:
            content = file.getvalue().decode('utf-8')
            lines = content.split('\n')
            data, headers = [], None
            for line in lines:
                if "Symbol,Quantity" in line: continue
                if "Name,Total Return" in line:
                    headers = line.strip().split(',')
                    continue
                if headers and line.strip(): data.append(line)
            if not headers: return None
            df = pd.read_csv(io.StringIO("\n".join([",".join(headers)] + data)))

        if 'Name' not in df.columns: return None
        df = df[~df['Name'].astype(str).str.startswith('.')].copy()
        
        df['entry_date'] = pd.to_datetime(df['Created At']).dt.date
        df['expiration'] = pd.to_datetime(df['Expiration']).dt.date
        for c in ['Total Return $', 'Max Loss', 'Total Return %', 'Net Debit/Credit']:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            
        df['id'] = df['Name'] + "_" + df['entry_date'].astype(str)
        df['strategy'] = df['Name'].apply(get_strategy)
        df['abs_delta'] = df['Delta'].abs()
        df['stability_ratio'] = np.where(df['abs_delta']>0.01, df['Theta']/df['abs_delta'], 0.0)
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return None

def sync_db(df, status):
    conn = get_db()
    c = conn.cursor()
    count = 0
    for _, r in df.iterrows():
        c.execute("SELECT id FROM trades WHERE id=?", (r['id'],))
        exists = c.fetchone()
        vals = (r.get('Total Return $',0), r.get('Theta',0), r.get('Delta',0), r.get('Gamma',0), 
                r.get('Vega',0), status, r.get('stability_ratio',0), r.get('Total Return %',0), 
                r.get('Max Loss',0), datetime.now())
        if exists:
            c.execute('''UPDATE trades SET pnl=?, theta=?, delta=?, gamma=?, vega=?, status=?, 
                stability_ratio=?, roi=?, max_loss=?, updated_at=? WHERE id=?''', vals + (r['id'],))
        else:
            c.execute('''INSERT INTO trades (pnl, theta, delta, gamma, vega, status, stability_ratio, 
                roi, max_loss, updated_at, id, name, strategy, entry_date, debit) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', vals + (r['id'], r['Name'], r['strategy'], 
                r['entry_date'], r.get('Net Debit/Credit',0)))
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
    fig = px.area(closed, x='exit_date', y='cum_pnl', title="Equity Curve", color_discrete_sequence=['#00CC96'])
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
    hm = hm.reindex(['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])
    fig = px.imshow(hm, labels=dict(x="Week", y="Day", color="PnL"), color_continuous_scale="RdBu", title="PnL Heatmap")
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

# --- MAIN APP ---
def main():
    init_db()
    st.title("🛡️ Allantis Trade Guardian")
    with st.sidebar:
        st.subheader("Strategy Rules")
        with st.expander("112 Strategy"): st.write("**Exit:** 40-50 Days.\n**Rule:** No touch first 30 days.")
        with st.expander("M200"): st.write("**Entry:** Wed.\n**Target:** $7.5k-$8.5k.\n**Day 14:** Valley Check.")
        with st.expander("Gates"): st.write("Stability > 1.0 = Safe\nStability < 0.25 = Gamble")

    t1, t2, t3, t4, t5 = st.tabs(["🏠 Command", "⚡ Active", "📜 Journal", "📊 Analytics", "⚙️ Config"])
    df = load_data()

    with t1:
        if df.empty: st.info("Upload data in Config tab.")
        else:
            c1,c2,c3,c4 = st.columns(4)
            closed = df[df['status']=='Closed']
            wr = len(closed[closed['pnl']>0])/len(closed)*100 if len(closed)>0 else 0
            c1.metric("Realized PnL", f"${closed['pnl'].sum():,.0f}")
            c2.metric("Open PnL", f"${df[df['status']=='Open']['pnl'].sum():,.0f}")
            c3.metric("Win Rate", f"{wr:.1f}%")
            c4.metric("Trades", len(df))
            st.divider()
            col1, col2 = st.columns([2,1])
            with col1: plot_equity(df)
            with col2:
                st.subheader("🔔 Alerts")
                alerts = []
                for _,r in df[df['status']=='Open'].iterrows():
                    if "M200" in r['strategy'] and 13<=r['days_held']<=15: alerts.append(f"⚠️ {r['name']}: Day 14 Check")
                    if r['max_loss']<0 and r['pnl']<(r['max_loss']*0.8): alerts.append(f"🚨 {r['name']}: Max Loss Risk")
                    if r['stability_ratio']<0.25 and r['days_held']>5: alerts.append(f"📉 {r['name']}: Unstable")
                if alerts: 
                    for a in alerts: st.markdown(a)
                else: st.success("All Clear")

    with t2:
        active = df[df['status']=='Open']
        if active.empty: st.info("No active trades.")
        else:
            sel = st.multiselect("Filter Strategy", active['strategy'].unique(), active['strategy'].unique())
            filt = active[active['strategy'].isin(sel)]
            c1,c2,c3 = st.columns(3)
            c1.progress(min(1.0, max(0.0,(filt['delta'].sum()+100)/200)), f"Delta: {filt['delta'].sum():.1f}")
            c2.progress(min(1.0, max(0.0,filt['theta'].sum()/500)), f"Theta: {filt['theta'].sum():.1f}")
            st.divider()
            cols = st.columns(3)
            for i, r in filt.reset_index().iterrows():
                with cols[i%3]:
                    st.markdown(f'<div class="trade-card"><h4>{r["name"]}</h4><p>{r["strategy"]} | Day {r["days_held"]}</p></div>', unsafe_allow_html=True)
                    m1, m2 = st.columns(2)
                    m1.metric("PnL", f"${r['pnl']:.0f}")
                    m2.metric("Theta", f"{r['theta']:.1f}")
                    plot_gauge(r['stability_ratio'])
                    with st.expander("Details"):
                        st.write(f"Entry: {r['entry_date']}\nMax Loss: {r['max_loss']}")

    with t3:
        closed = df[df['status']=='Closed']
        if closed.empty: st.info("No history.")
        else:
            st.dataframe(closed[['entry_date','exit_date','name','strategy','days_held','pnl','roi']], 
                column_config={"pnl": st.column_config.ProgressColumn("PnL", format="$%.2f", min_value=-5000, max_value=5000),
                               "roi": st.column_config.NumberColumn("ROI", format="%.2f%%")},
                use_container_width=True, hide_index=True)

    with t4:
        if not df.empty:
            plot_calendar(df)
            st.divider()
            c1, c2 = st.columns(2)
            strat = df.groupby('strategy')['pnl'].sum().reset_index()
            c1.plotly_chart(px.bar(strat, x='strategy', y='pnl', color='pnl', title="Strategy PnL"), use_container_width=True)
            c2.plotly_chart(px.histogram(df[df['status']=='Closed'], x='pnl', title="PnL Dist"), use_container_width=True)
            st.subheader("Simulate")
            sc1, sc2, sc3 = st.columns(3)
            s_strat = sc1.selectbox("Strat", ["M200", "112"])
            qty = sc2.number_input("Qty", 1, 10)
            if sc3.button("Sim"): st.success(f"+{qty}x {s_strat} adds ~${(150 if 'M' in s_strat else 50)*qty} Theta")

    with t5:
        c1, c2 = st.columns(2)
        act = c1.file_uploader("Active Trades", key="a")
        if act and c1.button("Sync Active"):
            d = process_file(act)
            if d is not None: st.success(f"Synced {sync_db(d, 'Open')} trades"); st.cache_data.clear()
        cls = c2.file_uploader("Closed Trades", key="c")
        if cls and c2.button("Sync Closed"):
            d = process_file(cls)
            if d is not None: st.success(f"Synced {sync_db(d, 'Closed')} trades"); st.cache_data.clear()
        if st.expander("Reset DB").button("WIPE DATA"):
            conn = get_db(); conn.execute("DELETE FROM trades"); conn.commit(); conn.close()
            st.cache_data.clear(); st.error("Done")

if __name__ == "__main__":
    main()
