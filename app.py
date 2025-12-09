import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")
st.title("🛡️ Allantis Trade Guardian")

# --- SIDEBAR ---
st.sidebar.header("Daily Workflow")
uploaded_files = st.sidebar.file_uploader(
    "Drop Active & History Files (Excel/CSV)", 
    accept_multiple_files=True
)

st.sidebar.divider()
st.sidebar.header("⚙️ View Settings")
show_closed = st.sidebar.checkbox("Show Expired Trades in Analytics", value=True)

# --- FALLBACK CONSTANTS ---
STATIC_BASELINES = {
    '130/160': {'yield': 0.13, 'pnl': 500, 'roi': 6.8, 'dit': 36},
    '160/190': {'yield': 0.28, 'pnl': 700, 'roi': 12.7, 'dit': 44},
    'M200':    {'yield': 0.56, 'pnl': 900, 'roi': 11.1, 'dit': 41}
}

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

def safe_fmt(val, fmt_str):
    try:
        if isinstance(val, (int, float)): return fmt_str.format(val)
        return str(val)
    except: return str(val)

# --- SMART EXIT ENGINE ---
def get_action_signal(strat, status, days_held, pnl, benchmarks_dict):
    action = ""
    signal_type = "NONE" 
    
    if status == "Active":
        # 1. TAKE PROFIT RULE
        benchmark = benchmarks_dict.get(strat, {})
        target = benchmark.get('pnl', 0)
        if target == 0: 
            target = STATIC_BASELINES.get(strat, {}).get('pnl', 9999)
            
        if pnl >= target:
            return f"TAKE PROFIT (Hit ${target:,.0f})", "SUCCESS"

        # 2. STRATEGY SPECIFIC RULES
        if strat == '130/160':
            if 25 <= days_held <= 35 and pnl < 100:
                return "KILL (Stale >25d)", "ERROR"
            
        elif strat == '160/190':
            if days_held < 30:
                return "COOKING (Do Not Touch)", "INFO"
            elif 30 <= days_held <= 40:
                return "WATCH (Profit Zone)", "WARNING"

        elif strat == 'M200':
            if 12 <= days_held <= 16:
                if pnl > 200: return "DAY 14 CHECK (Green)", "SUCCESS"
                else: return "DAY 14 CHECK (Red)", "WARNING"
                
    return action, signal_type

# --- CORE PROCESSING ---
@st.cache_data
def process_data(files):
    all_data = []
    
    for f in files:
        try:
            filename = f.name.lower()
            df = None
            
            # EXCEL READER
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                df_raw = pd.read_excel(f, header=None, engine='openpyxl')
                header_idx = -1
                for i, row in df_raw.head(20).iterrows():
                    row_str = " ".join(row.astype(str).values)
                    if "Name" in row_str and "Total Return" in row_str:
                        header_idx = i
                        break
                if header_idx != -1:
                    df = df_raw.iloc[header_idx+1:].copy()
                    df.columns = df_raw.iloc[header_idx]

            # CSV READER
            else:
                content = f.getvalue().decode("utf-8")
                lines = content.split('\n')
                header_idx = 0
                for i, line in enumerate(lines[:20]):
                    if "Name" in line and "Total Return" in line:
                        header_idx = i
                        break
                df = pd.read_csv(io.StringIO(content), skiprows=header_idx)
            
            if df is not None:
                for _, row in df.iterrows():
                    created_val = row.get('Created At', '')
                    is_valid_date = False
                    if isinstance(created_val, (pd.Timestamp, datetime)):
                        is_valid_date = True
                        start_dt = created_val
                    elif isinstance(created_val, str) and len(created_val) > 8 and ':' in created_val:
                        is_valid_date = True
                        try: start_dt = pd.to_datetime(created_val)
                        except: is_valid_date = False

                    if is_valid_date:
                        name = row.get('Name', 'Unknown')
                        group = str(row.get('Group', ''))
                        strat = get_strategy(group)
                        
                        pnl = clean_num(row.get('Total Return $', 0))
                        debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                        
                        theta = clean_num(row.get('Theta', 0))
                        delta = clean_num(row.get('Delta', 0))
                        gamma = clean_num(row.get('Gamma', 0))
                        vega = clean_num(row.get('Vega', 0))
                        
                        status = "Active" if "active" in filename else "Expired"
                        
                        exp_val = row.get('Expiration', '')
                        has_exp_date = False
                        try: 
                            if not pd.isna(exp_val) and str(exp_val).strip() != '':
                                end_dt = pd.to_datetime(exp_val)
                                has_exp_date = True
                        except: pass

                        if status == "Expired" and pnl == 0 and not has_exp_date:
                            status = "Active"
                        
                        if status == "Expired" and has_exp_date:
                            pass
                        else:
                            end_dt = datetime.now()
                            
                        days_held = (end_dt - start_dt).days
                        if days_held < 1: days_held = 1 

                        # METRICS
                        roi = (pnl / debit * 100) if debit > 0 else 0
                        daily_yield = roi / days_held

                        lot_size = 1
                        if strat == '130/160' and debit > 6000: lot_size = 2
                        elif strat == '130/160' and debit > 10000: lot_size = 3
                        elif strat == '160/190' and debit > 8000: lot_size = 2
                        elif strat == 'M200' and debit > 12000: lot_size = 2
                            
                        debit_lot = debit / max(1, lot_size)
                        
                        grade = "C"
                        reason = "Standard"
                        
                        if strat == '130/160':
                            if debit_lot > 4800: grade = "F"; reason = "Overpriced (> $4.8k)"
                            elif 3500 <= debit_lot <= 4500: grade = "A+"; reason = "Sweet Spot"
                            else: grade = "B"; reason = "Acceptable"
                        elif strat == '160/190':
                            if 4800 <= debit_lot <= 5500: grade = "A"; reason = "Ideal Pricing"
                            else: grade = "C"; reason = "Check Pricing"
                        elif strat == 'M200':
                            if 7500 <= debit_lot <= 8500: grade = "A"; reason = "Perfect Entry"
                            else: grade = "B"; reason = "Variance"

                        alerts = []
                        if strat == '130/160' and status == "Active" and 25 <= days_held <= 35 and pnl < 100:
                            alerts.append("💀 STALE CAPITAL")

                        all_data.append({
                            "Name": name, "Strategy": strat, "Status": status,
                            "P&L": pnl, "Debit": debit, "Debit/Lot": debit_lot, 
                            "Grade": grade, "Reason": reason, "Alerts": " ".join(alerts), 
                            "Days Held": days_held, "Daily Yield %": daily_yield, "ROI": roi,
                            "Theta": theta, "Gamma": gamma, "Vega": vega, "Delta": delta,
                            "Entry Date": start_dt
                        })
        
        except Exception:
            pass 
            
    df = pd.DataFrame(all_data)
    if not df.empty:
        df = df.sort_values(by=['Name', 'Days Held'], ascending=[True, False])
        df['Latest'] = ~df.duplicated(subset=['Name', 'Strategy'], keep='first')
        
    return df

# --- MAIN APP ---
if uploaded_files:
    df = process_data(uploaded_files)
    
    # --- CALCULATE BENCHMARKS ---
    expired_df = df[df['Status'] == 'Expired']
    benchmarks = STATIC_BASELINES.copy()
    
    if not expired_df.empty:
        hist_grp = expired_df.groupby('Strategy')
        for strat, grp in hist_grp:
            winners = grp[grp['P&L'] > 0]
            if not winners.empty:
                benchmarks[strat] = {
                    'yield': grp['Daily Yield %'].mean(),
                    'pnl': winners['P&L'].mean(),
                    'roi': winners['ROI'].mean(),
                    'dit': winners['Days Held'].mean()
                }
            
    # TABS
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Active Dashboard", "🧪 Trade Validator", "📈 Analytics", "📖 Rule Book"])
    
    # 1. ACTIVE DASHBOARD
    with tab1:
        if not df.empty:
            active_df = df[(df['Status'] == 'Active') & (df['Latest'] == True)].copy()
            
            if active_df.empty:
                st.info("📭 No active trades found. Upload a current Active File.")
            else:
                # --- PORTFOLIO HEALTH SIDEBAR ---
                port_yield = active_df['Daily Yield %'].mean()
                if port_yield < 0.10:
                    st.sidebar.error(f"🚨 Yield Critical: {port_yield:.2f}%")
                elif port_yield < 0.15:
                    st.sidebar.warning(f"⚠️ Yield Low: {port_yield:.2f}%")
                else:
                    st.sidebar.success(f"✅ Yield Healthy: {port_yield:.2f}%")

                # --- ACTION LOGIC ---
                act_list = []
                sig_list = []
                for _, row in active_df.iterrows():
                    bench = benchmarks.get(row['Strategy'], {}).get('pnl', 0)
                    act, sig = get_action_signal(
                        row['Strategy'], row['Status'], row['Days Held'], row['P&L'], benchmarks
                    )
                    act_list.append(act)
                    sig_list.append(sig)
                    
                active_df['Action'] = act_list
                active_df['Signal_Type'] = sig_list
                
                # --- STRATEGY TABS ---
                st.markdown("### 🏛️ Active Trades by Strategy")
                
                strat_tabs = st.tabs(["📋 Strategy Overview", "🔹 130/160", "🔸 160/190", "🐳 M200"])
                
                # Styles
                def style_table(styler):
                    return styler.map(lambda v: 'background-color: #d1e7dd; color: #0f5132; font-weight: bold' if 'TAKE PROFIT' in str(v) 
                                           else 'background-color: #f8d7da; color: #842029; font-weight: bold' if 'KILL' in str(v) 
                                           else '', subset=['Action']) \
                                 .map(lambda v: 'color: #0f5132; font-weight: bold' if 'A' in str(v) 
                                           else 'color: #842029; font-weight: bold' if 'F' in str(v) 
                                           else '', subset=['Grade'])

                cols = ['Name', 'Action', 'Grade', 'Daily Yield %', 'P&L', 'Debit', 'Days Held', 'Theta', 'Delta', 'Gamma', 'Vega']

                def render_tab(tab, strategy_name):
                    with tab:
                        subset = active_df[active_df['Strategy'] == strategy_name].copy()
                        bench = benchmarks.get(strategy_name, {'pnl':0, 'roi':0, 'dit':0, 'yield':0})
                        
                        # ALERTS (LOCALIZED)
                        urgent = subset[subset['Action'] != ""]
                        if not urgent.empty:
                            st.markdown(f"**🚨 Action Center ({len(urgent)})**")
                            for _, row in urgent.iterrows():
                                sig = row['Signal_Type']
                                msg = f"**{row['Name']}**: {row['Action']}"
                                if sig == "SUCCESS": st.success(msg)
                                elif sig == "ERROR": st.error(msg)
                                elif sig == "WARNING": st.warning(msg)
                                else: st.info(msg)
                            st.divider()

                        # METRICS
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Hist. Avg Win", f"${bench['pnl']:,.0f}")
                        c2.metric("Target Yield", f"{bench['yield']:.2f}%/d")
                        c3.metric("Avg Hold", f"{bench['dit']:.0f}d")
                        
                        # TABLE
                        if not subset.empty:
                            sum_row = pd.DataFrame({
                                'Name': ['TOTAL'], 'Action': ['-'], 'Grade': ['-'],
                                'Daily Yield %': [subset['Daily Yield %'].mean()],
                                'P&L': [subset['P&L'].sum()], 'Debit': [subset['Debit'].sum()],
                                'Days Held': [subset['Days Held'].mean()],
                                'Theta': [subset['Theta'].sum()], 'Delta': [subset['Delta'].sum()],
                                'Gamma': [subset['Gamma'].sum()], 'Vega': [subset['Vega'].sum()]
                            })
                            display = pd.concat([subset[cols], sum_row], ignore_index=True)
                            
                            st.dataframe(
                                style_table(display.style)
                                .format({
                                    'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Daily Yield %': "{:.2f}%",
                                    'Theta': "{:.1f}", 'Delta': "{:.1f}", 'Gamma': "{:.2f}", 'Vega': "{:.0f}",
                                    'Days Held': "{:.0f}"
                                })
                                .apply(lambda x: ['background-color: #e6e9ef; color: black; font-weight: bold' if x.name == len(display)-1 else '' for _ in x], axis=1),
                                use_container_width=True, height=400
                            )
                        else:
                            st.info("No active trades.")

                # TAB 1: OVERVIEW
                with strat_tabs[0]:
                    # Risk Metrics Dashboard (NEW)
                    with st.expander("📊 Portfolio Risk Metrics", expanded=True):
                        total_delta = active_df['Delta'].sum()
                        total_theta = active_df['Theta'].sum()
                        total_cap = active_df['Debit'].sum()
                        
                        r1, r2, r3 = st.columns(3)
                        r1.metric("Net Portfolio Delta", f"{total_delta:,.1f}", delta="Bullish" if total_delta > 0 else "Bearish")
                        r2.metric("Daily Theta Income", f"${total_theta:,.0f}")
                        r3.metric("Capital Deployment", f"${total_cap:,.0f}")

                    # Aggregation
                    strat_agg = active_df.groupby('Strategy').agg({
                        'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum', 'Delta': 'sum',
                        'Name': 'count', 'Daily Yield %': 'mean' 
                    }).reset_index()
                    
                    strat_agg['Trend'] = strat_agg.apply(lambda r: "🟢 Improving" if r['Daily Yield %'] >= benchmarks.get(r['Strategy'], {}).get('yield', 0) else "🔴 Lagging", axis=1)
                    strat_agg['Target %'] = strat_agg['Strategy'].apply(lambda x: benchmarks.get(x, {}).get('yield', 0))
                    
                    total_row = pd.DataFrame({
                        'Strategy': ['TOTAL'], 
                        'P&L': [strat_agg['P&L'].sum()],
                        'Debit': [strat_agg['Debit'].sum()],
                        'Theta': [strat_agg['Theta'].sum()], 
                        'Delta': [strat_agg['Delta'].sum()],
                        'Name': [strat_agg['Name'].sum()], 
                        'Daily Yield %': [active_df['Daily Yield %'].mean()],
                        'Trend': ['-'], 'Target %': ['-']
                    })
                    
                    final_agg = pd.concat([strat_agg, total_row], ignore_index=True)
                    
                    display_agg = final_agg[['Strategy', 'Trend', 'Daily Yield %', 'Target %', 'P&L', 'Debit', 'Theta', 'Delta', 'Name']].copy()
                    display_agg.columns = ['Strategy', 'Trend', 'Yield/Day', 'Target', 'Total P&L', 'Total Debit', 'Net Theta', 'Net Delta', 'Active Trades']
                    
                    def highlight_trend(val):
                        if '🟢' in str(val): return 'color: green; font-weight: bold'
                        if '🔴' in str(val): return 'color: red; font-weight: bold'
                        return ''

                    def style_total(row):
                        if row['Strategy'] == 'TOTAL':
                            return ['background-color: #e6e9ef; color: black; font-weight: bold'] * len(row)
                        return [''] * len(row) # Fixed Indentation here

                    st.dataframe(
                        display_agg.style
                        .format({
                            'Total P&L': "${:,.0f}", 'Total Debit': "${:,.0f}",
                            'Net Theta': "{:,.0f}", 'Net Delta': "{:,.1f}",
                            'Yield/Day': lambda x: safe_fmt(x, "{:.2f}%"), 'Target': lambda x: safe_fmt(x, "{:.2f}%")
                        })
                        .map(highlight_trend, subset=['Trend'])
                        .apply(style_total, axis=1), 
                        use_container_width=True
                    )
                    
                    # CSV Export (NEW)
                    csv = active_df.to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Download Active Trades CSV", csv, "active_snapshot.csv", "text/csv")

                render_tab(strat_tabs[1], '130/160')
                render_tab(strat_tabs[2], '160/190')
                render_tab(strat_tabs[3], 'M200')

    # 2. VALIDATOR
    with tab2:
        st.markdown("### 🧪 Pre-Flight Audit")
        
        with st.expander("ℹ️ Grading System Legend", expanded=True):
            st.markdown("""
            | Strategy | Grade | Debit Range (Per Lot) | Verdict |
            | :--- | :--- | :--- | :--- |
            | **130/160** | **A+** | `$3,500 - $4,500` | ✅ **Sweet Spot** (Highest statistical win rate) |
            | **130/160** | **B** | `< $3,500` or `$4,500-$4,800` | ⚠️ **Acceptable** (Watch volatility) |
            | **130/160** | **F** | `> $4,800` | ⛔ **Overpriced** (Historical failure rate 100%) |
            | **160/190** | **A** | `$4,800 - $5,500` | ✅ **Ideal** Pricing |
            | **160/190** | **C** | `> $5,500` | ⚠️ **Expensive** (Reduces ROI efficiency) |
            | **M200** | **A** | `$7,500 - $8,500` | ✅ **Perfect** "Whale" sizing |
            | **M200** | **B** | Any other price | ⚠️ **Variance** from mean |
            """)
            
        model_file = st.file_uploader("Upload Model File", key="mod")
        if model_file:
            m_df = process_data([model_file])
            if not m_df.empty:
                row = m_df.iloc[0]
                st.divider()
                st.subheader(f"Audit: {row['Name']}")
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Strategy", row['Strategy'])
                c2.metric("Debit Total", f"${row['Debit']:,.0f}")
                c3.metric("Debit Per Lot", f"${row['Debit/Lot']:,.0f}")
                
                if not expired_df.empty:
                    similar = expired_df[
                        (expired_df['Strategy'] == row['Strategy']) & 
                        (expired_df['Debit/Lot'].between(row['Debit/Lot']*0.9, row['Debit/Lot']*1.1))
                    ]
                    if not similar.empty:
                        avg_win = similar[similar['P&L']>0]['P&L'].mean()
                        st.info(f"📊 **Historical Context:** Found {len(similar)} similar trades. Average Win: **${avg_win:,.0f}**")
                
                if "A" in row['Grade']:
                    st.success(f"✅ **APPROVED:** {row['Reason']}")
                elif "F" in row['Grade']:
                    st.error(f"⛔ **REJECT:** {row['Reason']}")
                else:
                    st.warning(f"⚠️ **CHECK:** {row['Reason']}")

    # 3. ANALYTICS
    with tab3:
        if not df.empty:
            st.subheader("📈 Analytics & Trends")
            
            # --- DATE FILTER ---
            if 'Entry Date' in df.columns:
                min_date = df['Entry Date'].min()
                max_date = df['Entry Date'].max()
                date_range = st.date_input("Filter by Entry Date", [min_date, max_date])
                
                if len(date_range) == 2:
                    start_d, end_d = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
                    end_d = end_d + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                    filtered_df = df[(df['Entry Date'] >= start_d) & (df['Entry Date'] <= end_d)]
                else:
                    filtered_df = df
            else:
                filtered_df = df
            
            # --- TABS FOR ANALYTICS ---
            an_tabs = st.tabs(["🚀 Efficiency", "⚔️ Head-to-Head", "🔥 Heatmap"])
            
            # TAB 1: EFFICIENCY SCATTER
            with an_tabs[0]:
                active_sub = filtered_df[filtered_df['Status'] == 'Active'].copy()
                if not active_sub.empty:
                    fig = px.scatter(
                        active_sub, x='Days Held', y='Daily Yield %', color='Strategy', size='Debit',
                        hover_data=['Name', 'P&L'], title="Real-Time Efficiency: Yield vs Age"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No active data for chart.")

            # TAB 2: HEAD TO HEAD (NEW)
            with an_tabs[1]:
                expired_sub = filtered_df[filtered_df['Status'] == 'Expired'].copy()
                if not expired_sub.empty:
                    perf = expired_sub.groupby('Strategy').agg({
                        'P&L': ['count', 'sum', 'mean'],
                        'Days Held': 'mean',
                        'Daily Yield %': 'mean'
                    }).reset_index()
                    perf.columns = ['Strategy', 'Count', 'Total P&L', 'Avg P&L', 'Avg Days', 'Avg Daily Yield']
                    st.dataframe(perf.style.format({'Total P&L': "${:,.0f}", 'Avg P&L': "${:,.0f}", 'Avg Days': "{:.0f}", 'Avg Daily Yield': "{:.2f}%"}), use_container_width=True)
                else:
                    st.info("No historical data available.")

            # TAB 3: HEATMAP (NEW)
            with an_tabs[2]:
                expired_sub = filtered_df[filtered_df['Status'] == 'Expired'].copy()
                if not expired_sub.empty:
                    fig = px.density_heatmap(
                        expired_sub, x="Days Held", y="Strategy", z="P&L", 
                        histfunc="avg", title="Profit Heatmap: Where is the Sweet Spot?",
                        color_continuous_scale="RdBu"
                    )
                    st.plotly_chart(fig, use_container_width=True)

    # 4. RULE BOOK
    with tab4:
        st.markdown("""
        # 📖 Trading Constitution
        
        ### 1. 130/160 Strategy (Income Engine)
        * **Target Entry:** Monday.
        * **Debit Target:** `$3,500 - $4,500` per lot.
        * **Stop Rule:** Never pay > `$4,800` per lot.
        * **Management:** Kill if trade is **25 days old** and profit is flat/negative.
        
        ### 2. 160/190 Strategy (Compounder)
        * **Target Entry:** Friday.
        * **Debit Target:** `~$5,200` per lot.
        * **Sizing:** Trade **1 Lot** (Scaling to 2 lots reduces ROI).
        * **Exit:** Hold for **40-50 Days**. Do not touch in first 30 days.
        
        ### 3. M200 Strategy (Whale)
        * **Target Entry:** Wednesday.
        * **Debit Target:** `$7,500 - $8,500` per lot.
        * **Management:** Check P&L at **Day 14**.
            * If Green > $200: Exit or Roll.
            * If Red/Flat: HOLD. Do not exit in the "Dip Valley" (Day 15-50).
        """)
        st.divider()
        st.caption("Allantis Trade Guardian v28.0 | Enterprise Edition")

else:
    st.info("👋 Upload TODAY'S Active file to see health.")
