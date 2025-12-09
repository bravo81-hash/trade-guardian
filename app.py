import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
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
    """
    Generates actionable signals based on rules + historical benchmarks.
    """
    action = ""
    signal_type = "NONE" 
    
    if status == "Active":
        # 1. TAKE PROFIT RULE (Use passed benchmarks)
        benchmark = benchmarks_dict.get(strat, {})
        # Fallback to static if dynamic benchmark is missing/zero
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
                        
                        # Status Detection
                        status = "Active" if "active" in filename else "Expired"
                        
                        # Check expiration date existence
                        exp_val = row.get('Expiration', '')
                        has_exp_date = False
                        try: 
                            if not pd.isna(exp_val) and str(exp_val).strip() != '':
                                end_dt = pd.to_datetime(exp_val)
                                has_exp_date = True
                        except: pass

                        # If marked Expired but has $0 P&L and NO expiration date, treat as Active
                        if status == "Expired" and pnl == 0 and not has_exp_date:
                            status = "Active"
                        
                        if status == "Expired" and has_exp_date:
                            pass # end_dt already set
                        else:
                            end_dt = datetime.now()
                            
                        days_held = (end_dt - start_dt).days
                        
                        # Handle Same Day Trades
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
                            "Theta": theta, "Gamma": gamma, "Vega": vega, "Delta": delta
                        })
        
        except Exception:
            pass # Skip failed files silently (or log if needed)
            
    df = pd.DataFrame(all_data)
    if not df.empty:
        df = df.sort_values(by=['Name', 'Days Held'], ascending=[True, False])
        # Deduplication: Keep most recent snapshot
        df['Latest'] = ~df.duplicated(subset=['Name', 'Strategy'], keep='first')
        
    return df

# --- MAIN APP ---
if uploaded_files:
    df = process_data(uploaded_files)
    
    # --- VALIDATION WARNINGS ---
    if not df.empty:
        unknowns = df[df['Strategy'] == 'Other']
        if not unknowns.empty:
            st.sidebar.warning(f"ℹ️ {len(unknowns)} trades have 'Other' strategy.")

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
                # --- ACTION LOGIC ---
                act_list = []
                sig_list = []
                
                for _, row in active_df.iterrows():
                    bench = benchmarks.get(row['Strategy'], {}).get('pnl', 0)
                    act, sig = get_action_signal(
                        row['Strategy'], 
                        row['Status'], 
                        row['Days Held'], 
                        row['P&L'], 
                        benchmarks
                    )
                    act_list.append(act)
                    sig_list.append(sig)
                    
                active_df['Action'] = act_list
                active_df['Signal_Type'] = sig_list
                
                # --- STRATEGY TABS ---
                st.markdown("### 🏛️ Active Trades by Strategy")
                
                strat_tabs = st.tabs(["📋 Strategy Overview", "🔹 130/160", "🔸 160/190", "🐳 M200"])
                
                # Styles (Updated to .map for future-proofing)
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
                        
                        # 1. ALERT TILES (LOCALIZED)
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

                        # 2. METRICS HEADER
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Hist. Avg Win", f"${bench['pnl']:,.0f}")
                        c2.metric("Target Yield", f"{bench['yield']:.2f}%/d")
                        c3.metric("Avg Hold", f"{bench['dit']:.0f}d")
                        
                        # 3. TABLE
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
                    strat_agg = active_df.groupby('Strategy').agg({
                        'P&L': 'sum', 'Debit': 'sum', 'Theta': 'sum', 'Delta': 'sum',
                        'Name': 'count', 'Daily Yield %': 'mean' 
                    }).reset_index()
                    
                    # Trend Logic
                    strat_agg['Trend'] = strat_agg.apply(lambda r: "🟢 Improving" if r['Daily Yield %'] >= benchmarks.get(r['Strategy'], {}).get('yield', 0) else "🔴 Lagging", axis=1)
                    strat_agg['Target %'] = strat_agg['Strategy'].apply(lambda x: benchmarks.get(x, {}).get('yield', 0))
                    
                    # Total Row (FIXED: Added 'Debit' to prevent KeyError)
                    total_row = pd.DataFrame({
                        'Strategy': ['TOTAL'], 
                        'P&L': [strat_agg['P&L'].sum()],
                        'Debit': [strat_agg['Debit'].sum()], # <--- CRITICAL FIX HERE
                        'Theta': [strat_agg['Theta'].sum()], 
                        'Delta': [strat_agg['Delta'].sum()],
                        'Name': [strat_agg['Name'].sum()], 
                        'Daily Yield %': [active_df['Daily Yield %'].mean()],
                        'Trend': ['-'], 'Target %': ['-']
                    })
                    
                    final_agg = pd.concat([strat_agg, total_row], ignore_index=True)
                    
                    # Display Config
                    display_agg = final_agg[['Strategy', 'Trend', 'Daily Yield %', 'Target %', 'P&L', 'Debit', 'Theta', 'Delta', 'Name']].copy()
                    display_agg.columns = ['Strategy', 'Trend', 'Yield/Day', 'Target', 'Total P&L', 'Total Debit', 'Net Theta', 'Net Delta', 'Active Trades']
                    
                    def highlight_trend(val):
                        if '🟢' in str(val): return 'color: green; font-weight: bold'
                        if '🔴' in str(val): return 'color: red; font-weight: bold'
                        return ''

                    def style_total(row):
                        if row['Strategy'] == 'TOTAL':
                            return ['background-color: #e6e9ef; color: black; font-weight: bold'] * len(row)
                        return [''] * len(row)

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
            
            active_df = df[df['Status'] == 'Active'].copy()
            if not active_df.empty:
                st.markdown("#### 🚀 Capital Efficiency Curve (Active Trades)")
                st.caption("Are your trades gaining momentum or stalling? Look for the 'Dip Valley' around Day 15-25.")
                
                fig = px.scatter(
                    active_df, 
                    x='Days Held', 
                    y='Daily Yield %', 
                    color='Strategy', 
                    size='Debit',
                    hover_data=['Name', 'P&L'],
                    title="Real-Time Efficiency: Yield vs Age"
                )
                
                # Dynamic Baseline Lines
                y_130 = benchmarks.get('130/160', {}).get('yield', 0.13)
                y_m200 = benchmarks.get('M200', {}).get('yield', 0.56)
                
                fig.add_hline(y=y_130, line_dash="dash", line_color="blue", annotation_text=f"130/160 Target ({y_130:.2f}%)")
                fig.add_hline(y=y_m200, line_dash="dash", line_color="green", annotation_text=f"M200 Target ({y_m200:.2f}%)")
                st.plotly_chart(fig, use_container_width=True)

            expired_df = df[df['Status'] == 'Expired'].copy()
            if not expired_df.empty:
                st.divider()
                st.markdown("#### 🏆 Historical Performance")
                c1, c2, c3 = st.columns(3)
                win_rate = (len(expired_df[expired_df['P&L'] > 0]) / len(expired_df)) * 100
                c1.metric("Win Rate", f"{win_rate:.1f}%")
                c2.metric("Total Profit", f"${expired_df['P&L'].sum():,.0f}")
                c3.metric("Trades Analyzed", len(expired_df))
                
                st.plotly_chart(
                    px.scatter(
                        expired_df, 
                        x='Debit/Lot', 
                        y='P&L', 
                        color='Strategy', 
                        title="Winning Zone: Entry Price vs Profit"
                    ), 
                    use_container_width=True
                )
            else:
                st.info("No Expired trades found in current upload. Drop historical files to see long-term stats.")

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

else:
    st.info("👋 Upload TODAY'S Active file to see health.")
