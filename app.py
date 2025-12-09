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
    "Drop TODAY'S Active File (Excel/CSV)", 
    accept_multiple_files=True
)

st.sidebar.divider()
st.sidebar.header("⚙️ View Settings")
show_closed = st.sidebar.checkbox("Show Expired Trades in Analytics", value=True)

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
                        
                        # RAW GREEKS
                        theta = clean_num(row.get('Theta', 0))
                        delta = clean_num(row.get('Delta', 0))
                        gamma = clean_num(row.get('Gamma', 0))
                        vega = clean_num(row.get('Vega', 0))
                        
                        status = "Active" if "active" in filename else "Expired"
                        if status == "Expired" and pnl == 0: status = "Active"
                        
                        if status == "Expired":
                            try: end_dt = pd.to_datetime(row.get('Expiration'))
                            except: end_dt = datetime.now()
                        else:
                            end_dt = datetime.now()
                            
                        days_held = (end_dt - start_dt).days
                        if days_held < 0: days_held = 0

                        # LOT SIZE LOGIC
                        lot_size = 1
                        if strat == '130/160' and debit > 6000: lot_size = 2
                        elif strat == '130/160' and debit > 10000: lot_size = 3
                        elif strat == '160/190' and debit > 8000: lot_size = 2
                        elif strat == 'M200' and debit > 12000: lot_size = 2
                            
                        debit_lot = debit / max(1, lot_size)
                        
                        # GRADING LOGIC
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

                        # ALERTS (Simple Time/P&L Check only)
                        alerts = []
                        if strat == '130/160' and status == "Active" and 25 <= days_held <= 35 and pnl < 100:
                            alerts.append("💀 STALE CAPITAL")

                        all_data.append({
                            "Name": name, "Strategy": strat, "Status": status,
                            "P&L": pnl, "Debit": debit, "Debit/Lot": debit_lot, 
                            "Grade": grade, "Reason": reason, "Alerts": " ".join(alerts), 
                            "Days Held": days_held,
                            "Theta": theta, "Gamma": gamma, "Vega": vega, "Delta": delta
                        })
                
        except: pass
            
    df = pd.DataFrame(all_data)
    if not df.empty:
        df = df.sort_values(by=['Name', 'Days Held'], ascending=[True, False])
        df['Latest'] = ~df.duplicated(subset=['Name', 'Strategy'], keep='first')
        
    return df

# --- MAIN APP ---
if uploaded_files:
    df = process_data(uploaded_files)
    
    # TABS
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Active Dashboard", "🧪 Trade Validator", "📈 Analytics", "📖 Rule Book"])
    
    # 1. ACTIVE DASHBOARD
    with tab1:
        if not df.empty:
            active_df = df[(df['Status'] == 'Active') & (df['Latest'] == True)].copy()
            
            # --- TOP METRICS ---
            c1, c2, c3 = st.columns(3)
            c1.metric("Active Trades", len(active_df))
            c2.metric("Portfolio Theta", f"{active_df['Theta'].sum():.0f}")
            c3.metric("Open P&L", f"${active_df['P&L'].sum():,.0f}")
            
            st.divider()
            
            # --- STRATEGY OVERVIEW (WITH TOTAL) ---
            st.markdown("### 🏛️ Strategy Overview")
            strat_agg = active_df.groupby('Strategy').agg({
                'P&L': 'sum',
                'Theta': 'sum',
                'Delta': 'sum',
                'Vega': 'sum',
                'Name': 'count'
            }).reset_index()
            
            # Create Total Row
            total_row = pd.DataFrame({
                'Strategy': ['TOTAL'],
                'P&L': [strat_agg['P&L'].sum()],
                'Theta': [strat_agg['Theta'].sum()],
                'Delta': [strat_agg['Delta'].sum()],
                'Vega': [strat_agg['Vega'].sum()],
                'Name': [strat_agg['Name'].sum()]
            })
            
            # Append Total
            final_agg = pd.concat([strat_agg, total_row], ignore_index=True)
            final_agg.columns = ['Strategy', 'Total P&L', 'Net Theta', 'Net Delta', 'Net Vega', 'Trades']
            
            st.dataframe(
                final_agg.style.format({
                    'Total P&L': "${:,.0f}", 'Net Theta': "{:,.0f}", 'Net Delta': "{:,.1f}", 'Net Vega': "{:,.0f}"
                }).apply(lambda x: ['font-weight: bold; background-color: #f0f2f6' if x.name == len(final_agg)-1 else '' for _ in x], axis=1), 
                use_container_width=True
            )
            
            st.divider()
            st.markdown("### 📋 Trade Details (By Strategy)")
            
            # Styling Function
            def style_rows(row):
                if 'A' in str(row['Grade']): return ['color: green; font-weight: bold'] * len(row)
                if 'F' in str(row['Grade']): return ['color: red; font-weight: bold'] * len(row)
                return [''] * len(row)

            # Columns to Display
            disp_cols = ['Name', 'Grade', 'P&L', 'Debit', 'Debit/Lot', 'Days Held', 'Delta', 'Gamma', 'Theta', 'Vega', 'Alerts']
            
            # 130/160 SECTION
            with st.expander("🔹 130/160 Strategies", expanded=True):
                s1 = active_df[active_df['Strategy'] == '130/160']
                if not s1.empty:
                    st.dataframe(s1[disp_cols].style.apply(style_rows, axis=1).format({'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Debit/Lot': "${:,.0f}", 'Gamma': "{:.2f}"}), use_container_width=True)
                else: st.info("No active trades.")

            # 160/190 SECTION
            with st.expander("🔸 160/190 Strategies", expanded=True):
                s2 = active_df[active_df['Strategy'] == '160/190']
                if not s2.empty:
                    st.dataframe(s2[disp_cols].style.apply(style_rows, axis=1).format({'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Debit/Lot': "${:,.0f}", 'Gamma': "{:.2f}"}), use_container_width=True)
                else: st.info("No active trades.")

            # M200 SECTION
            with st.expander("🐳 M200 Strategies", expanded=True):
                s3 = active_df[active_df['Strategy'] == 'M200']
                if not s3.empty:
                    st.dataframe(s3[disp_cols].style.apply(style_rows, axis=1).format({'P&L': "${:,.0f}", 'Debit': "${:,.0f}", 'Debit/Lot': "${:,.0f}", 'Gamma': "{:.2f}"}), use_container_width=True)
                else: st.info("No active trades.")

    # 2. VALIDATOR
    with tab2:
        st.markdown("### 🧪 Pre-Flight Audit")
        
        with st.expander("ℹ️ Grading System Legend (Click to view)", expanded=True):
            st.markdown("""
            | Grade | Strategy | Metric | Why? |
            | :--- | :--- | :--- | :--- |
            | **A+** | 130/160 | Debit `$3.5k - $4.5k` | Historical Sweet Spot. Highest Win Rate. |
            | **F** | 130/160 | Debit `> $4.8k` | **Danger Zone.** 100% of historical losses occurred here. |
            | **A** | 160/190 | Debit `$4.8k - $5.5k` | Ideal pricing for this structure. |
            | **A** | M200 | Debit `$7.5k - $8.5k` | Matches the "Whale" winner profile. |
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
            st.subheader("📈 Historical Analytics")
            expired_df = df[df['Status'] == 'Expired'].copy()
            
            if not expired_df.empty:
                # Summary Stats
                c1, c2, c3 = st.columns(3)
                win_rate = (len(expired_df[expired_df['P&L'] > 0]) / len(expired_df)) * 100
                c1.metric("Win Rate", f"{win_rate:.1f}%")
                c2.metric("Total Profit", f"${expired_df['P&L'].sum():,.0f}")
                c3.metric("Trades Analyzed", len(expired_df))
                
                st.divider()
                
                c1, c2 = st.columns(2)
                with c1: 
                    st.markdown("**Profit by Strategy**")
                    st.plotly_chart(px.bar(expired_df, x='Strategy', y='P&L', color='Strategy'), use_container_width=True)
                
                with c2: 
                    st.markdown("**Entry Price Sweet Spots**")
                    st.plotly_chart(
                        px.scatter(
                            expired_df, 
                            x='Debit/Lot', 
                            y='P&L', 
                            color='Strategy', 
                            hover_data=['Days Held'],
                            title="Win Profile: Price vs Profit"
                        ), 
                        use_container_width=True
                    )
            else:
                st.info("No Expired trades found in current upload. Drop historical files to populate analytics.")

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
