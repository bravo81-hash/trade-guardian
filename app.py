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
st.sidebar.header("📂 Data Import")
uploaded_files = st.sidebar.file_uploader(
    "1. Drop ALL History & Active Files Here (Excel or CSV)", 
    accept_multiple_files=True
)

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

def get_grade_and_reason(strat, debit_lot, status, days_held, pnl, theta_lot):
    grade = "C"
    reason = "Standard"
    alert = ""
    
    # 1. PRICING GRADES
    if strat == '130/160':
        if debit_lot > 4800:
            grade = "F"
            reason = "⛔ Overpriced (>$4.8k)"
        elif 3500 <= debit_lot <= 4500:
            grade = "A+"
            reason = "✅ Sweet Spot ($3.5k-$4.5k)"
        else:
            grade = "B"
            reason = "⚠️ Acceptable"
            
        if status == "Active" and 20 <= days_held <= 30 and pnl < 100:
            alert = "💀 KILL ZONE (Stale)"

    elif strat == '160/190':
        if 4800 <= debit_lot <= 5500:
            grade = "A"
            reason = "✅ Ideal Pricing"
        elif debit_lot < 4800:
            grade = "B+"
            reason = "✅ Good Value"
        else:
            grade = "C"
            reason = "⚠️ Expensive"
            
        if status == "Active" and days_held < 30 and pnl < 0:
            alert = "⏳ Cooking (Wait)"

    elif strat == 'M200':
        if 7500 <= debit_lot <= 8500: grade = "A"; reason = "✅ Perfect Size"
        elif debit_lot > 9000: grade = "D"; reason = "⛔ Too Expensive"
        else: grade = "B"; reason = "⚠️ Variance"
            
        if status == "Active" and 13 <= days_held <= 16:
            if pnl > 200: alert = "💰 Day 14 Check"
            else: alert = "🔒 Hold to Day 60"
            
    # 2. GREEK HEALTH CHECK (New Feature)
    # Rule: You want >1.0 Theta for every $1k Debit.
    theta_yield = abs(theta_lot) / (debit_lot / 1000) if debit_lot > 0 else 0
    greek_status = f"{theta_yield:.1f} Decay/$$"
    
    if theta_yield < 0.6:
        greek_status += " (⚠️ LOW)"
    elif theta_yield > 1.0:
        greek_status += " (✅ STRONG)"
        
    return grade, reason, alert, greek_status

# --- CORE PROCESSING ---
@st.cache_data
def process_data(files):
    all_data = []
    
    for f in files:
        try:
            filename = f.name.lower()
            df = None
            
            # EXCEL
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

            # CSV
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
                        
                        status = "Active" if "active" in filename else "Expired"
                        if status == "Expired" and pnl == 0: status = "Active"
                        
                        if status == "Expired":
                            try: end_dt = pd.to_datetime(row.get('Expiration'))
                            except: end_dt = datetime.now()
                        else:
                            end_dt = datetime.now()
                            
                        days_held = (end_dt - start_dt).days
                        if days_held < 0: days_held = 0

                        # Lot Size
                        lot_size = 1
                        if strat == '130/160' and debit > 6000: lot_size = 2
                        elif strat == '130/160' and debit > 10000: lot_size = 3
                        elif strat == '160/190' and debit > 8000: lot_size = 2
                        elif strat == 'M200' and debit > 12000: lot_size = 2
                            
                        debit_lot = debit / max(1, lot_size)
                        theta_lot = theta / max(1, lot_size)
                        delta_lot = delta / max(1, lot_size)
                        
                        grade, reason, alert, greek_stat = get_grade_and_reason(strat, debit_lot, status, days_held, pnl, theta_lot)

                        all_data.append({
                            "Name": name, "Strategy": strat, "Status": status,
                            "P&L": pnl, "Debit/Lot": debit_lot, "Grade": grade,
                            "Reason": reason, "Alert": alert, "Days Held": days_held,
                            "Greek Health": greek_stat, "Delta/Lot": delta_lot, "Theta/Lot": theta_lot
                        })
                
        except: pass
            
    df = pd.DataFrame(all_data)
    if not df.empty:
        df['Status_Rank'] = df['Status'].apply(lambda x: 1 if x == 'Expired' else 0)
        df = df.sort_values(by=['Name', 'Status_Rank'], ascending=[True, True])
        df = df.drop_duplicates(subset=['Name', 'Strategy'], keep='last')
        
    return df

# --- MAIN APP ---
if uploaded_files:
    df = process_data(uploaded_files)
    
    # TABS
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Active Health", "🧪 Trade Validator", "📈 Deep Analytics", "📖 Rule Book"])
    
    # 1. ACTIVE HEALTH
    with tab1:
        if not df.empty:
            active_df = df[df['Status'] == 'Active'].copy()
            
            # Metrics
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Active Trades", len(active_df))
            c2.metric("Total Open P&L", f"${active_df['P&L'].sum():,.2f}")
            c3.metric("Avg P&L", f"${active_df['P&L'].mean():,.2f}")
            alerts = active_df[active_df['Alert'] != ""]
            c4.metric("Alerts", len(alerts), delta_color="inverse")
            
            # STYLING FUNCTION
            def style_dataframe(row):
                cols = ['' for _ in row]
                # Color entire row based on Grade
                if 'A' in str(row['Grade']):
                    return ['background-color: #d4edda; color: #155724'] * len(row) # Light Green
                elif 'F' in str(row['Grade']):
                    return ['background-color: #f8d7da; color: #721c24'] * len(row) # Light Red
                elif 'D' in str(row['Grade']):
                    return ['background-color: #f8d7da; color: #721c24'] * len(row)
                return cols

            # DISPLAY TABLE
            st.subheader("📋 Portfolio Overview")
            display_cols = ['Name', 'Strategy', 'Debit/Lot', 'Grade', 'Reason', 'Greek Health', 'Days Held', 'P&L', 'Alert']
            
            st.dataframe(
                active_df[display_cols].style.apply(style_dataframe, axis=1),
                use_container_width=True,
                height=500
            )

    # 2. VALIDATOR
    with tab2:
        st.markdown("### 🧪 Pre-Flight Audit")
        
        with st.expander("ℹ️ Grading Legend (Click to Expand)", expanded=True):
            st.markdown("""
            | Grade | Meaning | Action |
            | :--- | :--- | :--- |
            | **A+ / A** | **Perfect Entry.** Matches historical winners in price & structure. | ✅ **GO** |
            | **B+ / B** | **Good Value.** Slightly cheap/expensive but acceptable. | ✅ **GO** |
            | **C** | **Average.** No statistical edge found. | ⚠️ **CAUTION** |
            | **D / F** | **Failure Zone.** Historically this price point loses 100% of the time. | ⛔ **NO GO** |
            """)
            
        model_file = st.file_uploader("Upload Model File", key="mod")
        if model_file:
            m_df = process_data([model_file])
            if not m_df.empty:
                row = m_df.iloc[0]
                st.divider()
                st.subheader(f"Result: {row['Name']}")
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Strategy", row['Strategy'])
                c2.metric("Debit/Lot", f"${row['Debit/Lot']:,.0f}")
                c3.metric("Greek Health", row['Greek Health'])
                
                if "A" in row['Grade']:
                    st.success(f"## ✅ APPROVED ({row['Grade']})\n{row['Reason']}")
                elif "F" in row['Grade'] or "D" in row['Grade']:
                    st.error(f"## ⛔ REJECT ({row['Grade']})\n{row['Reason']}")
                else:
                    st.warning(f"## ⚠️ CAUTION ({row['Grade']})\n{row['Reason']}")

    # 3. ANALYTICS
    with tab3:
        if not df.empty:
            st.subheader("📈 Portfolio Intelligence")
            
            # GREEK EXPOSURE
            c1, c2 = st.columns(2)
            active_df = df[df['Status'] == 'Active']
            with c1:
                st.metric("Net Portfolio Delta", f"{active_df['Delta/Lot'].sum():.2f}")
                st.caption("Positive = Bullish | Negative = Bearish/Hedge")
            with c2:
                st.metric("Net Portfolio Theta", f"{active_df['Theta/Lot'].sum():.2f}")
                st.caption("Daily Time Decay collected")

            st.divider()
            
            # HISTORICAL PERFORMANCE
            expired_df = df[df['Status'] == 'Expired']
            if not expired_df.empty:
                st.markdown("#### Historical Strategy Efficiency (Closed Trades)")
                strat_stats = expired_df.groupby('Strategy').agg({
                    'P&L': 'sum',
                    'Debit/Lot': 'mean'
                }).reset_index()
                
                # ROI Calc
                strat_stats['ROI %'] = (strat_stats['P&L'] / (strat_stats['Debit/Lot'] * len(expired_df))) * 100 
                # Note: Rough ROI approx for visualization
                
                c1, c2 = st.columns(2)
                with c1: st.plotly_chart(px.bar(expired_df, x='Strategy', y='P&L', color='Strategy', title="Total Realized P&L"), use_container_width=True)
                with c2: st.plotly_chart(px.box(expired_df, x='Strategy', y='Debit/Lot', title="Winning Entry Prices (Distribution)"), use_container_width=True)

    # 4. RULE BOOK
    with tab4:
        st.markdown("""
        # 📖 Trading Rules & Cheat Sheet
        
        ### 1. 130/160 Strategy (Income)
        * **Target Debit:** `$3,500 - $4,500` per lot.
        * **Red Flag:** > `$4,800` (Expensive).
        * **Exit:** Profit > $500/lot OR 25 Days old.
        
        ### 2. 160/190 Strategy (Compounder)
        * **Target Debit:** `$4,800 - $5,500` per lot.
        * **Sizing:** **1 Lot** is better. (2-Lot ROI drops from 15% -> 7%).
        * **Exit:** Hold 40+ Days.
        
        ### 3. M200 Strategy (Whale)
        * **Target Debit:** `$7,500 - $8,500` per lot.
        * **Exit:** Check Day 14. If Green, roll. If Red, hold to Day 60.
        
        ### 4. Greek Health Rules
        * **Theta Efficiency:** You want **>1.0 Theta** for every **$1k Debit**.
            * *Example:* Debit $4,000 -> Needs >4.0 Theta.
            * *Why:* If Theta is low (e.g., 0.6 per $1k), you are paying too much premium for too little decay.
        """)

else:
    st.info("👋 Upload your files to generate the dashboard.")
