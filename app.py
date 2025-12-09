import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")
st.title("🛡️ Allantis Trade Guardian: Professional")

# --- SIDEBAR ---
st.sidebar.header("📂 Data Import")
uploaded_files = st.sidebar.file_uploader(
    "Drop Active/Expired Files (Excel/CSV)", 
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
                    # Date Validation
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
                        
                        # GREEKS
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

                        # Sizing Logic
                        lot_size = 1
                        if strat == '130/160' and debit > 6000: lot_size = 2
                        elif strat == '130/160' and debit > 10000: lot_size = 3
                        elif strat == '160/190' and debit > 8000: lot_size = 2
                        elif strat == 'M200' and debit > 12000: lot_size = 2
                            
                        debit_lot = debit / max(1, lot_size)
                        
                        # --- GRADING LOGIC (PRICE ONLY) ---
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

                        # --- GREEK RATIOS (INFORMATIONAL) ---
                        # Avoid div by zero
                        safe_theta = abs(theta) if abs(theta) > 0.1 else 0.1
                        
                        # 1. Explosion Ratio (Gamma / Theta)
                        # How much does price hurt vs time helps?
                        expl_ratio = abs(gamma / safe_theta)
                        
                        # 2. Vol Ratio (Vega / Theta)
                        # Are we trading Vol or Time?
                        vol_ratio = abs(vega / safe_theta)
                        
                        # ALERTS (Only Critical Ones)
                        alerts = []
                        if expl_ratio > 1.0: 
                            alerts.append("CRITICAL GAMMA: Risk > Reward")
                        if strat == '130/160' and status == "Active" and 25 <= days_held <= 35 and pnl < 100:
                            alerts.append("STALE CAPITAL: >25 Days Flat")

                        all_data.append({
                            "Name": name, "Strategy": strat, "Status": status,
                            "P&L": pnl, "Debit/Lot": debit_lot, "Grade": grade,
                            "Reason": reason, "Alerts": " | ".join(alerts), 
                            "Days Held": days_held,
                            "Theta": theta, "Gamma": gamma, "Vega": vega,
                            "Gamma/Theta": expl_ratio, "Vega/Theta": vol_ratio
                        })
                
        except: pass
            
    df = pd.DataFrame(all_data)
    if not df.empty:
        # Sort and Dedup
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
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Active Trades", len(active_df))
            c2.metric("Portfolio Theta", f"{active_df['Theta'].sum():.0f}", help="Daily Time Decay")
            c3.metric("Open P&L", f"${active_df['P&L'].sum():,.0f}")
            
            # Critical Alerts Only
            critical = active_df[active_df['Alerts'] != ""]
            if not critical.empty:
                st.error(f"🚨 **Attention Needed ({len(critical)})**")
                for _, r in critical.iterrows():
                    st.write(f"• **{r['Name']}**: {r['Alerts']}")
            
            st.markdown("### 📋 Trade Monitor")
            
            # Display Columns
            cols = ['Name', 'Strategy', 'Grade', 'Debit/Lot', 'P&L', 'Days Held', 'Gamma/Theta', 'Vega/Theta']
            
            # Styling
            def color_grade(val):
                if 'A' in str(val): return 'color: green; font-weight: bold'
                if 'F' in str(val): return 'color: red; font-weight: bold'
                return 'color: orange'

            def color_ratios(val):
                # Gamma Ratio formatting
                if isinstance(val, float):
                    if val > 0.8: return 'background-color: #f8d7da; color: red' # Dangerous
                    if val < 0.3: return 'color: green' # Safe
                return ''

            # Render Table
            st.dataframe(
                active_df[cols].style
                .applymap(color_grade, subset=['Grade'])
                .applymap(color_ratios, subset=['Gamma/Theta'])
                .format({'Debit/Lot': "${:,.0f}", 'P&L': "${:,.0f}", 'Gamma/Theta': "{:.2f}", 'Vega/Theta': "{:.1f}"}),
                use_container_width=True,
                height=600
            )
            
            st.caption("**Metrics Key:**")
            st.caption("* **Gamma/Theta:** Measures stability. Lower (< 0.5) is better. If > 1.0, price risk is critical.")
            st.caption("* **Vega/Theta:** Measures volatility sensitivity. Lower is better for income trading.")

    # 2. VALIDATOR
    with tab2:
        st.markdown("### 🧪 Pre-Flight Audit")
        model_file = st.file_uploader("Upload Model File", key="mod")
        if model_file:
            m_df = process_data([model_file])
            if not m_df.empty:
                row = m_df.iloc[0]
                st.divider()
                st.subheader(f"Audit: {row['Name']}")
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Grade", row['Grade'])
                c2.metric("Gamma/Theta", f"{row['Gamma/Theta']:.2f}")
                c3.metric("Vega/Theta", f"{row['Vega/Theta']:.1f}")
                
                if "A" in row['Grade']:
                    st.success(f"✅ **APPROVED:** {row['Reason']}")
                elif "F" in row['Grade']:
                    st.error(f"⛔ **REJECT:** {row['Reason']}")
                else:
                    st.warning(f"⚠️ **CHECK:** {row['Reason']}")

    # 3. ANALYTICS
    with tab3:
        if not df.empty:
            st.subheader("📈 Portfolio Intelligence")
            expired_df = df[df['Status'] == 'Expired'].copy()
            if not expired_df.empty:
                # FIXED: Simple Scatter Plot (No Size variable to avoid crash)
                st.plotly_chart(
                    px.scatter(
                        expired_df, 
                        x='Debit/Lot', 
                        y='P&L', 
                        color='Strategy', 
                        hover_data=['Theta', 'Days Held'],
                        title="Winning Zone: Entry Price vs Profit"
                    ), 
                    use_container_width=True
                )
            else:
                st.info("No expired trades found for analytics.")

    # 4. RULE BOOK
    with tab4:
        st.markdown("""
        # 📖 Trading Constitution
        
        ### 1. 130/160 Strategy
        * **Entry:** Monday.
        * **Debit:** `$3.5k - $4.5k` per lot.
        * **Safety:** Gamma/Theta Ratio should stay `< 0.5`.
        * **Kill Rule:** If Trade is 25 days old & flat, close it.
        
        ### 2. 160/190 Strategy
        * **Entry:** Friday.
        * **Debit:** `~$5.2k` per lot.
        * **Exit:** Hold 40+ Days.
        
        ### 3. M200 Strategy
        * **Entry:** Wednesday.
        * **Debit:** `$7.5k - $8.5k` per lot.
        * **Safety:** Day 14 Check (Green=Roll, Red=Hold).
        """)

else:
    st.info("👋 Upload Active/Expired files to begin.")
