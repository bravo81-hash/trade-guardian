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
    "Drop TODAY'S Active File Here", 
    accept_multiple_files=True,
    help="You only need the latest file. The app calculates age/health from the 'Created At' date inside."
)

# --- CONFIG: STRATEGY RISK LIMITS (Derived from YOUR Data) ---
RISK_CONFIG = {
    '130/160': {'gamma_limit': 1.5, 'vega_limit': 15.0},  # Runs hotter, allow 1.5 ratio
    '160/190': {'gamma_limit': 1.0, 'vega_limit': 10.0},  # More stable, tighter limit
    'M200':    {'gamma_limit': 0.8, 'vega_limit': 8.0}    # Big capital, needs safety
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
                        # Smart Status: If file says Expired but P&L is 0/unrealized, it's Active
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
                        
                        # GRADING LOGIC (Entry Price)
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

                        # --- NEW GREEK ANALYSIS (Calibrated) ---
                        safe_theta = abs(theta) if abs(theta) > 0.1 else 0.1
                        
                        # Ratios
                        expl_ratio = abs(gamma / safe_theta)
                        vol_ratio = abs(vega / safe_theta)
                        
                        # Get Limits for this Strategy
                        limits = RISK_CONFIG.get(strat, {'gamma_limit': 1.0, 'vega_limit': 10.0})
                        
                        # Alerts
                        alerts = []
                        if expl_ratio > limits['gamma_limit']: 
                            alerts.append(f"⚠️ GAMMA SPKE ({expl_ratio:.1f})")
                            
                        # Stale Capital Rule (Time Based)
                        if strat == '130/160' and status == "Active" and 25 <= days_held <= 35 and pnl < 100:
                            alerts.append("💀 STALE (25d Flat)")

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
            
            # Metrics
            c1, c2, c3 = st.columns(3)
            c1.metric("Active Trades", len(active_df))
            c2.metric("Portfolio Theta", f"{active_df['Theta'].sum():.0f}", help="Daily Time Decay")
            c3.metric("Open P&L", f"${active_df['P&L'].sum():,.0f}")
            
            # ALERTS
            critical = active_df[active_df['Alerts'] != ""]
            if not critical.empty:
                st.error(f"🚨 **Risk Alerts ({len(critical)})**")
                for _, r in critical.iterrows():
                    st.write(f"• **{r['Name']}**: {r['Alerts']}")
            
            st.markdown("### 📋 Trade Monitor")
            
            # Columns
            cols = ['Name', 'Strategy', 'Grade', 'Debit/Lot', 'P&L', 'Days Held', 'Gamma/Theta', 'Vega/Theta']
            
            # STYLING
            def color_rows(row):
                # Grade Colors
                if 'A' in str(row['Grade']): return ['color: green; font-weight: bold'] * len(row)
                if 'F' in str(row['Grade']): return ['color: red; font-weight: bold'] * len(row)
                return [''] * len(row)

            def highlight_risk(s):
                # Highlight ONLY the specific risky cell
                return ['background-color: #fff3cd' if 'GAMMA' in str(v) else '' for v in s]

            st.dataframe(
                active_df[cols].style
                .apply(color_rows, axis=1)
                .format({'Debit/Lot': "${:,.0f}", 'P&L': "${:,.0f}", 'Gamma/Theta': "{:.2f}", 'Vega/Theta': "{:.1f}"}),
                use_container_width=True,
                height=600
            )

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
                
                c1, c2 = st.columns(2)
                c1.metric("Grade", row['Grade'])
                c2.metric("Gamma/Theta", f"{row['Gamma/Theta']:.2f}")
                
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
                 # Simple Scatter
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

    # 4. RULE BOOK
    with tab4:
        st.markdown("""
        # 📖 Trading Constitution (Calibrated)
        
        ### 1. 130/160 Strategy
        * **Target:** Monday Entry. `$3.5k - $4.5k` Debit.
        * **Risk Limit:** `Gamma/Theta < 1.5` (Your strategy runs hot, 1.5 is normal).
        
        ### 2. 160/190 Strategy
        * **Target:** Friday Entry. `~$5.2k` Debit.
        * **Risk Limit:** `Gamma/Theta < 1.0`.
        
        ### 3. M200 Strategy
        * **Target:** Wednesday Entry. `$7.5k - $8.5k` Debit.
        * **Risk Limit:** `Gamma/Theta < 0.8` (Needs stability).
        """)

else:
    st.info("👋 Upload TODAY'S Active file to see health.")
