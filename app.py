import streamlit as st
import pandas as pd
import numpy as np
import io
import plotly.express as px
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian Pro", layout="wide", page_icon="🛡️")
st.title("🛡️ Allantis Trade Guardian: Quant Edition")

# --- SIDEBAR ---
st.sidebar.header("📂 Data Import")
uploaded_files = st.sidebar.file_uploader(
    "Drop Active/Expired Files (Excel/CSV)", 
    accept_multiple_files=True,
    help="Drop multiple active files from different dates to see trends (experimental)."
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

def analyze_greeks(theta, gamma, vega, delta):
    issues = []
    
    # 1. EXPLOSION RATIO (Gamma Risk relative to Income)
    # Avoid div by zero
    if abs(theta) < 0.1: theta = 0.1
    
    explosion_ratio = abs(gamma / theta)
    if explosion_ratio > 0.5:
        issues.append("⚠️ HIGH GAMMA RISK: Price moves hurt 2x more than Time helps.")
        
    # 2. IV SAFETY NET (Vega Exposure)
    vol_ratio = abs(vega / theta)
    if vol_ratio > 10.0:
        issues.append("⚠️ HIGH VEGA: You are over-exposed to Volatility spikes.")
        
    # 3. DELTA DRIFT (Directional Stress)
    # If Delta is larger than 50% of Theta, the trade is too directional
    if abs(delta) > abs(theta) * 0.5:
        issues.append("⚠️ DELTA DRIFT: Trade has become too directional.")
        
    return issues, explosion_ratio, vol_ratio

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
                        
                        # THE GREEKS
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

                        # Lot Size
                        lot_size = 1
                        if strat == '130/160' and debit > 6000: lot_size = 2
                        elif strat == '130/160' and debit > 10000: lot_size = 3
                        elif strat == '160/190' and debit > 8000: lot_size = 2
                        elif strat == 'M200' and debit > 12000: lot_size = 2
                            
                        debit_lot = debit / max(1, lot_size)
                        
                        # GRADING & GREEK ANALYSIS
                        greek_issues, exp_ratio, vol_ratio = analyze_greeks(theta, gamma, vega, delta)
                        
                        grade = "C"
                        reason = "Standard"
                        
                        if strat == '130/160':
                            if debit_lot > 4800: grade = "F"; reason = "⛔ Overpriced"
                            elif 3500 <= debit_lot <= 4500: grade = "A+"; reason = "✅ Sweet Spot"
                            else: grade = "B"
                        elif strat == '160/190':
                            if 4800 <= debit_lot <= 5500: grade = "A"; reason = "✅ Ideal"
                            else: grade = "C"; reason = "⚠️ Check Price"
                        elif strat == 'M200':
                            if 7500 <= debit_lot <= 8500: grade = "A"; reason = "✅ Perfect"
                            else: grade = "B"; reason = "⚠️ Variance"
                        
                        # Combine Alerts
                        alerts = []
                        if greek_issues: alerts.extend(greek_issues)
                        
                        # Timeline Alert
                        if strat == '130/160' and status == "Active" and 20 <= days_held <= 30 and pnl < 100:
                            alerts.append("💀 KILL ZONE (Stale Capital)")

                        all_data.append({
                            "Name": name, "Strategy": strat, "Status": status,
                            "P&L": pnl, "Debit/Lot": debit_lot, "Grade": grade,
                            "Reason": reason, "Alerts": " | ".join(alerts), 
                            "Days Held": days_held,
                            "Theta": theta, "Delta": delta, "Gamma": gamma, "Vega": vega,
                            "Explosion Ratio": exp_ratio, "Vol Ratio": vol_ratio
                        })
                
        except: pass
            
    df = pd.DataFrame(all_data)
    if not df.empty:
        # Sort so we see most recent / relevant first
        df = df.sort_values(by=['Name', 'Days Held'], ascending=[True, False])
        # Note: We do NOT drop duplicates here if we want to see trends, 
        # but for the main dashboard, we usually want the latest snapshot.
        # Let's create a 'Latest' view.
        df['Latest'] = ~df.duplicated(subset=['Name', 'Strategy'], keep='first')
        
    return df

# --- MAIN APP ---
if uploaded_files:
    df = process_data(uploaded_files)
    
    # TABS
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Active Health (Quant)", "🧪 Trade Validator", "📈 Deep Analytics", "📖 Rule Book"])
    
    # 1. ACTIVE HEALTH
    with tab1:
        if not df.empty:
            # Filter for Latest Active Trades
            active_df = df[(df['Status'] == 'Active') & (df['Latest'] == True)].copy()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Active Trades", len(active_df))
            c2.metric("Portfolio Delta", f"{active_df['Delta'].sum():.2f}", help="Total directional exposure")
            c3.metric("Portfolio Theta", f"{active_df['Theta'].sum():.0f}", help="Daily time decay collected")
            
            # Show Alerts
            problem_trades = active_df[active_df['Alerts'] != ""]
            if not problem_trades.empty:
                st.error(f"🚨 **Risk Alerts ({len(problem_trades)})**")
                for _, r in problem_trades.iterrows():
                    st.write(f"• **{r['Name']}**: {r['Alerts']}")
            
            st.markdown("### 🔬 Deep Greek Analysis")
            
            # Prepare Table Data
            view = active_df[['Name', 'Strategy', 'Grade', 'P&L', 'Debit/Lot', 'Theta', 'Explosion Ratio', 'Vol Ratio', 'Alerts']]
            
            def color_rows(row):
                styles = []
                # Grade Coloring
                base_color = ''
                if 'A' in str(row['Grade']): base_color = 'background-color: #d4edda; color: #155724'
                elif 'F' in str(row['Grade']): base_color = 'background-color: #f8d7da; color: #721c24'
                
                # Risk Coloring (Overrides Grade)
                if 'HIGH GAMMA' in str(row['Alerts']): base_color = 'background-color: #fff3cd; color: #856404' # Yellow warning
                
                return [base_color] * len(row)

            st.dataframe(view.style.apply(color_rows, axis=1), use_container_width=True, height=600)
            
            st.caption("""
            **Columns Key:**
            * **Explosion Ratio:** (Gamma/Theta). If > 0.5, price moves hurt more than time helps.
            * **Vol Ratio:** (Vega/Theta). If > 10.0, you are betting on IV drop, not Time.
            """)

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
                c2.metric("Explosion Ratio", f"{row['Explosion Ratio']:.2f}")
                c3.metric("Vol Ratio", f"{row['Vol Ratio']:.1f}")
                
                if "A" in row['Grade'] and row['Explosion Ratio'] < 0.3:
                    st.success("✅ **GREEN LIGHT**: Price & Structure are optimal.")
                elif row['Explosion Ratio'] > 0.5:
                    st.warning("⚠️ **CAUTION**: Gamma risk is high. Ensure you are comfortable with price swings.")
                elif "F" in row['Grade']:
                    st.error("⛔ **REJECT**: Overpriced structure.")

    # 3. ANALYTICS
    with tab3:
        if not df.empty:
            st.subheader("📈 Portfolio Intelligence")
            expired_df = df[df['Status'] == 'Expired']
            if not expired_df.empty:
                st.plotly_chart(px.scatter(expired_df, x='Debit/Lot', y='P&L', color='Strategy', size='Theta', title="Win Profile: Price vs Decay"), use_container_width=True)

    # 4. RULE BOOK
    with tab4:
        st.markdown("""
        # 📖 Trading Rules & Quant Limits
        
        ### 1. 130/160 Strategy
        * **Debit:** `$3.5k - $4.5k` per lot.
        * **Gamma Limit:** `Explosion Ratio < 0.4`.
        
        ### 2. 160/190 Strategy
        * **Debit:** `~$5.2k` per lot.
        * **Exit:** Hold 40+ Days.
        
        ### 3. M200 Strategy
        * **Debit:** `$7.5k - $8.5k` per lot.
        
        ### 4. Greek Safety Rules
        * **Explosion Ratio (Gamma/Theta):** Must be **< 0.5**.
            * *Meaning:* If > 0.5, a 1% market move hurts your P&L more than 1 day of theta helps it.
        * **Vol Ratio (Vega/Theta):** Must be **< 10.0**.
            * *Meaning:* If > 10, an IV spike can wipe out weeks of progress.
        * **Delta Drift:** Net Delta should not exceed **50% of Theta**.
        """)

else:
    st.info("👋 Upload Active/Expired files to begin Quant Analysis.")
