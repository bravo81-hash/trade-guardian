import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import io
import plotly.express as px  # Using Plotly for interactive charts

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")

st.title("🛡️ Allantis Trade Guardian")

# --- TABS ---
tab1, tab2, tab3 = st.tabs(["📊 Active Health", "🧪 Model Trade Lab", "📈 Deep Analytics"])

# --- SIDEBAR ---
st.sidebar.header("📂 Data Import")
uploaded_files = st.sidebar.file_uploader(
    "1. Drop ALL History & Active Files Here", 
    accept_multiple_files=True, 
    type="csv",
    help="You can drop multiple files. The app will merge them and remove duplicates automatically."
)

# --- HELPER FUNCTIONS ---
def clean_num(x):
    try: return float(str(x).replace('$','').replace(',',''))
    except: return 0.0

def get_strategy(group_name):
    g = str(group_name).upper()
    if "M200" in g: return "M200"
    elif "160/190" in g: return "160/190"
    elif "130/160" in g: return "130/160"
    return "Other"

# --- CORE PROCESSING ENGINE ---
@st.cache_data # Caches the data so it doesn't reload on every click
def process_data(files):
    all_data = []
    
    for f in files:
        try:
            content = f.getvalue().decode("utf-8")
            if content.startswith("[source"):
                df = pd.read_csv(io.StringIO(content), skiprows=1)
            else:
                df = pd.read_csv(io.StringIO(content))
            
            # Auto-detect Status based on filename
            fname = f.name.lower()
            file_status = "Active" if "active" in fname else "Expired"
            
            for _, row in df.iterrows():
                # Parent Row Check
                if len(str(row.get('Created At', ''))) > 10 and ':' in str(row.get('Created At', '')):
                    
                    trade_name = row.get('Name', 'Unknown')
                    group = str(row.get('Group', ''))
                    strat = get_strategy(group)
                    
                    # Metrics
                    pnl = clean_num(row.get('Total Return $', 0))
                    debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                    theta = clean_num(row.get('Theta', 0))
                    
                    # Dates
                    start_dt = pd.to_datetime(row.get('Created At'))
                    if file_status == "Expired":
                        end_dt = pd.to_datetime(row.get('Expiration', datetime.now()))
                        # If P&L is realized, force status to Expired even if file says Active (rare edge case)
                        final_status = "Expired"
                    else:
                        end_dt = datetime.now()
                        final_status = "Active"
                        
                    days_held = (end_dt - start_dt).days
                    if days_held < 0: days_held = 0
                    
                    # Day of Week
                    entry_day = start_dt.day_name()
                    
                    # Lot Size Logic
                    lot_size = 1
                    if strat == '130/160':
                        if debit > 6000: lot_size = 2
                        elif debit > 10000: lot_size = 3
                    elif strat == '160/190':
                        if debit > 8000: lot_size = 2
                    elif strat == 'M200':
                        if debit > 12000: lot_size = 2
                        
                    debit_lot = debit / max(1, lot_size)
                    theta_lot = theta / max(1, lot_size)
                    
                    # GRADING (The Rule Book)
                    grade = "C"
                    alert = ""
                    
                    if strat == '130/160':
                        if debit_lot > 4800: grade = "F"
                        elif 3500 <= debit_lot <= 4500: grade = "A+"
                        else: grade = "B"
                        
                        if final_status == "Active" and 20 <= days_held <= 30 and pnl < 100:
                            alert = "💀 KILL ZONE (Stale Capital)"

                    elif strat == '160/190':
                        if 4800 <= debit_lot <= 5500: grade = "A"
                        elif debit_lot < 4800: grade = "B+"
                        else: grade = "C"
                        
                        if final_status == "Active" and days_held < 30 and pnl < 0:
                            alert = "⏳ Cooking (Hold)"

                    elif strat == 'M200':
                        if 7500 <= debit_lot <= 8500: grade = "A"
                        elif debit_lot > 9000: grade = "D"
                        else: grade = "B"
                        
                        if final_status == "Active" and 13 <= days_held <= 16:
                            if pnl > 200: alert = "💰 Day 14 Check: Consider Exit"
                            else: alert = "🔒 Hold to Day 60"
                            
                    all_data.append({
                        "Name": trade_name,
                        "Strategy": strat,
                        "Status": final_status,
                        "P&L": pnl,
                        "Debit": debit,
                        "Debit/Lot": debit_lot,
                        "Days Held": days_held,
                        "Grade": grade,
                        "Alert": alert,
                        "Entry Day": entry_day
                    })
                    
        except: pass
        
    df = pd.DataFrame(all_data)
    
    # DEDUPLICATION LOGIC
    # If same trade exists, keep the one that is 'Expired' (finalized) or the latest 'Active'
    if not df.empty:
        # Sort so 'Expired' comes last (to be kept)
        df['Status_Rank'] = df['Status'].apply(lambda x: 1 if x == 'Expired' else 0)
        df = df.sort_values(by=['Name', 'Status_Rank'], ascending=[True, True])
        df = df.drop_duplicates(subset=['Name', 'Strategy'], keep='last')
        
    return df

# --- LOAD DATA ---
if uploaded_files:
    df = process_data(uploaded_files)
else:
    df = pd.DataFrame()

# ==========================================
# TAB 1: ACTIVE PORTFOLIO HEALTH
# ==========================================
with tab1:
    if not df.empty:
        active_df = df[df['Status'] == 'Active'].copy()
        
        # Top Metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Active Trades", len(active_df))
        c2.metric("Open P&L", f"${active_df['P&L'].sum():,.2f}")
        c3.metric("Avg P&L / Trade", f"${active_df['P&L'].mean():,.2f}")
        alerts = active_df[active_df['Alert'] != ""]
        c4.metric("Alerts", len(alerts), delta_color="inverse")
        
        # Alerts Banner
        if not alerts.empty:
            st.error(f"⚠️ **Action Required on {len(alerts)} Trades:**")
            for _, row in alerts.iterrows():
                st.write(f"- **{row['Name']}**: {row['Alert']} (Held {row['Days Held']}d)")
        
        # Detailed Table
        st.subheader("📋 Active Trade Report Card")
        def color_grade(val):
            color = 'red' if 'F' in val or 'D' in val else 'green' if 'A' in val else 'orange'
            return f'color: {color}; font-weight: bold'

        display_cols = ['Name', 'Strategy', 'Debit/Lot', 'Grade', 'Days Held', 'P&L']
        st.dataframe(active_df[display_cols].style.applymap(color_grade, subset=['Grade']), use_container_width=True)
    else:
        st.info("👈 Upload files to begin.")

# ==========================================
# TAB 2: MODEL TRADE LAB (PRE-FLIGHT)
# ==========================================
with tab2:
    st.markdown("### 🧪 Trade Validator")
    st.markdown("Drop a hypothetical trade file here to grade it before execution.")
    model_file = st.file_uploader("Upload Model CSV", type="csv", key="model_uploader")
    
    if model_file:
        model_df = process_data([model_file])
        if not model_df.empty:
            for _, row in model_df.iterrows():
                st.divider()
                st.subheader(f"Analyzing: {row['Name']}")
                
                # Verdict
                color = "green" if "A" in row['Grade'] else "red" if "F" in row['Grade'] else "orange"
                st.markdown(f":{color}[**Grade: {row['Grade']}**]")
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Strategy", row['Strategy'])
                c2.metric("Debit Per Lot", f"${row['Debit/Lot']:,.2f}")
                c3.metric("Projected Outcome", "High Prob" if "A" in row['Grade'] else "Low Prob")
                
                if "A" in row['Grade']:
                    st.success("✅ **GREEN LIGHT:** This trade structure matches your historical winners.")
                elif "F" in row['Grade']:
                    st.error("⛔ **STOP:** This trade is historically overpriced and has a 100% failure rate.")
                else:
                    st.warning("⚠️ **CAUTION:** Structure is acceptable but not optimal.")

# ==========================================
# TAB 3: ANALYTICS & INSIGHTS (NEW)
# ==========================================
with tab3:
    if not df.empty:
        st.subheader("📈 Performance Dashboard")
        
        # 1. Strategy Comparison
        st.markdown("#### Strategy Performance")
        strat_summary = df.groupby('Strategy').agg({
            'P&L': ['sum', 'mean', 'count'],
            'Debit': 'mean'
        }).reset_index()
        strat_summary.columns = ['Strategy', 'Total P&L', 'Avg P&L', 'Trades', 'Avg Debit']
        st.dataframe(strat_summary, use_container_width=True)
        
        # 2. Charts
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("**Profit by Strategy**")
            fig_bar = px.bar(strat_summary, x='Strategy', y='Total P&L', color='Strategy', text_auto=True)
            st.plotly_chart(fig_bar, use_container_width=True)
            
        with c2:
            st.markdown("**Day of Week Edge**")
            # Group by Entry Day
            day_stats = df.groupby('Entry Day')['P&L'].mean().reset_index()
            # Sort days correctly
            days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            day_stats['Entry Day'] = pd.Categorical(day_stats['Entry Day'], categories=days_order, ordered=True)
            day_stats = day_stats.sort_values('Entry Day')
            
            fig_line = px.line(day_stats, x='Entry Day', y='P&L', markers=True, title="Avg P&L per Entry Day")
            st.plotly_chart(fig_line, use_container_width=True)
            
        # 3. Scatter: Debit vs Result
        st.markdown("#### The 'Sweet Spot' Analysis (Debit Paid vs P&L)")
        fig_scatter = px.scatter(
            df, x="Debit/Lot", y="P&L", color="Strategy", 
            hover_data=["Name"], 
            title="Are you paying too much? (Cluster Analysis)"
        )
        # Add Reference Lines for your rules
        fig_scatter.add_vline(x=4800, line_dash="dash", line_color="red", annotation_text="130/160 Danger Zone")
        st.plotly_chart(fig_scatter, use_container_width=True)
        
    else:
        st.info("Upload data to generate analytics.")