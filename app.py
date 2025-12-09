import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import io
import plotly.express as px

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian", layout="wide", page_icon="🛡️")
st.title("🛡️ Allantis Trade Guardian")

# --- SIDEBAR ---
st.sidebar.header("📂 Data Import")
uploaded_files = st.sidebar.file_uploader(
    "1. Drop ALL History & Active Files Here", 
    accept_multiple_files=True, 
    help="You can drop multiple files. The app will merge them automatically."
)

# Debug Toggle
show_debug = st.sidebar.checkbox("Show Debug Data", value=False, help="Check this if your dashboard is blank to see why.")

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

# --- CORE PROCESSING ENGINE (SMART PARSER) ---
@st.cache_data
def process_data(files):
    all_data = []
    debug_logs = []
    
    for f in files:
        try:
            content = f.getvalue().decode("utf-8")
            
            # --- SMART HEADER DETECTION ---
            # We look for the line that contains specific TWS columns
            lines = content.split('\n')
            header_row = 0
            found_header = False
            
            for i, line in enumerate(lines[:20]): # Scan first 20 lines
                if "Name" in line and "Total Return" in line:
                    header_row = i
                    found_header = True
                    break
            
            if not found_header:
                debug_logs.append(f"❌ Could not find header in {f.name}")
                continue
                
            # Read CSV from the detected header row
            df = pd.read_csv(io.StringIO(content), skiprows=header_row)
            
            # Auto-detect Status based on filename
            fname = f.name.lower()
            file_status = "Active" if "active" in fname else "Expired"
            
            trades_found = 0
            for _, row in df.iterrows():
                # Parent Row Check: Must have a valid Date in 'Created At'
                created_at = str(row.get('Created At', ''))
                
                # Robust date check
                if len(created_at) > 8 and ('-' in created_at or '/' in created_at) and ':' in created_at:
                    
                    trades_found += 1
                    trade_name = row.get('Name', 'Unknown')
                    group = str(row.get('Group', ''))
                    strat = get_strategy(group)
                    
                    # Metrics
                    pnl = clean_num(row.get('Total Return $', 0))
                    debit = abs(clean_num(row.get('Net Debit/Credit', 0)))
                    theta = clean_num(row.get('Theta', 0))
                    
                    # Dates
                    try:
                        start_dt = pd.to_datetime(created_at)
                    except:
                        continue # Skip malformed dates

                    if file_status == "Expired":
                        end_dt = pd.to_datetime(row.get('Expiration', datetime.now()))
                        final_status = "Expired"
                    else:
                        end_dt = datetime.now()
                        final_status = "Active"
                        
                    days_held = (end_dt - start_dt).days
                    if days_held < 0: days_held = 0
                    
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
                    
                    # GRADING LOGIC
                    grade = "C"
                    alert = ""
                    
                    if strat == '130/160':
                        if debit_lot > 4800: grade = "F"
                        elif 3500 <= debit_lot <= 4500: grade = "A+"
                        else: grade = "B"
                        if final_status == "Active" and 20 <= days_held <= 30 and pnl < 100:
                            alert = "💀 KILL ZONE"

                    elif strat == '160/190':
                        if 4800 <= debit_lot <= 5500: grade = "A"
                        elif debit_lot < 4800: grade = "B+"
                        else: grade = "C"
                        if final_status == "Active" and days_held < 30 and pnl < 0:
                            alert = "⏳ Cooking"

                    elif strat == 'M200':
                        if 7500 <= debit_lot <= 8500: grade = "A"
                        elif debit_lot > 9000: grade = "D"
                        else: grade = "B"
                        if final_status == "Active" and 13 <= days_held <= 16:
                            if pnl > 200: alert = "💰 Day 14 Check"
                            else: alert = "🔒 Hold to Day 60"
                            
                    all_data.append({
                        "Name": trade_name,
                        "Strategy": strat,
                        "Status": final_status,
                        "P&L": pnl,
                        "Debit/Lot": debit_lot,
                        "Days Held": days_held,
                        "Grade": grade,
                        "Alert": alert,
                        "Entry Day": start_dt.day_name()
                    })
            
            debug_logs.append(f"✅ Parsed {f.name}: Found {trades_found} trades.")
                    
        except Exception as e:
            debug_logs.append(f"❌ Error parsing {f.name}: {str(e)}")
        
    df = pd.DataFrame(all_data)
    
    # Deduplication
    if not df.empty:
        df['Status_Rank'] = df['Status'].apply(lambda x: 1 if x == 'Expired' else 0)
        df = df.sort_values(by=['Name', 'Status_Rank'], ascending=[True, True])
        df = df.drop_duplicates(subset=['Name', 'Strategy'], keep='last')
        
    return df, debug_logs

# --- MAIN APP LOGIC ---

# 1. Process Files
if uploaded_files:
    df, logs = process_data(uploaded_files)
    
    # Debug Area
    if show_debug:
        st.sidebar.divider()
        st.sidebar.subheader("🔍 Debug Logs")
        for log in logs:
            st.sidebar.caption(log)
        if not df.empty:
            st.sidebar.dataframe(df.head())
        else:
            st.sidebar.warning("DataFrame is empty.")

    # 2. Render Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Active Health", "🧪 Model Trade Lab", "📈 Deep Analytics"])
    
    with tab1:
        if not df.empty:
            active_df = df[df['Status'] == 'Active'].copy()
            
            # Metrics
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Active Trades", len(active_df))
            c2.metric("Open P&L", f"${active_df['P&L'].sum():,.2f}")
            if len(active_df) > 0:
                c3.metric("Avg P&L", f"${active_df['P&L'].mean():,.2f}")
            
            alerts = active_df[active_df['Alert'] != ""]
            c4.metric("Alerts", len(alerts), delta_color="inverse")
            
            if not alerts.empty:
                st.error(f"Action Required: {len(alerts)} Trades")
                for _, r in alerts.iterrows():
                    st.write(f"**{r['Name']}**: {r['Alert']}")
            
            # Table
            st.subheader("Active Trade Grades")
            def color_grade(val):
                color = 'red' if 'F' in val or 'D' in val else 'green' if 'A' in val else 'orange'
                return f'color: {color}; font-weight: bold'

            display_cols = ['Name', 'Strategy', 'Debit/Lot', 'Grade', 'Days Held', 'P&L']
            st.dataframe(active_df[display_cols].style.applymap(color_grade, subset=['Grade']), use_container_width=True)
            
        else:
            st.warning("No trades found. Check 'Show Debug Data' in sidebar.")

    with tab3:
        if not df.empty:
            st.subheader("Analytics")
            st.markdown("#### Strategy Performance")
            strat_summary = df.groupby('Strategy').agg({'P&L': ['sum', 'count']}).reset_index()
            strat_summary.columns = ['Strategy', 'Total P&L', 'Trades']
            st.dataframe(strat_summary, use_container_width=True)
            
            c1, c2 = st.columns(2)
            with c1:
                fig = px.bar(strat_summary, x='Strategy', y='Total P&L', color='Strategy', title="Total P&L")
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                fig2 = px.scatter(df, x="Debit/Lot", y="P&L", color="Strategy", title="Debit vs P&L Sweet Spots")
                fig2.add_vline(x=4800, line_dash="dash", line_color="red")
                st.plotly_chart(fig2, use_container_width=True)

    with tab2:
        st.markdown("### Pre-Flight Check")
        model_file = st.file_uploader("Upload Model CSV", key="model")
        if model_file:
            m_df, _ = process_data([model_file])
            if not m_df.empty:
                for _, row in m_df.iterrows():
                    st.divider()
                    st.subheader(f"{row['Name']}")
                    color = "green" if "A" in row['Grade'] else "red" if "F" in row['Grade'] else "orange"
                    st.markdown(f":{color}[**Grade: {row['Grade']}**]")
                    st.write(f"Strategy: {row['Strategy']} | Debit/Lot: ${row['Debit/Lot']:,.2f}")
else:
    st.info("👋 Upload your files in the sidebar.")
    if show_debug:
        st.sidebar.warning("Waiting for file upload...")
