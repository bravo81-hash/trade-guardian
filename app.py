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

# Debug Toggle
show_debug = st.sidebar.checkbox("Show Debug Logs", value=False)

# --- HELPER FUNCTIONS ---
def get_strategy(group_name):
    g = str(group_name).upper()
    if "M200" in g: return "M200"
    elif "160/190" in g: return "160/190"
    elif "130/160" in g: return "130/160"
    return "Other"

# --- CORE PROCESSING ENGINE ---
@st.cache_data
def process_data(files):
    all_data = []
    debug_logs = []
    
    for f in files:
        try:
            filename = f.name.lower()
            df = None
            
            # PATH A: EXCEL FILES (.xlsx)
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                # Read raw excel
                df_raw = pd.read_excel(f, header=None, engine='openpyxl')
                
                # Smart Header Search
                header_idx = -1
                for i, row in df_raw.head(20).iterrows():
                    # Convert row to string to search for keywords
                    row_str = " ".join(row.astype(str).values)
                    if "Name" in row_str and "Total Return" in row_str:
                        header_idx = i
                        break
                
                if header_idx != -1:
                    df = df_raw.iloc[header_idx+1:].copy()
                    df.columns = df_raw.iloc[header_idx]
                    debug_logs.append(f"✅ Found Excel header at row {header_idx} in {f.name}")
                else:
                    debug_logs.append(f"❌ Could not find header in Excel file {f.name}")
                    continue

            # PATH B: CSV FILES (.csv)
            else:
                content = f.getvalue().decode("utf-8")
                lines = content.split('\n')
                header_idx = 0
                for i, line in enumerate(lines[:20]):
                    if "Name" in line and "Total Return" in line:
                        header_idx = i
                        break
                df = pd.read_csv(io.StringIO(content), skiprows=header_idx)
                debug_logs.append(f"✅ Found CSV header at row {header_idx} in {f.name}")
            
            # --- COMMON PARSING LOGIC ---
            if df is not None:
                trades_found = 0
                for _, row in df.iterrows():
                    # Robust Date Check
                    created_val = row.get('Created At', '')
                    is_valid_date = False
                    
                    # Check if it's already a datetime object (Excel) or a string (CSV)
                    if isinstance(created_val, (pd.Timestamp, datetime)):
                        is_valid_date = True
                        start_dt = created_val
                    elif isinstance(created_val, str) and len(created_val) > 8 and ':' in created_val:
                        is_valid_date = True
                        try:
                            start_dt = pd.to_datetime(created_val)
                        except:
                            is_valid_date = False

                    if is_valid_date:
                        trades_found += 1
                        name = row.get('Name', 'Unknown')
                        group = str(row.get('Group', ''))
                        strat = get_strategy(group)
                        
                        # Clean Numbers
                        def clean(x):
                            try: return float(str(x).replace('$','').replace(',',''))
                            except: return 0.0
                            
                        pnl = clean(row.get('Total Return $', 0))
                        debit = abs(clean(row.get('Net Debit/Credit', 0)))
                        theta = clean(row.get('Theta', 0))
                        
                        # Status Logic
                        status = "Active" if "active" in filename else "Expired"
                        # Fallback: Expired file but P&L is 0 often means active in TWS exports
                        if status == "Expired" and pnl == 0: status = "Active"
                        
                        if status == "Expired":
                            end_val = row.get('Expiration', datetime.now())
                            try: end_dt = pd.to_datetime(end_val)
                            except: end_dt = datetime.now()
                        else:
                            end_dt = datetime.now()
                            
                        days_held = (end_dt - start_dt).days
                        if days_held < 0: days_held = 0

                        # Lot Size Logic (Heuristic)
                        lot_size = 1
                        if strat == '130/160':
                            if debit > 6000: lot_size = 2
                            elif debit > 10000: lot_size = 3
                        elif strat == '160/190':
                            if debit > 8000: lot_size = 2
                        elif strat == 'M200':
                            if debit > 12000: lot_size = 2
                            
                        debit_lot = debit / max(1, lot_size)
                        
                        # Grade Logic
                        grade = "C"
                        alert = ""
                        
                        if strat == '130/160':
                            if debit_lot > 4800: grade = "F"
                            elif 3500 <= debit_lot <= 4500: grade = "A+"
                            else: grade = "B"
                            if status == "Active" and 20 <= days_held <= 30 and pnl < 100:
                                alert = "💀 KILL ZONE"
                        elif strat == '160/190':
                            if 4800 <= debit_lot <= 5500: grade = "A"
                            else: grade = "C"
                            if status == "Active" and days_held < 30 and pnl < 0:
                                alert = "⏳ Cooking"
                        elif strat == 'M200':
                            if 7500 <= debit_lot <= 8500: grade = "A"
                            else: grade = "B"
                            if status == "Active" and 13 <= days_held <= 16:
                                if pnl > 200: alert = "💰 Day 14 Check"
                                else: alert = "🔒 Hold to Day 60"

                        all_data.append({
                            "Name": name, "Strategy": strat, "Status": status,
                            "P&L": pnl, "Debit/Lot": debit_lot, "Grade": grade,
                            "Alert": alert, "Days Held": days_held
                        })
                
                debug_logs.append(f"ℹ️ Extracted {trades_found} trades from {f.name}")

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

if uploaded_files:
    df, logs = process_data(uploaded_files)
    
    # Debug Panel
    if show_debug:
        st.sidebar.divider()
        st.sidebar.write("Debug Logs:")
        for log in logs: st.sidebar.caption(log)
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Active Health", "🧪 Trade Validator", "📈 Analytics"])
    
    with tab1:
        if not df.empty:
            active_df = df[df['Status'] == 'Active'].copy()
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Active Trades", len(active_df))
            c2.metric("Open P&L", f"${active_df['P&L'].sum():,.2f}")
            alerts = active_df[active_df['Alert'] != ""]
            c4.metric("Alerts", len(alerts), delta_color="inverse")
            
            if not alerts.empty:
                st.error(f"Action Required on {len(alerts)} Trades")
                for _, r in alerts.iterrows():
                    st.write(f"**{r['Name']}**: {r['Alert']}")
            
            def color_grade(val):
                color = 'red' if 'F' in val or 'D' in val else 'green' if 'A' in val else 'orange'
                return f'color: {color}; font-weight: bold'

            st.dataframe(active_df[['Name', 'Strategy', 'Debit/Lot', 'Grade', 'P&L']].style.applymap(color_grade, subset=['Grade']), use_container_width=True)
        else:
            st.info("No trades parsed. Check Debug Logs in sidebar.")

    with tab3:
        if not df.empty:
            st.subheader("Strategy Performance")
            summ = df.groupby('Strategy').agg({'P&L': 'sum', 'Name': 'count'}).reset_index()
            c1, c2 = st.columns(2)
            with c1: st.plotly_chart(px.bar(summ, x='Strategy', y='P&L', color='Strategy'), use_container_width=True)
            with c2: st.plotly_chart(px.scatter(df, x='Debit/Lot', y='P&L', color='Strategy', title="Debit Sweet Spots"), use_container_width=True)

    with tab2:
        st.markdown("### Pre-Flight Check")
        model_file = st.file_uploader("Upload Model File (Excel/CSV)", key="mod")
        if model_file:
            m_df, _ = process_data([model_file])
            if not m_df.empty:
                row = m_df.iloc[0]
                st.metric(f"Grade: {row['Grade']}", f"${row['Debit/Lot']:,.0f}")
                if "A" in row['Grade']: st.success("✅ GO")
                elif "F" in row['Grade']: st.error("⛔ NO GO")
                else: st.warning("⚠️ CAUTION")
else:
    st.info("👋 Upload Active/Expired files in the sidebar.")
