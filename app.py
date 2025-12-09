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

def get_grade_and_reason(strat, debit_lot, status, days_held, pnl):
    grade = "C"
    reason = "Standard Entry"
    alert = ""
    
    if strat == '130/160':
        if debit_lot > 4800:
            grade = "F"
            reason = "⛔ OVERPRICED: >$4,800 has 100% failure rate."
        elif 3500 <= debit_lot <= 4500:
            grade = "A+"
            reason = "✅ SWEET SPOT: Ideal pricing ($3.5k-$4.5k)."
        else:
            grade = "B"
            reason = "⚠️ ACCEPTABLE: Slightly outside ideal range."
            
        if status == "Active" and 20 <= days_held <= 30 and pnl < 100:
            alert = "💀 KILL ZONE (Stale Capital)"

    elif strat == '160/190':
        if 4800 <= debit_lot <= 5500:
            grade = "A"
            reason = "✅ IDEAL: Matches historical winners."
        elif debit_lot < 4800:
            grade = "B+"
            reason = "✅ GOOD VALUE: Cheap entry."
        else:
            grade = "C"
            reason = "⚠️ EXPENSIVE: >$5,500 reduces ROI."
            
        if status == "Active" and days_held < 30 and pnl < 0:
            alert = "⏳ COOKING: Expect flat P&L until Day 30."

    elif strat == 'M200':
        if 7500 <= debit_lot <= 8500:
            grade = "A"
            reason = "✅ PERFECT: Structural match."
        elif debit_lot > 9000:
            grade = "D"
            reason = "⛔ EXPENSIVE: Drags down ROI."
        else:
            grade = "B"
            reason = "⚠️ VARIANCE: Acceptable."
            
        if status == "Active" and 13 <= days_held <= 16:
            if pnl > 200: alert = "💰 DAY 14 CHECK: Consider Exit"
            else: alert = "🔒 HOLD: Commit to Day 60."

    return grade, reason, alert

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
                else:
                    debug_logs.append(f"❌ No header found in {f.name}")
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
            
            # --- PARSING LOGIC ---
            if df is not None:
                for _, row in df.iterrows():
                    # Robust Date Check
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
                        
                        def clean(x):
                            try: return float(str(x).replace('$','').replace(',',''))
                            except: return 0.0
                            
                        pnl = clean(row.get('Total Return $', 0))
                        debit = abs(clean(row.get('Net Debit/Credit', 0)))
                        
                        # Status Logic
                        status = "Active" if "active" in filename else "Expired"
                        if status == "Expired" and pnl == 0: status = "Active"
                        
                        if status == "Expired":
                            end_val = row.get('Expiration', datetime.now())
                            try: end_dt = pd.to_datetime(end_val)
                            except: end_dt = datetime.now()
                        else:
                            end_dt = datetime.now()
                            
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
                        
                        # Get Grade
                        grade, reason, alert = get_grade_and_reason(strat, debit_lot, status, days_held, pnl)

                        all_data.append({
                            "Name": name, "Strategy": strat, "Status": status,
                            "P&L": pnl, "Debit/Lot": debit_lot, "Grade": grade,
                            "Reason": reason, "Alert": alert, "Days Held": days_held
                        })
                
        except Exception as e:
            debug_logs.append(f"❌ Error parsing {f.name}: {str(e)}")
            
    df = pd.DataFrame(all_data)
    if not df.empty:
        df['Status_Rank'] = df['Status'].apply(lambda x: 1 if x == 'Expired' else 0)
        df = df.sort_values(by=['Name', 'Status_Rank'], ascending=[True, True])
        df = df.drop_duplicates(subset=['Name', 'Strategy'], keep='last')
        
    return df, debug_logs

# --- MAIN APP LOGIC ---

if uploaded_files:
    df, logs = process_data(uploaded_files)
    
    # TABS
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Active Health", "🧪 Trade Validator", "📈 Deep Analytics", "📖 Rule Book"])
    
    # ----------------------------------------------------
    # TAB 1: ACTIVE HEALTH
    # ----------------------------------------------------
    with tab1:
        if not df.empty:
            active_df = df[df['Status'] == 'Active'].copy()
            
            # Key Metrics
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Running Trades", len(active_df))
            c2.metric("Open P&L", f"${active_df['P&L'].sum():,.2f}")
            
            alerts = active_df[active_df['Alert'] != ""]
            c4.metric("Action Alerts", len(alerts), delta_color="inverse")
            
            if not alerts.empty:
                st.error(f"🚨 **Action Required ({len(alerts)}):**")
                for _, r in alerts.iterrows():
                    st.write(f"• **{r['Name']}**: {r['Alert']}")
            
            st.markdown("### 📋 Active Portfolio Report")
            
            for index, row in active_df.iterrows():
                with st.expander(f"{row['Name']} | {row['Grade']} | P&L: ${row['P&L']:,.0f}"):
                    c1, c2, c3 = st.columns(3)
                    c1.write(f"**Strategy:** {row['Strategy']}")
                    c2.write(f"**Debit/Lot:** ${row['Debit/Lot']:,.0f}")
                    c3.write(f"**Days Held:** {row['Days Held']}")
                    
                    # Color Badge
                    color = "green" if "A" in row['Grade'] else "red" if "F" in row['Grade'] else "orange"
                    st.markdown(f"**Verdict:** :{color}[{row['Grade']} - {row['Reason']}]")

    # ----------------------------------------------------
    # TAB 2: TRADE VALIDATOR (MODEL)
    # ----------------------------------------------------
    with tab2:
        st.markdown("### 🧪 Pre-Flight Audit")
        st.info("Drop a CSV/Excel file of a **proposed** trade here to see if it passes the rules.")
        
        model_file = st.file_uploader("Upload Model File", key="mod")
        
        if model_file:
            m_df, _ = process_data([model_file])
            if not m_df.empty:
                row = m_df.iloc[0]
                
                st.divider()
                st.markdown(f"### Result for: {row['Name']}")
                
                # Big Banner
                if "A" in row['Grade']:
                    st.success(f"## ✅ GO: {row['Grade']}")
                elif "F" in row['Grade']:
                    st.error(f"## ⛔ NO-GO: {row['Grade']}")
                else:
                    st.warning(f"## ⚠️ CAUTION: {row['Grade']}")
                
                c1, c2 = st.columns(2)
                c1.metric("Detected Strategy", row['Strategy'])
                c2.metric("Debit Per Lot", f"${row['Debit/Lot']:,.0f}")
                
                st.markdown(f"**Analysis:** {row['Reason']}")
                st.markdown("---")

    # ----------------------------------------------------
    # TAB 3: DEEP ANALYTICS
    # ----------------------------------------------------
    with tab3:
        if not df.empty:
            expired_df = df[df['Status'] == 'Expired']
            
            st.markdown("### 🏆 Historical Performance (Expired Data)")
            
            if not expired_df.empty:
                # Calc Win Rate
                wins = len(expired_df[expired_df['P&L'] > 0])
                total = len(expired_df)
                win_rate = (wins / total) * 100
                profit_factor = expired_df[expired_df['P&L']>0]['P&L'].sum() / abs(expired_df[expired_df['P&L']<0]['P&L'].sum())
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Win Rate", f"{win_rate:.1f}%")
                c2.metric("Profit Factor", f"{profit_factor:.2f}")
                c3.metric("Total Realized P&L", f"${expired_df['P&L'].sum():,.0f}")
                
                st.markdown("#### Strategy Breakdown")
                strat_stats = expired_df.groupby('Strategy').agg({
                    'P&L': ['sum', 'mean', 'count'],
                    'Days Held': 'mean'
                }).reset_index()
                strat_stats.columns = ['Strategy', 'Total Profit', 'Avg Win', 'Trades', 'Avg Duration']
                st.dataframe(strat_stats, use_container_width=True)
                
                c1, c2 = st.columns(2)
                with c1: st.plotly_chart(px.bar(expired_df, x='Strategy', y='P&L', color='Strategy', title="Realized P&L Distribution"), use_container_width=True)
                with c2: st.plotly_chart(px.scatter(expired_df, x='Debit/Lot', y='P&L', color='Strategy', title="Debit vs Outcome"), use_container_width=True)
                
            else:
                st.warning("No Expired trades found in uploads. Drop your history files to populate this tab.")

    # ----------------------------------------------------
    # TAB 4: THE RULE BOOK
    # ----------------------------------------------------
    with tab4:
        st.markdown("""
        # 📖 The Trading Constitution
        
        ### 1. The "130/160 - ALLANTIS" (Income Engine)
        * **Best Entry:** Monday.
        * **Debit Limit:** `$3,500 – $4,500` per Lot.
        * **HARD STOP:** Never pay > **$4,800**. (100% Fail Rate).
        * **Exit Rule:** Kill if 20-25 days old and flat P&L.
        
        ### 2. The "160/190 - ALLANTIS" (Compounder)
        * **Best Entry:** Friday.
        * **Debit Limit:** `~$5,200` per Lot.
        * **Sizing:** Trade **1 Lot** (2 Lots reduces ROI).
        * **Exit Rule:** Hold for 40-50 Days. Do not touch in first 30 days.
        
        ### 3. The "M200" Strategy (Whale)
        * **Best Entry:** Wednesday.
        * **Debit Limit:** `$7,500 – $8,500` per Lot.
        * **Exit Rule (The U-Turn):** Check P&L at **Day 14**.
            * If Green > $200 -> Exit/Roll.
            * If Red/Flat -> HOLD to Day 60.
        """)

else:
    st.info("👋 Upload Active & Expired files in the sidebar to begin.")
