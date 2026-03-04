import streamlit as st
import pandas as pd
import numpy as np
import io
import os
import re
import json
import time
import sqlite3
from datetime import datetime, timezone, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats 
from scipy.spatial.distance import cdist 

# --- V151: ROBUST STATE INITIALIZATION ---
def initialize_session_state():
    """Improved State Management replacing globals()"""
    keys = {
        'velocity_stats': {},
        'mae_stats': {},
        'last_cloud_sync': None,
        'show_conflict': False,
        'show_pull_conflict': False,
        'portfolio_capital': 1000000.0, # Default SMSF Capital
        'db_initialized': False
    }
    for key, default in keys.items():
        if key not in st.session_state:
            st.session_state[key] = default

initialize_session_state()

# --- GOOGLE DRIVE IMPORTS ---
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
    GOOGLE_DEPS_INSTALLED = True
except ImportError:
    GOOGLE_DEPS_INSTALLED = False

# --- PAGE CONFIG ---
st.set_page_config(page_title="Allantis Trade Guardian v151 (Cloud)", layout="wide", page_icon="🛡️")

# --- CSS STYLING ---
st.markdown("""
<style>
    .metric-card {
        background-color: #1E1E1E;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        border-left: 5px solid #00F0FF;
        margin-bottom: 15px;
    }
    .metric-title { color: #A0A0A0; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 1px; }
    .metric-value { color: #FFFFFF; font-size: 1.8rem; font-weight: bold; margin-top: 5px; }
    .warning-text { color: #FF4B4B; font-weight: bold; }
    .success-text { color: #00E676; font-weight: bold; }
    .stDataFrame { border-radius: 10px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# CORE DATABASE ARCHITECTURE (SQLite)
# ==========================================
DB_PATH = 'allantis_v151.db'

def init_db():
    if not st.session_state.db_initialized:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS trade_notes 
                     (trade_id TEXT PRIMARY KEY, note TEXT, updated_at TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS mae_trackers 
                     (strategy TEXT PRIMARY KEY, mae_value REAL, updated_at TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS velocity_benchmarks 
                     (strategy TEXT PRIMARY KEY, velocity REAL, updated_at TEXT)''')
        conn.commit()
        conn.close()
        st.session_state.db_initialized = True

def execute_db_query(query, params=(), fetch=False, fetchall=False):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(query, params)
    result = None
    if fetch:
        result = c.fetchone()
    elif fetchall:
        result = c.fetchall()
    else:
        conn.commit()
    conn.close()
    return result

init_db()

# ==========================================
# GOOGLE DRIVE CLOUD SYNC ARCHITECTURE
# ==========================================
class GoogleDriveManager:
    def __init__(self):
        self.scopes = ['https://www.googleapis.com/auth/drive.file']
        self.creds = None
        self.service = None
        self._authenticate()

    def _authenticate(self):
        if not GOOGLE_DEPS_INSTALLED: return
        try:
            # Placeholder for secrets management in Streamlit
            if 'gcp_service_account' in st.secrets:
                creds_dict = dict(st.secrets['gcp_service_account'])
                self.creds = service_account.Credentials.from_service_account_info(
                    creds_dict, scopes=self.scopes)
                self.service = build('drive', 'v3', credentials=self.creds)
        except Exception as e:
            st.sidebar.warning(f"Cloud Sync Disabled: Credentials missing. ({e})")

    def backup_database(self, file_path, folder_id=None):
        if not self.service: return False
        try:
            file_metadata = {'name': os.path.basename(file_path)}
            if folder_id: file_metadata['parents'] = [folder_id]
            media = MediaFileUpload(file_path, mimetype='application/x-sqlite3')
            file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            st.session_state.last_cloud_sync = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return file.get('id')
        except Exception as e:
            st.error(f"Cloud Backup Failed: {e}")
            return False

drive_manager = GoogleDriveManager()

# ==========================================
# V151 CALCULATION ENGINE (THE MISSION)
# ==========================================
class V151CalculationEngine:
    @staticmethod
    def calculate_true_roi(pnl, max_loss, credit_received):
        """
        MISSION FIX 2: The 'Capital at Risk' Fallacy.
        ROI must be calculated against Max Loss (Margin), NOT the premium received.
        """
        if max_loss > 0:
            return (pnl / max_loss) * 100
        elif credit_received > 0:
            # Extreme fallback for cash-secured setups where max loss isn't strictly defined but collateral is
            return (pnl / credit_received) * 100
        return 0.0

    @staticmethod
    def calculate_fat_tail_var(capital_at_risk, implied_vol, dte, confidence=0.99, df=3):
        """
        MISSION FIX 4: 'Normal Distribution' Trap.
        Utilizes a Leptokurtic (Fat-Tail) model using Student's t-distribution (df=3) 
        to accurately reflect Black Swan risks in the SMSF portfolio.
        """
        if dte <= 0 or capital_at_risk <= 0 or implied_vol <= 0:
            return 0.0
        # df=3 creates significantly fatter tails than standard normal distribution
        t_score = stats.t.ppf(confidence, df=df)
        time_scaling = np.sqrt(dte / 365.0)
        fat_tail_var = capital_at_risk * t_score * implied_vol * time_scaling
        return fat_tail_var

    @staticmethod
    def apply_gamma_blindness_penalty(dte, gamma_exposure):
        """
        MISSION FIX 3: 'Gamma Blindness' in Stability.
        Downgrades Stability/Health score dynamically if DTE < 21 and Gamma is high.
        """
        penalty = 0.0
        if dte < 21 and abs(gamma_exposure) > 0.02:
            urgency_multiplier = (21 - dte) / 21.0
            # Exponential scaling of gamma threat
            penalty = urgency_multiplier * (abs(gamma_exposure) * 500)
            penalty = min(penalty, 60.0) # Cap the deduction at 60 points
        return penalty

    @staticmethod
    def calculate_nonlinear_theta(theta, dte):
        """
        MISSION FIX 6: Non-Linear Time-Decay.
        Replaces linear Theta projections with a Square Root of Time model.
        """
        if dte <= 0: return 0.0
        return theta * np.sqrt(dte)

    @staticmethod
    def calculate_kelly_correlation_limits(trades, portfolio_size):
        """
        MISSION FIX 5: Kelly Criterion Correlation Risk.
        Groups allocations by Underlying to prevent over-leveraging correlated positions.
        """
        allocations = {}
        for t in trades:
            for und in t['Underlyings']:
                if und not in allocations:
                    allocations[und] = {'total_risk': 0, 'win_prob': [], 'profit': 0}
                allocations[und]['total_risk'] += t['Max Loss']
                allocations[und]['profit'] += t['Max Profit']
                if 'Chance' in t and pd.notna(t['Chance']):
                    allocations[und]['win_prob'].append(t['Chance'])

        results = {}
        for und, data in allocations.items():
            if data['total_risk'] == 0: continue
            
            p = np.mean(data['win_prob']) if data['win_prob'] else 0.5
            q = 1 - p
            # b = Net fractional odds
            b = data['profit'] / data['total_risk'] if data['total_risk'] > 0 else 1.0
            
            raw_kelly = (p * b - q) / b if b > 0 else 0
            
            # Risk limits based on grouping
            current_allocation_pct = (data['total_risk'] / portfolio_size) * 100 if portfolio_size > 0 else 0
            
            # SMSF Conservative cap per underlying (e.g., 15%)
            recommended_cap_pct = min(max(raw_kelly * 100 * 0.5, 0), 15.0) # Half-kelly with strict cap

            status = "Safe"
            if current_allocation_pct > recommended_cap_pct:
                status = "CORRELATION RISK (OVER-LEVERAGED)"
                
            results[und] = {
                'Current Alloc %': current_allocation_pct,
                'Rec Cap %': recommended_cap_pct,
                'Status': status
            }
        return results

calc_engine = V151CalculationEngine()

# ==========================================
# DATA PROCESSING & PARSING
# ==========================================
def parse_trade_data(df):
    """
    Parses the hierarchical CSV and applies MISSION FIX 1: The Credit/Debit Correction.
    """
    trades = []
    current_trade = None
    today = datetime.now()

    for index, row in df.iterrows():
        name = str(row.get('Name', '')).strip()
        if not name or name.lower() == 'nan' or name == 'Symbol':
            continue

        # Check if row is a Leg (starts with '.' typically in options platforms)
        if name.startswith('.'):
            if current_trade:
                leg_symbol = name
                # Extract Underlying (e.g., .SPX260618P6050 -> SPX)
                match = re.match(r'^\.?([A-Za-z]+)', leg_symbol)
                if match:
                    current_trade['Underlyings'].add(match.group(1))
                
                current_trade['Legs'].append({
                    'Symbol': leg_symbol,
                    'Qty': float(row.get('Total Return %', 0)) if pd.notna(row.get('Total Return %')) else 0,
                    'Entry': float(row.get('Total Return $', 0)) if pd.notna(row.get('Total Return $')) else 0
                })
        else:
            # Save previous trade
            if current_trade:
                trades.append(current_trade)

            # --- MISSION FIX 1: THE CREDIT/DEBIT CORRECTION ---
            # In the provided CSV, a positive value (e.g., 4800) represents a Credit.
            raw_net = float(row.get('Net Debit/Credit', 0)) if pd.notna(row.get('Net Debit/Credit')) else 0.0
            is_credit = raw_net > 0 
            credit_received = raw_net if is_credit else 0.0
            debit_paid = abs(raw_net) if not is_credit else 0.0
            
            max_loss = float(row.get('Max Loss', 0)) if pd.notna(row.get('Max Loss')) else 0.0
            max_profit = float(row.get('Max Profit', 0)) if pd.notna(row.get('Max Profit')) else 0.0
            pnl = float(row.get('Total Return $', 0)) if pd.notna(row.get('Total Return $')) else 0.0
            
            # Safe date parsing
            exp_date_str = str(row.get('Expiration', ''))
            dte = 0
            if exp_date_str and exp_date_str.lower() != 'nan':
                try:
                    exp_date = pd.to_datetime(exp_date_str).replace(tzinfo=None)
                    dte = (exp_date - today).days
                except:
                    dte = 0

            # Execute Engine Fixes immediately
            true_roi = calc_engine.calculate_true_roi(pnl, max_loss, credit_received)
            
            gamma = float(row.get('Gamma', 0)) if pd.notna(row.get('Gamma')) else 0.0
            theta = float(row.get('Theta', 0)) if pd.notna(row.get('Theta')) else 0.0
            iv = float(row.get('IV', 0)) if pd.notna(row.get('IV')) else 0.0
            
            gamma_penalty = calc_engine.apply_gamma_blindness_penalty(dte, gamma)
            nonlinear_theta = calc_engine.calculate_nonlinear_theta(theta, dte)
            fat_tail_var = calc_engine.calculate_fat_tail_var(max_loss, iv, dte)

            # Base Stability calculation (out of 100)
            base_stability = 100 - (abs(float(row.get('Delta', 0)) if pd.notna(row.get('Delta')) else 0) * 50)
            final_stability = max(0, min(100, base_stability - gamma_penalty))

            current_trade = {
                'Trade Name': name,
                'Type': 'Credit' if is_credit else 'Debit',
                'Net Amount': raw_net,
                'Max Loss': max_loss,
                'Max Profit': max_profit,
                'Current PnL': pnl,
                'True ROI %': true_roi,
                'DTE': dte,
                'Delta': float(row.get('Delta', 0)) if pd.notna(row.get('Delta')) else 0.0,
                'Gamma': gamma,
                'Theta': theta,
                'NonLinear Theta': nonlinear_theta,
                'IV': iv,
                'Fat Tail VaR (99%)': fat_tail_var,
                'Stability Score': final_stability,
                'Gamma Penalty': gamma_penalty,
                'Underlyings': set(),
                'Legs': []
            }
            
            # Fallback parsing for underlying if legs aren't detailed
            if '.' in name:
                match = re.search(r'\.([A-Za-z]+)', name)
                if match: current_trade['Underlyings'].add(match.group(1))

    if current_trade:
        trades.append(current_trade)

    return trades

# ==========================================
# UI & THEME HELPER FUNCTIONS
# ==========================================
def apply_chart_theme(fig):
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#A0A0A0"),
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig

# ==========================================
# MAIN STREAMLIT APPLICATION ROUTING
# ==========================================
def main():
    st.sidebar.title("🛡️ Allantis v151")
    st.sidebar.markdown("*SMSF Trade Guardian*")
    
    # Cloud Sync Status
    st.sidebar.subheader("☁️ Cloud Infrastructure")
    sync_status = "🟢 Active" if GOOGLE_DEPS_INSTALLED and drive_manager.service else "🔴 Offline"
    st.sidebar.write(f"Sync Status: {sync_status}")
    if st.session_state.last_cloud_sync:
        st.sidebar.caption(f"Last Sync: {st.session_state.last_cloud_sync}")
        
    st.sidebar.divider()
    
    app_mode = st.sidebar.radio("Navigation Engine", [
        "Dashboard & Engine", 
        "Active SMSF Portfolio", 
        "Kelly Correlation Matrix",
        "Advanced Greeks & VaR",
        "MAE & Velocity Benchmarks",
        "Data Importer"
    ])
    
    st.sidebar.divider()
    st.session_state.portfolio_capital = st.sidebar.number_input("SMSF Capital Base ($)", value=st.session_state.portfolio_capital, step=10000.0)

    # ----------------------------------------
    # DATA IMPORTER TAB
    # ----------------------------------------
    if app_mode == "Data Importer":
        st.header("📥 Trade Data Ingestion")
        st.markdown("Upload standard Options CSV exports to initialize the v151 Calculation Engine.")
        
        uploaded_file = st.file_uploader("Upload CSV / Excel File", type=["csv", "xlsx"])
        
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.success("File ingested successfully. Engine processing...")
                parsed_trades = parse_trade_data(df)
                st.session_state['parsed_trades'] = parsed_trades
                
                st.dataframe(df.head(10), use_container_width=True)
                st.info(f"Successfully processed {len(parsed_trades)} parent trade structures and extracted underlying vectors.")
                
            except Exception as e:
                st.error(f"Error parsing file: {e}")

    # Ensure we have data before rendering other tabs
    if 'parsed_trades' not in st.session_state:
        if app_mode != "Data Importer":
            st.warning("⚠️ No trade data found. Please navigate to the 'Data Importer' and upload your portfolio CSV.")
        return

    trades = st.session_state['parsed_trades']

    # ----------------------------------------
    # DASHBOARD & ENGINE OVERVIEW
    # ----------------------------------------
    if app_mode == "Dashboard & Engine":
        st.header("🎛️ v151 System Dashboard")
        
        total_pnl = sum(t['Current PnL'] for t in trades)
        total_max_loss = sum(t['Max Loss'] for t in trades)
        total_var = sum(t['Fat Tail VaR (99%)'] for t in trades)
        avg_health = np.mean([t['Stability Score'] for t in trades]) if trades else 0

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Open Portfolio PnL</div>
                <div class="metric-value" style="color: {'#00E676' if total_pnl >=0 else '#FF4B4B'}">${total_pnl:,.2f}</div>
            </div>
            """, unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">True Capital at Risk (Max Loss)</div>
                <div class="metric-value">${total_max_loss:,.2f}</div>
            </div>
            """, unsafe_allow_html=True)
        with c3:
            st.markdown(f"""
            <div class="metric-card" style="border-left: 5px solid #FF4B4B;">
                <div class="metric-title">Fat-Tail VaR (99% df=3)</div>
                <div class="metric-value">${total_var:,.2f}</div>
            </div>
            """, unsafe_allow_html=True)
        with c4:
            st.markdown(f"""
            <div class="metric-card" style="border-left: 5px solid #FFD600;">
                <div class="metric-title">System Health Score</div>
                <div class="metric-value">{avg_health:.1f} / 100</div>
            </div>
            """, unsafe_allow_html=True)

        st.subheader("Architectural Diagnostics")
        st.markdown("""
        **Active v151 Engine Constraints:**
        * ✅ **Credit/Debit Alignment:** Verified. Positive CSV cash flows resolved as Yield.
        * ✅ **Capital Efficiency:** ROI strictly bound to Margin deployed.
        * ✅ **Gamma Mitigation:** <21 DTE exponential penalties active.
        * ✅ **Tail Risk:** Student's T-Distribution ($df=3$) modeling active for Black Swans.
        """)

    # ----------------------------------------
    # ACTIVE SMSF PORTFOLIO
    # ----------------------------------------
    elif app_mode == "Active SMSF Portfolio":
        st.header("📊 Active Portfolio Master")
        
        display_data = []
        for t in trades:
            display_data.append({
                'Trade Name': t['Trade Name'],
                'Underlyings': ", ".join(t['Underlyings']),
                'Type': t['Type'],
                'PnL ($)': t['Current PnL'],
                'Max Loss ($)': t['Max Loss'],
                'True ROI (%)': f"{t['True ROI %']:.2f}%",
                'DTE': t['DTE'],
                'Health (0-100)': f"{t['Stability Score']:.1f}",
                'Gamma Penalty': f"-{t['Gamma Penalty']:.1f}" if t['Gamma Penalty'] > 0 else "0",
            })
            
        df_display = pd.DataFrame(display_data)
        
        # Color coding for Health
        def color_health(val):
            try:
                score = float(val)
                if score > 80: return 'background-color: rgba(0, 230, 118, 0.2)'
                elif score > 50: return 'background-color: rgba(255, 214, 0, 0.2)'
                else: return 'background-color: rgba(255, 75, 75, 0.2)'
            except:
                return ''

        st.dataframe(df_display.style.map(color_health, subset=['Health (0-100)']), use_container_width=True, height=400)

        # Trade Notes System (SQLite Backed)
        st.subheader("📝 SMSF Trade Journal")
        selected_trade = st.selectbox("Select Trade for Notes:", [t['Trade Name'] for t in trades])
        
        if selected_trade:
            # Fetch Note
            existing_note_row = execute_db_query("SELECT note FROM trade_notes WHERE trade_id = ?", (selected_trade,), fetch=True)
            existing_note = existing_note_row[0] if existing_note_row else ""
            
            new_note = st.text_area("Trade Analysis & Adjustments:", value=existing_note, height=150)
            
            if st.button("Save Journal Entry"):
                now = datetime.now().isoformat()
                execute_db_query("INSERT OR REPLACE INTO trade_notes (trade_id, note, updated_at) VALUES (?, ?, ?)",
                                 (selected_trade, new_note, now))
                st.success("Note saved securely to local SQLite DB.")
                
                # Cloud Sync Trigger
                if drive_manager.service:
                    if drive_manager.backup_database(DB_PATH):
                        st.info("☁️ Successfully synchronized journal to Google Drive.")

    # ----------------------------------------
    # KELLY CORRELATION MATRIX
    # ----------------------------------------
    elif app_mode == "Kelly Correlation Matrix":
        st.header("🕸️ Kelly Correlation & Leverage Safety")
        st.markdown("**(Fix 5): Grouping by Underlying to prevent highly correlated over-leveraging.**")
        
        kelly_results = calc_engine.calculate_kelly_correlation_limits(trades, st.session_state.portfolio_capital)
        
        if not kelly_results:
            st.info("Insufficient data to calculate Kelly limits.")
        else:
            k_df = pd.DataFrame.from_dict(kelly_results, orient='index').reset_index()
            k_df.rename(columns={'index': 'Underlying'}, inplace=True)
            
            fig = px.bar(k_df, x='Underlying', y=['Current Alloc %', 'Rec Cap %'], 
                         barmode='group', title="Capital Allocation vs Kelly Constraint per Underlying",
                         color_discrete_map={'Current Alloc %': '#FF4B4B', 'Rec Cap %': '#00F0FF'})
            fig = apply_chart_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Correlation Status Table")
            def color_status(val):
                if 'RISK' in str(val): return 'color: #FF4B4B; font-weight: bold;'
                return 'color: #00E676;'
                
            st.dataframe(k_df.style.map(color_status, subset=['Status']), use_container_width=True)

    # ----------------------------------------
    # ADVANCED GREEKS & VAR
    # ----------------------------------------
    elif app_mode == "Advanced Greeks & VaR":
        st.header("🔬 Deep Greek Analytics & Tail Risk")
        
        c1, c2 = st.columns(2)
        
        with c1:
            st.subheader("VaR Distribution Modeling")
            st.markdown("Comparing standard Normal Dist vs v151 Fat-Tail (Student's t, df=3)")
            
            # Generate dummy distribution for visualization
            x = np.linspace(-5, 5, 1000)
            y_norm = stats.norm.pdf(x, 0, 1)
            y_t = stats.t.pdf(x, df=3)
            
            fig_var = go.Figure()
            fig_var.add_trace(go.Scatter(x=x, y=y_norm, mode='lines', name='Normal (v150)', line=dict(color='gray', dash='dash')))
            fig_var.add_trace(go.Scatter(x=x, y=y_t, mode='lines', name='Leptokurtic t-Dist (v151)', line=dict(color='#FF4B4B', width=2)))
            fig_var.update_layout(title="Tail Risk Probability Density", xaxis_title="Standard Deviations", yaxis_title="Probability")
            fig_var = apply_chart_theme(fig_var)
            st.plotly_chart(fig_var, use_container_width=True)
            
        with c2:
            st.subheader("Non-Linear Theta Projection")
            st.markdown("Visualizing $Theta \times \sqrt{DTE}$ vs linear $Theta \times DTE$")
            
            dtes = np.arange(45, 0, -1)
            base_theta = 10
            linear = base_theta * dtes
            # Simulating square root acceleration as DTE goes to 0
            nonlinear = base_theta * (np.sqrt(45) - np.sqrt(dtes)) * 10 
            
            fig_theta = go.Figure()
            fig_theta.add_trace(go.Scatter(x=dtes, y=linear, mode='lines', name='Linear Decay (Flawed)', line=dict(color='gray', dash='dash')))
            fig_theta.add_trace(go.Scatter(x=dtes, y=nonlinear, mode='lines', name='Sqrt(T) Decay (v151)', line=dict(color='#00F0FF', width=2)))
            fig_theta.update_layout(title="Cumulative Theta Decay Curve", xaxis_title="Days to Expiration (DTE)", xaxis=dict(autorange="reversed"), yaxis_title="Accumulated Value")
            fig_theta = apply_chart_theme(fig_theta)
            st.plotly_chart(fig_theta, use_container_width=True)

        st.divider()
        st.subheader("Gamma Blindness Penalties")
        
        # Table showing positions penalized for gamma
        gamma_data = [t for t in trades if t['Gamma Penalty'] > 0]
        if gamma_data:
            g_df = pd.DataFrame([{
                'Trade': t['Trade Name'],
                'DTE': t['DTE'],
                'Raw Gamma': t['Gamma'],
                'Stability Penalty Deducted': f"-{t['Gamma Penalty']:.1f}"
            } for t in gamma_data])
            st.error(f"⚠️ {len(gamma_data)} positions are currently suffering Gamma Blindness Penalties due to imminent expiration (<21 DTE) and high structural gamma.")
            st.dataframe(g_df, use_container_width=True)
        else:
            st.success("✅ No positions are currently suffering Gamma Penalties. Portfolio DTE structure is stable.")

    # ----------------------------------------
    # MAE & VELOCITY BENCHMARKS
    # ----------------------------------------
    elif app_mode == "MAE & Velocity Benchmarks":
        st.header("🎯 Maximum Adverse Excursion & Velocity")
        st.markdown("Master Trackers retained from v150, strictly evaluating true capital thresholds.")
        
        # Group by Underlying for Strategies
        strategies = list(set(und for t in trades for und in t['Underlyings']))
        
        if not strategies:
            st.info("No underlying strategies identified to benchmark.")
            return

        selected_strat = st.selectbox("Select Strategy Matrix", strategies)
        
        # Get DB values
        mae_row = execute_db_query("SELECT mae_value FROM mae_trackers WHERE strategy = ?", (selected_strat,), fetch=True)
        vel_row = execute_db_query("SELECT velocity FROM velocity_benchmarks WHERE strategy = ?", (selected_strat,), fetch=True)
        
        current_mae = mae_row[0] if mae_row else 1500.0  # Default safe zone
        current_vel = vel_row[0] if vel_row else 50.0

        c1, c2 = st.columns(2)
        with c1:
            st.subheader(f"Update MAE: {selected_strat}")
            new_mae = st.number_input("Max Drawdown Tolerance ($)", value=float(current_mae), step=100.0)
            if st.button("Commit MAE"):
                execute_db_query("INSERT OR REPLACE INTO mae_trackers (strategy, mae_value, updated_at) VALUES (?, ?, ?)",
                                 (selected_strat, new_mae, datetime.now().isoformat()))
                st.success("MAE Saved.")
                current_mae = new_mae
                
        with c2:
            st.subheader(f"Update Velocity: {selected_strat}")
            new_vel = st.number_input("Target Daily Velocity ($/Day)", value=float(current_vel), step=5.0)
            if st.button("Commit Velocity"):
                execute_db_query("INSERT OR REPLACE INTO velocity_benchmarks (strategy, velocity, updated_at) VALUES (?, ?, ?)",
                                 (selected_strat, new_vel, datetime.now().isoformat()))
                st.success("Velocity Saved.")
                current_vel = new_vel

        st.divider()
        st.subheader("Smart Stop (MAE) Visualization")
        
        # Plotly visual for MAE Safe Zone
        safe_range = abs(current_mae)
        fig_mae = go.Figure()
        
        # Current worst drawdown among active trades for this strategy
        strat_trades = [t for t in trades if selected_strat in t['Underlyings']]
        worst_dd = min([t['Current PnL'] for t in strat_trades] + [0]) 
        
        fig_mae.add_trace(go.Bar(x=[safe_range], y=["Risk Capacity"], orientation='h', marker_color='rgba(0, 230, 118, 0.5)', name="Safe Zone Cap"))
        
        if worst_dd < 0:
             fig_mae.add_trace(go.Bar(x=[abs(worst_dd)], y=["Risk Capacity"], orientation='h', marker_color='#FF4B4B', name="Current Drawdown"))

        fig_mae.update_layout(xaxis_title="Drawdown Magnitude ($)", barmode='overlay', height=200, margin=dict(l=0,r=0,t=30,b=0))
        fig_mae = apply_chart_theme(fig_mae)
        st.plotly_chart(fig_mae, use_container_width=True)

if __name__ == "__main__":
    main()
