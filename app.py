import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

import ingestion     # our upgraded parser + DB writer
import db            # our SQLite database engine

# ---------------------------------------------------------
# STREAMLIT PAGE CONFIG
# ---------------------------------------------------------
st.set_page_config(
    page_title="Allantis Trade Guardian",
    layout="wide",
    page_icon="🛡️"
)

st.title("🛡️ Allantis Trade Guardian v36.0")

# ---------------------------------------------------------
# SIDEBAR — DAILY WORKFLOW
# ---------------------------------------------------------
st.sidebar.header("Daily Workflow")

uploaded_files = st.sidebar.file_uploader(
    "Drop Active & History Files (Excel/CSV)",
    accept_multiple_files=True
)

st.sidebar.divider()

# ---------------------------------------------------------
# SIDEBAR — STRATEGY SETTINGS
# ---------------------------------------------------------
st.sidebar.header("⚙️ Strategy Settings")

market_regime = st.sidebar.selectbox(
    "Current Market Regime",
    ["Neutral (Standard)", "Bullish (Aggr. Targets)", "Bearish (Safe Targets)"],
    index=0,
    help="Bullish: +10% Profit Target | Bearish: -10% Profit Target"
)

show_closed = st.sidebar.checkbox(
    "Show Expired Trades in Analytics",
    value=True
)

# ---------------------------------------------------------
# MARKET REGIME MULTIPLIER
# ---------------------------------------------------------
regime_mult = 1.0
if "Bullish" in market_regime:
    regime_mult = 1.10
if "Bearish" in market_regime:
    regime_mult = 0.90


# ---------------------------------------------------------
# HELPER (unchanged from v35)
# ---------------------------------------------------------
def safe_fmt(val, fmt_str):
    try:
        if isinstance(val, (int, float)):
            return fmt_str.format(val)
        return str(val)
    except:
        return str(val)
# ---------------------------------------------------------
# SMART EXIT ENGINE (IDENTICAL to v35 logic)
# ---------------------------------------------------------
def get_action_signal(strategy, status, days_held, pnl, benchmarks_dict):
    action = ""
    signal_type = "NONE"

    if status == "Active":

        # TAKE PROFIT RULE
        benchmark = benchmarks_dict.get(strategy, {})
        base_target = benchmark.get("pnl", 0)

        if base_target == 0:
            base_target = 9999  # fallback

        final_target = base_target * regime_mult

        if pnl >= final_target:
            return f"TAKE PROFIT (Hit ${final_target:,.0f})", "SUCCESS"

        # STRATEGY LOGIC
        if strategy == "130/160":
            if 25 <= days_held <= 35 and pnl < 100:
                return "KILL (Stale >25d)", "ERROR"

        elif strategy == "160/190":
            if days_held < 30:
                return "COOKING (Do Not Touch)", "INFO"
            elif 30 <= days_held <= 40:
                return "WATCH (Profit Zone)", "WARNING"

        elif strategy == "M200":
            if 12 <= days_held <= 16:
                if pnl > 200:
                    return "DAY 14 CHECK (Green)", "SUCCESS"
                else:
                    return "DAY 14 CHECK (Red)", "WARNING"

    return action, signal_type



# ---------------------------------------------------------
# INGEST UPLOADED FILES
# ---------------------------------------------------------
if uploaded_files:
    # Write to DB and return dataframe for UI
    df = ingestion.ingest_files(uploaded_files)
else:
    # Load existing DB trades if no files uploaded
    rows = db.load_all_trades()
    df = pd.DataFrame(rows) if rows else pd.DataFrame()

if df is None or df.empty:
    st.info("👋 Upload today's ACTIVE file to begin.")
    st.stop()


# ---------------------------------------------------------
# BENCHMARK CALCULATION (same as v35)
# ---------------------------------------------------------
if df is None or df.empty:
    st.info("👋 Upload today's ACTIVE file to begin.")
    st.stop()

# ADD THIS:
df.columns = df.columns.str.lower()

required_cols = [
    "trade_id", "name", "strategy", "status", "pnl", "debit",
    "debit_per_lot", "grade", "reason", "alerts", "days_held",
    "daily_yield", "roi", "entry_date", "expiration_date",
    "lot_size", "latest_flag", "theta", "delta", "gamma", "vega"
]

for col in required_cols:
    if col not in df.columns:
        df[col] = None

# fall back to base config
benchmarks = {
    "130/160": {"yield": 0.13, "pnl": 500, "roi": 6.8, "dit": 36},
    "160/190": {"yield": 0.28, "pnl": 700, "roi": 12.7, "dit": 44},
    "M200":    {"yield": 0.56, "pnl": 900, "roi": 11.1, "dit": 41}
}

if not expired_df.empty:
    grp = expired_df.groupby("strategy")
    for strat, g in grp:
        winners = g[g["pnl"] > 0]
        if not winners.empty:
            benchmarks[strat] = {
                "yield": g["daily_yield"].mean(),
                "pnl": winners["pnl"].mean(),
                "roi": winners["roi"].mean(),
                "dit": g["days_held"].mean()
            }


# ---------------------------------------------------------
# MAIN TABS
# ---------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Active Dashboard",
    "🧪 Trade Validator",
    "📈 Analytics",
    "📖 Rule Book"
])
# ---------------------------------------------------------
# TAB 1 — ACTIVE DASHBOARD
# ---------------------------------------------------------
with tab1:

    active_df = df[(df["status"] == "Active")].copy()

    if active_df.empty:
        st.info("📭 No active trades found. Upload a current Active File.")
    else:

        # PORTFOLIO HEALTH (same as v35)
        port_yield = active_df["daily_yield"].mean()
        if port_yield < 0.10:
            st.sidebar.error(f"🚨 Yield Critical: {port_yield:.2f}%")
        elif port_yield < 0.15:
            st.sidebar.warning(f"⚠️ Yield Low: {port_yield:.2f}%")
        else:
            st.sidebar.success(f"✅ Yield Healthy: {port_yield:.2f}%")

        # APPLY ACTION LOGIC
        act_list = []
        sig_list = []

        for _, row in active_df.iterrows():
            act, sig = get_action_signal(
                row["strategy"],
                row["status"],
                row["days_held"],
                row["pnl"],
                benchmarks
            )
            act_list.append(act)
            sig_list.append(sig)

        active_df["Action"] = act_list
        active_df["Signal_Type"] = sig_list

        # STRATEGY TABS
        st.markdown("### 🏛️ Active Trades by Strategy")
        strat_tabs = st.tabs(["📋 Strategy Overview", "🔹 130/160", "🔸 160/190", "🐳 M200"])

        # COLUMNS to display
        cols = [
            "name", "Action", "grade", "daily_yield", "pnl", "debit",
            "days_held", "theta", "delta", "gamma", "vega", "Notes"
        ]

        # -----------------------------------------------------
        # RENDER STRATEGY SUBTAB
        # -----------------------------------------------------
        def render_tab(tab, strategy_name):
            with tab:
                subset = active_df[active_df["strategy"] == strategy_name].copy()
                bench = benchmarks.get(strategy_name, BASE_CONFIG.get(strategy_name))

                target_disp = bench["pnl"] * regime_mult

                # ACTION CENTER
                urgent = subset[subset["Action"] != ""]
                if not urgent.empty:
                    st.markdown(f"**🚨 Action Center ({len(urgent)})**")
                    for _, row in urgent.iterrows():
                        msg = f"**{row['name']}**: {row['Action']}"
                        sig = row["Signal_Type"]
                        if sig == "SUCCESS": st.success(msg)
                        elif sig == "ERROR": st.error(msg)
                        elif sig == "WARNING": st.warning(msg)
                        else: st.info(msg)
                    st.divider()

                # FOUR SMALL METRICS
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Hist. Avg Win", f"${bench['pnl']:,.0f}")
                c2.metric("Target Yield", f"{bench['yield']:.2f}%/d")
                c3.metric("Target Profit", f"${target_disp:,.0f}")
                c4.metric("Avg Hold", f"{bench['dit']:.0f}d")

                if not subset.empty:

                    # TOTAL ROW
                    total_row = pd.DataFrame({
                        "name": ["TOTAL"],
                        "Action": ["-"],
                        "grade": ["-"],
                        "daily_yield": [subset["daily_yield"].mean()],
                        "pnl": [subset["pnl"].sum()],
                        "debit": [subset["debit"].sum()],
                        "days_held": [subset["days_held"].mean()],
                        "theta": [subset["theta"].sum()],
                        "delta": [subset["delta"].sum()],
                        "gamma": [subset["gamma"].sum()],
                        "vega": [subset["vega"].sum()],
                        "Notes": [""]
                    })

                    display_df = pd.concat([subset[cols], total_row], ignore_index=True)

                    # STYLING (identical)
                    st.dataframe(
                        display_df.style
                        .format({
                            "pnl": "${:,.0f}",
                            "debit": "${:,.0f}",
                            "daily_yield": "{:.2f}%",
                            "theta": "{:.1f}",
                            "delta": "{:.1f}",
                            "gamma": "{:.2f}",
                            "vega": "{:.0f}",
                            "days_held": "{:.0f}"
                        })
                        .applymap(
                            lambda v: "background-color: #d1e7dd; color: #0f5132; font-weight: bold"
                            if "TAKE PROFIT" in str(v)
                            else "background-color: #f8d7da; color: #842029; font-weight: bold"
                            if "KILL" in str(v)
                            else "",
                            subset=["Action"]
                        )
                        .applymap(
                            lambda v: "color: #0f5132; font-weight: bold"
                            if "A" in str(v)
                            else "color: #842029; font-weight: bold"
                            if "F" in str(v)
                            else "",
                            subset=["grade"]
                        )
                        .apply(
                            lambda x: [
                                "background-color: #d1d5db; color: black; font-weight: bold"
                                if x.name == len(display_df) - 1
                                else ""
                                for _ in x
                            ],
                            axis=1
                        ),
                        use_container_width=True
                    )

                else:
                    st.info("No active trades.")

        # OVERVIEW TAB
        with strat_tabs[0]:

            with st.expander("📊 Portfolio Risk Metrics", expanded=True):
                total_delta = active_df["delta"].sum()
                total_theta = active_df["theta"].sum()
                total_cap = active_df["debit"].sum()

                r1, r2, r3 = st.columns(3)
                r1.metric("Net Delta", f"{total_delta:,.1f}",
                          delta="Bullish" if total_delta > 0 else "Bearish")
                r2.metric("Daily Theta", f"${total_theta:,.0f}")
                r3.metric("Capital at Risk", f"${total_cap:,.0f}")

            # AGGREGATE BY STRATEGY
            strat_agg = active_df.groupby("strategy").agg({
                "pnl": "sum",
                "debit": "sum",
                "theta": "sum",
                "delta": "sum",
                "name": "count",
                "daily_yield": "mean"
            }).reset_index()

            strat_agg["Trend"] = strat_agg.apply(
                lambda r: "🟢 Improving" if r["daily_yield"] >= benchmarks.get(r["strategy"], {}).get("yield", 0)
                else "🔴 Lagging",
                axis=1
            )
            strat_agg["Target %"] = strat_agg["strategy"].apply(
                lambda s: benchmarks.get(s, {}).get("yield", 0)
            )

            # TOTAL ROW
            total_row = pd.DataFrame({
                "strategy": ["TOTAL"],
                "pnl": [strat_agg["pnl"].sum()],
                "debit": [strat_agg["debit"].sum()],
                "theta": [strat_agg["theta"].sum()],
                "delta": [strat_agg["delta"].sum()],
                "name": [strat_agg["name"].sum()],
                "daily_yield": [active_df["daily_yield"].mean()],
                "Trend": ["-"],
                "Target %": ["-"]
            })

            final_agg = pd.concat([strat_agg, total_row], ignore_index=True)

            display_agg = final_agg[[
                "strategy", "Trend", "daily_yield", "Target %",
                "pnl", "debit", "theta", "delta", "name"
            ]].copy()

            display_agg.columns = [
                "Strategy", "Trend", "Yield/Day", "Target",
                "Total P&L", "Total Debit", "Net Theta",
                "Net Delta", "Active Trades"
            ]

            def highlight_trend(val):
                if "🟢" in str(val):
                    return "color: green; font-weight: bold"
                if "🔴" in str(val):
                    return "color: red; font-weight: bold"
                return ""

            def style_total(row):
                if row["Strategy"] == "TOTAL":
                    return ["background-color: #d1d5db; color: black; font-weight: bold"] * len(row)
                return [""] * len(row)

            st.dataframe(
                display_agg.style
                .format({
                    "Total P&L": "${:,.0f}",
                    "Total Debit": "${:,.0f}",
                    "Net Theta": "{:,.0f}",
                    "Net Delta": "{:,.1f}",
                    "Yield/Day": lambda x: safe_fmt(x, "{:.2f}%"),
                    "Target": lambda x: safe_fmt(x, "{:.2f}%")
                })
                .applymap(highlight_trend, subset=["Trend"])
                .apply(style_total, axis=1),
                use_container_width=True
            )

            csv = active_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Download Active Trades CSV",
                csv,
                "active_snapshot.csv",
                "text/csv"
            )

        # INDIVIDUAL STRATEGY TABS
        render_tab(strat_tabs[1], "130/160")
        render_tab(strat_tabs[2], "160/190")
        render_tab(strat_tabs[3], "M200")

# ---------------------------------------------------------
# TAB 2 — TRADE VALIDATOR (PRE-FLIGHT AUDIT)
# ---------------------------------------------------------
with tab2:
    st.markdown("### 🧪 Pre-Flight Audit")

    with st.expander("ℹ️ Grading System Legend", expanded=True):
        st.markdown("""
        | Strategy | Grade | Debit Range (Per Lot) | Verdict |
        | --- | --- | --- | --- |
        | 130/160 | A+ | $3,500 - $4,500 | Sweet Spot |
        | 130/160 | B | < $3,500 or $4,500-$4,800 | Acceptable |
        | 130/160 | F | > $4,800 | Overpriced |
        | 160/190 | A | $4,800 - $5,500 | Ideal Pricing |
        | 160/190 | C | > $5,500 | Expensive |
        | M200 | A | $7,500 - $8,500 | Perfect Entry |
        | M200 | B | Any other price | Variance |
        """)

    model_file = st.file_uploader("Upload Model File", key="validator_file")

    if model_file:
        model_df = ingestion.ingest_files([model_file])

        if not model_df.empty:
            row = model_df.iloc[0]
            st.divider()
            st.subheader(f"Audit: {row['name']}")

            c1, c2, c3 = st.columns(3)
            c1.metric("Strategy", row["strategy"])
            c2.metric("Debit Total", f"${row['debit']:,.0f}")
            c3.metric("Debit Per Lot", f"${row['debit_per_lot']:,.0f}")

            # Historical context
            expired_only = df[df["status"] == "Expired"].copy()
            if not expired_only.empty:
                similar = expired_only[
                    (expired_only["strategy"] == row["strategy"]) &
                    (expired_only["debit_per_lot"].between(
                        row["debit_per_lot"] * 0.9,
                        row["debit_per_lot"] * 1.1
                    ))
                ]
                if not similar.empty:
                    avg_win = similar[similar["pnl"] > 0]["pnl"].mean()
                    st.info(f"📊 Found {len(similar)} similar trades. Avg Win: ${avg_win:,.0f}")

            # Verdict
            if "A" in row["grade"]:
                st.success(f"APPROVED — {row['reason']}")
            elif "F" in row["grade"]:
                st.error(f"REJECT — {row['reason']}")
            else:
                st.warning(f"CHECK — {row['reason']}")


# ---------------------------------------------------------
# TAB 3 — ANALYTICS
# ---------------------------------------------------------
with tab3:
    st.subheader("📈 Analytics & Trends")

    # DATE FILTER
    if "entry_date" in df.columns:
        try:
            df["entry_date"] = pd.to_datetime(df["entry_date"])
        except:
            pass

        min_date = df["entry_date"].min()
        max_date = df["entry_date"].max()

        date_range = st.date_input("Filter by Entry Date Range", [min_date, max_date])

        if len(date_range) == 2:
            start_d = pd.to_datetime(date_range[0])
            end_d = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            filtered_df = df[
                (df["entry_date"] >= start_d) &
                (df["entry_date"] <= end_d)
            ]
        else:
            filtered_df = df
    else:
        filtered_df = df

    # TABS INSIDE ANALYTICS
    analytics_tabs = st.tabs([
        "🚀 Efficiency",
        "⏳ Time vs Money",
        "⚔️ Head-to-Head",
        "🔥 Heatmap"
    ])


    # -----------------------------------------------------
    # 1. Efficiency Scatter (Active)
    # -----------------------------------------------------
    with analytics_tabs[0]:
        active_sub = filtered_df[filtered_df["status"] == "Active"]

        if not active_sub.empty:
            st.markdown("#### 🚀 Yield Efficiency (Active Trades)")

            fig = px.scatter(
                active_sub,
                x="days_held",
                y="daily_yield",
                color="strategy",
                size="debit",
                hover_data=["name", "pnl"],
                title="Real-Time Efficiency: Yield vs Days Held"
            )

            # Add benchmark lines
            if "130/160" in benchmarks:
                target_yield = benchmarks["130/160"]["yield"]
                fig.add_hline(
                    y=target_yield,
                    line_dash="dot",
                    line_color="blue",
                    annotation_text=f"130/160 Target ({target_yield:.2f}%)"
                )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No active trades available for efficiency analysis.")


    # -----------------------------------------------------
    # 2. PNL vs DIT (Expired)
    # -----------------------------------------------------
    with analytics_tabs[1]:
        expired_sub = filtered_df[filtered_df["status"] == "Expired"]

        if not expired_sub.empty:
            fig = px.scatter(
                expired_sub,
                x="days_held",
                y="pnl",
                color="strategy",
                size="debit",
                hover_data=["name"],
                title="P&L vs Days Held (Expired Trades)"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No expired trades available for analysis.")


    # -----------------------------------------------------
    # 3. Head-to-Head (Strategy Comparison)
    # -----------------------------------------------------
    with analytics_tabs[2]:
        expired_sub = filtered_df[filtered_df["status"] == "Expired"]

        if not expired_sub.empty:
            perf = expired_sub.groupby("strategy").agg({
                "pnl": ["count", "sum", "mean"],
                "days_held": "mean",
                "daily_yield": "mean"
            }).reset_index()

            perf.columns = [
                "Strategy", "Count", "Total P&L",
                "Avg P&L", "Avg Days", "Avg Daily Yield"
            ]

            st.dataframe(
                perf.style.format({
                    "Total P&L": "${:,.0f}",
                    "Avg P&L": "${:,.0f}",
                    "Avg Days": "{:.0f}",
                    "Avg Daily Yield": "{:.2f}%"
                }),
                use_container_width=True
            )
        else:
            st.info("Not enough expired trades for head‑to‑head comparison.")


    # -----------------------------------------------------
    # 4. Heatmap (Profit Density)
    # -----------------------------------------------------
    with analytics_tabs[3]:
        expired_sub = filtered_df[filtered_df["status"] == "Expired"]

        if not expired_sub.empty:
            fig = px.density_heatmap(
                expired_sub,
                x="days_held",
                y="strategy",
                z="pnl",
                histfunc="avg",
                color_continuous_scale="RdBu",
                title="Profit Heatmap — Where Are The Sweet Spots?"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No expired trades to visualize.")

# ---------------------------------------------------------
# TAB 4 — RULE BOOK
# ---------------------------------------------------------
with tab4:
    st.markdown("""
    # 📖 Allantis Trading Constitution

    ### 1. 130/160 Strategy (Income Engine)
    • Target Entry: Monday  
    • Debit Target: $3,500 - $4,500 per lot  
    • Hard Stop: Never pay > $4,800 per lot  
    • Management Rule: Kill if trade is >25 days old and P&L is flat/negative  

    ### 2. 160/190 Strategy (Compounder)
    • Target Entry: Friday  
    • Debit Target: ~$5,200 per lot  
    • Sizing: 1 lot preferred  
    • Do NOT touch first 30 days  
    • Exit around 40–50 days  

    ### 3. M200 Strategy (Whale)
    • Entry: Wednesday  
    • Debit Target: $7,500 - $8,500 per lot  
    • Day‑14 Rule:  
      • If P&L > $200 → Exit or roll  
      • If P&L <= $200 → HOLD (avoid day 15–50 dip valley)  
    """)

    st.divider()
    st.caption("Allantis Trade Guardian v36.0 — DB Powered Edition")
    st.sidebar.divider()

    st.sidebar.markdown("""
    ### 🎯 Quick Start
    1. Upload today's Active file  
    2. Review Portfolio Health in the sidebar  
    3. Check Action Center for urgent trades  
    4. Explore benchmarks & analytics  
    5. All uploaded trades now persist forever in the database  
    """)
