import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import date, datetime, timedelta

# Get the Snowpark session provided by Snowflake
session = get_active_session()

st.set_page_config(page_title="Market Intelligence Platform (MIP)", layout="wide")

st.title("Market Intelligence Platform (MIP)")
st.caption("Snowflake-native POC • AlphaVantage data • All analytics in SQL / SPs")

# Sidebar navigation
page = st.sidebar.radio(
    "Navigation",
    ["Market Overview", "Patterns & Learning", "Signals & Recommendations", "Outcome Evaluation"],
)


# --- Helper functions ---


def run_sql(query: str):
    """Convenience wrapper to run a SQL statement and return a Snowpark DataFrame."""
    return session.sql(query)


def to_pandas(df):
    """Convert Snowpark DataFrame to pandas safely (or return None)."""
    if df is None:
        return None
    try:
        return df.to_pandas()
    except Exception:
        return None


# --- Pages ---


def render_market_overview():
    st.subheader("Market Overview")

    st.markdown(
        """
        This view shows the **latest bar** per symbol and interval from the MART layer:
        `MIP.MART.MARKET_LATEST_PER_SYMBOL`.
        """
    )

    # Optional filters
    col1, col2, col3 = st.columns(3)
    with col1:
        market_type_choice = st.radio(
            "Market type",
            options=["All", "Stocks only", "FX only"],
            index=0,
            horizontal=True,
        )
    with col2:
        symbol_options_df = to_pandas(
            run_sql(
                """
                select distinct SYMBOL
                from MIP.MART.MARKET_LATEST_PER_SYMBOL
                order by SYMBOL
                """
            )
        )
        symbol_choices = (
            symbol_options_df["SYMBOL"].tolist() if symbol_options_df is not None else []
        )
        selected_symbols = st.multiselect(
            "Filter by symbol (optional)",
            options=symbol_choices,
            default=[],
            help="Leave empty for all symbols.",
        )
    with col3:
        interval_filter = st.selectbox(
            "Interval (minutes)", options=["All", 5, 1440], index=0
        )

    base_query = """
        select
            TS,
            SYMBOL,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME,
            INGESTED_AT
        from MIP.MART.MARKET_LATEST_PER_SYMBOL
        where 1 = 1
    """

    filters = []
    if market_type_choice == "Stocks only":
        filters.append("and MARKET_TYPE = 'STOCK'")
    elif market_type_choice == "FX only":
        filters.append("and MARKET_TYPE = 'FX'")

    if selected_symbols:
        symbol_list = ",".join([f"'{sym}'" for sym in selected_symbols])
        filters.append(f"and SYMBOL in ({symbol_list})")

    if interval_filter != "All":
        filters.append(f"and INTERVAL_MINUTES = {interval_filter}")

    query = base_query + "\n" + "\n".join(filters) + "\norder by MARKET_TYPE, SYMBOL;"

    df_sp = run_sql(query)
    df_pd = to_pandas(df_sp)

    if df_pd is None or df_pd.empty:
        st.info("No market data available yet. Try running the ingestion procedure.")
    else:
        st.dataframe(df_pd, use_container_width=True)


def render_patterns_learning():
    st.subheader("Patterns & Learning")

    st.markdown(
        """
        This page shows patterns defined in `MIP.APP.PATTERN_DEFINITION`.
        For now, we only seed a single demo pattern: **MOMENTUM_DEMO**.
        """
    )

    # Option to reseed demo pattern (idempotent)
    if st.button("Seed / refresh MOMENTUM_DEMO pattern"):
        res = run_sql("call MIP.APP.SP_SEED_MIP_DEMO()").collect()
        msg = res[0][0] if res and len(res[0]) > 0 else "Seed procedure completed."
        st.success(msg)

    df_sp = run_sql(
        """
        select
            PATTERN_ID,
            NAME,
            ENABLED,
            DESCRIPTION,
            CREATED_AT,
            CREATED_BY,
            UPDATED_AT,
            UPDATED_BY
        from MIP.APP.PATTERN_DEFINITION
        order by PATTERN_ID
        """
    )
    df_pd = to_pandas(df_sp)

    if df_pd is None or df_pd.empty:
        st.info("No patterns defined yet. Click the button above to seed MOMENTUM_DEMO.")
    else:
        st.dataframe(df_pd, use_container_width=True)


def render_signals_recommendations():
    st.subheader("Signals & Recommendations")

    st.markdown(
        """
        This page triggers the **momentum-based signal** stored procedure
        `MIP.APP.SP_GENERATE_MOMENTUM_RECS(P_MIN_RETURN)` and shows the
        resulting entries from `MIP.APP.RECOMMENDATION_LOG`.

        All analytics and signal logic live inside Snowflake SQL / stored procedures.
        Python is only used to call them and display data.
        """
    )

    with st.form("momentum_form"):
        col1, col2 = st.columns(2)
        with col1:
            min_return = st.number_input(
                "Minimum simple return threshold",
                min_value=0.0,
                max_value=1.0,
                value=0.002,
                step=0.001,
                format="%.4f",
                help="E.g. 0.002 = 0.2% positive return between bars.",
            )
        with col2:
            st.caption("Signals are currently generated only for STOCK, 5-minute bars.")

        submitted = st.form_submit_button("Generate momentum signals")

    if submitted:
        # Call the stored procedure
        call_sql = f"call MIP.APP.SP_GENERATE_MOMENTUM_RECS({min_return})"
        res = run_sql(call_sql).collect()
        msg = res[0][0] if res and len(res[0]) > 0 else "Signal procedure completed."
        st.success(msg)

    st.markdown("### Latest recommendations (joined with pattern name)")

    pattern_df = to_pandas(
        run_sql(
            """
            select PATTERN_NAME
            from MIP.APP.PATTERN_DEFINITION
            order by PATTERN_NAME
            """
        )
    )
    pattern_options = ["All patterns"] + (
        pattern_df["PATTERN_NAME"].tolist() if pattern_df is not None else []
    )
    selected_pattern = st.selectbox("Pattern filter", options=pattern_options, index=0)

    rec_query = """
        select
            r.RECOMMENDATION_ID,
            r.GENERATED_AT,
            p.NAME as PATTERN_NAME,
            r.SYMBOL,
            r.MARKET_TYPE,
            r.INTERVAL_MINUTES,
            r.TS,
            r.SCORE,
            r.DETAILS
        from MIP.APP.RECOMMENDATION_LOG r
        join MIP.APP.PATTERN_DEFINITION p
          on p.PATTERN_ID = r.PATTERN_ID
        where 1=1
    """

    if selected_pattern != "All patterns":
        rec_query += f"\n        and p.PATTERN_NAME = '{selected_pattern}'"

    rec_query += "\n        order by r.GENERATED_AT desc\n        limit 200"

    df_sp = run_sql(rec_query)
    df_pd = to_pandas(df_sp)

    if df_pd is None or df_pd.empty:
        st.info("No recommendations yet. Run the momentum signal above.")
    else:
        st.dataframe(df_pd, use_container_width=True)


def render_outcome_evaluation():
    st.subheader("Outcome Evaluation")

    st.markdown(
        """
        Outcome evaluation will use `MIP.APP.OUTCOME_EVALUATION` to track how
        well recommendations performed after a given time horizon.

        The table exists, but we have not yet implemented the evaluation
        stored procedure. For now, this page just shows the raw table.
        """
    )

    df_sp = run_sql(
        """
        select
            OUTCOME_ID,
            RECOMMENDATION_ID,
            EVALUATED_AT,
            HORIZON_MINUTES,
            RETURN_REALIZED,
            OUTCOME_LABEL,
            DETAILS
        from MIP.APP.OUTCOME_EVALUATION
        order by EVALUATED_AT desc
        limit 200
        """
    )
    df_pd = to_pandas(df_sp)

    if df_pd is None or df_pd.empty:
        st.info("No outcome evaluations recorded yet.")
    else:
        st.dataframe(df_pd, use_container_width=True)

    st.markdown("### Backtesting (historical performance)")

    today = date.today()
    default_from = today - timedelta(days=30)

    with st.expander("Backtest settings", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            horizon_minutes = st.number_input(
                "Horizon (minutes)", min_value=1, value=15, step=1
            )
            hit_threshold = st.number_input(
                "Hit threshold", value=0.002, format="%.6f"
            )
            miss_threshold = st.number_input(
                "Miss threshold", value=-0.002, format="%.6f"
            )
        with col2:
            date_range = st.date_input(
                "Date range",
                value=(default_from, today),
            )

        run_backtest = st.button("Run backtest")

    if run_backtest:
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            from_date, to_date = date_range
        else:
            from_date = default_from
            to_date = today

        from_ts = datetime.combine(from_date, datetime.min.time())
        to_ts = datetime.combine(to_date, datetime.min.time())
        from_ts_str = from_ts.strftime("%Y-%m-%d %H:%M:%S")
        to_ts_str = to_ts.strftime("%Y-%m-%d %H:%M:%S")

        call_sql = f"""
            call MIP.APP.SP_RUN_BACKTEST(
                {int(horizon_minutes)},
                {hit_threshold},
                {miss_threshold},
                to_timestamp_ntz('{from_ts_str}'),
                to_timestamp_ntz('{to_ts_str}'),
                'STOCK',
                5
            )
        """

        with st.spinner("Running backtest..."):
            res = run_sql(call_sql).collect()
        msg = res[0][0] if res and len(res[0]) > 0 else "Backtest procedure completed."
        st.success(msg)

    st.markdown("### Recent backtest runs")

    history_df = to_pandas(
        run_sql(
            """
            select
                BACKTEST_RUN_ID,
                CREATED_AT,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                HORIZON_MINUTES,
                HIT_THRESHOLD,
                MISS_THRESHOLD,
                FROM_TS,
                TO_TS,
                NOTES
            from MIP.APP.BACKTEST_RUN
            order by CREATED_AT desc
            limit 50
            """
        )
    )

    if history_df is None or history_df.empty:
        st.info("No backtest runs recorded yet.")
        return

    st.dataframe(history_df, use_container_width=True)

    st.markdown("### Backtest results")

    selected_run_id = st.selectbox(
        "Select a backtest run",
        options=history_df["BACKTEST_RUN_ID"].tolist(),
    )

    if selected_run_id:
        result_query = f"""
            select
                r.BACKTEST_RUN_ID,
                r.PATTERN_ID,
                p.PATTERN_NAME,
                r.SYMBOL,
                r.TRADE_COUNT,
                r.HIT_COUNT,
                r.MISS_COUNT,
                r.NEUTRAL_COUNT,
                r.HIT_RATE,
                r.AVG_RETURN,
                r.STD_RETURN,
                r.CUM_RETURN
            from MIP.APP.BACKTEST_RESULT r
            left join MIP.APP.PATTERN_DEFINITION p
              on p.PATTERN_ID = r.PATTERN_ID
            where r.BACKTEST_RUN_ID = {selected_run_id}
            order by r.CUM_RETURN desc
        """

        result_df = to_pandas(run_sql(result_query))

        if result_df is None or result_df.empty:
            st.info("No results found for the selected backtest run.")
        else:
            st.dataframe(result_df, use_container_width=True)


# --- Router ---
if page == "Market Overview":
    render_market_overview()
elif page == "Patterns & Learning":
    render_patterns_learning()
elif page == "Signals & Recommendations":
    render_signals_recommendations()
else:
    render_outcome_evaluation()
