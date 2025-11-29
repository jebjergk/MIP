import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
from datetime import date, datetime, timedelta
import json

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


def get_market_selection(key_prefix: str = ""):
    """Shared market selector returning market type and interval minutes."""
    options = {
        "Stocks (5-min)": ("STOCK", 5),
        "FX (Daily)": ("FX", 1440),
    }
    choice = st.radio(
        "Market / timeframe",
        options=list(options.keys()),
        horizontal=True,
        key=f"market_selector_{key_prefix}",
    )
    return options.get(choice, ("STOCK", 5))


# --- Pages ---


def render_market_overview():
    st.subheader("Market Overview")

    st.markdown(
        """
        This view shows the **latest bar** per symbol and interval from the MART layer:
        `MIP.MART.MARKET_LATEST_PER_SYMBOL`.
        """
    )

    st.subheader("Data Ingestion")
    st.caption(
        "Manual ingestion of AlphaVantage bars. "
        "This calls the Snowflake stored procedure MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS() "
        "once. The scheduled task remains suspended by default to keep costs low."
    )

    if st.button("Run ingestion now"):
        with st.spinner("Running AlphaVantage ingestion…"):
            try:
                session.sql("call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS()").collect()
                st.success("Ingestion completed successfully.")
            except Exception as e:
                st.error(f"Ingestion failed: {e}")

    try:
        last_ts_df = session.sql(
            """
            SELECT MAX(TS) AS LAST_TS
            FROM MIP.MART.MARKET_BARS
            """
        ).to_pandas()

        last_ts = last_ts_df["LAST_TS"].iloc[0]
        if last_ts is not None:
            st.caption(f"Last bar timestamp in MARKET_BARS: {last_ts}")
    except Exception:
        st.caption("Could not determine last bar timestamp.")

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

    st.caption(
        "This only (re)creates the demo pattern; it no longer removes other patterns."
    )

    st.markdown("### Learning cycle controls")

    selected_market_type, selected_interval_minutes = get_market_selection(
        "learning_cycle"
    )

    with st.form("learning_cycle_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            horizon_minutes = st.number_input(
                "Horizon (minutes)", min_value=1, value=15, step=1
            )
        with col2:
            hit_threshold = st.number_input(
                "Hit threshold", value=0.002, format="%.6f"
            )
        with col3:
            miss_threshold = st.number_input(
                "Miss threshold", value=-0.002, format="%.6f"
            )

        col_min, col_window = st.columns(2)
        with col_min:
            min_return = st.number_input(
                "Minimum return filter", value=0.0, format="%.6f"
            )

        with col_window:
            window_mode = st.radio(
                "Date window", ["Last N days", "Custom range"], index=0
            )
            if window_mode == "Last N days":
                last_n_days = st.number_input(
                    "Last N days", min_value=1, value=30, step=1
                )
                date_from = datetime.now() - timedelta(days=int(last_n_days))
                date_to = datetime.now()
            else:
                default_from = date.today() - timedelta(days=30)
                default_to = date.today()
                date_range = st.date_input(
                    "From / to (inclusive)", value=(default_from, default_to)
                )
                if (
                    isinstance(date_range, (list, tuple))
                    and len(date_range) == 2
                    and all(date_range)
                ):
                    date_from = datetime.combine(date_range[0], datetime.min.time())
                    date_to = datetime.combine(date_range[1], datetime.max.time())
                else:
                    date_from = datetime.combine(default_from, datetime.min.time())
                    date_to = datetime.combine(default_to, datetime.max.time())

        run_cycle = st.form_submit_button("Run learning cycle")

    if run_cycle:
        from_ts_str = date_from.strftime("%Y-%m-%d %H:%M:%S")
        to_ts_str = date_to.strftime("%Y-%m-%d %H:%M:%S")
        from_ts_sql = f"to_timestamp_ntz('{from_ts_str}')"
        to_ts_sql = f"to_timestamp_ntz('{to_ts_str}')"

        call_sql = f"""
            call MIP.APP.SP_RUN_MIP_LEARNING_CYCLE(
                '{selected_market_type}',
                {selected_interval_minutes},
                {horizon_minutes},
                {min_return},
                {hit_threshold},
                {miss_threshold},
                {from_ts_sql},
                {to_ts_sql},
                TRUE,
                TRUE,
                TRUE,
                TRUE,
                TRUE
            )
        """

        with st.spinner("Running full learning cycle…"):
            try:
                res = run_sql(call_sql).collect()
                summary_raw = res[0][0] if res else None

                # Stored procedure may return a Snowflake OBJECT, JSON string, or plain text
                if isinstance(summary_raw, str):
                    try:
                        summary = json.loads(summary_raw)
                    except Exception:
                        summary = summary_raw
                else:
                    summary = summary_raw

                st.success("Learning cycle completed.")
                if summary is not None:
                    if isinstance(summary, dict):
                        st.caption(
                            "Ran learning cycle for "
                            f"{summary.get('market_type', 'N/A')} / "
                            f"{summary.get('interval_minutes', 'N/A')} with "
                            f"horizon {summary.get('horizon_minutes', 'N/A')} minutes."
                        )
                        st.caption(
                            f"Backtest run ID: {summary.get('backtest_run_id', 'N/A')}, "
                            f"window: {summary.get('from_ts', 'N/A')} → {summary.get('to_ts', 'N/A')}"
                        )
                        try:
                            st.json(summary)
                        except Exception:
                            st.write(summary)
                    else:
                        st.caption(str(summary))
                else:
                    st.info("No summary returned by the stored procedure.")
            except Exception as e:
                st.error(f"Learning cycle failed: {e}")

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

    st.markdown("### Pattern performance (trained metrics)")

    perf_filter = st.radio(
        "Pattern status filter",
        options=["All", "Active", "Inactive"],
        horizontal=True,
    )

    perf_query = """
        SELECT
            PATTERN_ID,
            PATTERN_NAME,
            DESCRIPTION,
            IS_ACTIVE,
            LAST_TRADE_COUNT,
            LAST_HIT_RATE,
            LAST_CUM_RETURN,
            LAST_AVG_RETURN,
            LAST_STD_RETURN,
            PATTERN_SCORE,
            LAST_TRAINED_AT,
            LAST_BACKTEST_RUN_ID
        FROM MIP.APP.PATTERN_DEFINITION
        ORDER BY PATTERN_SCORE DESC NULLS LAST
    """

    perf_df = to_pandas(run_sql(perf_query))

    if perf_df is None or perf_df.empty:
        st.info("No pattern performance metrics available yet.")
        return

    if perf_filter == "Active":
        perf_df = perf_df[perf_df["IS_ACTIVE"] == "Y"]
    elif perf_filter == "Inactive":
        perf_df = perf_df[perf_df["IS_ACTIVE"] != "Y"]

    if perf_df.empty:
        st.info("No patterns match the selected filter.")
        return

    display_cols = [
        "PATTERN_NAME",
        "IS_ACTIVE",
        "LAST_TRADE_COUNT",
        "LAST_HIT_RATE",
        "LAST_CUM_RETURN",
        "PATTERN_SCORE",
        "LAST_BACKTEST_RUN_ID",
        "LAST_TRAINED_AT",
    ]

    st.dataframe(perf_df[display_cols], use_container_width=True)

    chart_df = perf_df.copy()
    chart_df = chart_df[chart_df["PATTERN_SCORE"].notna()]
    chart_df["PATTERN_SCORE"] = chart_df["PATTERN_SCORE"].astype(float)
    chart_df["PATTERN_NAME"] = chart_df["PATTERN_NAME"].astype(str)
    chart_df["IS_ACTIVE"] = chart_df["IS_ACTIVE"].astype(str)

    if chart_df.empty:
        return

    score_chart = (
        alt.Chart(chart_df.head(20))
        .mark_bar()
        .encode(
            x=alt.X("PATTERN_SCORE:Q", title="Pattern score"),
            y=alt.Y("PATTERN_NAME:N", sort="-x", title="Pattern"),
            color=alt.Color("IS_ACTIVE:N", title="Active"),
            tooltip=[
                "PATTERN_NAME",
                "PATTERN_SCORE",
                "IS_ACTIVE",
                "LAST_TRADE_COUNT",
                "LAST_HIT_RATE",
                "LAST_CUM_RETURN",
            ],
        )
        .properties(height=400, title="Top patterns by score")
    )

    st.altair_chart(score_chart, use_container_width=True)


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

    selected_market_type, selected_interval_minutes = get_market_selection("signals")

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
            st.caption("Momentum signals will run for the selected market and interval.")

        submitted = st.form_submit_button("Generate momentum signals")

    if submitted:
        # Call the stored procedure
        call_sql = f"call MIP.APP.SP_GENERATE_MOMENTUM_RECS({min_return}, '{selected_market_type}', {selected_interval_minutes})"
        res = run_sql(call_sql).collect()
        msg = res[0][0] if res and len(res[0]) > 0 else "Signal procedure completed."
        st.success(msg)

    st.markdown("### Latest recommendations (joined with pattern name)")

    pattern_df = to_pandas(
        run_sql(
            """
            select NAME as PATTERN_NAME
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
        where r.MARKET_TYPE = '{selected_market_type}'
          and r.INTERVAL_MINUTES = {selected_interval_minutes}
    """

    if selected_pattern != "All patterns":
        rec_query += f"\n        and p.NAME = '{selected_pattern}'"

    rec_query += "\n        order by r.GENERATED_AT desc\n        limit 200"

    df_sp = run_sql(rec_query)
    df_pd = to_pandas(df_sp)

    if df_pd is None or df_pd.empty:
        st.info("No recommendations yet. Run the momentum signal above.")
    else:
        st.dataframe(df_pd, use_container_width=True)

    st.markdown("### Signal visualizer")

    symbol_options_df = to_pandas(
        run_sql(
            f"""
            select distinct SYMBOL
            from (
                select SYMBOL
                from MIP.MART.MARKET_BARS
                where MARKET_TYPE = '{selected_market_type}'
                  and INTERVAL_MINUTES = {selected_interval_minutes}
                union
                select SYMBOL
                from MIP.APP.RECOMMENDATION_LOG
                where MARKET_TYPE = '{selected_market_type}'
                  and INTERVAL_MINUTES = {selected_interval_minutes}
            )
            order by SYMBOL
            """
        )
    )

    symbol_choices = (
        symbol_options_df["SYMBOL"].tolist() if symbol_options_df is not None else []
    )

    if not symbol_choices:
        st.info("No symbols available for the selected market / interval yet.")
        return

    col_symbol, col_window = st.columns([2, 1])
    with col_symbol:
        selected_symbol = st.selectbox(
            "Select symbol", options=symbol_choices, key="signal_symbol_selector"
        )
    with col_window:
        window_days = st.number_input(
            "Days of history", min_value=1, max_value=60, value=7, step=1
        )

    from_ts = datetime.now() - timedelta(days=window_days)
    from_ts_str = from_ts.strftime("%Y-%m-%d %H:%M:%S")

    price_df = to_pandas(
        run_sql(
            f"""
            select
                TS,
                CLOSE
            from MIP.MART.MARKET_BARS
            where SYMBOL = '{selected_symbol}'
              and MARKET_TYPE = '{selected_market_type}'
              and INTERVAL_MINUTES = {selected_interval_minutes}
              and TS >= to_timestamp_ntz('{from_ts_str}')
            order by TS
            """
        )
    )

    rec_vis_df = to_pandas(
        run_sql(
            f"""
            select
                r.TS,
                r.RECOMMENDATION_ID,
                coalesce(p.NAME, concat('Pattern ', r.PATTERN_ID)) as PATTERN_NAME,
                mb.CLOSE
            from MIP.APP.RECOMMENDATION_LOG r
            left join MIP.APP.PATTERN_DEFINITION p
              on p.PATTERN_ID = r.PATTERN_ID
            left join MIP.MART.MARKET_BARS mb
              on mb.SYMBOL = r.SYMBOL
             and mb.MARKET_TYPE = r.MARKET_TYPE
             and mb.INTERVAL_MINUTES = r.INTERVAL_MINUTES
             and mb.TS = r.TS
            where r.SYMBOL = '{selected_symbol}'
              and r.MARKET_TYPE = '{selected_market_type}'
              and r.INTERVAL_MINUTES = {selected_interval_minutes}
              and r.TS >= to_timestamp_ntz('{from_ts_str}')
            order by r.TS
            """
        )
    )

    if price_df is None or price_df.empty:
        st.info("No price data available for the selected symbol / window.")
        return

    price_df["CLOSE"] = price_df["CLOSE"].astype(float)

    price_line = (
        alt.Chart(price_df)
        .mark_line()
        .encode(
            x=alt.X("TS:T", title="Time"),
            y=alt.Y("CLOSE:Q", title="Price"),
            tooltip=["TS", "CLOSE"],
        )
    )

    if rec_vis_df is not None and not rec_vis_df.empty:
        rec_vis_df["CLOSE"] = rec_vis_df["CLOSE"].astype(float)
        rec_vis_df["PATTERN_NAME"] = rec_vis_df["PATTERN_NAME"].astype(str)

        signal_points = (
            alt.Chart(rec_vis_df)
            .mark_point(size=80, shape="triangle-up")
            .encode(
                x="TS:T",
                y="CLOSE:Q",
                color=alt.Color("PATTERN_NAME:N", title="Pattern"),
                tooltip=["TS", "CLOSE", "PATTERN_NAME"],
            )
        )

        chart = (price_line + signal_points).properties(height=400)
    else:
        chart = price_line.properties(height=400)

    st.altair_chart(chart, use_container_width=True)


def render_outcome_evaluation():
    st.subheader("Outcome Evaluation")

    st.markdown(
        """
        Outcome evaluation uses `MIP.APP.SP_EVALUATE_MOMENTUM_OUTCOMES` to track how
        well recommendations performed after a given time horizon for the selected
        market and interval.
        """
    )

    selected_market_type, selected_interval_minutes = get_market_selection("outcomes")

    with st.form("outcome_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            horizon_minutes = st.number_input(
                "Horizon (minutes)", min_value=1, value=15, step=1
            )
        with col2:
            hit_threshold = st.number_input(
                "Hit threshold", value=0.002, format="%.6f"
            )
        with col3:
            miss_threshold = st.number_input(
                "Miss threshold", value=-0.002, format="%.6f"
            )

        evaluate = st.form_submit_button("Evaluate outcomes")

    if evaluate:
        call_sql = f"""
            call MIP.APP.SP_EVALUATE_MOMENTUM_OUTCOMES(
                {int(horizon_minutes)},
                {hit_threshold},
                {miss_threshold},
                '{selected_market_type}',
                {selected_interval_minutes}
            )
        """
        with st.spinner("Evaluating outcomes..."):
            res = run_sql(call_sql).collect()
        msg = res[0][0] if res and len(res[0]) > 0 else "Outcome evaluation completed."
        st.success(msg)

    outcome_query = f"""
        select
            o.OUTCOME_ID,
            o.RECOMMENDATION_ID,
            o.EVALUATED_AT,
            o.HORIZON_MINUTES,
            o.RETURN_REALIZED,
            o.OUTCOME_LABEL,
            o.DETAILS,
            r.SYMBOL,
            r.MARKET_TYPE,
            r.INTERVAL_MINUTES
        from MIP.APP.OUTCOME_EVALUATION o
        join MIP.APP.RECOMMENDATION_LOG r
          on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
        where r.MARKET_TYPE = '{selected_market_type}'
          and r.INTERVAL_MINUTES = {selected_interval_minutes}
        order by o.EVALUATED_AT desc
        limit 200
    """

    df_sp = run_sql(outcome_query)
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
                '{selected_market_type}',
                {selected_interval_minutes}
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
            where MARKET_TYPE = '{selected_market_type}'
              and INTERVAL_MINUTES = {selected_interval_minutes}
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
                p.NAME as PATTERN_NAME,
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

            pattern_perf_df = to_pandas(
                run_sql(
                    f"""
                    select
                        r.PATTERN_ID,
                        coalesce(p.NAME, concat('Pattern ', r.PATTERN_ID)) as PATTERN_NAME,
                        r.TRADE_COUNT,
                        r.CUM_RETURN
                    from MIP.APP.BACKTEST_RESULT r
                    left join MIP.APP.PATTERN_DEFINITION p
                      on p.PATTERN_ID = r.PATTERN_ID
                    where r.BACKTEST_RUN_ID = {selected_run_id}
                    order by r.CUM_RETURN desc
                    """
                )
            )

            if pattern_perf_df is not None and not pattern_perf_df.empty:
                df = pattern_perf_df.copy().reset_index(drop=True)

                for col in ["CUM_RETURN", "HIT_RATE", "TRADE_COUNT"]:
                    if col in df.columns:
                        df[col] = df[col].astype(float)

                df["PATTERN_NAME"] = df["PATTERN_NAME"].astype(str)

                if df.empty:
                    st.info("No backtest results to display.")
                    return

                df = df.sort_values("CUM_RETURN", ascending=False)

                cum_chart = (
                    alt.Chart(df.head(25))
                    .mark_bar()
                    .encode(
                        x=alt.X("CUM_RETURN:Q", title="Cumulative return"),
                        y=alt.Y("PATTERN_NAME:N", sort="-x", title="Pattern"),
                        tooltip=[
                            "PATTERN_NAME",
                            "CUM_RETURN",
                            "TRADE_COUNT",
                            "HIT_RATE",
                        ],
                    )
                    .properties(height=400)
                )

                st.altair_chart(cum_chart, use_container_width=True)

                if "HIT_RATE" in df.columns:
                    hit_chart = (
                        alt.Chart(df.head(25))
                        .mark_bar()
                        .encode(
                            x=alt.X(
                                "HIT_RATE:Q",
                                title="Hit rate",
                                axis=alt.Axis(format=".0%"),
                            ),
                            y=alt.Y("PATTERN_NAME:N", sort="-x", title="Pattern"),
                            tooltip=["PATTERN_NAME", "HIT_RATE", "TRADE_COUNT"],
                        )
                        .properties(height=400)
                    )

                    st.altair_chart(hit_chart, use_container_width=True)

                scatter_df = to_pandas(
                    run_sql(
                        f"""
                        select
                            r.TRADE_COUNT,
                            r.HIT_RATE,
                            coalesce(p.NAME, concat('Pattern ', r.PATTERN_ID)) as PATTERN_NAME
                        from MIP.APP.BACKTEST_RESULT r
                        left join MIP.APP.PATTERN_DEFINITION p
                          on p.PATTERN_ID = r.PATTERN_ID
                        where r.BACKTEST_RUN_ID = {selected_run_id}
                        order by r.TRADE_COUNT desc
                        """
                    )
                )

                if scatter_df is not None and not scatter_df.empty:
                    scatter_chart = alt.Chart(scatter_df).mark_circle(size=80).encode(
                        x=alt.X("TRADE_COUNT:Q", title="Trade count"),
                        y=alt.Y("HIT_RATE:Q", title="Hit rate"),
                        tooltip=["PATTERN_NAME:N", "TRADE_COUNT:Q", "HIT_RATE:Q"],
                    ).properties(title="Hit rate vs trade count")

                    st.altair_chart(scatter_chart, use_container_width=True)


# --- Router ---
if page == "Market Overview":
    render_market_overview()
elif page == "Patterns & Learning":
    render_patterns_learning()
elif page == "Signals & Recommendations":
    render_signals_recommendations()
else:
    render_outcome_evaluation()
