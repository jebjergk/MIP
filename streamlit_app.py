import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
from datetime import date, datetime, timedelta
import json
import math
import pandas as pd

# Get the Snowpark session provided by Snowflake
session = get_active_session()

st.set_page_config(page_title="Market Intelligence Platform (MIP)", layout="wide")

st.title("Market Intelligence Platform (MIP)")
st.caption("Snowflake-native POC • AlphaVantage data • All analytics in SQL / SPs")

# Sidebar navigation
page = st.sidebar.radio(
    "Navigation",
    [
        "Morning Brief",
        "Ingestion",
        "Market Overview",
        "Patterns & Learning",
        "Signals & Recommendations",
        "Outcome Evaluation",
    ],
)


# --- Helper functions ---


SIGNAL_GENERATOR_SP = "MIP.APP.SP_GENERATE_MOMENTUM_RECS"


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


def get_market_timeframe_options(conn):
    """Return market/timeframe tuples from active pattern definitions."""
    query = """
        select
            coalesce(PARAMS_JSON:market_type::string, 'STOCK') as MARKET_TYPE,
            coalesce(PARAMS_JSON:interval_minutes::number, 1440) as INTERVAL_MINUTES
        from MIP.APP.PATTERN_DEFINITION
        where coalesce(IS_ACTIVE, 'N') = 'Y'
          and coalesce(ENABLED, true)
        order by MARKET_TYPE, INTERVAL_MINUTES
    """
    rows = conn.sql(query).collect()
    return [(row["MARKET_TYPE"], int(row["INTERVAL_MINUTES"])) for row in rows]


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


def sql_literal(value):
    """Render a Python value as a SQL literal."""
    if value is None:
        return "null"
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


def normalize_optional_value(value):
    """Normalize pandas/NaN values to None for SQL."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def fetch_ingest_universe():
    query = """
        select
            SYMBOL,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            IS_ENABLED,
            PRIORITY,
            CREATED_AT,
            NOTES
        from MIP.APP.INGEST_UNIVERSE
        order by PRIORITY desc, MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
    """
    return to_pandas(run_sql(query))


def save_ingest_universe_updates(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return

    if "TO_DELETE" in df.columns:
        delete_mask = df["TO_DELETE"].fillna(False)
    else:
        delete_mask = pd.Series(False, index=df.index)
    delete_rows = df[delete_mask].copy()
    upsert_rows = df[~delete_mask].copy()

    if not delete_rows.empty:
        delete_tuples = ",\n                ".join(
            f"({sql_literal(row['MARKET_TYPE'])}, {sql_literal(row['SYMBOL'])}, "
            f"{sql_literal(int(row['INTERVAL_MINUTES']))})"
            for _, row in delete_rows.iterrows()
        )
        delete_sql = f"""
            delete from MIP.APP.INGEST_UNIVERSE
             where (MARKET_TYPE, SYMBOL, INTERVAL_MINUTES) in (
                {delete_tuples}
             )
        """
        run_sql(delete_sql).collect()

    if upsert_rows.empty:
        return

    values_rows = ",\n                ".join(
        f"({sql_literal(row['SYMBOL'])}, {sql_literal(row['MARKET_TYPE'])}, "
        f"{sql_literal(int(row['INTERVAL_MINUTES']))}, {sql_literal(bool(row['IS_ENABLED']))}, "
        f"{sql_literal(int(row['PRIORITY']))}, {sql_literal(normalize_optional_value(row.get('NOTES')))})"
        for _, row in upsert_rows.iterrows()
    )

    merge_sql = f"""
        merge into MIP.APP.INGEST_UNIVERSE t
        using (
            select
                column1 as SYMBOL,
                column2 as MARKET_TYPE,
                column3 as INTERVAL_MINUTES,
                column4 as IS_ENABLED,
                column5 as PRIORITY,
                column6 as NOTES
            from values
                {values_rows}
        ) s
           on t.SYMBOL = s.SYMBOL
          and t.MARKET_TYPE = s.MARKET_TYPE
          and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
        when matched then update set
            t.IS_ENABLED = s.IS_ENABLED,
            t.PRIORITY = s.PRIORITY,
            t.NOTES = s.NOTES
        when not matched then insert (
            SYMBOL,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            IS_ENABLED,
            PRIORITY,
            NOTES
        ) values (
            s.SYMBOL,
            s.MARKET_TYPE,
            s.INTERVAL_MINUTES,
            s.IS_ENABLED,
            s.PRIORITY,
            s.NOTES
        )
    """
    run_sql(merge_sql).collect()


def run_momentum_generator(
    min_return: float,
    market_type: str,
    interval_minutes: int,
    lookback_days: int | None = None,
    min_zscore: float | None = None,
):
    """Call the canonical momentum signal generator stored procedure."""

    call_sql = (
        "call "
        f"{SIGNAL_GENERATOR_SP}("
        f"{sql_literal(min_return)}, "
        f"{sql_literal(market_type)}, "
        f"{sql_literal(interval_minutes)}, "
        f"{sql_literal(lookback_days)}, "
        f"{sql_literal(min_zscore)})"
    )
    res = run_sql(call_sql).collect()
    return res[0][0] if res and len(res[0]) > 0 else "Signal procedure completed."


def run_post_ingest_health_checks():
    """Run basic post-ingest data quality checks and return structured results."""
    results = {
        "errors": [],
        "counts": None,
        "dup_bars_summary": None,
        "dup_bars_samples": None,
        "dup_returns_summary": None,
        "dup_returns_samples": None,
        "recent_ts": None,
    }

    try:
        counts_df = to_pandas(
            run_sql(
                """
                select
                    count(*) as TOTAL_ROWS,
                    max(INGESTED_AT) as LAST_INGESTED_AT,
                    sum(
                        case
                            when INGESTED_AT = (
                                select max(INGESTED_AT) from MIP.MART.MARKET_BARS
                            )
                            then 1
                            else 0
                        end
                    ) as LAST_INGEST_ROWS
                from MIP.MART.MARKET_BARS
                """
            )
        )
        results["counts"] = counts_df
    except Exception as exc:
        results["errors"].append(f"Row count check failed: {exc}")

    try:
        dup_bars_summary = to_pandas(
            run_sql(
                """
                with dupes as (
                    select
                        MARKET_TYPE,
                        SYMBOL,
                        INTERVAL_MINUTES,
                        TS,
                        count(*) as DUP_COUNT
                    from MIP.MART.MARKET_BARS
                    group by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS
                    having count(*) > 1
                )
                select
                    count(*) as DUP_KEYS,
                    sum(DUP_COUNT - 1) as DUP_EXTRA_ROWS
                from dupes
                """
            )
        )
        dup_bars_samples = to_pandas(
            run_sql(
                """
                select
                    MARKET_TYPE,
                    SYMBOL,
                    INTERVAL_MINUTES,
                    TS,
                    count(*) as DUP_COUNT
                from MIP.MART.MARKET_BARS
                group by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS
                having count(*) > 1
                order by DUP_COUNT desc, TS desc
                limit 5
                """
            )
        )
        results["dup_bars_summary"] = dup_bars_summary
        results["dup_bars_samples"] = dup_bars_samples
    except Exception as exc:
        results["errors"].append(f"MARKET_BARS duplicate check failed: {exc}")

    try:
        dup_returns_summary = to_pandas(
            run_sql(
                """
                with dupes as (
                    select
                        MARKET_TYPE,
                        SYMBOL,
                        INTERVAL_MINUTES,
                        TS,
                        count(*) as DUP_COUNT
                    from MIP.MART.MARKET_RETURNS
                    group by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS
                    having count(*) > 1
                )
                select
                    count(*) as DUP_KEYS,
                    sum(DUP_COUNT - 1) as DUP_EXTRA_ROWS
                from dupes
                """
            )
        )
        dup_returns_samples = to_pandas(
            run_sql(
                """
                select
                    MARKET_TYPE,
                    SYMBOL,
                    INTERVAL_MINUTES,
                    TS,
                    count(*) as DUP_COUNT
                from MIP.MART.MARKET_RETURNS
                group by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS
                having count(*) > 1
                order by DUP_COUNT desc, TS desc
                limit 5
                """
            )
        )
        results["dup_returns_summary"] = dup_returns_summary
        results["dup_returns_samples"] = dup_returns_samples
    except Exception as exc:
        results["errors"].append(f"MARKET_RETURNS duplicate check failed: {exc}")

    try:
        recent_ts_df = to_pandas(
            run_sql(
                """
                select
                    MARKET_TYPE,
                    INTERVAL_MINUTES,
                    max(TS) as MOST_RECENT_TS,
                    count(*) as ROWS_IN_MARKET
                from MIP.MART.MARKET_BARS
                group by MARKET_TYPE, INTERVAL_MINUTES
                order by MARKET_TYPE, INTERVAL_MINUTES
                """
            )
        )
        results["recent_ts"] = recent_ts_df
    except Exception as exc:
        results["errors"].append(f"Most recent TS check failed: {exc}")

    return results


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


def render_ingestion():
    st.subheader("Ingestion")
    st.caption(
        "Manual ingestion of AlphaVantage bars. "
        "This calls the Snowflake stored procedure MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS() "
        "once. The scheduled task remains suspended by default to keep costs low."
    )

    if st.button("Run ingestion now"):
        with st.spinner("Running AlphaVantage ingestion…"):
            try:
                res = session.sql("call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS()").collect()
                msg = res[0][0] if res and len(res[0]) > 0 else "Ingestion completed successfully."
                st.success(msg)
                st.session_state["post_ingest_checks"] = run_post_ingest_health_checks()
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

    st.markdown("### Ingest universe admin")
    st.caption("View and manage the enabled ingestion universe for AlphaVantage.")

    if "ingest_universe_df" not in st.session_state:
        universe_df = fetch_ingest_universe()
        if universe_df is None or universe_df.empty:
            universe_df = pd.DataFrame(
                columns=[
                    "SYMBOL",
                    "MARKET_TYPE",
                    "INTERVAL_MINUTES",
                    "IS_ENABLED",
                    "PRIORITY",
                    "CREATED_AT",
                    "NOTES",
                ]
            )
        universe_df = universe_df.copy()
        universe_df["TO_DELETE"] = False
        st.session_state["ingest_universe_df"] = universe_df

    if st.button("Add symbol"):
        new_row = pd.DataFrame(
            [
                {
                    "SYMBOL": "",
                    "MARKET_TYPE": "STOCK",
                    "INTERVAL_MINUTES": 1440,
                    "IS_ENABLED": True,
                    "PRIORITY": 100,
                    "CREATED_AT": None,
                    "NOTES": "",
                    "TO_DELETE": False,
                }
            ]
        )
        st.session_state["ingest_universe_df"] = pd.concat(
            [st.session_state["ingest_universe_df"], new_row],
            ignore_index=True,
        )
        st.rerun()

    display_df = st.session_state["ingest_universe_df"].copy()
    if not display_df.empty:
        display_df = display_df.sort_values(
            by=["IS_ENABLED", "PRIORITY", "MARKET_TYPE", "SYMBOL", "INTERVAL_MINUTES"],
            ascending=[False, False, True, True, True],
            na_position="last",
        )

    edited_df = st.data_editor(
        display_df,
        use_container_width=True,
        disabled=["CREATED_AT"],
        column_config={
            "IS_ENABLED": st.column_config.CheckboxColumn("Enabled"),
            "PRIORITY": st.column_config.NumberColumn("Priority", step=1),
            "CREATED_AT": st.column_config.DatetimeColumn("Created at"),
            "TO_DELETE": st.column_config.CheckboxColumn("Delete"),
        },
        key="ingest_universe_editor",
    )
    st.session_state["ingest_universe_df"] = edited_df

    enabled_count = int(
        edited_df.loc[~edited_df["TO_DELETE"].fillna(False), "IS_ENABLED"]
        .fillna(False)
        .sum()
    )
    if enabled_count > 25:
        st.warning(
            "More than 25 symbols are enabled. Ingestion will process only the top "
            "25 by priority."
        )

    if st.button("Save universe updates"):
        cleaned_df = edited_df.copy()
        cleaned_df["SYMBOL"] = cleaned_df["SYMBOL"].astype(str).str.strip().str.upper()
        cleaned_df["MARKET_TYPE"] = (
            cleaned_df["MARKET_TYPE"].astype(str).str.strip().str.upper()
        )
        cleaned_df["INTERVAL_MINUTES"] = pd.to_numeric(
            cleaned_df["INTERVAL_MINUTES"], errors="coerce"
        )
        cleaned_df["PRIORITY"] = (
            pd.to_numeric(cleaned_df["PRIORITY"], errors="coerce").fillna(0).astype(int)
        )
        cleaned_df["IS_ENABLED"] = cleaned_df["IS_ENABLED"].fillna(False).astype(bool)
        cleaned_df["TO_DELETE"] = cleaned_df["TO_DELETE"].fillna(False).astype(bool)

        missing_mask = (
            cleaned_df["SYMBOL"].eq("")
            | cleaned_df["MARKET_TYPE"].eq("")
            | cleaned_df["INTERVAL_MINUTES"].isna()
        )
        if missing_mask.any():
            st.error("All rows must include market type, symbol, and interval minutes.")
        else:
            duplicate_mask = cleaned_df.loc[~cleaned_df["TO_DELETE"]].duplicated(
                ["MARKET_TYPE", "SYMBOL", "INTERVAL_MINUTES"], keep=False
            )
            if duplicate_mask.any():
                st.error("Duplicate rows found for the same market type, symbol, and interval.")
            else:
                cleaned_df["INTERVAL_MINUTES"] = cleaned_df["INTERVAL_MINUTES"].astype(
                    int
                )
                save_ingest_universe_updates(cleaned_df)
                st.success("Ingest universe updated.")
                st.session_state.pop("ingest_universe_df", None)
                st.rerun()

    st.markdown("### Post-ingest health checks")
    st.caption(
        "Lightweight checks after ingestion: row counts, duplicates by natural key, "
        "and the most recent timestamp per market/interval."
    )

    if st.button("Run health checks", key="run_post_ingest_checks"):
        st.session_state["post_ingest_checks"] = run_post_ingest_health_checks()

    checks = st.session_state.get("post_ingest_checks")

    if checks:
        errors = checks.get("errors", [])
        counts_df = checks.get("counts")
        dup_bars_summary = checks.get("dup_bars_summary")
        dup_returns_summary = checks.get("dup_returns_summary")

        dup_bars_keys = (
            int(dup_bars_summary["DUP_KEYS"].iloc[0])
            if dup_bars_summary is not None and not dup_bars_summary.empty
            else 0
        )
        dup_returns_keys = (
            int(dup_returns_summary["DUP_KEYS"].iloc[0])
            if dup_returns_summary is not None and not dup_returns_summary.empty
            else 0
        )

        total_rows = (
            int(counts_df["TOTAL_ROWS"].iloc[0])
            if counts_df is not None and not counts_df.empty
            else 0
        )

        if errors:
            status = "FAIL"
        elif dup_bars_keys > 0 or dup_returns_keys > 0:
            status = "FAIL"
        elif total_rows == 0:
            status = "WARN"
        else:
            status = "OK"

        if status == "OK":
            st.success("Post-ingest status: OK")
        elif status == "WARN":
            st.warning("Post-ingest status: WARN")
        else:
            st.error("Post-ingest status: FAIL")

        if errors:
            st.write("Errors encountered:")
            for err in errors:
                st.write(f"- {err}")

        if counts_df is not None and not counts_df.empty:
            last_ingest_ts = counts_df["LAST_INGESTED_AT"].iloc[0]
            last_ingest_rows = counts_df["LAST_INGEST_ROWS"].iloc[0]
            st.caption(
                f"Total rows in MARKET_BARS: {total_rows:,}. "
                f"Rows ingested/merged in latest batch: {last_ingest_rows:,}. "
                f"Latest INGESTED_AT: {last_ingest_ts}."
            )

        if dup_bars_summary is not None and not dup_bars_summary.empty:
            st.caption(
                "MARKET_BARS duplicates by (MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS): "
                f"{dup_bars_keys} duplicate keys."
            )

        if dup_returns_summary is not None and not dup_returns_summary.empty:
            st.caption(
                "MARKET_RETURNS duplicates by (MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS): "
                f"{dup_returns_keys} duplicate keys."
            )

        recent_ts_df = checks.get("recent_ts")
        if recent_ts_df is not None and not recent_ts_df.empty:
            st.markdown("**Most recent bar timestamp per market/interval**")
            st.dataframe(recent_ts_df, use_container_width=True)

        dup_bars_samples = checks.get("dup_bars_samples")
        if dup_bars_samples is not None and not dup_bars_samples.empty:
            st.markdown("**Sample duplicate keys in MARKET_BARS**")
            st.dataframe(dup_bars_samples, use_container_width=True)

        dup_returns_samples = checks.get("dup_returns_samples")
        if dup_returns_samples is not None and not dup_returns_samples.empty:
            st.markdown("**Sample duplicate keys in MARKET_RETURNS**")
            st.dataframe(dup_returns_samples, use_container_width=True)


def render_morning_brief():
    st.subheader("Morning Brief")
    st.caption(
        "Snapshot of the most recent recommendations, outcome KPIs, top signals, and "
        "data health checks. Facts only, based on stored tables."
    )

    selected_market_type, selected_interval_minutes = get_market_selection("morning")

    st.markdown("### Latest recommendations")

    latest_run_df = to_pandas(
        run_sql(
            f"""
            select max(GENERATED_AT) as GENERATED_AT
            from MIP.APP.RECOMMENDATION_LOG
            where MARKET_TYPE = '{selected_market_type}'
              and INTERVAL_MINUTES = {selected_interval_minutes}
            """
        )
    )
    latest_run_ts = (
        latest_run_df["GENERATED_AT"].iloc[0]
        if latest_run_df is not None and not latest_run_df.empty
        else None
    )
    latest_run_ts_str = None
    if latest_run_ts is not None:
        try:
            latest_run_ts_str = latest_run_ts.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            latest_run_ts_str = str(latest_run_ts)
    if latest_run_ts_str == "NaT":
        latest_run_ts_str = None

    latest_tab, last_day_tab = st.tabs(["Latest run", "Last 24 hours"])

    with latest_tab:
        if latest_run_ts_str is None:
            st.info("No recommendation runs found for the selected market/interval.")
        else:
            st.caption(f"Latest run timestamp: {latest_run_ts_str}")

            latest_counts_df = to_pandas(
                run_sql(
                    f"""
                    select
                        coalesce(p.NAME, concat('Pattern ', r.PATTERN_ID)) as PATTERN_NAME,
                        count(*) as RECOMMENDATION_COUNT
                    from MIP.APP.RECOMMENDATION_LOG r
                    left join MIP.APP.PATTERN_DEFINITION p
                      on p.PATTERN_ID = r.PATTERN_ID
                    where r.MARKET_TYPE = '{selected_market_type}'
                      and r.INTERVAL_MINUTES = {selected_interval_minutes}
                      and r.GENERATED_AT = to_timestamp_ntz('{latest_run_ts_str}')
                    group by PATTERN_NAME
                    order by RECOMMENDATION_COUNT desc, PATTERN_NAME
                    """
                )
            )

            if latest_counts_df is None or latest_counts_df.empty:
                st.info("No recommendations logged for the latest run.")
            else:
                st.markdown("**Counts by pattern**")
                st.dataframe(latest_counts_df, use_container_width=True)

            latest_recs_df = to_pandas(
                run_sql(
                    f"""
                    select
                        r.GENERATED_AT,
                        coalesce(p.NAME, concat('Pattern ', r.PATTERN_ID)) as PATTERN_NAME,
                        r.SYMBOL,
                        r.TS,
                        r.SCORE
                    from MIP.APP.RECOMMENDATION_LOG r
                    left join MIP.APP.PATTERN_DEFINITION p
                      on p.PATTERN_ID = r.PATTERN_ID
                    where r.MARKET_TYPE = '{selected_market_type}'
                      and r.INTERVAL_MINUTES = {selected_interval_minutes}
                      and r.GENERATED_AT = to_timestamp_ntz('{latest_run_ts_str}')
                    order by r.SCORE desc nulls last, r.TS desc
                    limit 50
                    """
                )
            )

            st.markdown("**Latest run recommendations (top 50 by score)**")
            if latest_recs_df is None or latest_recs_df.empty:
                st.info("No recommendations available for the latest run.")
            else:
                st.dataframe(latest_recs_df, use_container_width=True)

    with last_day_tab:
        st.caption("Last 24 hours of recommendation activity.")

        recent_counts_df = to_pandas(
            run_sql(
                f"""
                select
                    coalesce(p.NAME, concat('Pattern ', r.PATTERN_ID)) as PATTERN_NAME,
                    count(*) as RECOMMENDATION_COUNT
                from MIP.APP.RECOMMENDATION_LOG r
                left join MIP.APP.PATTERN_DEFINITION p
                  on p.PATTERN_ID = r.PATTERN_ID
                where r.MARKET_TYPE = '{selected_market_type}'
                  and r.INTERVAL_MINUTES = {selected_interval_minutes}
                  and r.GENERATED_AT >= dateadd('day', -1, current_timestamp())
                group by PATTERN_NAME
                order by RECOMMENDATION_COUNT desc, PATTERN_NAME
                """
            )
        )

        if recent_counts_df is None or recent_counts_df.empty:
            st.info("No recommendations in the last 24 hours.")
        else:
            st.markdown("**Counts by pattern (last 24 hours)**")
            st.dataframe(recent_counts_df, use_container_width=True)

        top_symbols_df = to_pandas(
            run_sql(
                f"""
                select
                    r.SYMBOL,
                    count(*) as SIGNAL_COUNT
                from MIP.APP.RECOMMENDATION_LOG r
                where r.MARKET_TYPE = '{selected_market_type}'
                  and r.INTERVAL_MINUTES = {selected_interval_minutes}
                  and r.GENERATED_AT >= dateadd('day', -1, current_timestamp())
                group by r.SYMBOL
                order by SIGNAL_COUNT desc, r.SYMBOL
                limit 20
                """
            )
        )

        st.markdown("**Top symbols (last 24 hours)**")
        if top_symbols_df is None or top_symbols_df.empty:
            st.info("No symbol activity in the last 24 hours.")
        else:
            st.dataframe(top_symbols_df, use_container_width=True)

    st.markdown("### Outcome KPIs by pattern (last 30 days)")

    outcome_kpi_df = to_pandas(
        run_sql(
            f"""
            select
                coalesce(p.NAME, concat('Pattern ', r.PATTERN_ID)) as PATTERN_NAME,
                count(*) as OUTCOME_COUNT,
                sum(case when o.OUTCOME_LABEL = 'HIT' then 1 else 0 end) as HIT_COUNT,
                avg(case when o.OUTCOME_LABEL = 'HIT' then 1 else 0 end) as HIT_RATE,
                avg(o.RETURN_REALIZED) as AVG_RETURN,
                sum(o.RETURN_REALIZED) as CUM_RETURN
            from MIP.APP.OUTCOME_EVALUATION o
            join MIP.APP.RECOMMENDATION_LOG r
              on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
            left join MIP.APP.PATTERN_DEFINITION p
              on p.PATTERN_ID = r.PATTERN_ID
            where r.MARKET_TYPE = '{selected_market_type}'
              and r.INTERVAL_MINUTES = {selected_interval_minutes}
              and o.EVALUATED_AT >= dateadd('day', -30, current_timestamp())
            group by PATTERN_NAME
            order by CUM_RETURN desc nulls last, OUTCOME_COUNT desc
            """
        )
    )

    if outcome_kpi_df is None or outcome_kpi_df.empty:
        st.info("No outcome KPIs available for the last 30 days.")
    else:
        st.dataframe(outcome_kpi_df, use_container_width=True)

    st.markdown("### Data health checks")
    if st.button("Refresh health checks", key="refresh_morning_checks"):
        st.session_state["morning_health_checks"] = run_post_ingest_health_checks()

    if "morning_health_checks" not in st.session_state:
        st.session_state["morning_health_checks"] = run_post_ingest_health_checks()

    checks = st.session_state.get("morning_health_checks")
    if checks:
        errors = checks.get("errors", [])
        counts_df = checks.get("counts")
        dup_bars_summary = checks.get("dup_bars_summary")
        dup_returns_summary = checks.get("dup_returns_summary")

        dup_bars_keys = (
            int(dup_bars_summary["DUP_KEYS"].iloc[0])
            if dup_bars_summary is not None and not dup_bars_summary.empty
            else 0
        )
        dup_returns_keys = (
            int(dup_returns_summary["DUP_KEYS"].iloc[0])
            if dup_returns_summary is not None and not dup_returns_summary.empty
            else 0
        )

        total_rows = (
            int(counts_df["TOTAL_ROWS"].iloc[0])
            if counts_df is not None and not counts_df.empty
            else 0
        )

        if errors:
            status = "FAIL"
        elif dup_bars_keys > 0 or dup_returns_keys > 0:
            status = "FAIL"
        elif total_rows == 0:
            status = "WARN"
        else:
            status = "OK"

        if status == "OK":
            st.success("Health check status: OK")
        elif status == "WARN":
            st.warning("Health check status: WARN")
        else:
            st.error("Health check status: FAIL")

        if errors:
            st.write("Errors encountered:")
            for err in errors:
                st.write(f"- {err}")

        if counts_df is not None and not counts_df.empty:
            last_ingest_ts = counts_df["LAST_INGESTED_AT"].iloc[0]
            last_ingest_rows = counts_df["LAST_INGEST_ROWS"].iloc[0]
            st.caption(
                f"Total rows in MARKET_BARS: {total_rows:,}. "
                f"Rows ingested/merged in latest batch: {last_ingest_rows:,}. "
                f"Latest INGESTED_AT: {last_ingest_ts}."
            )

        if dup_bars_summary is not None and not dup_bars_summary.empty:
            st.caption(
                "MARKET_BARS duplicates by (MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS): "
                f"{dup_bars_keys} duplicate keys."
            )

        if dup_returns_summary is not None and not dup_returns_summary.empty:
            st.caption(
                "MARKET_RETURNS duplicates by (MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS): "
                f"{dup_returns_keys} duplicate keys."
            )

        recent_ts_df = checks.get("recent_ts")
        if recent_ts_df is not None and not recent_ts_df.empty:
            st.markdown("**Most recent bar timestamp per market/interval**")
            st.dataframe(recent_ts_df, use_container_width=True)


def render_patterns_learning():
    st.subheader("Patterns & Learning")

    st.markdown(
        """
        This page shows patterns defined in `MIP.APP.PATTERN_DEFINITION`.
        For now, we only seed a single demo pattern: **MOMENTUM_DEMO**.
        """
    )

    st.info(
        "Learning Cycle uses existing ingested data; run ingestion separately on the "
        "Ingestion page if needed."
    )

    st.markdown("### Data freshness")
    st.caption(
        "Latest bar timestamp per market/interval, plus the most recent ingestion timestamp."
    )
    freshness_df = to_pandas(
        run_sql(
            """
            select
                MARKET_TYPE,
                INTERVAL_MINUTES,
                max(TS) as MOST_RECENT_TS,
                max(INGESTED_AT) as LAST_INGESTED_AT
            from MIP.MART.MARKET_BARS
            group by MARKET_TYPE, INTERVAL_MINUTES
            order by MARKET_TYPE, INTERVAL_MINUTES
            """
        )
    )

    if freshness_df is None or freshness_df.empty:
        st.info("No market bars available yet. Run ingestion to populate data.")
    else:
        last_ingest_ts = freshness_df["LAST_INGESTED_AT"].max()
        if last_ingest_ts is not None:
            st.caption(f"Last ingestion timestamp: {last_ingest_ts}")
        st.dataframe(freshness_df, use_container_width=True)

    # Option to reseed demo pattern (idempotent)
    if st.button("Seed / refresh MOMENTUM_DEMO pattern"):
        res = run_sql("call MIP.APP.SP_SEED_MIP_DEMO()").collect()
        msg = res[0][0] if res and len(res[0]) > 0 else "Seed procedure completed."
        st.success(msg)

    st.caption(
        "This only (re)creates the demo pattern; it no longer removes other patterns."
    )

    st.markdown("### Learning cycle controls")

    market_timeframe_options = get_market_timeframe_options(session)

    def format_market_timeframe_option(option):
        market_type, interval_minutes = option
        market_label = {"STOCK": "Stocks", "FX": "FX"}.get(
            market_type, str(market_type).title()
        )
        interval_label = (
            "Daily" if interval_minutes == 1440 else f"{interval_minutes}-min"
        )
        return f"{market_label} ({interval_label})"

    selected_market_timeframes = st.multiselect(
        "Select one or more market / timeframe combinations",
        options=market_timeframe_options,
        default=market_timeframe_options,
        format_func=format_market_timeframe_option,
        key="market_selector_learning_cycle",
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
        if not selected_market_timeframes:
            st.warning("Select at least one market / timeframe combination.")
            return

        from_ts_str = date_from.strftime("%Y-%m-%d %H:%M:%S")
        to_ts_str = date_to.strftime("%Y-%m-%d %H:%M:%S")
        from_ts_sql = f"to_timestamp_ntz('{from_ts_str}')"
        to_ts_sql = f"to_timestamp_ntz('{to_ts_str}')"

        with st.spinner("Running full learning cycle…"):
            summaries = []
            run_results = []
            for market_type, interval_minutes in selected_market_timeframes:
                interval_sql = "NULL" if interval_minutes is None else str(int(interval_minutes))
                call_sql = f"""
                    call MIP.APP.SP_RUN_MIP_LEARNING_CYCLE(
                        '{market_type}',
                        {interval_sql},
                        {horizon_minutes},
                        {min_return},
                        {hit_threshold},
                        {miss_threshold},
                        {from_ts_sql},
                        {to_ts_sql},
                        FALSE,
                        TRUE,
                        TRUE,
                        TRUE,
                        TRUE
                    )
                """
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
                    summaries.append(summary)
                    run_results.append(
                        {
                            "market_type": market_type,
                            "interval_minutes": interval_minutes,
                            "status": "Success",
                        }
                    )
                except Exception as e:
                    run_results.append(
                        {
                            "market_type": market_type,
                            "interval_minutes": interval_minutes,
                            "status": "Failed",
                            "message": str(e),
                        }
                    )

        if run_results:
            st.markdown("#### Run summary")
            st.dataframe(run_results, use_container_width=True)

        if summaries:
            st.success("Learning cycle completed.")
            for summary in summaries:
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

    metric_cols = [
        "LAST_TRADE_COUNT",
        "LAST_HIT_RATE",
        "LAST_CUM_RETURN",
        "LAST_AVG_RETURN",
        "LAST_STD_RETURN",
        "PATTERN_SCORE",
    ]

    has_metrics = perf_df[metric_cols].notna().any().any()

    if not has_metrics:
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

    pattern_defaults_df = to_pandas(
        run_sql(
            """
            select
                coalesce(PARAMS_JSON:lookback_days::number, 1) as LOOKBACK_DAYS,
                coalesce(PARAMS_JSON:min_return::float, 0.002) as MIN_RETURN,
                coalesce(PARAMS_JSON:min_zscore::float, 1.0) as MIN_ZSCORE
            from MIP.APP.PATTERN_DEFINITION
            where upper(NAME) = 'MOMENTUM_DEMO'
            limit 1
            """
        )
    )
    default_lookback_days = 1
    default_min_return = 0.002
    default_min_zscore = 1.0
    if pattern_defaults_df is not None and not pattern_defaults_df.empty:
        default_lookback_days = int(pattern_defaults_df["LOOKBACK_DAYS"].iloc[0])
        default_min_return = float(pattern_defaults_df["MIN_RETURN"].iloc[0])
        default_min_zscore = float(pattern_defaults_df["MIN_ZSCORE"].iloc[0])

    with st.form("momentum_form"):
        st.markdown("#### Pattern Defaults (read-only)")
        defaults_col1, defaults_col2, defaults_col3 = st.columns(3)
        with defaults_col1:
            st.number_input(
                "min_return",
                value=default_min_return,
                format="%.4f",
                disabled=True,
            )
        with defaults_col2:
            st.number_input(
                "lookback_days",
                min_value=1,
                value=default_lookback_days,
                step=1,
                disabled=True,
            )
        with defaults_col3:
            st.number_input(
                "min_zscore",
                value=default_min_zscore,
                format="%.2f",
                disabled=True,
            )

        st.markdown("#### Overrides (optional)")
        override_col1, override_col2 = st.columns(2)
        with override_col1:
            override_min_return = st.checkbox(
                "Override min_return",
                value=False,
                key="override_min_return",
            )
            min_return_override_value = st.number_input(
                "min_return override value",
                min_value=0.0,
                max_value=1.0,
                value=default_min_return,
                step=0.001,
                format="%.4f",
                help="E.g. 0.002 = 0.2% positive return between bars.",
                disabled=not override_min_return,
            )
        with override_col2:
            override_lookback_days = st.checkbox(
                "Override lookback_days",
                value=False,
                key="override_lookback_days",
            )
            lookback_days_override_value = st.number_input(
                "lookback_days override value",
                min_value=1,
                value=default_lookback_days,
                step=1,
                disabled=not override_lookback_days,
            )

        override_col3, override_col4 = st.columns(2)
        with override_col3:
            override_min_zscore = st.checkbox(
                "Override min_zscore",
                value=False,
                key="override_min_zscore",
            )
            min_zscore_override_value = st.number_input(
                "min_zscore override value",
                value=default_min_zscore,
                format="%.2f",
                disabled=not override_min_zscore,
            )
        with override_col4:
            st.caption("Momentum signals will run for the selected market and interval.")

        active_overrides = []
        if override_min_return:
            active_overrides.append(f"min_return={min_return_override_value:.4f}")
        if override_lookback_days:
            active_overrides.append(f"lookback_days={int(lookback_days_override_value)}")
        if override_min_zscore:
            active_overrides.append(f"min_zscore={min_zscore_override_value:.2f}")

        if active_overrides:
            st.success(f"Active overrides: {', '.join(active_overrides)}")
        else:
            st.caption("Active overrides: None")

        st.caption("Use overrides above to adjust per-run thresholds.")

        submitted = st.form_submit_button("Generate momentum signals")

    if submitted:
        try:
            min_return_value = (
                min_return_override_value if override_min_return else None
            )
            lookback_days_value = (
                int(lookback_days_override_value) if override_lookback_days else None
            )
            min_zscore_value = (
                min_zscore_override_value if override_min_zscore else None
            )
            msg = run_momentum_generator(
                min_return_value,
                selected_market_type,
                selected_interval_minutes,
                lookback_days=lookback_days_value,
                min_zscore=min_zscore_value,
            )
            if "Warnings:" in msg:
                st.warning(msg)
            else:
                st.success(msg)
        except Exception as e:
            st.error(f"Momentum generation failed: {e}")

    st.markdown("### Latest Run Results")
    st.caption(
        "Summary of the most recent recommendation generation run for the selected "
        "market and interval."
    )

    latest_run_counts = to_pandas(
        run_sql(
            f"""
            with latest_run as (
                select max(GENERATED_AT) as GENERATED_AT
                from MIP.APP.RECOMMENDATION_LOG
                where MARKET_TYPE = '{selected_market_type}'
                  and INTERVAL_MINUTES = {selected_interval_minutes}
            )
            select
                r.PATTERN_ID,
                count(*) as RECOMMENDATION_COUNT
            from MIP.APP.RECOMMENDATION_LOG r
            join latest_run lr
              on r.GENERATED_AT = lr.GENERATED_AT
            where r.MARKET_TYPE = '{selected_market_type}'
              and r.INTERVAL_MINUTES = {selected_interval_minutes}
            group by r.PATTERN_ID
            order by RECOMMENDATION_COUNT desc, r.PATTERN_ID
            """
        )
    )

    if latest_run_counts is None or latest_run_counts.empty:
        st.info("No recent run results found for the selected market/interval.")
    else:
        st.markdown("**Counts by pattern**")
        st.dataframe(latest_run_counts, use_container_width=True)

    latest_recs_df = to_pandas(
        run_sql(
            f"""
            select
                PATTERN_ID,
                SYMBOL,
                TS,
                SCORE,
                PATTERN_KEY
            from MIP.APP.RECOMMENDATION_LOG
            where MARKET_TYPE = '{selected_market_type}'
              and INTERVAL_MINUTES = {selected_interval_minutes}
            order by GENERATED_AT desc, TS desc
            limit 50
            """
        )
    )

    st.markdown("**Latest 50 recommendations**")
    if latest_recs_df is None or latest_recs_df.empty:
        st.info("No recommendations available yet.")
    else:
        st.dataframe(latest_recs_df, use_container_width=True)

    active_defaults_df = to_pandas(
        run_sql(
            """
            select
                PATTERN_ID,
                NAME as PATTERN_NAME,
                PARAMS_JSON
            from MIP.APP.PATTERN_DEFINITION
            where coalesce(IS_ACTIVE, 'N') = 'Y'
              and coalesce(ENABLED, true)
            order by PATTERN_ID
            """
        )
    )

    st.markdown("**Active pattern defaults**")
    if active_defaults_df is None or active_defaults_df.empty:
        st.info("No active pattern defaults available.")
    else:
        st.dataframe(active_defaults_df, use_container_width=True)

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
if page == "Morning Brief":
    render_morning_brief()
elif page == "Ingestion":
    render_ingestion()
elif page == "Market Overview":
    render_market_overview()
elif page == "Patterns & Learning":
    render_patterns_learning()
elif page == "Signals & Recommendations":
    render_signals_recommendations()
else:
    render_outcome_evaluation()
