import json
import math
from datetime import date, datetime, timedelta

import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

from ui.layout import apply_layout, render_badge, section_header

# Get the Snowpark session provided by Snowflake
session = get_active_session()

SIGNAL_GENERATOR_SP = "MIP.APP.SP_GENERATE_MOMENTUM_RECS"
DAILY_PIPELINE_TASK = "MIP.APP.TASK_RUN_DAILY_PIPELINE"
DAILY_PIPELINE_SP = "MIP.APP.SP_RUN_DAILY_PIPELINE"


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


def sql_literal(value):
    """Render a Python value as a SQL literal."""
    if value is None:
        return "null"
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, (date, datetime)):
        return f"'{value}'"
    return str(value)


def normalize_optional_value(value):
    """Normalize pandas/NaN values to None for SQL."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def warn_low_sample_counts(df: pd.DataFrame, threshold: int = 30) -> None:
    """Warn when KPI sample counts fall below a threshold."""
    if df is None or df.empty or "SAMPLE_COUNT" not in df.columns:
        return
    low_sample = df[df["SAMPLE_COUNT"].fillna(0) < threshold]
    if not low_sample.empty:
        st.warning(
            f"{len(low_sample)} KPI rows have sample_count < {threshold}. "
            "Interpret hit rates and returns with caution."
        )


def get_market_selection(key_prefix: str = ""):
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


def parse_task_parts(task_name: str) -> tuple[str, str, str]:
    """Return database, schema, and task name from a fully-qualified task string."""
    parts = task_name.split(".")
    if len(parts) != 3:
        return ("", "", task_name)
    return parts[0], parts[1], parts[2]


def fetch_task_metadata(task_name: str):
    """Fetch task metadata from INFORMATION_SCHEMA.TASKS."""
    database, schema, task = parse_task_parts(task_name)
    if not database or not schema:
        return None, "Task name must be fully-qualified (DB.SCHEMA.TASK)."
    query = f"""
        select
            name,
            state,
            schedule
        from {database}.information_schema.tasks
        where name = {sql_literal(task)}
          and schema_name = {sql_literal(schema)}
          and database_name = {sql_literal(database)}
    """
    try:
        return to_pandas(run_sql(query)), None
    except Exception as exc:
        return None, str(exc)


def fetch_task_history(task_name: str, limit: int = 20):
    """Fetch recent task history rows for a task."""
    database, _, _ = parse_task_parts(task_name)
    if not database:
        return None
    query = f"""
        select
            start_time,
            end_time,
            state,
            error_message
        from table({database}.information_schema.task_history(
            task_name => {sql_literal(task_name)},
            result_limit => {limit}
        ))
        order by start_time desc
    """
    return to_pandas(run_sql(query))


def fetch_audit_log_options(column_name: str):
    query = f"""
        select distinct {column_name}
        from MIP.APP.MIP_AUDIT_LOG
        where {column_name} is not null
        order by {column_name}
    """
    return to_pandas(run_sql(query))


def fetch_audit_log(
    date_start: date,
    date_end: date,
    statuses: list[str] | None = None,
    event_types: list[str] | None = None,
    run_id: str | None = None,
    limit: int = 200,
):
    filters = []
    if date_start:
        filters.append(f"event_ts >= {sql_literal(date_start)}")
    if date_end:
        filters.append(f"event_ts < dateadd(day, 1, {sql_literal(date_end)})")
    if statuses:
        status_list = ", ".join(sql_literal(status) for status in statuses)
        filters.append(f"status in ({status_list})")
    if event_types:
        type_list = ", ".join(sql_literal(event_type) for event_type in event_types)
        filters.append(f"event_type in ({type_list})")
    if run_id:
        filters.append(f"run_id = {sql_literal(run_id)}")

    where_clause = f"where {' and '.join(filters)}" if filters else ""
    query = f"""
        select
            event_ts,
            run_id,
            parent_run_id,
            event_type,
            event_name,
            status,
            rows_affected,
            details,
            error_message,
            invoked_by_user,
            invoked_by_role,
            invoked_warehouse,
            query_id,
            session_id
        from MIP.APP.MIP_AUDIT_LOG
        {where_clause}
        order by event_ts desc
        limit {limit}
    """
    return to_pandas(run_sql(query))


def fetch_portfolios():
    query = """
        select
            p.PORTFOLIO_ID,
            p.PROFILE_ID,
            p.NAME,
            p.BASE_CURRENCY,
            p.STARTING_CASH,
            p.LAST_SIMULATION_RUN_ID,
            p.LAST_SIMULATED_AT,
            p.FINAL_EQUITY,
            p.TOTAL_RETURN,
            p.MAX_DRAWDOWN,
            p.WIN_DAYS,
            p.LOSS_DAYS,
            p.STATUS,
            p.BUST_AT,
            p.NOTES
        from MIP.APP.PORTFOLIO p
        order by p.PORTFOLIO_ID
    """
    return to_pandas(run_sql(query))


def fetch_portfolio_profiles():
    query = """
        select
            PROFILE_ID,
            NAME,
            MAX_POSITIONS,
            MAX_POSITION_PCT,
            BUST_EQUITY_PCT,
            BUST_ACTION,
            DRAWDOWN_STOP_PCT,
            DESCRIPTION
        from MIP.APP.PORTFOLIO_PROFILE
        order by PROFILE_ID
    """
    return to_pandas(run_sql(query))


def create_portfolio(
    name: str,
    starting_cash: float,
    base_currency: str,
    notes: str | None,
    profile_id: int | None,
):
    query = f"""
        insert into MIP.APP.PORTFOLIO (
            NAME,
            BASE_CURRENCY,
            STARTING_CASH,
            NOTES,
            PROFILE_ID
        )
        values (
            {sql_literal(name)},
            {sql_literal(base_currency)},
            {starting_cash},
            {sql_literal(notes)},
            {sql_literal(profile_id)}
        )
    """
    run_sql(query).collect()


def fetch_portfolio_run_id(portfolio_id: int):
    query = f"""
        select LAST_SIMULATION_RUN_ID
        from MIP.APP.PORTFOLIO
        where PORTFOLIO_ID = {portfolio_id}
    """
    df = to_pandas(run_sql(query))
    if df is None or df.empty:
        return None
    return df.iloc[0]["LAST_SIMULATION_RUN_ID"]


def fetch_portfolio_daily(portfolio_id: int, run_id: str):
    query = f"""
        select
            TS,
            CASH,
            EQUITY_VALUE,
            TOTAL_EQUITY,
            OPEN_POSITIONS,
            DAILY_PNL,
            DAILY_RETURN,
            PEAK_EQUITY,
            DRAWDOWN,
            STATUS
        from MIP.APP.PORTFOLIO_DAILY
        where PORTFOLIO_ID = {portfolio_id}
          and RUN_ID = {sql_literal(run_id)}
        order by TS
    """
    return to_pandas(run_sql(query))


def fetch_portfolio_trades(portfolio_id: int, run_id: str):
    query = f"""
        select
            TRADE_TS,
            SYMBOL,
            SIDE,
            PRICE,
            QUANTITY,
            NOTIONAL,
            REALIZED_PNL,
            CASH_AFTER,
            SCORE
        from MIP.APP.PORTFOLIO_TRADES
        where PORTFOLIO_ID = {portfolio_id}
          and RUN_ID = {sql_literal(run_id)}
        order by TRADE_TS, SYMBOL
    """
    return to_pandas(run_sql(query))


def fetch_portfolio_positions(portfolio_id: int, run_id: str):
    query = f"""
        select
            SYMBOL,
            ENTRY_TS,
            ENTRY_PRICE,
            QUANTITY,
            COST_BASIS,
            ENTRY_SCORE,
            ENTRY_INDEX,
            HOLD_UNTIL_INDEX
        from MIP.APP.PORTFOLIO_POSITIONS
        where PORTFOLIO_ID = {portfolio_id}
          and RUN_ID = {sql_literal(run_id)}
        order by SYMBOL
    """
    return to_pandas(run_sql(query))


def check_task_privilege(task_name: str, privilege: str):
    """Return True/False if SYSTEM$HAS_PRIVILEGE can be checked, else None."""
    query = f"""
        select system$has_privilege('TASK', {sql_literal(task_name)}, {sql_literal(privilege)}) as HAS_PRIVILEGE
    """
    try:
        rows = run_sql(query).collect()
        if not rows:
            return None
        return str(rows[0]["HAS_PRIVILEGE"]).lower() in ("true", "1", "yes")
    except Exception:
        return None


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


def render_signal_visualizer(symbol: str, market_type: str, interval_minutes: int) -> None:
    window_days = st.number_input(
        "Days of history", min_value=1, max_value=90, value=10, step=1, key="signal_window"
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
            where SYMBOL = '{symbol}'
              and MARKET_TYPE = '{market_type}'
              and INTERVAL_MINUTES = {interval_minutes}
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
            where r.SYMBOL = '{symbol}'
              and r.MARKET_TYPE = '{market_type}'
              and r.INTERVAL_MINUTES = {interval_minutes}
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

        chart = (price_line + signal_points).properties(height=280)
    else:
        chart = price_line.properties(height=280)

    st.altair_chart(chart, use_container_width=True)


@st.cache_data(ttl=300)
def fetch_latest_pipeline_event():
    query = """
        select
            event_ts,
            run_id,
            status,
            error_message
        from MIP.APP.MIP_AUDIT_LOG
        order by event_ts desc
        limit 1
    """
    return to_pandas(run_sql(query))


@st.cache_data(ttl=300)
def fetch_data_freshness():
    bars_query = """
        select
            MARKET_TYPE,
            INTERVAL_MINUTES,
            max(TS) as LATEST_BAR_TS
        from MIP.MART.MARKET_BARS
        group by MARKET_TYPE, INTERVAL_MINUTES
        order by MARKET_TYPE, INTERVAL_MINUTES
    """
    returns_query = """
        select max(TS) as LATEST_RETURNS_TS
        from MIP.MART.MARKET_RETURNS
    """
    recs_query = """
        select max(GENERATED_AT) as LATEST_REC_TS
        from MIP.APP.RECOMMENDATION_LOG
    """
    return {
        "bars": to_pandas(run_sql(bars_query)),
        "returns": to_pandas(run_sql(returns_query)),
        "recs": to_pandas(run_sql(recs_query)),
    }


@st.cache_data(ttl=300)
def fetch_opportunity_snapshot():
    count_query = """
        select count(*) as OPPORTUNITY_COUNT
        from MIP.APP.V_OPPORTUNITY_FEED
        where TS >= dateadd('hour', -24, current_timestamp())
    """
    top_query = """
        select
            SYMBOL,
            MARKET_TYPE,
            PATTERN_ID,
            PATTERN_KEY,
            TS,
            SCORE,
            abs(SCORE) as ABS_SCORE
        from MIP.APP.V_OPPORTUNITY_FEED
        where TS >= dateadd('hour', -24, current_timestamp())
        order by abs(SCORE) desc, TS desc
        limit 5
    """
    return {
        "count": to_pandas(run_sql(count_query)),
        "top": to_pandas(run_sql(top_query)),
    }


@st.cache_data(ttl=300)
def fetch_portfolio_overview():
    query = """
        select
            p.PORTFOLIO_ID,
            p.NAME,
            p.LAST_SIMULATION_RUN_ID,
            p.LAST_SIMULATED_AT,
            p.FINAL_EQUITY,
            p.STARTING_CASH,
            p.TOTAL_RETURN
        from MIP.APP.PORTFOLIO p
        order by p.LAST_SIMULATED_AT desc nulls last, p.PORTFOLIO_ID desc
        limit 1
    """
    return to_pandas(run_sql(query))


@st.cache_data(ttl=300)
def fetch_portfolio_sparkline(portfolio_id: int, run_id: str):
    query = f"""
        select TS, TOTAL_EQUITY
        from MIP.APP.PORTFOLIO_DAILY
        where PORTFOLIO_ID = {portfolio_id}
          and RUN_ID = {sql_literal(run_id)}
          and TS >= dateadd('day', -90, current_timestamp())
        order by TS
    """
    return to_pandas(run_sql(query))


@st.cache_data(ttl=300)
def fetch_portfolio_latest_daily(portfolio_id: int, run_id: str):
    query = f"""
        select
            TS,
            CASH,
            EQUITY_VALUE,
            TOTAL_EQUITY,
            OPEN_POSITIONS
        from MIP.APP.PORTFOLIO_DAILY
        where PORTFOLIO_ID = {portfolio_id}
          and RUN_ID = {sql_literal(run_id)}
        order by TS desc
        limit 1
    """
    return to_pandas(run_sql(query))


@st.cache_data(ttl=300)
def fetch_opportunity_feed(
    market_types: list[str],
    intervals: list[int],
    pattern_ids: list[str],
    lookback_days: int,
    min_abs_score: float,
):
    filters = [f"TS >= dateadd('day', -{lookback_days}, current_timestamp())"]
    if market_types:
        market_list = ", ".join(sql_literal(mt) for mt in market_types)
        filters.append(f"MARKET_TYPE in ({market_list})")
    if intervals:
        interval_list = ", ".join(str(int(i)) for i in intervals)
        filters.append(f"INTERVAL_MINUTES in ({interval_list})")
    if pattern_ids:
        pattern_list = ", ".join(sql_literal(pid) for pid in pattern_ids)
        filters.append(f"PATTERN_ID in ({pattern_list})")
    if min_abs_score is not None:
        filters.append(f"abs(SCORE) >= {min_abs_score}")

    where_clause = " and ".join(filters) if filters else "1=1"
    query = f"""
        select
            SYMBOL,
            MARKET_TYPE,
            PATTERN_ID,
            PATTERN_KEY,
            TS,
            SCORE,
            abs(SCORE) as ABS_SCORE,
            INTERVAL_MINUTES
        from MIP.APP.V_OPPORTUNITY_FEED
        where {where_clause}
        order by TS desc, abs(SCORE) desc
        limit 200
    """
    return to_pandas(run_sql(query))


@st.cache_data(ttl=300)
def fetch_symbol_bars(symbols: tuple[str, ...], market_type: str, interval_minutes: int, limit: int):
    if not symbols:
        return None
    symbols_list = ", ".join(sql_literal(symbol) for symbol in symbols)
    query = f"""
        select
            SYMBOL,
            TS,
            CLOSE
        from MIP.MART.MARKET_BARS
        where MARKET_TYPE = {sql_literal(market_type)}
          and INTERVAL_MINUTES = {interval_minutes}
          and SYMBOL in ({symbols_list})
          and TS >= dateadd('day', -30, current_timestamp())
        qualify row_number() over (partition by SYMBOL order by TS desc) <= {limit}
        order by SYMBOL, TS
    """
    return to_pandas(run_sql(query))


@st.cache_data(ttl=300)
def fetch_scorecard(horizon_days: int, lookback_days: int):
    query = f"""
        select
            coalesce(p.NAME, concat('Pattern ', s.PATTERN_ID)) as PATTERN_NAME,
            s.PATTERN_ID,
            s.MARKET_TYPE,
            s.INTERVAL_MINUTES,
            s.SAMPLE_COUNT,
            s.HIT_RATE,
            s.AVG_FORWARD_RETURN,
            s.MEDIAN_FORWARD_RETURN,
            s.MIN_FORWARD_RETURN,
            s.MAX_FORWARD_RETURN,
            s.LAST_SIGNAL_DATE,
            s.PATTERN_STATUS
        from MIP.APP.V_PATTERN_SCORECARD s
        left join MIP.APP.PATTERN_DEFINITION p
          on p.PATTERN_ID = s.PATTERN_ID
        where s.HORIZON_DAYS = {horizon_days}
          and s.LAST_SIGNAL_DATE >= dateadd('day', -{lookback_days}, current_timestamp())
        order by
            case s.PATTERN_STATUS
                when 'TRUSTED' then 1
                when 'WATCH' then 2
                else 3
            end,
            s.HIT_RATE desc,
            s.SAMPLE_COUNT desc
    """
    return to_pandas(run_sql(query))


def render_market_overview_compact():
    st.caption(
        "Latest bar per symbol from MIP.MART.MARKET_LATEST_PER_SYMBOL, with filters."
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        market_type_choice = st.radio(
            "Market type",
            options=["All", "Stocks only", "FX only"],
            index=0,
            horizontal=True,
            key="admin_market_type",
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
            key="admin_symbols",
        )
    with col3:
        interval_filter = st.selectbox(
            "Interval (minutes)", options=["All", 5, 1440], index=0, key="admin_interval"
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

    df_pd = to_pandas(run_sql(query))
    if df_pd is None or df_pd.empty:
        st.info("No market data available yet.")
    else:
        st.dataframe(df_pd, use_container_width=True, height=320)


def render_recommendations_viewer():
    st.caption("Latest recommendation runs and recent signals.")
    selected_market_type, selected_interval_minutes = get_market_selection("admin_recs")

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
    if latest_run_ts is not None:
        st.caption(f"Latest run timestamp: {latest_run_ts}")

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
    if latest_recs_df is None or latest_recs_df.empty:
        st.info("No recommendations available yet.")
    else:
        st.dataframe(latest_recs_df, use_container_width=True, height=260)


def render_outcome_evaluation_admin():
    st.caption("Outcome evaluation and historical backtests.")
    selected_market_type, selected_interval_minutes = get_market_selection("admin_outcomes")

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

    df_pd = to_pandas(run_sql(outcome_query))
    if df_pd is None or df_pd.empty:
        st.info("No outcome evaluations recorded yet.")
    else:
        st.dataframe(df_pd, use_container_width=True, height=260)

    st.markdown("### Backtesting (historical performance)")

    today = date.today()
    default_from = today - timedelta(days=30)

    with st.expander("Backtest settings", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            horizon_minutes = st.number_input(
                "Backtest horizon (minutes)", min_value=1, value=15, step=1
            )
            hit_threshold = st.number_input(
                "Backtest hit threshold", value=0.002, format="%.6f"
            )
            miss_threshold = st.number_input(
                "Backtest miss threshold", value=-0.002, format="%.6f"
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
            f"""
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

    st.dataframe(history_df, use_container_width=True, height=240)

    st.markdown("### Backtest results")

    selected_run_id = st.selectbox(
        "Select a backtest run",
        options=history_df["BACKTEST_RUN_ID"].tolist(),
        key="admin_backtest_run",
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
            st.dataframe(result_df, use_container_width=True, height=260)


def render_overview():
    section_header("Pipeline health")
    st.caption(
        "Normal operation: daily task runs ingestion → signals → evaluation → KPIs. "
        "Use Admin/Ops for manual runs."
    )

    pipeline_df = fetch_latest_pipeline_event()
    if pipeline_df is None or pipeline_df.empty:
        st.info("No pipeline audit events available yet.")
    else:
        row = pipeline_df.iloc[0]
        status = row.get("STATUS") or "Unknown"
        cols = st.columns(3)
        cols[0].metric("Last run", str(row.get("EVENT_TS")))
        cols[1].metric("Status", status)
        error_message = row.get("ERROR_MESSAGE")
        error_display = (
            str(error_message)[:120] + "…" if error_message and len(str(error_message)) > 120 else error_message
        )
        cols[2].metric("Last error", error_display or "—")

    section_header("Data freshness")
    freshness = fetch_data_freshness()
    bars_df = freshness.get("bars")
    returns_df = freshness.get("returns")
    recs_df = freshness.get("recs")

    freshness_cols = st.columns(3)
    latest_returns = (
        returns_df["LATEST_RETURNS_TS"].iloc[0] if returns_df is not None and not returns_df.empty else None
    )
    latest_recs = recs_df["LATEST_REC_TS"].iloc[0] if recs_df is not None and not recs_df.empty else None
    freshness_cols[0].metric("Latest returns TS", str(latest_returns) if latest_returns else "—")
    freshness_cols[1].metric("Latest recs TS", str(latest_recs) if latest_recs else "—")
    if bars_df is not None and not bars_df.empty:
        st.dataframe(bars_df, use_container_width=True, height=220)
    else:
        st.info("No market bars found yet.")

    section_header("Portfolio snapshot")
    portfolio_df = fetch_portfolio_overview()
    if portfolio_df is None or portfolio_df.empty:
        st.info("No portfolio runs yet.")
    else:
        portfolio_row = portfolio_df.iloc[0]
        cols = st.columns(5)
        cols[0].metric("Portfolio", str(portfolio_row.get("NAME")))

        run_id = portfolio_row.get("LAST_SIMULATION_RUN_ID")
        latest_daily = None
        if run_id:
            latest_daily = fetch_portfolio_latest_daily(int(portfolio_row["PORTFOLIO_ID"]), run_id)
        latest_row = latest_daily.iloc[0] if latest_daily is not None and not latest_daily.empty else None

        cols[1].metric(
            "Equity",
            f"${latest_row['TOTAL_EQUITY']:,.2f}"
            if latest_row is not None and pd.notnull(latest_row.get("TOTAL_EQUITY"))
            else "—",
        )
        cols[2].metric(
            "Cash",
            f"${latest_row['CASH']:,.2f}"
            if latest_row is not None and pd.notnull(latest_row.get("CASH"))
            else "—",
        )
        cols[3].metric(
            "Market value",
            f"${latest_row['EQUITY_VALUE']:,.2f}"
            if latest_row is not None and pd.notnull(latest_row.get("EQUITY_VALUE"))
            else "—",
        )
        cols[4].metric(
            "Open positions",
            int(latest_row["OPEN_POSITIONS"])
            if latest_row is not None and pd.notnull(latest_row.get("OPEN_POSITIONS"))
            else "—",
        )

        if run_id:
            spark_df = fetch_portfolio_sparkline(int(portfolio_row["PORTFOLIO_ID"]), run_id)
            if spark_df is not None and not spark_df.empty:
                spark_df["TOTAL_EQUITY"] = spark_df["TOTAL_EQUITY"].astype(float)
                spark_chart = (
                    alt.Chart(spark_df)
                    .mark_line()
                    .encode(x="TS:T", y=alt.Y("TOTAL_EQUITY:Q", title=""))
                    .properties(height=120)
                )
                st.altair_chart(spark_chart, use_container_width=True)

    section_header("Opportunities snapshot")
    snapshot = fetch_opportunity_snapshot()
    count_df = snapshot.get("count")
    top_df = snapshot.get("top")
    count_value = (
        int(count_df["OPPORTUNITY_COUNT"].iloc[0]) if count_df is not None and not count_df.empty else 0
    )
    st.metric("Opportunities (last 24h)", count_value)
    if top_df is None or top_df.empty:
        st.info("No recent opportunities available.")
    else:
        st.dataframe(top_df, use_container_width=True, height=220)


def render_opportunities():
    section_header("Opportunity screener")
    filters = st.columns([2, 2, 2, 2, 1, 1])

    market_types_df = to_pandas(run_sql("select distinct MARKET_TYPE from MIP.APP.V_OPPORTUNITY_FEED order by MARKET_TYPE"))
    market_options = (
        market_types_df["MARKET_TYPE"].dropna().tolist() if market_types_df is not None else []
    )
    interval_df = to_pandas(run_sql("select distinct INTERVAL_MINUTES from MIP.APP.V_OPPORTUNITY_FEED order by INTERVAL_MINUTES"))
    interval_options = (
        interval_df["INTERVAL_MINUTES"].dropna().astype(int).tolist() if interval_df is not None else []
    )
    pattern_df = to_pandas(run_sql("select distinct PATTERN_ID from MIP.APP.V_OPPORTUNITY_FEED order by PATTERN_ID"))
    pattern_options = (
        pattern_df["PATTERN_ID"].dropna().astype(str).tolist() if pattern_df is not None else []
    )

    with filters[0]:
        selected_markets = st.multiselect("Market type", options=market_options, default=market_options)
    with filters[1]:
        selected_intervals = st.multiselect("Interval (min)", options=interval_options, default=interval_options)
    with filters[2]:
        selected_patterns = st.multiselect("Pattern", options=pattern_options, default=[])
    with filters[3]:
        lookback_days = st.selectbox("Lookback", options=[1, 3, 7], index=1, format_func=lambda x: f"{x}d")
    with filters[4]:
        min_abs_score = st.number_input("Min abs(score)", min_value=0.0, value=0.0, step=0.1)
    with filters[5]:
        limit_rows = st.number_input("Rows", min_value=25, max_value=200, value=100, step=25)

    feed_df = fetch_opportunity_feed(
        selected_markets,
        selected_intervals,
        selected_patterns,
        lookback_days,
        min_abs_score,
    )

    if feed_df is None or feed_df.empty:
        st.info("No opportunities match the selected filters.")
        return

    feed_df = feed_df.sort_values(["TS", "ABS_SCORE"], ascending=[False, False]).head(int(limit_rows))

    sparkline_data = {}
    for market_type in feed_df["MARKET_TYPE"].dropna().unique():
        symbol_list = (
            feed_df.loc[feed_df["MARKET_TYPE"] == market_type, "SYMBOL"].dropna().unique().tolist()
        )
        bars_df = fetch_symbol_bars(tuple(symbol_list), market_type, 1440, 20)
        if bars_df is None or bars_df.empty:
            continue
        for symbol, group in bars_df.groupby("SYMBOL"):
            sparkline_data[symbol] = group.sort_values("TS")["CLOSE"].astype(float).tolist()

    feed_df = feed_df.copy()
    feed_df["SPARKLINE"] = feed_df["SYMBOL"].map(sparkline_data)

    st.dataframe(
        feed_df[
            [
                "SYMBOL",
                "MARKET_TYPE",
                "PATTERN_KEY",
                "PATTERN_ID",
                "TS",
                "SCORE",
                "ABS_SCORE",
                "SPARKLINE",
            ]
        ],
        use_container_width=True,
        height=420,
        column_config={
            "SPARKLINE": st.column_config.LineChartColumn("Trend", width="small"),
            "ABS_SCORE": st.column_config.NumberColumn("Abs(score)", format="%.3f"),
        },
    )

    section_header("Signal visualizer")
    selection_label = (
        feed_df["SYMBOL"].astype(str)
        + " • "
        + feed_df["MARKET_TYPE"].astype(str)
        + " • "
        + feed_df["INTERVAL_MINUTES"].astype(str)
    )
    selection_options = dict(zip(selection_label.tolist(), feed_df.index.tolist()))
    selected_option = st.selectbox("Select opportunity", options=list(selection_options.keys()))
    selected_row = feed_df.loc[selection_options[selected_option]]
    st.caption(
        f"Selected: {selected_row['SYMBOL']} • {selected_row['MARKET_TYPE']} • interval {selected_row['INTERVAL_MINUTES']}"
    )
    render_signal_visualizer(
        selected_row["SYMBOL"],
        selected_row["MARKET_TYPE"],
        int(selected_row["INTERVAL_MINUTES"]),
    )


def render_portfolio():
    section_header("Portfolio")
    st.caption("Risk-first view of simulations and holdings.")

    portfolios_df = fetch_portfolios()
    profiles_df = fetch_portfolio_profiles()
    selection = None
    portfolio_options = {}

    if portfolios_df is None or portfolios_df.empty:
        st.info("Create a portfolio to run simulations.")
    else:
        portfolio_options = {
            f"{row['PORTFOLIO_ID']} • {row['NAME']}": row["PORTFOLIO_ID"]
            for _, row in portfolios_df.iterrows()
        }
        selection = st.selectbox("Select portfolio", list(portfolio_options.keys()))
        portfolio_id = portfolio_options[selection]
        portfolio_row = portfolios_df[portfolios_df["PORTFOLIO_ID"] == portfolio_id].iloc[0]

        profile_row = None
        if (
            profiles_df is not None
            and not profiles_df.empty
            and pd.notnull(portfolio_row["PROFILE_ID"])
        ):
            profile_match = profiles_df[
                profiles_df["PROFILE_ID"] == portfolio_row["PROFILE_ID"]
            ]
            if not profile_match.empty:
                profile_row = profile_match.iloc[0]

        risk_cols = st.columns(3)
        risk_cols[0].metric(
            "Equity",
            f"${portfolio_row['FINAL_EQUITY']:,.2f}" if pd.notnull(portfolio_row["FINAL_EQUITY"]) else "—",
        )
        risk_cols[1].metric(
            "Cash",
            f"${portfolio_row['STARTING_CASH']:,.2f}" if pd.notnull(portfolio_row["STARTING_CASH"]) else "—",
        )
        risk_cols[2].metric(
            "Max drawdown",
            f"{portfolio_row['MAX_DRAWDOWN']:.2%}" if pd.notnull(portfolio_row["MAX_DRAWDOWN"]) else "—",
        )
        badge_cols = st.columns(3)
        with badge_cols[0]:
            st.markdown("**Status**")
            render_badge(portfolio_row.get("STATUS") or "—")
        with badge_cols[1]:
            st.markdown("**Bust action**")
            render_badge(profile_row["BUST_ACTION"] if profile_row is not None else "—")
        with badge_cols[2]:
            st.markdown("**Risk profile**")
            render_badge(profile_row["NAME"] if profile_row is not None else "—")

        run_id = fetch_portfolio_run_id(portfolio_id)
        daily_df = fetch_portfolio_daily(portfolio_id, run_id) if run_id else None
        positions_df = fetch_portfolio_positions(portfolio_id, run_id) if run_id else None
        trades_df = fetch_portfolio_trades(portfolio_id, run_id) if run_id else None

        if daily_df is not None and not daily_df.empty:
            latest = daily_df.iloc[-1]
            open_positions = int(latest["OPEN_POSITIONS"]) if pd.notnull(latest["OPEN_POSITIONS"]) else 0
            max_positions = (
                int(profile_row["MAX_POSITIONS"])
                if profile_row is not None and pd.notnull(profile_row["MAX_POSITIONS"])
                else None
            )
            exposure_pct = (
                open_positions / max_positions if max_positions else None
            )
            volatility = (
                daily_df["DAILY_RETURN"].astype(float).std() if "DAILY_RETURN" in daily_df.columns else None
            )

            extra_cols = st.columns(4)
            extra_cols[0].metric("Open positions", open_positions)
            extra_cols[1].metric("Exposure %", f"{exposure_pct:.0%}" if exposure_pct is not None else "—")
            extra_cols[2].metric("Volatility", f"{volatility:.2%}" if volatility is not None else "—")
            extra_cols[3].metric(
                "Bust threshold",
                f"{profile_row['BUST_EQUITY_PCT']:.2%}" if profile_row is not None and pd.notnull(profile_row["BUST_EQUITY_PCT"]) else "—",
            )

            eq_chart = (
                alt.Chart(daily_df)
                .mark_line()
                .encode(x="TS:T", y=alt.Y("TOTAL_EQUITY:Q", title="Equity"))
                .properties(height=220)
            )
            dd_chart = (
                alt.Chart(daily_df)
                .mark_area(opacity=0.4)
                .encode(x="TS:T", y=alt.Y("DRAWDOWN:Q", title="Drawdown"))
                .properties(height=180)
            )

            chart_cols = st.columns([2, 1])
            with chart_cols[0]:
                section_header("Equity curve")
                st.altair_chart(eq_chart, use_container_width=True)
            with chart_cols[1]:
                section_header("Drawdown")
                st.altair_chart(dd_chart, use_container_width=True)
        else:
            st.info("Run a simulation to see equity and drawdown charts.")

        section_header("Holdings")
        if positions_df is not None and not positions_df.empty:
            st.dataframe(positions_df, use_container_width=True, height=260)
        else:
            st.info("No open positions for this run.")

        section_header("Recent trades")
        if trades_df is not None and not trades_df.empty:
            st.dataframe(trades_df, use_container_width=True, height=260)
        else:
            st.info("No trades recorded for this run.")

    with st.expander("Actions", expanded=False):
        st.caption("Simulation controls and portfolio creation live here.")
        with st.expander("Create portfolio", expanded=False):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                new_name = st.text_input("Portfolio name", value="Paper Portfolio")
            with col2:
                new_starting_cash = st.number_input(
                    "Starting cash",
                    min_value=0.0,
                    value=100000.0,
                    step=1000.0,
                )
            with col3:
                new_base_currency = st.selectbox(
                    "Base currency",
                    options=["USD", "EUR", "GBP"],
                    index=0,
                )
            with col4:
                if profiles_df is not None and not profiles_df.empty:
                    profile_options = {
                        f"{row['NAME']}": row["PROFILE_ID"]
                        for _, row in profiles_df.iterrows()
                    }
                    new_profile_name = st.selectbox(
                        "Risk profile",
                        options=list(profile_options.keys()),
                        index=0,
                    )
                    new_profile_id = profile_options.get(new_profile_name)
                else:
                    st.warning("No portfolio profiles found. Create one in SQL to assign.")
                    new_profile_id = None
            new_notes = st.text_area("Notes", value="")
            if st.button("Create portfolio"):
                if new_name and new_starting_cash is not None:
                    create_portfolio(
                        new_name,
                        new_starting_cash,
                        new_base_currency,
                        new_notes,
                        new_profile_id,
                    )
                    st.success("Portfolio created.")

        section_header("Run simulation")
        today = date.today()
        default_start = today - timedelta(days=90)
        sim_col1, sim_col2, sim_col3 = st.columns(3)
        with sim_col1:
            from_date = st.date_input("From date", value=default_start)
        with sim_col2:
            to_date = st.date_input("To date", value=today)
        with sim_col3:
            market_type = st.selectbox("Market type", options=["STOCK", "FX"], index=0)

        use_profile_limits = st.checkbox(
            "Use profile risk limits",
            value=True,
            help="When enabled, max positions and max position % come from the selected profile.",
        )

        param_col1, param_col2, param_col3, param_col4 = st.columns(4)
        with param_col1:
            hold_days = st.number_input("Hold days", min_value=1, value=5, step=1)
        with param_col2:
            max_positions = st.number_input("Max positions", min_value=1, value=10, step=1, disabled=use_profile_limits)
        with param_col3:
            max_position_pct = st.number_input(
                "Max position %",
                min_value=0.01,
                max_value=1.0,
                value=0.10,
                step=0.01,
                format="%.2f",
                disabled=use_profile_limits,
            )
        with param_col4:
            min_abs_score = st.number_input("Min abs score", min_value=0.0, value=0.0, step=0.1)

        if st.button("Run simulation"):
            if portfolios_df is None or portfolios_df.empty or selection is None:
                st.warning("Create a portfolio first.")
            else:
                selected_id = portfolio_options[selection]
                max_positions_sql = "null" if use_profile_limits else max_positions
                max_position_pct_sql = "null" if use_profile_limits else max_position_pct
                query = f"""
                    call MIP.APP.SP_SIMULATE_PORTFOLIO(
                        {selected_id},
                        {sql_literal(from_date)},
                        {sql_literal(to_date)},
                        {hold_days},
                        {max_positions_sql},
                        {max_position_pct_sql},
                        {min_abs_score},
                        {sql_literal(market_type)}
                    )
                """
                results = run_sql(query).collect()
                if results:
                    st.success("Simulation completed.")
                    st.json(results[0][0])
                else:
                    st.warning("Simulation completed, but no result was returned.")


def render_training_trust():
    section_header("Pattern trust scorecard")
    st.caption("TRUSTED patterns are used for portfolio simulation.")

    horizon_days = st.selectbox("Horizon (days)", options=[5, 10, 20], index=0)
    lookback_days = st.selectbox("Lookback window", options=[30, 60, 90], index=2)

    scorecard_df = fetch_scorecard(horizon_days, lookback_days)
    if scorecard_df is None or scorecard_df.empty:
        st.info("No scorecard data available yet.")
        return

    scorecard_df = scorecard_df.copy()
    scorecard_df["SAMPLE_WARNING"] = scorecard_df["SAMPLE_COUNT"].fillna(0).astype(int) < 30

    def sample_badge(value: bool) -> str:
        return "⚠️ <30" if value else ""

    scorecard_df["SAMPLE_FLAG"] = scorecard_df["SAMPLE_WARNING"].map(sample_badge)

    st.dataframe(
        scorecard_df[
            [
                "PATTERN_NAME",
                "PATTERN_STATUS",
                "SAMPLE_COUNT",
                "HIT_RATE",
                "AVG_FORWARD_RETURN",
                "MEDIAN_FORWARD_RETURN",
                "LAST_SIGNAL_DATE",
                "SAMPLE_FLAG",
            ]
        ],
        use_container_width=True,
        height=420,
        column_config={
            "HIT_RATE": st.column_config.NumberColumn("Hit rate", format="%.2%"),
            "AVG_FORWARD_RETURN": st.column_config.NumberColumn("Avg return", format="%.3%"),
            "MEDIAN_FORWARD_RETURN": st.column_config.NumberColumn("Median return", format="%.3%"),
        },
    )

    warn_low_sample_counts(scorecard_df)


def render_admin_ops():
    section_header("Admin / Ops")
    st.caption(
        "Normal operation: daily task runs ingestion → signals → evaluation → KPIs. "
        "Use Admin/Ops for manual runs."
    )

    tabs = st.tabs(["Pipeline", "Ingestion", "Patterns", "Audit Log", "Advanced"])

    with tabs[0]:
        section_header("Pipeline controls")
        task_metadata, task_metadata_error = fetch_task_metadata(DAILY_PIPELINE_TASK)
        task_state = None
        task_schedule = None

        if task_metadata is not None and not task_metadata.empty:
            row = task_metadata.iloc[0]
            task_state = row.get("STATE")
            task_schedule = row.get("SCHEDULE")
        else:
            if task_metadata_error:
                st.warning(
                    "Task metadata is unavailable due to an error. "
                    f"Details: {task_metadata_error}"
                )
            else:
                st.info(
                    "Task metadata is unavailable. Confirm the task exists and that "
                    "your role has MONITOR or OWNERSHIP on the task."
                )

        col_task, col_state, col_schedule = st.columns(3)
        with col_task:
            st.markdown("**Task**")
            st.write(DAILY_PIPELINE_TASK)
        with col_state:
            st.markdown("**State**")
            st.write(task_state or "Unknown")
        with col_schedule:
            st.markdown("**Schedule**")
            st.write(task_schedule or "Unknown")

        if st.button("Run pipeline now"):
            with st.spinner("Running SP_RUN_DAILY_PIPELINE..."):
                try:
                    run_sql(f"call {DAILY_PIPELINE_SP}()").collect()
                    st.success("Pipeline triggered successfully.")
                except Exception as exc:
                    st.error(f"Failed to run pipeline: {exc}")

        st.markdown("### Task controls")
        operate_allowed = check_task_privilege(DAILY_PIPELINE_TASK, "OPERATE")
        ownership_allowed = check_task_privilege(DAILY_PIPELINE_TASK, "OWNERSHIP")
        can_manage = any(value is True for value in [operate_allowed, ownership_allowed])
        missing_privs = [
            priv
            for priv, allowed in [("OPERATE", operate_allowed), ("OWNERSHIP", ownership_allowed)]
            if allowed is False
        ]

        if can_manage:
            control_help = None
        elif missing_privs:
            control_help = f"Missing privilege(s): {', '.join(sorted(missing_privs))}."
        else:
            control_help = (
                "Unable to verify privileges. Ensure the role has OPERATE or OWNERSHIP."
            )

        control_cols = st.columns(2)
        with control_cols[0]:
            if st.button(
                "Suspend task",
                disabled=(
                    not can_manage
                    or (task_state is not None and str(task_state).upper() == "SUSPENDED")
                ),
                help=control_help,
            ):
                try:
                    run_sql(f"alter task {DAILY_PIPELINE_TASK} suspend").collect()
                    st.success("Task suspended.")
                except Exception as exc:
                    st.error(f"Failed to suspend task: {exc}")
        with control_cols[1]:
            if st.button(
                "Resume task",
                disabled=(
                    not can_manage
                    or (task_state is not None and str(task_state).upper() == "STARTED")
                ),
                help=control_help,
            ):
                try:
                    run_sql(f"alter task {DAILY_PIPELINE_TASK} resume").collect()
                    st.success("Task resumed.")
                except Exception as exc:
                    st.error(f"Failed to resume task: {exc}")

        if not can_manage:
            if missing_privs:
                st.info(
                    "Task controls are disabled. Missing privilege(s): "
                    f"{', '.join(sorted(missing_privs))}."
                )
            else:
                st.info(
                    "Task controls are disabled. Unable to verify task privileges "
                    "with the current role."
                )

        st.markdown("### Recent task runs")
        history_df = fetch_task_history(DAILY_PIPELINE_TASK, limit=20)
        if history_df is None or history_df.empty:
            st.caption("No task history available.")
        else:
            st.dataframe(history_df, use_container_width=True, height=320)

    with tabs[1]:
        section_header("Ingestion universe")
        st.caption("Manage AlphaVantage ingestion scope and run ingestion manually.")

        if st.button("Run ingestion now"):
            with st.spinner("Running AlphaVantage ingestion…"):
                try:
                    res = session.sql("call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS()").collect()
                    msg = res[0][0] if res and len(res[0]) > 0 else "Ingestion completed successfully."
                    st.success(msg)
                    st.session_state["post_ingest_checks"] = run_post_ingest_health_checks()
                except Exception as e:
                    st.error(f"Ingestion failed: {e}")

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
        st.metric("Enabled symbols", enabled_count)
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

    with tabs[2]:
        section_header("Pattern management")
        if st.button("Seed / refresh MOMENTUM_DEMO pattern"):
            res = run_sql("call MIP.APP.SP_SEED_MIP_DEMO()").collect()
            msg = res[0][0] if res and len(res[0]) > 0 else "Seed procedure completed."
            st.success(msg)

        patterns_df = to_pandas(
            run_sql(
                """
                select
                    PATTERN_ID,
                    NAME,
                    ENABLED,
                    PARAMS_JSON,
                    DESCRIPTION,
                    UPDATED_AT
                from MIP.APP.PATTERN_DEFINITION
                order by PATTERN_ID
                """
            )
        )

        if patterns_df is None or patterns_df.empty:
            st.info("No patterns defined yet.")
        else:
            edited_patterns = st.data_editor(
                patterns_df,
                use_container_width=True,
                disabled=["PATTERN_ID", "NAME", "PARAMS_JSON", "DESCRIPTION", "UPDATED_AT"],
                column_config={
                    "ENABLED": st.column_config.CheckboxColumn("Enabled"),
                },
                key="pattern_editor",
            )

            if st.button("Save pattern toggles"):
                changes = edited_patterns[["PATTERN_ID", "ENABLED"]].copy()
                for _, row in changes.iterrows():
                    run_sql(
                        f"""
                        update MIP.APP.PATTERN_DEFINITION
                        set ENABLED = {sql_literal(bool(row['ENABLED']))}
                        where PATTERN_ID = {int(row['PATTERN_ID'])}
                        """
                    ).collect()
                st.success("Pattern toggles updated.")

            st.markdown("### Pattern KPIs")
            kpi_filters_df = to_pandas(
                run_sql(
                    """
                    select distinct
                        MARKET_TYPE,
                        INTERVAL_MINUTES
                    from MIP.APP.V_PATTERN_KPIS
                    order by MARKET_TYPE, INTERVAL_MINUTES
                    """
                )
            )

            if kpi_filters_df is None or kpi_filters_df.empty:
                st.info("No pattern KPI data available yet.")
            else:
                market_options = kpi_filters_df["MARKET_TYPE"].dropna().unique().tolist()
                selected_kpi_market = st.selectbox(
                    "KPI market type", options=market_options, key="kpi_market_type"
                )

                interval_options = (
                    kpi_filters_df[kpi_filters_df["MARKET_TYPE"] == selected_kpi_market][
                        "INTERVAL_MINUTES"
                    ]
                    .dropna()
                    .astype(int)
                    .unique()
                    .tolist()
                )
                interval_options = sorted(interval_options)
                selected_kpi_interval = st.selectbox(
                    "KPI interval (minutes)",
                    options=interval_options,
                    key="kpi_interval_minutes",
                )

                horizon_options_df = to_pandas(
                    run_sql(
                        f"""
                        select distinct HORIZON_DAYS
                        from MIP.APP.V_PATTERN_KPIS
                        where MARKET_TYPE = '{selected_kpi_market}'
                          and INTERVAL_MINUTES = {selected_kpi_interval}
                        order by HORIZON_DAYS
                        """
                    )
                )
                horizon_options = (
                    horizon_options_df["HORIZON_DAYS"].dropna().astype(int).tolist()
                    if horizon_options_df is not None and not horizon_options_df.empty
                    else []
                )
                selected_horizons = st.multiselect(
                    "Horizon days",
                    options=horizon_options,
                    default=horizon_options,
                    key="kpi_horizon_days",
                )

                if selected_horizons:
                    horizon_list = ", ".join(str(int(h)) for h in selected_horizons)
                    kpi_query = f"""
                        select
                            coalesce(p.NAME, concat('Pattern ', k.PATTERN_ID)) as PATTERN_NAME,
                            k.HORIZON_DAYS,
                            k.SAMPLE_COUNT,
                            k.HIT_RATE,
                            k.AVG_FORWARD_RETURN,
                            k.MEDIAN_FORWARD_RETURN,
                            k.MIN_FORWARD_RETURN,
                            k.MAX_FORWARD_RETURN,
                            k.LAST_CALCULATED_AT
                        from MIP.APP.V_PATTERN_KPIS k
                        left join MIP.APP.PATTERN_DEFINITION p
                          on p.PATTERN_ID = k.PATTERN_ID
                        where k.MARKET_TYPE = '{selected_kpi_market}'
                          and k.INTERVAL_MINUTES = {selected_kpi_interval}
                          and k.HORIZON_DAYS in ({horizon_list})
                        order by k.HORIZON_DAYS, k.SAMPLE_COUNT desc, PATTERN_NAME
                    """

                    kpi_df = to_pandas(run_sql(kpi_query))
                else:
                    kpi_df = None

                if kpi_df is None or kpi_df.empty:
                    st.info("No KPI rows found for the selected filters.")
                else:
                    warn_low_sample_counts(kpi_df)
                    st.dataframe(kpi_df, use_container_width=True, height=260)

            st.markdown("### Pattern performance metrics")
            perf_df = to_pandas(
                run_sql(
                    """
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
                )
            )
            if perf_df is None or perf_df.empty:
                st.info("No pattern performance metrics available yet.")
            else:
                st.dataframe(perf_df, use_container_width=True, height=260)

    with tabs[3]:
        section_header("Audit log viewer")
        audit_filters = st.columns(4)
        default_end = date.today()
        default_start = default_end - timedelta(days=7)
        with audit_filters[0]:
            audit_start = st.date_input(
                "Start date", value=default_start, key="audit_start_date"
            )
        with audit_filters[1]:
            audit_end = st.date_input("End date", value=default_end, key="audit_end_date")

        status_options_df = fetch_audit_log_options("status")
        status_options = (
            status_options_df["STATUS"].dropna().tolist()
            if status_options_df is not None and not status_options_df.empty
            else []
        )
        with audit_filters[2]:
            audit_statuses = st.multiselect(
                "Status",
                options=status_options,
                default=status_options if status_options else None,
                key="audit_statuses",
            )

        event_type_options_df = fetch_audit_log_options("event_type")
        event_type_options = (
            event_type_options_df["EVENT_TYPE"].dropna().tolist()
            if event_type_options_df is not None and not event_type_options_df.empty
            else []
        )
        with audit_filters[3]:
            audit_event_types = st.multiselect(
                "Event type",
                options=event_type_options,
                default=event_type_options if event_type_options else None,
                key="audit_event_types",
            )

        audit_run_id = st.text_input(
            "Run ID (optional)",
            value="",
            help="Filter audit events by a specific run_id.",
            key="audit_run_id",
        )

        audit_df = fetch_audit_log(
            audit_start,
            audit_end,
            statuses=audit_statuses or None,
            event_types=audit_event_types or None,
            run_id=audit_run_id.strip() or None,
            limit=200,
        )

        if audit_df is None or audit_df.empty:
            st.caption("No audit log entries for the selected filters.")
        else:
            summary_cols = [
                "EVENT_TS",
                "EVENT_TYPE",
                "EVENT_NAME",
                "STATUS",
                "ROWS_AFFECTED",
                "RUN_ID",
                "ERROR_MESSAGE",
            ]
            summary_cols = [col for col in summary_cols if col in audit_df.columns]
            st.dataframe(audit_df[summary_cols], use_container_width=True, height=320)

            for _, row in audit_df.iterrows():
                title = (
                    f"{row.get('EVENT_TS')} • {row.get('EVENT_TYPE')} • "
                    f"{row.get('EVENT_NAME')} • {row.get('STATUS')}"
                )
                with st.expander(title):
                    st.write(f"Run ID: {row.get('RUN_ID')}")
                    if row.get("PARENT_RUN_ID"):
                        st.write(f"Parent Run ID: {row.get('PARENT_RUN_ID')}")
                    details = row.get("DETAILS")
                    if details:
                        st.json(details)
                    error_message = row.get("ERROR_MESSAGE")
                    if error_message:
                        st.error(error_message)

    with tabs[4]:
        section_header("Signals & evaluation")
        with st.expander("Market overview (latest bars)", expanded=False):
            render_market_overview_compact()

        with st.expander("Recommendations viewer", expanded=False):
            render_recommendations_viewer()

        with st.expander("Outcome evaluation & backtests", expanded=False):
            render_outcome_evaluation_admin()

        with st.expander("Generate momentum signals", expanded=False):
            market_type = st.selectbox("Market type", options=["STOCK", "FX"], index=0)
            interval_minutes = st.selectbox("Interval minutes", options=[5, 1440], index=1)
            min_return = st.number_input("min_return", value=0.002, format="%.4f")
            lookback_days = st.number_input("lookback_days", min_value=1, value=1, step=1)
            min_zscore = st.number_input("min_zscore", value=1.0, format="%.2f")

            if st.button("Generate momentum signals"):
                try:
                    msg = run_momentum_generator(
                        min_return,
                        market_type,
                        interval_minutes,
                        lookback_days=lookback_days,
                        min_zscore=min_zscore,
                    )
                    if "Warnings:" in msg:
                        st.warning(msg)
                    else:
                        st.success(msg)
                except Exception as e:
                    st.error(f"Momentum generation failed: {e}")

        with st.expander("Run learning cycle", expanded=False):
            market_timeframe_df = to_pandas(
                run_sql(
                    """
                    select
                        coalesce(PARAMS_JSON:market_type::string, 'STOCK') as MARKET_TYPE,
                        coalesce(PARAMS_JSON:interval_minutes::number, 1440) as INTERVAL_MINUTES
                    from MIP.APP.PATTERN_DEFINITION
                    where coalesce(IS_ACTIVE, 'N') = 'Y'
                      and coalesce(ENABLED, true)
                    order by MARKET_TYPE, INTERVAL_MINUTES
                    """
                )
            )
            if market_timeframe_df is None or market_timeframe_df.empty:
                st.info("No active patterns found for learning cycle.")
            else:
                options = [
                    (row["MARKET_TYPE"], int(row["INTERVAL_MINUTES"]))
                    for _, row in market_timeframe_df.iterrows()
                ]
                selected = st.multiselect("Market / timeframe", options=options, default=options)

                horizon_minutes = st.number_input("Horizon (minutes)", min_value=1, value=1440, step=1)
                hit_threshold = st.number_input("Hit threshold", value=0.002, format="%.6f")
                miss_threshold = st.number_input("Miss threshold", value=-0.002, format="%.6f")
                min_return = st.number_input(
                    "Min recommendation score to evaluate", value=0.0, format="%.6f"
                )
                incompatible_intervals = [
                    interval
                    for _, interval in selected
                    if interval is not None and horizon_minutes < int(interval)
                ]
                if incompatible_intervals:
                    st.warning(
                        "Horizon minutes is shorter than the selected interval minutes "
                        f"({sorted(set(incompatible_intervals))}). Please align the horizon with "
                        "the bar interval (e.g., 1440 for daily bars)."
                    )
                date_from = date.today() - timedelta(days=30)
                date_to = date.today()
                date_range = st.date_input("From / to (inclusive)", value=(date_from, date_to))
                if (
                    isinstance(date_range, (list, tuple))
                    and len(date_range) == 2
                    and all(date_range)
                ):
                    date_from = datetime.combine(date_range[0], datetime.min.time())
                    date_to = datetime.combine(date_range[1], datetime.max.time())

                if st.button("Run learning cycle"):
                    if not selected:
                        st.warning("Select at least one market / timeframe combination.")
                    else:
                        from_ts_str = date_from.strftime("%Y-%m-%d %H:%M:%S")
                        to_ts_str = date_to.strftime("%Y-%m-%d %H:%M:%S")
                        summaries = []
                        run_results = []
                        for market_type, interval_minutes in selected:
                            interval_sql = "NULL" if interval_minutes is None else str(int(interval_minutes))
                            call_sql = f"""
                                call MIP.APP.SP_RUN_MIP_LEARNING_CYCLE(
                                    '{market_type}',
                                    {interval_sql},
                                    {horizon_minutes},
                                    {min_return},
                                    {hit_threshold},
                                    {miss_threshold},
                                    to_timestamp_ntz('{from_ts_str}'),
                                    to_timestamp_ntz('{to_ts_str}'),
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
                            st.dataframe(run_results, use_container_width=True)
                        if summaries:
                            st.success("Learning cycle completed.")
                            for summary in summaries:
                                if summary is not None:
                                    if isinstance(summary, dict):
                                        st.json(summary)
                                    else:
                                        st.caption(str(summary))


# --- App layout ---
apply_layout(
    "Market Intelligence Platform",
    "Compact, portfolio-style console for pipeline health, opportunities, and risk.",
)

page = st.sidebar.radio(
    "Navigation",
    [
        "Overview",
        "Opportunities",
        "Portfolio",
        "Training & Trust",
        "Admin / Ops",
    ],
)

if page == "Overview":
    render_overview()
elif page == "Opportunities":
    render_opportunities()
elif page == "Portfolio":
    render_portfolio()
elif page == "Training & Trust":
    render_training_trust()
else:
    render_admin_ops()
