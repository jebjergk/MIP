-- v_morning_brief_with_delta.sql
-- Purpose: Compare latest morning brief with prior run and summarize deltas

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_MORNING_BRIEF_WITH_DELTA as
with ordered as (
    select
        as_of_ts,
        portfolio_id,
        run_id,
        brief,
        row_number() over (
            partition by portfolio_id
            order by as_of_ts desc
        ) as rn
    from MIP.AGENT_OUT.MORNING_BRIEF
    where portfolio_id = 1
),
curr as (
    select
        as_of_ts,
        run_id,
        brief
    from ordered
    where rn = 1
),
prev as (
    select
        as_of_ts,
        run_id,
        brief
    from ordered
    where rn = 2
),
curr_trusted as (
    select
        object_construct(
            'pattern_id', value:pattern_id,
            'market_type', value:market_type,
            'interval_minutes', value:interval_minutes,
            'horizon_bars', value:horizon_bars
        ) as signal_key
    from curr,
        lateral flatten(input => curr.brief:signals:trusted_now)
),
prev_trusted as (
    select
        object_construct(
            'pattern_id', value:pattern_id,
            'market_type', value:market_type,
            'interval_minutes', value:interval_minutes,
            'horizon_bars', value:horizon_bars
        ) as signal_key
    from prev,
        lateral flatten(input => prev.brief:signals:trusted_now)
),
trusted_added as (
    select
        array_agg(signal_key) as items
    from (
        select c.signal_key
        from curr_trusted c
        left join prev_trusted p
            on to_json(c.signal_key) = to_json(p.signal_key)
        where p.signal_key is null
    )
),
trusted_removed as (
    select
        array_agg(signal_key) as items
    from (
        select p.signal_key
        from prev_trusted p
        left join curr_trusted c
            on to_json(c.signal_key) = to_json(p.signal_key)
        where c.signal_key is null
    )
),
curr_watch as (
    select
        object_construct(
            'pattern_id', value:pattern_id,
            'market_type', value:market_type,
            'interval_minutes', value:interval_minutes,
            'horizon_bars', value:horizon_bars
        ) as signal_key
    from curr,
        lateral flatten(input => curr.brief:signals:watch_negative)
),
prev_watch as (
    select
        object_construct(
            'pattern_id', value:pattern_id,
            'market_type', value:market_type,
            'interval_minutes', value:interval_minutes,
            'horizon_bars', value:horizon_bars
        ) as signal_key
    from prev,
        lateral flatten(input => prev.brief:signals:watch_negative)
),
watch_negative_added as (
    select
        array_agg(signal_key) as items
    from (
        select c.signal_key
        from curr_watch c
        left join prev_watch p
            on to_json(c.signal_key) = to_json(p.signal_key)
        where p.signal_key is null
    )
),
watch_negative_removed as (
    select
        array_agg(signal_key) as items
    from (
        select p.signal_key
        from prev_watch p
        left join curr_watch c
            on to_json(c.signal_key) = to_json(p.signal_key)
        where c.signal_key is null
    )
),
risk_changes as (
    select
        object_construct(
            'total_return', object_construct(
                'curr', curr.brief:risk:latest:total_return::float,
                'prev', prev.brief:risk:latest:total_return::float,
                'delta', curr.brief:risk:latest:total_return::float
                    - prev.brief:risk:latest:total_return::float
            ),
            'max_drawdown', object_construct(
                'curr', curr.brief:risk:latest:max_drawdown::float,
                'prev', prev.brief:risk:latest:max_drawdown::float,
                'delta', curr.brief:risk:latest:max_drawdown::float
                    - prev.brief:risk:latest:max_drawdown::float
            ),
            'daily_volatility', object_construct(
                'curr', curr.brief:risk:latest:daily_volatility::float,
                'prev', prev.brief:risk:latest:daily_volatility::float,
                'delta', curr.brief:risk:latest:daily_volatility::float
                    - prev.brief:risk:latest:daily_volatility::float
            ),
            'drawdown_stop_pct', object_construct(
                'curr', curr.brief:risk:latest:drawdown_stop_pct::float,
                'prev', prev.brief:risk:latest:drawdown_stop_pct::float,
                'delta', curr.brief:risk:latest:drawdown_stop_pct::float
                    - prev.brief:risk:latest:drawdown_stop_pct::float
            ),
            'risk_status', object_construct(
                'curr', curr.brief:risk:latest:risk_status::string,
                'prev', prev.brief:risk:latest:risk_status::string,
                'changed', case
                    when prev.brief is null then null
                    else curr.brief:risk:latest:risk_status::string
                        <> prev.brief:risk:latest:risk_status::string
                end
            ),
            'stop_reason', object_construct(
                'curr', curr.brief:risk:latest:stop_reason::string,
                'prev', prev.brief:risk:latest:stop_reason::string,
                'changed', case
                    when prev.brief is null then null
                    else curr.brief:risk:latest:stop_reason::string
                        <> prev.brief:risk:latest:stop_reason::string
                end
            )
        ) as item
    from curr
    left join prev
        on true
)
select
    current_timestamp() as as_of_ts,
    object_construct(
        'curr', (select brief from curr),
        'prev_meta', object_construct(
            'prev_as_of_ts', (select as_of_ts from prev),
            'prev_run_id', (select run_id from prev)
        ),
        'delta', object_construct(
            'trusted_added', coalesce((select items from trusted_added), array_construct()),
            'trusted_removed', coalesce((select items from trusted_removed), array_construct()),
            'watch_negative_added', coalesce((select items from watch_negative_added), array_construct()),
            'watch_negative_removed', coalesce((select items from watch_negative_removed), array_construct()),
            'risk_changes', (select item from risk_changes)
        )
    ) as brief;
