-- v_morning_brief_with_delta.sql
-- Purpose: Compare latest morning brief with prior run and summarize deltas

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_MORNING_BRIEF_WITH_DELTA as
with portfolio_scope as (
    select PORTFOLIO_ID
    from MIP.APP.PORTFOLIO
    where STATUS = 'ACTIVE'
),
ordered as (
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
    where portfolio_id in (select PORTFOLIO_ID from portfolio_scope)
),
curr as (
    select
        portfolio_id,
        as_of_ts,
        run_id,
        brief
    from ordered
    where rn = 1
),
prev as (
    select
        portfolio_id,
        as_of_ts,
        run_id,
        brief
    from ordered
    where rn = 2
),
curr_trusted as (
    select
        curr.portfolio_id,
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
        prev.portfolio_id,
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
        c.portfolio_id,
        array_agg(c.signal_key) as items
    from curr_trusted c
    left join prev_trusted p
        on p.portfolio_id = c.portfolio_id
       and to_json(c.signal_key) = to_json(p.signal_key)
    where p.signal_key is null
    group by c.portfolio_id
),
trusted_removed as (
    select
        p.portfolio_id,
        array_agg(p.signal_key) as items
    from prev_trusted p
    left join curr_trusted c
        on c.portfolio_id = p.portfolio_id
       and to_json(c.signal_key) = to_json(p.signal_key)
    where c.signal_key is null
    group by p.portfolio_id
),
curr_watch as (
    select
        curr.portfolio_id,
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
        prev.portfolio_id,
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
        c.portfolio_id,
        array_agg(c.signal_key) as items
    from curr_watch c
    left join prev_watch p
        on p.portfolio_id = c.portfolio_id
       and to_json(c.signal_key) = to_json(p.signal_key)
    where p.signal_key is null
    group by c.portfolio_id
),
watch_negative_removed as (
    select
        p.portfolio_id,
        array_agg(p.signal_key) as items
    from prev_watch p
    left join curr_watch c
        on c.portfolio_id = p.portfolio_id
       and to_json(c.signal_key) = to_json(p.signal_key)
    where c.signal_key is null
    group by p.portfolio_id
),
risk_changes as (
    select
        curr.portfolio_id,
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
        on prev.portfolio_id = curr.portfolio_id
)
select
    curr.portfolio_id,
    current_timestamp() as as_of_ts,
    object_construct(
        'curr', curr.brief,
        'prev_meta', object_construct(
            'prev_as_of_ts', prev.as_of_ts,
            'prev_run_id', prev.run_id
        ),
        'delta', object_construct(
            'trusted_added', coalesce(ta.items, array_construct()),
            'trusted_removed', coalesce(tr.items, array_construct()),
            'watch_negative_added', coalesce(wa.items, array_construct()),
            'watch_negative_removed', coalesce(wr.items, array_construct()),
            'risk_changes', rc.item
        )
    ) as brief
from curr
left join prev
    on prev.portfolio_id = curr.portfolio_id
left join trusted_added ta
    on ta.portfolio_id = curr.portfolio_id
left join trusted_removed tr
    on tr.portfolio_id = curr.portfolio_id
left join watch_negative_added wa
    on wa.portfolio_id = curr.portfolio_id
left join watch_negative_removed wr
    on wr.portfolio_id = curr.portfolio_id
left join risk_changes rc
    on rc.portfolio_id = curr.portfolio_id;
