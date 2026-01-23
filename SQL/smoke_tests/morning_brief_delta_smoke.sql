-- morning_brief_delta_smoke.sql
-- Smoke test for delta signal key changes

create temporary table temp_morning_brief (
    as_of_ts timestamp_ntz,
    portfolio_id number,
    run_id string,
    brief variant
);

insert into temp_morning_brief (as_of_ts, portfolio_id, run_id, brief)
select
    '2024-01-02 00:00:00'::timestamp_ntz,
    1,
    'RUN_2',
    object_construct(
        'signals', object_construct(
            'trusted_now', array_construct(
                object_construct(
                    'pattern_id', 101,
                    'market_type', 'STOCK',
                    'interval_minutes', 15,
                    'horizon_bars', 4
                ),
                object_construct(
                    'pattern_id', 102,
                    'market_type', 'FX',
                    'interval_minutes', 30,
                    'horizon_bars', 6
                )
            ),
            'watch_negative', array_construct()
        ),
        'risk', object_construct(
            'latest', object_construct(
                'total_return', 0.02,
                'max_drawdown', 0.05,
                'daily_volatility', 0.12,
                'drawdown_stop_pct', 0.15,
                'risk_status', 'OK',
                'stop_reason', null
            )
        )
    )
union all
select
    '2024-01-01 00:00:00'::timestamp_ntz,
    1,
    'RUN_1',
    object_construct(
        'signals', object_construct(
            'trusted_now', array_construct(
                object_construct(
                    'pattern_id', 101,
                    'market_type', 'STOCK',
                    'interval_minutes', 15,
                    'horizon_bars', 4
                )
            ),
            'watch_negative', array_construct()
        ),
        'risk', object_construct(
            'latest', object_construct(
                'total_return', 0.01,
                'max_drawdown', 0.06,
                'daily_volatility', 0.11,
                'drawdown_stop_pct', 0.15,
                'risk_status', 'OK',
                'stop_reason', null
            )
        )
    );

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
    from temp_morning_brief
    where portfolio_id = 1
),
curr as (
    select brief from ordered where rn = 1
),
prev as (
    select brief from ordered where rn = 2
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
delta as (
    select
        object_construct(
            'trusted_added', coalesce(
                (
                    select array_agg(c.signal_key)
                    from curr_trusted c
                    left join prev_trusted p
                        on to_json(c.signal_key) = to_json(p.signal_key)
                    where p.signal_key is null
                ),
                array_construct()
            ),
            'trusted_removed', coalesce(
                (
                    select array_agg(p.signal_key)
                    from prev_trusted p
                    left join curr_trusted c
                        on to_json(c.signal_key) = to_json(p.signal_key)
                    where c.signal_key is null
                ),
                array_construct()
            )
        ) as delta
)
select
    array_size(delta:trusted_added) + array_size(delta:trusted_removed) as trusted_changes,
    1 as expected_changes
from delta;
