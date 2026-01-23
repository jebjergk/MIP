-- v_morning_brief_json.sql
-- Purpose: Morning brief JSON view for trusted signals, risk, and attribution summary

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_MORNING_BRIEF_JSON as
with trusted_now as (
    select
        array_agg(
            object_construct(
                'pattern_id', pattern_id,
                'market_type', market_type,
                'interval_minutes', interval_minutes,
                'horizon_bars', horizon_bars,
                'trust_label', trust_label,
                'recommended_action', recommended_action,
                'reason', reason
            )
        ) within group (order by reason:avg_return::float desc, reason:n_success::int desc) as items
    from (
        select
            pattern_id,
            market_type,
            interval_minutes,
            horizon_bars,
            trust_label,
            recommended_action,
            reason
        from MIP.MART.V_TRUSTED_SIGNAL_POLICY
        where trust_label = 'TRUSTED'
        order by reason:avg_return::float desc, reason:n_success::int desc
        limit 10
    )
),
watch_negative as (
    select
        array_agg(
            object_construct(
                'pattern_id', pattern_id,
                'market_type', market_type,
                'interval_minutes', interval_minutes,
                'horizon_bars', horizon_bars,
                'trust_label', trust_label,
                'recommended_action', recommended_action,
                'previous_trust_label', previous_trust_label,
                'previous_recommended_action', previous_recommended_action,
                'reason', reason,
                'brief_category', brief_category
            )
        ) within group (order by reason:avg_return::float asc) as items
    from (
        select
            pattern_id,
            market_type,
            interval_minutes,
            horizon_bars,
            trust_label,
            recommended_action,
            previous_trust_label,
            previous_recommended_action,
            reason,
            brief_category
        from MIP.MART.V_AGENT_DAILY_SIGNAL_BRIEF
        where brief_category = 'WATCH_NEGATIVE_RETURN'
        order by reason:avg_return::float asc
        limit 10
    )
),
changes as (
    select
        array_agg(
            object_construct(
                'pattern_id', pattern_id,
                'market_type', market_type,
                'interval_minutes', interval_minutes,
                'horizon_bars', horizon_bars,
                'trust_label', trust_label,
                'recommended_action', recommended_action,
                'previous_trust_label', previous_trust_label,
                'previous_recommended_action', previous_recommended_action,
                'reason', reason,
                'brief_category', brief_category
            )
        ) within group (order by as_of_ts desc) as items
    from (
        select
            pattern_id,
            market_type,
            interval_minutes,
            horizon_bars,
            trust_label,
            recommended_action,
            previous_trust_label,
            previous_recommended_action,
            reason,
            brief_category,
            as_of_ts
        from MIP.MART.V_AGENT_DAILY_SIGNAL_BRIEF
        where previous_trust_label is not null
          and previous_trust_label <> trust_label
        order by as_of_ts desc
        limit 20
    )
),
latest_run as (
    select
        run_id,
        from_ts,
        to_ts
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
    where portfolio_id = 1
    order by to_ts desc, as_of_ts desc
    limit 1
),
latest_risk as (
    select
        object_construct(
            'run_id', run_id,
            'from_ts', from_ts,
            'to_ts', to_ts,
            'total_return', total_return,
            'max_drawdown', max_drawdown,
            'daily_volatility', daily_volatility,
            'stop_reason', stop_reason,
            'drawdown_stop_ts', drawdown_stop_ts,
            'risk_status', risk_status,
            'drawdown_stop_pct', drawdown_stop_pct
        ) as item,
        run_id
    from MIP.MART.V_AGENT_DAILY_RISK_BRIEF
    where portfolio_id = 1
      and run_id = (select run_id from latest_run)
    qualify row_number() over (order by as_of_ts desc) = 1
),
by_market_type as (
    select
        array_agg(
            object_construct(
                'market_type', market_type,
                'total_realized_pnl', total_realized_pnl,
                'roundtrips', roundtrips,
                'win_rate', win_rate,
                'top_contributors', top_contributors,
                'top_detractors', top_detractors
            )
        ) within group (order by market_type) as items
    from (
        select
            market_type,
            total_realized_pnl,
            roundtrips,
            win_rate,
            top_contributors,
            top_detractors
        from MIP.MART.V_AGENT_DAILY_ATTRIBUTION_BRIEF
        where portfolio_id = 1
          and run_id = (select run_id from latest_run)
          and market_type in ('STOCK', 'FX')
        order by market_type
        limit 2
    )
)
select
    current_timestamp() as AS_OF_TS,
    object_construct(
        'signals', object_construct(
            'trusted_now', coalesce((select items from trusted_now), array_construct()),
            'watch_negative', coalesce((select items from watch_negative), array_construct()),
            'changes', coalesce((select items from changes), array_construct())
        ),
        'risk', object_construct(
            'latest', (select item from latest_risk)
        ),
        'attribution', object_construct(
            'latest_run_id', (select run_id from latest_run),
            'by_market_type', coalesce((select items from by_market_type), array_construct())
        )
    ) as BRIEF;
