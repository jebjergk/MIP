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
morning_brief_delta as (
    select
        brief
    from MIP.MART.V_MORNING_BRIEF_WITH_DELTA
),
latest_run as (
    select
        run_id,
        from_ts,
        to_ts
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
    where portfolio_id = 1
    order by to_ts desc
    limit 1
),
latest_kpis as (
    select
        run_id,
        from_ts,
        to_ts,
        total_return,
        avg_daily_return,
        daily_volatility,
        max_drawdown,
        row_number() over (order by to_ts desc) as rn
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
    where portfolio_id = 1
),
kpi_deltas as (
    select
        object_construct(
            'run_id', curr.run_id,
            'total_return', object_construct(
                'curr', curr.total_return,
                'prev', prev.total_return,
                'delta', curr.total_return - prev.total_return
            ),
            'avg_daily_return', object_construct(
                'curr', curr.avg_daily_return,
                'prev', prev.avg_daily_return,
                'delta', curr.avg_daily_return - prev.avg_daily_return
            ),
            'daily_volatility', object_construct(
                'curr', curr.daily_volatility,
                'prev', prev.daily_volatility,
                'delta', curr.daily_volatility - prev.daily_volatility
            ),
            'max_drawdown', object_construct(
                'curr', curr.max_drawdown,
                'prev', prev.max_drawdown,
                'delta', curr.max_drawdown - prev.max_drawdown
            )
        ) as item
    from latest_kpis curr
    left join latest_kpis prev
        on prev.rn = 2
    where curr.rn = 1
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
entry_gate_status as (
    select
        object_construct(
            'entries_blocked', ENTRIES_BLOCKED,
            'block_reason', BLOCK_REASON,
            'risk_status', RISK_STATUS,
            'drawdown_stop_ts', DRAWDOWN_STOP_TS,
            'open_positions', OPEN_POSITIONS
        ) as item
    from MIP.MART.V_PORTFOLIO_RISK_GATE
    where PORTFOLIO_ID = 1
),
latest_exposure as (
    select
        run_id,
        ts,
        cash,
        total_equity,
        open_positions,
        row_number() over (order by ts desc) as rn
    from MIP.APP.PORTFOLIO_DAILY
    where portfolio_id = 1
      and run_id = (select run_id from latest_run)
),
exposure_deltas as (
    select
        object_construct(
            'run_id', curr.run_id,
            'as_of_ts', curr.ts,
            'cash', object_construct(
                'curr', curr.cash,
                'prev', prev.cash,
                'delta', curr.cash - prev.cash
            ),
            'total_equity', object_construct(
                'curr', curr.total_equity,
                'prev', prev.total_equity,
                'delta', curr.total_equity - prev.total_equity
            ),
            'open_positions', object_construct(
                'curr', curr.open_positions,
                'prev', prev.open_positions,
                'delta', curr.open_positions - prev.open_positions
            )
        ) as item
    from latest_exposure curr
    left join latest_exposure prev
        on prev.rn = 2
    where curr.rn = 1
),
latest_proposal_run as (
    select
        run_id
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where portfolio_id = 1
    order by proposed_at desc
    limit 1
),
proposal_summary as (
    select
        object_construct(
            'run_id', (select run_id from latest_proposal_run),
            'total', count(*),
            'proposed', count_if(status = 'PROPOSED'),
            'approved', count_if(status in ('APPROVED', 'EXECUTED')),
            'rejected', count_if(status = 'REJECTED'),
            'executed', count_if(status = 'EXECUTED')
        ) as item
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where portfolio_id = 1
      and run_id = (select run_id from latest_proposal_run)
),
proposal_rejections as (
    select
        array_agg(
            object_construct(
                'proposal_id', proposal_id,
                'symbol', symbol,
                'market_type', market_type,
                'interval_minutes', interval_minutes,
                'validation_errors', validation_errors
            )
        ) as items
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where portfolio_id = 1
      and run_id = (select run_id from latest_proposal_run)
      and status = 'REJECTED'
),
executed_trades as (
    select
        array_agg(
            object_construct(
                'trade_id', trade_id,
                'symbol', symbol,
                'market_type', market_type,
                'side', side,
                'price', price,
                'quantity', quantity,
                'notional', notional,
                'trade_ts', trade_ts,
                'score', score
            )
        ) within group (order by trade_ts desc) as items
    from MIP.APP.PORTFOLIO_TRADES
    where portfolio_id = 1
      and run_id = to_varchar((select run_id from latest_proposal_run))
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
            'changes', coalesce((select brief from morning_brief_delta), object_construct())
        ),
        'risk', object_construct(
            'latest', (select item from latest_risk)
        ),
        'portfolio', object_construct(
            'kpis', coalesce((select item from kpi_deltas), object_construct()),
            'exposure', coalesce((select item from exposure_deltas), object_construct())
        ),
        'proposals', object_construct(
            'summary', coalesce((select item from proposal_summary), object_construct()),
            'rejected', coalesce((select items from proposal_rejections), array_construct()),
            'executed_trades', coalesce((select items from executed_trades), array_construct())
        ),
        'pipeline_run_id', (select run_id from latest_proposal_run),
        'attribution', object_construct(
            'latest_run_id', (select run_id from latest_run),
            'by_market_type', coalesce((select items from by_market_type), array_construct())
        )
    ) as BRIEF;
