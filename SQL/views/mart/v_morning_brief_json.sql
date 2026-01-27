-- v_morning_brief_json.sql
-- Purpose: Morning brief JSON view for trusted signals, risk, and attribution summary

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_MORNING_BRIEF_JSON (
    PORTFOLIO_ID,
    AS_OF_TS,
    BRIEF
) as
with portfolio_scope as (
    select PORTFOLIO_ID
    from MIP.APP.PORTFOLIO
    where STATUS = 'ACTIVE'
),
trusted_now as (
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
        portfolio_id,
        brief
    from MIP.MART.V_MORNING_BRIEF_WITH_DELTA
),
latest_run as (
    select
        portfolio_id,
        run_id,
        from_ts,
        to_ts
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
    qualify row_number() over (
        partition by portfolio_id
        order by to_ts desc
    ) = 1
),
latest_kpis as (
    select
        portfolio_id,
        run_id,
        from_ts,
        to_ts,
        total_return,
        avg_daily_return,
        daily_volatility,
        max_drawdown,
        row_number() over (
            partition by portfolio_id
            order by to_ts desc
        ) as rn
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
),
kpi_deltas as (
    select
        curr.portfolio_id,
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
        on prev.portfolio_id = curr.portfolio_id
       and prev.rn = 2
    where curr.rn = 1
),
latest_risk as (
    select
        r.portfolio_id,
        object_construct(
            'run_id', r.run_id,
            'from_ts', r.from_ts,
            'to_ts', r.to_ts,
            'total_return', r.total_return,
            'max_drawdown', r.max_drawdown,
            'daily_volatility', r.daily_volatility,
            'stop_reason', r.stop_reason,
            'drawdown_stop_ts', r.drawdown_stop_ts,
            'risk_status', r.risk_status,
            'drawdown_stop_pct', r.drawdown_stop_pct
        ) as item,
        r.run_id
    from MIP.MART.V_AGENT_DAILY_RISK_BRIEF r
    join latest_run lr
      on lr.portfolio_id = r.portfolio_id
     and lr.run_id = r.run_id
    qualify row_number() over (
        partition by r.portfolio_id
        order by r.as_of_ts desc
    ) = 1
),
latest_exposure as (
    select
        d.portfolio_id,
        d.run_id,
        d.ts,
        d.cash,
        d.total_equity,
        d.open_positions,
        row_number() over (
            partition by d.portfolio_id
            order by d.ts desc
        ) as rn
    from MIP.APP.PORTFOLIO_DAILY d
    join latest_run lr
      on lr.portfolio_id = d.portfolio_id
     and lr.run_id = d.run_id
),
exposure_deltas as (
    select
        curr.portfolio_id,
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
        on prev.portfolio_id = curr.portfolio_id
       and prev.rn = 2
    where curr.rn = 1
),
latest_proposal_run as (
    select
        portfolio_id,
        run_id
    from (
        select
            portfolio_id,
            run_id,
            row_number() over (
                partition by portfolio_id
                order by proposed_at desc
            ) as rn
        from MIP.AGENT_OUT.ORDER_PROPOSALS
    )
    where rn = 1
),
proposal_summary as (
    select
        p.portfolio_id,
        object_construct(
            'run_id', p.run_id,
            'total', count(*),
            'proposed', count_if(status = 'PROPOSED'),
            'approved', count_if(status in ('APPROVED', 'EXECUTED')),
            'rejected', count_if(status = 'REJECTED'),
            'executed', count_if(status = 'EXECUTED')
        ) as item
    from MIP.AGENT_OUT.ORDER_PROPOSALS op
    join latest_proposal_run p
      on p.portfolio_id = op.portfolio_id
     and p.run_id = op.run_id
    group by p.portfolio_id, p.run_id
),
proposal_rejections as (
    select
        p.portfolio_id,
        array_agg(
            object_construct(
                'proposal_id', proposal_id,
                'symbol', symbol,
                'market_type', market_type,
                'interval_minutes', interval_minutes,
                'validation_errors', validation_errors
            )
        ) as items
    from MIP.AGENT_OUT.ORDER_PROPOSALS op
    join latest_proposal_run p
      on p.portfolio_id = op.portfolio_id
     and p.run_id = op.run_id
    where op.status = 'REJECTED'
    group by p.portfolio_id
),
executed_trades as (
    select
        p.portfolio_id,
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
    join latest_proposal_run p
      on p.portfolio_id = PORTFOLIO_TRADES.portfolio_id
     and PORTFOLIO_TRADES.run_id = to_varchar(p.run_id)
    group by p.portfolio_id
),
by_market_type as (
    select
        b.portfolio_id,
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
    from MIP.MART.V_AGENT_DAILY_ATTRIBUTION_BRIEF b
    join latest_run lr
      on lr.portfolio_id = b.portfolio_id
     and lr.run_id = b.run_id
    where b.market_type in ('STOCK', 'FX')
    group by b.portfolio_id
)
select
    p.portfolio_id,
    current_timestamp() as AS_OF_TS,
    object_construct(
        'signals', object_construct(
            'trusted_now', coalesce(tn.items, array_construct()),
            'watch_negative', coalesce(wn.items, array_construct()),
            'changes', coalesce(mbd.brief, object_construct())
        ),
        'risk', object_construct(
            'latest', lrisk.item
        ),
        'portfolio', object_construct(
            'kpis', coalesce(kpi.item, object_construct()),
            'exposure', coalesce(exposure.item, object_construct())
        ),
        'proposals', object_construct(
            'summary', coalesce(psummary.item, object_construct()),
            'rejected', coalesce(preject.items, array_construct()),
            'executed_trades', coalesce(etrades.items, array_construct())
        ),
        'pipeline_run_id', lpr.run_id,
        'attribution', object_construct(
            'latest_run_id', lrun.run_id,
            'by_market_type', coalesce(bmt.items, array_construct())
        )
    ) as BRIEF
from portfolio_scope p
cross join trusted_now tn
cross join watch_negative wn
left join morning_brief_delta mbd
  on mbd.portfolio_id = p.portfolio_id
left join latest_run lrun
  on lrun.portfolio_id = p.portfolio_id
left join kpi_deltas kpi
  on kpi.portfolio_id = p.portfolio_id
left join latest_risk lrisk
  on lrisk.portfolio_id = p.portfolio_id
left join exposure_deltas exposure
  on exposure.portfolio_id = p.portfolio_id
left join latest_proposal_run lpr
  on lpr.portfolio_id = p.portfolio_id
left join proposal_summary psummary
  on psummary.portfolio_id = p.portfolio_id
left join proposal_rejections preject
  on preject.portfolio_id = p.portfolio_id
left join executed_trades etrades
  on etrades.portfolio_id = p.portfolio_id
left join by_market_type bmt
  on bmt.portfolio_id = p.portfolio_id;
