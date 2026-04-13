-- 05_bracket_risk_shape.sql — POPULATION A. Stop/target extraction precedence:
--   1) PARAM_SNAPSHOT:executable_bracket (skip if blocked:true)
--   2) PARAM_SNAPSHOT:committee_bracket_baseline
--   3) LIVE_ORDERS protective legs (TP/SL idempotency suffix or ORDER_TYPE hints)
--   4) else NULL + BRACKET_EXTRACTION_FAILED

use role MIP_ADMIN_ROLE;
use database MIP;

with
params as (
    select to_timestamp_ntz('2026-04-07') as reset_ts
),
live_portfolios as (
    select PORTFOLIO_ID, IBKR_ACCOUNT_ID
    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where upper(coalesce(ADAPTER_MODE, '')) = 'LIVE'
),
entry_leg_orders as (
    select lo.ORDER_ID, lo.ACTION_ID, lo.FILLED_AT, lo.AVG_FILL_PRICE, lo.IDEMPOTENCY_KEY,
        lo.ORDER_TYPE, lo.LIMIT_PRICE,
        la.PORTFOLIO_ID, la.SYMBOL, la.SIDE, la.PROPOSAL_ID, la.PARAM_SNAPSHOT, la.STATUS, la.UPDATED_AT
    from MIP.LIVE.LIVE_ORDERS lo
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = lo.ACTION_ID
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    where lo.FILLED_AT is not null and coalesce(lo.QTY_FILLED, 0) > 0
      and not regexp_like(coalesce(lo.IDEMPOTENCY_KEY, ''), ':(TP|SL|tp|sl)$')
      and upper(coalesce(lo.ACTION_INTENT, la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
),
first_entry_fill as (
    select * from entry_leg_orders
    qualify row_number() over (partition by ACTION_ID order by FILLED_AT asc nulls last, ORDER_ID asc) = 1
),
executed_entries_scoped as (
    select
        la.ACTION_ID as entry_action_id,
        la.PORTFOLIO_ID,
        la.SYMBOL,
        la.SIDE,
        la.PROPOSAL_ID,
        la.PARAM_SNAPSHOT,
        f.AVG_FILL_PRICE as order_entry_avg_price,
        f.QTY_FILLED as order_entry_qty,
        coalesce(f.FILLED_AT, tc.ENTRY_TS, iff(f.FILLED_AT is null and tc.ENTRY_TS is null
            and upper(coalesce(la.STATUS, '')) in ('EXECUTED','EXECUTION_REQUESTED','INTENT_APPROVED','REVALIDATED_PASS'),
            la.UPDATED_AT, null)) as canonical_entry_ts,
        tc.CLOSEOUT_ID,
        tc.REALIZED_SIZE
    from MIP.LIVE.LIVE_ACTIONS la
    inner join live_portfolios lp on lp.PORTFOLIO_ID = la.PORTFOLIO_ID
    left join first_entry_fill f on f.ACTION_ID = la.ACTION_ID
    left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = la.ACTION_ID
    where upper(coalesce(la.ACTION_INTENT, iff(upper(coalesce(la.SIDE, '')) = 'SELL', 'EXIT', 'ENTRY'))) = 'ENTRY'
      and (f.ACTION_ID is not null or tc.ENTRY_ACTION_ID is not null)
),
executed_entries_post_reset as (
    select e.* from executed_entries_scoped e cross join params p
    where e.canonical_entry_ts is not null and e.canonical_entry_ts >= p.reset_ts
),
protective_prices as (
    select
        lo.ACTION_ID,
        max(iff(
            regexp_like(coalesce(lo.IDEMPOTENCY_KEY, ''), ':(TP|tp)$')
            or contains(upper(coalesce(lo.ORDER_TYPE, '')), 'TP')
            or contains(upper(coalesce(lo.ORDER_TYPE, '')), 'TAKE_PROFIT'),
            try_to_double(lo.LIMIT_PRICE::varchar),
            null
        )) as broker_tp_limit_price,
        max(iff(
            regexp_like(coalesce(lo.IDEMPOTENCY_KEY, ''), ':(SL|sl)$')
            or contains(upper(coalesce(lo.ORDER_TYPE, '')), 'STOP')
            or contains(upper(coalesce(lo.ORDER_TYPE, '')), 'STP'),
            try_to_double(lo.LIMIT_PRICE::varchar),
            null
        )) as broker_sl_limit_price
    from MIP.LIVE.LIVE_ORDERS lo
    group by lo.ACTION_ID
),
parsed as (
    select
        e.*,
        try_to_double(e.PARAM_SNAPSHOT:executable_bracket:target_return::varchar) as eb_target_return,
        try_to_double(e.PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::varchar) as eb_stop_loss_pct,
        coalesce(e.PARAM_SNAPSHOT:executable_bracket:blocked::boolean, false) as eb_blocked,
        try_to_double(e.PARAM_SNAPSHOT:committee_bracket_baseline:realistic_target_return::varchar) as cbb_realistic_tr,
        try_to_double(e.PARAM_SNAPSHOT:committee_bracket_baseline:acceptable_early_exit_target_return::varchar) as cbb_early_tr,
        try_to_double(e.PARAM_SNAPSHOT:committee_bracket_baseline:stop_loss_pct::varchar) as cbb_stop_loss_pct,
        pp.broker_tp_limit_price,
        pp.broker_sl_limit_price
    from executed_entries_post_reset e
    left join protective_prices pp on pp.ACTION_ID = e.entry_action_id
),
derived as (
    select
        p.*,
        case
            when not p.eb_blocked
                 and p.eb_target_return is not null and p.eb_target_return > 0
                 and p.eb_stop_loss_pct is not null and p.eb_stop_loss_pct > 0
                then 'EXECUTABLE_BRACKET'
            when p.cbb_stop_loss_pct is not null and p.cbb_stop_loss_pct > 0
                 and coalesce(p.cbb_early_tr, p.cbb_realistic_tr) is not null
                 and coalesce(p.cbb_early_tr, p.cbb_realistic_tr) > 0
                then 'COMMITTEE_BRACKET_BASELINE'
            when p.broker_tp_limit_price is not null and p.broker_sl_limit_price is not null
                then 'ORDER_BROKER_PROTECTIVE'
            else 'EXTRACTION_FAILED'
        end as bracket_source_layer,
        case
            when not p.eb_blocked
                 and p.eb_target_return is not null and p.eb_stop_loss_pct is not null
                then p.eb_target_return
            when coalesce(p.cbb_early_tr, p.cbb_realistic_tr) is not null
                then coalesce(p.cbb_early_tr, p.cbb_realistic_tr)
            else null
        end as eff_target_return_pct,
        case
            when not p.eb_blocked
                 and p.eb_stop_loss_pct is not null
                then p.eb_stop_loss_pct
            when p.cbb_stop_loss_pct is not null
                then p.cbb_stop_loss_pct
            else null
        end as eff_stop_loss_pct,
        iff(
            upper(p.side) = 'BUY' and p.order_entry_avg_price is not null and p.broker_tp_limit_price is not null,
            (p.broker_tp_limit_price - p.order_entry_avg_price) / nullif(p.order_entry_avg_price, 0),
            iff(
                upper(p.side) = 'SELL' and p.order_entry_avg_price is not null and p.broker_tp_limit_price is not null,
                (p.order_entry_avg_price - p.broker_tp_limit_price) / nullif(p.order_entry_avg_price, 0),
                null
            )
        ) as broker_tp_return_pct,
        iff(
            upper(p.side) = 'BUY' and p.order_entry_avg_price is not null and p.broker_sl_limit_price is not null,
            (p.order_entry_avg_price - p.broker_sl_limit_price) / nullif(p.order_entry_avg_price, 0),
            iff(
                upper(p.side) = 'SELL' and p.order_entry_avg_price is not null and p.broker_sl_limit_price is not null,
                (p.broker_sl_limit_price - p.order_entry_avg_price) / nullif(p.order_entry_avg_price, 0),
                null
            )
        ) as broker_sl_return_pct
    from parsed p
),
final_bracket as (
    select
        d.*,
        case
            when d.bracket_source_layer = 'ORDER_BROKER_PROTECTIVE'
                then d.broker_tp_return_pct
            else d.eff_target_return_pct
        end as nominal_target_return_pct,
        case
            when d.bracket_source_layer = 'ORDER_BROKER_PROTECTIVE'
                then d.broker_sl_return_pct
            else d.eff_stop_loss_pct
        end as nominal_stop_loss_pct,
        d.bracket_source_layer = 'EXTRACTION_FAILED' as bracket_extraction_failed
    from derived d
),
nav_snap as (
    select
        lp.PORTFOLIO_ID,
        lp.IBKR_ACCOUNT_ID,
        bs.NET_LIQUIDATION_EUR,
        bs.SNAPSHOT_TS,
        row_number() over (
            partition by lp.PORTFOLIO_ID
            order by bs.SNAPSHOT_TS desc
        ) as rn
    from live_portfolios lp
    inner join MIP.LIVE.BROKER_SNAPSHOTS bs
        on bs.IBKR_ACCOUNT_ID = lp.IBKR_ACCOUNT_ID
       and bs.SNAPSHOT_TYPE = 'NAV'
)
select
    f.entry_action_id,
    f.symbol,
    f.side,
    f.canonical_entry_ts,
    f.order_entry_avg_price,
    f.bracket_source_layer,
    f.bracket_extraction_failed,
    round(f.nominal_target_return_pct, 6) as nominal_target_return_pct,
    round(f.nominal_stop_loss_pct, 6) as nominal_stop_loss_pct,
    iff(
        f.nominal_stop_loss_pct is not null and f.nominal_stop_loss_pct > 0,
        div0(f.nominal_target_return_pct, f.nominal_stop_loss_pct),
        null
    ) as nominal_reward_risk,
    abs(f.order_entry_avg_price * coalesce(f.realized_size, f.order_entry_qty)) as line_notional_proxy,
    ns.NET_LIQUIDATION_EUR as latest_nav_eur,
    iff(
        ns.NET_LIQUIDATION_EUR is not null and ns.NET_LIQUIDATION_EUR > 0,
        div0(
            abs(f.order_entry_avg_price * coalesce(f.realized_size, f.order_entry_qty, 0)),
            ns.NET_LIQUIDATION_EUR
        ),
        null
    ) as line_vs_nav_proxy,
    f.PARAM_SNAPSHOT:executable_bracket:calibrated::boolean as executable_bracket_calibrated_flag
from final_bracket f
left join nav_snap ns on ns.PORTFOLIO_ID = f.PORTFOLIO_ID and ns.rn = 1
order by f.canonical_entry_ts asc;
