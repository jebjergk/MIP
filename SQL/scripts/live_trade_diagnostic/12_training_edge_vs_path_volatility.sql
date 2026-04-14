-- 12_training_edge_vs_path_volatility.sql
-- Phase 3 — H1: Tiny expected edge vs real path volatility (DUQ101771, 8 closed FIFO losers).
-- Anchor: broker-truth entry/exit from closed_fifo CTE (same as 11).
-- Read-only.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Active training gate thresholds (symbol-global defaults in digest)
select
    'TRAINING_GATE_ACTIVE' as section,
    MIN_SIGNALS,
    MIN_SIGNALS_BOOTSTRAP,
    MIN_HIT_RATE,
    MIN_AVG_RETURN
from MIP.APP.TRAINING_GATE_PARAMS
where IS_ACTIVE
qualify row_number() over (order by PARAM_SET) = 1;

-- 2) Pattern / interval grain: trusted digest JSON for patterns used in cohort (2, 3), STOCK, 1440
select
    'TRAINING_DIGEST_PATTERN_GRAIN' as section,
    td.SYMBOL,
    td.PATTERN_ID,
    left(td.SNAPSHOT_JSON::varchar, 2000) as snapshot_json_truncated
from MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL td
where td.MARKET_TYPE = 'STOCK'
  and td.PATTERN_ID in (2, 3)
  and upper(trim(td.SYMBOL)) in (
      'SBUX', 'APA', 'XOM', 'COP', 'WMT', 'JNJ', 'NEE', 'PG'
  );

-- 3) Recommendation outcomes for the exact RECOMMENDATION_IDs behind the 8 proposals
with props as (
    select PROPOSAL_ID, RECOMMENDATION_ID, SYMBOL, SIGNAL_PATTERN_ID, INTERVAL_MINUTES
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where PROPOSAL_ID in (6809, 6810, 6811, 6814, 7002, 7003, 7005, 7008)
)
select
    'OUTCOMES_BY_HORIZON' as section,
    p.PROPOSAL_ID,
    p.SYMBOL,
    p.SIGNAL_PATTERN_ID as pattern_id,
    p.INTERVAL_MINUTES,
    o.HORIZON_BARS,
    o.REALIZED_RETURN,
    o.HIT_FLAG,
    o.EVAL_STATUS
from props p
left join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = p.RECOMMENDATION_ID
order by p.PROPOSAL_ID, o.HORIZON_BARS;

-- 4) Join training + path volatility flags onto the 8 closed trades
with closed_fifo as (
    select * from values
        ('SBUX', '29bef288-cf20-424b-8aec-f112e9add739', 6814,
         '2026-04-07T18:20:47'::timestamp_ntz, 94.91, '2026-04-07T18:51:18'::timestamp_ntz, 94.41, -1.0),
        ('APA', 'f14f4e4f-1d3f-40a1-9e6b-99eda96f0c0f', 6809,
         '2026-04-07T18:25:25'::timestamp_ntz, 43.19, '2026-04-08T13:30:50'::timestamp_ntz, 36.88, -18.93),
        ('COP', 'd4a18543-9785-4d24-ae6c-3ed62f194776', 6811,
         '2026-04-07T18:27:46'::timestamp_ntz, 131.53, '2026-04-08T13:32:10'::timestamp_ntz, 121.65, -9.88),
        ('XOM', '82590777-f2b9-4060-bfbd-cb0a7c022d43', 6810,
         '2026-04-07T18:46:15'::timestamp_ntz, 162.30, '2026-04-08T13:30:58'::timestamp_ntz, 151.84, -10.46),
        ('WMT', '39a042a1-414a-442f-91cf-6f843a031ebe', 7003,
         '2026-04-09T19:55:20'::timestamp_ntz, 129.24, '2026-04-10T14:32:05'::timestamp_ntz, 126.95, -2.29),
        ('JNJ', '858c0adc-6443-48cd-a267-14a75d35d4e1', 7005,
         '2026-04-09T18:12:21'::timestamp_ntz, 243.64, '2026-04-13T13:32:16'::timestamp_ntz, 237.18, -6.46),
        ('NEE', '8cd64d92-11ad-4db1-a3bb-36c89eff3e09', 7008,
         '2026-04-09T18:40:43'::timestamp_ntz, 94.68, '2026-04-13T15:21:47'::timestamp_ntz, 92.69, -3.98),
        ('PG', '34d2756e-4ec1-44ae-9c05-71a3cd4f4ad7', 7002,
         '2026-04-09T19:25:25'::timestamp_ntz, 146.69, '2026-04-13T15:30:14'::timestamp_ntz, 142.81, -3.88)
    as t(symbol, entry_action_id, proposal_id, entry_ts, entry_px, exit_ts, exit_px, fifo_realized_usd)
),
gate as (
    select MIN_AVG_RETURN, MIN_HIT_RATE
    from MIP.APP.TRAINING_GATE_PARAMS
    where IS_ACTIVE
    qualify row_number() over (order by PARAM_SET) = 1
),
op as (
    select PROPOSAL_ID, RECOMMENDATION_ID, SIGNAL_PATTERN_ID, INTERVAL_MINUTES
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where PROPOSAL_ID in (6809, 6810, 6811, 6814, 7002, 7003, 7005, 7008)
),
oh1 as (
    select SYMBOL, TS::date as d, HIGH, LOW, CLOSE
    from MIP.MART.MARKET_BARS
    where MARKET_TYPE = 'STOCK'
      and INTERVAL_MINUTES = 1440
),
entry_day as (
    select c.symbol, c.entry_ts, c.entry_px, c.exit_ts, c.exit_px, c.fifo_realized_usd, c.entry_action_id, c.proposal_id,
        e.HIGH as entry_day_high, e.LOW as entry_day_low,
        (e.HIGH - e.LOW) / nullif(c.entry_px, 0) as entry_day_range_pct
    from closed_fifo c
    left join oh1 e
        on upper(trim(e.SYMBOL)) = upper(trim(c.symbol))
       and e.d = c.entry_ts::date
),
next_day as (
    select c.symbol, n.HIGH as next_day_high, n.LOW as next_day_low, n.CLOSE as next_day_close,
        abs(n.LOW - c.entry_px) / nullif(c.entry_px, 0) as next_session_adverse_pct_vs_entry
    from closed_fifo c
    left join oh1 n
        on upper(trim(n.SYMBOL)) = upper(trim(c.symbol))
       and n.d = dateadd('day', 1, c.entry_ts::date)
),
h1_ret as (
    select o.RECOMMENDATION_ID, o.HORIZON_BARS, o.REALIZED_RETURN
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    where o.HORIZON_BARS = 1
      and o.EVAL_STATUS = 'SUCCESS'
),
la as (
    select
        ACTION_ID,
        try_to_double(PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::varchar) as eb_sl,
        try_to_double(PARAM_SNAPSHOT:committee_bracket_baseline:stop_loss_pct::varchar) as cbb_sl,
        coalesce(PARAM_SNAPSHOT:executable_bracket:blocked::boolean, false) as eb_blk
    from MIP.LIVE.LIVE_ACTIONS
    where ACTION_ID in (select entry_action_id from closed_fifo)
)
select
    e.symbol,
    e.proposal_id,
    op.SIGNAL_PATTERN_ID as pattern_id,
    op.INTERVAL_MINUTES,
    h1.REALIZED_RETURN as training_h1_realized_return,
    g.MIN_AVG_RETURN as gate_min_avg_return,
    g.MIN_HIT_RATE as gate_min_hit_rate,
    e.entry_day_range_pct,
    n.next_session_adverse_pct_vs_entry,
    (e.exit_px - e.entry_px) / nullif(e.entry_px, 0) as broker_round_trip_return_pct,
    case when not la.eb_blk and la.eb_sl is not null then la.eb_sl when la.cbb_sl is not null then la.cbb_sl else null end
        as eff_stop_loss_pct,
    iff(
        abs((e.exit_px - e.entry_px) / nullif(e.entry_px, 0)) > coalesce(g.MIN_AVG_RETURN, 0) * 50,
        'LOSS_DWARFS_GATE_MIN_AVG_RETURN',
        'LOSS_WITHIN_ORDER_OF_GATE'
    ) as flag_vs_gate,
    case
        when case when not la.eb_blk and la.eb_sl is not null then la.eb_sl when la.cbb_sl is not null then la.cbb_sl else null end is null
            then 'NO_STOP_PCT_IN_SNAPSHOT'
        when e.entry_day_range_pct > case when not la.eb_blk and la.eb_sl is not null then la.eb_sl when la.cbb_sl is not null then la.cbb_sl end
            then 'DAILY_RANGE_GT_STOP'
        else 'DAILY_RANGE_LTE_STOP'
    end as flag_range_vs_stop,
    iff(
        coalesce(abs(n.next_session_adverse_pct_vs_entry), 0) > 0.03,
        'NEXT_SESSION_MOVE_LARGE',
        'NEXT_SESSION_MOVE_MODERATE_OR_NULL'
    ) as flag_next_session
from entry_day e
cross join gate g
left join op on op.PROPOSAL_ID = e.proposal_id
left join h1_ret h1 on h1.RECOMMENDATION_ID = op.RECOMMENDATION_ID
left join next_day n on n.symbol = e.symbol
left join la on la.ACTION_ID = e.entry_action_id
order by e.entry_ts;
