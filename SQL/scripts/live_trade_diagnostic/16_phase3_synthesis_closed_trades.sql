-- 16_phase3_synthesis_closed_trades.sql
-- Phase 3 — Single cross-hypothesis synthesis row set for the 8 broker-truth closed FIFO losers.
-- Combines flags from training (H1), bracket (H2), propagation (H3), bar freshness (H4).
-- Dominant hypothesis label is heuristic; see memo for evidence vs inference.
-- Read-only.

use role MIP_ADMIN_ROLE;
use database MIP;

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
op as (
    select PROPOSAL_ID, SIGNAL_PATTERN_ID, INTERVAL_MINUTES, RECOMMENDATION_ID
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where PROPOSAL_ID in (6809, 6810, 6811, 6814, 7002, 7003, 7005, 7008)
),
h1 as (
    select RECOMMENDATION_ID, REALIZED_RETURN
    from MIP.APP.RECOMMENDATION_OUTCOMES
    where HORIZON_BARS = 1
      and EVAL_STATUS = 'SUCCESS'
),
la as (
    select
        ACTION_ID,
        ONE_MIN_BAR_TS,
        EXECUTION_PRICE_SOURCE,
        try_to_double(PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::varchar) as eb_sl,
        try_to_double(PARAM_SNAPSHOT:committee_bracket_baseline:stop_loss_pct::varchar) as cbb_sl,
        coalesce(PARAM_SNAPSHOT:executable_bracket:blocked::boolean, false) as eb_blk
    from MIP.LIVE.LIVE_ACTIONS
    where ACTION_ID in (select entry_action_id from closed_fifo)
),
tc as (
    select ENTRY_ACTION_ID, CLOSEOUT_ID from MIP.LIVE.TRADE_CLOSEOUT
    where ENTRY_ACTION_ID in (select entry_action_id from closed_fifo)
),
base as (
    select
        c.symbol,
        c.entry_ts,
        c.fifo_realized_usd,
        op.SIGNAL_PATTERN_ID as pattern_id,
        op.INTERVAL_MINUTES,
        h1.REALIZED_RETURN as training_h1_expected_return,
        case when not la.eb_blk and la.eb_sl is not null then la.eb_sl when la.cbb_sl is not null then la.cbb_sl else null end
            as stop_pct,
        datediff('second', la.ONE_MIN_BAR_TS, c.entry_ts) as bar_lag_sec_vs_entry,
        tc.CLOSEOUT_ID is not null as has_closeout,
        case
            when symbol in ('APA', 'XOM', 'COP') and abs((c.exit_px - c.entry_px) / nullif(c.entry_px, 0)) > 0.05
                then 'GAP_OR_OVERNIGHT_MOVE'
            when case when not la.eb_blk and la.eb_sl is not null then la.eb_sl when la.cbb_sl is not null then la.cbb_sl else null end is not null
                 and abs(c.exit_px - (c.entry_px * (1 - coalesce(
                     case when not la.eb_blk and la.eb_sl is not null then la.eb_sl when la.cbb_sl is not null then la.cbb_sl end, 0)))) / nullif(c.entry_px, 0) < 0.002
                then 'BRACKET_TOO_VULNERABLE'
            when datediff('second', la.ONE_MIN_BAR_TS, c.entry_ts) between 480 and 1020
                then 'STALE_MARKET_CONTEXT_SUSPECT'
            when coalesce(h1.REALIZED_RETURN, 0) < 0.002 and abs((c.exit_px - c.entry_px) / nullif(c.entry_px, 0)) > 0.01
                then 'TINY_EDGE_PATH_FRAGILE'
            when tc.CLOSEOUT_ID is null
                then 'OUTCOME_PROPAGATION_BROKEN'
            else 'MIXED'
        end as dominant_hypothesis_heuristic,
        case
            when tc.CLOSEOUT_ID is null then 'HIGH_CONF_PROPAGATION_FAIL'
            when datediff('second', la.ONE_MIN_BAR_TS, c.entry_ts) > 900 then 'HIGH_CONF_BAR_LAG'
            when symbol in ('APA', 'XOM', 'COP') and abs((c.exit_px - c.entry_px) / nullif(c.entry_px, 0)) > 0.05 then 'HIGH_CONF_GAP'
            else 'MEDIUM_CONF'
        end as confidence_note
    from closed_fifo c
    left join op on op.PROPOSAL_ID = c.proposal_id
    left join h1 on h1.RECOMMENDATION_ID = op.RECOMMENDATION_ID
    left join la on la.ACTION_ID = c.entry_action_id
    left join tc on tc.ENTRY_ACTION_ID = c.entry_action_id
)
select * from base
order by entry_ts;

