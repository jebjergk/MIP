-- 18_strategy_shape_misalignment.sql
-- Phase 4 — Freshness gate truth test vs broker cohort + strategy-shape / large-cap stopout taxonomy.
-- Anchor: 8 closed FIFO losers DUQ101771 (same closed_fifo keys as 11/15).
-- Read-only. Run with query_snowflake.py -s N per statement.
--
-- Statements: 1=USE ROLE, 2=USE DATABASE, 3=freshness gate, 4=strategy-shape, 5=large-cap only.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Freshness gate truth test (bar end vs broker entry vs portfolio threshold)
with closed_fifo as (
    select * from values
        ('SBUX', '29bef288-cf20-424b-8aec-f112e9add739',
         '2026-04-07T18:20:47'::timestamp_ntz),
        ('APA', 'f14f4e4f-1d3f-40a1-9e6b-99eda96f0c0f',
         '2026-04-07T18:25:25'::timestamp_ntz),
        ('COP', 'd4a18543-9785-4d24-ae6c-3ed62f194776',
         '2026-04-07T18:27:46'::timestamp_ntz),
        ('XOM', '82590777-f2b9-4060-bfbd-cb0a7c022d43',
         '2026-04-07T18:46:15'::timestamp_ntz),
        ('WMT', '39a042a1-414a-442f-91cf-6f843a031ebe',
         '2026-04-09T19:55:20'::timestamp_ntz),
        ('JNJ', '858c0adc-6443-48cd-a267-14a75d35d4e1',
         '2026-04-09T18:12:21'::timestamp_ntz),
        ('NEE', '8cd64d92-11ad-4db1-a3bb-36c89eff3e09',
         '2026-04-09T18:40:43'::timestamp_ntz),
        ('PG', '34d2756e-4ec1-44ae-9c05-71a3cd4f4ad7',
         '2026-04-09T19:25:25'::timestamp_ntz)
    as t(symbol, entry_action_id, entry_ts)
),
thresh as (
    select coalesce(QUOTE_FRESHNESS_THRESHOLD_SEC, 900) as freshness_sec
    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
    where PORTFOLIO_ID = 1
    limit 1
),
la as (
    select ACTION_ID, ONE_MIN_BAR_TS, UPDATED_AT
    from MIP.LIVE.LIVE_ACTIONS
    where ACTION_ID in (select entry_action_id from closed_fifo)
)
select
    c.symbol,
    c.entry_ts as entry_ts,
    la.one_min_bar_ts as one_min_bar_ts,
    datediff('second', la.one_min_bar_ts, c.entry_ts) as observed_lag_bar_end_to_entry_sec,
    datediff('second', la.one_min_bar_ts, la.updated_at) as observed_lag_bar_end_to_action_updated_sec,
    (select freshness_sec from thresh) as freshness_threshold_sec,
    /* Gate in live.py uses (now - bar_ts) at revalidation; UPDATED_AT is a weak proxy for "row touch" time. */
    iff(
        datediff('second', la.one_min_bar_ts, la.updated_at) <= (select freshness_sec from thresh),
        'PASS',
        'FAIL'
    ) as would_pass_bar_age_vs_action_updated,
    /* Strict reading: should bar end be within threshold seconds of broker entry? */
    iff(
        datediff('second', la.one_min_bar_ts, c.entry_ts) <= (select freshness_sec from thresh),
        'PASS',
        'FAIL'
    ) as would_pass_bar_end_vs_entry_under_same_threshold,
    case
        when la.one_min_bar_ts is null then 'UNKNOWN'
        when datediff('second', la.one_min_bar_ts, c.entry_ts) > (select freshness_sec from thresh)
            or datediff('second', la.one_min_bar_ts, la.updated_at) > (select freshness_sec from thresh)
            then 'HIGH'
        when datediff('second', la.one_min_bar_ts, c.entry_ts) > 300
            then 'MEDIUM'
        else 'LOW'
    end as delay_contamination_risk
from closed_fifo c
join la on la.action_id = c.entry_action_id
cross join thresh
order by c.entry_ts;

-- 2) Strategy-shape comparison + classification (includes training gate context)
with closed_fifo as (
    select * from values
        ('SBUX', '29bef288-cf20-424b-8aec-f112e9add739', 6814,
         '2026-04-07T18:20:47'::timestamp_ntz, 94.91, '2026-04-07T18:51:18'::timestamp_ntz, 94.41),
        ('APA', 'f14f4e4f-1d3f-40a1-9e6b-99eda96f0c0f', 6809,
         '2026-04-07T18:25:25'::timestamp_ntz, 43.19, '2026-04-08T13:30:50'::timestamp_ntz, 36.88),
        ('COP', 'd4a18543-9785-4d24-ae6c-3ed62f194776', 6811,
         '2026-04-07T18:27:46'::timestamp_ntz, 131.53, '2026-04-08T13:32:10'::timestamp_ntz, 121.65),
        ('XOM', '82590777-f2b9-4060-bfbd-cb0a7c022d43', 6810,
         '2026-04-07T18:46:15'::timestamp_ntz, 162.30, '2026-04-08T13:30:58'::timestamp_ntz, 151.84),
        ('WMT', '39a042a1-414a-442f-91cf-6f843a031ebe', 7003,
         '2026-04-09T19:55:20'::timestamp_ntz, 129.24, '2026-04-10T14:32:05'::timestamp_ntz, 126.95),
        ('JNJ', '858c0adc-6443-48cd-a267-14a75d35d4e1', 7005,
         '2026-04-09T18:12:21'::timestamp_ntz, 243.64, '2026-04-13T13:32:16'::timestamp_ntz, 237.18),
        ('NEE', '8cd64d92-11ad-4db1-a3bb-36c89eff3e09', 7008,
         '2026-04-09T18:40:43'::timestamp_ntz, 94.68, '2026-04-13T15:21:47'::timestamp_ntz, 92.69),
        ('PG', '34d2756e-4ec1-44ae-9c05-71a3cd4f4ad7', 7002,
         '2026-04-09T19:25:25'::timestamp_ntz, 146.69, '2026-04-13T15:30:14'::timestamp_ntz, 142.81)
    as t(symbol, entry_action_id, proposal_id, entry_ts, entry_px, exit_ts, exit_px)
),
gate as (
    select MIN_AVG_RETURN, MIN_HIT_RATE
    from MIP.APP.TRAINING_GATE_PARAMS
    where IS_ACTIVE
    qualify row_number() over (order by PARAM_SET) = 1
),
la as (
    select
        la.action_id,
        try_to_double(la.param_snapshot:executable_bracket:stop_loss_pct::varchar) as eb_sl_pct,
        try_to_double(la.param_snapshot:executable_bracket:target_return::varchar) as eb_tp_pct,
        coalesce(la.param_snapshot:executable_bracket:blocked::boolean, false) as eb_blocked,
        try_to_double(la.param_snapshot:committee_bracket_baseline:stop_loss_pct::varchar) as cbb_sl_pct,
        try_to_double(la.param_snapshot:committee_bracket_baseline:realistic_target_return::varchar) as cbb_rt_pct,
        try_to_double(la.param_snapshot:committee_bracket_baseline:acceptable_early_exit_target_return::varchar) as cbb_early_pct
    from MIP.LIVE.LIVE_ACTIONS la
),
enriched as (
    select
        c.*,
        case
            when not la.eb_blocked and la.eb_sl_pct is not null then 100.0 * la.eb_sl_pct
            when la.cbb_sl_pct is not null then 100.0 * la.cbb_sl_pct
            else null
        end as stop_pct_if_available,
        case
            when not la.eb_blocked and la.eb_tp_pct is not null then 100.0 * la.eb_tp_pct
            when coalesce(la.cbb_early_pct, la.cbb_rt_pct) is not null
                then 100.0 * coalesce(la.cbb_early_pct, la.cbb_rt_pct)
            else null
        end as target_pct_if_available,
        gate.min_avg_return,
        gate.min_hit_rate
    from closed_fifo c
    left join la on la.action_id = c.entry_action_id
    cross join gate
),
day_entry as (
    select
        e.*,
        d.high as entry_day_high,
        d.low as entry_day_low,
        d.close as entry_day_close
    from enriched e
    left join MIP.MART.MARKET_BARS d
        on upper(trim(d.symbol)) = upper(trim(e.symbol))
       and d.market_type = 'STOCK'
       and d.interval_minutes = 1440
       and d.ts::date = e.entry_ts::date
),
day_next as (
    select
        de.*,
        n.high as next_cal_day_high,
        n.low as next_cal_day_low
    from day_entry de
    left join MIP.MART.MARKET_BARS n
        on upper(trim(n.symbol)) = upper(trim(de.symbol))
       and n.market_type = 'STOCK'
       and n.interval_minutes = 1440
       and n.ts::date = dateadd('day', 1, de.entry_ts::date)
),
recovery as (
    select
        dn.*,
        r.max_hi as post_exit_max_daily_high,
        (r.max_hi - dn.exit_px) / nullif(dn.exit_px, 0) * 100.0 as post_exit_recovery_pct
    from day_next dn
    left join (
        select
            c.symbol,
            c.exit_ts,
            c.exit_px,
            max(b.high) as max_hi
        from closed_fifo c
        left join MIP.MART.MARKET_BARS b
            on upper(trim(b.symbol)) = upper(trim(c.symbol))
           and b.market_type = 'STOCK'
           and b.interval_minutes = 1440
           and b.ts::date >= c.exit_ts::date
           and b.ts::date <= '2026-04-13'
        group by c.symbol, c.exit_ts, c.exit_px
    ) r on r.symbol = dn.symbol and r.exit_ts = dn.exit_ts and r.exit_px = dn.exit_px
)
select
    symbol,
    entry_px,
    exit_px,
    (exit_px - entry_px) / nullif(entry_px, 0) * 100.0 as realized_loss_pct,
    stop_pct_if_available,
    target_pct_if_available,
    100.0 * min_avg_return as min_avg_return_pct_gate,
    100.0 * min_hit_rate as min_hit_rate_pct_gate,
    case
        when entry_day_high is not null and entry_day_low is not null
            then (entry_day_high - entry_day_low) / nullif(entry_px, 0) * 100.0
        else null
    end as entry_day_range_pct,
    case
        when next_cal_day_high is not null and next_cal_day_low is not null
            then (next_cal_day_high - next_cal_day_low) / nullif(entry_px, 0) * 100.0
        else null
    end as next_calendar_day_range_pct,
    post_exit_recovery_pct,
    /* Diagnostic only: illustrative wider stop envelope = 1.5x persisted stop % */
    stop_pct_if_available * 1.5 as hypothetical_wider_stop_pct_x1_5_diagnostic,
    case
        when upper(symbol) in ('APA', 'COP', 'XOM')
             and exit_ts::date > entry_ts::date
             and (exit_px - entry_px) / nullif(entry_px, 0) * 100.0 < -5
            then 'GAP_DOMINATED'
        when stop_pct_if_available is not null
             and abs((exit_px - entry_px) / nullif(entry_px, 0) * 100.0) <= stop_pct_if_available * 1.2
             and (exit_px - entry_px) / nullif(entry_px, 0) < 0
            then 'STOP_DOMINATED'
        when min_avg_return is not null
             and abs((exit_px - entry_px) / nullif(entry_px, 0)) < 0.02
             and (stop_pct_if_available is not null and stop_pct_if_available < 3.0)
             and entry_day_high is not null
             and (entry_day_high - entry_day_low) / nullif(entry_px, 0) * 100.0 > coalesce(stop_pct_if_available, 0) * 1.5
            then 'SMALL_EDGE_TIGHT_RISK'
        when post_exit_recovery_pct is not null
             and post_exit_recovery_pct > 2.0
             and abs((exit_px - entry_px) / nullif(entry_px, 0) * 100.0) < 8.0
             and upper(symbol) in ('WMT', 'JNJ', 'NEE', 'PG', 'SBUX')
            then 'PATIENT_HOLD_THESIS_CONFLICT'
        else 'INSUFFICIENT_EVIDENCE'
    end as strategy_shape_classification
from recovery
order by entry_ts;

-- 3) Large-cap / “strong name” subset only (same rows, filter)
with closed_fifo as (
    select * from values
        ('SBUX', '29bef288-cf20-424b-8aec-f112e9add739', 6814,
         '2026-04-07T18:20:47'::timestamp_ntz, 94.91, '2026-04-07T18:51:18'::timestamp_ntz, 94.41),
        ('WMT', '39a042a1-414a-442f-91cf-6f843a031ebe', 7003,
         '2026-04-09T19:55:20'::timestamp_ntz, 129.24, '2026-04-10T14:32:05'::timestamp_ntz, 126.95),
        ('JNJ', '858c0adc-6443-48cd-a267-14a75d35d4e1', 7005,
         '2026-04-09T18:12:21'::timestamp_ntz, 243.64, '2026-04-13T13:32:16'::timestamp_ntz, 237.18),
        ('NEE', '8cd64d92-11ad-4db1-a3bb-36c89eff3e09', 7008,
         '2026-04-09T18:40:43'::timestamp_ntz, 94.68, '2026-04-13T15:21:47'::timestamp_ntz, 92.69),
        ('PG', '34d2756e-4ec1-44ae-9c05-71a3cd4f4ad7', 7002,
         '2026-04-09T19:25:25'::timestamp_ntz, 146.69, '2026-04-13T15:30:14'::timestamp_ntz, 142.81)
    as t(symbol, entry_action_id, proposal_id, entry_ts, entry_px, exit_ts, exit_px)
),
gate as (
    select MIN_AVG_RETURN from MIP.APP.TRAINING_GATE_PARAMS where IS_ACTIVE
    qualify row_number() over (order by PARAM_SET) = 1
),
la as (
    select
        la.action_id,
        try_to_double(la.param_snapshot:executable_bracket:stop_loss_pct::varchar) as eb_sl_pct,
        coalesce(la.param_snapshot:executable_bracket:blocked::boolean, false) as eb_blocked,
        try_to_double(la.param_snapshot:committee_bracket_baseline:stop_loss_pct::varchar) as cbb_sl_pct
    from MIP.LIVE.LIVE_ACTIONS la
),
base as (
    select
        c.symbol,
        c.entry_px,
        c.exit_px,
        (c.exit_px - c.entry_px) / nullif(c.entry_px, 0) * 100.0 as realized_loss_pct,
        case
            when not la.eb_blocked and la.eb_sl_pct is not null then 100.0 * la.eb_sl_pct
            when la.cbb_sl_pct is not null then 100.0 * la.cbb_sl_pct
            else null
        end as stop_pct_if_available,
        d.high as ed_high,
        d.low as ed_low
    from closed_fifo c
    left join la on la.action_id = c.entry_action_id
    left join MIP.MART.MARKET_BARS d
        on upper(trim(d.symbol)) = upper(trim(c.symbol))
       and d.market_type = 'STOCK'
       and d.interval_minutes = 1440
       and d.ts::date = c.entry_ts::date
    cross join gate
)
select
    symbol,
    realized_loss_pct,
    stop_pct_if_available,
    (ed_high - ed_low) / nullif(entry_px, 0) * 100.0 as entry_day_range_pct,
    case
        when upper(symbol) in ('APA', 'COP', 'XOM') then 'N/A_ENERGY_NOT_IN_STRONG_NAME_SET'
        when stop_pct_if_available is not null
             and realized_loss_pct < 0
             and abs(realized_loss_pct) <= stop_pct_if_available * 1.15
            then 'VOLATILITY_OR_DISCIPLINED_STOP_VS_RANGE'
        when realized_loss_pct between -3 and 0
             and stop_pct_if_available is not null
             and (ed_high - ed_low) / nullif(entry_px, 0) * 100.0 > stop_pct_if_available * 1.2
            then 'DRIFT_OR_SCRATCH_IN_WIDE_DAY'
        when realized_loss_pct < -3
             and (ed_high - ed_low) / nullif(entry_px, 0) * 100.0 > abs(realized_loss_pct)
            then 'GAP_OR_NEWS_SHOCK_NOT_SOLVED_BY_STOP_WIDTH_ALONE'
        else 'REVIEW_MANUALLY'
    end as large_cap_stopout_category,
    'Compare realized_loss_pct to entry_day_range_pct (wide-stop column in stmt 4 is diagnostic only)' as note
from base
order by symbol;
