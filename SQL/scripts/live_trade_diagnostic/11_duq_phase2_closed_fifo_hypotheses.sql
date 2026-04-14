-- 11_duq_phase2_closed_fifo_hypotheses.sql
-- Phase-2 diagnostics: canonical population = 8 closed FIFO round-trips (DUQ101771, post-reset).
-- Broker-truth entry/exit times and prices are fixed below; MIP objects are enrichment only.
-- Read-only.

use role MIP_ADMIN_ROLE;
use database MIP;

-- A) Cross-trade enrichment: regime (stale), committee verdict, proposal pattern, bar anchor, bracket %, prior daily close
with closed_fifo as (
    select * from values
        ('SBUX', '29bef288-cf20-424b-8aec-f112e9add739', 6814, '19079058-7ebb-4d59-a28b-7abefc29f969',
         '2026-04-07T18:20:47'::timestamp_ntz, 94.91, '2026-04-07T18:51:18'::timestamp_ntz, 94.41, -1.0),
        ('APA', 'f14f4e4f-1d3f-40a1-9e6b-99eda96f0c0f', 6809, '261454aa-53b5-4516-918e-45094975b0a9',
         '2026-04-07T18:25:25'::timestamp_ntz, 43.19, '2026-04-08T13:30:50'::timestamp_ntz, 36.88, -18.93),
        ('COP', 'd4a18543-9785-4d24-ae6c-3ed62f194776', 6811, '56117a48-61c3-4707-b4e9-c0e1159ff6d0',
         '2026-04-07T18:27:46'::timestamp_ntz, 131.53, '2026-04-08T13:32:10'::timestamp_ntz, 121.65, -9.88),
        ('XOM', '82590777-f2b9-4060-bfbd-cb0a7c022d43', 6810, '3c8965a0-348a-4737-ba7d-0c244512064b',
         '2026-04-07T18:46:15'::timestamp_ntz, 162.30, '2026-04-08T13:30:58'::timestamp_ntz, 151.84, -10.46),
        ('WMT', '39a042a1-414a-442f-91cf-6f843a031ebe', 7003, 'edf6a213-17ad-42ee-a457-80bde1c88730',
         '2026-04-09T19:55:20'::timestamp_ntz, 129.24, '2026-04-10T14:32:05'::timestamp_ntz, 126.95, -2.29),
        ('JNJ', '858c0adc-6443-48cd-a267-14a75d35d4e1', 7005, '0c14eee9-472d-4ea1-a714-c534b2100821',
         '2026-04-09T18:12:21'::timestamp_ntz, 243.64, '2026-04-13T13:32:16'::timestamp_ntz, 237.18, -6.46),
        ('NEE', '8cd64d92-11ad-4db1-a3bb-36c89eff3e09', 7008, '7f8a65b2-6870-42ad-bab1-846bbd19feb1',
         '2026-04-09T18:40:43'::timestamp_ntz, 94.68, '2026-04-13T15:21:47'::timestamp_ntz, 92.69, -3.98),
        ('PG', '34d2756e-4ec1-44ae-9c05-71a3cd4f4ad7', 7002, '1a46237f-ea45-498d-a1bd-97d308becc43',
         '2026-04-09T19:25:25'::timestamp_ntz, 146.69, '2026-04-13T15:30:14'::timestamp_ntz, 142.81, -3.88)
    as t(symbol, entry_action_id, proposal_id, committee_run_id, entry_ts, entry_px, exit_ts, exit_px, fifo_realized_usd)
),
la as (
    select
        la.ACTION_ID,
        la.ONE_MIN_BAR_TS,
        la.ONE_MIN_BAR_CLOSE,
        la.EXECUTION_PRICE_SOURCE,
        try_to_double(la.PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::varchar) as eb_sl_pct,
        try_to_double(la.PARAM_SNAPSHOT:executable_bracket:target_return::varchar) as eb_tp_pct,
        coalesce(la.PARAM_SNAPSHOT:executable_bracket:blocked::boolean, false) as eb_blocked,
        try_to_double(la.PARAM_SNAPSHOT:committee_bracket_baseline:stop_loss_pct::varchar) as cbb_sl_pct,
        try_to_double(la.PARAM_SNAPSHOT:committee_bracket_baseline:realistic_target_return::varchar) as cbb_rt_pct,
        try_to_double(la.PARAM_SNAPSHOT:committee_bracket_baseline:acceptable_early_exit_target_return::varchar) as cbb_early_pct
    from MIP.LIVE.LIVE_ACTIONS la
),
mr_last as (
    select REGIME_DATE, REGIME, REGIME_CONFIDENCE
    from MIP.MART.V_MARKET_REGIME
    qualify row_number() over (order by REGIME_DATE desc) = 1
),
daily as (
    select SYMBOL, TS, CLOSE, HIGH, LOW
    from MIP.MART.MARKET_BARS
    where MARKET_TYPE = 'STOCK'
      and INTERVAL_MINUTES = 1440
)
select
    c.symbol,
    c.entry_ts,
    c.entry_px,
    c.exit_ts,
    c.exit_px,
    c.fifo_realized_usd,
    c.proposal_id,
    c.committee_run_id,
    cv.RECOMMENDATION as committee_recommendation,
    cv.SIZE_FACTOR as committee_size_factor,
    op.SIGNAL_PATTERN_ID,
    op.INTERVAL_MINUTES as signal_interval_minutes,
    la.ONE_MIN_BAR_TS,
    la.ONE_MIN_BAR_CLOSE,
    la.EXECUTION_PRICE_SOURCE,
    abs(c.entry_px - la.ONE_MIN_BAR_CLOSE) / nullif(c.entry_px, 0) as abs_slip_vs_1m_bar_pct,
    case
        when not la.eb_blocked and la.eb_sl_pct is not null then la.eb_sl_pct
        when la.cbb_sl_pct is not null then la.cbb_sl_pct
        else null
    end as eff_stop_loss_pct,
    case
        when not la.eb_blocked and la.eb_tp_pct is not null then la.eb_tp_pct
        when coalesce(la.cbb_early_pct, la.cbb_rt_pct) is not null
            then coalesce(la.cbb_early_pct, la.cbb_rt_pct)
        else null
    end as eff_target_return_pct,
    d.CLOSE as prior_session_daily_close,
    (c.entry_px - d.CLOSE) / nullif(d.CLOSE, 0) as entry_vs_prior_close_pct,
    (c.exit_px - c.entry_px) / nullif(c.entry_px, 0) as round_trip_return_pct,
    m.REGIME as market_regime_last_available,
    m.REGIME_DATE as market_regime_as_of_date,
    m.REGIME_CONFIDENCE as market_regime_confidence,
    convert_timezone('UTC', 'America/New_York', c.entry_ts) as entry_ts_et,
    hour(convert_timezone('UTC', 'America/New_York', c.entry_ts)) as entry_hour_et,
    /* RTH rough buckets: open 9:30-11, midday 11-15, last hour 15-16 */
    case
        when hour(convert_timezone('UTC', 'America/New_York', c.entry_ts)) < 11 then 'OPEN_MORNING'
        when hour(convert_timezone('UTC', 'America/New_York', c.entry_ts)) < 15 then 'MIDDAY'
        else 'LATE_AFTERNOON'
    end as entry_session_bucket_et
from closed_fifo c
left join la on la.ACTION_ID = c.entry_action_id
left join MIP.LIVE.COMMITTEE_VERDICT cv on cv.RUN_ID = c.committee_run_id
left join MIP.AGENT_OUT.ORDER_PROPOSALS op on op.PROPOSAL_ID = c.proposal_id
left join daily d
    on upper(trim(d.SYMBOL)) = upper(trim(c.symbol))
   /* Prior calendar day's daily bar close (approx prior session; ignores long weekends). */
   and d.TS::date = dateadd('day', -1, c.entry_ts::date)
cross join mr_last m
order by c.entry_ts;

-- B) 60m bars same calendar date as entry (MARKET_BARS TS treated as exchange-session clock; caveat in memo).
--    Rest-of-session after entry: min low / max high vs entry (noise vs stop proxy).
with closed_fifo as (
    select * from values
        ('SBUX', '2026-04-07T18:20:47'::timestamp_ntz, 94.91),
        ('APA', '2026-04-07T18:25:25'::timestamp_ntz, 43.19),
        ('COP', '2026-04-07T18:27:46'::timestamp_ntz, 131.53),
        ('XOM', '2026-04-07T18:46:15'::timestamp_ntz, 162.30),
        ('WMT', '2026-04-09T19:55:20'::timestamp_ntz, 129.24),
        ('JNJ', '2026-04-09T18:12:21'::timestamp_ntz, 243.64),
        ('NEE', '2026-04-09T18:40:43'::timestamp_ntz, 94.68),
        ('PG', '2026-04-09T19:25:25'::timestamp_ntz, 146.69)
    as t(symbol, entry_ts, entry_px)
),
has_60m as (
    select distinct upper(trim(SYMBOL)) sym
    from MIP.MART.MARKET_BARS
    where MARKET_TYPE = 'STOCK'
      and INTERVAL_MINUTES = 60
)
select
    c.symbol,
    c.entry_ts,
    c.entry_px,
    min(iff(b.TS >= c.entry_ts, b.LOW, null)) as min_low_rest_of_entry_day,
    max(iff(b.TS >= c.entry_ts, b.HIGH, null)) as max_high_rest_of_entry_day,
    (min(iff(b.TS >= c.entry_ts, b.LOW, null)) - c.entry_px) / nullif(c.entry_px, 0) as mae_pct_rest_of_day,
    (max(iff(b.TS >= c.entry_ts, b.HIGH, null)) - c.entry_px) / nullif(c.entry_px, 0) as mfe_pct_rest_of_day,
    iff(h.sym is null, 'NO_60M_BARS_FOR_SYMBOL', 'HAS_60M') as bar_60m_availability
from closed_fifo c
left join has_60m h on h.sym = upper(trim(c.symbol))
left join MIP.MART.MARKET_BARS b
    on upper(trim(b.SYMBOL)) = upper(trim(c.symbol))
   and b.MARKET_TYPE = 'STOCK'
   and b.INTERVAL_MINUTES = 60
   and b.TS::date = c.entry_ts::date
   and b.TS >= c.entry_ts
group by c.symbol, c.entry_ts, c.entry_px, h.sym
order by c.entry_ts;

-- C) Post-exit recovery proxy: max close on daily bars from exit date through 2026-04-13 vs exit_px
with closed_fifo as (
    select * from values
        ('SBUX', '2026-04-07T18:51:18'::timestamp_ntz, 94.41),
        ('APA', '2026-04-08T13:30:50'::timestamp_ntz, 36.88),
        ('COP', '2026-04-08T13:32:10'::timestamp_ntz, 121.65),
        ('XOM', '2026-04-08T13:30:58'::timestamp_ntz, 151.84),
        ('WMT', '2026-04-10T14:32:05'::timestamp_ntz, 126.95),
        ('JNJ', '2026-04-13T13:32:16'::timestamp_ntz, 237.18),
        ('NEE', '2026-04-13T15:21:47'::timestamp_ntz, 92.69),
        ('PG', '2026-04-13T15:30:14'::timestamp_ntz, 142.81)
    as t(symbol, exit_ts, exit_px)
)
select
    c.symbol,
    c.exit_ts::date as exit_date,
    c.exit_px,
    max(b.HIGH) as max_daily_high_after_exit,
    max(b.CLOSE) as max_daily_close_after_exit,
    (max(b.HIGH) - c.exit_px) / nullif(c.exit_px, 0) as max_recovery_from_exit_high_pct
from closed_fifo c
left join MIP.MART.MARKET_BARS b
    on upper(trim(b.SYMBOL)) = upper(trim(c.symbol))
   and b.MARKET_TYPE = 'STOCK'
   and b.INTERVAL_MINUTES = 1440
   and b.TS::date >= c.exit_ts::date
   and b.TS::date <= '2026-04-13'
group by c.symbol, c.exit_ts, c.exit_px
order by c.exit_ts;

-- D) Committee verdict JSON: explicit BEAR / REGIME tokens (all 8 runs)
select
    RUN_ID,
    RECOMMENDATION,
    iff(contains(upper(VERDICT_JSON::varchar), 'BEAR'), true, false) as has_bear_token,
    iff(contains(upper(VERDICT_JSON::varchar), 'REGIME'), true, false) as has_regime_token
from MIP.LIVE.COMMITTEE_VERDICT
where RUN_ID in (
    '19079058-7ebb-4d59-a28b-7abefc29f969',
    '261454aa-53b5-4516-918e-45094975b0a9',
    '56117a48-61c3-4707-b4e9-c0e1159ff6d0',
    '3c8965a0-348a-4737-ba7d-0c244512064b',
    'edf6a213-17ad-42ee-a457-80bde1c88730',
    '0c14eee9-472d-4ea1-a714-c534b2100821',
    '7f8a65b2-6870-42ad-bab1-846bbd19feb1',
    '1a46237f-ea45-498d-a1bd-97d308becc43'
)
order by RUN_ID;
