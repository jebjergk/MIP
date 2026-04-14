-- 13_bracket_construction_vs_realized_path.sql
-- Phase 3 — H2: Bracket construction vs realized path (8 closed FIFO losers).
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
la as (
    select
        ACTION_ID,
        try_to_double(PARAM_SNAPSHOT:executable_bracket:stop_loss_pct::varchar) as eb_sl_pct,
        try_to_double(PARAM_SNAPSHOT:executable_bracket:target_return::varchar) as eb_tp_pct,
        coalesce(PARAM_SNAPSHOT:executable_bracket:blocked::boolean, false) as eb_blocked,
        try_to_double(PARAM_SNAPSHOT:committee_bracket_baseline:stop_loss_pct::varchar) as cbb_sl_pct,
        try_to_double(PARAM_SNAPSHOT:committee_bracket_baseline:realistic_target_return::varchar) as cbb_rt_pct,
        try_to_double(PARAM_SNAPSHOT:committee_bracket_baseline:acceptable_early_exit_target_return::varchar) as cbb_early_pct
    from MIP.LIVE.LIVE_ACTIONS
    where ACTION_ID in (select entry_action_id from closed_fifo)
),
oh as (
    select SYMBOL, TS::date as d, HIGH, LOW
    from MIP.MART.MARKET_BARS
    where MARKET_TYPE = 'STOCK'
      and INTERVAL_MINUTES = 1440
),
enriched as (
    select
        c.*,
        case
            when not la.eb_blocked and la.eb_sl_pct is not null then la.eb_sl_pct
            when la.cbb_sl_pct is not null then la.cbb_sl_pct
            else null
        end as eff_sl_pct,
        case
            when not la.eb_blocked and la.eb_tp_pct is not null then la.eb_tp_pct
            when coalesce(la.cbb_early_pct, la.cbb_rt_pct) is not null then coalesce(la.cbb_early_pct, la.cbb_rt_pct)
            else null
        end as eff_tp_pct,
        la.eb_sl_pct,
        la.cbb_sl_pct,
        la.eb_tp_pct,
        la.cbb_early_pct,
        la.cbb_rt_pct,
        (e.HIGH - e.LOW) / nullif(c.entry_px, 0) as entry_day_range_pct,
        (x.HIGH - x.LOW) / nullif(c.entry_px, 0) as exit_day_range_pct,
        abs(c.exit_px - c.entry_px) / nullif(c.entry_px, 0) as path_to_exit_abs_pct
    from closed_fifo c
    left join la on la.ACTION_ID = c.entry_action_id
    left join oh e on upper(trim(e.SYMBOL)) = upper(trim(c.symbol)) and e.d = c.entry_ts::date
    left join oh x on upper(trim(x.SYMBOL)) = upper(trim(c.symbol)) and x.d = c.exit_ts::date
),
recovery as (
    select
        e.symbol,
        e.exit_ts,
        max(b.HIGH) as max_high_after_exit
    from enriched e
    inner join MIP.MART.MARKET_BARS b
        on upper(trim(b.SYMBOL)) = upper(trim(e.symbol))
       and b.MARKET_TYPE = 'STOCK'
       and b.INTERVAL_MINUTES = 1440
       and b.TS::date >= e.exit_ts::date
       and b.TS::date <= '2026-04-13'
    group by e.symbol, e.exit_ts
)
select
    e.symbol,
    e.entry_px,
    e.exit_px,
    e.fifo_realized_usd,
    e.eff_sl_pct,
    e.eff_tp_pct,
    e.eb_sl_pct,
    e.cbb_sl_pct,
    e.entry_day_range_pct,
    e.exit_day_range_pct,
    e.path_to_exit_abs_pct,
    (r.max_high_after_exit - e.exit_px) / nullif(e.exit_px, 0) as recovery_high_vs_exit_pct,
    case
        when e.eff_sl_pct is null and e.eff_tp_pct is null and e.eb_sl_pct is null and e.cbb_sl_pct is null
            then 'BRACKET_DATA_MISSING'
        when e.eff_sl_pct is not null
             and abs(e.exit_px - (e.entry_px * (1 - e.eff_sl_pct))) / nullif(e.entry_px, 0) < 0.002
            then 'STOP_AT_OR_NEAR_BRACKET'
        else 'EXIT_NOT_AT_COMPUTED_STOP'
    end as bracket_exit_label,
    case
        when e.eff_sl_pct is not null and e.entry_day_range_pct is not null and e.eff_sl_pct < e.entry_day_range_pct * 0.5
            then 'STOP_INSIDE_NORMAL_RANGE'
        when e.eff_sl_pct is not null and e.entry_day_range_pct is not null and abs(e.eff_sl_pct - e.entry_day_range_pct) < 0.003
            then 'STOP_AROUND_FULL_DAY_RANGE'
        when e.eff_sl_pct is null
            then 'BRACKET_DATA_MISSING'
        else 'STOP_GE_HALF_DAY_RANGE_OR_OTHER'
    end as stop_vs_range_label,
    case
        when e.symbol in ('APA', 'XOM', 'COP') and e.exit_ts::date > e.entry_ts::date and e.path_to_exit_abs_pct > 0.05
            then 'GAP_MOVE_DOMINATED'
        when e.path_to_exit_abs_pct > 0.05
            then 'GAP_MOVE_DOMINATED'
        else 'INTRADAY_SCALE_LOSS'
    end as move_scale_label
from enriched e
left join recovery r
    on r.symbol = e.symbol
   and r.exit_ts = e.exit_ts
order by e.entry_ts;
