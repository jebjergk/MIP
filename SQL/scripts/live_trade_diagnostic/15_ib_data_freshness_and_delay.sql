-- 15_ib_data_freshness_and_delay.sql
-- Phase 3 — H4: IB / 1m bar freshness vs broker entry time (8 closed FIFO losers).
-- Compares LIVE_ACTIONS.ONE_MIN_BAR_TS (persisted execution anchor) to broker-truth ENTRY_TS.
-- Code reference: mip_ui_api live.py revalidation path — MARKET_BARS IB 1m + bar_age_sec vs QUOTE_FRESHNESS_THRESHOLD_SEC (default 900).
-- Read-only.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Portfolio freshness threshold (seconds)
select
    'PORTFOLIO_FRESHNESS_CONFIG' as section,
    PORTFOLIO_ID,
    coalesce(QUOTE_FRESHNESS_THRESHOLD_SEC, 900) as quote_freshness_threshold_sec
from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
where PORTFOLIO_ID = 1;

-- 2) Per closed trade: bar end vs broker entry, classification
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
    as t(symbol, entry_action_id, entry_ts_broker)
),
la as (
    select ACTION_ID, ONE_MIN_BAR_TS, ONE_MIN_BAR_CLOSE, EXECUTION_PRICE_SOURCE, UPDATED_AT
    from MIP.LIVE.LIVE_ACTIONS
    where ACTION_ID in (select entry_action_id from closed_fifo)
)
select
    c.symbol,
    c.entry_ts_broker as broker_entry_ts,
    la.ONE_MIN_BAR_TS as persisted_one_min_bar_end_ts,
    la.EXECUTION_PRICE_SOURCE,
    datediff('second', la.ONE_MIN_BAR_TS, c.entry_ts_broker) as bar_end_to_broker_entry_sec,
    datediff('second', la.ONE_MIN_BAR_TS, la.UPDATED_AT) as bar_end_to_action_row_update_sec,
    case
        when la.ONE_MIN_BAR_TS is null then 'UNKNOWN'
        when datediff('second', la.ONE_MIN_BAR_TS, c.entry_ts_broker) <= 120 then 'FRESH_BAR_VS_ENTRY'
        when datediff('second', la.ONE_MIN_BAR_TS, c.entry_ts_broker) <= 360 then 'MINOR_LAG'
        when datediff('second', la.ONE_MIN_BAR_TS, c.entry_ts_broker) between 480 and 1020 then 'LIKELY_15_MIN_DELAY_BAND'
        when datediff('second', la.ONE_MIN_BAR_TS, c.entry_ts_broker) > 900 then 'MATERIAL_LAG'
        else 'MODERATE_LAG'
    end as freshness_class_vs_broker_entry,
    iff(
        datediff('second', la.ONE_MIN_BAR_TS, la.UPDATED_AT) > coalesce((
            select QUOTE_FRESHNESS_THRESHOLD_SEC from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where PORTFOLIO_ID = 1 limit 1
        ), 900),
        true,
        false
    ) as bar_to_row_update_exceeds_portfolio_threshold
from closed_fifo c
join la on la.ACTION_ID = c.entry_action_id
order by c.entry_ts_broker;

-- 3) Latest stored IB 1m bar per cohort symbol (as of now — operational snapshot, not historical replay)
select
    'LATEST_IB_1M_BAR_AS_OF_QUERY' as section,
    b.SYMBOL,
    max(b.TS) as last_bar_ts,
    max(b.INGESTED_AT) as last_ingested_at
from MIP.MART.MARKET_BARS b
where b.MARKET_TYPE = 'STOCK'
  and b.INTERVAL_MINUTES = 1
  and upper(coalesce(b.SOURCE, '')) = 'IBKR'
  and upper(trim(b.SYMBOL)) in ('SBUX', 'APA', 'XOM', 'COP', 'WMT', 'JNJ', 'NEE', 'PG')
group by b.SYMBOL
order by b.SYMBOL;
