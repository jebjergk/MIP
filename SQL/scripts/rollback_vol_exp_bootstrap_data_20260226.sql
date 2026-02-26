-- rollback_vol_exp_bootstrap_data_20260226.sql
-- Purpose: Restore pre-VOL_EXP-load data state by removing bootstrap-loaded symbols/data.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Remove outcomes first (FK to recommendations).
delete from MIP.APP.RECOMMENDATION_OUTCOMES o
using MIP.APP.RECOMMENDATION_LOG r
where o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
  and r.INTERVAL_MINUTES = 1440
  and upper(r.SYMBOL) in ('AMD','MU','CAT','BA','SHOP','SOXX','XLE','GBP/JPY');

-- Remove recommendations generated for the VOL_EXP symbols.
delete from MIP.APP.RECOMMENDATION_LOG
where INTERVAL_MINUTES = 1440
  and upper(SYMBOL) in ('AMD','MU','CAT','BA','SHOP','SOXX','XLE','GBP/JPY');

-- Remove loaded market bars for VOL_EXP symbols.
delete from MIP.MART.MARKET_BARS
where INTERVAL_MINUTES = 1440
  and upper(SYMBOL) in ('AMD','MU','CAT','BA','SHOP','SOXX','XLE','GBP/JPY');

-- Remove seeded VOL_EXP universe rows added by this rollout.
delete from MIP.APP.INGEST_UNIVERSE
where INTERVAL_MINUTES = 1440
  and upper(SYMBOL) in ('AMD','MU','CAT','BA','SHOP','SOXX','XLE','GBP/JPY')
  and upper(coalesce(SYMBOL_COHORT, 'CORE')) = 'VOL_EXP';

-- Remove bootstrap run logs for this rollout.
delete from MIP.APP.VOL_EXP_BOOTSTRAP_SYMBOL_LOG
where RUN_ID in (
    '2955751b-d759-4f31-b449-1d7a98a15e3a',
    '5660beb0-804e-4b2b-8570-741f0d9c4076',
    'fc6690b0-6431-4cf2-b1cb-2a68814ca7ad',
    '57b923c3-2d62-482c-9894-1b0b53e5d4e6',
    'cddb1aea-970d-4239-9601-8a72eb74252f'
);

delete from MIP.APP.VOL_EXP_BOOTSTRAP_RUN_LOG
where RUN_ID in (
    '2955751b-d759-4f31-b449-1d7a98a15e3a',
    '5660beb0-804e-4b2b-8570-741f0d9c4076',
    'fc6690b0-6431-4cf2-b1cb-2a68814ca7ad',
    '57b923c3-2d62-482c-9894-1b0b53e5d4e6',
    'cddb1aea-970d-4239-9601-8a72eb74252f'
);

