-- 11_news_phased_smoke.sql
-- Purpose: Phase D smoke for train/apply news calibration.

use role MIP_ADMIN_ROLE;
use database MIP;

set phased_training_version = (
    select 'NEWS_CAL_V1_' || to_char(current_timestamp(), 'YYYYMMDDHH24MISS')
);

set phased_train_run_id = (
    select 'NEWS_TRAIN_' || to_char(current_timestamp(), 'YYYYMMDDHH24MISS')
);

set phased_apply_run_id = (
    select 'NEWS_APPLY_' || to_char(current_timestamp(), 'YYYYMMDDHH24MISS')
);

set phased_portfolio_id = (
    select min(PORTFOLIO_ID)
    from MIP.APP.PORTFOLIO
    where STATUS = 'ACTIVE'
);

set phased_proposal_run = (
    select 'NEWS_PHASED_PROP_' || to_char(current_timestamp(), 'YYYYMMDDHH24MISS')
);

call MIP.APP.SP_TRAIN_NEWS_CALIBRATION(
    $phased_train_run_id,
    $phased_training_version,
    '2025-09-01'::date,
    current_date(),
    null
);

-- Sync active training version for apply default behavior.
update MIP.APP.APP_CONFIG
   set CONFIG_VALUE = $phased_training_version,
       UPDATED_AT = current_timestamp()
 where CONFIG_KEY = 'DAILY_NEWS_CAL_ACTIVE_TRAINING_VERSION';

call MIP.APP.SP_AGENT_PROPOSE_TRADES($phased_portfolio_id, $phased_proposal_run, null);

call MIP.APP.SP_APPLY_NEWS_CALIBRATION(
    $phased_apply_run_id,
    $phased_training_version,
    null,
    $phased_portfolio_id
);

select
    count(*) as TRAINED_ROWS,
    coalesce(count_if(ELIGIBLE_FLAG), 0) as ELIGIBLE_ROWS
from MIP.APP.DAILY_NEWS_CALIBRATION_TRAINED
where TRAINING_VERSION = $phased_training_version;

select
    PROPOSAL_ID,
    PORTFOLIO_ID,
    SYMBOL,
    SOURCE_SIGNALS:news_score_adj::float as NEWS_SCORE_ADJ,
    SOURCE_SIGNALS:news_score_adj_calibrated::float as NEWS_SCORE_ADJ_CALIBRATED,
    SOURCE_SIGNALS:news_calibration_multiplier::float as NEWS_CAL_MULTIPLIER,
    SOURCE_SIGNALS:news_calibration_eligible::boolean as NEWS_CAL_ELIGIBLE,
    RATIONALE:news_calibration_buckets as NEWS_CAL_BUCKETS
from MIP.AGENT_OUT.ORDER_PROPOSALS
where PORTFOLIO_ID = $phased_portfolio_id
  and STATUS = 'PROPOSED'
order by PROPOSAL_ID desc
limit 50;

select
    RUN_ID,
    TRAINING_VERSION,
    TARGET_RUN_ID,
    PROPOSALS_TOUCHED,
    PROPOSALS_WITH_ELIGIBLE_MULT,
    AVG_MULTIPLIER
from MIP.APP.DAILY_NEWS_CALIBRATION_APPLY_LOG
where RUN_ID = $phased_apply_run_id
  and TRAINING_VERSION = $phased_training_version;
