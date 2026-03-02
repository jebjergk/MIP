-- 387_sp_apply_news_calibration.sql
-- Purpose: Apply trained news calibration multipliers to current proposals.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_APPLY_NEWS_CALIBRATION(
    P_RUN_ID string,
    P_TRAINING_VERSION string default null,
    P_TARGET_RUN_ID string default null,
    P_PORTFOLIO_ID number default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, uuid_string());
    v_training_version string;
    v_target_run_id string;
    v_default_mult float := 1.0;
    v_rows_touched number := 0;
    v_rows_with_mult number := 0;
    v_avg_mult float := null;
begin
    execute immediate 'use schema MIP.APP';

    v_training_version := coalesce(
        :P_TRAINING_VERSION,
        (
            select max(case when CONFIG_KEY = 'DAILY_NEWS_CAL_ACTIVE_TRAINING_VERSION' then CONFIG_VALUE end)
            from MIP.APP.APP_CONFIG
            where CONFIG_KEY in ('DAILY_NEWS_CAL_ACTIVE_TRAINING_VERSION')
        ),
        'CURRENT'
    );

    v_default_mult := coalesce(
        (
            select max(case when CONFIG_KEY = 'DAILY_NEWS_CAL_DEFAULT_MULT' then try_to_double(CONFIG_VALUE) end)
            from MIP.APP.APP_CONFIG
            where CONFIG_KEY = 'DAILY_NEWS_CAL_DEFAULT_MULT'
        ),
        1.0
    );

    v_target_run_id := coalesce(
        :P_TARGET_RUN_ID,
        (
            select max(RUN_ID_VARCHAR)
            from MIP.AGENT_OUT.ORDER_PROPOSALS
            where STATUS = 'PROPOSED'
              and (:P_PORTFOLIO_ID is null or PORTFOLIO_ID = :P_PORTFOLIO_ID)
        )
    );

    if (v_target_run_id is null) then
        v_target_run_id := '__ALL_PROPOSED__';
    end if;

    create or replace temporary table TMP_NEWS_CAL_APPLY as
    with proposal_scope as (
        select
            p.PROPOSAL_ID,
            p.PORTFOLIO_ID,
            p.RUN_ID_VARCHAR,
            p.MARKET_TYPE,
            p.SOURCE_SIGNALS,
            coalesce(try_to_double(to_varchar(p.SOURCE_SIGNALS:news_score_adj)), 0.0) as NEWS_SCORE_ADJ,
            try_to_double(to_varchar(p.SOURCE_SIGNALS:news_features:news_pressure)) as NEWS_PRESSURE,
            try_to_double(to_varchar(p.SOURCE_SIGNALS:news_features:news_sentiment)) as NEWS_SENTIMENT,
            try_to_double(to_varchar(p.SOURCE_SIGNALS:news_features:uncertainty_score)) as NEWS_UNCERTAINTY_SCORE,
            try_to_double(to_varchar(p.SOURCE_SIGNALS:news_features:event_risk_score)) as NEWS_EVENT_RISK_SCORE
        from MIP.AGENT_OUT.ORDER_PROPOSALS p
        where p.STATUS = 'PROPOSED'
          and (
              :v_target_run_id = '__ALL_PROPOSED__'
              or p.RUN_ID_VARCHAR = :v_target_run_id
          )
          and (:P_PORTFOLIO_ID is null or p.PORTFOLIO_ID = :P_PORTFOLIO_ID)
    ),
    proposal_buckets as (
        select
            s.*,
            case
                when s.NEWS_PRESSURE is null then 'UNKNOWN'
                when s.NEWS_PRESSURE >= 0.80 then 'P4_VERY_HIGH'
                when s.NEWS_PRESSURE >= 0.60 then 'P3_HIGH'
                when s.NEWS_PRESSURE >= 0.30 then 'P2_MED'
                else 'P1_LOW'
            end as NEWS_PRESSURE_BUCKET,
            case
                when s.NEWS_SENTIMENT is null then 'UNKNOWN'
                when s.NEWS_SENTIMENT >= 0.25 then 'POS'
                when s.NEWS_SENTIMENT <= -0.25 then 'NEG'
                else 'NEU'
            end as NEWS_SENTIMENT_BUCKET,
            case
                when s.NEWS_UNCERTAINTY_SCORE is null then 'UNKNOWN'
                when s.NEWS_UNCERTAINTY_SCORE >= 0.60 then 'HIGH'
                when s.NEWS_UNCERTAINTY_SCORE >= 0.30 then 'MED'
                else 'LOW'
            end as NEWS_UNCERTAINTY_BUCKET,
            case
                when s.NEWS_EVENT_RISK_SCORE is null then 'UNKNOWN'
                when s.NEWS_EVENT_RISK_SCORE >= 0.70 then 'HIGH'
                when s.NEWS_EVENT_RISK_SCORE >= 0.35 then 'MED'
                else 'LOW'
            end as NEWS_EVENT_RISK_BUCKET
        from proposal_scope s
    ),
    candidate_mult as (
        select
            p.PROPOSAL_ID,
            c.MULTIPLIER_CAPPED,
            c.ELIGIBLE_FLAG,
            c.N_OUTCOMES,
            row_number() over (
                partition by p.PROPOSAL_ID
                order by iff(c.ELIGIBLE_FLAG, 1, 0) desc, c.N_OUTCOMES desc, c.HORIZON_BARS desc
            ) as RN
        from proposal_buckets p
        left join MIP.APP.DAILY_NEWS_CALIBRATION_TRAINED c
          on c.TRAINING_VERSION = :v_training_version
         and c.MARKET_TYPE = p.MARKET_TYPE
         and c.NEWS_PRESSURE_BUCKET = p.NEWS_PRESSURE_BUCKET
         and c.NEWS_SENTIMENT_BUCKET = p.NEWS_SENTIMENT_BUCKET
         and c.NEWS_UNCERTAINTY_BUCKET = p.NEWS_UNCERTAINTY_BUCKET
         and c.NEWS_EVENT_RISK_BUCKET = p.NEWS_EVENT_RISK_BUCKET
    )
    select
        p.PROPOSAL_ID,
        p.PORTFOLIO_ID,
        p.RUN_ID_VARCHAR,
        p.NEWS_SCORE_ADJ,
        p.NEWS_PRESSURE_BUCKET,
        p.NEWS_SENTIMENT_BUCKET,
        p.NEWS_UNCERTAINTY_BUCKET,
        p.NEWS_EVENT_RISK_BUCKET,
        coalesce(cm.MULTIPLIER_CAPPED, :v_default_mult) as NEWS_CALIBRATION_MULTIPLIER,
        coalesce(cm.ELIGIBLE_FLAG, false) as NEWS_CALIBRATION_ELIGIBLE
    from proposal_buckets p
    left join candidate_mult cm
      on cm.PROPOSAL_ID = p.PROPOSAL_ID
     and cm.RN = 1;

    update MIP.AGENT_OUT.ORDER_PROPOSALS t
       set SOURCE_SIGNALS = object_insert(
               object_insert(
                   object_insert(
                       t.SOURCE_SIGNALS,
                       'news_calibration_multiplier',
                       a.NEWS_CALIBRATION_MULTIPLIER,
                       true
                   ),
                   'news_score_adj_calibrated',
                   a.NEWS_SCORE_ADJ * a.NEWS_CALIBRATION_MULTIPLIER,
                   true
               ),
               'news_calibration_eligible',
               a.NEWS_CALIBRATION_ELIGIBLE,
               true
           ),
           RATIONALE = object_insert(
               object_insert(
                   object_insert(
                       object_insert(
                           t.RATIONALE,
                           'news_calibration_multiplier',
                           a.NEWS_CALIBRATION_MULTIPLIER,
                           true
                       ),
                       'news_score_adj_calibrated',
                       a.NEWS_SCORE_ADJ * a.NEWS_CALIBRATION_MULTIPLIER,
                       true
                   ),
                   'news_calibration_eligible',
                   a.NEWS_CALIBRATION_ELIGIBLE,
                   true
               ),
               'news_calibration_buckets',
               object_construct(
                   'pressure', a.NEWS_PRESSURE_BUCKET,
                   'sentiment', a.NEWS_SENTIMENT_BUCKET,
                   'uncertainty', a.NEWS_UNCERTAINTY_BUCKET,
                   'event_risk', a.NEWS_EVENT_RISK_BUCKET
               ),
               true
           ),
           TARGET_WEIGHT = greatest(
               0.01,
               least(0.25, coalesce(t.TARGET_WEIGHT, 0.01) * a.NEWS_CALIBRATION_MULTIPLIER)
           )
      from TMP_NEWS_CAL_APPLY a
     where t.PROPOSAL_ID = a.PROPOSAL_ID;

    select
        count(*) as ROWS_TOUCHED,
        coalesce(count_if(NEWS_CALIBRATION_ELIGIBLE), 0) as ROWS_WITH_MULT,
        avg(NEWS_CALIBRATION_MULTIPLIER) as AVG_MULT
      into :v_rows_touched, :v_rows_with_mult, :v_avg_mult
      from TMP_NEWS_CAL_APPLY;

    merge into MIP.APP.DAILY_NEWS_CALIBRATION_APPLY_LOG t
    using (
        select
            :v_run_id as RUN_ID,
            :v_training_version as TRAINING_VERSION,
            current_timestamp() as APPLIED_AT,
            :v_target_run_id as TARGET_RUN_ID,
            :P_PORTFOLIO_ID as PORTFOLIO_ID,
            :v_rows_touched as PROPOSALS_TOUCHED,
            :v_rows_with_mult as PROPOSALS_WITH_ELIGIBLE_MULT,
            :v_avg_mult as AVG_MULTIPLIER,
            object_construct(
                'default_multiplier', :v_default_mult
            ) as DETAILS
    ) src
    on t.RUN_ID = src.RUN_ID
   and t.TRAINING_VERSION = src.TRAINING_VERSION
    when matched then update set
        t.APPLIED_AT = src.APPLIED_AT,
        t.TARGET_RUN_ID = src.TARGET_RUN_ID,
        t.PORTFOLIO_ID = src.PORTFOLIO_ID,
        t.PROPOSALS_TOUCHED = src.PROPOSALS_TOUCHED,
        t.PROPOSALS_WITH_ELIGIBLE_MULT = src.PROPOSALS_WITH_ELIGIBLE_MULT,
        t.AVG_MULTIPLIER = src.AVG_MULTIPLIER,
        t.DETAILS = src.DETAILS
    when not matched then insert (
        RUN_ID, TRAINING_VERSION, APPLIED_AT, TARGET_RUN_ID, PORTFOLIO_ID,
        PROPOSALS_TOUCHED, PROPOSALS_WITH_ELIGIBLE_MULT, AVG_MULTIPLIER, DETAILS
    ) values (
        src.RUN_ID, src.TRAINING_VERSION, src.APPLIED_AT, src.TARGET_RUN_ID, src.PORTFOLIO_ID,
        src.PROPOSALS_TOUCHED, src.PROPOSALS_WITH_ELIGIBLE_MULT, src.AVG_MULTIPLIER, src.DETAILS
    );

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'training_version', :v_training_version,
        'target_run_id', :v_target_run_id,
        'rows_touched', :v_rows_touched,
        'rows_with_eligible_multiplier', :v_rows_with_mult,
        'avg_multiplier', :v_avg_mult
    );
end;
$$;
