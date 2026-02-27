-- 367_daily_symbol_calibration_policy.sql
-- Purpose: Daily symbol personalization (Mode C retrain, no shadow pipeline)
-- Scope: daily-only policy layer (INTERVAL_MINUTES = 1440), additive and versioned.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ---------------------------------------------------------------------------
-- Config keys (single-switch rollout + parameterized thresholds)
-- ---------------------------------------------------------------------------
merge into MIP.APP.APP_CONFIG t
using (
    select 'ENABLE_DAILY_SYMBOL_CALIBRATION' as CONFIG_KEY, 'false' as CONFIG_VALUE,
           'Feature flag for daily symbol personalization policy layer' as DESCRIPTION
    union all
    select 'DAILY_POLICY_ACTIVE_TRAINING_VERSION', 'CURRENT',
           'Active DAILY policy training version consumed by policy/decision views'
    union all
    select 'DAILY_POLICY_BASELINE_VERSION', 'CURRENT',
           'Baseline DAILY policy version for rollback'
    union all
    select 'DAILY_POLICY_CAL_MIN_N', '120',
           'Minimum symbol outcomes for calibration eligibility'
    union all
    select 'DAILY_POLICY_CAL_MAX_CI_WIDTH', '0.12',
           'Maximum confidence interval width for symbol calibration eligibility'
    union all
    select 'DAILY_POLICY_CAL_MULT_CAP_LO', '0.80',
           'Lower multiplier cap for symbol calibration'
    union all
    select 'DAILY_POLICY_CAL_MULT_CAP_HI', '1.20',
           'Upper multiplier cap for symbol calibration'
    union all
    select 'DAILY_POLICY_CAL_SHRINK_K', '200',
           'Shrinkage parameter k for n/(n+k) shrink-to-1.0'
    union all
    select 'DAILY_POLICY_CAL_MIN_N_HORIZON', '250',
           'Minimum outcomes required for optional horizon override'
    union all
    select 'DAILY_POLICY_CAL_MAX_CI_WIDTH_HORIZON', '0.10',
           'Maximum CI width for optional horizon override'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());

create or replace view MIP.APP.V_TRAINING_VERSION_CURRENT as
select
    'DAILY_POLICY' as POLICY_NAME,
    coalesce(
        max(case when CONFIG_KEY = 'DAILY_POLICY_ACTIVE_TRAINING_VERSION' then CONFIG_VALUE end),
        'CURRENT'
    ) as TRAINING_VERSION,
    current_timestamp() as UPDATED_AT
from MIP.APP.APP_CONFIG
where CONFIG_KEY in ('DAILY_POLICY_ACTIVE_TRAINING_VERSION');

-- ---------------------------------------------------------------------------
-- Trained artifacts (versioned, additive)
-- ---------------------------------------------------------------------------
create table if not exists MIP.APP.DAILY_SYMBOL_CALIBRATION_TRAINED (
    TRAINING_VERSION        string        not null,
    SYMBOL                  string        not null,
    MARKET_TYPE             string        not null,
    PATTERN_ID              number        not null,
    HORIZON_BARS            number        not null,
    N_OUTCOMES              number,
    PATTERN_METRIC          float,
    SYMBOL_METRIC           float,
    SYMBOL_MEDIAN_METRIC    float,
    SYMBOL_WIN_RATE         float,
    RAW_MULTIPLIER          float,
    SHRINK_FACTOR           float,
    SHRUNK_MULTIPLIER       float,
    MULTIPLIER_CAPPED       float,
    CI_WIDTH                float,
    STABILITY_SCORE         float,
    STABILITY_OK            boolean,
    ELIGIBLE_FLAG           boolean,
    REASON                  string,
    RUN_ID                  string,
    CALCULATED_AT           timestamp_ntz default current_timestamp(),
    constraint PK_DAILY_SYMBOL_CALIBRATION_TRAINED primary key (
        TRAINING_VERSION, SYMBOL, MARKET_TYPE, PATTERN_ID, HORIZON_BARS
    )
);

create table if not exists MIP.APP.DAILY_POLICY_EFFECTIVE_TRAINED (
    TRAINING_VERSION        string        not null,
    SYMBOL                  string        not null,
    MARKET_TYPE             string        not null,
    PATTERN_ID              number        not null,
    HORIZON_BARS            number        not null,
    PATTERN_TARGET          float,
    SYMBOL_MULTIPLIER       float,
    EFFECTIVE_TARGET        float,
    TARGET_SOURCE           string,
    EFFECTIVE_HORIZON_BARS  number,
    HORIZON_SOURCE          string,
    N_OUTCOMES              number,
    CI_WIDTH                float,
    STABILITY_OK            boolean,
    FALLBACK_REASON         string,
    ELIGIBLE_FLAG           boolean,
    RUN_ID                  string,
    CALCULATED_AT           timestamp_ntz default current_timestamp(),
    constraint PK_DAILY_POLICY_EFFECTIVE_TRAINED primary key (
        TRAINING_VERSION, SYMBOL, MARKET_TYPE, PATTERN_ID, HORIZON_BARS
    )
);

create table if not exists MIP.APP.DAILY_CALIBRATION_EVAL_RUNS (
    RUN_ID                           string        not null,
    TRAINING_VERSION                 string        not null,
    MARKET_TYPE                      string,
    START_DATE                       date,
    END_DATE                         date,
    STATUS                           string,
    TOTAL_SYMBOL_BUCKETS             number,
    ELIGIBLE_SYMBOL_BUCKETS          number,
    ELIGIBLE_SHARE                   float,
    MEDIAN_MULTIPLIER                float,
    P95_MULTIPLIER                   float,
    SIGNAL_INVARIANCE_OK             boolean,
    TARGET_RANGE_OK                  boolean,
    HORIZON_CONSISTENCY_OK           boolean,
    NO_TARGET_INFLATION_REGRESSION_OK boolean,
    DETAILS                          variant,
    STARTED_AT                       timestamp_ntz default current_timestamp(),
    FINISHED_AT                      timestamp_ntz,
    constraint PK_DAILY_CALIBRATION_EVAL_RUNS primary key (RUN_ID, TRAINING_VERSION)
);

create or replace view MIP.MART.V_DAILY_POLICY_EFFECTIVE_ACTIVE as
with active_version as (
    select TRAINING_VERSION
    from MIP.APP.V_TRAINING_VERSION_CURRENT
    where POLICY_NAME = 'DAILY_POLICY'
),
latest_rows as (
    select
        p.*,
        row_number() over (
            partition by p.TRAINING_VERSION, p.SYMBOL, p.MARKET_TYPE, p.PATTERN_ID, p.HORIZON_BARS
            order by p.CALCULATED_AT desc
        ) as RN
    from MIP.APP.DAILY_POLICY_EFFECTIVE_TRAINED p
    join active_version av
      on av.TRAINING_VERSION = p.TRAINING_VERSION
)
select
    TRAINING_VERSION,
    SYMBOL,
    MARKET_TYPE,
    PATTERN_ID,
    HORIZON_BARS,
    PATTERN_TARGET,
    SYMBOL_MULTIPLIER,
    EFFECTIVE_TARGET,
    TARGET_SOURCE,
    EFFECTIVE_HORIZON_BARS,
    HORIZON_SOURCE,
    N_OUTCOMES,
    CI_WIDTH,
    STABILITY_OK,
    FALLBACK_REASON,
    ELIGIBLE_FLAG,
    RUN_ID,
    CALCULATED_AT
from latest_rows
where RN = 1;

-- ---------------------------------------------------------------------------
-- Retrain procedure (Mode C): retrain policy artifacts only
-- ---------------------------------------------------------------------------
create or replace procedure MIP.APP.SP_RETRAIN_DAILY_POLICY_SYMBOL_CAL(
    P_RUN_ID string,
    P_TRAINING_VERSION string,
    P_START_DATE date default '2025-09-01'::date,
    P_END_DATE date default current_date(),
    P_MARKET_TYPE string default 'STOCK',
    P_WAREHOUSE_OVERRIDE string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, uuid_string());
    v_training_version string := coalesce(:P_TRAINING_VERSION, 'DAILY_CAL_V1');
    v_start_date date := coalesce(:P_START_DATE, '2025-09-01'::date);
    v_end_date date := coalesce(:P_END_DATE, current_date());
    v_market_type string := :P_MARKET_TYPE;
    v_wh string := :P_WAREHOUSE_OVERRIDE;

    v_min_n number := 120;
    v_max_ci_width float := 0.12;
    v_mult_cap_lo float := 0.80;
    v_mult_cap_hi float := 1.20;
    v_shrink_k float := 200;
    v_min_n_horizon number := 250;
    v_max_ci_width_horizon float := 0.10;

    v_total_buckets number := 0;
    v_eligible_buckets number := 0;
    v_eligible_share float := 0;
    v_median_mult float := null;
    v_p95_mult float := null;
begin
    if (v_wh is not null) then
        execute immediate 'use warehouse ' || :v_wh;
    end if;

    select
        coalesce(max(case when CONFIG_KEY = 'DAILY_POLICY_CAL_MIN_N' then try_to_number(CONFIG_VALUE) end), :v_min_n),
        coalesce(max(case when CONFIG_KEY = 'DAILY_POLICY_CAL_MAX_CI_WIDTH' then try_to_double(CONFIG_VALUE) end), :v_max_ci_width),
        coalesce(max(case when CONFIG_KEY = 'DAILY_POLICY_CAL_MULT_CAP_LO' then try_to_double(CONFIG_VALUE) end), :v_mult_cap_lo),
        coalesce(max(case when CONFIG_KEY = 'DAILY_POLICY_CAL_MULT_CAP_HI' then try_to_double(CONFIG_VALUE) end), :v_mult_cap_hi),
        coalesce(max(case when CONFIG_KEY = 'DAILY_POLICY_CAL_SHRINK_K' then try_to_double(CONFIG_VALUE) end), :v_shrink_k),
        coalesce(max(case when CONFIG_KEY = 'DAILY_POLICY_CAL_MIN_N_HORIZON' then try_to_number(CONFIG_VALUE) end), :v_min_n_horizon),
        coalesce(max(case when CONFIG_KEY = 'DAILY_POLICY_CAL_MAX_CI_WIDTH_HORIZON' then try_to_double(CONFIG_VALUE) end), :v_max_ci_width_horizon)
      into :v_min_n, :v_max_ci_width, :v_mult_cap_lo, :v_mult_cap_hi, :v_shrink_k, :v_min_n_horizon, :v_max_ci_width_horizon
      from MIP.APP.APP_CONFIG
     where CONFIG_KEY in (
         'DAILY_POLICY_CAL_MIN_N',
         'DAILY_POLICY_CAL_MAX_CI_WIDTH',
         'DAILY_POLICY_CAL_MULT_CAP_LO',
         'DAILY_POLICY_CAL_MULT_CAP_HI',
         'DAILY_POLICY_CAL_SHRINK_K',
         'DAILY_POLICY_CAL_MIN_N_HORIZON',
         'DAILY_POLICY_CAL_MAX_CI_WIDTH_HORIZON'
     );

    create or replace temporary table TMP_DAILY_CAL_SOURCE as
    select
        r.SYMBOL,
        r.MARKET_TYPE,
        r.PATTERN_ID,
        o.HORIZON_BARS,
        o.ENTRY_TS::date as ENTRY_DATE,
        o.REALIZED_RETURN,
        iff(o.HIT_FLAG, 1.0, 0.0) as HIT_FLOAT
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where r.INTERVAL_MINUTES = 1440
      and o.EVAL_STATUS = 'SUCCESS'
      and o.REALIZED_RETURN is not null
      and o.ENTRY_TS::date between :v_start_date and :v_end_date
      and (:v_market_type is null or r.MARKET_TYPE = :v_market_type);

    create or replace temporary table TMP_DAILY_CAL_PATTERN as
    select
        MARKET_TYPE,
        PATTERN_ID,
        HORIZON_BARS,
        count(*) as PATTERN_N,
        avg(REALIZED_RETURN) as PATTERN_METRIC
    from TMP_DAILY_CAL_SOURCE
    group by MARKET_TYPE, PATTERN_ID, HORIZON_BARS;

    create or replace temporary table TMP_DAILY_CAL_SYMBOL as
    with base as (
        select
            s.SYMBOL,
            s.MARKET_TYPE,
            s.PATTERN_ID,
            s.HORIZON_BARS,
            count(*) as N_OUTCOMES,
            avg(s.REALIZED_RETURN) as SYMBOL_METRIC,
            median(s.REALIZED_RETURN) as SYMBOL_MEDIAN_METRIC,
            avg(s.HIT_FLOAT) as SYMBOL_WIN_RATE,
            stddev_samp(s.REALIZED_RETURN) as SYMBOL_STDDEV
        from TMP_DAILY_CAL_SOURCE s
        group by s.SYMBOL, s.MARKET_TYPE, s.PATTERN_ID, s.HORIZON_BARS
    ),
    drift as (
        select
            x.SYMBOL,
            x.MARKET_TYPE,
            x.PATTERN_ID,
            x.HORIZON_BARS,
            abs(
                avg(case when x.RN_DESC <= 30 then x.REALIZED_RETURN end)
                - avg(case when x.RN_DESC > 30 and x.RN_DESC <= 60 then x.REALIZED_RETURN end)
            ) as STABILITY_SCORE
        from (
            select
                s.*,
                row_number() over (
                    partition by s.SYMBOL, s.MARKET_TYPE, s.PATTERN_ID, s.HORIZON_BARS
                    order by s.ENTRY_DATE desc
                ) as RN_DESC
            from TMP_DAILY_CAL_SOURCE s
        ) x
        group by x.SYMBOL, x.MARKET_TYPE, x.PATTERN_ID, x.HORIZON_BARS
    )
    select
        b.SYMBOL,
        b.MARKET_TYPE,
        b.PATTERN_ID,
        b.HORIZON_BARS,
        b.N_OUTCOMES,
        p.PATTERN_METRIC,
        b.SYMBOL_METRIC,
        b.SYMBOL_MEDIAN_METRIC,
        b.SYMBOL_WIN_RATE,
        iff(b.N_OUTCOMES > 1 and b.SYMBOL_STDDEV is not null, 1.96 * b.SYMBOL_STDDEV / sqrt(b.N_OUTCOMES), null) as CI_WIDTH,
        coalesce(d.STABILITY_SCORE, 0) as STABILITY_SCORE
    from base b
    join TMP_DAILY_CAL_PATTERN p
      on p.MARKET_TYPE = b.MARKET_TYPE
     and p.PATTERN_ID = b.PATTERN_ID
     and p.HORIZON_BARS = b.HORIZON_BARS
    left join drift d
      on d.SYMBOL = b.SYMBOL
     and d.MARKET_TYPE = b.MARKET_TYPE
     and d.PATTERN_ID = b.PATTERN_ID
     and d.HORIZON_BARS = b.HORIZON_BARS;

    merge into MIP.APP.DAILY_SYMBOL_CALIBRATION_TRAINED t
    using (
        select
            :v_training_version as TRAINING_VERSION,
            s.SYMBOL,
            s.MARKET_TYPE,
            s.PATTERN_ID,
            s.HORIZON_BARS,
            s.N_OUTCOMES,
            s.PATTERN_METRIC,
            s.SYMBOL_METRIC,
            s.SYMBOL_MEDIAN_METRIC,
            s.SYMBOL_WIN_RATE,
            iff(s.PATTERN_METRIC is not null and s.PATTERN_METRIC > 0, s.SYMBOL_METRIC / s.PATTERN_METRIC, 1.0) as RAW_MULTIPLIER,
            iff(s.N_OUTCOMES > 0, s.N_OUTCOMES / (s.N_OUTCOMES + :v_shrink_k), 0.0) as SHRINK_FACTOR,
            1.0 + iff(s.N_OUTCOMES > 0, s.N_OUTCOMES / (s.N_OUTCOMES + :v_shrink_k), 0.0)
                * (iff(s.PATTERN_METRIC is not null and s.PATTERN_METRIC > 0, s.SYMBOL_METRIC / s.PATTERN_METRIC, 1.0) - 1.0) as SHRUNK_MULTIPLIER,
            least(:v_mult_cap_hi, greatest(:v_mult_cap_lo,
                1.0 + iff(s.N_OUTCOMES > 0, s.N_OUTCOMES / (s.N_OUTCOMES + :v_shrink_k), 0.0)
                * (iff(s.PATTERN_METRIC is not null and s.PATTERN_METRIC > 0, s.SYMBOL_METRIC / s.PATTERN_METRIC, 1.0) - 1.0)
            )) as MULTIPLIER_CAPPED,
            s.CI_WIDTH,
            s.STABILITY_SCORE,
            iff(s.STABILITY_SCORE <= :v_max_ci_width, true, false) as STABILITY_OK,
            iff(s.N_OUTCOMES >= :v_min_n
                and coalesce(s.CI_WIDTH, 999) <= :v_max_ci_width
                and s.STABILITY_SCORE <= :v_max_ci_width, true, false) as ELIGIBLE_FLAG,
            case
                when s.N_OUTCOMES < :v_min_n then 'INSUFFICIENT_N'
                when coalesce(s.CI_WIDTH, 999) > :v_max_ci_width then 'CI_TOO_WIDE'
                when s.STABILITY_SCORE > :v_max_ci_width then 'UNSTABLE_RECENT_DRIFT'
                else 'ELIGIBLE'
            end as REASON,
            :v_run_id as RUN_ID,
            current_timestamp() as CALCULATED_AT
        from TMP_DAILY_CAL_SYMBOL s
    ) src
    on t.TRAINING_VERSION = src.TRAINING_VERSION
   and t.SYMBOL = src.SYMBOL
   and t.MARKET_TYPE = src.MARKET_TYPE
   and t.PATTERN_ID = src.PATTERN_ID
   and t.HORIZON_BARS = src.HORIZON_BARS
    when matched then update set
        t.N_OUTCOMES = src.N_OUTCOMES,
        t.PATTERN_METRIC = src.PATTERN_METRIC,
        t.SYMBOL_METRIC = src.SYMBOL_METRIC,
        t.SYMBOL_MEDIAN_METRIC = src.SYMBOL_MEDIAN_METRIC,
        t.SYMBOL_WIN_RATE = src.SYMBOL_WIN_RATE,
        t.RAW_MULTIPLIER = src.RAW_MULTIPLIER,
        t.SHRINK_FACTOR = src.SHRINK_FACTOR,
        t.SHRUNK_MULTIPLIER = src.SHRUNK_MULTIPLIER,
        t.MULTIPLIER_CAPPED = src.MULTIPLIER_CAPPED,
        t.CI_WIDTH = src.CI_WIDTH,
        t.STABILITY_SCORE = src.STABILITY_SCORE,
        t.STABILITY_OK = src.STABILITY_OK,
        t.ELIGIBLE_FLAG = src.ELIGIBLE_FLAG,
        t.REASON = src.REASON,
        t.RUN_ID = src.RUN_ID,
        t.CALCULATED_AT = src.CALCULATED_AT
    when not matched then insert (
        TRAINING_VERSION, SYMBOL, MARKET_TYPE, PATTERN_ID, HORIZON_BARS,
        N_OUTCOMES, PATTERN_METRIC, SYMBOL_METRIC, SYMBOL_MEDIAN_METRIC, SYMBOL_WIN_RATE,
        RAW_MULTIPLIER, SHRINK_FACTOR, SHRUNK_MULTIPLIER, MULTIPLIER_CAPPED,
        CI_WIDTH, STABILITY_SCORE, STABILITY_OK, ELIGIBLE_FLAG, REASON, RUN_ID, CALCULATED_AT
    ) values (
        src.TRAINING_VERSION, src.SYMBOL, src.MARKET_TYPE, src.PATTERN_ID, src.HORIZON_BARS,
        src.N_OUTCOMES, src.PATTERN_METRIC, src.SYMBOL_METRIC, src.SYMBOL_MEDIAN_METRIC, src.SYMBOL_WIN_RATE,
        src.RAW_MULTIPLIER, src.SHRINK_FACTOR, src.SHRUNK_MULTIPLIER, src.MULTIPLIER_CAPPED,
        src.CI_WIDTH, src.STABILITY_SCORE, src.STABILITY_OK, src.ELIGIBLE_FLAG, src.REASON, src.RUN_ID, src.CALCULATED_AT
    );

    merge into MIP.APP.DAILY_POLICY_EFFECTIVE_TRAINED t
    using (
        select
            :v_training_version as TRAINING_VERSION,
            s.SYMBOL,
            s.MARKET_TYPE,
            s.PATTERN_ID,
            s.HORIZON_BARS,
            s.PATTERN_METRIC as PATTERN_TARGET,
            iff(s.ELIGIBLE_FLAG, s.MULTIPLIER_CAPPED, 1.0) as SYMBOL_MULTIPLIER,
            s.PATTERN_METRIC * iff(s.ELIGIBLE_FLAG, s.MULTIPLIER_CAPPED, 1.0) as EFFECTIVE_TARGET,
            iff(s.ELIGIBLE_FLAG, 'PATTERN+SYMBOL', 'PATTERN_ONLY') as TARGET_SOURCE,
            s.HORIZON_BARS as EFFECTIVE_HORIZON_BARS,
            'PATTERN_ONLY' as HORIZON_SOURCE,
            s.N_OUTCOMES,
            s.CI_WIDTH,
            s.STABILITY_OK,
            iff(s.ELIGIBLE_FLAG, null, s.REASON) as FALLBACK_REASON,
            s.ELIGIBLE_FLAG,
            :v_run_id as RUN_ID,
            current_timestamp() as CALCULATED_AT
        from MIP.APP.DAILY_SYMBOL_CALIBRATION_TRAINED s
        where s.TRAINING_VERSION = :v_training_version
    ) src
    on t.TRAINING_VERSION = src.TRAINING_VERSION
   and t.SYMBOL = src.SYMBOL
   and t.MARKET_TYPE = src.MARKET_TYPE
   and t.PATTERN_ID = src.PATTERN_ID
   and t.HORIZON_BARS = src.HORIZON_BARS
    when matched then update set
        t.PATTERN_TARGET = src.PATTERN_TARGET,
        t.SYMBOL_MULTIPLIER = src.SYMBOL_MULTIPLIER,
        t.EFFECTIVE_TARGET = src.EFFECTIVE_TARGET,
        t.TARGET_SOURCE = src.TARGET_SOURCE,
        t.EFFECTIVE_HORIZON_BARS = src.EFFECTIVE_HORIZON_BARS,
        t.HORIZON_SOURCE = src.HORIZON_SOURCE,
        t.N_OUTCOMES = src.N_OUTCOMES,
        t.CI_WIDTH = src.CI_WIDTH,
        t.STABILITY_OK = src.STABILITY_OK,
        t.FALLBACK_REASON = src.FALLBACK_REASON,
        t.ELIGIBLE_FLAG = src.ELIGIBLE_FLAG,
        t.RUN_ID = src.RUN_ID,
        t.CALCULATED_AT = src.CALCULATED_AT
    when not matched then insert (
        TRAINING_VERSION, SYMBOL, MARKET_TYPE, PATTERN_ID, HORIZON_BARS,
        PATTERN_TARGET, SYMBOL_MULTIPLIER, EFFECTIVE_TARGET, TARGET_SOURCE,
        EFFECTIVE_HORIZON_BARS, HORIZON_SOURCE,
        N_OUTCOMES, CI_WIDTH, STABILITY_OK, FALLBACK_REASON, ELIGIBLE_FLAG,
        RUN_ID, CALCULATED_AT
    ) values (
        src.TRAINING_VERSION, src.SYMBOL, src.MARKET_TYPE, src.PATTERN_ID, src.HORIZON_BARS,
        src.PATTERN_TARGET, src.SYMBOL_MULTIPLIER, src.EFFECTIVE_TARGET, src.TARGET_SOURCE,
        src.EFFECTIVE_HORIZON_BARS, src.HORIZON_SOURCE,
        src.N_OUTCOMES, src.CI_WIDTH, src.STABILITY_OK, src.FALLBACK_REASON, src.ELIGIBLE_FLAG,
        src.RUN_ID, src.CALCULATED_AT
    );

    select
        count(*) as TOTAL_BUCKETS,
        count_if(ELIGIBLE_FLAG) as ELIGIBLE_BUCKETS,
        avg(iff(ELIGIBLE_FLAG, 1.0, 0.0)) as ELIGIBLE_SHARE,
        median(MULTIPLIER_CAPPED) as MEDIAN_MULTIPLIER,
        percentile_cont(0.95) within group (order by MULTIPLIER_CAPPED) as P95_MULTIPLIER
      into :v_total_buckets, :v_eligible_buckets, :v_eligible_share, :v_median_mult, :v_p95_mult
      from MIP.APP.DAILY_SYMBOL_CALIBRATION_TRAINED
     where TRAINING_VERSION = :v_training_version;

    merge into MIP.APP.DAILY_CALIBRATION_EVAL_RUNS t
    using (
        select
            :v_run_id as RUN_ID,
            :v_training_version as TRAINING_VERSION,
            :v_market_type as MARKET_TYPE,
            :v_start_date as START_DATE,
            :v_end_date as END_DATE,
            'SUCCESS' as STATUS,
            :v_total_buckets as TOTAL_SYMBOL_BUCKETS,
            :v_eligible_buckets as ELIGIBLE_SYMBOL_BUCKETS,
            :v_eligible_share as ELIGIBLE_SHARE,
            :v_median_mult as MEDIAN_MULTIPLIER,
            :v_p95_mult as P95_MULTIPLIER,
            null as SIGNAL_INVARIANCE_OK,
            null as TARGET_RANGE_OK,
            null as HORIZON_CONSISTENCY_OK,
            null as NO_TARGET_INFLATION_REGRESSION_OK,
            object_construct(
                'min_n', :v_min_n,
                'max_ci_width', :v_max_ci_width,
                'mult_cap_lo', :v_mult_cap_lo,
                'mult_cap_hi', :v_mult_cap_hi,
                'shrink_k', :v_shrink_k,
                'min_n_horizon', :v_min_n_horizon,
                'max_ci_width_horizon', :v_max_ci_width_horizon
            ) as DETAILS,
            current_timestamp() as STARTED_AT,
            current_timestamp() as FINISHED_AT
    ) src
    on t.RUN_ID = src.RUN_ID
   and t.TRAINING_VERSION = src.TRAINING_VERSION
    when matched then update set
        t.MARKET_TYPE = src.MARKET_TYPE,
        t.START_DATE = src.START_DATE,
        t.END_DATE = src.END_DATE,
        t.STATUS = src.STATUS,
        t.TOTAL_SYMBOL_BUCKETS = src.TOTAL_SYMBOL_BUCKETS,
        t.ELIGIBLE_SYMBOL_BUCKETS = src.ELIGIBLE_SYMBOL_BUCKETS,
        t.ELIGIBLE_SHARE = src.ELIGIBLE_SHARE,
        t.MEDIAN_MULTIPLIER = src.MEDIAN_MULTIPLIER,
        t.P95_MULTIPLIER = src.P95_MULTIPLIER,
        t.DETAILS = src.DETAILS,
        t.STARTED_AT = src.STARTED_AT,
        t.FINISHED_AT = src.FINISHED_AT
    when not matched then insert (
        RUN_ID, TRAINING_VERSION, MARKET_TYPE, START_DATE, END_DATE, STATUS,
        TOTAL_SYMBOL_BUCKETS, ELIGIBLE_SYMBOL_BUCKETS, ELIGIBLE_SHARE,
        MEDIAN_MULTIPLIER, P95_MULTIPLIER,
        SIGNAL_INVARIANCE_OK, TARGET_RANGE_OK, HORIZON_CONSISTENCY_OK, NO_TARGET_INFLATION_REGRESSION_OK,
        DETAILS, STARTED_AT, FINISHED_AT
    ) values (
        src.RUN_ID, src.TRAINING_VERSION, src.MARKET_TYPE, src.START_DATE, src.END_DATE, src.STATUS,
        src.TOTAL_SYMBOL_BUCKETS, src.ELIGIBLE_SYMBOL_BUCKETS, src.ELIGIBLE_SHARE,
        src.MEDIAN_MULTIPLIER, src.P95_MULTIPLIER,
        src.SIGNAL_INVARIANCE_OK, src.TARGET_RANGE_OK, src.HORIZON_CONSISTENCY_OK, src.NO_TARGET_INFLATION_REGRESSION_OK,
        src.DETAILS, src.STARTED_AT, src.FINISHED_AT
    );

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'training_version', :v_training_version,
        'market_type', :v_market_type,
        'start_date', :v_start_date,
        'end_date', :v_end_date,
        'total_symbol_buckets', :v_total_buckets,
        'eligible_symbol_buckets', :v_eligible_buckets,
        'eligible_share', :v_eligible_share,
        'median_multiplier', :v_median_mult,
        'p95_multiplier', :v_p95_mult
    );
end;
$$;

