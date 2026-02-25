-- 350_intraday_v2_foundation_tables.sql
-- Purpose: Intraday v2 decoupled foundation schema (state-first research stack).
-- Notes:
--   - Additive/idempotent DDL only.
--   - Keeps daily registry/training objects untouched.

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------------------------
-- 1) Intraday v2 pattern registry
-----------------------------------------
create table if not exists MIP.APP.INTRA_PATTERN_DEFS (
    PATTERN_ID         number        autoincrement,
    PATTERN_NAME       string        not null,
    VERSION            string        not null default 'v1',
    PATTERN_FAMILY     string        not null,
    PATTERN_TYPE       string        not null,
    PARAMS_JSON        variant,
    IS_ENABLED         boolean       not null default true,
    VALID_FROM_TS      timestamp_ntz not null default current_timestamp(),
    VALID_TO_TS        timestamp_ntz,
    CREATED_AT         timestamp_ntz not null default current_timestamp(),
    CREATED_BY         string        not null default current_user(),
    UPDATED_AT         timestamp_ntz,
    UPDATED_BY         string,
    constraint PK_INTRA_PATTERN_DEFS primary key (PATTERN_ID),
    constraint UQ_INTRA_PATTERN_DEFS_NAME_VER unique (PATTERN_NAME, VERSION)
);

-----------------------------------------
-- 2) Intraday horizon definitions
-----------------------------------------
create table if not exists MIP.APP.INTRA_HORIZON_DEF (
    HORIZON_BARS       number        not null,
    HORIZON_NAME       string        not null,
    DESCRIPTION        string,
    IS_ACTIVE          boolean       not null default true,
    VALID_FROM_TS      timestamp_ntz not null default current_timestamp(),
    VALID_TO_TS        timestamp_ntz,
    VERSION_TAG        string        not null default 'v1',
    CREATED_AT         timestamp_ntz not null default current_timestamp(),
    UPDATED_AT         timestamp_ntz,
    constraint PK_INTRA_HORIZON_DEF primary key (HORIZON_BARS),
    constraint UQ_INTRA_HORIZON_DEF_NAME unique (HORIZON_NAME)
);

merge into MIP.APP.INTRA_HORIZON_DEF t
using (
    select 4 as HORIZON_BARS, 'H04' as HORIZON_NAME, '4 bars (~1 hour on 15m)' as DESCRIPTION, true as IS_ACTIVE, 'v1' as VERSION_TAG
    union all
    select 8, 'H08', '8 bars (~2 hours on 15m)', true, 'v1'
    union all
    select 16, 'H16', '16 bars (~4 hours on 15m)', true, 'v1'
) s
on t.HORIZON_BARS = s.HORIZON_BARS
when matched then update set
    t.HORIZON_NAME = s.HORIZON_NAME,
    t.DESCRIPTION = s.DESCRIPTION,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.VERSION_TAG = s.VERSION_TAG,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    HORIZON_BARS, HORIZON_NAME, DESCRIPTION, IS_ACTIVE, VERSION_TAG
) values (
    s.HORIZON_BARS, s.HORIZON_NAME, s.DESCRIPTION, s.IS_ACTIVE, s.VERSION_TAG
);

-- Snowflake standard-table PK/UQ constraints are informational, so enforce one row per HORIZON_BARS.
delete from MIP.APP.INTRA_HORIZON_DEF t
using (
    select
        HORIZON_BARS,
        CREATED_AT,
        row_number() over (
            partition by HORIZON_BARS
            order by CREATED_AT desc
        ) as RN
    from MIP.APP.INTRA_HORIZON_DEF
) d
where t.HORIZON_BARS = d.HORIZON_BARS
  and t.CREATED_AT = d.CREATED_AT
  and d.RN > 1;

-----------------------------------------
-- 3) Versioned bucket definitions
-----------------------------------------
create table if not exists MIP.APP.STATE_BUCKET_DEF (
    BUCKET_VERSION     string        not null,
    STATE_BUCKET_ID    string        not null,
    DIRECTION_CLASS    string        not null,
    REGIME_CLASS       string        not null,
    CONFIDENCE_TIER    string        not null,
    THRESHOLD_JSON     variant,
    IS_ACTIVE          boolean       not null default true,
    CREATED_AT         timestamp_ntz not null default current_timestamp(),
    UPDATED_AT         timestamp_ntz,
    constraint PK_STATE_BUCKET_DEF primary key (BUCKET_VERSION, STATE_BUCKET_ID)
);

-----------------------------------------
-- 4) State snapshots (15m OHLC-derived only)
-----------------------------------------
create table if not exists MIP.APP.STATE_SNAPSHOT_15M (
    MARKET_TYPE                    string        not null,
    SYMBOL                         string        not null,
    INTERVAL_MINUTES               number        not null,
    TS                             timestamp_ntz not null,
    BELIEF_DIRECTION               float,
    BELIEF_STRENGTH                float,
    BELIEF_STABILITY               float,
    REACTION_SPEED                 float,
    DRIFT_VS_IMPULSE               float,
    RECOVERY_TIME                  float,
    MTF_ALIGNMENT                  float,
    CHOP_INDEX                     float,
    VOL_DIRECTION_ALIGNMENT        float,
    STATE_BUCKET_ID                string        not null,
    METRIC_VERSION                 string        not null,
    BUCKET_VERSION                 string        not null,
    SOURCE_BAR_KEY_HASH            string,
    CALCULATED_AT                  timestamp_ntz not null default current_timestamp(),
    constraint PK_STATE_SNAPSHOT_15M primary key (MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS)
)
cluster by (INTERVAL_MINUTES, MARKET_TYPE, SYMBOL, TS);

-----------------------------------------
-- 5) Optional state transitions
-----------------------------------------
create table if not exists MIP.APP.STATE_TRANSITIONS (
    MARKET_TYPE                    string        not null,
    SYMBOL                         string        not null,
    INTERVAL_MINUTES               number        not null,
    TS_FROM                        timestamp_ntz not null,
    TS_TO                          timestamp_ntz not null,
    FROM_STATE_BUCKET_ID           string        not null,
    TO_STATE_BUCKET_ID             string        not null,
    DURATION_BARS                  number        not null,
    METRIC_VERSION                 string        not null,
    BUCKET_VERSION                 string        not null,
    CALCULATED_AT                  timestamp_ntz not null default current_timestamp(),
    constraint PK_STATE_TRANSITIONS primary key (MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS_FROM, TS_TO)
)
cluster by (INTERVAL_MINUTES, MARKET_TYPE, SYMBOL, TS_FROM);

-----------------------------------------
-- 6) Intraday v2 signals
-----------------------------------------
create table if not exists MIP.APP.INTRA_SIGNALS (
    SIGNAL_ID                      number        autoincrement,
    SIGNAL_NK_HASH                 string        not null,
    PATTERN_ID                     number        not null,
    MARKET_TYPE                    string        not null,
    SYMBOL                         string        not null,
    INTERVAL_MINUTES               number        not null,
    SIGNAL_TS                      timestamp_ntz not null,
    SIGNAL_SIDE                    string        not null,
    SCORE                          number(38,10),
    STATE_BUCKET_ID                string        not null,
    STATE_SNAPSHOT_TS              timestamp_ntz not null,
    FEATURES_JSON                  variant,
    SOURCE_MODE                    string        not null,
    RUN_ID                         string,
    METRIC_VERSION                 string        not null,
    BUCKET_VERSION                 string        not null,
    GENERATED_AT                   timestamp_ntz not null default current_timestamp(),
    constraint PK_INTRA_SIGNALS primary key (SIGNAL_ID),
    constraint UQ_INTRA_SIGNALS_NK_HASH unique (SIGNAL_NK_HASH)
)
cluster by (INTERVAL_MINUTES, MARKET_TYPE, SYMBOL, SIGNAL_TS);

-----------------------------------------
-- 7) Intraday v2 outcomes
-----------------------------------------
create table if not exists MIP.APP.INTRA_OUTCOMES (
    SIGNAL_ID                      number        not null,
    SIGNAL_NK_HASH                 string        not null,
    HORIZON_BARS                   number        not null,
    ENTRY_TS                       timestamp_ntz not null,
    EXIT_TS                        timestamp_ntz,
    ENTRY_PX                       number(38,8),
    EXIT_PX                        number(38,8),
    RETURN_GROSS                   number(38,8),
    RETURN_NET                     number(38,8),
    DIRECTION                      string,
    HIT_FLAG                       boolean,
    EVAL_STATUS                    string,
    MFE                            number(38,8),
    MAE                            number(38,8),
    METRIC_VERSION                 string        not null,
    BUCKET_VERSION                 string        not null,
    CALCULATED_AT                  timestamp_ntz not null default current_timestamp(),
    constraint PK_INTRA_OUTCOMES primary key (SIGNAL_ID, HORIZON_BARS)
)
cluster by (HORIZON_BARS, ENTRY_TS);

-----------------------------------------
-- 8) Trust snapshots (state-conditioned)
-----------------------------------------
create table if not exists MIP.APP.INTRA_TRUST_STATS (
    PATTERN_ID                     number        not null,
    MARKET_TYPE                    string        not null,
    INTERVAL_MINUTES               number        not null,
    HORIZON_BARS                   number        not null,
    STATE_BUCKET_ID                string        not null,
    TRAIN_WINDOW_START             timestamp_ntz not null,
    TRAIN_WINDOW_END               timestamp_ntz not null,
    CALCULATED_AT                  timestamp_ntz not null,
    N_SIGNALS                      number        not null,
    N_HITS                         number        not null,
    HIT_RATE                       float,
    AVG_RETURN_NET                 float,
    RETURN_STDDEV                  float,
    CI_LOW                         float,
    CI_HIGH                        float,
    CI_WIDTH                       float,
    FALLBACK_LEVEL                 string,
    FALLBACK_SOURCE_BUCKET_ID      string,
    METRIC_VERSION                 string        not null,
    BUCKET_VERSION                 string        not null,
    TRUST_VERSION                  string        not null,
    TERRAIN_VERSION                string,
    CREATED_AT                     timestamp_ntz not null default current_timestamp(),
    constraint PK_INTRA_TRUST_STATS primary key (
        PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS, STATE_BUCKET_ID,
        CALCULATED_AT, TRAIN_WINDOW_START, TRAIN_WINDOW_END
    )
)
cluster by (CALCULATED_AT, INTERVAL_MINUTES, MARKET_TYPE, PATTERN_ID, HORIZON_BARS);

-----------------------------------------
-- 9) Opportunity terrain
-----------------------------------------
create table if not exists MIP.APP.OPPORTUNITY_TERRAIN_15M (
    PATTERN_ID                     number        not null,
    MARKET_TYPE                    string        not null,
    SYMBOL                         string        not null,
    INTERVAL_MINUTES               number        not null,
    TS                             timestamp_ntz not null,
    HORIZON_BARS                   number        not null,
    STATE_BUCKET_ID                string        not null,
    EDGE                           float,
    UNCERTAINTY                    float,
    SUITABILITY                    float,
    EDGE_Z                         float,
    UNCERTAINTY_Z                  float,
    SUITABILITY_Z                  float,
    TERRAIN_SCORE                  float,
    SHRINKAGE_K                    float,
    WEIGHTS_JSON                   variant,
    METRIC_VERSION                 string        not null,
    BUCKET_VERSION                 string        not null,
    TERRAIN_VERSION                string        not null,
    CALCULATED_AT                  timestamp_ntz not null default current_timestamp(),
    constraint PK_OPPORTUNITY_TERRAIN_15M primary key (
        PATTERN_ID, MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS, HORIZON_BARS, STATE_BUCKET_ID
    )
)
cluster by (INTERVAL_MINUTES, MARKET_TYPE, SYMBOL, TS, HORIZON_BARS);

-----------------------------------------
-- 10) Backfill run log
-----------------------------------------
create table if not exists MIP.APP.INTRA_BACKFILL_RUN_LOG (
    RUN_ID                         string        not null,
    CHUNK_ID                       string        not null,
    START_TS                       timestamp_ntz not null,
    END_TS                         timestamp_ntz not null,
    PATTERN_SET                    string        not null,
    PATTERN_SET_HASH               string        not null,
    FORCE_RECOMPUTE                boolean       not null,
    METRIC_VERSION                 string        not null,
    BUCKET_VERSION                 string        not null,
    TRUST_VERSION                  string        not null,
    TERRAIN_VERSION                string        not null,
    STATUS                         string        not null,
    STARTED_AT                     timestamp_ntz,
    COMPLETED_AT                   timestamp_ntz,
    ROWS_STATE_SNAPSHOT            number,
    ROWS_STATE_TRANSITIONS         number,
    ROWS_SIGNALS                   number,
    ROWS_OUTCOMES                  number,
    ROWS_TRUST                     number,
    ROWS_TERRAIN                   number,
    ERROR_MESSAGE                  string,
    DETAILS                        variant,
    CREATED_AT                     timestamp_ntz not null default current_timestamp(),
    UPDATED_AT                     timestamp_ntz,
    constraint PK_INTRA_BACKFILL_RUN_LOG primary key (RUN_ID, CHUNK_ID),
    constraint UQ_INTRA_BACKFILL_SCOPE unique (
        START_TS, END_TS, PATTERN_SET_HASH, FORCE_RECOMPUTE, CHUNK_ID,
        METRIC_VERSION, BUCKET_VERSION, TRUST_VERSION, TERRAIN_VERSION
    )
);
