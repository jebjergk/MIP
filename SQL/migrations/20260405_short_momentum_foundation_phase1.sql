-- Short Momentum Foundation — Phase 1
-- DDL, APP_CONFIG gate, V_PATTERN_METADATA_UI, seed MOMENTUM_SHORT (disabled).

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.APP.RECOMMENDATION_LOG
    add column if not exists SIGNAL_DIRECTION varchar;

comment on column MIP.APP.RECOMMENDATION_LOG.SIGNAL_DIRECTION is 'LONG or SHORT per signal; NULL means legacy LONG semantics.';

alter table MIP.APP.PATTERN_DEFINITION add column if not exists PATTERN_FAMILY varchar;
alter table MIP.APP.PATTERN_DEFINITION add column if not exists DISPLAY_NAME varchar;
alter table MIP.APP.PATTERN_DEFINITION add column if not exists DISPLAY_SHORT_NAME varchar;
alter table MIP.APP.PATTERN_DEFINITION add column if not exists SIGNAL_DIRECTION varchar;
alter table MIP.APP.PATTERN_DEFINITION add column if not exists PARAM_FAST number;
alter table MIP.APP.PATTERN_DEFINITION add column if not exists PARAM_SLOW number;
alter table MIP.APP.PATTERN_DEFINITION add column if not exists PARAM_HORIZON varchar;

comment on column MIP.APP.PATTERN_DEFINITION.SIGNAL_DIRECTION is 'Pattern default direction for metadata; log column is authoritative per signal.';

update MIP.APP.PATTERN_DEFINITION
set
    DISPLAY_NAME = coalesce(DISPLAY_NAME, NAME),
    DISPLAY_SHORT_NAME = coalesce(DISPLAY_SHORT_NAME, NAME)
where DISPLAY_NAME is null
   or DISPLAY_SHORT_NAME is null;

update MIP.APP.PATTERN_DEFINITION
set PATTERN_FAMILY = coalesce(
    PATTERN_FAMILY,
    case
        when coalesce(PATTERN_TYPE, 'MOMENTUM') = 'MEAN_REVERSION' then 'MEAN_REVERSION'
        when PATTERN_TYPE in ('ORB', 'PULLBACK_CONTINUATION', 'BEARISH_MOMENTUM') then 'MOMENTUM'
        when PATTERN_TYPE = 'MOMENTUM_SHORT' then 'MOMENTUM'
        else 'MOMENTUM'
    end
)
where PATTERN_FAMILY is null;

update MIP.APP.PATTERN_DEFINITION
set SIGNAL_DIRECTION = coalesce(
    SIGNAL_DIRECTION,
    case when PATTERN_TYPE = 'MOMENTUM_SHORT' then 'SHORT' else 'LONG' end
)
where SIGNAL_DIRECTION is null;

update MIP.APP.PATTERN_DEFINITION
set
    PARAM_FAST = coalesce(PARAM_FAST, try_to_number(PARAMS_JSON:fast_window::string)),
    PARAM_SLOW = coalesce(PARAM_SLOW, try_to_number(PARAMS_JSON:slow_window::string))
where PARAMS_JSON is not null
  and (PARAM_FAST is null or PARAM_SLOW is null);

merge into MIP.APP.APP_CONFIG t
using (
    select
        'SHORT_MOMENTUM_GENERATION_ENABLED' as CONFIG_KEY,
        'false' as CONFIG_VALUE,
        'When true, SP_GENERATE_MOMENTUM_RECS may emit SHORT (MOMENTUM_SHORT) rows.' as DESCRIPTION
    union all
    select
        'SHORT_MOMENTUM_PATTERN_TYPES',
        'MOMENTUM_SHORT',
        'Comma-separated PATTERN_TYPE values for short momentum when generation is enabled.'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());

create or replace view MIP.MART.V_PATTERN_METADATA_UI as
select
    pd.PATTERN_ID,
    pd.PATTERN_TYPE,
    coalesce(
        pd.PATTERN_FAMILY,
        case
            when coalesce(pd.PATTERN_TYPE, 'MOMENTUM') = 'MEAN_REVERSION' then 'MEAN_REVERSION'
            when pd.PATTERN_TYPE in ('ORB', 'PULLBACK_CONTINUATION', 'BEARISH_MOMENTUM') then 'MOMENTUM'
            when pd.PATTERN_TYPE = 'MOMENTUM_SHORT' then 'MOMENTUM'
            else 'MOMENTUM'
        end
    ) as PATTERN_FAMILY,
    coalesce(
        pd.SIGNAL_DIRECTION,
        case when pd.PATTERN_TYPE = 'MOMENTUM_SHORT' then 'SHORT' else 'LONG' end
    ) as SIGNAL_DIRECTION,
    coalesce(pd.DISPLAY_NAME, pd.NAME) as DISPLAY_NAME,
    coalesce(pd.DISPLAY_SHORT_NAME, pd.NAME) as DISPLAY_SHORT_NAME,
    nullif(
        trim(
            concat_ws(
                ' | ',
                iff(
                    coalesce(pd.PARAM_FAST, pd.PARAMS_JSON:fast_window::float) is not null,
                    'Fast ' || coalesce(pd.PARAM_FAST::varchar, to_varchar(pd.PARAMS_JSON:fast_window::float)),
                    null
                ),
                iff(
                    coalesce(pd.PARAM_SLOW, pd.PARAMS_JSON:slow_window::float) is not null,
                    'Slow ' || coalesce(pd.PARAM_SLOW::varchar, to_varchar(pd.PARAMS_JSON:slow_window::float)),
                    null
                ),
                pd.PARAM_HORIZON
            )
        ),
        ''
    ) as PARAM_SUMMARY
from MIP.APP.PATTERN_DEFINITION pd;

merge into MIP.APP.PATTERN_DEFINITION t
using (
    select
        'STOCK_MOMENTUM_FAST_SHORT' as NAME,
        'Stock momentum short (fast, daily) — research only' as DESCRIPTION,
        'MOMENTUM_SHORT' as PATTERN_TYPE,
        object_construct(
            'fast_window', 20,
            'slow_window', 3,
            'lookback_days', 30,
            'min_return', 0.002,
            'min_zscore', 1.0,
            'market_type', 'STOCK',
            'interval_minutes', 1440
        ) as PARAMS_JSON,
        'MOMENTUM' as PATTERN_FAMILY,
        'Momentum (Fast, SHORT)' as DISPLAY_NAME,
        'Mom F SHORT' as DISPLAY_SHORT_NAME,
        'SHORT' as SIGNAL_DIRECTION,
        20 as PARAM_FAST,
        3 as PARAM_SLOW,
        '1440m' as PARAM_HORIZON,
        'N' as IS_ACTIVE,
        false as ENABLED
    union all
    select
        'STOCK_MOMENTUM_SLOW_SHORT',
        'Stock momentum short (slow, daily) — research only',
        'MOMENTUM_SHORT',
        object_construct(
            'fast_window', 30,
            'slow_window', 2,
            'lookback_days', 60,
            'min_return', 0.001,
            'min_zscore', 0.75,
            'market_type', 'STOCK',
            'interval_minutes', 1440
        ),
        'MOMENTUM',
        'Momentum (Slow, SHORT)',
        'Mom S SHORT',
        'SHORT',
        30,
        2,
        '1440m',
        'N',
        false
) s
on t.NAME = s.NAME
when matched then update set
    t.DESCRIPTION = s.DESCRIPTION,
    t.PATTERN_TYPE = s.PATTERN_TYPE,
    t.PARAMS_JSON = s.PARAMS_JSON,
    t.PATTERN_FAMILY = s.PATTERN_FAMILY,
    t.DISPLAY_NAME = s.DISPLAY_NAME,
    t.DISPLAY_SHORT_NAME = s.DISPLAY_SHORT_NAME,
    t.SIGNAL_DIRECTION = s.SIGNAL_DIRECTION,
    t.PARAM_FAST = s.PARAM_FAST,
    t.PARAM_SLOW = s.PARAM_SLOW,
    t.PARAM_HORIZON = s.PARAM_HORIZON,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.ENABLED = s.ENABLED
when not matched then insert (
    NAME, DESCRIPTION, PATTERN_TYPE, PARAMS_JSON, PATTERN_FAMILY,
    DISPLAY_NAME, DISPLAY_SHORT_NAME, SIGNAL_DIRECTION,
    PARAM_FAST, PARAM_SLOW, PARAM_HORIZON, IS_ACTIVE, ENABLED
)
values (
    s.NAME, s.DESCRIPTION, s.PATTERN_TYPE, s.PARAMS_JSON, s.PATTERN_FAMILY,
    s.DISPLAY_NAME, s.DISPLAY_SHORT_NAME, s.SIGNAL_DIRECTION,
    s.PARAM_FAST, s.PARAM_SLOW, s.PARAM_HORIZON, s.IS_ACTIVE, s.ENABLED
);
