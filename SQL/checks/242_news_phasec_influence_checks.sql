-- 242_news_phasec_influence_checks.sql
-- Purpose: Phase C checks for proposal-time news influence math and bounds.

use role MIP_ADMIN_ROLE;
use database MIP;

with news_cfg as (
    select
        coalesce(max(iff(CONFIG_KEY = 'NEWS_ENABLED', lower(CONFIG_VALUE), null)), 'false') as NEWS_ENABLED,
        coalesce(max(iff(CONFIG_KEY = 'NEWS_DISPLAY_ONLY', lower(CONFIG_VALUE), null)), 'true') as NEWS_DISPLAY_ONLY,
        coalesce(max(iff(CONFIG_KEY = 'NEWS_INFLUENCE_ENABLED', lower(CONFIG_VALUE), null)), 'false') as NEWS_INFLUENCE_ENABLED,
        coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_DECAY_TAU_HOURS', CONFIG_VALUE, null))), 24) as NEWS_DECAY_TAU_HOURS,
        coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_PRESSURE_HOT', CONFIG_VALUE, null))), 0.12) as NEWS_PRESSURE_HOT,
        coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_UNCERTAINTY_HIGH', CONFIG_VALUE, null))), 0.08) as NEWS_UNCERTAINTY_HIGH,
        coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_EVENT_RISK_HIGH', CONFIG_VALUE, null))), 0.10) as NEWS_EVENT_RISK_HIGH,
        coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_SCORE_MAX_ABS', CONFIG_VALUE, null))), 0.20) as NEWS_SCORE_MAX_ABS,
        coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_STALENESS_THRESHOLD_MINUTES', CONFIG_VALUE, null))), 180) as NEWS_STALE_MINUTES
    from MIP.APP.APP_CONFIG
),
news_candidates as (
    select
        s.RECOMMENDATION_ID,
        n.NEWS_COUNT,
        n.NEWS_CONTEXT_BADGE,
        n.NOVELTY_SCORE,
        n.BURST_SCORE,
        n.UNCERTAINTY_FLAG,
        n.TOP_HEADLINES,
        n.LAST_NEWS_PUBLISHED_AT,
        n.LAST_INGESTED_AT,
        n.SNAPSHOT_TS,
        row_number() over (
            partition by s.RECOMMENDATION_ID
            order by n.SNAPSHOT_TS desc, n.CREATED_AT desc
        ) as RN
    from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
    left join MIP.NEWS.NEWS_INFO_STATE_DAILY n
      on n.SYMBOL = s.SYMBOL
     and n.MARKET_TYPE = s.MARKET_TYPE
     and n.SNAPSHOT_TS <= s.SIGNAL_TS
),
news_latest as (
    select * from news_candidates where RN = 1
),
eligible as (
    select
        s.RECOMMENDATION_ID,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.SCORE,
        s.SIGNAL_TS,
        cfg.NEWS_ENABLED,
        cfg.NEWS_DISPLAY_ONLY,
        cfg.NEWS_INFLUENCE_ENABLED,
        cfg.NEWS_DECAY_TAU_HOURS,
        cfg.NEWS_PRESSURE_HOT,
        cfg.NEWS_UNCERTAINTY_HIGH,
        cfg.NEWS_EVENT_RISK_HIGH,
        cfg.NEWS_SCORE_MAX_ABS,
        cfg.NEWS_STALE_MINUTES,
        nl.NEWS_COUNT,
        nl.NEWS_CONTEXT_BADGE,
        nl.NOVELTY_SCORE as NEWS_NOVELTY_SCORE,
        nl.BURST_SCORE as NEWS_BURST_SCORE,
        nl.UNCERTAINTY_FLAG as NEWS_UNCERTAINTY_FLAG,
        nl.TOP_HEADLINES as NEWS_TOP_HEADLINES,
        nl.LAST_NEWS_PUBLISHED_AT as NEWS_LAST_PUBLISHED_AT,
        nl.LAST_INGESTED_AT as NEWS_LAST_INGESTED_AT,
        nl.SNAPSHOT_TS as NEWS_SNAPSHOT_TS,
        iff(
            nl.SNAPSHOT_TS is null,
            null,
            datediff('minute', nl.SNAPSHOT_TS, s.SIGNAL_TS)
        ) as NEWS_SNAPSHOT_AGE_MINUTES,
        iff(
            nl.SNAPSHOT_TS is null,
            null,
            datediff('minute', nl.SNAPSHOT_TS, s.SIGNAL_TS) > cfg.NEWS_STALE_MINUTES
        ) as NEWS_IS_STALE
    from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS s
    cross join news_cfg cfg
    left join news_latest nl
      on nl.RECOMMENDATION_ID = s.RECOMMENDATION_ID
),
deduped as (
    select
        e.*
    from eligible e
    qualify row_number() over (
        partition by e.SYMBOL
        order by e.SCORE desc, e.RECOMMENDATION_ID
    ) = 1
),
scored as (
    select
        d.*,
        iff(
            d.NEWS_SNAPSHOT_AGE_MINUTES is null,
            0.0,
            exp(
                -greatest(d.NEWS_SNAPSHOT_AGE_MINUTES, 0) / 60.0
                / nullif(d.NEWS_DECAY_TAU_HOURS, 0)
            )
        ) as NEWS_RECENCY_WEIGHT,
        case upper(coalesce(d.NEWS_CONTEXT_BADGE, ''))
            when 'HOT' then 1.0
            when 'WARM' then 0.5
            when 'COLD' then -0.25
            else 0.0
        end as NEWS_PRESSURE_SCORE,
        least(
            greatest(
                greatest(
                    coalesce(d.NEWS_BURST_SCORE, 0.0),
                    iff(coalesce(d.NEWS_UNCERTAINTY_FLAG, false), 0.7, 0.0),
                    iff(coalesce(d.NEWS_IS_STALE, false), 1.0, 0.0)
                ),
                0.0
            ),
            1.0
        ) as NEWS_EVENT_RISK_PROXY
    from deduped d
),
final_scored as (
    select
        s.*,
        least(
            greatest(
                iff(
                    s.NEWS_ENABLED = 'true'
                    and s.NEWS_INFLUENCE_ENABLED = 'true'
                    and s.NEWS_DISPLAY_ONLY <> 'true',
                    s.NEWS_RECENCY_WEIGHT
                    * (
                        s.NEWS_PRESSURE_SCORE * abs(s.NEWS_PRESSURE_HOT)
                        - iff(coalesce(s.NEWS_UNCERTAINTY_FLAG, false), abs(s.NEWS_UNCERTAINTY_HIGH), 0.0)
                        - (s.NEWS_EVENT_RISK_PROXY * abs(s.NEWS_EVENT_RISK_HIGH))
                    ),
                    0.0
                ),
                -abs(s.NEWS_SCORE_MAX_ABS)
            ),
            abs(s.NEWS_SCORE_MAX_ABS)
        ) as NEWS_SCORE_ADJ,
        s.SCORE + least(
            greatest(
                iff(
                    s.NEWS_ENABLED = 'true'
                    and s.NEWS_INFLUENCE_ENABLED = 'true'
                    and s.NEWS_DISPLAY_ONLY <> 'true',
                    s.NEWS_RECENCY_WEIGHT
                    * (
                        s.NEWS_PRESSURE_SCORE * abs(s.NEWS_PRESSURE_HOT)
                        - iff(coalesce(s.NEWS_UNCERTAINTY_FLAG, false), abs(s.NEWS_UNCERTAINTY_HIGH), 0.0)
                        - (s.NEWS_EVENT_RISK_PROXY * abs(s.NEWS_EVENT_RISK_HIGH))
                    ),
                    0.0
                ),
                -abs(s.NEWS_SCORE_MAX_ABS)
            ),
            abs(s.NEWS_SCORE_MAX_ABS)
        ) as FINAL_SCORE
    from scored s
),
checks as (
    select
        'CANDIDATES_AVAILABLE_FOR_VALIDATION' as check_name,
        iff(count(*) > 0, 'PASS', 'FAIL') as status,
        count(*)::string as observed,
        '> 0 candidate rows' as expected
    from final_scored

    union all

    select
        'NEWS_ADJ_IS_BOUNDED',
        iff(
            coalesce(
                count_if(
                    abs(coalesce(NEWS_SCORE_ADJ, 0))
                    > coalesce(NEWS_SCORE_MAX_ABS, 0.20) + 1e-9
                ),
                0
            ) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(
            count_if(
                abs(coalesce(NEWS_SCORE_ADJ, 0))
                > coalesce(NEWS_SCORE_MAX_ABS, 0.20) + 1e-9
            ),
            0
        )::string,
        '0 rows exceeding max absolute news score adjustment'
    from final_scored

    union all

    select
        'FINAL_SCORE_EQUALS_BASE_PLUS_ADJ',
        iff(
            coalesce(
                count_if(
                    abs(
                        coalesce(FINAL_SCORE, 0)
                        - (
                            coalesce(SCORE, 0)
                            + coalesce(NEWS_SCORE_ADJ, 0)
                        )
                    ) > 1e-6
                ),
                0
            ) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(
            count_if(
                abs(
                    coalesce(FINAL_SCORE, 0)
                    - (
                        coalesce(SCORE, 0)
                        + coalesce(NEWS_SCORE_ADJ, 0)
                    )
                ) > 1e-6
            ),
            0
        )::string,
        '0 rows where final_score != base_score + news_score_adj'
    from final_scored

    union all

    select
        'NEWS_INFLUENCE_GATED_BY_FLAGS',
        iff(
            coalesce(
                count_if(
                    (NEWS_ENABLED <> 'true' or NEWS_INFLUENCE_ENABLED <> 'true' or NEWS_DISPLAY_ONLY = 'true')
                    and abs(coalesce(NEWS_SCORE_ADJ, 0)) > 1e-9
                ),
                0
            ) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(
            count_if(
                (NEWS_ENABLED <> 'true' or NEWS_INFLUENCE_ENABLED <> 'true' or NEWS_DISPLAY_ONLY = 'true')
                and abs(coalesce(NEWS_SCORE_ADJ, 0)) > 1e-9
            ),
            0
        )::string,
        '0 rows with non-zero adjustment when influence gates are off'
    from final_scored
)
select *
from checks
order by check_name;
