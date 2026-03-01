-- 242_news_phase5_decision_context_checks.sql
-- Purpose: Phase 5 checks for decision-time news context wiring in proposals.

use role MIP_ADMIN_ROLE;
use database MIP;

set phase5_portfolio_id = (
    select min(PORTFOLIO_ID)
    from MIP.APP.PORTFOLIO
    where STATUS = 'ACTIVE'
);

set phase5_run_id = (
    select 'NEWS_PHASE5_' || to_char(current_timestamp(), 'YYYYMMDDHH24MISS')
);

call MIP.APP.SP_AGENT_PROPOSE_TRADES($phase5_portfolio_id, $phase5_run_id, null);

with scope as (
    select *
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where RUN_ID_VARCHAR = $phase5_run_id
      and PORTFOLIO_ID = $phase5_portfolio_id
),
checks as (
    select
        'PROPOSALS_CREATED_OR_EMPTY' as check_name,
        'PASS' as status,
        count(*)::string as observed,
        '0+ proposal rows for this test run' as expected
    from scope

    union all

    select
        'NEWS_FIELDS_PRESENT_WHEN_CONTEXT_PRESENT',
        iff(
            coalesce(count_if(
                SOURCE_SIGNALS:news_context is not null
                and (
                    SOURCE_SIGNALS:news_snapshot_age_minutes is null
                    or SOURCE_SIGNALS:news_is_stale is null
                )
            ), 0) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(count_if(
            SOURCE_SIGNALS:news_context is not null
            and (
                SOURCE_SIGNALS:news_snapshot_age_minutes is null
                or SOURCE_SIGNALS:news_is_stale is null
            )
        ), 0)::string,
        '0 rows missing staleness fields when news_context exists'
    from scope

    union all

    select
        'AS_OF_JOIN_CONTRACT_SNAPSHOT_LE_SIGNAL_TS',
        iff(
            coalesce(count_if(
                SOURCE_SIGNALS:news_context is not null
                and try_to_timestamp_ntz(SOURCE_SIGNALS:news_context:snapshot_ts::string) > SIGNAL_TS
            ), 0) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(count_if(
            SOURCE_SIGNALS:news_context is not null
            and try_to_timestamp_ntz(SOURCE_SIGNALS:news_context:snapshot_ts::string) > SIGNAL_TS
        ), 0)::string,
        '0 rows with news snapshot newer than signal_ts'
    from scope

    union all

    select
        'DISPLAY_ONLY_NO_DIRECTIONAL_OVERRIDE_FIELDS',
        iff(
            coalesce(count_if(
                SOURCE_SIGNALS:news_directional_override is not null
                or RATIONALE:news_directional_override is not null
            ), 0) = 0,
            'PASS',
            'FAIL'
        ),
        coalesce(count_if(
            SOURCE_SIGNALS:news_directional_override is not null
            or RATIONALE:news_directional_override is not null
        ), 0)::string,
        '0 directional override fields present'
    from scope
)
select *
from checks
order by check_name;
