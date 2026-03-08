use role MIP_ADMIN_ROLE;
use database MIP;

select
    'NEWS_POLICY_COLUMNS_QUERYABLE' as check_name,
    count(*) as cnt
from MIP.LIVE.LIVE_ACTIONS
where NEWS_CONTEXT_POLICY_VERSION is not null
   or NEWS_CONTEXT_STATE is not null
   or NEWS_FRESHNESS_BUCKET is not null;

select
    'NEWS_EXECUTION_REASON_CODES_30D' as check_name,
    count(*) as cnt
from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where EVENT_TS >= dateadd(day, -30, current_timestamp())
  and EVENT_NAME in ('LIVE_REVALIDATION', 'LIVE_EXECUTION_BLOCKED', 'LIVE_EXECUTION_REQUESTED')
  and (
      INFLUENCE_DELTA:news_context_state is not null
      or OUTCOME_STATE:news_context_snapshot is not null
  );

select
    'NEWS_MONITORING_ESCALATION_30D' as check_name,
    count(*) as cnt
from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where EVENT_TS >= dateadd(day, -30, current_timestamp())
  and EVENT_NAME = 'LIVE_EARLY_EXIT_MONITOR_RUN'
  and (
      INFLUENCE_DELTA:news_monitoring_escalation is not null
      or OUTCOME_STATE:news_monitoring is not null
  );

