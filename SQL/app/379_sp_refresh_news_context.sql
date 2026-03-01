-- 379_sp_refresh_news_context.sql
-- Purpose: One-call refresh for decision-time news context.
-- Runs ingestion, symbol mapping, then info-state compute.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.NEWS.SP_REFRESH_NEWS_CONTEXT(
    P_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, replace(uuid_string(), '-', ''));
    v_ingest variant;
    v_map variant;
    v_compute variant;
begin
    v_ingest := (call MIP.NEWS.SP_INGEST_RSS_NEWS(false, null));
    v_map := (call MIP.NEWS.SP_MAP_NEWS_SYMBOLS(null));
    v_compute := (call MIP.NEWS.SP_COMPUTE_INFO_STATE_DAILY(current_timestamp(), :v_run_id));

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'ingest', :v_ingest,
        'map', :v_map,
        'compute', :v_compute
    );
end;
$$;
