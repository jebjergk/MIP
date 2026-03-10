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
    v_event_run_id string;
    v_ingest variant;
    v_ingest_status string;
    v_ingest_rows_staged number := 0;
    v_ingest_rows_inserted number := 0;
    v_ingest_error_count number := 0;
    v_ingest_errors variant;
    v_seed_subs variant;
    v_seed_status string;
    v_seed_subs_total number := 0;
    v_seed_subs_enabled number := 0;
    v_map variant;
    v_map_status string;
    v_map_rows_merged number := 0;
    v_compute variant;
    v_compute_status string;
    v_compute_rows_written number := 0;
    v_agg variant;
    v_agg_status string;
    v_agg_rows_written number := 0;
begin
    v_event_run_id := (
        call MIP.APP.SP_LOG_EVENT(
            'NEWS_PIPELINE',
            'SP_REFRESH_NEWS_CONTEXT',
            'START',
            null,
            object_construct('run_id', :v_run_id),
            null,
            :v_run_id,
            null
        )
    );

    v_ingest := (call MIP.NEWS.SP_INGEST_RSS_NEWS(false, null));
    v_ingest_status := coalesce(v_ingest:"status"::string, 'UNKNOWN');
    v_ingest_rows_staged := coalesce(v_ingest:"rows_staged"::number, 0);
    v_ingest_rows_inserted := coalesce(v_ingest:"rows_inserted"::number, 0);
    v_ingest_error_count := coalesce(array_size(v_ingest:"errors"), 0);
    v_ingest_errors := coalesce(v_ingest:"errors", array_construct());

    call MIP.APP.SP_LOG_EVENT(
        'NEWS_PIPELINE',
        'NEWS_INGEST',
        iff(:v_ingest_status = 'SUCCESS', 'SUCCESS', 'WARN'),
        :v_ingest_rows_inserted,
        object_construct(
            'run_id', :v_run_id,
            'ingest_status', :v_ingest_status,
            'rows_staged', :v_ingest_rows_staged,
            'rows_inserted', :v_ingest_rows_inserted,
            'error_count', :v_ingest_error_count,
            'errors', :v_ingest_errors
        ),
        iff(:v_ingest_error_count > 0, 'One or more sources failed during ingest.', null),
        :v_run_id,
        :v_event_run_id
    );

    v_seed_subs := (call MIP.NEWS.SP_SEED_NEWS_SOURCE_SUBSCRIPTIONS_FROM_UNIVERSE());
    v_seed_status := coalesce(v_seed_subs:"status"::string, 'UNKNOWN');
    v_seed_subs_total := coalesce(v_seed_subs:"subscriptions_total"::number, 0);
    v_seed_subs_enabled := coalesce(v_seed_subs:"subscriptions_enabled"::number, 0);

    call MIP.APP.SP_LOG_EVENT(
        'NEWS_PIPELINE',
        'NEWS_SUBSCRIPTION_SEED',
        iff(:v_seed_status = 'SUCCESS', 'SUCCESS', 'WARN'),
        :v_seed_subs_enabled,
        object_construct(
            'run_id', :v_run_id,
            'seed_status', :v_seed_status,
            'subscriptions_total', :v_seed_subs_total,
            'subscriptions_enabled', :v_seed_subs_enabled
        ),
        null,
        :v_run_id,
        :v_event_run_id
    );

    v_map := (call MIP.NEWS.SP_MAP_NEWS_SYMBOLS(null));
    v_map_status := coalesce(v_map:"status"::string, 'UNKNOWN');
    v_map_rows_merged := coalesce(v_map:"mapped_rows_merged"::number, 0);

    call MIP.APP.SP_LOG_EVENT(
        'NEWS_PIPELINE',
        'NEWS_MAP',
        iff(:v_map_status = 'SUCCESS', 'SUCCESS', 'WARN'),
        :v_map_rows_merged,
        object_construct(
            'run_id', :v_run_id,
            'map_status', :v_map_status,
            'mapped_rows_merged', :v_map_rows_merged
        ),
        null,
        :v_run_id,
        :v_event_run_id
    );

    v_compute := (call MIP.NEWS.SP_COMPUTE_INFO_STATE_DAILY(current_timestamp(), :v_run_id));
    v_compute_status := coalesce(v_compute:"status"::string, 'UNKNOWN');
    v_compute_rows_written := coalesce(v_compute:"rows_written"::number, 0);

    call MIP.APP.SP_LOG_EVENT(
        'NEWS_PIPELINE',
        'NEWS_COMPUTE',
        iff(:v_compute_status = 'SUCCESS', 'SUCCESS', 'WARN'),
        :v_compute_rows_written,
        object_construct(
            'run_id', :v_run_id,
            'compute_status', :v_compute_status,
            'rows_written', :v_compute_rows_written
        ),
        null,
        :v_run_id,
        :v_event_run_id
    );

    v_agg := (call MIP.NEWS.SP_AGGREGATE_NEWS_EVENTS(current_timestamp(), :v_run_id));
    v_agg_status := coalesce(v_agg:"status"::string, 'UNKNOWN');
    v_agg_rows_written := coalesce(v_agg:"rows_written"::number, 0);

    call MIP.APP.SP_LOG_EVENT(
        'NEWS_PIPELINE',
        'NEWS_AGGREGATE',
        iff(:v_agg_status = 'SUCCESS', 'SUCCESS', 'WARN'),
        :v_agg_rows_written,
        object_construct(
            'run_id', :v_run_id,
            'aggregate_status', :v_agg_status,
            'rows_written', :v_agg_rows_written
        ),
        null,
        :v_run_id,
        :v_event_run_id
    );

    if (v_ingest_rows_staged = 0 and v_ingest_error_count > 0) then
        call MIP.APP.SP_LOG_EVENT(
            'NEWS_PIPELINE',
            'SP_REFRESH_NEWS_CONTEXT',
            'WARN',
            0,
            object_construct(
                'run_id', :v_run_id,
                'ingest_status', :v_ingest_status,
                'ingest_error_count', :v_ingest_error_count,
                'errors', :v_ingest_errors
            ),
            'All sources failed during ingestion; keeping prior news snapshot and avoiding task auto-suspend.',
            :v_run_id,
            :v_event_run_id
        );
    end if;

    call MIP.APP.SP_LOG_EVENT(
        'NEWS_PIPELINE',
        'SP_REFRESH_NEWS_CONTEXT',
        'SUCCESS',
        :v_compute_rows_written,
        object_construct(
            'run_id', :v_run_id,
            'ingest_status', :v_ingest_status,
            'map_status', :v_map_status,
            'compute_status', :v_compute_status
            ,
            'aggregate_status', :v_agg_status
        ),
        null,
        :v_run_id,
        :v_event_run_id
    );

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'ingest', :v_ingest,
        'seed_subscriptions', :v_seed_subs,
        'map', :v_map,
        'compute', :v_compute,
        'aggregate', :v_agg
    );
end;
$$;
