-- 140_sp_run_daily_training.sql
-- Purpose: Orchestrate daily training loop for recommendation outcomes and KPIs

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_DAILY_TRAINING()
returns variant
language sql
execute as caller
as
$$
declare
    v_from_ts          timestamp_ntz := dateadd(day, -90, current_date());
    v_to_ts            timestamp_ntz := current_timestamp();
    v_return_rows      number := 0;
    v_kpi_rows         number := 0;
    v_msg_ingest       string := 'Skipped ingestion (run separately).';
    v_msg_returns      string;
    v_msg_signals      string;
    v_msg_eval         string;
begin
    select count(*)
      into :v_return_rows
      from MIP.MART.MARKET_RETURNS;

    v_msg_returns := 'Market returns view checked (' || v_return_rows || ' rows).';

    call MIP.APP.SP_GENERATE_MOMENTUM_RECS(null, null, null, null, null);
    v_msg_signals := 'Momentum recommendations generated for enabled patterns.';

    call MIP.APP.SP_EVALUATE_RECOMMENDATIONS(:v_from_ts, :v_to_ts);
    v_msg_eval := 'Recommendation outcomes evaluated for trading-day horizons.';

    select count(*)
      into :v_kpi_rows
      from MIP.APP.V_PATTERN_KPIS;

    return object_construct(
        'from_ts', v_from_ts,
        'to_ts', v_to_ts,
        'market_returns_rows', v_return_rows,
        'kpi_rows', v_kpi_rows,
        'msg_ingest', v_msg_ingest,
        'msg_returns', v_msg_returns,
        'msg_signals', v_msg_signals,
        'msg_evaluate', v_msg_eval
    );
end;
$$;
