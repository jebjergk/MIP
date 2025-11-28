-- 130_sp_run_mip_learning_cycle.sql
-- Purpose: Orchestrate the full MIP learning cycle across ingest, signals, evaluation, backtest, and training

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_MIP_LEARNING_CYCLE(
    P_MARKET_TYPE        string,
    P_INTERVAL_MINUTES   number,
    P_HORIZON_MINUTES    number,
    P_MIN_RETURN         number,
    P_HIT_THRESHOLD      number,
    P_MISS_THRESHOLD     number,
    P_FROM_TS            timestamp_ntz,
    P_TO_TS              timestamp_ntz,
    P_DO_INGEST          boolean,
    P_DO_SIGNALS         boolean,
    P_DO_EVALUATE        boolean,
    P_DO_BACKTEST        boolean,
    P_DO_TRAIN           boolean
)
returns variant
language sql
as
$$
declare
    v_from_ts          timestamp_ntz;
    v_to_ts            timestamp_ntz;
    v_backtest_run_id  number;
    v_msg_ingest       varchar;
    v_msg_signals      varchar;
    v_msg_eval         varchar;
    v_msg_backtest     varchar;
    v_msg_train        varchar;
    v_do_ingest        boolean;
    v_do_signals       boolean;
    v_do_evaluate      boolean;
    v_do_backtest      boolean;
    v_do_train         boolean;
begin
    v_to_ts   := coalesce(P_TO_TS, current_timestamp());
    v_from_ts := coalesce(P_FROM_TS, dateadd('day', -7, v_to_ts));

    v_do_ingest   := coalesce(P_DO_INGEST, true);
    v_do_signals  := coalesce(P_DO_SIGNALS, true);
    v_do_evaluate := coalesce(P_DO_EVALUATE, true);
    v_do_backtest := coalesce(P_DO_BACKTEST, true);
    v_do_train    := coalesce(P_DO_TRAIN, true);

    if (v_do_ingest) then
        call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS();
        -- select RESULT into v_msg_ingest from table(MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS());
    end if;

    if (v_do_signals) then
        call MIP.APP.SP_GENERATE_MOMENTUM_RECS(
            P_MIN_RETURN,
            P_MARKET_TYPE,
            P_INTERVAL_MINUTES
        );
    end if;

    if (v_do_evaluate) then
        call MIP.APP.SP_EVALUATE_MOMENTUM_OUTCOMES(
            P_HORIZON_MINUTES,
            P_HIT_THRESHOLD,
            P_MISS_THRESHOLD,
            P_MARKET_TYPE,
            P_INTERVAL_MINUTES
        );
    end if;

    if (v_do_backtest) then
        call MIP.APP.SP_RUN_BACKTEST(
            P_HORIZON_MINUTES,
            P_HIT_THRESHOLD,
            P_MISS_THRESHOLD,
            v_from_ts,
            v_to_ts,
            P_MARKET_TYPE,
            P_INTERVAL_MINUTES
        );

        select max(BACKTEST_RUN_ID)
          into v_backtest_run_id
          from MIP.APP.BACKTEST_RUN
         where MARKET_TYPE = P_MARKET_TYPE
           and INTERVAL_MINUTES = P_INTERVAL_MINUTES;
    else
        v_backtest_run_id := null;
    end if;

    if (v_do_train) then
        call MIP.APP.SP_TRAIN_PATTERNS_FROM_BACKTEST(
            v_backtest_run_id,
            P_MARKET_TYPE,
            P_INTERVAL_MINUTES
        );
    end if;

    return object_construct(
        'market_type',       P_MARKET_TYPE,
        'interval_minutes',  P_INTERVAL_MINUTES,
        'horizon_minutes',   P_HORIZON_MINUTES,
        'from_ts',           v_from_ts,
        'to_ts',             v_to_ts,
        'backtest_run_id',   v_backtest_run_id,
        'did_ingest',        v_do_ingest,
        'did_signals',       v_do_signals,
        'did_evaluate',      v_do_evaluate,
        'did_backtest',      v_do_backtest,
        'did_train',         v_do_train,
        'msg_ingest',        v_msg_ingest,
        'msg_signals',       v_msg_signals,
        'msg_evaluate',      v_msg_eval,
        'msg_backtest',      v_msg_backtest,
        'msg_train',         v_msg_train
    );
end;
$$;
