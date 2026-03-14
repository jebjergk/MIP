-- 408_sp_refresh_pw_symbol_shadow_trades.sql
-- Purpose: Maintain background shadow trades for symbol-oriented proposals
-- (non-pattern proposals) so Parallel Worlds can continuously learn from
-- mark-to-market outcomes even when a pattern-linked recommendation is absent.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.PW_SYMBOL_SHADOW_TRADE (
    SHADOW_TRADE_ID         number autoincrement,
    PROPOSAL_ID             number not null,
    RUN_ID_VARCHAR          varchar(64),
    PORTFOLIO_ID            number not null,
    SYMBOL                  varchar not null,
    MARKET_TYPE             varchar,
    SIDE                    varchar not null,
    PROPOSAL_STATUS         varchar,
    ENTRY_TS                timestamp_ntz not null,
    ENTRY_PRICE             number(18,8),
    ENTRY_PRICE_TS          timestamp_ntz,
    TARGET_WEIGHT           number(18,8),
    EST_NOTIONAL            number(18,4),
    STATUS                  varchar default 'OPEN', -- OPEN | CLOSED
    LAST_MARK_TS            timestamp_ntz,
    LAST_MARK_PRICE         number(18,8),
    LAST_MARK_RETURN_PCT    number(18,8),
    CLOSED_TS               timestamp_ntz,
    CLOSED_PRICE            number(18,8),
    REALIZED_RETURN_PCT     number(18,8),
    DETAILS                 variant,
    CREATED_AT              timestamp_ntz default current_timestamp(),
    UPDATED_AT              timestamp_ntz default current_timestamp(),
    constraint PK_PW_SYMBOL_SHADOW_TRADE primary key (SHADOW_TRADE_ID),
    constraint UQ_PW_SYMBOL_SHADOW_TRADE_PROPOSAL unique (PROPOSAL_ID)
);

create or replace procedure MIP.APP.SP_REFRESH_PW_SYMBOL_SHADOW_TRADES(
    P_RUN_ID varchar,
    P_AS_OF_TS timestamp_ntz
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id varchar := :P_RUN_ID;
    v_as_of_ts timestamp_ntz := :P_AS_OF_TS;
    v_upsert_count number := 0;
    v_mark_count number := 0;
    v_close_count number := 0;
begin
    if (:v_run_id is null) then
        return object_construct(
            'status', 'SKIPPED_NO_RUN_ID',
            'inserted_count', 0,
            'marked_count', 0,
            'closed_count', 0
        );
    end if;

    merge into MIP.APP.PW_SYMBOL_SHADOW_TRADE as target
    using (
        with latest_px as (
            select
                SYMBOL,
                MARKET_TYPE,
                TS,
                CLOSE
            from MIP.MART.MARKET_BARS
            where INTERVAL_MINUTES = 1440
              and TS <= :v_as_of_ts
            qualify row_number() over (
                partition by SYMBOL, MARKET_TYPE
                order by TS desc
            ) = 1
        )
        select
            op.PROPOSAL_ID,
            op.RUN_ID_VARCHAR,
            op.PORTFOLIO_ID,
            op.SYMBOL,
            op.MARKET_TYPE,
            upper(coalesce(op.SIDE, 'BUY')) as SIDE,
            op.STATUS as PROPOSAL_STATUS,
            coalesce(op.EXECUTED_AT, op.APPROVED_AT, op.PROPOSED_AT, :v_as_of_ts) as ENTRY_TS,
            coalesce(
                px.CLOSE,
                try_to_double(op.SOURCE_SIGNALS:entry_price::string),
                try_to_double(op.SOURCE_SIGNALS:price::string)
            ) as ENTRY_PRICE,
            px.TS as ENTRY_PRICE_TS,
            coalesce(op.TARGET_WEIGHT, 0.05) as TARGET_WEIGHT,
            coalesce(p.STARTING_CASH, 100000) * coalesce(op.TARGET_WEIGHT, 0.05) as EST_NOTIONAL,
            object_construct(
                'source', 'ORDER_PROPOSALS',
                'source_kind', 'SYMBOL_ORIENTED_NON_PATTERN',
                'run_id', op.RUN_ID_VARCHAR,
                'source_signals', op.SOURCE_SIGNALS,
                'rationale', op.RATIONALE
            ) as DETAILS
        from MIP.AGENT_OUT.ORDER_PROPOSALS op
        left join MIP.APP.PORTFOLIO p
          on p.PORTFOLIO_ID = op.PORTFOLIO_ID
        left join latest_px px
          on px.SYMBOL = op.SYMBOL
         and px.MARKET_TYPE = op.MARKET_TYPE
        where op.RUN_ID_VARCHAR = :v_run_id
          and op.SYMBOL is not null
          and upper(coalesce(op.SIDE, '')) in ('BUY', 'SELL')
          and op.STATUS in ('PROPOSED', 'APPROVED', 'EXECUTED')
          and op.SIGNAL_PATTERN_ID is null
    ) as source
    on target.PROPOSAL_ID = source.PROPOSAL_ID
    when matched then update set
        target.RUN_ID_VARCHAR = source.RUN_ID_VARCHAR,
        target.PROPOSAL_STATUS = source.PROPOSAL_STATUS,
        target.TARGET_WEIGHT = source.TARGET_WEIGHT,
        target.EST_NOTIONAL = source.EST_NOTIONAL,
        target.ENTRY_PRICE = coalesce(target.ENTRY_PRICE, source.ENTRY_PRICE),
        target.ENTRY_PRICE_TS = coalesce(target.ENTRY_PRICE_TS, source.ENTRY_PRICE_TS),
        target.DETAILS = source.DETAILS,
        target.UPDATED_AT = current_timestamp()
    when not matched then insert (
        PROPOSAL_ID,
        RUN_ID_VARCHAR,
        PORTFOLIO_ID,
        SYMBOL,
        MARKET_TYPE,
        SIDE,
        PROPOSAL_STATUS,
        ENTRY_TS,
        ENTRY_PRICE,
        ENTRY_PRICE_TS,
        TARGET_WEIGHT,
        EST_NOTIONAL,
        STATUS,
        DETAILS,
        CREATED_AT,
        UPDATED_AT
    ) values (
        source.PROPOSAL_ID,
        source.RUN_ID_VARCHAR,
        source.PORTFOLIO_ID,
        source.SYMBOL,
        source.MARKET_TYPE,
        source.SIDE,
        source.PROPOSAL_STATUS,
        source.ENTRY_TS,
        source.ENTRY_PRICE,
        source.ENTRY_PRICE_TS,
        source.TARGET_WEIGHT,
        source.EST_NOTIONAL,
        'OPEN',
        source.DETAILS,
        current_timestamp(),
        current_timestamp()
    );

    v_upsert_count := SQLROWCOUNT;

    update MIP.APP.PW_SYMBOL_SHADOW_TRADE as t
       set LAST_MARK_TS = px.TS,
           LAST_MARK_PRICE = px.CLOSE,
           LAST_MARK_RETURN_PCT = iff(
               t.ENTRY_PRICE is null or t.ENTRY_PRICE = 0 or px.CLOSE is null,
               null,
               iff(
                   upper(coalesce(t.SIDE, 'BUY')) = 'SELL',
                   (t.ENTRY_PRICE - px.CLOSE) / t.ENTRY_PRICE,
                   (px.CLOSE - t.ENTRY_PRICE) / t.ENTRY_PRICE
               )
           ),
           UPDATED_AT = current_timestamp()
      from (
          select
              SYMBOL,
              MARKET_TYPE,
              TS,
              CLOSE
          from MIP.MART.MARKET_BARS
          where INTERVAL_MINUTES = 1440
            and TS <= :v_as_of_ts
          qualify row_number() over (
              partition by SYMBOL, MARKET_TYPE
              order by TS desc
          ) = 1
      ) px
     where t.SYMBOL = px.SYMBOL
       and t.MARKET_TYPE = px.MARKET_TYPE
       and t.STATUS = 'OPEN';

    v_mark_count := SQLROWCOUNT;

    update MIP.APP.PW_SYMBOL_SHADOW_TRADE
       set STATUS = 'CLOSED',
           CLOSED_TS = :v_as_of_ts,
           CLOSED_PRICE = LAST_MARK_PRICE,
           REALIZED_RETURN_PCT = LAST_MARK_RETURN_PCT,
           UPDATED_AT = current_timestamp()
     where STATUS = 'OPEN'
       and datediff(
           day,
           ENTRY_TS::date,
           :v_as_of_ts::date
       ) >= coalesce(try_to_number(DETAILS:rationale:sim_committee:hold_bars::string), 5);

    v_close_count := SQLROWCOUNT;

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'as_of_ts', :v_as_of_ts::string,
        'upsert_count', :v_upsert_count,
        'marked_count', :v_mark_count,
        'closed_count', :v_close_count
    );
end;
$$;

