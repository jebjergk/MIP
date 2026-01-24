-- 189_sp_validate_and_execute_proposals.sql
-- Purpose: Validate proposals, apply constraints, and execute paper trades

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_VALIDATE_AND_EXECUTE_PROPOSALS(
    P_PORTFOLIO_ID number,
    P_RUN_ID number
)
returns variant
language sql
execute as caller
as
$$
declare
    v_profile variant;
    v_profile_id number;
    v_max_positions number;
    v_max_position_pct float;
    v_total_equity number(18,2);
    v_rejected_count number := 0;
    v_approved_count number := 0;
    v_executed_count number := 0;
    v_proposal_count number := 0;
    v_validation_counts variant;
begin
    v_profile := (
        select object_construct(
            'profile_id', p.PROFILE_ID,
            'max_positions', prof.MAX_POSITIONS,
            'max_position_pct', prof.MAX_POSITION_PCT
        )
        from MIP.APP.PORTFOLIO p
        left join MIP.APP.PORTFOLIO_PROFILE prof
          on prof.PROFILE_ID = p.PROFILE_ID
       where p.PORTFOLIO_ID = :P_PORTFOLIO_ID
    );

    v_profile_id := v_profile:profile_id::number;
    v_max_positions := v_profile:max_positions::number;
    v_max_position_pct := v_profile:max_position_pct::float;

    if (v_profile_id is null) then
        return object_construct(
            'status', 'ERROR',
            'message', 'Portfolio not found',
            'portfolio_id', :P_PORTFOLIO_ID
        );
    end if;

    v_max_positions := coalesce(v_max_positions, 5);
    v_max_position_pct := coalesce(v_max_position_pct, 1.0);

    create or replace temporary table TMP_PROPOSAL_VALIDATION as
    with latest_prices as (
        select
            SYMBOL,
            MARKET_TYPE,
            CLOSE,
            row_number() over (
                partition by SYMBOL, MARKET_TYPE
                order by TS desc
            ) as rn
        from MIP.MART.MARKET_BARS
        where INTERVAL_MINUTES = 1440
    ),
    proposals as (
        select
            p.*,
            row_number() over (
                order by p.PROPOSED_AT, p.PROPOSAL_ID
            ) as proposal_rank
        from MIP.AGENT_OUT.ORDER_PROPOSALS p
        where p.RUN_ID = :P_RUN_ID
          and p.PORTFOLIO_ID = :P_PORTFOLIO_ID
          and p.STATUS = 'PROPOSED'
    )
    select
        p.PROPOSAL_ID,
        p.proposal_rank,
        p.TARGET_WEIGHT,
        p.SYMBOL,
        p.MARKET_TYPE,
        p.INTERVAL_MINUTES,
        p.SOURCE_SIGNALS,
        v.SYMBOL as eligible_symbol,
        v.IS_ELIGIBLE as eligible_flag,
        lp.CLOSE as latest_price,
        array_construct_compact(
            iff(p.RECOMMENDATION_ID is null, 'MISSING_RECOMMENDATION_ID', null),
            iff(p.RECOMMENDATION_ID is not null and v.RECOMMENDATION_ID is null, 'NO_SIGNAL_MATCH', null),
            iff(v.RECOMMENDATION_ID is not null and not v.IS_ELIGIBLE, 'INELIGIBLE_SIGNAL', null),
            iff(p.TARGET_WEIGHT > :v_max_position_pct, 'EXCEEDS_MAX_POSITION_PCT', null),
            iff(p.proposal_rank > :v_max_positions, 'EXCEEDS_MAX_POSITIONS', null),
            iff(lp.CLOSE is null, 'MISSING_PRICE', null)
        ) as validation_errors
    from proposals p
    left join MIP.APP.V_SIGNALS_ELIGIBLE_TODAY v
      on v.RECOMMENDATION_ID = p.RECOMMENDATION_ID
    left join latest_prices lp
      on lp.SYMBOL = p.SYMBOL
     and lp.MARKET_TYPE = p.MARKET_TYPE
     and lp.rn = 1;

    v_proposal_count := coalesce(
        (select count(*) from TMP_PROPOSAL_VALIDATION),
        0
    );

    v_validation_counts := (
        select object_construct(
            'rejected', count_if(array_size(validation_errors) > 0),
            'approved', count_if(array_size(validation_errors) = 0)
        )
        from TMP_PROPOSAL_VALIDATION
    );

    v_rejected_count := coalesce(v_validation_counts:rejected::number, 0);
    v_approved_count := coalesce(v_validation_counts:approved::number, 0);

    update MIP.AGENT_OUT.ORDER_PROPOSALS as p
       set STATUS = 'REJECTED',
           VALIDATION_ERRORS = v.validation_errors
      from TMP_PROPOSAL_VALIDATION v
     where p.PROPOSAL_ID = v.PROPOSAL_ID
       and array_size(v.validation_errors) > 0;

    update MIP.AGENT_OUT.ORDER_PROPOSALS as p
       set STATUS = 'APPROVED',
           APPROVED_AT = current_timestamp(),
           VALIDATION_ERRORS = null
      from TMP_PROPOSAL_VALIDATION v
     where p.PROPOSAL_ID = v.PROPOSAL_ID
       and array_size(v.validation_errors) = 0;

    v_total_equity := coalesce(
        (
            select TOTAL_EQUITY
              from MIP.APP.PORTFOLIO_DAILY
             where PORTFOLIO_ID = :P_PORTFOLIO_ID
             order by TS desc
             limit 1
        ),
        (
            select STARTING_CASH
              from MIP.APP.PORTFOLIO
             where PORTFOLIO_ID = :P_PORTFOLIO_ID
        )
    );

    merge into MIP.APP.PORTFOLIO_TRADES as target
    using (
        select
            p.PROPOSAL_ID,
            :P_PORTFOLIO_ID as PORTFOLIO_ID,
            to_varchar(:P_RUN_ID) as RUN_ID,
            p.SYMBOL,
            p.MARKET_TYPE,
            1440 as INTERVAL_MINUTES,
            current_timestamp() as TRADE_TS,
            p.SIDE,
            lp.CLOSE as PRICE,
            iff(lp.CLOSE is null or lp.CLOSE = 0, null, (:v_total_equity * p.TARGET_WEIGHT) / lp.CLOSE) as QUANTITY,
            :v_total_equity * p.TARGET_WEIGHT as NOTIONAL,
            null as REALIZED_PNL,
            :v_total_equity as CASH_AFTER,
            p.SOURCE_SIGNALS:score::number as SCORE
        from MIP.AGENT_OUT.ORDER_PROPOSALS p
        join TMP_PROPOSAL_VALIDATION v
          on v.PROPOSAL_ID = p.PROPOSAL_ID
        join (
            select SYMBOL, MARKET_TYPE, CLOSE
            from (
                select
                    SYMBOL,
                    MARKET_TYPE,
                    CLOSE,
                    row_number() over (
                        partition by SYMBOL, MARKET_TYPE
                        order by TS desc
                    ) as rn
                from MIP.MART.MARKET_BARS
                where INTERVAL_MINUTES = 1440
            )
            where rn = 1
        ) lp
          on lp.SYMBOL = p.SYMBOL
         and lp.MARKET_TYPE = p.MARKET_TYPE
        where p.RUN_ID = :P_RUN_ID
          and p.PORTFOLIO_ID = :P_PORTFOLIO_ID
          and p.STATUS = 'APPROVED'
    ) as source
    on target.PORTFOLIO_ID = source.PORTFOLIO_ID
       and target.PROPOSAL_ID = source.PROPOSAL_ID
    when not matched then
        insert (
            PROPOSAL_ID,
            PORTFOLIO_ID,
            RUN_ID,
            SYMBOL,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            TRADE_TS,
            SIDE,
            PRICE,
            QUANTITY,
            NOTIONAL,
            REALIZED_PNL,
            CASH_AFTER,
            SCORE
        )
        values (
            source.PROPOSAL_ID,
            source.PORTFOLIO_ID,
            source.RUN_ID,
            source.SYMBOL,
            source.MARKET_TYPE,
            source.INTERVAL_MINUTES,
            source.TRADE_TS,
            source.SIDE,
            source.PRICE,
            source.QUANTITY,
            source.NOTIONAL,
            source.REALIZED_PNL,
            source.CASH_AFTER,
            source.SCORE
        );

    v_executed_count := v_approved_count;

    update MIP.AGENT_OUT.ORDER_PROPOSALS
       set STATUS = 'EXECUTED',
           EXECUTED_AT = current_timestamp()
     where RUN_ID = :P_RUN_ID
       and PORTFOLIO_ID = :P_PORTFOLIO_ID
       and STATUS = 'APPROVED';

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :P_RUN_ID,
        'portfolio_id', :P_PORTFOLIO_ID,
        'proposal_count', :v_proposal_count,
        'approved_count', :v_approved_count,
        'rejected_count', :v_rejected_count,
        'executed_count', :v_executed_count
    );
end;
$$;
