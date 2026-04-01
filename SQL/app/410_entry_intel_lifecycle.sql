-- 410_entry_intel_lifecycle.sql
-- Entry Intelligence Snapshot (EIS) + action link + trade closeout backbone.
-- Phase 2: real WORLDS_SPEC (HOD) + ALPHA_SPEC via F_BUILD_ENTRY_INTEL_FOR_PROPOSAL.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema APP;

-- Failure visibility when SP_ENSURE_ENTRY_INTEL_FOR_RUN fails inside agent propose (Phase 2).
create table if not exists MIP.APP.EIS_ENSURE_FAILURE_LOG (
    LOG_ID          varchar(36)   not null,
    RUN_ID          varchar(64),
    PORTFOLIO_ID    number(38,0),
    PROPOSAL_ID     number(38,0),
    ERROR_MESSAGE   varchar(8192),
    CREATED_TS      timestamp_ntz not null default current_timestamp(),
    constraint PK_EIS_ENSURE_FAILURE_LOG primary key (LOG_ID)
);

-- Deterministic HOD + alpha payload for one proposal (read-only; used by ensure procs and smoke).
create or replace function MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL(P_PROPOSAL_ID number)
returns variant
language sql
as
$$
    with op as (
        select
            p.proposal_id,
            p.portfolio_id,
            p.symbol,
            p.market_type,
            p.signal_pattern_id,
            coalesce(p.interval_minutes, p.signal_interval_minutes, 1440) as eff_interval,
            p.proposed_at,
            p.run_id_varchar
        from mip.agent_out.order_proposals p
        where p.proposal_id = P_PROPOSAL_ID
    ),
    hod_raw as (
        select
            o.horizon_bars,
            o.realized_return::float as rr
        from mip.app.recommendation_log r
        inner join mip.app.recommendation_outcomes o
            on o.recommendation_id = r.recommendation_id
        inner join op
            on r.symbol = op.symbol
           and r.market_type = op.market_type
           and r.interval_minutes = op.eff_interval
           and (op.signal_pattern_id is null or r.pattern_id = op.signal_pattern_id)
        where o.eval_status = 'SUCCESS'
          and o.realized_return is not null
    ),
    horizon_ranked as (
        select
            horizon_bars,
            count(*) as hz_n,
            row_number() over (order by count(*) desc, horizon_bars desc) as rn
        from hod_raw
        group by horizon_bars
    ),
    picked_h as (
        select horizon_bars from horizon_ranked where rn = 1
    ),
    hod_f as (
        select h.*
        from hod_raw h
        where exists (select 1 from picked_h ph where ph.horizon_bars = h.horizon_bars)
    ),
    agg as (
        select
            count(*) as n,
            coalesce(sum(iff(rr > 0.005, 1, 0)), 0) as n_up,
            coalesce(sum(iff(rr >= -0.005 and rr <= 0.005, 1, 0)), 0) as n_base,
            coalesce(sum(iff(rr < -0.005, 1, 0)), 0) as n_down,
            avg(iff(rr > 0.005, rr, null)) as avg_up,
            avg(iff(rr >= -0.005 and rr <= 0.005, rr, null)) as avg_base,
            avg(iff(rr < -0.005, rr, null)) as avg_down,
            avg(rr) as avg_all
        from hod_f
    ),
    j as (
        select
            op.proposal_id,
            op.portfolio_id,
            op.symbol,
            op.market_type,
            op.signal_pattern_id,
            op.eff_interval,
            op.proposed_at,
            op.run_id_varchar,
            coalesce(agg.n, 0) as n,
            agg.n_up,
            agg.n_base,
            agg.n_down,
            agg.avg_up,
            agg.avg_base,
            agg.avg_down,
            agg.avg_all,
            (select ph.horizon_bars from picked_h ph limit 1) as horizon_bars
        from op
        left join agg on 1 = 1
    ),
    calc as (
        select
            j.*,
            iff(j.n >= 12, false, true) as insufficient_sample,
            iff(j.n > 0, j.n_up::float / j.n, null) as p_up,
            iff(j.n > 0, j.n_base::float / j.n, null) as p_base,
            iff(j.n > 0, j.n_down::float / j.n, null) as p_down,
            iff(
                j.n >= 12,
                (j.n_up::float / nullif(j.n, 0)) * coalesce(j.avg_up, 0)
                + (j.n_base::float / nullif(j.n, 0)) * coalesce(j.avg_base, 0)
                + (j.n_down::float / nullif(j.n, 0)) * coalesce(j.avg_down, 0),
                null
            ) as ev_gross,
            0.002::float as cost_floor,
            iff(
                j.n >= 12,
                (j.n_up::float / nullif(j.n, 0)) * coalesce(j.avg_up, 0)
                + (j.n_base::float / nullif(j.n, 0)) * coalesce(j.avg_base, 0)
                + (j.n_down::float / nullif(j.n, 0)) * coalesce(j.avg_down, 0)
                - 0.002,
                null
            ) as ev_net,
            iff(j.n >= 60, 'HIGH', iff(j.n >= 25, 'MEDIUM', 'LOW'))::varchar as confidence_band,
            iff(
                j.n < 12,
                'HIGH'::varchar,
                iff(
                    (j.n_down::float / nullif(j.n, 0)) >= 0.35
                    or coalesce(j.avg_down, 0) < -0.015,
                    'HIGH',
                    iff(
                        (j.n_down::float / nullif(j.n, 0)) >= 0.22
                        or coalesce(j.avg_down, 0) < -0.008,
                        'MEDIUM',
                        'LOW'
                    )
                )
            ) as downside_risk_band,
            iff(j.n < 12, 'SKIP',
                iff(
                    ((j.n_up::float / nullif(j.n, 0)) * coalesce(j.avg_up, 0)
                     + (j.n_base::float / nullif(j.n, 0)) * coalesce(j.avg_base, 0)
                     + (j.n_down::float / nullif(j.n, 0)) * coalesce(j.avg_down, 0)
                     - 0.002) < 0,
                    'SKIP',
                    iff(
                        ((j.n_up::float / nullif(j.n, 0)) * coalesce(j.avg_up, 0)
                         + (j.n_base::float / nullif(j.n, 0)) * coalesce(j.avg_base, 0)
                         + (j.n_down::float / nullif(j.n, 0)) * coalesce(j.avg_down, 0)
                         - 0.002) < 0.005
                        or iff(j.n >= 60, 'HIGH', iff(j.n >= 25, 'MEDIUM', 'LOW')) = 'LOW'
                        or iff(
                            (j.n_down::float / nullif(j.n, 0)) >= 0.35
                            or coalesce(j.avg_down, 0) < -0.015,
                            'HIGH',
                            iff(
                                (j.n_down::float / nullif(j.n, 0)) >= 0.22
                                or coalesce(j.avg_down, 0) < -0.008,
                                'MEDIUM',
                                'LOW'
                            )
                        ) = 'HIGH',
                        'REDUCE',
                        'ENTER'
                    )
                )
            )::varchar as recommended_action
        from j
    ),
    sized as (
        select
            calc.*,
            iff(
                calc.recommended_action = 'SKIP',
                'XS',
                iff(
                    calc.recommended_action = 'REDUCE',
                    iff(calc.confidence_band = 'LOW', 'XS', 'S'),
                    iff(
                        calc.recommended_action = 'ENTER'
                        and calc.confidence_band = 'HIGH'
                        and calc.downside_risk_band = 'LOW',
                        'M',
                        'S'
                    )
                )
            )::varchar as recommended_size_band
        from calc
    ),
    narr as (
        select
            sized.*,
            concat(
                'EIS v1 n=', sized.n::varchar,
                ' hz=', coalesce(sized.horizon_bars::varchar, 'none'),
                ' EVnet=', coalesce(round(sized.ev_net, 6)::varchar, 'na'),
                ' act=', sized.recommended_action,
                ' sz=', sized.recommended_size_band
            ) as alpha_summary_text
        from sized
    )
    select object_construct(
        'WORLDS_SPEC',
        object_construct(
            'schema_version', 'WORLDS_SPEC_V1',
            'generated_ts', to_varchar(narr.proposed_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3'),
            'symbol', narr.symbol,
            'proposal_id', narr.proposal_id,
            'pattern_id', narr.signal_pattern_id,
            'interval_minutes', narr.eff_interval,
            'market_type', narr.market_type,
            'historical_distribution',
            object_construct(
                'upside_probability', narr.p_up,
                'base_probability', narr.p_base,
                'downside_probability', narr.p_down,
                'upside_avg_return', narr.avg_up,
                'base_avg_return', narr.avg_base,
                'downside_avg_return', narr.avg_down,
                'sample_size', narr.n
            ),
            'supporting',
            object_construct(
                'horizon_bars', narr.horizon_bars,
                'bucket_thresholds', object_construct('up_gt', 0.005, 'down_lt', -0.005),
                'selection_rule', 'max_count_horizon_then_larger_horizon_bars',
                'hod_source', 'RECOMMENDATION_LOG_JOIN_RECOMMENDATION_OUTCOMES',
                'insufficient_sample', narr.insufficient_sample,
                'min_sample_for_full_alpha', 12
            )
        ),
        'ALPHA_SPEC',
        object_construct(
            'alpha_schema_version', 'ALPHA_SPEC_V1',
            'expected_value_gross', narr.ev_gross,
            'estimated_cost_floor', narr.cost_floor,
            'expected_value_net', narr.ev_net,
            'confidence_band', narr.confidence_band,
            'downside_risk_band', narr.downside_risk_band,
            'recommended_action', narr.recommended_action,
            'recommended_size_band', narr.recommended_size_band,
            'alpha_reason_codes',
            array_construct(
                iff(narr.insufficient_sample, 'INSUFFICIENT_HOD_SAMPLE', 'HOD_SAMPLE_OK'),
                'EV_RULE_V1',
                'SIZE_MAP_V1'
            ),
            'alpha_summary_text', substr(narr.alpha_summary_text, 1, 400)
        )
    )::variant
    from narr
$$;

use schema LIVE;

create table if not exists MIP.LIVE.ENTRY_INTEL_SNAPSHOT (
    SNAPSHOT_ID       varchar(36)   not null,
    PROPOSAL_ID       number(38,0)  not null,
    PORTFOLIO_ID      number(38,0)  not null,
    EIS_VERSION       number(38,0)  not null default 1,
    CREATED_TS        timestamp_ntz not null default current_timestamp(),
    SIGNAL_RUN_ID     varchar(64),
    SYMBOL            varchar(32),
    PATTERN_ID        number(38,0),
    WORLDS_SPEC       variant       not null,
    ALPHA_SPEC        variant       not null,
    SOURCE_VERSION    varchar(32),
    EIS_NOTE          varchar(1024),
    constraint PK_ENTRY_INTEL_SNAPSHOT primary key (SNAPSHOT_ID),
    constraint UQ_ENTRY_INTEL_PROPOSAL_VERSION unique (PROPOSAL_ID, EIS_VERSION)
);

create table if not exists MIP.LIVE.ENTRY_INTEL_ACTION_LINK (
    LINK_ID           varchar(36)   not null,
    SNAPSHOT_ID       varchar(36)   not null,
    PROPOSAL_ID       number(38,0)  not null,
    ENTRY_ACTION_ID   varchar(64)   not null,
    LINK_CREATED_TS   timestamp_ntz not null default current_timestamp(),
    constraint PK_ENTRY_INTEL_ACTION_LINK primary key (LINK_ID),
    constraint UQ_ENTRY_INTEL_ACTION_LINK_ACTION unique (ENTRY_ACTION_ID),
    constraint FK_EIAL_SNAPSHOT foreign key (SNAPSHOT_ID) references MIP.LIVE.ENTRY_INTEL_SNAPSHOT(SNAPSHOT_ID)
);

create table if not exists MIP.LIVE.TRADE_CLOSEOUT (
    CLOSEOUT_ID           varchar(36)   not null,
    ENTRY_ACTION_ID       varchar(64)   not null,
    SNAPSHOT_ID           varchar(36),
    EXIT_TYPE             varchar(32)   not null,
    REALIZED_RETURN_PCT   float,
    HOLDING_PERIOD_SEC    number(38,0),
    EXIT_TS               timestamp_ntz not null default current_timestamp(),
    ALIGNMENT_JSON        variant       not null,
    CREATED_TS            timestamp_ntz not null default current_timestamp(),
    constraint PK_TRADE_CLOSEOUT primary key (CLOSEOUT_ID),
    constraint UQ_TRADE_CLOSEOUT_ENTRY unique (ENTRY_ACTION_ID),
    constraint FK_TC_SNAPSHOT foreign key (SNAPSHOT_ID) references MIP.LIVE.ENTRY_INTEL_SNAPSHOT(SNAPSHOT_ID)
);

create or replace procedure MIP.APP.SP_ENSURE_ENTRY_INTEL_FOR_RUN(
    P_RUN_ID varchar,
    P_PORTFOLIO_ID number
)
returns variant
language sql
execute as caller
as
$$
declare
    v_inserted number := 0;
    v_payload variant;
    v_has number;
    c1 cursor for
        select op.proposal_id as pid
        from MIP.AGENT_OUT.ORDER_PROPOSALS op
        where op.run_id_varchar = :P_RUN_ID
          and op.portfolio_id = :P_PORTFOLIO_ID;
begin
    for row1 in c1 do
        select count(*) into :v_has
        from MIP.LIVE.ENTRY_INTEL_SNAPSHOT e
        where e.proposal_id = row1.pid;

        if (:v_has = 0) then
            select mip.app.f_build_entry_intel_for_proposal(row1.pid) into :v_payload;

            insert into MIP.LIVE.ENTRY_INTEL_SNAPSHOT (
                SNAPSHOT_ID,
                PROPOSAL_ID,
                PORTFOLIO_ID,
                EIS_VERSION,
                SIGNAL_RUN_ID,
                SYMBOL,
                PATTERN_ID,
                WORLDS_SPEC,
                ALPHA_SPEC,
                SOURCE_VERSION,
                EIS_NOTE
            )
            select
                uuid_string(),
                op.proposal_id,
                op.portfolio_id,
                1,
                op.run_id_varchar,
                op.symbol,
                op.signal_pattern_id,
                coalesce(
                    get_path(:v_payload, 'WORLDS_SPEC'),
                    object_construct('schema_version', 'WORLDS_SPEC_V1', 'build_error', true)
                ),
                coalesce(
                    get_path(:v_payload, 'ALPHA_SPEC'),
                    object_construct('alpha_schema_version', 'ALPHA_SPEC_V1', 'build_error', true)
                ),
                'EIS_SCHEMA_V2',
                null
            from MIP.AGENT_OUT.ORDER_PROPOSALS op
            where op.proposal_id = row1.pid;

            v_inserted := :v_inserted + sqlrowcount;
        end if;
    end for;

    return object_construct(
        'status', 'SUCCESS',
        'inserted_count', :v_inserted,
        'run_id', :P_RUN_ID,
        'portfolio_id', :P_PORTFOLIO_ID
    );
end;
$$;

create or replace procedure MIP.APP.SP_ENSURE_ENTRY_INTEL_FOR_PROPOSAL(
    P_PROPOSAL_ID number
)
returns variant
language sql
execute as caller
as
$$
declare
    v_inserted number := 0;
    v_payload variant;
    v_has number;
begin
    select count(*) into :v_has
    from MIP.LIVE.ENTRY_INTEL_SNAPSHOT e
    where e.proposal_id = :P_PROPOSAL_ID;

    if (:v_has > 0) then
        return object_construct(
            'status', 'SUCCESS',
            'inserted_count', 0,
            'proposal_id', :P_PROPOSAL_ID,
            'note', 'EIS_ALREADY_EXISTS'
        );
    end if;

    select mip.app.f_build_entry_intel_for_proposal(:P_PROPOSAL_ID) into :v_payload;

    insert into MIP.LIVE.ENTRY_INTEL_SNAPSHOT (
        SNAPSHOT_ID,
        PROPOSAL_ID,
        PORTFOLIO_ID,
        EIS_VERSION,
        SIGNAL_RUN_ID,
        SYMBOL,
        PATTERN_ID,
        WORLDS_SPEC,
        ALPHA_SPEC,
        SOURCE_VERSION,
        EIS_NOTE
    )
    select
        uuid_string(),
        op.proposal_id,
        op.portfolio_id,
        1,
        op.run_id_varchar,
        op.symbol,
        op.signal_pattern_id,
        coalesce(
            get_path(:v_payload, 'WORLDS_SPEC'),
            object_construct('schema_version', 'WORLDS_SPEC_V1', 'build_error', true)
        ),
        coalesce(
            get_path(:v_payload, 'ALPHA_SPEC'),
            object_construct('alpha_schema_version', 'ALPHA_SPEC_V1', 'build_error', true)
        ),
        'EIS_SCHEMA_V2',
        null
    from MIP.AGENT_OUT.ORDER_PROPOSALS op
    where op.proposal_id = :P_PROPOSAL_ID;

    v_inserted := sqlrowcount;
    return object_construct(
        'status', 'SUCCESS',
        'inserted_count', :v_inserted,
        'proposal_id', :P_PROPOSAL_ID
    );
end;
$$;
