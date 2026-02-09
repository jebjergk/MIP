-- v_daily_digest_snapshot.sql
-- Purpose: Deterministic state snapshot for the Daily Intelligence Digest.
-- Assembles facts from canonical truth views into a single VARIANT per portfolio.
-- Includes interest detectors that fire when something notable happened.
-- This view is content-only; the procedure supplies (AS_OF_TS, RUN_ID) for MERGE key.
--
-- Source views (all canonical / already exist):
--   MIP.MART.V_PORTFOLIO_RISK_GATE          — gate/health status
--   MIP.MART.V_PORTFOLIO_RISK_STATE         — risk state, entries blocked
--   MIP.MART.V_PORTFOLIO_RUN_KPIS           — portfolio performance KPIs
--   MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL — open positions
--   MIP.MART.V_TRUSTED_SIGNAL_POLICY        — trust labels per pattern
--   MIP.APP.V_SIGNALS_ELIGIBLE_TODAY         — today's eligible signals
--   MIP.AGENT_OUT.ORDER_PROPOSALS            — proposals this run
--   MIP.APP.PORTFOLIO_TRADES                 — executed trades
--   MIP.APP.PORTFOLIO_DAILY                  — daily equity snapshots
--   MIP.APP.PORTFOLIO                        — portfolio config
--   MIP.APP.PORTFOLIO_PROFILE                — profile thresholds
--   MIP.APP.MIP_AUDIT_LOG                    — pipeline freshness
--   MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT      — prior snapshot for deltas

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_DAILY_DIGEST_SNAPSHOT (
    PORTFOLIO_ID,
    SNAPSHOT_JSON
) as
with
-- Active episode per portfolio (for episode-scoped data)
active_episode as (
    select
        pe.PORTFOLIO_ID,
        pe.EPISODE_ID,
        pe.PROFILE_ID as EPISODE_PROFILE_ID,
        pe.START_TS   as EPISODE_START_TS,
        pe.START_EQUITY as EPISODE_START_EQUITY,
        row_number() over (
            partition by pe.PORTFOLIO_ID
            order by pe.START_TS desc
        ) as EPISODE_NUMBER_DESC
    from MIP.APP.PORTFOLIO_EPISODE pe
    where pe.STATUS = 'ACTIVE'
),
-- Total episodes per portfolio (to show "Episode 3 of 3" etc.)
episode_counts as (
    select
        PORTFOLIO_ID,
        count(*) as TOTAL_EPISODES
    from MIP.APP.PORTFOLIO_EPISODE
    group by PORTFOLIO_ID
),

portfolio_scope as (
    select
        p.PORTFOLIO_ID,
        p.NAME as PORTFOLIO_NAME,
        -- Use episode START_EQUITY when available (correct after crystallize);
        -- fall back to PORTFOLIO.STARTING_CASH for portfolios without episodes.
        coalesce(ae.EPISODE_START_EQUITY, p.STARTING_CASH) as STARTING_CASH,
        p.FINAL_EQUITY,
        p.TOTAL_RETURN,
        p.STATUS as PORTFOLIO_STATUS,
        coalesce(pp.MAX_POSITIONS, 5) as MAX_POSITIONS,
        coalesce(pp.MAX_POSITION_PCT, 0.20) as MAX_POSITION_PCT,
        coalesce(pp.DRAWDOWN_STOP_PCT, 0.10) as DRAWDOWN_STOP_PCT,
        ae.EPISODE_ID,
        ae.EPISODE_START_TS,
        ae.EPISODE_START_EQUITY,
        coalesce(ec.TOTAL_EPISODES, 1) as TOTAL_EPISODES
    from MIP.APP.PORTFOLIO p
    left join MIP.APP.PORTFOLIO_PROFILE pp
        on pp.PROFILE_ID = p.PROFILE_ID
    left join active_episode ae
        on ae.PORTFOLIO_ID = p.PORTFOLIO_ID
    left join episode_counts ec
        on ec.PORTFOLIO_ID = p.PORTFOLIO_ID
    where p.STATUS = 'ACTIVE'
),

-- Gate / health / risk
risk_gate as (
    select
        rg.PORTFOLIO_ID,
        rg.RISK_STATUS,
        rg.ENTRIES_BLOCKED,
        rg.BLOCK_REASON,
        coalesce(rg.OPEN_POSITIONS, 0) as OPEN_POSITIONS,
        rg.MAX_DRAWDOWN,
        rg.DRAWDOWN_STOP_TS
    from MIP.MART.V_PORTFOLIO_RISK_GATE rg
),

-- Capacity
capacity as (
    select
        ps.PORTFOLIO_ID,
        ps.MAX_POSITIONS,
        coalesce(rg.OPEN_POSITIONS, 0) as OPEN_POSITIONS,
        ps.MAX_POSITIONS - coalesce(rg.OPEN_POSITIONS, 0) as REMAINING_CAPACITY,
        ps.MAX_POSITION_PCT
    from portfolio_scope ps
    left join risk_gate rg
        on rg.PORTFOLIO_ID = ps.PORTFOLIO_ID
),

-- Pipeline freshness
pipeline_freshness as (
    select
        RUN_ID as LATEST_PIPELINE_RUN_ID,
        EVENT_TS as LATEST_PIPELINE_RUN_TS,
        STATUS as LATEST_PIPELINE_STATUS
    from MIP.APP.MIP_AUDIT_LOG
    where EVENT_TYPE = 'PIPELINE'
      and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
    order by EVENT_TS desc
    limit 1
),

-- Latest market bars timestamp
latest_market as (
    select max(TS) as LATEST_MARKET_TS
    from MIP.MART.MARKET_BARS
),

-- Signals summary: counts by market type, top symbols by readiness
signals_by_market as (
    select
        MARKET_TYPE,
        count(*) as SIGNAL_COUNT,
        count_if(IS_ELIGIBLE) as ELIGIBLE_COUNT
    from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
    group by MARKET_TYPE
),
signals_summary_agg as (
    select
        array_agg(
            object_construct(
                'market_type', MARKET_TYPE,
                'signal_count', SIGNAL_COUNT,
                'eligible_count', ELIGIBLE_COUNT
            )
        ) within group (order by SIGNAL_COUNT desc) as BY_MARKET_TYPE,
        sum(SIGNAL_COUNT) as TOTAL_SIGNALS,
        sum(ELIGIBLE_COUNT) as TOTAL_ELIGIBLE
    from signals_by_market
),
top_ready_symbols as (
    select
        array_agg(
            object_construct(
                'symbol', SYMBOL,
                'market_type', MARKET_TYPE,
                'score', SCORE,
                'trust_label', TRUST_LABEL
            )
        ) within group (order by SCORE desc) as items
    from (
        select SYMBOL, MARKET_TYPE, SCORE, TRUST_LABEL
        from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
        where IS_ELIGIBLE
        order by SCORE desc
        limit 10
    )
),

-- Trust labels: current distribution
trust_distribution as (
    select
        count_if(TRUST_LABEL = 'TRUSTED') as TRUSTED_COUNT,
        count_if(TRUST_LABEL = 'WATCH') as WATCH_COUNT,
        count_if(TRUST_LABEL = 'UNTRUSTED') as UNTRUSTED_COUNT,
        count(*) as TOTAL_PATTERNS
    from MIP.MART.V_TRUSTED_SIGNAL_POLICY
),

-- Proposals summary (per portfolio, latest run)
latest_proposal_run as (
    select
        PORTFOLIO_ID,
        coalesce(RUN_ID_VARCHAR, to_varchar(RUN_ID)) as RUN_ID
    from (
        select
            PORTFOLIO_ID,
            RUN_ID_VARCHAR,
            RUN_ID,
            row_number() over (
                partition by PORTFOLIO_ID
                order by PROPOSED_AT desc
            ) as rn
        from MIP.AGENT_OUT.ORDER_PROPOSALS
    )
    where rn = 1
),
proposals_summary as (
    select
        lpr.PORTFOLIO_ID,
        count(*) as PROPOSED_COUNT,
        count_if(op.STATUS = 'REJECTED') as REJECTED_COUNT,
        count_if(op.STATUS = 'EXECUTED') as EXECUTED_COUNT,
        count_if(op.STATUS = 'APPROVED') as APPROVED_COUNT,
        -- Top reject reasons
        array_agg(distinct op.VALIDATION_ERRORS) within group (order by op.VALIDATION_ERRORS)
            as TOP_REJECT_REASONS
    from MIP.AGENT_OUT.ORDER_PROPOSALS op
    join latest_proposal_run lpr
        on lpr.PORTFOLIO_ID = op.PORTFOLIO_ID
       and lpr.RUN_ID = coalesce(op.RUN_ID_VARCHAR, to_varchar(op.RUN_ID))
    where op.STATUS = 'REJECTED'
       or op.STATUS is not null
    group by lpr.PORTFOLIO_ID
),

-- Trades summary (per portfolio, latest run)
trades_summary as (
    select
        pt.PORTFOLIO_ID,
        count(*) as TRADE_COUNT,
        sum(case when SIDE = 'BUY' then 1 else 0 end) as BUY_COUNT,
        sum(case when SIDE = 'SELL' then 1 else 0 end) as SELL_COUNT,
        sum(coalesce(REALIZED_PNL, 0)) as TOTAL_REALIZED_PNL
    from MIP.APP.PORTFOLIO_TRADES pt
    join latest_proposal_run lpr
        on lpr.PORTFOLIO_ID = pt.PORTFOLIO_ID
       and lpr.RUN_ID = pt.RUN_ID
    group by pt.PORTFOLIO_ID
),

-- Latest KPIs (curr + prev for deltas)
kpis_ranked as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        TOTAL_RETURN,
        MAX_DRAWDOWN,
        FINAL_EQUITY,
        row_number() over (
            partition by PORTFOLIO_ID
            order by TO_TS desc
        ) as rn
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
),

-- Latest exposure (episode-scoped: only rows from active episode)
exposure_ranked as (
    select
        d.PORTFOLIO_ID,
        d.RUN_ID,
        d.TS,
        d.CASH,
        d.TOTAL_EQUITY,
        d.OPEN_POSITIONS,
        row_number() over (
            partition by d.PORTFOLIO_ID
            order by d.TS desc
        ) as rn
    from MIP.APP.PORTFOLIO_DAILY d
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e
      on e.PORTFOLIO_ID = d.PORTFOLIO_ID
    where (
        (d.EPISODE_ID is not null and d.EPISODE_ID = e.EPISODE_ID)
        or (d.EPISODE_ID is null and (e.EPISODE_ID is null or d.TS >= e.START_TS))
    )
),

-- Prior snapshot for delta detectors
prior_snapshot as (
    select
        PORTFOLIO_ID,
        SNAPSHOT_JSON,
        AS_OF_TS as PRIOR_AS_OF_TS,
        row_number() over (
            partition by PORTFOLIO_ID
            order by AS_OF_TS desc
        ) as rn
    from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
),

-- ===== PATTERN CATALOG + OBSERVATIONS =====
-- Pattern definitions with raw params for LLM context
pattern_catalog as (
    select coalesce(
        array_agg(
            object_construct(
                'pattern_id', pd.PATTERN_ID,
                'name', pd.NAME,
                'description', pd.DESCRIPTION,
                'market_type', pd.PARAMS_JSON:market_type::string,
                'interval_minutes', pd.PARAMS_JSON:interval_minutes::number,
                'lookback_days', pd.PARAMS_JSON:lookback_days::number,
                'params', pd.PARAMS_JSON
            )
        ) within group (order by pd.PATTERN_ID),
        array_construct()
    ) as PATTERNS
    from MIP.APP.PATTERN_DEFINITION pd
    where pd.IS_ACTIVE = 'Y'
      and pd.ENABLED = true
),

-- Per-pattern signal observations for today
pattern_obs_detail as (
    select
        s.PATTERN_ID,
        pd.NAME as PATTERN_NAME,
        s.MARKET_TYPE,
        s.INTERVAL_MINUTES,
        count(*) as SIGNALS_GENERATED,
        count_if(s.IS_ELIGIBLE) as SIGNALS_ELIGIBLE,
        count_if(s.TRUST_LABEL = 'TRUSTED') as SIGNALS_TRUSTED,
        count_if(s.TRUST_LABEL = 'WATCH') as SIGNALS_WATCH,
        count_if(s.TRUST_LABEL = 'UNTRUSTED') as SIGNALS_UNTRUSTED,
        round(min(s.SCORE), 6) as MIN_SCORE,
        round(max(s.SCORE), 6) as MAX_SCORE,
        round(avg(s.SCORE), 6) as AVG_SCORE,
        pd.PARAMS_JSON as PARAMS_JSON
    from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
    join MIP.APP.PATTERN_DEFINITION pd on pd.PATTERN_ID = s.PATTERN_ID
    group by s.PATTERN_ID, pd.NAME, s.MARKET_TYPE, s.INTERVAL_MINUTES, pd.PARAMS_JSON
),

-- Near-miss examples per pattern (WATCH signals closest to trust)
pattern_near_miss_raw as (
    select
        s.PATTERN_ID,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.SCORE,
        s.GATING_REASON:policy_reason as POLICY_REASON,
        row_number() over (partition by s.PATTERN_ID order by s.SCORE desc) as rn
    from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
    where s.TRUST_LABEL = 'WATCH'
),
pattern_near_miss as (
    select
        PATTERN_ID,
        array_agg(
            object_construct(
                'symbol', SYMBOL,
                'market_type', MARKET_TYPE,
                'observed_score', round(SCORE, 6),
                'trust_policy_metrics', POLICY_REASON
            )
        ) within group (order by rn) as NEAR_MISS_EXAMPLES
    from pattern_near_miss_raw
    where rn <= 3
    group by PATTERN_ID
),

-- Aggregate pattern observations into single array
pattern_obs_agg as (
    select coalesce(
        array_agg(
            object_construct(
                'pattern_id', pod.PATTERN_ID,
                'pattern_name', pod.PATTERN_NAME,
                'market_type', pod.MARKET_TYPE,
                'interval_minutes', pod.INTERVAL_MINUTES,
                'signals_generated', pod.SIGNALS_GENERATED,
                'signals_eligible', pod.SIGNALS_ELIGIBLE,
                'signals_trusted', pod.SIGNALS_TRUSTED,
                'signals_watch', pod.SIGNALS_WATCH,
                'signals_untrusted', pod.SIGNALS_UNTRUSTED,
                'observed_score_range', object_construct(
                    'min', pod.MIN_SCORE,
                    'max', pod.MAX_SCORE,
                    'avg', pod.AVG_SCORE
                ),
                'thresholds', object_construct(
                    'min_return', pod.PARAMS_JSON:min_return::float,
                    'min_zscore', pod.PARAMS_JSON:min_zscore::float,
                    'fast_window', pod.PARAMS_JSON:fast_window::number,
                    'slow_window', pod.PARAMS_JSON:slow_window::number,
                    'lookback_days', pod.PARAMS_JSON:lookback_days::number
                ),
                'explain', object_construct(
                    'zscore_basis', 'Per-symbol return distribution over '
                        || coalesce(pod.PARAMS_JSON:lookback_days::string, '?')
                        || ' lookback days of ' || pod.MARKET_TYPE
                        || ' ' || pod.INTERVAL_MINUTES::string || '-min bars',
                    'fast_window_means', 'Recent behavior: average over last '
                        || coalesce(pod.PARAMS_JSON:fast_window::string, '?') || ' bars',
                    'slow_window_means', 'Baseline: average over last '
                        || coalesce(pod.PARAMS_JSON:slow_window::string, '?') || ' bars',
                    'score_is', 'Observed return (close-to-close) of the triggering bar'
                ),
                'near_miss_examples', coalesce(pnm.NEAR_MISS_EXAMPLES, array_construct())
            )
        ) within group (order by pod.SIGNALS_GENERATED desc),
        array_construct()
    ) as PATTERN_OBS
    from pattern_obs_detail pod
    left join pattern_near_miss pnm on pnm.PATTERN_ID = pod.PATTERN_ID
),

-- Training gate thresholds (system-level trust gating params)
-- Uses UNION ALL with defaults to guarantee exactly one row even if table is empty
training_gate_thresholds as (
    select MIN_SIGNALS, MIN_HIT_RATE, MIN_AVG_RETURN from (
        select
            coalesce(MIN_SIGNALS, 40) as MIN_SIGNALS,
            coalesce(MIN_HIT_RATE, 0.55) as MIN_HIT_RATE,
            coalesce(MIN_AVG_RETURN, 0.0005) as MIN_AVG_RETURN,
            1 as PRIORITY
        from MIP.APP.TRAINING_GATE_PARAMS
        where IS_ACTIVE = true
        union all
        select 40, 0.55, 0.0005, 99
    )
    order by PRIORITY
    limit 1
),

-- ===== INTEREST DETECTORS =====
-- Each detector: name, fired (boolean), severity, detail (variant)

-- Detector 1: Gate status changed
det_gate_changed as (
    select
        rg.PORTFOLIO_ID,
        'GATE_CHANGED' as DETECTOR,
        iff(
            ps.SNAPSHOT_JSON is not null
            and rg.RISK_STATUS != coalesce(ps.SNAPSHOT_JSON:gate:risk_status::string, rg.RISK_STATUS),
            true, false
        ) as FIRED,
        'HIGH' as SEVERITY,
        object_construct(
            'from', coalesce(ps.SNAPSHOT_JSON:gate:risk_status::string, 'UNKNOWN'),
            'to', rg.RISK_STATUS
        ) as DETAIL
    from risk_gate rg
    left join prior_snapshot ps
        on ps.PORTFOLIO_ID = rg.PORTFOLIO_ID and ps.rn = 1
),

-- Detector 2: Health / entries blocked changed
det_health_changed as (
    select
        rg.PORTFOLIO_ID,
        'HEALTH_CHANGED' as DETECTOR,
        iff(
            ps.SNAPSHOT_JSON is not null
            and rg.ENTRIES_BLOCKED != coalesce(ps.SNAPSHOT_JSON:gate:entries_blocked::boolean, rg.ENTRIES_BLOCKED),
            true, false
        ) as FIRED,
        'HIGH' as SEVERITY,
        object_construct(
            'entries_blocked_now', rg.ENTRIES_BLOCKED,
            'entries_blocked_prev', coalesce(ps.SNAPSHOT_JSON:gate:entries_blocked::boolean, null),
            'block_reason', rg.BLOCK_REASON
        ) as DETAIL
    from risk_gate rg
    left join prior_snapshot ps
        on ps.PORTFOLIO_ID = rg.PORTFOLIO_ID and ps.rn = 1
),

-- Detector 3: Trust label transitions (WATCH→TRUSTED or TRUSTED→WATCH)
det_trust_changed as (
    select
        ps2.PORTFOLIO_ID,
        'TRUST_CHANGED' as DETECTOR,
        iff(count(*) > 0, true, false) as FIRED,
        'MEDIUM' as SEVERITY,
        object_construct(
            'transitions_count', count(*),
            'sample', coalesce(any_value(
                object_construct(
                    'pattern_id', tsp.PATTERN_ID,
                    'trust_label_now', tsp.TRUST_LABEL,
                    'market_type', tsp.MARKET_TYPE
                )
            ), object_construct())
        ) as DETAIL
    from portfolio_scope ps2
    cross join MIP.MART.V_TRUSTED_SIGNAL_POLICY tsp
    left join prior_snapshot ps
        on ps.PORTFOLIO_ID = ps2.PORTFOLIO_ID and ps.rn = 1
    where ps.SNAPSHOT_JSON is not null
    group by ps2.PORTFOLIO_ID
),

-- Detector 4: Near misses (eligible = false but close to thresholds)
det_near_miss as (
    select
        ps2.PORTFOLIO_ID,
        'NEAR_MISS' as DETECTOR,
        iff(near_miss_count > 0, true, false) as FIRED,
        'MEDIUM' as SEVERITY,
        object_construct(
            'near_miss_count', near_miss_count,
            'symbols', near_miss_symbols
        ) as DETAIL
    from portfolio_scope ps2
    cross join (
        select
            count(*) as near_miss_count,
            coalesce(
                array_agg(distinct SYMBOL) within group (order by SYMBOL),
                array_construct()
            ) as near_miss_symbols
        from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
        where not IS_ELIGIBLE
          and TRUST_LABEL = 'WATCH'
    )
),

-- Detector 5: Proposal funnel (where candidates drop off)
det_proposal_funnel as (
    select
        ps2.PORTFOLIO_ID,
        'PROPOSAL_FUNNEL' as DETECTOR,
        true as FIRED,
        'LOW' as SEVERITY,
        object_construct(
            'total_signals', coalesce(ssa.TOTAL_SIGNALS, 0),
            'eligible', coalesce(ssa.TOTAL_ELIGIBLE, 0),
            'proposed', coalesce(psum.PROPOSED_COUNT, 0),
            'rejected', coalesce(psum.REJECTED_COUNT, 0),
            'executed', coalesce(psum.EXECUTED_COUNT, 0),
            'biggest_dropoff', case
                when coalesce(ssa.TOTAL_SIGNALS, 0) > 0 and coalesce(ssa.TOTAL_ELIGIBLE, 0) = 0 then 'SIGNALS_TO_ELIGIBLE'
                when coalesce(ssa.TOTAL_ELIGIBLE, 0) > 0 and coalesce(psum.PROPOSED_COUNT, 0) = 0 then 'ELIGIBLE_TO_PROPOSED'
                when coalesce(psum.PROPOSED_COUNT, 0) > 0 and coalesce(psum.EXECUTED_COUNT, 0) = 0 then 'PROPOSED_TO_EXECUTED'
                else 'NONE'
            end
        ) as DETAIL
    from portfolio_scope ps2
    cross join signals_summary_agg ssa
    left join proposals_summary psum
        on psum.PORTFOLIO_ID = ps2.PORTFOLIO_ID
),

-- Detector 6: Nothing happened (no new bars, no candidates, no trades)
det_nothing_happened as (
    select
        ps2.PORTFOLIO_ID,
        'NOTHING_HAPPENED' as DETECTOR,
        iff(
            coalesce(ssa.TOTAL_SIGNALS, 0) = 0
            and coalesce(tsum.TRADE_COUNT, 0) = 0
            and coalesce(psum.PROPOSED_COUNT, 0) = 0,
            true, false
        ) as FIRED,
        'LOW' as SEVERITY,
        object_construct(
            'reason', case
                when coalesce(ssa.TOTAL_SIGNALS, 0) = 0 then 'NO_SIGNALS_TODAY'
                when coalesce(psum.PROPOSED_COUNT, 0) = 0 then 'NO_CANDIDATES'
                else 'NO_TRADES_EXECUTED'
            end,
            'signal_count', coalesce(ssa.TOTAL_SIGNALS, 0),
            'proposal_count', coalesce(psum.PROPOSED_COUNT, 0),
            'trade_count', coalesce(tsum.TRADE_COUNT, 0)
        ) as DETAIL
    from portfolio_scope ps2
    cross join signals_summary_agg ssa
    left join proposals_summary psum on psum.PORTFOLIO_ID = ps2.PORTFOLIO_ID
    left join trades_summary tsum on tsum.PORTFOLIO_ID = ps2.PORTFOLIO_ID
),

-- Detector 7: Portfolio capacity (remaining slots, saturation risk)
det_capacity as (
    select
        cap.PORTFOLIO_ID,
        'CAPACITY_STATE' as DETECTOR,
        iff(cap.REMAINING_CAPACITY <= 1, true, false) as FIRED,
        iff(cap.REMAINING_CAPACITY = 0, 'HIGH', 'MEDIUM') as SEVERITY,
        object_construct(
            'max_positions', cap.MAX_POSITIONS,
            'open_positions', cap.OPEN_POSITIONS,
            'remaining_capacity', cap.REMAINING_CAPACITY,
            'saturation_pct', round(
                iff(cap.MAX_POSITIONS > 0,
                    cap.OPEN_POSITIONS * 100.0 / cap.MAX_POSITIONS,
                    0
                ), 1
            )
        ) as DETAIL
    from capacity cap
),

-- Detector 8: Conflicts (strong signal blocked by portfolio rule)
det_conflicts as (
    select
        ps2.PORTFOLIO_ID,
        'CONFLICT_BLOCKED' as DETECTOR,
        iff(conflict_count > 0, true, false) as FIRED,
        'MEDIUM' as SEVERITY,
        object_construct(
            'conflict_count', conflict_count,
            'top_reasons', top_reasons
        ) as DETAIL
    from portfolio_scope ps2
    cross join (
        select
            count(*) as conflict_count,
            coalesce(
                array_agg(distinct VALIDATION_ERRORS) within group (order by VALIDATION_ERRORS),
                array_construct()
            ) as top_reasons
        from MIP.AGENT_OUT.ORDER_PROPOSALS
        where STATUS = 'REJECTED'
          and PROPOSED_AT::date = current_date()
    )
),

-- Detector 9: KPI movement (significant return or drawdown change)
det_kpi_movement as (
    select
        curr.PORTFOLIO_ID,
        'KPI_MOVEMENT' as DETECTOR,
        iff(
            prev.TOTAL_RETURN is not null
            and abs(coalesce(curr.TOTAL_RETURN, 0) - coalesce(prev.TOTAL_RETURN, 0)) > 0.005,
            true, false
        ) as FIRED,
        'MEDIUM' as SEVERITY,
        object_construct(
            'total_return_curr', curr.TOTAL_RETURN,
            'total_return_prev', prev.TOTAL_RETURN,
            'total_return_delta', coalesce(curr.TOTAL_RETURN, 0) - coalesce(prev.TOTAL_RETURN, 0),
            'max_drawdown_curr', curr.MAX_DRAWDOWN,
            'max_drawdown_prev', prev.MAX_DRAWDOWN,
            'max_drawdown_delta', coalesce(curr.MAX_DRAWDOWN, 0) - coalesce(prev.MAX_DRAWDOWN, 0)
        ) as DETAIL
    from kpis_ranked curr
    left join kpis_ranked prev
        on prev.PORTFOLIO_ID = curr.PORTFOLIO_ID and prev.rn = 2
    where curr.rn = 1
),

-- Detector 10: Training progress (trust distribution change)
det_training_progress as (
    select
        ps2.PORTFOLIO_ID,
        'TRAINING_PROGRESS' as DETECTOR,
        iff(
            ps.SNAPSHOT_JSON is not null
            and td.TRUSTED_COUNT != coalesce(ps.SNAPSHOT_JSON:training:trusted_count::number, td.TRUSTED_COUNT),
            true, false
        ) as FIRED,
        'LOW' as SEVERITY,
        object_construct(
            'trusted_count', td.TRUSTED_COUNT,
            'watch_count', td.WATCH_COUNT,
            'untrusted_count', td.UNTRUSTED_COUNT,
            'prev_trusted_count', ps.SNAPSHOT_JSON:training:trusted_count::number
        ) as DETAIL
    from portfolio_scope ps2
    cross join trust_distribution td
    left join prior_snapshot ps
        on ps.PORTFOLIO_ID = ps2.PORTFOLIO_ID and ps.rn = 1
),

-- Assemble all detectors into a single array per portfolio
all_detectors as (
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_gate_changed
    union all
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_health_changed
    union all
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_trust_changed
    union all
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_near_miss
    union all
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_proposal_funnel
    union all
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_nothing_happened
    union all
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_capacity
    union all
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_conflicts
    union all
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_kpi_movement
    union all
    select PORTFOLIO_ID, DETECTOR, FIRED, SEVERITY, DETAIL from det_training_progress
),
detectors_agg as (
    select
        PORTFOLIO_ID,
        array_agg(
            object_construct(
                'detector', DETECTOR,
                'fired', FIRED,
                'severity', SEVERITY,
                'detail', DETAIL
            )
        ) within group (
            order by
                iff(FIRED, 0, 1),
                case SEVERITY when 'HIGH' then 1 when 'MEDIUM' then 2 else 3 end,
                DETECTOR
        ) as DETECTORS
    from all_detectors
    group by PORTFOLIO_ID
)

-- ===== FINAL SELECT: assemble the snapshot JSON =====
select
    ps.PORTFOLIO_ID,
    object_construct(
        'timestamps', object_construct(
            'as_of_ts', current_timestamp(),
            'snapshot_created_at', current_timestamp(),
            'latest_market_ts', lm.LATEST_MARKET_TS,
            'latest_pipeline_run_ts', pf.LATEST_PIPELINE_RUN_TS
        ),
        'gate', object_construct(
            'risk_status', coalesce(rg.RISK_STATUS, 'OK'),
            'entries_blocked', coalesce(rg.ENTRIES_BLOCKED, false),
            'block_reason', rg.BLOCK_REASON,
            'max_drawdown', rg.MAX_DRAWDOWN,
            'drawdown_stop_ts', rg.DRAWDOWN_STOP_TS
        ),
        'capacity', object_construct(
            'max_positions', cap.MAX_POSITIONS,
            'open_positions', cap.OPEN_POSITIONS,
            'remaining_capacity', cap.REMAINING_CAPACITY,
            'max_position_pct', ps.MAX_POSITION_PCT,
            'saturation_pct', round(
                iff(cap.MAX_POSITIONS > 0,
                    cap.OPEN_POSITIONS * 100.0 / cap.MAX_POSITIONS,
                    0
                ), 1
            )
        ),
        'pipeline', object_construct(
            'latest_run_id', pf.LATEST_PIPELINE_RUN_ID,
            'latest_run_ts', pf.LATEST_PIPELINE_RUN_TS,
            'latest_status', pf.LATEST_PIPELINE_STATUS
        ),
        'signals', object_construct(
            'by_market_type', coalesce(ssa.BY_MARKET_TYPE, array_construct()),
            'total_signals', coalesce(ssa.TOTAL_SIGNALS, 0),
            'total_eligible', coalesce(ssa.TOTAL_ELIGIBLE, 0),
            'top_ready_symbols', coalesce(trs.items, array_construct())
        ),
        'proposals', object_construct(
            'proposed_count', coalesce(psum.PROPOSED_COUNT, 0),
            'rejected_count', coalesce(psum.REJECTED_COUNT, 0),
            'executed_count', coalesce(psum.EXECUTED_COUNT, 0),
            'approved_count', coalesce(psum.APPROVED_COUNT, 0),
            'top_reject_reasons', coalesce(psum.TOP_REJECT_REASONS, array_construct())
        ),
        'trades', object_construct(
            'trade_count', coalesce(tsum.TRADE_COUNT, 0),
            'buy_count', coalesce(tsum.BUY_COUNT, 0),
            'sell_count', coalesce(tsum.SELL_COUNT, 0),
            'total_realized_pnl', coalesce(tsum.TOTAL_REALIZED_PNL, 0)
        ),
        'training', object_construct(
            'trusted_count', td.TRUSTED_COUNT,
            'watch_count', td.WATCH_COUNT,
            'untrusted_count', td.UNTRUSTED_COUNT,
            'total_patterns', td.TOTAL_PATTERNS
        ),
        'kpis', object_construct(
            'total_return', curr_kpi.TOTAL_RETURN,
            'max_drawdown', curr_kpi.MAX_DRAWDOWN,
            'final_equity', curr_kpi.FINAL_EQUITY,
            'prev_total_return', prev_kpi.TOTAL_RETURN,
            'prev_max_drawdown', prev_kpi.MAX_DRAWDOWN,
            'return_delta', coalesce(curr_kpi.TOTAL_RETURN, 0) - coalesce(prev_kpi.TOTAL_RETURN, 0),
            'drawdown_delta', coalesce(curr_kpi.MAX_DRAWDOWN, 0) - coalesce(prev_kpi.MAX_DRAWDOWN, 0)
        ),
        'exposure', object_construct(
            'cash', curr_exp.CASH,
            'total_equity', curr_exp.TOTAL_EQUITY,
            'open_positions', curr_exp.OPEN_POSITIONS,
            'prev_cash', prev_exp.CASH,
            'prev_total_equity', prev_exp.TOTAL_EQUITY,
            'equity_delta', coalesce(curr_exp.TOTAL_EQUITY, 0) - coalesce(prev_exp.TOTAL_EQUITY, 0)
        ),
        'portfolio_meta', object_construct(
            'name', ps.PORTFOLIO_NAME,
            'starting_cash', ps.STARTING_CASH,
            'drawdown_stop_pct', ps.DRAWDOWN_STOP_PCT
        ),
        'episode', object_construct(
            'episode_id', ps.EPISODE_ID,
            'episode_start_ts', ps.EPISODE_START_TS,
            'episode_start_equity', ps.EPISODE_START_EQUITY,
            'total_episodes', ps.TOTAL_EPISODES,
            'is_first_episode', iff(ps.TOTAL_EPISODES <= 1, true, false)
        ),
        'patterns', coalesce(pc.PATTERNS, array_construct()),
        'pattern_observations', coalesce(poa.PATTERN_OBS, array_construct()),
        'training_thresholds', object_construct(
            'min_signals', tgt.MIN_SIGNALS,
            'min_hit_rate', tgt.MIN_HIT_RATE,
            'min_avg_return', tgt.MIN_AVG_RETURN
        ),
        'detectors', coalesce(det.DETECTORS, array_construct()),
        'prior_snapshot_ts', priors.PRIOR_AS_OF_TS
    ) as SNAPSHOT_JSON
from portfolio_scope ps
-- Gate / risk
left join risk_gate rg
    on rg.PORTFOLIO_ID = ps.PORTFOLIO_ID
-- Capacity
left join capacity cap
    on cap.PORTFOLIO_ID = ps.PORTFOLIO_ID
-- Pipeline freshness (single row, cross join)
cross join pipeline_freshness pf
-- Latest market ts (single row)
cross join latest_market lm
-- Signals
cross join signals_summary_agg ssa
left join top_ready_symbols trs on 1=1
-- Trust distribution (single row)
cross join trust_distribution td
-- Proposals
left join proposals_summary psum
    on psum.PORTFOLIO_ID = ps.PORTFOLIO_ID
-- Trades
left join trades_summary tsum
    on tsum.PORTFOLIO_ID = ps.PORTFOLIO_ID
-- KPIs (curr + prev)
left join kpis_ranked curr_kpi
    on curr_kpi.PORTFOLIO_ID = ps.PORTFOLIO_ID and curr_kpi.rn = 1
left join kpis_ranked prev_kpi
    on prev_kpi.PORTFOLIO_ID = ps.PORTFOLIO_ID and prev_kpi.rn = 2
-- Exposure (curr + prev)
left join exposure_ranked curr_exp
    on curr_exp.PORTFOLIO_ID = ps.PORTFOLIO_ID and curr_exp.rn = 1
left join exposure_ranked prev_exp
    on prev_exp.PORTFOLIO_ID = ps.PORTFOLIO_ID and prev_exp.rn = 2
-- Patterns + observations (system-wide, single rows)
cross join pattern_catalog pc
cross join pattern_obs_agg poa
cross join training_gate_thresholds tgt
-- Detectors
left join detectors_agg det
    on det.PORTFOLIO_ID = ps.PORTFOLIO_ID
-- Prior snapshot
left join prior_snapshot priors
    on priors.PORTFOLIO_ID = ps.PORTFOLIO_ID and priors.rn = 1;
