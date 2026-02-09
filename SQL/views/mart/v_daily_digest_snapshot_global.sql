-- v_daily_digest_snapshot_global.sql
-- Purpose: Global (system-wide) deterministic snapshot for the Daily Intelligence Digest.
-- Aggregates across ALL active portfolios and the entire signal/training universe.
-- Returns exactly one row with SNAPSHOT_JSON.
--
-- Sources: same canonical views as portfolio snapshot, but aggregated at system level.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_DAILY_DIGEST_SNAPSHOT_GLOBAL (
    SNAPSHOT_JSON
) as
with
-- ── Portfolio aggregate (episode-aware) ─────────────────────
active_episodes as (
    select
        pe.PORTFOLIO_ID,
        pe.EPISODE_ID,
        pe.START_TS as EPISODE_START_TS,
        pe.START_EQUITY as EPISODE_START_EQUITY
    from MIP.APP.PORTFOLIO_EPISODE pe
    where pe.STATUS = 'ACTIVE'
),
episode_counts as (
    select
        PORTFOLIO_ID,
        count(*) as TOTAL_EPISODES
    from MIP.APP.PORTFOLIO_EPISODE
    group by PORTFOLIO_ID
),
portfolio_agg as (
    select
        count(*) as ACTIVE_PORTFOLIOS,
        array_agg(
            object_construct(
                'portfolio_id', p.PORTFOLIO_ID,
                'name', p.NAME,
                'total_return', p.TOTAL_RETURN,
                'final_equity', p.FINAL_EQUITY,
                'status', p.STATUS,
                'episode_id', ae.EPISODE_ID,
                'episode_start_ts', ae.EPISODE_START_TS,
                'episode_start_equity', ae.EPISODE_START_EQUITY,
                'total_episodes', coalesce(ec.TOTAL_EPISODES, 1)
            )
        ) within group (order by p.PORTFOLIO_ID) as PORTFOLIO_SUMMARY
    from MIP.APP.PORTFOLIO p
    left join active_episodes ae
        on ae.PORTFOLIO_ID = p.PORTFOLIO_ID
    left join episode_counts ec
        on ec.PORTFOLIO_ID = p.PORTFOLIO_ID
    where p.STATUS = 'ACTIVE'
),

-- ── Gate status across portfolios ───────────────────────────
gate_agg as (
    select
        count(*) as TOTAL_PORTFOLIOS,
        count_if(RISK_STATUS = 'OK') as OK_COUNT,
        count_if(RISK_STATUS = 'WARN') as WARN_COUNT,
        count_if(ENTRIES_BLOCKED) as BLOCKED_COUNT,
        sum(coalesce(OPEN_POSITIONS, 0)) as TOTAL_OPEN_POSITIONS,
        array_agg(
            object_construct(
                'portfolio_id', PORTFOLIO_ID,
                'risk_status', RISK_STATUS,
                'entries_blocked', ENTRIES_BLOCKED,
                'block_reason', BLOCK_REASON,
                'open_positions', coalesce(OPEN_POSITIONS, 0)
            )
        ) within group (order by PORTFOLIO_ID) as PER_PORTFOLIO
    from MIP.MART.V_PORTFOLIO_RISK_GATE
),

-- ── Capacity aggregate ──────────────────────────────────────
capacity_agg as (
    select
        sum(coalesce(pp.MAX_POSITIONS, 5)) as TOTAL_MAX_POSITIONS,
        sum(coalesce(rg.OPEN_POSITIONS, 0)) as TOTAL_OPEN,
        sum(coalesce(pp.MAX_POSITIONS, 5)) - sum(coalesce(rg.OPEN_POSITIONS, 0)) as TOTAL_REMAINING
    from MIP.APP.PORTFOLIO p
    left join MIP.APP.PORTFOLIO_PROFILE pp on pp.PROFILE_ID = p.PROFILE_ID
    left join MIP.MART.V_PORTFOLIO_RISK_GATE rg on rg.PORTFOLIO_ID = p.PORTFOLIO_ID
    where p.STATUS = 'ACTIVE'
),

-- ── Pipeline freshness ──────────────────────────────────────
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

-- ── Latest market bars ──────────────────────────────────────
latest_market as (
    select
        max(TS) as LATEST_MARKET_TS,
        count(distinct SYMBOL) as SYMBOLS_WITH_BARS,
        count(distinct MARKET_TYPE) as MARKET_TYPES_WITH_BARS
    from MIP.MART.MARKET_BARS
    where TS >= dateadd(day, -7, current_timestamp()::timestamp_ntz)
),

-- ── Signals summary (system-wide) ───────────────────────────
signals_by_market as (
    select
        MARKET_TYPE,
        count(*) as SIGNAL_COUNT,
        count_if(IS_ELIGIBLE) as ELIGIBLE_COUNT,
        count(distinct SYMBOL) as UNIQUE_SYMBOLS
    from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
    group by MARKET_TYPE
),
signals_agg as (
    select
        coalesce(sum(SIGNAL_COUNT), 0) as TOTAL_SIGNALS,
        coalesce(sum(ELIGIBLE_COUNT), 0) as TOTAL_ELIGIBLE,
        coalesce(sum(UNIQUE_SYMBOLS), 0) as TOTAL_UNIQUE_SYMBOLS,
        coalesce(
            array_agg(
                object_construct(
                    'market_type', MARKET_TYPE,
                    'signal_count', SIGNAL_COUNT,
                    'eligible_count', ELIGIBLE_COUNT,
                    'unique_symbols', UNIQUE_SYMBOLS
                )
            ) within group (order by SIGNAL_COUNT desc),
            array_construct()
        ) as BY_MARKET_TYPE
    from signals_by_market
),

-- ── Top readiness symbols (top 10) ──────────────────────────
top_ready as (
    select coalesce(
        array_agg(
            object_construct(
                'symbol', SYMBOL,
                'market_type', MARKET_TYPE,
                'score', SCORE,
                'trust_label', TRUST_LABEL
            )
        ) within group (order by SCORE desc),
        array_construct()
    ) as items
    from (
        select SYMBOL, MARKET_TYPE, SCORE, TRUST_LABEL
        from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
        where IS_ELIGIBLE
        order by SCORE desc
        limit 10
    )
),

-- ── Trust distribution (system-wide) ────────────────────────
trust_dist as (
    select
        count_if(TRUST_LABEL = 'TRUSTED') as TRUSTED_COUNT,
        count_if(TRUST_LABEL = 'WATCH') as WATCH_COUNT,
        count_if(TRUST_LABEL = 'UNTRUSTED') as UNTRUSTED_COUNT,
        count(*) as TOTAL_PATTERNS
    from MIP.MART.V_TRUSTED_SIGNAL_POLICY
),

-- ── Proposal funnel across all portfolios ───────────────────
proposals_agg as (
    select
        count(*) as TOTAL_PROPOSED,
        count_if(STATUS = 'REJECTED') as TOTAL_REJECTED,
        count_if(STATUS = 'EXECUTED') as TOTAL_EXECUTED,
        count_if(STATUS = 'APPROVED') as TOTAL_APPROVED,
        count(distinct PORTFOLIO_ID) as PORTFOLIOS_WITH_PROPOSALS
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where PROPOSED_AT::date = current_date()
),

-- ── Trades across all portfolios ────────────────────────────
trades_agg as (
    select
        count(*) as TOTAL_TRADES,
        sum(case when SIDE = 'BUY' then 1 else 0 end) as TOTAL_BUYS,
        sum(case when SIDE = 'SELL' then 1 else 0 end) as TOTAL_SELLS,
        sum(coalesce(REALIZED_PNL, 0)) as TOTAL_REALIZED_PNL,
        count(distinct PORTFOLIO_ID) as PORTFOLIOS_WITH_TRADES
    from MIP.APP.PORTFOLIO_TRADES
    where TRADE_TS::date = current_date()
),

-- ── Prior global snapshot for delta detection ───────────────
prior_global as (
    select
        SNAPSHOT_JSON,
        AS_OF_TS as PRIOR_AS_OF_TS
    from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
    where SCOPE = 'GLOBAL'
      and PORTFOLIO_ID is null
    order by AS_OF_TS desc
    limit 1
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

-- ===== GLOBAL INTEREST DETECTORS =====

-- G1: Any gate changed across portfolios
det_gate_any_changed as (
    select
        'GATE_CHANGED_ANY' as DETECTOR,
        iff(
            pg.SNAPSHOT_JSON is not null
            and ga.WARN_COUNT != coalesce(pg.SNAPSHOT_JSON:gates:warn_count::number, ga.WARN_COUNT),
            true, false
        ) as FIRED,
        'HIGH' as SEVERITY,
        object_construct(
            'warn_count_now', ga.WARN_COUNT,
            'warn_count_prev', pg.SNAPSHOT_JSON:gates:warn_count::number,
            'blocked_count_now', ga.BLOCKED_COUNT
        ) as DETAIL
    from gate_agg ga
    left join prior_global pg on 1=1
),

-- G2: Trust distribution changed
det_trust_delta as (
    select
        'TRUST_DELTA' as DETECTOR,
        iff(
            pg.SNAPSHOT_JSON is not null
            and td.TRUSTED_COUNT != coalesce(pg.SNAPSHOT_JSON:training:trusted_count::number, td.TRUSTED_COUNT),
            true, false
        ) as FIRED,
        'MEDIUM' as SEVERITY,
        object_construct(
            'trusted_now', td.TRUSTED_COUNT,
            'trusted_prev', pg.SNAPSHOT_JSON:training:trusted_count::number,
            'watch_now', td.WATCH_COUNT,
            'watch_prev', pg.SNAPSHOT_JSON:training:watch_count::number
        ) as DETAIL
    from trust_dist td
    left join prior_global pg on 1=1
),

-- G3: No new bars (system-level)
det_no_new_bars as (
    select
        'NO_NEW_BARS' as DETECTOR,
        iff(
            pf.LATEST_PIPELINE_STATUS in ('SUCCESS_WITH_SKIPS') or sa.TOTAL_SIGNALS = 0,
            true, false
        ) as FIRED,
        'LOW' as SEVERITY,
        object_construct(
            'latest_pipeline_status', pf.LATEST_PIPELINE_STATUS,
            'total_signals_today', sa.TOTAL_SIGNALS,
            'latest_market_ts', lm.LATEST_MARKET_TS
        ) as DETAIL
    from pipeline_freshness pf
    cross join signals_agg sa
    cross join latest_market lm
),

-- G4: Global proposal funnel
det_proposal_funnel_global as (
    select
        'PROPOSAL_FUNNEL_GLOBAL' as DETECTOR,
        true as FIRED,
        'LOW' as SEVERITY,
        object_construct(
            'total_signals', sa.TOTAL_SIGNALS,
            'total_eligible', sa.TOTAL_ELIGIBLE,
            'total_proposed', pa.TOTAL_PROPOSED,
            'total_rejected', pa.TOTAL_REJECTED,
            'total_executed', pa.TOTAL_EXECUTED,
            'biggest_dropoff', case
                when sa.TOTAL_SIGNALS > 0 and sa.TOTAL_ELIGIBLE = 0 then 'SIGNALS_TO_ELIGIBLE'
                when sa.TOTAL_ELIGIBLE > 0 and pa.TOTAL_PROPOSED = 0 then 'ELIGIBLE_TO_PROPOSED'
                when pa.TOTAL_PROPOSED > 0 and pa.TOTAL_EXECUTED = 0 then 'PROPOSED_TO_EXECUTED'
                else 'NONE'
            end
        ) as DETAIL
    from signals_agg sa
    cross join proposals_agg pa
),

-- G5: Nothing happened (system-wide)
det_nothing_happened_global as (
    select
        'NOTHING_HAPPENED' as DETECTOR,
        iff(
            sa.TOTAL_SIGNALS = 0
            and pa.TOTAL_PROPOSED = 0
            and ta.TOTAL_TRADES = 0,
            true, false
        ) as FIRED,
        'LOW' as SEVERITY,
        object_construct(
            'reason', case
                when sa.TOTAL_SIGNALS = 0 then 'NO_SIGNALS_TODAY'
                when pa.TOTAL_PROPOSED = 0 then 'NO_PROPOSALS_TODAY'
                else 'NO_TRADES_TODAY'
            end
        ) as DETAIL
    from signals_agg sa
    cross join proposals_agg pa
    cross join trades_agg ta
),

-- G6: Capacity saturation across system
det_capacity_global as (
    select
        'CAPACITY_GLOBAL' as DETECTOR,
        iff(ca.TOTAL_REMAINING <= 1, true, false) as FIRED,
        iff(ca.TOTAL_REMAINING = 0, 'HIGH', 'MEDIUM') as SEVERITY,
        object_construct(
            'total_max', ca.TOTAL_MAX_POSITIONS,
            'total_open', ca.TOTAL_OPEN,
            'total_remaining', ca.TOTAL_REMAINING,
            'saturation_pct', round(
                iff(ca.TOTAL_MAX_POSITIONS > 0,
                    ca.TOTAL_OPEN * 100.0 / ca.TOTAL_MAX_POSITIONS,
                    0
                ), 1
            )
        ) as DETAIL
    from capacity_agg ca
),

-- G7: Signal count vs yesterday (readiness movers)
det_signal_count_change as (
    select
        'SIGNAL_COUNT_CHANGE' as DETECTOR,
        iff(
            pg.SNAPSHOT_JSON is not null
            and sa.TOTAL_SIGNALS != coalesce(pg.SNAPSHOT_JSON:signals:total_signals::number, sa.TOTAL_SIGNALS),
            true, false
        ) as FIRED,
        'MEDIUM' as SEVERITY,
        object_construct(
            'signals_now', sa.TOTAL_SIGNALS,
            'signals_prev', pg.SNAPSHOT_JSON:signals:total_signals::number,
            'eligible_now', sa.TOTAL_ELIGIBLE,
            'eligible_prev', pg.SNAPSHOT_JSON:signals:total_eligible::number,
            'delta', sa.TOTAL_SIGNALS - coalesce(pg.SNAPSHOT_JSON:signals:total_signals::number, sa.TOTAL_SIGNALS)
        ) as DETAIL
    from signals_agg sa
    left join prior_global pg on 1=1
),

-- Assemble detectors
all_detectors as (
    select DETECTOR, FIRED, SEVERITY, DETAIL from det_gate_any_changed
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_trust_delta
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_no_new_bars
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_proposal_funnel_global
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_nothing_happened_global
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_capacity_global
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_signal_count_change
),
detectors_arr as (
    select array_agg(
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
)

-- ===== FINAL: assemble global snapshot JSON =====
select
    object_construct(
        'scope', 'GLOBAL',
        'timestamps', object_construct(
            'as_of_ts', current_timestamp(),
            'snapshot_created_at', current_timestamp(),
            'latest_market_ts', lm.LATEST_MARKET_TS,
            'latest_pipeline_run_ts', pf.LATEST_PIPELINE_RUN_TS
        ),
        'system', object_construct(
            'active_portfolios', pa2.ACTIVE_PORTFOLIOS,
            'portfolio_summary', pa2.PORTFOLIO_SUMMARY
        ),
        'gates', object_construct(
            'total_portfolios', ga.TOTAL_PORTFOLIOS,
            'ok_count', ga.OK_COUNT,
            'warn_count', ga.WARN_COUNT,
            'blocked_count', ga.BLOCKED_COUNT,
            'total_open_positions', ga.TOTAL_OPEN_POSITIONS,
            'per_portfolio', ga.PER_PORTFOLIO
        ),
        'capacity', object_construct(
            'total_max_positions', ca.TOTAL_MAX_POSITIONS,
            'total_open', ca.TOTAL_OPEN,
            'total_remaining', ca.TOTAL_REMAINING,
            'saturation_pct', round(
                iff(ca.TOTAL_MAX_POSITIONS > 0,
                    ca.TOTAL_OPEN * 100.0 / ca.TOTAL_MAX_POSITIONS,
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
            'total_signals', sa.TOTAL_SIGNALS,
            'total_eligible', sa.TOTAL_ELIGIBLE,
            'total_unique_symbols', sa.TOTAL_UNIQUE_SYMBOLS,
            'by_market_type', sa.BY_MARKET_TYPE,
            'top_ready_symbols', tr.items
        ),
        'proposals', object_construct(
            'total_proposed', prp.TOTAL_PROPOSED,
            'total_rejected', prp.TOTAL_REJECTED,
            'total_executed', prp.TOTAL_EXECUTED,
            'total_approved', prp.TOTAL_APPROVED,
            'portfolios_with_proposals', prp.PORTFOLIOS_WITH_PROPOSALS
        ),
        'trades', object_construct(
            'total_trades', ta.TOTAL_TRADES,
            'total_buys', ta.TOTAL_BUYS,
            'total_sells', ta.TOTAL_SELLS,
            'total_realized_pnl', ta.TOTAL_REALIZED_PNL,
            'portfolios_with_trades', ta.PORTFOLIOS_WITH_TRADES
        ),
        'training', object_construct(
            'trusted_count', td.TRUSTED_COUNT,
            'watch_count', td.WATCH_COUNT,
            'untrusted_count', td.UNTRUSTED_COUNT,
            'total_patterns', td.TOTAL_PATTERNS
        ),
        'patterns', coalesce(pc.PATTERNS, array_construct()),
        'pattern_observations', coalesce(poa.PATTERN_OBS, array_construct()),
        'training_thresholds', object_construct(
            'min_signals', tgt.MIN_SIGNALS,
            'min_hit_rate', tgt.MIN_HIT_RATE,
            'min_avg_return', tgt.MIN_AVG_RETURN
        ),
        'detectors', coalesce(det.DETECTORS, array_construct()),
        'prior_snapshot_ts', pg.PRIOR_AS_OF_TS
    ) as SNAPSHOT_JSON
from portfolio_agg pa2
cross join gate_agg ga
cross join capacity_agg ca
cross join pipeline_freshness pf
cross join latest_market lm
cross join signals_agg sa
cross join top_ready tr
cross join trust_dist td
cross join proposals_agg prp
cross join trades_agg ta
cross join pattern_catalog pc
cross join pattern_obs_agg poa
cross join training_gate_thresholds tgt
cross join detectors_arr det
left join prior_global pg on 1=1;
