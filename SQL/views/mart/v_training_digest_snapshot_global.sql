-- v_training_digest_snapshot_global.sql
-- Purpose: Global (system-wide) deterministic training snapshot for Training Journey Digest.
-- Aggregates training maturity, trust distribution, stage movements, threshold gaps.
-- Returns exactly one row with SNAPSHOT_JSON.
--
-- Sources: V_TRUSTED_SIGNAL_POLICY, V_SIGNAL_OUTCOME_KPIS, REC_TRAINING_KPIS,
--          RECOMMENDATION_LOG, RECOMMENDATION_OUTCOMES, TRAINING_GATE_PARAMS, PATTERN_DEFINITION.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_GLOBAL (
    SNAPSHOT_JSON
) as
with
-- ── Training gate thresholds ────────────────────────────────
gate_params as (
    select
        coalesce(MIN_SIGNALS, 40)          as MIN_SIGNALS,
        coalesce(MIN_SIGNALS_BOOTSTRAP, 5) as MIN_SIGNALS_BOOTSTRAP,
        coalesce(MIN_HIT_RATE, 0.55)       as MIN_HIT_RATE,
        coalesce(MIN_AVG_RETURN, 0.0005)   as MIN_AVG_RETURN
    from MIP.APP.TRAINING_GATE_PARAMS
    where IS_ACTIVE
    qualify row_number() over (order by PARAM_SET) = 1
),

-- ── Per-symbol maturity computation ─────────────────────────
-- Replicates the Python scoring: sample (0-30), coverage (0-40), horizons (0-30)
symbol_recs as (
    select
        r.MARKET_TYPE,
        r.SYMBOL,
        r.PATTERN_ID,
        count(*) as RECS_TOTAL,
        max(r.TS) as LATEST_SIGNAL_TS
    from MIP.APP.RECOMMENDATION_LOG r
    where r.INTERVAL_MINUTES = 1440
    group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID
),
symbol_outcomes as (
    select
        r.MARKET_TYPE,
        r.SYMBOL,
        r.PATTERN_ID,
        count(*) as OUTCOMES_TOTAL,
        count(distinct o.HORIZON_BARS) as HORIZONS_COVERED,
        sum(case when o.EVAL_STATUS = 'SUCCESS' then 1 else 0 end) as SUCCESS_COUNT,
        avg(case when o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as AVG_RETURN
    from MIP.APP.RECOMMENDATION_LOG r
    join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
    where r.INTERVAL_MINUTES = 1440
    group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID
),
symbol_maturity as (
    select
        sr.MARKET_TYPE,
        sr.SYMBOL,
        sr.PATTERN_ID,
        sr.RECS_TOTAL,
        coalesce(so.OUTCOMES_TOTAL, 0) as OUTCOMES_TOTAL,
        coalesce(so.HORIZONS_COVERED, 0) as HORIZONS_COVERED,
        coalesce(so.SUCCESS_COUNT, 0) as SUCCESS_COUNT,
        so.AVG_RETURN,
        sr.LATEST_SIGNAL_TS,
        -- Coverage ratio
        case when sr.RECS_TOTAL > 0 and (sr.RECS_TOTAL * 5) > 0
            then least(1.0, coalesce(so.OUTCOMES_TOTAL, 0)::float / (sr.RECS_TOTAL * 5))
            else 0.0 end as COVERAGE_RATIO,
        -- Maturity score components
        least(30.0, 30.0 * least(1.0, sr.RECS_TOTAL::float / gp.MIN_SIGNALS)) as SCORE_SAMPLE,
        least(40.0, 40.0 * case when sr.RECS_TOTAL > 0 and (sr.RECS_TOTAL * 5) > 0
            then least(1.0, coalesce(so.OUTCOMES_TOTAL, 0)::float / (sr.RECS_TOTAL * 5))
            else 0.0 end) as SCORE_COVERAGE,
        least(30.0, 30.0 * coalesce(so.HORIZONS_COVERED, 0)::float / 5.0) as SCORE_HORIZONS
    from symbol_recs sr
    left join symbol_outcomes so
        on so.MARKET_TYPE = sr.MARKET_TYPE and so.SYMBOL = sr.SYMBOL and so.PATTERN_ID = sr.PATTERN_ID
    cross join gate_params gp
),
symbol_scored as (
    select
        sm.*,
        least(100.0, greatest(0.0, sm.SCORE_SAMPLE + sm.SCORE_COVERAGE + sm.SCORE_HORIZONS)) as MATURITY_SCORE,
        case
            when (sm.SCORE_SAMPLE + sm.SCORE_COVERAGE + sm.SCORE_HORIZONS) < 25 then 'INSUFFICIENT'
            when (sm.SCORE_SAMPLE + sm.SCORE_COVERAGE + sm.SCORE_HORIZONS) < 50 then 'WARMING_UP'
            when (sm.SCORE_SAMPLE + sm.SCORE_COVERAGE + sm.SCORE_HORIZONS) < 75 then 'LEARNING'
            else 'CONFIDENT'
        end as MATURITY_STAGE
    from symbol_maturity sm
),

-- ── Stage distribution ──────────────────────────────────────
stage_dist as (
    select
        count(*) as TOTAL_SYMBOLS,
        count_if(MATURITY_STAGE = 'INSUFFICIENT') as INSUFFICIENT_COUNT,
        count_if(MATURITY_STAGE = 'WARMING_UP') as WARMING_UP_COUNT,
        count_if(MATURITY_STAGE = 'LEARNING') as LEARNING_COUNT,
        count_if(MATURITY_STAGE = 'CONFIDENT') as CONFIDENT_COUNT,
        round(avg(MATURITY_SCORE), 1) as AVG_MATURITY_SCORE,
        round(median(MATURITY_SCORE), 1) as MEDIAN_MATURITY_SCORE,
        sum(RECS_TOTAL) as TOTAL_RECOMMENDATIONS,
        sum(OUTCOMES_TOTAL) as TOTAL_OUTCOMES,
        sum(SUCCESS_COUNT) as TOTAL_SUCCESSES,
        round(avg(COVERAGE_RATIO), 3) as AVG_COVERAGE_RATIO
    from symbol_scored
),

-- ── Trust distribution (from policy view) ───────────────────
trust_dist as (
    select
        count_if(TRUST_LABEL = 'TRUSTED') as TRUSTED_COUNT,
        count_if(TRUST_LABEL = 'WATCH') as WATCH_COUNT,
        count_if(TRUST_LABEL = 'UNTRUSTED') as UNTRUSTED_COUNT,
        count(*) as TOTAL_POLICY_ROWS
    from MIP.MART.V_TRUSTED_SIGNAL_POLICY
),

-- ── Top movers: symbols closest to next stage (near-miss) ──
near_miss_symbols as (
    select coalesce(
        array_agg(
            object_construct(
                'symbol', SYMBOL,
                'market_type', MARKET_TYPE,
                'maturity_score', round(MATURITY_SCORE, 1),
                'maturity_stage', MATURITY_STAGE,
                'gap_to_next', round(
                    case MATURITY_STAGE
                        when 'INSUFFICIENT' then 25.0 - MATURITY_SCORE
                        when 'WARMING_UP' then 50.0 - MATURITY_SCORE
                        when 'LEARNING' then 75.0 - MATURITY_SCORE
                        else 0
                    end, 1
                ),
                'recs_total', RECS_TOTAL,
                'outcomes_total', OUTCOMES_TOTAL,
                'coverage_ratio', round(COVERAGE_RATIO, 3)
            )
        ) within group (order by
            case MATURITY_STAGE
                when 'INSUFFICIENT' then 25.0 - MATURITY_SCORE
                when 'WARMING_UP' then 50.0 - MATURITY_SCORE
                when 'LEARNING' then 75.0 - MATURITY_SCORE
                else 999
            end asc
        ),
        array_construct()
    ) as ITEMS
    from (
        select * from symbol_scored
        where MATURITY_STAGE != 'CONFIDENT'
        order by case MATURITY_STAGE
            when 'LEARNING' then 1
            when 'WARMING_UP' then 2
            when 'INSUFFICIENT' then 3
        end, MATURITY_SCORE desc
        limit 10
    )
),

-- ── Top confident symbols (most ready) ──────────────────────
top_confident as (
    select coalesce(
        array_agg(
            object_construct(
                'symbol', SYMBOL,
                'market_type', MARKET_TYPE,
                'maturity_score', round(MATURITY_SCORE, 1),
                'recs_total', RECS_TOTAL,
                'outcomes_total', OUTCOMES_TOTAL,
                'avg_return', round(coalesce(AVG_RETURN, 0), 6)
            )
        ) within group (order by MATURITY_SCORE desc),
        array_construct()
    ) as ITEMS
    from (
        select * from symbol_scored
        where MATURITY_STAGE = 'CONFIDENT'
        order by MATURITY_SCORE desc
        limit 5
    )
),

-- ── Prior global training snapshot for delta detection ──────
prior_snapshot as (
    select
        SNAPSHOT_JSON,
        AS_OF_TS as PRIOR_AS_OF_TS
    from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
    where SCOPE = 'GLOBAL_TRAINING'
      and SYMBOL is null
    order by AS_OF_TS desc
    limit 1
),

-- ===== TRAINING-SPECIFIC INTEREST DETECTORS =====

-- D1: Stage distribution changed
det_stage_changed as (
    select
        'STAGE_DISTRIBUTION_CHANGED' as DETECTOR,
        iff(
            ps.SNAPSHOT_JSON is not null
            and (
                sd.CONFIDENT_COUNT != coalesce(ps.SNAPSHOT_JSON:stages:confident_count::number, sd.CONFIDENT_COUNT)
                or sd.LEARNING_COUNT != coalesce(ps.SNAPSHOT_JSON:stages:learning_count::number, sd.LEARNING_COUNT)
            ),
            true, false
        ) as FIRED,
        'HIGH' as SEVERITY,
        object_construct(
            'confident_now', sd.CONFIDENT_COUNT,
            'confident_prev', ps.SNAPSHOT_JSON:stages:confident_count::number,
            'learning_now', sd.LEARNING_COUNT,
            'learning_prev', ps.SNAPSHOT_JSON:stages:learning_count::number
        ) as DETAIL
    from stage_dist sd
    left join prior_snapshot ps on 1=1
),

-- D2: Trust distribution changed
det_trust_changed as (
    select
        'TRUST_DISTRIBUTION_CHANGED' as DETECTOR,
        iff(
            ps.SNAPSHOT_JSON is not null
            and td.TRUSTED_COUNT != coalesce(ps.SNAPSHOT_JSON:trust:trusted_count::number, td.TRUSTED_COUNT),
            true, false
        ) as FIRED,
        'HIGH' as SEVERITY,
        object_construct(
            'trusted_now', td.TRUSTED_COUNT,
            'trusted_prev', ps.SNAPSHOT_JSON:trust:trusted_count::number,
            'watch_now', td.WATCH_COUNT,
            'watch_prev', ps.SNAPSHOT_JSON:trust:watch_count::number
        ) as DETAIL
    from trust_dist td
    left join prior_snapshot ps on 1=1
),

-- D3: Large outcomes delta
det_outcomes_delta as (
    select
        'LARGE_OUTCOMES_DELTA' as DETECTOR,
        iff(
            ps.SNAPSHOT_JSON is not null
            and abs(sd.TOTAL_OUTCOMES - coalesce(ps.SNAPSHOT_JSON:stages:total_outcomes::number, sd.TOTAL_OUTCOMES)) > 50,
            true, false
        ) as FIRED,
        'MEDIUM' as SEVERITY,
        object_construct(
            'outcomes_now', sd.TOTAL_OUTCOMES,
            'outcomes_prev', ps.SNAPSHOT_JSON:stages:total_outcomes::number,
            'delta', sd.TOTAL_OUTCOMES - coalesce(ps.SNAPSHOT_JSON:stages:total_outcomes::number, 0)
        ) as DETAIL
    from stage_dist sd
    left join prior_snapshot ps on 1=1
),

-- D4: Stalled training (no new outcomes vs prior)
det_stalled as (
    select
        'TRAINING_STALLED' as DETECTOR,
        iff(
            ps.SNAPSHOT_JSON is not null
            and sd.TOTAL_OUTCOMES = coalesce(ps.SNAPSHOT_JSON:stages:total_outcomes::number, -1),
            true, false
        ) as FIRED,
        'MEDIUM' as SEVERITY,
        object_construct(
            'total_outcomes', sd.TOTAL_OUTCOMES,
            'prior_outcomes', ps.SNAPSHOT_JSON:stages:total_outcomes::number,
            'reason', 'No new outcomes evaluated since prior snapshot'
        ) as DETAIL
    from stage_dist sd
    left join prior_snapshot ps on 1=1
),

-- D5: Near-miss symbols (close to next stage)
det_near_miss as (
    select
        'NEAR_MISS_STAGE_ADVANCE' as DETECTOR,
        iff(count(*) > 0, true, false) as FIRED,
        'MEDIUM' as SEVERITY,
        object_construct(
            'symbols_within_5pts', count(*),
            'closest_symbol', max_by(SYMBOL, MATURITY_SCORE),
            'closest_score', round(max(MATURITY_SCORE), 1)
        ) as DETAIL
    from symbol_scored
    where MATURITY_STAGE != 'CONFIDENT'
      and case MATURITY_STAGE
          when 'INSUFFICIENT' then 25.0 - MATURITY_SCORE
          when 'WARMING_UP' then 50.0 - MATURITY_SCORE
          when 'LEARNING' then 75.0 - MATURITY_SCORE
      end <= 5.0
),

-- D6: Low coverage mismatch (many recs, few outcomes)
det_coverage_mismatch as (
    select
        'COVERAGE_MISMATCH' as DETECTOR,
        iff(count(*) > 0, true, false) as FIRED,
        'LOW' as SEVERITY,
        object_construct(
            'symbols_with_low_coverage', count(*),
            'avg_coverage', round(avg(COVERAGE_RATIO), 3)
        ) as DETAIL
    from symbol_scored
    where RECS_TOTAL >= 20
      and COVERAGE_RATIO < 0.3
),

-- D7: Nothing happened (no new recommendations)
det_nothing_happened as (
    select
        'NO_NEW_TRAINING_DATA' as DETECTOR,
        iff(
            ps.SNAPSHOT_JSON is not null
            and sd.TOTAL_RECOMMENDATIONS = coalesce(ps.SNAPSHOT_JSON:stages:total_recommendations::number, -1)
            and sd.TOTAL_OUTCOMES = coalesce(ps.SNAPSHOT_JSON:stages:total_outcomes::number, -1),
            true, false
        ) as FIRED,
        'LOW' as SEVERITY,
        object_construct(
            'total_recs', sd.TOTAL_RECOMMENDATIONS,
            'total_outcomes', sd.TOTAL_OUTCOMES,
            'reason', 'No new recommendations or outcomes since prior snapshot'
        ) as DETAIL
    from stage_dist sd
    left join prior_snapshot ps on 1=1
),

-- Assemble detectors
all_detectors as (
    select DETECTOR, FIRED, SEVERITY, DETAIL from det_stage_changed
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_trust_changed
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_outcomes_delta
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_stalled
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_near_miss
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_coverage_mismatch
    union all select DETECTOR, FIRED, SEVERITY, DETAIL from det_nothing_happened
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

-- ===== FINAL: assemble global training snapshot JSON =====
select
    object_construct(
        'scope', 'GLOBAL_TRAINING',
        'timestamps', object_construct(
            'as_of_ts', current_timestamp(),
            'snapshot_created_at', current_timestamp()
        ),
        'thresholds', object_construct(
            'min_signals', gp.MIN_SIGNALS,
            'min_signals_bootstrap', gp.MIN_SIGNALS_BOOTSTRAP,
            'min_hit_rate', gp.MIN_HIT_RATE,
            'min_avg_return', gp.MIN_AVG_RETURN
        ),
        'stages', object_construct(
            'total_symbols', sd.TOTAL_SYMBOLS,
            'insufficient_count', sd.INSUFFICIENT_COUNT,
            'warming_up_count', sd.WARMING_UP_COUNT,
            'learning_count', sd.LEARNING_COUNT,
            'confident_count', sd.CONFIDENT_COUNT,
            'avg_maturity_score', sd.AVG_MATURITY_SCORE,
            'median_maturity_score', sd.MEDIAN_MATURITY_SCORE,
            'total_recommendations', sd.TOTAL_RECOMMENDATIONS,
            'total_outcomes', sd.TOTAL_OUTCOMES,
            'total_successes', sd.TOTAL_SUCCESSES,
            'avg_coverage_ratio', sd.AVG_COVERAGE_RATIO
        ),
        'trust', object_construct(
            'trusted_count', td.TRUSTED_COUNT,
            'watch_count', td.WATCH_COUNT,
            'untrusted_count', td.UNTRUSTED_COUNT,
            'total_policy_rows', td.TOTAL_POLICY_ROWS
        ),
        'near_miss_symbols', nm.ITEMS,
        'top_confident_symbols', tc.ITEMS,
        'detectors', coalesce(det.DETECTORS, array_construct()),
        'prior_snapshot_ts', ps.PRIOR_AS_OF_TS
    ) as SNAPSHOT_JSON
from stage_dist sd
cross join trust_dist td
cross join gate_params gp
cross join near_miss_symbols nm
cross join top_confident tc
cross join detectors_arr det
left join prior_snapshot ps on 1=1;
