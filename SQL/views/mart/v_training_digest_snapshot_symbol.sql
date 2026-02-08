-- v_training_digest_snapshot_symbol.sql
-- Purpose: Per-symbol deterministic training snapshot for Training Journey Digest.
-- One row per (SYMBOL, MARKET_TYPE) with training facts, threshold gaps, stage info.
--
-- Sources: RECOMMENDATION_LOG, RECOMMENDATION_OUTCOMES, V_TRUSTED_SIGNAL_POLICY,
--          V_SIGNAL_OUTCOME_KPIS, TRAINING_GATE_PARAMS.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL (
    SYMBOL,
    MARKET_TYPE,
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

-- ── Per-symbol recommendation counts ────────────────────────
symbol_recs as (
    select
        r.MARKET_TYPE,
        r.SYMBOL,
        r.PATTERN_ID,
        count(*) as RECS_TOTAL,
        max(r.TS) as LATEST_SIGNAL_TS,
        min(r.TS) as FIRST_SIGNAL_TS
    from MIP.APP.RECOMMENDATION_LOG r
    where r.INTERVAL_MINUTES = 1440
    group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID
),

-- ── Per-symbol outcome aggregates ───────────────────────────
symbol_outcomes as (
    select
        r.MARKET_TYPE,
        r.SYMBOL,
        r.PATTERN_ID,
        count(*) as OUTCOMES_TOTAL,
        count(distinct o.HORIZON_BARS) as HORIZONS_COVERED,
        sum(case when o.EVAL_STATUS = 'SUCCESS' then 1 else 0 end) as SUCCESS_COUNT,
        sum(case when o.EVAL_STATUS = 'SUCCESS' and o.HIT_FLAG then 1 else 0 end) as HIT_COUNT,
        avg(case when o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as AVG_RETURN,
        max(o.CALCULATED_AT) as LATEST_EVAL_TS,
        -- Per-horizon outcomes
        sum(case when o.HORIZON_BARS = 1 and o.EVAL_STATUS = 'SUCCESS' then 1 else 0 end) as H1_COUNT,
        avg(case when o.HORIZON_BARS = 1 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as H1_AVG_RETURN,
        sum(case when o.HORIZON_BARS = 5 and o.EVAL_STATUS = 'SUCCESS' then 1 else 0 end) as H5_COUNT,
        avg(case when o.HORIZON_BARS = 5 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as H5_AVG_RETURN,
        sum(case when o.HORIZON_BARS = 20 and o.EVAL_STATUS = 'SUCCESS' then 1 else 0 end) as H20_COUNT,
        avg(case when o.HORIZON_BARS = 20 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as H20_AVG_RETURN
    from MIP.APP.RECOMMENDATION_LOG r
    join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
    where r.INTERVAL_MINUTES = 1440
    group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID
),

-- ── Trust labels from policy view (one per pattern_id + market_type) ──
trust_labels as (
    select
        v.MARKET_TYPE,
        v.PATTERN_ID,
        v.TRUST_LABEL,
        v.RECOMMENDED_ACTION,
        v.REASON
    from MIP.MART.V_TRUSTED_SIGNAL_POLICY v
    qualify row_number() over (partition by v.PATTERN_ID, v.MARKET_TYPE order by v.AS_OF_TS desc) = 1
),

-- ── Compute maturity per (symbol, market_type, pattern_id) ──
symbol_maturity as (
    select
        sr.MARKET_TYPE,
        sr.SYMBOL,
        sr.PATTERN_ID,
        sr.RECS_TOTAL,
        coalesce(so.OUTCOMES_TOTAL, 0) as OUTCOMES_TOTAL,
        coalesce(so.HORIZONS_COVERED, 0) as HORIZONS_COVERED,
        coalesce(so.SUCCESS_COUNT, 0) as SUCCESS_COUNT,
        coalesce(so.HIT_COUNT, 0) as HIT_COUNT,
        so.AVG_RETURN,
        sr.LATEST_SIGNAL_TS,
        sr.FIRST_SIGNAL_TS,
        so.LATEST_EVAL_TS,
        -- Coverage
        case when sr.RECS_TOTAL > 0 and (sr.RECS_TOTAL * 5) > 0
            then least(1.0, coalesce(so.OUTCOMES_TOTAL, 0)::float / (sr.RECS_TOTAL * 5))
            else 0.0 end as COVERAGE_RATIO,
        -- Hit rate
        case when coalesce(so.SUCCESS_COUNT, 0) > 0
            then coalesce(so.HIT_COUNT, 0)::float / so.SUCCESS_COUNT
            else null end as HIT_RATE,
        -- Maturity components
        least(30.0, 30.0 * least(1.0, sr.RECS_TOTAL::float / gp.MIN_SIGNALS)) as SCORE_SAMPLE,
        least(40.0, 40.0 * case when sr.RECS_TOTAL > 0 and (sr.RECS_TOTAL * 5) > 0
            then least(1.0, coalesce(so.OUTCOMES_TOTAL, 0)::float / (sr.RECS_TOTAL * 5))
            else 0.0 end) as SCORE_COVERAGE,
        least(30.0, 30.0 * coalesce(so.HORIZONS_COVERED, 0)::float / 5.0) as SCORE_HORIZONS,
        -- Per-horizon detail
        so.H1_COUNT, so.H1_AVG_RETURN,
        so.H5_COUNT, so.H5_AVG_RETURN,
        so.H20_COUNT, so.H20_AVG_RETURN,
        -- Thresholds
        gp.MIN_SIGNALS,
        gp.MIN_HIT_RATE,
        gp.MIN_AVG_RETURN
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
)

-- ===== FINAL: one row per (SYMBOL, MARKET_TYPE) =====
select
    ss.SYMBOL,
    ss.MARKET_TYPE,
    object_construct(
        'scope', 'SYMBOL_TRAINING',
        'symbol', ss.SYMBOL,
        'market_type', ss.MARKET_TYPE,
        'pattern_id', ss.PATTERN_ID,
        'timestamps', object_construct(
            'as_of_ts', current_timestamp(),
            'first_signal_ts', ss.FIRST_SIGNAL_TS,
            'latest_signal_ts', ss.LATEST_SIGNAL_TS,
            'latest_eval_ts', ss.LATEST_EVAL_TS
        ),
        'maturity', object_construct(
            'score', round(ss.MATURITY_SCORE, 1),
            'stage', ss.MATURITY_STAGE,
            'score_sample', round(ss.SCORE_SAMPLE, 1),
            'score_coverage', round(ss.SCORE_COVERAGE, 1),
            'score_horizons', round(ss.SCORE_HORIZONS, 1)
        ),
        'evidence', object_construct(
            'recs_total', ss.RECS_TOTAL,
            'outcomes_total', ss.OUTCOMES_TOTAL,
            'success_count', ss.SUCCESS_COUNT,
            'hit_count', ss.HIT_COUNT,
            'horizons_covered', ss.HORIZONS_COVERED,
            'coverage_ratio', round(ss.COVERAGE_RATIO, 3),
            'hit_rate', round(coalesce(ss.HIT_RATE, 0), 4),
            'avg_return', round(coalesce(ss.AVG_RETURN, 0), 6)
        ),
        'threshold_gaps', object_construct(
            'min_signals', ss.MIN_SIGNALS,
            'signals_gap', greatest(0, ss.MIN_SIGNALS - ss.RECS_TOTAL),
            'signals_met', ss.RECS_TOTAL >= ss.MIN_SIGNALS,
            'min_hit_rate', ss.MIN_HIT_RATE,
            'hit_rate_gap', round(greatest(0, ss.MIN_HIT_RATE - coalesce(ss.HIT_RATE, 0)), 4),
            'hit_rate_met', coalesce(ss.HIT_RATE, 0) >= ss.MIN_HIT_RATE,
            'min_avg_return', ss.MIN_AVG_RETURN,
            'avg_return_gap', round(greatest(0, ss.MIN_AVG_RETURN - coalesce(ss.AVG_RETURN, 0)), 6),
            'avg_return_met', coalesce(ss.AVG_RETURN, 0) >= ss.MIN_AVG_RETURN
        ),
        'horizons', object_construct(
            'h1_count', coalesce(ss.H1_COUNT, 0),
            'h1_avg_return', round(coalesce(ss.H1_AVG_RETURN, 0), 6),
            'h5_count', coalesce(ss.H5_COUNT, 0),
            'h5_avg_return', round(coalesce(ss.H5_AVG_RETURN, 0), 6),
            'h20_count', coalesce(ss.H20_COUNT, 0),
            'h20_avg_return', round(coalesce(ss.H20_AVG_RETURN, 0), 6)
        ),
        'trust', object_construct(
            'trust_label', coalesce(tl.TRUST_LABEL, 'UNKNOWN'),
            'recommended_action', coalesce(tl.RECOMMENDED_ACTION, 'UNKNOWN'),
            'reason', tl.REASON
        ),
        'journey_stage', case ss.MATURITY_STAGE
            when 'INSUFFICIENT' then 'Collecting evidence'
            when 'WARMING_UP' then 'Evaluating outcomes'
            when 'LEARNING' then 'Earning trust'
            when 'CONFIDENT' then 'Trade-eligible'
        end
    ) as SNAPSHOT_JSON
from symbol_scored ss
left join trust_labels tl
    on tl.MARKET_TYPE = ss.MARKET_TYPE
    and tl.PATTERN_ID = ss.PATTERN_ID;
