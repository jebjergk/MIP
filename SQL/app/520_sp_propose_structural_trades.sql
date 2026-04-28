/*  ================================================================
    520_sp_propose_structural_trades.sql
    MIP Structural Strategy Framework — Trade Proposals
    Phase 4a: Generate ranked trade proposals from ELIGIBLE setups
    using trust, regime, significance, and confidence scoring.
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_PROPOSE_STRUCTURAL_TRADES(
    P_PORTFOLIO_ID  NUMBER  DEFAULT NULL,
    P_MAX_PROPOSALS INTEGER DEFAULT 8,   -- Phase 2: raised from 5 to 8
    P_AS_OF_DATE    DATE    DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_as_of       DATE := COALESCE(P_AS_OF_DATE, CURRENT_DATE());
    v_run_start   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_proposals   INTEGER := 0;
BEGIN

    -- ============================================================
    -- STEP 1: Seed risk policy if empty
    -- ============================================================
    MERGE INTO MIP.APP.STRUCTURAL_RISK_POLICY tgt
    USING (
        SELECT * FROM (VALUES
            ('BREAKOUT_RETEST_LONG','LONG','CONFIRMED_CLOSE_BEYOND',0.3,5.0,'MFE_RISK_MULTIPLE',1.0,'STRUCTURAL','{}','OPEN_RUNNER',20,TRUE,'1.0'),
            ('SUPPORT_WICK_LONG','LONG','SINGLE_CLOSE_BEYOND',0.0,4.0,'MFE_RISK_MULTIPLE',1.5,'PROGRESS_BASED','{"breakeven_at":1.0,"lock_50_at":2.0}','STAGED_PARTIAL',15,TRUE,'1.0'),
            ('THREE_BAR_REVERSAL_LONG','LONG','SINGLE_CLOSE_BEYOND',0.0,4.0,'STRUCTURAL_LEVEL_BREAK',NULL,'HYBRID','{"switch_at":2.0}','STAGED_PARTIAL',20,TRUE,'1.0'),
            ('TREND_PULLBACK_LONG','LONG','SINGLE_CLOSE_BEYOND',0.0,3.0,'STRUCTURAL_LEVEL_BREAK',NULL,'STRUCTURAL','{}','OPEN_RUNNER',20,TRUE,'1.0'),
            ('BREAKDOWN_RETEST_SHORT','SHORT','CONFIRMED_CLOSE_BEYOND',0.3,5.0,'MFE_RISK_MULTIPLE',1.0,'PROGRESS_BASED','{"breakeven_at":1.0,"lock_50_at":2.0,"lock_60_at":3.0}','STAGED_PARTIAL',20,TRUE,'1.0'),
            ('RESISTANCE_WICK_SHORT','SHORT','SINGLE_CLOSE_BEYOND',0.0,4.0,'MFE_RISK_MULTIPLE',1.5,'PROGRESS_BASED','{"breakeven_at":1.0,"lock_50_at":2.0}','STAGED_PARTIAL',15,TRUE,'1.0'),
            ('THREE_BAR_REVERSAL_SHORT','SHORT','SINGLE_CLOSE_BEYOND',0.0,4.0,'STRUCTURAL_LEVEL_BREAK',NULL,'HYBRID','{"switch_at":1.5}','STAGED_PARTIAL',20,TRUE,'1.0'),
            ('FAILED_BREAKOUT_SHORT','SHORT','SINGLE_CLOSE_BEYOND',0.0,3.0,'IMMEDIATE',NULL,'STRUCTURAL','{"fast_escalation":true}','STAGED_PARTIAL',15,TRUE,'1.0')
        ) AS v(SF,DIR,PIR,IBUF,MIDP,TAT,TAP,TS,TP,ES,MHB,IA,PV)
    ) src
    ON  tgt.SETUP_FAMILY = src.SF AND tgt.DIRECTION = src.DIR AND tgt.POLICY_VERSION = src.PV
    WHEN NOT MATCHED THEN INSERT (
        SETUP_FAMILY, DIRECTION, PRICE_INVALIDATION_RULE, INVALIDATION_BUFFER_ATR,
        MAX_INVALIDATION_DISTANCE_PCT, TRAIL_ACTIVATION_TYPE, TRAIL_ACTIVATION_PARAM,
        TRAIL_STYLE, TRAIL_PARAMS, EXIT_STYLE, MAX_HOLD_BARS, IS_ACTIVE, POLICY_VERSION
    ) VALUES (
        src.SF, src.DIR, src.PIR, src.IBUF, src.MIDP, src.TAT, src.TAP,
        src.TS, PARSE_JSON(src.TP), src.ES, src.MHB, src.IA, src.PV
    );

    -- ============================================================
    -- STEP 2: Generate proposals from ELIGIBLE setups
    --   Rank by composite score: trust + regime + significance + confidence
    -- ============================================================
    INSERT INTO MIP.APP.STRUCTURAL_TRADE_PROPOSALS (
        SETUP_EVENT_ID, PORTFOLIO_ID, SYMBOL, DIRECTION, SETUP_FAMILY,
        ENTRY_ZONE_LOW, ENTRY_ZONE_HIGH,
        PRICE_INVALIDATION_LEVEL, INVALIDATION_RULE,
        TRAIL_STYLE, TRAIL_PARAMS, EXIT_STYLE,
        STRUCTURE_CONFIDENCE, LEVEL_SIGNIFICANCE, REGIME_COMPAT,
        MEANINGFUL_HIT_RATE, PATH_SURVIVAL_HIT_RATE, MFE_MAE_RATIO,
        RISK_CLASS, CONFLICT_RESOLUTION, RATIONALE_TEXT,
        COMMITTEE_PAYLOAD, STATUS
    )
    WITH eligible_setups AS (
        SELECT
            se.*,
            -- Get trust metrics for this family/market_type
            tr.MEANINGFUL_HIT_RATE AS TRUST_MHR,
            tr.PATH_SURVIVAL_HIT_RATE AS TRUST_PSHR,
            tr.MFE_MAE_RATIO AS TRUST_RATIO,
            tr.TRUST_LABEL,
            tr.TRAIL_STRATEGY_RECOMMENDATION,
            tr.EXIT_STYLE_RECOMMENDATION,
            -- Get risk policy
            rp.EXIT_STYLE AS POLICY_EXIT_STYLE,
            rp.TRAIL_STYLE AS POLICY_TRAIL_STYLE,
            rp.TRAIL_PARAMS AS POLICY_TRAIL_PARAMS,
            -- Path stats
            ps.MEDIAN_MFE, ps.MEDIAN_MAE, ps.PCT_ADVERSE_BEFORE_FAVORABLE,
            ps.GAP_RISK_CONTRIBUTION,
            -- Composite ranking score (Phase 2 v1: trust weight added, weights rebalanced)
            -- Weights: SC=0.25, LS=0.17, REGIME=0.20, MHR=0.18, PSHR=0.05
            --          TRUST: TRUSTED=+0.15, PROVISIONAL=+0.08, RESEARCH=+0.00
            -- Total for TRUSTED~=0.85, PROVISIONAL~=0.78, RESEARCH~=0.70 (GOOD regime, avg signals)
            COALESCE(se.STRUCTURE_CONFIDENCE, 0.5) * 0.25
            + COALESCE(se.LEVEL_SIGNIFICANCE, 0.3) * 0.17
            + CASE WHEN se.REGIME_COMPAT = 'GOOD'    THEN 0.20
                   WHEN se.REGIME_COMPAT = 'NEUTRAL' THEN 0.10
                   ELSE 0.0 END
            + COALESCE(tr.MEANINGFUL_HIT_RATE, 0.3)    * 0.18
            + COALESCE(tr.PATH_SURVIVAL_HIT_RATE, 0.2) * 0.05
            + CASE WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'TRUSTED'     THEN 0.15
                   WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'PROVISIONAL' THEN 0.08
                   ELSE 0.0 END
            AS COMPOSITE_SCORE,

            -- Global rank (used as overall top-N floor)
            ROW_NUMBER() OVER (
                ORDER BY
                    COALESCE(se.STRUCTURE_CONFIDENCE, 0.5) * 0.25
                    + COALESCE(se.LEVEL_SIGNIFICANCE, 0.3) * 0.17
                    + CASE WHEN se.REGIME_COMPAT = 'GOOD'    THEN 0.20
                           WHEN se.REGIME_COMPAT = 'NEUTRAL' THEN 0.10
                           ELSE 0.0 END
                    + COALESCE(tr.MEANINGFUL_HIT_RATE, 0.3)    * 0.18
                    + COALESCE(tr.PATH_SURVIVAL_HIT_RATE, 0.2) * 0.05
                    + CASE WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'TRUSTED'     THEN 0.15
                           WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'PROVISIONAL' THEN 0.08
                           ELSE 0.0 END
                DESC
            ) AS RANK_N,

            -- Per-direction rank for soft directional ceiling (Phase 2: max 75% per direction)
            ROW_NUMBER() OVER (
                PARTITION BY se.DIRECTION
                ORDER BY
                    COALESCE(se.STRUCTURE_CONFIDENCE, 0.5) * 0.25
                    + COALESCE(se.LEVEL_SIGNIFICANCE, 0.3) * 0.17
                    + CASE WHEN se.REGIME_COMPAT = 'GOOD'    THEN 0.20
                           WHEN se.REGIME_COMPAT = 'NEUTRAL' THEN 0.10
                           ELSE 0.0 END
                    + COALESCE(tr.MEANINGFUL_HIT_RATE, 0.3)    * 0.18
                    + COALESCE(tr.PATH_SURVIVAL_HIT_RATE, 0.2) * 0.05
                    + CASE WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'TRUSTED'     THEN 0.15
                           WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'PROVISIONAL' THEN 0.08
                           ELSE 0.0 END
                DESC
            ) AS RANK_N_DIR
        FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se
        LEFT JOIN MIP.APP.STRUCTURAL_SETUP_TRUST tr
          ON tr.SETUP_FAMILY = se.SETUP_FAMILY
         AND tr.MARKET_TYPE = se.MARKET_TYPE
         AND tr.EVAL_WINDOW = 20  -- use best window
        LEFT JOIN MIP.APP.STRUCTURAL_RISK_POLICY rp
          ON rp.SETUP_FAMILY = se.SETUP_FAMILY AND rp.DIRECTION = se.DIRECTION AND rp.IS_ACTIVE = TRUE
        LEFT JOIN MIP.APP.STRUCTURAL_PATH_STATS ps
          ON ps.SETUP_FAMILY = se.SETUP_FAMILY AND ps.MARKET_TYPE = se.MARKET_TYPE AND ps.EVAL_WINDOW = 20
        WHERE se.SETUP_STATUS IN ('DETECTED', 'ELIGIBLE')
          AND se.SETUP_DATE >= DATEADD('day', -5, :v_as_of)
          AND COALESCE(tr.TRUST_LABEL, 'RESEARCH') IN ('TRUSTED', 'PROVISIONAL', 'RESEARCH')
          AND COALESCE(se.ENTRY_ZONE_HIGH, 0) >= COALESCE(se.ENTRY_ZONE_LOW, 0)  -- Phase 1 sanity guard: exclude any inverted zones that survived detection
    )
    SELECT
        es.SETUP_EVENT_ID,
        :P_PORTFOLIO_ID,
        es.SYMBOL,
        es.DIRECTION,
        es.SETUP_FAMILY,
        es.ENTRY_ZONE_LOW,
        es.ENTRY_ZONE_HIGH,
        es.PRICE_INVALIDATION_LEVEL,
        es.INVALIDATION_RULE,
        COALESCE(es.POLICY_TRAIL_STYLE, es.TRAIL_STYLE),
        COALESCE(es.POLICY_TRAIL_PARAMS, es.TRAIL_PARAMS),
        COALESCE(es.POLICY_EXIT_STYLE, es.EXIT_STYLE_RECOMMENDATION, 'STRUCTURAL_TARGET'),
        es.STRUCTURE_CONFIDENCE,
        es.LEVEL_SIGNIFICANCE,
        es.REGIME_COMPAT,
        es.TRUST_MHR,
        es.TRUST_PSHR,
        es.TRUST_RATIO,
        es.RISK_CLASS,
        NULL,  -- CONFLICT_RESOLUTION
        -- RATIONALE_TEXT
        es.SETUP_FAMILY || ' on ' || es.SYMBOL
            || ' | Trust: ' || COALESCE(es.TRUST_LABEL, 'UNKNOWN')
            || ' | MHR: ' || COALESCE(ROUND(es.TRUST_MHR * 100, 1)::VARCHAR, '?') || '%'
            || ' | Conf: ' || ROUND(es.STRUCTURE_CONFIDENCE, 2)
            || ' | Level Sig: ' || COALESCE(ROUND(es.LEVEL_SIGNIFICANCE, 2)::VARCHAR, '?')
            || ' | Regime: ' || es.REGIME_COMPAT
            || ' | Rank: ' || es.RANK_N,
        -- COMMITTEE_PAYLOAD
        OBJECT_CONSTRUCT(
            'setup_family', es.SETUP_FAMILY,
            'direction', es.DIRECTION,
            'symbol', es.SYMBOL,
            'structural_state', es.STRUCTURAL_STATE,
            'entry_zone', ARRAY_CONSTRUCT(es.ENTRY_ZONE_LOW, es.ENTRY_ZONE_HIGH),
            'invalidation', es.PRICE_INVALIDATION_LEVEL,
            'confidence', es.STRUCTURE_CONFIDENCE,
            'level_significance', es.LEVEL_SIGNIFICANCE,
            'regime_compat', es.REGIME_COMPAT,
            'trust_label', COALESCE(es.TRUST_LABEL, 'UNKNOWN'),
            'meaningful_hit_rate', es.TRUST_MHR,
            'path_survival_rate', es.TRUST_PSHR,
            'median_mfe', es.MEDIAN_MFE,
            'median_mae', es.MEDIAN_MAE,
            'pct_adverse_before_favorable', es.PCT_ADVERSE_BEFORE_FAVORABLE,
            'gap_risk', es.GAP_RISK_CONTRIBUTION,
            'composite_score', es.COMPOSITE_SCORE
        ),
        'PROPOSED'
    FROM eligible_setups es
    -- Phase 2: top-N global floor + soft directional ceiling (max 75% per direction)
    -- Allows 6/2 or 5/3 splits; prevents 8/0 monopoly; never forces 4/4 symmetry
    WHERE es.RANK_N <= :P_MAX_PROPOSALS
      AND es.RANK_N_DIR <= FLOOR(:P_MAX_PROPOSALS * 0.75);

    -- Committee 2.0: immutable proposal snapshot (one row per new proposal this run)
    INSERT INTO MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT (
        PROPOSAL_ID, PROPOSAL_TS, SYMBOL, SIDE, SETUP_FAMILY,
        STRUCTURAL_STATE, REGIME_STATE, TRUST_LABEL,
        ENTRY_ZONE_JSON, INVALIDATION_JSON, PATH_METRICS_JSON, MFE_MAE_JSON,
        TRAILING_STYLE, PROPOSAL_SUMMARY_JSON
    )
    SELECT
        p.PROPOSAL_ID,
        p.CREATED_AT,
        p.SYMBOL,
        p.DIRECTION,
        p.SETUP_FAMILY,
        p.COMMITTEE_PAYLOAD:structural_state::STRING,
        p.REGIME_COMPAT,
        COALESCE(p.COMMITTEE_PAYLOAD:trust_label::STRING, 'UNKNOWN'),
        OBJECT_CONSTRUCT('low', p.ENTRY_ZONE_LOW, 'high', p.ENTRY_ZONE_HIGH),
        OBJECT_CONSTRUCT('level', p.PRICE_INVALIDATION_LEVEL, 'rule', p.INVALIDATION_RULE),
        OBJECT_CONSTRUCT(
            'meaningful_hit_rate', p.MEANINGFUL_HIT_RATE,
            'path_survival_hit_rate', p.PATH_SURVIVAL_HIT_RATE,
            'mfe_mae_ratio', p.MFE_MAE_RATIO,
            'median_mfe', p.COMMITTEE_PAYLOAD:median_mfe,
            'median_mae', p.COMMITTEE_PAYLOAD:median_mae,
            'pct_adverse_before_favorable', p.COMMITTEE_PAYLOAD:pct_adverse_before_favorable,
            'gap_risk', p.COMMITTEE_PAYLOAD:gap_risk
        ),
        OBJECT_CONSTRUCT(
            'mfe_mae_ratio', p.MFE_MAE_RATIO,
            'meaningful_hit_rate', p.MEANINGFUL_HIT_RATE,
            'path_survival_hit_rate', p.PATH_SURVIVAL_HIT_RATE
        ),
        p.TRAIL_STYLE,
        p.COMMITTEE_PAYLOAD
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
    WHERE p.CREATED_AT >= :v_run_start
      AND NOT EXISTS (
          SELECT 1 FROM MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT s
          WHERE s.PROPOSAL_ID = p.PROPOSAL_ID
      );

    SELECT COUNT(*) INTO :v_proposals
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    WHERE CREATED_AT >= :v_run_start;

    RETURN OBJECT_CONSTRUCT(
        'status',      'SUCCESS',
        'as_of_date',  :v_as_of,
        'proposals',   :v_proposals,
        'elapsed_sec', DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;
