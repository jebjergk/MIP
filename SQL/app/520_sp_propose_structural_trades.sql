/*  ================================================================
    520_sp_propose_structural_trades.sql
    MIP Structural Strategy Framework — Trade Proposals
    Phase 4a: Generate ranked trade proposals from ELIGIBLE setups
    using trust, regime, significance, and confidence scoring.

    Phase 6+ Blocker Remediation (Apr 2026):
      F9  — Freshness boost: same-day setups receive +0.05 composite bonus
            so fresh detections are not crowded out by older eligibles in
            the 5-day lookback window.
      F10 — Cross-run dedup: proposal insert is now MERGE keyed on
            (SETUP_EVENT_ID, PORTFOLIO_ID). The same setup is never
            re-proposed across runs.
      C   — BRL conditional package:
              C1 trust treatment: BREAKOUT_RETEST_LONG uses GREATEST(MHR,PSHR)
                  for the path-component, plus a calibrated +0.04 trust bonus
                  reflecting strong MHR despite structurally low PSHR.
              C2 exit style: BRL is updated to STAGED_PARTIAL with
                  breakeven_at=1.0 / lock_50_at=2.0 trail params via a
                  targeted UPDATE after the seed MERGE.
              C3 gap-risk-aware sizing: BRL proposals are forced to RISK_CLASS
                  'GAP_AWARE' (9-char token to fit TEXT(10) column) which is
                  honored by structural_committee.py compute_structural_thesis_assessment
                  with a 0.50x size_mult clamp (RISK_CLASS_GAP_AWARE_SIZE_CLAMP).
                  The legacy sizing_multiplier=0.5 + gap_risk_aware=true fields
                  are still carried in COMMITTEE_PAYLOAD for diagnostic/audit
                  purposes, but binding now happens through RISK_CLASS rather
                  than the payload field. RATIONALE_TEXT annotated for operator clarity.
      D   — Explicit SHORT freeze: eligible_setups WHERE clause restricted to
            DIRECTION='LONG'. Lift criterion: at least one SHORT family must
            achieve MFE/MAE >= 1.0 over the trust evaluation window before
            this filter is removed.
      E   — Explicit FX exclusion: eligible_setups WHERE clause excludes
            MARKET_TYPE='FX'. The detector emits FX setups for analytics but
            the operating account does not trade FX. NULL/unknown MARKET_TYPE
            is treated as STOCK to preserve behavior on unclassified equity
            setups. Lift criterion: define an FX execution / risk-treatment
            path in the broker bridge before lifting.

    Phase 7 PQI Fix 3 (Apr 2026):
      Symbol-level recent-trade cooldown. eligible_setups WHERE clause
      now suppresses any symbol that already has a STRUCTURAL LIVE_ACTION
      either currently in-flight (PENDING_OPEN_VALIDATION,
      EXECUTION_REQUESTED, REVALIDATED_PASS, INTENT_APPROVED, OPEN_BLOCKED,
      PROPOSED) or recently closed within the cooldown window (default 7
      calendar days, controlled by P_SYMBOL_COOLDOWN_DAYS). This prevents
      same-symbol reproposal immediately after a trade was sent, executed,
      or rejected — the dominant root cause of ORCL/COST style noise. NULL
      P_PORTFOLIO_ID is portfolio-agnostic (cooldown applies across all
      portfolios). Lift criterion: only relax once a discretionary
      override path exists (operator-acknowledged "yes, propose again").
    ================================================================ */

CREATE OR REPLACE PROCEDURE MIP.APP.SP_PROPOSE_STRUCTURAL_TRADES(
    P_PORTFOLIO_ID         NUMBER  DEFAULT NULL,
    P_MAX_PROPOSALS        INTEGER DEFAULT 8,   -- Phase 2: raised from 5 to 8
    P_AS_OF_DATE           DATE    DEFAULT NULL,
    P_SYMBOL_COOLDOWN_DAYS INTEGER DEFAULT 7    -- Phase 7 PQI Fix 3
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
    -- STEP 1b: Phase 6 C2 — BRL exit style upgrade
    -- BRL produces strong meaningful-move success (71.7%) but the
    -- OPEN_RUNNER style lets early gains evaporate. Switch to
    -- STAGED_PARTIAL with breakeven_at=1.0 / lock_50_at=2.0.
    -- Targeted UPDATE because seed MERGE only INSERTs WHEN NOT MATCHED.
    -- ============================================================
    UPDATE MIP.APP.STRUCTURAL_RISK_POLICY
       SET TRAIL_STYLE   = 'PROGRESS_BASED',
           TRAIL_PARAMS  = PARSE_JSON('{"breakeven_at":1.0,"lock_50_at":2.0}'),
           EXIT_STYLE    = 'STAGED_PARTIAL'
     WHERE SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'
       AND DIRECTION    = 'LONG'
       AND IS_ACTIVE    = TRUE;

    -- ============================================================
    -- STEP 2: Generate proposals from ELIGIBLE setups
    --   Rank by composite score: trust + regime + significance + confidence
    --   F10: MERGE prevents cross-run duplicates by (SETUP_EVENT_ID, PORTFOLIO_ID)
    -- ============================================================
    MERGE INTO MIP.APP.STRUCTURAL_TRADE_PROPOSALS tgt
    USING (
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
                -- Composite ranking score (Phase 6 v2)
                -- Base weights: SC=0.25, LS=0.17, REGIME=0.20, MHR=0.18, PSHR=0.05
                -- Trust bonus:  TRUSTED=+0.15, PROVISIONAL=+0.08, RESEARCH=+0.00
                -- Phase 6 F9:   +0.05 freshness boost when SETUP_DATE = AS_OF
                -- Phase 6 C1:   BRL-specific overrides:
                --                 path component uses GREATEST(MHR,PSHR) instead of PSHR
                --                 +0.04 calibrated trust bonus (mid-tier between RESEARCH and PROVISIONAL)
                COALESCE(se.STRUCTURE_CONFIDENCE, 0.5) * 0.25
                + COALESCE(se.LEVEL_SIGNIFICANCE, 0.3) * 0.17
                + CASE WHEN se.REGIME_COMPAT = 'GOOD'    THEN 0.20
                       WHEN se.REGIME_COMPAT = 'NEUTRAL' THEN 0.10
                       ELSE 0.0 END
                + COALESCE(tr.MEANINGFUL_HIT_RATE, 0.3)    * 0.18
                + CASE WHEN se.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'
                       THEN GREATEST(COALESCE(tr.MEANINGFUL_HIT_RATE, 0.3),
                                     COALESCE(tr.PATH_SURVIVAL_HIT_RATE, 0.2)) * 0.05
                       ELSE COALESCE(tr.PATH_SURVIVAL_HIT_RATE, 0.2) * 0.05 END
                + CASE WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'TRUSTED'     THEN 0.15
                       WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'PROVISIONAL' THEN 0.08
                       WHEN se.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'             THEN 0.04
                       ELSE 0.0 END
                + CASE WHEN se.SETUP_DATE = :v_as_of THEN 0.05 ELSE 0.0 END
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
                        + CASE WHEN se.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'
                               THEN GREATEST(COALESCE(tr.MEANINGFUL_HIT_RATE, 0.3),
                                             COALESCE(tr.PATH_SURVIVAL_HIT_RATE, 0.2)) * 0.05
                               ELSE COALESCE(tr.PATH_SURVIVAL_HIT_RATE, 0.2) * 0.05 END
                        + CASE WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'TRUSTED'     THEN 0.15
                               WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'PROVISIONAL' THEN 0.08
                               WHEN se.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'             THEN 0.04
                               ELSE 0.0 END
                        + CASE WHEN se.SETUP_DATE = :v_as_of THEN 0.05 ELSE 0.0 END
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
                        + CASE WHEN se.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'
                               THEN GREATEST(COALESCE(tr.MEANINGFUL_HIT_RATE, 0.3),
                                             COALESCE(tr.PATH_SURVIVAL_HIT_RATE, 0.2)) * 0.05
                               ELSE COALESCE(tr.PATH_SURVIVAL_HIT_RATE, 0.2) * 0.05 END
                        + CASE WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'TRUSTED'     THEN 0.15
                               WHEN COALESCE(tr.TRUST_LABEL, 'RESEARCH') = 'PROVISIONAL' THEN 0.08
                               WHEN se.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'             THEN 0.04
                               ELSE 0.0 END
                        + CASE WHEN se.SETUP_DATE = :v_as_of THEN 0.05 ELSE 0.0 END
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
              -- Phase 6 E: explicit FX exclusion.
              -- The structural detector produces FX setups for analytical purposes
              -- (TREND_PULLBACK_LONG / THREE_BAR_REVERSAL_LONG on EUR/USD, GBP/JPY, etc.),
              -- but the operating account does not trade FX. Without this filter, the
              -- raised P_MAX_PROPOSALS=8 cap + F9 freshness boost + F10b ranking-pool
              -- exclusion opens enough rank slots that FX setups surface in the proposal
              -- slate (5 FX proposals observed on 2026-04-28). NULL/unknown MARKET_TYPE
              -- is treated as STOCK to preserve historical behavior on un-classified
              -- equity setups (e.g. ORCL/JPM rows with NULL MARKET_TYPE). Lift criterion:
              -- once FX execution path exists in the broker bridge and FX risk treatment
              -- is defined, swap this for a tradable-market whitelist.
              AND COALESCE(se.MARKET_TYPE, 'STOCK') <> 'FX'
              -- Phase 6 D: explicit SHORT freeze.
              -- Lift criterion: at least one SHORT family must achieve MFE/MAE >= 1.0
              -- over the trust evaluation window before removing this filter.
              -- Current SHORT MFE/MAE: 3BR_S=0.617, FB_S=0.663, BDR_S=0.594, RW_S=0.105 (all below threshold).
              AND se.DIRECTION = 'LONG'
              -- Phase 6 F10b: exclude setups that already have a proposal record so they
              -- do not consume rank slots that should go to genuinely new candidates.
              -- Without this guard, a previously-proposed setup that remains ELIGIBLE in
              -- the 5-day lookback window still occupies a top-N slot and gets silently
              -- skipped by the MERGE, crowding out fresh setups (e.g. BRL CRWD ranking #11
              -- behind already-proposed older PROVISIONAL TPL setups).
              AND NOT EXISTS (
                  SELECT 1 FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
                  WHERE p.SETUP_EVENT_ID = se.SETUP_EVENT_ID
                    AND COALESCE(p.PORTFOLIO_ID, -1) = COALESCE(:P_PORTFOLIO_ID, -1)
              )
              -- Phase 7 PQI Fix 3: symbol-level recent-trade cooldown.
              -- Suppresses any symbol that already has a STRUCTURAL LIVE_ACTION
              -- in-flight (active states) or recently closed within the cooldown
              -- window. This is the structural answer to ORCL/COST style noise:
              -- a setup detector firing every day on a symbol we just sent to the
              -- broker should not turn into a re-proposal until the prior trade
              -- has been adjudicated and the cooldown has elapsed.
              AND NOT EXISTS (
                  SELECT 1 FROM MIP.LIVE.LIVE_ACTIONS la
                  WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
                    AND la.SYMBOL = se.SYMBOL
                    AND (
                        -- Active in-flight states: never repropose while a live action exists.
                        la.STATUS IN (
                            'PROPOSED',
                            'INTENT_APPROVED',
                            'PENDING_OPEN_VALIDATION',
                            'OPEN_BLOCKED',
                            'REVALIDATED_PASS',
                            'EXECUTION_REQUESTED'
                        )
                        -- Recently terminal states: cooldown window since last touch.
                        OR (
                            la.STATUS IN ('CANCELLED', 'REJECTED', 'SUPERSEDED')
                            AND la.UPDATED_AT >= DATEADD('day', -:P_SYMBOL_COOLDOWN_DAYS, :v_as_of)
                        )
                    )
              )
              -- Phase 7 PQI Fix 5: conflicting opposite-direction signal suppression.
              -- A LONG setup must not be proposed if the same symbol has an ELIGIBLE
              -- SHORT setup with confidence >= 0.65 in the same lookback window.
              -- This protects against COST-style cases where TPL LONG fires while
              -- THREE_BAR_REVERSAL_SHORT is also ELIGIBLE on the same name with
              -- meaningful confidence.
              AND NOT EXISTS (
                  SELECT 1 FROM MIP.APP.STRUCTURAL_SETUP_EVENTS se2
                  WHERE se2.SYMBOL = se.SYMBOL
                    AND se2.MARKET_TYPE = se.MARKET_TYPE
                    AND se2.DIRECTION <> se.DIRECTION
                    AND se2.SETUP_STATUS IN ('DETECTED', 'ELIGIBLE')
                    AND se2.SETUP_DATE >= DATEADD('day', -3, :v_as_of)
                    AND COALESCE(se2.STRUCTURE_CONFIDENCE, 0) >= 0.65
              )
        )
        SELECT
            es.SETUP_EVENT_ID,
            :P_PORTFOLIO_ID                                   AS PORTFOLIO_ID,
            es.SYMBOL,
            es.DIRECTION,
            es.SETUP_FAMILY,
            es.ENTRY_ZONE_LOW,
            es.ENTRY_ZONE_HIGH,
            es.PRICE_INVALIDATION_LEVEL,
            es.INVALIDATION_RULE,
            COALESCE(es.POLICY_TRAIL_STYLE, es.TRAIL_STYLE)   AS TRAIL_STYLE,
            COALESCE(es.POLICY_TRAIL_PARAMS, es.TRAIL_PARAMS) AS TRAIL_PARAMS,
            COALESCE(es.POLICY_EXIT_STYLE, es.EXIT_STYLE_RECOMMENDATION, 'STRUCTURAL_TARGET') AS EXIT_STYLE,
            es.STRUCTURE_CONFIDENCE,
            es.LEVEL_SIGNIFICANCE,
            es.REGIME_COMPAT,
            es.TRUST_MHR                                      AS MEANINGFUL_HIT_RATE,
            es.TRUST_PSHR                                     AS PATH_SURVIVAL_HIT_RATE,
            es.TRUST_RATIO                                    AS MFE_MAE_RATIO,
            -- Phase 6 C3: BRL gap-risk-aware sizing — force RISK_CLASS to GAP_AWARE.
            -- BRL gap_risk_contribution = 0.574 vs TPL = 0.084 (~6.8x).
            -- structural_committee.py:compute_structural_thesis_assessment honors
            -- GAP_AWARE with a 0.50x size_mult clamp + RISK_CLASS_GAP_AWARE_SIZE_CLAMP
            -- reason tag, materially reducing BRL position size at execution time.
            -- Token deliberately short (9 chars) to fit RISK_CLASS TEXT(10) column.
            CASE WHEN es.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'
                 THEN 'GAP_AWARE'
                 ELSE es.RISK_CLASS END                       AS RISK_CLASS,
            NULL                                              AS CONFLICT_RESOLUTION,
            es.SETUP_FAMILY || ' on ' || es.SYMBOL
                || ' | Trust: ' || COALESCE(es.TRUST_LABEL, 'UNKNOWN')
                || ' | MHR: ' || COALESCE(ROUND(es.TRUST_MHR * 100, 1)::VARCHAR, '?') || '%'
                || ' | Conf: ' || ROUND(es.STRUCTURE_CONFIDENCE, 2)
                || ' | Level Sig: ' || COALESCE(ROUND(es.LEVEL_SIGNIFICANCE, 2)::VARCHAR, '?')
                || ' | Regime: ' || es.REGIME_COMPAT
                || CASE WHEN es.SETUP_DATE = :v_as_of THEN ' | FRESH (same-day +0.05)' ELSE '' END
                || CASE WHEN es.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG'
                        THEN ' | BRL: gap-risk-aware sizing (sizing_multiplier=0.5), STAGED_PARTIAL exit'
                        ELSE '' END
                || ' | Rank: ' || es.RANK_N                   AS RATIONALE_TEXT,
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
                'composite_score', es.COMPOSITE_SCORE,
                'fresh_same_day', (es.SETUP_DATE = :v_as_of),
                -- Phase 6 C3: BRL-specific sizing hint — downstream sizing logic
                -- should honor this multiplier. Default 1.0 for non-BRL families.
                'sizing_multiplier',
                    CASE WHEN es.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG' THEN 0.5 ELSE 1.0 END,
                'gap_risk_aware_sizing',
                    (es.SETUP_FAMILY = 'BREAKOUT_RETEST_LONG')
            )                                                 AS COMMITTEE_PAYLOAD,
            'PROPOSED'                                        AS STATUS
        FROM eligible_setups es
        -- Phase 2: top-N global floor + soft directional ceiling (max 75% per direction)
        WHERE es.RANK_N <= :P_MAX_PROPOSALS
          AND es.RANK_N_DIR <= FLOOR(:P_MAX_PROPOSALS * 0.75)
    ) src
    -- Phase 6 F10: dedup key — one proposal per (setup event, portfolio).
    -- A given setup is never re-proposed across runs even if it remains
    -- ELIGIBLE for multiple consecutive day windows.
    ON  tgt.SETUP_EVENT_ID = src.SETUP_EVENT_ID
    AND COALESCE(tgt.PORTFOLIO_ID, -1) = COALESCE(src.PORTFOLIO_ID, -1)
    WHEN NOT MATCHED THEN INSERT (
        SETUP_EVENT_ID, PORTFOLIO_ID, SYMBOL, DIRECTION, SETUP_FAMILY,
        ENTRY_ZONE_LOW, ENTRY_ZONE_HIGH,
        PRICE_INVALIDATION_LEVEL, INVALIDATION_RULE,
        TRAIL_STYLE, TRAIL_PARAMS, EXIT_STYLE,
        STRUCTURE_CONFIDENCE, LEVEL_SIGNIFICANCE, REGIME_COMPAT,
        MEANINGFUL_HIT_RATE, PATH_SURVIVAL_HIT_RATE, MFE_MAE_RATIO,
        RISK_CLASS, CONFLICT_RESOLUTION, RATIONALE_TEXT,
        COMMITTEE_PAYLOAD, STATUS
    ) VALUES (
        src.SETUP_EVENT_ID, src.PORTFOLIO_ID, src.SYMBOL, src.DIRECTION, src.SETUP_FAMILY,
        src.ENTRY_ZONE_LOW, src.ENTRY_ZONE_HIGH,
        src.PRICE_INVALIDATION_LEVEL, src.INVALIDATION_RULE,
        src.TRAIL_STYLE, src.TRAIL_PARAMS, src.EXIT_STYLE,
        src.STRUCTURE_CONFIDENCE, src.LEVEL_SIGNIFICANCE, src.REGIME_COMPAT,
        src.MEANINGFUL_HIT_RATE, src.PATH_SURVIVAL_HIT_RATE, src.MFE_MAE_RATIO,
        src.RISK_CLASS, src.CONFLICT_RESOLUTION, src.RATIONALE_TEXT,
        src.COMMITTEE_PAYLOAD, src.STATUS
    );

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
