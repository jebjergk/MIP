/**
 * Page-level Explain Center contexts. sources.object must only reference
 * canonical Snowflake objects (see docs/ux/74_CANONICAL_OBJECTS.md).
 */

export const HOME_EXPLAIN_CONTEXT = {
  id: 'home',
  title: 'Home',
  what: 'This page is your entry point: hero, quick actions to Portfolio and Morning Brief and Training and Suggestions, and a system-at-a-glance summary of the last pipeline run, new evaluations, and latest brief.',
  why: 'So you can see at a glance whether the system is alive and jump to the right place without hunting through the menu.',
  how: 'Live metrics come from GET /live/metrics (MIP_AUDIT_LOG for last run, MORNING_BRIEF for latest brief, RECOMMENDATION_OUTCOMES for counts). Quick action cards link to Portfolio (id 1), Brief, Training Status, and Suggestions.',
  sources: [
    { object: 'MIP.APP.MIP_AUDIT_LOG', purpose: 'Last pipeline run and status.' },
    { object: 'MIP.AGENT_OUT.MORNING_BRIEF', purpose: 'Latest brief as-of timestamp per portfolio.' },
    { object: 'MIP.APP.RECOMMENDATION_OUTCOMES', purpose: 'Total outcomes and count since last run.' },
  ],
  fields: [
    { key: 'last_run', label: 'Last pipeline run', meaning: 'When the last daily pipeline run completed and its status.', glossaryKey: 'live.last_pipeline_run' },
    { key: 'since_last_run', label: 'New evaluations since last run', meaning: 'Number of outcome rows calculated after the last run completed.', glossaryKey: 'live.new_evaluations_since_last_run' },
    { key: 'latest_brief', label: 'Latest brief (as-of)', meaning: 'When the most recent morning brief was generated for the selected portfolio.', glossaryKey: 'live.data_freshness' },
  ],
}

export const PORTFOLIO_EXPLAIN_CONTEXT = {
  id: 'portfolio',
  title: 'Portfolio',
  what: 'Shows portfolio list and detail: positions, trades, and run-level KPIs when available.',
  why: 'So you can see what you hold, what was traded, and how the portfolio performed over a run.',
  how: 'Data is read from Portfolio and related tables; KPIs may come from MART views. Daily bars only where applicable.',
  sources: [
    { object: 'MIP.APP.PORTFOLIO', purpose: 'Portfolio metadata and status.' },
    { object: 'MIP.APP.PORTFOLIO_POSITIONS', purpose: 'Current positions.' },
    { object: 'MIP.APP.PORTFOLIO_TRADES', purpose: 'Trade history.' },
    { object: 'MIP.MART.V_PORTFOLIO_RUN_KPIS', purpose: 'Run-level KPIs when used.' },
  ],
  fields: [
    { key: 'portfolio_id', label: 'Portfolio ID', meaning: 'Unique identifier for the portfolio.' },
    { key: 'status', label: 'Status', meaning: 'Current state (e.g. active, closed).', glossaryKey: 'portfolio.status' },
    { key: 'final_equity', label: 'Final equity', meaning: 'Total value at end of run.', glossaryKey: 'portfolio.final_equity' },
    { key: 'total_return', label: 'Total return', meaning: 'Percentage gain or loss from start to end.', glossaryKey: 'portfolio.total_return' },
  ],
}

export const RUNS_EXPLAIN_CONTEXT = {
  id: 'runs',
  title: 'Runs (Audit)',
  what: 'Lists recent pipeline runs and lets you drill into a run to see its timeline and interpreted summary.',
  why: 'So you can confirm the pipeline ran, see success or failure, and understand what each step did.',
  how: 'Runs are read from MIP_AUDIT_LOG (EVENT_TYPE=PIPELINE, EVENT_NAME=SP_RUN_DAILY_PIPELINE). Steps and details come from the same table; the audit interpreter turns DETAILS JSON into narrative.',
  sources: [
    { object: 'MIP.APP.MIP_AUDIT_LOG', purpose: 'Pipeline runs and steps; RUN_ID, EVENT_TS, STATUS, DETAILS.' },
  ],
  fields: [
    { key: 'run_id', label: 'Run ID', meaning: 'Unique identifier for the pipeline run.' },
    { key: 'started_at', label: 'Started at', meaning: 'When the run started.' },
    { key: 'completed_at', label: 'Completed at', meaning: 'When the run finished.' },
    { key: 'status', label: 'Status', meaning: 'Outcome: SUCCESS, FAIL, RUNNING, or skip reason.', glossaryKey: 'audit.run_status' },
  ],
}

export const MORNING_BRIEF_EXPLAIN_CONTEXT = {
  id: 'brief',
  title: 'Morning Brief',
  what: 'Shows the latest morning brief for the selected portfolio: as-of time, pipeline run id, and the full brief content.',
  why: 'So you can read the narrative and signals the system produced for that portfolio.',
  how: 'Fetched from MORNING_BRIEF by portfolio_id; as_of_ts and pipeline_run_id come from the row or from the brief JSON attribution.',
  sources: [
    { object: 'MIP.AGENT_OUT.MORNING_BRIEF', purpose: 'Latest brief per portfolio; AS_OF_TS, PIPELINE_RUN_ID, BRIEF JSON.' },
  ],
  fields: [
    { key: 'as_of_ts', label: 'As-of timestamp', meaning: 'When the brief was built.', glossaryKey: 'brief.as_of_ts' },
    { key: 'pipeline_run_id', label: 'Pipeline run ID', meaning: 'Run that produced this brief.', glossaryKey: 'brief.pipeline_run_id' },
  ],
}

export const TRAINING_STATUS_EXPLAIN_CONTEXT = {
  id: 'training',
  title: 'Training Status',
  what: 'Shows how much evaluated history we have per symbol/pattern: maturity score, stage, sample size, outcomes, horizons covered, and coverage ratio.',
  why: 'So you can see which symbol/pattern combinations have enough data to trust and which are still warming up.',
  how: 'Daily bars only. Outcomes use EVAL_STATUS=COMPLETED; realized return is computed from entry to exit. Maturity combines recs, outcomes, and horizon coverage; optional TRAINING_GATE_PARAMS for thresholds.',
  sources: [
    { object: 'MIP.APP.RECOMMENDATION_LOG', purpose: 'Recommendations per symbol/pattern/interval.' },
    { object: 'MIP.APP.RECOMMENDATION_OUTCOMES', purpose: 'Evaluated outcomes (REALIZED_RETURN, HIT_FLAG, EVAL_STATUS).' },
    { object: 'MIP.APP.TRAINING_GATE_PARAMS', purpose: 'Optional thresholds (e.g. MIN_SIGNALS) when used.' },
  ],
  fields: [
    { key: 'maturity_score', label: 'Maturity score', meaning: '0–100 score from sample size, coverage, and horizons.', glossaryKey: 'training_status.maturity_score' },
    { key: 'maturity_stage', label: 'Maturity stage', meaning: 'Label: INSUFFICIENT, WARMING_UP, LEARNING, CONFIDENT.', glossaryKey: 'training_status.maturity_stage' },
    { key: 'recs_total', label: 'Recs total', meaning: 'Number of recommendations for this group.', glossaryKey: 'training_status.recs_total' },
    { key: 'outcomes_total', label: 'Outcomes total', meaning: 'Number of evaluated outcomes.', glossaryKey: 'training_status.outcomes_total' },
    { key: 'horizons_covered', label: 'Horizons covered', meaning: 'How many time windows (1, 3, 5, 10, 20 bars) have data.', glossaryKey: 'training_status.horizons_covered' },
    { key: 'coverage_ratio', label: 'Coverage ratio', meaning: 'Share of possible outcomes that were evaluated (0–1).', glossaryKey: 'training_status.coverage_ratio' },
  ],
}

export const SUGGESTIONS_EXPLAIN_CONTEXT = {
  id: 'suggestions',
  title: 'Suggestions',
  what: 'Ranked symbol/pattern candidates from evaluated outcome history. Strong candidates (recs ≥ 10, horizons ≥ 3) and early signals (recs ≥ 3, horizons ≥ 3) with clear labels.',
  why: 'So you can see which symbol/pattern combinations have the best combination of data maturity and historical performance, without executing trades.',
  how: 'Data from /performance/summary (RECOMMENDATION_LOG + RECOMMENDATION_OUTCOMES, daily bars, EVAL_STATUS completed). Score = 0.6×maturity + 0.2×(mean_return×1000) + 0.2×(pct_positive×100). Early signals use effective_score = score × min(1, recs/10).',
  sources: [
    { object: 'MIP.APP.RECOMMENDATION_LOG', purpose: 'Recommendations (daily bars).' },
    { object: 'MIP.APP.RECOMMENDATION_OUTCOMES', purpose: 'Realized return and hit flag per horizon.' },
  ],
  fields: [
    { key: 'suggestion_score', label: 'Suggestion score', meaning: 'Blend of maturity and 5-bar mean return and pct positive.', glossaryKey: 'suggestions.suggestion_score' },
    { key: 'effective_score', label: 'Effective score', meaning: 'Score downweighted for early signals: score × min(1, recs/10).', glossaryKey: 'suggestions.effective_score' },
    { key: 'recs_total', label: 'Sample size (recs total)', meaning: 'Number of recommendations for this symbol/pattern.', glossaryKey: 'suggestions.sample_size' },
    { key: 'strong_candidate', label: 'Strong candidate', meaning: 'recs ≥ 10 and horizons ≥ 3.', glossaryKey: 'suggestions.strong_candidate' },
    { key: 'early_signal', label: 'Early signal', meaning: 'recs ≥ 3 and horizons ≥ 3 but fewer than 10 recs; low confidence.', glossaryKey: 'suggestions.early_signal' },
  ],
}

export const TODAY_EXPLAIN_CONTEXT = {
  id: 'today',
  title: 'Today',
  what: 'Composition view: system status, portfolio situation, and today’s candidates from evaluated history.',
  why: 'So you can see in one place whether the system is alive, what the portfolio looks like, and which symbol/pattern candidates are worth looking at today.',
  how: 'Data is composed from status, portfolios, briefs, and training/signals; daily bars only. Candidates are ranked by maturity and outcome history.',
  sources: [
    { object: 'MIP.APP.MIP_AUDIT_LOG', purpose: 'Last pipeline run status.' },
    { object: 'MIP.APP.PORTFOLIO', purpose: 'Portfolio list and risk state.' },
    { object: 'MIP.AGENT_OUT.MORNING_BRIEF', purpose: 'Latest brief when used.' },
  ],
  fields: [],
}

export const DEBUG_EXPLAIN_CONTEXT = {
  id: 'debug',
  title: 'Debug',
  what: 'Route smoke checks: tests that each API endpoint responds.',
  why: 'So you can quickly see which endpoints are reachable and which fail.',
  how: 'Each row calls one API route; green means OK, red means error. No Snowflake objects; API only.',
  sources: [],
  fields: [],
}

/** Build section context for Suggestions Evidence Drawer (selected item + horizon). */
export function buildSuggestionsEvidenceContext(item, horizonBars) {
  const horizonLabel = horizonBars ? `${horizonBars}-day horizon` : 'Selected horizon'
  return {
    id: 'suggestions-evidence',
    title: `Suggestions: ${horizonLabel}`,
    what: `Evidence drawer for this symbol/pattern at ${horizonBars || '?'} bars: what history suggests, horizon strip, distribution of realized returns, and confidence.`,
    why: 'So you can see the full picture for one candidate before deciding where to look next.',
    how: `Daily bars only. Realized return = (exit price - entry price) / entry price over ${horizonBars || 'N'} bars. Distribution comes from GET /performance/distribution (RECOMMENDATION_OUTCOMES, EVAL_STATUS=COMPLETED).`,
    sources: [
      { object: 'MIP.APP.RECOMMENDATION_LOG', purpose: 'Recommendations for this symbol/pattern.' },
      { object: 'MIP.APP.RECOMMENDATION_OUTCOMES', purpose: 'Realized return per outcome; CALCULATED_AT for freshness.' },
    ],
    fields: [
      { key: 'horizon_bars', label: 'Horizon (bars)', meaning: 'Holding period in bars (e.g. 5 = 5 days for daily).', glossaryKey: 'performance.horizon_bars' },
      { key: 'realized_return', label: 'Realized return', meaning: 'Return from entry to exit over this horizon (decimal).', glossaryKey: 'performance.realized_return', calc: ' (exit - entry) / entry' },
      { key: 'pct_positive', label: '% positive', meaning: 'Share of outcomes with positive return.', glossaryKey: 'performance.pct_positive' },
      { key: 'mean_realized_return', label: 'Mean realized return', meaning: 'Average return over this horizon.', glossaryKey: 'performance.mean_realized_return' },
      { key: 'distribution', label: 'Distribution chart', meaning: 'Histogram of realized returns for this horizon; shows spread of outcomes.' },
    ],
  }
}
