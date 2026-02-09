/**
 * Page-level Explain Center contexts. sources.object must only reference
 * canonical Snowflake objects (see docs/ux/74_CANONICAL_OBJECTS.md).
 * 
 * Enhanced with comprehensive user guide content for in-app help.
 */

export const HOME_EXPLAIN_CONTEXT = {
  id: 'home',
  title: 'Home',
  what: `Your entry point to the system. This page gives you a quick health check and shortcuts to the most important areas.

**What you'll see:**
- **Last Pipeline Run** — When the system last processed market data. If this is old (e.g., more than a day), something may be wrong.
- **New Evaluations** — How many trading signals were measured for performance since the last run.
- **Latest Digest** — When the most recent AI digest was generated.
- **Quick Action Cards** — One-click shortcuts to Cockpit, Portfolios, Training Status, and Suggestions.`,

  why: `Start here each day to confirm the system is running normally. If the "Last Pipeline Run" shows a recent timestamp with SUCCESS status, everything is healthy. The quick actions help you jump directly to what matters without hunting through menus.`,

  how: `The page fetches live metrics from the API, which reads from:
- **MIP_AUDIT_LOG** — For pipeline run status and timing
- **DAILY_DIGEST_SNAPSHOT** — For the latest digest timestamp
- **RECOMMENDATION_OUTCOMES** — For evaluation counts

The data refreshes each time you visit the page.`,

  sources: [
    { object: 'MIP.APP.MIP_AUDIT_LOG', purpose: 'Last pipeline run and status.' },
    { object: 'MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT', purpose: 'Latest digest as-of timestamp.' },
    { object: 'MIP.APP.RECOMMENDATION_OUTCOMES', purpose: 'Total outcomes and count since last run.' },
  ],
  fields: [
    { key: 'last_run', label: 'Last pipeline run', meaning: 'When the last daily pipeline run completed. Shows time ago (e.g., "2 hours ago") and status (SUCCESS, FAIL, etc.). A healthy system shows a recent successful run.', glossaryKey: 'live.last_pipeline_run' },
    { key: 'since_last_run', label: 'New evaluations since last run', meaning: 'Number of signal outcomes that were calculated after the last run. Higher numbers mean more historical signals have been measured for performance.', glossaryKey: 'live.new_evaluations_since_last_run' },
    { key: 'latest_brief', label: 'Latest digest (as-of)', meaning: 'The market date covered by the most recent AI digest. This tells you how current the trading recommendations are.', glossaryKey: 'live.data_freshness' },
  ],
}

export const PORTFOLIO_EXPLAIN_CONTEXT = {
  id: 'portfolio',
  title: 'Portfolio',
  what: `Your portfolio management center. View all portfolios or drill into one for detailed performance analysis.

**List View (all portfolios):**
- **Gate** — Risk status: SAFE (green), CAUTION (yellow), or STOPPED (red)
- **Health** — Overall portfolio health indicator
- **Equity** — Current total value (cash + positions)
- **Paid Out** — Money withdrawn or distributed
- **Active Episode** — Current trading period
- **Status** — ACTIVE, BUST, or STOPPED

**Detail View (single portfolio):**
- **Header Metrics** — Starting cash, final equity, total return, max drawdown, win/loss days
- **Charts** — Equity over time, drawdown chart, trades per day, risk regime strip
- **Snapshot Cards** — Cash & exposure, open positions, recent trades, risk gate status
- **Episode Timeline** — History of all trading episodes with expandable details`,

  why: `This is where you monitor portfolio health and performance. Check the header metrics to see how you're doing overall. The charts show trends over time — look for upward equity trends and shallow drawdowns. The snapshot cards give you the current state at a glance.

**Key things to watch:**
- **Drawdown approaching threshold** — If the drawdown chart nears the dotted warning line, entries may be blocked soon
- **Risk gate status** — When STOPPED, only exits are allowed
- **Episode changes** — A new episode means the portfolio was reset; historical data is preserved but metrics restart`,

  how: `Data comes from multiple sources:
- **PORTFOLIO** table — Basic info, status, starting cash
- **PORTFOLIO_DAILY** — Daily equity snapshots for charts
- **PORTFOLIO_POSITIONS** — Current holdings
- **PORTFOLIO_TRADES** — Trade history
- **V_PORTFOLIO_RUN_KPIS** — Aggregated metrics per run

All data is scoped to the current episode (trading period).`,

  sources: [
    { object: 'MIP.APP.PORTFOLIO', purpose: 'Portfolio metadata, status, and configuration.' },
    { object: 'MIP.APP.PORTFOLIO_POSITIONS', purpose: 'Current open positions with entry prices.' },
    { object: 'MIP.APP.PORTFOLIO_TRADES', purpose: 'Complete trade history (buys and sells).' },
    { object: 'MIP.APP.PORTFOLIO_DAILY', purpose: 'Daily equity, cash, and drawdown snapshots.' },
    { object: 'MIP.APP.PORTFOLIO_EPISODE', purpose: 'Episode boundaries and lifecycle state.' },
    { object: 'MIP.MART.V_PORTFOLIO_RUN_KPIS', purpose: 'Aggregated run-level KPIs (return, drawdown, win rate).' },
  ],
  fields: [
    { key: 'starting_cash', label: 'Starting cash', meaning: 'The amount of money the portfolio started with at the beginning of the current episode. This is your baseline for calculating returns.', glossaryKey: 'portfolio.starting_cash' },
    { key: 'final_equity', label: 'Final equity', meaning: 'Current total portfolio value = cash on hand + market value of all open positions. This is what you would have if you closed everything now.', glossaryKey: 'portfolio.final_equity' },
    { key: 'total_return', label: 'Total return', meaning: 'Percentage gain or loss since the episode started. Calculated as (final_equity - starting_cash) / starting_cash. Positive = profit, negative = loss.', glossaryKey: 'portfolio.total_return' },
    { key: 'max_drawdown', label: 'Max drawdown', meaning: 'The largest peak-to-trough decline during this episode. Shows the worst-case scenario you experienced. Example: -10% means the portfolio dropped 10% from its highest point before recovering.', glossaryKey: 'portfolio.max_drawdown' },
    { key: 'win_days', label: 'Win days', meaning: 'Number of days with positive returns (equity went up from previous day).', glossaryKey: 'portfolio.win_days' },
    { key: 'loss_days', label: 'Loss days', meaning: 'Number of days with negative returns (equity went down from previous day).', glossaryKey: 'portfolio.loss_days' },
    { key: 'status', label: 'Status', meaning: 'Current portfolio state: ACTIVE (normal trading), STOPPED (risk limits breached, exits only), or BUST (below minimum threshold, trading halted).', glossaryKey: 'portfolio.status' },
    { key: 'risk_gate', label: 'Risk gate', meaning: 'Safety mechanism that controls whether new positions can be opened. SAFE = normal trading. CAUTION = be careful. STOPPED = only exits allowed.', glossaryKey: 'risk_gate.mode' },
  ],
}

export const RISK_GATE_EXPLAIN_CONTEXT = {
  id: 'risk_gate',
  title: 'Risk Gate',
  what: `The Risk Gate is the safety mechanism that controls whether new trades can be opened.

**Three Modes:**
- **SAFE (Normal)** — Full trading allowed. You can open new positions and close existing ones.
- **CAUTION** — Warning state. Entries still allowed, but be careful — you're approaching limits.
- **STOPPED (Defensive)** — Entries blocked. You can only close or reduce existing positions.

The gate exists to prevent losses from snowballing. Once you hit the drawdown threshold, the system stops adding risk until conditions improve or you reset the episode.`,

  why: `So you see at a glance what the system will and won't do: whether you can add risk or should only reduce it. No guessing from raw codes or logs.

**Key insight:** The gate never blocks exits — you can always reduce exposure. It only restricts opening new positions when risk is elevated.`,

  how: `The API reads V_PORTFOLIO_RISK_GATE and V_PORTFOLIO_RISK_STATE, then normalizes them into a single plain-language object: mode, entries_allowed, exits_allowed, reason_text, and what_to_do_now. The UI shows the permission matrix and recommended actions.

**What triggers STOPPED?**
- **Drawdown stop** — Portfolio dropped too much from peak (e.g., past 10% threshold)
- **Bust threshold** — Portfolio value fell below minimum (e.g., 60% of starting cash)`,

  sources: [
    { object: 'MIP.MART.V_PORTFOLIO_RISK_GATE', purpose: 'Risk gate state per portfolio (entries blocked, allowed actions).' },
    { object: 'MIP.MART.V_PORTFOLIO_RISK_STATE', purpose: 'Risk state and stop reason (ALLOWED_ACTIONS, STOP_REASON, RISK_STATUS).' },
  ],
  fields: [
    {
      key: 'entries_allowed',
      label: 'Entries allowed',
      meaning: 'Whether opening new positions (buy, add) is allowed. When false, only closing or reducing positions is permitted.',
      calc: 'Derived from ENTRIES_BLOCKED: entries_allowed = NOT entries_blocked.',
      glossaryKey: 'risk_gate.entries_allowed',
    },
    {
      key: 'exits_only',
      label: 'Exits only',
      meaning: 'Mode where only closing or reducing positions is allowed; no new entries. The gate never blocks unwinding.',
      glossaryKey: 'risk_gate.defensive_mode',
    },
    {
      key: 'drawdown_stop',
      label: 'Drawdown stop',
      meaning: 'A safety rule that pauses new entries when the portfolio has lost too much from its peak. You can still close or reduce positions.',
      glossaryKey: 'risk_gate.drawdown_stop',
    },
    {
      key: 'mode',
      label: 'Mode (Normal / Caution / Defensive)',
      meaning: 'Human-friendly label: Normal = full trading, Caution = be careful with new positions, Defensive = exits only.',
      glossaryKey: 'risk_gate.mode',
    },
    {
      key: 'reason_text',
      label: 'Reason text',
      meaning: 'One-sentence plain-language reason for the current gate state (e.g., within limits, drawdown hit, safety pause).',
      glossaryKey: 'risk_gate.reason_text',
    },
    {
      key: 'what_to_do_now',
      label: 'What to do now',
      meaning: 'Recommended actions for the current mode — use Suggestions, avoid opening many positions, or only close/reduce.',
      glossaryKey: 'risk_gate.what_to_do_now',
    },
  ],
}

export const RUNS_EXPLAIN_CONTEXT = {
  id: 'runs',
  title: 'Audit Viewer (Run Explorer)',
  what: `Detailed history of all pipeline runs, including errors and step-by-step execution.

**Left Pane — Run List:**
- All pipeline runs with status, duration, and timestamp
- Filter by status (SUCCESS, FAIL) or date range
- Click any run to see details

**Right Pane — Run Details:**
- **Summary Cards** — Status, duration, as-of date, portfolio count, error count
- **Run Narrative** — Plain English: what happened, why, impact, next steps
- **Error Panel** — If failed: error message, SQL query ID, debug SQL to investigate
- **Step Timeline** — Visual flow of each step (Ingestion → Returns → Recommendations → etc.)`,

  why: `Use this to:
1. **Confirm the pipeline ran** — Check for SUCCESS status
2. **Diagnose failures** — See which step failed without digging through logs
3. **Understand skip reasons** — Why some steps were skipped (e.g., no new data)
4. **Debug issues** — Copy error details and debug SQL for investigation`,

  how: `Runs are read from MIP_AUDIT_LOG (EVENT_TYPE=PIPELINE). Steps come from PIPELINE_STEP events. Error details include SQLSTATE, query ID, and context. Debug SQL is auto-generated for the selected run.

**Pipeline steps in order:**
1. Ingestion — Fetch market data
2. Returns — Calculate price changes
3. Recommendations — Generate signals
4. Evaluation — Measure past signal performance
5. Simulation — Run portfolio paper trading
6. Proposals — Create trade suggestions
7. Execution — Execute approved trades
8. Daily Digest — Generate AI narrative summary`,

  sources: [
    { object: 'MIP.APP.MIP_AUDIT_LOG', purpose: 'Pipeline runs and steps; RUN_ID, EVENT_TS, STATUS, ERROR_MESSAGE, DURATION_MS, DETAILS.' },
    { object: 'INFORMATION_SCHEMA.QUERY_HISTORY', purpose: 'Query details for failed queries (via ERROR_QUERY_ID).' },
  ],
  fields: [
    { key: 'run_id', label: 'Run ID', meaning: 'Unique identifier for the pipeline run. UUID format. Click to copy.' },
    { key: 'started_at', label: 'Started at', meaning: 'When the run started.' },
    { key: 'completed_at', label: 'Completed at', meaning: 'When the run finished.' },
    { key: 'status', label: 'Status', meaning: 'Outcome: SUCCESS (all steps passed), FAIL (at least one step failed), SUCCESS_WITH_SKIPS (completed but some steps skipped).', glossaryKey: 'audit.run_status' },
    { key: 'duration_ms', label: 'Duration', meaning: 'Total time taken for the run in milliseconds.' },
    { key: 'error_message', label: 'Error Message', meaning: 'The error message captured when a step failed.' },
    { key: 'error_query_id', label: 'Query ID', meaning: 'Snowflake query ID of the failed statement. Use to look up in QUERY_HISTORY for full details.' },
    { key: 'step_timeline', label: 'Step Timeline', meaning: 'Visual flow showing each pipeline step. Green = success, red = failed, gray = skipped.' },
    { key: 'debug_sql', label: 'Debug SQL', meaning: 'Pre-generated SQL queries to investigate this run in Snowflake. Copy and run in your SQL editor.' },
  ],
}

// Morning Brief page was removed — its functionality is now in the Cockpit.
// Keeping a minimal context for backward compatibility if anything references it.
export const MORNING_BRIEF_EXPLAIN_CONTEXT = {
  id: 'cockpit',
  title: 'Cockpit',
  what: `The Cockpit is your unified dashboard combining AI-generated narratives, portfolio status, training progress, and upcoming symbols. It replaces the old Morning Brief, Daily Digest, and Today pages.`,
  why: `Use the Cockpit for your daily check-in. Everything you need is in one place.`,
  how: `The Cockpit fetches data from Daily Digest (AI narrative), Training Digest, and portfolio views.`,
  sources: [
    { object: 'MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT', purpose: 'AI digest snapshot with facts.' },
    { object: 'MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE', purpose: 'AI-generated narrative for daily digest.' },
    { object: 'MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT', purpose: 'Training state snapshot.' },
    { object: 'MIP.APP.PORTFOLIO', purpose: 'Portfolio status and current state.' },
  ],
  fields: [],
}

export const TRAINING_STATUS_EXPLAIN_CONTEXT = {
  id: 'training',
  title: 'Training Status',
  what: `How well the system has learned each pattern for each symbol.

**Table Columns:**
- **Market Type** — STOCK, ETF, or FX
- **Symbol** — The asset (e.g., AAPL, GOOGL)
- **Pattern** — Pattern ID (which detection algorithm)
- **Interval** — Time interval (1440 = daily bars)
- **Maturity** — Learning stage and confidence score
- **Sample Size** — How many times this pattern was observed
- **Coverage** — Percentage of observations that have been evaluated
- **Horizons** — Which forward periods have been measured (1, 3, 5, 10, 20 bars)
- **Avg Outcomes** — Average returns at each horizon (H1, H3, H5, H10, H20)

**Maturity Stages:**
- **INSUFFICIENT** — Not enough data to draw conclusions
- **WARMING_UP** — Starting to gather data
- **LEARNING** — Building confidence
- **CONFIDENT** — Enough data for reliable predictions`,

  why: `Use this to understand:
1. **Which patterns to trust** — CONFIDENT patterns have proven themselves
2. **Where data is thin** — INSUFFICIENT or WARMING_UP need more time
3. **Performance by horizon** — Some patterns work better for short-term, others for longer holds

**Key insight:** Only patterns that reach sufficient maturity are used for trade proposals. Early patterns are tracked but not acted upon.`,

  how: `Data comes from:
- **RECOMMENDATION_LOG** — All recommendations (signals) generated
- **RECOMMENDATION_OUTCOMES** — Evaluated results for each signal

Maturity score combines:
- Sample size (how many observations)
- Coverage ratio (what percentage evaluated)
- Horizon coverage (how many time windows measured)

Daily bars only. Realized return = (exit price - entry price) / entry price.`,

  sources: [
    { object: 'MIP.APP.RECOMMENDATION_LOG', purpose: 'Recommendations per symbol/pattern/interval.' },
    { object: 'MIP.APP.RECOMMENDATION_OUTCOMES', purpose: 'Evaluated outcomes (REALIZED_RETURN, HIT_FLAG, EVAL_STATUS).' },
  ],
  fields: [
    { key: 'maturity_score', label: 'Maturity score', meaning: '0-100 score based on sample size, coverage, and horizons. Higher = more reliable predictions.', glossaryKey: 'training_status.maturity_score' },
    { key: 'maturity_stage', label: 'Maturity stage', meaning: 'Human-readable label: INSUFFICIENT (need more data), WARMING_UP (early stage), LEARNING (building confidence), CONFIDENT (reliable).', glossaryKey: 'training_status.maturity_stage' },
    { key: 'recs_total', label: 'Sample size', meaning: 'Total number of recommendations (signals) for this symbol/pattern. More samples = more reliable statistics.', glossaryKey: 'training_status.recs_total' },
    { key: 'outcomes_total', label: 'Outcomes total', meaning: 'Number of evaluated outcomes. Each recommendation can have up to 5 outcomes (one per horizon).', glossaryKey: 'training_status.outcomes_total' },
    { key: 'horizons_covered', label: 'Horizons covered', meaning: 'How many time windows (1, 3, 5, 10, 20 bars) have evaluation data. More horizons = better understanding of pattern behavior.', glossaryKey: 'training_status.horizons_covered' },
    { key: 'coverage_ratio', label: 'Coverage ratio', meaning: 'Share of possible outcomes that were evaluated (0-1). 1.0 = all possible evaluations completed.', glossaryKey: 'training_status.coverage_ratio' },
  ],
}

export const SUGGESTIONS_EXPLAIN_CONTEXT = {
  id: 'suggestions',
  title: 'Suggestions',
  what: `Ranked trading opportunities based on historical performance.

**Two Categories:**
- **Strong Candidates** — Patterns with 10+ recommendations (more reliable)
- **Early Signals** — Patterns with 3-9 recommendations (still learning, lower confidence)

**For Each Suggestion:**
- **Rank** — Overall ranking based on suggestion score
- **Symbol/Pattern** — Which stock and which detection pattern
- **Suggestion Score** — Combined score (higher = better opportunity)
- **Maturity Stage** — INSUFFICIENT -> WARMING_UP -> LEARNING -> CONFIDENT
- **Maturity Score** — 0-100% confidence in this pattern
- **What History Suggests** — Summary of past performance
- **Horizon Strip** — Performance at 1, 3, 5, 10, 20 bars forward

**Evidence Drawer:** Click any suggestion to see detailed charts and statistics.`,

  why: `This is where you find trading ideas. The system ranks opportunities by combining:
1. **Data maturity** — How much we know about this pattern
2. **Historical performance** — How well it performed in the past

**Key insight:** Strong candidates are more reliable than early signals. Early signals are promising but need more data to confirm.`,

  how: `Score calculation:
- **Suggestion Score** = 0.6 x maturity + 0.2 x (mean_return x 1000) + 0.2 x (pct_positive x 100)
- **Effective Score** (for early signals) = score x min(1, recs/10)

Data from RECOMMENDATION_LOG + RECOMMENDATION_OUTCOMES. Daily bars only. Only EVAL_STATUS=COMPLETED outcomes are used.`,

  sources: [
    { object: 'MIP.APP.RECOMMENDATION_LOG', purpose: 'Recommendations (signals) for each symbol/pattern.' },
    { object: 'MIP.APP.RECOMMENDATION_OUTCOMES', purpose: 'Realized return and hit flag per horizon.' },
  ],
  fields: [
    { key: 'suggestion_score', label: 'Suggestion score', meaning: 'Blend of maturity and performance. Higher = better trading opportunity.', glossaryKey: 'suggestions.suggestion_score' },
    { key: 'effective_score', label: 'Effective score', meaning: 'Score adjusted for sample size. Early signals (fewer than 10 recs) are downweighted.', glossaryKey: 'suggestions.effective_score' },
    { key: 'recs_total', label: 'Sample size', meaning: 'Number of recommendations for this symbol/pattern. More = more reliable.', glossaryKey: 'suggestions.sample_size' },
    { key: 'strong_candidate', label: 'Strong candidate', meaning: 'Has 10+ recommendations and 3+ horizons evaluated. More reliable for trading.', glossaryKey: 'suggestions.strong_candidate' },
    { key: 'early_signal', label: 'Early signal', meaning: 'Has 3-9 recommendations. Promising but needs more data. Treat with caution.', glossaryKey: 'suggestions.early_signal' },
    { key: 'horizon_strip', label: 'Horizon strip', meaning: 'Shows performance at each holding period (1, 3, 5, 10, 20 bars). Green = positive return, red = negative.', glossaryKey: 'suggestions.horizon_strip' },
  ],
}

export const TODAY_EXPLAIN_CONTEXT = {
  id: 'today',
  title: 'Today',
  what: `Your daily dashboard. Everything about today's trading situation in one place.

**Sections:**

**System Status** — Database connection health (green = OK)

**Portfolio Selector** — Switch between different portfolios

**Risk Gate** — Current gate state: SAFE, CAUTION, or STOPPED

**Recent Run Events** — What happened in the last pipeline run

**Today's Insights** — Ranked list of trading candidates based on pattern detection and historical performance

**AI Narrative** — AI-generated digest for your portfolio`,

  why: `Use this page for your daily check-in. In one glance you can:

1. **Verify the system is healthy** — Green status, recent pipeline run
2. **Check your risk state** — Can you open new positions?
3. **See today's opportunities** — Ranked candidates with clear reasoning
4. **Access AI narrative** — Full details without leaving the page

**Reading Today's Insights:**
- **Maturity Score** — How confident we are in this pattern (0-100%)
- **Today Score** — How strong the current signal is
- **Why Shown** — The specific reason this candidate appears`,

  how: `This page composes data from multiple sources:
- Status check from API health endpoint
- Portfolio data from PORTFOLIO table
- Risk state from V_PORTFOLIO_RISK_GATE
- Candidates from RECOMMENDATION_LOG filtered by training maturity
- Digest from DAILY_DIGEST_NARRATIVE

All data uses daily bars only.`,

  sources: [
    { object: 'MIP.APP.MIP_AUDIT_LOG', purpose: 'Last pipeline run status and timing.' },
    { object: 'MIP.APP.PORTFOLIO', purpose: 'Portfolio list, status, and current equity.' },
    { object: 'MIP.MART.V_PORTFOLIO_RISK_GATE', purpose: 'Risk gate state (entries allowed/blocked).' },
    { object: 'MIP.APP.RECOMMENDATION_LOG', purpose: 'Today\'s signals for ranking candidates.' },
    { object: 'MIP.APP.RECOMMENDATION_OUTCOMES', purpose: 'Historical performance for maturity scoring.' },
    { object: 'MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE', purpose: 'Latest AI digest narrative.' },
  ],
  fields: [
    { key: 'system_status', label: 'System status', meaning: 'Overall health: Green = database connected, pipeline running. Red = connection issue or failure.', glossaryKey: 'live.system_status' },
    { key: 'risk_gate', label: 'Risk gate', meaning: 'Trading permission state: SAFE = normal, CAUTION = careful, STOPPED = exits only.', glossaryKey: 'risk_gate.mode' },
    { key: 'maturity_score', label: 'Maturity score', meaning: 'Confidence in a pattern (0-100%). Higher = more data, more reliable.', glossaryKey: 'training_status.maturity_score' },
    { key: 'today_score', label: 'Today score', meaning: 'Strength of the current signal. Higher = stronger pattern match today.', glossaryKey: 'signals.score' },
    { key: 'why_shown', label: 'Why shown', meaning: 'Specific reason this candidate appears (e.g., "TRUSTED pattern with strong signal").', glossaryKey: 'today.why_shown' },
  ],
}

export const SIGNALS_EXPLAIN_CONTEXT = {
  id: 'signals',
  title: 'Signals',
  what: `A searchable list of all trading signals generated by the system.

**Table Columns:**
- **Symbol** — The stock/asset (e.g., AAPL, GOOGL)
- **Market** — Type of asset: STOCK, ETF, or FX
- **Pattern** — Which detection pattern generated this signal
- **Score** — Signal strength (higher = stronger pattern match)
- **Trust** — Pattern reliability: TRUSTED, WATCH, or UNTRUSTED
- **Action** — BUY or SELL
- **Eligible** — Can this signal be traded today?
- **Signal Time** — When the signal was generated

**Filters:**
- Filter by symbol, market type, pattern, trust level, run ID, or date
- Use filters to narrow down signals by symbol, market type, trust level, or date`,

  why: `Use this page to:
1. **Browse all signals** — See everything the system detected
2. **Filter by criteria** — Find signals for specific symbols or patterns
3. **Understand trust levels** — See which patterns are reliable

**Trust Levels:**
- **TRUSTED** — 30+ successes, 80%+ coverage, positive returns. Used for trade proposals.
- **WATCH** — Promising but needs more data. Monitored but not traded.
- **UNTRUSTED** — Negative returns or insufficient data. Ignored.`,

  how: `Signals are generated by the RECOMMENDATIONS step of the daily pipeline. Pattern detection algorithms scan price data and produce signals when patterns are detected.

Each signal includes:
- The symbol and pattern that triggered it
- A score indicating strength
- Trust level based on historical performance
- Eligibility for trading (based on filters and rules)`,

  sources: [
    { object: 'MIP.APP.RECOMMENDATION_LOG', purpose: 'All generated signals with scores, patterns, and timestamps.' },
    { object: 'MIP.MART.V_TRUSTED_SIGNALS', purpose: 'Trust classification for each pattern.' },
    { object: 'MIP.MART.V_SIGNALS_ELIGIBLE_TODAY', purpose: 'Eligibility status for today\'s trading.' },
  ],
  fields: [
    { key: 'symbol', label: 'Symbol', meaning: 'The stock or asset ticker (e.g., AAPL, MSFT, EUR/USD).', glossaryKey: 'signals.symbol' },
    { key: 'market_type', label: 'Market type', meaning: 'Asset class: STOCK (individual stocks), ETF (exchange-traded funds), or FX (foreign exchange).', glossaryKey: 'signals.market_type' },
    { key: 'pattern_id', label: 'Pattern', meaning: 'Which detection algorithm generated this signal. Different patterns look for different price behaviors.', glossaryKey: 'signals.pattern_id' },
    { key: 'score', label: 'Score', meaning: 'Signal strength. Higher values = stronger pattern match. Used for ranking.', glossaryKey: 'signals.score' },
    { key: 'trust_label', label: 'Trust', meaning: 'Reliability: TRUSTED (proven), WATCH (promising), UNTRUSTED (avoid). Only TRUSTED patterns generate proposals.', glossaryKey: 'signals.trust_label' },
    { key: 'action', label: 'Action', meaning: 'Suggested direction: BUY (price expected to rise) or SELL (price expected to fall).', glossaryKey: 'signals.action' },
    { key: 'eligible', label: 'Eligible', meaning: 'Whether this signal can be traded today. False if already holding, at position limit, or other restrictions.', glossaryKey: 'signals.eligible' },
  ],
}

export const MARKET_TIMELINE_EXPLAIN_CONTEXT = {
  id: 'market-timeline',
  title: 'Market Timeline',
  what: `End-to-end view of what happened with each symbol — from signals to proposals to trades.

**Main Grid:**
Each card shows a symbol with:
- **Counts** — How many signals, proposals, and trades for that symbol
- **Trust badges** — Trust level for this symbol's patterns
- **ACTION badge** — Shows if there's a proposal for today

**Expanded Detail View (click a symbol):**
- **OHLC Chart** — Price candlesticks with event overlays (signals, proposals, trades marked on the chart)
- **Decision Narrative** — AI-generated explanation of what happened
- **Trust Summary** — Trust levels by pattern for this symbol
- **Recent Events** — Table of signals, proposals, trades with timestamps`,

  why: `Use this for deep investigation into a specific symbol:
1. **See the full journey** — Signal -> Proposal -> Trade timeline
2. **Visual context** — Events overlaid on the price chart
3. **Understand decisions** — Why proposals were made or rejected
4. **Pattern performance** — Trust summary shows which patterns work for this symbol

**Key insight:** The chart shows WHERE on the price timeline events occurred, helping you see if signals appeared at good entry/exit points.`,

  how: `Data is composed from multiple sources:
- **RECOMMENDATION_LOG** — Signals for this symbol
- **ORDER_PROPOSALS** — Proposals (suggested trades)
- **PORTFOLIO_TRADES** — Executed trades
- **MARKET_BARS** — OHLC price data for charts

The decision narrative is generated by analyzing the sequence of events and explaining the reasoning.`,

  sources: [
    { object: 'MIP.APP.RECOMMENDATION_LOG', purpose: 'Signals per symbol with timestamps.' },
    { object: 'MIP.AGENT_OUT.ORDER_PROPOSALS', purpose: 'Trade proposals with status.' },
    { object: 'MIP.APP.PORTFOLIO_TRADES', purpose: 'Executed trades.' },
    { object: 'MIP.MART.MARKET_BARS', purpose: 'OHLC price data for charts.' },
    { object: 'MIP.MART.V_TRUSTED_SIGNALS', purpose: 'Trust classification per pattern.' },
  ],
  fields: [
    { key: 'signal_count', label: 'Signals', meaning: 'Number of signals generated for this symbol in the selected window.', glossaryKey: 'market_timeline.signal_count' },
    { key: 'proposal_count', label: 'Proposals', meaning: 'Number of trade proposals created (passed initial filters).', glossaryKey: 'market_timeline.proposal_count' },
    { key: 'trade_count', label: 'Trades', meaning: 'Number of actual trades executed for this symbol.', glossaryKey: 'market_timeline.trade_count' },
    { key: 'trust_badge', label: 'Trust badge', meaning: 'Trust level for this symbol\'s primary pattern. TRUSTED, WATCH, or UNTRUSTED.', glossaryKey: 'market_timeline.trust_badge' },
    { key: 'action_badge', label: 'ACTION badge', meaning: 'Shows if there\'s an active proposal for this symbol today.', glossaryKey: 'market_timeline.action_badge' },
    { key: 'ohlc_chart', label: 'OHLC chart', meaning: 'Candlestick price chart with events overlaid. Signals, proposals, and trades are marked at their occurrence points.', glossaryKey: 'market_timeline.ohlc_chart' },
    { key: 'decision_narrative', label: 'Decision narrative', meaning: 'Plain-English explanation of what happened with this symbol and why.', glossaryKey: 'market_timeline.decision_narrative' },
  ],
}

export const DEBUG_EXPLAIN_CONTEXT = {
  id: 'debug',
  title: 'Debug',
  what: `Route smoke checks: tests that each API endpoint responds correctly.

**Table Shows:**
- **URL** — API endpoint being tested
- **Status** — Green = OK (responding), Red = Error (failed)
- **Preview** — Sample of the response data

Use "Copy diagnostics" to copy all results for troubleshooting.`,

  why: `Use this page to quickly verify all API endpoints are working. If you're experiencing issues in other parts of the app, check here first to identify which endpoints may be failing.`,

  how: `Each row calls one API route and checks the response. This is a client-side smoke test — no Snowflake queries, just API health checks.`,

  sources: [],
  fields: [],
}

/** Build section context for Suggestions Evidence Drawer (selected item + horizon). */
export function buildSuggestionsEvidenceContext(item, horizonBars) {
  const horizonLabel = horizonBars ? `${horizonBars}-day horizon` : 'Selected horizon'
  return {
    id: 'suggestions-evidence',
    title: `Evidence: ${horizonLabel}`,
    what: `Detailed evidence for this symbol/pattern at the ${horizonBars || '?'}-bar horizon.

**What you'll see:**
- **What history suggests** — Summary of past performance
- **Horizon strip** — Returns at 1, 3, 5, 10, 20 bars
- **Distribution chart** — Histogram of realized returns
- **Confidence metrics** — Sample size, hit rate, coverage`,

    why: `Use this drawer to dig deeper into a suggestion before deciding to act. The distribution chart shows the spread of outcomes — a tight distribution means more predictable results.`,

    how: `Daily bars only. Realized return = (exit price - entry price) / entry price over ${horizonBars || 'N'} bars.

Data comes from RECOMMENDATION_OUTCOMES with EVAL_STATUS=COMPLETED.`,

    sources: [
      { object: 'MIP.APP.RECOMMENDATION_LOG', purpose: 'Recommendations for this symbol/pattern.' },
      { object: 'MIP.APP.RECOMMENDATION_OUTCOMES', purpose: 'Realized return per outcome.' },
    ],
    fields: [
      { key: 'horizon_bars', label: 'Horizon (bars)', meaning: 'Holding period in bars (e.g., 5 = 5 days for daily bars).', glossaryKey: 'performance.horizon_bars' },
      { key: 'realized_return', label: 'Realized return', meaning: 'Return from entry to exit over this horizon (as decimal, e.g., 0.05 = 5%).', glossaryKey: 'performance.realized_return', calc: '(exit - entry) / entry' },
      { key: 'pct_positive', label: '% positive', meaning: 'Share of outcomes with positive return. Higher = more consistent wins.', glossaryKey: 'performance.pct_positive' },
      { key: 'mean_realized_return', label: 'Mean return', meaning: 'Average return over this horizon across all samples.', glossaryKey: 'performance.mean_realized_return' },
      { key: 'distribution', label: 'Distribution chart', meaning: 'Histogram showing the spread of returns. Tight = predictable, wide = variable.', glossaryKey: 'performance.distribution' },
    ],
  }
}
