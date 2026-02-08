# UX Data Contract

Data contract for the read-only UX: object names, schemas (column list and types where useful), and grain. Only the canonical tables and views listed below are referenced.

## MIP.APP.MIP_AUDIT_LOG

- **Grain**: One event log entry per row.
- **Columns**: EVENT_TS (timestamp_ntz), RUN_ID (string), PARENT_RUN_ID (string), EVENT_TYPE (string), EVENT_NAME (string), STATUS (string), ROWS_AFFECTED (number), DETAILS (variant), ERROR_MESSAGE (string), INVOKED_BY_USER, INVOKED_BY_ROLE, INVOKED_WAREHOUSE, QUERY_ID, SESSION_ID.
- **Source**: `MIP/SQL/app/055_app_audit_log.sql`.

## MIP.AGENT_OUT.MORNING_BRIEF

- **Grain**: One brief per (PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME).
- **Columns**: BRIEF_ID (number identity), AS_OF_TS (timestamp_ntz), PORTFOLIO_ID (number), RUN_ID (varchar 64), BRIEF (variant), PIPELINE_RUN_ID (varchar 64), AGENT_NAME (varchar 128), STATUS (varchar 64), BRIEF_JSON (variant), CREATED_AT (timestamp_ntz), SIGNAL_RUN_ID (varchar 64).
- **Source**: `MIP/SQL/app/185_agent_out_morning_brief.sql`.

## MIP.APP.PORTFOLIO

- **Grain**: One portfolio per row.
- **Columns** (exact from 160_app_portfolio_tables.sql): PORTFOLIO_ID (number autoincrement), PROFILE_ID (number), NAME (string), BASE_CURRENCY (string default 'USD'), STARTING_CASH (number 18,2), LAST_SIMULATION_RUN_ID (string), LAST_SIMULATED_AT (timestamp_ntz), FINAL_EQUITY (number 18,2), TOTAL_RETURN (number 18,6), MAX_DRAWDOWN (number 18,6), WIN_DAYS (number), LOSS_DAYS (number), STATUS (string default 'ACTIVE'), BUST_AT (timestamp_ntz), NOTES (string), CREATED_AT (timestamp_ntz), UPDATED_AT (timestamp_ntz).
- **Source**: `MIP/SQL/app/160_app_portfolio_tables.sql`.

## MIP.APP.PORTFOLIO_POSITIONS

- **Grain**: One position entry per (PORTFOLIO_ID, RUN_ID, SYMBOL, ENTRY_TS).
- **Key columns**: PORTFOLIO_ID, RUN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, ENTRY_TS, ENTRY_PRICE, QUANTITY, COST_BASIS, ENTRY_SCORE, ENTRY_INDEX, HOLD_UNTIL_INDEX, CREATED_AT.
- **Source**: `MIP/SQL/app/160_app_portfolio_tables.sql`.

## MIP.APP.PORTFOLIO_TRADES

- **Grain**: One trade event per TRADE_ID.
- **Key columns**: TRADE_ID, PROPOSAL_ID, PORTFOLIO_ID, RUN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, TRADE_TS, SIDE, PRICE, QUANTITY, NOTIONAL, REALIZED_PNL, CASH_AFTER, SCORE, CREATED_AT.
- **Source**: `MIP/SQL/app/160_app_portfolio_tables.sql`.

## MIP.APP.PORTFOLIO_DAILY

- **Grain**: One portfolio-day per (PORTFOLIO_ID, RUN_ID, TS).
- **Key columns**: PORTFOLIO_ID, RUN_ID, TS, CASH, EQUITY_VALUE, TOTAL_EQUITY, OPEN_POSITIONS, DAILY_PNL, DAILY_RETURN, PEAK_EQUITY, DRAWDOWN, STATUS, CREATED_AT.
- **Source**: `MIP/SQL/app/160_app_portfolio_tables.sql`.

## MIP.MART.V_PORTFOLIO_RISK_GATE

- **Grain**: One row per portfolio; entry gate from latest run KPIs and open positions.
- **Key columns**: PORTFOLIO_ID, LATEST_RUN_ID, AS_OF_TS, CURRENT_BAR_INDEX, OPEN_POSITIONS, DRAWDOWN_STOP_TS, FIRST_FLAT_NO_POSITIONS_TS, DRAWDOWN_STOP_PCT, MAX_DRAWDOWN, ENTRIES_BLOCKED, BLOCK_REASON, RISK_STATUS.
- **Source**: `MIP/SQL/views/mart/v_portfolio_risk_gate.sql`.

## MIP.MART.V_PORTFOLIO_RISK_STATE

- **Grain**: One row per portfolio; centralized risk gating state (entry permissions).
- **Key columns**: PORTFOLIO_ID, RUN_ID, AS_OF_TS, OPEN_POSITIONS, ENTRIES_BLOCKED, ALLOWED_ACTIONS, STOP_REASON, BLOCK_REASON, RISK_STATUS.
- **Source**: `MIP/SQL/views/mart/v_portfolio_risk_state.sql`.

## MIP.MART.V_PORTFOLIO_RUN_KPIS

- **Grain**: One row per (PORTFOLIO_ID, RUN_ID).
- **Key columns**: PORTFOLIO_ID, RUN_ID, FROM_TS, TO_TS, TRADING_DAYS, STARTING_CASH, FINAL_EQUITY, TOTAL_RETURN, MAX_DRAWDOWN, PEAK_EQUITY, WIN_DAYS, LOSS_DAYS, DRAWDOWN_STOP_TS, etc.
- **Source**: `MIP/SQL/views/mart/v_portfolio_run_kpis.sql`.

## MIP.MART.V_PORTFOLIO_RUN_EVENTS

- **Grain**: One row per (PORTFOLIO_ID, RUN_ID); stop/event markers.
- **Key columns**: PORTFOLIO_ID, RUN_ID, DRAWDOWN_STOP_TS, FIRST_FLAT_NO_POSITIONS_TS, STOP_REASON.
- **Source**: `MIP/SQL/views/mart/v_portfolio_run_events.sql`.

## MIP.MART.V_TRAINING_LEADERBOARD

- **Grain**: One row per (PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS); training outcome KPIs.
- **Key columns**: PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS, N_SUCCESS, HIT_RATE_SUCCESS, AVG_RETURN_SUCCESS, SHARPE_LIKE_SUCCESS, LAST_SIGNAL_TS.
- **Source**: `MIP/SQL/mart/035_mart_training_views.sql`.

## MIP.APP.V_SIGNALS_ELIGIBLE_TODAY

- **Grain**: One row per eligible signal (today + history); control-plane view.
- **Key columns**: RUN_ID, RECOMMENDATION_ID, TS, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, PATTERN_ID, SCORE, DETAILS, TRUST_LABEL, RECOMMENDED_ACTION, IS_ELIGIBLE, GATING_REASON.
- **Source**: `MIP/SQL/app/165_signals_eligible_today.sql`.

## Trusted views and recommendation tables

- **V_TRUSTED_SIGNAL_CLASSIFICATION** (MIP.APP): trust/gating classification for signals; used by V_SIGNALS_ELIGIBLE_TODAY.
- **RECOMMENDATION_LOG** (MIP.APP): recommendation events; referenced by table catalog and trusted views.
- **RECOMMENDATION_OUTCOMES** (MIP.APP): evaluation results per recommendation/horizon.
- **PATTERN_DEFINITION** (MIP.APP): pattern metadata; optional join for labels in Training Status v1.
- **TRAINING_GATE_PARAMS** (MIP.APP): optional thresholds for Training Status v1; one active row (e.g. MIN_SIGNALS, MIN_HIT_RATE).

### Training Status v1 (GET /training/status)

- **Sources**: MIP.APP.RECOMMENDATION_LOG, MIP.APP.RECOMMENDATION_OUTCOMES; optional PATTERN_DEFINITION (labels), TRAINING_GATE_PARAMS (MIN_SIGNALS).
- **Grain**: One row per (market_type, symbol, pattern_id, interval_minutes) for INTERVAL_MINUTES = 1440.
- **Output fields**: market_type, symbol, pattern_id, interval_minutes, as_of_ts; recs_total, outcomes_total, horizons_covered, coverage_ratio; avg_outcome_h1, avg_outcome_h3, avg_outcome_h5, avg_outcome_h10, avg_outcome_h20 (null if missing); maturity_score (0–100), maturity_stage (INSUFFICIENT / WARMING_UP / LEARNING / CONFIDENT), reasons[] (plain-language). Exact SQL in [72_UX_QUERIES.md](72_UX_QUERIES.md).
- **V_TRUSTED_SIGNAL_POLICY**, **V_SIGNAL_OUTCOME_KPIS** (MIP.MART): policy and outcome KPIs feeding trust classification.

## MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT

- **Grain**: One snapshot per (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID).
- **Columns**: SNAPSHOT_ID (number identity), SCOPE (varchar 16 default 'PORTFOLIO'), PORTFOLIO_ID (number, nullable — NULL for GLOBAL), AS_OF_TS (timestamp_ntz), RUN_ID (varchar 64), SNAPSHOT_JSON (variant), SOURCE_FACTS_HASH (varchar 64), CREATED_AT (timestamp_ntz default current_timestamp()).
- **Source**: `MIP/SQL/app/200_agent_out_daily_digest_snapshot.sql`, migration `200b_alter_digest_add_scope.sql`.
- **MERGE key**: (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID) — idempotent; reruns update same row.
- **SCOPE values**: `PORTFOLIO` (per-portfolio digest), `GLOBAL` (system-wide digest with PORTFOLIO_ID = NULL).
- **PORTFOLIO SNAPSHOT_JSON top-level keys**: timestamps, gate, capacity, pipeline, signals, proposals, trades, training, kpis, exposure, portfolio_meta, detectors, prior_snapshot_ts.
- **GLOBAL SNAPSHOT_JSON top-level keys**: scope, timestamps, system, gates, capacity, pipeline, signals, proposals, trades, training, detectors, prior_snapshot_ts.
- **Portfolio detectors**: GATE_CHANGED, HEALTH_CHANGED, TRUST_CHANGED, NEAR_MISS, PROPOSAL_FUNNEL, NOTHING_HAPPENED, CAPACITY_STATE, CONFLICT_BLOCKED, KPI_MOVEMENT, TRAINING_PROGRESS.
- **Global detectors**: GATE_CHANGED_ANY, TRUST_DELTA, NO_NEW_BARS, PROPOSAL_FUNNEL_GLOBAL, NOTHING_HAPPENED, CAPACITY_GLOBAL, SIGNAL_COUNT_CHANGE.

## MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE

- **Grain**: One narrative per (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME).
- **Columns**: NARRATIVE_ID (number identity), SCOPE (varchar 16 default 'PORTFOLIO'), PORTFOLIO_ID (number, nullable — NULL for GLOBAL), AS_OF_TS (timestamp_ntz), RUN_ID (varchar 64), AGENT_NAME (varchar 128 default 'DAILY_DIGEST'), NARRATIVE_TEXT (string), NARRATIVE_JSON (variant), MODEL_INFO (varchar 256), SOURCE_FACTS_HASH (varchar 64), CREATED_AT (timestamp_ntz default current_timestamp()).
- **Source**: `MIP/SQL/app/201_agent_out_daily_digest_narrative.sql`, migration `200b_alter_digest_add_scope.sql`.
- **MERGE key**: (SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME) — idempotent.
- **NARRATIVE_JSON keys**: headline (string), what_changed (array), what_matters (array), waiting_for (array), where_to_look (array of {label, route}).
- **MODEL_INFO**: Cortex model name (e.g. 'mistral-large2') or 'DETERMINISTIC_FALLBACK' if Cortex was unavailable.
- **SOURCE_FACTS_HASH**: Must match the corresponding snapshot's hash — proves narrative is grounded in deterministic facts.

## MIP.MART.V_DAILY_DIGEST_SNAPSHOT

- **Grain**: One row per active portfolio; deterministic snapshot assembled from canonical truth views.
- **Output columns**: PORTFOLIO_ID (number), SNAPSHOT_JSON (variant).
- **Source**: `MIP/SQL/views/mart/v_daily_digest_snapshot.sql`.
- **Not persisted**: This is a view. The stored procedure materialises snapshots into `DAILY_DIGEST_SNAPSHOT`.

## MIP.MART.V_DAILY_DIGEST_SNAPSHOT_GLOBAL

- **Grain**: Exactly one row; system-wide deterministic snapshot aggregated across all active portfolios.
- **Output columns**: SNAPSHOT_JSON (variant).
- **Source**: `MIP/SQL/views/mart/v_daily_digest_snapshot_global.sql`.
- **SNAPSHOT_JSON keys**: scope, timestamps, system (active_portfolios, portfolio_summary), gates (ok_count, warn_count, blocked_count, per_portfolio), capacity (total_max_positions, total_open, total_remaining, saturation_pct), pipeline, signals (total_signals, total_eligible, by_market_type, top_ready_symbols), proposals (total_proposed, total_rejected, total_executed), trades (total_trades, total_buys, total_sells, total_realized_pnl), training (trusted_count, watch_count, untrusted_count), detectors, prior_snapshot_ts.
- **Not persisted**: This is a view. The stored procedure materialises global snapshots into `DAILY_DIGEST_SNAPSHOT` with SCOPE='GLOBAL'.

## MIP.AGENT_OUT.ORDER_PROPOSALS

- **Grain**: One proposal per PROPOSAL_ID.
- **Key columns**: PROPOSAL_ID, RUN_ID (varchar 64), RUN_ID_VARCHAR (varchar 64 canonical run key), PORTFOLIO_ID, PROPOSED_AT, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, SIDE, TARGET_WEIGHT, RECOMMENDATION_ID, SIGNAL_TS, STATUS, VALIDATION_ERRORS, APPROVED_AT, EXECUTED_AT, etc.
- **Source**: `MIP/SQL/app/187_agent_out_order_proposals.sql`.

---

## Executed Fields: Truthfulness and Verification

The Morning Brief displays an "Executed: N trades" (or "actions") count. This section defines the canonical source and verification semantics.

### Canonical Source

**Primary source**: `MIP.APP.PORTFOLIO_TRADES` (trade ledger)  
**Fallback source**: `MORNING_BRIEF.BRIEF:proposals.executed_trades[]` (brief record)

### Verification Status

The API returns a `verification_status` field indicating the data quality:

| Status | Meaning | UI Display |
|--------|---------|------------|
| `VERIFIED` | Count matches rows in PORTFOLIO_TRADES for this RUN_ID | "N trades" with ✓ badge |
| `MISMATCH` | Brief record differs from actual trade table | "N trades" with ⚠ badge + note |
| `EMPTY` | PORTFOLIO_TRADES has no rows for this run (possibly reset) | "N actions (from brief record)" with ? badge |
| `UNVERIFIABLE` | No RUN_ID or query failed | "— (unverifiable)" |

### Query Logic

1. Query `PORTFOLIO_TRADES WHERE PORTFOLIO_ID = :pid AND RUN_ID = :run_id`
2. Compare count to `brief_json.proposals.summary.executed`
3. Return verification status and actual rows (or brief record as fallback)

### API Response Fields

```json
{
  "summary": {
    "executed_count": 3,
    "executed_label": "trades",  // or "actions"
    "executed_trades_preview": [...],  // up to 10 rows
    "executed_trades_source": "MIP.APP.PORTFOLIO_TRADES",
    "executed_trades_note": null,  // or "trade history cleared by reset"
    "verification_status": "VERIFIED",  // VERIFIED | MISMATCH | EMPTY | UNVERIFIABLE
    "brief_record_count": 3  // what the brief JSON says
  }
}
```

### Reset Boundary Handling

After a portfolio reset (new episode), the trade history is cleared. The API handles this by:

1. Detecting `as_of_ts < episode_start_ts` (brief is from before reset)
2. If `verification_status == "EMPTY"` and `brief_record_count > 0`:
   - Show "N actions (from brief record)"
   - Note: "trade history cleared by reset"
3. UI shows a ? badge indicating data cannot be verified

### UI Behavior

- **Clickable "Executed" link**: Opens a modal/drawer showing the actual trade rows
- **Verification badge**: ✓ (green) = verified, ⚠ (yellow) = mismatch, ? (gray) = unverified
- **Modal header**: Shows verification status banner explaining the data source
- **Empty state**: If no rows, explains why (reset, unverifiable, etc.)

### Design Principles

1. **Never claim trades happened unless we can show the rows**
2. **Distinguish "trades" (verified ledger entries) from "actions" (brief record only)**
3. **Provide audit trail**: clicking shows proof or explains why proof is unavailable

---

## Training Journey Digest

### Tables

#### `MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT`

| Column | Type | Description |
|---|---|---|
| `SNAPSHOT_ID` | number (identity) | Auto-increment PK |
| `SCOPE` | varchar(32) | `GLOBAL_TRAINING` or `SYMBOL_TRAINING` |
| `SYMBOL` | varchar(32) | NULL for global scope |
| `MARKET_TYPE` | varchar(32) | NULL for global scope |
| `AS_OF_TS` | timestamp_ntz | Pipeline effective timestamp |
| `RUN_ID` | varchar(64) | Pipeline run identifier |
| `SNAPSHOT_JSON` | variant | Deterministic training facts |
| `SOURCE_FACTS_HASH` | varchar(64) | SHA-256 of serialised snapshot |
| `CREATED_AT` | timestamp_ntz | Row creation timestamp |

**Unique key**: `(SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID)`

#### `MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE`

| Column | Type | Description |
|---|---|---|
| `NARRATIVE_ID` | number (identity) | Auto-increment PK |
| `SCOPE` | varchar(32) | `GLOBAL_TRAINING` or `SYMBOL_TRAINING` |
| `SYMBOL` | varchar(32) | NULL for global scope |
| `MARKET_TYPE` | varchar(32) | NULL for global scope |
| `AS_OF_TS` | timestamp_ntz | Pipeline effective timestamp |
| `RUN_ID` | varchar(64) | Pipeline run identifier |
| `AGENT_NAME` | varchar(128) | Default `TRAINING_DIGEST` |
| `NARRATIVE_TEXT` | string | Raw narrative text from Cortex |
| `NARRATIVE_JSON` | variant | Structured: headline, what_changed, what_matters, waiting_for, where_to_look, journey |
| `MODEL_INFO` | varchar(256) | Model used (e.g. `mistral-large2`) or `DETERMINISTIC_FALLBACK` |
| `SOURCE_FACTS_HASH` | varchar(64) | Must match snapshot hash |
| `CREATED_AT` | timestamp_ntz | Row creation timestamp |

**Unique key**: `(SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID, AGENT_NAME)`

### Views

#### `MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_GLOBAL`

Single-row view returning `SNAPSHOT_JSON` with global training facts:

```json
{
  "scope": "GLOBAL_TRAINING",
  "timestamps": { "as_of_ts": "...", "snapshot_created_at": "..." },
  "thresholds": { "min_signals": 40, "min_hit_rate": 0.55, "min_avg_return": 0.0005 },
  "stages": {
    "total_symbols": 25,
    "insufficient_count": 12,
    "warming_up_count": 5,
    "learning_count": 5,
    "confident_count": 3,
    "avg_maturity_score": 38.5,
    "total_recommendations": 1200,
    "total_outcomes": 3500,
    "avg_coverage_ratio": 0.584
  },
  "trust": { "trusted_count": 8, "watch_count": 5, "untrusted_count": 20 },
  "near_miss_symbols": [ { "symbol": "...", "gap_to_next": 3.2, ... } ],
  "top_confident_symbols": [ { "symbol": "...", "maturity_score": 92.0, ... } ],
  "detectors": [ { "detector": "...", "fired": true, "severity": "HIGH", "detail": {...} } ]
}
```

#### `MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL`

One row per `(SYMBOL, MARKET_TYPE)` with `SNAPSHOT_JSON`:

```json
{
  "scope": "SYMBOL_TRAINING",
  "symbol": "AAPL",
  "market_type": "STOCK",
  "maturity": { "score": 62.5, "stage": "LEARNING", "score_sample": 30, "score_coverage": 20.5, "score_horizons": 12 },
  "evidence": { "recs_total": 45, "outcomes_total": 120, "hit_count": 65, "coverage_ratio": 0.533, "hit_rate": 0.5417, "avg_return": 0.0003 },
  "threshold_gaps": {
    "min_signals": 40, "signals_gap": 0, "signals_met": true,
    "min_hit_rate": 0.55, "hit_rate_gap": 0.0083, "hit_rate_met": false,
    "min_avg_return": 0.0005, "avg_return_gap": 0.0002, "avg_return_met": false
  },
  "trust": { "trust_label": "WATCH", "recommended_action": "MONITOR" },
  "journey_stage": "Earning trust"
}
```

### Narrative JSON Schema

Both global and per-symbol narratives share this schema:

```json
{
  "headline": "One sentence summary",
  "what_changed": ["bullet 1", "bullet 2"],
  "what_matters": ["bullet 1", "bullet 2"],
  "waiting_for": ["bullet 1", "bullet 2"],
  "where_to_look": [{"label": "Training Status", "route": "/training"}],
  "journey": ["Collecting evidence (12 symbols)", "Evaluating outcomes (5 symbols)", "Earning trust (5 symbols)", "Trade-eligible (3 symbols)"]
}
```

### Maturity Stages

| Stage | Score Range | Description |
|---|---|---|
| `INSUFFICIENT` | 0–24 | Not enough data; collecting evidence |
| `WARMING_UP` | 25–49 | Building sample; evaluating early outcomes |
| `LEARNING` | 50–74 | Meaningful data; approaching trust thresholds |
| `CONFIDENT` | 75–100 | Strong coverage; trade-eligible |

### Scoring Components (0–100)

| Component | Max Points | Computation |
|---|---|---|
| Sample size | 30 | `30 × min(1, recs_total / MIN_SIGNALS)` |
| Coverage | 40 | `40 × min(1, outcomes_total / (recs_total × 5))` |
| Horizons | 30 | `30 × horizons_covered / 5` |
