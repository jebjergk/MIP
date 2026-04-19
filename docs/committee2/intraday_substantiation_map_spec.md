# Intraday substantiation map (`INTRADAY_SUBSTANTIATION_MAP`)

Evidence-only Committee 2.0 artifact: compares **expected** behavior (from proposal/setup templates) to **observed** 15m session price/volume in a compact chart. Does **not** change stance, confidence, or chair output.

## Data

- **Source:** **IBKR direct** at hearing refresh — `app.services.ibkr_live_bars.run_agent_ibkr_live_bars` → `cursorfiles/fetch_ibkr_live_bars.py` (15m bars, **no Snowflake**; stored 15m in MIP is not required).
- **Window:** Configurable `window_bars` (default 48), oldest → newest (`meta.window_*_ts`).
- **Degrade:** Fewer than two valid bars, IB unreachable, or subprocess failure → **no artifact** (no UI tile).

## Payload (schema v1)

- `headline`: `"Expected vs observed today"` (UI may bind literally).
- `bars[]`: `{ ts, o, h, l, c, v }`.
- `overlays`: `entry_zone`, `invalidation` (level, rule, breached), `last_price`, `window_start_range` (first bar H/L — **not** labeled as session opening range), `window_start_label`, `support_levels` / `resistance_levels` (e.g. Window low/high, optional prior close).
- `proposal_marker`: optional `{ ts, label, bar_index }` when `PROPOSAL_TS` ∈ window.
- `markers[]`: capped (≤5) swing / event markers for sparse on-chart glyphs.
- `captions`: `proposal_expectation`, `session_behavior_badge`, `interpretation_line`, `trader_verdict_line` (fixed three strings), `verdict_bucket` (`SUPPORTS` | `MIXED` | `CHALLENGES`).

## Verdict lines (trader-facing)

| `verdict_bucket` | `trader_verdict_line`                          |
|------------------|------------------------------------------------|
| SUPPORTS         | Today supports the proposal                    |
| MIXED            | Today is mixed vs proposal                     |
| CHALLENGES       | Today challenges immediate entry               |

Derived only from intraday heuristics (zone, invalidation breach, chop, extension); **not** from committee stance.

## UI

- **LPA:** Tile immediately after **Geometry vs proposal** when `exhibit_intraday_substantiation_map` is present.
- **Full hearing:** Same exhibit from API top-level or `artifacts`.

## Operations

- Built in `_run_refresh` after `compute_hearing_bundle`. IB fetch errors are swallowed so committee refresh does not fail when TWS/Gateway is down or bars are unavailable.
