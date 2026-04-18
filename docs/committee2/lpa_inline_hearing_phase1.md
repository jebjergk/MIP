# LPA inline Committee 2.0 — Phase 1

## Purpose

Live Portfolio Activity (LPA) is the **default surface** for structural **entry**: one POST to `POST /live/trades/actions/{action_id}/committee2/orchestrate` refreshes the hearing, commits the final decision to the action, and materializes LIVE. The API returns an **`inline_hearing`** DTO so the web client can render **compact proof exhibits** without reverse-engineering raw artifact JSON.

Standalone **Structural Committee Hearing** and the timeline remain available for deep review.

## Operator journey (entry)

1. Open LPA → Pending Decisions → structural entry row.
2. Click **Run Committee 2.0** (or **Refresh decision** after a prior run). Progress messages rotate client-side until the response returns.
3. Expand **Show proof exhibits** to review geometry, path, regime continuity, protection, fingerprint, “since proposal” strip, and chair board.
4. Use **Open full hearing** for the full room; optional **Advanced → Legacy SSE sync** for the older stream path.
5. Continue approvals / revalidation / Submit as today.

## `inline_hearing` keys (Phase 1)

| Key | Role |
|-----|------|
| `action_id`, `proposal_id`, `hearing_id` | Binding |
| `symbol`, `setup_family`, `direction`, `trust_label` | Identity |
| `stance`, `confidence` | Echo of hearing row |
| `hearing_ts`, `hearing_updated_at`, `evidence_bar_date`, `stale_hint` | Freshness |
| `exhibit_geometry_hero` | Zone, last price, distance %, invalidation, optional `post_proposal_path_trace` |
| `exhibit_path_quality` | Adverse-before-favorable, MHR, label, interpretation |
| `exhibit_regime_continuity` | Proposal vs now regime/structure + continuity verdict |
| `exhibit_protection` | Cushion %, breach, trail/size posture |
| `exhibit_symbol_fingerprint` | Honest one-liner, bullets, badge |
| `what_changed_strip` | Up to 5 short lines |
| `chair_board` | Supports, tensions, execution shaping |
| `roles_compact` | Collapsed specialist summaries |
| `artifacts` | Raw artifacts (optional consumer use) |

## Engine notes (differentiation)

- **Confidence** is a clamped continuous score: stance anchor plus adjustments from zone distance, path metrics, breach, thesis/regime/chase flags (see `compute_hearing_bundle` in `mip_ui_api` `engine.py`).
- **Chair** supports/tensions cite numeric evidence (zone distance, path stats, invalidation cushion, etc.).
- **SYMBOL_BEHAVIOR** / **SYMBOL_FINGERPRINT** use trust, vol, path bucket, and geometry distance; when inputs are thin, the copy states that explicitly.

## Structural exit (LPA copy)

Structural **exit** is **execution-only** (`STRUCTURAL_EXIT_EXECUTION_ONLY`). LPA user-visible strings avoid implying “Committee 2.0” for exit; the legacy SSE button is labeled **Replay execution verdict**.

## Reserved (not Phase 1)

Bounded LLM / politician disclosure layers, second hidden sync passes, heavy charting libraries, and multi-proposal choosers on LPA are **out of scope** for Phase 1. The `inline_hearing` model leaves room for future fields without breaking clients that ignore unknown keys.
