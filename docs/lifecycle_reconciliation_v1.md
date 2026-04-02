# Lifecycle broker reconciliation (RECON_V1)

## Target state

- **Interactive Brokers** is the source of truth for **whether a position exists** and **at what quantity** (latest NAV-linked `BROKER_SNAPSHOTS` POSITION rows, surfaced via symbol-tracker tiles).
- **MIP** owns **lifecycle intent** (`LIVE_ACTIONS`), **orders/fills** (`LIVE_ORDERS`), **entry intelligence linkage** (`ENTRY_INTEL_ACTION_LINK`, `ENTRY_INTEL_SNAPSHOT`), and **closeout** (`TRADE_CLOSEOUT`).
- When reality diverges—especially **broker-originated** trades or flattens—MIP must **classify** the situation, **persist** it, and **surface** it in LIC without inventing links.

## Reconciliation classes (v1)

| Class | Meaning |
|--------|---------|
| `LINKED` | Open IB position; bootstrap lifecycle row present (`EXECUTED` entry + link + intel path used by LIC). |
| `POSITION_IN_IB_NOT_IN_MIP` | IB open position; no `ENTRY` row in `LIVE_ACTIONS` for that symbol (pure broker-originated from MIP’s perspective). |
| `BROKER_ORIGINATED_UNLINKED` | IB position; `ENTRY` exists but **no** `ENTRY_INTEL_ACTION_LINK` (or not the linked path LIC uses). |
| `STATUS_MISMATCH` | IB position; link may exist but **status vs fills** disagree (e.g. `EXECUTED` without fill rows, or non-terminal status while IB shows size). |
| `POSITION_DRIFT` | Same as linked for analysis purposes, but **quantity** on the MIP entry (`PROPOSED_QTY`) differs materially from IB size. |
| `FLAT_IN_IB_NOT_IN_MIP` | **Ghost**: `EXECUTED` + link + **no** `TRADE_CLOSEOUT`, symbol **not** in current open IB tiles — IB flat, MIP still “open”. |

`RECONCILED` is reserved for future use (e.g. explicit operator clears); v1 does not emit it on tiles.

## Detection logic and confidence (v1)

**Inputs**

- Open symbols and quantities from **bootstrap tiles** (same query chain as `/symbol-tracker/tiles`).
- Latest **`ENTRY`** `LIVE_ACTIONS` row per symbol (`UPDATED_AT` desc).
- **`ENTRY_INTEL_ACTION_LINK`** existence for that action id.
- **`LIVE_ORDERS`**: any row with status in `FILLED`, `PARTIAL_FILL`, `PARTIALLY_FILLED` for that action id.
- **Bootstrap lifecycle** map (`fetch_entry_lifecycle_rows` / `build_operator_entry_lifecycle`) — if present, `LINKED` is the baseline for intel.

**Rules (deterministic)**

1. If lifecycle payload has `has_entry_intel_link` **true** → base `LINKED`; if `PROPOSED_QTY` vs IB abs qty fails tolerance → `POSITION_DRIFT`.
2. Else if no `ENTRY` row → `POSITION_IN_IB_NOT_IN_MIP`.
3. Else if no link on latest `ENTRY` → `BROKER_ORIGINATED_UNLINKED`.
4. Else if status ≠ `EXECUTED` **or** no fill flag on orders → `STATUS_MISMATCH`.
5. Else (link + `EXECUTED` + fills but no lifecycle row — e.g. missing EIS) → `STATUS_MISMATCH` (do not pretend full intel).

**Tolerance (qty drift)**  
Absolute difference &gt; `1` **and** &gt; `5%` of `max(broker_qty, proposed_qty)` → `POSITION_DRIFT`.

**Limits / caveats**

- v1 does **not** match individual fills to IB lots; no options multi-leg pairing.
- Ghost detection depends on **closeout** presence; delayed closeout writes can false-positive `FLAT_IN_IB_NOT_IN_MIP`.
- Stale `LIFECYCLE_RECONCILIATION_STATE` rows may remain for symbols that **closed** until a later cleanup policy is added.

## Persistence

- **`MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE`**: one row per `(PORTFOLIO_ID, SYMBOL)` — latest evaluation at last successful bootstrap persist.
- **`MIP.LIVE.LIFECYCLE_RECONCILIATION_EVENT`**: append-only when **`RECONCILIATION_CLASS` changes** vs previous state.

Grants: `MIP_UI_API_ROLE` — `SELECT`, `INSERT`, `UPDATE` on `STATE`; `SELECT`, `INSERT` on `EVENT` (see migration).

## LIC / API

Bootstrap JSON adds:

- `reconciliation_by_symbol`: map keyed by symbol (open tiles + same keys as tracker).
- `reconciliation_meta`: `rule_version`, `ghost_symbols[]`, `matching_rules_summary`.

LIC Snapshot tab shows a **Broker reconciliation** banner when `requires_operator_attention` is true, and a **portfolio-level ghost** strip when `ghost_symbols` is non-empty.

## Safe behavior when not `LINKED`

**Decisioning / automation**

- Do **not** treat pre-trade analysis, committee narrative, or expectation bands as authoritative for **sizing or risk** when class is not `LINKED` (including `POSITION_DRIFT`).
- Prefer existing live gates (`BROKER_RECONCILIATION_REQUIRED`, drift flags) where they apply; RECON_V1 is **orthogonal visibility** for lifecycle vs broker.
- **Never** insert fake `ENTRY_INTEL_ACTION_LINK` rows to silence the UI.

**Operations**

- **Broker-originated entry**: classify `POSITION_IN_IB_NOT_IN_MIP` or `BROKER_ORIGINATED_UNLINKED`; backfill real MIP actions + links only when auditable.
- **Broker-originated flatten**: expect `FLAT_IN_IB_NOT_IN_MIP` for ghost symbols until `TRADE_CLOSEOUT` (or lifecycle correction) reflects reality.

## PBR-style case (proposal 6107 pattern)

**Recommendation:** classify as **`BROKER_ORIGINATED_UNLINKED`** / **`STATUS_MISMATCH`** (depending on whether an `ENTRY` row exists and whether `EXECUTED` matches fills)—**not** a synthetic link.

- **Manual one-off repair** is appropriate only if operators can **truthfully** attach entry intel to the real `ENTRY_ACTION_ID` (e.g. research import produced EIS + `insert_entry_intel_action_link`).
- **Partial link with marker** is **not** implemented in v1: if the link is not trustworthy, remain explicit `STATUS_MISMATCH` / unlinked.

## Smoke / validation

See `MIP/SQL/smoke/22_lifecycle_reconciliation_v1.sql`.

**Scenarios to exercise in a dev account**

1. **Direct IB entry**: open a small position in IB only → expect `POSITION_IN_IB_NOT_IN_MIP` on that symbol after snapshot ingest + LIC bootstrap.
2. **Direct IB flatten**: close in IB while MIP still has `EXECUTED`+link+no closeout → expect symbol in `reconciliation_meta.ghost_symbols` (`FLAT_IN_IB_NOT_IN_MIP`) on next bootstrap.
