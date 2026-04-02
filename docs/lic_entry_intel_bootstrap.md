# Live Intelligence Cockpit — entry analysis bootstrap

## Data flow

1. **Page load** calls `GET /live-intelligence/bootstrap` once.
2. The bootstrap payload includes **`entry_lifecycle_by_symbol`**: for each open position symbol, if there is a latest **EXECUTED** `LIVE_ACTIONS` row with an **`ENTRY_INTEL_ACTION_LINK`**, the API attaches operator-facing blocks built from:
   - `ENTRY_INTEL_SNAPSHOT` (`WORLDS_SPEC`, `ALPHA_SPEC`)
   - `COMMITTEE_VERDICT` (when `COMMITTEE_RUN_ID` is set on the entry action)
   - `TRADE_CLOSEOUT` (when present)
3. The React app stores this map in **`entryLifecycleBySymbol`** and passes the row for the **selected symbol** into **`LicEntryIntelligencePanel`** (Snapshot tab).

## Page-open vs runtime

| Source | What updates |
|--------|----------------|
| **Snowflake bootstrap** | Positions, analog episodes, portfolio context, **entry lifecycle** |
| **`refreshLive` / `useVisibleInterval`** | IB live bars + `deterministic-step` only — **does not** refetch bootstrap or mutate `entryLifecycleBySymbol` |

Operators must click **Reload bootstrap (Snowflake)** to refresh entry analysis after new links, committee runs, or closeouts.

## No Snowflake polling compliance

- There is **no** timer or interval that calls `/live-intelligence/bootstrap` or `/live/entry-intel/*` while the page stays open.
- The existing ~30s interval only runs **`refreshLive`** (IB + deterministic step), which does not include entry lifecycle.

## Related API (optional inspection)

- `GET /live/entry-intel/summary/by-action/{action_id}` — same underlying facts as bootstrap per action; useful for curl/debug.
- `GET /live/entry-intel/closeout/by-entry-action/{id}` — raw + parsed closeout when debugging.

## Validation (manual)

1. **Linked V2 baseline, open position:** open LIC → Snapshot → see **Pre-trade recommendation** and **Similar setups**; **Outcome** explains still open; expand **Technical details** for IDs.
2. **Closed with closeout:** if a symbol still appears with a closeout row, **Outcome** shows alignment / realized classes (unusual while position open).
3. **No link:** message under **Entry analysis** — no crash.
4. **Network:** after load, confirm no repeated `bootstrap` requests until explicit reload (DevTools).
