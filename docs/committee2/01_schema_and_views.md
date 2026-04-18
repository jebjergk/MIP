# Committee 2.0 — Schema

| Object | Purpose |
|--------|---------|
| `MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT` | Immutable frozen proposal; `UNIQUE(PROPOSAL_ID)` |
| `MIP.APP.COMMITTEE_HEARING` | Current hearing; `UNIQUE(PROPOSAL_ID)`; VARIANT evidence/deltas/chair/operational |
| `MIP.APP.COMMITTEE_ROLE_OUTPUT` | PK `(HEARING_ID, ROLE_NAME)` |
| `MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT` | PK `(HEARING_ID, ARTIFACT_KIND)` |
| `MIP.APP.COMMITTEE_FINAL_DECISION` | Durable commit; `UNIQUE(HEARING_ID)` |

**Deploy order:** [`540_committee2_tables.sql`](../../SQL/app/540_committee2_tables.sql) → redeploy [`520_sp_propose_structural_trades.sql`](../../SQL/app/520_sp_propose_structural_trades.sql) → [`20260418_committee2_config_grants.sql`](../../SQL/migrations/20260418_committee2_config_grants.sql).

**Existing proposals:** run [`backfill_committee2_proposal_snapshots.sql`](../../SQL/scripts/backfill_committee2_proposal_snapshots.sql) once if proposals predate the snapshot table.

**Stage 4 marts:** join `COMMITTEE_FINAL_DECISION` to live trades for evaluation (future).
