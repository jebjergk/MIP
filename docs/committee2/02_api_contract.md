# Committee 2.0 — API

Base path: `/committee` (FastAPI router).

| Method | Path | Notes |
|--------|------|------|
| POST | `/hearing/open` | Body: `{ "proposal_id": number, "force_rebuild"?: boolean }`. Get-or-build; idempotent hearing id per proposal. |
| GET | `/hearing/{hearing_id}` | Full dossier payload. |
| POST | `/hearing/{hearing_id}/refresh` | Atomic recompute. |
| POST | `/hearing/{hearing_id}/commit` | Body optional `action_id`, `trade_id`, `note`. Returns `already_committed` if duplicate. |
| GET | `/proposal/{proposal_id}/final-decision` | Latest committed row or null. |

**503** if `COMMITTEE2_ENABLED` is false.

**Operational** fields live under `operational`, `chair.execution_shaping`, `deltas`; **explanatory** copy is not used for execution logic.
