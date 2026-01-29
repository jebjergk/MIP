# Agent v0 Design Constants

Stable, deterministic "Morning Brief" agent for daily runs.

## Conventions

| Item | Value |
|------|--------|
| **Agent name** | `AGENT_V0_MORNING_BRIEF` |
| **Determinism key** | `(as_of_ts, signal_run_id, agent_name)` |
| **Output schema** | `MIP.AGENT_OUT` |
| **Allowed writes** | `MIP.AGENT_OUT.*` and `MIP.APP.MIP_AUDIT_LOG` only |

## Config

- **Table:** `MIP.APP.AGENT_CONFIG`
- **Keys:** `agent_name`, `min_n_signals`, `top_n_patterns`, `top_n_candidates`, `ranking_formula`, `enabled`
- Defaults for `AGENT_V0_MORNING_BRIEF`: min_n_signals=20, top_n_patterns=5, top_n_candidates=5, ranking_formula=`HIT_RATE_SUCCESS * AVG_RETURN_SUCCESS`, enabled=true

## Output table

- Agent morning brief rows: **`MIP.AGENT_OUT.AGENT_MORNING_BRIEF`** (spec name "MORNING_BRIEF"; this name coexists with portfolio `MORNING_BRIEF` in the same schema).
- Optional run log: **`MIP.AGENT_OUT.AGENT_RUN_LOG`**.

## Idempotency

- All DDL: `create table if not exists` / `create or replace` as appropriate.
- Upsert into agent output by `(AS_OF_TS, SIGNAL_RUN_ID, AGENT_NAME)` (MERGE in `SP_AGENT_GENERATE_MORNING_BRIEF`).
