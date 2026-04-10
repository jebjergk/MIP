# MIP Tape observer (Phase 1)

Read-only **Tape** service: executed trades + L1 quotes → rolling metrics → HTTP snapshot for Living Chart. Does **not** write to Snowflake and does not affect committee, training, or execution.

## Run locally (bash helper)

```bash
# from repo root — Git Bash / WSL / Linux / macOS
export TAPE_OBSERVER_SIMULATE=1   # optional: no TWS
bash MIP/apps/mip_market_observer/scripts/tape-observer.sh start
bash MIP/apps/mip_market_observer/scripts/tape-observer.sh status
```

See [`scripts/tape-observer.sh`](scripts/tape-observer.sh) for `stop | restart | health | init`.

## Run locally (manual)

```powershell
cd MIP\apps\mip_market_observer
pip install -r requirements.txt
# Optional: fast warmup for demos (default baseline samples = 30 wall-clock minutes)
# $env:TAPE_WARMUP_MIN_BASELINE = "5"
# Simulated tape (no TWS) for UI wiring:
$env:TAPE_OBSERVER_SIMULATE = "1"
python -m uvicorn app.main:app --host 127.0.0.1 --port 8095
```

## IBKR (live)

1. Start TWS or IB Gateway (paper typical: port **4002**).
2. Do **not** set `TAPE_OBSERVER_SIMULATE`.
3. Optional: `IBKR_HOST`, `IBKR_PORT`, `TAPE_IB_CLIENT_ID` (default **991**).

The UI API proxies snapshots when `TAPE_OBSERVER_BASE_URL` is set, e.g. `http://127.0.0.1:8095`.

## Endpoints

- `GET /health` — liveness + `ib_connected`
- `GET /tape/v1/snapshot?symbol=TSLA` — full Phase 1 snapshot (`snapshot_schema_version`, `threshold_profile_version`)

## Phase 2 (snapshot schema `2.0.0`)

Extended `move_quality`, `burst_score`, vacuum / absorption / exhaustion scores, `session_regime`, `overlay_hints` for the chart. Threshold bundle: `tape_phase2_v1`.

## Phase 3 — replay + debug

```env
TAPE_REPLAY_JSONL=C:/path/tape_periodic.jsonl
TAPE_REPLAY_ANOMALY_JSONL=C:/path/tape_anomaly.jsonl
TAPE_REPLAY_INTERVAL_SEC=5
TAPE_REPLAY_ANOMALY_COOLDOWN_SEC=12
TAPE_DEBUG_ENDPOINT=1
```

- `GET /tape/v1/snapshot/debug?symbol=TSLA` — full snapshot JSON without writing replay (requires `TAPE_DEBUG_ENDPOINT=1` on the observer).

## UI (Living Chart — binary tape)

Snapshots include **`tape_active_for_ui`** (boolean). It is `true` only when **all** hold: `feed_health == live`, `warmup_state == ready`, `quote_sizes_available`, `side_confidence_aggregate == high`, finite `buy_volume_60s` / `sell_volume_60s`, and IB connected (or simulate mode). A **dwell** then requires the strict gate to stay true for at least **`TAPE_UI_DWELL_SEC`** (default 8) and **`TAPE_UI_MIN_SNAPSHOTS`** (default 3) consecutive snapshot builds; any failure resets dwell.

**Exact age/warmup math** is documented in [`app/threshold_profile.py`](app/threshold_profile.py) and [`app/warmup.py`](app/warmup.py).

The **frontend shows tape strip, chips, overlays, and the recent buy/sell module only when `tape_active_for_ui === true`**. `VITE_TAPE_OBSERVER_ENABLED=1` only allows the client to **call** the tape API — it does not show tape UI by itself.

Set in `mip_ui_web` env:

```env
VITE_TAPE_OBSERVER_ENABLED=1
```

And in **mip_ui_api** repo root `.env` (see `mip_ui_api` `config.py`):

```env
TAPE_OBSERVER_BASE_URL=http://127.0.0.1:8095
```

When `tape_active_for_ui` is false, the observer logs a throttled **diagnostic** reason (`tape_active_for_ui=false symbol=... reason=...`) — not intended as end-user chart copy.

Proxy debug: `GET /api/observation/tape/v1/snapshot/debug?symbol=TSLA` (same gate on observer).

## Ops

Control script: [`scripts/tape-observer.sh`](scripts/tape-observer.sh) and [`TAPE_BASH_SCRIPT_REMINDER.md`](TAPE_BASH_SCRIPT_REMINDER.md).
