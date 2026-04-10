# MIP Tape observer (Phase 1)

Read-only **Tape** service: executed trades + L1 quotes → rolling metrics → HTTP snapshot for Living Chart. Does **not** write to Snowflake and does not affect committee, training, or execution.

## Run locally

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

## UI

Set in `mip_ui_web` env:

```env
VITE_TAPE_OBSERVER_ENABLED=1
```

And in **mip_ui_api** `.env`:

```env
TAPE_OBSERVER_BASE_URL=http://127.0.0.1:8095
```
