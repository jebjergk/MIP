# Tape observer — bash control script

Implemented: [`scripts/tape-observer.sh`](scripts/tape-observer.sh)

## Quick use (Git Bash / WSL / Linux / macOS)

From anywhere:

```bash
bash MIP/apps/mip_market_observer/scripts/tape-observer.sh start
bash MIP/apps/mip_market_observer/scripts/tape-observer.sh status
bash MIP/apps/mip_market_observer/scripts/tape-observer.sh health
bash MIP/apps/mip_market_observer/scripts/tape-observer.sh init
bash MIP/apps/mip_market_observer/scripts/tape-observer.sh stop
```

Or `cd MIP/apps/mip_market_observer/scripts && chmod +x tape-observer.sh && ./tape-observer.sh start`

## Simulate (no TWS)

```bash
export TAPE_OBSERVER_SIMULATE=1
bash MIP/apps/mip_market_observer/scripts/tape-observer.sh restart
```

## Files created at runtime

- `.tape-observer.pid` — process id (gitignored)
- `.tape-observer.log` — uvicorn stdout/stderr (gitignored)

## mip_ui_api

Set `TAPE_OBSERVER_BASE_URL=http://127.0.0.1:8095` (or your `TAPE_HOST`/`TAPE_PORT`) and restart the API.
