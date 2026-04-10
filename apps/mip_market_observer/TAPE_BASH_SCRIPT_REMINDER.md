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

## Windows: “Python wurde nicht gefunden” / Store opens

`python` on PATH is often the Microsoft Store stub. The script now prefers `mip_market_observer/.venv`, then repo `cursorfiles/.venv/Scripts/python.exe`, then `py -3`, and only then `python3`/`python` (each must pass `import sys`).

- **Quick fix:** `export PYTHON="/c/Users/you/.../mip_0.7/cursorfiles/.venv/Scripts/python.exe"` then `bash ... tape-observer.sh start` (use your real path; install observer deps in that venv or use a dedicated `mip_market_observer/.venv`).
- **Or:** Settings → Apps → Advanced app settings → App execution aliases → turn **off** `python.exe` and `python3.exe`.

If `cursorfiles` venv lacks packages, create `MIP/apps/mip_market_observer/.venv` and `pip install -r requirements.txt` there; `start` will use it first.
