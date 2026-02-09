# MIP UI API

Read-only FastAPI backend for MIP: pipeline runs, portfolios, AI digests, and training status. No writes to Snowflake.

## Setup

1. Copy `.env.example` from repo root to repo root as `.env` and set Snowflake connection variables.
2. From repo root: `pip install -r MIP/apps/mip_ui_api/requirements.txt`
3. Run: `uvicorn app.main:app --reload --app-dir MIP/apps/mip_ui_api`

Or from `MIP/apps/mip_ui_api`: `uvicorn app.main:app --reload`

## Endpoints

- `GET /runs` — recent pipeline runs
- `GET /runs/{run_id}` — timeline + interpreted summary (summary_cards, narrative_bullets)
- `GET /portfolios` — portfolio list
- `GET /portfolios/{portfolio_id}` — portfolio header
- `GET /portfolios/{portfolio_id}/snapshot?run_id=...` — positions, trades, daily, KPIs, risk
- `GET /digest/latest?portfolio_id=...` — latest AI digest for portfolio
- `GET /training/digest/latest` — latest global training digest
- `GET /training/status` — training status
