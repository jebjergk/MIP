# MIP UI Web

React (Vite) frontend for MIP: Home, Portfolio, Audit Viewer, Morning Brief, Training Status. Read-only; no trading approvals or agent orchestration.

## Setup

1. Ensure the MIP UI API is running (see `MIP/apps/mip_ui_api/README.md`).
2. From repo root: `npm install` in `MIP/apps/mip_ui_web`, or from `MIP/apps/mip_ui_web`: `npm install`.
3. Run: `npm run dev` (from `MIP/apps/mip_ui_web`). Vite proxies `/api` to the backend at `http://127.0.0.1:8000`.

## Glossary (tooltips)

- **Single source of truth:** `MIP/docs/ux/UX_METRIC_GLOSSARY.yml`.
- **Generate JSON** (after editing YAML):
  - From repo root: `python scripts/generate_glossary_json.py` (requires `pip install pyyaml`), or
  - From `MIP/apps/mip_ui_web`: `npm run generate:glossary` (requires `yaml` devDependency).
  This writes `MIP/docs/ux/UX_METRIC_GLOSSARY.json` and `MIP/apps/mip_ui_web/src/data/UX_METRIC_GLOSSARY.json`.
- **Validate** (CI / pre-commit): run the same script with `--check`. It exits 1 if generated JSON would differ from on-disk JSON.
  - From repo root: `python scripts/generate_glossary_json.py --check`
  - From `MIP/apps/mip_ui_web`: `npm run validate:glossary`
  Use in CI or pre-commit so uncommitted JSON changes fail the check.

## Pages

- **Home** — landing and nav.
- **Portfolios** — list; click a portfolio for header + snapshot (positions, trades, daily, KPIs, risk).
- **Audit Viewer** — recent runs; click a run for timeline + interpreted summary.
- **Morning Brief** — select portfolio and load latest brief.
- **Training Status** — leaderboard row count and top entries (first draft).
