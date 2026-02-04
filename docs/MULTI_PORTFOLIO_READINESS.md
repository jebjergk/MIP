# Multi-Portfolio Readiness

The MIP stack is designed so **multiple portfolios can be active at the same time**. All Snowflake objects, pipeline scripts, API, and UX scope by `PORTFOLIO_ID` and support multiple active portfolios.

---

## 1. Snowflake tables

- **PORTFOLIO**: One row per portfolio; `STATUS = 'ACTIVE'` for those in use. No singleton assumption.
- **PORTFOLIO_DAILY**, **PORTFOLIO_TRADES**, **PORTFOLIO_POSITIONS**, **PORTFOLIO_EPISODE**: All keyed by `PORTFOLIO_ID`; multiple portfolios coexist.
- **MORNING_BRIEF**, **ORDER_PROPOSALS**: Include `PORTFOLIO_ID`; briefs and proposals are per portfolio.
- **PORTFOLIO_PROFILE**: Shared by portfolios via `PORTFOLIO.PROFILE_ID`; each portfolio points to one profile.

---

## 2. Snowflake views

- **V_PORTFOLIO_OPEN_POSITIONS_CANONICAL**: Joins by `PORTFOLIO_ID`; when an active episode exists, open positions are scoped to that episode (`ENTRY_TS >= episode.START_TS`). One logical set of “open” positions per portfolio.
- **V_PORTFOLIO_RISK_GATE**, **V_PORTFOLIO_RUN_KPIS**, **V_PORTFOLIO_RUN_EVENTS**: Partition or join by `PORTFOLIO_ID`; “latest run” is per portfolio.
- **V_PORTFOLIO_ACTIVE_EPISODE**: One row per portfolio with an ACTIVE episode.
- **V_MORNING_BRIEF_JSON** (and brief-related views): Built from `PORTFOLIO` where `STATUS = 'ACTIVE'`; content is per portfolio.

No view assumes a single “current” portfolio; all are multi-portfolio safe.

---

## 3. Pipeline scripts and stored procedures

- **SP_PIPELINE_RUN_PORTFOLIOS**: Loops over all `STATUS = 'ACTIVE'` portfolios and calls `SP_PIPELINE_RUN_PORTFOLIO(P_PORTFOLIO_ID, ...)` for each.
- **SP_PIPELINE_WRITE_MORNING_BRIEFS**: Loops over all `STATUS = 'ACTIVE'` portfolios and calls `SP_PIPELINE_WRITE_MORNING_BRIEF(P_PORTFOLIO_ID, ...)` for each. No `PORTFOLIO_ID > 0` filter; all active portfolios are processed.
- **SP_RUN_PORTFOLIO_SIMULATION**, **SP_AGENT_PROPOSE_TRADES**, **SP_VALIDATE_AND_EXECUTE_PROPOSALS**, **SP_WRITE_MORNING_BRIEF**, **SP_START_PORTFOLIO_EPISODE**, etc.: All take `P_PORTFOLIO_ID` (or equivalent); no hardcoded portfolio.

Operational scripts (e.g. `reset_portfolio_to_initial.sql`) use a variable such as `reset_portfolio_id`; the operator sets it for the portfolio to reset.

---

## 4. API (UX API)

- **GET /portfolios**: Returns all portfolios (list).
- **GET /portfolios/{portfolio_id}**: Single portfolio header.
- **GET /portfolios/{portfolio_id}/snapshot**, **/episodes**, **/timeline**, etc.: Every route is scoped by `portfolio_id` in the path.
- **GET /today?portfolio_id=...**: Optional `portfolio_id`; response is for that portfolio.
- **GET /live/metrics?portfolio_id=...**: Optional; defaults to `1` if omitted. Used by header/Suggestions; UI passes the chosen default portfolio.
- **GET /briefs/latest?portfolio_id=...**: Required for per-portfolio brief.

All portfolio-specific data is keyed by `portfolio_id`; the API does not assume a single active portfolio.

---

## 5. UX (mip_ui_web)

- **PortfolioContext**: Fetches `GET /portfolios` once; exposes `portfolios`, `defaultPortfolioId` (first ACTIVE by `PORTFOLIO_ID`), and `useDefaultPortfolioId()` (default or fallback `1`).
- **Home**: Uses `useDefaultPortfolioId()` for live metrics and “Default portfolio” quick link; “View Portfolios” links to the full list.
- **LiveHeader**: Uses `useDefaultPortfolioId()` for `GET /live/metrics` so the header reflects the default portfolio.
- **Suggestions**: Uses `useDefaultPortfolioId()` for live metrics.
- **Today**: Uses `usePortfolios()`; when URL has no `portfolio_id`, sets `?portfolio_id=<defaultPortfolioId>`. Portfolio dropdown in the status line switches portfolios.
- **Morning Brief**: Uses `usePortfolios()` for the list; default selection is `defaultPortfolioId` when available.
- **Portfolio (detail)**: Route `/portfolios/:portfolioId` and “Back to list”; list shows all portfolios.

Multiple active portfolios are supported: list, default (first active), and per-page selectors where needed.

---

## 6. Adding a second portfolio (Portfolio #2)

- **Bootstrap script**: `MIP/SQL/scripts/bootstrap_portfolio_2.sql` creates a second portfolio named `PORTFOLIO_2_LOW_RISK` (PROFILE_ID = 2, LOW_RISK), starting cash 100000, STATUS ACTIVE, and starts its initial ACTIVE episode. Idempotent: safe to rerun.
- **Pipeline**: No config change needed; `SP_PIPELINE_RUN_PORTFOLIOS` and `SP_PIPELINE_WRITE_MORNING_BRIEFS` select all `STATUS = 'ACTIVE'` portfolios.
- **Smoke (SQL)**: `MIP/SQL/smoke/portfolio_2_smoke.sql` — run after bootstrap; confirms two portfolios and Portfolio #2 active episode with PROFILE_ID = 2.
- **Smoke (API)**: After bootstrap, `GET /portfolios` returns two rows; each row includes `gate_state` (SAFE/CAUTION/STOPPED) and `active_episode` (episode_id, start_ts, profile_id). `GET /portfolios/{p2}/snapshot` returns that portfolio’s snapshot including active_episode and risk/profile.
