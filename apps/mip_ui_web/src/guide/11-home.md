# 11. Home

Your landing page. A quick overview of system health and shortcuts to the most-used pages.

## What You See on This Page

### Hero Banner

The title "Market Intelligence Platform" with the tagline "Daily-bar research • outcomes-based learning • explainable suggestions." This is decorative — it reminds you of MIP's purpose.

### Quick Actions

Five shortcut cards that take you directly to key pages:

| Card | Where it goes | What it shows |
|------|---------------|---------------|
| **View Portfolios** | /portfolios | All portfolios — positions, trades, episodes |
| **Default Portfolio** | /portfolios/1 | Quick link to your primary portfolio |
| **Open Cockpit** | /cockpit | AI narratives, portfolio status, training |
| **Open Training Status** | /training | Maturity by symbol and pattern |
| **Open Suggestions** | /suggestions | Ranked candidates from outcome history |

### System at a Glance — Three Metric Cards

- **Last pipeline run** — Shows how long ago the daily pipeline finished, plus its status (SUCCESS / FAILED / RUNNING). Example: "2 hours ago" with a green "SUCCESS" badge means the pipeline ran 2 hours ago and completed normally.

- **New evaluations since last run** — How many new outcome evaluations have been calculated since the last pipeline run. Example: "+12" means 12 new outcomes were computed. This number grows as time passes and more horizons become evaluable. If it says "0", no new outcomes are ready yet.

- **Latest digest (as-of)** — When the most recent AI digest was generated. Example: "3 hours ago" means the digest covers data up to 3 hours ago. "No digest yet" means no digest exists (run the pipeline first).
