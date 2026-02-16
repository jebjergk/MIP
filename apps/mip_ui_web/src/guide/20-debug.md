# 20. Debug

A technical health check page. Calls the backend API endpoints one by one and shows whether each responds correctly. Primarily for developers and system administrators.

## What It Does

Automatically fires requests to 5 key API endpoints and reports the results:

| Endpoint | What it checks |
|----------|----------------|
| `/api/status` | Is the API server running? |
| `/api/runs` | Can we fetch pipeline runs? |
| `/api/portfolios` | Can we fetch the portfolio list? |
| `/api/digest/latest` | Is the latest digest available? |
| `/api/training/status` | Is training data accessible? |

## How to Read Results

| Status | Meaning | Action |
|--------|---------|--------|
| **200** | Success — endpoint is working | No action needed |
| **404** | Not found — endpoint doesn't exist | Check if the API server is running the correct version |
| **500** | Server error — backend crashed | Check Snowflake credentials and database connectivity |
| **0 (network)** | Cannot reach server at all | Check if the API server is running, check proxy/CORS settings |

The **Copy diagnostics** button copies all results as JSON to your clipboard — useful for sharing with support.
