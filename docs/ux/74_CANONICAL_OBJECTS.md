# Canonical Objects (UX Allowlist)

Authoritative list of Snowflake objects the read-only UX may reference. Derived from [71_UX_DATA_CONTRACT.md](71_UX_DATA_CONTRACT.md) and [72_UX_QUERIES.md](72_UX_QUERIES.md). Group by Tables / Views.

## Tables

**Tables (MIP.APP):** MIP_AUDIT_LOG, PORTFOLIO, PORTFOLIO_POSITIONS, PORTFOLIO_TRADES, PORTFOLIO_DAILY

**Tables (MIP.AGENT_OUT):** MORNING_BRIEF, ORDER_PROPOSALS

## Views

**Views (MIP.MART):** V_PORTFOLIO_RUN_KPIS, V_PORTFOLIO_RUN_EVENTS, V_PORTFOLIO_RISK_GATE, V_PORTFOLIO_RISK_STATE, V_TRAINING_LEADERBOARD, V_TRUSTED_SIGNAL_POLICY, V_SIGNAL_OUTCOME_KPIS

**Views (MIP.APP):** V_SIGNALS_ELIGIBLE_TODAY, V_TRUSTED_SIGNAL_CLASSIFICATION

## Stored Procedures

(none for UX read-only scope)

## Checks

(none for UX scope)

## Note

RECOMMENDATION_LOG and RECOMMENDATION_OUTCOMES are referenced in the data contract text; include them in the allowlist since the UX may reference them indirectly. APP_CONFIG is explicitly excluded (not in UX contract).
