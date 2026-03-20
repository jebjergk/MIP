# 28. AI Agent Decisions

This page shows what the committee decided for each proposed trade in simulation and live workflows.

Think of it as the decision courtroom: verdict, reason, evidence, and execution-readiness checks.

## What You Can Do Here

- Review **Simulation Decisions** (paper environment)
- Review **Live Decisions** (real execution workflow)
- Filter by status and optional run id
- Open a row to inspect detailed committee outputs and rationale

## Key Concepts on This Page

| Label | Plain-English meaning |
|-------|------------------------|
| **Committee verdict** | Final yes/no/maybe decision from the AI committee |
| **Committee summary** | Short explanation of why that verdict happened |
| **Reason codes** | Structured tags explaining blocks, caution, or approval |
| **Revalidation outcome** | Whether the proposal still passed checks right before execution |
| **Operational status** | Where the action is in validation/compliance/intent/execution lifecycle |

## Typical Status Journey

In both simulation and live workflows, the exact set can differ, but the common path is:

`PROPOSED -> APPROVED -> REJECTED or EXECUTED`

Live mode includes additional operational statuses (validation, compliance, and intent stages) before final execution.

## How to interpret reason codes

- Treat reason codes as machine-readable diagnostics, not prose.
- A single action can have multiple reason codes (for example confidence + policy + freshness).
- Repeating reason patterns across many rows usually indicate a policy threshold or data-freshness issue, not a one-off anomaly.

Use Runs and Live Portfolio Activity to confirm whether the reason was expected for that run window.

## How To Read A Row Quickly

1. Check **status** first (where in the flow this action is).
2. Check **committee verdict/summary** (why it moved or got blocked).
3. Check **reason codes** and **revalidation outcome**.
4. Open details for full reasoning, payload, and lifecycle metadata.

## Best Time To Use This Page

- After a pipeline run, to confirm what the committee accepted or rejected
- During incident review, when expected trades did not execute
- Before changing strategy rules, to see recurring rejection patterns

## Common mistakes to avoid

- **Assuming APPROVED means already executed**: execution can still fail or be blocked later.
- **Reading one row in isolation**: always compare with run context and neighboring decisions.
- **Ignoring mode**: simulation and live rows can share labels but represent different operational constraints.
