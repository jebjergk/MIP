# 16. AI Agent Decisions

This page shows what the committee decided for each proposed trade, in both simulation and live workflows.

Think of it as the "decision courtroom": you see the verdict, the reason, and the evidence.

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

## Typical Status Journey

In both simulation and live workflows, the exact set can differ, but the common path is:

`PROPOSED -> APPROVED -> REJECTED or EXECUTED`

Live mode includes additional operational statuses (validation, compliance, and intent stages) before final execution.

## How To Read A Row Quickly

1. Check **status** first (where in the flow this action is).
2. Check **committee verdict/summary** (why it moved or got blocked).
3. Open details for full reasoning and metadata.

## Best Time To Use This Page

- After a pipeline run, to confirm what the committee accepted or rejected
- During incident review, when expected trades did not execute
- Before changing strategy rules, to see recurring rejection patterns
