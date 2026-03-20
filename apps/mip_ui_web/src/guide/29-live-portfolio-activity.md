# 29. Live Portfolio Activity

Route: `/live-portfolio-activity`

Live Portfolio Activity is the operational timeline for live-linked paper workflow.

It connects import-time proposal sourcing, validation checks, approvals, and execution lifecycle outcomes.

## What you can verify here

- Current live-paper portfolio state and recent activity
- Actions moving through validation, compliance, and execution lifecycle
- Transitions that may need approval, investigation, or re-run follow-up
- Timestamps and reason fields for delayed or blocked actions

## Typical workflow

1. Confirm portfolio state freshness.
2. Review newest activity entries and status outcomes.
3. Trace unexpected blocks or delays to reason fields.
4. Cross-check with AI Agent Decisions for committee rationale.
5. Cross-check with Runs for pipeline/run-level context.

## When to use this page

- During live-paper operating windows.
- After approvals, to verify expected state transitions.
- When investigating execution/validation drift.

## Common status interpretation guidance

- A row can pass committee review but still fail later operational checks.
- Delays are often freshness/validation-related rather than strategy-related.
- Repeated transition stalls usually point to a systemic gate (policy, approvals, or data freshness), not random failure.

## Fast troubleshooting path

1. Find latest blocked/delayed row in activity feed.
2. Capture status + reason fields and timestamp.
3. Open matching decision in AI Agent Decisions.
4. Open matching run in Runs by time window/run ID.
5. Confirm whether block was expected policy behavior or an incident.
