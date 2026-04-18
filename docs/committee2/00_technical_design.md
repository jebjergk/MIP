# Committee 2.0 — Technical design

## Purpose

Structural-only **hearing room**: compares **immutable proposal snapshot** to **hearing-time evidence**, returns **bounded execution-shaping stance** with full **evidence_refs** traceability. Does **not** replace broker-safe gates or `MIP.LIVE.COMMITTEE_*`.

## Locked behaviors

- **One `COMMITTEE_HEARING` row per `PROPOSAL_ID`**; refresh **overwrites**; no product history of intermediate refreshes.
- **`COMMITTEE_FINAL_DECISION`**: `UNIQUE(HEARING_ID)`; commit **idempotent** (`already_committed`).
- **Atomic refresh**: hearing + roles + artifacts in **one transaction** (FastAPI + Snowflake).
- **Frozen final decision** row must not require re-reading mutable hearing tables for evaluation fields.

## Architecture

- **Snowflake**: snapshots (from `SP_PROPOSE_STRUCTURAL_TRADES`), hearing tables, final decision, structural timeline views for bars.
- **FastAPI** (`routers/committee.py`): open / get / refresh / commit / final-decision; calls `committee/engine.py`.
- **React**: `/structural-committee/:hearingId` dossier UI; entry from Structural Timeline.

## Stances

`DENY` < `DEFER` < `WAIT_RECLAIM` < `APPROVE_REDUCED` < `APPROVE` — explicit caps in engine (e.g. invalidation breach → `DENY`).

## Context / LLM (later)

Stage 2–3: disclosure and LLM phrasing **flag-gated**; context never raises stance alone.
