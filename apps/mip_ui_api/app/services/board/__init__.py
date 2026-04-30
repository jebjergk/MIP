"""
Board explanation service module.

Read-only helpers for surfacing the agentic proposal board's full
audit trail (per-specialist verdicts, chair synthesis, run metadata)
behind a single `proposal_id` lookup. Sprint 3 of the Phase 2 plan.

The board persistence tables (PROPOSAL_BOARD_*) are the source of
truth; this module never writes to them.
"""
