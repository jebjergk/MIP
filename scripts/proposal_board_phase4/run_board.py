#!/usr/bin/env python
"""
Phase 4 Cortex Agentic Proposal Board — daily entry point.

This script replaces the disabled SP_RUN_PROPOSAL_BOARD stored procedure.
It runs the full multi-agent orchestration:
  Stage 0: snapshot dossiers + stage evidence pack cache
  Stage 1: 5 specialist Cortex Agents in parallel (per dossier)
  Stage 2: Python conflict detection
  Stage 3: challenge turn (objectless AGENT_RUN, persisted to INTERACTION_V2)
  Stage 4: revision turn (objectless AGENT_RUN, persisted to INTERACTION_V2 +
           AGENT_OUTCOME_V2 update)
  Stage 5: chair Cortex Agent (only after all 5 specialists durable)
  Stage 6: publication policy + STRUCTURAL_TRADE_PROPOSALS insert

Usage examples
--------------
# Cost-controlled daily run (recommended):
python -m MIP.scripts.proposal_board_phase4.run_board \
    --max-candidates 5 --max-rounds 1 --inter-concurrency 2

# Dry-run on focus symbols (no STRUCTURAL_TRADE_PROPOSALS insert):
python -m MIP.scripts.proposal_board_phase4.run_board \
    --symbols CRWD,AAPL,AMZN,PANW --dry-run

# Full daily run (uncapped — requires explicit override):
python -m MIP.scripts.proposal_board_phase4.run_board --allow-budget-override

# As-of override + portfolio:
python -m MIP.scripts.proposal_board_phase4.run_board \
    --as-of 2026-04-30 --portfolio 1 --max-proposals 8 --max-candidates 8
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

# Make `MIP.scripts.proposal_board_phase4.orchestrator` importable when invoked
# as a script (python MIP/scripts/proposal_board_phase4/run_board.py).
_THIS_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _THIS_DIR.parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from MIP.scripts.proposal_board_phase4.orchestrator import orchestrate_phase4_board  # noqa: E402


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_symbols(s: str) -> list[str]:
    return [t.strip().upper() for t in s.split(",") if t.strip()]


_VALID_MARKET_TYPES = {"STOCK", "ETF", "FX"}


def _parse_market_types(s: str) -> list[str]:
    raw = [t.strip().upper() for t in s.split(",") if t.strip()]
    bad = [t for t in raw if t not in _VALID_MARKET_TYPES]
    if bad:
        raise argparse.ArgumentTypeError(
            f"Unknown market_type(s): {bad}. Allowed: {sorted(_VALID_MARKET_TYPES)}"
        )
    seen: list[str] = []
    for t in raw:
        if t not in seen:
            seen.append(t)
    return seen


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the Phase 4 Cortex Agentic Proposal Board.",
    )
    parser.add_argument("--portfolio", type=int, default=None,
                        help="Optional portfolio_id filter.")
    parser.add_argument("--as-of", type=_parse_date, default=None,
                        help="As-of date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--symbols", type=_parse_symbols, default=None,
                        help="Optional comma-separated symbol filter.")
    parser.add_argument("--market-types", type=_parse_market_types,
                        default=["STOCK"],
                        help=(
                            "Comma-separated market types to evaluate. Defaults to "
                            "STOCK (MIP does not currently trade ETF or FX). "
                            "Pass STOCK,ETF,FX to include everything. "
                            f"Allowed values: {sorted(_VALID_MARKET_TYPES)}."
                        ))
    parser.add_argument("--max-proposals", type=int, default=8,
                        help="Max proposals to publish to STRUCTURAL_TRADE_PROPOSALS.")
    parser.add_argument("--max-rounds", type=int, default=2,
                        help="Max challenge+revision rounds (default 2, hard max 3).")
    parser.add_argument("--inter-concurrency", type=int, default=2,
                        help="Max dossiers running specialists in parallel.")
    parser.add_argument("--per-concurrency", type=int, default=5,
                        help="Max specialists per dossier in parallel.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run all stages but skip STRUCTURAL_TRADE_PROPOSALS insert.")
    parser.add_argument("--max-candidates", type=int, default=None,
                        help=(
                            "Cap on symbols sent to Cortex agents after eligibility filter. "
                            "None = uncapped legacy mode (also requires --allow-budget-override). "
                            "Recommended daily value: 5-8."
                        ))
    parser.add_argument("--daily-call-budget", type=int, default=80,
                        help=(
                            "Max estimated agent sessions for this run. "
                            "Run aborts before Cortex fan-out if projected sessions exceed this. "
                            "Denominated in agent sessions (~7/candidate). "
                            "Use --allow-budget-override to bypass."
                        ))
    parser.add_argument("--allow-budget-override", action="store_true",
                        help=(
                            "Bypass --daily-call-budget check. Must be explicit. "
                            "Not implied by --symbols or any other flag. "
                            "Also required for uncapped mode (no --max-candidates)."
                        ))
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    max_rounds = max(1, min(3, int(args.max_rounds)))
    if max_rounds != args.max_rounds:
        logging.getLogger(__name__).warning(
            "phase4 run_board: clamped --max-rounds %s to %s (hard cap 3)",
            args.max_rounds, max_rounds,
        )

    _log = logging.getLogger(__name__)

    # Warn loudly if running uncapped without an explicit override.
    if args.max_candidates is None and not args.allow_budget_override:
        _log.warning(
            "phase4 run_board: WARNING — no --max-candidates set and no "
            "--allow-budget-override. This is uncapped legacy mode and may incur "
            "very high Cortex Agent cost. Recommended: --max-candidates 5 or 8."
        )

    result = asyncio.run(
        orchestrate_phase4_board(
            portfolio_id=args.portfolio,
            as_of_date=args.as_of,
            symbols_filter=args.symbols,
            market_types_filter=args.market_types,
            max_proposals=args.max_proposals,
            max_rounds=max_rounds,
            inter_dossier_concurrency=args.inter_concurrency,
            per_dossier_concurrency=args.per_concurrency,
            dry_run=args.dry_run,
            max_candidates=args.max_candidates,
            daily_call_budget=args.daily_call_budget,
            allow_budget_override=args.allow_budget_override,
        )
    )

    print(json.dumps({
        "run_id": result.run_id,
        "status": result.status,
        "as_of_date": result.as_of_date.isoformat(),
        "dossier_count": result.dossier_count,
        "valid_dossier_count": result.valid_dossier_count,
        "invalid_dossier_count": result.invalid_dossier_count,
        "published_count": result.published_count,
        "research_published_count": result.research_published_count,
        "skipped_count": result.skipped_count,
        "eligible_count": result.eligible_count,
        "genuine_eligible_count": result.genuine_eligible_count,
        "cost_capped_count": result.cost_capped_count,
        "eligibility_skipped_count": result.eligibility_skipped_count,
        "eligibility_skip_breakdown": result.eligibility_skip_breakdown,
        "candidate_mode": result.candidate_mode,
        "estimated_agent_sessions": result.estimated_agent_sessions,
        "chair_propose_count": result.chair_propose_count,
        "props_executable_count": result.props_executable_count,
        "imported_to_lpa_count": result.imported_to_lpa_count,
        "pre_board_stock_only_gate": result.pre_board_stock_only_gate,
        "pre_board_market_type_integrity_gate": result.pre_board_market_type_integrity_gate,
        "pre_board_gate_checks": result.pre_board_gate_checks,
        "ibkr_account_mode": result.ibkr_account_mode,
        "short_publication_allowed": result.short_publication_allowed,
        "market_types_filter": args.market_types,
        "error": result.error,
        "dry_run": args.dry_run,
    }, indent=2, default=str))

    # -------------------------------------------------------------------------
    # PHASE 4 COST / ATTRITION SUMMARY — printed at end of every run.
    # Painfully visible for operator review.
    # -------------------------------------------------------------------------
    _dominant_reason = "N/A"
    skip_bd = result.eligibility_skip_breakdown or {}
    genuine_blocks = {k: v for k, v in skip_bd.items()
                      if k != "NOT_SENT_TO_AGENT_PANEL_COST_CAP"}
    if genuine_blocks:
        _dominant_reason = max(genuine_blocks, key=genuine_blocks.get)  # type: ignore[arg-type]
    elif result.chair_propose_count == 0 and result.eligible_count > 0:
        _dominant_reason = "CHAIR_NO_PROPOSE (all agents ran but chair declined)"

    _dry_tag = "  [DRY-RUN — no STRUCTURAL_TRADE_PROPOSALS insert]" if args.dry_run else ""

    _summary_lines = [
        "",
        "=" * 72,
        "  PHASE 4 COST / ATTRITION SUMMARY" + _dry_tag,
        "=" * 72,
        f"  Run ID              : {result.run_id}",
        f"  As-of date          : {result.as_of_date}",
        f"  Final status        : {result.status}",
        f"  Mode                : {result.candidate_mode}",
        "-" * 72,
        "  PRE-BOARD SAFETY GATES (fail-closed, before Cortex fan-out)",
        f"  PRE_BOARD_STOCK_ONLY_GATE          : {result.pre_board_stock_only_gate}",
        f"  PRE_BOARD_MARKET_TYPE_INTEGRITY_GATE: {result.pre_board_market_type_integrity_gate}",
    ]
    for _ck, _cv in sorted((result.pre_board_gate_checks or {}).items()):
        _flag = "  <== FAIL" if (_cv and not _ck.startswith("C1b")) else ""
        _summary_lines.append(f"      {_ck:<38}: {_cv}{_flag}")
    _summary_lines += [
        "-" * 72,
        f"  Candidates snapshotted  : {result.dossier_count}",
        f"  Eligible genuine        : {result.genuine_eligible_count}"
        + (f"  (eligibility blocks: {sum(genuine_blocks.values())})" if genuine_blocks else ""),
        f"  Skipped by cost cap     : {result.cost_capped_count}"
        + ("  [NOT a quality rejection]" if result.cost_capped_count else ""),
        f"  Selected for agents     : {result.eligible_count}",
        "-" * 72,
        f"  Agent sessions est.     : {result.estimated_agent_sessions}"
        + f"  (~{result.eligible_count} × 7 sessions/candidate)",
        f"  Agent sessions actual   : see ACCOUNT_USAGE.QUERY_HISTORY (cortex-agent-* tags)",
        "-" * 72,
        f"  Chair propose           : {result.chair_propose_count}",
        f"  Final slate published   : {result.published_count}",
        f"  Research-only (WATCH)   : {result.research_published_count}"
        + ("  [operator review only — never executable]" if result.research_published_count else ""),
        f"  Struct proposals exec.  : {result.props_executable_count}",
        f"  Imported to LPA         : {result.imported_to_lpa_count}",
        "-" * 72,
        f"  Dominant skip reason    : {_dominant_reason}",
    ]
    if result.error:
        _summary_lines.append(f"  ERROR                   : {result.error[:120]}")
    _summary_lines.append("=" * 72)
    _summary_lines.append("")

    print("\n".join(_summary_lines), flush=True)

    return 0 if result.status in ("COMPLETE", "COMPLETE_NO_DOSSIERS") else 1


if __name__ == "__main__":
    sys.exit(main())
