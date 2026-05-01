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
# Dry-run on focus symbols (no STRUCTURAL_TRADE_PROPOSALS insert):
python -m MIP.scripts.proposal_board_phase4.run_board \
    --symbols CRWD,AAPL,AMZN,PANW --dry-run

# Full daily run:
python -m MIP.scripts.proposal_board_phase4.run_board

# As-of override + portfolio:
python -m MIP.scripts.proposal_board_phase4.run_board \
    --as-of 2026-04-30 --portfolio 1 --max-proposals 8
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
        "skipped_count": result.skipped_count,
        "eligible_count": result.eligible_count,
        "eligibility_skipped_count": result.eligibility_skipped_count,
        "eligibility_skip_breakdown": result.eligibility_skip_breakdown,
        "ibkr_account_mode": result.ibkr_account_mode,
        "short_publication_allowed": result.short_publication_allowed,
        "market_types_filter": args.market_types,
        "error": result.error,
        "dry_run": args.dry_run,
    }, indent=2, default=str))

    return 0 if result.status in ("COMPLETE", "COMPLETE_NO_DOSSIERS") else 1


if __name__ == "__main__":
    sys.exit(main())
