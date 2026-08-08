"""Canonical Brooks INTRADAY Adviser baseline (V1.0)."""

from __future__ import annotations

from typing import Any

ADVISER_VERSION = "BROOKS_INTRADAY_ADVISER_V1_0"
CONFIRMATION_CONTRACT_VERSION = 2
CONFIRMATION_LOGIC_KIND = "BOOLEAN_TREE"
WAKE_POLICY = "THESIS_DRIVEN"
WATCH_POLICY = "EDGE_LOGICAL_KEY"
INVALIDATION_POLICY = "EDGE_TRIGGERED"
WAKE_CONTRACT_PATCH = 1
RAG_CORPUS_VERSION = "v1.1_static_2026-08-07"
EXECUTION_DIRECTION = "LONG_ONLY"
LAB_STARTING_CASH = 1000
CANONICAL_QUERY_TAG = "BROOKS_ADVISER_INTRADAY_V1_0"


def canonical_attempt_config(*, symbol: str, trading_date: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "trading_date": trading_date,
        "mode": "ADVISER_SESSION",
        "adviser_version": ADVISER_VERSION,
        "confirmation_contract_version": CONFIRMATION_CONTRACT_VERSION,
        "confirmation_logic": CONFIRMATION_LOGIC_KIND,
        "wake_policy": WAKE_POLICY,
        "watch_policy": WATCH_POLICY,
        "invalidation_policy": INVALIDATION_POLICY,
        "rag_corpus_version": RAG_CORPUS_VERSION,
        "execution_direction": EXECUTION_DIRECTION,
        "lab_starting_cash": LAB_STARTING_CASH,
    }


def canonical_cost_metadata() -> dict[str, Any]:
    return {
        "adviser_version": ADVISER_VERSION,
        "confirmation_contract_version": CONFIRMATION_CONTRACT_VERSION,
        "confirmation_logic": CONFIRMATION_LOGIC_KIND,
        "wake_policy": WAKE_POLICY,
        "watch_policy": WATCH_POLICY,
        "invalidation_policy": INVALIDATION_POLICY,
        "rag_corpus_version": RAG_CORPUS_VERSION,
        "execution_direction": EXECUTION_DIRECTION,
        "lab_starting_cash": LAB_STARTING_CASH,
    }


def canonical_run_pin(
    *,
    adviser_attempt_id: str,
    simulation_attempt_id: str,
) -> dict[str, Any]:
    return {
        "adviser_attempt_id": adviser_attempt_id,
        "context_attempt_id": adviser_attempt_id,
        "simulation_attempt_id": simulation_attempt_id,
        "context_ruleset": ADVISER_VERSION,
        "adviser_version": ADVISER_VERSION,
        "confirmation_contract_version": CONFIRMATION_CONTRACT_VERSION,
        "confirmation_logic": CONFIRMATION_LOGIC_KIND,
        "wake_policy": WAKE_POLICY,
        "watch_policy": WATCH_POLICY,
        "invalidation_policy": INVALIDATION_POLICY,
        "rag_corpus_version": RAG_CORPUS_VERSION,
        "execution_direction": EXECUTION_DIRECTION,
        "lab_starting_cash": LAB_STARTING_CASH,
        "query_tag": CANONICAL_QUERY_TAG,
    }


def is_canonical_adviser_foundation(adv: dict[str, Any] | None) -> bool:
    """True only for BROOKS_INTRADAY_ADVISER_V1_0 run pins / attempt metadata."""
    if not adv or not adv.get("adviser_attempt_id"):
        return False
    return str(adv.get("adviser_version") or "") == ADVISER_VERSION
