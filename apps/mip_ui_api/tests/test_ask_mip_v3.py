"""Ask MIP 2.0 — schema, artifacts, retrieval routing, eval suite scenarios."""
from __future__ import annotations

import pathlib

import pytest
import yaml

from app.services.ask import artifact_loader
from app.services.ask.intent import classify_intent
from app.services.ask.retrieval_router import retrieve_for_ask_v3
from app.services.ask.runtime_schema import AskRuntimePayload, AskV3RequestBody


def test_ask_v3_request_body_accepts_runtime():
    body = AskV3RequestBody(
        question="What is slippage?",
        route="/cockpit",
        runtime=AskRuntimePayload(page_id="cockpit", symbol="AAPL", portfolio_id=1),
    )
    assert body.runtime.symbol == "AAPL"
    assert body.runtime.portfolio_id == 1


def test_effective_page_id_from_runtime_overrides_route():
    from app.services.ask.retrieval_router import effective_page_id

    rt = AskRuntimePayload(page_id="symbol_tracker", page_route="/symbol-tracker")
    assert effective_page_id("/training", rt) == "symbol_tracker"


def test_page_contract_loads_symbol_tracker():
    artifact_loader.reset_caches()
    pc = artifact_loader.get_page_contract("symbol_tracker")
    assert pc is not None
    assert pc.get("page_id") == "symbol_tracker"
    assert "Living Chart" in (pc.get("page_title") or "")


def test_retrieval_includes_page_contract_and_domain():
    artifact_loader.reset_caches()
    rt = AskRuntimePayload(page_id="symbol_tracker", visible_widget_ids=["living_chart_main"])
    intent, bundle = retrieve_for_ask_v3(
        "What is slippage?",
        "/symbol-tracker",
        "Living Chart",
        None,
        rt,
        [],
    )
    assert intent == "trading_concept"
    types = {s.source_type for s in bundle.artifact_sources} | {s.source_type for s in bundle.domain_sources}
    assert "PAGE_CONTRACT" in types
    assert "DOMAIN_KNOWLEDGE" in types


def test_state_diagnosis_intent():
    assert classify_intent("Why is this symbol marked PREPARE?", []) == "state_diagnosis"


def test_source_lineage_intent():
    assert classify_intent("Where does this figure come from?", []) == "source_lineage"


def _eval_suite_path() -> pathlib.Path:
    root = pathlib.Path(__file__).resolve().parents[4]
    return root / "MIP" / "knowledge" / "ask_mip" / "eval_suite" / "scenarios.yaml"


def test_eval_suite_yaml_scenarios():
    path = _eval_suite_path()
    assert path.is_file(), f"missing {path}"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    scenarios = raw.get("scenarios") or []
    assert len(scenarios) >= 5
    for sc in scenarios:
        q = sc.get("question")
        route = sc.get("route")
        rt_dict = sc.get("runtime") or {}
        rt = AskRuntimePayload.model_validate(rt_dict) if rt_dict else None
        intent, bundle = retrieve_for_ask_v3(q, route, None, None, rt, [])
        exp_intent = sc.get("expect_intent")
        if exp_intent:
            assert intent == exp_intent, f"{sc.get('id')}: intent {intent} != {exp_intent}"
        one_of = sc.get("expect_intent_one_of")
        if one_of:
            assert intent in one_of, f"{sc.get('id')}: intent {intent} not in {one_of}"
        need = sc.get("expect_source_types_contain") or []
        found = {s.source_type for s in bundle.artifact_sources + bundle.domain_sources + bundle.doc_sources}
        for t in need:
            assert t in found, f"{sc.get('id')}: missing source {t} in {found}"
