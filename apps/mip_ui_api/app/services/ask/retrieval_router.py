"""
Source-aware retrieval: choose knowledge corpora from intent + runtime context.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.services.ask import artifact_loader
from app.services.ask.docs_retriever import retrieve_docs
from app.services.ask.intent import classify_intent
from app.services.ask.models import SourceAttribution
from app.services.ask.runtime_schema import AskRuntimePayload


@dataclass
class RetrievalBundle:
    """Assembled context strings and provenance for the composer."""

    doc_chunks: list[str] = field(default_factory=list)
    docs_confidence: float = 0.0
    doc_sources: list[SourceAttribution] = field(default_factory=list)
    artifact_blocks: list[str] = field(default_factory=list)
    artifact_sources: list[SourceAttribution] = field(default_factory=list)
    domain_blocks: list[str] = field(default_factory=list)
    domain_sources: list[SourceAttribution] = field(default_factory=list)
    source_groups_used: list[str] = field(default_factory=list)


def effective_page_id(route: str | None, runtime: AskRuntimePayload | None) -> str | None:
    if runtime and runtime.page_id:
        return str(runtime.page_id).strip() or None
    return artifact_loader.page_id_for_route(route or (runtime.page_route if runtime else None))


def plan_source_groups(intent: str) -> list[str]:
    """Ordered list of source group names for logging / policy."""
    if intent in ("trading_concept", "market_research_concept"):
        return ["domain_knowledge", "glossary", "guide", "page_artifacts"]
    if intent == "term_definition":
        return ["glossary", "guide", "page_artifacts", "domain_knowledge"]
    if intent == "source_lineage":
        return ["page_artifacts", "guide", "glossary", "domain_knowledge"]
    if intent == "state_diagnosis":
        return ["page_artifacts", "glossary", "guide", "domain_knowledge"]
    if intent in ("metric_explanation", "mip_feature_behavior"):
        return ["page_artifacts", "glossary", "guide", "domain_knowledge"]
    if intent == "follow_up_clarification":
        return ["page_artifacts", "glossary", "guide"]
    # mixed
    return ["page_artifacts", "glossary", "guide", "domain_knowledge"]


def retrieve_for_ask_v3(
    question: str,
    route: str | None,
    page_title: str | None,
    page_hint: str | None,
    runtime: AskRuntimePayload | None,
    history: list[dict[str, str]],
) -> tuple[str, RetrievalBundle]:
    """
    Returns (intent, bundle).
    """
    intent = classify_intent(question, history)
    groups = plan_source_groups(intent)
    bundle = RetrievalBundle(source_groups_used=groups)
    effective_route = route or (runtime.page_route if runtime else None)
    page_id = effective_page_id(effective_route, runtime)

    # Guide chunks (always useful for MIP-specific unless pure domain — still include with lower priority)
    doc_chunks, docs_conf, doc_sources = retrieve_docs(question, effective_route, page_title, page_hint)
    if "guide" in groups:
        bundle.doc_chunks = doc_chunks
        bundle.docs_confidence = docs_conf
        bundle.doc_sources = doc_sources

    # Page / widget / metric artifacts
    if "page_artifacts" in groups and page_id:
        pc = artifact_loader.get_page_contract(page_id)
        if pc:
            bundle.artifact_blocks.append(artifact_loader.format_page_contract_for_prompt(pc))
            bundle.artifact_sources.append(
                SourceAttribution("PAGE_CONTRACT", str(pc.get("artifact_id", page_id)), "Page contract", 1.0)
            )
        widget_ids: list[str] = []
        if runtime:
            widget_ids.extend(runtime.visible_widget_ids or [])
            if runtime.selected_widget_id:
                widget_ids.append(runtime.selected_widget_id)
        for wid in dict.fromkeys(widget_ids):
            wc = artifact_loader.get_widget_contract(wid)
            if wc:
                bundle.artifact_blocks.append(artifact_loader.format_widget_contract_for_prompt(wc))
                bundle.artifact_sources.append(
                    SourceAttribution(
                        "WIDGET_CONTRACT",
                        str(wc.get("artifact_id", wid)),
                        f"Widget {wid}",
                        1.0,
                    )
                )
        metric_ids: set[str] = set()
        if runtime and runtime.current_kpi_snapshot:
            for mid in artifact_loader.infer_metric_ids_from_snapshot(runtime.current_kpi_snapshot):
                metric_ids.add(mid)
        for m in artifact_loader.metrics_for_page(page_id):
            mid = str(m.get("metric_id") or "")
            if mid:
                metric_ids.add(mid)
        for mid in metric_ids:
            mc = artifact_loader.get_metric_contract(mid)
            if mc:
                bundle.artifact_blocks.append(artifact_loader.format_metric_for_prompt(mc))
                bundle.artifact_sources.append(
                    SourceAttribution("METRIC_DICTIONARY", str(mc.get("artifact_id", mid)), f"Metric {mid}", 1.0)
                )

    # Domain pack (trading education)
    if "domain_knowledge" in groups:
        for entry in artifact_loader.match_domain_topics_by_question(question):
            bundle.domain_blocks.append(artifact_loader.format_domain_topic_for_prompt(entry))
            bundle.domain_sources.append(
                SourceAttribution(
                    "DOMAIN_KNOWLEDGE",
                    str(entry.get("artifact_id", entry.get("topic_id", "domain"))),
                    str(entry.get("title", "Domain")),
                    0.95,
                )
            )

    return intent, bundle
