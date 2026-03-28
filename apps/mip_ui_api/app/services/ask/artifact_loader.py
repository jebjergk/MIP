"""
Load Ask MIP 2.0 knowledge artifacts (YAML) from MIP/knowledge/ask_mip/.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from app.cursorfiles_paths import find_mip_workspace_root

logger = logging.getLogger(__name__)

_ROUTE_TO_PAGE_ID: dict[str, str] = {
    "/symbol-tracker": "symbol_tracker",
    "/living-chart": "symbol_tracker",
    "/training": "training_status",
    "/cockpit": "cockpit",
    "/home": "home",
    "/performance-dashboard": "performance_dashboard",
    "/decision-console": "decision_console",
    "/live-portfolio-activity": "live_portfolio_activity",
    "/live-portfolio-config": "live_portfolio_config",
}

_KNOWLEDGE_ROOT: Path | None = None
_PAGE_BY_ID: dict[str, dict[str, Any]] | None = None
_WIDGETS_BY_ID: dict[str, dict[str, Any]] | None = None
_METRICS_BY_ID: dict[str, dict[str, Any]] | None = None
_DOMAIN_BY_TOPIC: dict[str, dict[str, Any]] | None = None


def knowledge_root() -> Path:
    global _KNOWLEDGE_ROOT
    if _KNOWLEDGE_ROOT is None:
        root = find_mip_workspace_root()
        _KNOWLEDGE_ROOT = root / "MIP" / "knowledge" / "ask_mip"
    return _KNOWLEDGE_ROOT


def reset_caches() -> None:
    """Test hook."""
    global _PAGE_BY_ID, _WIDGETS_BY_ID, _METRICS_BY_ID, _DOMAIN_BY_TOPIC
    _PAGE_BY_ID = None
    _WIDGETS_BY_ID = None
    _METRICS_BY_ID = None
    _DOMAIN_BY_TOPIC = None


def _load_yaml_dir(subdir: str) -> list[dict[str, Any]]:
    base = knowledge_root() / subdir
    if not base.is_dir():
        logger.warning("Ask MIP knowledge dir missing: %s", base)
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(base.glob("*.yaml")):
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                data["_source_file"] = str(path.name)
                out.append(data)
        except Exception:
            logger.exception("Failed to load knowledge file %s", path)
    return out


def _load_domain_trading() -> list[dict[str, Any]]:
    base = knowledge_root() / "domain_knowledge" / "trading"
    if not base.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(base.glob("*.yaml")):
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                data["_source_file"] = str(path.name)
                out.append(data)
        except Exception:
            logger.exception("Failed to load domain file %s", path)
    return out


def _ensure_indexes() -> None:
    global _PAGE_BY_ID, _WIDGETS_BY_ID, _METRICS_BY_ID, _DOMAIN_BY_TOPIC
    if _PAGE_BY_ID is not None:
        return
    _PAGE_BY_ID = {}
    for row in _load_yaml_dir("page_contracts"):
        pid = row.get("page_id")
        if pid:
            _PAGE_BY_ID[str(pid)] = row
    _WIDGETS_BY_ID = {}
    for row in _load_yaml_dir("widget_contracts"):
        wid = row.get("widget_id")
        if wid:
            _WIDGETS_BY_ID[str(wid)] = row
    _METRICS_BY_ID = {}
    for row in _load_yaml_dir("metric_dictionary"):
        mid = row.get("metric_id")
        if mid:
            _METRICS_BY_ID[str(mid)] = row
    _DOMAIN_BY_TOPIC = {}
    for row in _load_domain_trading():
        tid = row.get("topic_id")
        if tid:
            _DOMAIN_BY_TOPIC[str(tid)] = row


def page_id_for_route(route: str | None) -> str | None:
    if not route:
        return None
    return _ROUTE_TO_PAGE_ID.get(route)


def get_page_contract(page_id: str | None) -> dict[str, Any] | None:
    if not page_id:
        return None
    _ensure_indexes()
    return _PAGE_BY_ID.get(page_id) if _PAGE_BY_ID else None


def get_widget_contract(widget_id: str) -> dict[str, Any] | None:
    _ensure_indexes()
    return _WIDGETS_BY_ID.get(widget_id) if _WIDGETS_BY_ID else None


def get_metric_contract(metric_id: str) -> dict[str, Any] | None:
    _ensure_indexes()
    return _METRICS_BY_ID.get(metric_id) if _METRICS_BY_ID else None


def metrics_for_page(page_id: str | None) -> list[dict[str, Any]]:
    if not page_id:
        return []
    _ensure_indexes()
    return [m for m in (_METRICS_BY_ID or {}).values() if str(m.get("page_id") or "") == page_id]


def domain_topic(topic_id: str) -> dict[str, Any] | None:
    _ensure_indexes()
    return _DOMAIN_BY_TOPIC.get(topic_id) if _DOMAIN_BY_TOPIC else None


def list_domain_topics() -> list[str]:
    _ensure_indexes()
    return list((_DOMAIN_BY_TOPIC or {}).keys())


def _yaml_to_context_lines(data: dict[str, Any], title: str) -> str:
    """Flatten YAML to a readable block for LLM context (no fabricated keys)."""
    lines = [f"## {title}", f"(artifact_id: {data.get('artifact_id', 'unknown')})"]
    skip = {"_source_file", "artifact_id"}
    for k, v in data.items():
        if k in skip:
            continue
        if v is None or v == [] or v == {}:
            continue
        if isinstance(v, (list, dict)):
            lines.append(f"{k}:")
            blob = yaml.safe_dump(v, default_flow_style=False, allow_unicode=True).strip()
            for part in blob.splitlines():
                lines.append(f"  {part}")
        else:
            lines.append(f"{k}: {v}")
    return "\n".join(lines)


def format_page_contract_for_prompt(contract: dict[str, Any]) -> str:
    return _yaml_to_context_lines(contract, "Page contract (app truth)")


def format_widget_contract_for_prompt(contract: dict[str, Any]) -> str:
    return _yaml_to_context_lines(contract, "Widget contract (app truth)")


def format_metric_for_prompt(contract: dict[str, Any]) -> str:
    return _yaml_to_context_lines(contract, "Metric dictionary entry (app truth)")


def format_domain_topic_for_prompt(entry: dict[str, Any]) -> str:
    return _yaml_to_context_lines(entry, "Domain knowledge (general markets)")


def match_domain_topics_by_question(question: str) -> list[dict[str, Any]]:
    """Very small keyword router; expand with aliases over time."""
    q = question.lower()
    hits: list[dict[str, Any]] = []
    if "slippage" in q:
        t = domain_topic("slippage")
        if t:
            hits.append(t)
    return hits


def infer_metric_ids_from_snapshot(snapshot: dict[str, Any] | None) -> list[str]:
    if not snapshot:
        return []
    keys = {str(k).lower().replace(" ", "_") for k in snapshot.keys()}
    out: list[str] = []
    _ensure_indexes()
    for mid, row in (_METRICS_BY_ID or {}).items():
        name = str(row.get("metric_name") or "").lower().replace(" ", "_")
        disp = str(row.get("display_label") or "").lower().replace(" ", "_")
        if mid in keys or name in keys or disp in keys:
            out.append(mid)
    return out
