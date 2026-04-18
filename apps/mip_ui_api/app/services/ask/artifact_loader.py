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
    "/": "cockpit",
    "/cockpit": "cockpit",
    "/home": "home",
    "/runs": "audit_runs",
    "/runs/:runId": "audit_runs",
    "/structural-training": "structural_training",
    "/training": "training_status",
    "/structural-timeline": "structural_market_timeline",
    "/market-timeline": "market_timeline",
    "/symbol-tracker": "symbol_tracker",
    "/living-chart": "symbol_tracker",
    "/live-intelligence": "live_intelligence",
    "/parallel-worlds": "parallel_worlds",
    "/learning-ledger": "learning_ledger",
    "/performance-dashboard": "performance_dashboard",
    "/decision-console": "decision_console",
    "/intraday/early-exit": "intraday_early_exit",
    "/live-portfolio-activity": "live_portfolio_activity",
    "/live-portfolio-config": "live_portfolio_config",
    "/news-intelligence": "news_intelligence",
    "/intraday/dashboard": "intraday_dashboard",
    "/intraday/pattern/:patternId": "intraday_pattern_detail",
    "/intraday/terrain": "intraday_terrain",
    "/intraday/health": "intraday_health",
    "/debug": "debug",
}

_KNOWLEDGE_ROOT: Path | None = None
_PAGE_BY_ID: dict[str, dict[str, Any]] | None = None
_SECTIONS_BY_ID: dict[str, dict[str, Any]] | None = None
_WIDGETS_BY_ID: dict[str, dict[str, Any]] | None = None
_METRICS_BY_ID: dict[str, dict[str, Any]] | None = None
_DOMAIN_BY_TOPIC: dict[str, dict[str, Any]] | None = None
_WORKFLOWS_BY_ID: dict[str, dict[str, Any]] | None = None
_STATE_LOGIC_BY_ID: dict[str, dict[str, Any]] | None = None
_BACKEND_BY_ID: dict[str, dict[str, Any]] | None = None
_KNOWN_ISSUES_BY_ID: dict[str, dict[str, Any]] | None = None
_HANDBOOK_BY_ID: dict[str, dict[str, Any]] | None = None


def knowledge_root() -> Path:
    global _KNOWLEDGE_ROOT
    if _KNOWLEDGE_ROOT is None:
        root = find_mip_workspace_root()
        _KNOWLEDGE_ROOT = root / "MIP" / "knowledge" / "ask_mip"
    return _KNOWLEDGE_ROOT


def reset_caches() -> None:
    """Test hook."""
    global _PAGE_BY_ID, _SECTIONS_BY_ID, _WIDGETS_BY_ID, _METRICS_BY_ID, _DOMAIN_BY_TOPIC
    global _WORKFLOWS_BY_ID, _STATE_LOGIC_BY_ID, _BACKEND_BY_ID, _KNOWN_ISSUES_BY_ID, _HANDBOOK_BY_ID
    _PAGE_BY_ID = None
    _SECTIONS_BY_ID = None
    _WIDGETS_BY_ID = None
    _METRICS_BY_ID = None
    _DOMAIN_BY_TOPIC = None
    _WORKFLOWS_BY_ID = None
    _STATE_LOGIC_BY_ID = None
    _BACKEND_BY_ID = None
    _KNOWN_ISSUES_BY_ID = None
    _HANDBOOK_BY_ID = None


def _artifact_active(row: dict[str, Any]) -> bool:
    return str(row.get("status", "active")).lower() != "deprecated"


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


def _index_keyed(rows: list[dict[str, Any]], key_field: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        k = row.get(key_field)
        if k:
            out[str(k)] = row
    return out


def _ensure_indexes() -> None:
    global _PAGE_BY_ID, _SECTIONS_BY_ID, _WIDGETS_BY_ID, _METRICS_BY_ID, _DOMAIN_BY_TOPIC
    global _WORKFLOWS_BY_ID, _STATE_LOGIC_BY_ID, _BACKEND_BY_ID, _KNOWN_ISSUES_BY_ID, _HANDBOOK_BY_ID
    if _PAGE_BY_ID is not None:
        return
    _PAGE_BY_ID = {}
    for row in _load_yaml_dir("page_contracts"):
        pid = row.get("page_id")
        if pid:
            _PAGE_BY_ID[str(pid)] = row
    _SECTIONS_BY_ID = _index_keyed(_load_yaml_dir("section_contracts"), "section_id")
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
    _WORKFLOWS_BY_ID = _index_keyed(_load_yaml_dir("workflow_registry"), "workflow_id")
    _STATE_LOGIC_BY_ID = _index_keyed(_load_yaml_dir("state_logic_registry"), "logic_id")
    _BACKEND_BY_ID = _index_keyed(_load_yaml_dir("backend_object_registry"), "object_id")
    _KNOWN_ISSUES_BY_ID = _index_keyed(_load_yaml_dir("known_issues"), "issue_id")
    _HANDBOOK_BY_ID = _index_keyed(_load_yaml_dir("handbook_modules"), "module_id")


def page_id_for_route(route: str | None) -> str | None:
    if not route:
        return None
    direct = _ROUTE_TO_PAGE_ID.get(route)
    if direct:
        return direct
    # Prefix fallbacks (e.g. /runs/123 -> audit_runs)
    best: tuple[int, str] | None = None
    for pattern, pid in _ROUTE_TO_PAGE_ID.items():
        if pattern.endswith(":runId") or pattern.endswith(":patternId"):
            continue
        if route.startswith(pattern + "/"):
            score = len(pattern)
            if best is None or score > best[0]:
                best = (score, pid)
    if route.startswith("/runs/"):
        return "audit_runs"
    if route.startswith("/intraday/pattern/"):
        return "intraday_pattern_detail"
    return best[1] if best else None


def get_page_contract(page_id: str | None) -> dict[str, Any] | None:
    if not page_id:
        return None
    _ensure_indexes()
    row = _PAGE_BY_ID.get(page_id) if _PAGE_BY_ID else None
    if not row or not _artifact_active(row):
        return None
    return row


def sections_for_page(page_id: str | None) -> list[dict[str, Any]]:
    if not page_id:
        return []
    _ensure_indexes()
    out: list[dict[str, Any]] = []
    for row in (_SECTIONS_BY_ID or {}).values():
        if not _artifact_active(row):
            continue
        if str(row.get("page_id") or "") == page_id:
            out.append(row)
    return out


def get_widget_contract(widget_id: str) -> dict[str, Any] | None:
    _ensure_indexes()
    row = _WIDGETS_BY_ID.get(widget_id) if _WIDGETS_BY_ID else None
    if row and not _artifact_active(row):
        return None
    return row


def get_metric_contract(metric_id: str) -> dict[str, Any] | None:
    _ensure_indexes()
    row = _METRICS_BY_ID.get(metric_id) if _METRICS_BY_ID else None
    if row and not _artifact_active(row):
        return None
    return row


def metrics_for_page(page_id: str | None) -> list[dict[str, Any]]:
    if not page_id:
        return []
    _ensure_indexes()
    return [
        m
        for m in (_METRICS_BY_ID or {}).values()
        if _artifact_active(m) and str(m.get("page_id") or "") == page_id
    ]


def domain_topic(topic_id: str) -> dict[str, Any] | None:
    _ensure_indexes()
    return _DOMAIN_BY_TOPIC.get(topic_id) if _DOMAIN_BY_TOPIC else None


def list_domain_topics() -> list[str]:
    _ensure_indexes()
    return list((_DOMAIN_BY_TOPIC or {}).keys())


def backend_objects_for_page(page_id: str | None, limit: int = 6) -> list[dict[str, Any]]:
    if not page_id:
        return []
    _ensure_indexes()
    out: list[dict[str, Any]] = []
    for row in (_BACKEND_BY_ID or {}).values():
        if not _artifact_active(row):
            continue
        pages = [str(p) for p in (row.get("consumed_by_pages") or [])]
        if page_id in pages:
            out.append(row)
    return out[:limit]


def match_workflows_for_question(question: str, page_id: str | None, limit: int = 3) -> list[dict[str, Any]]:
    q = question.lower()
    out: list[dict[str, Any]] = []
    _ensure_indexes()
    for row in (_WORKFLOWS_BY_ID or {}).values():
        if not _artifact_active(row):
            continue
        kws = [str(x).lower() for x in (row.get("retrieval_keywords") or [])]
        related = [str(x) for x in (row.get("related_pages") or [])]
        hit = any(k in q for k in kws if k)
        if page_id and page_id in related:
            hit = True
        if hit:
            out.append(row)
    return out[:limit]


def match_state_logic_for_context(
    question: str,
    badge_labels: list[str] | None,
    limit: int = 4,
) -> list[dict[str, Any]]:
    q = question.lower()
    badges = [str(b).lower() for b in (badge_labels or [])]
    out: list[dict[str, Any]] = []
    _ensure_indexes()
    for row in (_STATE_LOGIC_BY_ID or {}).values():
        if not _artifact_active(row):
            continue
        kws = [str(x).lower() for x in (row.get("retrieval_keywords") or [])]
        name = str(row.get("name") or "").lower()
        logic_id = str(row.get("logic_id") or "").lower()
        hit = any(k in q for k in kws if k)
        if not hit and badges:
            hit = any(b in name or b in logic_id or b in q for b in badges)
        if hit:
            out.append(row)
    return out[:limit]


def match_known_issues_for_question(question: str, limit: int = 2) -> list[dict[str, Any]]:
    q = question.lower()
    cues = ("bug", "issue", "wrong", "broken", "disagree", "contradict", "surprising", "unexpected", "mismatch")
    if not any(c in q for c in cues):
        return []
    _ensure_indexes()
    out: list[dict[str, Any]] = []
    for row in (_KNOWN_ISSUES_BY_ID or {}).values():
        if not _artifact_active(row):
            continue
        kws = [str(x).lower() for x in (row.get("retrieval_keywords") or [])]
        if any(k in q for k in kws if k):
            out.append(row)
    if not out:
        for row in (_KNOWN_ISSUES_BY_ID or {}).values():
            if _artifact_active(row):
                out.append(row)
    return out[:limit]


def match_handbook_modules(question: str, page_id: str | None, limit: int = 3) -> list[dict[str, Any]]:
    q = question.lower()
    training_cues = (
        "maturity",
        "trust",
        "confidence",
        "qualif",
        "sample size",
        "outcome",
        "evaluation",
        "pattern",
        "training",
        "gate",
        "production",
        "research",
    )
    if not any(c in q for c in training_cues):
        if page_id not in ("training_status", "structural_training", "decision_console"):
            return []
    _ensure_indexes()
    out: list[dict[str, Any]] = []
    for row in (_HANDBOOK_BY_ID or {}).values():
        if not _artifact_active(row):
            continue
        kws = [str(x).lower() for x in (row.get("retrieval_keywords") or [])]
        mid = str(row.get("module_id") or "").lower()
        title = str(row.get("title") or "").lower()
        hit = any(k in q for k in kws if k) or (mid and mid in q) or (title and title in q)
        pages = [str(x) for x in (row.get("where_seen_in_product") or [])]
        if page_id and page_id in pages:
            hit = True
        if hit:
            out.append(row)
    return out[:limit]


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


def format_section_contract_for_prompt(contract: dict[str, Any]) -> str:
    return _yaml_to_context_lines(contract, "Section contract (app truth)")


def format_widget_contract_for_prompt(contract: dict[str, Any]) -> str:
    return _yaml_to_context_lines(contract, "Widget contract (app truth)")


def format_metric_for_prompt(contract: dict[str, Any]) -> str:
    return _yaml_to_context_lines(contract, "Metric dictionary entry (app truth)")


def format_domain_topic_for_prompt(entry: dict[str, Any]) -> str:
    return _yaml_to_context_lines(entry, "Domain knowledge (general markets)")


def format_workflow_for_prompt(entry: dict[str, Any]) -> str:
    return _yaml_to_context_lines(entry, "Workflow registry (app truth)")


def format_state_logic_for_prompt(entry: dict[str, Any]) -> str:
    return _yaml_to_context_lines(entry, "State / status logic (app truth)")


def format_backend_object_for_prompt(entry: dict[str, Any]) -> str:
    return _yaml_to_context_lines(entry, "Backend object registry (app truth)")


def format_known_issue_for_prompt(entry: dict[str, Any]) -> str:
    return _yaml_to_context_lines(entry, "Known issue (app truth)")


def format_handbook_module_for_prompt(entry: dict[str, Any]) -> str:
    return _yaml_to_context_lines(entry, "Training handbook module (conceptual)")


def match_domain_topics_by_question(question: str) -> list[dict[str, Any]]:
    q = question.lower()
    hits: list[dict[str, Any]] = []
    _ensure_indexes()
    for row in (_DOMAIN_BY_TOPIC or {}).values():
        if not _artifact_active(row):
            continue
        tid = str(row.get("topic_id") or "").lower()
        title = str(row.get("title") or "").lower()
        aliases = [str(a).lower() for a in (row.get("retrieval_aliases") or [])]
        if tid and tid in q:
            hits.append(row)
        elif title and title in q:
            hits.append(row)
        elif any(a and a in q for a in aliases):
            hits.append(row)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for h in hits:
        aid = str(h.get("artifact_id") or "")
        if aid in seen:
            continue
        seen.add(aid)
        out.append(h)
    return out


def infer_metric_ids_from_snapshot(snapshot: dict[str, Any] | None) -> list[str]:
    if not snapshot:
        return []
    keys = {str(k).lower().replace(" ", "_") for k in snapshot.keys()}
    out: list[str] = []
    _ensure_indexes()
    for mid, row in (_METRICS_BY_ID or {}).items():
        if not _artifact_active(row):
            continue
        name = str(row.get("metric_name") or "").lower().replace(" ", "_")
        disp = str(row.get("display_label") or "").lower().replace(" ", "_")
        if mid in keys or name in keys or disp in keys:
            out.append(mid)
    return out
