"""Executable boolean confirmation trees (ALL_OF / ANY_OF / PREDICATE leaves)."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

from .adviser_invalidation_v01 import _ensure_predicate_id

LogicNodeKind = Literal["ALL_OF", "ANY_OF", "PREDICATE"]
PredicateStatus = Literal["pending", "satisfied", "expired", "failed"]

_GROUP_TYPES = frozenset({"ALL_OF", "ANY_OF"})

_OR_PROSE_RE = re.compile(
    r"\bOR\b|either\b|one of the following|one of three|one of two|one of\b|alternatively",
    re.IGNORECASE,
)
_AND_PROSE_RE = re.compile(
    r"\ball of\b|\bboth\b|\beach must\b|\band also\b|simultaneously|\ball three\b",
    re.IGNORECASE,
)

_PREDICATE_FIELD_KEYS = frozenset(
    {
        "predicate_type",
        "level",
        "direction",
        "description",
        "valid_from_bar",
        "valid_until_bar",
        "valid_from_bar_offset",
        "valid_until_bar_offset",
        "valid_window_bars",
        "bar_requirement",
        "bar_offset",
        "minimum_body_fraction",
        "min_body_fraction",
        "minimum_close_location",
        "min_close_location",
        "minimum_legs",
        "minimum_bars",
        "support_level",
        "require_bullish",
        "minimum",
        "after_signal",
        "signal_bar",
        "id",
    }
)


@dataclass
class ConfirmationLogicNode:
    node_type: LogicNodeKind
    node_id: str = ""
    children: list[ConfirmationLogicNode] = field(default_factory=list)
    predicate: dict[str, Any] | None = None
    valid_from_bar: int = 0
    valid_until_bar: int = 0
    status: PredicateStatus = "pending"
    satisfied_at_bar: int | None = None

    def to_dict(self) -> dict[str, Any]:
        if self.node_type == "PREDICATE":
            out = dict(self.predicate or {})
            out["type"] = "PREDICATE"
            out["predicate_type"] = (self.predicate or {}).get("type", "")
            out["node_id"] = self.node_id
            out["valid_from_bar"] = self.valid_from_bar
            out["valid_until_bar"] = self.valid_until_bar
            out["status"] = self.status
            out["satisfied_at_bar"] = self.satisfied_at_bar
            return out
        return {
            "type": self.node_type,
            "node_id": self.node_id,
            "children": [c.to_dict() for c in self.children],
        }

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> ConfirmationLogicNode:
        return parse_confirmation_logic_node(raw)


def _new_node_id() -> str:
    return uuid.uuid4().hex[:12]


def _predicate_dict_from_leaf_raw(raw: dict[str, Any]) -> dict[str, Any]:
    ptype = str(raw.get("predicate_type") or raw.get("type") or "").upper()
    if ptype in _GROUP_TYPES or ptype == "PREDICATE":
        ptype = str(raw.get("predicate_type") or "").upper()
    pred: dict[str, Any] = {}
    for k, v in raw.items():
        if k in ("type", "predicate_type", "children", "node_id", "status", "satisfied_at_bar"):
            continue
        if k in _PREDICATE_FIELD_KEYS or k.startswith("valid_") or k.startswith("min"):
            pred[k] = v
    if ptype:
        pred["type"] = ptype
    elif "type" in raw and str(raw["type"]).upper() not in _GROUP_TYPES:
        pred["type"] = str(raw["type"]).upper()
    return _ensure_predicate_id(pred)


def parse_confirmation_logic_node(raw: dict[str, Any]) -> ConfirmationLogicNode:
    if not isinstance(raw, dict):
        raise ValueError("confirmation_logic must be an object")
    ntype = str(raw.get("type", "")).upper()
    if ntype in _GROUP_TYPES:
        children_raw = raw.get("children") or []
        if not isinstance(children_raw, list) or not children_raw:
            raise ValueError(f"{ntype} requires non-empty children")
        return ConfirmationLogicNode(
            node_type=ntype,  # type: ignore[arg-type]
            node_id=str(raw.get("node_id") or _new_node_id()),
            children=[parse_confirmation_logic_node(c) for c in children_raw],
        )
    if ntype == "PREDICATE" or raw.get("predicate_type"):
        pred = _predicate_dict_from_leaf_raw(raw)
        if not pred.get("type"):
            raise ValueError("PREDICATE leaf requires predicate_type")
        return ConfirmationLogicNode(
            node_type="PREDICATE",
            node_id=str(raw.get("node_id") or _new_node_id()),
            predicate=pred,
            valid_from_bar=int(raw.get("valid_from_bar", 0)),
            valid_until_bar=int(raw.get("valid_until_bar", 0)),
            status=str(raw.get("status") or "pending"),  # type: ignore[arg-type]
            satisfied_at_bar=raw.get("satisfied_at_bar"),
        )
    raise ValueError(f"unsupported confirmation_logic node type: {ntype}")


def parse_confirmation_logic(raw: dict[str, Any]) -> ConfirmationLogicNode:
    root = parse_confirmation_logic_node(raw)
    if root.node_type not in _GROUP_TYPES:
        return ConfirmationLogicNode(
            node_type="ALL_OF",
            node_id=_new_node_id(),
            children=[root],
        )
    return root


def logic_all_predicate_leaves(node: ConfirmationLogicNode) -> list[ConfirmationLogicNode]:
    if node.node_type == "PREDICATE":
        return [node]
    out: list[ConfirmationLogicNode] = []
    for ch in node.children:
        out.extend(logic_all_predicate_leaves(ch))
    return out


def logic_fingerprint(root: ConfirmationLogicNode) -> str:
    def walk(n: ConfirmationLogicNode) -> Any:
        if n.node_type == "PREDICATE":
            p = dict(n.predicate or {})
            p.pop("id", None)
            return ("P", json.dumps(p, sort_keys=True))
        return (n.node_type, [walk(c) for c in n.children])

    blob = json.dumps(walk(root), sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def wrap_predicates_as_logic(
    preds: list[dict[str, Any]],
    *,
    combinator: Literal["ALL_OF", "ANY_OF"] = "ALL_OF",
) -> dict[str, Any]:
    children: list[dict[str, Any]] = []
    for p in preds:
        pt = str(p.get("type", "")).upper()
        leaf: dict[str, Any] = {"type": "PREDICATE", "predicate_type": pt}
        for k, v in p.items():
            if k == "type":
                continue
            leaf[k] = v
        children.append(leaf)
    return {"type": combinator, "children": children}


def validate_logic_prose_consistency(
    root: ConfirmationLogicNode,
    *,
    confirmation_conditions: list[str],
    reasoning_summary: str = "",
    current_thesis: str = "",
) -> tuple[ConfirmationLogicNode, list[dict[str, Any]]]:
    """Repair root combinator when prose clearly disagrees; emit audit events."""
    events: list[dict[str, Any]] = []
    text = " ".join(confirmation_conditions or []) + " " + (reasoning_summary or "") + " " + (current_thesis or "")
    has_or = bool(_OR_PROSE_RE.search(text))
    has_and = bool(_AND_PROSE_RE.search(text))
    leaf_count = len(logic_all_predicate_leaves(root))

    if root.node_type in _GROUP_TYPES and leaf_count > 1:
        if has_or and not has_and and root.node_type == "ALL_OF":
            events.append(
                {
                    "type": "CONFIRMATION_LOGIC_REPAIRED",
                    "detail": "prose indicates OR alternatives; root ALL_OF converted to ANY_OF",
                }
            )
            root = ConfirmationLogicNode(
                node_type="ANY_OF",
                node_id=root.node_id or _new_node_id(),
                children=list(root.children),
            )
        elif has_and and not has_or and root.node_type == "ANY_OF":
            events.append(
                {
                    "type": "CONFIRMATION_LOGIC_REPAIRED",
                    "detail": "prose indicates cumulative AND; root ANY_OF converted to ALL_OF",
                }
            )
            root = ConfirmationLogicNode(
                node_type="ALL_OF",
                node_id=root.node_id or _new_node_id(),
                children=list(root.children),
            )
        elif has_or and root.node_type == "ALL_OF":
            events.append(
                {
                    "type": "CONFIRMATION_LOGIC_REJECTED",
                    "detail": "OR prose with ALL_OF root could not be auto-repaired (mixed AND/OR)",
                }
            )
    return root, events


def prepare_confirmation_logic_for_freeze(
    *,
    confirmation_logic: dict[str, Any] | None,
    confirmation_predicates: list[dict[str, Any]],
    confirmation_conditions: list[str],
    reasoning_summary: str = "",
    current_thesis: str = "",
) -> tuple[ConfirmationLogicNode, list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    if confirmation_logic:
        root = parse_confirmation_logic(confirmation_logic)
    elif confirmation_predicates:
        root = parse_confirmation_logic(wrap_predicates_as_logic(confirmation_predicates, combinator="ALL_OF"))
        events.append(
            {
                "type": "CONFIRMATION_LOGIC_INFERRED",
                "detail": "missing confirmation_logic; wrapped flat predicates as ALL_OF pending prose check",
            }
        )
    else:
        raise ValueError("ARM_LONG requires confirmation_logic or confirmation_predicates")

    root, repair_events = validate_logic_prose_consistency(
        root,
        confirmation_conditions=confirmation_conditions,
        reasoning_summary=reasoning_summary,
        current_thesis=current_thesis,
    )
    events.extend(repair_events)
    return root, events


def assign_validity_windows(root: ConfirmationLogicNode, *, signal_bar_index: int, infer_fn) -> None:
    for leaf in logic_all_predicate_leaves(root):
        assert leaf.predicate is not None
        vf, vu = infer_fn(leaf.predicate, signal_bar_index=signal_bar_index)
        leaf.valid_from_bar = vf
        leaf.valid_until_bar = vu


def logic_leaf_is_terminal(status: PredicateStatus) -> bool:
    return status in ("satisfied", "expired", "failed")


def logic_node_satisfied(node: ConfirmationLogicNode) -> bool:
    if node.node_type == "PREDICATE":
        return node.status == "satisfied"
    if node.node_type == "ALL_OF":
        return bool(node.children) and all(logic_node_satisfied(c) for c in node.children)
    if node.node_type == "ANY_OF":
        return any(logic_node_satisfied(c) for c in node.children)
    return False


def logic_node_permanently_failed(node: ConfirmationLogicNode) -> bool:
    if node.node_type == "PREDICATE":
        return node.status in ("failed", "expired")
    if node.node_type == "ALL_OF":
        if not node.children:
            return True
        return any(logic_node_permanently_failed(c) for c in node.children)
    if node.node_type == "ANY_OF":
        if not node.children:
            return True
        return all(logic_node_permanently_failed(c) for c in node.children)
    return False


def logic_satisfaction_detail(root: ConfirmationLogicNode) -> dict[str, Any]:
    """Report which branch/path satisfied and terminal states of alternatives."""

    def walk(n: ConfirmationLogicNode, path: str) -> dict[str, Any]:
        if n.node_type == "PREDICATE":
            p = n.predicate or {}
            return {
                "path": path,
                "predicate_type": p.get("type"),
                "level": p.get("level"),
                "status": n.status,
                "satisfied_at_bar": n.satisfied_at_bar,
            }
        child_reports = [walk(c, f"{path}/{c.node_type}:{i}") for i, c in enumerate(n.children)]
        satisfied_children = [i for i, c in enumerate(n.children) if logic_node_satisfied(c)]
        return {
            "path": path,
            "combinator": n.node_type,
            "satisfied": logic_node_satisfied(n),
            "satisfied_child_indices": satisfied_children,
            "children": child_reports,
        }

    satisfied_paths: list[str] = []

    def collect_satisfied_paths(n: ConfirmationLogicNode, path: str) -> None:
        if n.node_type == "PREDICATE":
            if n.status == "satisfied":
                satisfied_paths.append(path)
            return
        for i, c in enumerate(n.children):
            if logic_node_satisfied(c):
                collect_satisfied_paths(c, f"{path}/{n.node_type}:{i}")
            elif n.node_type == "ANY_OF":
                continue
            else:
                collect_satisfied_paths(c, f"{path}/{n.node_type}:{i}")

    collect_satisfied_paths(root, "root")
    leaves = logic_all_predicate_leaves(root)
    return {
        "root_combinator": root.node_type,
        "satisfied_paths": satisfied_paths,
        "tree": walk(root, "root"),
        "predicates": [
            {
                "predicate_type": (l.predicate or {}).get("type"),
                "level": (l.predicate or {}).get("level"),
                "status": l.status,
                "valid_from_bar": l.valid_from_bar,
                "valid_until_bar": l.valid_until_bar,
                "satisfied_at_bar": l.satisfied_at_bar,
            }
            for l in leaves
        ],
    }
