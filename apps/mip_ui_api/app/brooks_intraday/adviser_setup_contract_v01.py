"""Finite ARM_LONG confirmation lifecycle and setup contract (Adviser V0.1)."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

from .adviser_confirmation_logic_v01 import (
    ConfirmationLogicNode,
    assign_validity_windows,
    logic_all_predicate_leaves,
    logic_fingerprint,
    logic_node_permanently_failed,
    logic_node_satisfied,
    logic_satisfaction_detail,
    parse_confirmation_logic,
    prepare_confirmation_logic_for_freeze,
)
from .adviser_invalidation_v01 import LEVEL_TOL, _ensure_predicate_id
from .objective_ruleset_v01 import BarGeometry, compute_geometry, price_above, price_below

CONFIRMATION_CONTRACT_VERSION = 2

WAKE_CONFIRMATION_COMPLETE = "CONFIRMATION_COMPLETE"

SUPPORTED_CONFIRMATION_TYPES = frozenset(
    {
        "LEVEL_BREAK",
        "CLOSE_ABOVE",
        "BREAK_ABOVE_LEVEL",
        "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
        "BULL_FOLLOW_THROUGH",
        "BREAKOUT_PULLBACK_HOLD_CONFIRMED",
        "FAILED_BEAR_BREAKOUT_CONFIRMED",
        "FAILED_BREAKOUT_CONFIRMED",
        "SIGNAL_BAR_CONFIRMED",
        "PULLBACK_LEG_COMPLETED",
        "MIN_BODY_FRACTION",
        "BULL_BAR",
    }
)

PredicateStatus = Literal["pending", "satisfied", "expired", "failed"]


@dataclass
class MandatoryConfirmation:
    predicate: dict[str, Any]
    valid_from_bar: int
    valid_until_bar: int
    status: PredicateStatus = "pending"
    satisfied_at_bar: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "predicate": self.predicate,
            "valid_from_bar": self.valid_from_bar,
            "valid_until_bar": self.valid_until_bar,
            "status": self.status,
            "satisfied_at_bar": self.satisfied_at_bar,
        }


@dataclass
class SetupContract:
    setup_id: str
    setup_started_at_bar: int
    setup_started_at_et: str
    setup_family: str
    confirmation_contract_version: int
    confirmation_logic: ConfirmationLogicNode = field(
        default_factory=lambda: ConfirmationLogicNode(node_type="ALL_OF")
    )
    optional_confirmation_text: list[str] = field(default_factory=list)
    armed: bool = True
    confirmation_complete_wake_fired: bool = False
    ladder_extensions: list[dict[str, Any]] = field(default_factory=list)
    last_confirmation_satisfaction: dict[str, Any] | None = None

    @property
    def mandatory(self) -> list[MandatoryConfirmation]:
        out: list[MandatoryConfirmation] = []
        for leaf in logic_all_predicate_leaves(self.confirmation_logic):
            out.append(
                MandatoryConfirmation(
                    predicate=dict(leaf.predicate or {}),
                    valid_from_bar=leaf.valid_from_bar,
                    valid_until_bar=leaf.valid_until_bar,
                    status=leaf.status,
                    satisfied_at_bar=leaf.satisfied_at_bar,
                )
            )
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "setup_id": self.setup_id,
            "setup_started_at_bar": self.setup_started_at_bar,
            "setup_started_at_et": self.setup_started_at_et,
            "setup_family": self.setup_family,
            "confirmation_contract_version": self.confirmation_contract_version,
            "confirmation_logic": self.confirmation_logic.to_dict(),
            "mandatory": [m.to_dict() for m in self.mandatory],
            "optional_confirmation_text": self.optional_confirmation_text,
            "armed": self.armed,
            "confirmation_complete_wake_fired": self.confirmation_complete_wake_fired,
            "ladder_extensions": self.ladder_extensions,
            "last_confirmation_satisfaction": self.last_confirmation_satisfaction,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> SetupContract:
        version = int(data.get("confirmation_contract_version", 1))
        if data.get("confirmation_logic"):
            logic = parse_confirmation_logic(data["confirmation_logic"])
        elif data.get("mandatory"):
            from .adviser_confirmation_logic_v01 import wrap_predicates_as_logic

            preds = [m["predicate"] for m in data["mandatory"]]
            logic = parse_confirmation_logic(wrap_predicates_as_logic(preds, combinator="ALL_OF"))
            for leaf, m in zip(logic_all_predicate_leaves(logic), data["mandatory"]):
                leaf.valid_from_bar = int(m["valid_from_bar"])
                leaf.valid_until_bar = int(m["valid_until_bar"])
                leaf.status = m.get("status", "pending")
                leaf.satisfied_at_bar = m.get("satisfied_at_bar")
        else:
            logic = ConfirmationLogicNode(node_type="ALL_OF", children=[])

        return SetupContract(
            setup_id=str(data["setup_id"]),
            setup_started_at_bar=int(data["setup_started_at_bar"]),
            setup_started_at_et=str(data["setup_started_at_et"]),
            setup_family=str(data["setup_family"]),
            confirmation_contract_version=version,
            confirmation_logic=logic,
            optional_confirmation_text=list(data.get("optional_confirmation_text") or []),
            armed=bool(data.get("armed", True)),
            confirmation_complete_wake_fired=bool(data.get("confirmation_complete_wake_fired", False)),
            ladder_extensions=list(data.get("ladder_extensions") or []),
            last_confirmation_satisfaction=data.get("last_confirmation_satisfaction"),
        )


def setup_family_key(mandatory_preds: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for p in sorted(mandatory_preds, key=lambda x: json.dumps(x, sort_keys=True)):
        ptype = str(p.get("type", "")).upper()
        level = p.get("level")
        direction = str(p.get("direction", "")).upper()
        parts.append(f"{ptype}:{level}:{direction}")
    blob = "|".join(parts)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def normalize_mandatory_predicates(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        p = _ensure_predicate_id(dict(item))
        ptype = str(p.get("type", "")).upper()
        if ptype == "BULL_FOLLOW_THROUGH" and (p.get("after_signal") or p.get("signal_bar")):
            p["type"] = "BULL_FOLLOW_THROUGH_AFTER_SIGNAL"
        if ptype and ptype not in SUPPORTED_CONFIRMATION_TYPES:
            continue
        out.append(p)
    return out[:8]


def infer_validity_window(
    pred: dict[str, Any],
    *,
    signal_bar_index: int,
) -> tuple[int, int]:
    """Bar-index window [valid_from, valid_until] inclusive for this mandatory confirmation."""
    p = pred
    vf = p.get("valid_from_bar")
    vu = p.get("valid_until_bar")
    if vf is not None and vu is not None:
        return int(vf), int(vu)

    vfo = p.get("valid_from_bar_offset")
    vuo = p.get("valid_until_bar_offset")
    if vfo is not None or vuo is not None:
        start = signal_bar_index + int(vfo if vfo is not None else 1)
        end = signal_bar_index + int(vuo if vuo is not None else vfo if vfo is not None else 1)
        return start, max(start, end)

    bar_req = str(p.get("bar_requirement") or "").lower()
    bar_off = p.get("bar_offset")
    ptype = str(p.get("type", "")).upper()

    if bar_off is not None:
        b = signal_bar_index + int(bar_off)
        return b, b
    if "next bar" in bar_req or bar_req == "next":
        b = signal_bar_index + 1
        return b, b

    if ptype == "BULL_FOLLOW_THROUGH_AFTER_SIGNAL":
        b = signal_bar_index + 1
        return b, b

    if ptype in ("SIGNAL_BAR_CONFIRMED",):
        return signal_bar_index, signal_bar_index + 1

    window = int(p.get("valid_window_bars", 6))
    return signal_bar_index + 1, signal_bar_index + window


def _pullback_legs_completed(recent: list[Any], pred: dict[str, Any]) -> bool:
    minimum = int(pred.get("minimum_legs", 2))
    if len(recent) < minimum + 1:
        return False
    legs = 0
    prev_dir = None
    for b in recent[-(minimum + 3) :]:
        g = compute_geometry(open_=b.open, high=b.high, low=b.low, close=b.close, volume=b.volume)
        if prev_dir and g.direction != prev_dir and g.direction in ("BULLISH", "BEARISH"):
            legs += 1
        if g.direction in ("BULLISH", "BEARISH"):
            prev_dir = g.direction
    return legs >= minimum


def _failed_bear_breakout_confirmed(bar: Any, prev_bar: Any | None, pred: dict[str, Any]) -> bool:
    if prev_bar is None:
        return False
    level = float(pred.get("level", pred.get("support_level", 0)))
    low_probe = bar.low < level - LEVEL_TOL
    now_above = price_above(bar.close, level, tol=LEVEL_TOL)
    g = compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)
    return low_probe and now_above and g.direction == "BULLISH"


def evaluate_mandatory_predicate(
    pred: dict[str, Any],
    *,
    bar: Any,
    bar_index: int,
    recent: list[Any],
    prev_bar: Any | None,
    prev_geom: dict | None,
    g: BarGeometry,
) -> bool:
    from .adviser_predicate_eval_v01 import evaluate_brooks_predicate_state

    return evaluate_brooks_predicate_state(
        pred,
        bar=bar,
        bar_index=bar_index,
        recent=recent,
        prev_bar=prev_bar,
        prev_geom=prev_geom,
        g=g,
    )


def tick_mandatory_confirmations(
    contract: SetupContract,
    *,
    bar_index: int,
    bar: Any,
    recent: list[Any],
    prev_bar: Any | None,
    prev_geom: dict | None,
    g: BarGeometry,
) -> None:
    for leaf in logic_all_predicate_leaves(contract.confirmation_logic):
        if leaf.status in ("satisfied", "expired", "failed"):
            continue
        if bar_index < leaf.valid_from_bar:
            continue
        if bar_index > leaf.valid_until_bar:
            leaf.status = "expired"
            continue
        assert leaf.predicate is not None
        if evaluate_mandatory_predicate(
            leaf.predicate,
            bar=bar,
            bar_index=bar_index,
            recent=recent,
            prev_bar=prev_bar,
            prev_geom=prev_geom,
            g=g,
        ):
            leaf.status = "satisfied"
            leaf.satisfied_at_bar = bar_index
        elif bar_index == leaf.valid_until_bar:
            leaf.status = "failed"


def all_mandatory_satisfied(contract: SetupContract) -> bool:
    return logic_node_satisfied(contract.confirmation_logic)


def any_mandatory_expired_or_failed(contract: SetupContract) -> bool:
    return logic_node_permanently_failed(contract.confirmation_logic)


def mandatory_logical_fingerprint(mandatory: list[MandatoryConfirmation]) -> str:
    preds = [m.predicate for m in mandatory]
    return setup_family_key(preds)


def setup_family_from_logic(root: ConfirmationLogicNode) -> str:
    return logic_fingerprint(root)


def freeze_setup_contract(
    *,
    confirmation_logic: ConfirmationLogicNode,
    optional_text: list[str],
    signal_bar_index: int,
    signal_bar_et: str,
    existing: SetupContract | None = None,
) -> SetupContract:
    assign_validity_windows(
        confirmation_logic,
        signal_bar_index=signal_bar_index,
        infer_fn=infer_validity_window,
    )
    family = setup_family_from_logic(confirmation_logic)
    setup_id = (
        existing.setup_id if existing and existing.setup_family == family else f"{family}-{uuid.uuid4().hex[:8]}"
    )
    started_bar = (
        existing.setup_started_at_bar if existing and existing.setup_family == family else signal_bar_index
    )
    started_et = (
        existing.setup_started_at_et if existing and existing.setup_family == family else signal_bar_et
    )

    logic_copy = parse_confirmation_logic(json.loads(json.dumps(confirmation_logic.to_dict())))

    return SetupContract(
        setup_id=setup_id,
        setup_started_at_bar=started_bar,
        setup_started_at_et=started_et,
        setup_family=family,
        confirmation_contract_version=CONFIRMATION_CONTRACT_VERSION,
        confirmation_logic=logic_copy,
        optional_confirmation_text=list(optional_text or []),
        armed=True,
        confirmation_complete_wake_fired=existing.confirmation_complete_wake_fired if existing else False,
        ladder_extensions=list(existing.ladder_extensions) if existing else [],
    )


def should_reset_setup(
    *,
    thesis_invalidated: bool,
    new_action: str,
    position_qty_before: int,
    position_qty_after: int,
) -> bool:
    if thesis_invalidated:
        return True
    if new_action.upper() == "INVALIDATE":
        return True
    if position_qty_before != position_qty_after:
        return True
    return False


def freeze_setup_contract_from_predicates(
    *,
    mandatory_preds: list[dict[str, Any]],
    optional_text: list[str],
    signal_bar_index: int,
    signal_bar_et: str,
    existing: SetupContract | None = None,
    combinator: Literal["ALL_OF", "ANY_OF"] = "ALL_OF",
) -> SetupContract:
    from .adviser_confirmation_logic_v01 import wrap_predicates_as_logic

    norm = normalize_mandatory_predicates(mandatory_preds)
    if not norm:
        raise ValueError("requires at least one confirmation predicate")
    root = parse_confirmation_logic(wrap_predicates_as_logic(norm, combinator=combinator))
    return freeze_setup_contract(
        confirmation_logic=root,
        optional_text=optional_text,
        signal_bar_index=signal_bar_index,
        signal_bar_et=signal_bar_et,
        existing=existing,
    )


def process_adviser_setup_response(
    contract: SetupContract | None,
    *,
    prior_action: str,
    new_action: str,
    wake_reason: str,
    mandatory_preds_from_llm: list[dict[str, Any]],
    confirmation_logic_raw: dict[str, Any] | None = None,
    optional_text: list[str],
    confirmation_extension_reason: str | None,
    signal_bar_index: int,
    signal_bar_et: str,
    thesis_invalidated: bool,
    position_changed: bool,
    position_qty_before: int = 0,
    position_qty_after: int = 0,
    reasoning_summary: str = "",
    current_thesis: str = "",
) -> tuple[SetupContract | None, list[dict[str, Any]]]:
    """Returns (updated contract, audit events)."""
    events: list[dict[str, Any]] = []

    if should_reset_setup(
        thesis_invalidated=thesis_invalidated,
        new_action=new_action,
        position_qty_before=position_qty_before,
        position_qty_after=position_qty_after,
    ):
        if contract:
            events.append({"type": "SETUP_RESET", "reason": "invalidation_or_position"})
        return None, events

    act = new_action.upper()
    if act != "ARM_LONG" and act != "CONSIDER_ENTRY":
        if act in ("WATCH_LONG",) and contract and not thesis_invalidated:
            if contract:
                contract.optional_confirmation_text = list(optional_text or [])
            return contract, events
        if thesis_invalidated or act == "INVALIDATE":
            return None, events
        return contract, events

    norm_new = normalize_mandatory_predicates(mandatory_preds_from_llm)

    if contract is None and act == "ARM_LONG":
        try:
            logic_root, prep_events = prepare_confirmation_logic_for_freeze(
                confirmation_logic=confirmation_logic_raw,
                confirmation_predicates=norm_new,
                confirmation_conditions=optional_text,
                reasoning_summary=reasoning_summary,
                current_thesis=current_thesis,
            )
        except ValueError:
            return None, events
        events.extend(prep_events)
        c = freeze_setup_contract(
            confirmation_logic=logic_root,
            optional_text=optional_text,
            signal_bar_index=signal_bar_index,
            signal_bar_et=signal_bar_et,
        )
        events.append({"type": "SETUP_ARMED", "setup_id": c.setup_id})
        return c, events

    if contract is None and act == "CONSIDER_ENTRY":
        return None, events

    if contract and act == "ARM_LONG":
        new_fp = contract.setup_family
        logic_root = None
        if norm_new or confirmation_logic_raw:
            try:
                logic_root, prep_events = prepare_confirmation_logic_for_freeze(
                    confirmation_logic=confirmation_logic_raw,
                    confirmation_predicates=norm_new,
                    confirmation_conditions=optional_text,
                    reasoning_summary=reasoning_summary,
                    current_thesis=current_thesis,
                )
                new_fp = setup_family_from_logic(logic_root)
                events.extend(prep_events)
            except ValueError:
                logic_root = None

        old_fp = contract.setup_family
        if logic_root is not None and new_fp != old_fp:
            if wake_reason == WAKE_CONFIRMATION_COMPLETE:
                reason = (confirmation_extension_reason or "").strip()
                if not reason:
                    events.append(
                        {
                            "type": "CONFIRMATION_LADDER_REJECTED",
                            "detail": "new mandatory confirmation without confirmation_extension_reason",
                        }
                    )
                else:
                    events.append(
                        {
                            "type": "CONFIRMATION_LADDER_EXTENDED",
                            "setup_id": contract.setup_id,
                            "reason": reason,
                        }
                    )
                    contract.ladder_extensions.append({"reason": reason, "bar_et": signal_bar_et})
                    contract = freeze_setup_contract(
                        confirmation_logic=logic_root,
                        optional_text=optional_text,
                        signal_bar_index=signal_bar_index,
                        signal_bar_et=signal_bar_et,
                        existing=contract,
                    )
            else:
                events.append({"type": "MANDATORY_CONFIRMATION_HELD", "setup_id": contract.setup_id})
        contract.optional_confirmation_text = list(optional_text or [])
        return contract, events

    if contract:
        contract.optional_confirmation_text = list(optional_text or [])
    return contract, events


def entry_allowed_consider_entry(
    action: str,
    contract: SetupContract | None,
    *,
    bar: Any,
    bar_index: int,
    recent: list[Any],
    prev_bar: Any | None,
    prev_geom: dict | None,
    g: BarGeometry,
) -> tuple[bool, str]:
    if action.upper() != "CONSIDER_ENTRY":
        return False, "action_not_consider_entry"
    if contract is None or not logic_all_predicate_leaves(contract.confirmation_logic):
        return False, "no_frozen_setup_contract"
    tick_mandatory_confirmations(
        contract,
        bar_index=bar_index,
        bar=bar,
        recent=recent,
        prev_bar=prev_bar,
        prev_geom=prev_geom,
        g=g,
    )
    if not all_mandatory_satisfied(contract):
        pending = [
            (leaf.predicate or {}).get("type")
            for leaf in logic_all_predicate_leaves(contract.confirmation_logic)
            if leaf.status != "satisfied"
        ]
        return False, f"frozen_confirmations_pending:{','.join(str(x) for x in pending)}"
    return True, "frozen_confirmations_satisfied"


def evaluate_confirmation_complete_wake(
    contract: SetupContract | None,
    *,
    bar_index: int,
    bar: Any,
    recent: list[Any],
    prev_bar: Any | None,
    prev_geom: dict | None,
    g: BarGeometry,
    last_action: str,
) -> bool:
    if contract is None or not contract.armed:
        return False
    if last_action.upper() != "ARM_LONG":
        return False
    if contract.confirmation_complete_wake_fired:
        return False
    tick_mandatory_confirmations(
        contract,
        bar_index=bar_index,
        bar=bar,
        recent=recent,
        prev_bar=prev_bar,
        prev_geom=prev_geom,
        g=g,
    )
    if all_mandatory_satisfied(contract):
        contract.last_confirmation_satisfaction = logic_satisfaction_detail(contract.confirmation_logic)
        return True
    return False


def mark_confirmation_complete_fired(contract: SetupContract) -> None:
    contract.confirmation_complete_wake_fired = True


def build_confirmation_complete_wake_detail(contract: SetupContract) -> str:
    detail = logic_satisfaction_detail(contract.confirmation_logic)
    paths = detail.get("satisfied_paths") or []
    preds_ok = [p for p in detail.get("predicates") or [] if p.get("status") == "satisfied"]
    preds_bad = [p for p in detail.get("predicates") or [] if p.get("status") in ("failed", "expired")]
    return (
        f"setup {contract.setup_id}: boolean confirmation satisfied; "
        f"paths={paths}; satisfied={preds_ok}; failed_or_expired={preds_bad}"
    )
