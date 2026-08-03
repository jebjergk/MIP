"""Pattern lifecycle validation (Phase 5B)."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

LIFECYCLE_ORDER = ("POSSIBLE", "DEVELOPING", "CONFIRMED", "FAILED", "EXPIRED")

ALLOWED_TRANSITIONS: set[tuple[str, str]] = {
    ("POSSIBLE", "DEVELOPING"),
    ("POSSIBLE", "CONFIRMED"),
    ("POSSIBLE", "FAILED"),
    ("POSSIBLE", "EXPIRED"),
    ("DEVELOPING", "CONFIRMED"),
    ("DEVELOPING", "FAILED"),
    ("DEVELOPING", "EXPIRED"),
    # V0.1 records synthetic POSSIBLE in history then jumps to CONFIRMED on same step — allow direct
    ("POSSIBLE", "CONFIRMED"),
}

FORBIDDEN_FINAL_FROM: dict[str, set[str]] = {
    "EXPIRED": {"CONFIRMED"},
    "FAILED": {"CONFIRMED"},
}


def _parse_ts(ts: Any) -> datetime | None:
    if ts is None:
        return None
    try:
        return datetime.fromisoformat(str(ts)[:19])
    except ValueError:
        return None


def _bar_index_delta(start: datetime | None, end: datetime | None, *, minutes: int = 5) -> float | None:
    if not start or not end:
        return None
    return (end - start).total_seconds() / (60 * minutes)


def validate_lifecycle_history(history: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    if not history:
        errors.append("empty_lifecycle_history")
        return errors
    seen: list[str] = []
    for entry in history:
        lc = entry.get("lifecycle")
        if lc:
            seen.append(str(lc).upper())
    for i in range(1, len(seen)):
        prev, cur = seen[i - 1], seen[i]
        if prev == cur:
            continue
        if (prev, cur) not in ALLOWED_TRANSITIONS:
            errors.append(f"illegal_transition:{prev}->{cur}")
    final = seen[-1] if seen else None
    if final in FORBIDDEN_FINAL_FROM:
        for prior in seen[:-1]:
            if prior in FORBIDDEN_FINAL_FROM[final]:
                errors.append(f"forbidden_after_{prior}_then_{final}")
    if final == "FAILED" and "POSSIBLE" not in seen and "DEVELOPING" not in seen:
        errors.append("failed_without_prior_candidate")
    if final == "CONFIRMED" and "FAILED" in seen:
        errors.append("confirmed_after_failed")
    return errors


def audit_patterns(patterns: list[dict[str, Any]]) -> dict[str, Any]:
    by_family: dict[str, list[dict]] = defaultdict(list)
    for p in patterns:
        fam = p.get("pattern_family") or "?"
        by_family[fam].append(p)

    family_reports: dict[str, Any] = {}
    illegal_total = 0
    dup_support: list[dict] = []

    for fam, items in sorted(by_family.items()):
        lc_counts = Counter(p.get("lifecycle_status") for p in items)
        to_confirm: list[float] = []
        to_fail: list[float] = []
        to_expire: list[float] = []
        illegal = 0
        for p in items:
            hist = p.get("lifecycle_history_json") or []
            errs = validate_lifecycle_history(hist if isinstance(hist, list) else [])
            if errs:
                illegal += 1
            start = _parse_ts(p.get("start_ts"))
            if p.get("lifecycle_status") == "CONFIRMED":
                to_confirm.append(_bar_index_delta(start, _parse_ts(p.get("confirmation_ts"))) or 0)
            if p.get("lifecycle_status") == "FAILED":
                to_fail.append(_bar_index_delta(start, _parse_ts(p.get("failure_ts"))) or 0)
            if p.get("lifecycle_status") == "EXPIRED":
                to_expire.append(_bar_index_delta(start, _parse_ts(p.get("expiry_ts"))) or 0)

        family_reports[fam] = {
            "instances": len(items),
            "lifecycle": dict(lc_counts),
            "avg_bars_to_confirm": sum(to_confirm) / len(to_confirm) if to_confirm else None,
            "avg_bars_to_fail": sum(to_fail) / len(to_fail) if to_fail else None,
            "avg_bars_to_expire": sum(to_expire) / len(to_expire) if to_expire else None,
            "illegal_transitions": illegal,
        }
        illegal_total += illegal

    # duplicate supporting observation sets
    seen_support: dict[str, list[str]] = defaultdict(list)
    for p in patterns:
        sup = p.get("supporting_observation_ids")
        if isinstance(sup, list):
            key = "|".join(sorted(str(x) for x in sup))
            if key:
                seen_support[key].append(p.get("pattern_instance_id", ""))
    for key, ids in seen_support.items():
        if len(ids) > 1:
            dup_support.append({"support_key": key[:80], "pattern_ids": ids[:10], "count": len(ids)})

    return {
        "by_family": family_reports,
        "total_instances": len(patterns),
        "illegal_transition_instances": illegal_total,
        "duplicate_supporting_bars_groups": len(dup_support),
        "duplicate_supporting_bars_sample": dup_support[:50],
    }


def audit_h1_h2(patterns: list[dict[str, Any]]) -> dict[str, Any]:
    h1_possible = [p for p in patterns if p.get("pattern_family") == "POSSIBLE_H1_LONG"]
    h1_confirmed = [p for p in patterns if "CONFIRMED_H1" in str(p.get("pattern_family", ""))]
    h1_failed = [p for p in patterns if p.get("pattern_family") == "FAILED_H1_LONG"]
    h2 = [p for p in patterns if "H2" in str(p.get("pattern_family", ""))]

    orphan_failed = []
    for p in h1_failed:
        hist = p.get("lifecycle_history_json") or []
        if isinstance(hist, list):
            lcs = [h.get("lifecycle") for h in hist]
            if "POSSIBLE" not in lcs and "DEVELOPING" not in lcs:
                orphan_failed.append(p.get("pattern_instance_id"))

    orphan_h2 = []
    for p in h2:
        if not p.get("parent_pattern_instance_id"):
            orphan_h2.append(p.get("pattern_instance_id"))

    ever_h1_candidate = 0
    for p in patterns:
        fam = str(p.get("pattern_family", ""))
        if "H1" in fam or any(
            h.get("rule_id", "").startswith("H1") for h in (p.get("lifecycle_history_json") or []) if isinstance(p.get("lifecycle_history_json"), list)
        ):
            if "H1_CANDIDATE" in str(p.get("supporting_rule_ids")) or fam.startswith("POSSIBLE_H1") or fam.startswith("FAILED_H1") or "CONFIRMED_H1" in fam:
                ever_h1_candidate += 1

    return {
        "possible_h1_open": len(h1_possible),
        "confirmed_h1": len(h1_confirmed),
        "failed_h1": len(h1_failed),
        "h2_candidates": len(h2),
        "orphan_failed_h1": len(orphan_failed),
        "orphan_failed_h1_ids_sample": orphan_failed[:20],
        "orphan_h2": len(orphan_h2),
        "orphan_h2_ids_sample": orphan_h2[:20],
        "estimated_h1_ever_created": len(h1_possible) + len(h1_failed) + len(h1_confirmed),
    }
