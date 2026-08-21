#!/usr/bin/env python3
"""Build 2026-07-24 fresh-day evaluation report from raw extract."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "cursorfiles" / "brooks_724_fresh_day_raw.json"
OUT_MD = ROOT / "cursorfiles" / "brooks_724_fresh_day_evaluation_report.md"
OUT_JSON = ROOT / "cursorfiles" / "brooks_724_fresh_day_evaluation_report.json"

ATTEMPTS = {
    "CRM": "f76c5f27-b3dd-436e-8935-31520a2c1b21",
    "MSFT": "88c67578-d99f-4518-b756-ebe7db3a94ac",
    "NKE": "bcf1aaf0-d700-4cc4-9a1b-93d0eadbe135",
    "NVDA": "8549c812-5f3d-494b-8d3c-7dd3d0d8e24f",
}
FEE_RT = 2.0


def _find_call(calls: list, bar_et: str) -> dict | None:
    return next((c for c in calls if c.get("BAR_TS_ET") == bar_et), None)


def _opp(opps: list, bar_et: str) -> dict | None:
    return next((o for o in opps if o.get("bar_et") == bar_et), None)


def _brooks_entry_class(opp: dict) -> tuple[str, str]:
    q = opp.get("brooks_entry_quality") or ""
    w = opp.get("trade_worthiness_verdict") or ""
    te = (opp.get("traders_equation_assessment") or "").lower()
    loc = (opp.get("location_assessment") or "").lower()
    if w == "TRADE_WORTHY" and q == "STRONG":
        if "marginal" in te or "small" in loc:
            return "BROOKS_REASONABLE", "Strong mechanical setup with acknowledged tight range or marginal follow-through at entry."
        return "BROOKS_STRONG", "Coherent cycle, confirmation, location, and equation at decision time."
    if w == "TRADE_WORTHY" and q == "ACCEPTABLE":
        return "BROOKS_REASONABLE", "Valid Brooks structure with acceptable but non-standout quality; marginal R:R acknowledged at entry."
    if w == "TRADE_WORTHY" and q == "MARGINAL":
        return "BROOKS_MARGINAL", "Synthesis trade-worthy but quality marginal (extension/location); lab gate correctly withheld execution."
    if w == "VALID_BUT_PASS":
        return "BROOKS_MARGINAL", "Valid structure but synthesis passed on location/cycle grounds."
    return "BROOKS_REASONABLE", "Persisted synthesis fields support reasonable Brooks alignment."


def main() -> None:
    raw = json.loads(RAW.read_text(encoding="utf-8"))
    report: dict = {
        "meta": {
            "trading_date": "2026-07-24",
            "evaluation_type": "fresh_day_oos_scorecard",
            "config": {
                "ace_lite_patch": 2,
                "entry_synthesis_patch": 2,
                "entry_synthesis_contract_version": "1.1",
            },
            "attempts": ATTEMPTS,
            "fee_assumption_per_round_trip_usd": FEE_RT,
            "data_source": "persisted BROOKS_INTRADAY_ADVISER_ATTEMPT/CALL only",
        },
        "executive_scorecard": {},
        "decision_time_brooks_evaluation": [],
        "missed_opportunity_review": [],
        "nke_special_review": {},
        "exit_risk_management_review": [],
        "architecture_health": {"none": [], "observation": [], "material_defect": []},
        "fresh_day_verdict": {},
    }

    trades_detail = {
        "MSFT": {
            "entry_et": "13:15",
            "exit_et": "13:20",
            "entry_price": 384.29,
            "exit_price": 383.96,
            "initial_stop": 383.55,
            "exit_reason": "THESIS_FAILURE / PATTERN_BASED (failed immediate follow-through; swing_break_low)",
            "trade_intent": "SWING",
            "entry_quality": "STRONG",
            "trade_worthiness": "TRADE_WORTHY",
            "gross_pnl": -0.66,
        },
        "NKE": {
            "entry_et": "14:45",
            "exit_et": "15:55",
            "entry_price": 41.93,
            "exit_price": 41.60,
            "initial_stop": 41.76,
            "exit_reason": "THESIS_FAILURE / STRUCTURAL_BREACH (close below pullback low 41.69)",
            "trade_intent": "SWING",
            "entry_quality": "ACCEPTABLE",
            "trade_worthiness": "TRADE_WORTHY",
            "gross_pnl": -7.59,
        },
    }

    all_trades = []
    for sym in ["CRM", "MSFT", "NKE", "NVDA"]:
        s = raw["symbols"][sym]
        sim = s.get("sim_trades") or {}
        opps = s.get("entry_opportunities") or []
        if sym in trades_detail:
            t = trades_detail[sym]
            opp = _opp(opps, t["entry_et"]) or {}
            bc, why = _brooks_entry_class(opp)
            gross = t["gross_pnl"]
            net = gross - FEE_RT
            row = {
                "symbol": sym,
                "executed_trades": 1,
                "trades": [{**t, "fees_est": FEE_RT, "net_pnl": net, "brooks_class": bc, "brooks_class_reason": why}],
            }
            all_trades.append({**t, "symbol": sym, "fees_est": FEE_RT, "net_pnl": net, "brooks_class": bc})
            report["decision_time_brooks_evaluation"].append(
                {
                    "symbol": sym,
                    "bar_et": t["entry_et"],
                    "brooks_class": bc,
                    "reason": why,
                    "dimensions": {
                        "market_cycle": opp.get("market_cycle_assessment"),
                        "always_in_control": opp.get("always_in_assessment"),
                        "location": opp.get("location_assessment"),
                        "opposing_evidence": opp.get("opposing_evidence"),
                        "traders_equation": opp.get("traders_equation_assessment"),
                        "setup": opp.get("setup_assessment"),
                        "confirmation": opp.get("confirmation_status"),
                        "trade_worthiness": opp.get("trade_worthiness_verdict"),
                    },
                }
            )
            report["exit_risk_management_review"].append(
                {
                    "symbol": sym,
                    "entry_et": t["entry_et"],
                    "exit_et": t["exit_et"],
                    "gross_pnl": gross,
                    "management_class": "GOOD_CONTROLLED_LOSS",
                    "reason": "Loss contained; exit on thesis failure with structural/pattern basis, not runaway hold.",
                }
            )
        else:
            row = {"symbol": sym, "executed_trades": 0, "trades": []}
        row["gross_pnl"] = sum(x["gross_pnl"] for x in row["trades"])
        row["net_pnl"] = sum(x["net_pnl"] for x in row["trades"])
        row["consider_entries"] = [
            {
                "bar_et": o.get("bar_et"),
                "execution_status": o.get("execution_status"),
                "blocker": o.get("blocker"),
                "entry_quality": o.get("brooks_entry_quality"),
                "trade_worthiness": o.get("trade_worthiness_verdict"),
            }
            for o in opps
        ]
        report["executive_scorecard"][sym] = row

    wins = [t for t in all_trades if t["gross_pnl"] > 0]
    losses = [t for t in all_trades if t["gross_pnl"] <= 0]
    gross = sum(t["gross_pnl"] for t in all_trades)
    report["aggregate"] = {
        "trades": len(all_trades),
        "wins": len(wins),
        "losses": len(losses),
        "gross_pnl": gross,
        "total_fees_est": FEE_RT * len(all_trades),
        "net_pnl": gross - FEE_RT * len(all_trades),
        "avg_winner": None,
        "avg_loser": sum(t["gross_pnl"] for t in losses) / len(losses) if losses else None,
        "payoff_ratio": None,
        "profit_factor": 0.0,
    }

    report["missed_opportunity_review"] = [
        {
            "symbol": "NKE",
            "time_window": "10:20–11:25",
            "market_structure": "Morning bull leg from ~40.88 toward session high; two-legged breakout-pullback-follow-through.",
            "cycle_control_recognized": True,
            "watch_arm": "ARM_LONG 10:30, 10:50",
            "consider_entry": "10:55, 11:10",
            "blocker": "quality (MARGINAL: near_session_high_extension; 10:55 also trading_range_without_textbook_structure)",
            "brooks_coherent_pass": True,
            "classification": "MISSED_BUT_DEBATABLE",
            "note": "Small absolute move; buying at session-high extension in sub-$1 range is a defensible Brooks pass despite strong mechanical setup.",
        },
        {
            "symbol": "NVDA",
            "time_window": "10:50–12:05",
            "market_structure": "Range breakout then strong intraday bull trend to 211+.",
            "cycle_control_recognized": True,
            "watch_arm": "Multiple ARM cycles",
            "consider_entry": "10:50, 10:55, 12:00, 12:05",
            "blocker": "quality (MARGINAL: near session high / extension entries)",
            "brooks_coherent_pass": True,
            "classification": "LEGITIMATE_PASS",
            "note": "Repeated extension-at-high CONSIDER_ENTRY blocked by quality gate; consistent selective discipline, not a missed structural entry at support.",
        },
        {
            "symbol": "CRM",
            "time_window": "11:00–12:00",
            "market_structure": "Strong bull trend to new session highs.",
            "cycle_control_recognized": True,
            "watch_arm": True,
            "consider_entry": "11:10, 11:20, 12:20 (VALID_BUT_PASS)",
            "blocker": "quality MARGINAL or synthesis VALID_BUT_PASS",
            "brooks_coherent_pass": True,
            "classification": "LEGITIMATE_PASS",
        },
    ]

    nke_calls = raw["symbols"]["NKE"]["calls"]
    report["nke_special_review"] = {
        "timeline": [
            {"bar_et": b, "action": (_find_call(nke_calls, b) or {}).get("ACTION"), "wake": (_find_call(nke_calls, b) or {}).get("WAKE_REASON")}
            for b in ["10:30", "10:50", "10:55", "11:10", "11:30", "11:40", "14:40", "14:45"]
        ],
        "bull_control_morning": "Mostly yes — regime progressed TRADING_RANGE → INTRADAY_BULL_BREAKOUT → STRONG_BULL_TREND by 11:10; ARM and CONSIDER_ENTRY fired.",
        "why_1055_1110_not_executed": "NOT_ACTED_MARGINAL — brooks_entry_quality MARGINAL despite TRADE_WORTHY synthesis (near_session_high_extension).",
        "morning_passes_brooks_coherent": True,
        "afternoon_vs_morning": "Different and modestly better methodologically: afternoon 14:45 was pullback-hold in STRONG_BULL_TREND (ACCEPTABLE, not MARGINAL), not session-high extension chase.",
        "ace_lite_continuity_evidence": "No material continuity break; 11:40 INVALIDATE on trend weakening was lifecycle-coherent; afternoon re-arm was a new setup family.",
        "outcome_not_defect_proxy": True,
    }

    report["architecture_health"]["observation"] = [
        "NKE: ADVISER_MANAGEMENT_EVIDENCE_MISMATCH on HOLD bars (narrative 'consecutive_bears' vs packet max_consecutive_bear_since_entry ≤1) — non-blocking.",
        "NKE exit: thesis_failure_reference_bar_not_found warning — exit still structurally supported.",
        "Multiple SETUP_CONTRACT_REJECTED events (unsupported predicate types) — normal contract hygiene, setups re-armed successfully.",
        "Quality vs synthesis split (TRADE_WORTHY + MARGINAL quality → NOT_ACTED) — by design on 7/24.",
        "PAA NO_CLEAR_LONG present all day; daily_bias_role=higher_timeframe_context_only — not acting as veto.",
        "NKE 10:55 synthesis market_cycle still TRADING_RANGE while call state INTRADAY_BULL_BREAKOUT — minor cycle label lag.",
    ]
    report["architecture_health"]["none"] = [
        "No loss of market-cycle continuity requiring engineering reopen.",
        "No PAA veto of executed entries.",
        "No invented probability/R thresholds blocking trades.",
        "No execution/persistence anomalies on completed sessions.",
    ]

    report["fresh_day_verdict"] = {
        "verdict": "AMBER — CONTINUE, BUT WATCH",
        "q1_positive_gross_edge": "No — 2 trades, 0 wins, gross −$8.25.",
        "q2_contained_failed_trades": "Yes — MSFT −$0.66 in 5 minutes on thesis failure; NKE −$7.59 exited on structural breach, not open-ended hold.",
        "q3_material_missed_for_methodology": "No clear MATERIAL_MISSED — NKE morning blocked on extension quality is debatable but Brooks-coherent.",
        "q4_defect_stop_validation": "No material repeated defect observed.",
        "q5_next_block_without_changes": "Yes — proceed to next fresh validation block; monitor extension-entry quality gate behavior.",
    }

    OUT_JSON.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    md = []
    md.append("# Brooks Intraday Adviser — 2026-07-24 Fresh-Day Evaluation Pack")
    md.append("")
    md.append("**Method:** Read-only forensic on completed sessions. ACE-Lite V1.1 / Entry Synthesis patch 2. No reruns, no tuning.")
    md.append("")
    md.append("## Attempt IDs")
    md.append("")
    for sym, aid in ATTEMPTS.items():
        calls = raw["symbols"][sym]["calls_count"]
        md.append(f"- **{sym}** `{aid}` ({calls} calls, COMPLETED)")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## 1. Executive Scorecard")
    md.append("")
    for sym in ["CRM", "MSFT", "NKE", "NVDA"]:
        sc = report["executive_scorecard"][sym]
        md.append(f"### {sym}")
        md.append("")
        if sc["executed_trades"] == 0:
            md.append("| Field | Value |")
            md.append("|-------|-------|")
            md.append("| Executed trades | **0** |")
            md.append(f"| Gross P&L | $0.00 |")
            md.append(f"| CONSIDER_ENTRY opps | {len(sc['consider_entries'])} (all blocked) |")
            for ce in sc["consider_entries"]:
                md.append(f"| — {ce['bar_et']} | {ce['execution_status']} / {ce['blocker']} / Q={ce['entry_quality']} / W={ce['trade_worthiness']} |")
        else:
            t = sc["trades"][0]
            md.append("| Field | Value |")
            md.append("|-------|-------|")
            md.append("| Executed trades | **1** |")
            md.append(f"| Entry | {t['entry_et']} @ {t['entry_price']} |")
            md.append(f"| Exit | {t['exit_et']} @ {t['exit_price']} |")
            md.append(f"| Gross P&L | ${t['gross_pnl']:.2f} |")
            md.append(f"| Est. fees ($1/order RT) | ${t['fees_est']:.2f} |")
            md.append(f"| Net P&L | ${t['net_pnl']:.2f} |")
            md.append(f"| Initial structural stop | {t['initial_stop']} |")
            md.append(f"| Exit reason | {t['exit_reason']} |")
            md.append(f"| Trade intent | {t['trade_intent']} |")
            md.append(f"| Entry quality | {t['entry_quality']} |")
            md.append(f"| Trade-worthiness | {t['trade_worthiness']} |")
        md.append("")

    agg = report["aggregate"]
    md.append("### Aggregate (executed trades only)")
    md.append("")
    md.append("| Metric | Value |")
    md.append("|--------|-------|")
    md.append(f"| Trades | {agg['trades']} |")
    md.append(f"| Wins / Losses | {agg['wins']} / {agg['losses']} |")
    md.append(f"| Gross P&L | ${agg['gross_pnl']:.2f} |")
    md.append(f"| Total est. fees | ${agg['total_fees_est']:.2f} |")
    md.append(f"| Net P&L (@ $1/order) | ${agg['net_pnl']:.2f} |")
    md.append(f"| Avg loser (gross) | ${agg['avg_loser']:.2f} |")
    md.append("| Payoff ratio | N/A (no winners) |")
    md.append("| Profit factor | 0 (no gross wins) |")
    md.append("")
    md.append("*Gross performance is reported separately from $1,000-account fee economics.*")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## 2. Decision-Time Brooks Evaluation (executed trades)")
    md.append("")
    for ev in report["decision_time_brooks_evaluation"]:
        md.append(f"### {ev['symbol']} @ {ev['bar_et']} — **{ev['brooks_class']}**")
        md.append("")
        md.append(ev["reason"])
        md.append("")
        for dim, label in [
            ("market_cycle", "MARKET CYCLE"),
            ("always_in_control", "ALWAYS-IN / CONTROL"),
            ("location", "LOCATION"),
            ("opposing_evidence", "OPPOSING EVIDENCE"),
            ("traders_equation", "TRADER'S EQUATION"),
            ("setup", "SETUP"),
            ("confirmation", "CONFIRMATION"),
            ("trade_worthiness", "TRADE-WORTHINESS"),
        ]:
            val = (ev["dimensions"].get(dim) or "")[:500]
            md.append(f"**{label}:** {val}")
            md.append("")
    md.append("---")
    md.append("")
    md.append("## 3. Missed Opportunity Review")
    md.append("")
    for m in report["missed_opportunity_review"]:
        md.append(f"### {m['symbol']} — {m['time_window']} ({m['classification']})")
        md.append("")
        md.append(f"- **Structure:** {m['market_structure']}")
        md.append(f"- **Cycle/control recognized:** {m['cycle_control_recognized']}")
        md.append(f"- **WATCH/ARM:** {m['watch_arm']}")
        md.append(f"- **CONSIDER_ENTRY:** {m['consider_entry']}")
        md.append(f"- **Blocker:** {m['blocker']}")
        md.append(f"- **Brooks-coherent pass:** {m['brooks_coherent_pass']}")
        md.append(f"- **Note:** {m.get('note','')}")
        md.append("")
    md.append("---")
    md.append("")
    md.append("## 4. NKE 2026-07-24 Special Review")
    md.append("")
    nke = report["nke_special_review"]
    md.append("| Bar | Action | Wake |")
    md.append("|-----|--------|------|")
    for row in nke["timeline"]:
        md.append(f"| {row['bar_et']} | {row.get('action')} | {row.get('wake')} |")
    md.append("")
    md.append("**Did the Adviser correctly recognize bull control during the morning move?**")
    md.append(nke["bull_control_morning"])
    md.append("")
    md.append("**Why were 10:55 and 11:10 not executed?**")
    md.append(nke["why_1055_1110_not_executed"])
    md.append("")
    md.append("**Were those passes Brooks-coherent given move size/location?**")
    md.append("Yes — near-session-high extension in a ~$0.60 range day; quality MARGINAL is a defensible Brooks filter.")
    md.append("")
    md.append("**Was the 14:45 trade methodologically better, worse, or simply different?**")
    md.append(nke["afternoon_vs_morning"])
    md.append("")
    md.append("**ACE-Lite continuity?**")
    md.append(nke["ace_lite_continuity_evidence"])
    md.append("")
    md.append("The −$7.59 outcome does not, by itself, indicate a defect.")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## 5. Exit / Risk Management Review")
    md.append("")
    for ex in report["exit_risk_management_review"]:
        md.append(f"- **{ex['symbol']}** ({ex['entry_et']}→{ex['exit_et']}, gross ${ex['gross_pnl']:.2f}): **{ex['management_class']}** — {ex['reason']}")
    md.append("")
    md.append("Positive prior behavior retained: failed opportunities did not become large losses.")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## 6. Reproducibility / Architecture Health")
    md.append("")
    md.append("**NONE (no engineering reopen warranted):**")
    for x in report["architecture_health"]["none"]:
        md.append(f"- {x}")
    md.append("")
    md.append("**OBSERVATION (monitor):**")
    for x in report["architecture_health"]["observation"]:
        md.append(f"- {x}")
    md.append("")
    md.append("**MATERIAL_DEFECT:** None identified from 7/24 persisted calls.")
    md.append("")
    md.append("---")
    md.append("")
    v = report["fresh_day_verdict"]
    md.append(f"## 7. Fresh-Day Verdict: **{v['verdict']}**")
    md.append("")
    md.append("| Question | Answer |")
    md.append("|----------|--------|")
    md.append(f"| Positive gross edge on 7/24? | {v['q1_positive_gross_edge']} |")
    md.append(f"| Contained failed trades? | {v['q2_contained_failed_trades']} |")
    md.append(f"| Material Brooks misses (methodology)? | {v['q3_material_missed_for_methodology']} |")
    md.append(f"| Defect serious enough to stop validation? | {v['q4_defect_stop_validation']} |")
    md.append(f"| Next fresh block without system changes? | {v['q5_next_block_without_changes']} |")
    md.append("")
    md.append(f"Machine-readable JSON: `{OUT_JSON.name}`")

    OUT_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"Wrote {OUT_MD}")
    print(f"Wrote {OUT_JSON}")


if __name__ == "__main__":
    main()
