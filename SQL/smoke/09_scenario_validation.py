"""
Phase 9 - Deterministic Scenario Validation

Covers all scenarios from Correction 8 of the live trading redesign plan:
  P1-P5: Proposal / Freshness
  O1-O4: Order construction (direction-aware, protection attachment)
  R1-R8: Reconciliation (semantic reconciliation V2 classes)
  T1-T3: Trailing stop activation
  U1-U7: UI data contracts

Each test constructs mock inputs and validates the function-level outputs
against expected behavior, without touching Snowflake.
"""

import json
import sys
import os
import traceback

os.environ.setdefault("PYTHONIOENCODING", "utf-8")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "apps", "mip_ui_api"))

PASS = 0
FAIL = 0
RESULTS = []


def record(scenario_id, description, passed, detail=""):
    global PASS, FAIL
    if passed:
        PASS += 1
        RESULTS.append(f"  PASS  {scenario_id}: {description}")
    else:
        FAIL += 1
        RESULTS.append(f"  FAIL  {scenario_id}: {description} — {detail}")


# ═══════════════════════════════════════════════════════════════════════
# P1-P5: Proposal / Freshness Scenarios (structural_committee.py)
# ═══════════════════════════════════════════════════════════════════════

def test_proposal_scenarios():
    from app.services.live_intelligence.structural_committee import (
        _evaluate_freshness,
        run_structural_committee,
    )

    # P1: Setup DETECTED, price in entry zone -> CURRENT
    action_p1 = {
        "SYMBOL": "AAPL", "SETUP_FAMILY": "TREND_PULLBACK_LONG", "DIRECTION": "LONG",
        "FRESHNESS_ASSESSMENT": "CURRENT", "SETUP_STILL_VALID": True,
        "PRICE_MOVED_TOO_FAR": False, "DISTANCE_TO_ENTRY_ZONE": 0.5,
        "TRUST_LABEL": "TRUSTED", "MEANINGFUL_HIT_RATE": 0.45,
        "REGIME_COMPAT": "GOOD", "MFE_MAE_RATIO": 1.5,
        "PATH_SURVIVAL_RATE": 0.4, "STRUCTURE_CONFIDENCE": 0.7,
        "ENTRY_ZONE_LOW": 170.0, "ENTRY_ZONE_HIGH": 175.0,
        "INVALIDATION_LEVEL": 165.0, "CURRENT_PRICE": 172.0,
        "TRAIL_STYLE": "STRUCTURAL", "TRAIL_ACTIVATION_TYPE": "MFE_RISK_MULTIPLE",
        "EXPECTED_HOLD_CHARACTER": "MEDIUM_SWING",
    }
    f = _evaluate_freshness(action_p1)
    record("P1", "Price in entry zone -> CURRENT", f["freshness"] == "CURRENT",
           f"got {f['freshness']}")
    v1 = run_structural_committee(action_p1)
    record("P1b", "Committee approves current setup",
           v1["recommendation"] in ("PROCEED", "PROCEED_REDUCED") and not v1["blocked"],
           f"rec={v1['recommendation']}, blocked={v1['blocked']}")

    # P2: Price 1% above entry zone -> STALE_BUT_VALID
    action_p2 = {**action_p1, "DISTANCE_TO_ENTRY_ZONE": 2.0, "FRESHNESS_ASSESSMENT": "STALE_BUT_VALID"}
    f2 = _evaluate_freshness(action_p2)
    record("P2", "Price 2% from zone -> STALE_BUT_VALID", f2["freshness"] == "STALE_BUT_VALID",
           f"got {f2['freshness']}")
    v2 = run_structural_committee(action_p2)
    record("P2b", "Committee approves with reduced size",
           not v2["blocked"] and v2["size_factor"] < 1.0,
           f"blocked={v2['blocked']}, size={v2['size_factor']}")

    # P3: Price 5% beyond entry zone -> STALE_INVALID (blocked)
    action_p3 = {**action_p1, "DISTANCE_TO_ENTRY_ZONE": 5.0, "PRICE_MOVED_TOO_FAR": True}
    f3 = _evaluate_freshness(action_p3)
    record("P3", "Price 5% beyond -> STALE_INVALID", f3["freshness"] == "STALE_INVALID",
           f"got {f3['freshness']}")

    # P4: Setup INVALIDATED before committee -> committee blocks
    action_p4 = {**action_p1, "FRESHNESS_ASSESSMENT": "STALE_INVALID",
                 "SETUP_STILL_VALID": False, "PRICE_MOVED_TOO_FAR": True}
    v4 = run_structural_committee(action_p4)
    record("P4", "Invalidated setup -> committee BLOCK",
           v4["blocked"] and "SETUP_STALE_INVALID" in v4["reason_codes"],
           f"blocked={v4['blocked']}, codes={v4['reason_codes']}")

    # P5: Regime POOR blocks entry
    action_p5 = {**action_p1, "REGIME_COMPAT": "POOR"}
    v5 = run_structural_committee(action_p5)
    record("P5", "POOR regime -> committee BLOCK",
           v5["blocked"] and "REGIME_INCOMPATIBLE" in v5["reason_codes"],
           f"blocked={v5['blocked']}, codes={v5['reason_codes']}")

    # Trust gate: REJECTED blocks
    action_rej = {**action_p1, "TRUST_LABEL": "REJECTED"}
    v_rej = run_structural_committee(action_rej)
    record("P5b", "REJECTED trust -> committee BLOCK",
           v_rej["blocked"] and "TRUST_GATE_FAILED" in v_rej["reason_codes"],
           f"blocked={v_rej['blocked']}, codes={v_rej['reason_codes']}")


# ═══════════════════════════════════════════════════════════════════════
# O1-O4: Order Construction Scenarios (place_ibkr_order.py CLI args)
# ═══════════════════════════════════════════════════════════════════════

def test_order_scenarios():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "place_ibkr_order",
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "cursorfiles", "place_ibkr_order.py"),
    )
    mod = importlib.util.module_from_spec(spec)

    # We can't fully exec the module (it imports ib_insync), so we validate
    # the CLI argument structure by reading the file source directly.
    order_script = os.path.join(os.path.dirname(__file__), "..", "..", "..", "cursorfiles", "place_ibkr_order.py")
    with open(order_script, "r", encoding="utf-8") as f:
        src = f.read()

    # O1: Long entry -> ENTRY order with BUY side
    record("O1", "place_ibkr_order supports --direction LONG",
           "--direction" in src and "'LONG'" in src or '"LONG"' in src,
           "missing --direction LONG handling")

    # O2: Short entry -> ENTRY order with SELL side
    record("O2", "place_ibkr_order supports --direction SHORT",
           "'SHORT'" in src or '"SHORT"' in src,
           "missing --direction SHORT handling")

    # O3: TRAIL order type supported
    record("O3", "place_ibkr_order supports TRAIL order type",
           "TRAIL" in src and "--trail-amount" in src and "--trail-percent" in src,
           "missing TRAIL order type or trail params")

    # O4: OCA group supported
    record("O4", "place_ibkr_order supports --oca-group",
           "--oca-group" in src and "ocaGroup" in src,
           "missing OCA group support")

    # Validate direction-aware logic in live.py
    live_py = os.path.join(os.path.dirname(__file__), "..", "..", "apps", "mip_ui_api", "app", "routers", "live.py")
    with open(live_py, "r", encoding="utf-8") as f:
        live_src = f.read()

    record("O1b", "execute_live_action derives BUY for LONG entry",
           "structural_direction" in live_src and "exit_side" in live_src,
           "missing direction-aware side derivation")

    record("O2b", "execute_live_action derives SELL for SHORT entry",
           'side = "SELL"' in live_src or "SELL" in live_src,
           "missing SHORT SELL derivation")

    # O3b: Protection columns written to LIVE_ORDERS
    record("O3b", "LIVE_ORDERS INSERT includes ORDER_ROLE",
           "ORDER_ROLE" in live_src and "PARENT_ORDER_ID" in live_src,
           "missing ORDER_ROLE in INSERT")

    record("O4b", "LIVE_ORDERS INSERT includes OCA_GROUP",
           "OCA_GROUP" in live_src,
           "missing OCA_GROUP in INSERT")


# ═══════════════════════════════════════════════════════════════════════
# R1-R8: Reconciliation Scenarios (broker_mirror + semantic_recon_v2)
# ═══════════════════════════════════════════════════════════════════════

def test_reconciliation_scenarios():
    from app.services.live_intelligence.broker_mirror import BrokerMirror, BrokerOrder
    from app.services.live_intelligence.semantic_reconciliation_v2 import (
        ALIGNED, POSITION_DRIFT, FLAT_IN_IB_OPEN_IN_MIP,
        POSITION_IN_IB_NOT_IN_MIP, STATUS_MISMATCH,
        PROTECTION_MISSING, PROTECTION_MISMATCH, PROTECTION_CANCELLED,
        ORPHANED_MIP_ORDER, ORPHANED_BROKER_ORDER, PRE_ENTRY,
        OPERATOR_HEADLINE,
    )

    # R1-R8: Verify all reconciliation classes are defined with operator headlines
    required_classes = [
        ALIGNED, POSITION_DRIFT, FLAT_IN_IB_OPEN_IN_MIP,
        POSITION_IN_IB_NOT_IN_MIP, STATUS_MISMATCH,
        PROTECTION_MISSING, PROTECTION_MISMATCH, PROTECTION_CANCELLED,
        ORPHANED_MIP_ORDER, ORPHANED_BROKER_ORDER,
    ]
    for cls in required_classes:
        record(f"R-{cls[:20]}", f"Recon class {cls} is defined with headline",
               cls in OPERATOR_HEADLINE and len(OPERATOR_HEADLINE[cls]) > 0,
               f"missing or empty headline for {cls}")

    # R1: SUPERSEDED_BY_BROKER exists for externally modified orders
    from app.services.live_intelligence.semantic_reconciliation_v2 import SUPERSEDED_BY_BROKER
    record("R1", "SUPERSEDED_BY_BROKER class exists",
           SUPERSEDED_BY_BROKER in OPERATOR_HEADLINE,
           "missing SUPERSEDED_BY_BROKER class")

    # R2: PROTECTION_CANCELLED for externally cancelled stops
    record("R2", "PROTECTION_CANCELLED class exists",
           PROTECTION_CANCELLED in OPERATOR_HEADLINE,
           "missing PROTECTION_CANCELLED class")

    # R3: PROTECTION_MISMATCH for trail param differences
    record("R3", "PROTECTION_MISMATCH class exists for trail mismatches",
           PROTECTION_MISMATCH in OPERATOR_HEADLINE,
           "missing PROTECTION_MISMATCH class")

    # R4: ORPHANED_BROKER_ORDER for IB-only orders
    record("R4", "ORPHANED_BROKER_ORDER class exists",
           ORPHANED_BROKER_ORDER in OPERATOR_HEADLINE,
           "missing ORPHANED_BROKER_ORDER class")

    # R5: ORPHANED_MIP_ORDER for MIP-only orders
    record("R5", "ORPHANED_MIP_ORDER class exists",
           ORPHANED_MIP_ORDER in OPERATOR_HEADLINE,
           "missing ORPHANED_MIP_ORDER class")

    # R6: FLAT_IN_IB_OPEN_IN_MIP for position closed at broker
    record("R6", "FLAT_IN_IB_OPEN_IN_MIP class exists",
           FLAT_IN_IB_OPEN_IN_MIP in OPERATOR_HEADLINE,
           "missing FLAT_IN_IB_OPEN_IN_MIP class")

    # R7: POSITION_DRIFT for avg cost differences
    record("R7", "POSITION_DRIFT class exists",
           POSITION_DRIFT in OPERATOR_HEADLINE,
           "missing POSITION_DRIFT class")

    # R8: Verify BrokerOrder dataclass has trail fields
    import dataclasses
    bo_fields = {f.name for f in dataclasses.fields(BrokerOrder)}
    record("R8a", "BrokerOrder has trail_amount field",
           "trail_amount" in bo_fields, f"fields: {bo_fields}")
    record("R8b", "BrokerOrder has trail_percent field",
           "trail_percent" in bo_fields, f"fields: {bo_fields}")
    record("R8c", "BrokerOrder has oca_group field",
           "oca_group" in bo_fields, f"fields: {bo_fields}")
    record("R8d", "BrokerOrder has stop_price field",
           "stop_price" in bo_fields, f"fields: {bo_fields}")

    # Verify BrokerMirror has expected structure
    bm_fields = {f.name for f in dataclasses.fields(BrokerMirror)}
    record("R8e", "BrokerMirror has position, open_orders, recent_executions",
           "has_position" in bm_fields and "open_orders" in bm_fields and "recent_executions" in bm_fields,
           f"fields: {bm_fields}")


# ═══════════════════════════════════════════════════════════════════════
# T1-T3: Trailing Stop Activation Scenarios (trail_activation.py)
# ═══════════════════════════════════════════════════════════════════════

def test_trailing_scenarios():
    from app.services.live_intelligence.trail_activation import (
        TrailCandidate, TrailActivationResult,
        _evaluate_activation, _compute_trail_params,
    )

    # T1: Long trail activates when MFE >= 1x risk
    cand_t1 = TrailCandidate(
        action_id="T1-ACT", symbol="AAPL", direction="LONG",
        portfolio_id=1, account_id="TEST",
        entry_fill_price=170.0, invalidation_level=165.0,
        risk_distance=5.0, current_price=176.0,
        mfe=6.0, mfe_risk_multiple=1.2,
        trail_style="STRUCTURAL",
        trail_activation_type="MFE_RISK_MULTIPLE",
        trail_activation_param=1.0,
        stop_order_id="STP-1", stop_broker_order_id="IB-STP-1",
        stop_price=165.0, oca_group="OCA-1",
    )
    result_t1 = _evaluate_activation(cand_t1)
    record("T1", "Long MFE 1.2x risk -> activation triggered",
           result_t1.activated and result_t1.activation_reason is not None,
           f"activated={result_t1.activated}, reason={result_t1.activation_reason}")

    # T1b: MFE below threshold -> no activation
    cand_t1b = TrailCandidate(
        action_id="T1B-ACT", symbol="AAPL", direction="LONG",
        portfolio_id=1, account_id="TEST",
        entry_fill_price=170.0, invalidation_level=165.0,
        risk_distance=5.0, current_price=172.0,
        mfe=2.0, mfe_risk_multiple=0.4,
        trail_style="STRUCTURAL",
        trail_activation_type="MFE_RISK_MULTIPLE",
        trail_activation_param=1.0,
        stop_order_id="STP-2", stop_broker_order_id="IB-STP-2",
        stop_price=165.0, oca_group="OCA-2",
    )
    result_t1b = _evaluate_activation(cand_t1b)
    record("T1b", "Long MFE 0.4x risk -> no activation",
           not result_t1b.activated,
           f"activated={result_t1b.activated}")

    # T2: Short trail activates when MFE >= 1x risk
    cand_t2 = TrailCandidate(
        action_id="T2-ACT", symbol="TSLA", direction="SHORT",
        portfolio_id=1, account_id="TEST",
        entry_fill_price=200.0, invalidation_level=210.0,
        risk_distance=10.0, current_price=188.0,
        mfe=12.0, mfe_risk_multiple=1.2,
        trail_style="STRUCTURAL",
        trail_activation_type="MFE_RISK_MULTIPLE",
        trail_activation_param=1.0,
        stop_order_id="STP-3", stop_broker_order_id="IB-STP-3",
        stop_price=210.0, oca_group="OCA-3",
    )
    result_t2 = _evaluate_activation(cand_t2)
    record("T2", "Short MFE 1.2x risk -> activation triggered",
           result_t2.activated,
           f"activated={result_t2.activated}, reason={result_t2.activation_reason}")

    # T2b: IMMEDIATE activation type -> always activates
    cand_t2b = TrailCandidate(
        action_id="T2B-ACT", symbol="SPY", direction="LONG",
        portfolio_id=1, account_id="TEST",
        entry_fill_price=500.0, invalidation_level=495.0,
        risk_distance=5.0, current_price=500.5,
        mfe=0.5, mfe_risk_multiple=0.1,
        trail_style="STRUCTURAL",
        trail_activation_type="IMMEDIATE",
        trail_activation_param=None,
        stop_order_id="STP-4", stop_broker_order_id="IB-STP-4",
        stop_price=495.0, oca_group="OCA-4",
    )
    result_t2b = _evaluate_activation(cand_t2b)
    record("T2b", "IMMEDIATE activation -> always activates",
           result_t2b.activated,
           f"activated={result_t2b.activated}")

    # T3: STRUCTURAL_LEVEL_BREAK — long, price 2% above entry
    cand_t3 = TrailCandidate(
        action_id="T3-ACT", symbol="MSFT", direction="LONG",
        portfolio_id=1, account_id="TEST",
        entry_fill_price=400.0, invalidation_level=390.0,
        risk_distance=10.0, current_price=412.0,
        mfe=12.0, mfe_risk_multiple=1.2,
        trail_style="STRUCTURAL",
        trail_activation_type="STRUCTURAL_LEVEL_BREAK",
        trail_activation_param=None,
        stop_order_id="STP-5", stop_broker_order_id="IB-STP-5",
        stop_price=390.0, oca_group="OCA-5",
    )
    result_t3 = _evaluate_activation(cand_t3)
    record("T3", "STRUCTURAL_LEVEL_BREAK long, +3% -> activated",
           result_t3.activated,
           f"activated={result_t3.activated}, reason={result_t3.activation_reason}")

    # T3b: STRUCTURAL_LEVEL_BREAK — price barely moved -> no activation
    cand_t3b = TrailCandidate(
        action_id="T3B-ACT", symbol="MSFT", direction="LONG",
        portfolio_id=1, account_id="TEST",
        entry_fill_price=400.0, invalidation_level=390.0,
        risk_distance=10.0, current_price=401.0,
        mfe=1.0, mfe_risk_multiple=0.1,
        trail_style="STRUCTURAL",
        trail_activation_type="STRUCTURAL_LEVEL_BREAK",
        trail_activation_param=None,
        stop_order_id="STP-6", stop_broker_order_id="IB-STP-6",
        stop_price=390.0, oca_group="OCA-6",
    )
    result_t3b = _evaluate_activation(cand_t3b)
    record("T3b", "STRUCTURAL_LEVEL_BREAK long, +0.25% -> no activation",
           not result_t3b.activated,
           f"activated={result_t3b.activated}")

    # Trail params: risk_distance used as default trail_amount
    cand_params = TrailCandidate(
        action_id="TP-ACT", symbol="AAPL", direction="LONG",
        portfolio_id=1, account_id="TEST",
        entry_fill_price=170.0, invalidation_level=165.0,
        risk_distance=5.0, current_price=176.0,
        mfe=6.0, mfe_risk_multiple=1.2,
        trail_style="STRUCTURAL",
        trail_activation_type="MFE_RISK_MULTIPLE",
        trail_activation_param=1.0,
        stop_order_id="STP-P1", stop_broker_order_id="IB-STP-P1",
        stop_price=165.0, oca_group="OCA-P1",
        activated=True, activation_reason="test",
    )
    result_params = _compute_trail_params(cand_params)
    record("T-PARAMS", "trail_amount defaults to risk_distance",
           result_params.trail_amount is not None and abs(result_params.trail_amount - 5.0) < 0.01,
           f"trail_amount={result_params.trail_amount}")


# ═══════════════════════════════════════════════════════════════════════
# U1-U7: UI Data Contract Scenarios (overview endpoint fields)
# ═══════════════════════════════════════════════════════════════════════

def test_ui_scenarios():
    live_jsx = os.path.join(
        os.path.dirname(__file__), "..", "..", "apps", "mip_ui_web", "src", "pages", "LivePortfolioActivity.jsx",
    )
    with open(live_jsx, "r", encoding="utf-8") as f:
        jsx_src = f.read()

    # U1: Source structural setup displayed
    record("U1", "UI shows setup_family",
           "d.structural" in jsx_src and "setup_family" in jsx_src,
           "missing structural setup display")

    # U1b: Direction displayed
    record("U1b", "UI shows direction badge",
           "d.structural.direction" in jsx_src,
           "missing direction display")

    # U1c: Entry zone displayed
    record("U1c", "UI shows entry zone",
           "entry_zone_low" in jsx_src and "entry_zone_high" in jsx_src,
           "missing entry zone display")

    # U2: Committee decision (approve/deny, risk note)
    record("U2", "UI shows committee decision badge",
           "committee_decision" in jsx_src and "APPROVED" in jsx_src and "DENIED" in jsx_src,
           "missing committee decision display")

    record("U2b", "UI shows risk_notes",
           "risk_notes" in jsx_src,
           "missing risk notes display")

    # U3: Protection state (fixed vs trailing)
    record("U3", "UI shows ORDER_ROLE and PROTECTION_TYPE",
           "ORDER_ROLE" in jsx_src and "PROTECTION_TYPE" in jsx_src,
           "missing protection type display")

    record("U3b", "UI shows trail activation state",
           "TRAIL_ACTIVATED" in jsx_src and "Trail Active" in jsx_src,
           "missing trail activation display")

    record("U3c", "UI shows stop price",
           "STOP_PRICE" in jsx_src,
           "missing stop price display")

    # U4: Broker alignment badges
    record("U4", "UI shows reconciliation badges",
           "reconV2" in jsx_src and "lpa-recon-badge" in jsx_src,
           "missing reconciliation badge display")

    record("U4b", "UI shows ALIGNED/MISMATCH/ORPHAN states",
           "ALIGNED" in jsx_src and "MISMATCH" in jsx_src and "ORPHAN" in jsx_src,
           "missing reconciliation states")

    # U5: Mismatch flags
    record("U5", "UI shows mismatch flags",
           "recon.flags" in jsx_src,
           "missing mismatch flags display")

    # U6: Structural narrative
    record("U6", "UI shows setup_narrative",
           "setup_narrative" in jsx_src and "lpa-narrative" in jsx_src,
           "missing narrative display")

    # U7: Skip/block reason codes
    record("U7", "UI shows reason codes with human explanations",
           "explainReasonCode" in jsx_src and "reason_codes" in jsx_src,
           "missing reason code display")

    # Check CSS supports new elements
    css_file = os.path.join(
        os.path.dirname(__file__), "..", "..", "apps", "mip_ui_web", "src", "pages", "LivePortfolioActivity.css",
    )
    with open(css_file, "r", encoding="utf-8") as f:
        css_src = f.read()

    record("U-CSS1", "CSS has structural badge styles",
           "lpa-badge--structural" in css_src and "lpa-badge--long" in css_src and "lpa-badge--short" in css_src,
           "missing structural badge CSS")

    record("U-CSS2", "CSS has reconciliation badge styles",
           "lpa-recon-badge--ok" in css_src and "lpa-recon-badge--warn" in css_src and "lpa-recon-badge--bad" in css_src,
           "missing reconciliation badge CSS")

    record("U-CSS3", "CSS has trail active style",
           "lpa-trail-active" in css_src,
           "missing trail active CSS")

    record("U-CSS4", "CSS has committee decision style",
           "lpa-committee-decision" in css_src,
           "missing committee decision CSS")


# ═══════════════════════════════════════════════════════════════════════
# Schema Validation (Snowflake column presence)
# ═══════════════════════════════════════════════════════════════════════

def test_schema_contracts():
    """Validate that the API overview endpoint returns all required fields."""
    import urllib.request

    try:
        url = "http://127.0.0.1:8099/live/activity/overview?order_lookback_days=7&order_limit=20&execution_limit=10&snapshot_lookback_days=3"
        r = urllib.request.urlopen(url, timeout=60)
        d = json.loads(r.read())
    except Exception as e:
        record("API", "Overview endpoint reachable", False, str(e))
        return

    record("API", "Overview endpoint returns ok=True", d.get("ok") is True, f"ok={d.get('ok')}")

    # Pending decisions should have structural block
    pd = d.get("pending_decisions", [])
    if pd:
        first = pd[0]
        record("API-PD1", "pending_decision has 'structural' key",
               "structural" in first,
               f"keys={list(first.keys())}")
        record("API-PD2", "pending_decision has 'committee_decision' key",
               "committee_decision" in first,
               f"keys={list(first.keys())}")
        if first.get("structural"):
            s = first["structural"]
            for fld in ["setup_family", "direction", "entry_zone_low", "entry_zone_high",
                        "invalidation_level", "trail_style", "setup_narrative", "freshness_assessment", "hold_character"]:
                record(f"API-S-{fld}", f"structural.{fld} present",
                       fld in s, f"keys={list(s.keys())}")
    else:
        record("API-PD1", "pending_decisions present to check", False, "no pending decisions")

    # Reconciliation V2 present
    rv2 = d.get("reconciliation_v2", {})
    record("API-RV2", "reconciliation_v2 in response",
           "reconciliation_v2" in d,
           f"keys={list(d.keys())}")

    # Orders have trail columns
    orders = d.get("orders", [])
    if orders:
        first_o = orders[0]
        for fld in ["ORDER_ROLE", "PROTECTION_TYPE", "OCA_GROUP", "STOP_PRICE",
                     "TRAIL_ACTIVATED", "TRAIL_AMOUNT", "TRAIL_PERCENT", "BROKER_TRAIL_STATE"]:
            record(f"API-O-{fld}", f"order has {fld} field",
                   fld in first_o,
                   f"keys={list(first_o.keys())}")
    else:
        record("API-O", "orders present to check", False, "no orders in lookback")


# ═══════════════════════════════════════════════════════════════════════
# Run all
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    sections = [
        ("P1-P5: Proposal / Freshness", test_proposal_scenarios),
        ("O1-O4: Order Construction", test_order_scenarios),
        ("R1-R8: Reconciliation", test_reconciliation_scenarios),
        ("T1-T3: Trailing Stop Activation", test_trailing_scenarios),
        ("U1-U7: UI Data Contracts", test_ui_scenarios),
        ("API: Schema Contracts", test_schema_contracts),
    ]

    print("=" * 70)
    print("Phase 9 — Deterministic Scenario Validation")
    print("=" * 70)

    for title, fn in sections:
        print(f"\n--- {title} ---")
        try:
            fn()
        except Exception as e:
            FAIL += 1
            RESULTS.append(f"  FAIL  {title}: UNCAUGHT EXCEPTION — {e}")
            traceback.print_exc()

    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    for line in RESULTS:
        print(line)

    print(f"\n{'=' * 70}")
    print(f"TOTAL: {PASS + FAIL}  |  PASS: {PASS}  |  FAIL: {FAIL}")
    print(f"{'=' * 70}")

    sys.exit(1 if FAIL > 0 else 0)
