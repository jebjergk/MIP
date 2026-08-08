"""Regression tests for ARM_LONG setup confirmation lifecycle (POC 5+ boolean logic)."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date

from app.brooks_intraday.adviser_confirmation_logic_v01 import (
    logic_all_predicate_leaves,
    parse_confirmation_logic,
    prepare_confirmation_logic_for_freeze,
    validate_logic_prose_consistency,
    wrap_predicates_as_logic,
)
from app.brooks_intraday.adviser_setup_contract_v01 import (
    SetupContract,
    all_mandatory_satisfied,
    any_mandatory_expired_or_failed,
    entry_allowed_consider_entry,
    evaluate_confirmation_complete_wake,
    freeze_setup_contract,
    freeze_setup_contract_from_predicates,
    infer_validity_window,
    logic_node_satisfied,
    mark_confirmation_complete_fired,
    process_adviser_setup_response,
    tick_mandatory_confirmations,
)
from app.brooks_intraday.adviser_wake_v01 import WAKE_CONFIRMATION_COMPLETE
from app.brooks_intraday.historical_bar_repository import load_bars_from_store
from app.brooks_intraday.objective_ruleset_v01 import compute_geometry


@dataclass
class _Bar:
    open: float
    high: float
    low: float
    close: float
    volume: float = 1.0


def _g(bar: _Bar):
    return compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)


def _leaves(contract):
    return logic_all_predicate_leaves(contract.confirmation_logic)


class SetupContractTests(unittest.TestCase):
    def test_next_bar_window_is_single_bar(self):
        pred = {"type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL", "level": 249.11}
        vf, vu = infer_validity_window(pred, signal_bar_index=10)
        self.assertEqual((vf, vu), (11, 11))

    def test_immediate_follow_through_fails_on_249_03(self):
        contract = freeze_setup_contract_from_predicates(
            mandatory_preds=[
                {
                    "type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
                    "level": 249.11,
                    "valid_from_bar_offset": 1,
                    "valid_until_bar_offset": 1,
                }
            ],
            optional_text=[],
            signal_bar_index=10,
            signal_bar_et="11:00",
        )
        fail_bar = _Bar(249.1, 249.35, 249.0, 249.03)
        tick_mandatory_confirmations(
            contract,
            bar_index=11,
            bar=fail_bar,
            recent=[fail_bar],
            prev_bar=_Bar(248.5, 249.31, 248.4, 249.11),
            prev_geom=None,
            g=_g(fail_bar),
        )
        self.assertEqual(_leaves(contract)[0].status, "failed")
        self.assertFalse(all_mandatory_satisfied(contract))

    def test_follow_through_satisfies_on_close_above(self):
        contract = freeze_setup_contract_from_predicates(
            mandatory_preds=[
                {
                    "type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
                    "level": 249.11,
                    "valid_from_bar_offset": 1,
                    "valid_until_bar_offset": 1,
                }
            ],
            optional_text=[],
            signal_bar_index=10,
            signal_bar_et="11:00",
        )
        ok_bar = _Bar(249.0, 249.6, 248.95, 249.52)
        tick_mandatory_confirmations(
            contract,
            bar_index=11,
            bar=ok_bar,
            recent=[ok_bar],
            prev_bar=_Bar(248.5, 249.31, 248.4, 249.11),
            prev_geom=None,
            g=_g(ok_bar),
        )
        self.assertEqual(_leaves(contract)[0].status, "satisfied")
        self.assertTrue(all_mandatory_satisfied(contract))

    def test_expired_next_bar_cannot_stay_pending_forever(self):
        contract = freeze_setup_contract_from_predicates(
            mandatory_preds=[
                {
                    "type": "LEVEL_BREAK",
                    "level": 249.11,
                    "direction": "ABOVE",
                    "valid_from_bar_offset": 1,
                    "valid_until_bar_offset": 1,
                }
            ],
            optional_text=[],
            signal_bar_index=10,
            signal_bar_et="11:00",
        )
        b = _Bar(248.0, 248.5, 247.8, 248.2)
        tick_mandatory_confirmations(contract, bar_index=12, bar=b, recent=[b], prev_bar=b, prev_geom=None, g=_g(b))
        self.assertIn(_leaves(contract)[0].status, ("expired", "failed"))

    def test_mandatory_persists_across_adviser_calls(self):
        c = freeze_setup_contract_from_predicates(
            mandatory_preds=[{"type": "LEVEL_BREAK", "level": 250.0, "direction": "ABOVE", "valid_window_bars": 8}],
            optional_text=[],
            signal_bar_index=5,
            signal_bar_et="10:00",
        )
        sid = c.setup_id
        c2, events = process_adviser_setup_response(
            c,
            prior_action="ARM_LONG",
            new_action="ARM_LONG",
            wake_reason="THESIS_WATCH_CONDITION_MET",
            mandatory_preds_from_llm=[{"type": "LEVEL_BREAK", "level": 251.0, "direction": "ABOVE"}],
            confirmation_logic_raw=None,
            optional_text=["new prose thesis wording only"],
            confirmation_extension_reason=None,
            signal_bar_index=8,
            signal_bar_et="10:15",
            thesis_invalidated=False,
            position_changed=False,
            position_qty_before=0,
            position_qty_after=0,
        )
        self.assertIsNotNone(c2)
        assert c2 is not None
        self.assertEqual(c2.setup_id, sid)
        self.assertEqual(_leaves(c2)[0].predicate["level"], 250.0)
        self.assertTrue(any(e["type"] == "MANDATORY_CONFIRMATION_HELD" for e in events))

    def test_setup_id_stable_when_family_unchanged(self):
        preds = [{"type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL", "level": 249.11}]
        c1 = freeze_setup_contract_from_predicates(
            mandatory_preds=preds, optional_text=[], signal_bar_index=1, signal_bar_et="09:35"
        )
        c2 = freeze_setup_contract_from_predicates(
            mandatory_preds=preds, optional_text=[], signal_bar_index=9, signal_bar_et="10:05", existing=c1
        )
        self.assertEqual(c1.setup_family, c2.setup_family)
        self.assertEqual(c1.setup_id, c2.setup_id)

    def test_extension_requires_reason_after_confirmation_complete(self):
        c = freeze_setup_contract_from_predicates(
            mandatory_preds=[{"type": "LEVEL_BREAK", "level": 249.11, "direction": "ABOVE"}],
            optional_text=[],
            signal_bar_index=1,
            signal_bar_et="09:35",
        )
        _leaves(c)[0].status = "satisfied"
        _, events = process_adviser_setup_response(
            c,
            prior_action="ARM_LONG",
            new_action="ARM_LONG",
            wake_reason=WAKE_CONFIRMATION_COMPLETE,
            mandatory_preds_from_llm=[{"type": "LEVEL_BREAK", "level": 250.0, "direction": "ABOVE"}],
            confirmation_logic_raw=None,
            optional_text=[],
            confirmation_extension_reason=None,
            signal_bar_index=2,
            signal_bar_et="09:40",
            thesis_invalidated=False,
            position_changed=False,
            position_qty_before=0,
            position_qty_after=0,
        )
        self.assertTrue(any(e["type"] == "CONFIRMATION_LADDER_REJECTED" for e in events))

    def test_extension_with_reason_logs_ladder_extended(self):
        c = freeze_setup_contract_from_predicates(
            mandatory_preds=[{"type": "LEVEL_BREAK", "level": 249.11, "direction": "ABOVE"}],
            optional_text=[],
            signal_bar_index=1,
            signal_bar_et="09:35",
        )
        _leaves(c)[0].status = "satisfied"
        c2, events = process_adviser_setup_response(
            c,
            prior_action="ARM_LONG",
            new_action="ARM_LONG",
            wake_reason=WAKE_CONFIRMATION_COMPLETE,
            mandatory_preds_from_llm=[{"type": "LEVEL_BREAK", "level": 250.0, "direction": "ABOVE"}],
            confirmation_logic_raw=None,
            optional_text=[],
            confirmation_extension_reason="Failed bear follow-through changed support level.",
            signal_bar_index=2,
            signal_bar_et="09:40",
            thesis_invalidated=False,
            position_changed=False,
            position_qty_before=0,
            position_qty_after=0,
        )
        self.assertTrue(any(e["type"] == "CONFIRMATION_LADDER_EXTENDED" for e in events))
        self.assertIsNotNone(c2)

    def test_invalidation_resets_setup(self):
        c = freeze_setup_contract_from_predicates(
            mandatory_preds=[{"type": "LEVEL_BREAK", "level": 249.11, "direction": "ABOVE"}],
            optional_text=[],
            signal_bar_index=1,
            signal_bar_et="09:35",
        )
        c2, events = process_adviser_setup_response(
            c,
            prior_action="ARM_LONG",
            new_action="WATCH_LONG",
            wake_reason="THESIS_INVALIDATED",
            mandatory_preds_from_llm=[],
            confirmation_logic_raw=None,
            optional_text=[],
            confirmation_extension_reason=None,
            signal_bar_index=2,
            signal_bar_et="09:40",
            thesis_invalidated=True,
            position_changed=False,
            position_qty_before=0,
            position_qty_after=0,
        )
        self.assertIsNone(c2)
        self.assertTrue(any(e["type"] == "SETUP_RESET" for e in events))

    def test_confirmation_complete_wake_when_satisfied(self):
        contract = freeze_setup_contract_from_predicates(
            mandatory_preds=[
                {
                    "type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
                    "level": 249.11,
                    "valid_from_bar_offset": 1,
                    "valid_until_bar_offset": 1,
                }
            ],
            optional_text=[],
            signal_bar_index=10,
            signal_bar_et="11:00",
        )
        _leaves(contract)[0].status = "satisfied"
        _leaves(contract)[0].satisfied_at_bar = 11
        bar = _Bar(249.0, 249.6, 248.95, 249.52)
        self.assertTrue(
            evaluate_confirmation_complete_wake(
                contract,
                bar_index=11,
                bar=bar,
                recent=[bar],
                prev_bar=bar,
                prev_geom=None,
                g=_g(bar),
                last_action="ARM_LONG",
            )
        )

    def test_consider_entry_requires_adviser_and_frozen_contract(self):
        contract = freeze_setup_contract_from_predicates(
            mandatory_preds=[
                {
                    "type": "LEVEL_BREAK",
                    "level": 10.0,
                    "direction": "ABOVE",
                    "valid_from_bar_offset": 0,
                    "valid_until_bar_offset": 5,
                }
            ],
            optional_text=[],
            signal_bar_index=0,
            signal_bar_et="09:30",
        )
        bar = _Bar(9.5, 10.2, 9.4, 10.05)
        _leaves(contract)[0].status = "satisfied"
        allowed, reason = entry_allowed_consider_entry(
            "CONSIDER_ENTRY",
            contract,
            bar=bar,
            bar_index=1,
            recent=[bar],
            prev_bar=None,
            prev_geom=None,
            g=_g(bar),
        )
        self.assertTrue(allowed)
        self.assertEqual(reason, "frozen_confirmations_satisfied")

    def test_no_confirmation_no_entry(self):
        allowed, reason = entry_allowed_consider_entry(
            "CONSIDER_ENTRY",
            None,
            bar=_Bar(10, 10, 10, 10),
            bar_index=1,
            recent=[],
            prev_bar=None,
            prev_geom=None,
            g=_g(_Bar(10, 10, 10, 10)),
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "no_frozen_setup_contract")

    def test_arm_long_not_auto_entry(self):
        contract = freeze_setup_contract_from_predicates(
            mandatory_preds=[{"type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"}],
            optional_text=[],
            signal_bar_index=0,
            signal_bar_et="09:30",
        )
        _leaves(contract)[0].status = "satisfied"
        allowed, _ = entry_allowed_consider_entry(
            "ARM_LONG",
            contract,
            bar=_Bar(9.5, 10.2, 9.4, 10.05),
            bar_index=1,
            recent=[],
            prev_bar=None,
            prev_geom=None,
            g=_g(_Bar(9.5, 10.2, 9.4, 10.05)),
        )
        self.assertFalse(allowed)


class BooleanConfirmationTests(unittest.TestCase):
    def test_any_of_completes_when_one_branch_succeeds(self):
        logic = parse_confirmation_logic(
            {
                "type": "ANY_OF",
                "children": [
                    {"type": "PREDICATE", "predicate_type": "LEVEL_BREAK", "level": 999.0, "direction": "ABOVE"},
                    {"type": "PREDICATE", "predicate_type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"},
                ],
            }
        )
        c = freeze_setup_contract(
            confirmation_logic=logic,
            optional_text=[],
            signal_bar_index=0,
            signal_bar_et="09:30",
        )
        _leaves(c)[1].status = "satisfied"
        self.assertTrue(all_mandatory_satisfied(c))

    def test_failed_sibling_does_not_kill_any_of(self):
        logic = parse_confirmation_logic(
            {
                "type": "ANY_OF",
                "children": [
                    {"type": "PREDICATE", "predicate_type": "LEVEL_BREAK", "level": 1.0, "direction": "ABOVE"},
                    {"type": "PREDICATE", "predicate_type": "LEVEL_BREAK", "level": 2.0, "direction": "ABOVE"},
                ],
            }
        )
        c = freeze_setup_contract(
            confirmation_logic=logic, optional_text=[], signal_bar_index=0, signal_bar_et="09:30"
        )
        leaves = _leaves(c)
        leaves[0].status = "failed"
        leaves[1].status = "satisfied"
        self.assertTrue(all_mandatory_satisfied(c))
        self.assertFalse(any_mandatory_expired_or_failed(c))

    def test_all_of_requires_all_children(self):
        logic = parse_confirmation_logic(
            {
                "type": "ALL_OF",
                "children": [
                    {"type": "PREDICATE", "predicate_type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"},
                    {"type": "PREDICATE", "predicate_type": "LEVEL_BREAK", "level": 11.0, "direction": "ABOVE"},
                ],
            }
        )
        c = freeze_setup_contract(
            confirmation_logic=logic, optional_text=[], signal_bar_index=0, signal_bar_et="09:30"
        )
        _leaves(c)[0].status = "satisfied"
        self.assertFalse(all_mandatory_satisfied(c))
        _leaves(c)[1].status = "satisfied"
        self.assertTrue(all_mandatory_satisfied(c))

    def test_nested_all_of_any_of(self):
        logic = parse_confirmation_logic(
            {
                "type": "ALL_OF",
                "children": [
                    {"type": "PREDICATE", "predicate_type": "SIGNAL_BAR_CONFIRMED", "level": 246.68},
                    {
                        "type": "ANY_OF",
                        "children": [
                            {
                                "type": "PREDICATE",
                                "predicate_type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
                                "level": 246.68,
                            },
                            {
                                "type": "PREDICATE",
                                "predicate_type": "FAILED_BEAR_BREAKOUT_CONFIRMED",
                                "level": 245.66,
                            },
                        ],
                    },
                ],
            }
        )
        c = freeze_setup_contract(
            confirmation_logic=logic, optional_text=[], signal_bar_index=4, signal_bar_et="09:50"
        )
        leaves = {l.predicate["type"]: l for l in _leaves(c)}
        leaves["SIGNAL_BAR_CONFIRMED"].status = "satisfied"
        leaves["FAILED_BEAR_BREAKOUT_CONFIRMED"].status = "failed"
        leaves["BULL_FOLLOW_THROUGH_AFTER_SIGNAL"].status = "satisfied"
        self.assertTrue(all_mandatory_satisfied(c))

    def test_expiry_inside_any_of_does_not_fail_whole(self):
        logic = parse_confirmation_logic(
            {
                "type": "ANY_OF",
                "children": [
                    {
                        "type": "PREDICATE",
                        "predicate_type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
                        "level": 246.68,
                        "valid_from_bar_offset": 1,
                        "valid_until_bar_offset": 1,
                    },
                    {
                        "type": "PREDICATE",
                        "predicate_type": "SIGNAL_BAR_CONFIRMED",
                        "level": 246.68,
                        "valid_from_bar_offset": 0,
                        "valid_until_bar_offset": 2,
                    },
                ],
            }
        )
        c = freeze_setup_contract(
            confirmation_logic=logic, optional_text=[], signal_bar_index=4, signal_bar_et="09:50"
        )
        leaves = _leaves(c)
        leaves[0].status = "failed"
        leaves[1].status = "satisfied"
        self.assertTrue(all_mandatory_satisfied(c))

    def test_expiry_inside_all_of_fails_whole(self):
        logic = parse_confirmation_logic(
            {
                "type": "ALL_OF",
                "children": [
                    {"type": "PREDICATE", "predicate_type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"},
                    {"type": "PREDICATE", "predicate_type": "LEVEL_BREAK", "level": 11.0, "direction": "ABOVE"},
                ],
            }
        )
        c = freeze_setup_contract(
            confirmation_logic=logic, optional_text=[], signal_bar_index=0, signal_bar_et="09:30"
        )
        _leaves(c)[0].status = "failed"
        _leaves(c)[1].status = "satisfied"
        self.assertFalse(all_mandatory_satisfied(c))
        self.assertTrue(any_mandatory_expired_or_failed(c))

    def test_confirmation_complete_fires_once(self):
        contract = freeze_setup_contract_from_predicates(
            mandatory_preds=[{"type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"}],
            optional_text=[],
            signal_bar_index=0,
            signal_bar_et="09:30",
        )
        _leaves(contract)[0].status = "satisfied"
        bar = _Bar(9.5, 10.2, 9.4, 10.05)
        self.assertTrue(
            evaluate_confirmation_complete_wake(
                contract,
                bar_index=1,
                bar=bar,
                recent=[bar],
                prev_bar=bar,
                prev_geom=None,
                g=_g(bar),
                last_action="ARM_LONG",
            )
        )
        mark_confirmation_complete_fired(contract)
        self.assertFalse(
            evaluate_confirmation_complete_wake(
                contract,
                bar_index=2,
                bar=bar,
                recent=[bar],
                prev_bar=bar,
                prev_geom=None,
                g=_g(bar),
                last_action="ARM_LONG",
            )
        )

    def test_boolean_tree_survives_setup_freeze(self):
        raw = wrap_predicates_as_logic(
            [{"type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"}], combinator="ANY_OF"
        )
        root = parse_confirmation_logic(raw)
        c = freeze_setup_contract(
            confirmation_logic=root, optional_text=[], signal_bar_index=0, signal_bar_et="09:30"
        )
        restored = SetupContract.from_dict(c.to_dict())
        self.assertEqual(
            restored.confirmation_logic.to_dict()["type"],
            "ANY_OF",
        )

    def test_boolean_tree_cannot_silently_change_on_rearm(self):
        c = freeze_setup_contract_from_predicates(
            mandatory_preds=[{"type": "LEVEL_BREAK", "level": 10.0, "direction": "ABOVE"}],
            optional_text=[],
            signal_bar_index=0,
            signal_bar_et="09:30",
        )
        fp = c.setup_family
        c2, events = process_adviser_setup_response(
            c,
            prior_action="ARM_LONG",
            new_action="ARM_LONG",
            wake_reason="SAFETY_REFRESH",
            mandatory_preds_from_llm=[{"type": "LEVEL_BREAK", "level": 99.0, "direction": "ABOVE"}],
            confirmation_logic_raw=wrap_predicates_as_logic(
                [{"type": "LEVEL_BREAK", "level": 99.0, "direction": "ABOVE"}], combinator="ALL_OF"
            ),
            optional_text=[],
            confirmation_extension_reason=None,
            signal_bar_index=3,
            signal_bar_et="09:45",
            thesis_invalidated=False,
            position_changed=False,
        )
        assert c2 is not None
        self.assertEqual(c2.setup_family, fp)
        self.assertTrue(any(e["type"] == "MANDATORY_CONFIRMATION_HELD" for e in events))

    def test_or_prose_cannot_become_all_of(self):
        root = parse_confirmation_logic(
            wrap_predicates_as_logic(
                [
                    {"type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL", "level": 1.0},
                    {"type": "SIGNAL_BAR_CONFIRMED", "level": 1.0},
                ],
                combinator="ALL_OF",
            )
        )
        root, events = validate_logic_prose_consistency(
            root,
            confirmation_conditions=["Path A", "OR path B"],
            reasoning_summary="one of two confirmations",
        )
        self.assertEqual(root.node_type, "ANY_OF")
        self.assertTrue(any(e["type"] == "CONFIRMATION_LOGIC_REPAIRED" for e in events))

    def test_and_prose_cannot_become_any_of(self):
        root = parse_confirmation_logic(
            wrap_predicates_as_logic(
                [
                    {"type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL", "level": 1.0},
                    {"type": "SIGNAL_BAR_CONFIRMED", "level": 1.0},
                ],
                combinator="ANY_OF",
            )
        )
        root, events = validate_logic_prose_consistency(
            root,
            confirmation_conditions=["Both must confirm", "all of the following"],
            reasoning_summary="",
        )
        self.assertEqual(root.node_type, "ALL_OF")
        self.assertTrue(any(e["type"] == "CONFIRMATION_LOGIC_REPAIRED" for e in events))

    def test_poc5_0950_any_of_true_at_0955(self):
        """POC 5 09:50 ARM_LONG: ANY_OF three paths; logic true at 09:55 on stored AMZN bars."""
        logic_raw = {
            "type": "ANY_OF",
            "children": [
                {
                    "type": "PREDICATE",
                    "predicate_type": "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
                    "level": 246.68,
                    "valid_from_bar_offset": 1,
                    "valid_until_bar_offset": 2,
                },
                {
                    "type": "PREDICATE",
                    "predicate_type": "FAILED_BEAR_BREAKOUT_CONFIRMED",
                    "level": 245.66,
                    "valid_from_bar_offset": 1,
                    "valid_until_bar_offset": 3,
                },
                {
                    "type": "PREDICATE",
                    "predicate_type": "SIGNAL_BAR_CONFIRMED",
                    "level": 246.68,
                    "valid_from_bar_offset": 0,
                    "valid_until_bar_offset": 1,
                },
            ],
        }
        root, _ = prepare_confirmation_logic_for_freeze(
            confirmation_logic=logic_raw,
            confirmation_predicates=[],
            confirmation_conditions=[
                "Bull follow-through above 246.68",
                "OR failed bear breakout",
                "OR signal bar",
            ],
            reasoning_summary="one of three confirmations",
        )
        contract = freeze_setup_contract(
            confirmation_logic=root,
            optional_text=[],
            signal_bar_index=4,
            signal_bar_et="09:50",
        )
        bars = [b for b in load_bars_from_store("AMZN", date(2026, 7, 13)) if b.rth]
        first_true_idx = None
        wake_at = None
        for i in range(4, len(bars)):
            bar = bars[i]
            recent = bars[max(0, i - 19) : i + 1]
            prev = bars[i - 1] if i > 0 else None
            pg = (
                compute_geometry(open_=prev.open, high=prev.high, low=prev.low, close=prev.close, volume=prev.volume)
                if prev
                else None
            )
            prev_geom = (
                {"direction": pg.direction, "range": pg.total_range, "high": prev.high, "low": prev.low}
                if pg and prev
                else None
            )
            g = compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)
            tick_mandatory_confirmations(
                contract,
                bar_index=i,
                bar=bar,
                recent=recent,
                prev_bar=prev,
                prev_geom=prev_geom,
                g=g,
            )
            if all_mandatory_satisfied(contract) and first_true_idx is None:
                first_true_idx = i
            if evaluate_confirmation_complete_wake(
                contract,
                bar_index=i,
                bar=bar,
                recent=recent,
                prev_bar=prev,
                prev_geom=prev_geom,
                g=g,
                last_action="ARM_LONG",
            ):
                wake_at = i
                break
        self.assertIsNotNone(first_true_idx)
        assert first_true_idx is not None
        self.assertEqual(bars[first_true_idx].ts_ny.strftime("%H:%M"), "09:55")
        self.assertIsNotNone(wake_at)
        assert wake_at is not None
        self.assertEqual(wake_at, first_true_idx)


if __name__ == "__main__":
    unittest.main()
