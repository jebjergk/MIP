"""Protective bracket leg (SL/TP) closeout capture and exit-path regression tests."""
from __future__ import annotations

import json
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import entry_intel_hooks as hooks


class TestProtectiveLegDetection(unittest.TestCase):
    def test_is_protective_tp_sl_suffix(self):
        self.assertTrue(hooks.is_protective_leg_order({"IDEMPOTENCY_KEY": "abc:TP"}))
        self.assertTrue(hooks.is_protective_leg_order({"IDEMPOTENCY_KEY": "abc:SL"}))
        self.assertTrue(hooks.is_protective_leg_order({"IDEMPOTENCY_KEY": "x:tp"}))
        self.assertFalse(hooks.is_protective_leg_order({"IDEMPOTENCY_KEY": "abc:ENTRY"}))
        self.assertFalse(hooks.is_protective_leg_order({"IDEMPOTENCY_KEY": ""}))
        self.assertFalse(hooks.is_protective_leg_order({}))

    def test_role_and_exit_type(self):
        self.assertEqual(hooks.get_protective_leg_role({"IDEMPOTENCY_KEY": "k:SL"}), "SL")
        self.assertEqual(hooks.get_protective_leg_role({"IDEMPOTENCY_KEY": "k:TP"}), "TP")
        self.assertEqual(hooks.protective_leg_exit_type_code({"IDEMPOTENCY_KEY": "k:SL"}), "SL")
        self.assertEqual(hooks.protective_leg_exit_type_code({"IDEMPOTENCY_KEY": "k:TP"}), "TP")


def _make_cursor(fetchone_chain: list, fetchall_chain: list) -> MagicMock:
    cur = MagicMock()
    fo = list(fetchone_chain)
    fa = list(fetchall_chain)

    def _fetchone():
        if not fo:
            raise AssertionError("fetchone underflow")
        return fo.pop(0)

    def _fetchall():
        if not fa:
            return []
        return fa.pop(0)

    cur.fetchone.side_effect = _fetchone
    cur.fetchall.side_effect = _fetchall
    return cur


class TestProtectiveLegCloseoutWrite(unittest.TestCase):
    def test_protective_sl_filled_writes_closeout_once(self):
        oid = str(uuid.uuid4())
        entry_aid = "entry-act-1"
        order_row = (
            oid,
            entry_aid,
            "idem:SL",
            "FILLED",
            "SBUX",
            "SELL",
            "STP",
            100.0,
            100.0,
            95.5,
            None,
            "broker-1",
        )
        la_row = ("ENTRY", "EXECUTED")
        link_row = ("snap-1", 42)
        resolve_la = (42, "BUY", None)
        entry_fills = [
            ("o1", 100.0, 90.0, None, "FILLED", "idem"),
        ]
        exit_fill = (100.0, 95.5, None, "FILLED")
        alpha_spec = {"recommended_action": "ENTER", "expected_value_net": 0.1}
        eis_row = (json.dumps(alpha_spec), "src1", 1)

        # fetchone order: order, la, dup, link, resolve_la, exit_agg, committee, eis
        cur = _make_cursor(
            fetchone_chain=[
                order_row,
                la_row,
                None,
                link_row,
                resolve_la,
                exit_fill,
                None,
                eis_row,
            ],
            fetchall_chain=[entry_fills],
        )
        out = hooks.maybe_write_trade_closeout_on_protective_leg_filled(cur, oid)
        self.assertEqual(out["outcome"], "written")
        self.assertEqual(out["path"], "protective_leg")
        self.assertEqual(out["exit_type"], "SL")
        insert_calls = [
            ca
            for ca in cur.execute.call_args_list
            if ca.args
            and ca.args[0].upper().strip().startswith("INSERT")
            and "TRADE_CLOSEOUT" in ca.args[0].upper()
        ]
        self.assertEqual(len(insert_calls), 1)
        params = insert_calls[0].args[1]
        self.assertEqual(params[1], entry_aid)
        self.assertIsNone(params[5])

    def test_protective_not_filled_skips(self):
        cur = _make_cursor(
            fetchone_chain=[
                ("oid", "a1", "x:TP", "PARTIAL_FILL", "S", "SELL", "LMT", 100.0, 50.0, 10.0, None, None),
            ],
            fetchall_chain=[],
        )
        out = hooks.maybe_write_trade_closeout_on_protective_leg_filled(cur, "oid")
        self.assertEqual(out["outcome"], "skipped")
        self.assertEqual(out["reason"], "order_not_filled")

    def test_duplicate_closeout_second_call(self):
        oid = str(uuid.uuid4())
        entry_aid = "e1"
        order_row = (oid, entry_aid, "i:TP", "FILLED", "S", "SELL", "LMT", 10.0, 10.0, 20.0, None, None)
        cur = _make_cursor(
            fetchone_chain=[
                order_row,
                ("ENTRY", "EXECUTED"),
                (1,),
            ],
            fetchall_chain=[],
        )
        out = hooks.maybe_write_trade_closeout_on_protective_leg_filled(cur, oid)
        self.assertEqual(out["outcome"], "skipped")
        self.assertEqual(out["reason"], "duplicate_closeout")

    def test_incomplete_fill_qty_skips(self):
        cur = _make_cursor(
            fetchone_chain=[
                ("oid", "a1", "i:SL", "FILLED", "S", "BUY", "STP", 100.0, 40.0, 50.0, None, None),
            ],
            fetchall_chain=[],
        )
        out = hooks.maybe_write_trade_closeout_on_protective_leg_filled(cur, "oid")
        self.assertEqual(out["outcome"], "skipped")
        self.assertEqual(out["reason"], "incomplete_fill")


class TestExitActionCloseoutPath(unittest.TestCase):
    def test_exit_intent_skipped_when_not_exit(self):
        cur = _make_cursor(
            fetchone_chain=[(1, "SBUX", "ENTRY", None, None)],
            fetchall_chain=[],
        )
        out = hooks.maybe_write_trade_closeout_on_exit_filled(cur, "exit-1")
        self.assertEqual(out["outcome"], "skipped")
        self.assertEqual(out["reason"], "action_intent_not_exit")

    def test_exit_filled_writes_closeout(self):
        exit_aid = "exit-x"
        entry_aid = "entry-x"
        cur = _make_cursor(
            fetchone_chain=[
                (1, "SBUX", "EXIT", "MANUAL", None),
                (entry_aid, "snap-z", 9),
                None,
                ("snap-z", 9),
                (9, "BUY", None),
                None,
                (json.dumps({"recommended_action": "ENTER"}), "s", 1),
            ],
            fetchall_chain=[
                [("eo1", 10.0, 100.0, None, "FILLED", "plain-key")],
                [("xo1", 10.0, 101.0, None, "FILLED")],
            ],
        )
        out = hooks.maybe_write_trade_closeout_on_exit_filled(cur, exit_aid)
        self.assertEqual(out["outcome"], "written")
        self.assertEqual(out["path"], "exit_action")
        insert_calls = [
            ca
            for ca in cur.execute.call_args_list
            if ca.args
            and ca.args[0].upper().strip().startswith("INSERT")
            and "TRADE_CLOSEOUT" in ca.args[0].upper()
        ]
        self.assertEqual(len(insert_calls), 1)
        p = insert_calls[0].args[1]
        self.assertEqual(p[1], entry_aid)
        self.assertEqual(p[5], exit_aid)


if __name__ == "__main__":
    unittest.main()
