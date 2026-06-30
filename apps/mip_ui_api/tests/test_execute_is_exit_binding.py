"""Regression: execute_live_action must bind is_exit before REAL short-selling gate."""
import inspect
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.routers import live


class TestExecuteIsExitBinding(unittest.TestCase):
    def test_is_exit_assigned_before_real_short_selling_gate(self):
        src = inspect.getsource(live.execute_live_action)
        assign_idx = src.index("is_exit = _exec_intent == \"EXIT\"")
        gate_idx = src.index("Gate 5b — REAL: short selling")
        self.assertLess(
            assign_idx,
            gate_idx,
            "is_exit must be defined before Gate 5b references it",
        )


if __name__ == "__main__":
    unittest.main()
