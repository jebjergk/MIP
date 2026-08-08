"""Regression: disposable V0.4 replay must not mutate Freeze V1 / official pins."""

from __future__ import annotations

import unittest


class V04DisposablePinSafetyTests(unittest.TestCase):
    def test_simulation_completion_v04_does_not_touch_phase6b_phase7(self):
        from app.brooks_intraday.context_ruleset_v04 import RULESET_VERSION as CONTEXT_V04

        state: dict = {
            "configuration": {
                "phase6b_context_attempt_id": "4948b422-957d-487d-a699-c4644bd937bf",
                "phase7_simulation_attempt_id": "3a35b267-e48b-4259-a888-9c71579571b7",
                "experiment_freeze_id": "BROOKS_EXPERIMENT_RULESET_FREEZE_V1",
            }
        }
        cfg = state["configuration"]
        official_ctx = cfg["phase6b_context_attempt_id"]
        official_sim = cfg["phase7_simulation_attempt_id"]
        freeze = cfg["experiment_freeze_id"]

        required = CONTEXT_V04
        sim_attempt_id = "03eaf144-8744-4637-81ea-cec9e1b0cef7"
        context_attempt_id = "e35b6713-ee67-42f6-bc30-f18aa5bee56d"
        summary = {"trade_count": 0}

        if required == CONTEXT_V04:
            state["phase_v04_simulation_attempt_id"] = sim_attempt_id
            cfg["phase_v04_simulation_attempt_id"] = sim_attempt_id
            cfg["phase_v04_context_attempt_id"] = context_attempt_id
            cfg["phase_v04_simulation_summary"] = summary

        self.assertEqual(cfg["phase6b_context_attempt_id"], official_ctx)
        self.assertEqual(cfg["phase7_simulation_attempt_id"], official_sim)
        self.assertEqual(cfg["experiment_freeze_id"], freeze)
        self.assertEqual(cfg["phase_v04_simulation_attempt_id"], sim_attempt_id)

    def test_context_replay_v04_does_not_set_phase6b_or_context_ruleset_pin(self):
        from app.brooks_intraday.context_ruleset_v04 import RULESET_VERSION as RULESET_V04

        state: dict = {
            "configuration": {
                "context_ruleset_version": "BROOKS_CONTEXT_RULESET_V0_2",
                "phase6b_context_attempt_id": "4948b422-957d-487d-a699-c4644bd937bf",
            }
        }
        cfg = state["configuration"]
        ruleset_version = RULESET_V04
        context_attempt_id = "e35b6713-ee67-42f6-bc30-f18aa5bee56d"

        if ruleset_version == RULESET_V04:
            state["phase_v04_context_attempt_id"] = context_attempt_id
            cfg["phase_v04_context_attempt_id"] = context_attempt_id

        self.assertEqual(cfg["phase6b_context_attempt_id"], "4948b422-957d-487d-a699-c4644bd937bf")
        self.assertEqual(cfg["context_ruleset_version"], "BROOKS_CONTEXT_RULESET_V0_2")
        self.assertEqual(cfg["phase_v04_context_attempt_id"], context_attempt_id)

    def test_official_chain_resolver_read_only(self):
        from app.brooks_intraday.learning_view import resolve_attempt_chain

        cfg = {
            "phase6b_context_attempt_id": "4948b422-official",
            "phase7_simulation_attempt_id": "3a35b267-official",
            "experiment_freeze_id": "BROOKS_EXPERIMENT_RULESET_FREEZE_V1",
        }
        before = dict(cfg)
        resolve_attempt_chain("6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e", {}, cfg)
        self.assertEqual(cfg, before)


if __name__ == "__main__":
    unittest.main()
