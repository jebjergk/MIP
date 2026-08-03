"""Phase 8 — integrated learning and simulation review UI."""

from __future__ import annotations

import unittest
from datetime import datetime

from app.brooks_intraday.learning_constants import (
    OFFICIAL_ATTEMPT_CHAIN,
    PILOT_RUN_ID,
    RECONSTRUCTED_DOSSIER_BADGE,
    ZERO_TRADE_EXPLANATION,
)
from app.brooks_intraday.learning_view import (
    build_certification_summary,
    build_explanation_sections,
    build_provenance_header,
    extract_state_transitions,
    resolve_attempt_chain,
    transition_context_markers,
    _cap_grid_to_replay,
    _merge_symbol_grid,
)
from app.brooks_intraday.simulation_certification import HISTORICAL_SIMULATION


class Phase8ProvenanceTests(unittest.TestCase):
    def test_pilot_attempt_chain(self):
        chain = OFFICIAL_ATTEMPT_CHAIN[PILOT_RUN_ID]
        self.assertEqual(chain["objective_attempt_id"], "53a502f5-dec4-4bff-8106-f6637574163e")
        self.assertEqual(chain["pattern_attempt_id"], "d83d5bab-4e02-4aec-9bd7-0da54b1395d3")
        self.assertEqual(chain["context_attempt_id"], "63ecc779-3dbb-4468-8bc1-a8bbd0f17342")
        self.assertEqual(chain["simulation_attempt_id"], "4bd264d3-c061-4bee-8534-c4eb99532438")

    def test_resolve_attempts_from_official_defaults(self):
        attempts = resolve_attempt_chain(PILOT_RUN_ID, {}, {})
        self.assertEqual(attempts["simulation_attempt_id"], HISTORICAL_SIMULATION)

    def test_reconstructed_badge_in_provenance(self):
        prov = build_provenance_header(
            run_id=PILOT_RUN_ID,
            attempts=resolve_attempt_chain(PILOT_RUN_ID, {}, {}),
            cfg={},
            dossiers=[{"dossier_origin": "HISTORICAL_RECONSTRUCTION", "source_hash": "abc"}],
        )
        self.assertEqual(prov["dossier_provenance"]["reconstructed_badge"], RECONSTRUCTED_DOSSIER_BADGE)


class Phase8TransitionTests(unittest.TestCase):
    def test_transitions_not_every_bar(self):
        rows = [
            {"bar_ts": "2026-07-21T09:30:00", "symbol": "AAPL", "state_before": "A", "state_after": "OBS", "selected_action": "WAIT"},
            {"bar_ts": "2026-07-21T09:35:00", "symbol": "AAPL", "state_before": "OBS", "state_after": "OBS", "selected_action": "WAIT"},
            {"bar_ts": "2026-07-21T09:40:00", "symbol": "AAPL", "state_before": "OBS", "state_after": "SETUP", "selected_action": "WAIT"},
        ]
        tr = extract_state_transitions(rows)
        self.assertEqual(len(tr), 2)

    def test_markers_only_on_action_transition(self):
        ctx = {"payload_json": {"marker_flags_json": ["thesis_invalidated"]}}
        self.assertEqual(transition_context_markers(ctx, is_transition=False), [])
        self.assertEqual(transition_context_markers(ctx, is_transition=True), ["thesis_invalidated"])


class Phase8GridTests(unittest.TestCase):
    def test_full_week_row_count(self):
        bars = [{"ts_utc": f"2026-07-21T{h:02d}:{m:02d}:00"} for h in range(9, 16) for m in range(0, 60, 5)]
        grid = _merge_symbol_grid(
            bars=bars[:390],
            observations=[],
            context_rows=[],
            pattern_links=[],
            trades=[],
            blocked=[],
            symbol="AAPL",
        )
        self.assertEqual(len(grid), min(390, len(bars)))

    def test_replay_cap(self):
        grid = [
            {"bar_ts": "2026-07-21T09:30:00"},
            {"bar_ts": "2026-07-21T09:35:00"},
            {"bar_ts": "2026-07-21T09:40:00"},
        ]
        capped = _cap_grid_to_replay(grid, datetime.fromisoformat("2026-07-21T09:35:00"))
        self.assertEqual(len(capped), 2)


class Phase8SimulationPresentationTests(unittest.TestCase):
    def test_zero_trade_explanation(self):
        sections = build_explanation_sections(
            obs=None,
            ctx={"selected_action": "DO_NOT_ENTER"},
            patterns=[],
            simulation_effect="—",
            zero_trade_week=True,
        )
        self.assertIn("CONSIDER_ENTRY", sections["zero_trade_note"] or "")

    def test_certification_excludes_fixture_pl_from_historical(self):
        summary = build_certification_summary(include_fixtures=False)
        self.assertEqual(summary["historical_simulation_attempt_id"], HISTORICAL_SIMULATION)
        self.assertIn("fixture P/L", summary["must_not"][0])
        self.assertNotIn("fixtures", summary)

    def test_zero_trade_copy_constant(self):
        self.assertIn("$1,000", ZERO_TRADE_EXPLANATION)


if __name__ == "__main__":
    unittest.main()
