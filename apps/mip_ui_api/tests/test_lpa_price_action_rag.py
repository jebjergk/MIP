import json
import unittest
from unittest.mock import Mock, patch

from app.committee.lpa_price_action_rag import (
    MAX_RAG_CARDS,
    MAX_RAG_RETRIES,
    MAX_SINGLE_CARD_CHARS,
    MAX_TOTAL_LITERATURE_SUPPORT_CHARS,
    RagConfig,
    derive_entry_zone_state,
    derive_intraday_15m_status,
    get_literature_support,
    read_rag_config,
)
from app.committee.shadow_board import _chair_context_message, _specialist_user_message
from app.committee.shadow_types import ROLE_SLICE_MAP, build_shadow_evidence_pack
from app.committee.shadow_types import parse_chair_ruling


def _pack_slices():
    return {
        "proposal_meta": {"symbol": "TEST", "side": "LONG", "setup_family": "BREAKOUT"},
        "phase4_thesis_verdict": {"final_thesis": "Bull breakout continuation"},
        "thesis_summary": {"proposal_summary": "Long continuation"},
        "entry_zone": {"zone_low": 100, "zone_high": 102, "latest_price": 101},
        "invalidation": {"invalidation_level": 96, "invalidation_breached": False},
        "phase4_dossier_context": {"nearest_support": 99, "nearest_resistance": 108},
        "intraday_session_picture": {
            "session_available": True,
            "verdict_bucket": "SUPPORTS",
            "operator_line": "Reclaim held.",
        },
    }


def _kwargs(**overrides):
    values = {
        "session_id": "session-1",
        "hearing_id": "hearing-1",
        "proposal_id": 1,
        "symbol": "TEST",
        "side": "LONG",
        "pack_slices": _pack_slices(),
        "action_id": "action-1",
        "evidence_pack_hash": "hash-1",
        "config": RagConfig(enabled=True),
    }
    values.update(overrides)
    return values


class LpaPriceActionRagTests(unittest.TestCase):
    def test_disabled_does_not_touch_audit_or_search(self):
        search = Mock()
        with patch("app.committee.lpa_price_action_rag._load_replay") as replay:
            result = get_literature_support(
                **_kwargs(config=RagConfig(enabled=False)),
                search_fn=search,
            )
        self.assertEqual(result, {"enabled": False, "status": "DISABLED"})
        search.assert_not_called()
        replay.assert_not_called()

    def test_config_read_failure_is_disabled(self):
        with patch("app.committee.lpa_price_action_rag.get_connection", side_effect=RuntimeError("db down")):
            cfg = read_rag_config()
        self.assertFalse(cfg.enabled)
        self.assertFalse(cfg.config_available)

    def test_short_skips_before_retrieval(self):
        search = Mock()
        with patch("app.committee.lpa_price_action_rag._load_replay") as replay:
            result = get_literature_support(**_kwargs(side="SHORT"), search_fn=search)
        self.assertEqual(result["status"], "SKIPPED_SIDE_NOT_LONG")
        self.assertEqual(result["methodologist_effect"], "NO_MATERIAL_EFFECT")
        search.assert_not_called()
        replay.assert_not_called()

    @patch("app.committee.lpa_price_action_rag._finish_audit")
    @patch("app.committee.lpa_price_action_rag._acquire_guard", return_value=True)
    @patch("app.committee.lpa_price_action_rag._load_replay", return_value=None)
    def test_one_call_caps_and_sanitizes(self, _replay, _guard, finish):
        calls = []

        def search(query, top_k):
            calls.append((query, top_k))
            return {
                "status": "USED",
                "retrieval_id": "ret-1",
                "results": [
                    {
                        "card_id": f"card-{i}",
                        "title": f"Approved long card {i}",
                        "methodologist_view": "x" * 1200,
                        "relevance_reason": "Apply only when observed evidence matches.",
                        "supports": "APPROVE" if i == 0 else "MIXED",
                        "snippet": "y" * 1600,
                    }
                    for i in range(8)
                ],
            }

        result = get_literature_support(
            **_kwargs(config=RagConfig(enabled=True, max_cards=MAX_RAG_CARDS)),
            search_fn=search,
        )
        self.assertEqual(len(calls), 1)
        self.assertLessEqual(len(result["cards"]), MAX_RAG_CARDS)
        self.assertEqual(result["cost_guard"]["retries"], MAX_RAG_RETRIES)
        self.assertLessEqual(result["cost_guard"]["total_chars"], MAX_TOTAL_LITERATURE_SUPPORT_CHARS)
        for card in result["cards"]:
            self.assertLessEqual(len(card["snippet"]), MAX_SINGLE_CARD_CHARS)
        finish.assert_called_once()

    @patch("app.committee.lpa_price_action_rag._finish_audit")
    @patch("app.committee.lpa_price_action_rag._acquire_guard", return_value=True)
    @patch("app.committee.lpa_price_action_rag._load_replay", return_value=None)
    def test_failure_is_unavailable_without_retry(self, _replay, _guard, finish):
        search = Mock(side_effect=TimeoutError("timed out"))
        result = get_literature_support(**_kwargs(), search_fn=search)
        self.assertEqual(result["status"], "UNAVAILABLE")
        self.assertEqual(result["methodologist_effect"], "NO_MATERIAL_EFFECT")
        self.assertEqual(search.call_count, 1)
        self.assertEqual(result["cost_guard"]["retries"], 0)
        finish.assert_called_once()

    @patch("app.committee.lpa_price_action_rag._load_replay")
    def test_audit_replay_makes_zero_calls(self, replay):
        replay.return_value = {
            "enabled": True,
            "status": "USED",
            "cards": [{"card_id": "stored"}],
            "methodologist_effect": "NO_MATERIAL_EFFECT",
        }
        search = Mock()
        result = get_literature_support(**_kwargs(), search_fn=search)
        self.assertTrue(result["replayed_from_audit"])
        self.assertEqual(result["cards"][0]["card_id"], "stored")
        search.assert_not_called()

    def test_intraday_and_entry_zone_normalization(self):
        self.assertEqual(
            derive_intraday_15m_status({"session_available": True, "verdict_bucket": "SUPPORTS"}),
            "USED_SUPPORTIVE",
        )
        self.assertEqual(
            derive_intraday_15m_status({"session_available": True, "verdict_bucket": "CHALLENGES"}),
            "NOT_SUPPORTIVE_FALLBACK_TO_PRIOR",
        )
        self.assertEqual(
            derive_entry_zone_state({"zone_low": 100, "zone_high": 102, "latest_price": 101}),
            "INSIDE_ENTRY_ZONE",
        )
        self.assertEqual(
            derive_entry_zone_state({"zone_low": 100, "zone_high": 102, "latest_price": 110}),
            "ENTRY_ZONE_MISSED",
        )

    def test_pack_slice_is_available_to_all_existing_agents(self):
        support = {"enabled": True, "status": "NO_RELEVANT_LONG_CARDS", "cards": []}
        pack = build_shadow_evidence_pack(
            hearing={"HEARING_ID": "h", "EVIDENCE_JSON": json.dumps({"latest_price": 101})},
            snapshot={"ENTRY_ZONE_JSON": {"low": 100, "high": 102}},
            proposal={"PROPOSAL_ID": 1, "SYMBOL": "TEST", "DIRECTION": "LONG"},
            roles=[],
            artifacts=[],
            literature_support=support,
        )
        self.assertEqual(pack.slices["literature_support"]["status"], "NO_RELEVANT_LONG_CARDS")
        for role, slices in ROLE_SLICE_MAP.items():
            self.assertIn("literature_support", slices, role)

    def test_specialist_prompt_has_advisory_role_relevance_guardrails(self):
        msg = _specialist_user_message(
            "h", "ENTRY_GEOMETRY",
            {"literature_support": {"status": "USED"}},
        )
        self.assertIn("Use literature_support only if relevant to your role", msg)
        self.assertIn("do not repeat it mechanically", msg)
        self.assertIn("Observed MIP evidence remains the source of truth", msg)
        self.assertIn("never suggest taking a short trade", msg)

    def test_chair_prompt_preserves_approval_and_missing_confirmation_logic(self):
        msg = _chair_context_message(
            "h", {}, [], [],
            {"literature_support": {"status": "USED"}},
        )
        self.assertIn("Remain capable of APPROVE", msg)
        self.assertIn("APPROVE_REDUCED", msg)
        self.assertIn("NOT_SUPPORTIVE_FALLBACK_TO_PRIOR", msg)
        self.assertIn("Never recommend taking a short trade", msg)

    def test_chair_parses_methodologist_effect(self):
        ruling = parse_chair_ruling(json.dumps({
            "shadow_stance": "WAIT_RECLAIM",
            "shadow_confidence": 0.7,
            "methodologist_effect": {
                "used": True,
                "effect": "SUPPORTED_WAIT",
                "summary": "The Price Action & Trading Methodologist supports waiting for confirmation.",
            },
        }))
        self.assertTrue(ruling.methodologist_effect.used)
        self.assertEqual(ruling.methodologist_effect.effect, "SUPPORTED_WAIT")


if __name__ == "__main__":
    unittest.main()
