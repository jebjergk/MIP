import json
import unittest
from datetime import date, timedelta
from unittest.mock import Mock, patch

from pydantic import ValidationError

from app.price_action.geometry import Bar, analyse_geometry
from app.price_action.models import AnalyseRequest
from app.price_action.router import analyse_price_action
from app.price_action.service import (
    MAX_RAG_CALLS,
    MAX_RAG_CARD_CHARS,
    MAX_RAG_CARDS,
    MAX_RAG_CARDS_PER_TOPIC,
    MAX_RAG_TOTAL_CHARS,
    RAG_TOPICS,
    RuntimeConfig,
    _methodologist_response_format,
    _retrieve_topic,
    _write_audit,
    analyse,
    call_methodologist,
    deterministic_methodologist,
    read_runtime_config,
    retrieve_literature,
)


def _bars(count=120):
    start = date(2026, 1, 1)
    rows = []
    for index in range(count):
        baseline = 100 + index * 0.2
        wave = (index % 10 - 5) * 0.25
        close = baseline + wave
        rows.append(Bar(
            day=start + timedelta(days=index),
            open=close - 0.2,
            high=close + 1.0,
            low=close - 1.0,
            close=close,
            volume=1000 + index,
        ))
    return rows


def _config(**overrides):
    values = {"enabled": True, "audit_enabled": False}
    values.update(overrides)
    return RuntimeConfig(**values)


def _contract_models():
    response = analyse(
        AnalyseRequest(symbol="TEST", lookback_bars=120),
        config=_config(),
        bars=_bars(),
    )
    return response.detected_geometry, response.situation_model


def _rag_payload(topic, count=2):
    return {
        "status": "USED",
        "topic": topic,
        "results": [
            {
                "card_id": f"{topic}-{index}",
                "topic": topic,
                "concept": f"Concept {index}",
                "methodologist_view": "Daily-compatible long evaluation concept.",
                "relevance_reason": "Matched this chart situation.",
                "applies_to": "LONG_EVALUATION",
            }
            for index in range(count)
        ],
    }


class PriceActionAnalyserTests(unittest.TestCase):
    def test_methodologist_response_format_enforces_required_types(self):
        schema = _methodologist_response_format()["schema"]
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(
            schema["properties"]["verdict"]["properties"]["confidence"]["type"],
            "number",
        )
        self.assertEqual(
            schema["properties"]["expert_read"]["properties"]["what_supports_the_long"]["type"],
            "array",
        )
        annotation = schema["properties"]["annotations"]["items"]
        self.assertIn("label", annotation["required"])
        self.assertNotIn("text", annotation["properties"])

    def test_request_contract_aliases_to_lookback_bars_and_is_long_only(self):
        for key in ("lookback_bars", "lookback", "lookback_days"):
            request = AnalyseRequest(symbol=" aapl ", **{key: 90})
            self.assertEqual(request.symbol, "AAPL")
            self.assertEqual(request.lookback_bars, 90)
            self.assertIn("lookback_bars", request.model_dump())
            self.assertNotIn("lookback", request.model_dump())
        with self.assertRaises(ValidationError):
            AnalyseRequest(symbol="AAPL", lookback_bars=100)
        unsupported = analyse_price_action(
            AnalyseRequest(symbol="AAPL", lookback_bars=90, side="short")
        )
        self.assertEqual(unsupported.status_code, 400)
        self.assertEqual(json.loads(unsupported.body)["status"], "UNSUPPORTED_SIDE")

    def test_geometry_has_chart_series_and_exact_annotation_contract(self):
        first = analyse_geometry(_bars())
        second = analyse_geometry(_bars())
        self.assertEqual(first, second)
        geometry, situation, annotations = first
        self.assertEqual(len(geometry["ema20_series"]), 120)
        self.assertIn("support_resistance_zones", geometry)
        self.assertIn("range_boxes", geometry)
        self.assertIn("market_structure", situation)
        types = {item.type for item in annotations}
        self.assertIn("HORIZONTAL_LEVEL", types)
        self.assertIn("EMA_LINE", types)
        self.assertIn("MARKER", types)
        self.assertTrue(all(item.source == "detected_geometry" for item in annotations))

    def test_config_reads_all_caps_clamps_hard_maxima_and_missing_cap_disables_rag(self):
        values = {
            "PRICE_ACTION_ANALYSER_ENABLED": "true",
            "PRICE_ACTION_ANALYSER_PIVOT_LEFT": "99",
            "PRICE_ACTION_ANALYSER_PIVOT_RIGHT": "3",
            "PRICE_ACTION_ANALYSER_MAX_LOOKBACK_BARS": "999",
            "PRICE_ACTION_ANALYSER_RAG_ENABLED": "true",
            "PRICE_ACTION_ANALYSER_RAG_MAX_CALLS": "99",
            "PRICE_ACTION_ANALYSER_RAG_MAX_RETRIES": "12",
            "PRICE_ACTION_ANALYSER_RAG_MAX_TOTAL_CARDS": "99",
            "PRICE_ACTION_ANALYSER_RAG_MAX_CARDS_PER_TOPIC": "99",
            "PRICE_ACTION_ANALYSER_RAG_MAX_CARD_CHARS": "9999",
            "PRICE_ACTION_ANALYSER_RAG_MAX_TOTAL_CHARS": "99999",
            "PRICE_ACTION_ANALYSER_RAG_TIMEOUT_SECONDS": "99",
            "PRICE_ACTION_ANALYSER_LLM_ENABLED": "true",
            "PRICE_ACTION_ANALYSER_AUDIT_ENABLED": "true",
        }
        connection = Mock()
        connection.cursor.return_value = Mock()
        rows = [{"CONFIG_KEY": key, "CONFIG_VALUE": value} for key, value in values.items()]
        with patch("app.price_action.service.get_connection", return_value=connection), patch(
            "app.price_action.service.fetch_all", return_value=rows
        ):
            config = read_runtime_config()
        self.assertTrue(config.rag_enabled)
        self.assertEqual(config.pivot_left, 10)
        self.assertEqual(config.max_lookback_bars, 250)
        self.assertEqual(config.rag_max_calls, MAX_RAG_CALLS)
        self.assertEqual(config.rag_max_retries, 0)
        self.assertEqual(config.rag_max_total_cards, MAX_RAG_CARDS)
        self.assertEqual(config.rag_max_cards_per_topic, MAX_RAG_CARDS_PER_TOPIC)
        self.assertEqual(config.rag_max_card_chars, MAX_RAG_CARD_CHARS)
        self.assertEqual(config.rag_max_total_chars, MAX_RAG_TOTAL_CHARS)
        self.assertEqual(config.rag_timeout_seconds, 10)

        incomplete = rows[:-1]
        incomplete = [
            row for row in incomplete
            if row["CONFIG_KEY"] != "PRICE_ACTION_ANALYSER_RAG_MAX_TOTAL_CHARS"
        ]
        with patch("app.price_action.service.get_connection", return_value=connection), patch(
            "app.price_action.service.fetch_all", return_value=incomplete
        ):
            self.assertFalse(read_runtime_config().rag_enabled)

    def test_rag_uses_three_exact_grouped_topics_and_hard_caps(self):
        calls = []

        def retrieve(query, topic, top_k, side):
            calls.append((query, topic, top_k, side))
            payload = _rag_payload(topic, 20)
            for card in payload["results"]:
                card["methodologist_view"] = "x" * 3000
            return payload

        geometry, situation = _contract_models()
        result = retrieve_literature(
            geometry,
            situation,
            _config(rag_enabled=True),
            retrieve,
        )
        self.assertEqual([call[1] for call in calls], list(RAG_TOPICS))
        self.assertTrue(all(call[2] == 2 and call[3] == "LONG" for call in calls))
        self.assertLessEqual(result["retrieval_count"], MAX_RAG_CALLS)
        self.assertLessEqual(result["card_count"], MAX_RAG_CARDS)
        self.assertEqual(result["retries"], 0)
        self.assertLessEqual(result["total_chars"], MAX_RAG_TOTAL_CHARS)
        self.assertTrue(
            all(len(card["methodologist_view"]) <= MAX_RAG_CARD_CHARS for card in result["cards"])
        )

    def test_retrieval_calls_price_action_procedure_not_search_preview(self):
        cursor = Mock()
        cursor.fetchone.return_value = (json.dumps(_rag_payload("MARKET_STRUCTURE")),)
        connection = Mock()
        connection.cursor.return_value = cursor
        with patch("app.price_action.service.get_connection", return_value=connection):
            result = _retrieve_topic("query", "MARKET_STRUCTURE", 2, "LONG", 10)
        executed_sql = " ".join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertIn("SP_SEARCH_PRICE_ACTION_KNOWLEDGE", executed_sql)
        self.assertNotIn("SEARCH_PREVIEW", executed_sql)
        self.assertEqual(result["status"], "USED")

    def test_methodologist_strict_richer_schema_and_grounded_annotation(self):
        geometry, situation = _contract_models()
        calls = []
        grounded = geometry["swings"][-1]

        def llm(model, prompt):
            calls.append((model, prompt))
            return json.dumps({
                "symbol": "TEST",
                "timeframe": "1D",
                "side": "LONG",
                "analysis_status": "OK",
                "expert_read": {
                    "market_context": "Constructive daily context.",
                    "trend_state": "UPTREND",
                    "current_pattern": "Pullback candidate.",
                    "support_resistance_read": "Support remains below price.",
                    "long_location_quality": "CHASING",
                    "continuation_vs_failure_risk": "Continuation needs a better entry.",
                    "what_supports_the_long": ["Price is above EMA20."],
                    "what_weakens_the_long": ["Price is extended."],
                    "confirmation_needed": "Wait for a pullback.",
                    "invalidation_logic": "Loss of support weakens the long.",
                },
                "plain_explanation": {
                    "summary": "The trend is constructive but price is extended.",
                    "what_the_chart_is_doing": "Price is rising near recent highs.",
                    "why_it_matters": "Buying here may be chasing.",
                    "what_to_wait_for": "A pullback toward support.",
                    "simple_risk_warning": "Support can fail.",
                },
                "verdict": {
                    "decision": "WAIT_PULLBACK",
                    "confidence": 0.74,
                    "reason_summary": "Location is extended.",
                    "not_a_short_recommendation": True,
                },
                "annotations": [{
                    "type": "TEXT_NOTE",
                    "label": "Relevant swing",
                    "date": grounded["date"],
                    "price": grounded["price"],
                }],
            })

        result = call_methodologist("test-model", geometry, situation, [], llm)
        self.assertEqual(len(calls), 1)
        self.assertNotIn('"ema20_series"', calls[0][1])
        self.assertLess(len(calls[0][1]), 24000)
        self.assertEqual(result.analysis_status, "OK")
        self.assertEqual(result.verdict.decision, "WAIT_PULLBACK")
        self.assertTrue(result.verdict.not_a_short_recommendation)
        self.assertEqual(result.annotations[0].source, "methodologist_interpretation")

    def test_invalid_llm_output_fails_open_to_deterministic_long_only_read(self):
        geometry, situation = _contract_models()
        calls = []

        def llm(_model, _prompt):
            calls.append(1)
            return json.dumps({"verdict": "SHORT"})

        result = call_methodologist("test-model", geometry, situation, [], llm)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result.analysis_status, "METHODOLOGIST_UNAVAILABLE")
        self.assertIn(result.verdict.decision, {
            "LONG_APPROVE", "LONG_APPROVE_REDUCED", "WAIT_PULLBACK", "WAIT_RECLAIM",
            "DEFER", "REJECT", "NO_CLEAR_LONG",
        })
        self.assertTrue(result.verdict.not_a_short_recommendation)

    def test_geometry_only_trading_range_uses_current_situation_contract(self):
        geometry, situation = _contract_models()
        geometry["structure"]["trend"] = "MIXED"
        geometry["long_location"] = "NEUTRAL"
        geometry["entry_location"]["long_location_quality"] = "ACCEPTABLE"
        situation["market_cycle"] = "TRADING_RANGE"
        situation["breakout_followthrough"] = "NONE"

        result = deterministic_methodologist(geometry, situation)

        self.assertEqual(result.verdict.decision, "DEFER")
        self.assertTrue(result.verdict.not_a_short_recommendation)

    @patch("app.price_action.service._write_audit")
    def test_geometry_only_response_has_chart_and_fallback_and_respects_audit_flag(self, audit):
        response = analyse(
            AnalyseRequest(symbol="TEST", lookback_bars=120),
            config=_config(),
            bars=_bars(),
        )
        self.assertEqual(response.lookback_bars, 120)
        self.assertEqual(len(response.bars), 120)
        self.assertEqual(response.bars[-1].ema20, response.detected_geometry["ema20"]["value"])
        self.assertEqual(response.rag_status, "DISABLED")
        self.assertEqual(response.methodologist.analysis_status, "GEOMETRY_ONLY")
        self.assertTrue(response.methodologist.plain_explanation)
        audit.assert_not_called()

    @patch("app.price_action.service._write_audit")
    def test_audit_enabled_writes_once(self, audit):
        response = analyse(
            AnalyseRequest(symbol="TEST", lookback_bars=90),
            config=_config(audit_enabled=True),
            bars=_bars(90),
        )
        audit.assert_called_once()
        self.assertEqual(response.bar_count, 90)

    def test_audit_targets_exact_knowledge_table_and_columns(self):
        response = analyse(
            AnalyseRequest(symbol="TEST", lookback_bars=90),
            config=_config(),
            bars=_bars(90),
        )
        cursor = Mock()
        connection = Mock()
        connection.cursor.return_value = cursor
        with patch("app.price_action.service.get_connection", return_value=connection):
            _write_audit(response, AnalyseRequest(symbol="TEST", lookback_bars=90), False, 12)
        sql = cursor.execute.call_args.args[0]
        self.assertIn("MIP.KNOWLEDGE.PRICE_ACTION_ANALYSER_AUDIT", sql)
        self.assertIn("METHODOLOGIST_OUTPUT_JSON", sql)
        self.assertIn("LOOKBACK_BARS", sql)
        self.assertNotIn("MIP.APP.PRICE_ACTION_ANALYSER_AUDIT", sql)


if __name__ == "__main__":
    unittest.main()
