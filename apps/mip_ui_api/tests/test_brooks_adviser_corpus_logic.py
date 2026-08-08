"""Offline tests for Adviser corpus dedupe/classify (no Snowflake)."""

from __future__ import annotations

import unittest

# Import helpers from build script path
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
BUILD = ROOT / "cursorfiles" / "brooks_intraday_adviser_corpus_build.py"
import importlib.util

_spec = importlib.util.spec_from_file_location("brooks_intraday_adviser_corpus_build", BUILD)
corpus = importlib.util.module_from_spec(_spec)
assert _spec.loader
_spec.loader.exec_module(corpus)


class CorpusLogicTests(unittest.TestCase):
    def test_short_recipe_excluded_bear_context_kept(self):
        short_card = {
            "CONCEPT_NAME": "Low 2 Short Setup in Bear Trend",
            "SETUP_DEFINITION": "Sell short on low 2 signal bar in bear trend.",
            "MARKET_CONTEXT": "",
            "CARD_ROLE": "SETUP_RECIPE",
            "MIP_USAGE_TIER": "AGENT_READY_NOW",
            "DAILY_BAR_COMPATIBILITY": "INTRADAY_NATIVE",
        }
        bear_ctx = {
            "CONCEPT_NAME": "Failed Bull Breakout Warning",
            "SETUP_DEFINITION": "Failed bull breakout often leads to trading range hostile to new longs.",
            "MARKET_CONTEXT": "Bear follow-through after failed bull breakout.",
            "CARD_ROLE": "MARKET_CONTEXT",
            "MIP_USAGE_TIER": "FUTURE_INTRADAY",
            "DAILY_BAR_COMPATIBILITY": "INTRADAY_NATIVE",
        }
        self.assertEqual(corpus.classify_adviser(short_card), "SHORT_EXECUTION_EXCLUDE")
        self.assertEqual(corpus.classify_adviser(bear_ctx), "BEARISH_CONTEXT_FOR_LONGS")

    def test_bull_trend_bar_not_bearish_context(self):
        card = {
            "CONCEPT_NAME": "Bull Trend Bar Setup",
            "SETUP_DEFINITION": "Strong bull trend bar: body above median, close near high.",
            "MARKET_CONTEXT": "Can appear in bull trend or transition from trading range or bear trend.",
            "CARD_ROLE": "SETUP_RECIPE",
            "MIP_USAGE_TIER": "AGENT_READY_NOW",
            "DAILY_BAR_COMPATIBILITY": "DAILY_COMPATIBLE",
        }
        base = corpus.classify_adviser(card)
        fixed = corpus.correct_adviser_class(card, base)
        self.assertEqual(fixed, "ADVISER_CORE")
        self.assertEqual(corpus.assign_execution_relevance(card, fixed), "LONG_ENTRY")

    def test_breakout_pullback_long_not_bearish(self):
        card = {
            "CONCEPT_NAME": "Breakout Pullback Long Setup",
            "SETUP_DEFINITION": "Break above resistance, pullback holds, buy stop above pullback high.",
            "MARKET_CONTEXT": "Bull breakout with pullback to prior resistance.",
            "CARD_ROLE": "SETUP_RECIPE",
            "MIP_USAGE_TIER": "AGENT_READY_NOW",
            "DAILY_BAR_COMPATIBILITY": "INTRADAY_NATIVE",
        }
        cls = corpus.correct_adviser_class(card, corpus.classify_adviser(card))
        self.assertEqual(cls, "ADVISER_CORE")

    def test_short_setup_title_becomes_bearish_context_only(self):
        card = {
            "CONCEPT_NAME": "Low 2 Short Setup in Bear Trend",
            "SETUP_DEFINITION": "Short setup after low 2 in bear trend.",
            "MARKET_CONTEXT": "Bear trend, selling pressure.",
            "CARD_ROLE": "SETUP_RECIPE",
            "MIP_USAGE_TIER": "AGENT_READY_NOW",
            "DAILY_BAR_COMPATIBILITY": "INTRADAY_NATIVE",
        }
        cls = corpus.correct_adviser_class(card, corpus.classify_adviser(card))
        self.assertEqual(cls, "BEARISH_CONTEXT_FOR_LONGS")
        rel = corpus.assign_execution_relevance(card, cls)
        self.assertEqual(rel, "BEARISH_CONTEXT_ONLY")
        self.assertNotEqual(rel, "LONG_ENTRY")

    def test_dedupe_group_keeps_one(self):
        cards = [
            {
                "CARD_ID": "a",
                "DUPLICATE_GROUP_ID": "g1",
                "CANONICAL_CANDIDATE": True,
                "CONCEPT_NAME": "X",
                "SETUP_DEFINITION": "long text " * 20,
                "SOURCE_BOOK": "book",
            },
            {
                "CARD_ID": "b",
                "DUPLICATE_GROUP_ID": "g1",
                "CANONICAL_CANDIDATE": False,
                "CONCEPT_NAME": "X copy",
                "SETUP_DEFINITION": "short",
                "SOURCE_BOOK": "book",
            },
        ]
        retained, stats = corpus.dedupe_cards(cards)
        self.assertEqual(len(retained), 1)
        self.assertEqual(retained[0]["CARD_ID"], "a")
        self.assertEqual(stats["duplicates_removed"], 1)


if __name__ == "__main__":
    unittest.main()
