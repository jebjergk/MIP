# Brooks INTRADAY_ADVISER corpus — quality gate report

**RAG_SCOPE:** `INTRADAY_ADVISER` · **EXECUTION_DIRECTION:** `LONG_ONLY`

## Final statistics

{
  "rag_scope": "INTRADAY_ADVISER",
  "execution_direction": "LONG_ONLY",
  "source_cards": 1482,
  "starting_cards": 1482,
  "unique_semantic_concepts_after_dedupe": 1370,
  "duplicates_removed": 112,
  "contextual_variants_retained": 0,
  "duplicate_group_removals": 112,
  "near_duplicate_removals": 0,
  "deduplicated_cards_retained": 1370,
  "metadata_classification_fixes": 62,
  "metadata_fix_card_ids_sample": [
    "00058664d1c2e548",
    "00ba59f041c53b15",
    "091de34e73b82722",
    "0f74ca9822dd77d8",
    "13d2e5eac83f8d48",
    "17829e1ce5b01367",
    "1cda6e0d160e9833",
    "228f5560a59caa36",
    "2b5ab2d78b102eec",
    "2ceacb90e184196c",
    "332c01ffca2e8608",
    "360147fd5f4824a9",
    "44268a05c0601fbe",
    "4470be74f15343b7",
    "462152d0746bcabf",
    "4c762c344a67c004",
    "592563be806292f1",
    "68707a984e3c78bd",
    "68c18ea2f1ff37e6",
    "68fb12760b589471",
    "69ef833d9459d4b7",
    "6a92aec3a1b6a98c",
    "6e1021d973d60c89",
    "6fd524a2a63742a6",
    "7133b21304e6bdbc",
    "77b28353da198223",
    "7b795c8ab4098f67",
    "7c14aa4df551c353",
    "80c231327f56ed54",
    "874e63edbc9aacd6",
    "88070ce99eb4f36e",
    "8ba65bb9aec8ea0d",
    "8ceccc77afcc9571",
    "9147b5c8779e2225",
    "948dd11d85119553",
    "971321ea0aaaf2fd",
    "9c134016b40f0cb7",
    "a67f5c8d6ac082ba",
    "a714d3720288f2ba",
    "a9d97b0200439352"
  ],
  "short_execution_excluded": 57,
  "excluded_short_execution_card_ids_sample": [
    "381b0b1a63d1ed55",
    "2efadf39d12610a7",
    "a9d97b0200439352",
    "df6a1c5f81cd0cc9",
    "49f1709ee5d6e748",
    "6369abf857c9380e",
    "9fd72736a31283a6",
    "1e0f40ce0af798df",
    "e9b612fbbb5e7f71",
    "426476f5a3c30d2d",
    "cdbdfcc244b6574e",
    "7c89660bd1048a24",
    "74c88b6faa899250",
    "71867b85df99b39d",
    "f061888ae0c6fa4e",
    "e808a76a335be7ad",
    "b633b21c5faca207",
    "18bdd782a0666423",
    "3d63dfbd5544a7c2",
    "e2e9fb8694e2d930",
    "366658925569fa05",
    "8599c48dc59e3395",
    "71399a78cd4cdba1",
    "64d6579eac23a776",
    "6895179c5b911a8a",
    "075d887c2bd1c06c",
    "57bfe23325fd4ab6",
    "25ee57703ca9d6d1",
    "d69124db76ff1c57",
    "c44a1fa8dd027107"
  ],
  "bearish_context_only_in_corpus": 415,
  "execution_relevance_counts": {
    "LONG_CONTEXT": 67,
    "BEARISH_CONTEXT_ONLY": 415,
    "LONG_ENTRY": 355,
    "LONG_FAILURE": 101,
    "LONG_MANAGEMENT": 269
  },
  "long_entry_with_short_language_violations": 0,
  "not_suitable_excluded": 1,
  "needs_human_review_held_back": 105,
  "held_back_confirmed_excluded_from_corpus": 105,
  "final_adviser_candidate_cards": 1207,
  "intraday_native_in_corpus": 645,
  "bearish_context_for_longs": 413,
  "long_management": 266,
  "adviser_core": 503,
  "adviser_supporting": 25,
  "human_queue_card_ids_distinct": 379,
  "human_inspect_after_automated_qa_estimate": 105
}

## Coverage after cleanup

{
  "market_state": {
    "candidates": 368,
    "core": 172,
    "coverage": "STRONG"
  },
  "trend_vs_range": {
    "candidates": 730,
    "core": 281,
    "coverage": "STRONG"
  },
  "pullbacks": {
    "candidates": 744,
    "core": 340,
    "coverage": "STRONG"
  },
  "second_entries": {
    "candidates": 129,
    "core": 60,
    "coverage": "STRONG"
  },
  "breakouts": {
    "candidates": 610,
    "core": 296,
    "coverage": "STRONG"
  },
  "failed_breakouts": {
    "candidates": 112,
    "core": 53,
    "coverage": "STRONG"
  },
  "reversals": {
    "candidates": 552,
    "core": 249,
    "coverage": "STRONG"
  },
  "signal_follow_through": {
    "candidates": 160,
    "core": 55,
    "coverage": "STRONG"
  }
}

## Terminology (expanded search)

{
  "h1_h2": {
    "hit_count": 219,
    "diagnosis": "terminology/tagging gap in narrow audit keywords",
    "sample_ids": [
      "7bf25083dc697d3f",
      "f5cfc3f5f0fb70c9",
      "e7bb079d8eb16b42",
      "b4f8716665690e9e",
      "1498e749fa0f23cb",
      "17829e1ce5b01367",
      "39c0f70bfd0bcb1d",
      "b2cfb04c0d6b5717"
    ]
  },
  "ii_ioi_oo": {
    "hit_count": 4,
    "diagnosis": "genuinely sparse in extracted cards \u2014 verify manually",
    "sample_ids": [
      "85a28c98d500a21a",
      "7df8acbb17ea20d4",
      "cdb3168ddc300b6b",
      "d7334916db19d3cf"
    ]
  },
  "late_session_eod": {
    "hit_count": 26,
    "diagnosis": "terminology/tagging gap in narrow audit keywords",
    "sample_ids": [
      "2095ff174294f970",
      "4ce29ba86f6d67ab",
      "e2a80f4271864a40",
      "36d31d78e0d3752d",
      "b9c823d5dd6549f1",
      "d1596900f46f072d",
      "c47e73612d00faec",
      "760c45b4e2ea61f2"
    ]
  },
  "minor_reversal": {
    "hit_count": 44,
    "diagnosis": "terminology/tagging gap in narrow audit keywords",
    "sample_ids": [
      "cea3881cd132fc7e",
      "39851ef1289145e6",
      "de6f02c3d883f49a",
      "0c143f4561b21bc3",
      "80c231327f56ed54",
      "2efadf39d12610a7",
      "7fbe618c4515ca2f",
      "582d2243686a2950"
    ]
  }
}

## Human QA pack (representative cards)

### trend — `8caf229edae0f3f1`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Trend Bar as Part of Climax and Reversal Structure
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative trend; QA score 100
- **Metadata:** `{"market_state": ["TREND"], "setup_family": ["CLIMAX", "REVERSAL"], "decision_stage": ["POSITION_MANAGEMENT"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "TRADE_MANAGEMENT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TWO_BAR_THREE_BAR_REVERSAL", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT", "EXIT_RISK"]}`

```
Concept: Trend Bar as Part of Climax and Reversal Structure
Market context: Markets with trending moves and climaxes
Setup: Every trend bar is simultaneously a spike, breakout, gap, and part or all of a vacuum and a climax. A climax ends with the first pause bar (e.g., doji, inside bar, or reversal bar). A climactic reversal requires a buy climax (series of bull trend bars) followed by a bear breakout bar.
```

### trend — `b348cd0d110e934c`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Bull Trend Bar Setup
- **Class:** ADVISER_CORE
- **Why:** Representative trend; QA score 100
- **Metadata:** `{"market_state": ["TRADING_RANGE", "TREND"], "setup_family": ["FAILED_BREAKOUT"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "FAILED_BREAKOUT", "execution_relevance": "LONG_ENTRY", "execution_direction": "LONG_ONLY"}`

```
Concept: Bull Trend Bar Setup
Market context: Occurs within an existing bull trend or during a transition from trading range or bear trend to bull trend.
Setup: A bull trend bar is a bar with a close above its open (white candle) and a body size about the same or larger than the median body size of the last 5-10 bars. Additional strength signs include the open near the low, close near the high, close at or above closes and highs of prior bars, high above prior highs, and small tails. This bar in
```

### trading_range — `38650e44d47934f8`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Doji Bar as One-Bar Trading Range
- **Class:** ADVISER_CORE
- **Why:** Representative trading_range; QA score 100
- **Metadata:** `{"market_state": ["TRADING_RANGE"], "setup_family": ["TRADING_RANGE"], "decision_stage": ["CONTEXT"], "directional_context": ["LONG"], "adviser_role": "MARKET_CONTEXT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TRADING_RANGE", "execution_relevance": "LONG_CONTEXT", "execution_direction": "LONG_ONLY"}`

```
Concept: Doji Bar as One-Bar Trading Range
Market context: Any market and timeframe where bar bodies can be measured
Setup: A doji bar is a bar with a tiny or nonexistent body, indicating that bulls and bears were in equilibrium during the bar and no directional control was established. It functions as a one-bar trading range.
```

### trading_range — `b348cd0d110e934c`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Bull Trend Bar Setup
- **Class:** ADVISER_CORE
- **Why:** Representative trading_range; QA score 100
- **Metadata:** `{"market_state": ["TRADING_RANGE", "TREND"], "setup_family": ["FAILED_BREAKOUT"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "FAILED_BREAKOUT", "execution_relevance": "LONG_ENTRY", "execution_direction": "LONG_ONLY"}`

```
Concept: Bull Trend Bar Setup
Market context: Occurs within an existing bull trend or during a transition from trading range or bear trend to bull trend.
Setup: A bull trend bar is a bar with a close above its open (white candle) and a body size about the same or larger than the median body size of the last 5-10 bars. Additional strength signs include the open near the low, close near the high, close at or above closes and highs of prior bars, high above prior highs, and small tails. This bar in
```

### h1_h2_second_entry — `e7bb079d8eb16b42`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Risk of Early Reversal Entry in Strong Bear Trend
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative h1_h2_second_entry; QA score 100
- **Metadata:** `{"market_state": ["TREND"], "setup_family": ["CLIMAX", "H1_H2", "REVERSAL", "SECOND_ENTRY"], "decision_stage": ["INTERPRET"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["STOP_RISK"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TWO_BAR_THREE_BAR_REVERSAL", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT", "EXIT_RISK"]}`

```
Concept: Risk of Early Reversal Entry in Strong Bear Trend
Market context: In a strong bear trend or sell climax, the first reversal attempt is often risky and prone to failure.
Setup: Buying the first reversal bar in a strong bear trend is risky because downward momentum is strong and the reversal bar may be weak (e.g., a doji). It is safer to wait for a strong bull trend bar or a second entry after signs of a higher low or reduced selling pressure.
```

### h1_h2_second_entry — `7c8c2b9d2e5439dc`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Buy Climax and Two-Legged Correction
- **Class:** LONG_MANAGEMENT
- **Why:** Representative h1_h2_second_entry; QA score 100
- **Metadata:** `{"market_state": ["TREND"], "setup_family": ["CLIMAX", "PULLBACK"], "decision_stage": ["POSITION_MANAGEMENT"], "directional_context": ["LONG"], "adviser_role": "TRADE_MANAGEMENT", "intraday_compatibility": "DAILY_COMPATIBLE_WITH_CAUTION", "position_state": ["LONG", "IN_TRADE"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TREND_PULLBACK", "execution_relevance": "LONG_MANAGEMENT", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["EXIT_RISK"]}`

```
Concept: Buy Climax and Two-Legged Correction
Market context: After a strong trend with large bars and small tails
Setup: A buy climax is identified by two or more large bull bars with small tails after a strong trend. This often leads to a two-legged sideways to down correction lasting at least 10 bars.
```

### breakout — `8caf229edae0f3f1`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Trend Bar as Part of Climax and Reversal Structure
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative breakout; QA score 100
- **Metadata:** `{"market_state": ["TREND"], "setup_family": ["CLIMAX", "REVERSAL"], "decision_stage": ["POSITION_MANAGEMENT"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "TRADE_MANAGEMENT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TWO_BAR_THREE_BAR_REVERSAL", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT", "EXIT_RISK"]}`

```
Concept: Trend Bar as Part of Climax and Reversal Structure
Market context: Markets with trending moves and climaxes
Setup: Every trend bar is simultaneously a spike, breakout, gap, and part or all of a vacuum and a climax. A climax ends with the first pause bar (e.g., doji, inside bar, or reversal bar). A climactic reversal requires a buy climax (series of bull trend bars) followed by a bear breakout bar.
```

### breakout — `b72b35cfa409837a`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Failed Breakout Setup and Failure Mode
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative breakout; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["BREAKOUT_PULLBACK", "FAILED_BREAKOUT", "FOLLOW_THROUGH"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "BREAKOUT_PULLBACK", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT"]}`

```
Concept: Failed Breakout Setup and Failure Mode
Market context: Occurs when a breakout attempt fails to produce follow-through and reverses, trapping traders who entered on the breakout.
Setup: A breakout bar (trend bar breaking a prior structural level such as a trend channel line) that is not followed by additional trend bars in the breakout direction and is quickly reversed by opposite trend bars. This failure traps breakout traders and often leads to a reversal or strong pullback.
```

### failed_breakout — `b72b35cfa409837a`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Failed Breakout Setup and Failure Mode
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative failed_breakout; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["BREAKOUT_PULLBACK", "FAILED_BREAKOUT", "FOLLOW_THROUGH"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "BREAKOUT_PULLBACK", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT"]}`

```
Concept: Failed Breakout Setup and Failure Mode
Market context: Occurs when a breakout attempt fails to produce follow-through and reverses, trapping traders who entered on the breakout.
Setup: A breakout bar (trend bar breaking a prior structural level such as a trend channel line) that is not followed by additional trend bars in the breakout direction and is quickly reversed by opposite trend bars. This failure traps breakout traders and often leads to a reversal or strong pullback.
```

### failed_breakout — `b04ead135f2c5c1c`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Failed Breakout Leading to Breakout Pullback Short Setup
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative failed_breakout; QA score 100
- **Metadata:** `{"market_state": ["TREND"], "setup_family": ["BREAKOUT_PULLBACK", "FAILED_BREAKOUT", "SIGNAL_BAR"], "decision_stage": ["CONTEXT"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "MARKET_CONTEXT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "BREAKOUT_PULLBACK", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT"]}`

```
Concept: Failed Breakout Leading to Breakout Pullback Short Setup
Market context: A failed breakout in a bear trend can reverse into a breakout pullback short setup, leading to continuation of the bear trend.
Setup: After a failed bull breakout (e.g., reversal bar with bear body), the market reverses down below the breakout bar low, creating a breakout pullback short setup.
```

### breakout_pullback — `b04ead135f2c5c1c`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Failed Breakout Leading to Breakout Pullback Short Setup
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative breakout_pullback; QA score 100
- **Metadata:** `{"market_state": ["TREND"], "setup_family": ["BREAKOUT_PULLBACK", "FAILED_BREAKOUT", "SIGNAL_BAR"], "decision_stage": ["CONTEXT"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "MARKET_CONTEXT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "BREAKOUT_PULLBACK", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT"]}`

```
Concept: Failed Breakout Leading to Breakout Pullback Short Setup
Market context: A failed breakout in a bear trend can reverse into a breakout pullback short setup, leading to continuation of the bear trend.
Setup: After a failed bull breakout (e.g., reversal bar with bear body), the market reverses down below the breakout bar low, creating a breakout pullback short setup.
```

### breakout_pullback — `deda9126153bd829`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Breakout Pullback Long Setup
- **Class:** ADVISER_CORE
- **Why:** Representative breakout_pullback; QA score 100
- **Metadata:** `{"market_state": ["TRADING_RANGE"], "setup_family": ["BREAKOUT_PULLBACK"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["STOP_RISK", "LOCATION_RISK"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "BREAKOUT_PULLBACK", "execution_relevance": "LONG_ENTRY", "execution_direction": "LONG_ONLY"}`

```
Concept: Breakout Pullback Long Setup
Market context: Occurs after a breakout above a trading range or resistance level, followed by a pullback that tests the breakout level.
Setup: After a breakout above a trading range or resistance, the market pulls back to retest the breakout level. Entry is on a buy stop above the high of the pullback bar, anticipating continuation of the breakout trend.
```

### double_bottom — `de6f02c3d883f49a`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Micro Double Bottom and Micro Double Top Patterns
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative double_bottom; QA score 100
- **Metadata:** `{"market_state": ["TREND"], "setup_family": ["DOUBLE_BOTTOM", "FAILED_BREAKOUT"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "FAILED_BREAKOUT", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT"]}`

```
Concept: Micro Double Bottom and Micro Double Top Patterns
Market context: Small reversal patterns formed by consecutive or nearly consecutive bars with nearly identical lows (micro double bottom) or highs (micro double top).
Setup: Micro double bottom: two bars with nearly identical lows, often forming a one-bar bear flag if in a bear spike (bear trend bar closing near low followed by bull trend bar opening near low). Micro double top: two bars with nearly identical highs, often forming a one-b
```

### double_bottom — `f5cfc3f5f0fb70c9`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Double Bottom Pullback Long Setup
- **Class:** LONG_MANAGEMENT
- **Why:** Representative double_bottom; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["DOUBLE_BOTTOM", "H1_H2", "PULLBACK", "SECOND_ENTRY"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["LONG", "IN_TRADE"], "risk_context": ["STOP_RISK"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TREND_PULLBACK", "execution_relevance": "LONG_MANAGEMENT", "execution_direction": "LONG_ONLY"}`

```
Concept: Double Bottom Pullback Long Setup
Market context: Occurs after a failed attempt to make a lower low, forming a double bottom with a pullback that often retraces more than 50% of the prior move down.
Setup: A double bottom pullback long setup forms when the market fails to make a new lower low on the second attempt down, creating a double bottom. The pullback typically extends more than 50% of the prior down move, often nearly the entire way back to the double bottom level. Entry is on a
```

### wedge — `69b0725aabeecbdd`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Spike and Channel Bull Trend Pattern
- **Class:** ADVISER_CORE
- **Why:** Representative wedge; QA score 100
- **Metadata:** `{"market_state": ["TRADING_RANGE", "TREND"], "setup_family": ["DOUBLE_BOTTOM", "PULLBACK", "WEDGE"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE_WITH_CAUTION", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TREND_PULLBACK", "execution_relevance": "LONG_ENTRY", "execution_direction": "LONG_ONLY"}`

```
Concept: Spike and Channel Bull Trend Pattern
Market context: Market transitions from a sharp one-sided spike move into a channel with overlapping bars, indicating a weaker bull trend and possible trading range start.
Setup: A strong bull spike (sharp move up) is followed by a pullback and then a channel of overlapping bars forming a wedge or trend channel. This channel often acts as a bull flag and may lead to a test of the channel low.
```

### wedge — `17829e1ce5b01367`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Failed Low 2 Buy Setup and Subsequent Low 4 Short Setup
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative wedge; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["FAILED_BREAKOUT", "WEDGE"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "FAILED_BREAKOUT", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT"]}`

```
Concept: Failed Low 2 Buy Setup and Subsequent Low 4 Short Setup
Market context: Occurs after a spike up and failed low 2 buy setup
Setup: A failed low 2 buy setup occurs when the market fails to hold above the prior low 2 buy level, often followed by a low 4 short setup which is a reversal down from wedge tops or channel tops.
```

### signal_bar — `d8751271fdd49532`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Breakout Entry and Failure at Prior Swing High
- **Class:** ADVISER_CORE
- **Why:** Representative signal_bar; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["BREAKOUT_PULLBACK", "SIGNAL_BAR"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "BREAKOUT_PULLBACK", "execution_relevance": "LONG_FAILURE", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["DO_NOT_LONG_CONTEXT", "LONG_FAILURE_MODE"]}`

```
Concept: Breakout Entry and Failure at Prior Swing High
Market context: Market rallies back up to a prior swing high after a pullback.
Setup: Entry occurs when the market breaks above a prior swing high or bull signal bar high, either by buying one tick above the old high or after a pullback buying one tick above the prior bar's high, expecting breakout resumption.
```

### signal_bar — `68121a4d149f18a5`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Signal Bar and Entry Bar Relationship
- **Class:** ADVISER_CORE
- **Why:** Representative signal_bar; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["FOLLOW_THROUGH", "SIGNAL_BAR", "TRADING_RANGE"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TRADING_RANGE", "execution_relevance": "LONG_CONTEXT", "execution_direction": "LONG_ONLY"}`

```
Concept: Signal Bar and Entry Bar Relationship
Market context: Every bar can serve as a signal bar for both long and short trades depending on breakout direction.
Setup: A signal bar is the prior closed bar that signals a trade entry on the breakout of its high (for longs) or low (for shorts). The current bar after the signal bar is the entry bar. Follow-through bars after entry increase the probability of a profitable trade.
```

### follow_through — `b72b35cfa409837a`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Failed Breakout Setup and Failure Mode
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative follow_through; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["BREAKOUT_PULLBACK", "FAILED_BREAKOUT", "FOLLOW_THROUGH"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "BREAKOUT_PULLBACK", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT"]}`

```
Concept: Failed Breakout Setup and Failure Mode
Market context: Occurs when a breakout attempt fails to produce follow-through and reverses, trapping traders who entered on the breakout.
Setup: A breakout bar (trend bar breaking a prior structural level such as a trend channel line) that is not followed by additional trend bars in the breakout direction and is quickly reversed by opposite trend bars. This failure traps breakout traders and often leads to a reversal or strong pullback.
```

### follow_through — `cbc12a6f5dddc42a`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Trading Range Formation from Two-Sided Trading
- **Class:** ADVISER_CORE
- **Why:** Representative follow_through; QA score 100
- **Metadata:** `{"market_state": ["TRADING_RANGE"], "setup_family": ["FOLLOW_THROUGH", "TRADING_RANGE"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE_WITH_CAUTION", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TRADING_RANGE", "execution_relevance": "LONG_ENTRY", "execution_direction": "LONG_ONLY"}`

```
Concept: Trading Range Formation from Two-Sided Trading
Market context: Occurs after climactic moves or when bulls and bears are balanced, resulting in sideways price action.
Setup: A trading range forms when bulls and bears both attempt to push price in opposite directions without sustained follow-through, often after climactic spikes or reversals. This is characterized by bars with mixed closes and opens, dojis, and alternating trend bars, lasting from a single bar to many bars.
```

### channel — `b72b35cfa409837a`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Failed Breakout Setup and Failure Mode
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative channel; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["BREAKOUT_PULLBACK", "FAILED_BREAKOUT", "FOLLOW_THROUGH"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "BREAKOUT_PULLBACK", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT"]}`

```
Concept: Failed Breakout Setup and Failure Mode
Market context: Occurs when a breakout attempt fails to produce follow-through and reverses, trapping traders who entered on the breakout.
Setup: A breakout bar (trend bar breaking a prior structural level such as a trend channel line) that is not followed by additional trend bars in the breakout direction and is quickly reversed by opposite trend bars. This failure traps breakout traders and often leads to a reversal or strong pullback.
```

### channel — `69b0725aabeecbdd`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Spike and Channel Bull Trend Pattern
- **Class:** ADVISER_CORE
- **Why:** Representative channel; QA score 100
- **Metadata:** `{"market_state": ["TRADING_RANGE", "TREND"], "setup_family": ["DOUBLE_BOTTOM", "PULLBACK", "WEDGE"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE_WITH_CAUTION", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TREND_PULLBACK", "execution_relevance": "LONG_ENTRY", "execution_direction": "LONG_ONLY"}`

```
Concept: Spike and Channel Bull Trend Pattern
Market context: Market transitions from a sharp one-sided spike move into a channel with overlapping bars, indicating a weaker bull trend and possible trading range start.
Setup: A strong bull spike (sharp move up) is followed by a pullback and then a channel of overlapping bars forming a wedge or trend channel. This channel often acts as a bull flag and may lead to a test of the channel low.
```

### climax — `b8d681453b4c4517`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Vacuum Effect in Climax Reversals
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative climax; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["CLIMAX", "REVERSAL"], "decision_stage": ["POSITION_MANAGEMENT"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "TRADE_MANAGEMENT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["LOCATION_RISK"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TWO_BAR_THREE_BAR_REVERSAL", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT", "EXIT_RISK"]}`

```
Concept: Vacuum Effect in Climax Reversals
Market context: Markets showing sharp spikes followed by reversals at obvious support or resistance
Setup: The vacuum effect occurs when a spike (trend bar) ends in a reversal due to strong traders stepping aside and waiting to trade at key levels, causing a temporary imbalance that is quickly corrected.
```

### climax — `8caf229edae0f3f1`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Trend Bar as Part of Climax and Reversal Structure
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative climax; QA score 100
- **Metadata:** `{"market_state": ["TREND"], "setup_family": ["CLIMAX", "REVERSAL"], "decision_stage": ["POSITION_MANAGEMENT"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "TRADE_MANAGEMENT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TWO_BAR_THREE_BAR_REVERSAL", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT", "EXIT_RISK"]}`

```
Concept: Trend Bar as Part of Climax and Reversal Structure
Market context: Markets with trending moves and climaxes
Setup: Every trend bar is simultaneously a spike, breakout, gap, and part or all of a vacuum and a climax. A climax ends with the first pause bar (e.g., doji, inside bar, or reversal bar). A climactic reversal requires a buy climax (series of bull trend bars) followed by a bear breakout bar.
```

### bearish_context — `b8d681453b4c4517`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Vacuum Effect in Climax Reversals
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative bearish_context; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["CLIMAX", "REVERSAL"], "decision_stage": ["POSITION_MANAGEMENT"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "TRADE_MANAGEMENT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["LOCATION_RISK"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TWO_BAR_THREE_BAR_REVERSAL", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT", "EXIT_RISK"]}`

```
Concept: Vacuum Effect in Climax Reversals
Market context: Markets showing sharp spikes followed by reversals at obvious support or resistance
Setup: The vacuum effect occurs when a spike (trend bar) ends in a reversal due to strong traders stepping aside and waiting to trade at key levels, causing a temporary imbalance that is quickly corrected.
```

### bearish_context — `b348cd0d110e934c`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Bull Trend Bar Setup
- **Class:** ADVISER_CORE
- **Why:** Representative bearish_context; QA score 100
- **Metadata:** `{"market_state": ["TRADING_RANGE", "TREND"], "setup_family": ["FAILED_BREAKOUT"], "decision_stage": ["ENTRY_CONFIRMATION", "WATCH"], "directional_context": ["LONG"], "adviser_role": "SETUP_INTERPRETATION", "intraday_compatibility": "DAILY_COMPATIBLE", "position_state": ["FLAT", "LONG"], "risk_context": ["GENERAL"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "FAILED_BREAKOUT", "execution_relevance": "LONG_ENTRY", "execution_direction": "LONG_ONLY"}`

```
Concept: Bull Trend Bar Setup
Market context: Occurs within an existing bull trend or during a transition from trading range or bear trend to bull trend.
Setup: A bull trend bar is a bar with a close above its open (white candle) and a body size about the same or larger than the median body size of the last 5-10 bars. Additional strength signs include the open near the low, close near the high, close at or above closes and highs of prior bars, high above prior highs, and small tails. This bar in
```

### trade_management — `aa58f557bfc91294`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Late and Missed Entries Position Sizing and Stop Placement
- **Class:** LONG_MANAGEMENT
- **Why:** Representative trade_management; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["STOP_MANAGEMENT"], "decision_stage": ["POSITION_MANAGEMENT"], "directional_context": ["LONG"], "adviser_role": "TRADE_MANAGEMENT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["LONG", "IN_TRADE"], "risk_context": ["STOP_RISK"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TRAILING_STOP_POSITION_MANAGEMENT", "execution_relevance": "LONG_MANAGEMENT", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["EXIT_RISK"]}`

```
Concept: Late and Missed Entries Position Sizing and Stop Placement
Market context: When entering a trend late or after missing the original entry, the trader should size the position to match the remaining risk and use the same trailing stop distance as if entered originally.
Setup: Enter the market at the current price with a position size proportional to the size of the remaining swing portion you would still hold, using the same protective stop distance as the original entry.
```

### trade_management — `3b57cc15a2951322`
- **Book:** Al Brooks   Trading Price Action Trends
- **Concept:** Trade Management and Entry Strategy in Bull Channels for Beginners vs Experienced Traders
- **Class:** BEARISH_CONTEXT_FOR_LONGS
- **Why:** Representative trade_management; QA score 100
- **Metadata:** `{"market_state": ["UNSPECIFIED"], "setup_family": ["H1_H2", "PULLBACK", "SECOND_ENTRY", "SIGNAL_BAR"], "decision_stage": ["POSITION_MANAGEMENT"], "directional_context": ["LONG", "BEARISH_CONTEXT_FOR_LONG"], "adviser_role": "TRADE_MANAGEMENT", "intraday_compatibility": "INTRADAY_NATIVE", "position_state": ["FLAT", "LONG"], "risk_context": ["LOCATION_RISK"], "source_book": "Al Brooks   Trading Price Action Trends", "concept_family": "TREND_PULLBACK", "execution_relevance": "BEARISH_CONTEXT_ONLY", "execution_direction": "LONG_ONLY", "adviser_execution_tags": ["BEARISH_CONTEXT_FOR_LONGS", "DO_NOT_LONG_CONTEXT"]}`

```
Concept: Trade Management and Entry Strategy in Bull Channels for Beginners vs Experienced Traders
Market context: Bull channels are difficult to trade due to frequent pullbacks and reversals; beginners should trade only with the trend and wait for high-quality setups, while experienced traders may trade countertrend near resistance.
Setup: Beginners should only buy in bull channels, preferably on high-quality setups such as a high 2 with a bull signal bar near the moving average and away from t
```
