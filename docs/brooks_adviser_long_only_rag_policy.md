# Brooks Intraday Adviser — long-only execution RAG policy (V0.1)

**Status:** Policy only — no corpus re-index, no Adviser build in this stage.

## Execution direction

`EXECUTION_DIRECTION = LONG_ONLY`

The Adviser may recommend **long** entries, holds, stops, and exits. It must **never** recommend opening or managing a **short** position.

## Include (bearish knowledge for context)

Knowledge required to interpret price action hostile to longs:

- Bear trends, bear breakouts, failed bull breakouts
- Bear follow-through, selling pressure
- Bear channels / micro channels, bearish reversals
- Climaxes at tops, resistance, trapped bulls
- Conditions that invalidate or delay long thesis

## Exclude (short-entry corpus)

Do **not** index or surface for Adviser **action** prompts when primary purpose is:

- Short-entry setup or execution
- Short-specific stop placement
- Short position management or profit targets

Bearish cards remain in the library for **interpretation**; filtering applies at retrieval/ranking for Adviser wake prompts, not at DELETE from `LITERATURE_CONCEPT_CARD`.

## Implementation note (future)

When `BROOKS_INTRADAY_ADVISER_V0_1` RAG wakes run:

- Tag queries with `execution_direction=LONG_ONLY`.
- **Hard exclude** `execution_relevance=EXCLUDED_SHORT_EXECUTION` (not indexed).
- **Long entry / watch:** allow `LONG_ENTRY`, `LONG_CONTEXT`, `LONG_FAILURE`, `BEARISH_CONTEXT_ONLY`.
- **Position management:** also allow `LONG_MANAGEMENT`.
- Adviser action schema stays long-only (no short action enum).

Corpus metadata field: `execution_relevance` on each INTRADAY_ADVISER document (see static deployment plan).
