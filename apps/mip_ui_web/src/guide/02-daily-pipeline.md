# 2. The Daily Pipeline

Every day, MIP runs an automated event sequence.

If you remember only one thing, remember this chain:

`Data -> Signals -> Outcomes -> Trust -> Proposals -> Execution -> Digest`

## Step-by-Step Event Flow

1. **Ingest bars**  
   Pull fresh OHLC market bars.

2. **Detect signals**  
   Pattern detectors scan new bars and log candidate opportunities.

3. **Evaluate outcomes**  
   Older signals become measurable over forward horizons (H1/H3/H5/H10/H20).

4. **Update learning metrics**  
   Maturity, coverage, hit rate, and average return are recalculated.

5. **Recompute trust eligibility**  
   Patterns/symbols with enough evidence move toward (or away from) trade eligibility.

6. **Generate proposals**  
   Eligible signals become trade proposals.

7. **Apply risk + execute (paper/live flow dependent)**  
   Gates and limits are checked before execution.

8. **Publish narrative outputs**  
   Cockpit and related pages show what changed, what matters, and what to watch.

## Where You Can Verify Each Step In The UI

| Pipeline Step | Best UI page to verify |
|---------------|-------------------------|
| Ingest/run health | Runs (Audit Viewer) |
| Signal generation | Market Timeline / AI Agent Decisions |
| Outcome evaluation and learning metrics | Training Status |
| Proposal and decision flow | AI Agent Decisions |
| Gate and position behavior | Portfolio / Decision Console |
| Narrative summary | Cockpit |

> **Weekends & Holidays:** If no new market data arrives (weekends, holidays), the pipeline still runs but skips signal generation. Training evaluation and digest generation still happen, so you'll always see a fresh AI narrative.
