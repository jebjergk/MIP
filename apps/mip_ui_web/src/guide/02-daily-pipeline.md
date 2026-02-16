# 2. The Daily Pipeline

Every day, MIP runs an automated pipeline. Think of it as a checklist the system goes through:

1. **Fetch new market bars** — Get the latest price data (open, high, low, close) for all tracked symbols — stocks, FX pairs, etc.
2. **Generate signals** — Run each pattern definition to detect interesting price action in today's bars.
3. **Evaluate old outcomes** — Look back at signals from previous days and check: did the price actually go up afterwards?
4. **Update training metrics** — Recalculate hit rate, average return, and maturity score for every symbol/pattern combination.
5. **Update trust labels** — Re-evaluate which patterns have earned TRUSTED status based on the latest metrics.
6. **Generate trade proposals** — For trusted signals, propose buy or sell orders to the portfolio.
7. **Execute trades** — Fill approved orders through the portfolio engine (if risk gate allows).
8. **Generate AI digest** — Create a narrative summary of everything that happened — what changed, what matters, what to watch.

> **Weekends & Holidays:** If no new market data arrives (weekends, holidays), the pipeline still runs but skips signal generation. Training evaluation and digest generation still happen, so you'll always see a fresh AI narrative.
