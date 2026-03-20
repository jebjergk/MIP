# 26. News Intelligence

News Intelligence is MIP's evidence-backed news layer for decision context and policy-aware monitoring.

It is designed for explainability and monitoring, not narrative guesswork or discretionary overrides.

## What It Shows

- **Market context KPIs** — symbols with news, stale symbols, HOT symbols, average snapshot age
- **Reader summary** — deterministic bullets derived from stored features
- **Top headlines** — title + URL only (no full-text republishing)
- **Symbol cards** — badge/count/freshness/uncertainty/novelty/burst per symbol
- **Decision Impact** — proposal-level news influence fields from payload evidence

## Decision Impact Panel

The panel reads proposal payloads and only surfaces rows with real news evidence.

- **Proposals scoped** — number of proposal rows scanned in scope
- **With news context** — proposals carrying `news_context`
- **With news adjustment** — proposals carrying `news_score_adj`
- **Blocked new entry** — proposals gated by news risk/staleness
- **Top impacts table** — only rows with evidence (`news_context`, `news_score_adj`, `news_block_new_entry`, or `news_reasons`)

If there is no valid current news context, the impact table may be empty even when proposals exist.

## Sidebar HOT / Ticker

Navigation includes a compact **HOT** marker for unseen, decision-relevant news.

- Appears when unseen snapshot has HOT symbols and headline evidence
- Clears when you open News Intelligence
- Reappears only when new unseen relevant news arrives

## Guardrails

- No valid article URL -> no headline link contribution
- Mock/feed/XML-style URLs are excluded from context/badges
- Freshness and staleness are explicit in the payload/UI
- News influence is config-gated and bounded
- News context can change ranking/caution behavior, but does not bypass core risk/trust/policy gates

## How to use this page in operations

1. Start with KPI cards to assess freshness and relevance.
2. Inspect HOT symbols and top headlines for context shifts.
3. Open Decision Impact to see where proposals were adjusted or blocked by news policy.
4. Correlate with AI Agent Decisions and Runs before changing policy thresholds.

## Common interpretation mistakes

- **"No rows in impact table means no proposals."**  
  Not always. It can also mean proposals existed but had no qualifying news evidence fields.
- **"HOT means must trade now."**  
  No. HOT means decision-relevant news context is present, not that policy gates are automatically passed.
- **"News can force execution."**  
  No. News context is one input; execution still requires full validation and policy alignment.
