# MIP Training Handbook

Deeper conceptual companion to the in-app **User Guide** (`mip_ui_web/src/guide/`). These chapters explain *why* MIP behaves as it does: training evidence, trust, proposals, live realism, and research vs production.

## Alignment

- Each chapter lists `artifact_refs` (knowledge YAML `artifact_id` values) in frontmatter.
- Machine-retrievable summaries live in [`MIP/knowledge/ask_mip/handbook_modules/`](../../knowledge/ask_mip/handbook_modules/).
- Validate knowledge artifacts: `python scripts/validate_knowledge.py` (repo root).

## Chapters

1. [Philosophy and mental model](01-philosophy.md)
2. [Trust, maturity, and qualification](02-trust-and-maturity.md)
3. [Lifecycle: signal → recommendation → proposal → execution](03-lifecycle.md)
4. [Research vs live, evidence limits](04-research-vs-live.md)
5. [Glossary and concept map](05-glossary.md)
