---
title: Philosophy and mental model
artifact_refs:
  - wf_signal_to_recommendation
  - wf_proposal_to_execution
module_ids: []
---

# Philosophy and mental model

MIP is built around **evidence-first automation**: patterns are detected, outcomes are measured, and only after enough stable history do signals earn trust for operational workflows. The UI separates **research intelligence** (what happened historically, how confident we are) from **live execution** (what the broker can actually do under constraints).

When interpreting any screen, ask:

1. **Grain** — Is this symbol-local, pattern-level, or portfolio-level?
2. **Time** — Is this as-of a pipeline run, live now, or simulated?
3. **Gate** — Is this a hard block, a soft warning, or narrative context?

Cross-check apparent contradictions across pages using the workflow registry in Ask MIP: the same symbol can be “interesting” in research while correctly blocked in live if viability or trust differs.
