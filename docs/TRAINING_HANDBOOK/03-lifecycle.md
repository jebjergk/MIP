---
title: Lifecycle — signal to execution
artifact_refs:
  - wf_signal_to_recommendation
  - wf_proposal_to_execution
  - obj_order_proposals
module_ids: []
---

# Lifecycle: signal → recommendation → proposal → execution

End-to-end flow:

1. **Signals** are detected and logged with context (pattern, horizon, direction).
2. **Training** accumulates outcomes and drives maturity/trust.
3. **Recommendations** package candidate actions for review surfaces (e.g. Decision Console).
4. **Proposals** translate approved intent into orders with sizing and brackets.
5. **Viability and committee** checks enforce risk, realism, and policy.
6. **Execution** hits the broker; **Live Portfolio Activity** reconciles truth.

A failure at any stage should be read diagnostically: missing data, gate thresholds, broker constraints, or latency — not as a single “bug” without context.
