---
title: Research vs live
artifact_refs:
  - issue_ui_broker_sync
  - obj_live_portfolio_config
module_ids: []
---

# Research vs live, evidence limits

Research surfaces optimize for **clarity of evidence**: what the pattern did historically, under which assumptions, with what sample size. Live surfaces optimize for **broker truth and risk**: fills, whole-share constraints, adapter mode, drift, and session freshness.

The UI may briefly disagree with the broker during refresh windows; treat the broker blotter as authoritative for positions and fills. Slippage, delayed quotes, and bracket modeling are configuration-dependent — see Live Portfolio Config and domain knowledge packs for general trading context.
