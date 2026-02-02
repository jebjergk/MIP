# Plain-language rules for UX copy

All tooltips, Explain Center content, and glossary entries should read like they’re written for a **smart non-trader**. Follow these rules so the product stays interpretable and trustworthy.

## Style rules

1. **Max one sentence per bullet.**  
   Keep each bullet or list item to a single, complete sentence. If you need a second idea, use a second bullet.

2. **Avoid jargon.**  
   Do not use terms such as: alpha, beta, Sharpe ratio, volatility clustering, momentum factor, risk parity, drawdown duration, Calmar, Sortino, information ratio, tracking error, basis points (prefer “percent” or “percentage points” where possible), unless you define them in plain language in the same context.

3. **Always include What / Why / How (and optionally Next).**
   - **What:** What this thing is (one clear sentence).
   - **Why:** Why we show it or why it matters to the user.
   - **How:** How it’s computed or where the data comes from (in simple terms).
   - **Next:** Optional; what the user can do next (e.g. “Use Training Status to see which symbol/patterns have enough data.”).

4. **Prefer active voice and “you” where it helps.**  
   “You can filter by symbol” is clearer than “The table can be filtered by symbol.”

5. **No marketing fluff.**  
   Be direct. Avoid phrases like “leverage,” “powerful,” “seamless,” “robust” unless they carry a precise, explainable meaning.

6. **Data lineage in plain language.**  
   When referencing Snowflake objects, use the canonical names (e.g. `MIP.APP.RECOMMENDATION_OUTCOMES`) and add one short sentence on purpose: e.g. “Stores evaluated outcomes: realized return and hit flag per recommendation and horizon.”

## Glossary and Explain Center

- **Glossary entries** (e.g. in `docs/ux/UX_METRIC_GLOSSARY.yml`) should include:
  - `short`: One line (for tooltips).
  - `long`: What / Why / How in a few sentences.
  - Optional: `what`, `why`, `how`, `next` for structured display in the Explain Drawer and tooltips.

- **Explain Center** page/section contexts must provide:
  - `what`, `why`, `how` (plain-language paragraphs).
  - `sources`: Only canonical Snowflake objects; each with a one-sentence `purpose`.
  - `fields`: Explicit list of fields with `label`, `meaning`, and optional `calc` (how computed).

- **InfoTooltip** and the Explain Drawer renderer should display these sections consistently (e.g. when `what`/`why`/`how` are present, show them under clear headings).

## Acceptance

- Every tooltip and explain section reads like it’s written for a smart non-trader.
- No unexplained jargon.
- What / Why / How (and optional Next) are present where applicable.
- Data sources use only canonical object names and a brief, plain-language purpose.
