# Ask MIP human evaluation rubric

Use for periodic quality review when automated retrieval tests pass but answer quality must be judged.

## Dimensions (0–2 each)

| Dimension | 0 | 1 | 2 |
|-----------|---|---|---|
| **Correctness** | Wrong or unsafe | Mostly right, minor gaps | Accurate for MIP + context |
| **Grounding** | Invents internals | Mixes inference without labeling | Clear app vs general vs inference |
| **UI pointers** | Missing | Vague | Specific route + panel |
| **Uncertainty** | Overconfident | OK | Appropriately hedged |
| **Usefulness** | Off-topic | Adequate | Actionable for the user |

## Sampling

- At least **5** questions per release from `scenarios.yaml`.
- At least **5** ad-hoc questions from active pages (Cockpit, Training, Living Chart, Decision Console, Live Activity).
- Record `intent`, `source_types`, and model name.

## Failure triggers

- States a threshold or formula not present in artifacts or glossary.
- Contradicts page contract without calling out drift.
- Presents general trading advice as MIP configuration.
