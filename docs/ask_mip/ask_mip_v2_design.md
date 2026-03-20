# Ask MIP v2 Technical Design

## Objectives

- Keep MIP-local truth authoritative.
- Answer from MIP docs first, then glossary.
- Use external clarification only for general market concepts.
- Return provenance and confidence with every answer.
- Log unresolved terms and retrieval gaps to drive glossary curation.

## Source Priority

1. `DOC` (MIP guide and UX docs)
2. `GLOSSARY` (approved MIP glossary terms and aliases)
3. `WEB` (general clarification only when policy allows)
4. `INFERENCE` (explicitly marked when coverage is weak)

## Retrieval And Resolution Contract

`POST /ask/v2` request:
- `question`
- `route`
- `history[]`

`POST /ask/v2` response:
- `answer` (backward-compatible markdown summary)
- `sections[]` (`mip_specific`, `terminology`, `general_clarification`, `uncertainty_note`)
- `sources[]` with provenance (`DOC`, `GLOSSARY`, `WEB`, `INFERENCE`)
- `confidence` (`docs_confidence`, `glossary_confidence`, `web_confidence`, `overall`)
- `did_you_mean[]`
- `unknown_terms[]`
- `fallback_used`

## Policy Rules

- Web fallback allowed only when:
  - docs and glossary confidence are below configured thresholds,
  - intent is web-eligible (`term_definition`, `trading_concept`, `market_research_concept`, `mixed`),
  - query is not asking for MIP-internal formulas/thresholds/state claims.
- Web fallback blocked for:
  - undocumented MIP-specific behavior,
  - internal thresholds/formulas/feature-state assertions.
- External clarification cannot override doc-backed MIP behavior.

## Storage Design

- Canonical glossary and aliases in `MIP.APP`.
- Review/telemetry and unresolved backlog in `MIP.AGENT_OUT`.
- Coverage rollups in `MIP.MART` views.

## Rollout

1. Docs + glossary only.
2. Controlled web clarification for narrow intent classes.
3. Provenance badges and did-you-mean UI.
4. Glossary admin and telemetry dashboards.
