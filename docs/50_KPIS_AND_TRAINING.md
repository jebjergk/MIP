# KPIs & Training Metrics

## What is measured (plain language)
- **Coverage / maturity**: whether enough future bars exist to evaluate a recommendation at a given horizon. In `REC_OUTCOME_COVERAGE`, `N_SUCCESS` counts outcomes with `EVAL_STATUS='SUCCESS'`, and `COVERAGE_RATE` is `N_SUCCESS / N_TOTAL`. This acts as a “maturity” indicator for how much of the recommendation set has fully evaluated outcomes.【F:SQL/mart/030_mart_rec_outcome_views.sql†L8-L27】
- **Return metrics**: `REC_OUTCOME_PERF` aggregates realized returns (average, median, min/max) for matured outcomes to show performance after the fact.【F:SQL/mart/030_mart_rec_outcome_views.sql†L29-L55】
- **Hit rate**: `REC_OUTCOME_PERF` computes `HIT_RATE` as the average of boolean `HIT_FLAG` values. `HIT_FLAG` is set when a realized return meets the minimum return threshold (default 0.0 unless specified).【F:SQL/mart/030_mart_rec_outcome_views.sql†L29-L55】【F:SQL/app/105_sp_evaluate_recommendations.sql†L92-L124】
- **Score correlation**: `REC_OUTCOME_PERF` computes `SCORE_RETURN_CORR` (correlation between recommendation score and realized return). A positive value means higher scores tend to align with better outcomes; a negative value implies the opposite.【F:SQL/mart/030_mart_rec_outcome_views.sql†L29-L55】
- **Training KPIs**: `REC_TRAINING_KPIS` adds expectancy, volatility, and recent-period (30/90-day) stats for a more operational view of pattern behavior.【F:SQL/mart/020_mart_rec_training_kpis.sql†L1-L109】

## Hit rate threshold semantics
- `SP_EVALUATE_RECOMMENDATIONS` sets `HIT_FLAG` using a minimum return threshold (`MIN_RETURN_THRESHOLD`) and labels each outcome with `HIT_RULE='THRESHOLD'`. This means a “hit” is simply any realized return that meets or exceeds the configured threshold.【F:SQL/app/105_sp_evaluate_recommendations.sql†L92-L124】

## How to read KPI outputs (non-trader friendly)
- **Coverage rate near 1.0**: most recommendations have “matured,” meaning the future bars needed for evaluation exist. This is a data completeness signal, not a performance signal.【F:SQL/mart/030_mart_rec_outcome_views.sql†L8-L27】
- **Average return**: shows the typical realized outcome after the recommendation date. Positive means the recommendations tended to be followed by price increases; negative means the opposite.【F:SQL/mart/030_mart_rec_outcome_views.sql†L29-L55】
- **Hit rate**: a simple success rate based on a minimum return threshold. If the threshold is 0.0, a hit means any non-negative outcome.【F:SQL/app/105_sp_evaluate_recommendations.sql†L92-L124】
- **Score correlation**: indicates whether a higher model score translates to better outcomes. A value near zero implies the score is not very informative in its current form.【F:SQL/mart/030_mart_rec_outcome_views.sql†L29-L55】

## Known unknowns / TODO
- **Missing from repo:** None identified for the objects explicitly requested in this documentation pack.
