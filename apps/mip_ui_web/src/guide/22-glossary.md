# Key Terms Glossary

| Term | Definition |
|------|------------|
| **Signal** | A detection by a pattern that interesting price action occurred. Not a trade, just evidence. |
| **Pattern** | A named strategy with parameters (for example min return and z-score) that scans market data for signals. |
| **Horizon** | A forward time window (1, 3, 5, 10, 20 bars) used to evaluate what happened after a signal. |
| **Hit Rate** | Percentage of evaluated outcomes that were favorable. |
| **Avg Return** | Average realized return across evaluated outcomes. |
| **Maturity Score** | A quality score showing how complete and reliable the evidence is for a pattern/symbol. |
| **Trust Label** | TRUSTED, WATCH, or UNTRUSTED; used to determine whether signals can influence decisions. |
| **Proposal** | A suggested action generated after trusted-signal and policy checks. |
| **Decision** | Committee verdict and rationale applied to a proposal before execution intent. |
| **Risk Gate** | Safety status controlling whether new entries are allowed. |
| **Revalidation** | Last-moment checks that confirm a proposal is still eligible before execution. |
| **Drawdown** | Percentage decline from a portfolio's peak equity. |
| **Episode** | A lifecycle segment for performance accounting and reporting. |
| **Coverage Ratio** | Fraction of signals with complete evaluable outcomes. |
| **Pipeline** | The recurring process: ingest data, detect signals, evaluate outcomes, update trust, produce decisions and summaries. |
| **Run ID** | Unique identifier for a pipeline run, used for audit and debugging. |
| **Counterfactual** | "What-if" result from an alternative rule setup in Parallel Worlds. |
| **Regret** | Amount a counterfactual outperformed actual results, used for policy review. |
| **Stability Score** | How consistent policy behavior is across time and market regimes. |
| **Learning Ledger** | Evidence-linked history connecting decision logic to realized outcomes. |
| **News Intelligence** | Structured news context that can influence ranking or caution but does not bypass policy gates. |
| **Decision Impact** | Explanation of how context (including news) affected ranking, confidence, or action eligibility. |
| **Activation Guard** | Readiness checks that must pass before live-linked activity can progress. |
| **Live Portfolio Activity** | Operational activity log for the live-linked paper workflow. |
| **Symbol Tracker** | Symbol-first monitoring view used to identify and investigate active symbols. |
| **Performance Dashboard** | Cross-portfolio comparison view for returns, drawdown, and trend behavior. |
| **Server-Sent Events (SSE)** | Streaming protocol used for pushing UI updates without repeated polling. |
