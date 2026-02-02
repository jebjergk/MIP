import { useState, useEffect } from 'react'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import InfoTooltip from '../components/InfoTooltip'
import LoadingState from '../components/LoadingState'
import { useExplainMode } from '../context/ExplainModeContext'
import { getGlossaryEntry } from '../data/glossary'
import './Suggestions.css'

const SCOPE_SUG = 'suggestions'
const SCOPE_PERF = 'performance'

const DEFAULT_MIN_SAMPLE = 10

function formatPct(n) {
  if (n == null || Number.isNaN(n)) return '—'
  return `${Number(n).toFixed(1)}%`
}

function formatNum(n) {
  if (n == null || Number.isNaN(n)) return '—'
  const x = Number(n)
  return Number.isInteger(x) ? String(x) : x.toFixed(4)
}

export default function Suggestions() {
  const { explainMode } = useExplainMode()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [minSample, setMinSample] = useState(DEFAULT_MIN_SAMPLE)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`${API_BASE}/performance/suggestions?min_sample=${encodeURIComponent(minSample)}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((d) => {
        if (!cancelled) setData(d)
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [minSample])

  const suggestions = data?.suggestions ?? []
  const totalFiltered = data?.suggestions?.length ?? 0

  if (loading) {
    return (
      <>
        <h1>Suggestions</h1>
        <LoadingState />
      </>
    )
  }
  if (error) {
    return (
      <>
        <h1>Suggestions</h1>
        <ErrorState message={error} />
      </>
    )
  }

  return (
    <>
      <h1>Suggestions</h1>
      {explainMode && (
        <p className="suggestions-intro">
          Ranked symbol/pattern by evaluated outcome history (deterministic score). Minimum sample filter applied. Research guidance only—no trade execution.
          <InfoTooltip scope={SCOPE_SUG} key="no_trades_notice" variant="short" />
        </p>
      )}

      <section className="suggestions-filters" aria-label="Filters">
        <label>
          Min sample
          <InfoTooltip scope={SCOPE_SUG} key="min_sample" variant="short" />
          <input
            type="number"
            min={1}
            max={500}
            value={minSample}
            onChange={(e) => setMinSample(Math.max(1, parseInt(e.target.value, 10) || DEFAULT_MIN_SAMPLE))}
            aria-label="Minimum sample size"
          />
        </label>
      </section>

      <p className="suggestions-count">
        Showing {totalFiltered} suggestion{totalFiltered !== 1 ? 's' : ''} (min outcomes ≥ {minSample}).
      </p>

      <div className="suggestions-list">
        {suggestions.map((row, i) => (
          <article key={`${row.symbol}-${row.pattern_id}-${row.market_type}`} className="suggestion-card" data-rank={i + 1}>
            <div className="suggestion-header">
              <span className="suggestion-rank">#{i + 1}</span>
              <span className="suggestion-symbol">{row.symbol ?? '—'}</span>
              <span className="suggestion-pattern">pattern {row.pattern_id ?? '—'}</span>
              <span className="suggestion-market">{row.market_type ?? '—'}</span>
              <span
                className="suggestion-score"
                title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'rank_score')?.short : undefined}
              >
                Score: {formatNum(row.rank_score)}
              </span>
              <InfoTooltip scope={SCOPE_SUG} key="rank_score" variant="short" />
            </div>
            <div className="suggestion-metrics">
              <span title={explainMode ? getGlossaryEntry('training_status', 'recs_total')?.short : undefined}>
                Recs: {formatNum(row.n_recs)}
              </span>
              <InfoTooltip scope="training_status" key="recs_total" variant="short" />
              <span title={explainMode ? getGlossaryEntry(SCOPE_PERF, 'mean_outcome')?.short : undefined}>
                Outcomes: {formatNum(row.n_outcomes)}
              </span>
              <InfoTooltip scope={SCOPE_PERF} key="mean_outcome" variant="short" />
              <span title={explainMode ? getGlossaryEntry(SCOPE_PERF, 'pct_positive')?.short : undefined}>
                Positive: {row.pct_positive != null ? `${row.pct_positive}%` : '—'}
              </span>
              <InfoTooltip scope={SCOPE_PERF} key="pct_positive" variant="short" />
              {row.best_horizon_bars != null && (
                <>
                  <span title={explainMode ? getGlossaryEntry('training_status', 'horizons_covered')?.short : undefined}>
                    Best H: {row.best_horizon_bars} bars
                  </span>
                  <InfoTooltip scope="training_status" key="horizons_covered" variant="short" />
                </>
              )}
              {row.best_mean_outcome != null && (
                <>
                  <span title={explainMode ? getGlossaryEntry(SCOPE_PERF, 'mean_outcome')?.short : undefined}>
                    Best mean: {row.best_mean_outcome}%
                  </span>
                  <InfoTooltip scope={SCOPE_PERF} key="mean_outcome" variant="short" />
                </>
              )}
            </div>
            <p className="suggestion-explanation" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'explanation_summary')?.short : undefined}>
              {row.explanation ?? '—'}
              <InfoTooltip scope={SCOPE_SUG} key="explanation_summary" variant="short" />
            </p>
            {row.horizons?.length > 0 && (
              <details className="suggestion-horizons-details">
                <summary>Per-horizon metrics</summary>
                <table className="suggestion-horizons-table">
                  <thead>
                    <tr>
                      <th>Horizon <InfoTooltip scope={SCOPE_PERF} key="mean_outcome" variant="short" /></th>
                      <th>N <InfoTooltip scope="training_status" key="recs_total" variant="short" /></th>
                      <th>Mean <InfoTooltip scope={SCOPE_PERF} key="mean_outcome" variant="short" /></th>
                      <th>% pos <InfoTooltip scope={SCOPE_PERF} key="pct_positive" variant="short" /></th>
                      <th>Min <InfoTooltip scope={SCOPE_PERF} key="min_outcome" variant="short" /></th>
                      <th>Max <InfoTooltip scope={SCOPE_PERF} key="max_outcome" variant="short" /></th>
                    </tr>
                  </thead>
                  <tbody>
                    {row.horizons.map((h, j) => (
                      <tr key={j}>
                        <td>{h.horizon_bars} bars</td>
                        <td>{formatNum(h.n_outcomes)}</td>
                        <td>{h.mean_outcome != null ? `${(Number(h.mean_outcome) * 100).toFixed(2)}%` : '—'}</td>
                        <td>{h.pct_positive != null ? `${(Number(h.pct_positive) * 100).toFixed(1)}%` : '—'}</td>
                        <td>{h.min_outcome != null ? `${(Number(h.min_outcome) * 100).toFixed(2)}%` : '—'}</td>
                        <td>{h.max_outcome != null ? `${(Number(h.max_outcome) * 100).toFixed(2)}%` : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </details>
            )}
          </article>
        ))}
      </div>

      {suggestions.length === 0 && (
        <EmptyState
          title="No suggestions match"
          action="Lower the min sample above or run more pipelines to generate recommendations and outcomes."
          explanation="No symbol/pattern pairs meet the minimum outcomes count. Suggestions are built from evaluated history (RECOMMENDATION_LOG + RECOMMENDATION_OUTCOMES)."
          reasons={['Pipeline has not run yet.', 'No data in MIP.APP.RECOMMENDATION_LOG / RECOMMENDATION_OUTCOMES.', 'Min sample filter is too high.']}
        />
      )}
    </>
  )
}
