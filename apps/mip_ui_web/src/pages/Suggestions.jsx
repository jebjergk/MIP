import { useState, useEffect, useMemo } from 'react'
import { API_BASE } from '../App'
import ErrorState from '../components/ErrorState'
import InfoTooltip from '../components/InfoTooltip'
import LoadingState from '../components/LoadingState'
import { useExplainMode } from '../context/ExplainModeContext'
import { getGlossaryEntry } from '../data/glossary'
import './Suggestions.css'

const SCOPE_TS = 'training_status'
const SCOPE_SUG = 'suggestions'

const MIN_SAMPLE = 10

function get(row, k) {
  return row[k] ?? row[k?.toUpperCase()]
}

function formatPct(n) {
  if (n == null || Number.isNaN(n)) return '—'
  return `${(Number(n) * 100).toFixed(1)}%`
}

function formatNum(n) {
  if (n == null || Number.isNaN(n)) return '—'
  const x = Number(n)
  return Number.isInteger(x) ? String(x) : x.toFixed(4)
}

function stageGlossaryKey(stage) {
  if (!stage) return 'maturity_stage'
  const s = String(stage).toUpperCase()
  if (s === 'INSUFFICIENT') return 'stage_insufficient'
  if (s === 'WARMING_UP') return 'stage_warming_up'
  if (s === 'LEARNING') return 'stage_learning'
  if (s === 'CONFIDENT') return 'stage_confident'
  return 'maturity_stage'
}

/** Count how many of avg_outcome_h1..h20 are present and positive. */
function countPositiveHorizons(row) {
  let count = 0
  for (const h of ['avg_outcome_h1', 'avg_outcome_h3', 'avg_outcome_h5', 'avg_outcome_h10', 'avg_outcome_h20']) {
    const v = get(row, h)
    if (v != null && !Number.isNaN(Number(v)) && Number(v) > 0) count++
  }
  return count
}

/** Deterministic plain-English explanation (no LLM). */
function buildExplanation(row) {
  const symbol = get(row, 'symbol') ?? 'Symbol'
  const patternId = get(row, 'pattern_id') ?? '—'
  const recs = get(row, 'recs_total')
  const coverage = get(row, 'coverage_ratio')
  const stage = get(row, 'maturity_stage') ?? '—'
  const positiveHorizons = countPositiveHorizons(row)
  const recsStr = recs != null ? String(recs) : 'some'
  const coverageStr = coverage != null ? formatPct(coverage) : '—'
  return `${symbol} (pattern ${patternId}) has ${recsStr} recommendations and ${coverageStr} outcome coverage. Maturity: ${stage}. Average outcomes are positive for ${positiveHorizons} of 5 horizons.`
}

export default function Suggestions() {
  const { explainMode } = useExplainMode()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [minSample, setMinSample] = useState(MIN_SAMPLE)

  useEffect(() => {
    let cancelled = false
    fetch(`${API_BASE}/training/status`)
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
  }, [])

  const rows = data?.rows ?? []
  const ranked = useMemo(() => {
    const filtered = rows.filter((r) => {
      const n = get(r, 'recs_total')
      return n != null && Number(n) >= minSample
    })
    return [...filtered].sort((a, b) => {
      const sa = Number(get(a, 'maturity_score')) || 0
      const sb = Number(get(b, 'maturity_score')) || 0
      if (sb !== sa) return sb - sa
      const ra = Number(get(a, 'recs_total')) || 0
      const rb = Number(get(b, 'recs_total')) || 0
      return rb - ra
    })
  }, [rows, minSample])

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
          Ranked symbol/pattern by data maturity (deterministic score). Minimum sample filter applied. Informational only—no trade execution.
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
            onChange={(e) => setMinSample(Math.max(1, parseInt(e.target.value, 10) || MIN_SAMPLE))}
            aria-label="Minimum sample size"
          />
        </label>
      </section>

      <p className="suggestions-count">
        Showing {ranked.length} of {rows.length} rows (min sample ≥ {minSample}).
      </p>

      <div className="suggestions-list">
        {ranked.map((row, i) => {
          const stage = get(row, 'maturity_stage')
          const stageKey = stageGlossaryKey(stage)
          const stageTitle = explainMode ? (getGlossaryEntry(SCOPE_TS, stageKey)?.short ?? stage) : undefined
          const explanation = buildExplanation(row)
          return (
            <article key={i} className="suggestion-card" data-rank={i + 1}>
              <div className="suggestion-header">
                <span className="suggestion-rank">#{i + 1}</span>
                <span className="suggestion-symbol">{get(row, 'symbol') ?? '—'}</span>
                <span className="suggestion-pattern">pattern {get(row, 'pattern_id') ?? '—'}</span>
                <span className="suggestion-market">{get(row, 'market_type') ?? '—'}</span>
                <span
                  className={`suggestion-stage suggestion-stage-${(stage || '').toLowerCase().replace('_', '-')}`}
                  title={stageTitle}
                >
                  {stage ?? '—'}
                </span>
                <InfoTooltip scope={SCOPE_TS} key={stageKey} variant="short" />
                <span className="suggestion-score" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'rank_score')?.short : undefined}>
                  Score: {formatNum(get(row, 'maturity_score'))}
                </span>
                <InfoTooltip scope={SCOPE_SUG} key="rank_score" variant="short" />
              </div>
              <div className="suggestion-metrics">
                <span title={explainMode ? getGlossaryEntry(SCOPE_TS, 'recs_total')?.short : undefined}>
                  Sample: {formatNum(get(row, 'recs_total'))}
                </span>
                <InfoTooltip scope={SCOPE_TS} key="recs_total" variant="short" />
                <span title={explainMode ? getGlossaryEntry(SCOPE_TS, 'coverage_ratio')?.short : undefined}>
                  Coverage: {formatPct(get(row, 'coverage_ratio'))}
                </span>
                <InfoTooltip scope={SCOPE_TS} key="coverage_ratio" variant="short" />
                <span title={explainMode ? getGlossaryEntry(SCOPE_TS, 'horizons_covered')?.short : undefined}>
                  Horizons: {formatNum(get(row, 'horizons_covered'))}
                </span>
                <InfoTooltip scope={SCOPE_TS} key="horizons_covered" variant="short" />
              </div>
              <p className="suggestion-explanation" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'explanation_summary')?.short : undefined}>
                {explanation}
                <InfoTooltip scope={SCOPE_SUG} key="explanation_summary" variant="short" />
              </p>
            </article>
          )
        })}
      </div>

      {ranked.length === 0 && (
        <p className="suggestions-empty">
          No rows meet the minimum sample size. Lower the min sample or run more pipelines to generate recommendations.
        </p>
      )}
    </>
  )
}
