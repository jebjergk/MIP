import { useState, useEffect, useMemo } from 'react'
import { Link } from 'react-router-dom'
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
const SCOPE_TRAINING = 'training_status'

const MIN_RECS_REQUIRED = 10
const MIN_HORIZONS_REQUIRED = 3
const HORIZON_BARS_ORDER = [1, 3, 5, 10, 20]

function stageGlossaryKey(stage) {
  if (!stage) return 'maturity_stage'
  const s = String(stage).toUpperCase()
  if (s === 'INSUFFICIENT') return 'stage_insufficient'
  if (s === 'WARMING_UP') return 'stage_warming_up'
  if (s === 'LEARNING') return 'stage_learning'
  if (s === 'CONFIDENT') return 'stage_confident'
  return 'maturity_stage'
}

function formatPct(n) {
  if (n == null || Number.isNaN(n)) return '—'
  return `${Number(n).toFixed(1)}%`
}

function formatNum(n) {
  if (n == null || Number.isNaN(n)) return '—'
  const x = Number(n)
  return Number.isInteger(x) ? String(x) : x.toFixed(2)
}

/** Maturity proxy when Training Status has no row: sample + horizons + outcomes. */
function maturityProxy(recsTotal, horizonsCovered, outcomesTotal) {
  const recPart = Math.min(25, (recsTotal / 30) * 25)
  const horPart = (horizonsCovered / 5) * 25
  const outPart = Math.min(50, (outcomesTotal / 40) * 50)
  return Math.min(100, Math.round(recPart + horPart + outPart))
}

/** Suggestion score (transparent, deterministic): maturity*0.6 + mean_realized_return at h5*1000*0.2 + pct_positive at h5*100*0.2. */
function computeSuggestionScore(maturity, byHorizon) {
  const h5 = byHorizon.find((h) => h.horizon_bars === 5)
  if (!h5) return maturity * 0.6
  const mean = (h5.mean_realized_return ?? h5.mean_outcome) != null ? Number(h5.mean_realized_return ?? h5.mean_outcome) : 0
  const pct = h5.pct_positive != null ? Number(h5.pct_positive) : 0
  return maturity * 0.6 + mean * 1000 * 0.2 + pct * 100 * 0.2
}

/** Maturity stage label from score (INSUFFICIENT / WARMING_UP / LEARNING / CONFIDENT). */
function maturityStageLabel(score) {
  if (score == null || score < 25) return 'INSUFFICIENT'
  if (score < 50) return 'WARMING_UP'
  if (score < 75) return 'LEARNING'
  return 'CONFIDENT'
}

/** Build Training Status URL filtered to this symbol/pattern. */
function trainingUrl(item) {
  const p = new URLSearchParams()
  if (item?.symbol) p.set('symbol', item.symbol)
  if (item?.market_type) p.set('market_type', item.market_type)
  if (item?.pattern_id != null) p.set('pattern_id', String(item.pattern_id))
  const q = p.toString()
  return q ? `/training?${q}` : '/training'
}

/** Two-line "What history suggests" template. */
function whatHistorySuggests(recsTotal, outcomesTotal, byHorizon) {
  const best = [...(byHorizon || [])].sort((a, b) => (b?.pct_positive ?? 0) - (a?.pct_positive ?? 0))[0]
  const pctVal = best?.pct_positive != null ? Number(best.pct_positive) : null
  const pctStr = pctVal != null ? `${(pctVal * 100).toFixed(1)}%` : '—'
  const meanVal = best?.mean_realized_return ?? best?.mean_outcome
  const meanStr = meanVal != null ? `${(Number(meanVal) * 100).toFixed(2)}%` : '—'
  const h = best?.horizon_bars ?? '?'
  return [
    `Based on ${recsTotal ?? 0} recommendations and ${outcomesTotal ?? 0} evaluated outcomes, positive at ${pctStr} over the strongest horizon (${h} bars).`,
    `Mean return at that horizon: ${meanStr}.`,
  ]
}

/** "Why this is shown" plain-English line (deterministic, no LLM). */
function whyThisIsShown(item, maturity, stage, byHorizon) {
  const h5 = (byHorizon || []).find((h) => h.horizon_bars === 5)
  const meanH5 = h5?.mean_realized_return ?? h5?.mean_outcome
  const pctH5 = h5?.pct_positive != null ? (Number(h5.pct_positive) * 100).toFixed(1) : '—'
  const meanStr = meanH5 != null ? `${(Number(meanH5) * 100).toFixed(2)}%` : '—'
  return `Meets minimums (recs ≥ ${MIN_RECS_REQUIRED}, horizons ≥ ${MIN_HORIZONS_REQUIRED}). Ranked by suggestion score: maturity ${maturity.toFixed(0)} (${stage}), 5-bar mean return ${meanStr}, 5-bar pct positive ${pctH5}%.`
}

export default function Suggestions() {
  const { explainMode } = useExplainMode()
  const [summaryData, setSummaryData] = useState(null)
  const [trainingData, setTrainingData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedItem, setSelectedItem] = useState(null)

  useEffect(() => {
    if (!selectedItem) return
    const onKey = (e) => {
      if (e.key === 'Escape') setSelectedItem(null)
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [selectedItem])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`${API_BASE}/performance/summary`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((summary) => {
        if (cancelled) return
        setSummaryData(summary)
        return fetch(`${API_BASE}/training/status`).then((r) => (r.ok ? r.json() : null)).catch(() => null)
      })
      .then((training) => {
        if (!cancelled && training != null) setTrainingData(training)
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [])

  const trainingByKey = useMemo(() => {
    const rows = trainingData?.rows ?? []
    const map = {}
    rows.forEach((r) => {
      const k = `${r.market_type ?? r.MARKET_TYPE}|${r.symbol ?? r.SYMBOL}|${r.pattern_id ?? r.PATTERN_ID}`
      map[k] = r
    })
    return map
  }, [trainingData])

  const candidates = useMemo(() => {
    const items = summaryData?.items ?? []
    const filtered = items.filter((item) => {
      const recs = item.recs_total ?? 0
      const horizons = typeof item.horizons_covered === 'number' ? item.horizons_covered : (Array.isArray(item.horizons_covered) ? item.horizons_covered.length : 0)
      return recs >= MIN_RECS_REQUIRED && horizons >= MIN_HORIZONS_REQUIRED
    })
    return filtered.map((item) => {
      const key = `${item.market_type}|${item.symbol}|${item.pattern_id}`
      const trainingRow = trainingByKey[key]
      const maturity = trainingRow?.maturity_score != null ? Number(trainingRow.maturity_score) : maturityProxy(item.recs_total, typeof item.horizons_covered === 'number' ? item.horizons_covered : (item.horizons_covered || []).length, item.outcomes_total)
      const score = computeSuggestionScore(maturity, item.by_horizon || [])
      const [line1, line2] = whatHistorySuggests(item.recs_total, item.outcomes_total, item.by_horizon)
      const stage = trainingRow?.maturity_stage ?? maturityStageLabel(maturity)
      const whyShown = whyThisIsShown(item, maturity, stage, item.by_horizon || [])
      return {
        ...item,
        suggestion_score: Math.round(score * 100) / 100,
        maturity_score: maturity,
        maturity_stage: stage,
        what_history_line1: line1,
        what_history_line2: line2,
        why_this_is_shown: whyShown,
      }
    }).sort((a, b) => (b.suggestion_score ?? 0) - (a.suggestion_score ?? 0))
  }, [summaryData, trainingByKey])

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
      <p className="suggestions-intro">
        Ranked symbol/pattern candidates from evaluated outcome history (deterministic score). Explain mode on—tooltips on every metric.
        <InfoTooltip scope={SCOPE_SUG} key="no_trades_notice" variant="short" />
      </p>

      <p className="suggestions-requirements">
        Minimum to appear: recs_total ≥ {MIN_RECS_REQUIRED}
        <InfoTooltip scope={SCOPE_SUG} key="min_recs_required" variant="short" />
        , horizons_covered ≥ {MIN_HORIZONS_REQUIRED}
        <InfoTooltip scope={SCOPE_SUG} key="min_horizons_required" variant="short" />
        .
      </p>

      <p className="suggestions-count">
        Showing {candidates.length} candidate{candidates.length !== 1 ? 's' : ''}.
      </p>

      {candidates.length === 0 ? (
        <EmptyState
          title="Not enough evaluated history"
          action="Run more pipelines to generate recommendations and outcomes, then return here."
          explanation="No symbol/pattern pairs meet the minimum: recs_total ≥ 10 and at least 3 horizons (e.g. 1, 3, 5, 10, 20 bars) with outcome data. Suggestions are derived from /performance/summary (RECOMMENDATION_LOG + RECOMMENDATION_OUTCOMES, daily bars only)."
          reasons={[
            `Minimum required: recs_total ≥ ${MIN_RECS_REQUIRED}, horizons_covered ≥ ${MIN_HORIZONS_REQUIRED}.`,
            'Pipeline has not run enough, or data is for other intervals (we use daily bars only).',
            'Check Training Status to see which triples have sufficient data.',
          ]}
        />
      ) : (
        <div className="suggestions-list">
          {candidates.map((row, i) => (
            <article
              key={`${row.symbol}-${row.pattern_id}-${row.market_type}`}
              className="suggestion-card"
              data-rank={i + 1}
              onClick={() => setSelectedItem(row)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ') && setSelectedItem(row)}
              aria-label={`Open detail for ${row.symbol} pattern ${row.pattern_id}`}
            >
              {/* Row 1: symbol / market / pattern + rank + score */}
              <div className="suggestion-header">
                <span className="suggestion-rank">#{i + 1}</span>
                <span className="suggestion-triple">
                  <span className="suggestion-symbol">{row.symbol ?? '—'}</span>
                  <span className="suggestion-sep">/</span>
                  <span className="suggestion-market">{row.market_type ?? '—'}</span>
                  <span className="suggestion-sep">/</span>
                  <span className="suggestion-pattern">pattern {row.pattern_id ?? '—'}</span>
                </span>
                <span className="suggestion-score-block" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'suggestion_score_formula')?.short : undefined}>
                  <span className="suggestion-score-label">
                    Suggestion score
                    <InfoTooltip scope={SCOPE_SUG} key="suggestion_score" variant="short" />
                  </span>
                  <span className="suggestion-score">{formatNum(row.suggestion_score)}</span>
                </span>
              </div>

              {/* Sample size */}
              <div className="suggestion-sample" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'sample_size')?.short : undefined}>
                Sample size
                <InfoTooltip scope={SCOPE_SUG} key="sample_size" variant="short" />
                : <strong>{formatNum(row.recs_total)}</strong>
              </div>

              {/* Maturity badge + progress bar */}
              <div className="suggestion-maturity" title={explainMode ? getGlossaryEntry(SCOPE_TRAINING, 'maturity_score')?.short : undefined}>
                <span className={`suggestion-stage suggestion-stage-${(row.maturity_stage || '').toLowerCase().replace('_', '-')}`} title={explainMode ? getGlossaryEntry(SCOPE_TRAINING, stageGlossaryKey(row.maturity_stage))?.short : undefined}>
                  {row.maturity_stage ?? '—'}
                  <InfoTooltip scope={SCOPE_TRAINING} key={stageGlossaryKey(row.maturity_stage)} variant="short" />
                </span>
                <div className="suggestion-maturity-bar-wrap" title={explainMode ? getGlossaryEntry(SCOPE_TRAINING, 'maturity_score')?.long : undefined}>
                  <div className="suggestion-maturity-bar" style={{ width: `${Math.min(100, Math.max(0, row.maturity_score ?? 0))}%` }} aria-hidden="true" />
                </div>
              </div>

              {/* What history suggests */}
              <div className="suggestion-what-history" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'what_history_suggests')?.short : undefined}>
                <strong>What history suggests</strong>
                <InfoTooltip scope={SCOPE_SUG} key="what_history_suggests" variant="short" />
                <p className="suggestion-what-line1">{row.what_history_line1}</p>
                <p className="suggestion-what-line2">{row.what_history_line2}</p>
              </div>

              {/* Horizon strip: 1/3/5/10/20 — pct_positive and mean on hover */}
              <div className="suggestion-horizon-strip" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'horizon_strip')?.short : undefined}>
                <span className="suggestion-horizon-strip-label">
                  Horizon strip (1 / 3 / 5 / 10 / 20)
                  <InfoTooltip scope={SCOPE_SUG} key="horizon_strip" variant="short" />
                </span>
                <div className="suggestion-horizon-strip-cells">
                  {HORIZON_BARS_ORDER.map((hb) => {
                    const h = (row.by_horizon || []).find((x) => x.horizon_bars === hb)
                    const pctRaw = h?.pct_positive != null ? Number(h.pct_positive) : null
                    const pct = pctRaw != null ? pctRaw * 100 : null
                    const mean = (h?.mean_realized_return ?? h?.mean_outcome) != null ? Number(h.mean_realized_return ?? h.mean_outcome) * 100 : null
                    const val = pct != null ? pct : mean
                    const cls = val == null ? 'strip-cell empty' : val >= 50 ? 'strip-cell good' : 'strip-cell weak'
                    const hoverText = `${hb} bars: pct_positive ${pct != null ? formatPct(pct) : '—'}, mean_realized_return ${mean != null ? formatPct(mean) : '—'}`
                    return (
                      <div key={hb} className={cls} title={hoverText}>
                        <span className="strip-h">{hb}</span>
                        <span className="strip-v">{val != null ? formatPct(val) : '—'}</span>
                      </div>
                    )
                  })}
                </div>
              </div>

              <div className="suggestion-cross-links" onClick={(e) => e.stopPropagation()}>
                <Link to={trainingUrl(row)} className="suggestion-link">
                  Training Status →
                </Link>
                <Link to="/portfolios" className="suggestion-link" title="Check if you hold this symbol in a portfolio">
                  Portfolio (do I hold it?)
                </Link>
                <Link to="/brief" className="suggestion-link" title="See if this symbol is mentioned in the morning brief">
                  Morning Brief
                </Link>
              </div>
            </article>
          ))}
        </div>
      )}

      {selectedItem && (
        <div
          className="suggestion-drawer-backdrop"
          onClick={() => setSelectedItem(null)}
          onKeyDown={(e) => e.key === 'Escape' && setSelectedItem(null)}
          role="presentation"
        >
          <div
            className="suggestion-drawer"
            onClick={(e) => e.stopPropagation()}
            role="dialog"
            aria-label="Suggestion detail"
          >
            <div className="suggestion-drawer-header">
              <h2>
                {selectedItem.symbol} · pattern {selectedItem.pattern_id} · {selectedItem.market_type}
              </h2>
              <button type="button" className="suggestion-drawer-close" onClick={() => setSelectedItem(null)} aria-label="Close">
                ×
              </button>
            </div>
            <div className="suggestion-drawer-body">
              <div className="suggestion-drawer-cross-links">
                <Link to={trainingUrl(selectedItem)} className="suggestion-link" onClick={() => setSelectedItem(null)}>
                  Training Status (filtered to this symbol/pattern) →
                </Link>
                <Link to="/portfolios" className="suggestion-link" onClick={() => setSelectedItem(null)} title="Check if you hold this symbol">
                  Portfolio (do I hold it?)
                </Link>
                <Link to="/brief" className="suggestion-link" onClick={() => setSelectedItem(null)} title="See if mentioned in brief">
                  Morning Brief
                </Link>
              </div>

              <div className="suggestion-drawer-why" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'why_this_is_shown')?.short : undefined}>
                <strong>Why this is shown</strong>
                <InfoTooltip scope={SCOPE_SUG} key="why_this_is_shown" variant="short" />
                <p>{selectedItem.why_this_is_shown}</p>
              </div>

              <div className="suggestion-drawer-how" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'how_to_interpret')?.short : undefined}>
                <strong>How to interpret</strong>
                <InfoTooltip scope={SCOPE_SUG} key="how_to_interpret" variant="short" />
                <p>{getGlossaryEntry(SCOPE_SUG, 'how_to_interpret')?.long}</p>
              </div>

              <table className="suggestion-drawer-table">
                <thead>
                  <tr>
                    <th>Horizon <InfoTooltip scope={SCOPE_PERF} key="horizon_bars" variant="short" /></th>
                    <th>N <InfoTooltip scope={SCOPE_PERF} key="n" variant="short" /></th>
                    <th>Mean realized <InfoTooltip scope={SCOPE_PERF} key="mean_realized_return" variant="short" /></th>
                    <th>% positive <InfoTooltip scope={SCOPE_PERF} key="pct_positive" variant="short" /></th>
                    <th>% hit <InfoTooltip scope={SCOPE_PERF} key="pct_hit" variant="short" /></th>
                    <th>Min <InfoTooltip scope={SCOPE_PERF} key="min_outcome" variant="short" /></th>
                    <th>Max <InfoTooltip scope={SCOPE_PERF} key="max_outcome" variant="short" /></th>
                  </tr>
                </thead>
                <tbody>
                  {(selectedItem.by_horizon || []).sort((a, b) => (a.horizon_bars ?? 0) - (b.horizon_bars ?? 0)).map((h, j) => (
                    <tr key={j}>
                      <td>{h.horizon_bars} bars</td>
                      <td>{formatNum(h.n)}</td>
                      <td title={explainMode ? getGlossaryEntry(SCOPE_PERF, 'mean_realized_return')?.short : undefined}>
                        {(h.mean_realized_return ?? h.mean_outcome) != null ? `${(Number(h.mean_realized_return ?? h.mean_outcome) * 100).toFixed(2)}%` : '—'}
                      </td>
                      <td title={explainMode ? getGlossaryEntry(SCOPE_PERF, 'pct_positive')?.short : undefined}>
                        {h.pct_positive != null ? `${(Number(h.pct_positive) * 100).toFixed(1)}%` : '—'}
                      </td>
                      <td title={explainMode ? getGlossaryEntry(SCOPE_PERF, 'pct_hit')?.short : undefined}>
                        {h.pct_hit != null ? `${(Number(h.pct_hit) * 100).toFixed(1)}%` : '—'}
                      </td>
                      <td>{(h.min_realized_return ?? h.min_outcome) != null ? `${(Number(h.min_realized_return ?? h.min_outcome) * 100).toFixed(2)}%` : '—'}</td>
                      <td>{(h.max_realized_return ?? h.max_outcome) != null ? `${(Number(h.max_realized_return ?? h.max_outcome) * 100).toFixed(2)}%` : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
