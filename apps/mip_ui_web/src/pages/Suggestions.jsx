import { useState, useEffect, useMemo } from 'react'
import { Link } from 'react-router-dom'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'
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

/** Best horizon (bars) by pct_positive for "Why" micro-copy. */
function bestHorizonBars(byHorizon) {
  const best = [...(byHorizon || [])].sort((a, b) => (b?.pct_positive ?? 0) - (a?.pct_positive ?? 0))[0]
  return best?.horizon_bars ?? 5
}

/** Drawer micro-copy: What (n, horizon in days, mean %, pct %). */
function drawerWhat(item, horizonBars) {
  const n = item.outcomes_total ?? 0
  const h = (item.by_horizon || []).find((x) => x.horizon_bars === horizonBars)
  const meanVal = (h?.mean_realized_return ?? h?.mean_outcome) != null ? Number(h.mean_realized_return ?? h.mean_outcome) * 100 : null
  const pctVal = h?.pct_positive != null ? Number(h.pct_positive) * 100 : null
  const mean = meanVal != null ? meanVal.toFixed(2) : '—'
  const pct = pctVal != null ? pctVal.toFixed(1) : '—'
  return { n, horizon: horizonBars, mean, pct }
}

/** Build histogram bins from raw values (decimal returns). */
function buildHistogramBins(values, numBins = 20) {
  if (!values.length) return []
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 0.01
  const bins = Array.from({ length: numBins }, (_, i) => {
    const left = min + (i / numBins) * range
    const right = min + ((i + 1) / numBins) * range
    const mid = (left + right) / 2
    return {
      bin: i,
      left,
      right,
      mid,
      count: 0,
      label: `${(mid * 100).toFixed(1)}%`,
    }
  })
  values.forEach((v) => {
    if (v < min || v > max) return
    const i = v === max ? numBins - 1 : Math.floor((v - min) / range * numBins)
    if (i >= 0 && i < numBins) bins[i].count += 1
  })
  return bins
}

export default function Suggestions() {
  const { explainMode } = useExplainMode()
  const [summaryData, setSummaryData] = useState(null)
  const [trainingData, setTrainingData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedItem, setSelectedItem] = useState(null)
  const [selectedHorizon, setSelectedHorizon] = useState(5)
  const [distributionValues, setDistributionValues] = useState([])
  const [distributionLoading, setDistributionLoading] = useState(false)
  const [distributionError, setDistributionError] = useState(null)

  useEffect(() => {
    if (!selectedItem) return
    setSelectedHorizon(5)
    setDistributionValues([])
    setDistributionError(null)
  }, [selectedItem])

  useEffect(() => {
    if (!selectedItem) return
    let cancelled = false
    setDistributionLoading(true)
    setDistributionError(null)
    const params = new URLSearchParams()
    if (selectedItem.market_type) params.set('market_type', selectedItem.market_type)
    if (selectedItem.symbol) params.set('symbol', selectedItem.symbol)
    if (selectedItem.pattern_id != null) params.set('pattern_id', String(selectedItem.pattern_id))
    params.set('horizon_bars', String(selectedHorizon))
    params.set('limit', '2000')
    fetch(`${API_BASE}/performance/distribution?${params}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((data) => {
        if (!cancelled) setDistributionValues(data.realized_returns ?? [])
      })
      .catch((e) => {
        if (!cancelled) setDistributionError(e.message)
      })
      .finally(() => {
        if (!cancelled) setDistributionLoading(false)
      })
    return () => { cancelled = true }
  }, [selectedItem, selectedHorizon])

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

              {/* Sparkline-like horizon strip + full strip with values */}
              <div className="suggestion-horizon-strip" title={explainMode ? getGlossaryEntry(SCOPE_SUG, 'horizon_strip')?.short : undefined}>
                <span className="suggestion-horizon-strip-label">
                  Horizon strip (1 / 3 / 5 / 10 / 20)
                  <InfoTooltip scope={SCOPE_SUG} key="horizon_strip" variant="short" />
                </span>
                <div className="suggestion-sparkline" aria-hidden="true">
                  {HORIZON_BARS_ORDER.map((hb) => {
                    const h = (row.by_horizon || []).find((x) => x.horizon_bars === hb)
                    const mean = (h?.mean_realized_return ?? h?.mean_outcome) != null ? Number(h.mean_realized_return ?? h.mean_outcome) * 100 : null
                    const pct = h?.pct_positive != null ? Number(h.pct_positive) * 100 : null
                    const val = mean != null ? mean : (pct != null ? pct : 0)
                    const heightPct = val == null ? 0 : Math.min(100, Math.max(0, 50 + val))
                    const barCls = val == null ? 'suggestion-sparkline-bar' : val >= 0 ? 'suggestion-sparkline-bar has-value positive' : 'suggestion-sparkline-bar has-value negative'
                    const hoverText = `${hb} days: mean ${mean != null ? formatPct(mean) : '—'}, pct positive ${pct != null ? formatPct(pct) : '—'}`
                    return (
                      <div key={hb} className={barCls} style={{ height: `${heightPct}%` }} title={hoverText} />
                    )
                  })}
                </div>
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

      {selectedItem && (() => {
        const trainingRow = trainingByKey[`${selectedItem.market_type}|${selectedItem.symbol}|${selectedItem.pattern_id}`]
        const whatData = drawerWhat(selectedItem, selectedHorizon)
        const bestH = bestHorizonBars(selectedItem.by_horizon || [])
        const horizonChartData = (selectedItem.by_horizon || [])
          .filter((h) => HORIZON_BARS_ORDER.includes(h.horizon_bars))
          .sort((a, b) => (a.horizon_bars ?? 0) - (b.horizon_bars ?? 0))
          .map((h) => ({
            days: h.horizon_bars,
            mean_pct: (h.mean_realized_return ?? h.mean_outcome) != null ? Number(h.mean_realized_return ?? h.mean_outcome) * 100 : null,
            pct_positive: h.pct_positive != null ? Number(h.pct_positive) * 100 : null,
          }))
        const distBins = buildHistogramBins(distributionValues)
        const distMean = distributionValues.length ? distributionValues.reduce((a, b) => a + b, 0) / distributionValues.length : null
        const sortedDist = [...distributionValues].sort((a, b) => a - b)
        const distMedian = sortedDist.length ? sortedDist[Math.floor(sortedDist.length / 2)] : null
        const coverageRatio = trainingRow?.coverage_ratio != null ? Number(trainingRow.coverage_ratio) : null
        const reasons = trainingRow?.reasons ?? (Array.isArray(trainingRow?.reasons) ? trainingRow.reasons : [])

        return (
          <div
            className="suggestion-drawer-backdrop"
            onClick={() => setSelectedItem(null)}
            onKeyDown={(e) => e.key === 'Escape' && setSelectedItem(null)}
            role="presentation"
          >
            <div
              className="suggestion-drawer suggestion-drawer-evidence"
              onClick={(e) => e.stopPropagation()}
              role="dialog"
              aria-label="Evidence drawer"
            >
              <div className="suggestion-drawer-header">
                <h2>
                  {selectedItem.symbol} · {selectedItem.market_type} · pattern {selectedItem.pattern_id}
                </h2>
                <button type="button" className="suggestion-drawer-close" onClick={() => setSelectedItem(null)} aria-label="Close">
                  ×
                </button>
              </div>
              <div className="suggestion-drawer-body">
                <div className="suggestion-drawer-cross-links">
                  <Link to={trainingUrl(selectedItem)} className="suggestion-link" onClick={() => setSelectedItem(null)}>
                    Training Status →
                  </Link>
                  <Link to="/portfolios" className="suggestion-link" onClick={() => setSelectedItem(null)}>Portfolio</Link>
                  <Link to="/brief" className="suggestion-link" onClick={() => setSelectedItem(null)}>Morning Brief</Link>
                </div>

                {/* Section A: Plain language explanation (micro-copy) */}
                <section className="evidence-section evidence-explanations" aria-label="Explanations">
                  <div className="evidence-what">
                    <h3>What</h3>
                    {explainMode && <InfoTooltip scope={SCOPE_SUG} key="what_history_suggests" variant="short" />}
                    <p>
                      We evaluated this pattern <InfoTooltip scope={SCOPE_TRAINING} key="pattern_id" variant="short" />
                      {' '}<strong>{whatData.n}</strong> <InfoTooltip scope={SCOPE_SUG} key="sample_size" variant="short" />
                      {' '}times on daily data. At <strong>{whatData.horizon} days</strong> <InfoTooltip scope={SCOPE_PERF} key="horizon_days" variant="short" />
                      , the average <strong>realized return</strong> <InfoTooltip scope={SCOPE_PERF} key="realized_return" variant="short" />
                      {' '}was <strong>{whatData.mean}%</strong>, and outcomes were positive <strong>{whatData.pct}%</strong> of the time{' '}
                      <InfoTooltip scope={SCOPE_PERF} key="pct_positive" variant="short" />.
                    </p>
                  </div>
                  <div className="evidence-why">
                    <h3>Why</h3>
                    {explainMode && <InfoTooltip scope={SCOPE_SUG} key="why_this_is_shown" variant="short" />}
                    <p>
                      This appears because it has <strong>{selectedItem.maturity_stage ?? '—'} training maturity</strong>{' '}
                      <InfoTooltip scope={SCOPE_TRAINING} key="maturity_stage" variant="short" />
                      {' '}and shows its strongest results around <strong>{bestH} days</strong>.
                    </p>
                  </div>
                  <div className="evidence-how">
                    <h3>How</h3>
                    {explainMode && <InfoTooltip scope={SCOPE_SUG} key="how_to_interpret" variant="short" />}
                    <p>
                      <strong>Realized return</strong> <InfoTooltip scope={SCOPE_PERF} key="realized_return" variant="short" />
                      {' '}is computed from the <strong>entry price</strong> to the <strong>exit price</strong> at the chosen{' '}
                      <strong>horizon</strong> <InfoTooltip scope={SCOPE_PERF} key="horizon_bars" variant="short" />
                      . <strong>Hit rate</strong> <InfoTooltip scope={SCOPE_PERF} key="hit_rate" variant="short" />
                      {' '}shows how often the move exceeded the minimum threshold.
                    </p>
                  </div>
                </section>

                {/* Section B: Charts */}
                <section className="evidence-section evidence-charts" aria-label="Charts">
                  <h3>Horizon strip (mean return by days)</h3>
                  {explainMode && <InfoTooltip scope={SCOPE_SUG} key="horizon_strip" variant="short" />}
                  <div className="evidence-horizon-chart">
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart data={horizonChartData} margin={{ top: 8, right: 8, bottom: 24, left: 8 }}>
                        <XAxis dataKey="days" tickFormatter={(v) => `${v} days`} />
                        <YAxis tickFormatter={(v) => `${v}%`} />
                        <Tooltip formatter={(v) => [v != null ? `${Number(v).toFixed(2)}%` : '—', 'Mean return']} labelFormatter={(l) => `${l} days`} />
                        <Bar dataKey="mean_pct" name="Mean return" fill="#1976d2" radius={[4, 4, 0, 0]}>
                          {horizonChartData.map((_, i) => (
                            <Cell key={i} fill={horizonChartData[i].mean_pct >= 0 ? '#2e7d32' : '#c62828'} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <div className="evidence-pct-chart">
                    <span className="evidence-chart-label">% positive by horizon</span>
                    <ResponsiveContainer width="100%" height={120}>
                      <BarChart data={horizonChartData} margin={{ top: 4, right: 8, bottom: 24, left: 8 }}>
                        <XAxis dataKey="days" tickFormatter={(v) => `${v}d`} />
                        <YAxis domain={[0, 100]} tickFormatter={(v) => `${v}%`} />
                        <Tooltip formatter={(v) => [v != null ? `${Number(v).toFixed(1)}%` : '—', '% positive']} labelFormatter={(l) => `${l} days`} />
                        <Bar dataKey="pct_positive" name="% positive" fill="#1565c0" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>

                  <h3>Distribution of realized return ({selectedHorizon} days)</h3>
                  {explainMode && <InfoTooltip scope={SCOPE_PERF} key="realized_return" variant="short" />}
                  <div className="evidence-horizon-selector">
                    {HORIZON_BARS_ORDER.map((hb) => (
                      <button
                        key={hb}
                        type="button"
                        className={selectedHorizon === hb ? 'evidence-horizon-btn active' : 'evidence-horizon-btn'}
                        onClick={() => setSelectedHorizon(hb)}
                      >
                        {hb} days
                      </button>
                    ))}
                  </div>
                  {distributionLoading && <LoadingState />}
                  {distributionError && <ErrorState message={distributionError} />}
                  {!distributionLoading && !distributionError && distributionValues.length === 0 && (
                    <p className="evidence-empty">No distribution data for this horizon.</p>
                  )}
                  {!distributionLoading && !distributionError && distributionValues.length > 0 && (
                    <div className="evidence-dist-chart">
                      <ResponsiveContainer width="100%" height={240}>
                        <BarChart data={distBins} margin={{ top: 8, right: 8, bottom: 24, left: 8 }}>
                          <XAxis dataKey="label" interval={Math.max(0, Math.floor(distBins.length / 10))} />
                          <YAxis />
                          <Tooltip formatter={(v) => [v, 'Count']} labelFormatter={(l) => `Return: ${l}`} />
                          <Bar dataKey="count" name="Count" fill="#37474f" radius={[2, 2, 0, 0]} />
                          {distMean != null && <ReferenceLine x={distBins.find((b) => b.mid >= distMean)?.bin ?? 0} stroke="#c62828" strokeDasharray="3 3" />}
                          {distMedian != null && distMedian !== distMean && (
                            <ReferenceLine x={distBins.find((b) => b.mid >= distMedian)?.bin ?? 0} stroke="#1976d2" strokeDasharray="2 2" />
                          )}
                        </BarChart>
                      </ResponsiveContainer>
                      {explainMode && (distMean != null || distMedian != null) && (
                        <p className="evidence-dist-legend">
                          {distMean != null && <span>Mean: {(distMean * 100).toFixed(2)}%</span>}
                          {distMedian != null && <span> Median: {(distMedian * 100).toFixed(2)}%</span>}
                        </p>
                      )}
                    </div>
                  )}

                  {/* Confidence panel */}
                  <div className="evidence-confidence">
                    <h3>Confidence</h3>
                    {explainMode && <InfoTooltip scope={SCOPE_TRAINING} key="maturity_score" variant="short" />}
                    <div className="evidence-maturity-bar-wrap">
                      <div className="evidence-maturity-bar" style={{ width: `${Math.min(100, Math.max(0, selectedItem.maturity_score ?? 0))}%` }} aria-hidden="true" />
                    </div>
                    <span className="evidence-maturity-label">Maturity: {formatNum(selectedItem.maturity_score)}</span>
                    {coverageRatio != null && (
                      <p className="evidence-coverage" title={explainMode ? getGlossaryEntry(SCOPE_TRAINING, 'coverage_ratio')?.short : undefined}>
                        Coverage ratio: {(coverageRatio * 100).toFixed(0)}%
                        <InfoTooltip scope={SCOPE_TRAINING} key="coverage_ratio" variant="short" />
                      </p>
                    )}
                    {Array.isArray(reasons) && reasons.length > 0 && (
                      <ul className="evidence-reasons">
                        {reasons.map((r, i) => (
                          <li key={i}>{typeof r === 'string' ? r : (r && (r.reason ?? r.message)) ?? String(r)}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                </section>

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
                        <td>{(h.mean_realized_return ?? h.mean_outcome) != null ? `${(Number(h.mean_realized_return ?? h.mean_outcome) * 100).toFixed(2)}%` : '—'}</td>
                        <td>{h.pct_positive != null ? `${(Number(h.pct_positive) * 100).toFixed(1)}%` : '—'}</td>
                        <td>{h.pct_hit != null ? `${(Number(h.pct_hit) * 100).toFixed(1)}%` : '—'}</td>
                        <td>{(h.min_realized_return ?? h.min_outcome) != null ? `${(Number(h.min_realized_return ?? h.min_outcome) * 100).toFixed(2)}%` : '—'}</td>
                        <td>{(h.max_realized_return ?? h.max_outcome) != null ? `${(Number(h.max_realized_return ?? h.max_outcome) * 100).toFixed(2)}%` : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )
      })()}
    </>
  )
}
