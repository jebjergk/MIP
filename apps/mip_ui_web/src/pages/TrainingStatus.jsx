import { useState, useEffect, useMemo } from 'react'
import { API_BASE } from '../App'
import InfoTooltip from '../components/InfoTooltip'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import { useExplainMode } from '../context/ExplainModeContext'
import { getGlossaryEntry } from '../data/glossary'
import './TrainingStatus.css'

const SCOPE = 'training_status'

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
  return `${(Number(n) * 100).toFixed(1)}%`
}

function formatNum(n) {
  if (n == null || Number.isNaN(n)) return '—'
  const x = Number(n)
  return Number.isInteger(x) ? String(x) : x.toFixed(4)
}

export default function TrainingStatus() {
  const { explainMode } = useExplainMode()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [marketTypeFilter, setMarketTypeFilter] = useState('')
  const [symbolSearch, setSymbolSearch] = useState('')

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
  const get = (r, k) => r[k] ?? r[k.toUpperCase()]

  const marketTypes = useMemo(() => {
    const set = new Set(rows.map((r) => get(r, 'market_type')).filter(Boolean))
    return Array.from(set).sort()
  }, [rows])

  const filteredRows = useMemo(() => {
    return rows.filter((r) => {
      if (marketTypeFilter && get(r, 'market_type') !== marketTypeFilter) return false
      if (symbolSearch.trim()) {
        const sym = (get(r, 'symbol') ?? '').toLowerCase()
        if (!sym.includes(symbolSearch.trim().toLowerCase())) return false
      }
      return true
    })
  }, [rows, marketTypeFilter, symbolSearch])

  if (loading) return <p>Loading…</p>
  if (error) return <ErrorState message={error} />

  return (
    <>
      <h1>Training Status</h1>
      {explainMode && (
        <p className="training-status-intro">
          Per-asset training maturity (daily bars): sample size, coverage, horizons, and avg outcomes. Use filters to narrow by market or symbol.
        </p>
      )}

      <section className="training-status-filters" aria-label="Filters">
        <div className="training-filter-row">
          <label htmlFor="ts-market-type">
            Market type
            <InfoTooltip scope={SCOPE} key="filter_market_type" variant="short" />
          </label>
          <select
            id="ts-market-type"
            value={marketTypeFilter}
            onChange={(e) => setMarketTypeFilter(e.target.value)}
            aria-label="Filter by market type"
          >
            <option value="">All</option>
            {marketTypes.map((mt) => (
              <option key={mt} value={mt}>{mt}</option>
            ))}
          </select>
        </div>
        <div className="training-filter-row">
          <label htmlFor="ts-symbol">
            Symbol
            <InfoTooltip scope={SCOPE} key="filter_symbol" variant="short" />
          </label>
          <input
            id="ts-symbol"
            type="search"
            placeholder="Search symbol…"
            value={symbolSearch}
            onChange={(e) => setSymbolSearch(e.target.value)}
            aria-label="Search by symbol"
          />
        </div>
      </section>

      <div className="training-status-table-wrap">
        <table className="training-status-table">
          <thead>
            <tr>
              <th>Market type <InfoTooltip scope={SCOPE} key="market_type" variant="short" /></th>
              <th>Symbol <InfoTooltip scope={SCOPE} key="symbol" variant="short" /></th>
              <th>Pattern <InfoTooltip scope={SCOPE} key="pattern_id" variant="short" /></th>
              <th>Interval <InfoTooltip scope={SCOPE} key="interval_minutes" variant="short" /></th>
              <th>As of <InfoTooltip scope={SCOPE} key="as_of_ts" variant="short" /></th>
              <th>Maturity <InfoTooltip scope={SCOPE} key="maturity_score" variant="short" /></th>
              <th>Sample size <InfoTooltip scope={SCOPE} key="recs_total" variant="short" /></th>
              <th>Coverage <InfoTooltip scope={SCOPE} key="coverage_ratio" variant="short" /></th>
              <th>Horizons <InfoTooltip scope={SCOPE} key="horizons_covered" variant="short" /></th>
              <th>Avg H1 <InfoTooltip scope={SCOPE} key="avg_outcome_h1" variant="short" /></th>
              <th>Avg H3 <InfoTooltip scope={SCOPE} key="avg_outcome_h3" variant="short" /></th>
              <th>Avg H5 <InfoTooltip scope={SCOPE} key="avg_outcome_h5" variant="short" /></th>
              <th>Avg H10 <InfoTooltip scope={SCOPE} key="avg_outcome_h10" variant="short" /></th>
              <th>Avg H20 <InfoTooltip scope={SCOPE} key="avg_outcome_h20" variant="short" /></th>
            </tr>
          </thead>
          <tbody>
            {filteredRows.map((row, i) => {
              const maturityStage = get(row, 'maturity_stage')
              const score = get(row, 'maturity_score') != null ? Number(get(row, 'maturity_score')) : 0
              const stageKey = stageGlossaryKey(maturityStage)
              const stageTitle = explainMode ? (getGlossaryEntry(SCOPE, stageKey)?.short ?? maturityStage) : undefined
              return (
                <tr key={i}>
                  <td>{get(row, 'market_type') ?? '—'}</td>
                  <td>{get(row, 'symbol') ?? '—'}</td>
                  <td>{get(row, 'pattern_id') ?? '—'}</td>
                  <td>{get(row, 'interval_minutes') ?? '—'}</td>
                  <td>{get(row, 'as_of_ts') ?? '—'}</td>
                  <td className="training-maturity-cell">
                    <span
                      className={`training-maturity-badge training-stage-${(get(row, 'maturity_stage') || '').toLowerCase().replace('_', '-')}`}
                      title={stageTitle}
                    >
                      {get(row, 'maturity_stage') ?? '—'}
                    </span>
                    <InfoTooltip scope={SCOPE} key={stageKey} variant="short" />
                    <div className="training-progress-wrap" title={stageTitle}>
                      <div className="training-progress-bar" style={{ width: `${Math.min(100, Math.max(0, score))}%` }} />
                    </div>
                    <span className="training-score-num" title={explainMode ? getGlossaryEntry(SCOPE, 'maturity_score')?.short : undefined}>
                      {formatNum(get(row, 'maturity_score'))}
                    </span>
                  </td>
                  <td>{formatNum(get(row, 'recs_total'))}</td>
                  <td>{formatPct(get(row, 'coverage_ratio'))}</td>
                  <td>{formatNum(get(row, 'horizons_covered'))}</td>
                  <td>{formatNum(get(row, 'avg_outcome_h1'))}</td>
                  <td>{formatNum(get(row, 'avg_outcome_h3'))}</td>
                  <td>{formatNum(get(row, 'avg_outcome_h5'))}</td>
                  <td>{formatNum(get(row, 'avg_outcome_h10'))}</td>
                  <td>{formatNum(get(row, 'avg_outcome_h20'))}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {filteredRows.length === 0 && (
        <EmptyState
          title={rows.length === 0 ? 'No training status rows yet' : 'No rows match the current filters'}
          action={rows.length === 0 ? 'Run pipeline in Snowflake.' : 'Clear or adjust filters above.'}
          explanation={rows.length === 0 ? 'Training status comes from recommendation and outcome data. Run the pipeline to populate it.' : 'Try a different market type or symbol search.'}
          reasons={rows.length === 0 ? ['Pipeline has not run yet.', 'No recommendations or outcomes in MIP.APP.'] : []}
        />
      )}
    </>
  )
}
