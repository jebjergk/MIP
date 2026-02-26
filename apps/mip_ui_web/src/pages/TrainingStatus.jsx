import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { API_BASE } from '../App'
import InfoTooltip from '../components/InfoTooltip'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import TrainingTimelineInline from '../components/TrainingTimelineInline'
import TrainingDigestPanel from '../components/TrainingDigestPanel'
import { getGlossaryEntry } from '../data/glossary'
import './TrainingStatus.css'

const SCOPE = 'training_status'
const BASE_COLUMN_COUNT = 10 // Columns before horizon columns (expand, market, symbol, pattern, interval, as_of, maturity, sample, coverage, horizons)

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

/** Generate a unique key for a row */
function getRowKey(row, get) {
  return `${get(row, 'market_type')}-${get(row, 'symbol')}-${get(row, 'pattern_id')}`
}

export default function TrainingStatus() {
  const [searchParams] = useSearchParams()
  const appliedUrlRef = useRef(false)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [marketTypeFilter, setMarketTypeFilter] = useState('')
  const [symbolSearch, setSymbolSearch] = useState('')
  const [patternIdFilter, setPatternIdFilter] = useState('')
  const [expandedRowId, setExpandedRowId] = useState(null)
  const timelineCacheRef = useRef({}) // Cache for timeline data per row key
  useEffect(() => {
    if (appliedUrlRef.current) return
    appliedUrlRef.current = true
    const s = searchParams.get('symbol')
    const m = searchParams.get('market_type')
    const p = searchParams.get('pattern_id')
    if (s != null && s !== '') setSymbolSearch(s)
    if (m != null && m !== '') setMarketTypeFilter(m)
    if (p != null && p !== '') setPatternIdFilter(p)
  }, [searchParams])

  const intervalMinutes = 1440

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    setData(null)
    setExpandedRowId(null)
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
  const horizonDefs = data?.horizon_definitions ?? []
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
      if (patternIdFilter !== '' && String(get(r, 'pattern_id')) !== String(patternIdFilter)) return false
      return true
    })
  }, [rows, marketTypeFilter, symbolSearch, patternIdFilter])

  // Collapse expanded row if it's no longer in filtered results
  useEffect(() => {
    if (expandedRowId) {
      const stillExists = filteredRows.some((r) => getRowKey(r, get) === expandedRowId)
      if (!stillExists) {
        setExpandedRowId(null)
      }
    }
  }, [filteredRows, expandedRowId])

  // Keyboard handler for Esc to collapse
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape' && expandedRowId) {
        setExpandedRowId(null)
      }
    }
    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [expandedRowId])

  // Toggle row expansion
  const toggleRow = useCallback((rowKey) => {
    setExpandedRowId((prev) => (prev === rowKey ? null : rowKey))
  }, [])

  // Handle keyboard toggle on row
  const handleRowKeyDown = useCallback((e, rowKey) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      toggleRow(rowKey)
    }
  }, [toggleRow])

  // Cache setter for timeline data
  const setTimelineCache = useCallback((key, data) => {
    timelineCacheRef.current[key] = data
  }, [])

  if (loading) {
    return (
      <>
        <h1>Training Status</h1>
        <LoadingState />
      </>
    )
  }
  if (error) {
    return (
      <>
        <h1>Training Status</h1>
        <ErrorState message={error} />
      </>
    )
  }

  return (
    <>
      <h1>Training Status</h1>

      <p className="training-status-intro">
        Per-asset training maturity (daily bars): sample size, coverage, horizons, and avg outcomes. Use filters to narrow by market or symbol.
      </p>

      <TrainingDigestPanel scope="global" />

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
              <th className="training-expand-col" aria-label="Expand"></th>
              <th>Market type <InfoTooltip scope={SCOPE} key="market_type" variant="short" /></th>
              <th>Symbol <InfoTooltip scope={SCOPE} key="symbol" variant="short" /></th>
              <th>Pattern <InfoTooltip scope={SCOPE} key="pattern_id" variant="short" /></th>
              <th>Interval <InfoTooltip scope={SCOPE} key="interval_minutes" variant="short" /></th>
              <th>As of <InfoTooltip scope={SCOPE} key="as_of_ts" variant="short" /></th>
              <th>Maturity <InfoTooltip scope={SCOPE} key="maturity_score" variant="short" /></th>
              <th>Sample size <InfoTooltip scope={SCOPE} key="recs_total" variant="short" /></th>
              <th>Coverage <InfoTooltip scope={SCOPE} key="coverage_ratio" variant="short" /></th>
              <th>Horizons <InfoTooltip scope={SCOPE} key="horizons_covered" variant="short" /></th>
              {horizonDefs.map((h) => (
                <th key={h.key} title={h.label}>Avg {h.key}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filteredRows.map((row) => {
              const maturityStage = get(row, 'maturity_stage')
              const score = get(row, 'maturity_score') != null ? Number(get(row, 'maturity_score')) : 0
              const stageKey = stageGlossaryKey(maturityStage)
              const stageTitle = getGlossaryEntry(SCOPE, stageKey)?.short ?? maturityStage
              const rowKey = getRowKey(row, get)
              const isExpanded = expandedRowId === rowKey
              const cachedData = timelineCacheRef.current[rowKey]

              return (
                <React.Fragment key={rowKey}>
                  <tr 
                    className={`training-row ${isExpanded ? 'training-row-expanded' : ''}`}
                    onClick={() => toggleRow(rowKey)}
                    onKeyDown={(e) => handleRowKeyDown(e, rowKey)}
                    tabIndex={0}
                    role="button"
                    aria-expanded={isExpanded}
                    aria-label={`${get(row, 'symbol')} training details. Press Enter to ${isExpanded ? 'collapse' : 'expand'}.`}
                  >
                    <td className="training-expand-cell">
                      <span className={`training-expand-icon ${isExpanded ? 'training-expand-icon--open' : ''}`}>
                        &#9658;
                      </span>
                    </td>
                    <td>{get(row, 'market_type') ?? '—'}</td>
                    <td className="training-symbol-cell">{get(row, 'symbol') ?? '—'}</td>
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
                      <span className="training-score-num" title={getGlossaryEntry(SCOPE, 'maturity_score')?.short}>
                        {formatNum(get(row, 'maturity_score'))}
                      </span>
                    </td>
                    <td>{formatNum(get(row, 'recs_total'))}</td>
                    <td>{formatPct(get(row, 'coverage_ratio'))}</td>
                    <td>{formatNum(get(row, 'horizons_covered'))}</td>
                    {horizonDefs.map((h) => (
                      <td key={h.key}>{formatNum(get(row, `avg_outcome_${h.key.toLowerCase()}`))}</td>
                    ))}
                  </tr>
                  {isExpanded && (
                    <tr className="training-detail-row">
                      <td colSpan={BASE_COLUMN_COUNT + horizonDefs.length + 1} className="training-detail-cell">
                        <TrainingDigestPanel
                          scope="symbol"
                          symbol={get(row, 'symbol')}
                          marketType={get(row, 'market_type')}
                          patternId={get(row, 'pattern_id')}
                          compact
                        />
                        <TrainingTimelineInline
                          symbol={get(row, 'symbol')}
                          marketType={get(row, 'market_type')}
                          patternId={get(row, 'pattern_id')}
                          horizonBars={5}
                          intervalMinutes={intervalMinutes}
                          cachedData={cachedData}
                          onDataLoaded={(data) => setTimelineCache(rowKey, data)}
                          onClose={() => setExpandedRowId(null)}
                        />
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              )
            })}
          </tbody>
        </table>
      </div>

      {filteredRows.length === 0 && (
        <EmptyState
          title={rows.length === 0 ? 'No evaluated recommendations found yet' : 'No rows match the current filters'}
          action={rows.length === 0 ? 'Run pipeline in Snowflake.' : 'Clear or adjust filters above.'}
          explanation={rows.length === 0 ? 'Training status comes from recommendation and outcome data. Run the pipeline to populate it.' : 'Try a different market type or symbol search.'}
          reasons={rows.length === 0 ? ['Pipeline has not run yet.', 'No recommendations or outcomes in MIP.APP.'] : []}
        />
      )}
    </>
  )
}
