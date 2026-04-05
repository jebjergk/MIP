import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import { useLocation, useSearchParams } from 'react-router-dom'
import { API_BASE } from '../config/apiBase'
import InfoTooltip from '../components/InfoTooltip'
import GlossaryHoverCard from '../components/GlossaryHoverCard'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import { useAskMipRuntime } from '../context/AskMipRuntimeContext'
import TrainingTimelineInline from '../components/TrainingTimelineInline'
import { getGlossaryEntry } from '../data/glossary'
import './TrainingStatus.css'

const SCOPE = 'training_status'
const BASE_COLUMN_COUNT = 13 // expand, market, symbol, pattern_id, pattern label, direction, interval, as_of, maturity, trust, sample, coverage, horizons

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

/** Generate a unique key for a row (five-field identity + interval) */
function getRowKey(row, get) {
  const dir = String(get(row, 'signal_direction') || 'LONG').toUpperCase()
  return `${get(row, 'market_type')}-${get(row, 'symbol')}-${get(row, 'pattern_id')}-${get(row, 'interval_minutes')}-${dir}`
}

export default function TrainingStatus() {
  const { formatSymbolLabel } = useSymbolMeta()
  const { pathname } = useLocation()
  const { mergeAskMipRuntime } = useAskMipRuntime()
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

  useEffect(() => {
    const expanded = expandedRowId
      ? filteredRows.find((r) => getRowKey(r, get) === expandedRowId)
      : null
    mergeAskMipRuntime({
      page_id: 'training_status',
      page_route: pathname,
      session_mode: 'research',
      active_filters: {
        market_type: marketTypeFilter || null,
        symbol_search: symbolSearch || null,
        pattern_id: patternIdFilter || null,
      },
      visible_widget_ids: ['training_status_grid'],
      selected_widget_id: expandedRowId ? 'training_status_grid' : null,
            selected_row_context: expanded
        ? {
            symbol: get(expanded, 'symbol'),
            market_type: get(expanded, 'market_type'),
            pattern_id: get(expanded, 'pattern_id'),
            signal_direction: get(expanded, 'signal_direction'),
            maturity_stage: get(expanded, 'maturity_stage'),
          }
        : null,
      current_kpi_snapshot: {
        filtered_row_count: filteredRows.length,
        total_row_count: rows.length,
      },
    })
    return () => {
      mergeAskMipRuntime({
        page_id: null,
        active_filters: {},
        visible_widget_ids: [],
        selected_widget_id: null,
        selected_row_context: null,
        current_kpi_snapshot: null,
      })
    }
  }, [
    pathname,
    filteredRows,
    rows.length,
    marketTypeFilter,
    symbolSearch,
    patternIdFilter,
    expandedRowId,
    mergeAskMipRuntime,
  ])

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
      <p className="training-status-intro">
        Quick terms: trusted <GlossaryHoverCard scope="signals" entryKey="trusted" />, confidence <GlossaryHoverCard scope="signals" entryKey="confidence" />, max hold <GlossaryHoverCard scope="positions" entryKey="max_hold_days" />.
      </p>

      <section className="training-status-filters" aria-label="Filters">
        <div className="training-filter-row">
          <label htmlFor="ts-market-type">
            Market type
            <InfoTooltip scope={SCOPE} entryKey="filter_market_type" variant="short" />
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
            <InfoTooltip scope={SCOPE} entryKey="filter_symbol" variant="short" />
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
              <th>Market type <InfoTooltip scope={SCOPE} entryKey="market_type" variant="short" /></th>
              <th>Symbol <InfoTooltip scope={SCOPE} entryKey="symbol" variant="short" /></th>
              <th>Pattern <InfoTooltip scope={SCOPE} entryKey="pattern_id" variant="short" /></th>
              <th>Label</th>
              <th>Direction</th>
              <th>Interval <InfoTooltip scope={SCOPE} entryKey="interval_minutes" variant="short" /></th>
              <th>As of <InfoTooltip scope={SCOPE} entryKey="as_of_ts" variant="short" /></th>
              <th>Maturity <InfoTooltip scope={SCOPE} entryKey="maturity_score" variant="long" /></th>
              <th>Trust gate <InfoTooltip scope={SCOPE} entryKey="trust_gate" variant="long" /></th>
              <th>Sample size <InfoTooltip scope={SCOPE} entryKey="recs_total" variant="short" /></th>
              <th>Coverage <InfoTooltip scope={SCOPE} entryKey="coverage_ratio" variant="short" /></th>
              <th>Horizons <InfoTooltip scope={SCOPE} entryKey="horizons_covered" variant="short" /></th>
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
              const trustRaw = get(row, 'trust_gate')
              const trustGate =
                trustRaw == null || String(trustRaw).trim() === ''
                  ? null
                  : String(trustRaw).toUpperCase()
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
                    aria-label={`${formatSymbolLabel(get(row, 'symbol'), get(row, 'market_type'))} training details. Press Enter to ${isExpanded ? 'collapse' : 'expand'}.`}
                  >
                    <td className="training-expand-cell">
                      <span className={`training-expand-icon ${isExpanded ? 'training-expand-icon--open' : ''}`}>
                        &#9658;
                      </span>
                    </td>
                    <td>{get(row, 'market_type') ?? '—'}</td>
                    <td className="training-symbol-cell">{formatSymbolLabel(get(row, 'symbol') ?? '—', get(row, 'market_type'))}</td>
                    <td>{get(row, 'pattern_id') ?? '—'}</td>
                    <td className="training-pattern-label-cell" title={get(row, 'pattern_parameter_summary') || ''}>
                      {get(row, 'pattern_display_name') ?? '—'}
                    </td>
                    <td>
                      <span
                        className={`training-direction-badge training-direction-${String(get(row, 'signal_direction') || 'LONG').toLowerCase()}`}
                        title={get(row, 'pattern_family') || ''}
                      >
                        {String(get(row, 'signal_direction') || 'LONG').toUpperCase()}
                      </span>
                      {get(row, 'research_evidence_stage') ? (
                        <span className="training-research-stage" title="SHORT research evidence (not production trust)">
                          {' '}
                          {get(row, 'research_evidence_stage')}
                        </span>
                      ) : null}
                    </td>
                    <td>{get(row, 'interval_minutes') ?? '—'}</td>
                    <td>{get(row, 'as_of_ts') ?? '—'}</td>
                    <td className="training-maturity-cell">
                      <span
                        className={`training-maturity-badge training-stage-${(get(row, 'maturity_stage') || '').toLowerCase().replace('_', '-')}`}
                        title={stageTitle}
                      >
                        {get(row, 'maturity_stage') ?? '—'}
                      </span>
                      <InfoTooltip scope={SCOPE} entryKey={stageKey} variant="short" />
                      <div className="training-progress-wrap" title={stageTitle}>
                        <div className="training-progress-bar" style={{ width: `${Math.min(100, Math.max(0, score))}%` }} />
                      </div>
                      <span className="training-score-num" title={getGlossaryEntry(SCOPE, 'maturity_score')?.short}>
                        {formatNum(get(row, 'maturity_score'))}
                      </span>
                    </td>
                    <td>
                      {trustGate ? (
                        <span className={`training-trust-badge training-trust-${trustGate.toLowerCase().replace('_', '-')}`}>
                          {trustGate}
                        </span>
                      ) : (
                        '—'
                      )}
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
                      <td colSpan={BASE_COLUMN_COUNT + horizonDefs.length} className="training-detail-cell">
                        <TrainingTimelineInline
                          symbol={get(row, 'symbol')}
                          marketType={get(row, 'market_type')}
                          patternId={get(row, 'pattern_id')}
                          signalDirection={String(get(row, 'signal_direction') || 'LONG').toUpperCase()}
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
