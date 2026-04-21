import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import { useLocation, useSearchParams } from 'react-router-dom'
import { API_BASE } from '../config/apiBase'
import { useAskMipRuntime } from '../context/AskMipRuntimeContext'
import LoadingState from '../components/LoadingState'
import ErrorState from '../components/ErrorState'
import EmptyState from '../components/EmptyState'
import StlSymbolControls from '../components/structural-timeline/StlSymbolControls'
import StlSummaryStrip from '../components/structural-timeline/StlSummaryStrip'
import StlChart from '../components/structural-timeline/StlChart'
import StlEventRail from '../components/structural-timeline/StlEventRail'
import StlSetupDetail from '../components/structural-timeline/StlSetupDetail'
import StlSymbolSummary from '../components/structural-timeline/StlSymbolSummary'
import StlHearingLaunch from '../components/structural-timeline/StlHearingLaunch'
import './StructuralMarketTimeline.css'

const get = (r, k) => r[k] ?? r[k.toUpperCase()] ?? r[k.toLowerCase()]

const DEFAULT_OVERLAYS = {
  setupMarkers: true,
  proposalMarkers: true,
  tradeMarkers: true,
  stateStrip: true,
  levels: false,
  zones: false,
  regimeStrip: false,
  entryZones: false,
  invalidationLines: false,
}

export default function StructuralMarketTimeline() {
  const { pathname } = useLocation()
  const { mergeAskMipRuntime } = useAskMipRuntime()
  const [searchParams, setSearchParams] = useSearchParams()
  const detailRef = useRef(null)

  // Symbol & filters
  const [marketType, setMarketType] = useState(searchParams.get('market_type') || 'STOCK')
  const [symbol, setSymbol] = useState(searchParams.get('symbol') || '')
  const [dateRange, setDateRange] = useState(searchParams.get('range') || '180')
  const [chartMode, setChartMode] = useState('candle')
  const [dirFilter, setDirFilter] = useState('')
  const [familyFilter, setFamilyFilter] = useState('')
  const [overlays, setOverlays] = useState(DEFAULT_OVERLAYS)
  const [searchText, setSearchText] = useState('')

  // Symbol grid data
  const [symbolList, setSymbolList] = useState([])
  const [gridLoading, setGridLoading] = useState(true)

  // Detail data (loaded when symbol is selected)
  const [summary, setSummary] = useState(null)
  const [bars, setBars] = useState([])
  const [levels, setLevels] = useState([])
  const [setups, setSetups] = useState([])
  const [proposals, setProposals] = useState([])
  const [events, setEvents] = useState([])
  const [detailDataLoading, setDetailDataLoading] = useState(false)
  const [error, setError] = useState(null)

  // Selected setup within chart
  const [selectedSetupId, setSelectedSetupId] = useState(null)
  const [setupDetail, setSetupDetail] = useState(null)
  const [setupDetailLoading, setSetupDetailLoading] = useState(false)

  const dateStart = useMemo(() => {
    const d = new Date()
    d.setDate(d.getDate() - parseInt(dateRange, 10))
    return d.toISOString().slice(0, 10)
  }, [dateRange])

  // Sync filters → URL
  useEffect(() => {
    const p = {}
    if (symbol) p.symbol = symbol
    if (marketType) p.market_type = marketType
    if (dateRange !== '180') p.range = dateRange
    setSearchParams(p, { replace: true })
  }, [symbol, marketType, dateRange, setSearchParams])

  // Load symbol grid — uses the summary view for all symbols
  useEffect(() => {
    let cancelled = false
    setGridLoading(true)
    fetch(`${API_BASE}/structural-timeline/symbols?market_type=${marketType}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
      .then(d => { if (!cancelled) setSymbolList(d.symbols || []) })
      .catch(() => {})
      .finally(() => { if (!cancelled) setGridLoading(false) })
    return () => { cancelled = true }
  }, [marketType])

  // Load chart/event data when a symbol is selected
  useEffect(() => {
    if (!symbol) {
      setSummary(null); setBars([]); setLevels([]); setSetups([]); setProposals([]); setEvents([])
      return
    }
    let cancelled = false
    setDetailDataLoading(true)
    setError(null)
    setSelectedSetupId(null)
    setSetupDetail(null)

    const qs = `symbol=${encodeURIComponent(symbol)}&market_type=${marketType}&start=${dateStart}`

    Promise.all([
      fetch(`${API_BASE}/structural-timeline/summary?symbol=${encodeURIComponent(symbol)}&market_type=${marketType}`).then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText))),
      fetch(`${API_BASE}/structural-timeline/price?${qs}`).then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText))),
      fetch(`${API_BASE}/structural-timeline/levels?${qs}`).then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText))),
      fetch(`${API_BASE}/structural-timeline/setups?${qs}`).then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText))),
      fetch(`${API_BASE}/structural-timeline/proposals?${qs}`).then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText))),
      fetch(`${API_BASE}/structural-timeline/events?${qs}`).then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText))),
    ])
      .then(([s, p, l, st, pr, ev]) => {
        if (cancelled) return
        setSummary(s)
        setBars(p.bars || [])
        setLevels(l.levels || [])
        setSetups(st.setups || [])
        setProposals(pr.proposals || [])
        setEvents(ev.events || [])
      })
      .catch(e => { if (!cancelled) setError(e.message) })
      .finally(() => { if (!cancelled) setDetailDataLoading(false) })

    return () => { cancelled = true }
  }, [symbol, marketType, dateStart])

  // Load single setup detail
  useEffect(() => {
    if (!selectedSetupId) { setSetupDetail(null); return }
    let cancelled = false
    setSetupDetailLoading(true)
    fetch(`${API_BASE}/structural-timeline/detail?setup_event_id=${selectedSetupId}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
      .then(d => {
        if (cancelled) return
        setSetupDetail(d)
        setTimeout(() => {
          detailRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
        }, 50)
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setSetupDetailLoading(false) })
    return () => { cancelled = true }
  }, [selectedSetupId])

  const filteredSetups = useMemo(() => {
    return setups.filter(s => {
      if (dirFilter && get(s, 'DIRECTION') !== dirFilter) return false
      if (familyFilter && get(s, 'SETUP_FAMILY') !== familyFilter) return false
      return true
    })
  }, [setups, dirFilter, familyFilter])

  const familyOptions = useMemo(() =>
    [...new Set(setups.map(s => get(s, 'SETUP_FAMILY')).filter(Boolean))].sort()
  , [setups])

  const toggleOverlay = useCallback((key) => {
    setOverlays(prev => ({ ...prev, [key]: !prev[key] }))
  }, [])

  const handleSelectSetup = useCallback((id) => {
    setSelectedSetupId(prev => prev === id ? null : id)
  }, [])

  const handleSelectSymbol = useCallback((sym) => {
    setSymbol(sym)
    setSearchText('')
  }, [])

  const handleBack = useCallback(() => {
    setSymbol('')
  }, [])

  // Esc to deselect setup or go back to grid
  useEffect(() => {
    const h = e => {
      if (e.key === 'Escape') {
        if (selectedSetupId) setSelectedSetupId(null)
        else if (symbol) setSymbol('')
      }
    }
    document.addEventListener('keydown', h)
    return () => document.removeEventListener('keydown', h)
  }, [selectedSetupId, symbol])

  // AskMip context
  useEffect(() => {
    mergeAskMipRuntime({
      page_id: 'structural_market_timeline',
      page_route: pathname,
      session_mode: 'research',
      active_filters: { symbol, marketType, dateRange, dirFilter, familyFilter },
      visible_widget_ids: symbol ? ['stl_chart'] : ['stl_grid'],
      selected_widget_id: selectedSetupId ? 'stl_detail' : null,
      current_kpi_snapshot: { bars: bars.length, setups: setups.length },
    })
    return () => {
      mergeAskMipRuntime({ page_id: null, active_filters: {}, visible_widget_ids: [], selected_widget_id: null, current_kpi_snapshot: null })
    }
  }, [pathname, symbol, marketType, dateRange, dirFilter, familyFilter, bars.length, setups.length, selectedSetupId, mergeAskMipRuntime])

  // Filtered symbol grid
  const filteredSymbolList = useMemo(() => {
    if (!searchText) return symbolList
    const q = searchText.toUpperCase()
    return symbolList.filter(s => (get(s, 'SYMBOL') || '').includes(q))
  }, [symbolList, searchText])

  // ─── GRID VIEW (no symbol selected) ───
  if (!symbol) {
    return (
      <>
        <h1>Structural Market Timeline</h1>
        <p className="stl-intro">
          Select a symbol to inspect its structural context — levels, zones, setup detections, proposals, and trade outcomes overlaid on price.
        </p>

        <div className="stl-controls">
          <label>Market</label>
          <select value={marketType} onChange={e => setMarketType(e.target.value)}>
            <option value="STOCK">Stock</option>
            <option value="FX">FX</option>
          </select>
          <div className="stl-sep" />
          <label>Search</label>
          <input
            type="text"
            value={searchText}
            onChange={e => setSearchText(e.target.value)}
            placeholder="Filter symbols…"
            style={{ width: 140 }}
          />
        </div>

        {gridLoading && <LoadingState />}

        {!gridLoading && filteredSymbolList.length === 0 && (
          <EmptyState title="No symbols found" action="Adjust market type filter or run the structural pipeline." />
        )}

        {!gridLoading && filteredSymbolList.length > 0 && (
          <>
            <div className="stl-grid-summary">
              {filteredSymbolList.length} symbols with structural data
            </div>
            <div className="stl-symbol-grid">
              {filteredSymbolList.map(s => {
                const sym = get(s, 'SYMBOL')
                const mt = get(s, 'MARKET_TYPE')
                const setups = Number(get(s, 'TOTAL_SETUPS') || 0)
                const eligible = Number(get(s, 'ELIGIBLE_SETUPS') || 0)
                const propLifetime = Number(get(s, 'PROPOSALS_CREATED') || 0)
                const propActive = Number(get(s, 'ACTIVE_PROPOSALS') || 0)
                const trades = Number(get(s, 'TRADES_EXECUTED') || 0)
                const state = get(s, 'DOMINANT_STATE')
                const hasActiveProposals = propActive > 0
                // Tile edge color escalates: nothing → signal (eligible setup) → proposal.
                // TRADES_EXECUTED is lifetime/historical — kept as a badge below but
                // intentionally NOT used to color the tile (would mask actionable proposals).
                const tileClass = hasActiveProposals
                  ? 'stl-tile-proposed'
                  : eligible > 0
                    ? 'stl-tile-eligible'
                    : ''
                const propBadgeTitle = propLifetime > propActive
                  ? `${propActive} active proposal${propActive === 1 ? '' : 's'} (${propLifetime} lifetime, incl. expired/historical)`
                  : `${propActive} active proposal${propActive === 1 ? '' : 's'}`
                return (
                  <div
                    key={`${sym}-${mt}`}
                    className={`stl-symbol-tile ${tileClass}`}
                    onClick={() => handleSelectSymbol(sym)}
                    role="button"
                    tabIndex={0}
                    onKeyDown={e => { if (e.key === 'Enter') handleSelectSymbol(sym) }}
                  >
                    <div className="stl-tile-header">
                      <span className="stl-tile-symbol">{sym}</span>
                    </div>
                    <div className="stl-tile-counts">
                      <span className={`stl-tile-count ${setups > 0 ? 'stl-count-has' : ''}`} title="Setups">S:{setups}</span>
                      <span className={`stl-tile-count ${propActive > 0 ? 'stl-count-prop' : ''}`} title={propBadgeTitle}>P:{propActive}</span>
                      <span className={`stl-tile-count ${trades > 0 ? 'stl-count-trade' : ''}`} title="Trades">T:{trades}</span>
                    </div>
                    {state && <div className="stl-tile-state">{state.replace(/_/g, ' ')}</div>}
                  </div>
                )
              })}
            </div>
          </>
        )}
      </>
    )
  }

  // ─── DETAIL VIEW (symbol selected) ───
  return (
    <>
      <h1>Structural Market Timeline</h1>
      <p className="stl-intro">
        Inspect a symbol's structural context over time — levels, zones, setup detections, proposals, and trade outcomes overlaid on price.
      </p>

      <div style={{ marginBottom: '0.5rem' }}>
        <button className="stl-back-btn" onClick={handleBack}>&larr; Back to symbol list</button>
      </div>

      <StlSymbolControls
        symbolList={symbolList}
        symbol={symbol}
        setSymbol={handleSelectSymbol}
        marketType={marketType}
        setMarketType={setMarketType}
        dateRange={dateRange}
        setDateRange={setDateRange}
        chartMode={chartMode}
        setChartMode={setChartMode}
        dirFilter={dirFilter}
        setDirFilter={setDirFilter}
        familyFilter={familyFilter}
        setFamilyFilter={setFamilyFilter}
        familyOptions={familyOptions}
        overlays={overlays}
        toggleOverlay={toggleOverlay}
        get={get}
      />

      {detailDataLoading && <LoadingState />}
      {error && <ErrorState message={error} />}

      {!detailDataLoading && !error && (
        <>
          {summary && <StlSummaryStrip data={summary} get={get} />}

          <StlHearingLaunch proposals={proposals} get={get} />

          <StlChart
            bars={bars}
            levels={levels}
            setups={filteredSetups}
            proposals={proposals}
            overlays={overlays}
            chartMode={chartMode}
            selectedSetupId={selectedSetupId}
            onSelectSetup={handleSelectSetup}
            get={get}
          />

          <div ref={detailRef} className="stl-detail-anchor">
            {(selectedSetupId || setupDetailLoading) && (
              <StlSetupDetail
                detail={setupDetail}
                loading={setupDetailLoading}
                onClose={() => setSelectedSetupId(null)}
                get={get}
              />
            )}
          </div>

          <StlEventRail
            events={events}
            onSelectSetup={handleSelectSetup}
            selectedSetupId={selectedSetupId}
            get={get}
          />

          <StlSymbolSummary
            setups={setups}
            summary={summary}
            get={get}
          />
        </>
      )}
    </>
  )
}
