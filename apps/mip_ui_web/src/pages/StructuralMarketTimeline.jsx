import React, { useState, useEffect, useMemo, useCallback } from 'react'
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

  // Symbol & filters
  const [marketType, setMarketType] = useState(searchParams.get('market_type') || 'STOCK')
  const [symbol, setSymbol] = useState(searchParams.get('symbol') || '')
  const [dateRange, setDateRange] = useState(searchParams.get('range') || '180')
  const [chartMode, setChartMode] = useState('candle')
  const [dirFilter, setDirFilter] = useState('')
  const [familyFilter, setFamilyFilter] = useState('')
  const [overlays, setOverlays] = useState(DEFAULT_OVERLAYS)

  // Data
  const [symbolList, setSymbolList] = useState([])
  const [summary, setSummary] = useState(null)
  const [bars, setBars] = useState([])
  const [levels, setLevels] = useState([])
  const [setups, setSetups] = useState([])
  const [proposals, setProposals] = useState([])
  const [events, setEvents] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Selected setup
  const [selectedSetupId, setSelectedSetupId] = useState(null)
  const [setupDetail, setSetupDetail] = useState(null)
  const [detailLoading, setDetailLoading] = useState(false)

  // Date range calc
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

  // Load symbol list
  useEffect(() => {
    let cancelled = false
    const qs = marketType ? `?market_type=${marketType}` : ''
    fetch(`${API_BASE}/structural-timeline/symbols${qs}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
      .then(d => {
        if (cancelled) return
        const syms = d.symbols || []
        setSymbolList(syms)
        if (!symbol && syms.length) {
          setSymbol(get(syms[0], 'SYMBOL'))
        }
      })
      .catch(() => {})
    return () => { cancelled = true }
  }, [marketType]) // eslint-disable-line react-hooks/exhaustive-deps

  // Load all data when symbol changes
  useEffect(() => {
    if (!symbol) return
    let cancelled = false
    setLoading(true)
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
      .finally(() => { if (!cancelled) setLoading(false) })

    return () => { cancelled = true }
  }, [symbol, marketType, dateStart])

  // Load setup detail on selection
  useEffect(() => {
    if (!selectedSetupId) { setSetupDetail(null); return }
    let cancelled = false
    setDetailLoading(true)
    fetch(`${API_BASE}/structural-timeline/detail?setup_event_id=${selectedSetupId}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
      .then(d => { if (!cancelled) setSetupDetail(d) })
      .catch(() => {})
      .finally(() => { if (!cancelled) setDetailLoading(false) })
    return () => { cancelled = true }
  }, [selectedSetupId])

  // Filter setups client-side
  const filteredSetups = useMemo(() => {
    return setups.filter(s => {
      if (dirFilter && get(s, 'DIRECTION') !== dirFilter) return false
      if (familyFilter && get(s, 'SETUP_FAMILY') !== familyFilter) return false
      return true
    })
  }, [setups, dirFilter, familyFilter])

  // Setup family options
  const familyOptions = useMemo(() =>
    [...new Set(setups.map(s => get(s, 'SETUP_FAMILY')).filter(Boolean))].sort()
  , [setups])

  const toggleOverlay = useCallback((key) => {
    setOverlays(prev => ({ ...prev, [key]: !prev[key] }))
  }, [])

  const handleSelectSetup = useCallback((id) => {
    setSelectedSetupId(prev => prev === id ? null : id)
  }, [])

  // Esc to deselect
  useEffect(() => {
    const h = e => { if (e.key === 'Escape') setSelectedSetupId(null) }
    document.addEventListener('keydown', h)
    return () => document.removeEventListener('keydown', h)
  }, [])

  // AskMip context
  useEffect(() => {
    mergeAskMipRuntime({
      page_id: 'structural_timeline',
      page_route: pathname,
      session_mode: 'research',
      active_filters: { symbol, marketType, dateRange, dirFilter, familyFilter },
      visible_widget_ids: ['stl_chart'],
      selected_widget_id: selectedSetupId ? 'stl_detail' : null,
      current_kpi_snapshot: { bars: bars.length, setups: setups.length },
    })
    return () => {
      mergeAskMipRuntime({ page_id: null, active_filters: {}, visible_widget_ids: [], selected_widget_id: null, current_kpi_snapshot: null })
    }
  }, [pathname, symbol, marketType, dateRange, dirFilter, familyFilter, bars.length, setups.length, selectedSetupId, mergeAskMipRuntime])

  if (!symbol && !loading) {
    return (
      <>
        <h1>Structural Market Timeline</h1>
        <EmptyState title="Select a symbol" action="Choose a symbol from the controls above to view its structural timeline." />
      </>
    )
  }

  return (
    <>
      <h1>Structural Market Timeline</h1>
      <p className="stl-intro">
        Inspect a symbol's structural context over time — levels, zones, setup detections, proposals, and trade outcomes overlaid on price.
      </p>

      <StlSymbolControls
        symbolList={symbolList}
        symbol={symbol}
        setSymbol={setSymbol}
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

      {loading && <LoadingState />}
      {error && <ErrorState message={error} />}

      {!loading && !error && (
        <>
          {summary && <StlSummaryStrip data={summary} get={get} />}

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

          {selectedSetupId && (
            <StlSetupDetail
              detail={setupDetail}
              loading={detailLoading}
              onClose={() => setSelectedSetupId(null)}
              get={get}
            />
          )}

          <StlEventRail
            events={events}
            onSelectSetup={handleSelectSetup}
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
