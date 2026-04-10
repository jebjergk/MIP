import { Component, lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useLocation, useSearchParams } from 'react-router-dom'

import { API_BASE } from '../config/apiBase'
import { useAskMipRuntime } from '../context/AskMipRuntimeContext'
import useVisibleInterval from '../hooks/useVisibleInterval'
import { useTapeSnapshot, isTapeApiEnabled } from '../hooks/useTapeSnapshot'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import {
  buildLiveState,
  evaluateCommittee,
  generateExitRecommendation,
} from './symbolTrackerCommittee'
import {
  mergeBarsByTimestamp,
  buildLivingChartShapesAndTA,
} from '../lib/livingChartOverlays'
import {
  riskPressureTier,
  resolveLivingChartSymbolDisplay,
  resolveActiveConditionSummary,
  resolveDominantAttention,
  liveConditionChipsActive,
  stripOptionalCue,
  dominantActivePlotTag,
  selectFlowChipsForChart,
} from '../lib/livingChartVisualState'
import {
  computeFlowIntelligenceRaw,
  applyFlowVisibleState,
  getFlowAnnotationText,
} from '../lib/livingChartFlowIntelligence'
import GlossaryHoverCard from '../components/GlossaryHoverCard'

const LivingChartPlot = lazy(() => import('../components/livingChart/LivingChartPlot'))
import './SymbolTracker.css'

function fmtNum(value, digits = 2) {
  if (value == null) return '—'
  const n = Number(value)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

function fmtPct(value) {
  if (value == null) return '—'
  const n = Number(value)
  if (!Number.isFinite(n)) return '—'
  return `${(n * 100).toFixed(2)}%`
}

function fmtSigned(value, digits = 2) {
  if (value == null) return '—'
  const n = Number(value)
  if (!Number.isFinite(n)) return '—'
  const text = Math.abs(n).toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })
  if (n > 0) return `+${text}`
  if (n < 0) return `-${text}`
  return text
}

function fmtTime(iso) {
  if (!iso) return '—'
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return '—'
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

class LivingChartErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    console.error('LivingChartPlot error:', error, info?.componentStack)
  }

  render() {
    if (this.state.error) {
      return (
        <div className="lc-error lc-error--chart">
          Chart could not render ({this.state.error?.message || 'unknown error'}). Try line chart mode or reload the page.
        </div>
      )
    }
    return this.props.children
  }
}

function mergeIbLiveRows(prevData, livePayload) {
  if (!prevData || !Array.isArray(prevData.tiles)) return prevData
  const rows = Array.isArray(livePayload?.rows) ? livePayload.rows : []
  if (rows.length === 0) return prevData
  const bySymbol = new Map(rows.map((r) => [String(r.symbol || '').toUpperCase(), r]))
  const intervalMinutes = Number(livePayload?.interval_minutes)
  const barSecondsRaw = Number(livePayload?.intraday_bar_seconds)
  const hasBarSeconds = Number.isFinite(barSecondsRaw) && barSecondsRaw > 0
  const tiles = prevData.tiles.map((tile) => {
    const symbol = String(tile?.symbol || '').toUpperCase()
    const live = bySymbol.get(symbol)
    if (!live || !Array.isArray(live.bars) || live.bars.length === 0) return tile
    const prevBars = Array.isArray(tile?.chart?.bars) ? tile.chart.bars : []
    const bars = mergeBarsByTimestamp(prevBars, live.bars)
    const currentPrice = Number(live.current_price)
    const resolvedCurrent = Number.isFinite(currentPrice)
      ? currentPrice
      : Number(bars[bars.length - 1]?.close)
    const entry = Number(tile?.entry_price)
    const qty = Number(tile?.quantity)
    let unrealized = tile?.unrealized_pnl
    if (Number.isFinite(resolvedCurrent) && Number.isFinite(entry) && Number.isFinite(qty)) {
      unrealized = tile?.side === 'SHORT'
        ? (entry - resolvedCurrent) * qty
        : (resolvedCurrent - entry) * qty
    }
    return {
      ...tile,
      current_price: Number.isFinite(resolvedCurrent) ? resolvedCurrent : tile?.current_price,
      unrealized_pnl: unrealized,
      chart: {
        ...(tile?.chart || {}),
        interval_minutes: hasBarSeconds
          ? 0
          : (Number.isFinite(intervalMinutes) && intervalMinutes > 0
            ? intervalMinutes
            : tile?.chart?.interval_minutes),
        bar_seconds: hasBarSeconds ? barSecondsRaw : (tile?.chart?.bar_seconds ?? null),
        bars,
      },
      overlays: {
        ...(tile?.overlays || {}),
        current: Number.isFinite(resolvedCurrent) ? resolvedCurrent : tile?.overlays?.current,
      },
    }
  })
  return {
    ...prevData,
    tiles,
    updated_at: livePayload?.updated_at || new Date().toISOString(),
  }
}

export default function SymbolTracker() {
  const { formatSymbolLabel } = useSymbolMeta()
  const { pathname } = useLocation()
  const { mergeAskMipRuntime } = useAskMipRuntime()
  const [searchParams, setSearchParams] = useSearchParams()

  const [sensitivityMode, setSensitivityMode] = useState('BALANCED')
  const [chartStyle, setChartStyle] = useState('line')
  const [horizonBars, setHorizonBars] = useState(5)
  const [showAdvancedTA, setShowAdvancedTA] = useState(false)
  const [followLatest, setFollowLatest] = useState(true)
  const [viewportLocked, setViewportLocked] = useState(false)
  const [layoutRevision, setLayoutRevision] = useState(0)

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [data, setData] = useState({ tiles: [], updated_at: null })
  const [contextReloadAt, setContextReloadAt] = useState(null)
  const [liveUpdatedAt, setLiveUpdatedAt] = useState(null)
  const [selectedSymbol, setSelectedSymbol] = useState(null)

  const [committeeBySymbol, setCommitteeBySymbol] = useState({})
  const [exitRecBySymbol, setExitRecBySymbol] = useState({})

  const prevSelectedSymbolRef = useRef(null)
  const flowHystRef = useRef({})


  const fetchIbLive = useCallback(async (tiles) => {
    const symbols = (Array.isArray(tiles) ? tiles : [])
      .map((t) => ({
        symbol: t?.symbol,
        market_type: t?.market_type,
      }))
      .filter((t) => t.symbol)
    if (symbols.length === 0) return null
    const body = {
      mode: 'intraday',
      intraday_bar_seconds: 30,
      window_bars: 780,
      symbols,
    }
    const resp = await fetch(`${API_BASE}/symbol-tracker/ib-live`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!resp.ok) throw new Error(`IB live refresh failed (${resp.status})`)
    return resp.json()
  }, [])

  const runCommitteeCycle = useCallback((nextData) => {
    const nextTiles = Array.isArray(nextData?.tiles) ? nextData.tiles : []
    if (nextTiles.length === 0) {
      setCommitteeBySymbol({})
      setExitRecBySymbol({})
      setLiveUpdatedAt(nextData?.updated_at || new Date().toISOString())
      return
    }

    setCommitteeBySymbol((prevCommitteeMap) => {
      const nextCommitteeMap = {}
      const nextExitRecs = {}
      for (const tile of nextTiles) {
        const symbol = String(tile?.symbol || '').toUpperCase()
        const prevCommittee = prevCommitteeMap[symbol]
        const liveState = buildLiveState(tile, prevCommittee?.live_state || null, sensitivityMode)
        const committee = evaluateCommittee(tile, liveState, prevCommittee)
        nextCommitteeMap[symbol] = committee
        nextExitRecs[symbol] = generateExitRecommendation(tile, liveState, committee)
      }
      setExitRecBySymbol(nextExitRecs)
      setLiveUpdatedAt(nextData?.updated_at || new Date().toISOString())
      return nextCommitteeMap
    })
  }, [sensitivityMode])

  const tiles = useMemo(() => {
    return Array.isArray(data?.tiles) ? data.tiles : []
  }, [data?.tiles])

  const loadContext = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const params = new URLSearchParams({
        mode: 'intraday',
        chart_style: 'line',
        horizon_bars: '20',
        projection_mode: 'stitched',
        intraday_bar_seconds: '30',
        intraday_window_bars: '780',
      })
      const resp = await fetch(`${API_BASE}/symbol-tracker/tiles?${params.toString()}`)
      if (!resp.ok) throw new Error(`Failed to load Living Chart (${resp.status})`)
      const payload = await resp.json()
      setData(payload)
      setContextReloadAt(new Date().toISOString())
      setLiveUpdatedAt(payload?.updated_at || new Date().toISOString())

      const tiles = payload?.tiles || []
      const urlSym = String(new URLSearchParams(window.location.search).get('symbol') || '').toUpperCase()
      const pick = urlSym && tiles.some((t) => String(t?.symbol || '').toUpperCase() === urlSym)
        ? urlSym
        : (tiles[0]?.symbol ? String(tiles[0].symbol).toUpperCase() : null)
      setSelectedSymbol((prev) => {
        if (prev && tiles.some((t) => String(t?.symbol || '').toUpperCase() === prev)) return prev
        return pick
      })

      try {
        const ibPayload = await fetchIbLive(tiles)
        const merged = ibPayload ? mergeIbLiveRows(payload, ibPayload) : payload
        setData(merged)
        runCommitteeCycle(merged)
      } catch {
        runCommitteeCycle(payload)
      }
    } catch (e) {
      setError(e.message || 'Failed to load data.')
    } finally {
      setLoading(false)
    }
  }, [fetchIbLive, runCommitteeCycle])

  useEffect(() => {
    loadContext()
  }, [loadContext])

  useEffect(() => {
    const urlSym = String(searchParams.get('symbol') || '').toUpperCase()
    if (!urlSym || tiles.length === 0) return
    if (tiles.some((t) => String(t?.symbol || '').toUpperCase() === urlSym)) {
      setSelectedSymbol(urlSym)
    }
  }, [searchParams, tiles])

  /** Re-center chart on the new symbol: do not keep another symbol's pan/zoom. */
  useEffect(() => {
    const cur = selectedSymbol ? String(selectedSymbol).toUpperCase() : null
    const prev = prevSelectedSymbolRef.current
    if (cur && prev != null && cur !== prev) {
      setViewportLocked(false)
      setFollowLatest(true)
      setLayoutRevision((r) => r + 1)
    }
    if (cur) prevSelectedSymbolRef.current = cur
  }, [selectedSymbol])

  useEffect(() => {
    if (Array.isArray(data?.tiles) && data.tiles.length > 0) {
      runCommitteeCycle(data)
    }
  }, [sensitivityMode]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!import.meta.env.DEV || tiles.length === 0) return
    const samples = tiles.map((t) => {
      const sym = String(t.symbol || '').toUpperCase()
      const c = committeeBySymbol[sym]
      const e = exitRecBySymbol[sym]
      return { sym, stance: c?.committee_stance, urgency: e?.urgency }
    })
    if (tiles.length >= 2 && samples.every((s) => String(s.urgency || '').toUpperCase() === 'PREPARE')) {
      console.warn('[LivingChart] All open symbols have exit urgency PREPARE — check engine thresholds if unexpected.', samples)
    }
    if (tiles.length >= 2) {
      const primaryLabels = tiles.map((t) => {
        const sym = String(t.symbol || '').toUpperCase()
        return resolveLivingChartSymbolDisplay(
          t,
          committeeBySymbol[sym],
          exitRecBySymbol[sym],
          committeeBySymbol[sym]?.live_state || null,
        ).primaryAction
      })
      const uniq = new Set(primaryLabels)
      if (uniq.size === 1 && !primaryLabels.includes('HOLD')) {
        console.warn('[LivingChart] All symbols share the same non-HOLD primary action — verify data if unexpected.', primaryLabels)
      }
    }
    for (const t of tiles) {
      const sym = String(t.symbol || '').toUpperCase()
      if (!committeeBySymbol[sym]) {
        console.warn('[LivingChart] Missing committeeBySymbol entry for tiled symbol (possible key mismatch).', sym)
      }
    }
    try {
      const qs = new URLSearchParams(window.location.search)
      if (qs.get('lcDebug') === '1') {
        const bySym = {}
        for (const t of tiles) {
          const sym = String(t.symbol || '').toUpperCase()
          const c = committeeBySymbol[sym]
          const rowLive = c?.live_state || null
          bySym[sym] = resolveLivingChartSymbolDisplay(t, c, exitRecBySymbol[sym], rowLive)
        }
        window.__lcLivingChartDebug = { updatedAt: new Date().toISOString(), bySym }
      }
      if (qs.get('lcFlowDebug') === '1') {
        const flowDbg = {}
        for (const tile of tiles) {
          const sym = String(tile.symbol || '').toUpperCase()
          const barList = Array.isArray(tile?.chart?.bars) ? tile.chart.bars : []
          const rowComm = committeeBySymbol[sym]
          const raw = computeFlowIntelligenceRaw({
            bars: barList,
            tile,
            liveState: rowComm?.live_state || null,
            liveUpdatedAt: liveUpdatedAt,
          })
          flowDbg[sym] = { raw, hysteresis: flowHystRef.current[sym] }
        }
        window.__lcFlowDebug = { updatedAt: new Date().toISOString(), bySym: flowDbg }
      }
    } catch {
      /* ignore */
    }
  }, [tiles, committeeBySymbol, exitRecBySymbol, liveUpdatedAt])

  const refreshIbOnly = useCallback(async () => {
    try {
      setError('')
      const ibPayload = await fetchIbLive(data?.tiles || [])
      setLiveUpdatedAt(ibPayload?.updated_at || new Date().toISOString())
      if (ibPayload) {
        setData((prev) => {
          const merged = mergeIbLiveRows(prev, ibPayload)
          runCommitteeCycle(merged)
          return merged
        })
      } else {
        runCommitteeCycle(data)
      }
    } catch (e) {
      setLiveUpdatedAt(new Date().toISOString())
      runCommitteeCycle(data)
      setError(e.message || 'IB live refresh failed.')
    }
  }, [data, fetchIbLive, runCommitteeCycle])

  useVisibleInterval(refreshIbOnly, 30000)

  const activeTile = useMemo(() => {
    const sym = String(selectedSymbol || '').toUpperCase()
    return tiles.find((t) => String(t?.symbol || '').toUpperCase() === sym) || null
  }, [tiles, selectedSymbol])

  const committee = activeTile
    ? committeeBySymbol[String(activeTile.symbol || '').toUpperCase()]
    : null
  const exitRec = activeTile
    ? exitRecBySymbol[String(activeTile.symbol || '').toUpperCase()]
    : null
  const liveState = committee?.live_state || null

  const conditionalKeys = useMemo(() => {
    if (!activeTile || !liveState) return []
    const barList = Array.isArray(activeTile?.chart?.bars) ? activeTile.chart.bars : []
    const pack = buildLivingChartShapesAndTA({
      tile: activeTile,
      bars: barList,
      liveState,
      committee,
      exitRec,
      showAdvancedTA,
      horizonBars,
    })
    return pack.conditionalKeys || []
  }, [activeTile, liveState, committee, exitRec, showAdvancedTA, horizonBars])

  const activeConditionSummary = useMemo(
    () => (activeTile && liveState
      ? resolveActiveConditionSummary(conditionalKeys, liveState, activeTile)
      : { dominantKey: null, secondaryKey: null }),
    [activeTile, liveState, conditionalKeys],
  )

  const chartChips = useMemo(
    () => (activeTile && liveState
      ? liveConditionChipsActive(conditionalKeys, liveState, activeTile, 2)
      : []),
    [activeTile, liveState, conditionalKeys],
  )

  const dominantAttention = useMemo(
    () => resolveDominantAttention(activeConditionSummary.dominantKey),
    [activeConditionSummary.dominantKey],
  )

  const optionalStripCue = useMemo(() => {
    if (!activeTile) return null
    const cue = stripOptionalCue(
      activeTile,
      exitRec,
      liveState,
      committee,
      chartChips.map((c) => c.key),
      activeConditionSummary.dominantKey,
    )
    if (!cue) return null
    if (cue.key === 'path_watch' && activeConditionSummary.dominantKey === 'path_diverging') return null
    const chipLabels = new Set(chartChips.map((c) => c.label))
    if (chipLabels.has(cue.label)) return null
    return cue
  }, [activeTile, exitRec, liveState, committee, chartChips, activeConditionSummary.dominantKey])

  const activeRiskTier = useMemo(() => {
    if (!activeTile) return 'calm'
    return riskPressureTier(committee, exitRec, liveState, activeTile)
  }, [activeTile, committee, exitRec, liveState])

  const activeDisplay = useMemo(() => {
    if (!activeTile) return null
    return resolveLivingChartSymbolDisplay(activeTile, committee, exitRec, liveState)
  }, [activeTile, committee, exitRec, liveState])

  const [flowVisibleBySymbol, setFlowVisibleBySymbol] = useState({})

  useEffect(() => {
    const out = {}
    const uid = liveUpdatedAt || data?.updated_at
    for (const tile of tiles) {
      const sym = String(tile?.symbol || '').toUpperCase()
      const barList = Array.isArray(tile?.chart?.bars) ? tile.chart.bars : []
      const rowComm = committeeBySymbol[sym]
      const liveSt = rowComm?.live_state || null
      const raw = computeFlowIntelligenceRaw({
        bars: barList,
        tile,
        liveState: liveSt,
        liveUpdatedAt: uid,
      })
      const prevH = flowHystRef.current[sym] ?? null
      const vis = applyFlowVisibleState(prevH, raw)
      flowHystRef.current[sym] = vis.hysteresis
      out[sym] = vis
    }
    setFlowVisibleBySymbol(out)
  }, [tiles, committeeBySymbol, liveUpdatedAt, data?.updated_at])

  const activeFlowVisible = useMemo(() => {
    if (!activeTile) return null
    const sym = String(activeTile.symbol || '').toUpperCase()
    return flowVisibleBySymbol[sym] || null
  }, [activeTile, flowVisibleBySymbol])

  const tapeSnapshot = useTapeSnapshot(selectedSymbol, 3200)
  const tapeApiEnabled = isTapeApiEnabled()
  const tapeActive = tapeSnapshot?.tape_active_for_ui === true

  const positionPlotTag = useMemo(
    () => dominantActivePlotTag(activeConditionSummary.dominantKey),
    [activeConditionSummary.dominantKey],
  )

  const flowAnnotationText = useMemo(() => {
    if (positionPlotTag) return null
    return getFlowAnnotationText(activeFlowVisible?.annotation_key)
  }, [positionPlotTag, activeFlowVisible?.annotation_key])

  const flowBurstForPlot = useMemo(() => {
    if (!activeFlowVisible?.burst_overlay) return null
    const { burst_x0_ms, burst_x1_ms, burst_y0, burst_y1 } = activeFlowVisible
    if (burst_x0_ms == null || burst_x1_ms == null || burst_y0 == null || burst_y1 == null) return null
    return {
      x0_ms: burst_x0_ms,
      x1_ms: burst_x1_ms,
      y0: burst_y0,
      y1: burst_y1,
    }
  }, [activeFlowVisible])

  const flowChips = useMemo(
    () => selectFlowChipsForChart(activeFlowVisible, chartChips.length),
    [activeFlowVisible, chartChips.length],
  )

  useEffect(() => {
    const primary = activeDisplay?.primaryAction != null ? String(activeDisplay.primaryAction) : null
    const sub = activeDisplay?.secondaryFallback?.line
    const badges = primary ? [primary] : []
    const kpi = {}
    if (primary) kpi.primary_action = primary
    if (sub) kpi.caution_line = sub
    if (activeFlowVisible && activeFlowVisible.confidence !== 'UNAVAILABLE') {
      kpi.flow_direction = activeFlowVisible.flow_direction
      kpi.move_efficiency = activeFlowVisible.move_efficiency
      kpi.move_quality_line = activeFlowVisible.line2
      kpi.flow_caution_line = activeFlowVisible.line2
      kpi.absorption_state = activeFlowVisible.absorption_state
      kpi.exhaustion_state = activeFlowVisible.exhaustion_state
      kpi.unwind_risk = activeFlowVisible.unwind_risk
      kpi.liquidity_stress = activeFlowVisible.liquidity_stress
    }
    mergeAskMipRuntime({
      page_id: 'symbol_tracker',
      page_route: pathname,
      session_mode: 'live',
      symbol: selectedSymbol || null,
      visible_widget_ids: ['living_chart_main'],
      selected_widget_id: 'living_chart_main',
      current_kpi_snapshot: Object.keys(kpi).length ? kpi : null,
      current_badges_or_statuses: badges,
    })
    return () => {
      mergeAskMipRuntime({
        symbol: null,
        visible_widget_ids: [],
        selected_widget_id: null,
        current_kpi_snapshot: null,
        current_badges_or_statuses: [],
        page_id: null,
      })
    }
  }, [pathname, selectedSymbol, activeDisplay, activeFlowVisible, mergeAskMipRuntime])

  const bumpChartLayout = useCallback(() => {
    setLayoutRevision((r) => r + 1)
  }, [])

  const bars = useMemo(() => {
    if (!activeTile?.chart?.bars) return []
    return activeTile.chart.bars
  }, [activeTile])

  const selectSymbol = useCallback(
    (sym) => {
      const u = String(sym || '').toUpperCase()
      setSelectedSymbol(u)
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev)
        if (u) next.set('symbol', u)
        else next.delete('symbol')
        return next
      }, { replace: true })
    },
    [setSearchParams],
  )

  const onViewportLockedChange = useCallback((locked) => {
    if (locked) setViewportLocked(true)
  }, [])

  const unlockFollowLatest = useCallback(() => {
    setViewportLocked(false)
    setFollowLatest(true)
    bumpChartLayout()
  }, [bumpChartLayout])

  return (
    <div className="lc-page">
      <header className="lc-head">
        <div>
          <h2 className="lc-title">Living Chart</h2>
          <p className="lc-sub">
            Chart-first view for one open position — complements the{' '}
            <a className="lc-inline-link" href="/live-intelligence">Live Intelligence Cockpit</a>.
            Terms:{' '}
            <GlossaryHoverCard scope="risk" entryKey="portfolio_drawdown_pct" /> ·{' '}
            <GlossaryHoverCard scope="positions" entryKey="position_size_pct" />.
          </p>
        </div>
      </header>

      <div className="lc-toolbar" role="toolbar" aria-label="Chart controls">
        <div className="lc-toolbar-group">
          <span className="lc-toolbar-group-label">Display</span>
          <div className="lc-toolbar-group-fields">
            <label className="lc-field">
              <span>Chart</span>
              <select value={chartStyle} onChange={(e) => setChartStyle(e.target.value)}>
                <option value="line">Line</option>
                <option value="candles">Candles</option>
              </select>
            </label>
            <label className="lc-field">
              <span>Expected horizon</span>
              <select value={String(horizonBars)} onChange={(e) => setHorizonBars(Number(e.target.value))}>
                <option value="3">3 bars</option>
                <option value="5">5 bars</option>
                <option value="10">10 bars</option>
                <option value="20">20 bars</option>
              </select>
            </label>
            <label className="lc-field">
              <span>Risk sensitivity</span>
              <select value={sensitivityMode} onChange={(e) => setSensitivityMode(e.target.value)}>
                <option value="CONSERVATIVE">Conservative</option>
                <option value="BALANCED">Balanced</option>
                <option value="AGGRESSIVE">Aggressive</option>
              </select>
            </label>
          </div>
        </div>
        <div className="lc-toolbar-divider" aria-hidden />
        <div className="lc-toolbar-group">
          <span className="lc-toolbar-group-label">Follow price</span>
          <div className="lc-toolbar-group-fields lc-toolbar-group-fields--row">
            <label
              className="lc-check"
              title="Keeps the chart scrolled to the latest bar until you pan or zoom away."
            >
              <input
                type="checkbox"
                checked={followLatest}
                onChange={(e) => {
                  const on = e.target.checked
                  setFollowLatest(on)
                  if (on) {
                    setViewportLocked(false)
                    bumpChartLayout()
                  }
                }}
              />
              Track latest bars
            </label>
            {viewportLocked ? (
              <button
                type="button"
                className="lc-btn lc-btn--small"
                onClick={unlockFollowLatest}
                aria-label="Jump to latest bar and resume tracking latest bars"
                title="Re-centers the time axis on the latest bar and turns tracking back on. Your manual zoom may reset."
              >
                Jump to latest
              </button>
            ) : null}
            <label
              className="lc-check"
              title="Adds VWAP, Bollinger bands, and support/resistance on the chart."
            >
              <input
                type="checkbox"
                checked={showAdvancedTA}
                onChange={(e) => setShowAdvancedTA(e.target.checked)}
              />
              Extra chart context
            </label>
          </div>
        </div>
        <div className="lc-toolbar-actions" aria-label="Data refresh">
          <button
            type="button"
            className="lc-btn"
            onClick={refreshIbOnly}
            title="Fetches latest intraday bars and prices for open positions only."
          >
            Update intraday
          </button>
          <button
            type="button"
            className="lc-btn lc-btn--secondary"
            onClick={loadContext}
            title="Reloads full position context from the server (tiles, thesis, levels)."
          >
            Reload positions
          </button>
        </div>
      </div>

      {error ? <div className="lc-error">{error}</div> : null}
      {loading ? <div className="lc-loading">Loading…</div> : null}

      {!loading && tiles.length === 0 ? (
        <div className="lc-empty">No open positions found.</div>
      ) : null}

      {!loading && tiles.length > 0 ? (
        <div className="lc-body">
          <nav className="lc-rail" aria-label="Positions">
            {tiles.map((t) => {
              const sym = String(t.symbol || '').toUpperCase()
              const active = sym === String(selectedSymbol || '').toUpperCase()
              const rowComm = committeeBySymbol[sym]
              const rowExit = exitRecBySymbol[sym]
              const rowLive = rowComm?.live_state || null
              const rowDisplay = resolveLivingChartSymbolDisplay(t, rowComm, rowExit, rowLive)
              const rowRisk = rowDisplay.riskTier
              const label = formatSymbolLabel(t.symbol, t.market_type)
              return (
                <button
                  key={sym}
                  type="button"
                  title={`${label} · ${rowDisplay.primaryAction} · ${rowDisplay.riskLabel}`}
                  className={`lc-rail-btn lc-rail-btn--risk-${rowRisk}${active ? ' lc-rail-btn--active' : ''}`}
                  onClick={() => selectSymbol(sym)}
                >
                  <span className="lc-rail-top">
                    <span className="lc-rail-accent" aria-hidden />
                    <span
                      className={`lc-rail-urgency lc-rail-urgency--${rowRisk}`}
                      title={rowDisplay.riskLabel}
                      aria-hidden
                    />
                    <span className="lc-rail-sym">{label}</span>
                    <span className="lc-rail-posture">{rowDisplay.primaryActionShort}</span>
                  </span>
                  <span className={`lc-rail-pnl ${Number(t.unrealized_pnl) >= 0 ? 'lc-rail-pnl--pos' : 'lc-rail-pnl--neg'}`}>
                    {fmtSigned(t.unrealized_pnl, 0)}
                  </span>
                </button>
              )
            })}
          </nav>

          <div className="lc-main">
            {activeTile ? (
              <>
                <div className={`lc-strip lc-strip--risk-${activeRiskTier}`}>
                  <div className="lc-strip-monitor">
                    <div className="lc-strip-identity">
                      <span className="lc-strip-title">{formatSymbolLabel(activeTile.symbol, activeTile.market_type)}</span>
                      <span className="lc-strip-meta">{activeTile.side} · Qty {fmtNum(activeTile.quantity, 0)}</span>
                    </div>
                    <div className="lc-strip-state" aria-label="Position state">
                      {activeFlowVisible && activeFlowVisible.confidence !== 'UNAVAILABLE' ? (
                        <div className="lc-strip-flow" aria-label="Flow intelligence">
                          <span className="lc-flow-line">{activeFlowVisible.line1}</span>
                          <span className="lc-flow-line lc-flow-line--quality">{activeFlowVisible.line2}</span>
                        </div>
                      ) : null}
                      {tapeApiEnabled && tapeActive ? (
                        <div className="lc-strip-tape" aria-label="Tape observation">
                          <span className="lc-tape-line">
                            {tapeSnapshot.explanation_short || tapeSnapshot.move_quality || '—'}
                          </span>
                        </div>
                      ) : null}
                      {activeDisplay ? (
                        <div className="lc-strip-state-inner">
                          <span className="lc-state-primary">{activeDisplay.primaryAction}</span>
                          {dominantAttention ? (
                            <span className="lc-attention-cue" title="Live condition">{dominantAttention.line}</span>
                          ) : null}
                          {activeDisplay.secondaryFallback ? (
                            <span className="lc-state-fallback">{activeDisplay.secondaryFallback.line}</span>
                          ) : null}
                          <span className="lc-state-thesis">{activeDisplay.thesisState}</span>
                          <span className="lc-state-risk">{activeDisplay.riskLabel}</span>
                        </div>
                      ) : null}
                    </div>
                    <div className="lc-strip-kpis-wrap">
                      <div className="lc-strip-cluster" aria-label="Price and P and L">
                        <div><span>Last</span><b>{fmtNum(activeTile.current_price ?? activeTile.overlays?.current, 4)}</b></div>
                        <div><span>Entry</span><b>{fmtNum(activeTile.entry_price, 4)}</b></div>
                        <div><span>P&amp;L</span><b className={Number(activeTile.unrealized_pnl) >= 0 ? 'lc-kpi-pos' : 'lc-kpi-neg'}>{fmtSigned(activeTile.unrealized_pnl, 2)}</b></div>
                      </div>
                      <div className="lc-strip-cluster" aria-label="Distance to levels">
                        <div><span>Dist SL</span><b>{fmtPct(liveState?.derived_features?.distance_to_sl_pct ?? activeTile?.progress_metrics?.distance_to_sl_pct)}</b></div>
                        <div>
                          <span>Dist TP</span>
                          <b className={Number.isFinite(Number(activeTile?.overlays?.take_profit)) ? '' : 'lc-kpi-muted'}>
                            {Number.isFinite(Number(activeTile?.overlays?.take_profit))
                              ? fmtPct(liveState?.derived_features?.distance_to_tp_pct ?? activeTile?.progress_metrics?.distance_to_tp_pct)
                              : '—'}
                          </b>
                        </div>
                      </div>
                    </div>
                    <span className="lc-strip-ts">Live {fmtTime(liveUpdatedAt)}</span>
                  </div>
                  {optionalStripCue ? (
                    <div className="lc-strip-cue">
                      <span className={`lc-chip lc-chip--${optionalStripCue.tone}`}>{optionalStripCue.label}</span>
                    </div>
                  ) : null}
                </div>

                <div className="lc-chart-shell">
                  {(chartChips.length > 0 || flowChips.length > 0
                    || (tapeApiEnabled && tapeActive && tapeSnapshot?.active_chips?.length > 0)) ? (
                    <div className="lc-chart-cues" aria-label="Active live conditions">
                      {chartChips.map((b) => (
                        <span key={b.key} className={`lc-cue lc-cue--${b.tone}`}>{b.label}</span>
                      ))}
                      {flowChips.map((b) => (
                        <span key={b.key} className={`lc-cue lc-cue--flow lc-cue--${b.tone}`}>{b.label}</span>
                      ))}
                      {tapeApiEnabled && tapeActive && Array.isArray(tapeSnapshot?.active_chips)
                        ? tapeSnapshot.active_chips.map((label, idx) => (
                          <span key={`tape-${idx}-${label}`} className="lc-cue lc-cue--tape">{label}</span>
                        ))
                        : null}
                    </div>
                  ) : null}
                  {tapeApiEnabled && tapeActive ? (
                    <div className="lc-recent-tape" aria-label="Recent tape buy versus sell">
                      <div className="lc-recent-tape__head">
                        <span className="lc-recent-tape__title">Tape (recent 60s): Buy vs Sell</span>
                        <span
                          className="lc-recent-tape__help"
                          title="This shows who was more aggressive in recent prints, not total historical bar volume."
                        >
                          ?
                        </span>
                      </div>
                      <div className="lc-recent-tape__nums">
                        <span className="lc-recent-tape__buy">
                          Buy {fmtNum(tapeSnapshot.buy_volume_60s, 0)}
                        </span>
                        <span className="lc-recent-tape__sell">
                          Sell {fmtNum(tapeSnapshot.sell_volume_60s, 0)}
                        </span>
                        {Number(tapeSnapshot.unknown_volume_60s) > 0 ? (
                          <span className="lc-recent-tape__unk">
                            Unknown {fmtNum(tapeSnapshot.unknown_volume_60s, 0)}
                          </span>
                        ) : null}
                      </div>
                      {(() => {
                        const b = Number(tapeSnapshot.buy_volume_60s)
                        const s = Number(tapeSnapshot.sell_volume_60s)
                        const u = Number(tapeSnapshot.unknown_volume_60s)
                        const t = (Number.isFinite(b) ? b : 0) + (Number.isFinite(s) ? s : 0) + (Number.isFinite(u) && u > 0 ? u : 0)
                        if (!(t > 0)) return null
                        const pb = ((Number.isFinite(b) ? b : 0) / t) * 100
                        const ps = ((Number.isFinite(s) ? s : 0) / t) * 100
                        const pu = Math.max(0, 100 - pb - ps)
                        return (
                          <div className="lc-recent-tape__bar" aria-hidden>
                            <div className="lc-recent-tape__seg lc-recent-tape__seg--buy" style={{ width: `${pb}%` }} />
                            <div className="lc-recent-tape__seg lc-recent-tape__seg--sell" style={{ width: `${ps}%` }} />
                            {pu > 0.5 ? (
                              <div className="lc-recent-tape__seg lc-recent-tape__seg--unk" style={{ width: `${pu}%` }} />
                            ) : null}
                          </div>
                        )
                      })()}
                    </div>
                  ) : null}
                  <Suspense fallback={<div className="lc-loading">Loading chart…</div>}>
                    <LivingChartErrorBoundary>
                      <LivingChartPlot
                        tile={activeTile}
                        bars={bars}
                        chartStyle={chartStyle}
                        horizonBars={horizonBars}
                        showAdvancedTA={showAdvancedTA}
                        liveState={liveState}
                        committee={committee}
                        exitRec={exitRec}
                        conditionalKeys={conditionalKeys}
                        dominantActiveKey={activeConditionSummary.dominantKey}
                        followLatest={followLatest}
                        viewportLocked={viewportLocked}
                        onViewportLockedChange={onViewportLockedChange}
                        layoutRevision={layoutRevision}
                        flowBurst={flowBurstForPlot}
                        flowAnnotationText={flowAnnotationText}
                        tapeSnapshot={tapeSnapshot}
                        className="lc-plot"
                      />
                    </LivingChartErrorBoundary>
                  </Suspense>
                </div>

                <div className="lc-legend-min">
                  <span><i className="lc-swatch lc-swatch--price" /> Price</span>
                  <span><i className="lc-swatch lc-swatch--exp" /> Expected</span>
                  <span><i className="lc-swatch lc-swatch--entry" /> Entry</span>
                  <span><i className="lc-swatch lc-swatch--tp" /> Target</span>
                  <span><i className="lc-swatch lc-swatch--sl" /> Stop</span>
                </div>
              </>
            ) : (
              <div className="lc-empty">Select a position.</div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  )
}
