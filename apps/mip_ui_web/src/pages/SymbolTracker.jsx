import { Component, lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'

import { API_BASE } from '../config/apiBase'
import useVisibleInterval from '../hooks/useVisibleInterval'
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
  primaryPostureLabel,
  primaryPostureShort,
  thesisStateLabel,
  riskPressureLabel,
  liveConditionChips,
  stripOptionalCue,
} from '../lib/livingChartVisualState'
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

  useEffect(() => {
    if (Array.isArray(data?.tiles) && data.tiles.length > 0) {
      runCommitteeCycle(data)
    }
  }, [sensitivityMode]) // eslint-disable-line react-hooks/exhaustive-deps

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

  const chartChips = useMemo(
    () => (activeTile && liveState
      ? liveConditionChips(conditionalKeys, liveState, activeTile)
      : []),
    [activeTile, liveState, conditionalKeys],
  )

  const optionalStripCue = useMemo(
    () => (activeTile
      ? stripOptionalCue(activeTile, exitRec, liveState, committee, chartChips.map((c) => c.key))
      : null),
    [activeTile, exitRec, liveState, committee, chartChips],
  )

  const activeRiskTier = useMemo(() => {
    if (!activeTile) return 'calm'
    return riskPressureTier(committee, exitRec, liveState, activeTile)
  }, [activeTile, committee, exitRec, liveState])

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
          <span className="lc-toolbar-group-label">View</span>
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
          <span className="lc-toolbar-group-label">Time window</span>
          <div className="lc-toolbar-group-fields lc-toolbar-group-fields--row">
            <label
              className="lc-check"
              title="While checked, the chart stays scrolled to the newest bar until you pan or zoom away."
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
                title="Re-centers the time axis on the latest bar and turns tracking back on. Your manual zoom may reset."
              >
                Jump to latest
              </button>
            ) : null}
            <label className="lc-check">
              <input
                type="checkbox"
                checked={showAdvancedTA}
                onChange={(e) => setShowAdvancedTA(e.target.checked)}
              />
              More context (VWAP/BB/S/R)
            </label>
          </div>
        </div>
        <div className="lc-toolbar-actions">
          <button type="button" className="lc-btn" onClick={refreshIbOnly}>Refresh live</button>
          <button type="button" className="lc-btn lc-btn--secondary" onClick={loadContext}>Reload context</button>
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
              const rowRisk = riskPressureTier(rowComm, rowExit, rowLive, t)
              const postureS = primaryPostureShort(rowComm, rowExit)
              const label = formatSymbolLabel(t.symbol, t.market_type)
              return (
                <button
                  key={sym}
                  type="button"
                  title={`${label} · ${primaryPostureLabel(rowComm, rowExit)} · ${riskPressureLabel(rowComm, rowExit, rowLive, t)}`}
                  className={`lc-rail-btn lc-rail-btn--risk-${rowRisk}${active ? ' lc-rail-btn--active' : ''}`}
                  onClick={() => selectSymbol(sym)}
                >
                  <span className="lc-rail-top">
                    <span className="lc-rail-accent" aria-hidden />
                    <span className="lc-rail-sym">{label}</span>
                    <span className="lc-rail-posture">{postureS}</span>
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
                      <span className="lc-state-primary">{primaryPostureLabel(committee, exitRec)}</span>
                      <span className="lc-state-thesis">{thesisStateLabel(activeTile)}</span>
                      <span className="lc-state-risk">{riskPressureLabel(committee, exitRec, liveState, activeTile)}</span>
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
                  {chartChips.length > 0 ? (
                    <div className="lc-chart-cues" aria-label="Active chart overlays">
                      {chartChips.map((b) => (
                        <span key={b.key} className={`lc-cue lc-cue--${b.tone}`}>{b.label}</span>
                      ))}
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
                        followLatest={followLatest}
                        viewportLocked={viewportLocked}
                        onViewportLockedChange={onViewportLockedChange}
                        layoutRevision={layoutRevision}
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
