import { Component, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../config/apiBase'
import { fetchWithRetry } from '../utils/fetchRetry'
import useVisibleInterval from '../hooks/useVisibleInterval'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import LicTopTile from '../components/lic/LicTopTile'
import LicPositionRadar from '../components/lic/LicPositionRadar'
import LicWorldsScenarios from '../components/lic/LicWorldsScenarios'
import LicAnalogPanel from '../components/lic/LicAnalogPanel'
import LicSimulatorPanel from '../components/lic/LicSimulatorPanel'
import LicEntryIntelligencePanel from '../components/lic/LicEntryIntelligencePanel'
import {
  alignRadarTupleToBand,
  computeRawRadarTuple,
  radarInterpretationHint,
  radarTupleToChartData,
  smoothRadarScores,
} from '../components/lic/licPositionRadarModel'
import {
  bandLabel,
  caseFileImplicationDisplay,
  resolveDecisionPresentation,
} from '../components/lic/licDecisionPresentation'
import { buildVisibleTilePresentation } from '../components/lic/licTilePresentation'
import './LiveIntelligenceCockpit.css'
import { useAskMipPageRuntime } from '../hooks/useAskMipPageRuntime'

/** Prevents a single throw from blanking the whole app when API field shapes drift. */
class LicErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    console.error('LiveIntelligenceCockpit render error:', error, info?.componentStack)
  }

  render() {
    if (this.state.error) {
      const msg = this.state.error?.message || String(this.state.error)
      return (
        <div className="lic-page lic-page--crash">
          <h2 className="lic-crash-title">Live Intelligence could not render</h2>
          <p className="lic-crash-copy">
            This usually means a field from the API is not a string or number where the UI expects one. Open the
            browser console for the full stack. Error:
          </p>
          <pre className="lic-crash-pre">{msg}</pre>
          <button type="button" className="lic-crash-btn" onClick={() => window.location.reload()}>
            Reload page
          </button>
        </div>
      )
    }
    return this.props.children
  }
}

function sessionFeedRow() {
  const ts = new Date().toISOString()
  return {
    timestamp: ts,
    ts,
    symbol: 'SESSION',
    scope: 'session',
    transition: 'SESSION_START',
    state_transition: 'SESSION_START',
    what_changed: 'Baseline set — new rows only on material changes vs prior step.',
    reason: 'Baseline set — new rows only on material changes vs prior step.',
    action_implication: 'Scan tiles: primary action, snapshot, strip.',
    final_recommendation: '\u2014',
    severity: 'INFO',
  }
}

function feedEventHasBody(ev) {
  if (!ev || typeof ev !== 'object') return false
  if (String(ev.symbol || '').toUpperCase() === 'SESSION') return true
  const impl = String(ev.action_implication || '').trim()
  const r = String(ev.reason || ev.what_changed || '').trim()
  const tr = String(ev.state_transition || ev.transition || '').trim()
  return Boolean(impl || r || tr)
}

function attentionTooltip(intel) {
  const c = intel?.attention_components || {}
  const parts = Object.entries(c).map(([k, v]) => `${k}: ${v}`)
  return parts.length ? parts.join(' \u00b7 ') : 'Attention breakdown'
}

/** Coerce API values so React never receives objects as text children. */
function safeText(v, fallback = '\u2014') {
  if (v == null || v === '') return fallback
  if (typeof v === 'string' || typeof v === 'number') return String(v)
  if (typeof v === 'boolean') return v ? 'Yes' : 'No'
  if (typeof v === 'object') {
    try {
      return JSON.stringify(v).slice(0, 400)
    } catch {
      return fallback
    }
  }
  return String(v)
}

function attentionScoreNumber(intel) {
  const x = intel?.attention_score
  if (typeof x === 'number' && Number.isFinite(x)) return x
  const n = Number(x)
  return Number.isFinite(n) ? n : 0
}


function formatHoldingAge(iso) {
  if (!iso) return '—'
  const t = new Date(iso).getTime()
  if (!Number.isFinite(t)) return '—'
  const h = Math.floor((Date.now() - t) / 3600000)
  if (h < 72) return `${h}h`
  return `${Math.floor(h / 24)}d`
}

function formatMoney(n) {
  const x = Number(n)
  if (!Number.isFinite(x)) return '—'
  const s = Math.abs(x).toFixed(0)
  return x >= 0 ? `+$${s}` : `-$${s}`
}

function formatFeedTime(row) {
  const s = row.ts || row.timestamp
  if (!s) return ''
  const d = new Date(s)
  if (!Number.isFinite(d.getTime())) return String(s)
  return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

function asArray(x) {
  return Array.isArray(x) ? x : []
}

/** Avoid wiping tile intelligence when the API returns an empty map but we still have positions. */
function shouldApplyIntelligence(nextBySymbol, tileCount) {
  if (!nextBySymbol || typeof nextBySymbol !== 'object') return false
  if (tileCount <= 0) return true
  return Object.keys(nextBySymbol).length > 0
}


function mergeTrackerIb(trackerPayload, livePayload) {
  if (!trackerPayload || !Array.isArray(trackerPayload.tiles)) return trackerPayload
  const rows = Array.isArray(livePayload?.rows) ? livePayload.rows : []
  if (rows.length === 0) return trackerPayload
  const bySymbol = new Map(rows.map((r) => [String(r.symbol || '').toUpperCase(), r]))
  const intervalMinutes = Number(livePayload?.interval_minutes)
  const barSecondsRaw = Number(livePayload?.intraday_bar_seconds)
  const hasBarSeconds = Number.isFinite(barSecondsRaw) && barSecondsRaw > 0
  const tiles = trackerPayload.tiles.map((tile) => {
    const symbol = String(tile?.symbol || '').toUpperCase()
    const live = bySymbol.get(symbol)
    if (!live || !Array.isArray(live.bars) || live.bars.length === 0) return tile
    const bars = live.bars
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
    ...trackerPayload,
    tiles,
    updated_at: livePayload?.updated_at || new Date().toISOString(),
  }
}

/** Keep deterministic-step POST small: full prior intel includes evidence/worlds/sim blobs. */
const PRIOR_INTEL_SLIM_KEYS = new Set([
  'feed_fingerprint',
  'case_file_signature',
  'last_case_file_emit_at',
  'final_recommendation',
  'exit_urgency',
  'thesis_fracture',
  'novelty_state',
  'pattern_label',
  'attention_score',
  'last_ai_refresh_at',
  'last_material_change_at',
  'sl_near',
  'dsl_bucket',
  'target_room_bucket',
  'analog_tier_key',
  'dominant_world_id',
  'regret_bucket',
  'portfolio_factor_local',
])

function slimPriorIntelligence(intelBySymbol) {
  if (!intelBySymbol || typeof intelBySymbol !== 'object') return {}
  const out = {}
  for (const [sym, row] of Object.entries(intelBySymbol)) {
    if (!row || typeof row !== 'object') continue
    const slim = {}
    for (const k of PRIOR_INTEL_SLIM_KEYS) {
      if (Object.prototype.hasOwnProperty.call(row, k)) slim[k] = row[k]
    }
    out[sym] = slim
  }
  return out
}

const MAX_BARS_FOR_STEP = 192

const MAX_ANALOG_PER_SYMBOL = 96

function trimTilesForStep(tiles, maxBars = MAX_BARS_FOR_STEP) {
  if (!Array.isArray(tiles)) return []
  return tiles.map((tile) => {
    const bars = tile?.chart?.bars
    if (!Array.isArray(bars) || bars.length <= maxBars) return tile
    return {
      ...tile,
      chart: {
        ...(tile.chart || {}),
        bars: bars.slice(-maxBars),
      },
    }
  })
}

function analogEpisodesForTiles(analogBySymbol, tiles) {
  if (!analogBySymbol || typeof analogBySymbol !== 'object') return {}
  const syms = new Set(
    (Array.isArray(tiles) ? tiles : [])
      .map((t) => String(t?.symbol || '').toUpperCase())
      .filter(Boolean),
  )
  const out = {}
  for (const s of syms) {
    if (!Object.prototype.hasOwnProperty.call(analogBySymbol, s)) continue
    const eps = analogBySymbol[s]
    if (!Array.isArray(eps)) {
      out[s] = eps
      continue
    }
    out[s] = eps.length > MAX_ANALOG_PER_SYMBOL ? eps.slice(0, MAX_ANALOG_PER_SYMBOL) : eps
  }
  return out
}

/** Engine only uses pairwise correlation from bootstrap portfolio context. */
function slimPortfolioContextForStep(portfolioContext) {
  if (!portfolioContext || typeof portfolioContext !== 'object') return {}
  const pw = portfolioContext.pairwise_return_correlation
  if (pw && typeof pw === 'object') return { pairwise_return_correlation: pw }
  return {}
}

function LiveIntelligenceCockpitInner() {
  useAskMipPageRuntime('live_intelligence', ['lic_top_tile', 'lic_position_radar', 'lic_feed'])
  const { formatSymbolLabel } = useSymbolMeta()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [bootstrapVersion, setBootstrapVersion] = useState('')
  const [analogBySymbol, setAnalogBySymbol] = useState({})
  const [portfolioContext, setPortfolioContext] = useState({})
  const [trackerData, setTrackerData] = useState({ tiles: [] })
  const [intelligence, setIntelligence] = useState({})
  const [portfolioRegime, setPortfolioRegime] = useState({})
  const [feed, setFeed] = useState([])
  const [timeline, setTimeline] = useState([])
  const [selectedSymbol, setSelectedSymbol] = useState(null)
  const [portfolioFocusMode, setPortfolioFocusMode] = useState(false)
  const [detailTab, setDetailTab] = useState('snapshot')
  const [aiResult, setAiResult] = useState(null)
  const [aiBusy, setAiBusy] = useState(false)
  const [peakPnl, setPeakPnl] = useState({})
  const [bootReady, setBootReady] = useState(false)
  /** Snowflake-backed entry intel + committee + closeout — set only from bootstrap, never from IB refresh. */
  const [entryLifecycleBySymbol, setEntryLifecycleBySymbol] = useState({})
  const [reconciliationBySymbol, setReconciliationBySymbol] = useState({})
  const [reconciliationMeta, setReconciliationMeta] = useState({})
  const bootstrapGenRef = useRef(0)
  const refreshGenRef = useRef(0)
  const workspaceSectionRef = useRef(null)
  const portfolioScrollYRef = useRef(0)

  const selectSymbolForCockpit = useCallback(
    (rawSym) => {
      const sym = String(rawSym || '').toUpperCase()
      if (!sym) return
      if (portfolioFocusMode && selectedSymbol === sym) {
        setSelectedSymbol(null)
        setPortfolioFocusMode(false)
        return
      }
      if (!portfolioFocusMode && typeof window !== 'undefined') {
        portfolioScrollYRef.current = window.scrollY
      }
      setSelectedSymbol(sym)
      setPortfolioFocusMode(true)
    },
    [portfolioFocusMode, selectedSymbol],
  )

  const exitPortfolioFocus = useCallback(() => {
    setPortfolioFocusMode(false)
    const y = portfolioScrollYRef.current
    if (typeof window !== 'undefined') {
      window.requestAnimationFrame(() => {
        window.scrollTo({ top: y, behavior: 'smooth' })
      })
    }
  }, [])

  useEffect(() => {
    if (!selectedSymbol) {
      setPortfolioFocusMode(false)
    }
  }, [selectedSymbol])

  useEffect(() => {
    if (!portfolioFocusMode || !selectedSymbol) return
    const id = window.requestAnimationFrame(() => {
      workspaceSectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    })
    return () => window.cancelAnimationFrame(id)
  }, [portfolioFocusMode, selectedSymbol])

  const fetchIbLive = useCallback(async (tiles, options = {}) => {
    const windowBars = Number.isFinite(Number(options.windowBars)) ? Number(options.windowBars) : 780
    const softFail = options.softFail === true
    const symbols = (Array.isArray(tiles) ? tiles : [])
      .map((t) => ({ symbol: t?.symbol, market_type: t?.market_type }))
      .filter((t) => t.symbol)
    if (symbols.length === 0) return null
    const body = {
      mode: 'intraday',
      intraday_bar_seconds: 30,
      window_bars: Math.max(15, Math.min(800, Math.floor(windowBars))),
      symbols,
    }
    try {
      const resp = await fetchWithRetry(
        `${API_BASE}/live-intelligence/ib-live`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        },
        { retries: softFail ? 3 : 4, backoffMs: 500 },
      )
      if (!resp.ok) {
        if (softFail) return null
        throw new Error(`IB live failed (${resp.status})`)
      }
      return resp.json()
    } catch {
      if (softFail) return null
      throw new Error('IB live failed (network)')
    }
  }, [])

  const runStep = useCallback(async (tiles, priorIntel, priorPortfolioRegime) => {
    const positions = Array.isArray(tiles) ? tiles : []
    const body = {
      bootstrap_version: bootstrapVersion || '1.0.0',
      positions: trimTilesForStep(positions),
      prior_intelligence: slimPriorIntelligence(priorIntel),
      prior_portfolio_regime: priorPortfolioRegime || {},
      session_peak_pnl_by_symbol: peakPnl,
      analog_episodes_by_symbol: analogEpisodesForTiles(analogBySymbol, positions),
      portfolio_context: slimPortfolioContextForStep(portfolioContext),
    }
    const resp = await fetch(`${API_BASE}/live-intelligence/deterministic-step`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!resp.ok) throw new Error(`deterministic-step failed (${resp.status})`)
    return resp.json()
  }, [analogBySymbol, bootstrapVersion, peakPnl, portfolioContext])

  const loadBootstrap = useCallback(async () => {
    const gen = ++bootstrapGenRef.current
    setLoading(true)
    setError('')
    setBootReady(false)
    setEntryLifecycleBySymbol({})
    setReconciliationBySymbol({})
    setReconciliationMeta({})
    setFeed([])
    setTimeline([])
    try {
      const resp = await fetchWithRetry(`${API_BASE}/live-intelligence/bootstrap`, {}, { retries: 5, backoffMs: 600 })
      if (gen !== bootstrapGenRef.current) return
      if (!resp.ok) throw new Error(`Bootstrap failed (${resp.status})`)
      const data = await resp.json()
      if (gen !== bootstrapGenRef.current) return
      setBootstrapVersion(data.bootstrap_version || '')
      setAnalogBySymbol(data.analog_episodes_by_symbol || {})
      setPortfolioContext(data.portfolio_context || {})
      const tr = data.tracker || { tiles: [] }
      setTrackerData(tr)
      setEntryLifecycleBySymbol(
        data.entry_lifecycle_by_symbol && typeof data.entry_lifecycle_by_symbol === 'object'
          ? data.entry_lifecycle_by_symbol
          : {},
      )
      setReconciliationBySymbol(
        data.reconciliation_by_symbol && typeof data.reconciliation_by_symbol === 'object'
          ? data.reconciliation_by_symbol
          : {},
      )
      setReconciliationMeta(
        data.reconciliation_meta && typeof data.reconciliation_meta === 'object' ? data.reconciliation_meta : {},
      )
      setPortfolioFocusMode(false)
      if (!selectedSymbol && tr.tiles?.[0]?.symbol) {
        setSelectedSymbol(String(tr.tiles[0].symbol).toUpperCase())
      }
      const tileCount = (tr.tiles || []).length
      try {
        const ib = await fetchIbLive(tr.tiles || [])
        if (gen !== bootstrapGenRef.current) return
        const merged = ib ? mergeTrackerIb(tr, ib) : tr
        setTrackerData(merged)
        const step = await runStep(merged.tiles || [], {})
        if (gen !== bootstrapGenRef.current) return
        const nextIntel = step.intelligence_by_symbol || {}
        if (shouldApplyIntelligence(nextIntel, (merged.tiles || []).length)) {
          setIntelligence(nextIntel)
        }
        setPortfolioRegime(step.portfolio_regime || {})
        const ev = step.feed_events || []
        const session = sessionFeedRow()
        setFeed((f) => [...ev.filter(feedEventHasBody), session, ...f].slice(0, 120))
        setTimeline((t) => [
          ...ev.filter(feedEventHasBody).map((e) => ({ ...e, kind: 'MATERIAL' })),
          { ...session, kind: 'SESSION' },
          ...t,
        ].slice(0, 200))
        const peaks = {}
        ;(merged.tiles || []).forEach((tile) => {
          const s = String(tile.symbol || '').toUpperCase()
          const p = Number(tile.unrealized_pnl)
          if (s) peaks[s] = Number.isFinite(p) ? p : 0
        })
        setPeakPnl(peaks)
        setBootReady(true)
      } catch {
        if (gen !== bootstrapGenRef.current) return
        const step = await runStep(tr.tiles || [], {})
        if (gen !== bootstrapGenRef.current) return
        const nextIntel = step.intelligence_by_symbol || {}
        if (shouldApplyIntelligence(nextIntel, tileCount)) {
          setIntelligence(nextIntel)
        }
        setPortfolioRegime(step.portfolio_regime || {})
        const session = sessionFeedRow()
        setFeed([session])
        setTimeline([{ ...session, kind: 'SESSION' }])
        setBootReady(true)
      }
    } catch (e) {
      if (gen === bootstrapGenRef.current) {
        setError(e.message || 'Bootstrap error')
      }
    } finally {
      if (gen === bootstrapGenRef.current) {
        setLoading(false)
      }
    }
  }, [fetchIbLive, runStep, selectedSymbol])

  useEffect(() => {
    loadBootstrap()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const refreshLive = useCallback(async () => {
    if (!bootReady) return
    const gen = ++refreshGenRef.current
    try {
      setError('')
      const ib = await fetchIbLive(trackerData.tiles || [], {
        windowBars: 256,
        softFail: true,
      })
      if (gen !== refreshGenRef.current) return
      const merged = ib ? mergeTrackerIb(trackerData, ib) : trackerData
      if (ib) {
        setTrackerData(merged)
        setPeakPnl((prev) => {
          const next = { ...prev }
          ;(merged.tiles || []).forEach((tile) => {
            const s = String(tile.symbol || '').toUpperCase()
            const p = Number(tile.unrealized_pnl)
            if (!s || !Number.isFinite(p)) return
            next[s] = Math.max(next[s] ?? p, p)
          })
          return next
        })
      }
      const step = await runStep(merged.tiles || [], intelligence, portfolioRegime)
      if (gen !== refreshGenRef.current) return
      const nextIntel = step.intelligence_by_symbol || {}
      const nTiles = (merged.tiles || []).length
      if (shouldApplyIntelligence(nextIntel, nTiles)) {
        setIntelligence(nextIntel)
      }
      setPortfolioRegime(step.portfolio_regime || {})
      const ev = step.feed_events || []
      if (ev.length) {
        setFeed((f) => [...ev.filter(feedEventHasBody), ...f].slice(0, 120))
        setTimeline((t) => [...ev.filter(feedEventHasBody).map((e) => ({ ...e, kind: 'MATERIAL' })), ...t].slice(0, 200))
      }
    } catch (e) {
      if (gen === refreshGenRef.current) {
        setError(e.message || 'Live refresh failed')
      }
    }
  }, [bootReady, fetchIbLive, intelligence, portfolioRegime, runStep, trackerData])

  useVisibleInterval(refreshLive, 30000)

  const ranked = useMemo(() => {
    const tiles = trackerData.tiles || []
    return [...tiles].sort((a, b) => {
      const sa = String(a.symbol || '').toUpperCase()
      const sb = String(b.symbol || '').toUpperCase()
      const ia = attentionScoreNumber(intelligence[sa])
      const ib = attentionScoreNumber(intelligence[sb])
      return ib - ia
    })
  }, [trackerData.tiles, intelligence])

  const visibleRanked = useMemo(() => {
    if (!portfolioFocusMode || !selectedSymbol) return ranked
    return ranked.filter((t) => String(t.symbol || '').toUpperCase() === selectedSymbol)
  }, [ranked, portfolioFocusMode, selectedSymbol])

  const tilePresentation = useMemo(
    () =>
      buildVisibleTilePresentation(ranked, intelligence, (intel, tile) =>
        intel ? resolveDecisionPresentation(intel, tile).primary_action : '—',
      ),
    [ranked, intelligence],
  )

  const activeIntel = selectedSymbol ? intelligence[selectedSymbol] : null
  const activeTile = useMemo(() => {
    return (trackerData.tiles || []).find((t) => String(t.symbol || '').toUpperCase() === selectedSymbol) || null
  }, [trackerData.tiles, selectedSymbol])
  const workspacePres = useMemo(
    () => (activeIntel ? resolveDecisionPresentation(activeIntel, activeTile) : null),
    [activeIntel, activeTile],
  )

  const snapshotOverlay = useMemo(() => {
    if (!activeTile) return {}
    const o = activeTile.overlays || {}
    const exp = activeTile.expectation || {}
    const nf = (x) => {
      const v = Number(x)
      return Number.isFinite(v) ? v : null
    }
    return {
      entry: nf(o.entry ?? activeTile.entry_price),
      sl: nf(o.stop_loss),
      tp: nf(o.take_profit),
      med: nf(exp?.center_path?.[0]?.price),
    }
  }, [activeTile])

  const baseRadarTuple = useMemo(
    () => computeRawRadarTuple(activeIntel, activeTile, portfolioRegime),
    [activeIntel, activeTile, portfolioRegime],
  )

  const alignedRadarTuple = useMemo(
    () => alignRadarTupleToBand(baseRadarTuple, activeIntel?.final_recommendation),
    [baseRadarTuple, activeIntel?.final_recommendation],
  )

  const radarSmoothRef = useRef(null)
  const radarLastSymbolRef = useRef(null)
  const [radarDisplayTuple, setRadarDisplayTuple] = useState([50, 50, 50, 50, 50, 50])

  useEffect(() => {
    if (!activeIntel || !activeTile) {
      setRadarDisplayTuple([50, 50, 50, 50, 50, 50])
      radarSmoothRef.current = null
      return
    }
    if (radarLastSymbolRef.current !== selectedSymbol) {
      radarLastSymbolRef.current = selectedSymbol
      const snap = [...alignedRadarTuple]
      radarSmoothRef.current = snap
      setRadarDisplayTuple(snap)
      return
    }
    const next = smoothRadarScores(radarSmoothRef.current, alignedRadarTuple, 0.32)
    radarSmoothRef.current = next
    setRadarDisplayTuple(next)
  }, [alignedRadarTuple, selectedSymbol, activeIntel, activeTile])

  const radarChartData = useMemo(() => radarTupleToChartData(radarDisplayTuple), [radarDisplayTuple])

  const radarHintLine = useMemo(
    () => radarInterpretationHint(radarDisplayTuple, activeIntel?.final_recommendation),
    [radarDisplayTuple, activeIntel?.final_recommendation],
  )

  const snapshotStripMetrics = useMemo(() => {
    if (!activeTile) return { dsl: null, dtp: null, vsExpPct: null }
    const pm = activeTile.progress_metrics || {}
    const dsl = Number(pm.distance_to_sl_pct)
    const dtp = Number(pm.distance_to_tp_pct)
    const med = snapshotOverlay.med
    const cur = Number(activeTile.current_price)
    let vsExpPct = null
    if (Number.isFinite(cur) && med != null && Number.isFinite(med) && med !== 0) {
      vsExpPct = ((cur - med) / med) * 100
    }
    return {
      dsl: Number.isFinite(dsl) ? dsl : null,
      dtp: Number.isFinite(dtp) ? dtp : null,
      vsExpPct,
    }
  }, [activeTile, snapshotOverlay])

  const interactiveChartHref = useMemo(() => {
    if (!selectedSymbol) return '/living-chart'
    const q = new URLSearchParams()
    q.set('symbol', selectedSymbol)
    const ent = snapshotOverlay.entry
    const sl = snapshotOverlay.sl
    const tp = snapshotOverlay.tp
    if (ent != null && Number.isFinite(ent)) q.set('entry', String(ent))
    if (sl != null && Number.isFinite(sl)) q.set('stop', String(sl))
    if (tp != null && Number.isFinite(tp)) q.set('target', String(tp))
    if (activeIntel?.final_recommendation != null) {
      q.set('recommendation', bandLabel(activeIntel.final_recommendation))
    }
    return `/living-chart?${q.toString()}`
  }, [selectedSymbol, snapshotOverlay, activeIntel])

  const runAi = useCallback(async () => {
    if (!selectedSymbol || !activeIntel) return
    setAiBusy(true)
    setAiResult(null)
    try {
      const resp = await fetch(`${API_BASE}/live-intelligence/ai/enrich`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          symbol: selectedSymbol,
          intelligence: activeIntel,
          deterministic_snapshot: activeTile,
          force: true,
        }),
      })
      if (!resp.ok) throw new Error(`AI enrich failed (${resp.status})`)
      setAiResult(await resp.json())
    } catch (e) {
      setAiResult({ error: e.message })
    } finally {
      setAiBusy(false)
    }
  }, [activeIntel, activeTile, selectedSymbol])

  return (
    <div className="lic-page">
      <div className="lic-head">
        <div>
          <h2>Live Intelligence Cockpit</h2>
          <p>
            Snowflake bootstrap once (positions + entry analysis) · IB live refresh does not re-query entry analysis ·
            deterministic + event AI · v{bootstrapVersion || '…'}
          </p>
        </div>
        <div className="lic-actions">
          <button type="button" onClick={loadBootstrap}>Reload bootstrap (Snowflake)</button>
          <button type="button" onClick={refreshLive}>Refresh IB + step</button>
        </div>
      </div>

      {error ? <div className="lic-error">{error}</div> : null}

      <div className={`lic-banner ${portfolioRegime.active ? 'lic-banner--active' : ''}`}>
        {safeText(
          portfolioRegime.banner_text,
          'Portfolio regime: no latent book shock detected.',
        )}
      </div>

      {!loading && bootReady && Array.isArray(reconciliationMeta.ghost_symbols) && reconciliationMeta.ghost_symbols.length > 0 ? (
        <div className="lic-recon-ghost-banner" role="status">
          <strong>Lifecycle vs IB:</strong> MIP shows an open linked entry with no closeout for{' '}
          {reconciliationMeta.ghost_symbols.map((g) => safeText(g.symbol)).join(', ')} while the latest IB snapshot has
          no open position there — review broker fills and TRADE_CLOSEOUT.
        </div>
      ) : null}

      {loading ? <p className="lic-muted">Loading bootstrap…</p> : null}

      {!loading && bootReady && ranked.length === 0 ? (
        <p className="lic-empty-book">
          No open positions in the bootstrap tracker. If you expect holdings here, reload bootstrap or confirm the active portfolio in Living Chart.
        </p>
      ) : null}

      <div className={`lic-layout${portfolioFocusMode ? ' lic-layout--focus' : ''}`}>
      <div className="lic-grid">
        <div className="lic-main">
          {portfolioFocusMode ? (
            <div className="lic-focus-toolbar">
              <button type="button" className="lic-back-portfolio-btn" onClick={exitPortfolioFocus}>
                Back to Portfolio
              </button>
              <span className="lic-focus-toolbar-hint">Focused on one symbol — workspace below.</span>
            </div>
          ) : null}
          <div className="lic-leaderboard">
            {ranked.map((t) => {
              const s = String(t.symbol || '').toUpperCase()
              const intel = intelligence[s]
              return (
                <button
                  key={s}
                  type="button"
                  className={`lic-lb-chip ${selectedSymbol === s ? 'lic-lb-chip--on' : ''}`}
                  title={attentionTooltip(intel)}
                  onClick={() => selectSymbolForCockpit(s)}
                >
                  {formatSymbolLabel(t.symbol, t.market_type)}
                  {' · '}
                  <span className="lic-attn-band">{safeText(intel?.attention_band)}</span>
                  {' · '}
                  {intel == null ? '\u2014' : String(Math.round(attentionScoreNumber(intel)))}
                </button>
              )
            })}
          </div>

          <div className="lic-tiles">
            {visibleRanked.map((t) => {
              const s = String(t.symbol || '').toUpperCase()
              const intel = intelligence[s]
              const rd = intel?.recommendation_display || {}
              const pres = intel ? resolveDecisionPresentation(intel, t) : null
              const primaryAction = pres?.primary_action || '—'
              const capFull = safeText(rd.confidence_caption, '')
              const thesisPlain = safeText(intel?.thesis_plain, '')
              const copy = tilePresentation.get(s)
              const thesisLine = copy?.thesis ?? thesisPlain
              const thesisTooltip = (() => {
                const parts = []
                if (copy?.thesis) parts.push(copy.thesis)
                if (thesisPlain && thesisPlain !== copy?.thesis) parts.push(`Model: ${thesisPlain}`)
                return parts.length ? parts.join('\n\n') : undefined
              })()
              const driverLines = copy?.drivers?.length ? copy.drivers : asArray(intel?.decision_drivers).slice(0, 3)
              const chipSuppress = copy?.chipSuppress || {}
              const fallbackStrip = pres?.fallback_strip_text?.trim() || ''
              const confTitle = capFull ? `Decision confidence: ${capFull}` : 'Confidence blends thesis, tape, and analog match.'
              const confPct = (() => {
                const p = Math.round(Number(rd.confidence) * 100)
                return Number.isFinite(p) ? p : 0
              })()
              return (
                <LicTopTile
                  key={s}
                  symbolKey={s}
                  selected={selectedSymbol === s}
                  onSelect={() => selectSymbolForCockpit(s)}
                  symbolLabel={formatSymbolLabel(t.symbol, t.market_type)}
                  side={t.side}
                  priceStr={Number.isFinite(Number(t.current_price)) ? Number(t.current_price).toFixed(2) : '—'}
                  pnlStr={formatMoney(t.unrealized_pnl)}
                  pnlNeg={Number(t.unrealized_pnl) < 0}
                  ageStr={formatHoldingAge(t.opened_at)}
                  primaryAction={primaryAction}
                  showConfidence={rd.confidence != null}
                  confPct={confPct}
                  confTitle={confTitle}
                  recommendationBand={intel?.final_recommendation}
                  tile={t}
                  fallbackStrip={fallbackStrip}
                  thesisLine={thesisLine}
                  thesisTooltip={thesisTooltip}
                  driverLines={driverLines}
                  chipSuppress={chipSuppress}
                  intel={intel}
                />
              )
            })}
          </div>
        </div>

        <aside className="lic-aside" aria-label="Case file">
          <h4 className="lic-aside-title">Case file</h4>
          <div className="lic-feed-scroll">
            <div className="lic-feed">
              {feed.length === 0 ? <div className="lic-feed-empty">No material events yet.</div> : null}
              {feed.map((row, fri) => {
                if (!row || typeof row !== 'object') return null
                const k = `${row.ts || row.timestamp || ''}_${row.symbol}_${row.state_transition || row.transition || ''}_${fri}`
                const rawBody = row.reason || row.what_changed || row.why_now_human || ''
                const body = safeText(rawBody, '')
                const trans = safeText(row.state_transition || row.transition || '', '')
                const sev = safeText(row.severity || '', '')
                const sevSlug = String(sev).toLowerCase().replace(/\s+/g, '')
                const scope = safeText(row.scope || '', '')
                const impl = caseFileImplicationDisplay(row, intelligence)
                const rowTitle = body && body !== impl ? body : undefined
                return (
                  <div
                    key={k}
                    className={`lic-feed-row lic-feed-row--compact lic-feed-row--${sevSlug || 'info'}`}
                    title={rowTitle}
                  >
                    <div className="lic-feed-row-line1">
                      <span className="lic-feed-time">{formatFeedTime(row)}</span>
                      <span className="lic-feed-symscope">
                        <b>{safeText(row.symbol)}</b>
                        {scope ? <span className="lic-feed-scope"> · {scope}</span> : null}
                      </span>
                    </div>
                    {trans ? <div className="lic-feed-trans">{trans}</div> : null}
                    {impl ? (
                      <div className="lic-feed-action">{impl}</div>
                    ) : null}
                  </div>
                )
              })}
            </div>
          </div>
        </aside>
        </div>

        <section
          ref={workspaceSectionRef}
          className="lic-workspace lic-workspace--focus-anchor"
          aria-label="Selected symbol workspace"
        >
          {!selectedSymbol ? (
            <p className="lic-workspace-empty">Select a symbol from the tiles or attention strip to open the workspace.</p>
          ) : !activeIntel ? (
            <p className="lic-workspace-empty">No intelligence loaded for {selectedSymbol} yet.</p>
          ) : (
            <>
              <div className="lic-workspace-head">
                <h3 className="lic-workspace-title">Workspace · {selectedSymbol}</h3>
              </div>
              <div className="lic-tabs" role="tablist" aria-label="Drill-down panels">
                {[
                  ['snapshot', 'Snapshot'],
                  ['worlds', 'worlds'],
                  ['analog', 'analog'],
                  ['simulator', 'simulator'],
                  ['timeline', 'timeline'],
                  ['evidence', 'evidence'],
                  ['ai', 'ai'],
                ].map(([tab, label]) => (
                  <button
                    key={tab}
                    type="button"
                    role="tab"
                    aria-selected={detailTab === tab}
                    className={detailTab === tab ? 'lic-tab--on' : ''}
                    onClick={() => setDetailTab(tab)}
                  >
                    {label}
                  </button>
                ))}
              </div>

              {detailTab === 'snapshot' && (
                <div className="lic-drill-panel lic-drill-panel--snapshot lic-snapshot">
                  <LicEntryIntelligencePanel
                    lifecycle={
                      entryLifecycleBySymbol[selectedSymbol] ?? {
                        has_entry_intel_link: false,
                        unavailable_reason:
                          'No linked pre-trade analysis for this symbol in the last bootstrap. Use Reload bootstrap after the entry is linked to entry intelligence.',
                      }
                    }
                    reconciliation={reconciliationBySymbol[selectedSymbol]}
                  />
                  <div className="lic-snapshot-strip lic-chart-decision-strip">
                    <div className="lic-chart-decision-strip-left">
                      <div className="lic-chart-decision-primary">{safeText(workspacePres?.primary_action, '—')}</div>
                      <div className="lic-chart-decision-fallback">
                        {workspacePres?.fallback_action
                          ? `Fallback: ${safeText(workspacePres.fallback_action)}`
                          : 'No simulator fallback'}
                      </div>
                    </div>
                    <div className="lic-chart-decision-strip-right">
                      <div>
                        <span className="lic-chart-metric-k">To stop</span>{' '}
                        <span className="lic-chart-metric-v">
                          {snapshotStripMetrics.dsl != null
                            ? `${(snapshotStripMetrics.dsl * 100).toFixed(2)}%`
                            : '—'}
                        </span>
                      </div>
                      <div>
                        <span className="lic-chart-metric-k">To target</span>{' '}
                        <span className="lic-chart-metric-v">
                          {snapshotStripMetrics.dtp != null
                            ? `${(snapshotStripMetrics.dtp * 100).toFixed(2)}%`
                            : '—'}
                        </span>
                      </div>
                      <div>
                        <span className="lic-chart-metric-k">vs expected</span>{' '}
                        <span className="lic-chart-metric-v">
                          {snapshotStripMetrics.vsExpPct != null
                            ? `${snapshotStripMetrics.vsExpPct >= 0 ? '+' : ''}${snapshotStripMetrics.vsExpPct.toFixed(2)}%`
                            : '—'}
                        </span>
                      </div>
                    </div>
                  </div>
                  <div className="lic-snapshot-body">
                    <div className="lic-snapshot-copy">
                      <div className="lic-snapshot-foot-row lic-chart-decision-foot-row">
                        <span className="lic-chart-foot-k">Why this still works</span>
                        <span className="lic-chart-foot-v">{safeText(workspacePres?.primary_reason)}</span>
                      </div>
                      <div className="lic-snapshot-foot-row lic-chart-decision-foot-row">
                        <span className="lic-chart-foot-k">What would break it</span>
                        <span className="lic-chart-foot-v">{safeText(workspacePres?.flip_trigger)}</span>
                      </div>
                    </div>
                    <div className="lic-snapshot-radar">
                      <LicPositionRadar data={radarChartData} finalRecommendation={activeIntel?.final_recommendation} hint={radarHintLine} />
                    </div>
                  </div>
                  <div className="lic-snapshot-actions lic-chart-decision-actions">
                    <Link className="lic-open-interactive-chart" to={interactiveChartHref}>
                      Open Interactive Chart
                    </Link>
                  </div>
                </div>
              )}

              {detailTab === 'worlds' && (
                <div className="lic-drill-panel lic-worlds lic-worlds--scenarios">
                  <LicWorldsScenarios
                    analogSummary={activeIntel.analog_summary}
                    finalRecommendation={activeIntel.final_recommendation}
                  />
                </div>
              )}
              {detailTab === 'analog' && (
                <div className="lic-drill-panel lic-analog-panel">
                  <LicAnalogPanel analogUi={activeIntel.analog_ui} analogSummary={activeIntel.analog_summary} />
                </div>
              )}
              {detailTab === 'simulator' && (
                <div className="lic-drill-panel lic-sim-panel">
                  <LicSimulatorPanel
                    activeIntel={activeIntel}
                    workspacePres={workspacePres || resolveDecisionPresentation(activeIntel)}
                  />
                </div>
              )}
              {detailTab === 'timeline' && (
                <div className="lic-drill-panel lic-timeline">
                  {timeline.filter((x) => x.kind === 'MATERIAL' && x.symbol === selectedSymbol).length === 0 ? (
                    <p className="lic-muted">No material events for this symbol yet.</p>
                  ) : (
                    timeline
                      .filter((x) => x.kind === 'MATERIAL' && x.symbol === selectedSymbol)
                      .slice(0, 24)
                      .map((ev, idx) => (
                        <div key={`${ev.ts || ev.timestamp || idx}-${ev.state_transition || idx}`} className="lic-tl-row">
                          <div className="lic-tl-time">{formatFeedTime(ev)}</div>
                          <div className="lic-tl-sev">{safeText(ev.severity)}</div>
                          <div className="lic-tl-scope">{safeText(ev.scope)}</div>
                          <div className="lic-tl-trans">{safeText(ev.state_transition || ev.transition)}</div>
                          <div className="lic-tl-body">{safeText(ev.reason || ev.what_changed)}</div>
                          <div className="lic-tl-act">
                            Implication: {caseFileImplicationDisplay(ev, intelligence)}
                          </div>
                        </div>
                      ))
                  )}
                </div>
              )}
              {detailTab === 'evidence' && (
                <div className="lic-drill-panel lic-evidence">
                  {(() => {
                    const ev = activeIntel.evidence_sections || {}
                    const keys = ['tape', 'thesis', 'risk', 'analog', 'portfolio_factor', 'novelty']
                    const titles = {
                      tape: 'Tape',
                      thesis: 'Thesis',
                      risk: 'Risk geometry',
                      analog: 'Historical analog',
                      portfolio_factor: 'Portfolio factor',
                      novelty: 'Novelty',
                    }
                    return keys.map((k) => {
                      const sec = ev[k] || {}
                      const st = sec.status ? `Status: ${sec.status}` : ''
                      return (
                        <section key={k} className="lic-ev-block">
                          <h5 className="lic-ev-title">{titles[k]}</h5>
                          {st ? <p className="lic-ev-status">{st}</p> : null}
                          <p className="lic-ev-sum">{sec.summary}</p>
                          <div className="lic-ev-col">
                            <div className="lic-ev-supports">
                              <span className="lic-ev-tag">Supports recommendation</span>
                              <ul>{asArray(sec.supports).map((x, xi) => (<li key={`${k}-s-${xi}-${String(x)}`}>{x}</li>))}</ul>
                            </div>
                            <div className="lic-ev-opp">
                              <span className="lic-ev-tag lic-ev-tag--opp">Pushes the other way</span>
                              <ul>{asArray(sec.opposes).map((x, xi) => (<li key={`${k}-o-${xi}-${String(x)}`}>{x}</li>))}</ul>
                            </div>
                          </div>
                        </section>
                      )
                    })
                  })()}
                  <section className="lic-ev-block">
                    <h5 className="lic-ev-title">Synthesis</h5>
                    <div className="lic-ev-col">
                      <div className="lic-ev-supports">
                        <span className="lic-ev-tag">Why this call</span>
                        <ul>{asArray(activeIntel.supporting_signals).map((x, xi) => (<li key={`syn-s-${xi}-${String(x)}`}>{x}</li>))}</ul>
                      </div>
                      <div className="lic-ev-opp">
                        <span className="lic-ev-tag lic-ev-tag--opp">Counterpoints</span>
                        <ul>{asArray(activeIntel.opposing_signals).map((x, xi) => (<li key={`syn-o-${xi}-${String(x)}`}>{x}</li>))}</ul>
                      </div>
                    </div>
                  </section>
                  {activeIntel.portfolio_factor_local ? (
                    <p className="lic-ev-foot">{safeText(activeIntel.portfolio_factor_local.localized_plain)}</p>
                  ) : null}
                </div>
              )}
              {detailTab === 'ai' && (
                <div className="lic-drill-panel lic-drill-panel--ai">
                  <button type="button" onClick={runAi} disabled={aiBusy}>
                    {aiBusy ? 'Running…' : 'Run AI enrichment (event)'}
                  </button>
                  {aiBusy ? (
                    <p className="lic-muted" style={{ marginTop: 8 }}>Calling enrich endpoint…</p>
                  ) : aiResult != null ? (
                    <pre className="lic-detail-pre" style={{ marginTop: 8 }}>
                      {JSON.stringify(aiResult, null, 2)}
                    </pre>
                  ) : (
                    <p className="lic-muted" style={{ marginTop: 8 }}>
                      No run yet — use the button above to fetch event-level AI output.
                    </p>
                  )}
                </div>
              )}
            </>
          )}
        </section>
      </div>
    </div>
  )
}

export default function LiveIntelligenceCockpit() {
  return (
    <LicErrorBoundary>
      <LiveIntelligenceCockpitInner />
    </LicErrorBoundary>
  )
}
