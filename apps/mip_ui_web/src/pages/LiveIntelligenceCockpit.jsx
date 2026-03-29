import { Component, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { Line, LineChart, ReferenceArea, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { API_BASE } from '../config/apiBase'
import { fetchWithRetry } from '../utils/fetchRetry'
import useVisibleInterval from '../hooks/useVisibleInterval'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import LicTopTile from '../components/lic/LicTopTile'
import {
  bandLabel,
  caseFileImplicationDisplay,
  resolveDecisionPresentation,
} from '../components/lic/licDecisionPresentation'
import { buildVisibleTilePresentation } from '../components/lic/licTilePresentation'
import './LiveIntelligenceCockpit.css'

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
    action_implication: 'Scan tiles: primary action, chart, strip.',
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

function formatSimPct(v) {
  const x = Number(v)
  if (!Number.isFinite(x)) return '\u2014'
  return `${(x * 100).toFixed(1)}%`
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
  const [detailTab, setDetailTab] = useState('chart')
  const [aiResult, setAiResult] = useState(null)
  const [aiBusy, setAiBusy] = useState(false)
  const [peakPnl, setPeakPnl] = useState({})
  const [bootReady, setBootReady] = useState(false)
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

  const chartOverlay = useMemo(() => {
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
      lo: nf(exp?.lower_path?.[0]?.price),
      hi: nf(exp?.upper_path?.[0]?.price),
    }
  }, [activeTile])

  const chartDecisionSeries = useMemo(() => {
    const bars = activeTile?.chart?.bars || []
    const n = bars.length
    const centerPath = Array.isArray(activeTile?.expectation?.center_path)
      ? activeTile.expectation.center_path
      : []
    const expPrices = centerPath.map((p) => Number(p?.price)).filter((x) => Number.isFinite(x))
    return bars
      .map((b, i) => {
        const c = Number(b.close)
        if (!Number.isFinite(c)) return null
        let exp = null
        if (expPrices.length >= 2) {
          const t = n <= 1 ? 0 : i / (n - 1)
          const idx = t * (expPrices.length - 1)
          const lo = Math.floor(idx)
          const hi = Math.min(lo + 1, expPrices.length - 1)
          const f = idx - lo
          exp = expPrices[lo] * (1 - f) + expPrices[hi] * f
        } else if (expPrices.length === 1) {
          exp = expPrices[0]
        }
        return {
          i,
          c,
          exp: Number.isFinite(exp) ? exp : null,
          ts: String(b.ts || '').slice(11, 19) || String(i),
        }
      })
      .filter(Boolean)
  }, [activeTile])

  /** Half-widths for stop/target bands; reused for Y-domain so zones stay in view. */
  const chartBandHalfWidths = useMemo(() => {
    const series = chartDecisionSeries
    const prices = series.map((r) => r.c).filter((x) => Number.isFinite(x))
    const span = prices.length ? Math.max(...prices) - Math.min(...prices) : 0
    const cur = Number(activeTile?.current_price)
    const ref = Number.isFinite(cur) ? Math.abs(cur) : prices.length ? Math.abs(prices[prices.length - 1]) : 1
    const halfStop = Math.max(span * 0.018, ref * 1.5e-4, 1e-6)
    const halfTp = halfStop * 0.85
    return { halfStop, halfTp }
  }, [chartDecisionSeries, activeTile])

  const licChartYDomainRef = useRef(null)
  const licChartYDomainSymbolRef = useRef(selectedSymbol)
  if (licChartYDomainSymbolRef.current !== selectedSymbol) {
    licChartYDomainSymbolRef.current = selectedSymbol
    licChartYDomainRef.current = null
  }

  const chartYDomain = useMemo(() => {
    const co = chartOverlay
    const vals = []
    const add = (x) => {
      if (x != null && Number.isFinite(x)) vals.push(x)
    }
    for (const r of chartDecisionSeries) {
      add(r.c)
      if (r.exp != null) add(r.exp)
    }
    add(co.entry)
    add(co.sl)
    add(co.tp)
    const cur = Number(activeTile?.current_price)
    add(cur)
    const { halfStop, halfTp } = chartBandHalfWidths
    if (co.sl != null && Number.isFinite(co.sl)) {
      add(co.sl - halfStop)
      add(co.sl + halfStop)
    }
    if (co.tp != null && Number.isFinite(co.tp)) {
      add(co.tp - halfTp)
      add(co.tp + halfTp)
    }
    if (vals.length === 0) return null
    const lo = Math.min(...vals)
    const hi = Math.max(...vals)
    const span = hi - lo || Math.abs(hi) * 0.008 || 1
    const pad = span * 0.08
    const next = [lo - pad, hi + pad]

    const prev = licChartYDomainRef.current
    if (!prev || prev.length !== 2) {
      licChartYDomainRef.current = next
      return next
    }
    const [p0, p1] = prev
    const nSpan = next[1] - next[0]
    const pSpan = p1 - p0
    const slack = Math.max(nSpan, pSpan) * 0.015
    const insidePrev = vals.every((v) => v >= p0 + slack && v <= p1 - slack)
    if (insidePrev && nSpan <= pSpan * 0.97) {
      const alpha = 0.32
      const merged = [p0 * (1 - alpha) + next[0] * alpha, p1 * (1 - alpha) + next[1] * alpha]
      licChartYDomainRef.current = merged
      return merged
    }
    if (!insidePrev) {
      licChartYDomainRef.current = next
      return next
    }
    licChartYDomainRef.current = prev
    return prev
  }, [chartDecisionSeries, chartOverlay, activeTile, chartBandHalfWidths])

  const chartStopTargetBands = useMemo(() => {
    const sl = chartOverlay.sl
    const tp = chartOverlay.tp
    const { halfStop, halfTp } = chartBandHalfWidths
    let stop = null
    let target = null
    if (sl != null && Number.isFinite(sl)) stop = { y1: sl - halfStop, y2: sl + halfStop }
    if (tp != null && Number.isFinite(tp)) target = { y1: tp - halfTp, y2: tp + halfTp }
    return { stop, target }
  }, [chartOverlay, chartBandHalfWidths])

  const chartLineVisual = useMemo(() => {
    const b = String(activeIntel?.final_recommendation || 'STAY_COURSE').toUpperCase()
    if (b === 'EXIT_NOW') {
      return { liveStroke: '#f87171', liveWidth: 2.75, expectedStroke: '#64748b', expectedOpacity: 0.5 }
    }
    if (b === 'PREPARE_EXIT') {
      return { liveStroke: '#fbbf24', liveWidth: 2.55, expectedStroke: '#64748b', expectedOpacity: 0.48 }
    }
    if (b === 'WATCH_CLOSELY') {
      return { liveStroke: '#38bdf8', liveWidth: 2.45, expectedStroke: '#64748b', expectedOpacity: 0.58 }
    }
    return { liveStroke: '#0ea5e9', liveWidth: 2.5, expectedStroke: '#64748b', expectedOpacity: 0.62 }
  }, [activeIntel])

  const chartWorkspaceStripMetrics = useMemo(() => {
    if (!activeTile) return { dsl: null, dtp: null, vsExpPct: null }
    const pm = activeTile.progress_metrics || {}
    const dsl = Number(pm.distance_to_sl_pct)
    const dtp = Number(pm.distance_to_tp_pct)
    const med = chartOverlay.med
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
  }, [activeTile, chartOverlay])

  const interactiveChartHref = useMemo(() => {
    if (!selectedSymbol) return '/living-chart'
    const q = new URLSearchParams()
    q.set('symbol', selectedSymbol)
    const ent = chartOverlay.entry
    const sl = chartOverlay.sl
    const tp = chartOverlay.tp
    if (ent != null && Number.isFinite(ent)) q.set('entry', String(ent))
    if (sl != null && Number.isFinite(sl)) q.set('stop', String(sl))
    if (tp != null && Number.isFinite(tp)) q.set('target', String(tp))
    if (activeIntel?.final_recommendation != null) {
      q.set('recommendation', bandLabel(activeIntel.final_recommendation))
    }
    return `/living-chart?${q.toString()}`
  }, [selectedSymbol, chartOverlay, activeIntel])

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
            Snowflake bootstrap once · IB live only after load · deterministic + event AI · v{bootstrapVersion || '…'}
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
                {workspacePres ? (
                  <div className="lic-workspace-primary-pill" title="Official stance for this symbol">
                    {workspacePres.primary_action}
                  </div>
                ) : null}
              </div>
              {workspacePres ? (
                <div className="lic-reconcile" aria-label="Decision reconciliation">
                  <div className="lic-reconcile-row">
                    <span className="lic-reconcile-k">Primary action</span>
                    <span className="lic-reconcile-v lic-reconcile-v--primary">{workspacePres.primary_action}</span>
                  </div>
                  <div className="lic-reconcile-row">
                    <span className="lic-reconcile-k">Fallback action</span>
                    <span className="lic-reconcile-v lic-reconcile-v--fallback">
                      {workspacePres.fallback_action || '—'}
                    </span>
                  </div>
                  <div className="lic-reconcile-row">
                    <span className="lic-reconcile-k">Why primary still wins</span>
                    <span className="lic-reconcile-v">{safeText(workspacePres.primary_reason)}</span>
                  </div>
                  <div className="lic-reconcile-row">
                    <span className="lic-reconcile-k">Flip trigger</span>
                    <span className="lic-reconcile-v">{safeText(workspacePres.flip_trigger)}</span>
                  </div>
                </div>
              ) : null}
              <div className="lic-tabs" role="tablist" aria-label="Drill-down panels">
                {['chart', 'worlds', 'analog', 'simulator', 'timeline', 'evidence', 'ai'].map((tab) => (
                  <button
                    key={tab}
                    type="button"
                    role="tab"
                    aria-selected={detailTab === tab}
                    className={detailTab === tab ? 'lic-tab--on' : ''}
                    onClick={() => setDetailTab(tab)}
                  >
                    {tab}
                  </button>
                ))}
              </div>

              {detailTab === 'chart' && (
                <div className="lic-drill-panel lic-drill-panel--chart lic-chart-decision">
                  <div className="lic-chart-decision-strip">
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
                          {chartWorkspaceStripMetrics.dsl != null
                            ? `${(chartWorkspaceStripMetrics.dsl * 100).toFixed(2)}%`
                            : '—'}
                        </span>
                      </div>
                      <div>
                        <span className="lic-chart-metric-k">To target</span>{' '}
                        <span className="lic-chart-metric-v">
                          {chartWorkspaceStripMetrics.dtp != null
                            ? `${(chartWorkspaceStripMetrics.dtp * 100).toFixed(2)}%`
                            : '—'}
                        </span>
                      </div>
                      <div>
                        <span className="lic-chart-metric-k">vs expected</span>{' '}
                        <span className="lic-chart-metric-v">
                          {chartWorkspaceStripMetrics.vsExpPct != null
                            ? `${chartWorkspaceStripMetrics.vsExpPct >= 0 ? '+' : ''}${chartWorkspaceStripMetrics.vsExpPct.toFixed(2)}%`
                            : '—'}
                        </span>
                      </div>
                    </div>
                  </div>
                  <div className="lic-chart-decision-chartwell">
                    {chartDecisionSeries.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={chartDecisionSeries} margin={{ top: 6, right: 8, left: 0, bottom: 0 }}>
                          <XAxis dataKey="ts" tick={{ fill: '#94a3b8', fontSize: 9 }} stroke="#475569" />
                          <YAxis
                            domain={chartYDomain || ['auto', 'auto']}
                            tick={{ fill: '#94a3b8', fontSize: 9 }}
                            width={46}
                            stroke="#475569"
                            tickFormatter={(v) => (Number.isFinite(v) ? v.toFixed(2) : '')}
                          />
                          <Tooltip
                            contentStyle={{ background: '#1e293b', border: '1px solid #334155', color: '#f1f5f9' }}
                            labelStyle={{ color: '#cbd5e1' }}
                          />
                          {chartStopTargetBands.target ? (
                            <ReferenceArea
                              y1={chartStopTargetBands.target.y1}
                              y2={chartStopTargetBands.target.y2}
                              fill="#22c55e"
                              fillOpacity={0.07}
                              strokeOpacity={0}
                            />
                          ) : null}
                          {chartStopTargetBands.stop ? (
                            <ReferenceArea
                              y1={chartStopTargetBands.stop.y1}
                              y2={chartStopTargetBands.stop.y2}
                              fill="#f87171"
                              fillOpacity={0.2}
                              strokeOpacity={0}
                            />
                          ) : null}
                          {chartOverlay.lo != null ? (
                            <ReferenceLine
                              y={chartOverlay.lo}
                              stroke="#334155"
                              strokeDasharray="5 4"
                              strokeOpacity={0.45}
                            />
                          ) : null}
                          {chartOverlay.hi != null ? (
                            <ReferenceLine
                              y={chartOverlay.hi}
                              stroke="#334155"
                              strokeDasharray="5 4"
                              strokeOpacity={0.45}
                            />
                          ) : null}
                          {chartOverlay.entry != null ? (
                            <ReferenceLine
                              y={chartOverlay.entry}
                              stroke="#64748b"
                              strokeDasharray="6 4"
                              strokeWidth={1}
                            />
                          ) : null}
                          {chartOverlay.tp != null ? (
                            <ReferenceLine
                              y={chartOverlay.tp}
                              stroke="#22c55e"
                              strokeDasharray="4 5"
                              strokeOpacity={0.35}
                              strokeWidth={0.8}
                            />
                          ) : null}
                          {chartOverlay.sl != null ? (
                            <ReferenceLine y={chartOverlay.sl} stroke="#fb7185" strokeWidth={1.2} strokeDasharray="3 3" />
                          ) : null}
                          {chartDecisionSeries.some((r) => r.exp != null) ? (
                            <Line
                              type="monotone"
                              dataKey="exp"
                              name="Expected"
                              stroke={chartLineVisual.expectedStroke}
                              dot={false}
                              strokeWidth={1.15}
                              strokeDasharray="6 5"
                              strokeOpacity={chartLineVisual.expectedOpacity}
                              connectNulls
                            />
                          ) : null}
                          <Line
                            type="monotone"
                            dataKey="c"
                            name="Live"
                            stroke={chartLineVisual.liveStroke}
                            dot={(dotProps) => {
                              const { cx, cy, index } = dotProps
                              if (index !== chartDecisionSeries.length - 1 || cx == null || cy == null) return null
                              return (
                                <circle
                                  cx={cx}
                                  cy={cy}
                                  r={5}
                                  fill="#fbbf24"
                                  stroke="#0f172a"
                                  strokeWidth={1.2}
                                />
                              )
                            }}
                            strokeWidth={chartLineVisual.liveWidth}
                            activeDot={{ r: 5 }}
                          />
                        </LineChart>
                      </ResponsiveContainer>
                    ) : (
                      <div className="lic-chart-empty">No bars</div>
                    )}
                  </div>
                  <div className="lic-chart-decision-foot">
                    <div className="lic-chart-decision-foot-row">
                      <span className="lic-chart-foot-k">Why this still works</span>
                      <span className="lic-chart-foot-v">{safeText(workspacePres?.primary_reason)}</span>
                    </div>
                    <div className="lic-chart-decision-foot-row">
                      <span className="lic-chart-foot-k">What would break it</span>
                      <span className="lic-chart-foot-v">{safeText(workspacePres?.flip_trigger)}</span>
                    </div>
                  </div>
                  <div className="lic-chart-decision-actions">
                    <Link className="lic-open-interactive-chart" to={interactiveChartHref}>
                      Open Interactive Chart
                    </Link>
                  </div>
                </div>
              )}

              {detailTab === 'worlds' && (
                <div className="lic-drill-panel lic-worlds">
                  {asArray(activeIntel.scenario_worlds).map((w, wi) => (
                    <div
                      key={w?.id ?? w?.title ?? `w-${wi}`}
                      className={`lic-world-card${String(w?.id) === String(activeIntel?.dominant_world_id) ? ' lic-world-card--dominant' : ''}`}
                    >
                      <div className="lic-world-head">
                        <span className="lic-world-title">{safeText(w?.title)}</span>
                        <span className="lic-world-pct">{w?.probability_pct ?? Math.round((w?.probability || 0) * 100)}%</span>
                      </div>
                      <p className="lic-world-expl">{safeText(w?.explanation)}</p>
                      <div className="lic-world-sub">Triggers to watch</div>
                      <ul className="lic-world-triggers">
                        {asArray(w?.trigger_conditions).map((x, xi) => (
                          <li key={`${wi}-tr-${xi}`}>{safeText(x)}</li>
                        ))}
                      </ul>
                      <div className="lic-world-sub lic-world-sub--action">If this view dominates</div>
                      <div className="lic-world-action">{safeText(w?.action_if_dominant)}</div>
                    </div>
                  ))}
                </div>
              )}
              {detailTab === 'analog' && (
                <div className="lic-drill-panel lic-analog-panel">
                  {(() => {
                    const u = activeIntel.analog_ui || {}
                    const weak = u.low_similarity_note || (u.confidence_tier === 'weak' && (u.analog_count ?? 0) > 0)
                    const tier = safeText(u.confidence_tier, '')
                    const n = Number(u.analog_count ?? 0)
                    const whyConf =
                      n <= 0
                        ? 'No close analog cluster in the bootstrap slice — tier reflects missing history, not a bad tape read.'
                        : tier === 'strong'
                          ? 'High match quality and a usable sample size — backward-looking stats carry more weight.'
                          : tier === 'moderate'
                            ? 'Partial match to history — use analogs as context alongside tape and thesis.'
                            : 'Low match quality or thin sample — treat averages as exploratory only.'
                    return (
                      <>
                        <section className="lic-drill-section">
                          <h5 className="lic-drill-h">Analog confidence</h5>
                          <p className="lic-drill-lead">{safeText(u.chip_verdict || u.confidence_plain)}</p>
                          <p className="lic-drill-muted">{whyConf}</p>
                        </section>
                        <section className="lic-drill-section">
                          <h5 className="lic-drill-h">Bias & sample</h5>
                          <div className="lic-analog-grid lic-analog-grid--drill">
                            <div><span className="lic-k">Bias</span><span>{safeText(u.bias_plain)}</span></div>
                            <div><span className="lic-k">Episodes</span><span>{safeText(u.analog_count ?? 0)}</span></div>
                            <div><span className="lic-k">Winners</span><span>{safeText(u.winners ?? 0)}</span></div>
                            <div><span className="lic-k">Losers</span><span>{safeText(u.losers ?? 0)}</span></div>
                          </div>
                        </section>
                        <section className="lic-drill-section">
                          <h5 className="lic-drill-h">Forward outcome (historical)</h5>
                          <p className="lic-analog-line">{safeText(u.forward_outcome_summary)}</p>
                        </section>
                        <section className="lic-drill-section">
                          <h5 className="lic-drill-h">Exit timing hint</h5>
                          <p className="lic-analog-line">{safeText(u.exit_timing_hint_plain)}</p>
                        </section>
                        {weak ? (
                          <div className="lic-analog-weakbox">
                            <div className="lic-analog-weaktitle">Analog guidance weak</div>
                            <p className="lic-analog-warn">{safeText(u.low_similarity_note || 'Current path has low similarity to trained historical episodes — lean on tape, thesis, and risk limits.')}</p>
                          </div>
                        ) : null}
                      </>
                    )
                  })()}
                </div>
              )}
              {detailTab === 'simulator' && (
                <div className="lic-drill-panel lic-sim-panel">
                  {(() => {
                    const sim = activeIntel.action_simulation || {}
                    const best = sim.best_action || {}
                    const rows = asArray(sim.alternatives)
                    const bestKey = best?.action
                    const pres = workspacePres || resolveDecisionPresentation(activeIntel)
                    const misaligned = sim.aligns_with_tile_recommendation === false
                    return (
                      <>
                        <p className="lic-sim-tile-ref">
                          Primary stance remains <strong>{pres.primary_action}</strong>
                          <span className="lic-sim-tile-ref-hint" title="Official posture from the risk ladder; table is a heuristic net-score lens.">
                            {' '}· simulator net score = upside − downside (exploratory)
                          </span>
                        </p>
                        {misaligned && pres.fallback_action ? (
                          <p className="lic-sim-frame lic-sim-frame--sub">
                            Defensive scoring prefers <strong>{pres.fallback_action}</strong> if capital preservation is prioritized.
                            {' '}
                            <strong>{pres.primary_action}</strong> remains the official posture; rationale:{' '}
                            {(() => {
                              const pr = safeText(pres.primary_reason)
                              return pr.length > 160 ? `${pr.slice(0, 160)}…` : pr
                            })()}
                          </p>
                        ) : (
                          <p className="lic-sim-frame lic-sim-frame--sub">
                            Heuristic net-score leader matches the primary stance — use the table to compare trims and tightening
                            versus full exit under different regret assumptions.
                          </p>
                        )}
                        <div className={`lic-sim-best ${misaligned ? 'lic-sim-best--subordinate' : ''}`}>
                          <div className="lic-sim-best-label">
                            {misaligned ? 'Highest net-score action (defensive lens)' : 'Net-score leader (aligned with primary)'}
                          </div>
                          <div className="lic-sim-best-action">{safeText(best.label)}</div>
                          <p className="lic-sim-best-why">{safeText(best.why)}</p>
                        </div>
                        <section className="lic-drill-section">
                          <h5 className="lic-drill-h">All actions</h5>
                          <p className="lic-drill-muted">
                            Highlighted row is the heuristic net-score winner. Official stance stays the primary action in the
                            reconciliation block above when the two differ.
                          </p>
                          <div className="lic-sim-table-wrap">
                            <table className="lic-sim-table">
                              <thead>
                                <tr>
                                  <th>Action</th>
                                  <th>Upside</th>
                                  <th>Downside</th>
                                  <th>Giveback</th>
                                  <th>Regret</th>
                                </tr>
                              </thead>
                              <tbody>
                                {rows.map((r, ri) => (
                                  <tr
                                    key={r?.action ?? r?.label ?? `sim-${ri}`}
                                    className={r?.action === bestKey ? 'lic-sim-row--best' : undefined}
                                  >
                                    <td>{safeText(r?.label)}</td>
                                    <td>{formatSimPct(r?.expected_upside)}</td>
                                    <td>{formatSimPct(r?.expected_downside)}</td>
                                    <td>{formatSimPct(r?.giveback_risk)}</td>
                                    <td>{safeText(r?.regret_tilt)}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </section>
                        <section className="lic-drill-section">
                          <h5 className="lic-drill-h">Rationale by action</h5>
                          {rows.map((r, ri) => (
                            <p key={`${r?.action ?? ri}-rat`} className="lic-sim-rat">
                              <b>{r?.label}:</b> {r?.rationale}
                            </p>
                          ))}
                        </section>
                      </>
                    )
                  })()}
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
                    {aiBusy ? 'Running…' : 'Run AI committee (event)'}
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
