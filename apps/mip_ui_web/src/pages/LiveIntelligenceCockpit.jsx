import { Component, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { API_BASE } from '../App'
import { fetchWithRetry } from '../utils/fetchRetry'
import useVisibleInterval from '../hooks/useVisibleInterval'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import LicTileMiniChart from '../components/lic/LicTileMiniChart'
import { caseFileImplicationDisplay, resolveDecisionPresentation } from '../components/lic/licDecisionPresentation'
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
    what_changed: 'Baseline intelligence computed. New feed rows appear only when the server fingerprint changes vs your prior step (no spam on refresh).',
    reason: 'Baseline intelligence computed. New feed rows appear only when the server fingerprint changes vs your prior step (no spam on refresh).',
    action_implication: 'Review each tile for primary stance vs defensive fallback (reconciliation block in workspace).',
    final_recommendation: '\u2014',
    severity: 'INFO',
  }
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

/** Short tile copy + full string for title/tooltip drill-down. */
function truncateTileText(s, maxLen) {
  const t = String(s || '').trim()
  if (!t) return { short: '', full: '' }
  if (t.length <= maxLen) return { short: t, full: t }
  return { short: `${t.slice(0, Math.max(0, maxLen - 1))}…`, full: t }
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
  'analog_tier_key',
  'dominant_world_id',
  'regret_bucket',
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
  const [detailTab, setDetailTab] = useState('chart')
  const [aiResult, setAiResult] = useState(null)
  const [aiBusy, setAiBusy] = useState(false)
  const [peakPnl, setPeakPnl] = useState({})
  const [bootReady, setBootReady] = useState(false)
  const bootstrapGenRef = useRef(0)
  const refreshGenRef = useRef(0)

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
        setFeed((f) => [...ev, session, ...f].slice(0, 120))
        setTimeline((t) => [
          ...ev.map((e) => ({ ...e, kind: 'MATERIAL' })),
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
        setFeed((f) => [...ev, ...f].slice(0, 120))
        setTimeline((t) => [...ev.map((e) => ({ ...e, kind: 'MATERIAL' })), ...t].slice(0, 200))
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

  const tilePresentation = useMemo(
    () =>
      buildVisibleTilePresentation(ranked, intelligence, (intel) =>
        intel ? resolveDecisionPresentation(intel).primary_action : '—',
      ),
    [ranked, intelligence],
  )

  const activeIntel = selectedSymbol ? intelligence[selectedSymbol] : null
  const workspacePres = useMemo(
    () => (activeIntel ? resolveDecisionPresentation(activeIntel) : null),
    [activeIntel],
  )
  const activeTile = useMemo(() => {
    return (trackerData.tiles || []).find((t) => String(t.symbol || '').toUpperCase() === selectedSymbol) || null
  }, [trackerData.tiles, selectedSymbol])

  const chartData = useMemo(() => {
    const bars = activeTile?.chart?.bars || []
    const med = Number(activeTile?.expectation?.center_path?.[0]?.price)
    const m = Number.isFinite(med) ? med : null
    return bars
      .map((b, i) => ({
        i,
        c: Number(b.close),
        m,
        ts: String(b.ts || '').slice(11, 19) || i,
      }))
      .filter((r) => Number.isFinite(r.c))
  }, [activeTile])

  const chartCallouts = useMemo(() => {
    if (!activeTile) return []
    const lines = []
    const exp = activeTile.expectation || {}
    const med = Number(exp?.center_path?.[0]?.price)
    const cur = Number(activeTile.current_price)
    const pm = activeTile.progress_metrics || {}
    const dsl = Number(pm.distance_to_sl_pct)
    const dtp = Number(pm.distance_to_tp_pct)
    if (Number.isFinite(med) && Number.isFinite(cur)) {
      if (cur < med) {
        lines.push('Price sits below the expectation median — more path pressure versus the planned center.')
      } else if (cur > med) {
        lines.push('Price is above the expectation median — working better than the median path for now.')
      }
    }
    if (Number.isFinite(dsl) && dsl < 0.03) {
      lines.push('Stop is close — small adverse moves can matter quickly.')
    }
    if (Number.isFinite(dtp) && Math.abs(dtp) < 0.04) {
      lines.push('Near the take-profit zone — much of the modeled reward may already be in the price.')
    }
    return lines
  }, [activeTile])

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
          No open positions in the bootstrap tracker. If you expect holdings here, reload bootstrap or confirm the active portfolio in Symbol Tracker.
        </p>
      ) : null}

      <div className="lic-layout">
      <div className="lic-grid">
        <div className="lic-main">
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
                  onClick={() => setSelectedSymbol(s)}
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
            {ranked.map((t) => {
              const s = String(t.symbol || '').toUpperCase()
              const intel = intelligence[s]
              const rd = intel?.recommendation_display || {}
              const pres = intel ? resolveDecisionPresentation(intel) : null
              const primaryAction = pres?.primary_action || '—'
              const au = intel?.analog_ui || {}
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
              return (
                <button
                  key={s}
                  type="button"
                  className={`lic-tile ${selectedSymbol === s ? 'lic-tile--selected' : ''}`}
                  onClick={() => setSelectedSymbol(s)}
                >
                  <div className="lic-tile-head">
                    <div className="lic-tile-head-main">
                      <span className="lic-tile-sym">{formatSymbolLabel(t.symbol, t.market_type)}</span>
                      <span className="lic-tile-side">{t.side}</span>
                    </div>
                    <div className="lic-tile-head-metrics">
                      <span>{Number.isFinite(Number(t.current_price)) ? Number(t.current_price).toFixed(2) : '—'}</span>
                      <span className={Number(t.unrealized_pnl) < 0 ? 'lic-pnl-neg' : 'lic-pnl-pos'}>
                        {formatMoney(t.unrealized_pnl)}
                      </span>
                      <span className="lic-tile-age">{formatHoldingAge(t.opened_at)}</span>
                    </div>
                  </div>
                  <div className="lic-tile-rec">{primaryAction}</div>
                  {rd.confidence != null ? (
                    <div className="lic-tile-conf" title={confTitle}>
                      <span className="lic-tile-conf-pct">
                        Confidence{' '}
                        {(() => {
                          const p = Math.round(Number(rd.confidence) * 100)
                          return Number.isFinite(p) ? p : 0
                        })()}
                        %
                      </span>
                    </div>
                  ) : null}
                  <LicTileMiniChart tile={t} recommendationBand={intel?.final_recommendation} />
                  {fallbackStrip ? (
                    <div
                      className="lic-tile-fallback-strip"
                      title="Context and defensive scoring — official stance is the primary action above."
                    >
                      {fallbackStrip}
                    </div>
                  ) : null}
                  <div className="lic-tile-thesis" title={thesisTooltip}>
                    {thesisLine}
                  </div>
                  <ul className="lic-tile-drivers">
                    {driverLines.map((d, di) => {
                      const txt = safeText(d)
                      const t = truncateTileText(txt, 72)
                      return (
                        <li key={`${s}-d-${di}`} className="lic-tile-driver-li" title={t.full !== t.short ? t.full : undefined}>
                          {t.short || txt}
                        </li>
                      )
                    })}
                  </ul>
                  <div className="lic-tile-chips">
                    {!chipSuppress.attention ? (
                      <span className="lic-chip" title="Attention">
                        <span className="lic-chip-k">Attention</span>
                        <span className="lic-chip-v">{safeText(intel?.attention_band)}</span>
                      </span>
                    ) : null}
                    {!chipSuppress.analog ? (
                      <span className="lic-chip" title="Historical analog">
                        <span className="lic-chip-k">Analog</span>
                        <span className="lic-chip-v">{safeText(au.chip_verdict || au.confidence_plain, '—')}</span>
                      </span>
                    ) : null}
                    {!chipSuppress.factor ? (
                      <span className="lic-chip" title="Portfolio factor">
                        <span className="lic-chip-k">Factor</span>
                        <span className="lic-chip-v">{safeText(intel?.portfolio_factor_chip)}</span>
                      </span>
                    ) : null}
                    {!chipSuppress.regret ? (
                      <span className="lic-chip" title="Regret tilt">
                        <span className="lic-chip-k">Regret</span>
                        <span className="lic-chip-v">{safeText(intel?.regret_tilt_label)}</span>
                      </span>
                    ) : null}
                  </div>
                </button>
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

        <section className="lic-workspace" aria-label="Selected symbol workspace">
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
                <div className="lic-drill-panel lic-drill-panel--chart lic-chart-wrap lic-chart-wrap--drill">
                  {chartData.length > 0 ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={chartData}>
                        <XAxis dataKey="ts" tick={{ fill: '#e2e8f0', fontSize: 9 }} stroke="#64748b" />
                        <YAxis domain={['auto', 'auto']} tick={{ fill: '#e2e8f0', fontSize: 9 }} width={42} stroke="#64748b" />
                        <Tooltip
                          contentStyle={{ background: '#1e293b', border: '1px solid #334155', color: '#f1f5f9' }}
                          labelStyle={{ color: '#cbd5e1' }}
                        />
                        {chartOverlay.lo != null ? (
                          <ReferenceLine y={chartOverlay.lo} stroke="#475569" strokeDasharray="4 3" />
                        ) : null}
                        {chartOverlay.hi != null ? (
                          <ReferenceLine y={chartOverlay.hi} stroke="#475569" strokeDasharray="4 3" />
                        ) : null}
                        {chartOverlay.med != null ? (
                          <ReferenceLine y={chartOverlay.med} stroke="#a78bfa" strokeDasharray="2 2" />
                        ) : null}
                        {chartOverlay.entry != null ? (
                          <ReferenceLine y={chartOverlay.entry} stroke="#94a3b8" />
                        ) : null}
                        {chartOverlay.sl != null ? (
                          <ReferenceLine y={chartOverlay.sl} stroke="#f87171" />
                        ) : null}
                        {chartOverlay.tp != null ? (
                          <ReferenceLine y={chartOverlay.tp} stroke="#4ade80" />
                        ) : null}
                        <Line type="monotone" dataKey="c" name="Close" stroke="#38bdf8" dot={false} strokeWidth={2} />
                        {chartData.some((r) => r.m != null) ? (
                          <Line
                            type="stepAfter"
                            dataKey="m"
                            name="Median"
                            stroke="#a78bfa"
                            dot={false}
                            strokeWidth={1.2}
                            strokeDasharray="4 3"
                            connectNulls
                          />
                        ) : null}
                      </LineChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="lic-chart-empty">No bars</div>
                  )}
                  <div className="lic-chart-legend-keys">
                    <span><i className="lic-lg sw" />Close</span>
                    <span><i className="lic-lg med" />Expectation median</span>
                    <span><i className="lic-lg band" />Expectation band</span>
                    <span><i className="lic-lg ent" />Entry</span>
                    <span><i className="lic-lg sl" />Stop</span>
                    <span><i className="lic-lg tp" />Target</span>
                  </div>
                  {chartCallouts.length ? (
                    <ul className="lic-chart-callouts">
                      {chartCallouts.map((line, ci) => (
                        <li key={`co-${ci}`}>{line}</li>
                      ))}
                    </ul>
                  ) : null}
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
