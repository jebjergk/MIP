import { useCallback, useEffect, useMemo, useState } from 'react'
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { API_BASE } from '../App'
import useVisibleInterval from '../hooks/useVisibleInterval'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import './LiveIntelligenceCockpit.css'


function sessionFeedRow() {
  const ts = new Date().toISOString()
  return {
    timestamp: ts,
    ts,
    symbol: 'SESSION',
    state_transition: 'SESSION_START',
    what_changed: 'Baseline intelligence computed. New feed rows appear only when the server fingerprint changes vs your prior step (no spam on refresh).',
    action_implication: 'Review tiles for resolved stance and simulator alignment.',
    final_recommendation: '\u2014',
    severity: 'INFO',
  }
}

function attentionTooltip(intel) {
  const c = intel?.attention_components || {}
  const parts = Object.entries(c).map(([k, v]) => `${k}: ${v}`)
  return parts.length ? parts.join(' \u00b7 ') : 'Attention breakdown'
}

function bandLabel(band) {
  const m = {
    EXIT_NOW: 'Exit now',
    PREPARE_EXIT: 'Prepare exit',
    WATCH_CLOSELY: 'Watch closely',
    STAY_COURSE: 'Stay the course',
  }
  return m[band] || (band || '\u2014').replace(/_/g, ' ')
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

export default function LiveIntelligenceCockpit() {
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
  const [peakPnl, setPeakPnl] = useState({})

  const fetchIbLive = useCallback(async (tiles) => {
    const symbols = (Array.isArray(tiles) ? tiles : [])
      .map((t) => ({ symbol: t?.symbol, market_type: t?.market_type }))
      .filter((t) => t.symbol)
    if (symbols.length === 0) return null
    const body = {
      mode: 'intraday',
      intraday_bar_seconds: 30,
      window_bars: 780,
      symbols,
    }
    const resp = await fetch(`${API_BASE}/live-intelligence/ib-live`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!resp.ok) throw new Error(`IB live failed (${resp.status})`)
    return resp.json()
  }, [])

  const runStep = useCallback(async (tiles, priorIntel) => {
    const positions = Array.isArray(tiles) ? tiles : []
    const body = {
      bootstrap_version: bootstrapVersion || '1.0.0',
      positions,
      prior_intelligence: priorIntel,
      session_peak_pnl_by_symbol: peakPnl,
      analog_episodes_by_symbol: analogBySymbol,
      portfolio_context: portfolioContext,
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
    setLoading(true)
    setError('')
    setFeed([])
    setTimeline([])
    try {
      const resp = await fetch(`${API_BASE}/live-intelligence/bootstrap`)
      if (!resp.ok) throw new Error(`Bootstrap failed (${resp.status})`)
      const data = await resp.json()
      setBootstrapVersion(data.bootstrap_version || '')
      setAnalogBySymbol(data.analog_episodes_by_symbol || {})
      setPortfolioContext(data.portfolio_context || {})
      const tr = data.tracker || { tiles: [] }
      setTrackerData(tr)
      if (!selectedSymbol && tr.tiles?.[0]?.symbol) {
        setSelectedSymbol(String(tr.tiles[0].symbol).toUpperCase())
      }
      try {
        const ib = await fetchIbLive(tr.tiles || [])
        const merged = ib ? mergeTrackerIb(tr, ib) : tr
        setTrackerData(merged)
        const step = await runStep(merged.tiles || [], {})
        setIntelligence(step.intelligence_by_symbol || {})
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
      } catch {
        const step = await runStep(tr.tiles || [], {})
        setIntelligence(step.intelligence_by_symbol || {})
        setPortfolioRegime(step.portfolio_regime || {})
        const session = sessionFeedRow()
        setFeed([session])
        setTimeline([{ ...session, kind: 'SESSION' }])
      }
    } catch (e) {
      setError(e.message || 'Bootstrap error')
    } finally {
      setLoading(false)
    }
  }, [fetchIbLive, runStep, selectedSymbol])

  useEffect(() => {
    loadBootstrap()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const refreshLive = useCallback(async () => {
    try {
      setError('')
      const ib = await fetchIbLive(trackerData.tiles || [])
      if (!ib) return
      const merged = mergeTrackerIb(trackerData, ib)
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
      const step = await runStep(merged.tiles || [], intelligence)
      setIntelligence(step.intelligence_by_symbol || {})
      setPortfolioRegime(step.portfolio_regime || {})
      const ev = step.feed_events || []
      if (ev.length) {
        setFeed((f) => [...ev, ...f].slice(0, 120))
        setTimeline((t) => [...ev.map((e) => ({ ...e, kind: 'MATERIAL' })), ...t].slice(0, 200))
      }
    } catch (e) {
      setError(e.message || 'Live refresh failed')
    }
  }, [fetchIbLive, intelligence, runStep, trackerData])

  useVisibleInterval(refreshLive, 30000)

  const ranked = useMemo(() => {
    const tiles = trackerData.tiles || []
    return [...tiles].sort((a, b) => {
      const sa = String(a.symbol || '').toUpperCase()
      const sb = String(b.symbol || '').toUpperCase()
      const ia = intelligence[sa]?.attention_score ?? 0
      const ib = intelligence[sb]?.attention_score ?? 0
      return ib - ia
    })
  }, [trackerData.tiles, intelligence])

  const activeIntel = selectedSymbol ? intelligence[selectedSymbol] : null
  const activeTile = useMemo(() => {
    return (trackerData.tiles || []).find((t) => String(t.symbol || '').toUpperCase() === selectedSymbol) || null
  }, [trackerData.tiles, selectedSymbol])

  const chartData = useMemo(() => {
    const bars = activeTile?.chart?.bars || []
    return bars.map((b, i) => ({
      i,
      c: Number(b.close),
      ts: String(b.ts || '').slice(11, 19) || i,
    })).filter((r) => Number.isFinite(r.c))
  }, [activeTile])

  const runAi = useCallback(async () => {
    if (!selectedSymbol || !activeIntel) return
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
        {portfolioRegime.banner_text || 'Portfolio regime: no latent book shock detected.'}
      </div>

      {loading ? <p className="lic-muted">Loading bootstrap…</p> : null}

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
                  <span className="lic-attn-band">{intel?.attention_band || '—'}</span>
                  {' · '}
                  {intel?.attention_score?.toFixed?.(0) ?? '—'}
                </button>
              )
            })}
          </div>

          <div className="lic-tiles">
            {ranked.map((t) => {
              const s = String(t.symbol || '').toUpperCase()
              const intel = intelligence[s]
              const urg = intel?.exit_urgency || 'HOLD'
              const band = intel?.final_recommendation
              const pref = (intel?.action_simulation?.preferred_ranking || [])[0]
              return (
                <button
                  key={s}
                  type="button"
                  className={`lic-tile ${selectedSymbol === s ? 'lic-tile--selected' : ''}`}
                  onClick={() => setSelectedSymbol(s)}
                >
                  <h3>{formatSymbolLabel(t.symbol, t.market_type)} · {t.side}</h3>
                  <div className="lic-tile-band">{bandLabel(band)}</div>
                  <div className="lic-tile-meta">
                    <span>Thesis: {intel?.thesis_fracture || '—'}</span>
                    <span>Novelty: {intel?.novelty_state || '—'}</span>
                    <span>Action (aligned): {pref || '—'}</span>
                    <span className={`lic-urgency lic-urgency--${urg}`}>Urgency: {urg.replace(/_/g, ' ')}</span>
                  </div>
                  <div className="lic-tile-sub">
                    {intel?.portfolio_factor_chip ? <div>{intel.portfolio_factor_chip}</div> : null}
                    {intel?.analog_tile_line ? <div>{intel.analog_tile_line}</div> : null}
                  </div>
                  <div className="lic-tile-why">
                    {(intel?.why_now_bullets || []).slice(0, 3).map((b) => (
                      <div key={b}>• {b}</div>
                    ))}
                  </div>
                </button>
              )
            })}
          </div>
        </div>

        <aside className="lic-aside">
          <h4 className="lic-aside-title">Case file</h4>
          <div className="lic-feed">
            {feed.length === 0 ? <div className="lic-feed-empty">No material events yet.</div> : null}
            {feed.map((row) => {
              const k = `${row.ts || row.timestamp || ''}_${row.symbol}_${row.state_transition || row.transition || ''}`
              const body = row.what_changed || row.why_now_human || ''
              const trans = row.state_transition || row.transition || ''
              const sev = row.severity || ''
              return (
                <div key={k} className={`lic-feed-row lic-feed-row--${String(sev).toLowerCase()}`}>
                  <div className="lic-feed-row-head">
                    <b>{row.symbol}</b>
                    {row.final_recommendation && row.symbol !== 'SESSION' ? (
                      <span className="lic-feed-band"> · {bandLabel(row.final_recommendation)}</span>
                    ) : null}
                    {sev ? <span className="lic-feed-sev">{sev}</span> : null}
                  </div>
                  {trans ? <div className="lic-feed-trans">{trans}</div> : null}
                  <div className="lic-feed-body">{body}</div>
                  {row.action_implication ? (
                    <div className="lic-feed-action">Implication: {row.action_implication}</div>
                  ) : null}
                </div>
              )
            })}
          </div>

          {selectedSymbol && activeIntel ? (
            <>
              <h4 className="lic-aside-title lic-aside-title--spaced">Drill-down · {selectedSymbol}</h4>
              <div className="lic-tabs">
                {['chart', 'worlds', 'analog', 'simulator', 'timeline', 'evidence', 'ai'].map((tab) => (
                  <button
                    key={tab}
                    type="button"
                    className={detailTab === tab ? 'lic-tab--on' : ''}
                    onClick={() => setDetailTab(tab)}
                  >
                    {tab}
                  </button>
                ))}
              </div>

              {detailTab === 'chart' && (
                <div className="lic-chart-wrap">
                  {chartData.length > 0 ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={chartData}>
                        <XAxis dataKey="ts" tick={{ fill: '#e2e8f0', fontSize: 9 }} stroke="#64748b" />
                        <YAxis domain={['auto', 'auto']} tick={{ fill: '#e2e8f0', fontSize: 9 }} width={42} stroke="#64748b" />
                        <Tooltip
                          contentStyle={{ background: '#1e293b', border: '1px solid #334155', color: '#f1f5f9' }}
                          labelStyle={{ color: '#cbd5e1' }}
                        />
                        <Line type="monotone" dataKey="c" stroke="#38bdf8" dot={false} strokeWidth={1.5} />
                      </LineChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="lic-chart-empty">No bars</div>
                  )}
                </div>
              )}

              {detailTab === 'worlds' && (
                <pre className="lic-detail-pre">{JSON.stringify(activeIntel.scenario_worlds, null, 2)}</pre>
              )}
              {detailTab === 'analog' && (
                <div>
                  {activeIntel.analog_tile_line ? (
                    <p className="lic-analog-line">{activeIntel.analog_tile_line}</p>
                  ) : null}
                  <pre className="lic-detail-pre">{JSON.stringify(activeIntel.analog_summary, null, 2)}</pre>
                </div>
              )}
              {detailTab === 'simulator' && (
                <div>
                  {activeIntel.regret_tilt_label ? (
                    <p className="lic-analog-line">{activeIntel.regret_tilt_label}</p>
                  ) : null}
                  <pre className="lic-detail-pre">{JSON.stringify(activeIntel.action_simulation, null, 2)}</pre>
                </div>
              )}
              {detailTab === 'timeline' && (
                <pre className="lic-detail-pre">{JSON.stringify(timeline.filter((x) => x.symbol === selectedSymbol).slice(0, 20), null, 2)}</pre>
              )}
              {detailTab === 'evidence' && (
                <pre className="lic-detail-pre">{JSON.stringify({ tile: activeTile, derived: activeIntel.derived_features }, null, 2)}</pre>
              )}
              {detailTab === 'ai' && (
                <div>
                  <button type="button" onClick={runAi}>Run AI committee (event)</button>
                  <pre className="lic-detail-pre" style={{ marginTop: 8 }}>
                    {JSON.stringify(aiResult, null, 2)}
                  </pre>
                </div>
              )}
            </>
          ) : null}
        </aside>
      </div>
    </div>
  )
}
