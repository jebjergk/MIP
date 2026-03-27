import { useCallback, useEffect, useMemo, useState } from 'react'
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { API_BASE } from '../App'
import useVisibleInterval from '../hooks/useVisibleInterval'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import './LiveIntelligenceCockpit.css'

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
    const body = {
      bootstrap_version: bootstrapVersion || '1.0.0',
      positions: tiles,
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
        const step = await runStep(merged.tiles || {}, {})
        setIntelligence(step.intelligence_by_symbol || {})
        setPortfolioRegime(step.portfolio_regime || {})
        const ev = step.feed_events || []
        if (ev.length) {
          setFeed((f) => [...ev, ...f].slice(0, 120))
          setTimeline((t) => [...ev.map((e) => ({ ...e, kind: 'MATERIAL' })), ...t].slice(0, 200))
        }
        const peaks = {}
        ;(merged.tiles || []).forEach((tile) => {
          const s = String(tile.symbol || '').toUpperCase()
          const p = Number(tile.unrealized_pnl)
          if (s) peaks[s] = Number.isFinite(p) ? p : 0
        })
        setPeakPnl(peaks)
      } catch {
        const step = await runStep(tr.tiles || {}, {})
        setIntelligence(step.intelligence_by_symbol || {})
        setPortfolioRegime(step.portfolio_regime || {})
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
      const step = await runStep(merged.tiles || {}, intelligence)
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

      {loading ? <p style={{ color: '#94a3b8' }}>Loading bootstrap…</p> : null}

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
                  onClick={() => setSelectedSymbol(s)}
                >
                  {formatSymbolLabel(t.symbol, t.market_type)} · {intel?.attention_score?.toFixed?.(0) ?? '—'}
                </button>
              )
            })}
          </div>

          <div className="lic-tiles">
            {ranked.map((t) => {
              const s = String(t.symbol || '').toUpperCase()
              const intel = intelligence[s]
              const urg = intel?.exit_urgency || 'HOLD'
              return (
                <button
                  key={s}
                  type="button"
                  className={`lic-tile ${selectedSymbol === s ? 'lic-tile--selected' : ''}`}
                  onClick={() => setSelectedSymbol(s)}
                >
                  <h3>{formatSymbolLabel(t.symbol, t.market_type)} · {t.side}</h3>
                  <div className="lic-tile-meta">
                    <span>Thesis: {intel?.thesis_fracture || '—'}</span>
                    <span>Novelty: {intel?.novelty_state || '—'}</span>
                    <span>Preferred: {(intel?.action_simulation?.preferred_ranking || [])[0] || '—'}</span>
                    <span className={`lic-urgency lic-urgency--${urg}`}>{urg.replace('_', ' ')}</span>
                  </div>
                  <div style={{ marginTop: 6, fontSize: '0.72rem', color: '#cbd5e1' }}>
                    {intel?.why_now_delta?.human || ''}
                  </div>
                </button>
              )
            })}
          </div>
        </div>

        <aside className="lic-aside">
          <h4 style={{ margin: '0 0 0.5rem', fontSize: '0.85rem' }}>Case file</h4>
          <div className="lic-feed">
            {feed.length === 0 ? <div style={{ color: '#64748b', fontSize: '0.8rem' }}>No material events yet.</div> : null}
            {feed.map((row) => (
              <div key={`${row.ts}_${row.symbol}_${row.transition}`} className="lic-feed-row">
                <div><b>{row.symbol}</b> · {row.urgency}</div>
                <div>{row.why_now_human}</div>
              </div>
            ))}
          </div>

          {selectedSymbol && activeIntel ? (
            <>
              <h4 style={{ margin: '1rem 0 0.5rem', fontSize: '0.85rem' }}>Drill-down · {selectedSymbol}</h4>
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
                        <XAxis dataKey="ts" tick={{ fontSize: 9 }} />
                        <YAxis domain={['auto', 'auto']} tick={{ fontSize: 9 }} width={42} />
                        <Tooltip />
                        <Line type="monotone" dataKey="c" stroke="#38bdf8" dot={false} strokeWidth={1.5} />
                      </LineChart>
                    </ResponsiveContainer>
                  ) : (
                    <div style={{ color: '#64748b', fontSize: '0.75rem' }}>No bars</div>
                  )}
                </div>
              )}

              {detailTab === 'worlds' && (
                <pre className="lic-detail-pre">{JSON.stringify(activeIntel.scenario_worlds, null, 2)}</pre>
              )}
              {detailTab === 'analog' && (
                <pre className="lic-detail-pre">{JSON.stringify(activeIntel.analog_summary, null, 2)}</pre>
              )}
              {detailTab === 'simulator' && (
                <pre className="lic-detail-pre">{JSON.stringify(activeIntel.action_simulation, null, 2)}</pre>
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
