import { useCallback, useEffect, useMemo, useState } from 'react'
import { Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { API_BASE } from '../App'
import useVisibleInterval from '../hooks/useVisibleInterval'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import LicTileMiniChart from '../components/lic/LicTileMiniChart'
import './LiveIntelligenceCockpit.css'


function sessionFeedRow() {
  const ts = new Date().toISOString()
  return {
    timestamp: ts,
    ts,
    symbol: 'SESSION',
    state_transition: 'SESSION_START',
    what_changed: 'Baseline intelligence computed. New feed rows appear only when the server fingerprint changes vs your prior step (no spam on refresh).',
    reason: 'Baseline intelligence computed. New feed rows appear only when the server fingerprint changes vs your prior step (no spam on refresh).',
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

  const runStep = useCallback(async (tiles, priorIntel, priorPortfolioRegime) => {
    const positions = Array.isArray(tiles) ? tiles : []
    const body = {
      bootstrap_version: bootstrapVersion || '1.0.0',
      positions,
      prior_intelligence: priorIntel,
      prior_portfolio_regime: priorPortfolioRegime || {},
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
      const step = await runStep(merged.tiles || [], intelligence, portfolioRegime)
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
              const rd = intel?.recommendation_display || {}
              const headline = rd.headline || bandLabel(intel?.final_recommendation)
              const au = intel?.analog_ui || {}
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
                  <LicTileMiniChart tile={t} />
                  <div className="lic-tile-dominant">{headline}</div>
                  {rd.confidence != null ? (
                    <div className="lic-tile-conf">
                      Confidence {Math.round(Number(rd.confidence) * 100)}%
                      <span className="lic-tile-conf-cap">{rd.confidence_caption}</span>
                    </div>
                  ) : null}
                  <div className="lic-tile-thesis">{intel?.thesis_plain || '—'}</div>
                  <ul className="lic-tile-drivers">
                    {(intel?.decision_drivers || []).map((d) => (
                      <li key={d}>{d}</li>
                    ))}
                  </ul>
                  <div className="lic-tile-chips">
                    <span className="lic-chip">Attention: {intel?.attention_band || '—'}</span>
                    <span className="lic-chip">{intel?.novelty_plain || '—'}</span>
                    <span className="lic-chip">{au.confidence_plain || 'History match'}</span>
                    <span className="lic-chip">{intel?.portfolio_factor_chip || '—'}</span>
                    <span className="lic-chip">{intel?.regret_tilt_label || '—'}</span>
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
              const body = row.reason || row.what_changed || row.why_now_human || ''
              const trans = row.state_transition || row.transition || ''
              const sev = row.severity || ''
              return (
                <div key={k} className={`lic-feed-row lic-feed-row--${String(sev).toLowerCase()}`}>
                  <div className="lic-feed-time">{formatFeedTime(row)}</div>
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
                    <div className="lic-feed-action">Next move: {row.action_implication}</div>
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
                <div className="lic-chart-wrap lic-chart-wrap--drill">
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
                        <Line type="monotone" dataKey="c" stroke="#38bdf8" dot={false} strokeWidth={1.5} />
                      </LineChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="lic-chart-empty">No bars</div>
                  )}
                  <div className="lic-chart-legend-keys">
                    <span><i className="lic-lg sw" />Price</span>
                    <span><i className="lic-lg med" />Expectation median</span>
                    <span><i className="lic-lg band" />Expectation band</span>
                    <span><i className="lic-lg ent" />Entry</span>
                    <span><i className="lic-lg sl" />Stop</span>
                    <span><i className="lic-lg tp" />Target</span>
                  </div>
                </div>
              )}

              {detailTab === 'worlds' && (
                <div className="lic-worlds">
                  {(activeIntel.scenario_worlds || []).map((w) => (
                    <div key={w.id} className="lic-world-card">
                      <div className="lic-world-head">
                        <span className="lic-world-title">{w.title}</span>
                        <span className="lic-world-pct">{w.probability_pct ?? Math.round((w.probability || 0) * 100)}%</span>
                      </div>
                      <p className="lic-world-expl">{w.explanation}</p>
                      <div className="lic-world-sub">If this scenario dominates</div>
                      <ul className="lic-world-triggers">
                        {(w.trigger_conditions || []).map((x) => (
                          <li key={x}>{x}</li>
                        ))}
                      </ul>
                      <div className="lic-world-action">{w.action_if_dominant}</div>
                    </div>
                  ))}
                </div>
              )}
              {detailTab === 'analog' && (
                <div className="lic-analog-panel">
                  {(() => {
                    const u = activeIntel.analog_ui || {}
                    return (
                      <>
                        <div className="lic-analog-grid">
                          <div><span className="lic-k">Match quality</span><span>{u.confidence_plain}</span></div>
                          <div><span className="lic-k">Bias</span><span>{u.bias_plain}</span></div>
                          <div><span className="lic-k">Episodes</span><span>{u.analog_count ?? 0}</span></div>
                          <div><span className="lic-k">Win / loss mix</span><span>{u.winners ?? 0} / {u.losers ?? 0}</span></div>
                        </div>
                        <p className="lic-analog-line">{u.forward_outcome_summary}</p>
                        <p className="lic-analog-line">{u.exit_timing_hint_plain}</p>
                        {u.low_similarity_note ? (
                          <p className="lic-analog-warn">{u.low_similarity_note}</p>
                        ) : null}
                      </>
                    )
                  })()}
                </div>
              )}
              {detailTab === 'simulator' && (
                <div className="lic-sim-panel">
                  {(() => {
                    const sim = activeIntel.action_simulation || {}
                    const best = sim.best_action || {}
                    const rows = sim.alternatives || []
                    return (
                      <>
                        <div className="lic-sim-best">
                          <div className="lic-sim-best-label">Favored action</div>
                          <div className="lic-sim-best-action">{best.label}</div>
                          <p className="lic-sim-best-why">{best.why}</p>
                        </div>
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
                            {rows.map((r) => (
                              <tr key={r.action}>
                                <td>{r.label}</td>
                                <td>{r.expected_upside}</td>
                                <td>{r.expected_downside}</td>
                                <td>{r.giveback_risk}</td>
                                <td>{r.regret_tilt}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                        {rows.map((r) => (
                          <p key={`${r.action}-rat`} className="lic-sim-rat"><b>{r.label}:</b> {r.rationale}</p>
                        ))}
                      </>
                    )
                  })()}
                </div>
              )}
              {detailTab === 'timeline' && (
                <div className="lic-timeline">
                  {timeline.filter((x) => x.symbol === selectedSymbol).length === 0 ? (
                    <p className="lic-muted">No material events for this symbol yet.</p>
                  ) : (
                    timeline.filter((x) => x.symbol === selectedSymbol).slice(0, 24).map((ev, idx) => (
                      <div key={`${ev.ts || ev.timestamp || idx}-${ev.state_transition || idx}`} className="lic-tl-row">
                        <div className="lic-tl-time">{formatFeedTime(ev)}</div>
                        <div className="lic-tl-sev">{ev.severity}</div>
                        <div className="lic-tl-trans">{ev.state_transition}</div>
                        <div className="lic-tl-body">{ev.reason || ev.what_changed}</div>
                        <div className="lic-tl-act">Next: {ev.action_implication}</div>
                      </div>
                    ))
                  )}
                </div>
              )}
              {detailTab === 'evidence' && (
                <div className="lic-evidence">
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
                      return (
                        <section key={k} className="lic-ev-block">
                          <h5 className="lic-ev-title">{titles[k]}</h5>
                          <p className="lic-ev-sum">{sec.summary}</p>
                          <div className="lic-ev-col">
                            <div className="lic-ev-supports">
                              <span className="lic-ev-tag">Supports recommendation</span>
                              <ul>{(sec.supports || []).map((x) => (<li key={x}>{x}</li>))}</ul>
                            </div>
                            <div className="lic-ev-opp">
                              <span className="lic-ev-tag lic-ev-tag--opp">Pushes the other way</span>
                              <ul>{(sec.opposes || []).map((x) => (<li key={x}>{x}</li>))}</ul>
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
                        <ul>{(activeIntel.supporting_signals || []).map((x) => (<li key={x}>{x}</li>))}</ul>
                      </div>
                      <div className="lic-ev-opp">
                        <span className="lic-ev-tag lic-ev-tag--opp">Counterpoints</span>
                        <ul>{(activeIntel.opposing_signals || []).map((x) => (<li key={x}>{x}</li>))}</ul>
                      </div>
                    </div>
                  </section>
                  {activeIntel.portfolio_factor_local ? (
                    <p className="lic-ev-foot">{activeIntel.portfolio_factor_local.localized_plain}</p>
                  ) : null}
                </div>
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
