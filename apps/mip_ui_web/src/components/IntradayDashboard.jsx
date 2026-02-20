import React, { useState, useEffect, useMemo, useCallback } from 'react'
import { API_BASE } from '../App'
import IntradaySignalChart from './IntradaySignalChart'
import './IntradayDashboard.css'

/* ── Helpers ─────────────────────────────────────────── */

function fmtPct(v) {
  if (v == null) return '—'
  return `${(Number(v) * 100).toFixed(1)}%`
}
function fmtNum(v, d = 2) {
  if (v == null) return '—'
  return Number(v).toFixed(d)
}
function fmtTs(v) {
  if (!v) return '—'
  return new Date(v).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}
function horizonLabel(hb) {
  if (hb === -1) return 'EOD'
  return `+${hb}bar`
}

/* ── Small UI atoms ──────────────────────────────────── */

function StatusDot({ status }) {
  const color = { SUCCESS: '#2e7d32', PARTIAL: '#ef6c00', FAIL: '#c62828' }[status] || '#9e9e9e'
  return <span className="id-dot" style={{ background: color }} title={status} />
}

function TrustBadge({ status }) {
  const cls = { TRUSTED: 'id-badge--green', WATCH: 'id-badge--amber', IMMATURE: 'id-badge--grey', UNTRUSTED: 'id-badge--red' }[status] || 'id-badge--grey'
  return <span className={`id-badge ${cls}`}>{status || '—'}</span>
}

function ConfBadge({ level }) {
  const cls = { HIGH: 'id-badge--green', MEDIUM: 'id-badge--amber', LOW: 'id-badge--grey', NONE: 'id-badge--grey' }[level] || 'id-badge--grey'
  return <span className={`id-badge ${cls}`}>{level || 'NONE'}</span>
}

function StabilityBadge({ status }) {
  const cls = { STABLE: 'id-badge--green', IMPROVING: 'id-badge--blue', DEGRADING: 'id-badge--red', INSUFFICIENT_RECENT_DATA: 'id-badge--grey' }[status] || 'id-badge--grey'
  const label = status === 'INSUFFICIENT_RECENT_DATA' ? 'NO DATA' : (status || '—')
  return <span className={`id-badge ${cls}`}>{label}</span>
}

function TrendArrow({ drift }) {
  if (drift == null) return <span className="id-trend id-trend--flat">—</span>
  if (drift > 0.01) return <span className="id-trend id-trend--up">▲</span>
  if (drift < -0.01) return <span className="id-trend id-trend--down">▼</span>
  return <span className="id-trend id-trend--flat">→</span>
}

function Accordion({ title, subtitle, defaultOpen = false, children }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <section className="id-accordion">
      <button className="id-accordion-header" onClick={() => setOpen(!open)} aria-expanded={open}>
        <span className={`id-accordion-chevron ${open ? 'id-accordion-chevron--open' : ''}`}>&#9658;</span>
        <span className="id-accordion-title">{title}</span>
        {subtitle && <span className="id-accordion-subtitle">{subtitle}</span>}
      </button>
      {open && <div className="id-accordion-body">{children}</div>}
    </section>
  )
}

/* ── Derived data helpers ────────────────────────────── */

function deriveSystemStage(p, trustRows) {
  const totalSignals = Number(p.TOTAL_INTRADAY_SIGNALS) || 0
  const evaluated = Number(p.EVALUATED_OUTCOMES) || 0
  const trusted = trustRows.filter(r => r.TRUST_STATUS === 'TRUSTED').length
  if (trusted > 0) return 'CONFIDENT'
  if (evaluated >= 20) return 'LEARNING'
  if (totalSignals >= 5) return 'EMERGING'
  return 'INSUFFICIENT'
}

function groupByPatternType(trustRows) {
  const map = {}
  for (const r of trustRows) {
    const key = r.PATTERN_TYPE || 'UNKNOWN'
    if (!map[key]) map[key] = { type: key, rows: [] }
    map[key].rows.push(r)
  }
  for (const g of Object.values(map)) {
    g.rows.sort((a, b) => (Number(b.NET_SHARPE_LIKE) || 0) - (Number(a.NET_SHARPE_LIKE) || 0))
    g.best = g.rows[0]
    g.totalEvaluated = g.rows.reduce((s, r) => s + (Number(r.N_EVALUATED) || 0), 0)
    const trustOrder = { TRUSTED: 3, WATCH: 2, IMMATURE: 1, UNTRUSTED: 0 }
    g.bestTrust = g.rows.reduce((best, r) => (trustOrder[r.TRUST_STATUS] || 0) > (trustOrder[best] || 0) ? r.TRUST_STATUS : best, 'IMMATURE')
    const confOrder = { HIGH: 3, MEDIUM: 2, LOW: 1, NONE: 0 }
    g.bestConf = g.rows.reduce((best, r) => (confOrder[r.CONFIDENCE_LEVEL] || 0) > (confOrder[best] || 0) ? r.CONFIDENCE_LEVEL : best, 'NONE')
  }
  return Object.values(map).sort((a, b) => (Number(b.best?.NET_SHARPE_LIKE) || 0) - (Number(a.best?.NET_SHARPE_LIKE) || 0))
}

function groupScoreboardByPattern(trustRows) {
  const map = {}
  for (const r of trustRows) {
    const key = `${r.PATTERN_NAME || r.PATTERN_ID}`
    if (!map[key]) map[key] = { name: r.PATTERN_NAME || r.PATTERN_ID, type: r.PATTERN_TYPE, rows: [] }
    map[key].rows.push(r)
  }
  for (const g of Object.values(map)) {
    g.rows.sort((a, b) => (Number(b.NET_SHARPE_LIKE) || 0) - (Number(a.NET_SHARPE_LIKE) || 0))
    g.best = g.rows[0]
  }
  return Object.values(map).sort((a, b) => (Number(b.best?.NET_SHARPE_LIKE) || 0) - (Number(a.best?.NET_SHARPE_LIKE) || 0))
}

function getStabilityDrift(stabilityRows, patternType) {
  const match = stabilityRows.find(r => (r.PATTERN_TYPE || r.PATTERN_NAME || '').includes(patternType))
  return match?.HIT_RATE_DRIFT ?? null
}

/* ── Main Component ──────────────────────────────────── */

export default function IntradayDashboard() {
  const [pipeline, setPipeline] = useState(null)
  const [trust, setTrust] = useState(null)
  const [stability, setStability] = useState(null)
  const [excursion, setExcursion] = useState(null)
  const [loading, setLoading] = useState(true)
  const [showDiagnostics, setShowDiagnostics] = useState(() => {
    try { return localStorage.getItem('mip_intraday_diagnostics') === '1' } catch { return false }
  })
  const [expandedPatterns, setExpandedPatterns] = useState({})

  const toggleDiagnostics = useCallback(() => {
    setShowDiagnostics(prev => {
      const next = !prev
      try { localStorage.setItem('mip_intraday_diagnostics', next ? '1' : '0') } catch {}
      return next
    })
  }, [])

  const togglePattern = useCallback((key) => {
    setExpandedPatterns(prev => ({ ...prev, [key]: !prev[key] }))
  }, [])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    Promise.allSettled([
      fetch(`${API_BASE}/training/intraday/pipeline-status`).then(r => r.ok ? r.json() : null),
      fetch(`${API_BASE}/training/intraday/trust-scoreboard`).then(r => r.ok ? r.json() : null),
      fetch(`${API_BASE}/training/intraday/pattern-stability`).then(r => r.ok ? r.json() : null),
      fetch(`${API_BASE}/training/intraday/excursion-stats`).then(r => r.ok ? r.json() : null),
    ]).then(([pRes, tRes, sRes, eRes]) => {
      if (cancelled) return
      setPipeline(pRes.status === 'fulfilled' ? pRes.value : null)
      setTrust(tRes.status === 'fulfilled' ? tRes.value : null)
      setStability(sRes.status === 'fulfilled' ? sRes.value : null)
      setExcursion(eRes.status === 'fulfilled' ? eRes.value : null)
      setLoading(false)
    })
    return () => { cancelled = true }
  }, [])

  const p = pipeline || {}
  const trustRows = trust?.rows || []
  const stabilityRows = stability?.rows || []
  const excursionRows = excursion?.rows || []

  const systemStage = useMemo(() => deriveSystemStage(p, trustRows), [p, trustRows])
  const patternFamilies = useMemo(() => groupByPatternType(trustRows), [trustRows])
  const scoreboardGroups = useMemo(() => groupScoreboardByPattern(trustRows), [trustRows])
  const tradableCount = useMemo(() => trustRows.filter(r => r.TRUST_STATUS === 'TRUSTED').length, [trustRows])

  if (loading) return <div className="id-loading">Loading intraday cockpit…</div>

  const stageColor = { CONFIDENT: '#2e7d32', LEARNING: '#1565c0', EMERGING: '#ef6c00', INSUFFICIENT: '#9e9e9e' }[systemStage] || '#9e9e9e'

  return (
    <div className="id-dashboard">

      {/* ═══════════ LAYER 1: EXECUTIVE SUMMARY ═══════════ */}

      {/* Status Banner */}
      <div className="id-banner">
        <div className="id-banner-stage" style={{ borderColor: stageColor }}>
          <span className="id-banner-dot" style={{ background: stageColor }} />
          <span className="id-banner-label">System stage:</span>
          <strong style={{ color: stageColor }}>{systemStage}</strong>
        </div>
        <div className="id-banner-stats">
          <span title="Lifetime signals"><strong>{fmtNum(p.TOTAL_INTRADAY_SIGNALS, 0)}</strong> signals</span>
          <span className="id-banner-sep">·</span>
          <span title="Outcomes evaluated"><strong>{fmtNum(p.EVALUATED_OUTCOMES, 0)}</strong> evaluated</span>
          <span className="id-banner-sep">·</span>
          <span title="Symbols with data"><strong>{fmtNum(p.SYMBOLS_WITH_DATA, 0)}</strong> symbols</span>
          <span className="id-banner-sep">·</span>
          <span title="Patterns at TRUSTED level">Tradable: <strong>{tradableCount}</strong></span>
        </div>
      </div>

      {/* Pattern Readiness Tiles */}
      {patternFamilies.length > 0 && (
        <div className="id-tiles">
          {patternFamilies.map(fam => (
            <div key={fam.type} className="id-tile">
              <div className="id-tile-header">
                <span className="id-tile-type">{fam.type.replace(/_/g, ' ')}</span>
                <TrendArrow drift={getStabilityDrift(stabilityRows, fam.type)} />
              </div>
              <div className="id-tile-body">
                <div className="id-tile-metric">
                  <span className="id-tile-metric-label">Events</span>
                  <span className="id-tile-metric-value">{fam.totalEvaluated}</span>
                </div>
                <div className="id-tile-metric">
                  <span className="id-tile-metric-label">Trust</span>
                  <TrustBadge status={fam.bestTrust} />
                </div>
                <div className="id-tile-metric">
                  <span className="id-tile-metric-label">Confidence</span>
                  <ConfBadge level={fam.bestConf} />
                </div>
                <div className="id-tile-metric">
                  <span className="id-tile-metric-label">Best edge</span>
                  <span className={`id-tile-metric-value ${Number(fam.best?.AVG_NET_RETURN) > 0 ? 'id-positive' : Number(fam.best?.AVG_NET_RETURN) < 0 ? 'id-negative' : ''}`}>
                    {fmtPct(fam.best?.AVG_NET_RETURN)}
                  </span>
                </div>
              </div>
              <div className="id-tile-footer">
                Best horizon: <strong>{horizonLabel(fam.best?.HORIZON_BARS)}</strong>
                {' '}(Sharpe: {fmtNum(fam.best?.NET_SHARPE_LIKE)})
              </div>
            </div>
          ))}
        </div>
      )}
      {patternFamilies.length === 0 && (
        <p className="id-empty">No pattern data yet. Run the intraday pipeline to start learning.</p>
      )}

      {/* Compressed Pipeline Health */}
      <div className="id-pipeline-strip">
        <span>{p.IS_ENABLED ? <span className="id-pip-on">ON</span> : <span className="id-pip-off">OFF</span>}</span>
        <span><StatusDot status={p.LATEST_RUN_STATUS} /> {fmtTs(p.LATEST_RUN_STARTED_AT)}</span>
        <span>{fmtNum(p.LATEST_BARS_INGESTED, 0)} bars</span>
        <span>{fmtNum(p.LATEST_SIGNALS_GENERATED, 0)} signals</span>
        <span>{fmtNum(p.RUNS_LAST_7_DAYS, 0)} runs/7d</span>
        <span>{p.COMPUTE_SECONDS_LAST_7_DAYS != null ? `${fmtNum(p.COMPUTE_SECONDS_LAST_7_DAYS, 1)}s compute` : '—'}</span>
      </div>

      {/* ═══════════ LAYER 2: PATTERN INSIGHTS ═══════════ */}

      {/* Trust Scoreboard — compact: one row per pattern, expandable */}
      <section className="id-card">
        <h3 className="id-card-title">Pattern Trust Scoreboard</h3>
        <p className="id-card-subtitle">Best horizon per pattern — click to see all horizons</p>
        {scoreboardGroups.length === 0 ? (
          <p className="id-empty">No outcomes evaluated yet.</p>
        ) : (
          <div className="id-table-wrap">
            <table className="id-table id-table--interactive">
              <thead>
                <tr>
                  <th style={{ width: 24 }}></th>
                  <th>Pattern</th>
                  <th>Type</th>
                  <th>Best Horizon</th>
                  <th>Evaluated</th>
                  <th>Net Hit Rate</th>
                  <th>Avg Net Return</th>
                  <th>Sharpe</th>
                  <th>Trust</th>
                  <th>Confidence</th>
                </tr>
              </thead>
              <tbody>
                {scoreboardGroups.map(g => {
                  const isOpen = !!expandedPatterns[g.name]
                  const b = g.best
                  return (
                    <React.Fragment key={g.name}>
                      <tr className="id-row-clickable" onClick={() => togglePattern(g.name)} role="button" tabIndex={0}>
                        <td>
                          <span className={`id-expand-icon ${isOpen ? 'id-expand-icon--open' : ''}`}>&#9658;</span>
                        </td>
                        <td className="id-pattern-name">{g.name}</td>
                        <td>{g.type}</td>
                        <td>{horizonLabel(b?.HORIZON_BARS)}</td>
                        <td>{b?.N_EVALUATED}</td>
                        <td>{fmtPct(b?.NET_HIT_RATE)}</td>
                        <td className={Number(b?.AVG_NET_RETURN) > 0 ? 'id-positive' : Number(b?.AVG_NET_RETURN) < 0 ? 'id-negative' : ''}>
                          {fmtPct(b?.AVG_NET_RETURN)}
                        </td>
                        <td>{fmtNum(b?.NET_SHARPE_LIKE)}</td>
                        <td><TrustBadge status={b?.TRUST_STATUS} /></td>
                        <td><ConfBadge level={b?.CONFIDENCE_LEVEL} /></td>
                      </tr>
                      {isOpen && g.rows.map((r, i) => (
                        <tr key={i} className="id-subrow">
                          <td></td>
                          <td className="id-subrow-indent">{horizonLabel(r.HORIZON_BARS)}</td>
                          <td>{r.PATTERN_TYPE}</td>
                          <td>{horizonLabel(r.HORIZON_BARS)}</td>
                          <td>{r.N_EVALUATED}</td>
                          <td>{fmtPct(r.NET_HIT_RATE)}</td>
                          <td className={Number(r.AVG_NET_RETURN) > 0 ? 'id-positive' : Number(r.AVG_NET_RETURN) < 0 ? 'id-negative' : ''}>
                            {fmtPct(r.AVG_NET_RETURN)}
                          </td>
                          <td>{fmtNum(r.NET_SHARPE_LIKE)}</td>
                          <td><TrustBadge status={r.TRUST_STATUS} /></td>
                          <td><ConfBadge level={r.CONFIDENCE_LEVEL} /></td>
                        </tr>
                      ))}
                    </React.Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Signal Activity Chart — collapsible */}
      <Accordion title="Signal Activity" subtitle="Price chart with signal overlays" defaultOpen>
        <IntradaySignalChart />
      </Accordion>

      {/* ═══════════ LAYER 3: DEEP DIAGNOSTICS ═══════════ */}

      <div className="id-diagnostics-toggle">
        <button className="id-diag-btn" onClick={toggleDiagnostics}>
          {showDiagnostics ? 'Hide' : 'Show'} advanced diagnostics
        </button>
      </div>

      {showDiagnostics && (
        <>
          <Accordion title="Pattern Stability" subtitle="Detects drift between full history and recent window">
            {stabilityRows.length === 0 ? (
              <p className="id-empty">No stability data yet.</p>
            ) : (
              <div className="id-table-wrap">
                <table className="id-table">
                  <thead>
                    <tr>
                      <th>Pattern</th>
                      <th>Horizon</th>
                      <th>Full Hit Rate</th>
                      <th>Recent Hit Rate</th>
                      <th>Drift</th>
                      <th>Full Return</th>
                      <th>Recent Return</th>
                      <th>Return Drift</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stabilityRows.map((r, i) => (
                      <tr key={i}>
                        <td className="id-pattern-name">{r.PATTERN_NAME || r.PATTERN_ID}</td>
                        <td>{horizonLabel(r.HORIZON_BARS)}</td>
                        <td>{fmtPct(r.HIT_RATE_FULL)}</td>
                        <td>{fmtPct(r.HIT_RATE_RECENT)}</td>
                        <td className={Number(r.HIT_RATE_DRIFT) > 0 ? 'id-positive' : Number(r.HIT_RATE_DRIFT) < 0 ? 'id-negative' : ''}>
                          {r.HIT_RATE_DRIFT != null ? `${Number(r.HIT_RATE_DRIFT) > 0 ? '+' : ''}${fmtPct(r.HIT_RATE_DRIFT)}` : '—'}
                        </td>
                        <td>{fmtPct(r.AVG_NET_RETURN_FULL)}</td>
                        <td>{fmtPct(r.AVG_NET_RETURN_RECENT)}</td>
                        <td className={Number(r.RETURN_DRIFT) > 0 ? 'id-positive' : Number(r.RETURN_DRIFT) < 0 ? 'id-negative' : ''}>
                          {r.RETURN_DRIFT != null ? `${Number(r.RETURN_DRIFT) > 0 ? '+' : ''}${fmtPct(r.RETURN_DRIFT)}` : '—'}
                        </td>
                        <td><StabilityBadge status={r.STABILITY_STATUS} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Accordion>

          <Accordion title="Excursion Analysis" subtitle="Supports future stop-loss / take-profit design">
            {excursionRows.length === 0 ? (
              <p className="id-empty">No excursion data yet.</p>
            ) : (
              <div className="id-table-wrap">
                <table className="id-table">
                  <thead>
                    <tr>
                      <th>Pattern</th>
                      <th>Horizon</th>
                      <th>Evaluated</th>
                      <th>Avg MFE</th>
                      <th>Avg MAE</th>
                      <th>MFE (Win)</th>
                      <th>MAE (Loss)</th>
                      <th>MFE/MAE</th>
                    </tr>
                  </thead>
                  <tbody>
                    {excursionRows.map((r, i) => (
                      <tr key={i}>
                        <td className="id-pattern-name">{r.PATTERN_NAME || r.PATTERN_ID}</td>
                        <td>{horizonLabel(r.HORIZON_BARS)}</td>
                        <td>{r.N_EVALUATED}</td>
                        <td className="id-positive">{fmtPct(r.AVG_MFE)}</td>
                        <td className="id-negative">{fmtPct(r.AVG_MAE)}</td>
                        <td className="id-positive">{fmtPct(r.AVG_MFE_WINNERS)}</td>
                        <td className="id-negative">{fmtPct(r.AVG_MAE_LOSERS)}</td>
                        <td>{fmtNum(r.MFE_MAE_RATIO)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Accordion>
        </>
      )}
    </div>
  )
}
