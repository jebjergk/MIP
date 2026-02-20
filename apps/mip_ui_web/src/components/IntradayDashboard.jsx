import React, { useState, useEffect } from 'react'
import { API_BASE } from '../App'
import IntradaySignalChart from './IntradaySignalChart'
import './IntradayDashboard.css'

function StatusDot({ status }) {
  const color = {
    'SUCCESS': '#2e7d32',
    'PARTIAL': '#ef6c00',
    'FAIL': '#c62828',
    'SKIPPED_DISABLED': '#9e9e9e',
  }[status] || '#9e9e9e'
  return <span className="id-status-dot" style={{ background: color }} title={status} />
}

function TrustBadge({ status }) {
  const cls = {
    'TRUSTED': 'id-trust-trusted',
    'WATCH': 'id-trust-watch',
    'IMMATURE': 'id-trust-immature',
    'UNTRUSTED': 'id-trust-untrusted',
  }[status] || 'id-trust-immature'
  return <span className={`id-trust-badge ${cls}`}>{status || '—'}</span>
}

function StabilityBadge({ status }) {
  const cls = {
    'STABLE': 'id-stability-stable',
    'IMPROVING': 'id-stability-improving',
    'DEGRADING': 'id-stability-degrading',
    'INSUFFICIENT_RECENT_DATA': 'id-stability-insufficient',
  }[status] || 'id-stability-insufficient'
  const label = status === 'INSUFFICIENT_RECENT_DATA' ? 'NO DATA' : (status || '—')
  return <span className={`id-stability-badge ${cls}`}>{label}</span>
}

function fmtPct(v) {
  if (v == null) return '—'
  return `${(Number(v) * 100).toFixed(1)}%`
}

function fmtNum(v, decimals = 2) {
  if (v == null) return '—'
  return Number(v).toFixed(decimals)
}

function fmtTs(v) {
  if (!v) return '—'
  const d = new Date(v)
  return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

export default function IntradayDashboard() {
  const [pipeline, setPipeline] = useState(null)
  const [trust, setTrust] = useState(null)
  const [stability, setStability] = useState(null)
  const [excursion, setExcursion] = useState(null)
  const [loading, setLoading] = useState(true)

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

  if (loading) {
    return <div className="id-loading">Loading intraday dashboard...</div>
  }

  const p = pipeline || {}
  const trustRows = trust?.rows || []
  const stabilityRows = stability?.rows || []
  const excursionRows = excursion?.rows || []

  return (
    <div className="id-dashboard">
      {/* Pipeline Health Card */}
      <section className="id-card">
        <h3 className="id-card-title">Pipeline Health</h3>
        <div className="id-pipeline-grid">
          <div className="id-kpi">
            <span className="id-kpi-label">Status</span>
            <span className="id-kpi-value">
              {p.IS_ENABLED ? (
                <span className="id-enabled-badge">ENABLED</span>
              ) : (
                <span className="id-disabled-badge">DISABLED</span>
              )}
            </span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Interval</span>
            <span className="id-kpi-value">{p.INTERVAL_MINUTES || '—'}m</span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Latest Run</span>
            <span className="id-kpi-value">
              <StatusDot status={p.LATEST_RUN_STATUS} />
              {' '}{p.LATEST_RUN_STATUS || 'None'}
            </span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Last Run At</span>
            <span className="id-kpi-value">{fmtTs(p.LATEST_RUN_STARTED_AT)}</span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Bars Ingested</span>
            <span className="id-kpi-value">{fmtNum(p.LATEST_BARS_INGESTED, 0)}</span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Signals Generated</span>
            <span className="id-kpi-value">{fmtNum(p.LATEST_SIGNALS_GENERATED, 0)}</span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Total Intraday Bars</span>
            <span className="id-kpi-value">{fmtNum(p.TOTAL_INTRADAY_BARS, 0)}</span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Total Signals</span>
            <span className="id-kpi-value">{fmtNum(p.TOTAL_INTRADAY_SIGNALS, 0)}</span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Outcomes Evaluated</span>
            <span className="id-kpi-value">{fmtNum(p.EVALUATED_OUTCOMES, 0)}</span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Runs (7d)</span>
            <span className="id-kpi-value">{fmtNum(p.RUNS_LAST_7_DAYS, 0)}</span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Compute (7d)</span>
            <span className="id-kpi-value">{p.COMPUTE_SECONDS_LAST_7_DAYS != null ? `${fmtNum(p.COMPUTE_SECONDS_LAST_7_DAYS, 1)}s` : '—'}</span>
          </div>
          <div className="id-kpi">
            <span className="id-kpi-label">Symbols w/ Data</span>
            <span className="id-kpi-value">{fmtNum(p.SYMBOLS_WITH_DATA, 0)}</span>
          </div>
        </div>
      </section>

      {/* Signal Activity Chart */}
      <IntradaySignalChart />

      {/* Trust Scoreboard */}
      <section className="id-card">
        <h3 className="id-card-title">Pattern Trust Scoreboard</h3>
        <p className="id-card-subtitle">Fee-adjusted performance — patterns must pass trust gates before trading</p>
        {trustRows.length === 0 ? (
          <p className="id-empty">No intraday outcomes evaluated yet. Run the pipeline to start learning.</p>
        ) : (
          <div className="id-table-wrap">
            <table className="id-table">
              <thead>
                <tr>
                  <th>Pattern</th>
                  <th>Type</th>
                  <th>Horizon</th>
                  <th>Evaluated</th>
                  <th>Net Hit Rate</th>
                  <th>Avg Net Return</th>
                  <th>Sharpe-like</th>
                  <th>Avg Cost</th>
                  <th>Trust</th>
                  <th>Confidence</th>
                </tr>
              </thead>
              <tbody>
                {trustRows.map((r, i) => (
                  <tr key={i}>
                    <td className="id-pattern-name">{r.PATTERN_NAME || r.PATTERN_ID}</td>
                    <td>{r.PATTERN_TYPE}</td>
                    <td>{r.HORIZON_BARS}h</td>
                    <td>{r.N_EVALUATED}</td>
                    <td>{fmtPct(r.NET_HIT_RATE)}</td>
                    <td className={r.AVG_NET_RETURN > 0 ? 'id-positive' : r.AVG_NET_RETURN < 0 ? 'id-negative' : ''}>
                      {fmtPct(r.AVG_NET_RETURN)}
                    </td>
                    <td>{fmtNum(r.NET_SHARPE_LIKE)}</td>
                    <td>{fmtPct(r.AVG_ROUND_TRIP_COST)}</td>
                    <td><TrustBadge status={r.TRUST_STATUS} /></td>
                    <td>{r.CONFIDENCE_LEVEL}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Pattern Stability */}
      <section className="id-card">
        <h3 className="id-card-title">Pattern Stability</h3>
        <p className="id-card-subtitle">Comparing all-time vs recent 30-day performance to detect drift</p>
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
                  <th>Hit Rate Drift</th>
                  <th>Full Avg Return</th>
                  <th>Recent Avg Return</th>
                  <th>Return Drift</th>
                  <th>Stability</th>
                </tr>
              </thead>
              <tbody>
                {stabilityRows.map((r, i) => (
                  <tr key={i}>
                    <td className="id-pattern-name">{r.PATTERN_NAME || r.PATTERN_ID}</td>
                    <td>{r.HORIZON_BARS}h</td>
                    <td>{fmtPct(r.HIT_RATE_FULL)}</td>
                    <td>{fmtPct(r.HIT_RATE_RECENT)}</td>
                    <td className={r.HIT_RATE_DRIFT > 0 ? 'id-positive' : r.HIT_RATE_DRIFT < 0 ? 'id-negative' : ''}>
                      {r.HIT_RATE_DRIFT != null ? `${r.HIT_RATE_DRIFT > 0 ? '+' : ''}${fmtPct(r.HIT_RATE_DRIFT)}` : '—'}
                    </td>
                    <td>{fmtPct(r.AVG_NET_RETURN_FULL)}</td>
                    <td>{fmtPct(r.AVG_NET_RETURN_RECENT)}</td>
                    <td className={r.RETURN_DRIFT > 0 ? 'id-positive' : r.RETURN_DRIFT < 0 ? 'id-negative' : ''}>
                      {r.RETURN_DRIFT != null ? `${r.RETURN_DRIFT > 0 ? '+' : ''}${fmtPct(r.RETURN_DRIFT)}` : '—'}
                    </td>
                    <td><StabilityBadge status={r.STABILITY_STATUS} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Excursion Stats */}
      <section className="id-card">
        <h3 className="id-card-title">Excursion Analysis</h3>
        <p className="id-card-subtitle">Max favorable / adverse excursion per pattern — useful for stop-loss and take-profit design</p>
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
                  <th>MFE (Winners)</th>
                  <th>MAE (Losers)</th>
                  <th>MFE/MAE Ratio</th>
                </tr>
              </thead>
              <tbody>
                {excursionRows.map((r, i) => (
                  <tr key={i}>
                    <td className="id-pattern-name">{r.PATTERN_NAME || r.PATTERN_ID}</td>
                    <td>{r.HORIZON_BARS}h</td>
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
      </section>
    </div>
  )
}
