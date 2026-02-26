import { useEffect, useMemo, useState } from 'react'
import {
  Line,
  LineChart,
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { API_BASE } from '../../App'
import LoadingState from '../../components/LoadingState'
import ErrorState from '../../components/ErrorState'
import { EvidenceBadge, fmtNum, IntradayHeader, HelpTip } from './IntradayTrainingCommon'
import './IntradayTraining.css'

export default function IntradayDashboardPage() {
  const [dashboard, setDashboard] = useState(null)
  const [patterns, setPatterns] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    Promise.all([
      fetch(`${API_BASE}/intraday/dashboard`).then((r) => (r.ok ? r.json() : Promise.reject(new Error('Failed dashboard fetch')))),
      fetch(`${API_BASE}/intraday/patterns?limit=25`).then((r) => (r.ok ? r.json() : Promise.reject(new Error('Failed patterns fetch')))),
    ])
      .then(([dash, pats]) => {
        if (cancelled) return
        setDashboard(dash)
        setPatterns(pats?.rows ?? [])
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const fallbackSeries = useMemo(() => {
    const rows = dashboard?.fallback_mix_series ?? []
    return rows.map((r) => {
      const total = Number(r.TOTAL_ROWS || 0)
      const safe = total > 0 ? total : 1
      return {
        date: r.CALCULATED_DATE,
        exactPct: (Number(r.EXACT_ROWS || 0) / safe) * 100,
        regimePct: (Number(r.REGIME_ROWS || 0) / safe) * 100,
        globalPct: (Number(r.GLOBAL_ROWS || 0) / safe) * 100,
      }
    })
  }, [dashboard])

  if (loading) return <LoadingState />
  if (error) return <ErrorState message={error} />

  const k = dashboard?.kpis ?? {}
  const validation = dashboard?.validation ?? {}

  return (
    <div className="it-page">
      <IntradayHeader
        title="Intraday Training Dashboard"
        subtitle="State-aware training health for v2 intraday, with deterministic trust snapshots."
      />

      <div className="it-grid">
        <div className="it-card">
          <div className="it-kpi-value">{fmtNum(k.SIGNALS_7D, 0)}</div>
          <div className="it-kpi-label">Signals last 7d <HelpTip text="Total 15-minute intraday signals in the last 7 days." /></div>
        </div>
        <div className="it-card">
          <div className="it-kpi-value">{fmtNum(k.SIGNALS_30D, 0)}</div>
          <div className="it-kpi-label">Signals last 30d <HelpTip text="Used for dashboard KPI/series reconciliation checks." /></div>
        </div>
        <div className="it-card">
          <div className="it-kpi-value">{fmtNum(k.GLOBAL_ROWS, 0)}</div>
          <div className="it-kpi-label">Global fallback rows <HelpTip text="High values mean trust still relies heavily on broad fallback." /></div>
        </div>
        <div className="it-card">
          <div className="it-kpi-value">{fmtNum(k.TERRAIN_DISTINCT_SCORES, 0)}</div>
          <div className="it-kpi-label">Distinct terrain scores <HelpTip text="Non-zero and sizable values indicate non-degenerate terrain." /></div>
        </div>
      </div>

      <div className="it-card">
        <h3>Fallback Mix Over Time <HelpTip text="Exact -> Regime -> Global fallback split over recent trust snapshots." /></h3>
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={fallbackSeries}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line dataKey="exactPct" stroke="#2e7d32" name="Exact %" dot={false} />
            <Line dataKey="regimePct" stroke="#ef6c00" name="Regime %" dot={false} />
            <Line dataKey="globalPct" stroke="#c62828" name="Global %" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="it-card">
        <h3>Signals Per Day <HelpTip text="Daily signal count and breadth across symbols." /></h3>
        <ResponsiveContainer width="100%" height={240}>
          <BarChart data={dashboard?.signals_per_day ?? []}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="SIGNAL_DATE" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="SIGNALS_TOTAL" fill="#1d4f8c" name="Signals" />
            <Bar dataKey="SYMBOLS_COVERED" fill="#70a5d9" name="Symbols" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="it-card">
        <h3>Terrain Dispersion Health <HelpTip text="Distinct score count and score stddev by day." /></h3>
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={dashboard?.terrain_health ?? []}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="CALCULATED_DATE" />
            <YAxis yAxisId="left" />
            <YAxis yAxisId="right" orientation="right" />
            <Tooltip />
            <Legend />
            <Line yAxisId="left" dataKey="DISTINCT_SCORE_COUNT" stroke="#4caf50" name="Distinct scores" dot={false} />
            <Line yAxisId="right" dataKey="TERRAIN_STDDEV" stroke="#7b1fa2" name="Stddev" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="it-card">
        <h3>Pattern Summary <HelpTip text="Top patterns by 30d signals with evidence status badges." /></h3>
        <div className="it-table-wrap">
          <table className="it-table">
            <thead>
              <tr>
                <th>Pattern</th>
                <th>Type</th>
                <th>Signals 30d</th>
                <th>Avg Evidence N</th>
                <th>Global Rows</th>
                <th>Evidence Status</th>
              </tr>
            </thead>
            <tbody>
              {patterns.map((row) => (
                <tr key={row.PATTERN_ID}>
                  <td>{row.PATTERN_NAME || row.PATTERN_ID}</td>
                  <td>{row.PATTERN_TYPE || '—'}</td>
                  <td>{fmtNum(row.SIGNALS_30D, 0)}</td>
                  <td>{fmtNum(row.AVG_EVIDENCE_N, 1)}</td>
                  <td>{fmtNum(row.GLOBAL_ROWS, 0)}</td>
                  <td><EvidenceBadge fallbackLevel={Number(row.GLOBAL_ROWS) > 0 ? 'GLOBAL' : 'EXACT'} evidenceN={row.AVG_EVIDENCE_N} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="it-card">
        <h3>Validation Output <HelpTip text="Smoke checks for deterministic snapshot selection and KPI-series reconciliation." /></h3>
        <div className="it-validation">
          <span>Signals KPI: {fmtNum(validation.signals_30d_kpi, 0)}</span>
          <span>Series Sum (30d): {fmtNum(validation.signals_series_sum, 0)}</span>
          <span>Reconciled: {String(validation.signals_reconciled)}</span>
          <span>Snapshot deterministic: {String(validation.snapshot_deterministic)}</span>
          <span>Snapshot rows A/B: {fmtNum(validation.snapshot_rows_a, 0)} / {fmtNum(validation.snapshot_rows_b, 0)}</span>
        </div>
      </div>
    </div>
  )
}
