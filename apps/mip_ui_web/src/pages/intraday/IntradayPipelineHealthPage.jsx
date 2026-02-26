import { useEffect, useState } from 'react'
import { API_BASE } from '../../App'
import LoadingState from '../../components/LoadingState'
import ErrorState from '../../components/ErrorState'
import { fmtNum, IntradayHeader, HelpTip } from './IntradayTrainingCommon'
import './IntradayTraining.css'

export default function IntradayPipelineHealthPage() {
  const [health, setHealth] = useState(null)
  const [runs, setRuns] = useState([])
  const [selectedRun, setSelectedRun] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    Promise.all([
      fetch(`${API_BASE}/intraday/health`).then((r) => (r.ok ? r.json() : Promise.reject(new Error('Failed health fetch')))),
      fetch(`${API_BASE}/intraday/backfill/runs?limit=50`).then((r) => (r.ok ? r.json() : Promise.reject(new Error('Failed runs fetch')))),
    ])
      .then(([healthResp, runsResp]) => {
        if (cancelled) return
        setHealth(healthResp)
        setRuns(runsResp?.rows ?? [])
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [])

  const loadRun = (runId) => {
    fetch(`${API_BASE}/intraday/backfill/run/${encodeURIComponent(runId)}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`Run ${runId} not found`))))
      .then((d) => setSelectedRun(d))
      .catch((e) => setError(e.message))
  }

  if (loading) return <LoadingState />
  if (error) return <ErrorState message={error} />

  const p = health?.pipeline ?? {}

  return (
    <div className="it-page">
      <IntradayHeader
        title="Pipeline Health"
        subtitle="Operational status for intraday training runs, including backfill run diagnostics."
      />

      <div className="it-grid">
        <div className="it-card">
          <div className="it-kpi-value">{p.LATEST_RUN_STATUS || '—'}</div>
          <div className="it-kpi-label">Latest run status <HelpTip text="Most recent intraday pipeline run status." /></div>
        </div>
        <div className="it-card">
          <div className="it-kpi-value">{fmtNum(p.LATEST_SIGNALS_GENERATED, 0)}</div>
          <div className="it-kpi-label">Latest signals generated <HelpTip text="Signals generated in latest run." /></div>
        </div>
        <div className="it-card">
          <div className="it-kpi-value">{fmtNum(p.RUNS_LAST_7_DAYS, 0)}</div>
          <div className="it-kpi-label">Runs last 7 days <HelpTip text="Run frequency and cadence signal operational stability." /></div>
        </div>
        <div className="it-card">
          <div className="it-kpi-value">{fmtNum(p.COMPUTE_SECONDS_LAST_7_DAYS, 1)}</div>
          <div className="it-kpi-label">Compute seconds (7d) <HelpTip text="Total warehouse compute consumed by intraday runs in the last week." /></div>
        </div>
      </div>

      <div className="it-card">
        <h3>Backfill Status Mix <HelpTip text="Status distribution for recent backfill chunks." /></h3>
        <div className="it-table-wrap">
          <table className="it-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Run count</th>
              </tr>
            </thead>
            <tbody>
              {(health?.backfill_status_mix ?? []).map((row) => (
                <tr key={row.STATUS}>
                  <td>{row.STATUS}</td>
                  <td>{fmtNum(row.RUN_COUNT, 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="it-card">
        <h3>Backfill Runs <HelpTip text="Recent chunk runs from INTRA_BACKFILL_RUN_LOG (read-only)." /></h3>
        <div className="it-table-wrap">
          <table className="it-table">
            <thead>
              <tr>
                <th>Run ID</th>
                <th>Chunk</th>
                <th>Status</th>
                <th>Signals</th>
                <th>Outcomes</th>
                <th>Trust</th>
                <th>Terrain</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((row) => (
                <tr key={`${row.RUN_ID}-${row.CHUNK_ID}`}>
                  <td><button onClick={() => loadRun(row.RUN_ID)}>{row.RUN_ID}</button></td>
                  <td>{row.CHUNK_ID}</td>
                  <td>{row.STATUS}</td>
                  <td>{fmtNum(row.ROWS_SIGNALS, 0)}</td>
                  <td>{fmtNum(row.ROWS_OUTCOMES, 0)}</td>
                  <td>{fmtNum(row.ROWS_TRUST, 0)}</td>
                  <td>{fmtNum(row.ROWS_TERRAIN, 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {selectedRun && (
        <div className="it-card">
          <h3>Selected Run Detail <HelpTip text="Detailed row for one run id." /></h3>
          <div className="it-table-wrap">
            <table className="it-table">
              <tbody>
                {Object.entries(selectedRun).map(([k, v]) => (
                  <tr key={k}>
                    <th>{k}</th>
                    <td>{String(v ?? '—')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
