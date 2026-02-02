import { useState, useEffect } from 'react'
import { useParams, Link, useNavigate } from 'react-router-dom'
import { API_BASE } from '../App'
import InfoTooltip from '../components/InfoTooltip'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import { useExplainMode } from '../context/ExplainModeContext'
import { getGlossaryEntry } from '../data/glossary'
import './AuditViewer.css'

export default function AuditViewer() {
  const { runId } = useParams()
  const { explainMode } = useExplainMode()
  const statusBadgeTitle = explainMode ? getGlossaryEntry('ui', 'status_badge')?.long : undefined
  const [runs, setRuns] = useState([])
  const [runDetail, setRunDetail] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    async function load() {
      setLoading(true)
      setError(null)
      try {
        if (runId) {
          const res = await fetch(`${API_BASE}/runs/${encodeURIComponent(runId)}`)
          if (!res.ok) throw new Error(res.statusText)
          const data = await res.json()
          if (!cancelled) setRunDetail(data)
        } else {
          const res = await fetch(`${API_BASE}/runs?limit=30`)
          if (!res.ok) throw new Error(res.statusText)
          const data = await res.json()
          if (!cancelled) setRuns(data)
        }
      } catch (e) {
        if (!cancelled) setError(e.message)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => { cancelled = true }
  }, [runId])

  if (loading) return <p>Loading…</p>
  if (error) {
    return (
      <>
        {runId && <p><Link to="/runs">← Back to runs</Link></p>}
        <ErrorState message={error} />
      </>
    )
  }

  if (runId && runDetail) {
    const sections = runDetail.sections || []
    const phases = runDetail.phases || []
    const timeline = runDetail.timeline || []
    const interpretedNarrative = runDetail.interpreted_narrative

    return (
      <>
        <h1>Run: {runId}</h1>
        <p><Link to="/runs">← Back to runs</Link></p>

        {/* Top narrative: "What happened" (includes "no new bars" clearly when applicable) */}
        {interpretedNarrative && (
          <section className="audit-narrative" aria-label="What happened">
            <h2>What happened <InfoTooltip scope="audit" key="run_status" variant="short" /></h2>
            <p className="audit-narrative-text">{interpretedNarrative}</p>
          </section>
        )}

        {/* What happened and why — structured sections */}
        {sections.length > 0 && (
          <section className="audit-what-happened" aria-label="What happened and why">
            <h2>What happened and why <InfoTooltip scope="audit" key="run_status" variant="short" /></h2>
            <div className="audit-sections">
              {sections.map((sec, i) => (
                <div key={i} className="audit-section-card" data-phase={sec.phase_key}>
                  <h3 className="audit-section-headline">{sec.headline}</h3>
                  {sec.phase_label && <span className="audit-section-phase">{sec.phase_label}</span>}
                  <dl className="audit-section-dl">
                    <dt>What happened</dt>
                    <dd>{sec.what_happened}</dd>
                    <dt>Why</dt>
                    <dd>{sec.why}</dd>
                    <dt>Impact</dt>
                    <dd>{sec.impact}</dd>
                    {sec.next_check != null && sec.next_check !== '' && (
                      <>
                        <dt>Next check</dt>
                        <dd>{sec.next_check}</dd>
                      </>
                    )}
                  </dl>
                </div>
              ))}
            </div>
          </section>
        )}

        {/* Phases (grouped steps) */}
        {phases.length > 0 && (
          <section className="audit-phases" aria-label="Pipeline phases">
            <h3>Pipeline phases</h3>
            <ul className="audit-phases-list">
              {phases.map((p, i) => (
                <li key={i}>
                  <strong>{p.phase_label}</strong>
                  <ul>
                    {(p.events || []).map((ev, j) => (
                      <li key={j}>
                        <span className="status-badge" title={statusBadgeTitle}>{ev.status}</span>
                        {ev.duration_seconds != null && ` (${ev.duration_seconds.toFixed(1)}s)`}
                        {ev.portfolio_count != null && ` — ${ev.portfolio_count} portfolio(s)`}
                      </li>
                    ))}
                  </ul>
                </li>
              ))}
            </ul>
          </section>
        )}

        {/* Legacy summary cards (compact) */}
        <h3>Summary cards <InfoTooltip scope="audit" key="run_status" variant="short" /></h3>
        <ul>
          {(runDetail.summary_cards || []).map((c, i) => (
            <li key={i}>
              {c.step_name}: <span className="status-badge" title={statusBadgeTitle}>{c.status}</span>
              {c.duration_seconds != null && ` (${c.duration_seconds.toFixed(1)}s)`}
              {c.portfolio_count != null && ` — ${c.portfolio_count} portfolio(s)`}
            </li>
          ))}
        </ul>

        <h3>Narrative bullets</h3>
        <ul>
          {(runDetail.narrative_bullets || []).map((b, i) => (
            <li key={i}>{b}</li>
          ))}
        </ul>

        {/* Raw timeline with tooltips */}
        <h3>Timeline (raw) <InfoTooltip scope="audit" key="latest_ts" variant="short" /></h3>
        <div className="audit-timeline-wrap">
          <table className="audit-timeline-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Type</th>
                <th>Event</th>
                <th>Status</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>
              {timeline.slice(0, 50).map((row, i) => {
                const details = typeof row.DETAILS === 'string' ? row.DETAILS : (row.DETAILS ? JSON.stringify(row.DETAILS) : '')
                const tooltip = [
                  row.EVENT_TS,
                  row.EVENT_TYPE,
                  row.EVENT_NAME,
                  row.STATUS,
                  row.ERROR_MESSAGE ? `Error: ${row.ERROR_MESSAGE}` : '',
                  details ? `DETAILS: ${details.slice(0, 200)}${details.length > 200 ? '…' : ''}` : ''
                ].filter(Boolean).join(' | ')
                return (
                  <tr key={i} className="audit-timeline-row" title={tooltip}>
                    <td>{row.EVENT_TS}</td>
                    <td>{row.EVENT_TYPE}</td>
                    <td>{row.EVENT_NAME}</td>
                    <td><span className="status-badge" title={statusBadgeTitle}>{row.STATUS}</span></td>
                    <td className="audit-timeline-details">{details ? '✓' : '—'}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          {timeline.length > 50 && <p className="audit-timeline-more">Showing first 50 of {timeline.length} events.</p>}
        </div>
      </>
    )
  }

  if (runs.length === 0) {
    return (
      <>
        <h1>Runs</h1>
        <EmptyState
          title="No runs yet"
          action="Run pipeline in Snowflake (SP_RUN_DAILY_PIPELINE)."
          explanation="Pipeline runs appear here after the daily pipeline executes. Trigger it in Snowflake, then refresh."
          reasons={['Pipeline has not run yet.', 'MIP_AUDIT_LOG may be empty.']}
        />
      </>
    )
  }

  return (
    <>
      <h1>Runs</h1>
      <p>Recent pipeline runs. Click a run to see timeline and interpreted summary.</p>
      <table className="runs-table">
        <thead>
          <tr>
            <th>Started</th>
            <th>Completed</th>
            <th>Run ID</th>
            <th>Status <InfoTooltip scope="audit" key="run_status" variant="short" /></th>
            <th>Summary</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((r) => (
            <tr
              key={r.run_id}
              className="runs-table-row-clickable"
              onClick={() => navigate(`/runs/${encodeURIComponent(r.run_id)}`)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); navigate(`/runs/${encodeURIComponent(r.run_id)}`); } }}
              aria-label={`View run ${r.run_id}`}
            >
              <td>{r.started_at ?? '—'}</td>
              <td>{r.completed_at ?? '—'}</td>
              <td><Link to={`/runs/${encodeURIComponent(r.run_id)}`} onClick={(e) => e.stopPropagation()}>{r.run_id?.slice(0, 12)}{(r.run_id?.length || 0) > 12 ? '…' : ''}</Link></td>
              <td><span className="status-badge" title={statusBadgeTitle}>{r.status ?? '—'}</span></td>
              <td>{r.summary_hint ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}
