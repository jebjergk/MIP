import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import './Home.css'

/** After this many ms we show the dashboard with whatever facts we have (partial is OK). */
const DASHBOARD_MAX_WAIT_MS = 5000

export default function Home() {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [facts, setFacts] = useState({
    snowflakeOk: null,
    snowflakeMessage: null,
    portfoliosCount: null,
    latestBriefTs: null,
    latestRunTs: null,
  })

  useEffect(() => {
    let cancelled = false

    const mergeFacts = (partial) => {
      if (!cancelled) setFacts((prev) => ({ ...prev, ...partial }))
    }

    const stopLoading = () => {
      if (!cancelled) setLoading(false)
    }

    const p1 = fetch(`${API_BASE}/status`)
      .then((r) => (r.ok ? r.json() : {}))
      .then((d) => mergeFacts({ snowflakeOk: !!d.snowflake_ok, snowflakeMessage: d.snowflake_message ?? null }))
      .catch(() => mergeFacts({ snowflakeOk: false, snowflakeMessage: 'Not reachable' }))

    const p2 = fetch(`${API_BASE}/portfolios`)
      .then((r) => (r.ok ? r.json() : []))
      .then((list) => mergeFacts({ portfoliosCount: Array.isArray(list) ? list.length : 0 }))
      .catch(() => mergeFacts({ portfoliosCount: null }))

    const p3 = fetch(`${API_BASE}/briefs/latest?portfolio_id=1`)
      .then((r) => (r.ok ? r.json() : { found: false }))
      .then((d) => mergeFacts({ latestBriefTs: d.found ? d.as_of_ts : null }))
      .catch(() => mergeFacts({ latestBriefTs: null }))

    const p4 = fetch(`${API_BASE}/runs?limit=1`)
      .then((r) => (r.ok ? r.json() : []))
      .then((list) => {
        const first = Array.isArray(list) && list.length ? list[0] : null
        mergeFacts({ latestRunTs: first?.completed_at ?? first?.started_at ?? null })
      })
      .catch(() => mergeFacts({ latestRunTs: null }))

    // Show dashboard after 5s with whatever we have, or as soon as all four finish.
    const stopWaitingTimer = setTimeout(stopLoading, DASHBOARD_MAX_WAIT_MS)
    Promise.allSettled([p1, p2, p3, p4]).then(stopLoading)

    return () => {
      cancelled = true
      clearTimeout(stopWaitingTimer)
    }
  }, [])

  if (loading) {
    return (
      <>
        <h1>MIP UI</h1>
        <LoadingState message="Checking system status…" />
      </>
    )
  }

  if (error) {
    return (
      <>
        <h1>MIP UI</h1>
        <ErrorState message={error} />
        <section className="home-quick-links" aria-label="Quick links">
          <h2>Quick links</h2>
          <ul>
            <li><Link to="/runs">Runs</Link></li>
            <li><Link to="/portfolios">Portfolios</Link></li>
            <li><Link to="/brief">Morning Brief</Link></li>
            <li><Link to="/training">Training Status</Link></li>
            <li><Link to="/suggestions">Suggestions</Link></li>
            <li><Link to="/debug">Debug</Link></li>
          </ul>
        </section>
      </>
    )
  }

  return (
    <>
      <h1>MIP UI</h1>
      <p className="home-tagline">Read-only view of pipeline runs, portfolios, morning briefs, and training status.</p>

      <section className="home-dashboard" aria-label="System status">
        <h2>Is the system alive?</h2>
        <div className="home-facts">
          <div className="home-fact">
            <span className="home-fact-label">Snowflake</span>
            <span className={`home-fact-value home-fact-value--${facts.snowflakeOk === true ? 'ok' : facts.snowflakeOk === false ? 'down' : 'unknown'}`}>
              {facts.snowflakeOk === true ? 'OK' : facts.snowflakeOk === false ? (facts.snowflakeMessage || 'Not reachable') : '—'}
            </span>
          </div>
          <div className="home-fact">
            <span className="home-fact-label">Portfolios</span>
            <span className="home-fact-value">{facts.portfoliosCount != null ? String(facts.portfoliosCount) : '—'}</span>
          </div>
          <div className="home-fact">
            <span className="home-fact-label">Latest brief (portfolio 1)</span>
            <span className="home-fact-value">{facts.latestBriefTs ?? '—'}</span>
          </div>
          <div className="home-fact">
            <span className="home-fact-label">Latest run</span>
            <span className="home-fact-value">{facts.latestRunTs ?? '—'}</span>
          </div>
        </div>
      </section>

      <section className="home-quick-links" aria-label="Quick links">
        <h2>Quick links</h2>
        <ul>
          <li><Link to="/runs">Runs</Link> — recent pipeline runs and run detail</li>
          <li><Link to="/portfolios">Portfolios</Link> — list and detail</li>
          <li><Link to="/brief">Morning Brief</Link> — latest brief per portfolio</li>
          <li><Link to="/training">Training Status</Link> — training maturity</li>
          <li><Link to="/suggestions">Suggestions</Link> — ranked symbol/pattern</li>
          <li><Link to="/debug">Debug</Link> — route smoke checks</li>
        </ul>
      </section>

      <EmptyState
        title="Seeing empty pages?"
        action={<>Run the daily pipeline in Snowflake, then check <Link to="/runs">Runs</Link>.</>}
        explanation="MIP data is populated by the daily pipeline. If pages look empty, run SP_RUN_DAILY_PIPELINE in Snowflake."
        reasons={['Pipeline has not run yet.', 'Snowflake credentials may be missing or invalid.']}
      />
    </>
  )
}
