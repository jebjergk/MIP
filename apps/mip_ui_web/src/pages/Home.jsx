import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import InfoTooltip from '../components/InfoTooltip'
import LoadingState from '../components/LoadingState'
import { relativeTime } from '../components/LiveHeader'
import { useDefaultPortfolioId } from '../context/PortfolioContext'
import './Home.css'

export default function Home() {
  const defaultPortfolioId = useDefaultPortfolioId()
  const [loading, setLoading] = useState(true)
  const [liveMetrics, setLiveMetrics] = useState(null)
  useEffect(() => {
    let cancelled = false
    fetch(`${API_BASE}/live/metrics?portfolio_id=${defaultPortfolioId}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((data) => {
        if (!cancelled) setLiveMetrics(data)
      })
      .catch(() => {
        if (!cancelled) setLiveMetrics(null)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [defaultPortfolioId])

  if (loading) {
    return (
      <>
        <div className="home-hero home-hero--loading" aria-hidden="true">
          <div className="home-hero-overlay" />
          <div className="home-hero-content">
            <h1 className="home-hero-title">Market Intelligence Platform</h1>
            <p className="home-hero-subtext">Daily-bar research • outcomes-based learning • explainable suggestions</p>
          </div>
        </div>
        <LoadingState message="Loading…" />
      </>
    )
  }

  const lastRun = liveMetrics?.last_run
  const lastBrief = liveMetrics?.last_brief
  const outcomes = liveMetrics?.outcomes ?? {}
  const sinceLastRun = outcomes.since_last_run ?? 0

  return (
    <>
      <section className="home-hero" aria-label="Hero">
        <div className="home-hero-bg" role="img" aria-label="Market intelligence visual" />
        <div className="home-hero-overlay" aria-hidden="true" />
        <div className="home-hero-content">
          <h1 className="home-hero-title">
            Market Intelligence Platform
            <InfoTooltip scope="home" entryKey="hero_headline" variant="short" />
          </h1>
          <p className="home-hero-subtext">
            Daily-bar research • outcomes-based learning • explainable suggestions
            <InfoTooltip scope="home" entryKey="hero_subtext" variant="short" />
          </p>
        </div>
      </section>

      <section className="home-quick-actions" aria-label="Quick actions">
        <h2 className="home-section-title">
          Quick actions
          <InfoTooltip scope="home" entryKey="quick_actions" variant="short" />
        </h2>
        <div className="home-quick-actions-grid">
          <Link to="/portfolios" className="home-card home-card--link">
            <span className="home-card-title">View Portfolios</span>
            <span className="home-card-desc">All portfolios — positions, trades, episodes</span>
          </Link>
          {defaultPortfolioId != null && (
            <Link to={`/portfolios/${defaultPortfolioId}`} className="home-card home-card--link">
              <span className="home-card-title">Default portfolio</span>
              <span className="home-card-desc">Portfolio {defaultPortfolioId} — quick link</span>
            </Link>
          )}
          <Link to="/cockpit" className="home-card home-card--link">
            <span className="home-card-title">Open Cockpit</span>
            <span className="home-card-desc">AI narratives, portfolio status, training</span>
          </Link>
          <Link to="/training" className="home-card home-card--link">
            <span className="home-card-title">Open Training Status</span>
            <span className="home-card-desc">Maturity by symbol/pattern</span>
          </Link>
          <Link to="/suggestions" className="home-card home-card--link">
            <span className="home-card-title">Open Suggestions</span>
            <span className="home-card-desc">Ranked symbol/pattern candidates</span>
          </Link>
        </div>
      </section>

      <section className="home-glance" aria-label="System at a glance">
        <h2 className="home-section-title">
          System at a glance
          <InfoTooltip scope="home" entryKey="system_at_a_glance" variant="short" />
        </h2>
        <div className="home-glance-grid">
          <div className="home-card">
            <span className="home-card-label">
              Last pipeline run
              <InfoTooltip scope="live" entryKey="last_pipeline_run" variant="short" />
            </span>
            <span className="home-card-value">
              {lastRun ? (
                <>
                  {relativeTime(lastRun.completed_at ?? lastRun.started_at)}
                  <span className={`home-glance-badge home-glance-badge--${(lastRun.status || '').toLowerCase()}`}>
                    {lastRun.status ?? '—'}
                  </span>
                </>
              ) : (
                '—'
              )}
            </span>
          </div>
          <div className="home-card">
            <span className="home-card-label">
              New evaluations since last run
              <InfoTooltip scope="live" entryKey="new_evaluations_since_last_run" variant="short" />
            </span>
            <span className="home-card-value">
              {sinceLastRun > 0 ? `+${sinceLastRun}` : sinceLastRun === 0 ? '0' : '—'}
            </span>
          </div>
          <div className="home-card">
            <span className="home-card-label">
              Latest digest (as-of)
              <InfoTooltip scope="live" entryKey="data_freshness" variant="short" />
            </span>
            <span className="home-card-value">
              {lastBrief?.found ? relativeTime(lastBrief.as_of_ts) : 'No digest yet'}
            </span>
          </div>
        </div>
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
