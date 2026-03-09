import { useState, useEffect, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import LoadingState from '../components/LoadingState'
import { relativeTime } from '../components/LiveHeader'
import { useDefaultPortfolioId } from '../context/PortfolioContext'
import './Home.css'

function formatActionStatus(status) {
  const s = String(status || '').toUpperCase()
  if (!s) return 'Unknown'
  if (s.includes('REJECTED') || s.includes('BLOCKED') || s.includes('FAIL')) return 'Needs review'
  if (s.includes('EXECUTED') || s.includes('APPROVED') || s.includes('PASS')) return 'Completed'
  if (s.includes('PENDING') || s.includes('READY') || s.includes('PROPOSED')) return 'Pending'
  return s.replace(/_/g, ' ').toLowerCase()
}

export default function Home() {
  const defaultPortfolioId = useDefaultPortfolioId()
  const [loading, setLoading] = useState(true)
  const [liveMetrics, setLiveMetrics] = useState(null)
  const [liveActions, setLiveActions] = useState([])

  useEffect(() => {
    let cancelled = false
    Promise.all([
      fetch(`${API_BASE}/live/metrics?portfolio_id=${defaultPortfolioId}`)
        .then((r) => (r.ok ? r.json() : null))
        .catch(() => null),
      fetch(`${API_BASE}/live/trades/actions?pending_only=false&limit=300`)
        .then((r) => (r.ok ? r.json() : null))
        .catch(() => null),
    ])
      .then(([metrics, actionPayload]) => {
        if (cancelled) return
        setLiveMetrics(metrics)
        setLiveActions(Array.isArray(actionPayload?.actions) ? actionPayload.actions : [])
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
            <p className="home-hero-subtext">Daily-bar research • outcomes-based learning • committee decisions</p>
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
  const runAgeMinutes = useMemo(() => {
    const ts = lastRun?.completed_at ?? lastRun?.started_at
    if (!ts) return null
    const diff = Date.now() - new Date(ts).getTime()
    if (!Number.isFinite(diff)) return null
    return Math.max(0, Math.round(diff / 60000))
  }, [lastRun])

  const liveSummary = useMemo(() => {
    const rows = liveActions
    const pending = rows.filter((a) => {
      const s = String(a?.STATUS || '').toUpperCase()
      return s.includes('PENDING') || s.includes('READY') || s === 'PROPOSED' || s.includes('REVALIDATED')
    }).length
    const rejected = rows.filter((a) => String(a?.STATUS || '').toUpperCase().includes('REJECTED')).length
    const executed = rows.filter((a) => String(a?.STATUS || '').toUpperCase().includes('EXECUTED')).length
    const latest = rows
      .map((a) => ({ ts: a?.UPDATED_AT || a?.CREATED_AT, status: a?.STATUS }))
      .filter((a) => a.ts)
      .sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())[0]
    return { pending, rejected, executed, latestStatus: latest?.status || null, total: rows.length }
  }, [liveActions])

  const actionCenterAlerts = useMemo(() => {
    const alerts = []
    if (!lastRun) {
      alerts.push({ level: 'high', text: 'No completed pipeline run detected yet.' })
    }
    if (runAgeMinutes != null && runAgeMinutes > 24 * 60) {
      alerts.push({ level: 'high', text: `Pipeline freshness is stale (${Math.round(runAgeMinutes / 60)}h old).` })
    }
    if (liveSummary.pending > 0) {
      alerts.push({ level: 'med', text: `${liveSummary.pending} committee action(s) require review.` })
    }
    if (liveSummary.rejected > 0) {
      alerts.push({ level: 'med', text: `${liveSummary.rejected} action(s) were rejected and may need follow-up.` })
    }
    return alerts
  }, [lastRun, runAgeMinutes, liveSummary])

  const nextAction = useMemo(() => {
    if (liveSummary.pending > 0) {
      return { to: '/decision-console', label: 'Review pending AI Agent Decisions', reason: `${liveSummary.pending} pending` }
    }
    if (!lastRun || (runAgeMinutes != null && runAgeMinutes > 24 * 60)) {
      return { to: '/runs', label: 'Check pipeline runs and freshness', reason: 'Pipeline looks stale' }
    }
    if (sinceLastRun > 0) {
      return { to: '/learning-ledger', label: 'Review new learning updates', reason: `+${sinceLastRun} new evaluations` }
    }
    return { to: '/cockpit', label: 'Open Cockpit strategic view', reason: 'System stable' }
  }, [liveSummary.pending, lastRun, runAgeMinutes, sinceLastRun])

  return (
    <>
      <section className="home-hero" aria-label="Hero">
        <div className="home-hero-bg" role="img" aria-label="Market intelligence visual" />
        <div className="home-hero-overlay" aria-hidden="true" />
        <div className="home-hero-content">
          <h1 className="home-hero-title">Home Action Center</h1>
          <p className="home-hero-subtext">Triage now, act fast, route to the right page.</p>
          <div className="home-next-action">
            <span className="home-next-label">Next best action:</span>
            <Link to={nextAction.to} className="home-next-link">{nextAction.label}</Link>
            <span className="home-next-reason">{nextAction.reason}</span>
          </div>
        </div>
      </section>

      <section className="home-quick-actions" aria-label="Quick actions">
        <h2 className="home-section-title">Immediate actions</h2>
        <div className="home-quick-actions-grid">
          <Link to="/decision-console" className="home-card home-card--link">
            <span className="home-card-title">AI Agent Decisions</span>
            <span className="home-card-desc">{liveSummary.pending} pending • {liveSummary.executed} executed</span>
          </Link>
          <Link to="/cockpit" className="home-card home-card--link">
            <span className="home-card-title">Open Cockpit</span>
            <span className="home-card-desc">Strategic daily monitoring and narratives</span>
          </Link>
          <Link to="/runs" className="home-card home-card--link">
            <span className="home-card-title">Runs (Audit)</span>
            <span className="home-card-desc">Verify freshness and pipeline health</span>
          </Link>
          {defaultPortfolioId != null ? (
            <Link to={`/portfolios/${defaultPortfolioId}`} className="home-card home-card--link">
              <span className="home-card-title">Default Portfolio</span>
              <span className="home-card-desc">Portfolio {defaultPortfolioId} positions and risk</span>
            </Link>
          ) : (
            <Link to="/portfolios" className="home-card home-card--link">
              <span className="home-card-title">Portfolios</span>
              <span className="home-card-desc">All portfolios, gates, and exposures</span>
            </Link>
          )}
          <Link to="/learning-ledger" className="home-card home-card--link">
            <span className="home-card-title">Learning Ledger</span>
            <span className="home-card-desc">Review latest evaluated outcomes</span>
          </Link>
        </div>
      </section>

      <section className="home-glance" aria-label="System at a glance">
        <h2 className="home-section-title">Today control panel</h2>
        <div className="home-glance-grid">
          <div className="home-card">
            <span className="home-card-label">Last pipeline run</span>
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
            <span className="home-card-label">New evaluations since run</span>
            <span className="home-card-value">
              {sinceLastRun > 0 ? `+${sinceLastRun}` : sinceLastRun === 0 ? '0' : '—'}
            </span>
          </div>
          <div className="home-card">
            <span className="home-card-label">Latest digest (as-of)</span>
            <span className="home-card-value">
              {lastBrief?.found ? relativeTime(lastBrief.as_of_ts) : 'No digest yet'}
            </span>
          </div>
          <div className="home-card">
            <span className="home-card-label">Latest committee status</span>
            <span className="home-card-value">
              {liveSummary.latestStatus ? formatActionStatus(liveSummary.latestStatus) : 'No actions yet'}
            </span>
          </div>
          <div className="home-card">
            <span className="home-card-label">Rejected today</span>
            <span className="home-card-value">{liveSummary.rejected}</span>
          </div>
          <div className="home-card">
            <span className="home-card-label">Total tracked actions</span>
            <span className="home-card-value">{liveSummary.total}</span>
          </div>
        </div>
      </section>

      {actionCenterAlerts.length > 0 ? (
        <section className="home-alerts" aria-label="Alerts">
          <h2 className="home-section-title">Attention needed</h2>
          <div className="home-alert-list">
            {actionCenterAlerts.map((a, i) => (
              <div key={`${a.text}-${i}`} className={`home-alert home-alert--${a.level}`}>
                {a.text}
              </div>
            ))}
          </div>
        </section>
      ) : (
        <section className="home-alerts" aria-label="System status">
          <h2 className="home-section-title">Status</h2>
          <div className="home-alert home-alert--ok">System looks stable. No immediate blockers detected.</div>
        </section>
      )}

      {!lastRun && liveSummary.total === 0 && (
        <EmptyState
          title="No operating data yet"
          action={<>Run the daily pipeline in Snowflake, then check <Link to="/runs">Runs</Link>.</>}
          explanation="MIP data is populated by the daily pipeline. Once a run completes, action-center cards will populate."
          reasons={['Pipeline has not run yet.', 'Snowflake credentials may be missing or invalid.']}
        />
      )}
    </>
  )
}
