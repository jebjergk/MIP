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
  const [error, setError] = useState(null)
  const [liveMetrics, setLiveMetrics] = useState(null)
  const [liveActions, setLiveActions] = useState([])
  const [portfolioSnap, setPortfolioSnap] = useState(null)
  const [newsSnap, setNewsSnap] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    const metricsUrl = defaultPortfolioId != null
      ? `${API_BASE}/live/metrics?portfolio_id=${defaultPortfolioId}`
      : `${API_BASE}/live/metrics`
    const portfolioUrl = defaultPortfolioId != null
      ? `${API_BASE}/live/snapshot/latest?portfolio_id=${defaultPortfolioId}`
      : null
    Promise.all([
      fetch(metricsUrl)
        .then((r) => (r.ok ? r.json() : null))
        .catch(() => null),
      fetch(`${API_BASE}/live/trades/actions?pending_only=false&limit=300`)
        .then((r) => (r.ok ? r.json() : null))
        .catch(() => null),
      portfolioUrl
        ? fetch(portfolioUrl).then((r) => (r.ok ? r.json() : null)).catch(() => null)
        : Promise.resolve(null),
      fetch(`${API_BASE}/news/intelligence`)
        .then((r) => (r.ok ? r.json() : null))
        .catch(() => null),
    ])
      .then(([metrics, actionPayload, snap, news]) => {
        if (cancelled) return
        setLiveMetrics(metrics)
        setLiveActions(Array.isArray(actionPayload?.actions) ? actionPayload.actions : [])
        setPortfolioSnap(snap)
        setNewsSnap(news)
      })
      .catch(() => {
        if (!cancelled) setError('Failed to load home data.')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [defaultPortfolioId])

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

  const newsHighlights = useMemo(() => {
    if (!newsSnap) return null
    const ctx = newsSnap.market_context
    if (!ctx) return null
    const headlines = Array.isArray(ctx.top_headlines) ? ctx.top_headlines.slice(0, 3) : []
    const hotSymbols = Number(ctx.hot_symbols || 0)
    if (headlines.length === 0 && hotSymbols === 0) return null
    return { headlines, hotSymbols, generatedAt: newsSnap.generated_at }
  }, [newsSnap])

  if (loading) {
    return (
      <>
        <div className="home-hero home-hero--loading" aria-hidden="true">
          <div className="home-hero-overlay" />
          <div className="home-hero-content">
            <h1 className="home-hero-title">Market Intelligence Platform</h1>
            <p className="home-hero-subtext">Daily-bar research &bull; outcomes-based learning &bull; committee decisions</p>
          </div>
        </div>
        <LoadingState message="Loading home..." />
      </>
    )
  }

  return (
    <>
      <section className="home-hero" aria-label="Hero">
        <div className="home-hero-bg" role="img" aria-label="Market intelligence visual" />
        <div className="home-hero-overlay" aria-hidden="true" />
        <div className="home-hero-content">
          <h1 className="home-hero-title">Market Intelligence Platform</h1>
          <p className="home-hero-subtext">Daily-bar research &bull; outcomes-based learning &bull; committee decisions</p>
          <div className="home-next-action">
            <span className="home-next-label">Next best action:</span>
            <Link to={nextAction.to} className="home-next-link">{nextAction.label}</Link>
            <span className="home-next-reason">{nextAction.reason}</span>
          </div>
        </div>
      </section>

      {error && (
        <div className="home-alert home-alert--high" style={{ marginBottom: '1.5rem' }}>
          {error}
        </div>
      )}

      {actionCenterAlerts.length > 0 && (
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
      )}

      <section className="home-quick-actions" aria-label="Quick actions">
        <h2 className="home-section-title">Quick actions</h2>
        <div className="home-quick-actions-grid">
          <Link to="/decision-console" className="home-card home-card--link">
            <span className="home-card-title">AI Agent Decisions</span>
            <span className="home-card-desc">{liveSummary.pending} pending &bull; {liveSummary.executed} executed</span>
          </Link>
          <Link to="/cockpit" className="home-card home-card--link">
            <span className="home-card-title">Cockpit</span>
            <span className="home-card-desc">Strategic daily monitoring and narratives</span>
          </Link>
          {defaultPortfolioId != null ? (
            <Link to="/live-portfolio-activity" className="home-card home-card--link">
              <span className="home-card-title">Live Portfolio Activity</span>
              <span className="home-card-desc">Portfolio {defaultPortfolioId} broker truth and risk</span>
            </Link>
          ) : (
            <Link to="/live-portfolio-config" className="home-card home-card--link">
              <span className="home-card-title">Live Portfolio Config</span>
              <span className="home-card-desc">Configure active IB live portfolios</span>
            </Link>
          )}
          <Link to="/performance-dashboard" className="home-card home-card--link">
            <span className="home-card-title">Performance Dashboard</span>
            <span className="home-card-desc">Returns, drawdowns, and benchmark comparison</span>
          </Link>
          <Link to="/learning-ledger" className="home-card home-card--link">
            <span className="home-card-title">Learning Ledger</span>
            <span className="home-card-desc">Review latest evaluated outcomes</span>
          </Link>
          <Link to="/news-intelligence" className="home-card home-card--link">
            <span className="home-card-title">News Intelligence</span>
            <span className="home-card-desc">
              {newsHighlights ? `${newsHighlights.hotSymbols} hot symbol(s)` : 'Market news analysis'}
            </span>
          </Link>
        </div>
      </section>

      {portfolioSnap?.nav && (
        <section className="home-portfolio-snap" aria-label="Portfolio snapshot">
          <h2 className="home-section-title">Portfolio snapshot</h2>
          <div className="home-glance-grid">
            {portfolioSnap.nav.NET_LIQUIDATION_EUR != null && (
              <div className="home-card">
                <span className="home-card-label">Net liquidation (EUR)</span>
                <span className="home-card-value">&euro;{Number(portfolioSnap.nav.NET_LIQUIDATION_EUR).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
              </div>
            )}
            {portfolioSnap.nav.TOTAL_CASH_EUR != null && (
              <div className="home-card">
                <span className="home-card-label">Total cash (EUR)</span>
                <span className="home-card-value">&euro;{Number(portfolioSnap.nav.TOTAL_CASH_EUR).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
              </div>
            )}
            {portfolioSnap.nav.GROSS_POSITION_VALUE_EUR != null && (
              <div className="home-card">
                <span className="home-card-label">Gross position value (EUR)</span>
                <span className="home-card-value">&euro;{Number(portfolioSnap.nav.GROSS_POSITION_VALUE_EUR).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
              </div>
            )}
            {Array.isArray(portfolioSnap.positions) && (
              <div className="home-card">
                <span className="home-card-label">Open positions</span>
                <span className="home-card-value">{portfolioSnap.positions.length}</span>
              </div>
            )}
          </div>
        </section>
      )}

      <section className="home-glance" aria-label="System at a glance">
        <h2 className="home-section-title">System status</h2>
        <div className="home-glance-grid">
          <div className="home-card">
            <span className="home-card-label">Last pipeline run</span>
            <span className="home-card-value">
              {lastRun ? (
                <>
                  {relativeTime(lastRun.completed_at ?? lastRun.started_at)}
                  <span className={`home-glance-badge home-glance-badge--${(lastRun.status || '').toLowerCase()}`}>
                    {lastRun.status ?? '\u2014'}
                  </span>
                </>
              ) : (
                '\u2014'
              )}
            </span>
          </div>
          <div className="home-card">
            <span className="home-card-label">New evaluations since run</span>
            <span className="home-card-value">
              {sinceLastRun > 0 ? `+${sinceLastRun}` : sinceLastRun === 0 ? '0' : '\u2014'}
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

      {newsHighlights && newsHighlights.headlines.length > 0 && (
        <section className="home-news" aria-label="News highlights">
          <h2 className="home-section-title">
            News highlights
            <Link to="/news-intelligence" className="home-section-more">View all</Link>
          </h2>
          <div className="home-news-list">
            {newsHighlights.headlines.map((h, i) => (
              <div key={`${h.symbol}-${i}`} className="home-news-item">
                {h.badge && (
                  <span className={`home-news-badge home-news-badge--${String(h.badge).toLowerCase()}`}>
                    {h.badge}
                  </span>
                )}
                <span className="home-news-symbol">{h.symbol}</span>
                <span className="home-news-title">{h.title}</span>
              </div>
            ))}
          </div>
        </section>
      )}

      {actionCenterAlerts.length === 0 && (
        <section className="home-alerts" aria-label="System status">
          <div className="home-alert home-alert--ok">All clear &mdash; no immediate blockers detected.</div>
        </section>
      )}

      <section className="home-explore" aria-label="Explore MIP">
        <h2 className="home-section-title">Explore</h2>
        <div className="home-quick-actions-grid">
          <Link to="/market-timeline" className="home-card home-card--link">
            <span className="home-card-title">Market Timeline</span>
            <span className="home-card-desc">Historical daily bars and regime context</span>
          </Link>
          <Link to="/symbol-tracker" className="home-card home-card--link">
            <span className="home-card-title">Symbol Tracker</span>
            <span className="home-card-desc">Real-time symbol monitoring</span>
          </Link>
          <Link to="/training" className="home-card home-card--link">
            <span className="home-card-title">Training Status</span>
            <span className="home-card-desc">Model training progress and results</span>
          </Link>
          <Link to="/parallel-worlds" className="home-card home-card--link">
            <span className="home-card-title">Parallel Worlds</span>
            <span className="home-card-desc">What-if scenario simulations</span>
          </Link>
          <Link to="/runs" className="home-card home-card--link">
            <span className="home-card-title">Runs (Audit)</span>
            <span className="home-card-desc">Pipeline run history and diagnostics</span>
          </Link>
          <Link to="/guide" className="home-card home-card--link">
            <span className="home-card-title">User Guide</span>
            <span className="home-card-desc">Help documentation and getting started</span>
          </Link>
        </div>
      </section>

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
