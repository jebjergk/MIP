import { useState, useEffect, useMemo } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import LoadingState from '../components/LoadingState'
import TrainingDigestPanel from '../components/TrainingDigestPanel'
import { usePortfolios } from '../context/PortfolioContext'
import './Cockpit.css'

/* ── Helpers ─────────────────────────────────────────── */

function formatTs(ts) {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    return d.toLocaleString(undefined, {
      month: 'short', day: 'numeric', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    })
  } catch { return String(ts) }
}

function minutesAgo(ts) {
  if (!ts) return null
  try { return Math.round((Date.now() - new Date(ts).getTime()) / 60000) }
  catch { return null }
}

/* ── Sub-components ──────────────────────────────────── */

function AiBadge({ isAi, modelInfo }) {
  return isAi ? (
    <span className="ck-badge ck-badge--ai" title={`Model: ${modelInfo}`}>Cortex AI</span>
  ) : (
    <span className="ck-badge ck-badge--fallback">Deterministic</span>
  )
}

function FreshnessBadge({ createdAt }) {
  const mins = minutesAgo(createdAt)
  if (mins === null) return null
  const fresh = mins < 120
  return (
    <span className={`ck-badge ${fresh ? 'ck-badge--fresh' : 'ck-badge--stale'}`}>
      {fresh ? 'Fresh' : 'Stale'} ({mins < 60 ? `${mins}m ago` : `${Math.round(mins / 60)}h ago`})
    </span>
  )
}

function DigestSection({ title, icon, bullets, variant }) {
  if (!bullets || bullets.length === 0) return null
  return (
    <div className={`ck-section ck-section--${variant || 'default'}`}>
      <h4 className="ck-section-title">
        <span className="ck-section-icon">{icon}</span>
        {title}
      </h4>
      <ul className="ck-bullets">
        {bullets.map((b, i) => (
          <li key={i} className="ck-bullet">{typeof b === 'string' ? b : JSON.stringify(b)}</li>
        ))}
      </ul>
    </div>
  )
}

function DrillLinks({ whereToLook, links }) {
  const allLinks = []
  if (whereToLook?.length) {
    whereToLook.forEach((item) => {
      if (item?.route && item?.label) allLinks.push({ to: item.route, label: item.label })
    })
  }
  if (links) {
    ['signals', 'training', 'portfolio', 'market_timeline', 'digest'].forEach((key) => {
      if (links[key] && !allLinks.find((l) => l.to === links[key])) {
        const labels = { signals: 'Signals', training: 'Training', portfolio: 'Portfolio', market_timeline: 'Timeline', digest: 'Digest' }
        allLinks.push({ to: links[key], label: labels[key] || key })
      }
    })
  }
  if (!allLinks.length) return null
  return (
    <div className="ck-drill-links">
      {allLinks.map((l, i) => (
        <Link key={i} to={l.to} className="ck-drill-link">{l.label} &rarr;</Link>
      ))}
    </div>
  )
}

function DetectorPills({ detectors }) {
  if (!detectors?.length) return null
  const fired = detectors.filter((d) => d.fired)
  if (!fired.length) return null
  return (
    <div className="ck-detectors">
      {fired.map((d, i) => {
        const sev = (d.severity || 'low').toLowerCase()
        return (
          <span key={i} className={`ck-detector ck-detector--${sev}`} title={JSON.stringify(d.detail, null, 2)}>
            {(d.detector || '').replace(/_/g, ' ')}
          </span>
        )
      })}
    </div>
  )
}

/* ── Upcoming Symbols Card ───────────────────────────── */

function UpcomingSymbols({ trainingData }) {
  const snapshot = trainingData?.snapshot || {}
  const nearMiss = snapshot.near_miss_symbols || []
  const topConfident = snapshot.top_confident_symbols || []

  if (!nearMiss.length && !topConfident.length) {
    return (
      <div className="ck-card">
        <h3 className="ck-card-title">Upcoming Symbols</h3>
        <p className="ck-empty">No near-miss or upcoming symbol data available yet.</p>
      </div>
    )
  }

  return (
    <div className="ck-card">
      <h3 className="ck-card-title">Upcoming Symbols — Closest to Trade-Eligible</h3>
      <p className="ck-card-subtitle">Symbols closest to advancing to the next training stage. These are the most likely to unlock new trading opportunities.</p>

      {nearMiss.length > 0 && (
        <div className="ck-upcoming-list">
          {nearMiss.slice(0, 6).map((sym, i) => {
            const gap = sym.gap_to_next ?? '?'
            const score = sym.maturity_score ?? 0
            const stage = sym.maturity_stage || '?'
            const nextStage = stage === 'INSUFFICIENT' ? 'WARMING_UP' : stage === 'WARMING_UP' ? 'LEARNING' : stage === 'LEARNING' ? 'CONFIDENT' : 'NEXT'

            return (
              <div key={i} className="ck-upcoming-item">
                <div className="ck-upcoming-header">
                  <span className="ck-upcoming-rank">#{i + 1}</span>
                  <Link to={`/training?symbol=${sym.symbol}&market_type=${sym.market_type}`} className="ck-upcoming-symbol">
                    {sym.symbol}
                  </Link>
                  <span className="ck-upcoming-market">{sym.market_type}</span>
                  <span className={`ck-stage-pill ck-stage-pill--${(stage).toLowerCase().replace('_', '-')}`}>{stage}</span>
                </div>
                <div className="ck-upcoming-bar-wrap">
                  <div className="ck-upcoming-bar" style={{ width: `${Math.min(100, Math.max(0, score))}%` }} />
                </div>
                <div className="ck-upcoming-detail">
                  <span>Score: {score}/100</span>
                  <span className="ck-upcoming-gap">Gap to {nextStage}: {gap} pts</span>
                </div>
              </div>
            )
          })}
        </div>
      )}

      {topConfident.length > 0 && (
        <>
          <h4 className="ck-subsection-title">Trade-Ready (CONFIDENT)</h4>
          <div className="ck-upcoming-list">
            {topConfident.slice(0, 4).map((sym, i) => (
              <div key={i} className="ck-upcoming-item ck-upcoming-item--confident">
                <div className="ck-upcoming-header">
                  <Link to={`/training?symbol=${sym.symbol}&market_type=${sym.market_type}`} className="ck-upcoming-symbol">
                    {sym.symbol}
                  </Link>
                  <span className="ck-upcoming-market">{sym.market_type}</span>
                  <span className="ck-stage-pill ck-stage-pill--confident">CONFIDENT</span>
                  <span className="ck-upcoming-score">Score: {sym.maturity_score}/100</span>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

/* ── Insight Mini Cards (from Today API) ─────────────── */

function InsightsMini({ insights }) {
  if (!insights?.length) return null
  return (
    <div className="ck-card">
      <h3 className="ck-card-title">Today's Signal Candidates</h3>
      <p className="ck-card-subtitle">Symbols with eligible signals today, ranked by maturity and outcome history.</p>
      <div className="ck-insights-grid">
        {insights.slice(0, 6).map((item, i) => (
          <div key={i} className="ck-insight-mini">
            <div className="ck-insight-mini-header">
              <span className="ck-insight-mini-rank">#{i + 1}</span>
              <span className="ck-insight-mini-symbol">{item.symbol}</span>
              <span className={`ck-stage-pill ck-stage-pill--${(item.maturity_stage || '').toLowerCase().replace('_', '-')}`}>
                {item.maturity_stage}
              </span>
            </div>
            <div className="ck-upcoming-bar-wrap">
              <div className="ck-upcoming-bar" style={{ width: `${Math.min(100, Math.max(0, item.maturity_score ?? 0))}%` }} />
            </div>
            <p className="ck-insight-mini-why">{item.why_this_is_here || '—'}</p>
          </div>
        ))}
      </div>
    </div>
  )
}

/* ── Main Cockpit Page ───────────────────────────────── */

export default function Cockpit() {
  const [searchParams, setSearchParams] = useSearchParams()
  const portfolioIdParam = searchParams.get('portfolio_id')
  const portfolioId = portfolioIdParam ? parseInt(portfolioIdParam, 10) : null
  const { portfolios, defaultPortfolioId, loading: portfoliosLoading } = usePortfolios()

  const [digestGlobal, setDigestGlobal] = useState(null)
  const [digestPortfolio, setDigestPortfolio] = useState(null)
  const [trainingGlobal, setTrainingGlobal] = useState(null)
  const [todayData, setTodayData] = useState(null)
  const [loading, setLoading] = useState(true)

  // Default portfolio
  useEffect(() => {
    if (portfolioIdParam != null || portfoliosLoading || defaultPortfolioId == null) return
    setSearchParams({ portfolio_id: String(defaultPortfolioId) }, { replace: true })
  }, [defaultPortfolioId, portfolioIdParam, portfoliosLoading, setSearchParams])

  // Parallel fetch all data sources
  useEffect(() => {
    if (portfoliosLoading) return
    let cancelled = false
    setLoading(true)

    const fetches = [
      fetch(`${API_BASE}/digest/latest?scope=GLOBAL`).then(r => r.ok ? r.json() : null).catch(() => null),
      portfolioId
        ? fetch(`${API_BASE}/digest/latest?portfolio_id=${portfolioId}`).then(r => r.ok ? r.json() : null).catch(() => null)
        : Promise.resolve(null),
      fetch(`${API_BASE}/training/digest/latest`).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/today${portfolioId ? `?portfolio_id=${portfolioId}` : ''}`).then(r => r.ok ? r.json() : null).catch(() => null),
    ]

    Promise.all(fetches).then(([dg, dp, tg, td]) => {
      if (cancelled) return
      setDigestGlobal(dg)
      setDigestPortfolio(dp)
      setTrainingGlobal(tg)
      setTodayData(td)
      setLoading(false)
    })

    return () => { cancelled = true }
  }, [portfolioId, portfoliosLoading])

  const insights = todayData?.insights || []

  if (loading) {
    return (
      <>
        <h1>Cockpit</h1>
        <LoadingState message="Loading cockpit data..." />
      </>
    )
  }

  return (
    <div className="ck-page">
      <div className="ck-page-header">
        <h1>Cockpit</h1>
        {portfolios.length > 0 && (
          <label className="ck-portfolio-picker">
            Portfolio:
            <select
              value={portfolioId != null ? String(portfolioId) : ''}
              onChange={(e) => {
                const v = e.target.value
                setSearchParams(v ? { portfolio_id: v } : {})
              }}
            >
              <option value="">—</option>
              {portfolios.map((p) => {
                const id = p.PORTFOLIO_ID ?? p.portfolio_id
                return <option key={id} value={String(id)}>{p.NAME ?? p.name ?? id}</option>
              })}
            </select>
          </label>
        )}
      </div>

      {/* ═══ Row 1: Global Daily Digest + Portfolio Daily Digest ═══ */}
      <div className="ck-grid-2">
        {/* Global Digest */}
        <div className="ck-card ck-card--primary">
          <div className="ck-card-header">
            <h3 className="ck-card-title">System Overview</h3>
            <div className="ck-card-badges">
              <span className="ck-badge ck-badge--scope">Global</span>
              {digestGlobal?.found && <AiBadge isAi={digestGlobal.is_ai_narrative} modelInfo={digestGlobal.model_info} />}
              {digestGlobal?.found && <FreshnessBadge createdAt={digestGlobal.snapshot_created_at} />}
            </div>
          </div>
          {digestGlobal?.found ? (
            <>
              <p className="ck-headline">{digestGlobal.narrative?.headline || 'Global digest available'}</p>
              <DetectorPills detectors={digestGlobal.snapshot?.detectors} />
              <DigestSection title="What Changed" icon="&#x1F504;" bullets={digestGlobal.narrative?.what_changed} variant="changed" />
              <DigestSection title="What Matters" icon="&#x26A0;&#xFE0F;" bullets={digestGlobal.narrative?.what_matters} variant="matters" />
              <DigestSection title="Waiting For" icon="&#x23F3;" bullets={digestGlobal.narrative?.waiting_for} variant="waiting" />
              <DrillLinks whereToLook={digestGlobal.narrative?.where_to_look} links={digestGlobal.links} />
            </>
          ) : (
            <EmptyState title="No global digest yet" action="Run the pipeline to generate." />
          )}
        </div>

        {/* Portfolio Digest */}
        <div className="ck-card ck-card--portfolio">
          <div className="ck-card-header">
            <h3 className="ck-card-title">Portfolio Intelligence</h3>
            <div className="ck-card-badges">
              <span className="ck-badge ck-badge--scope">Portfolio</span>
              {digestPortfolio?.found && <AiBadge isAi={digestPortfolio.is_ai_narrative} modelInfo={digestPortfolio.model_info} />}
            </div>
          </div>
          {portfolioId == null ? (
            <p className="ck-empty">Select a portfolio above.</p>
          ) : digestPortfolio?.found ? (
            <>
              <p className="ck-headline">{digestPortfolio.narrative?.headline || 'Portfolio digest available'}</p>
              <DetectorPills detectors={digestPortfolio.snapshot?.detectors} />
              <DigestSection title="What Changed" icon="&#x1F504;" bullets={digestPortfolio.narrative?.what_changed} variant="changed" />
              <DigestSection title="What Matters" icon="&#x26A0;&#xFE0F;" bullets={digestPortfolio.narrative?.what_matters} variant="matters" />
              <DigestSection title="Waiting For" icon="&#x23F3;" bullets={digestPortfolio.narrative?.waiting_for} variant="waiting" />
              <DrillLinks whereToLook={digestPortfolio.narrative?.where_to_look} links={digestPortfolio.links} />
            </>
          ) : (
            <EmptyState title="No portfolio digest yet" action="Run the pipeline to generate." />
          )}
        </div>
      </div>

      {/* ═══ Row 2: Global Training Digest ═══ */}
      <TrainingDigestPanel scope="global" />

      {/* ═══ Row 3: Signal Candidates + Upcoming Symbols ═══ */}
      <div className="ck-grid-2">
        <InsightsMini insights={insights} />
        <UpcomingSymbols trainingData={trainingGlobal} />
      </div>
    </div>
  )
}
