import { useState, useEffect } from 'react'
import { Link, useParams } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import { usePortfolios } from '../context/PortfolioContext'
import './Digest.css'

/* ── Helpers ─────────────────────────────────────────────────── */

function formatTs(ts) {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    return d.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  } catch {
    return String(ts)
  }
}

function minutesAgo(ts) {
  if (!ts) return null
  try {
    return Math.round((Date.now() - new Date(ts).getTime()) / 60000)
  } catch {
    return null
  }
}

/* ── Sub-components ──────────────────────────────────────────── */

function FreshnessBadge({ createdAt }) {
  const mins = minutesAgo(createdAt)
  if (mins === null) return null
  const fresh = mins < 120
  return (
    <span className={`freshness-badge ${fresh ? 'freshness-badge--fresh' : 'freshness-badge--stale'}`}>
      {fresh ? 'Fresh' : 'Stale'} ({mins < 60 ? `${mins}m ago` : `${Math.round(mins / 60)}h ago`})
    </span>
  )
}

function AiBadge({ isAi, modelInfo }) {
  if (isAi) {
    return (
      <span className="digest-ai-badge digest-ai-badge--ai" title={`Model: ${modelInfo}`}>
        Cortex AI
      </span>
    )
  }
  return (
    <span className="digest-ai-badge digest-ai-badge--fallback">
      Deterministic
    </span>
  )
}

function DigestSection({ title, icon, bullets, variant }) {
  if (!bullets || bullets.length === 0) return null
  return (
    <div className={`digest-section digest-section--${variant || 'default'}`}>
      <h3 className="digest-section-title">
        <span className="digest-section-icon">{icon}</span>
        {title}
      </h3>
      <ul className="digest-bullets">
        {bullets.map((b, i) => (
          <li key={i} className="digest-bullet">{typeof b === 'string' ? b : JSON.stringify(b)}</li>
        ))}
      </ul>
    </div>
  )
}

function DrillDownCards({ links, whereToLook }) {
  // Merge AI-suggested links with standard links
  const allLinks = []

  // AI-suggested "where to look" first
  if (whereToLook && whereToLook.length > 0) {
    whereToLook.forEach((item) => {
      if (item && item.route && item.label) {
        allLinks.push({ to: item.route, label: item.label })
      }
    })
  }

  // Standard links as fallback / supplement
  if (links) {
    const standardLinks = [
      { key: 'signals', label: 'Signals Explorer' },
      { key: 'training', label: 'Training Status' },
      { key: 'portfolio', label: 'Portfolio' },
      { key: 'brief', label: 'Morning Brief' },
      { key: 'market_timeline', label: 'Market Timeline' },
      { key: 'runs', label: 'Audit Log' },
    ]
    standardLinks.forEach(({ key, label }) => {
      if (links[key] && !allLinks.find((l) => l.to === links[key])) {
        allLinks.push({ to: links[key], label })
      }
    })
  }

  if (allLinks.length === 0) return null

  return (
    <div className="digest-section">
      <h3 className="digest-section-title">
        <span className="digest-section-icon">&#x1F517;</span>
        Drill Down
      </h3>
      <div className="digest-links-grid">
        {allLinks.map((link, i) => (
          <Link key={i} to={link.to} className="digest-link-card">
            {link.label}
            <span className="digest-link-arrow">&rarr;</span>
          </Link>
        ))}
      </div>
    </div>
  )
}

function DetectorPills({ detectors }) {
  if (!detectors || detectors.length === 0) return null
  const fired = detectors.filter((d) => d.fired)
  if (fired.length === 0) return null

  return (
    <div className="digest-detectors">
      {fired.map((d, i) => {
        const sev = (d.severity || 'low').toLowerCase()
        return (
          <span
            key={i}
            className={`digest-detector-pill digest-detector-pill--${sev}`}
            title={JSON.stringify(d.detail, null, 2)}
          >
            {(d.detector || '').replace(/_/g, ' ')}
          </span>
        )
      })}
    </div>
  )
}

function SnapshotFacts({ snapshot }) {
  if (!snapshot) return null
  return (
    <details className="digest-facts-toggle">
      <summary>Show snapshot facts</summary>
      <div className="digest-facts-content">
        {JSON.stringify(snapshot, null, 2)}
      </div>
    </details>
  )
}

/* ── Main Page ───────────────────────────────────────────────── */

export default function Digest() {
  const { portfolioId: routePortfolioId } = useParams()
  const { portfolios } = usePortfolios()
  const [selectedPortfolioId, setSelectedPortfolioId] = useState(
    routePortfolioId ? Number(routePortfolioId) : null
  )
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Default to first portfolio if none selected
  useEffect(() => {
    if (!selectedPortfolioId && portfolios && portfolios.length > 0) {
      setSelectedPortfolioId(portfolios[0].portfolio_id)
    }
  }, [portfolios, selectedPortfolioId])

  // Override from route param
  useEffect(() => {
    if (routePortfolioId) {
      setSelectedPortfolioId(Number(routePortfolioId))
    }
  }, [routePortfolioId])

  // Fetch digest
  useEffect(() => {
    if (!selectedPortfolioId) return

    let cancelled = false
    setLoading(true)
    setError(null)

    fetch(`${API_BASE}/digest/latest?portfolio_id=${selectedPortfolioId}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((json) => {
        if (!cancelled) {
          setData(json)
          setLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.message)
          setLoading(false)
        }
      })

    return () => { cancelled = true }
  }, [selectedPortfolioId])

  /* ── Render ──────────────────────────────────────────────── */

  const narrative = data?.narrative || {}
  const snapshot = data?.snapshot || {}

  return (
    <div className="digest-page">
      <h1>Daily Intelligence Digest</h1>
      <p className="page-description">
        AI-generated narrative synthesising deterministic MIP facts into a daily story.
      </p>

      {/* Portfolio selector */}
      {portfolios && portfolios.length > 1 && (
        <div className="digest-controls">
          <label>
            Portfolio:
            <select
              value={selectedPortfolioId || ''}
              onChange={(e) => setSelectedPortfolioId(Number(e.target.value))}
            >
              {portfolios.map((p) => (
                <option key={p.portfolio_id} value={p.portfolio_id}>
                  {p.name || `Portfolio ${p.portfolio_id}`}
                </option>
              ))}
            </select>
          </label>
        </div>
      )}

      {loading && <LoadingState message="Loading digest..." />}
      {error && <ErrorState message={error} />}

      {!loading && !error && !data?.found && (
        <EmptyState
          message="No digest available yet."
          detail="Run the daily pipeline to generate your first Daily Intelligence Digest."
        />
      )}

      {!loading && !error && data?.found && (
        <>
          {/* Header: headline + meta */}
          <div className="digest-header">
            <h2 className="digest-headline">
              {narrative.headline || 'Daily digest available'}
            </h2>
            <div className="digest-meta">
              <span className="digest-meta-item">
                As of {formatTs(data.as_of_ts)}
              </span>
              <FreshnessBadge createdAt={data.snapshot_created_at} />
              <AiBadge isAi={data.is_ai_narrative} modelInfo={data.model_info} />
            </div>

            {/* Fired detectors */}
            <DetectorPills detectors={snapshot.detectors} />

            {/* Fallback banner */}
            {!data.is_ai_narrative && (
              <div className="digest-fallback-banner">
                No AI narrative available; showing deterministic summary from snapshot facts.
              </div>
            )}
          </div>

          {/* Bullet sections */}
          <DigestSection
            title="What Changed"
            icon="&#x1F504;"
            bullets={narrative.what_changed}
            variant="changed"
          />

          <DigestSection
            title="What Matters"
            icon="&#x26A0;&#xFE0F;"
            bullets={narrative.what_matters}
            variant="matters"
          />

          <DigestSection
            title="Waiting For"
            icon="&#x23F3;"
            bullets={narrative.waiting_for}
            variant="waiting"
          />

          {/* Drill-down cards */}
          <DrillDownCards
            links={data.links}
            whereToLook={narrative.where_to_look}
          />

          {/* Show facts toggle */}
          <SnapshotFacts snapshot={snapshot} />
        </>
      )}
    </div>
  )
}
