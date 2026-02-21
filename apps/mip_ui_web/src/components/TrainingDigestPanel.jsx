import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../App'
import './TrainingDigestPanel.css'

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

/* ── Sub-components ──────────────────────────────────── */

function AiBadge({ isAi, modelInfo }) {
  return isAi ? (
    <span className="td-ai-badge td-ai-badge--ai" title={`Model: ${modelInfo}`}>Cortex AI</span>
  ) : (
    <span className="td-ai-badge td-ai-badge--fallback">Deterministic</span>
  )
}

function JourneySteps({ journey }) {
  if (!journey || journey.length === 0) return null
  return (
    <div className="td-journey">
      {journey.map((step, i) => {
        const isCurrent = typeof step === 'string' && step.startsWith('>>')
        const label = isCurrent ? step.replace(/^>>\s*/, '') : step
        return (
          <span key={i} className={`td-journey-step ${isCurrent ? 'td-journey-step--current' : ''}`}>
            {isCurrent && <span className="td-journey-marker" />}
            {label}
            {i < journey.length - 1 && <span className="td-journey-arrow">&rarr;</span>}
          </span>
        )
      })}
    </div>
  )
}

function DigestSection({ title, icon, bullets, variant }) {
  if (!bullets || bullets.length === 0) return null
  return (
    <div className={`td-section td-section--${variant || 'default'}`}>
      <h4 className="td-section-title">
        <span className="td-section-icon">{icon}</span>
        {title}
      </h4>
      <ul className="td-bullets">
        {bullets.map((b, i) => (
          <li key={i} className="td-bullet">{typeof b === 'string' ? b : JSON.stringify(b)}</li>
        ))}
      </ul>
    </div>
  )
}

function DrillDownLinks({ links, whereToLook }) {
  const allLinks = []
  if (whereToLook && whereToLook.length > 0) {
    whereToLook.forEach((item) => {
      if (item?.route && item?.label) {
        allLinks.push({ to: item.route, label: item.label })
      }
    })
  }
  if (links) {
    const standardLinks = [
      { key: 'training', label: 'Training Status' },
      { key: 'signals', label: 'Decision Explorer' },
      { key: 'symbol_training', label: 'View Symbol' },
      { key: 'digest', label: 'Daily Digest' },
    ]
    standardLinks.forEach(({ key, label }) => {
      if (links[key] && !allLinks.find((l) => l.to === links[key])) {
        allLinks.push({ to: links[key], label })
      }
    })
  }
  if (allLinks.length === 0) return null
  return (
    <div className="td-links">
      {allLinks.map((link, i) => (
        <Link key={i} to={link.to} className="td-link">{link.label} &rarr;</Link>
      ))}
    </div>
  )
}

function DetectorPills({ detectors }) {
  if (!detectors || detectors.length === 0) return null
  const fired = detectors.filter((d) => d.fired)
  if (fired.length === 0) return null
  return (
    <div className="td-detectors">
      {fired.map((d, i) => {
        const sev = (d.severity || 'low').toLowerCase()
        return (
          <span key={i}
            className={`td-detector-pill td-detector-pill--${sev}`}
            title={JSON.stringify(d.detail, null, 2)}
          >
            {(d.detector || '').replace(/_/g, ' ')}
          </span>
        )
      })}
    </div>
  )
}

/* ── Main Panel ──────────────────────────────────────── */

/**
 * TrainingDigestPanel — reusable panel for global or per-symbol training digest.
 * Props:
 *   scope: 'global' | 'symbol'
 *   symbol: string (for symbol scope)
 *   marketType: string (for symbol scope)
 *   patternId: number (for symbol scope — identifies the specific pattern)
 *   compact: boolean (for inside expanders)
 */
export default function TrainingDigestPanel({ scope = 'global', symbol, marketType, patternId, compact = false }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [showFacts, setShowFacts] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    let url
    if (scope === 'symbol' && symbol && marketType) {
      url = `${API_BASE}/training/digest/symbol/latest?symbol=${encodeURIComponent(symbol)}&market_type=${encodeURIComponent(marketType)}`
      if (patternId != null) {
        url += `&pattern_id=${encodeURIComponent(patternId)}`
      }
    } else {
      url = `${API_BASE}/training/digest/latest`
    }

    fetch(url)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((json) => { if (!cancelled) { setData(json); setLoading(false) } })
      .catch((err) => { if (!cancelled) { setError(err.message); setLoading(false) } })

    return () => { cancelled = true }
  }, [scope, symbol, marketType, patternId])

  if (loading) {
    return <div className={`td-panel ${compact ? 'td-panel--compact' : ''}`}>
      <div className="td-loading">Loading training digest...</div>
    </div>
  }

  if (error) {
    return <div className={`td-panel ${compact ? 'td-panel--compact' : ''}`}>
      <div className="td-error">Training digest unavailable</div>
    </div>
  }

  if (!data?.found) {
    return <div className={`td-panel ${compact ? 'td-panel--compact' : ''}`}>
      <div className="td-empty">No training digest yet. Run the pipeline to generate one.</div>
    </div>
  }

  const narrative = data.narrative || {}
  const snapshot = data.snapshot || {}

  return (
    <div className={`td-panel ${compact ? 'td-panel--compact' : ''}`}>
      {/* Header */}
      <div className="td-header">
        <h3 className="td-headline">{narrative.headline || 'Training digest available'}</h3>
        <div className="td-meta">
          <span className="td-meta-item">As of {formatTs(data.as_of_ts)}</span>
          <AiBadge isAi={data.is_ai_narrative} modelInfo={data.model_info} />
        </div>
        <DetectorPills detectors={snapshot.detectors} />
      </div>

      {/* Journey steps */}
      <JourneySteps journey={narrative.journey} />

      {/* Bullet sections */}
      <DigestSection title="What Changed" icon="&#x1F504;" bullets={narrative.what_changed} variant="changed" />
      <DigestSection title="What Matters" icon="&#x26A0;&#xFE0F;" bullets={narrative.what_matters} variant="matters" />
      <DigestSection title="Waiting For" icon="&#x23F3;" bullets={narrative.waiting_for} variant="waiting" />

      {/* Drill-down links */}
      <DrillDownLinks links={data.links} whereToLook={narrative.where_to_look} />

      {/* Show facts toggle */}
      {!compact && (
        <details className="td-facts-toggle">
          <summary>Show training snapshot facts</summary>
          <div className="td-facts-content">{JSON.stringify(snapshot, null, 2)}</div>
        </details>
      )}
    </div>
  )
}
