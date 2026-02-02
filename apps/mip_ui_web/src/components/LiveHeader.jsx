import { useState, useEffect, useCallback } from 'react'
import { API_BASE } from '../App'
import InfoTooltip from './InfoTooltip'
import { useExplainMode } from '../context/ExplainModeContext'
import './LiveHeader.css'

const DEFAULT_PORTFOLIO_ID = 1
const POLL_INTERVAL_MS = 60_000

/** Human-readable relative time (e.g. "2 min ago", "1 hour ago"). */
function relativeTime(isoOrDate) {
  if (isoOrDate == null) return '—'
  const date = typeof isoOrDate === 'string' ? new Date(isoOrDate) : isoOrDate
  if (Number.isNaN(date.getTime())) return '—'
  const now = Date.now()
  const sec = Math.floor((now - date.getTime()) / 1000)
  if (sec < 0) return 'just now'
  if (sec < 60) return `${sec} sec ago`
  const min = Math.floor(sec / 60)
  if (min < 60) return `${min} min ago`
  const hr = Math.floor(min / 60)
  if (hr < 24) return `${hr} hour${hr !== 1 ? 's' : ''} ago`
  const day = Math.floor(hr / 24)
  return `${day} day${day !== 1 ? 's' : ''} ago`
}

export default function LiveHeader() {
  const { explainMode } = useExplainMode()
  const [metrics, setMetrics] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [lastFetchedAt, setLastFetchedAt] = useState(null)
  const [tick, setTick] = useState(0)

  const fetchMetrics = useCallback(() => {
    fetch(`${API_BASE}/live/metrics?portfolio_id=${DEFAULT_PORTFOLIO_ID}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((data) => {
        setMetrics(data)
        setLastFetchedAt(Date.now())
        setError(null)
      })
      .catch((e) => {
        setError(e.message)
        setMetrics(null)
      })
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => {
    fetchMetrics()
    const interval = setInterval(fetchMetrics, POLL_INTERVAL_MS)
    return () => clearInterval(interval)
  }, [fetchMetrics])

  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 1000)
    return () => clearInterval(id)
  }, [])

  const displaySeconds = lastFetchedAt != null ? Math.max(0, Math.floor((Date.now() - lastFetchedAt) / 1000)) : null

  if (loading && !metrics) {
    return (
      <div className="live-header" role="status" aria-label="Live metrics loading">
        <span className="live-header-label">Live</span>
        <span className="live-header-value">Loading…</span>
      </div>
    )
  }

  const lastRun = metrics?.last_run
  const lastBrief = metrics?.last_brief
  const outcomes = metrics?.outcomes ?? {}
  const sinceLastRun = outcomes.since_last_run ?? 0
  const lastCalculatedAt = outcomes.last_calculated_at ?? null

  return (
    <div className="live-header" role="region" aria-label="Live metrics">
      <span className="live-header-label">Live</span>

      {lastRun && (
        <span className="live-header-item" title={explainMode ? undefined : null}>
          Last pipeline run: {relativeTime(lastRun.completed_at ?? lastRun.started_at)}
          <span className={`live-header-badge live-header-badge--${(lastRun.status || '').toLowerCase()}`}>
            {lastRun.status ?? '—'}
          </span>
          <InfoTooltip scope="live" entryKey="last_pipeline_run" variant="short" />
        </span>
      )}

      {lastBrief?.found && (
        <span className="live-header-item" title={explainMode ? undefined : null}>
          Latest brief: {relativeTime(lastBrief.as_of_ts)}
          <InfoTooltip scope="live" entryKey="latest_brief" variant="short" />
        </span>
      )}

      {sinceLastRun > 0 && (
        <span className="live-header-item live-header-item--pulse" title={explainMode ? undefined : null}>
          New evaluations since last run: +{sinceLastRun}
          <InfoTooltip scope="live" entryKey="new_evaluations_since_last_run" variant="short" />
        </span>
      )}

      {lastCalculatedAt && (
        <span className="live-header-item" title={explainMode ? undefined : null}>
          Data freshness: outcomes updated {relativeTime(lastCalculatedAt)} ago
          <InfoTooltip scope="live" entryKey="data_freshness" variant="short" />
        </span>
      )}

      {displaySeconds != null && (
        <span className="live-header-item live-header-meta">
          Updated {displaySeconds} sec ago
        </span>
      )}

      {error && (
        <span className="live-header-error" role="alert">
          Live metrics unavailable
        </span>
      )}
    </div>
  )
}

export { relativeTime }
