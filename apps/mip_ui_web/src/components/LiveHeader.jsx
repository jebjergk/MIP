import { useState, useEffect, useCallback } from 'react'
import { API_BASE } from '../App'
import InfoTooltip from './InfoTooltip'
import { useDefaultPortfolioId } from '../context/PortfolioContext'
import './LiveHeader.css'

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
  const defaultPortfolioId = useDefaultPortfolioId()
  const [metrics, setMetrics] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [, setTick] = useState(0)

  const fetchMetrics = useCallback(() => {
    const url = defaultPortfolioId != null
      ? `${API_BASE}/live/metrics?portfolio_id=${defaultPortfolioId}`
      : `${API_BASE}/live/metrics`
    fetch(url)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((data) => {
        setMetrics(data)
        setError(null)
      })
      .catch((e) => {
        setError(e.message)
        setMetrics(null)
      })
      .finally(() => setLoading(false))
  }, [defaultPortfolioId])

  // Manual-refresh-first behavior: load once on mount/portfolio change.
  useEffect(() => {
    fetchMetrics()
  }, [fetchMetrics])

  // 1-minute UI tick for relative time display (pure client-side, no backend calls)
  useEffect(() => {
    const id = setInterval(() => setTick((v) => v + 1), 60000)
    return () => clearInterval(id)
  }, [])

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
  const lastRunAt = lastRun?.completed_at ?? lastRun?.started_at ?? null

  return (
    <div className="live-header" role="region" aria-label="Live metrics">
      <span className="live-header-label">Live</span>

      {lastRun && (
        <span className="live-header-item" title={lastRunAt ? `Completed ${new Date(lastRunAt).toLocaleString()}` : undefined}>
          Daily pipeline
          <span className={`live-header-badge live-header-badge--${(lastRun.status || '').toLowerCase()}`}>
            {lastRun.status ?? '—'}
          </span>
          {!lastCalculatedAt && lastRunAt ? <span> {relativeTime(lastRunAt)}</span> : null}
          <InfoTooltip scope="live" entryKey="last_pipeline_run" variant="short" />
        </span>
      )}

      {lastBrief?.found && (
        <span className="live-header-item" title={undefined}>
          Latest digest: {relativeTime(lastBrief.as_of_ts)}
          <InfoTooltip scope="live" entryKey="latest_brief" variant="short" />
        </span>
      )}

      {sinceLastRun > 0 && (
        <span className="live-header-item live-header-item--pulse" title={undefined}>
          New evaluations since last run: +{sinceLastRun}
          <InfoTooltip scope="live" entryKey="new_evaluations_since_last_run" variant="short" />
        </span>
      )}

      {lastCalculatedAt && (
        <span className="live-header-item" title={undefined}>
          Data freshness: outcomes updated {relativeTime(lastCalculatedAt)}
          <InfoTooltip scope="live" entryKey="data_freshness" variant="short" />
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
