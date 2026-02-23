import { useState, useEffect, useCallback } from 'react'
import { API_BASE } from '../App'
import InfoTooltip from './InfoTooltip'
import { useDefaultPortfolioId } from '../context/PortfolioContext'
import useVisibleInterval from '../hooks/useVisibleInterval'
import './LiveHeader.css'

const POLL_INTERVAL_MS = 900_000

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
  const [lastFetchedAt, setLastFetchedAt] = useState(null)
  const [tick, setTick] = useState(0)

  const fetchMetrics = useCallback(() => {
    fetch(`${API_BASE}/live/metrics?portfolio_id=${defaultPortfolioId}`)
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
  }, [defaultPortfolioId])

  // Poll Snowflake only while the tab is visible (prevents overnight warehouse spin)
  useVisibleInterval(fetchMetrics, POLL_INTERVAL_MS)

  // 1-second UI tick for relative time display (pure client-side, no cost)
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
  const lastIntradayRun = metrics?.last_intraday_run
  const lastBrief = metrics?.last_brief
  const outcomes = metrics?.outcomes ?? {}
  const sinceLastRun = outcomes.since_last_run ?? 0
  const lastCalculatedAt = outcomes.last_calculated_at ?? null

  return (
    <div className="live-header" role="region" aria-label="Live metrics">
      <span className="live-header-label">Live</span>

      {lastRun && (
        <span className="live-header-item" title={undefined}>
          Daily: {relativeTime(lastRun.completed_at ?? lastRun.started_at)}
          <span className={`live-header-badge live-header-badge--${(lastRun.status || '').toLowerCase()}`}>
            {lastRun.status ?? '—'}
          </span>
          <InfoTooltip scope="live" entryKey="last_pipeline_run" variant="short" />
        </span>
      )}

      {lastIntradayRun && (
        <span className="live-header-item" title={`${lastIntradayRun.bars_ingested ?? 0} bars, ${lastIntradayRun.signals_generated ?? 0} signals, ${lastIntradayRun.symbols_processed ?? 0} symbols`}>
          Intraday: {relativeTime(lastIntradayRun.completed_at ?? lastIntradayRun.started_at)}
          <span className={`live-header-badge live-header-badge--${(lastIntradayRun.status || '').toLowerCase()}`}>
            {lastIntradayRun.status ?? '—'}
          </span>
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
          Data freshness: outcomes updated {relativeTime(lastCalculatedAt)} ago
          <InfoTooltip scope="live" entryKey="data_freshness" variant="short" />
        </span>
      )}

      {displaySeconds != null && (
        <span className="live-header-item live-header-meta">
          Next refresh in {Math.max(0, Math.floor(POLL_INTERVAL_MS / 1000) - displaySeconds)} sec
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
