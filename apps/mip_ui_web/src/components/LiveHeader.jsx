import { useState, useEffect, useCallback } from 'react'
import { API_BASE } from '../App'
import InfoTooltip from './InfoTooltip'
import { useDefaultPortfolioId } from '../context/PortfolioContext'
import './LiveHeader.css'

const IBKR_LOGO_SRC = '/ibkr-logo.png'

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

function resolveLiveBroker(configPayload, defaultPortfolioId) {
  const configs = Array.isArray(configPayload?.configs) ? configPayload.configs : []
  if (configs.length === 0) return { label: 'Unknown', account: null }

  const target = defaultPortfolioId != null
    ? configs.find((c) => Number(c?.PORTFOLIO_ID) === Number(defaultPortfolioId))
    : configs.find((c) => Boolean(c?.IS_ACTIVE)) || configs[0]
  const cfg = target || configs[0]
  const account = cfg?.IBKR_ACCOUNT_ID || null
  if (account) {
    return { label: 'Interactive Brokers', short: 'IBKR', account: String(account) }
  }
  return { label: 'Unknown', short: 'N/A', account: null }
}

export default function LiveHeader() {
  const defaultPortfolioId = useDefaultPortfolioId()
  const [metrics, setMetrics] = useState(null)
  const [liveBroker, setLiveBroker] = useState({ label: 'Unknown', short: 'N/A', account: null })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [, setTick] = useState(0)

  const fetchMetrics = useCallback(() => {
    const url = defaultPortfolioId != null
      ? `${API_BASE}/live/metrics?portfolio_id=${defaultPortfolioId}`
      : `${API_BASE}/live/metrics`
    const cfgUrl = `${API_BASE}/live/portfolio-config`
    Promise.all([
      fetch(url).then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText)))),
      fetch(cfgUrl).then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText)))).catch(() => null),
    ])
      .then(([metricsData, cfgData]) => {
        setMetrics(metricsData)
        if (cfgData) {
          setLiveBroker(resolveLiveBroker(cfgData, defaultPortfolioId))
        }
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
  const outcomes = metrics?.outcomes ?? {}
  const sinceLastRun = outcomes.since_last_run ?? 0
  const lastCalculatedAt = outcomes.last_calculated_at ?? null
  const lastRunAt = lastRun?.completed_at ?? lastRun?.started_at ?? null

  return (
    <div className="live-header" role="region" aria-label="Live metrics">
      <span className="live-header-label">Live</span>
      <span className="live-header-item live-header-item--broker" title={liveBroker?.account ? `Account ${liveBroker.account}` : undefined}>
        Live Broker
        {liveBroker?.short === 'IBKR'
          ? (
            <img
              src={IBKR_LOGO_SRC}
              alt="Interactive Brokers"
              className="live-broker-logo-image"
              loading="lazy"
            />
          )
          : <span className="live-broker-fallback">{liveBroker?.short || 'N/A'}</span>}
        {liveBroker?.account ? <span className="live-header-meta">{liveBroker.account}</span> : null}
      </span>

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
