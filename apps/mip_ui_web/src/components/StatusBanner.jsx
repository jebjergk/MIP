import { useState, useEffect } from 'react'
import { API_BASE } from '../App'
import InfoTooltip from './InfoTooltip'
import './StatusBanner.css'

const STATUS = {
  OK: 'ok',           // green: API + Snowflake OK
  DEGRADED: 'degraded',  // yellow: API OK, Snowflake not reachable
  DOWN: 'down',       // red: API not reachable
  LOADING: 'loading',
}

export default function StatusBanner() {
  const [status, setStatus] = useState(STATUS.LOADING)
  const [snowflakeMessage, setSnowflakeMessage] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch(`${API_BASE}/status`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((data) => {
        if (cancelled) return
        setSnowflakeMessage(data.snowflake_message ?? null)
        if (data.api_ok && data.snowflake_ok) {
          setStatus(STATUS.OK)
        } else if (data.api_ok && !data.snowflake_ok) {
          setStatus(STATUS.DEGRADED)
        } else {
          setStatus(STATUS.DEGRADED)
        }
      })
      .catch(() => {
        if (!cancelled) setStatus(STATUS.DOWN)
      })
    return () => { cancelled = true }
  }, [])

  const degradedLabel = snowflakeMessage ?? 'Data backend not reachable'
  const label =
    status === STATUS.OK
      ? 'All systems OK'
      : status === STATUS.DEGRADED
        ? degradedLabel
        : status === STATUS.DOWN
          ? 'API not reachable'
          : null

  if (!label) return null

  return (
    <span
      className={`status-banner status-banner--${status}`}
      role="status"
      aria-live="polite"
      aria-label={label}
    >
      <span className="status-banner-dot" aria-hidden="true" />
      <span className="status-banner-label">{label}</span>
      <InfoTooltip scope="ui" entryKey="system_status" variant="short" />
    </span>
  )
}
