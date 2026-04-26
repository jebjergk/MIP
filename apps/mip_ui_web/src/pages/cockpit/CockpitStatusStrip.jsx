/**
 * Top status strip for the cockpit.
 *
 * Operator confidence at a glance: broker connectivity, market-data
 * freshness, daily pipeline, intraday overlay, optional shadow run
 * summary. Compact by design — never wraps onto multiple visual rows
 * unless the viewport is very narrow.
 */
import { Link } from 'react-router-dom'

function formatAge(tsIso) {
  if (!tsIso) return '\u2014'
  try {
    const ts = new Date(String(tsIso).endsWith('Z') ? tsIso : `${tsIso}Z`)
    const mins = Math.round((Date.now() - ts.getTime()) / 60000)
    if (!Number.isFinite(mins)) return '\u2014'
    if (mins < 1) return 'just now'
    if (mins < 60) return `${mins}m ago`
    const hours = Math.round(mins / 60)
    if (hours < 48) return `${hours}h ago`
    return `${Math.round(hours / 24)}d ago`
  } catch {
    return '\u2014'
  }
}

function StatusPill({ tone, label, sub }) {
  return (
    <div className={`ck-co-pill ck-co-pill--${tone}`}>
      <div className="ck-co-pill-label">{label}</div>
      {sub ? <div className="ck-co-pill-sub">{sub}</div> : null}
    </div>
  )
}

function brokerTone(connected) {
  return connected ? 'ok' : 'warn'
}

function marketTone(fresh) {
  return fresh ? 'ok' : 'warn'
}

function pipelineTone(status) {
  const s = String(status || '').toUpperCase()
  if (s === 'SUCCESS' || s === 'OK' || s === 'COMPLETED') return 'ok'
  if (s === 'INFO' || s === 'RUNNING') return 'info'
  if (!s) return 'neutral'
  return 'warn'
}

function overlayTone(overlayStatus) {
  const s = String(overlayStatus || '').toUpperCase()
  if (s === 'OK') return 'ok'
  if (s === 'PARTIAL') return 'info'
  if (s === 'MARKET_CLOSED') return 'neutral'
  return 'warn' // UNAVAILABLE
}

export default function CockpitStatusStrip({ status }) {
  if (!status) return null
  const shadowSummary = status.shadow_run_status_summary || {}
  const shadowSubParts = []
  for (const key of ['SUCCESS', 'PENDING', 'RUNNING', 'FAILED', 'MISSING']) {
    const n = Number(shadowSummary[key] || 0)
    if (n > 0) shadowSubParts.push(`${n} ${key.toLowerCase()}`)
  }
  const shadowSub = shadowSubParts.length ? shadowSubParts.join(' · ') : 'no rows'

  return (
    <div className="ck-co-status-strip">
      <StatusPill
        tone={brokerTone(status.broker_connected)}
        label={status.broker_connected ? 'Broker connected' : 'Broker offline'}
        sub={`Snapshot ${formatAge(status.latest_broker_snapshot_ts)}`}
      />
      <StatusPill
        tone={marketTone(status.market_data_fresh)}
        label={status.market_data_fresh ? 'Market data fresh' : 'Market data stale'}
        sub={`Last bar ${formatAge(status.latest_market_data_ts)}`}
      />
      <StatusPill
        tone={pipelineTone(status.daily_pipeline_status)}
        label={`Daily: ${status.daily_pipeline_status || 'unknown'}`}
        sub={`Run ${formatAge(status.latest_daily_run_ts)}`}
      />
      <StatusPill
        tone={overlayTone(status.intraday_overlay_status)}
        label={`Intraday: ${status.intraday_overlay_status || 'unknown'}`}
        sub={`Eval ${formatAge(status.latest_intraday_eval_ts)}`}
      />
      <StatusPill
        tone="info"
        label="Shadow runs"
        sub={shadowSub}
      />
      {status.note ? (
        <div className="ck-co-status-note">{status.note}</div>
      ) : null}
      <div className="ck-co-status-spacer" />
      <Link to="/live-portfolio-activity" className="ck-co-status-link">
        Live Portfolio &rarr;
      </Link>
    </div>
  )
}
