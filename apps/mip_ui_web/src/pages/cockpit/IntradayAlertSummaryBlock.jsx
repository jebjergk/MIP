/**
 * Compact intraday alert summary.
 *
 * Renders the top-level overlay_status badge (OK / PARTIAL /
 * UNAVAILABLE / MARKET_CLOSED) plus counts of HOLD / WATCH_NOW /
 * REVIEW_NOW / SELL_NOW. Critically: when overlay_status is
 * UNAVAILABLE or MARKET_CLOSED we render the badge prominently so
 * operators don't mistake "0 alerts" for "all clear".
 */
function statusTone(s) {
  const v = String(s || '').toUpperCase()
  if (v === 'OK') return 'ok'
  if (v === 'PARTIAL') return 'info'
  if (v === 'MARKET_CLOSED') return 'neutral'
  return 'warn'
}

function statusLabel(s) {
  const v = String(s || '').toUpperCase()
  if (v === 'OK') return 'Live overlay OK'
  if (v === 'PARTIAL') return 'Live overlay partial'
  if (v === 'UNAVAILABLE') return 'Live overlay unavailable'
  if (v === 'MARKET_CLOSED') return 'Market closed'
  return v || 'Unknown'
}

function formatAge(tsIso) {
  if (!tsIso) return '\u2014'
  try {
    const ts = new Date(String(tsIso).endsWith('Z') ? tsIso : `${tsIso}Z`)
    const mins = Math.round((Date.now() - ts.getTime()) / 60000)
    if (mins < 1) return 'just now'
    if (mins < 60) return `${mins}m ago`
    const hours = Math.round(mins / 60)
    return `${hours}h ago`
  } catch {
    return '\u2014'
  }
}

export default function IntradayAlertSummaryBlock({ data }) {
  if (!data) return null
  const tone = statusTone(data.overlay_status)
  const label = statusLabel(data.overlay_status)
  const noLive = (data.no_live_check_count || 0) + (data.market_closed_count || 0)

  return (
    <div className="ck-co-card ck-co-card--intraday">
      <div className="ck-co-card-header">
        <h2 className="ck-co-card-title">Intraday Alerts</h2>
        <span className={`ck-co-pulse-tag ck-co-pulse-tag--${tone}`}>{label}</span>
      </div>
      <div className="ck-co-intraday-grid">
        <div className="ck-co-intraday-cell ck-co-intraday-cell--critical">
          <div className="ck-co-kpi-value">{data.sell_now_count || 0}</div>
          <div className="ck-co-kpi-label">Sell now</div>
        </div>
        <div className="ck-co-intraday-cell ck-co-intraday-cell--warning">
          <div className="ck-co-kpi-value">{data.review_now_count || 0}</div>
          <div className="ck-co-kpi-label">Review now</div>
        </div>
        <div className="ck-co-intraday-cell ck-co-intraday-cell--info">
          <div className="ck-co-kpi-value">{data.watch_now_count || 0}</div>
          <div className="ck-co-kpi-label">Watch</div>
        </div>
        <div className="ck-co-intraday-cell">
          <div className="ck-co-kpi-value">{data.hold_count || 0}</div>
          <div className="ck-co-kpi-label">Hold</div>
        </div>
        <div className="ck-co-intraday-cell ck-co-intraday-cell--neutral">
          <div className="ck-co-kpi-value">{noLive}</div>
          <div className="ck-co-kpi-label">No live check</div>
        </div>
      </div>
      <div className="ck-co-card-footer">
        <span className="ck-co-card-sub">Last evaluation {formatAge(data.latest_eval_ts)}</span>
        {data.note ? (
          <span className="ck-co-card-sub ck-co-card-sub--warn"> · {data.note}</span>
        ) : null}
      </div>
    </div>
  )
}
