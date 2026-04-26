/**
 * Compact Market Pulse block for the cockpit (STOCK-only).
 *
 * Pure embedded context — no "Open Market Pulse" CTA. The cockpit is
 * the operator's dashboard; market pulse exists here to inform
 * judgment, not to launch into a separate page.
 */

function formatPct(val, decimals = 1) {
  if (val == null) return '\u2014'
  const n = Number(val)
  if (!Number.isFinite(n)) return '\u2014'
  return `${n >= 0 ? '+' : ''}${n.toFixed(decimals)}%`
}

function toneClass(label) {
  const s = String(label || '').toLowerCase()
  if (s === 'supportive') return 'ok'
  if (s === 'weak') return 'warn'
  if (s === 'no data') return 'neutral'
  return 'info'
}

export default function MarketPulseCompact({ data }) {
  if (!data || !data.available) {
    return (
      <div className="ck-co-card ck-co-card--market">
        <h2 className="ck-co-card-title">Market Pulse (STOCK)</h2>
        <p className="ck-co-empty">
          {data?.error ? `Market data unavailable: ${data.error}` : 'No market data yet.'}
        </p>
      </div>
    )
  }
  const breadthDenom = Number(data.breadth_total || 0)
  const breadthNumer = Number(data.breadth_up || 0)
  const breadthLabel = breadthDenom > 0
    ? `${breadthNumer} of ${breadthDenom} up`
    : '\u2014'
  const tone = toneClass(data.pulse_label)

  return (
    <div className="ck-co-card ck-co-card--market">
      <div className="ck-co-card-header">
        <h2 className="ck-co-card-title">Market Pulse (STOCK)</h2>
        <span className={`ck-co-pulse-tag ck-co-pulse-tag--${tone}`}>
          {data.pulse_label || 'Mixed'}
        </span>
      </div>
      <div className="ck-co-market-row">
        <div className="ck-co-market-cell">
          <div className="ck-co-kpi-label">Breadth</div>
          <div className="ck-co-kpi-value">{breadthLabel}</div>
        </div>
        <div className="ck-co-market-cell">
          <div className="ck-co-kpi-label">Avg return</div>
          <div className="ck-co-kpi-value">{formatPct(data.avg_return_pct, 2)}</div>
        </div>
        <div className="ck-co-market-cell">
          <div className="ck-co-kpi-label">Top</div>
          <div className="ck-co-kpi-value">
            {data.top_symbol || '\u2014'}{' '}
            <span className="ck-co-positive">{formatPct(data.top_return_pct, 1)}</span>
          </div>
        </div>
        <div className="ck-co-market-cell">
          <div className="ck-co-kpi-label">Bottom</div>
          <div className="ck-co-kpi-value">
            {data.bottom_symbol || '\u2014'}{' '}
            <span className="ck-co-negative">{formatPct(data.bottom_return_pct, 1)}</span>
          </div>
        </div>
      </div>
    </div>
  )
}
