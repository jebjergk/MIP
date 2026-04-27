/**
 * Single canonical Live Portfolio Overview block.
 *
 * Replaces multiple overlapping portfolio cards (live snapshot card,
 * IBKR positions expander summary, etc.) with one source of truth
 * fed from MIP.MART.V_LIVE_OPEN_POSITIONS + MIP.LIVE.BROKER_SNAPSHOTS
 * NAV row.
 */
import { Link } from 'react-router-dom'

import TradeProposalsPanel from './TradeProposalsPanel'

function formatMoney(val) {
  if (val == null) return '\u2014'
  const n = Number(val)
  if (!Number.isFinite(n)) return '\u2014'
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(n)
}

function formatPct(val) {
  if (val == null) return '\u2014'
  const n = Number(val)
  if (!Number.isFinite(n)) return '\u2014'
  return `${(n * 100).toFixed(1)}%`
}

function formatAge(tsIso) {
  if (!tsIso) return '\u2014'
  try {
    const ts = new Date(String(tsIso).endsWith('Z') ? tsIso : `${tsIso}Z`)
    const mins = Math.round((Date.now() - ts.getTime()) / 60000)
    if (mins < 1) return 'just now'
    if (mins < 60) return `${mins}m ago`
    const hours = Math.round(mins / 60)
    if (hours < 48) return `${hours}h ago`
    return `${Math.round(hours / 24)}d ago`
  } catch {
    return '\u2014'
  }
}

export default function LivePortfolioOverviewCard({ data, proposals }) {
  if (!data || data.portfolio_id == null) {
    return (
      <div className="ck-co-card ck-co-card--portfolio">
        <h2 className="ck-co-card-title">Live Portfolio</h2>
        <p className="ck-co-empty">No active live portfolio configured.</p>
      </div>
    )
  }
  return (
    <div className="ck-co-card ck-co-card--portfolio">
      <div className="ck-co-card-header">
        <h2 className="ck-co-card-title">Live Portfolio #{data.portfolio_id}</h2>
        <span className="ck-co-card-sub">Snapshot {formatAge(data.freshness_ts)}</span>
      </div>
      <div className="ck-co-kpi-grid">
        <div className="ck-co-kpi">
          <div className="ck-co-kpi-label">NAV</div>
          <div className="ck-co-kpi-value">{formatMoney(data.nav)}</div>
        </div>
        <div className="ck-co-kpi">
          <div className="ck-co-kpi-label">Cash</div>
          <div className="ck-co-kpi-value">{formatMoney(data.cash)}</div>
        </div>
        <div className="ck-co-kpi">
          <div className="ck-co-kpi-label">Invested</div>
          <div className="ck-co-kpi-value">{formatPct(data.invested_pct)}</div>
        </div>
        <div className="ck-co-kpi">
          <div className="ck-co-kpi-label">Open positions</div>
          <div className="ck-co-kpi-value">{data.open_position_count ?? 0}</div>
        </div>
        <div className="ck-co-kpi">
          <div className="ck-co-kpi-label">Working orders</div>
          <div className="ck-co-kpi-value">{data.working_order_count ?? 0}</div>
        </div>
      </div>

      <TradeProposalsPanel data={proposals} />

      <div className="ck-co-card-footer">
        <Link to="/live-portfolio-activity" className="ck-co-link ck-co-link--small">
          Open Live Portfolio Activity &rarr;
        </Link>
      </div>
    </div>
  )
}
