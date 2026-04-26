/**
 * Cockpit live trade table — operational, inline.
 *
 * Columns: Symbol · Held · Position · P&L · Plan status · Today
 *          · Recommendation · Expand
 *
 * Design notes (per cockpit trade-dashboard correction pass):
 *   - Position cell shows broker-truth direction + qty; replaces the
 *     bare "Held" + verdict pill columns.
 *   - P&L is broker-sourced (decimal fraction → single * 100 in
 *     formatPct). Two-line cell shows percent + dollars in subtle
 *     color, no fills, so large drawdowns are readable rather than
 *     intimidating.
 *   - Plan status answers "are we still on plan" (decoupled from
 *     Recommendation, which answers "what to do right now").
 *   - Shadow is intentionally off the main row; the expanded panel
 *     shows it as a subtle secondary line.
 */
import { Fragment, useState } from 'react'
import PositionRowExpanded from './PositionRowExpanded'

const EM = '\u2014'

function formatPct(val, decimals = 2) {
  if (val == null) return EM
  const n = Number(val)
  if (!Number.isFinite(n)) return EM
  return `${n >= 0 ? '+' : ''}${(n * 100).toFixed(decimals)}%`
}

function formatMoney(val) {
  if (val == null) return EM
  const n = Number(val)
  if (!Number.isFinite(n)) return EM
  const sign = n < 0 ? '-' : (n > 0 ? '+' : '')
  const abs = Math.abs(n)
  return `${sign}$${abs.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

function formatQty(val) {
  if (val == null) return EM
  const n = Number(val)
  if (!Number.isFinite(n)) return EM
  if (Math.trunc(n) === n) return String(Math.abs(Math.trunc(n)))
  return Math.abs(n).toFixed(4)
}

function pnlClass(val) {
  if (val == null) return 'ck-co-pnl'
  const n = Number(val)
  if (!Number.isFinite(n)) return 'ck-co-pnl'
  if (n > 0) return 'ck-co-pnl ck-co-pnl--up'
  if (n < 0) return 'ck-co-pnl ck-co-pnl--down'
  return 'ck-co-pnl'
}

function levelChipKind(level) {
  const l = String(level || '').toLowerCase()
  if (l === 'critical') return 'critical'
  if (l === 'warning') return 'warning'
  if (l === 'info') return 'info'
  if (l === 'ok') return 'ok'
  return 'neutral'
}

function Chip({ label, kind }) {
  return <span className={`ck-co-chip ck-co-chip--${kind}`}>{label}</span>
}

function ExpandButton({ open, onClick, label }) {
  return (
    <button
      type="button"
      className={`ck-co-expand-btn${open ? ' ck-co-expand-btn--open' : ''}`}
      onClick={onClick}
      aria-expanded={open}
      aria-label={open ? `Collapse ${label}` : `Expand ${label}`}
    >
      {open ? 'Hide' : 'Inspect'}
    </button>
  )
}

function PositionCell({ row }) {
  const sideRaw = String(row.side || '').toUpperCase()
  const side = sideRaw === 'LONG' ? 'Long' : (sideRaw === 'SHORT' ? 'Short' : EM)
  const sideKind = sideRaw === 'LONG' ? 'long' : (sideRaw === 'SHORT' ? 'short' : 'flat')
  return (
    <div className="ck-co-position-cell">
      <span className={`ck-co-side ck-co-side--${sideKind}`}>{side}</span>
      <span className="ck-co-position-qty">{formatQty(row.quantity)}</span>
    </div>
  )
}

function PnLCell({ row }) {
  const cls = pnlClass(row.unrealized_pnl_pct ?? row.unrealized_pnl)
  return (
    <div className={cls}>
      <div className="ck-co-pnl-pct">{formatPct(row.unrealized_pnl_pct, 2)}</div>
      <div className="ck-co-pnl-dollars">{formatMoney(row.unrealized_pnl)}</div>
    </div>
  )
}

export default function PositionHealthSummaryTable({ rows }) {
  const [expanded, setExpanded] = useState(() => new Set())

  const toggle = (key) => {
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  if (!rows || rows.length === 0) {
    return (
      <div className="ck-co-card">
        <h2 className="ck-co-card-title">Live trades</h2>
        <p className="ck-co-empty">No currently-open live positions.</p>
      </div>
    )
  }

  return (
    <div className="ck-co-card ck-co-card--ph">
      <div className="ck-co-card-header">
        <h2 className="ck-co-card-title">
          Live trades ({rows.length} open)
        </h2>
      </div>
      <div className="ck-co-table-wrap">
        <table className="ck-co-table ck-co-table--ph">
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Held</th>
              <th>Position</th>
              <th>P&amp;L</th>
              <th>Plan status</th>
              <th>Today</th>
              <th>Recommendation</th>
              <th aria-label="Expand" />
            </tr>
          </thead>
          <tbody>
            {rows.map(r => {
              const isOpen = expanded.has(r.position_episode_key)
              return (
                <Fragment key={r.position_episode_key}>
                  <tr className={isOpen ? 'ck-co-row--open' : ''}>
                    <td><strong>{r.symbol}</strong></td>
                    <td>{r.days_held != null ? `${r.days_held}d` : EM}</td>
                    <td><PositionCell row={r} /></td>
                    <td><PnLCell row={r} /></td>
                    <td>
                      <Chip
                        label={r.plan_status_label || EM}
                        kind={levelChipKind(r.plan_status_level)}
                      />
                    </td>
                    <td>
                      <Chip
                        label={r.today_label || EM}
                        kind={levelChipKind(r.today_level)}
                      />
                    </td>
                    <td>
                      <Chip
                        label={r.recommendation_label || EM}
                        kind={levelChipKind(r.recommendation_level)}
                      />
                    </td>
                    <td className="ck-co-expand-cell">
                      <ExpandButton
                        open={isOpen}
                        onClick={() => toggle(r.position_episode_key)}
                        label={r.symbol}
                      />
                    </td>
                  </tr>
                  {isOpen && (
                    <tr className="ck-co-row-expanded">
                      <td colSpan={8}>
                        <PositionRowExpanded row={r} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
