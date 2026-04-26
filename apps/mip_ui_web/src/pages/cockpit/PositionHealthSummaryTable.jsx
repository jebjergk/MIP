/**
 * Cockpit Position Health table — operational, inline.
 *
 * Columns: Symbol · Held · P&L · Real · Shadow · Today · Why · Expand.
 *
 * Rows expand inline (no navigation) to a chart-rich detail panel.
 *
 * Design notes (per cockpit correction pass):
 *   - Attention column dropped: Real / Shadow / Today already cover the
 *     operationally-relevant signals.
 *   - P&L is plain text (subtle color, no background fill) so the table
 *     reads cleanly even when % swings are large.
 *   - Shadow chip uses the backend-derived `shadow_relation` so it is
 *     immediately operationally useful (agrees / harsher / softer /
 *     wants out / pending / no shadow / failed).
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

function pnlClass(val) {
  if (val == null) return 'ck-co-pnl'
  const n = Number(val)
  if (!Number.isFinite(n)) return 'ck-co-pnl'
  if (n > 0) return 'ck-co-pnl ck-co-pnl--up'
  if (n < 0) return 'ck-co-pnl ck-co-pnl--down'
  return 'ck-co-pnl'
}

function realKind(verdict) {
  const v = String(verdict || '').toUpperCase()
  if (v === 'EXIT_REVIEW') return 'warn'
  if (v === 'WATCH') return 'info'
  if (v === 'KEEP') return 'ok'
  return 'neutral'
}

function shadowChipKind(level, relation) {
  const r = String(relation || '').toUpperCase()
  if (r === 'EXIT_NOW') return 'critical'
  const l = String(level || '').toLowerCase()
  if (l === 'critical') return 'critical'
  if (l === 'warning') return 'warning'
  if (l === 'info') return 'info'
  return 'neutral'
}

function todayKind(level) {
  const l = String(level || '').toLowerCase()
  if (l === 'critical') return 'critical'
  if (l === 'warning') return 'warning'
  if (l === 'info') return 'info'
  return 'neutral'
}

function VerdictPill({ label, kind }) {
  return <span className={`ck-co-verdict-pill ck-co-verdict-pill--${kind}`}>{label}</span>
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
        <h2 className="ck-co-card-title">Position Health</h2>
        <p className="ck-co-empty">No currently-open live positions.</p>
      </div>
    )
  }
  return (
    <div className="ck-co-card ck-co-card--ph">
      <div className="ck-co-card-header">
        <h2 className="ck-co-card-title">
          Live positions ({rows.length} open)
        </h2>
      </div>
      <div className="ck-co-table-wrap">
        <table className="ck-co-table ck-co-table--ph">
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Held</th>
              <th>P&amp;L</th>
              <th>Real</th>
              <th>Shadow</th>
              <th>Today</th>
              <th>Why</th>
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
                    <td>{r.days_held ?? EM}</td>
                    <td className={pnlClass(r.unrealized_pnl_pct)}>
                      {formatPct(r.unrealized_pnl_pct, 2)}
                    </td>
                    <td>
                      <VerdictPill
                        label={r.real_verdict_label || r.real_verdict || EM}
                        kind={realKind(r.real_verdict)}
                      />
                    </td>
                    <td>
                      <Chip
                        label={r.shadow_relation_label || 'Shadow: —'}
                        kind={shadowChipKind(r.shadow_relation_level, r.shadow_relation)}
                      />
                    </td>
                    <td>
                      <Chip
                        label={r.today_label || EM}
                        kind={todayKind(r.today_level)}
                      />
                    </td>
                    <td className="ck-co-why">{r.why_text}</td>
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
