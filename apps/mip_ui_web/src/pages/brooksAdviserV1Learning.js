import { normalizeBarTs } from './brooksLearningBarSelection'
import { formatLearningMoney } from './brooksTradeLearningG1'

/** V1.0 Adviser Learning View panels (presentation only — persisted Adviser fields). */

export function findGridRow(payload, barTs) {
  const key = normalizeBarTs(barTs)
  return (payload?.observation_grid || []).find((r) => normalizeBarTs(r.bar_ts) === key) || null
}

export function findEvidenceForBar(payload, barTs) {
  const key = normalizeBarTs(barTs)
  return (payload?.adviser_evidence_by_call || []).find((e) => normalizeBarTs(e.bar_ts) === key) || null
}

export function formatOhlc(ohlc) {
  if (!ohlc) return '—'
  return `${Number(ohlc.o).toFixed(2)}/${Number(ohlc.h).toFixed(2)}/${Number(ohlc.l).toFixed(2)}/${Number(ohlc.c).toFixed(2)}`
}

export function formatSessionSummaryLine(summary) {
  if (!summary) return '—'
  const trades = summary.trades ?? 0
  const pnl = summary.realized_pnl != null ? formatLearningMoney(summary.realized_pnl) : '$0.00'
  const pos = summary.final_position_qty ?? 0
  return `${trades} trade${trades === 1 ? '' : 's'} · ${pnl} · position ${pos}`
}

export function formatTradeRow(t) {
  const n = t.trade_number ?? '—'
  const exitLabel = String(t.exit_reason || 'exit').toLowerCase() === 'stop' ? 'stop' : 'exit'
  const pnl = t.realized_pnl != null ? formatLearningMoney(t.realized_pnl) : '—'
  return `Trade ${n} · ${t.entry_time_et} entry ${Number(t.entry_price).toFixed(2)} · ${t.exit_time_et} ${exitLabel} ${Number(t.exit_price).toFixed(2)} · ${t.quantity} sh · ${pnl}`
}
