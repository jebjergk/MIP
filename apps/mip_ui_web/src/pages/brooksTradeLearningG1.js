/** Phase G1 — canonical Brooks intraday learning review (matches backend constants). */

export const CANONICAL_G1 = {
  runId: '6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e',
  contextAttemptId: '3defa3de-d699-424a-8ceb-78020b453284',
  simulationAttemptId: '125eb282-3dc7-41a7-8fbf-602f75ad6b51',
  symbol: 'AMZN',
  tradingDate: '2026-07-13',
}

export function tradeLearningG1Url(apiBase, params) {
  const {
    runId,
    contextAttemptId,
    simulationAttemptId,
    symbol,
    tradingDate,
    replayThroughTs,
  } = params
  const qs = new URLSearchParams({
    symbol,
    trading_date: tradingDate,
  })
  if (contextAttemptId) qs.set('context_attempt_id', contextAttemptId)
  if (simulationAttemptId) qs.set('simulation_attempt_id', simulationAttemptId)
  if (replayThroughTs) qs.set('replay_through_ts', replayThroughTs)
  return `${apiBase}/runs/${encodeURIComponent(runId)}/trade-learning-g1?${qs}`
}

export function formatLearningMoney(val) {
  if (val == null || Number.isNaN(Number(val))) return '—'
  const n = Number(val)
  const sign = n >= 0 ? '+' : ''
  return `${sign}${new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
  }).format(n)}`
}

export function nyTimeFromBar(bar) {
  if (bar?.ts_ny) return String(bar.ts_ny).slice(11, 16)
  if (bar?.bar_ts_ny) return String(bar.bar_ts_ny).slice(11, 16)
  return '—'
}
