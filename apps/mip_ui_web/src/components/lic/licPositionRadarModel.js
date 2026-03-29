/**
 * Position Snapshot radar — six comparable 0–100 axes from in-memory LIC state only.
 *
 * 1) Thesis strength — activeIntel.confidence.thesis_confidence (0–1); else thesis_fracture map.
 * 2) Live vs expected — (current − med) / med; LONG: above expected scores higher; SHORT: inverted.
 *    score = 50 + 50 * tanh(dev / 0.004), clamped 0–100; no med → 50.
 * 3) Stop safety — distance_to_sl_pct (fraction); farther from stop → higher.
 *    clamp01((dsl − 0.005) / 0.12) * 100.
 * 4) Target opportunity — distance_to_tp_pct; more room → higher.
 *    clamp01((dtp − 0.01) / 0.12) * 100; invalid → 50.
 * 5) Analog support — 100 * match_quality * (0.5 + 0.5 * (w−l)/n) when n>0; n=0 → ~28.
 * 6) Portfolio fit — symbol_specific + regime inactive → 90; active + portfolio_wide → 28;
 *    active + mixed → 52; else 60.
 */

function clamp01(x) {
  if (!Number.isFinite(x)) return 0
  return Math.min(1, Math.max(0, x))
}

function clamp0100(x) {
  return Math.min(100, Math.max(0, Math.round(x)))
}

function thesisStrength(intel) {
  const tc = Number(intel?.confidence?.thesis_confidence)
  if (Number.isFinite(tc)) return clamp0100(tc * 100)
  const th = String(intel?.thesis_fracture || '').toUpperCase()
  const map = {
    THESIS_INTACT: 88,
    THESIS_STRETCHED: 68,
    THESIS_DAMAGED: 42,
    THESIS_BROKEN: 18,
  }
  return map[th] ?? 50
}

function liveVsExpectedAlignment(tile) {
  const exp = tile?.expectation || {}
  const path = Array.isArray(exp.center_path) ? exp.center_path : []
  const med = Number(path[0]?.price)
  const cur = Number(tile?.current_price)
  if (!Number.isFinite(med) || med === 0 || !Number.isFinite(cur)) return 50
  let dev = (cur - med) / med
  const side = String(tile?.side || 'LONG').toUpperCase()
  if (side === 'SHORT') dev = -dev
  const t = Math.tanh(dev / 0.004)
  return clamp0100(50 + 50 * t)
}

function stopSafety(tile) {
  const dsl = Number((tile?.progress_metrics || {}).distance_to_sl_pct)
  if (!Number.isFinite(dsl)) return 50
  return clamp0100(clamp01((dsl - 0.005) / 0.12) * 100)
}

function targetOpportunity(tile) {
  const dtp = Number((tile?.progress_metrics || {}).distance_to_tp_pct)
  if (!Number.isFinite(dtp)) return 50
  return clamp0100(clamp01((dtp - 0.01) / 0.12) * 100)
}

function analogSupport(intel) {
  const s = intel?.analog_summary || {}
  const mq = Number(s.match_quality)
  const w = Number(s.winners) || 0
  const l = Number(s.losers) || 0
  const n = w + l
  if (!Number.isFinite(mq)) return 28
  if (n === 0) return 28
  const tilt = 0.5 + 0.5 * ((w - l) / n)
  return clamp0100(100 * mq * tilt)
}

function portfolioFit(intel, portfolioRegime) {
  const localized = String(intel?.portfolio_factor_local?.localized || '')
  const active = Boolean(portfolioRegime?.active)
  if (!active && localized === 'symbol_specific') return 90
  if (active && localized === 'portfolio_wide') return 28
  if (active && localized === 'mixed') return 52
  return 60
}

/** @returns {number[]} length 6 */
export function computeRawRadarTuple(intel, tile, portfolioRegime) {
  if (!intel || !tile) return [50, 50, 50, 50, 50, 50]
  return [
    thesisStrength(intel),
    liveVsExpectedAlignment(tile),
    stopSafety(tile),
    targetOpportunity(tile),
    analogSupport(intel),
    portfolioFit(intel, portfolioRegime || {}),
  ]
}

/**
 * EMA per axis: S_t = alpha * x_t + (1 - alpha) * S_{t-1}
 * @param {number[]|null} prev length 6 or null
 * @param {number[]} next length 6
 */
export function smoothRadarScores(prev, next, alpha = 0.32) {
  if (!prev || prev.length !== 6) return next.slice()
  return next.map((x, i) => {
    const p = prev[i]
    const blended = alpha * x + (1 - alpha) * p
    return clamp0100(blended)
  })
}

export const RADAR_AXIS_META = [
  { key: 'thesis', label: 'Thesis strength' },
  { key: 'liveVsExpected', label: 'Live vs expected alignment' },
  { key: 'stopSafety', label: 'Stop safety' },
  { key: 'targetOpp', label: 'Target opportunity' },
  { key: 'analog', label: 'Analog support' },
  { key: 'portfolioFit', label: 'Portfolio fit' },
]

/** Recharts Radar: one row per spoke, dataKey "score" */
export function radarTupleToChartData(tuple) {
  return RADAR_AXIS_META.map((axis, i) => ({
    axisKey: axis.key,
    metric: axis.label,
    score: tuple[i] ?? 50,
    fullMark: 100,
  }))
}
