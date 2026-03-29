/**
 * Position Snapshot radar — six comparable 0–100 axes from in-memory LIC state only.
 *
 * 1) Thesis strength — activeIntel.confidence.thesis_confidence (0–1); else thesis_fracture map.
 * 2) Live vs expected — (current − med) / med; LONG: above expected scores higher; SHORT: inverted.
 *    score = 50 + 50 * tanh(dev / 0.004), clamped 0–100; no med → 50.
 * 3) Stop safety — farther from stop → higher; mapped 0–1 then pow 1.22 so danger reads near chart center.
 * 4) Target opportunity — more room → higher; pow 1.2 on normalized room so tight-to-target reads near center.
 *    PolarRadiusAxis domain [0,100]: 0 = center, 100 = outer edge (ideal hex).
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
  const u = clamp01((dsl - 0.004) / 0.11)
  return clamp0100(100 * u ** 1.22)
}

function targetOpportunity(tile) {
  const dtp = Number((tile?.progress_metrics || {}).distance_to_tp_pct)
  if (!Number.isFinite(dtp)) return 50
  const room = Math.max(0, dtp)
  const u = clamp01((room - 0.008) / 0.11)
  return clamp0100(100 * u ** 1.2)
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
  { key: 'thesis', label: 'Thesis strength', shortLabel: 'THESIS' },
  { key: 'liveVsExpected', label: 'Path vs expected', shortLabel: 'PATH' },
  { key: 'stopSafety', label: 'Stop safety', shortLabel: 'STOP' },
  { key: 'targetOpp', label: 'Target opportunity', shortLabel: 'TARGET' },
  { key: 'analog', label: 'Analog support', shortLabel: 'ANALOG' },
  { key: 'portfolioFit', label: 'Portfolio fit', shortLabel: 'PORTFOLIO' },
]

export function alignRadarTupleToBand(tuple, finalRecommendation) {
  const b = String(finalRecommendation || 'STAY_COURSE').toUpperCase()
  const profiles = {
    EXIT_NOW: [28, 24, 18, 32, 36, 40],
    PREPARE_EXIT: [46, 42, 40, 38, 50, 48],
    WATCH_CLOSELY: [66, 58, 56, 62, 64, 62],
    STAY_COURSE: [84, 78, 76, 78, 72, 80],
  }
  const p = profiles[b] || profiles.STAY_COURSE
  const baseW = b === 'EXIT_NOW' ? 0.58 : b === 'PREPARE_EXIT' ? 0.5 : b === 'WATCH_CLOSELY' ? 0.44 : 0.38
  const exitPull = [0.6, 0.64, 0.68, 0.52, 0.54, 0.5]
  const watchDent = [0.08, 0.1, 0.09, 0.07, 0.04, 0.05]
  return tuple.map((v, i) => {
    let w = b === 'EXIT_NOW' ? exitPull[i] : baseW
    let target = p[i]
    if (b === 'WATCH_CLOSELY') {
      target = clamp0100(p[i] - watchDent[i] * 100)
    }
    if (b === 'PREPARE_EXIT') {
      const skew = [0, -4, -2, -8, 2, 0]
      target = clamp0100(p[i] + skew[i])
    }
    return clamp0100((1 - w) * v + w * target)
  })
}

export function radarInterpretationHint(tuple, finalRecommendation) {
  const b = String(finalRecommendation || 'STAY_COURSE').toUpperCase()
  const [thesis, path, stop, target] = tuple
  const minI = tuple.indexOf(Math.min(...tuple))
  if (b === 'EXIT_NOW' || (stop < 36 && path < 42)) {
    if (minI === 2 || stop <= path) return 'Weak structure — stop risk dominates'
    return 'Weak structure — path off expectation'
  }
  if (b === 'PREPARE_EXIT' || (target < 44 && path < 55)) {
    return 'Distorted — reward shrinking'
  }
  if (b === 'STAY_COURSE' && thesis >= 64 && path >= 58 && stop >= 54) {
    return 'Balanced — thesis intact'
  }
  if (b === 'WATCH_CLOSELY') {
    return 'Slightly stressed — watch path and stop'
  }
  if (thesis >= 70 && stop >= 60) return 'Balanced — room to work'
  return 'Mixed posture — weigh thesis vs risk'
}

export function radarTupleToChartData(tuple) {
  return RADAR_AXIS_META.map((axis, i) => ({
    axisKey: axis.key,
    metric: axis.label,
    shortLabel: axis.shortLabel,
    score: tuple[i] ?? 50,
    ideal: 100,
    fullMark: 100,
  }))
}
