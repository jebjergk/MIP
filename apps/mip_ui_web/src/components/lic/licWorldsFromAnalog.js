/**
 * Derive three distribution scenarios (Upside / Base / Downside) from
 * intelligence.analog_summary — same forward realized_return samples as analog matching.
 */
function num(x) {
  const v = Number(x)
  return Number.isFinite(v) ? v : null
}

export function buildThreeWorldScenariosFromAnalog(summary) {
  const raw = Array.isArray(summary?.closest) ? summary.closest : []
  const rows = raw
    .map((ep) => ({
      rr: num(ep?.realized_return),
      hb: num(ep?.horizon_bars),
    }))
    .filter((r) => r.rr != null)

  const n = rows.length
  if (n < 3) {
    return {
      ok: false,
      reason: 'sparse',
      scenarios: null,
      matchQuality: num(summary?.match_quality),
    }
  }

  const sorted = [...rows].sort((a, b) => a.rr - b.rr)
  const i1 = Math.max(1, Math.floor(n / 3))
  const i2 = Math.max(i1 + 1, Math.floor((2 * n) / 3))
  const downside = sorted.slice(0, i1)
  const base = sorted.slice(i1, i2)
  const upside = sorted.slice(i2, n)

  const pack = (label, key, group) => {
    const rets = group.map((g) => g.rr)
    const avg = rets.reduce((a, b) => a + b, 0) / rets.length
    return {
      key,
      label,
      probability: group.length / n,
      avgReturn: avg,
      sampleSize: group.length,
    }
  }

  const scenarios = [
    pack('Upside', 'upside', upside),
    pack('Base', 'base', base),
    pack('Downside', 'downside', downside),
  ]

  const maxAbs = Math.max(0.008, ...scenarios.map((s) => Math.abs(s.avgReturn)))

  return {
    ok: true,
    reason: null,
    scenarios,
    maxAbsReturn: maxAbs,
    totalN: n,
    matchQuality: num(summary?.match_quality),
  }
}

export function worldsRecommendationMismatch(worldsResult, finalRecommendation) {
  if (!worldsResult?.ok || !worldsResult.scenarios) return false
  const b = String(finalRecommendation || 'STAY_COURSE').toUpperCase()
  const byKey = Object.fromEntries(worldsResult.scenarios.map((s) => [s.key, s]))
  const pu = byKey.upside?.probability ?? 0
  const pd = byKey.downside?.probability ?? 0

  if (b === 'EXIT_NOW' || b === 'PREPARE_EXIT') {
    return pu >= 0.42 && pd <= 0.28
  }
  if (b === 'STAY_COURSE') {
    return pd >= 0.45 && pu <= 0.25
  }
  return false
}

export function worldsSparseCaution(worldsResult) {
  const mq = worldsResult?.matchQuality
  if (mq == null) return false
  return mq < 0.18
}

/** Adaptive % display so tiny returns do not collapse to misleading 0.00%. */
export function formatScenarioReturnPct(decimalReturn) {
  if (!Number.isFinite(decimalReturn)) return '—'
  const pct = decimalReturn * 100
  if (Math.abs(pct) < 1e-9) return '0.00%'
  let decimals = 2
  if (Math.abs(pct) < 1) decimals = 3
  if (Math.abs(pct) < 0.1) decimals = 4
  const rounded = pct.toFixed(decimals)
  return `${pct > 0 ? '+' : ''}${rounded}%`
}

export function computeReturnDispersion(scenarios) {
  if (!Array.isArray(scenarios) || scenarios.length === 0) return 0
  const avgs = scenarios.map((s) => s.avgReturn)
  return Math.max(...avgs) - Math.min(...avgs)
}

/**
 * One-line interpretation grounded in probability, average return, sample size, and dispersion.
 */
export function scenarioCardInterpretation(scenario, ctx) {
  const { dispersion, maxAbsReturn } = ctx
  const { key, probability, avgReturn, sampleSize } = scenario
  const spreadPct = dispersion * 100
  const maxPct = maxAbsReturn * 100
  const bunched = spreadPct < 0.04 && maxPct < 0.25

  if (key === 'upside') {
    if (avgReturn < 0 && maxAbsReturn > 1e-8) {
      return 'Top-ranked third, but even this slice averaged below flat in matched history.'
    }
    if (bunched) {
      return 'Highest third by rank — numeric outcomes are very similar across worlds.'
    }
    if (probability >= 0.38) {
      return 'Often seen among matches, with the strongest average forward return here.'
    }
    if (probability <= 0.28) {
      return 'A thinner favorable slice — less frequent in the set but better on average.'
    }
    return 'Upper third of matched returns — positive tilt versus the other groups.'
  }
  if (key === 'base') {
    if (bunched) {
      return 'Middle third — typical slice; little separation in average returns overall.'
    }
    return 'Central third of the match set — modest average drift between the tails.'
  }
  if (avgReturn > 0 && !bunched) {
    return 'Bottom third by ranking — this slice still averaged positive (wide analog spread).'
  }
  if (bunched) {
    return 'Lowest third by rank — small gaps vs the other worlds in average return.'
  }
  if (sampleSize <= 2) {
    return 'Few episodes in this tail — weakest average forward outcomes where data allows.'
  }
  return 'Adverse tail of the match set — weaker forward returns on average.'
}

/** Per-card sample warning when evidence is thin */
export function worldsCardSampleWarning(scenario, totalN) {
  if (scenario.sampleSize <= 2) {
    return { level: 'strong', text: `Low confidence (n=${scenario.sampleSize})` }
  }
  if (totalN <= 5) {
    return { level: 'moderate', text: 'Very limited history in this match set' }
  }
  if (totalN <= 8 && scenario.sampleSize <= 3) {
    return { level: 'moderate', text: 'Sparse slice — thin evidence in this bucket' }
  }
  return null
}
