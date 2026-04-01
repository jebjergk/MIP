/**
 * Worlds scenarios from intelligence.analog_summary — same forward realized_return
 * samples as analog matching (unchanged payload / fields).
 */
const RETURN_BAND = 0.01

function num(x) {
  const v = Number(x)
  return Number.isFinite(v) ? v : null
}

function avgOf(arr) {
  if (!arr.length) return null
  return arr.reduce((a, b) => a + b, 0) / arr.length
}

export function buildThreeWorldScenariosFromAnalog(summary) {
  const raw = Array.isArray(summary?.closest) ? summary.closest : []
  const rows = raw
    .map((ep) => ({
      rr: num(ep?.realized_return),
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

  const upsideRows = rows.filter((r) => r.rr > RETURN_BAND)
  const baseRows = rows.filter((r) => r.rr >= -RETURN_BAND && r.rr <= RETURN_BAND)
  const downsideRows = rows.filter((r) => r.rr < -RETURN_BAND)

  const pack = (label, key, group) => {
    const rets = group.map((g) => g.rr)
    const avg = avgOf(rets)
    const mn = rets.length ? Math.min(...rets) : null
    const mx = rets.length ? Math.max(...rets) : null
    return {
      key,
      label,
      probability: group.length / n,
      avgReturn: avg,
      sampleSize: group.length,
      minReturn: mn,
      maxReturn: mx,
    }
  }

  const scenarios = [
    pack('Upside', 'upside', upsideRows),
    pack('Base', 'base', baseRows),
    pack('Downside', 'downside', downsideRows),
  ]

  return {
    ok: true,
    reason: null,
    scenarios,
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

export function worldsConfidenceTier(totalN) {
  if (totalN < 5) {
    return { key: 'very_low', emoji: String.fromCodePoint(0x1f534), label: 'VERY LOW CONFIDENCE', cardOpacity: 0.62 }
  }
  if (totalN < 15) {
    return { key: 'low', emoji: String.fromCodePoint(0x1f7e0), label: 'LOW CONFIDENCE', cardOpacity: 0.62 }
  }
  if (totalN < 40) {
    return { key: 'moderate', emoji: String.fromCodePoint(0x1f7e1), label: 'MODERATE CONFIDENCE', cardOpacity: 1 }
  }
  return { key: 'strong', emoji: String.fromCodePoint(0x1f7e2), label: 'STRONG CONFIDENCE', cardOpacity: 1 }
}

/** Minimum total n before showing literal 0% / 100% as percentages */
export const WORLDS_HARD_PERCENT_MIN_N = 40

export function worldsProbabilityStrengthLabel(probability) {
  const p = probability * 100
  if (p > 60) return 'Strong likelihood'
  if (p >= 45) return 'Moderate likelihood'
  if (p >= 30) return 'Weak likelihood'
  return 'Low likelihood'
}

/**
 * How to show bucket share: avoid false certainty on small samples.
 */
export function worldsBucketProbabilityPresentation(scenario, totalN) {
  if (scenario.sampleSize === 0) {
    return {
      showProbBlock: false,
      strengthLabel: null,
    }
  }
  const p = scenario.probability
  const hard = totalN >= WORLDS_HARD_PERCENT_MIN_N
  if (!hard) {
    if (p >= 1 - 1e-12) {
      return {
        showProbBlock: true,
        mainLine: 'All observed cases in this sample',
        subLine: 'Not a population rate — small match set',
        strengthLabel: null,
      }
    }
    if (p <= 1e-12) {
      return {
        showProbBlock: true,
        mainLine: 'None in this sample',
        subLine: 'No episodes fell in this bucket',
        strengthLabel: null,
      }
    }
  }
  return {
    showProbBlock: true,
    mainLine: `${(p * 100).toFixed(0)}%`,
    subLine: 'of matched sample',
    strengthLabel: worldsProbabilityStrengthLabel(p),
  }
}

export function formatScenarioReturnDisplay(decimalReturn) {
  if (decimalReturn == null || !Number.isFinite(decimalReturn)) {
    return { text: 'No data', tone: 'neutral' }
  }
  const pct = decimalReturn * 100
  const a = Math.abs(pct)
  if (a < 1e-12) {
    return { text: '0.00%', tone: 'neutral' }
  }
  if (a < 0.01) {
    return { text: '<0.01%', tone: 'neutral' }
  }
  const decimals = a >= 1 ? 2 : 3
  const rounded = pct.toFixed(decimals)
  const tone = pct > 1e-12 ? 'pos' : pct < -1e-12 ? 'neg' : 'neutral'
  const text = `${pct > 0 ? '+' : ''}${rounded}%`
  return { text, tone }
}

export function worldsDominantHeaderLine(scenarios, totalN) {
  if (!scenarios?.length) return 'No dominant outcome — mixed setup'
  const best = scenarios.reduce((a, s) => (s.probability > a.probability ? s : a), scenarios[0])
  if (best.probability < 0.5 || best.sampleSize === 0) {
    return 'No dominant outcome — mixed setup'
  }
  const limited = totalN < WORLDS_HARD_PERCENT_MIN_N
  const fullyConcentrated = best.probability >= 1 - 1e-12
  if (limited && fullyConcentrated) {
    return `Outcomes concentrated in ${best.label} (low confidence — not a forecast)`
  }
  if (limited) {
    return `Dominant outcome: ${best.label} (limited sample)`
  }
  return `Clear dominant outcome: ${best.label}`
}

export function worldsMostLikelyKey(scenarios) {
  if (!scenarios?.length) return null
  const withMass = scenarios.filter((s) => s.sampleSize > 0)
  if (!withMass.length) return null
  return withMass.reduce((a, s) => (s.probability > a.probability ? s : a), withMass[0]).key
}

const INTERPRET = {
  upside: 'Similar setups tend to continue higher.',
  base: 'Most outcomes stay flat with limited movement.',
  downside: 'There is meaningful downside risk after entry.',
}

const GUIDANCE = {
  upside: 'Supports holding / adding',
  base: 'Supports patience / no action',
  downside: 'Supports risk reduction / exit',
}

export function worldsScenarioInterpretation(key) {
  return INTERPRET[key] ?? ''
}

export function worldsScenarioGuidance(key) {
  return GUIDANCE[key] ?? ''
}
