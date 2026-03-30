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
