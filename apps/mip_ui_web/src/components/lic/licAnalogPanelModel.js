/**
 * Pure helpers for Analog drill tab — derived only from analog_ui + analog_summary payloads.
 */

export const ANALOG_SMALL_SAMPLE_N = 15

function num(x, d = 0) {
  const v = Number(x)
  return Number.isFinite(v) ? v : d
}

export function analogParsed(u) {
  const raw = u && typeof u === 'object' ? u : {}
  const w = Math.max(0, Math.floor(num(raw.winners, 0)))
  const l = Math.max(0, Math.floor(num(raw.losers, 0)))
  const n = Math.max(0, Math.floor(num(raw.analog_count, w + l)))
  const bias = String(raw.bias || 'inconclusive')
  const tier = String(raw.confidence_tier || '')
  return { w, l, n, bias, tier, raw }
}

export function analogIsWeak(parsed) {
  const { n, tier, raw } = parsed
  if (raw.low_similarity_note) return true
  return tier === 'weak' && n > 0
}

export function analogIsSmallSample(n) {
  return n > 0 && n < ANALOG_SMALL_SAMPLE_N
}

/** Show the one-line exploratory caveat under confidence messaging */
export function analogShowExploratoryCaveat(parsed) {
  return analogIsWeak(parsed) || (parsed.n > 0 && analogIsSmallSample(parsed.n))
}

export function analogAppendLowConfidence(parsed) {
  const { n, tier, raw } = parsed
  return tier === 'weak' || !!raw.low_similarity_note || (n > 0 && analogIsSmallSample(n))
}

/** Full headline including "Analog pattern:" and optional (low confidence) */
export function buildAnalogHeadline(parsed) {
  const { w, n, bias } = parsed
  let core
  if (n === 0) {
    core = 'No comparable sample'
  } else if (bias === 'winner_leaning') {
    core = `Favorable (${w}/${n} winners)`
  } else if (bias === 'loser_leaning') {
    core = `Historically unfavorable (${w}/${n} winners)`
  } else if (bias === 'mixed') {
    core = `Mixed outcomes (${w}/${n} winners)`
  } else {
    core = `Inconclusive (${w}/${n} episodes)`
  }
  let line = `Analog pattern: ${core}`
  if (n > 0 && analogAppendLowConfidence(parsed)) {
    line += ' (low confidence)'
  }
  return line
}

export function buildAnalogImplication(parsed) {
  const { n, bias } = parsed
  const weak = analogIsWeak(parsed)
  const small = analogIsSmallSample(n)

  if (n === 0 || bias === 'inconclusive') {
    return 'Implication: No analog edge — rely on thesis, tape, and risk limits'
  }
  if (bias === 'mixed') {
    return 'Implication: Neutral — no strong edge from history'
  }
  if (bias === 'loser_leaning') {
    if (weak || small) {
      return 'Implication: History leans unfavorable — weak match or small sample; still favor caution / risk reduction'
    }
    return 'Implication: Favor risk reduction / prepare exit'
  }
  if (bias === 'winner_leaning') {
    if (weak || small) {
      return 'Implication: History leans favorable — low confidence; continuation plausible but not proven'
    }
    return 'Implication: Supports holding / continuation'
  }
  return 'Implication: Neutral — no strong edge from history'
}

export function buildAnalogRates(parsed) {
  const { w, l, n } = parsed
  if (n <= 0) {
    return {
      winLine: null,
      lossLine: null,
      emptyRates: 'No win/loss data',
      smallSampleNote: null,
    }
  }
  const winPct = Math.round((w / n) * 100)
  const lossPct = Math.round((l / n) * 100)
  const small = analogIsSmallSample(n)
  const note = small ? '(small sample)' : null
  return {
    winLine: `Win rate: ${winPct}% (n=${n})`,
    lossLine: `Loss rate: ${lossPct}% (n=${n})`,
    emptyRates: null,
    smallSampleNote: note,
  }
}

export function buildAnalogBullets(parsed, summary) {
  const { w, l, n, bias, raw } = parsed
  const items = []
  const sum = summary && typeof summary === 'object' ? summary : {}

  if (n === 0) {
    items.push('No episodes matched closely enough in the bootstrap slice')
    if (raw.low_similarity_note) {
      items.push(String(raw.low_similarity_note).replace(/\s+/g, ' ').trim())
    }
    return items.slice(0, 5)
  }

  items.push(`${n} similar episodes`)

  const avg = sum.avg_forward_return
  if (avg != null && Number.isFinite(Number(avg))) {
    const p = Number(avg) * 100
    items.push(`Average forward return in cluster ~${p.toFixed(1)}%`)
  }

  if (bias === 'loser_leaning' && w === 0) {
    items.push('No winners observed in this cluster')
  } else if (bias === 'winner_leaning' && l === 0) {
    items.push('No losers observed in this cluster')
  }

  const cp = raw.confidence_plain
  if (cp && items.length < 5) {
    items.push(`Historical match: ${cp}`)
  }

  return items.slice(0, 5)
}

export function buildAnalogTiming(parsed, summary) {
  const { n, bias } = parsed
  const sum = summary && typeof summary === 'object' ? summary : {}
  const bars = sum.best_exit_hint_bars

  if (n > 0 && bars != null && Number.isFinite(Number(bars))) {
    const b = Math.round(Number(bars))
    if (bias === 'loser_leaning') {
      return `Typical failure window: ~${b} bars`
    }
    return `Typical review window: ~${b} bars`
  }

  const plain = parsed.raw.exit_timing_hint_plain
  if (plain && String(plain).trim()) {
    const s = String(plain).trim()
    return s.length <= 140 ? s : `${s.slice(0, 137)}…`
  }
  return 'Exit timing hint is thin until analog match improves.'
}

export function analogBiasBarFractions(parsed) {
  const { w, l, n } = parsed
  if (n <= 0) return { show: false, winPct: 0, lossPct: 0 }
  return {
    show: true,
    winPct: (w / n) * 100,
    lossPct: (l / n) * 100,
  }
}

/** Label for simple text bias dominance (optional supplement to bar) */
export function analogBiasDominanceLabel(parsed) {
  const { w, l, n } = parsed
  if (n <= 0) return null
  if (w >= l + 3) return 'Win-dominated history'
  if (l >= w + 3) return 'Loss-dominated history'
  return 'Balanced history'
}
