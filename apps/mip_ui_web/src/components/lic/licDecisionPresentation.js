/**
 * Single presentation resolver for Live Intelligence — primary vs fallback actions.
 * All UI copy for "what to do now" should derive from resolveDecisionPresentation(intel, tile?).
 * No backend calls; pure functions over bootstrap/step intelligence objects.
 */

function str(v, fb = '') {
  if (v == null || v === '') return fb
  if (typeof v === 'string' || typeof v === 'number') return String(v)
  return fb
}

function asArray(x) {
  return Array.isArray(x) ? x : []
}

/** Canonical four-band labels (dominant recommendation everywhere). */
export function bandLabel(band) {
  const m = {
    EXIT_NOW: 'EXIT OR CUT NOW',
    PREPARE_EXIT: 'PREPARE EXIT',
    WATCH_CLOSELY: 'WATCH CLOSELY',
    STAY_COURSE: 'HOLD',
  }
  const key = typeof band === 'string' ? band.toUpperCase() : band == null ? '' : String(band).toUpperCase()
  if (m[key]) return m[key]
  if (!key) return '—'
  return key.replace(/_/g, ' ')
}

function dominantWorldLead(intel) {
  const worlds = asArray(intel?.scenario_worlds)
  if (!worlds.length) return ''
  let best = worlds[0]
  let bestP = Number(best?.probability) || 0
  for (let i = 1; i < worlds.length; i += 1) {
    const p = Number(worlds[i]?.probability) || 0
    if (p > bestP) {
      bestP = p
      best = worlds[i]
    }
  }
  const title = str(best?.title, '')
  return title ? `${title} leads` : ''
}

function pickFallbackLabel(bestActionKey, primaryBand) {
  const k = String(bestActionKey || '').toLowerCase()
  const b = String(primaryBand || '').toUpperCase()
  if (k === 'exit_now' && b !== 'EXIT_NOW') return 'Defensive alternative'
  if (k === 'trim_50' || k === 'trim_25') return 'Lower-risk alternative'
  if (k === 'tighten_stop') return 'Protective tightening'
  return 'Fallback if risk rises'
}

function progressMetrics(tile) {
  const pm = tile?.progress_metrics || {}
  const dsl = Number(pm.distance_to_sl_pct)
  const dtp = Number(pm.distance_to_tp_pct)
  return {
    dsl: Number.isFinite(dsl) ? dsl : null,
    dtp: Number.isFinite(dtp) ? dtp : null,
  }
}

function medianPrice(tile) {
  const exp = tile?.expectation?.center_path
  if (!Array.isArray(exp) || !exp[0]) return null
  const p = Number(exp[0].price)
  return Number.isFinite(p) ? p : null
}

/** One clause beyond API summary: path vs median, aligned with tile drivers. */
function workspaceExtraClause(intel, tile) {
  const cur = Number(tile?.current_price)
  const med = medianPrice(tile)
  const side = String(tile?.side || '').toUpperCase()
  const parts = []
  if (Number.isFinite(cur) && med != null) {
    const eps = med * 0.002
    if (cur < med - eps) {
      parts.push('Price still trades under the expectation median — the path has to prove itself on the next sequences.')
    } else if (cur > med + eps) {
      parts.push('Price is still above the expectation median — working better than the median path for now.')
    }
  }
  const pnl = Number(tile?.unrealized_pnl)
  if (Number.isFinite(pnl) && side === 'LONG') {
    if (pnl > 0) {
      parts.push('Realized cushion on the position still offsets some near-stop anxiety.')
    } else if (pnl < 0) {
      parts.push('Underwater P&L means the next weak sequence carries more regret risk.')
    }
  }
  return parts.length ? parts[0] : ''
}

function primaryReasonFromMetrics(intel, tile) {
  const b = String(intel?.final_recommendation || 'STAY_COURSE').toUpperCase()
  const head = bandLabel(b)
  const th = String(intel?.thesis_fracture || '').toUpperCase()
  const { dsl, dtp } = progressMetrics(tile)
  const slTight = dsl != null && dsl < 0.035
  const slCrit = dsl != null && dsl < 0.02
  const nearTp = dtp != null && Math.abs(dtp) < 0.04
  const wideTp = dtp != null && Math.abs(dtp) > 0.06
  const mq = Number(intel?.analog_summary?.match_quality)
  const mqWeak = Number.isFinite(mq) && mq < 0.18
  const regime = String(intel?.portfolio_factor_local?.localized || '')
  const regimeActive = regime === 'portfolio_wide' || regime === 'mixed'

  if (b === 'EXIT_NOW') {
    let tail = 'immediate protection is warranted versus the remaining setup.'
    if (slCrit) {
      tail =
        'price sits inside the critical stop buffer — cut or hedge before slippage dominates.'
    } else if (th === 'THESIS_BROKEN' || th === 'THESIS_DAMAGED') {
      tail = 'thesis and risk stack no longer justify holding full size.'
    }
    return `${head} — ${tail}`
  }

  if (b === 'PREPARE_EXIT') {
    const bits = [
      `${head} wins because the setup is still alive, but reward-to-risk has narrowed enough to shift posture defensive — the path is weakening, stop danger is elevated, and remaining upside no longer clearly pays for the risk.`,
    ]
    if (nearTp) {
      bits.push(
        'Target is close — you are accepting giveback risk if you wait for the last increment of upside.',
      )
    } else if (mqWeak) {
      bits.push('Weak history match means you are leaning more on live price than backward-looking stats.')
    }
    if (regimeActive) {
      bits.push('Shared book stress is part of what you are still underwriting.')
    }
    return bits.join(' ')
  }

  if (b === 'WATCH_CLOSELY') {
    if (slTight) {
      return `${head} stays primary with stop pressure building — you are buying time while the path proves itself on the next bars.`
    }
    if (th === 'THESIS_STRETCHED') {
      return `${head} stays primary while the thesis is only stretched, not broken — you are accepting headline volatility until structure improves or fails.`
    }
    return `${head} stays primary because nothing has forced the ladder to exit yet, but several reads are fragile enough that the next adverse sequence could flip posture quickly.`
  }

  const bits = [`${head} remains primary because the path is still intact`]
  if (slCrit || slTight) {
    bits.push('stop danger is elevated but not yet at a forced-exit breach')
  } else {
    bits.push('stop danger is not yet dictating a forced exit')
  }
  if (nearTp) {
    bits.push(
      'target is nearby so upside is partly realized — you are underwriting giveback for a possible last push',
    )
  } else if (wideTp) {
    bits.push('meaningful target runway remains if the path holds')
  } else {
    bits.push('some upside room still exists versus the modeled target')
  }
  if (th === 'THESIS_INTACT') {
    bits.push('thesis is still coherent with price action')
  } else if (th === 'THESIS_STRETCHED') {
    bits.push(
      'thesis is stretched but not invalidated — you are accepting wobble while watching for a clean failure',
    )
  }
  if (mqWeak) bits.push('you are accepting thinner historical backup than usual')
  if (regimeActive) bits.push('shared-factor stress is a conscious tradeoff')
  return `${bits.join(', ')}.`
}

function buildPrimaryReason(intel, tile) {
  const api = str(intel?.final_recommendation_reason_summary, '').trim()
  const generic = /synthesized from tape/i.test(api)
  let base = api && !generic ? api : primaryReasonFromMetrics(intel, tile)
  const extra = workspaceExtraClause(intel, tile)
  if (extra) {
    const frag = extra.slice(0, 28).toLowerCase()
    if (!base.toLowerCase().includes(frag)) {
      base = `${base} ${extra}`.trim()
    }
  }
  return base
}

function buildFlipTrigger(intel, tile) {
  const b = String(intel?.final_recommendation || 'STAY_COURSE').toUpperCase()
  const th = String(intel?.thesis_fracture || '').toUpperCase()
  const { dsl, dtp } = progressMetrics(tile)
  const slNear = intel?.sl_near === true || (dsl != null && dsl < 0.02)
  const tightSl = dsl != null && dsl < 0.05
  const compressedTarget = dtp != null && Math.abs(dtp) < 0.045

  if (b === 'EXIT_NOW') {
    return (
      'Flip softer only if price reopens a clear buffer above the stop, the tape stabilizes, and thesis damage is walked back — not on one lucky tick.'
    )
  }

  if (b === 'PREPARE_EXIT') {
    return (
      'Flip to EXIT OR CUT NOW if weakness continues and price moves materially closer to stop than to target on the next sequences. ' +
      'Flip back toward HOLD only if the path stabilizes, stop pressure eases, and upside cushion clearly widens again.'
    )
  }

  if (b === 'WATCH_CLOSELY') {
    return (
      'Flip to PREPARE EXIT if the next weak sequence cuts target room while stop pressure increases, or if thesis damage deepens. ' +
      'Flip back toward HOLD if price holds the path and stop distance meaningfully widens.'
    )
  }

  // HOLD / STAY_COURSE
  const parts = []
  if (slNear || tightSl) {
    parts.push(
      'Flip if price keeps drifting toward the stop without a recovery bounce and remaining upside compresses further.',
    )
  } else {
    parts.push(
      'Flip if the next weak sequence shrinks target room while distance-to-stop trends down — i.e., risk is closing in faster than reward.',
    )
  }
  if (th === 'THESIS_INTACT' || th === 'THESIS_STRETCHED') {
    parts.push(
      'Also flip if thesis moves to damaged or broken while tape and stop context disagree with staying full size.',
    )
  }
  if (compressedTarget) {
    parts.push('Near target, treat a failure to hold gains as a signal to de-risk even if the stop is not tickling yet.')
  }
  return parts.join(' ')
}

/**
 * @returns {{
 *   primary_band: string,
 *   primary_action: string,
 *   fallback_action: string | null,
 *   fallback_label: string,
 *   fallback_strip_text: string,
 *   primary_reason: string,
 *   flip_trigger: string,
 *   sim_best_action: string,
 *   sim_best_label: string,
 * }}
 */
export function resolveDecisionPresentation(intel, tile) {
  const empty = {
    primary_band: '',
    primary_action: '—',
    fallback_action: null,
    fallback_label: '',
    fallback_strip_text: '',
    primary_reason: '',
    flip_trigger: '',
    sim_best_action: '',
    sim_best_label: '',
  }
  if (!intel || typeof intel !== 'object') return empty

  const primary_band = String(intel.final_recommendation || 'STAY_COURSE').toUpperCase()
  const primary_action = bandLabel(primary_band)

  const sim = intel.action_simulation || {}
  const best = sim.best_action || {}
  const sim_best_action = str(best.action, '')
  const sim_best_label = str(best.label, '')
  let fallback_action = null
  let fallback_label = ''
  if (sim.aligns_with_tile_recommendation === false && sim_best_label) {
    fallback_action = sim_best_label
    fallback_label = pickFallbackLabel(sim_best_action, primary_band)
  }

  const world = dominantWorldLead(intel)
  let fallback_strip_text = ''
  if (fallback_action) {
    fallback_strip_text = world ? `${world} · ${fallback_label}: ${fallback_action}` : `${fallback_label}: ${fallback_action}`
  } else if (world) {
    fallback_strip_text = world
  }

  const t = tile && typeof tile === 'object' ? tile : null

  return {
    primary_band,
    primary_action,
    fallback_action,
    fallback_label,
    fallback_strip_text,
    primary_reason: buildPrimaryReason(intel, t),
    flip_trigger: buildFlipTrigger(intel, t),
    sim_best_action,
    sim_best_label,
  }
}

/** Case file implication line — avoid naked "Exit now" when official stance is not exit. */
export function caseFileImplicationDisplay(row, intelligenceBySymbol) {
  const sym = String(row?.symbol || '').toUpperCase()
  const raw = row?.action_implication != null ? str(row.action_implication) : ''
  if (!sym || sym === 'SESSION' || !raw) return raw

  if (raw.includes('retained') || /[→]|->/.test(raw)) {
    return raw
  }

  const intel = intelligenceBySymbol?.[sym]
  if (!intel) return raw

  const pres = resolveDecisionPresentation(intel)
  const primaryExit = pres.primary_band === 'EXIT_NOW'
  const rawExitish = /\bexit now\b/i.test(raw) || /\bcut now\b/i.test(raw) || /^exit\b/i.test(raw.trim())

  if (!primaryExit && rawExitish) {
    const fb = pres.fallback_action || 'EXIT NOW'
    const lbl = pres.fallback_label || 'Defensive alternative'
    return `${sym}: ${pres.primary_action} retained; ${lbl.toLowerCase()}: ${fb}`
  }

  if (primaryExit && rawExitish) {
    return `${sym}: ${pres.primary_action} — follow risk protocol`
  }

  return raw
}
