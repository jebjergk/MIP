/**
 * Single presentation resolver for Live Intelligence — primary vs fallback actions.
 * All UI copy for "what to do now" should derive from resolveDecisionPresentation(intel).
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

function buildFlipTrigger(intel) {
  const parts = []
  if (intel?.sl_near) {
    parts.push('Break into or through the stop buffer, or a sustained move toward the stop')
  }
  const th = String(intel?.thesis_fracture || '').toUpperCase()
  if (th === 'THESIS_BROKEN') {
    parts.push('Thesis treated as broken for risk purposes (not just stretched)')
  } else if (th === 'THESIS_DAMAGED') {
    parts.push('Further thesis damage or conflicting tape vs the original setup')
  }
  const dsl = Number(intel?.progress_metrics?.distance_to_sl_pct)
  if (Number.isFinite(dsl) && dsl < 0.08 && dsl >= 0 && !intel?.sl_near) {
    parts.push('Stop danger tightening materially (distance to stop shrinking)')
  }
  if (!parts.length) {
    parts.push('Sustained divergence from the expected path plus rising execution or regret risk')
  }
  return parts.join('; ') + '.'
}

function buildPrimaryReason(intel) {
  const rs = str(intel?.final_recommendation_reason_summary, '').trim()
  if (rs) return rs
  const thesis = str(intel?.thesis_plain, '').trim()
  if (thesis) return thesis
  const drivers = asArray(intel?.decision_drivers)
  if (drivers.length) return str(drivers[0], '')
  return 'Resolved stance follows the deterministic risk ladder for this snapshot.'
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
export function resolveDecisionPresentation(intel) {
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

  return {
    primary_band,
    primary_action,
    fallback_action,
    fallback_label,
    fallback_strip_text,
    primary_reason: buildPrimaryReason(intel),
    flip_trigger: buildFlipTrigger(intel),
    sim_best_action,
    sim_best_label,
  }
}

/** Case file implication line — avoid naked "Exit now" when official stance is not exit. */
export function caseFileImplicationDisplay(row, intelligenceBySymbol) {
  const sym = String(row?.symbol || '').toUpperCase()
  const raw = row?.action_implication != null ? str(row.action_implication) : ''
  if (!sym || sym === 'SESSION' || !raw) return raw

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
