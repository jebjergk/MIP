/**
 * Pure presentation helpers for Living Chart — governed vocabulary.
 *
 * Per-symbol data binding (page layer): rail/header must pass the tile’s own
 * committee + exitRec from committeeBySymbol[sym] / exitRecBySymbol[sym];
 * selected header uses activeTile’s committee/exitRec only. Chart chips use
 * activeTile only. No shared “selected” object should be passed into other rows.
 */

/** Committee stance only — not exit urgency (prevents PREPARE flooding “posture”). */
export function committeePostureLabel(committee) {
  const s = String(committee?.committee_stance || 'UNKNOWN').toUpperCase()
  if (s === 'ESCALATE') return 'ALERT'
  if (s === 'WATCH_CLOSELY' || s === 'RISK_OFF') return 'WATCH'
  if (s === 'THESIS_INTACT') return 'HOLD'
  return 'HOLD'
}

/** Rail / compact — committee stance only. */
export function committeePostureShort(committee) {
  const s = String(committee?.committee_stance || 'UNKNOWN').toUpperCase()
  if (s === 'ESCALATE') return 'ALT'
  if (s === 'WATCH_CLOSELY' || s === 'RISK_OFF') return 'WATCH'
  return 'HOLD'
}

/** Exit layer only; null when HOLD (no extra chip). */
export function exitActionDisplay(exitRec) {
  const u = String(exitRec?.urgency || 'HOLD').toUpperCase()
  if (u === 'EXIT_NOW') return 'EXIT'
  if (u === 'PREPARE') return 'PREPARE'
  if (u === 'MONITOR') return 'MONITOR'
  return null
}

/**
 * Single headline action for strip/rail (governed set). Exit urgency outranks committee
 * for EXIT_NOW and PREPARE; MONITOR maps to WATCH; else committee stance up to WATCH.
 * @returns {'HOLD'|'WATCH'|'PREPARE EXIT'|'EXIT'}
 */
export function resolvePrimaryAction(committee, exitRec) {
  const urg = String(exitRec?.urgency || 'HOLD').toUpperCase()
  if (urg === 'EXIT_NOW') return 'EXIT'
  if (urg === 'PREPARE') return 'PREPARE EXIT'
  if (urg === 'MONITOR') return 'WATCH'

  const stance = String(committee?.committee_stance || '').toUpperCase()
  if (stance === 'ESCALATE' || stance === 'RISK_OFF' || stance === 'WATCH_CLOSELY') return 'WATCH'
  return 'HOLD'
}

/** Compact rail badge from primaryAction only. */
export function primaryActionShortLabel(primaryAction) {
  const p = String(primaryAction || 'HOLD')
  if (p === 'PREPARE EXIT') return 'PREP'
  return p
}

/**
 * Subordinate caution line — never peers primary; null when primary already states exit prep.
 * @returns {{ line: string } | null}
 */
export function resolveSecondaryFallback(primaryAction, committee, exitRec) {
  const pa = String(primaryAction || 'HOLD')
  if (pa === 'EXIT' || pa === 'PREPARE EXIT') return null

  const urg = String(exitRec?.urgency || 'HOLD').toUpperCase()
  const stance = String(committee?.committee_stance || '').toUpperCase()
  if (pa === 'WATCH' && urg === 'HOLD' && stance === 'ESCALATE') {
    return { line: 'If pressure continues: prepare exit' }
  }
  if (pa === 'WATCH' && urg === 'HOLD' && stance === 'WATCH_CLOSELY') {
    return { line: 'If pressure builds: prepare exit' }
  }
  return null
}

/** @returns {'calm'|'elevated'|'high'} */
export function riskPressureTier(committee, exitRec, liveState, tile) {
  const urg = String(exitRec?.urgency || 'HOLD').toUpperCase()
  if (urg === 'EXIT_NOW') return 'high'

  const distSl = liveState?.derived_features?.distance_to_sl_pct
  if (distSl != null && Number.isFinite(distSl) && Math.abs(distSl) < 0.012) return 'high'

  if (urg === 'PREPARE') return 'elevated'

  const stance = String(committee?.committee_stance || '').toUpperCase()
  if (stance === 'ESCALATE' || stance === 'RISK_OFF') return 'elevated'

  if (liveState?.derived_features?.inside_cone === false) return 'elevated'

  if (urg === 'MONITOR') return 'elevated'

  const thesis = String(tile?.thesis?.status || '').toUpperCase()
  if (thesis.includes('WEAK')) return 'elevated'

  return 'calm'
}

export function riskPressureLabel(committee, exitRec, liveState, tile) {
  const t = riskPressureTier(committee, exitRec, liveState, tile)
  return { calm: 'CALM', elevated: 'ELEVATED', high: 'HIGH' }[t] || 'CALM'
}

/**
 * Legacy tier for CSS hooks: calm | watch | fragile | danger
 * Maps to risk pressure for rail/strip accents (watch/fragile both = elevated pressure).
 */
export function healthTier(committee, exitRec, liveState, tile) {
  const t = riskPressureTier(committee, exitRec, liveState, tile)
  if (t === 'high') return 'danger'
  if (t === 'elevated') return 'watch'
  return 'calm'
}

export function healthLabel(tier) {
  const m = {
    calm: 'Calm',
    watch: 'Watch',
    fragile: 'Fragile',
    danger: 'At risk',
  }
  return m[tier] || tier
}

/** @deprecated Use committeePostureLabel — was conflating exit urgency into posture. */
export function primaryPostureLabel(committee, exitRec) {
  void exitRec
  return committeePostureLabel(committee)
}

/** @deprecated Use committeePostureShort */
export function primaryPostureShort(committee, exitRec) {
  void exitRec
  return committeePostureShort(committee)
}

/** Thesis from tile payload only — INTACT | FRAGILE | BROKEN */
export function thesisStateLabel(tile) {
  const raw = String(tile?.thesis?.status || '').trim()
  if (!raw) return 'INTACT'
  const u = raw.toUpperCase()
  if (u.includes('BROKEN') || u.includes('INVALID')) return 'BROKEN'
  if (u.includes('WEAK')) return 'FRAGILE'
  if (u.includes('INTACT')) return 'INTACT'
  return 'INTACT'
}

/**
 * Single contract for strip + rail + tooltips. Same resolver for every symbol.
 */
export function resolveLivingChartSymbolDisplay(tile, committee, exitRec, liveState) {
  const primaryAction = resolvePrimaryAction(committee, exitRec)
  return {
    primaryAction,
    primaryActionShort: primaryActionShortLabel(primaryAction),
    secondaryFallback: resolveSecondaryFallback(primaryAction, committee, exitRec),
    committeePosture: committeePostureLabel(committee),
    committeePostureShort: committeePostureShort(committee),
    exitUrgency: String(exitRec?.urgency || 'HOLD').toUpperCase(),
    exitActionLabel: exitActionDisplay(exitRec),
    thesisState: thesisStateLabel(tile),
    riskLabel: riskPressureLabel(committee, exitRec, liveState, tile),
    riskTier: riskPressureTier(committee, exitRec, liveState, tile),
  }
}

export function pathDriftActive(liveState) {
  const feats = liveState?.derived_features || {}
  return (
    feats.inside_cone === false
    && feats.deviation_from_h5_median != null
    && Number.isFinite(feats.deviation_from_h5_median)
    && Math.abs(feats.deviation_from_h5_median) > 0.01
  )
}

export function thesisStringFragile(tile) {
  const u = String(tile?.thesis?.status || '').toUpperCase()
  return u.includes('WEAK')
}

const CHIP_MAX_LEGACY = 4

/**
 * Canonical active-condition keys (temporal live state — not primary posture).
 * stop_pressure: from zone stop_danger
 */
/**
 * Expand pickConditionalZones keys into ordered canonical active keys (max meaningful sequence).
 * @returns {string[]} e.g. stop_pressure, target_near, path_diverging, mean_reversion, thesis_pressure
 */
export function resolveExpandedActiveKeys(conditionalKeys, liveState, tile) {
  const keys = Array.isArray(conditionalKeys) ? conditionalKeys : []
  const out = []
  const seen = new Set()
  const add = (canonical) => {
    if (!canonical || seen.has(canonical)) return
    seen.add(canonical)
    out.push(canonical)
  }

  for (const k of keys) {
    if (k === 'stop_danger') add('stop_pressure')
    else if (k === 'target_near') add('target_near')
    else if (k === 'mean_reversion') add('mean_reversion')
    else if (k === 'thesis_weakening') {
      if (pathDriftActive(liveState)) add('path_diverging')
      else add('thesis_pressure')
    }
  }
  if (liveState?.derived_features?.inside_cone === false && !seen.has('path_diverging')) {
    add('path_diverging')
  }
  return out
}

/**
 * @returns {{ dominantKey: string | null, secondaryKey: string | null }}
 */
export function resolveActiveConditionSummary(conditionalKeys, liveState, tile) {
  const expanded = resolveExpandedActiveKeys(conditionalKeys, liveState, tile)
  const dominantKey = expanded[0] ?? null
  const secondaryKey = expanded.length > 1 && expanded[1] !== dominantKey ? expanded[1] : null
  return { dominantKey, secondaryKey }
}

/**
 * One-line header attention (observational, not a trading command).
 * Never substitute for primary posture (HOLD / WATCH / PREPARE EXIT / EXIT).
 * @returns {{ line: string } | null}
 */
export function resolveDominantAttention(dominantKey) {
  if (!dominantKey) return null
  const m = {
    stop_pressure: 'Stop pressure rising',
    target_near: 'Target nearly hit',
    path_diverging: 'Path diverging',
    mean_reversion: 'Mean reversion active',
    thesis_pressure: 'Thesis weakening',
  }
  const line = m[dominantKey]
  return line ? { line } : null
}

/** Short tag for Plotly annotation near last bar (mirrors header). */
export function dominantActivePlotTag(dominantKey) {
  if (!dominantKey) return null
  const m = {
    stop_pressure: 'Stop pressure',
    target_near: 'Near target',
    path_diverging: 'Off path',
    mean_reversion: 'MR active',
    thesis_pressure: 'Thesis weak',
  }
  return m[dominantKey] || null
}

const ACTIVE_CHIP_MAX = 2

/**
 * Chart micro-chips: governed labels, at most two active conditions.
 */
export function liveConditionChipsActive(conditionalKeys, liveState, tile, max = ACTIVE_CHIP_MAX) {
  const expanded = resolveExpandedActiveKeys(conditionalKeys, liveState, tile)
  const out = []
  const push = (key, label, tone) => {
    if (out.length >= max) return
    out.push({ key, label, tone })
  }
  for (const c of expanded) {
    if (out.length >= max) break
    if (c === 'stop_pressure') push('stop_pressure', 'STOP PRESSURE', 'bad')
    else if (c === 'target_near') push('target_near', 'TARGET NEAR', 'good')
    else if (c === 'path_diverging') push('path_diverging', 'PATH DIVERGING', 'warn')
    else if (c === 'mean_reversion') push('mean_reversion', 'MR ACTIVE', 'info')
    else if (c === 'thesis_pressure') push('thesis_pressure', 'THESIS WEAKENING', 'warn')
  }
  return out
}

/**
 * Uppercase live condition chips for chart chrome (max 4) — legacy; prefer liveConditionChipsActive.
 * @param {string[]} conditionalKeys from pickConditionalZones
 */
export function liveConditionChips(conditionalKeys, liveState, tile) {
  const keys = Array.isArray(conditionalKeys) ? conditionalKeys : []
  const out = []
  const used = new Set()

  const push = (key, label, tone) => {
    if (out.length >= CHIP_MAX_LEGACY || used.has(key)) return
    used.add(key)
    out.push({ key, label, tone })
  }

  for (const k of keys) {
    if (k === 'stop_danger') push('stop_danger', 'STOP PRESSURE', 'bad')
    else if (k === 'target_near') push('target_near', 'NEAR TARGET', 'good')
    else if (k === 'mean_reversion') push('mean_reversion', 'MEAN REVERSION', 'info')
    else if (k === 'thesis_weakening') {
      if (pathDriftActive(liveState)) push('path_diverging', 'OFF EXPECTED PATH', 'warn')
      else if (thesisStringFragile(tile)) push('thesis_fragile', 'THESIS WEAKENING', 'warn')
      else push('thesis_weakening', 'THESIS WEAKENING', 'warn')
    }
  }

  const inside = liveState?.derived_features?.inside_cone
  if (inside === false && !used.has('path_diverging') && out.length < CHIP_MAX_LEGACY) {
    push('outside_cone', 'OFF EXPECTED PATH', 'warn')
  }

  return out.slice(0, CHIP_MAX_LEGACY)
}

/**
 * At most one strip cue; deduped vs chart chip keys (avoid repeating the same phrase as chart chips).
 * @param {Set<string>|string[]} chartChipKeys — chip `key` from liveConditionChipsActive
 * @param {string | null} [dominantActiveKey] — canonical key from resolveActiveConditionSummary; suppress cue if redundant
 */
export function stripOptionalCue(tile, exitRec, liveState, committee, chartChipKeys, dominantActiveKey = null) {
  const chartKeys = chartChipKeys instanceof Set ? chartChipKeys : new Set(chartChipKeys || [])
  const tp = Number(tile?.overlays?.take_profit)
  const hasTarget = Number.isFinite(tp)
  const distTp = liveState?.derived_features?.distance_to_tp_pct ?? tile?.progress_metrics?.distance_to_tp_pct
  const distTpN = distTp != null && Number.isFinite(Number(distTp)) ? Number(distTp) : null

  if (!hasTarget) {
    return null
  }

  if (
    hasTarget
    && distTpN != null
    && Math.abs(distTpN) < 0.03
    && !chartKeys.has('target_near')
  ) {
    if (dominantActiveKey === 'target_near') return null
    return { key: 'upside_limited', label: 'LIMITED UPSIDE', tone: 'warn' }
  }

  if (
    liveState?.derived_features?.inside_cone === false
    && !chartKeys.has('path_diverging')
    && !chartKeys.has('outside_cone')
    && !chartKeys.has('thesis_weakening')
    && !chartKeys.has('thesis_fragile')
  ) {
    if (dominantActiveKey === 'path_diverging') return null
    return { key: 'path_watch', label: 'OFF EXPECTED PATH', tone: 'warn' }
  }

  return null
}

/** @deprecated Prefer committeePostureShort */
export function postureShort(committee, exitRec = null) {
  void exitRec
  return committeePostureShort(committee)
}

/** @deprecated Prefer committeePostureLabel */
export function postureLabel(committee, exitRec = null) {
  void exitRec
  return committeePostureLabel(committee)
}

/** @deprecated Use thesisStateLabel */
export function thesisBadge(tile) {
  return thesisStateLabel(tile)
}

/** @deprecated Use liveConditionChips */
export function chartOverlayCueBadges(conditionalKeys) {
  return liveConditionChips(conditionalKeys, {}, {})
}
