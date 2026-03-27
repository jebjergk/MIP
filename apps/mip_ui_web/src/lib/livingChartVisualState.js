/**
 * Pure presentation helpers for Living Chart — single governed vocabulary (Pass 3).
 */

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

/** Primary posture: exit urgency overrides committee stance. */
export function primaryPostureLabel(committee, exitRec) {
  const urg = String(exitRec?.urgency || 'HOLD').toUpperCase()
  if (urg === 'EXIT_NOW') return 'EXIT'
  if (urg === 'PREPARE') return 'PREPARE EXIT'
  if (urg === 'MONITOR') return 'WATCH'
  const stance = String(committee?.committee_stance || '').toUpperCase()
  if (stance === 'ESCALATE' || stance === 'WATCH_CLOSELY' || stance === 'RISK_OFF') return 'WATCH'
  return 'HOLD'
}

/** Rail / compact (4–6 chars). */
export function primaryPostureShort(committee, exitRec) {
  const full = primaryPostureLabel(committee, exitRec)
  if (full === 'PREPARE EXIT') return 'PREP'
  return full
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

const CHIP_MAX = 4

/**
 * Uppercase live condition chips for chart chrome (max 4).
 * @param {string[]} conditionalKeys from pickConditionalZones
 */
export function liveConditionChips(conditionalKeys, liveState, tile) {
  const keys = Array.isArray(conditionalKeys) ? conditionalKeys : []
  const out = []
  const used = new Set()

  const push = (key, label, tone) => {
    if (out.length >= CHIP_MAX || used.has(key)) return
    used.add(key)
    out.push({ key, label, tone })
  }

  for (const k of keys) {
    if (k === 'stop_danger') push('stop_danger', 'STOP PRESSURE', 'bad')
    else if (k === 'target_near') push('target_near', 'TARGET NEAR', 'good')
    else if (k === 'mean_reversion') push('mean_reversion', 'MEAN REVERSION LIVE', 'info')
    else if (k === 'thesis_weakening') {
      if (pathDriftActive(liveState)) push('path_diverging', 'PATH DIVERGING', 'warn')
      else if (thesisStringFragile(tile)) push('thesis_fragile', 'THESIS FRAGILE', 'warn')
      else push('thesis_weakening', 'THESIS FRAGILE', 'warn')
    }
  }

  const inside = liveState?.derived_features?.inside_cone
  if (inside === false && !used.has('path_diverging') && out.length < CHIP_MAX) {
    push('outside_cone', 'PATH DIVERGING', 'warn')
  }

  return out.slice(0, CHIP_MAX)
}

/**
 * At most one strip cue; deduped vs chart chip keys and primary posture.
 * @param {Set<string>|string[]} chartChipKeys — `key` from liveConditionChips
 */
export function stripOptionalCue(tile, exitRec, liveState, committee, chartChipKeys) {
  const chartKeys = chartChipKeys instanceof Set ? chartChipKeys : new Set(chartChipKeys || [])
  const posture = primaryPostureLabel(committee, exitRec)
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
    return { key: 'upside_limited', label: 'UPSIDE LIMITED', tone: 'warn' }
  }

  if (
    liveState?.derived_features?.inside_cone === false
    && !chartKeys.has('path_diverging')
    && !chartKeys.has('outside_cone')
    && !chartKeys.has('thesis_weakening')
    && !chartKeys.has('thesis_fragile')
  ) {
    return { key: 'path_watch', label: 'OFF EXPECTED PATH', tone: 'warn' }
  }

  return null
}

/** @deprecated Prefer primaryPostureShort(committee, exitRec) */
export function postureShort(committee, exitRec = null) {
  return primaryPostureShort(committee, exitRec || {})
}

/** @deprecated Prefer primaryPostureLabel(committee, exitRec) */
export function postureLabel(committee, exitRec = null) {
  return primaryPostureLabel(committee, exitRec || {})
}

/** @deprecated Use thesisStateLabel */
export function thesisBadge(tile) {
  return thesisStateLabel(tile)
}

/** @deprecated Use liveConditionChips */
export function chartOverlayCueBadges(conditionalKeys) {
  return liveConditionChips(conditionalKeys, {}, {})
}
