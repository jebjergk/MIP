/**
 * Pure presentation helpers for Living Chart strip, rail, and chart chrome.
 */

const STANCE_SHORT = {
  THESIS_INTACT: 'INTACT',
  HOLD: 'HOLD',
  WATCH_CLOSELY: 'WATCH',
  RISK_OFF: 'RISK',
  ESCALATE: 'ALERT',
  UNKNOWN: '—',
}

const STANCE_DISPLAY = {
  THESIS_INTACT: 'Thesis intact',
  HOLD: 'Hold',
  WATCH_CLOSELY: 'Watch closely',
  RISK_OFF: 'Risk-off',
  ESCALATE: 'Escalate',
  UNKNOWN: 'Assessing',
}

export function postureShort(committee) {
  const s = String(committee?.committee_stance || 'UNKNOWN').toUpperCase()
  return STANCE_SHORT[s] || s.slice(0, 6)
}

export function postureLabel(committee) {
  const s = String(committee?.committee_stance || 'UNKNOWN').toUpperCase()
  return STANCE_DISPLAY[s] || s.replaceAll('_', ' ')
}

export function thesisBadge(tile) {
  const raw = String(tile?.thesis?.status || '').trim()
  if (!raw) return '—'
  const u = raw.toUpperCase()
  if (u.includes('INTACT')) return 'Thesis OK'
  if (u.includes('WEAK')) return 'Thesis fragile'
  if (u.includes('INVALID') || u.includes('BROKEN')) return 'Thesis broken'
  return raw.replaceAll('_', ' ').slice(0, 14)
}

/**
 * @returns {'calm'|'watch'|'fragile'|'danger'}
 */
export function healthTier(committee, exitRec, liveState, tile) {
  const urg = String(exitRec?.urgency || 'HOLD').toUpperCase()
  if (urg === 'EXIT_NOW') return 'danger'
  if (urg === 'PREPARE') return 'fragile'

  const distSl = liveState?.derived_features?.distance_to_sl_pct
  if (distSl != null && Number.isFinite(distSl) && Math.abs(distSl) < 0.012) return 'fragile'

  const stance = String(committee?.committee_stance || '').toUpperCase()
  if (stance === 'ESCALATE' || stance === 'RISK_OFF') return 'fragile'

  const inside = liveState?.derived_features?.inside_cone
  if (inside === false) return 'watch'

  if (urg === 'MONITOR') return 'watch'

  const thesis = String(tile?.thesis?.status || '').toUpperCase()
  if (thesis.includes('WEAK')) return 'watch'

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

/** Compact badges for chart chrome: only active overlay signals (not posture duplicate). */
export function chartOverlayCueBadges(conditionalKeys) {
  const keys = Array.isArray(conditionalKeys) ? conditionalKeys : []
  const badges = []
  for (const k of keys) {
    if (k === 'stop_danger') badges.push({ key: k, label: 'Near stop', tone: 'bad' })
    if (k === 'target_near') badges.push({ key: k, label: 'Near target', tone: 'good' })
    if (k === 'mean_reversion') badges.push({ key: k, label: 'Mean reversion', tone: 'info' })
    if (k === 'thesis_weakening') badges.push({ key: k, label: 'Path pressure', tone: 'warn' })
  }
  return badges
}
