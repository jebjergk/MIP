/** G2.3 — compact presentation helpers for the explanation panel. */

import { formatLearningMoney } from './brooksTradeLearningG1'

export function firstSentence(text) {
  if (!text) return null
  const trimmed = String(text).trim()
  const match = trimmed.match(/^[^.!?]+[.!?]/)
  if (match) return match[0].trim()
  return trimmed.length > 140 ? `${trimmed.slice(0, 137)}…` : trimmed
}

export function compactSystemNoticed(items) {
  if (!items?.length) return null
  if (items.length === 1) return firstSentence(items[0]) || items[0]
  return items.map((x) => firstSentence(x) || x).join(' ')
}

export function compactDecision(decision) {
  if (!decision) return null
  return String(decision).replace(/\s+/g, ' ').trim()
}

export function compactRiskLine(positionRisk) {
  if (!positionRisk?.length) return null
  return positionRisk
    .map((line) => String(line).replace(/\.$/, ''))
    .join(' · ')
}

export function buildCompactStory(story, headline) {
  const s = story || {}
  return {
    market: firstSentence(s.market_story),
    system: compactSystemNoticed(s.system_noticed),
    decision: compactDecision(s.decision),
    risk: compactRiskLine(s.position_risk),
    exitWhy: s.why && !/protective stop/i.test(headline || '') ? firstSentence(s.why) : null,
    stopTimeline: s.stop_timeline,
  }
}

export function formatCompactPositionRow(pos) {
  if (!pos) return null
  const parts = []
  if (pos.side === 'long') {
    parts.push(`Position Long ${pos.quantity || ''}`.trim())
  } else {
    parts.push('Position Flat')
  }
  if (pos.entry_price != null) {
    parts.push(`Entry $${Number(pos.entry_price).toFixed(2)}`)
  }
  if (pos.active_stop != null) {
    parts.push(`Stop $${Number(pos.active_stop).toFixed(3)}`)
  }
  if (pos.unrealized_pnl != null) {
    parts.push(`Unrealized ${formatLearningMoney(pos.unrealized_pnl)}`)
  }
  if (pos.realized_pnl != null) {
    parts.push(`Realized ${formatLearningMoney(pos.realized_pnl)}`)
  }
  return parts.join(' | ')
}

/** Evidence toggle is session UI state — bar changes must not reset it. */
export function nextEvidenceOpenState(currentOpen, _prevBarTs, _nextBarTs) {
  return currentOpen
}
