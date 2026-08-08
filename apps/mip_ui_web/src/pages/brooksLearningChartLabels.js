/** G2.2 — chart overlay labels (presentation only; dossier values unchanged). */

import { normalizeBarTs } from './brooksLearningBarSelection'
import { formatAxisPrice } from './brooksLearningChartDomain'

export function resistanceLevelLabel(price, { referenceBarTs, entryTs, entryPrice }) {
  const px = formatAxisPrice(Number(price), { stopPrecision: true })
  if (entryTs && referenceBarTs && entryPrice != null) {
    const afterEntry = normalizeBarTs(referenceBarTs) >= normalizeBarTs(entryTs)
    const isBreakoutLevel = Math.abs(Number(price) - Number(entryPrice)) > 0.2
      || Math.abs(Number(price) - 246.065) < 0.01
    if (afterEntry && isBreakoutLevel) {
      return `Former resistance $${px}`
    }
  }
  return `Prior resistance / breakout $${px}`
}

export function reclaimLevelLabel(price) {
  return `Reclaim $${formatAxisPrice(Number(price))}`
}

export function supportLevelLabel(price) {
  return `Support $${formatAxisPrice(Number(price))}`
}

export function initialStopLabel(price) {
  return `Initial stop $${formatAxisPrice(Number(price), { stopPrecision: true })}`
}

export function stopRaisedLabel(price, { isActive }) {
  const px = formatAxisPrice(Number(price), { stopPrecision: true })
  return isActive ? `Active stop $${px}` : `Stop raised $${px}`
}

export function dossierLevelLabels(levels, { referenceBarTs, entryTs, entryPrice }) {
  if (!levels) return []
  const out = []
  if (levels.resistance != null) {
    out.push({
      key: 'resistance',
      text: resistanceLevelLabel(levels.resistance, { referenceBarTs, entryTs, entryPrice }),
      price: levels.resistance,
    })
  }
  if (levels.reclaim != null) {
    out.push({ key: 'reclaim', text: reclaimLevelLabel(levels.reclaim), price: levels.reclaim })
  }
  if (levels.support != null) {
    out.push({ key: 'support', text: supportLevelLabel(levels.support), price: levels.support })
  }
  if (levels.do_not_chase != null) {
    out.push({
      key: 'do_not_chase',
      text: `Do not chase $${formatAxisPrice(Number(levels.do_not_chase))}`,
      price: levels.do_not_chase,
    })
  }
  return out
}

export const CHART_LEGEND = [
  { group: 'Trade', items: ['Entry / Exit markers'] },
  { group: 'Risk', items: ['Initial stop (dashed orange)', 'Active / raised stop (solid blue)'] },
  { group: 'Daily levels', items: ['Former resistance', 'Reclaim', 'Support (when in view)'] },
]

export const ADVISER_V1_CHART_LEGEND = [
  { group: 'Adviser', items: ['WATCH_LONG', 'ARM_LONG', 'CONSIDER_ENTRY', 'HOLD', 'EXIT', 'INVALIDATE'] },
  { group: 'Simulation', items: ['Sim entry / Sim exit', 'Active stop (per bar)'] },
  { group: 'Levels', items: ['Confirmation (green tick)', 'Invalidation (red tick)'] },
]
