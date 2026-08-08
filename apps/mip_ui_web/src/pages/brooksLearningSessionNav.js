/** G2.2 — session bar order, step navigation, visible chart window. */

import { normalizeBarTs } from './brooksLearningBarSelection'
import { sliceBarsForFitMode } from './brooksLearningChartDomain'

export function sortedSessionBars(allBars) {
  return [...(allBars || [])].sort((a, b) => {
    const ka = normalizeBarTs(a.ts_utc)
    const kb = normalizeBarTs(b.ts_utc)
    return ka < kb ? -1 : ka > kb ? 1 : 0
  })
}

export function barIndexInSession(allBars, barTs) {
  const key = normalizeBarTs(barTs)
  const sorted = sortedSessionBars(allBars)
  const idx = sorted.findIndex((b) => normalizeBarTs(b.ts_utc) === key)
  return idx
}

export function adjacentBarTimestamp(allBars, currentTs, delta) {
  const sorted = sortedSessionBars(allBars)
  const idx = barIndexInSession(allBars, currentTs)
  if (idx < 0) return null
  const next = sorted[idx + delta]
  return next?.ts_utc ?? null
}

export function barSessionMeta(allBars, barTs) {
  const sorted = sortedSessionBars(allBars)
  const idx = barIndexInSession(allBars, barTs)
  if (idx < 0) return null
  const bar = sorted[idx]
  const timeNy = String(bar.ts_ny || bar.ts_utc || '').replace(' ', 'T').slice(11, 16)
  return {
    index: idx + 1,
    total: sorted.length,
    timeNy: timeNy || '—',
    barTs: normalizeBarTs(bar.ts_utc),
  }
}

export function isBarInVisibleWindow(allBars, chartFit, tradeSummary, barTs) {
  const { bars } = sliceBarsForFitMode(allBars, chartFit, tradeSummary)
  const key = normalizeBarTs(barTs)
  return bars.some((b) => normalizeBarTs(b.ts_utc) === key)
}

/** If selection is outside Fit trade window, use full session bars for the chart. */
export function resolveChartBars(allBars, chartFit, tradeSummary, selectedBarTs) {
  const base = sliceBarsForFitMode(allBars, chartFit, tradeSummary)
  const key = normalizeBarTs(selectedBarTs)
  if (chartFit === 'trade' && key && !base.bars.some((b) => normalizeBarTs(b.ts_utc) === key)) {
    const session = sliceBarsForFitMode(allBars, 'session', tradeSummary)
    return { ...session, autoExpanded: true }
  }
  return { ...base, autoExpanded: false }
}

export function selectionBandGeometry(cx, slot, marginLeft, innerW, plotTop, plotH) {
  const half = slot / 2
  let x = cx - half
  let w = slot
  if (x < marginLeft) {
    w -= marginLeft - x
    x = marginLeft
  }
  const right = marginLeft + innerW
  if (x + w > right) {
    w = Math.max(1, right - x)
  }
  return { x, y: plotTop, width: w, height: plotH }
}

/** @returns true if chart highlight can attach to a visible candle (root cause diagnostic). */
export function selectionVisibleOnChart(allBars, chartFit, tradeSummary, selectedBarTs) {
  const { bars } = resolveChartBars(allBars, chartFit, tradeSummary, selectedBarTs)
  const key = normalizeBarTs(selectedBarTs)
  return !!key && bars.some((b) => normalizeBarTs(b.ts_utc) === key)
}
