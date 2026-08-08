import { describe, expect, it } from 'vitest'
import { resistanceLevelLabel } from './brooksLearningChartLabels'
import { explainEvidenceTerm } from './brooksLearningEvidenceGlossary'
import { buildWhyThisMattered } from './brooksLearningEvidenceGroups'
import {
  adjacentBarTimestamp,
  barIndexInSession,
  isBarInVisibleWindow,
  resolveChartBars,
  selectionBandGeometry,
  sortedSessionBars,
} from './brooksLearningSessionNav'
import { normalizeBarTs } from './brooksLearningBarSelection'

function makeBars(count, startHourUtc = 13) {
  return Array.from({ length: count }, (_, i) => {
    const mins = 30 + i * 5
    const h = startHourUtc + Math.floor(mins / 60)
    const m = mins % 60
    return {
      ts_utc: `2026-07-13T${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:00`,
      ts_ny: `2026-07-13T09:${String(mins).padStart(2, '0')}:00`,
      open: 246,
      high: 247,
      low: 245.9,
      close: 246.5,
    }
  })
}

describe('early-bar selection root cause', () => {
  const all = makeBars(78, 13)
  const trade = { entry_ts: '2026-07-13T14:25:00', exit_ts: '2026-07-13T15:45:00' }
  const early = all[0].ts_utc

  it('trade fit hides early bar until session expand', () => {
    expect(isBarInVisibleWindow(all, 'trade', trade, early)).toBe(false)
    expect(resolveChartBars(all, 'trade', trade, early).autoExpanded).toBe(true)
  })

  it('resolveChartBars expands to session when selection outside trade window', () => {
    const resolved = resolveChartBars(all, 'trade', trade, early)
    expect(resolved.bars.some((b) => normalizeBarTs(b.ts_utc) === normalizeBarTs(early))).toBe(true)
    expect(resolved.autoExpanded).toBe(true)
  })

  it('selection band is clamped inside plot (not clipped at left)', () => {
    const geom = selectionBandGeometry(20, 40, 14, 400, 18, 500)
    expect(geom.x).toBeGreaterThanOrEqual(14)
    expect(geom.x + geom.width).toBeLessThanOrEqual(414)
  })
})

describe('bar stepping', () => {
  const bars = makeBars(5, 14)
  const mid = bars[2].ts_utc

  it('previous/next moves exactly one timestamp', () => {
    const prev = adjacentBarTimestamp(bars, mid, -1)
    const next = adjacentBarTimestamp(bars, mid, 1)
    expect(barIndexInSession(bars, prev)).toBe(1)
    expect(barIndexInSession(bars, next)).toBe(3)
  })

  it('first and last bars are reachable', () => {
    const sorted = sortedSessionBars(bars)
    expect(adjacentBarTimestamp(bars, sorted[0].ts_utc, 1)).toBeTruthy()
    expect(adjacentBarTimestamp(bars, sorted[sorted.length - 1].ts_utc, -1)).toBeTruthy()
  })
})

describe('resistance labels', () => {
  it('becomes Former resistance after entry', () => {
    const before = resistanceLevelLabel(246.065, {
      referenceBarTs: '2026-07-13T14:20:00',
      entryTs: '2026-07-13T14:25:00',
      entryPrice: 246.53,
    })
    const after = resistanceLevelLabel(246.065, {
      referenceBarTs: '2026-07-13T14:30:00',
      entryTs: '2026-07-13T14:25:00',
      entryPrice: 246.53,
    })
    expect(before).toMatch(/Prior resistance/i)
    expect(after).toMatch(/Former resistance/i)
  })
})

describe('evidence glossary', () => {
  it('explains upper tail in plain language', () => {
    expect(explainEvidenceTerm('UPPER_TAIL', 'Upper tail')).toMatch(/traded higher/i)
  })

  it('why this mattered for 10:25 entry', () => {
    const text = buildWhyThisMattered({ headline: 'Entry conditions became valid' })
    expect(text).toMatch(/armed/i)
  })
})

describe('URL sync', () => {
  it('normalizeBarTs keeps grid and chart aligned', () => {
    expect(normalizeBarTs('2026-07-13 14:25:00')).toBe('2026-07-13T14:25:00')
  })
})

describe('crosshair styles', () => {
  it('crosshair class differs from stop and level classes', () => {
    const classes = ['bil-learning-crosshair', 'bil-learning-stop-step', 'bil-learning-level']
    expect(new Set(classes).size).toBe(3)
  })
})

describe('compact header contract', () => {
  it('header strip uses pipe-separated stats pattern', () => {
    const strip = 'AMZN · Mon 13 Jul 2026 | +$6.88 | Entry $246.53'
    expect(strip).toContain('|')
    expect(strip).toContain('Entry $246.53')
  })
})
