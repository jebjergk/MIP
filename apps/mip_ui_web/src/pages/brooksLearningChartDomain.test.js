import { describe, expect, it } from 'vitest'
import {
  candlePriceExtents,
  computeYDomain,
  dossierLevelsWithVisibility,
  generatePriceTicks,
  generateTimeTicksNy,
  overlayPricesNearCandles,
  sliceBarsForFitMode,
} from './brooksLearningChartDomain'

const sessionBars = [
  { open: 246, high: 246.6, low: 245.9, close: 246.53, ts_utc: '2026-07-13T14:25:00', ts_ny: '2026-07-13T10:25:00' },
  { open: 248, high: 249.1, low: 247.9, close: 248.8, ts_utc: '2026-07-13T15:05:00', ts_ny: '2026-07-13T11:05:00' },
  { open: 248.3, high: 248.5, low: 248.1, close: 248.25, ts_utc: '2026-07-13T15:45:00', ts_ny: '2026-07-13T11:45:00' },
]

describe('computeYDomain', () => {
  it('uses visible candle highs and lows with modest padding', () => {
    const { lo, hi } = computeYDomain(sessionBars, {})
    expect(lo).toBeLessThan(245.9)
    expect(hi).toBeGreaterThan(249.1)
    expect(hi - lo).toBeLessThan(5)
  })

  it('includes bar stops and nested ohlc in the domain', () => {
    const tight = [
      { open: 248, high: 248.05, low: 248, close: 248.02, ts_utc: '2026-07-13T14:25:00' },
      { open: 248.02, high: 248.04, low: 247.97, close: 248.01, active_stop: 247.95, ts_utc: '2026-07-13T14:30:00' },
    ]
    const { lo } = computeYDomain(tight, {})
    expect(lo).toBeLessThan(247.97)
    expect(lo).toBeLessThan(248)
  })

  it('does not expand Y domain for distant bar confirmation levels', () => {
    const withFarLevel = [
      ...sessionBars,
      {
        open: 246.5,
        high: 246.6,
        low: 246.4,
        close: 246.55,
        confirmation_levels: [{ level: 320.65 }],
        invalidation_levels: [{ level: 200 }],
        ts_utc: '2026-07-13T14:30:00',
      },
    ]
    const base = computeYDomain(sessionBars, {})
    const withLevels = computeYDomain(withFarLevel, {})
    expect(withLevels.lo).toBeCloseTo(base.lo, 2)
    expect(withLevels.hi).toBeCloseTo(base.hi, 2)
  })

  it('does not expand Y domain for dossier levels object alone', () => {
    const base = computeYDomain(sessionBars, {})
    const withFarLevels = computeYDomain(sessionBars, { levels: { support: 220, do_not_chase: 255 } })
    expect(withFarLevels.lo).toBeCloseTo(base.lo, 2)
    expect(withFarLevels.hi).toBeCloseTo(base.hi, 2)
  })

  it('includes nearby stops in domain via overlayPricesNearCandles', () => {
    const { lo, hi } = candlePriceExtents(sessionBars)
    const near = overlayPricesNearCandles(lo, hi, { initial_stop: 246.065, stop_steps: [{ stop_price: 248.17 }] })
    expect(near).toContain(246.065)
    expect(near).toContain(248.17)
    const far = overlayPricesNearCandles(lo, hi, { levels: { support: 200 } })
    expect(far).toHaveLength(0)
  })
})

describe('axis ticks', () => {
  it('generates price axis labels across domain', () => {
    const ticks = generatePriceTicks(246, 249)
    expect(ticks.length).toBeGreaterThan(2)
    expect(ticks[0]).toBeGreaterThanOrEqual(246)
  })

  it('generates NY time labels at sensible intervals', () => {
    const many = []
    for (let m = 9 * 60 + 30; m <= 12 * 60; m += 5) {
      const h = Math.floor(m / 60)
      const min = m % 60
      const label = `${String(h).padStart(2, '0')}:${String(min).padStart(2, '0')}`
      many.push({
        ts_utc: `2026-07-13T${String(h + 4).padStart(2, '0')}:${String(min).padStart(2, '0')}:00`,
        ts_ny: `2026-07-13T${label}:00`,
      })
    }
    const ticks = generateTimeTicksNy(many, 30)
    expect(ticks.some((t) => t.label === '09:30')).toBe(true)
    expect(ticks.some((t) => t.label === '10:00')).toBe(true)
    expect(ticks.length).toBeLessThan(20)
  })
})

describe('fit modes', () => {
  const all = []
  for (let i = 0; i < 78; i += 1) {
    const mins = 9 * 60 + 30 + i * 5
    const h = Math.floor(mins / 60)
    const m = mins % 60
    all.push({
      ts_utc: `2026-07-13T${String(h + 4).padStart(2, '0')}:${String(m).padStart(2, '0')}:00`,
      ts_ny: `2026-07-13T${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:00`,
      low: 246,
      high: 247,
    })
  }

  it('fit session returns all bars', () => {
    const { bars } = sliceBarsForFitMode(all, 'session', null)
    expect(bars.length).toBe(78)
  })

  it('fit trade windows around entry and exit', () => {
    const trade = {
      entry_ts: '2026-07-13T14:25:00',
      exit_ts: '2026-07-13T15:45:00',
    }
    const { bars } = sliceBarsForFitMode(all, 'trade', trade)
    expect(bars.length).toBeLessThan(78)
    expect(bars.length).toBeGreaterThan(10)
    expect(bars[0].ts_utc).toBe('2026-07-13T13:55:00')
  })
})

describe('dossier level legend', () => {
  it('marks far levels off-screen', () => {
    const meta = dossierLevelsWithVisibility({ support: 220, resistance: 248.5 }, 246, 249)
    const support = meta.find((m) => m.key === 'support')
    expect(support.inView).toBe(false)
    expect(support.offScreenHint).toMatch(/below visible range/)
  })
})
