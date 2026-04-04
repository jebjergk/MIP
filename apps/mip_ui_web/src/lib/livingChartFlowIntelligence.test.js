import { describe, it, expect } from 'vitest'
import {
  computeFlowIntelligenceRaw,
  applyFlowVisibleState,
  normalizeSeries,
  FLOW_STRIP_PREFIX_MOVE_QUALITY,
} from './livingChartFlowIntelligence'
import { selectFlowChipsForChart } from './livingChartVisualState'

function makeBars(n, { trend = 0 } = {}) {
  const bars = []
  const base = 100
  for (let i = 0; i < n; i += 1) {
    const c = base + i * trend
    const ts = new Date(Date.UTC(2024, 0, 1, 14, 0, i * 30)).toISOString()
    bars.push({
      ts,
      open: c - 0.02,
      high: c + 0.15,
      low: c - 0.15,
      close: c,
      volume: 1_000_000 + i * 1000,
    })
  }
  return bars
}

const tileStock = {
  market_type: 'STOCK',
  chart: { bar_seconds: 30, interval_minutes: 0 },
}

describe('livingChartFlowIntelligence', () => {
  it('returns UNAVAILABLE when fewer than min_bars', () => {
    const bars = makeBars(10)
    const raw = computeFlowIntelligenceRaw({
      bars,
      tile: tileStock,
      liveState: {},
      liveUpdatedAt: new Date().toISOString(),
    })
    expect(raw.confidence).toBe('UNAVAILABLE')
    expect(raw.flow_direction).toBe('UNAVAILABLE')
  })

  it('is deterministic for the same inputs', () => {
    const bars = makeBars(40, { trend: 0.01 })
    const liveUpdatedAt = '2024-01-01T15:00:00.000Z'
    const a = computeFlowIntelligenceRaw({
      bars,
      tile: tileStock,
      liveState: { derived_features: {} },
      liveUpdatedAt,
    })
    const b = computeFlowIntelligenceRaw({
      bars,
      tile: tileStock,
      liveState: { derived_features: {} },
      liveUpdatedAt,
    })
    expect(a.flow_direction).toBe(b.flow_direction)
    expect(a.move_efficiency).toBe(b.move_efficiency)
    expect(a.primary_quality_key).toBe(b.primary_quality_key)
    expect(a.scores.efficiency_ratio).toBe(b.scores.efficiency_ratio)
  })

  it('normalizeSeries handles empty bars', () => {
    const s = normalizeSeries([])
    expect(s.tMs.length).toBe(0)
  })

  it('hysteresis converges after repeated confirmations with stable raw', () => {
    const bars = makeBars(40, { trend: 0.02 })
    const liveUpdatedAt = '2024-01-01T15:00:00.000Z'
    const raw = computeFlowIntelligenceRaw({
      bars,
      tile: tileStock,
      liveState: {},
      liveUpdatedAt,
    })
    expect(raw.confidence).not.toBe('UNAVAILABLE')
    let h = null
    let last = null
    for (let i = 0; i < 6; i += 1) {
      last = applyFlowVisibleState(h, raw)
      h = last.hysteresis
    }
    const vNext = applyFlowVisibleState(h, raw)
    expect(vNext.quality_key_visible).toBe(last.quality_key_visible)
  })

  it('strip line2 always uses Move quality prefix', () => {
    const bars = makeBars(40)
    const raw = computeFlowIntelligenceRaw({
      bars,
      tile: tileStock,
      liveState: {},
      liveUpdatedAt: new Date().toISOString(),
    })
    const vis = applyFlowVisibleState(null, raw)
    expect(vis.line2.startsWith(FLOW_STRIP_PREFIX_MOVE_QUALITY)).toBe(true)
  })
})

describe('selectFlowChipsForChart', () => {
  it('returns at most one chip when condition chips exist', () => {
    const vis = {
      confidence: 'HIGH',
      quality_key_visible: 'inefficient',
    }
    const chips = selectFlowChipsForChart(vis, 1)
    expect(chips.length).toBeLessThanOrEqual(1)
  })
})
