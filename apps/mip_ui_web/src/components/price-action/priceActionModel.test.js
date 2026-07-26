import { describe, expect, it } from 'vitest'
import { buildChartData, normalizeAnalysisResponse, provenanceOf } from './priceActionModel'

describe('priceActionModel', () => {
  it('normalizes geometry and methodologist output', () => {
    const result = normalizeAnalysisResponse({
      status: 'complete',
      cost_usd: 0.0123,
      analysis: {
        symbol: 'AAPL',
        bars: [
          { bar_date: '2026-07-24T00:00:00Z', open: 210, high: 214, low: 209, close: 213 },
        ],
        detected_geometry: {
          swings: [{ date: '2026-07-24', price: 214, swing_type: 'HH' }],
          zones: [{ zone_type: 'support', zone_low: 205, zone_high: 207 }],
          range_boxes: [{ start_date: '2026-07-01', end_date: '2026-07-20', range_low: 200, range_high: 210 }],
        },
        methodologist_interpretation: {
          verdict: 'WAIT FOR LONG CONFIRMATION',
          expert_read: 'Price is testing support.',
          supporting_factors: ['Higher low'],
          warning_factors: ['Resistance overhead'],
          knowledge_concepts: ['Support retest'],
          notes: [{ text: 'Watch the close', date: '2026-07-24', price: 213 }],
        },
      },
    })

    expect(result.symbol).toBe('AAPL')
    expect(result.cost).toBe(0.0123)
    expect(result.swings[0]).toMatchObject({ label: 'HH', provenance: 'detected_geometry' })
    expect(result.zones[0]).toMatchObject({ type: 'support', low: 205, high: 207 })
    expect(result.ranges).toHaveLength(1)
    expect(result.notes[0].provenance).toBe('methodologist_interpretation')
    expect(result.supportingFactors).toEqual(['Higher low'])
  })

  it('computes EMA20 when the API does not supply one', () => {
    const bars = buildChartData([
      { date: '2026-07-23', open: 99, high: 101, low: 98, close: 100 },
      { date: '2026-07-24', open: 100, high: 103, low: 99, close: 102 },
    ])

    expect(bars[0].ema20).toBe(100)
    expect(bars[1].ema20).toBeCloseTo(100.190476, 5)
    expect(bars[1].wick).toEqual([99, 103])
  })

  it('does not present a short recommendation as a verdict', () => {
    const result = normalizeAnalysisResponse({ verdict: 'SHORT BREAKDOWN' })
    expect(result.verdict).toBe('NO LONG SETUP')
  })

  it('keeps annotation provenance to the two supported values', () => {
    expect(provenanceOf({ source: 'methodologist_interpretation' })).toBe('methodologist_interpretation')
    expect(provenanceOf({ source: 'model_geometry_v2' })).toBe('detected_geometry')
  })

  it('normalizes the deployed backend contract and cost caps', () => {
    const result = normalizeAnalysisResponse({
      symbol: 'AAPL',
      bars: [{ date: '2026-07-24', open: 210, high: 214, low: 209, close: 213, ema20: 208 }],
      current_price: 213,
      detected_geometry: {
        swings: [{ date: '2026-07-24', price: 214, label: 'HH', source: 'detected_geometry' }],
        support_resistance_zones: [{
          type: 'SUPPORT', low: 205, high: 207, source: 'detected_geometry',
        }],
      },
      rag_status: 'USED',
      retrieval_count: 3,
      card_count: 2,
      total_chars: 900,
      methodologist_knowledge: [{ concept: 'Support retest' }],
      methodologist: {
        analysis_status: 'OK',
        expert_read: {
          market_context: 'Uptrend with pullback.',
          what_supports_the_long: ['Higher low'],
          what_weakens_the_long: ['Resistance overhead'],
        },
        plain_explanation: { summary: 'Constructive, but wait for confirmation.' },
        verdict: { decision: 'WAIT_PULLBACK', reason_summary: 'Location is extended.' },
        annotations: [],
      },
    })

    expect(result.verdict).toBe('WAIT_PULLBACK')
    expect(result.expertRead.market_context).toBe('Uptrend with pullback.')
    expect(result.simpleExplanation.summary).toContain('Constructive')
    expect(result.supportingFactors).toEqual(['Higher low'])
    expect(result.concepts).toEqual(['Support retest'])
    expect(result).toMatchObject({ ragStatus: 'USED', retrievalCount: 3, cardCount: 2, totalChars: 900 })
  })
})
