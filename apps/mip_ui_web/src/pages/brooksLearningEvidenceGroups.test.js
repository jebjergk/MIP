import { describe, expect, it } from 'vitest'
import { buildEvidenceGroups, formatEvidenceGroupsForDisplay } from './brooksLearningEvidenceGroups'

describe('grouped evidence', () => {
  const narrative1120 = {
    headline: 'Profit protection increased',
    evidence: [
      { code: 'BEAR_BAR', plain: 'Bear bar' },
      { code: 'CLOSE_NEAR_LOW', plain: 'Close near low' },
      { code: 'STRUCTURAL_SWING_LOW', plain: 'A meaningful higher low was confirmed', lifecycle: 'CONFIRMED' },
      { code: 'HIGHER_LOW', plain: 'Higher low' },
      { code: 'POSSIBLE_H1_LONG', plain: 'Possible H1 long' },
      { code: 'CONSECUTIVE_BEAR_BARS', plain: 'Consecutive bear bars' },
    ],
    technical: {
      symbol: 'AMZN',
      objective_terms: [{ term: 'BEAR_MICRO_CHANNEL' }],
      active_patterns: [{ pattern_family: 'LOCAL_DOUBLE_BOTTOM', lifecycle: 'DEVELOPING' }],
    },
  }

  it('puts key conclusion first', () => {
    const groups = formatEvidenceGroupsForDisplay(buildEvidenceGroups(narrative1120))
    expect(groups[0].title).toBe('Key conclusion')
    expect(groups[0].items.some((x) => /higher low/i.test(typeof x === 'string' ? x : x.plain))).toBe(true)
  })

  it('includes interpretation for trail bar', () => {
    const bundle = buildEvidenceGroups(narrative1120)
    expect(bundle.interpretation).toMatch(/higher structural low|protective stop/i)
  })

  it('raw codes remain in technical bucket on narrative', () => {
    expect(narrative1120.technical.active_patterns[0].pattern_family).toBe('LOCAL_DOUBLE_BOTTOM')
  })
})
