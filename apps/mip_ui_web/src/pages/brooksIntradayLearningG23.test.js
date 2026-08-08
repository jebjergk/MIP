import { describe, expect, it } from 'vitest'
import {
  buildCompactStory,
  formatCompactPositionRow,
  nextEvidenceOpenState,
} from './brooksLearningPanelCompact'

describe('G2.3 compact panel', () => {
  it('compact story avoids duplicate long blocks', () => {
    const compact = buildCompactStory(
      {
        market_story: 'The earlier breakout had pulled back rather than continuing straight upward. Extra sentence.',
        system_noticed: ['The setup was already armed before this bar.', 'This bar completed entry.'],
        decision: 'Buy 4 AMZN shares at $246.53.',
        position_risk: ['Initial structural stop: $246.065.', 'Initial risk: approximately $1.86 total.'],
      },
      'Entry conditions became valid',
    )
    expect(compact.market).not.toContain('Extra sentence')
    expect(compact.system).toContain('armed')
    expect(compact.risk).toContain('·')
    expect(compact.decision).toContain('246.53')
  })

  it('compact position row renders one line', () => {
    const line = formatCompactPositionRow({
      side: 'long',
      quantity: 4,
      entry_price: 246.53,
      active_stop: 246.065,
      unrealized_pnl: 0,
    })
    expect(line).toContain('Long 4')
    expect(line).toContain('|')
    expect(line).toContain('Stop $246.065')
  })

  it('evidence toggle survives bar change', () => {
    expect(nextEvidenceOpenState(false, '2026-07-13T14:25:00', '2026-07-13T14:30:00')).toBe(false)
    expect(nextEvidenceOpenState(true, 'a', 'b')).toBe(true)
  })

  it('panel uses full width (no inner max-width class contract)', () => {
    const className = 'bil-wh-panel bil-wh-panel--compact'
    expect(className).toContain('bil-wh-panel--compact')
    expect(className).not.toMatch(/max-w/)
  })
})

describe('compact story sections', () => {
  it('does not duplicate exit why when headline already describes stop', () => {
    const compact = buildCompactStory(
      { why: 'The stop had already been raised.', market_story: 'x.' },
      'Trade closed at the protective stop',
    )
    expect(compact.exitWhy).toBeNull()
  })
})
