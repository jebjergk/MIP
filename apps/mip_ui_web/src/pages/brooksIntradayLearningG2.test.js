import { describe, expect, it, vi } from 'vitest'
import {
  normalizeBarTs,
  scrollRowIntoView,
  narrativeSymbolMatches,
  findNarrativeForBar,
} from './brooksLearningBarSelection'

const ENTRY_TS = '2026-07-13T14:25:00'
const GRID_TS = '2026-07-13 14:25:00'

const payload = {
  symbol: 'AMZN',
  bar_narratives_by_ts: {
    '2026-07-13T14:25:00': {
      time_ny: '10:25',
      headline: 'Entry conditions became valid',
      story: {
        market_story: 'The earlier breakout had pulled back rather than continuing straight upward.',
        system_noticed: ['The setup was already armed before this bar.'],
        decision: 'Buy 4 AMZN shares at $246.53.',
        position_risk: [
          'Initial structural stop: $246.065.',
          'Initial risk: approximately $0.465 per share and $1.86 total.',
        ],
      },
      technical: { symbol: 'AMZN' },
    },
    '2026-07-13T15:20:00': {
      time_ny: '11:20',
      headline: 'Profit protection increased',
      story: {
        system_noticed: ['The structural swing low was confirmed on the previous bar.'],
        decision: 'Raise the active stop to $248.17.',
        stop_timeline: [{ label: 'Stop adjustment active' }],
      },
      technical: { symbol: 'AMZN' },
    },
    '2026-07-13T15:45:00': {
      time_ny: '11:45',
      headline: 'Trade closed at the protective stop',
      story: {
        decision: 'Exit 4 shares at $248.25.',
        position_risk: [
          'Realized profit: +$6.88.',
          'Peak unrealized profit had been +$11.28 at 11:05 New York.',
        ],
      },
      technical: { symbol: 'AMZN' },
    },
  },
}

describe('shared bar selection timestamp', () => {
  it('candle click and grid click normalize to the same key', () => {
    expect(normalizeBarTs(ENTRY_TS)).toBe(normalizeBarTs(GRID_TS))
  })

  it('resolves narrative from payload by normalized key', () => {
    const narr = findNarrativeForBar(payload, GRID_TS)
    expect(narr?.headline).toBe('Entry conditions became valid')
  })
})

describe('scrollRowIntoView', () => {
  it('calls scrollTo on grid container', () => {
    const scrollTo = vi.fn()
    scrollRowIntoView({ offsetTop: 300, clientHeight: 20 }, { clientHeight: 100, scrollTo })
    expect(scrollTo).toHaveBeenCalled()
  })
})

describe('canonical AMZN narratives in payload', () => {
  it('10:25 entry and initial risk', () => {
    const n = payload.bar_narratives_by_ts['2026-07-13T14:25:00']
    expect(n.story.decision).toContain('246.53')
    expect(n.story.position_risk.join(' ')).toContain('246.065')
    expect(n.story.market_story).not.toContain('6.88')
  })

  it('11:20 stop activation and no-lookahead', () => {
    const n = payload.bar_narratives_by_ts['2026-07-13T15:20:00']
    expect(n.story.decision).toContain('248.17')
    expect(n.story.system_noticed.join(' ')).toMatch(/previous bar/i)
    expect(n.story.stop_timeline?.length).toBeGreaterThan(0)
  })

  it('11:45 exit and realized P/L', () => {
    const n = payload.bar_narratives_by_ts['2026-07-13T15:45:00']
    expect(n.story.decision).toContain('248.25')
    expect(n.story.position_risk.join(' ')).toContain('6.88')
    expect(n.story.position_risk.join(' ')).toContain('11.28')
  })

  it('ordinary hold bar stays restrained', () => {
    const hold = {
      headline: 'Hold position',
      story: { market_story: 'The trade remains open.', decision: 'Hold the open trade' },
    }
    expect(hold.headline).toBe('Hold position')
    expect(hold.story.market_story).not.toContain('6.88')
  })
})

describe('symbol scoping', () => {
  it('AAPL workspace rejects AMZN narrative', () => {
    const amznNarr = payload.bar_narratives_by_ts['2026-07-13T14:25:00']
    expect(narrativeSymbolMatches({ symbol: 'AAPL' }, amznNarr)).toBe(false)
    expect(narrativeSymbolMatches({ symbol: 'AMZN' }, amznNarr)).toBe(true)
  })
})

describe('disclosure structure', () => {
  it('narrative includes evidence and technical buckets', () => {
    const n = {
      evidence: [{ code: 'X', plain: 'Example' }],
      technical: { selected_action: 'HOLD_POSITION' },
    }
    expect(n.evidence).toHaveLength(1)
    expect(n.technical.selected_action).toBe('HOLD_POSITION')
  })
})
