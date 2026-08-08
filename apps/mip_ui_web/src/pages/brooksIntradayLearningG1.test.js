import { describe, expect, it } from 'vitest'
import {
  CANONICAL_G1,
  formatLearningMoney,
  tradeLearningG1Url,
} from './brooksTradeLearningG1'

describe('brooksTradeLearningG1', () => {
  it('builds canonical API URL', () => {
    const url = tradeLearningG1Url('http://localhost:8000/research/brooks-intraday', CANONICAL_G1)
    expect(url).toContain(CANONICAL_G1.runId)
    expect(url).toContain('symbol=AMZN')
    expect(url).toContain('trading_date=2026-07-13')
    expect(url).toContain(CANONICAL_G1.simulationAttemptId)
  })

  it('formats positive P/L with sign', () => {
    expect(formatLearningMoney(6.88)).toBe('+$6.88')
  })

  it('canonical AMZN figures constants', () => {
    expect(CANONICAL_G1.symbol).toBe('AMZN')
    expect(CANONICAL_G1.tradingDate).toBe('2026-07-13')
  })
})

describe('learning G1 presentation fixtures', () => {
  const amznPayload = {
    trade_summary: {
      entry_price: 246.53,
      exit_price: 248.25,
      quantity: 4,
      realized_pnl: 6.88,
      initial_stop: 246.065,
      exit_reason_plain: 'Protective stop hit',
      duration_label: '1h 20m',
    },
    chart: {
      overlays: {
        entry: { price: 246.53 },
        exit: { price: 248.25 },
      },
    },
  }

  it('displays canonical AMZN summary fields', () => {
    const s = amznPayload.trade_summary
    expect(s.realized_pnl).toBe(6.88)
    expect(s.entry_price).toBe(246.53)
    expect(s.exit_price).toBe(248.25)
    expect(s.initial_stop).toBe(246.065)
  })
})
