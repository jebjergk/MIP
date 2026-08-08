import { describe, expect, it } from 'vitest'
import {
  filterSimulationEventsForWorkspace,
  sanitizeGridSimulationEffect,
} from './brooksReviewProvenance'

describe('brooksReviewProvenance', () => {
  it('AAPL workspace filters out AMZN simulation events', () => {
    const events = [
      { kind: 'TRADE_ENTRY', symbol: 'AMZN', ts: '2026-07-13T14:25:00', detail: {} },
      { kind: 'TRADE_ENTRY', symbol: 'AAPL', ts: '2026-07-13T15:00:00', detail: {} },
    ]
    const filtered = filterSimulationEventsForWorkspace(events, 'AAPL')
    expect(filtered).toHaveLength(1)
    expect(filtered[0].symbol).toBe('AAPL')
  })

  it('AMZN workspace keeps AMZN events for selected simulation attempt', () => {
    const events = [
      {
        kind: 'TRADE_EXIT',
        symbol: 'AMZN',
        ts: '2026-07-13T15:45:00',
        detail: { simulation_attempt_id: '125eb282-3dc7-41a7-8fbf-602f75ad6b51' },
      },
      {
        kind: 'TRADE_EXIT',
        symbol: 'AMZN',
        ts: '2026-07-13T15:20:00',
        detail: { simulation_attempt_id: '5aa3cd99-b11d-4ba6-b061-4a4f098d4140' },
      },
    ]
    const filtered = filterSimulationEventsForWorkspace(
      events,
      'AMZN',
      '125eb282-3dc7-41a7-8fbf-602f75ad6b51',
    )
    expect(filtered).toHaveLength(1)
    expect(filtered[0].detail.simulation_attempt_id).toBe('125eb282-3dc7-41a7-8fbf-602f75ad6b51')
  })

  it('suppresses AMZN ENTRY on AAPL grid row', () => {
    const row = {
      bar_ts: '2026-07-13T14:25:00',
      simulation_effect: 'ENTRY AMZN qty=4',
    }
    const { simulation_effect } = sanitizeGridSimulationEffect(row, 'AAPL')
    expect(simulation_effect).toBe('—')
  })
})
