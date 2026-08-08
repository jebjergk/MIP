/** Adviser foundation vs diagnostic legacy catalog selection. */

import { describe, expect, it } from 'vitest'
import { defaultSelectionFromCatalog } from './brooksLabReviewSelection'

describe('brooksLab foundation catalog', () => {
  const foundationCatalog = {
    catalog_mode: 'adviser_foundation',
    diagnostic_legacy: false,
    runs: [
      {
        run_id: 'run-1',
        symbols: ['AMZN'],
        available_chains: [
          {
            context_attempt_id: 'legacy-ctx',
            simulation_attempt_id: 'legacy-sim',
            official: true,
            primary_label: 'Official baseline',
            context_ruleset: 'BROOKS_CONTEXT_RULESET_V0_3',
          },
        ],
        available_symbol_sessions: [{ symbol: 'AMZN', trading_date: '2026-07-13' }],
      },
    ],
    defaults: { run_id: 'run-1', symbol: 'AMZN', trading_date: '2026-07-13', review_mode: 'sessions' },
  }

  it('does not select legacy chain ids in foundation mode', () => {
    const sel = defaultSelectionFromCatalog(foundationCatalog, new URLSearchParams())
    expect(sel.contextAttemptId).toBeNull()
    expect(sel.simulationAttemptId).toBeNull()
    expect(sel.adviserFoundationOnly).toBe(true)
    expect(sel.reviewMode).toBe('sessions')
  })
})
