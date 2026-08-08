import { describe, expect, it } from 'vitest'
import { CANONICAL_G1 } from './brooksTradeLearningG1'
import {
  CANONICAL_RUN_ID,
  catalogErrorFromHttp,
  catalogHasRuns,
  emptyCatalogMessage,
  findAmznTradeInChain,
  findCanonicalPmChain,
  findCanonicalRunEntry,
  normalizeReviewCatalogPayload,
} from './brooksLabReviewCatalog'
import {
  REVIEW_MODE_SESSIONS,
  REVIEW_MODE_TRADES,
  defaultSelectionFromCatalog,
  parseReviewSelectionFromSearchParams,
} from './brooksLabReviewSelection'

const SAMPLE_CATALOG = {
  defaults: {
    run_id: CANONICAL_G1.runId,
    context_attempt_id: CANONICAL_G1.contextAttemptId,
    simulation_attempt_id: CANONICAL_G1.simulationAttemptId,
    symbol: 'AMZN',
    trading_date: '2026-07-13',
  },
  canonical_trade_id: 'e0d236d5-c281-4c5c-8479-4c6a10f67356',
  runs: [
    {
      run_id: CANONICAL_G1.runId,
      week_start: '2026-07-13',
      week_end: '2026-07-17',
      status: 'COMPLETED',
      symbols: ['AAPL', 'AMZN', 'JPM', 'MCD'],
      has_learning_view_data: true,
      available_symbol_sessions: [{ symbol: 'AMZN', trading_date: '2026-07-13' }],
      available_chains: [
        {
          context_attempt_id: CANONICAL_G1.contextAttemptId,
          simulation_attempt_id: CANONICAL_G1.simulationAttemptId,
          primary_label: 'Canonical position-management review · +$6.88',
          canonical_pm_certification: true,
          available_trades: [
            {
              trade_id: 'e0d236d5-c281-4c5c-8479-4c6a10f67356',
              symbol: 'AMZN',
              trading_date: '2026-07-13',
              entry_ts: '2026-07-13T14:25:00',
              exit_ts: '2026-07-13T15:45:00',
              realized_pnl: 6.88,
            },
          ],
        },
      ],
    },
  ],
}

describe('brooksLabReviewCatalog', () => {
  it('accepts successful non-empty catalog', () => {
    const cat = normalizeReviewCatalogPayload(SAMPLE_CATALOG)
    expect(catalogHasRuns(cat)).toBe(true)
    expect(findCanonicalRunEntry(cat)?.run_id).toBe(CANONICAL_RUN_ID)
    const pm = findCanonicalPmChain(findCanonicalRunEntry(cat))
    expect(pm?.simulation_attempt_id).toBe(CANONICAL_G1.simulationAttemptId)
    expect(findAmznTradeInChain(pm)?.realized_pnl).toBe(6.88)
  })

  it('maps API 404 to actionable message', () => {
    expect(catalogErrorFromHttp(404, {})).toMatch(/404/)
  })

  it('maps API 500', () => {
    expect(catalogErrorFromHttp(500, { detail: 'db down' })).toMatch(/500|db down/)
  })

  it('treats adviser foundation with zero sessions as usable catalog', () => {
    const cat = normalizeReviewCatalogPayload({
      runs: [],
      defaults: { run_id: 'r1', symbol: 'AAPL', trading_date: '2026-07-15' },
      catalog_mode: 'adviser_foundation',
      v1_validation_sessions: [],
    })
    expect(catalogHasRuns(cat)).toBe(true)
    expect(emptyCatalogMessage(cat)).toBeNull()
  })

  it('detects empty legacy catalog', () => {
    const cat = normalizeReviewCatalogPayload({ runs: [], defaults: {} })
    expect(catalogHasRuns(cat)).toBe(false)
    expect(emptyCatalogMessage(cat)).toMatch(/no runs/i)
  })

  it('rejects malformed response', () => {
    expect(normalizeReviewCatalogPayload({ defaults: {} })).toBeNull()
    expect(normalizeReviewCatalogPayload(null)).toBeNull()
  })
})

describe('brooksLabReviewSelection with catalog', () => {
  it('defaults only when catalog contains canonical run', () => {
    const sel = defaultSelectionFromCatalog(SAMPLE_CATALOG)
    expect(sel?.runId).toBe(CANONICAL_G1.runId)
    expect(sel?.symbol).toBe('AMZN')
  })

  it('placeholder selection when adviser foundation has no sessions', () => {
    const cat = {
      catalog_mode: 'adviser_foundation',
      v1_validation_sessions: [],
      defaults: { run_id: 'r1', symbol: 'AMZN', trading_date: '2026-07-15' },
      runs: [],
    }
    const sel = defaultSelectionFromCatalog(cat)
    expect(sel?.contextAttemptId).toBeNull()
    expect(sel?.symbol).toBe('AMZN')
  })

  it('does not fabricate selection when legacy catalog is empty', () => {
    expect(defaultSelectionFromCatalog({ runs: [], defaults: {} })).toBeNull()
    const { selection } = parseReviewSelectionFromSearchParams(new URLSearchParams(), { runs: [] })
    expect(selection).toBeNull()
  })
})

describe('brooksIntradayLearningG24 catalog integration', () => {
  it('URL restore requires catalog runs', () => {
    const params = new URLSearchParams({
      run_id: CANONICAL_G1.runId,
      symbol: 'AMZN',
      trading_date: '2026-07-13',
      review_mode: REVIEW_MODE_TRADES,
    })
    const { selection } = parseReviewSelectionFromSearchParams(params, SAMPLE_CATALOG)
    expect(selection?.symbol).toBe('AMZN')
  })

  it('URL restore binds symbol/date to v1 attempt id', () => {
    const catalog = {
      catalog_mode: 'adviser_foundation',
      v1_validation_sessions: [
        {
          symbol: 'AAPL',
          trading_date: '2026-07-13',
          adviser_attempt_id: '8ac63a4f-b8d4-40ae-bfef-0c660c0f15e1',
          simulation_attempt_id: 'sim-a',
          run_id: 'run-a',
        },
      ],
      defaults: { run_id: 'run-a' },
      runs: [],
    }
    const params = new URLSearchParams({
      context_attempt_id: '8ac63a4f-b8d4-40ae-bfef-0c660c0f15e1',
      simulation_attempt_id: 'sim-a',
      symbol: 'MCD',
      trading_date: '2026-07-14',
      review_mode: REVIEW_MODE_SESSIONS,
    })
    const { selection } = parseReviewSelectionFromSearchParams(params, catalog)
    expect(selection?.symbol).toBe('AAPL')
    expect(selection?.tradingDate).toBe('2026-07-13')
  })
})
