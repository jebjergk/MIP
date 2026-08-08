import { describe, expect, it } from 'vitest'
import {
  parseTradeReviewHash,
  planTradeManagementReviewOpen,
  scrollToTradeReviewAnchor,
  shouldShowTradeReviewPanel,
  tradeReviewHash,
} from './brooksTradeManagementReview'

const TRADE = {
  trade_id: 'e0d236d5-c281-4c5c-8479-4c6a10f67356',
  symbol: 'AMZN',
  context_attempt_id: '3defa3de-d699-424a-8ceb-78020b453284',
  simulation_attempt_id: '125eb282-3dc7-41a7-8fbf-602f75ad6b51',
}

describe('brooksTradeManagementReview', () => {
  it('clicking Review plan selects AMZN workspace from AAPL', () => {
    const plan = planTradeManagementReviewOpen(TRADE, {
      activeSymbol: 'AAPL',
      reviewChainOverride: null,
    })
    expect(plan.workspaceSymbol).toBe('AMZN')
    expect(plan.switchSymbol).toBe(true)
    expect(plan.chainOverride.simulation_attempt_id).toBe(TRADE.simulation_attempt_id)
  })

  it('hash encodes selected trade for navigation state', () => {
    const h = tradeReviewHash(TRADE.trade_id)
    expect(parseTradeReviewHash(h)).toBe(TRADE.trade_id)
  })

  it('review panel visible while loading', () => {
    expect(
      shouldShowTradeReviewPanel({
        selectedTrade: TRADE,
        loading: true,
        review: null,
      }),
    ).toBe(true)
  })

  it('scroll anchor helper invokes scrollIntoView on review section', () => {
    let scrolled = false
    const el = {
      scrollIntoView() {
        scrolled = true
      },
    }
    expect(scrollToTradeReviewAnchor(() => el)).toBe(true)
    expect(scrolled).toBe(true)
  })
})
