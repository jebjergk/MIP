import { describe, expect, it } from 'vitest'
import {
  applyReviewChainChange,
  completeLocatorNavigation,
  createLocatorNavigationIntent,
  initialLabNavigationState,
  reduceLabNavigation,
  scrollDecision,
} from './brooksLabNavigation'

const ALT = {
  context_attempt_id: 'db054c9a-5a28-43af-b0aa-74e92606902e',
  simulation_attempt_id: 'b3be6da1-ee4a-4b11-970d-dad7fee43bb2',
}

const LOCATOR = {
  run_id: '6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e',
  symbol: 'AMZN',
  bar_ts: '2026-07-13T14:20:00',
  ...ALT,
}

describe('brooksLabNavigation', () => {
  it('selecting alternate chain clears stale trade management state', () => {
    const seeded = {
      ...initialLabNavigationState(),
      reviewChainOverride: ALT,
    }
    const { state } = applyReviewChainChange(seeded, null)
    expect(state.reviewChainOverride).toBeNull()
  })

  it('selecting alternate chain does not force locator scroll and clears stale selection', () => {
    const seeded = {
      ...initialLabNavigationState(),
      selectedBarTs: '2026-07-13T14:20:00',
      selectedPattern: { id: 'p1' },
      pendingLocatorNavigation: createLocatorNavigationIntent(LOCATOR, () => 'stale-token'),
    }
    const { state, decision } = applyReviewChainChange(seeded, ALT)
    expect(decision.scroll).toBe(false)
    expect(state.selectedBarTs).toBeNull()
    expect(state.selectedPattern).toBeNull()
    expect(state.pendingLocatorNavigation).toBeNull()
    expect(state.reviewChainOverride).toEqual(ALT)
  })

  it('data re-renders do not repeatedly scroll', () => {
    let state = reduceLabNavigation(initialLabNavigationState(), {
      type: 'LOCATOR_CLICKED',
      locatorParse: LOCATOR,
    })
    const first = completeLocatorNavigation(state)
    expect(first.scrolled).toBe(true)
    state = first.state

    for (const event of ['DATA_REFRESHED', 'ACCOUNT_REFRESHED', 'TRADE_BROWSER_REFRESHED']) {
      state = reduceLabNavigation(state, { type: event })
      expect(scrollDecision(state, event).scroll).toBe(false)
      expect(completeLocatorNavigation(state).scrolled).toBe(false)
    }
  })

  it('manual scrolling remains possible because refreshes never request scroll', () => {
    const state = {
      ...initialLabNavigationState(),
      selectedBarTs: '2026-07-13T14:20:00',
    }
    // User can scroll freely: no auto-scroll intent is created by refreshes.
    expect(scrollDecision(state, 'DATA_REFRESHED').scroll).toBe(false)
    expect(scrollDecision(reduceLabNavigation(state, { type: 'DATA_REFRESHED' }), 'DATA_REFRESHED').scroll).toBe(false)
  })

  it('clicking a trade locator scrolls exactly once', () => {
    let state = reduceLabNavigation(initialLabNavigationState(), {
      type: 'LOCATOR_CLICKED',
      locatorParse: LOCATOR,
    })
    expect(state.pendingLocatorNavigation?.symbol).toBe('AMZN')
    expect(state.reviewChainOverride).toEqual(ALT)

    const once = completeLocatorNavigation(state)
    expect(once.scrolled).toBe(true)
    expect(once.decision.target.bar_ts).toBe('2026-07-13T14:20:00')
    state = once.state
    expect(state.pendingLocatorNavigation).toBeNull()

    const again = completeLocatorNavigation(state)
    expect(again.scrolled).toBe(false)
  })

  it('subsequent component refreshes do not scroll again after locator consume', () => {
    let state = reduceLabNavigation(initialLabNavigationState(), {
      type: 'LOCATOR_CLICKED',
      locatorParse: LOCATOR,
    })
    state = completeLocatorNavigation(state).state
    state = reduceLabNavigation(state, { type: 'DATA_REFRESHED' })
    state = reduceLabNavigation(state, { type: 'TRADE_BROWSER_REFRESHED' })
    expect(scrollDecision(state, 'LOCATOR_READY').scroll).toBe(false)
    expect(completeLocatorNavigation(state).scrolled).toBe(false)
  })

  it('switching back to official chain clears stale selection/navigation state', () => {
    let state = reduceLabNavigation(initialLabNavigationState(), {
      type: 'LOCATOR_CLICKED',
      locatorParse: LOCATOR,
    })
    const { state: cleared, decision } = applyReviewChainChange(state, null)
    expect(decision.scroll).toBe(false)
    expect(cleared.reviewChainOverride).toBeNull()
    expect(cleared.selectedBarTs).toBeNull()
    expect(cleared.pendingLocatorNavigation).toBeNull()
  })

  it('page reload defaults to official pins with no scroll intent', () => {
    const dirty = reduceLabNavigation(initialLabNavigationState(), {
      type: 'LOCATOR_CLICKED',
      locatorParse: LOCATOR,
    })
    const reloaded = reduceLabNavigation(dirty, { type: 'PAGE_RELOAD' })
    expect(reloaded).toEqual(initialLabNavigationState())
    expect(reloaded.reviewChainOverride).toBeNull()
    expect(scrollDecision(reloaded, 'PAGE_RELOAD').scroll).toBe(false)
  })
})
