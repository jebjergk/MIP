/**
 * One-shot navigation intent for Brooks Intraday Lab.
 * Prevents scrollIntoView from re-firing on learning/trade data refreshes.
 */

export function createLocatorNavigationIntent(locatorParse, tokenFactory = defaultToken) {
  if (!locatorParse?.run_id || !locatorParse?.symbol) return null
  return {
    kind: 'locator',
    run_id: locatorParse.run_id,
    symbol: locatorParse.symbol,
    bar_ts: locatorParse.bar_ts || null,
    context_attempt_id: locatorParse.context_attempt_id || null,
    simulation_attempt_id: locatorParse.simulation_attempt_id || null,
    token: tokenFactory(),
  }
}

function defaultToken() {
  return `nav-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

export function initialLabNavigationState() {
  return {
    selectedBarTs: null,
    selectedPattern: null,
    patternDetail: null,
    pendingLocatorNavigation: null,
    lastConsumedScrollToken: null,
    reviewChainOverride: null,
  }
}

/**
 * Pure reducer for review-chain / locator navigation side-effects.
 * Scroll decisions are separate — this only manages intent state.
 */
export function reduceLabNavigation(state, action) {
  const prev = state || initialLabNavigationState()
  switch (action?.type) {
    case 'REVIEW_CHAIN_CHANGED': {
      return {
        ...prev,
        reviewChainOverride: action.override ?? null,
        selectedBarTs: null,
        selectedPattern: null,
        patternDetail: null,
        pendingLocatorNavigation: null,
      }
    }
    case 'LOCATOR_CLICKED': {
      const intent = action.intent || createLocatorNavigationIntent(action.locatorParse || {})
      if (!intent) return prev
      return {
        ...prev,
        pendingLocatorNavigation: intent,
        selectedBarTs: intent.bar_ts,
        selectedPattern: null,
        patternDetail: null,
        reviewChainOverride: (intent.context_attempt_id && intent.simulation_attempt_id)
          ? {
            context_attempt_id: intent.context_attempt_id,
            simulation_attempt_id: intent.simulation_attempt_id,
          }
          : null,
      }
    }
    case 'LOCATOR_SCROLL_CONSUMED': {
      const pending = prev.pendingLocatorNavigation
      if (!pending) return prev
      if (action.token && action.token !== pending.token) return prev
      return {
        ...prev,
        pendingLocatorNavigation: null,
        lastConsumedScrollToken: pending.token,
      }
    }
    case 'DATA_REFRESHED':
    case 'ACCOUNT_REFRESHED':
    case 'TRADE_BROWSER_REFRESHED':
      // Data refreshes never create or revive scroll intent.
      return prev
    case 'PAGE_RELOAD':
      return initialLabNavigationState()
    case 'CLEAR_SELECTION':
      return {
        ...prev,
        selectedBarTs: null,
        selectedPattern: null,
        patternDetail: null,
        pendingLocatorNavigation: null,
      }
    default:
      return prev
  }
}

/**
 * Decide whether the UI should call scrollIntoView for an event.
 * Only an explicit locator-ready handoff with unconsumed pending intent may scroll.
 */
export function scrollDecision(state, event) {
  const nav = state || initialLabNavigationState()
  if (event === 'DATA_REFRESHED' || event === 'ACCOUNT_REFRESHED' || event === 'TRADE_BROWSER_REFRESHED') {
    return { scroll: false, reason: 'data_refresh_never_scrolls' }
  }
  if (event === 'REVIEW_CHAIN_CHANGED') {
    return { scroll: false, reason: 'review_chain_keeps_viewport' }
  }
  if (event === 'PAGE_RELOAD') {
    return { scroll: false, reason: 'reload_defaults_official_no_scroll' }
  }
  if (event === 'LOCATOR_READY') {
    const pending = nav.pendingLocatorNavigation
    if (!pending) {
      return { scroll: false, reason: 'no_pending_locator' }
    }
    if (nav.lastConsumedScrollToken && nav.lastConsumedScrollToken === pending.token) {
      return { scroll: false, reason: 'already_consumed' }
    }
    return {
      scroll: true,
      reason: 'explicit_locator_click',
      target: {
        symbol: pending.symbol,
        bar_ts: pending.bar_ts,
        token: pending.token,
      },
    }
  }
  if (event === 'USER_BAR_CLICK') {
    return { scroll: true, reason: 'explicit_bar_click' }
  }
  return { scroll: false, reason: 'unknown_event' }
}

/** Apply a chain change and confirm no scroll is requested. */
export function applyReviewChainChange(state, override) {
  const next = reduceLabNavigation(state, { type: 'REVIEW_CHAIN_CHANGED', override })
  return {
    state: next,
    decision: scrollDecision(next, 'REVIEW_CHAIN_CHANGED'),
  }
}

/** Complete a locator navigation: one scroll decision, then consume intent. */
export function completeLocatorNavigation(state) {
  const decision = scrollDecision(state, 'LOCATOR_READY')
  if (!decision.scroll) {
    return { state, decision, scrolled: false }
  }
  const next = reduceLabNavigation(state, {
    type: 'LOCATOR_SCROLL_CONSUMED',
    token: decision.target.token,
  })
  return { state: next, decision, scrolled: true }
}
