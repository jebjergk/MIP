/**
 * Trade browser → management review navigation (pure helpers for tests).
 */

export function normalizeTradeSymbol(sym) {
  return String(sym || '').trim().toUpperCase()
}

export function tradeReviewHash(tradeId) {
  if (!tradeId) return ''
  return `#trade-review=${encodeURIComponent(String(tradeId))}`
}

export function parseTradeReviewHash(hash) {
  const raw = String(hash || '').replace(/^#/, '')
  if (!raw.startsWith('trade-review=')) return null
  return decodeURIComponent(raw.slice('trade-review='.length))
}

/**
 * @returns {{ workspaceSymbol: string, switchSymbol: boolean, chainOverride: object | null, hash: string, anchorId: string }}
 */
export function planTradeManagementReviewOpen(trade, { activeSymbol, reviewChainOverride }) {
  const workspaceSymbol = normalizeTradeSymbol(trade?.symbol)
  const active = normalizeTradeSymbol(activeSymbol)
  const chainOverride =
    trade?.context_attempt_id && trade?.simulation_attempt_id
      ? {
        context_attempt_id: trade.context_attempt_id,
        simulation_attempt_id: trade.simulation_attempt_id,
      }
      : reviewChainOverride
  return {
    workspaceSymbol,
    switchSymbol: Boolean(workspaceSymbol && workspaceSymbol !== active),
    chainOverride,
    hash: tradeReviewHash(trade?.trade_id),
    anchorId: 'bil-trade-mgmt-review',
  }
}

export function shouldShowTradeReviewPanel({ selectedTrade, loading, review }) {
  return Boolean(selectedTrade || loading || review?.error || review?.trade_id)
}

export function scrollToTradeReviewAnchor(getElement) {
  const el = typeof getElement === 'function' ? getElement() : getElement
  if (el?.scrollIntoView) {
    el.scrollIntoView({ behavior: 'smooth', block: 'start' })
    return true
  }
  return false
}
