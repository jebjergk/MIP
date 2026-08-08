/** G2.4 — shared Learning / Technical review selection and URL sync. */

import { CANONICAL_G1, formatLearningMoney } from './brooksTradeLearningG1'
import { catalogHasRuns } from './brooksLabReviewCatalog'
import { isDiagnosticLegacyFromSearchParams } from './brooksLabDiagnosticMode'

export const REVIEW_MODE_TRADES = 'trades'
export const REVIEW_MODE_SESSIONS = 'sessions'

const MONTHS_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const WEEKDAYS_SHORT = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']

export function formatShortDate(iso) {
  if (!iso) return '—'
  const d = new Date(`${String(iso).slice(0, 10)}T12:00:00`)
  if (Number.isNaN(d.getTime())) return String(iso).slice(0, 10)
  return `${WEEKDAYS_SHORT[d.getDay()]} ${d.getDate()} ${MONTHS_SHORT[d.getMonth()]}`
}

export function formatRunWeekLabel(run) {
  if (!run) return '—'
  const ws = run.week_start
  const we = run.week_end || ws
  if (!ws) return run.run_id?.slice(0, 8) || 'Run'
  const start = new Date(`${ws.slice(0, 10)}T12:00:00`)
  const end = new Date(`${(we || ws).slice(0, 10)}T12:00:00`)
  if (Number.isNaN(start.getTime())) return ws
  const sm = MONTHS_SHORT[start.getMonth()]
  const em = MONTHS_SHORT[end.getMonth()]
  const status = run.status ? String(run.status).charAt(0) + String(run.status).slice(1).toLowerCase() : ''
  if (start.getMonth() === end.getMonth()) {
    return `${start.getDate()}–${end.getDate()} ${sm} ${start.getFullYear()} · ${status}`
  }
  return `${start.getDate()} ${sm} – ${end.getDate()} ${em} ${end.getFullYear()} · ${status}`
}

export function nyTimeRange(entryTs, exitTs) {
  const pick = (ts) => {
    if (!ts) return null
    const s = String(ts).replace(' ', 'T')
    return s.slice(11, 16)
  }
  const a = pick(entryTs)
  const b = pick(exitTs)
  if (a && b) return `${a}–${b}`
  return a || b || ''
}

export function formatTradeOptionLabel(trade) {
  if (!trade) return '—'
  const sym = trade.symbol || '—'
  const day = formatShortDate(trade.trading_date || trade.entry_ts)
  const times = nyTimeRange(trade.entry_ts, trade.exit_ts)
  const pnl = formatLearningMoney(trade.realized_pnl)
  const parts = [sym, day]
  if (times) parts.push(times)
  parts.push(pnl)
  return parts.join(' · ')
}

export function v1SessionKey(session) {
  if (!session) return ''
  return `${session.symbol}|${session.trading_date}|${session.adviser_attempt_id}`
}

export function findV1Session(catalog, symbol, tradingDate, adviserAttemptId) {
  const list = catalog?.v1_validation_sessions || []
  if (adviserAttemptId) {
    const byId = list.find((s) => s.adviser_attempt_id === adviserAttemptId)
    if (byId) return byId
  }
  return list.find(
    (s) => s.symbol === symbol && String(s.trading_date).slice(0, 10) === String(tradingDate).slice(0, 10),
  ) || null
}

export function selectionFromV1Session(session, runId) {
  if (!session) return null
  return {
    runId: runId || session.run_id,
    contextAttemptId: session.adviser_attempt_id,
    simulationAttemptId: session.simulation_attempt_id,
    symbol: session.symbol,
    tradingDate: String(session.trading_date).slice(0, 10),
    tradeId: null,
    reviewMode: REVIEW_MODE_SESSIONS,
    barTs: null,
    adviserFoundationOnly: true,
  }
}

export function chainKey(chain) {
  if (!chain) return ''
  return `${chain.context_attempt_id}|${chain.simulation_attempt_id}`
}

export function findRunInCatalog(catalog, runId) {
  return (catalog?.runs || []).find((r) => r.run_id === runId) || null
}

export function findChainInRun(run, ctxId, simId) {
  const chains = run?.available_chains || []
  return chains.find(
    (c) => c.context_attempt_id === ctxId && c.simulation_attempt_id === simId,
  ) || null
}

export function pickDefaultChainForRun(run, { diagnosticLegacy = false } = {}) {
  const chains = (run?.available_chains || []).filter((c) => !c.disabled)
  if (!diagnosticLegacy) return chains[0] || null
  const canonical = chains.find((c) => c.canonical_pm_certification)
  if (canonical) return canonical
  const official = chains.find((c) => c.official)
  if (official) return official
  return chains[0] || null
}

export function pickDefaultTrade(trades, defaults) {
  if (!trades?.length) return null
  if (defaults?.symbol && defaults?.trading_date) {
    const match = trades.find(
      (t) => t.symbol === defaults.symbol && String(t.trading_date).slice(0, 10) === defaults.trading_date,
    )
    if (match) return match
  }
  return trades[0]
}

export function defaultSelectionFromCatalog(catalog, params) {
  if (!catalogHasRuns(catalog)) return null
  const diagnosticLegacy = catalog?.diagnostic_legacy
    || isDiagnosticLegacyFromSearchParams(params)
  const defs = catalog?.defaults || {}

  if (!diagnosticLegacy && catalog?.catalog_mode === 'adviser_foundation') {
    const v1List = catalog.v1_validation_sessions || []
    if (v1List.length) {
      const pin = v1List.find(
        (s) => s.adviser_attempt_id === defs.adviser_attempt_id,
      ) || v1List[0]
      return {
        runId: pin?.run_id || defs.run_id,
        contextAttemptId: pin?.adviser_attempt_id || defs.adviser_attempt_id,
        simulationAttemptId: pin?.simulation_attempt_id || defs.simulation_attempt_id,
        symbol: pin?.symbol || defs.symbol || 'MCD',
        tradingDate: pin?.trading_date || defs.trading_date || '2026-07-14',
        tradeId: null,
        reviewMode: REVIEW_MODE_SESSIONS,
        barTs: null,
        adviserFoundationOnly: true,
      }
    }
    return {
      runId: defs.run_id,
      contextAttemptId: null,
      simulationAttemptId: null,
      symbol: defs.symbol || 'AAPL',
      tradingDate: defs.trading_date || '2026-07-15',
      tradeId: null,
      reviewMode: REVIEW_MODE_SESSIONS,
      barTs: null,
      adviserFoundationOnly: true,
    }
  }

  if (!diagnosticLegacy && (catalog?.v1_validation_sessions || []).length) {
    const v1List = catalog.v1_validation_sessions
    const pin = v1List.find(
      (s) => s.adviser_attempt_id === defs.adviser_attempt_id,
    ) || v1List[0]
    return {
      runId: pin?.run_id || defs.run_id,
      contextAttemptId: pin?.adviser_attempt_id || defs.adviser_attempt_id,
      simulationAttemptId: pin?.simulation_attempt_id || defs.simulation_attempt_id,
      symbol: pin?.symbol || defs.symbol || 'MCD',
      tradingDate: pin?.trading_date || defs.trading_date || '2026-07-14',
      tradeId: null,
      reviewMode: REVIEW_MODE_SESSIONS,
      barTs: null,
      adviserFoundationOnly: true,
    }
  }

  const run = findRunInCatalog(catalog, defs.run_id || catalog?.runs?.[0]?.run_id)
    || catalog?.runs?.[0]
  if (!run) return null

  if (!diagnosticLegacy && defs.adviser_attempt_id) {
    return {
      runId: defs.run_id || run.run_id,
      contextAttemptId: defs.context_attempt_id || defs.adviser_attempt_id,
      simulationAttemptId: defs.simulation_attempt_id || null,
      symbol: defs.symbol || 'MCD',
      tradingDate: defs.trading_date || '2026-07-14',
      tradeId: null,
      reviewMode: REVIEW_MODE_SESSIONS,
      barTs: null,
      adviserFoundationOnly: true,
    }
  }
  const chain = diagnosticLegacy
    ? (findChainInRun(run, defs.context_attempt_id, defs.simulation_attempt_id)
      || pickDefaultChainForRun(run, { diagnosticLegacy: true }))
    : pickDefaultChainForRun(run, { diagnosticLegacy: false })
  const trades = chain?.available_trades || []
  const sessions = run.available_symbol_sessions || []
  if (!chain && !diagnosticLegacy) {
    const sess = sessions.find(
      (s) => s.symbol === defs.symbol && s.trading_date === defs.trading_date,
    ) || sessions[0]
    return {
      runId: run.run_id,
      contextAttemptId: null,
      simulationAttemptId: null,
      symbol: sess?.symbol || defs.symbol || run.symbols?.[0],
      tradingDate: sess?.trading_date || defs.trading_date,
      tradeId: null,
      reviewMode: REVIEW_MODE_SESSIONS,
      barTs: null,
      adviserFoundationOnly: true,
    }
  }
  const trade = pickDefaultTrade(trades, defs)
  return {
    runId: run.run_id,
    contextAttemptId: chain?.context_attempt_id || defs.context_attempt_id,
    simulationAttemptId: chain?.simulation_attempt_id || defs.simulation_attempt_id,
    symbol: trade?.symbol || defs.symbol || run.symbols?.[0],
    tradingDate: trade?.trading_date || defs.trading_date,
    tradeId: trade?.trade_id || catalog?.canonical_trade_id || null,
    reviewMode: diagnosticLegacy ? REVIEW_MODE_TRADES : (defs.review_mode || REVIEW_MODE_TRADES),
    barTs: null,
    adviserFoundationOnly: !diagnosticLegacy && !chain,
  }
}

export function parseReviewSelectionFromSearchParams(params, catalog) {
  if (!catalogHasRuns(catalog)) {
    return {
      selection: null,
      warnings: ['Review catalog has no runs; cannot restore selection from URL.'],
      fallbackApplied: true,
    }
  }
  const base = defaultSelectionFromCatalog(catalog, params)
  if (!base) {
    return {
      selection: null,
      warnings: ['Could not derive a default review selection from the catalog.'],
      fallbackApplied: true,
    }
  }
  const warnings = []
  let fallback = false

  const diagnosticLegacy = catalog?.diagnostic_legacy
    || isDiagnosticLegacyFromSearchParams(params)

  const runId = params.get('run_id') || base.runId
  let run = findRunInCatalog(catalog, runId)
  if (!run && !diagnosticLegacy && (catalog?.v1_validation_sessions || []).length) {
    run = null
  } else if (!run) {
    warnings.push('Run not found in catalog; using default run.')
    fallback = true
    run = findRunInCatalog(catalog, base.runId)
  }

  const ctxId = params.get('context_attempt_id') || base.contextAttemptId
  const simId = params.get('simulation_attempt_id') || base.simulationAttemptId
  const v1SessionsEarly = catalog?.v1_validation_sessions || []
  const v1ByAttemptId = ctxId
    ? v1SessionsEarly.find((s) => s.adviser_attempt_id === ctxId)
    : null
  let chain = run && ctxId && simId ? findChainInRun(run, ctxId, simId) : null
  if (run && !chain && diagnosticLegacy) {
    warnings.push('Review chain not available for this run; using default chain.')
    fallback = true
    chain = pickDefaultChainForRun(run, { diagnosticLegacy: true })
  } else if (run && !chain && !diagnosticLegacy) {
    chain = null
  }
  if (chain?.disabled) {
    warnings.push('Obsolete review chain is not selectable; using default chain.')
    fallback = true
    chain = pickDefaultChainForRun(run, { diagnosticLegacy })
  }

  const reviewMode = !diagnosticLegacy && (catalog?.v1_validation_sessions || []).length
    ? REVIEW_MODE_SESSIONS
    : (params.get('review_mode') === REVIEW_MODE_SESSIONS
      ? REVIEW_MODE_SESSIONS
      : REVIEW_MODE_TRADES)

  const trades = chain?.available_trades || []
  let tradeId = params.get('trade_id')
  let symbol = (params.get('symbol') || base.symbol || '').toUpperCase()
  let tradingDate = params.get('trading_date') || base.tradingDate
  if (v1ByAttemptId) {
    symbol = v1ByAttemptId.symbol
    tradingDate = v1ByAttemptId.trading_date
  }

  if (reviewMode === REVIEW_MODE_TRADES) {
    let trade = tradeId ? trades.find((t) => t.trade_id === tradeId) : null
    if (!trade && symbol && tradingDate) {
      trade = trades.find(
        (t) => t.symbol === symbol && String(t.trading_date).slice(0, 10) === tradingDate.slice(0, 10),
      )
    }
    if (!trade && trades.length) {
      if (tradeId || params.get('symbol')) {
        warnings.push('Trade not found for this chain; selecting first available trade.')
        fallback = true
      }
      trade = pickDefaultTrade(trades, catalog?.defaults)
    }
    if (trade) {
      tradeId = trade.trade_id
      symbol = trade.symbol
      tradingDate = trade.trading_date
    } else {
      tradeId = null
      if (reviewMode === REVIEW_MODE_TRADES && !trades.length && diagnosticLegacy) {
        warnings.push('No trades for this review chain.')
      }
    }
  } else {
    tradeId = null
    const v1Sessions = catalog?.v1_validation_sessions || []
    const v1Match = v1Sessions.find(
      (s) => s.symbol === symbol && String(s.trading_date).slice(0, 10) === String(tradingDate).slice(0, 10),
    )
    if (v1Match) {
      symbol = v1Match.symbol
      tradingDate = v1Match.trading_date
    } else {
      const sessions = run?.available_symbol_sessions || []
      const sess = sessions.find((s) => s.symbol === symbol && s.trading_date === tradingDate.slice(0, 10))
      if (!sess && sessions.length) {
        if (params.get('symbol') || params.get('trading_date')) {
          warnings.push('Symbol session not found; using first session.')
          fallback = true
        }
        symbol = sessions[0].symbol
        tradingDate = sessions[0].trading_date
      }
    }
  }

  const barTs = params.get('bar_ts') || null
  let contextAttemptIdOut = chain?.context_attempt_id || base.contextAttemptId
  let simulationAttemptIdOut = chain?.simulation_attempt_id || base.simulationAttemptId
  if (reviewMode === REVIEW_MODE_SESSIONS && (catalog?.v1_validation_sessions || []).length) {
    const v1 = catalog.v1_validation_sessions.find(
      (s) => s.symbol === symbol && String(s.trading_date).slice(0, 10) === String(tradingDate).slice(0, 10),
    )
    if (v1) {
      contextAttemptIdOut = v1.adviser_attempt_id
      simulationAttemptIdOut = v1.simulation_attempt_id
    }
  }

  return {
    selection: {
      runId: run?.run_id || base.runId,
      contextAttemptId: contextAttemptIdOut,
      simulationAttemptId: simulationAttemptIdOut,
      symbol,
      tradingDate: tradingDate ? String(tradingDate).slice(0, 10) : base.tradingDate,
      tradeId: reviewMode === REVIEW_MODE_TRADES ? tradeId : null,
      reviewMode,
      barTs,
      adviserFoundationOnly: !diagnosticLegacy,
    },
    warnings,
    fallbackApplied: fallback,
  }
}

export function syncReviewSelectionToUrl(selection, { view } = {}) {
  if (typeof window === 'undefined') return
  const url = new URL(window.location.href)
  const s = selection || {}
  url.searchParams.set('run_id', s.runId || '')
  url.searchParams.set('context_attempt_id', s.contextAttemptId || '')
  url.searchParams.set('simulation_attempt_id', s.simulationAttemptId || '')
  url.searchParams.set('symbol', s.symbol || '')
  url.searchParams.set('trading_date', s.tradingDate || '')
  if (s.reviewMode === REVIEW_MODE_SESSIONS) {
    url.searchParams.set('review_mode', REVIEW_MODE_SESSIONS)
    url.searchParams.delete('trade_id')
  } else {
    url.searchParams.set('review_mode', REVIEW_MODE_TRADES)
    if (s.tradeId) url.searchParams.set('trade_id', s.tradeId)
    else url.searchParams.delete('trade_id')
  }
  if (s.barTs) url.searchParams.set('bar_ts', s.barTs)
  else url.searchParams.delete('bar_ts')
  if (view === 'technical') url.searchParams.set('view', 'technical')
  else url.searchParams.delete('view')
  window.history.replaceState({}, '', url)
}

export function selectionToReviewProps(selection) {
  const s = selection || {}
  return {
    runId: s.runId,
    contextAttemptId: s.contextAttemptId,
    simulationAttemptId: s.simulationAttemptId,
    symbol: s.symbol,
    tradingDate: s.tradingDate,
    tradeId: s.tradeId,
    reviewMode: s.reviewMode || REVIEW_MODE_TRADES,
  }
}

export function selectionStatusLine(selection, catalog) {
  const v1 = findV1Session(catalog, selection?.symbol, selection?.tradingDate)
  const run = findRunInCatalog(catalog, selection?.runId)
  const chain = run
    ? findChainInRun(run, selection?.contextAttemptId, selection?.simulationAttemptId)
    : null
  const sym = selection?.symbol || '—'
  const day = formatShortDate(selection?.tradingDate)
  const chainLabel = v1?.selector_label || v1?.label || chain?.primary_label || 'Adviser V1.0 validation'
  if (catalog?.catalog_mode === 'adviser_foundation' && !catalog?.diagnostic_legacy) {
    return {
      reviewing: `Reviewing ${sym} · ${day}`,
      run: chainLabel,
      chain: v1?.adviser_attempt_id?.slice(0, 8) || 'V1.0',
    }
  }
  const runLabel = formatRunWeekLabel(run)
  return {
    reviewing: `Reviewing ${sym} · ${day}`,
    run: `Run ${runLabel.split(' · ')[0] || runLabel}`,
    chain: chainLabel,
  }
}

export function obsoleteChainSortedLast(chains) {
  const list = [...(chains || [])]
  list.sort((a, b) => {
    const ao = a.obsolete_certification ? 1 : 0
    const bo = b.obsolete_certification ? 1 : 0
    if (ao !== bo) return ao - bo
    return (a.primary_label || '').localeCompare(b.primary_label || '')
  })
  return list
}
