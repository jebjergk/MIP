/**
 * Review-only guards: simulation overlays must match workspace symbol and selected sim attempt.
 */

export function normalizeSymbol(sym) {
  return String(sym || '').trim().toUpperCase()
}

export function filterSimulationEventsForWorkspace(events, workspaceSymbol, simulationAttemptId = null) {
  const ws = normalizeSymbol(workspaceSymbol)
  if (!ws) return []
  return (events || []).filter((ev) => {
    if (normalizeSymbol(ev.symbol) !== ws) return false
    if (simulationAttemptId && ev.detail?.simulation_attempt_id) {
      if (String(ev.detail.simulation_attempt_id) !== String(simulationAttemptId)) return false
    }
    return true
  })
}

/**
 * @returns {{ ok: boolean, warning: string | null }}
 */
export function assertEventMatchesWorkspace(event, workspaceSymbol, simulationAttemptId = null) {
  const ws = normalizeSymbol(workspaceSymbol)
  const evSym = normalizeSymbol(event?.symbol)
  if (!ws || !evSym) {
    return { ok: true, warning: null }
  }
  if (evSym !== ws) {
    const msg = `Simulation event symbol ${evSym} does not match workspace ${ws}; overlay suppressed.`
    if (typeof console !== 'undefined' && console.warn) {
      console.warn('[BrooksIntradayLab]', msg, event)
    }
    return { ok: false, warning: msg }
  }
  if (simulationAttemptId && event?.detail?.simulation_attempt_id) {
    if (String(event.detail.simulation_attempt_id) !== String(simulationAttemptId)) {
      const msg = 'Simulation event attempt id does not match selected review chain; overlay suppressed.'
      if (typeof console !== 'undefined' && console.warn) {
        console.warn('[BrooksIntradayLab]', msg, event)
      }
      return { ok: false, warning: msg }
    }
  }
  return { ok: true, warning: null }
}

export function sanitizeGridSimulationEffect(row, workspaceSymbol) {
  const eff = row?.simulation_effect
  if (!eff || eff === '—') return { simulation_effect: eff || '—', warning: null }
  const entryMatch = /^ENTRY\s+(\S+)/i.exec(eff)
  if (entryMatch) {
    const tradeSym = normalizeSymbol(entryMatch[1])
    const ws = normalizeSymbol(workspaceSymbol)
    if (tradeSym && ws && tradeSym !== ws) {
      const warning = `Grid simulation cell suppressed: ${tradeSym} event on ${ws} workspace.`
      if (typeof console !== 'undefined' && console.warn) {
        console.warn('[BrooksIntradayLab]', warning, { bar_ts: row?.bar_ts, simulation_effect: eff })
      }
      return { simulation_effect: '—', warning }
    }
  }
  return { simulation_effect: eff, warning: null }
}

export function sanitizeChartBarSimulation(bar, workspaceSymbol) {
  return sanitizeGridSimulationEffect(bar, workspaceSymbol)
}
