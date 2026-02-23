import { useState, useEffect, useCallback, useRef } from 'react'
import { API_BASE } from '../App'
import useDecisionStream from '../hooks/useDecisionStream'
import useVisibleInterval from '../hooks/useVisibleInterval'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import './DecisionConsole.css'

/* ── helpers ──────────────────────────────────────────────────────── */

function fmtPct(v) { return v != null ? `${(v * 100).toFixed(2)}%` : '—' }
function fmtUsd(v) { return v != null ? `$${Number(v).toFixed(2)}` : '—' }
function fmtNum(v, d = 2) { return v != null ? Number(v).toFixed(d) : '—' }
function fmtMins(m) {
  if (m == null) return '—'
  if (m < 60) return `${m}m`
  const h = Math.floor(m / 60)
  const r = m % 60
  return r ? `${h}h ${r}m` : `${h}h`
}
function fmtTs(ts) {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  } catch { return ts }
}
function fmtDate(ts) {
  if (!ts) return '—'
  try { return new Date(ts).toLocaleDateString() } catch { return ts }
}

const SEVERITY_ICON = { green: '●', yellow: '◆', red: '▲' }
const STAGE_BADGE = {
  'on-track':        { label: 'On Track',        cls: 'dc-badge--green' },
  'exit-triggered':  { label: 'Exit Triggered',  cls: 'dc-badge--red' },
  'exited':          { label: 'Exited',           cls: 'dc-badge--red' },
}
const DECISION_TYPE_LABEL = {
  ENTRY_EVALUATION:      'Entry Evaluation',
  POSITION_MONITOR:      'Monitor',
  EARLY_EXIT_TRIGGER:    'Threshold Exit',
  EXIT_EXECUTED:         'Exit Executed',
  EXIT_SKIPPED:          'Exit Skipped',
}

/* ── Mode tabs ────────────────────────────────────────────────────── */

const MODES = [
  { id: 'positions', label: 'Open Positions' },
  { id: 'live',      label: 'Live Decisions' },
  { id: 'history',   label: 'History' },
]

/* ── Badge ────────────────────────────────────────────────────────── */

function StageBadge({ stage }) {
  const cfg = STAGE_BADGE[stage] || { label: stage || '—', cls: '' }
  return <span className={`dc-badge ${cfg.cls}`}>{cfg.label}</span>
}

/* ── Connection indicator ─────────────────────────────────────────── */

function ConnectionDot({ connected }) {
  return (
    <span className={`dc-conn-dot ${connected ? 'dc-conn-dot--on' : 'dc-conn-dot--off'}`}
          title={connected ? 'Live stream connected' : 'Disconnected'} />
  )
}

/* ── KPI Strip ────────────────────────────────────────────────────── */

function KpiStrip({ heartbeat, positions }) {
  const open = heartbeat?.open ?? positions?.length ?? 0
  const triggered = heartbeat?.triggered ?? 0
  const exited = heartbeat?.exited ?? 0
  return (
    <div className="dc-kpi-strip">
      <div className="dc-kpi"><span className="dc-kpi-val">{open}</span><span className="dc-kpi-label">Open</span></div>
      <div className="dc-kpi dc-kpi--red"><span className="dc-kpi-val">{triggered}</span><span className="dc-kpi-label">Triggered</span></div>
      <div className="dc-kpi dc-kpi--red"><span className="dc-kpi-val">{exited}</span><span className="dc-kpi-label">Exited</span></div>
    </div>
  )
}

/* ── Event Card (story card for the live feed) ────────────────────── */

function EventCard({ event, onSelect }) {
  const sev = event.severity || 'green'
  const icon = SEVERITY_ICON[sev] || '●'
  const typeLabel = DECISION_TYPE_LABEL[event.decision_type] || event.decision_type
  return (
    <div className={`dc-event-card dc-event-card--${sev}`} onClick={() => onSelect?.(event)}>
      <div className="dc-event-card-header">
        <span className={`dc-event-icon dc-event-icon--${sev}`}>{icon}</span>
        <span className="dc-event-symbol">{event.symbol}</span>
        <StageBadge stage={event.stage} />
        <span className="dc-event-type">{typeLabel}</span>
        <span className="dc-event-ts">{fmtTs(event.decision_ts)}</span>
      </div>
      <div className="dc-event-card-body">
        <p className="dc-event-summary">{event.summary}</p>
        {event.metrics && (
          <div className="dc-event-metrics">
            <span>Return: <b>{fmtPct(event.metrics.unrealized_return)}</b></span>
            <span>Target: <b>{fmtPct(event.metrics.target_return)}</b></span>
            {event.metrics.mfe_return != null && <span>MFE: <b>{fmtPct(event.metrics.mfe_return)}</b></span>}
            {event.metrics.pnl_delta != null && <span>Delta: <b>{fmtUsd(event.metrics.pnl_delta)}</b></span>}
          </div>
        )}
      </div>
      <div className="dc-event-card-footer">
        <span className="dc-event-mode">{event.mode}</span>
        <button className="dc-event-trace-btn" onClick={e => { e.stopPropagation(); onSelect?.(event) }}>View trace</button>
      </div>
    </div>
  )
}

/* ── Position Row (for Open Positions mode) ───────────────────────── */

function PositionRow({ pos, onSelect }) {
  const currentReturn = pos.CURRENT_RETURN
  const targetReturn = pos.TARGET_RETURN
  const distance = (currentReturn != null && targetReturn != null)
    ? targetReturn - currentReturn : null
  const stage = (pos.STAGE || 'on-track').toLowerCase()

  return (
    <div className={`dc-pos-row dc-pos-row--${stage === 'on-track' ? 'green' : 'red'}`}
         onClick={() => onSelect?.(pos)}>
      <div className="dc-pos-main">
        <span className="dc-pos-symbol">{pos.SYMBOL}</span>
        <StageBadge stage={stage} />
        <span className="dc-pos-portfolio">{pos.PORTFOLIO_NAME}</span>
      </div>
      <div className="dc-pos-metrics">
        <div className="dc-pos-metric">
          <span className="dc-pos-metric-label">Current</span>
          <span className={`dc-pos-metric-val ${currentReturn > 0 ? 'dc-val--pos' : currentReturn < 0 ? 'dc-val--neg' : ''}`}>
            {fmtPct(currentReturn)}
          </span>
        </div>
        <div className="dc-pos-metric">
          <span className="dc-pos-metric-label">Target</span>
          <span className="dc-pos-metric-val">{fmtPct(targetReturn)}</span>
        </div>
        <div className="dc-pos-metric">
          <span className="dc-pos-metric-label">Distance</span>
          <span className="dc-pos-metric-val">{distance != null ? fmtPct(distance) : '—'}</span>
        </div>
        <div className="dc-pos-metric">
          <span className="dc-pos-metric-label">Time in Trade</span>
          <span className="dc-pos-metric-val">{fmtMins(pos.MINUTES_IN_TRADE)}</span>
        </div>
        <div className="dc-pos-metric">
          <span className="dc-pos-metric-label">MFE</span>
          <span className="dc-pos-metric-val">{pos.MFE_RETURN != null ? fmtPct(pos.MFE_RETURN) : '—'}</span>
        </div>
      </div>
    </div>
  )
}

/* ── Position Inspector (gate trace timeline) ─────────────────────── */

function PositionInspector({ position, onClose }) {
  const [trace, setTrace] = useState(null)
  const [diff, setDiff] = useState(null)
  const [loading, setLoading] = useState(true)
  const [showJson, setShowJson] = useState(null)

  const portfolioId = position?.portfolio_id ?? position?.PORTFOLIO_ID
  const symbol = position?.symbol ?? position?.SYMBOL
  const entryTs = position?.entry_ts ?? position?.ENTRY_TS

  useEffect(() => {
    if (!portfolioId || !symbol || !entryTs) return
    setLoading(true)

    const params = new URLSearchParams({ portfolio_id: portfolioId, symbol, entry_ts: entryTs })

    Promise.all([
      fetch(`${API_BASE}/decisions/position-trace?${params}`).then(r => r.json()),
      fetch(`${API_BASE}/decisions/decision-diff?${params}`).then(r => r.json()).catch(() => null),
    ]).then(([traceData, diffData]) => {
      setTrace(traceData)
      setDiff(diffData?.diff || null)
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [portfolioId, symbol, entryTs])

  if (loading) return <div className="dc-inspector"><LoadingState message="Loading trace..." /></div>

  const timeline = trace?.timeline || []
  const state = trace?.state

  return (
    <div className="dc-inspector">
      <div className="dc-inspector-header">
        <h3>{symbol} — Gate Trace</h3>
        <button className="dc-inspector-close" onClick={onClose}>×</button>
      </div>

      {/* Decision Diff */}
      {diff && (
        <div className="dc-diff-panel">
          <h4>Decision Diff: Exit Now vs Hold</h4>
          <div className="dc-diff-grid">
            <div className="dc-diff-col dc-diff-col--exit">
              <span className="dc-diff-label">Exit Now</span>
              <span className="dc-diff-val">{fmtPct(diff.exit_now_return)}</span>
              <span className="dc-diff-pnl">{fmtUsd(diff.exit_now_pnl)}</span>
            </div>
            <div className="dc-diff-col dc-diff-col--vs">
              <span className="dc-diff-label">Delta</span>
              <span className={`dc-diff-val ${diff.pnl_delta > 0 ? 'dc-val--pos' : diff.pnl_delta < 0 ? 'dc-val--neg' : ''}`}>
                {fmtUsd(diff.pnl_delta)}
              </span>
            </div>
            <div className="dc-diff-col dc-diff-col--hold">
              <span className="dc-diff-label">Hold (Expected)</span>
              <span className="dc-diff-val">{fmtPct(diff.target_return)}</span>
              <span className="dc-diff-pnl">{fmtUsd(diff.hold_expected_pnl)}</span>
            </div>
          </div>
          {diff.bars_remaining != null && (
            <div className="dc-diff-footer">{diff.bars_remaining} bar(s) remaining to horizon</div>
          )}
        </div>
      )}

      {/* Position state summary */}
      {state && (
        <div className="dc-state-summary">
          <span>First Hit: {fmtTs(state.FIRST_HIT_TS) || '—'}</span>
          <span>MFE: {state.MFE_RETURN != null ? fmtPct(state.MFE_RETURN) : '—'}</span>
          <span>Last Evaluated: {fmtTs(state.LAST_EVALUATED_TS) || '—'}</span>
          <span>Exit Fired: {state.EARLY_EXIT_FIRED ? 'Yes' : 'No'}</span>
        </div>
      )}

      {/* Timeline */}
      <div className="dc-timeline">
        {timeline.length === 0 && <EmptyState message="No evaluations yet for this position" />}
        {timeline.map((evt, i) => (
          <div key={evt.event_id || i} className={`dc-timeline-node dc-timeline-node--${evt.severity || 'green'}`}>
            <div className="dc-timeline-dot" />
            <div className="dc-timeline-content">
              <div className="dc-timeline-header">
                <span className="dc-timeline-ts">{fmtTs(evt.bar_close_ts)}</span>
                <StageBadge stage={evt.stage} />
                <span className="dc-timeline-type">{DECISION_TYPE_LABEL[evt.decision_type] || evt.decision_type}</span>
              </div>
              <p className="dc-timeline-summary">{evt.summary}</p>
              <div className="dc-timeline-gates">
                <GatePill label="Threshold" pass={evt.gates?.threshold_reached}
                  val={evt.metrics?.effective_target != null
                    ? `${evt.metrics.multiplier ?? '?'}× (${fmtPct(evt.metrics.effective_target)})`
                    : fmtPct(evt.metrics?.target_return)} />
                <GatePill label="MFE" pass={evt.metrics?.mfe_return > 0}
                  val={fmtPct(evt.metrics?.mfe_return)} />
              </div>
              <button className="dc-json-toggle" onClick={() => setShowJson(showJson === evt.event_id ? null : evt.event_id)}>
                {showJson === evt.event_id ? 'Hide JSON' : 'Advanced'}
              </button>
              {showJson === evt.event_id && (
                <pre className="dc-json-block">{JSON.stringify(evt.reason_codes || evt, null, 2)}</pre>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function GatePill({ label, pass: passed, val }) {
  return (
    <span className={`dc-gate-pill ${passed ? 'dc-gate-pill--pass' : 'dc-gate-pill--fail'}`}>
      <span className="dc-gate-icon">{passed ? '✓' : '✗'}</span>
      <span className="dc-gate-label">{label}</span>
      {val && <span className="dc-gate-val">{val}</span>}
    </span>
  )
}

/* ── Filter Bar ───────────────────────────────────────────────────── */

function FilterBar({ filters, onChange, symbols, portfolios }) {
  return (
    <div className="dc-filter-bar">
      <select value={filters.portfolioId || ''} onChange={e => onChange({ ...filters, portfolioId: e.target.value || null })}>
        <option value="">All Portfolios</option>
        {portfolios.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
      </select>
      <select value={filters.symbol || ''} onChange={e => onChange({ ...filters, symbol: e.target.value || null })}>
        <option value="">All Symbols</option>
        {symbols.map(s => <option key={s} value={s}>{s}</option>)}
      </select>
      {filters.date && (
        <span className="dc-filter-active">
          Date: {filters.date}
          <button onClick={() => onChange({ ...filters, date: null })}>×</button>
        </span>
      )}
    </div>
  )
}

/* ── Main Page ────────────────────────────────────────────────────── */

export default function DecisionConsole() {
  const [mode, setMode] = useState('positions')
  const [positions, setPositions] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedPosition, setSelectedPosition] = useState(null)
  const [autoScroll, setAutoScroll] = useState(true)
  const [filters, setFilters] = useState({ portfolioId: null, symbol: null, date: null })
  const [historyEvents, setHistoryEvents] = useState([])
  const [historyLoading, setHistoryLoading] = useState(false)
  const [pinnedSymbols, setPinnedSymbols] = useState(new Set())
  const feedRef = useRef(null)

  const { events: liveEvents, heartbeat, connected } = useDecisionStream({
    portfolioId: filters.portfolioId ? Number(filters.portfolioId) : null,
    enabled: mode === 'live',
  })

  // Load open positions
  const loadPositions = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/decisions/open-positions`)
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
      const data = await resp.json()
      setPositions(data.positions || [])
      setError(null)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useVisibleInterval(loadPositions, mode === 'positions' ? 1800000 : null)

  useEffect(() => { loadPositions() }, [loadPositions])

  // Load history events
  useEffect(() => {
    if (mode !== 'history') return
    setHistoryLoading(true)
    const params = new URLSearchParams({ limit: '200' })
    if (filters.portfolioId) params.set('portfolio_id', filters.portfolioId)
    if (filters.symbol) params.set('symbol', filters.symbol)
    if (filters.date) params.set('date', filters.date)

    fetch(`${API_BASE}/decisions/events?${params}`)
      .then(r => r.json())
      .then(data => { setHistoryEvents(data.events || []); setHistoryLoading(false) })
      .catch(() => setHistoryLoading(false))
  }, [mode, filters])

  // Auto-scroll live feed
  useEffect(() => {
    if (autoScroll && feedRef.current && mode === 'live') {
      feedRef.current.scrollTop = 0
    }
  }, [liveEvents, autoScroll, mode])

  // Derive filter options
  const allSymbols = [...new Set(positions.map(p => p.SYMBOL))].sort()
  const allPortfolios = [...new Map(positions.map(p => [p.PORTFOLIO_ID, { id: p.PORTFOLIO_ID, name: p.PORTFOLIO_NAME }])).values()]

  // Filter events
  const displayEvents = mode === 'live' ? liveEvents : historyEvents
  const filteredEvents = displayEvents.filter(e => {
    if (filters.symbol && e.symbol !== filters.symbol) return false
    if (filters.portfolioId && String(e.portfolio_id) !== String(filters.portfolioId)) return false
    return true
  })

  // Filter positions
  const filteredPositions = positions.filter(p => {
    if (filters.symbol && p.SYMBOL !== filters.symbol) return false
    if (filters.portfolioId && String(p.PORTFOLIO_ID) !== String(filters.portfolioId)) return false
    return true
  })

  // Sort: pinned first, then by stage severity
  const stagePriority = { 'exited': 0, 'exit-triggered': 1, 'on-track': 2 }
  const sortedPositions = [...filteredPositions].sort((a, b) => {
    const aPinned = pinnedSymbols.has(a.SYMBOL) ? 0 : 1
    const bPinned = pinnedSymbols.has(b.SYMBOL) ? 0 : 1
    if (aPinned !== bPinned) return aPinned - bPinned
    return (stagePriority[a.STAGE?.toLowerCase()] ?? 9) - (stagePriority[b.STAGE?.toLowerCase()] ?? 9)
  })

  const handleSelectEvent = (evt) => {
    setSelectedPosition({
      portfolio_id: evt.portfolio_id,
      symbol: evt.symbol,
      entry_ts: evt.metrics?.entry_ts || positions.find(p => p.SYMBOL === evt.symbol && p.PORTFOLIO_ID === evt.portfolio_id)?.ENTRY_TS,
    })
  }

  const handleSelectPosition = (pos) => {
    setSelectedPosition({
      portfolio_id: pos.PORTFOLIO_ID ?? pos.portfolio_id,
      symbol: pos.SYMBOL ?? pos.symbol,
      entry_ts: pos.ENTRY_TS ?? pos.entry_ts,
      PORTFOLIO_ID: pos.PORTFOLIO_ID,
      SYMBOL: pos.SYMBOL,
      ENTRY_TS: pos.ENTRY_TS,
    })
  }

  const togglePin = (symbol) => {
    setPinnedSymbols(prev => {
      const next = new Set(prev)
      next.has(symbol) ? next.delete(symbol) : next.add(symbol)
      return next
    })
  }

  if (loading && mode === 'positions') return <div className="page"><LoadingState message="Loading positions..." /></div>

  return (
    <div className="page dc-page">
      <div className="dc-header">
        <div className="dc-header-left">
          <h2>Decision Console</h2>
          <ConnectionDot connected={connected || mode !== 'live'} />
        </div>
        <div className="dc-header-right">
          <KpiStrip heartbeat={heartbeat} positions={positions} />
        </div>
      </div>

      {/* Mode tabs */}
      <div className="dc-tabs">
        {MODES.map(m => (
          <button key={m.id}
            className={`dc-tab ${mode === m.id ? 'dc-tab--active' : ''}`}
            onClick={() => setMode(m.id)}>
            {m.label}
            {m.id === 'live' && connected && <span className="dc-tab-live-dot" />}
          </button>
        ))}
        {mode === 'live' && (
          <label className="dc-auto-scroll">
            <input type="checkbox" checked={autoScroll} onChange={e => setAutoScroll(e.target.checked)} />
            Auto-scroll
          </label>
        )}
        {mode === 'history' && (
          <input type="date" className="dc-date-picker"
            value={filters.date || ''}
            onChange={e => setFilters(f => ({ ...f, date: e.target.value || null }))} />
        )}
      </div>

      <FilterBar filters={filters} onChange={setFilters} symbols={allSymbols} portfolios={allPortfolios} />

      {error && <ErrorState message={error} />}

      <div className="dc-body">
        {/* Left pane: feed or positions list */}
        <div className="dc-left-pane">
          {mode === 'positions' && (
            <div className="dc-positions-list">
              {sortedPositions.length === 0 && <EmptyState message="No open daily positions" />}
              {sortedPositions.map((pos, i) => (
                <div key={`${pos.PORTFOLIO_ID}-${pos.SYMBOL}-${i}`} className="dc-pos-wrapper">
                  <button
                    className={`dc-pin-btn ${pinnedSymbols.has(pos.SYMBOL) ? 'dc-pin-btn--active' : ''}`}
                    onClick={e => { e.stopPropagation(); togglePin(pos.SYMBOL) }}
                    title={pinnedSymbols.has(pos.SYMBOL) ? 'Unpin' : 'Pin'}
                  >&#9733;</button>
                  <PositionRow pos={pos} onSelect={handleSelectPosition} />
                </div>
              ))}
            </div>
          )}

          {(mode === 'live' || mode === 'history') && (
            <div className="dc-feed" ref={feedRef}>
              {(mode === 'history' && historyLoading) && <LoadingState message="Loading history..." />}
              {filteredEvents.length === 0 && !historyLoading && (
                <EmptyState message={mode === 'live' ? 'Waiting for decision events...' : 'No events found for this filter'} />
              )}
              {filteredEvents.map((evt, i) => (
                <EventCard key={evt.event_id || i} event={evt} onSelect={handleSelectEvent} />
              ))}
            </div>
          )}
        </div>

        {/* Right pane: inspector */}
        {selectedPosition && (
          <div className="dc-right-pane">
            <PositionInspector position={selectedPosition} onClose={() => setSelectedPosition(null)} />
          </div>
        )}
      </div>
    </div>
  )
}
