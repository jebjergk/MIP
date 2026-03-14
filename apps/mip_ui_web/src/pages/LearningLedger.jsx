import { useCallback, useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../App'
import LoadingState from '../components/LoadingState'
import ErrorState from '../components/ErrorState'
import EmptyState from '../components/EmptyState'
import './LearningLedger.css'

function fmtTs(ts) {
  if (!ts) return '—'
  try {
    return new Date(ts).toLocaleString()
  } catch {
    return ts
  }
}

function fmtPct(v, digits = 2) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${(n * 100).toFixed(digits)}%`
}

function severityClass(sev) {
  const s = (sev || '').toLowerCase()
  if (s === 'high') return 'ledger-chip ledger-chip-high'
  if (s === 'medium') return 'ledger-chip ledger-chip-medium'
  return 'ledger-chip ledger-chip-info'
}

function latestChainEvent(chain) {
  const list = chain?.events || []
  return list.length ? list[list.length - 1] : null
}

function prettyEventName(name) {
  if (!name) return 'Unknown event'
  return String(name).replaceAll('_', ' ').toLowerCase().replace(/\b\w/g, (m) => m.toUpperCase())
}

function normalizeStatus(v, fallback = 'INFO') {
  const s = String(v || fallback).toUpperCase()
  if (s === 'SUCCESS' || s === 'INFO' || s === 'APPROVED' || s === 'EXECUTED') return 'SUCCESS'
  if (s.includes('FAIL') || s.includes('ERROR') || s === 'REJECTED') return 'FAIL'
  if (s.includes('SKIP') || s.includes('BLOCK') || s.includes('FALLBACK')) return 'BLOCKED'
  return s
}

function summarizeChain(selectedEvent, detail) {
  if (!selectedEvent) return 'Select a chain to see what changed and why.'
  const status = normalizeStatus(selectedEvent.status || selectedEvent.severity || 'INFO')
  const name = prettyEventName(selectedEvent.event_name || selectedEvent.title)
  const proposalCount = detail?.summary?.proposal_count ?? selectedEvent?.impact?.proposal_count
  const executed = detail?.summary?.filled_or_partial_orders ?? selectedEvent?.impact?.executed_count
  return `${name} ended as ${status}. Proposals: ${proposalCount ?? 0}. Executed/filled signals: ${executed ?? 0}.`
}

function getPortfolioIds(chain) {
  const ids = new Set((chain?.events || []).map((e) => e.portfolio_id).filter((v) => v != null))
  return Array.from(ids).sort((a, b) => Number(a) - Number(b))
}

function chainScopeLabel(chain) {
  if (!chain?.chain_key) return 'General chain'
  if (chain.chain_key.startsWith('action::')) return 'Live action chain'
  if (chain.chain_key.startsWith('proposal::')) return 'Proposal chain'
  if (chain.chain_key.startsWith('run::')) return 'Run chain'
  if (chain.chain_key.startsWith('ledger::')) return 'Ledger event chain'
  return 'General chain'
}

function eventMeaning(eventName) {
  const k = String(eventName || '').toUpperCase()
  if (k === 'PROPOSAL_SELECTION') return 'System selected which candidates became proposals.'
  if (k === 'PROPOSAL_VALIDATION_EXECUTION') return 'System validated proposals against rules and attempted execution.'
  if (k === 'LIVE_EXECUTION_BLOCKED') return 'Execution was blocked by constraints or risk gates.'
  if (k === 'LIVE_REVALIDATION') return 'System rechecked live execution eligibility before sending.'
  if (k === 'TRAINING_DIGEST_SNAPSHOT') return 'Learning snapshot changed trust/confidence state.'
  if (k === 'SP_AGENT_PROPOSE_TRADES') return 'Proposal generation step from deterministic signals.'
  return 'One logged step in the learning -> decision -> execution process.'
}

function summarizeInfluence(delta) {
  if (!delta || typeof delta !== 'object') return []
  const lines = []
  if (delta.eligibility_changed != null) lines.push(`Eligibility changed: ${delta.eligibility_changed ? 'yes' : 'no'}`)
  if (delta.ranking_adjustment_active != null) lines.push(`Ranking adjustment active: ${delta.ranking_adjustment_active ? 'yes' : 'no'}`)
  if (delta.size_constraints_applied != null) lines.push(`Size constraints applied: ${delta.size_constraints_applied ? 'yes' : 'no'}`)
  if (delta.trusted_rejected_count != null) lines.push(`Trusted-rejected count: ${delta.trusted_rejected_count}`)
  if (delta.live_execution_candidates != null) lines.push(`Live execution candidates: ${delta.live_execution_candidates}`)
  if (delta.sim_committee_applied != null) lines.push(`Committee applied: ${delta.sim_committee_applied ? 'yes' : 'no'}`)
  if (delta.max_position_pct != null) lines.push(`Max position pct: ${fmtPct(delta.max_position_pct)}`)
  if (delta.target_weight != null) lines.push(`Target weight: ${fmtPct(delta.target_weight)}`)
  return lines
}

function parseMaybeJson(v) {
  if (v == null) return null
  if (typeof v === 'object') return v
  if (typeof v === 'string') {
    try {
      return JSON.parse(v)
    } catch {
      return null
    }
  }
  return null
}

function field(row, ...keys) {
  for (const key of keys) {
    if (row?.[key] != null) return row[key]
  }
  return null
}

function proposalWhyText(proposal) {
  const source = parseMaybeJson(field(proposal, 'SOURCE_SIGNALS', 'source_signals')) || {}
  const rationale = parseMaybeJson(field(proposal, 'RATIONALE', 'rationale')) || {}
  const committee = rationale?.sim_committee || {}
  const parts = []
  if (source.trust_label) parts.push(`trust ${String(source.trust_label).toLowerCase()}`)
  if (source.score != null) parts.push(`score ${Number(source.score).toFixed(2)}`)
  if (source.pattern_id) parts.push(`pattern ${source.pattern_id}`)
  if (source.news_context?.news_context_badge) parts.push(`news ${String(source.news_context.news_context_badge).toLowerCase()}`)
  if (committee.should_enter === false) parts.push('committee blocked entry')
  if (committee.size_factor != null) parts.push(`size factor ${committee.size_factor}`)
  if (committee.summary) parts.push(String(committee.summary))
  if (!parts.length) return 'No detailed rationale recorded for this proposal.'
  return parts.join(' | ')
}

export default function LearningLedger() {
  const [events, setEvents] = useState([])
  const [chains, setChains] = useState([])
  const [selected, setSelected] = useState(null)
  const [selectedChainKey, setSelectedChainKey] = useState('')
  const [selectedTimelineKey, setSelectedTimelineKey] = useState('')
  const [detail, setDetail] = useState(null)
  const [effectiveness, setEffectiveness] = useState(null)
  const [feedSource, setFeedSource] = useState('derived_fallback')
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)
  const [error, setError] = useState('')
  const [eventTypeFilter, setEventTypeFilter] = useState('')
  const [portfolioFilter, setPortfolioFilter] = useState('')

  const loadFeed = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const params = new URLSearchParams({ limit: '120', group_by_chain: 'true' })
      if (eventTypeFilter) params.set('event_type', eventTypeFilter)
      if (portfolioFilter.trim()) params.set('portfolio_id', portfolioFilter.trim())
      const resp = await fetch(`${API_BASE}/learning-ledger/feed?${params.toString()}`)
      if (!resp.ok) throw new Error(`Failed to load feed (${resp.status})`)
      const data = await resp.json()
      const rows = data?.events || []
      const chainRows = data?.chains || []
      setFeedSource(data?.source || 'derived_fallback')
      setEvents(rows)
      setChains(chainRows)
      const fallbackChains = chainRows.length > 0
        ? chainRows
        : rows.map((e) => ({
          chain_key: e.event_key,
          latest_event_ts: e.event_ts,
          latest_title: e.title,
          latest_summary: e.summary,
          taxonomy_category: e.event_type,
          event_count: 1,
          run_id: e.run_id,
          portfolio_id: e.portfolio_id,
          events: [e],
        }))
      setSelectedChainKey((prev) => {
        if (prev && fallbackChains.some((c) => c.chain_key === prev)) return prev
        return fallbackChains[0]?.chain_key || ''
      })
      setSelected((prev) => {
        const chainEvents = chainRows.flatMap((c) => c.events || [])
        const previous = prev?.event_key
          ? (chainEvents.find((e) => e.event_key === prev.event_key) || rows.find((e) => e.event_key === prev.event_key))
          : null
        if (previous) return previous
        return latestChainEvent(chainRows[0]) || rows[0] || null
      })
    } catch (e) {
      setError(e.message || 'Failed to load learning ledger feed.')
    } finally {
      setLoading(false)
    }
  }, [eventTypeFilter, portfolioFilter])

  useEffect(() => {
    loadFeed()
  }, [loadFeed])

  useEffect(() => {
    if (!selected?.run_id && selected?.ledger_id == null) {
      setDetail(null)
      return
    }
    let cancelled = false
    const loadDetail = async () => {
      setDetailLoading(true)
      try {
        const params = new URLSearchParams()
        if (selected.run_id != null) params.set('run_id', String(selected.run_id))
        if (selected.event_name) params.set('event_name', String(selected.event_name))
        if (selected.portfolio_id != null) params.set('portfolio_id', String(selected.portfolio_id))
        if (selected.ledger_id != null) params.set('ledger_id', String(selected.ledger_id))
        const resp = await fetch(`${API_BASE}/learning-ledger/detail?${params.toString()}`)
        if (!resp.ok) throw new Error(`Failed to load detail (${resp.status})`)
        const data = await resp.json()
        if (!cancelled) setDetail(data)
      } catch {
        if (!cancelled) setDetail(null)
      } finally {
        if (!cancelled) setDetailLoading(false)
      }
    }
    loadDetail()
    return () => {
      cancelled = true
    }
  }, [selected])

  useEffect(() => {
    let cancelled = false
    const loadEffectiveness = async () => {
      try {
        const params = new URLSearchParams({ days: '30' })
        if (portfolioFilter.trim()) params.set('portfolio_id', portfolioFilter.trim())
        const resp = await fetch(`${API_BASE}/learning-ledger/effectiveness?${params.toString()}`)
        if (!resp.ok) return
        const data = await resp.json()
        if (!cancelled) setEffectiveness(data)
      } catch {
        if (!cancelled) setEffectiveness(null)
      }
    }
    loadEffectiveness()
    return () => {
      cancelled = true
    }
  }, [portfolioFilter, events.length])

  const visibleChains = useMemo(() => {
    if (chains.length > 0) return chains
    return events.map((e) => ({
      chain_key: e.event_key,
      latest_event_ts: e.event_ts,
      latest_title: e.title,
      latest_summary: e.summary,
      taxonomy_category: e.event_type,
      event_count: 1,
      run_id: e.run_id,
      portfolio_id: e.portfolio_id,
      events: [e],
    }))
  }, [chains, events])

  const selectedChain = useMemo(
    () => visibleChains.find((c) => c.chain_key === selectedChainKey) || visibleChains[0] || null,
    [visibleChains, selectedChainKey],
  )

  useEffect(() => {
    const e = latestChainEvent(selectedChain)
    if (e) {
      setSelected(e)
      setSelectedTimelineKey(e.event_key || '')
    }
  }, [selectedChain])

  const timelineRows = useMemo(() => {
    if (detail?.causal_chain?.length) {
      return detail.causal_chain.map((r) => ({
        key: `canonical-${r.LEDGER_ID || r.ledger_id}-${r.EVENT_TS || r.event_ts}`,
        eventKey: '',
        ledgerId: r.LEDGER_ID || r.ledger_id,
        ts: r.EVENT_TS || r.event_ts,
        eventName: r.EVENT_NAME || r.event_name,
        eventType: r.EVENT_TYPE || r.event_type || '',
        status: normalizeStatus(r.STATUS || r.status),
        rawStatus: r.STATUS || r.status || '',
        runId: r.RUN_ID || r.run_id || selectedChain?.run_id || '',
        portfolioId: r.PORTFOLIO_ID || r.portfolio_id || selectedChain?.portfolio_id || null,
        action: r.LIVE_ACTION_ID || r.live_action_id || '',
        order: r.LIVE_ORDER_ID || r.live_order_id || '',
        influenceDelta: r.INFLUENCE_DELTA || r.influence_delta || null,
        outcomeState: r.OUTCOME_STATE || r.outcome_state || null,
      }))
    }
    return (selectedChain?.events || []).map((r) => ({
      key: r.event_key,
      eventKey: r.event_key,
      ledgerId: r.ledger_id || null,
      ts: r.event_ts,
      eventName: r.event_name || r.title,
      eventType: r.event_type || '',
      status: normalizeStatus(r.status || r.severity),
      rawStatus: r.status || r.severity || '',
      runId: r.run_id || selectedChain?.run_id || '',
      portfolioId: r.portfolio_id ?? selectedChain?.portfolio_id ?? null,
      action: r.live_action_id || '',
      order: r.live_order_id || '',
      influenceDelta: r.impact || null,
      outcomeState: null,
    }))
  }, [detail, selectedChain])

  useEffect(() => {
    if (!timelineRows.length) {
      setSelectedTimelineKey('')
      return
    }
    setSelectedTimelineKey((prev) => (
      timelineRows.some((r) => r.key === prev) ? prev : timelineRows[timelineRows.length - 1].key
    ))
  }, [timelineRows])

  const selectedTimelineRow = useMemo(
    () => timelineRows.find((r) => r.key === selectedTimelineKey) || timelineRows[timelineRows.length - 1] || null,
    [timelineRows, selectedTimelineKey],
  )

  const influenceLines = useMemo(() => {
    const delta = selectedTimelineRow?.influenceDelta
      || detail?.ledger_event?.INFLUENCE_DELTA
      || detail?.ledger_event?.influence_delta
      || selected?.impact
      || {}
    return summarizeInfluence(delta)
  }, [detail, selected, selectedTimelineRow])

  const explainedProposals = useMemo(() => (
    (detail?.proposals || []).slice(0, 8).map((p) => ({
      id: field(p, 'PROPOSAL_ID', 'proposal_id'),
      symbol: field(p, 'SYMBOL', 'symbol'),
      status: field(p, 'STATUS', 'status'),
      targetWeight: field(p, 'TARGET_WEIGHT', 'target_weight'),
      why: proposalWhyText(p),
    }))
  ), [detail])

  const looksSparse = useMemo(() => {
    const chainEvents = selectedChain?.event_count ?? (selectedChain?.events?.length || 0)
    const summary = detail?.summary || {}
    return !!selectedChain && chainEvents > 1
      && (summary.proposal_count ?? 0) === 0
      && (summary.live_action_count ?? 0) === 0
      && (summary.live_order_count ?? 0) === 0
  }, [selectedChain, detail])

  const stats = useMemo(() => {
    const trainingCount = events.filter((e) => e.event_type === 'TRAINING_EVENT').length
    const decisionCount = events.filter((e) => e.event_type === 'DECISION_EVENT').length
    const highCount = events.filter((e) => (e.severity || '').toLowerCase() === 'high').length
    const newsInfluencedCount = events.filter((e) => e.news_influence_used).length
    return { trainingCount, decisionCount, highCount, newsInfluencedCount, total: events.length }
  }, [events])

  const onSelectTimelineRow = useCallback((row) => {
    setSelectedTimelineKey(row.key)
  }, [])

  if (loading) {
    return (
      <>
        <h1>Learning-to-Decision Ledger</h1>
        <LoadingState />
      </>
    )
  }

  if (error) {
    return (
      <>
        <h1>Learning-to-Decision Ledger</h1>
        <ErrorState message={error} />
      </>
    )
  }

  return (
    <div className="ledger-page">
      <header className="ledger-header">
        <div>
          <h1>Learning-to-Decision Ledger</h1>
          <p className="ledger-subtitle">
            Shows how learning-state changes influence proposal ranking, trust, sizing, live eligibility, and downstream trade outcomes.
          </p>
        </div>
        <button className="ledger-btn" onClick={loadFeed}>Refresh</button>
      </header>

      <section className="ledger-stats">
        <div className="ledger-stat-card"><span>Total events (feed)</span><b>{stats.total}</b></div>
        <div className="ledger-stat-card"><span>Causal chains (feed)</span><b>{visibleChains.length}</b></div>
        <div className="ledger-stat-card"><span>Training events (feed)</span><b>{stats.trainingCount}</b></div>
        <div className="ledger-stat-card"><span>Decision events (feed)</span><b>{stats.decisionCount}</b></div>
        <div className="ledger-stat-card"><span>News-influenced (feed)</span><b>{stats.newsInfluencedCount}</b></div>
        <div className="ledger-stat-card"><span>High severity (feed)</span><b>{stats.highCount}</b></div>
      </section>
      <p className="ledger-note">Feed cards show the latest snapshot window from the feed query, not full-history totals.</p>
      <section className="ledger-source-banner">
        <span>Feed source:</span>
        <b>{feedSource === 'canonical_ledger' ? 'Canonical immutable ledger' : 'Derived fallback (deploy ledger SQL to activate canonical mode)'}</b>
      </section>

      {effectiveness ? (
        <section className="ledger-effectiveness">
          <h3>Resulting Impact (30d)</h3>
          <p className="ledger-note">Portfolio-level 30-day context. This panel is not specific to the currently selected chain/event.</p>
          <div className="ledger-effectiveness-grid">
            <div><span>Proposals</span><b>{effectiveness?.proposal_summary?.PROPOSAL_COUNT ?? effectiveness?.proposal_summary?.proposal_count ?? 0}</b></div>
            <div><span>Executed proposals</span><b>{effectiveness?.proposal_summary?.EXECUTED_COUNT ?? effectiveness?.proposal_summary?.executed_count ?? 0}</b></div>
            <div><span>Live actions</span><b>{effectiveness?.live_summary?.LIVE_ACTION_COUNT ?? effectiveness?.live_summary?.live_action_count ?? 0}</b></div>
            <div><span>Filled/partial orders</span><b>{effectiveness?.live_summary?.FILLED_OR_PARTIAL_COUNT ?? effectiveness?.live_summary?.filled_or_partial_count ?? 0}</b></div>
            <div><span>News-influenced ledger events</span><b>{effectiveness?.news_effectiveness?.NEWS_INFLUENCED_EVENTS ?? effectiveness?.news_effectiveness?.news_influenced_events ?? 0}</b></div>
            <div><span>News-driven blocks</span><b>{effectiveness?.news_effectiveness?.NEWS_BLOCK_EVENTS ?? effectiveness?.news_effectiveness?.news_block_events ?? 0}</b></div>
          </div>
        </section>
      ) : null}

      <section className="ledger-filters">
        <label>
          Event type
          <select value={eventTypeFilter} onChange={(e) => setEventTypeFilter(e.target.value)}>
            <option value="">All</option>
            <option value="TRAINING_EVENT">Training events</option>
            <option value="DECISION_EVENT">Decision events</option>
          </select>
        </label>
        <label>
          Portfolio ID
          <input
            value={portfolioFilter}
            onChange={(e) => setPortfolioFilter(e.target.value)}
            placeholder="Optional portfolio id"
          />
        </label>
      </section>
      <section className="ledger-explainer">
        <b>How to read this page:</b>
        <span>
          A <b>chain</b> is a related group of events (same run/proposal/action). An <b>event</b> is one logged step,
          like proposal selection or validation. Click a chain on the left, then click a timeline row in the middle
          to inspect that exact step on the right.
        </span>
      </section>

      {events.length === 0 ? (
        <EmptyState
          title="No learning ledger events found"
          explanation="No qualifying training/decision events matched the current filter."
          action="Adjust filters or refresh."
        />
      ) : (
        <section className="ledger-workbench">
          <div className="ledger-column ledger-chain-column">
            <h2>Chains</h2>
            <p className="ledger-note">Pick one chain to read a single end-to-end story.</p>
            <div className="ledger-chain-list">
              {visibleChains.map((c) => {
                const e = latestChainEvent(c) || {}
                return (
                  <button
                    type="button"
                    key={c.chain_key}
                    className={`ledger-event ${selectedChain?.chain_key === c.chain_key ? 'ledger-event-selected' : ''}`}
                    onClick={() => setSelectedChainKey(c.chain_key)}
                  >
                    <div className="ledger-event-top">
                      <span className={severityClass(e.severity)}>{(e.severity || 'info').toUpperCase()}</span>
                      <span className="ledger-ts">{fmtTs(c.latest_event_ts || e.event_ts)}</span>
                    </div>
                    <h3>{c.latest_title || e.title || prettyEventName(e.event_name)}</h3>
                    <p>{c.latest_summary || e.summary || 'No summary available.'}</p>
                    <div className="ledger-meta">
                      <span>{chainScopeLabel(c)}</span>
                      <span>{c.taxonomy_category || e.event_type || 'GENERAL'}</span>
                      <span>events: {c.event_count ?? (c.events?.length || 0)}</span>
                      <span>run: {c.run_id || e.run_id || '—'}</span>
                      <span>
                        portfolios: {getPortfolioIds(c).length ? getPortfolioIds(c).join(', ') : (c.portfolio_id ?? e.portfolio_id ?? '—')}
                      </span>
                    </div>
                  </button>
                )
              })}
            </div>
          </div>

          <div className="ledger-column ledger-timeline-column">
            <h2>Timeline</h2>
            <p className="ledger-chain-summary">{summarizeChain(selected, detail)}</p>
            <p className="ledger-note">Click a row to inspect that exact event step.</p>
            {detailLoading ? <LoadingState /> : null}
            {!detailLoading && timelineRows.length === 0 ? (
              <p className="ledger-note">No timeline events found for this chain.</p>
            ) : (
              <ol className="ledger-timeline">
                {timelineRows.map((r, idx) => (
                  <li
                    key={r.key}
                    className={`ledger-timeline-item ${selectedTimelineRow?.key === r.key ? 'ledger-timeline-item-selected' : ''}`}
                    onClick={() => onSelectTimelineRow(r)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') onSelectTimelineRow(r)
                    }}
                    role="button"
                    tabIndex={0}
                  >
                    <div className="ledger-timeline-top">
                      <span className="ledger-ts">{fmtTs(r.ts)}</span>
                      <span className={severityClass(r.status === 'FAIL' ? 'high' : r.status === 'BLOCKED' ? 'medium' : 'info')}>
                        {r.status}
                      </span>
                    </div>
                    <div className="ledger-timeline-name">{prettyEventName(r.eventName)}</div>
                    <div className="ledger-meta">
                      <span>step: {idx + 1}/{timelineRows.length}</span>
                      <span>run: {r.runId || '—'}</span>
                      <span>portfolio: {r.portfolioId ?? '—'}</span>
                      <span>action: {r.action || '—'}</span>
                      <span>order: {r.order || '—'}</span>
                    </div>
                  </li>
                ))}
              </ol>
            )}
          </div>

          <aside className="ledger-column ledger-context-column">
            <h2>Step Inspector</h2>
            {!selected ? (
              <p className="ledger-note">Select a chain to view context.</p>
            ) : (
              <>
                <p className="ledger-chain-summary">
                  <b>{prettyEventName(selectedTimelineRow?.eventName || selected.event_name || selected.title)}</b>: {eventMeaning(selectedTimelineRow?.eventName || selected.event_name)}
                </p>
                <div className="ledger-impact-grid">
                  <div><span>Run</span><b>{selectedTimelineRow?.runId || selected.run_id || '—'}</b></div>
                  <div><span>Time</span><b>{fmtTs(selectedTimelineRow?.ts || selected.event_ts)}</b></div>
                  <div><span>Event</span><b>{prettyEventName(selectedTimelineRow?.eventName || selected.event_name || selected.title)}</b></div>
                  <div><span>Portfolio</span><b>{selectedTimelineRow?.portfolioId ?? selected.portfolio_id ?? '—'}</b></div>
                  <div><span>Status</span><b>{selectedTimelineRow?.status || normalizeStatus(selected.status || selected.severity)}</b></div>
                  <div><span>Type</span><b>{selectedTimelineRow?.eventType || selected.event_type || '—'}</b></div>
                  <div><span>Trusted delta</span><b>{selected.impact?.trusted_delta ?? '—'}</b></div>
                  <div><span>Executed</span><b>{detail?.summary?.executed_proposals ?? selected.impact?.executed_count ?? 0}</b></div>
                  <div><span>Live orders</span><b>{detail?.summary?.live_order_count ?? selected.impact?.live_order_count ?? 0}</b></div>
                  <div><span>Action ID</span><b>{selectedTimelineRow?.action || '—'}</b></div>
                  <div><span>Order ID</span><b>{selectedTimelineRow?.order || '—'}</b></div>
                  <div><span>Avg target weight</span><b>{fmtPct(selected.impact?.avg_target_weight, 1)}</b></div>
                </div>

                <section className="ledger-mini-section">
                  <h3>Causality summary</h3>
                  <ul>
                    <li>Audit events: {detail?.summary?.audit_event_count ?? 0}</li>
                    <li>Proposals: {detail?.summary?.proposal_count ?? 0}</li>
                    <li>Live actions: {detail?.summary?.live_action_count ?? 0}</li>
                    <li>Live orders: {detail?.summary?.live_order_count ?? 0}</li>
                    <li>Filled/partial: {detail?.summary?.filled_or_partial_orders ?? 0}</li>
                  </ul>
                </section>

                <section className="ledger-mini-section">
                  <h3>Affected proposals (and why)</h3>
                  <div className="ledger-table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>ID</th>
                          <th>Symbol</th>
                          <th>Status</th>
                          <th>Weight</th>
                          <th>Why selected/changed</th>
                        </tr>
                      </thead>
                      <tbody>
                        {explainedProposals.map((p) => (
                          <tr key={p.id}>
                            <td>{p.id}</td>
                            <td>{p.symbol || '—'}</td>
                            <td>{p.status || '—'}</td>
                            <td>{fmtPct(p.targetWeight)}</td>
                            <td>{p.why}</td>
                          </tr>
                        ))}
                        {explainedProposals.length === 0 ? (
                          <tr><td colSpan={5}>No proposals found for this run.</td></tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </section>

                {looksSparse ? (
                  <section className="ledger-mini-section">
                    <h3>Data quality warning</h3>
                    <p className="ledger-note">
                      This chain has multiple events, but linked proposal/action/order counts are all zero.
                      That usually means incomplete linkage fields in ledger/audit records for this run.
                    </p>
                  </section>
                ) : null}

                <section className="ledger-mini-section">
                  <h3>Plain-English effects</h3>
                  {influenceLines.length ? (
                    <ul>
                      {influenceLines.map((line) => <li key={line}>{line}</li>)}
                    </ul>
                  ) : (
                    <p className="ledger-note">No explicit influence-delta fields for this event.</p>
                  )}
                </section>

                <details>
                  <summary>Influence delta (raw)</summary>
                  <pre className="ledger-json">
                    {JSON.stringify(detail?.ledger_event?.INFLUENCE_DELTA || detail?.ledger_event?.influence_delta || {}, null, 2)}
                  </pre>
                </details>

                <details>
                  <summary>Training snapshot link</summary>
                  {detail?.training_snapshot ? (
                    <ul>
                      <li>Snapshot as_of: {fmtTs(detail.training_snapshot.AS_OF_TS || detail.training_snapshot.as_of_ts)}</li>
                      <li>Facts hash: {detail.training_snapshot.SOURCE_FACTS_HASH || detail.training_snapshot.source_facts_hash || '—'}</li>
                    </ul>
                  ) : (
                    <p className="ledger-note">No training snapshot tied to this run.</p>
                  )}
                </details>
              </>
            )}
          </aside>
        </section>
      )}
    </div>
  )
}
