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

export default function LearningLedger() {
  const [events, setEvents] = useState([])
  const [chains, setChains] = useState([])
  const [selected, setSelected] = useState(null)
  const [selectedChainKey, setSelectedChainKey] = useState('')
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
    if (e) setSelected(e)
  }, [selectedChain])

  const timelineRows = useMemo(() => {
    if (detail?.causal_chain?.length) {
      return detail.causal_chain.map((r) => ({
        key: `canonical-${r.LEDGER_ID || r.ledger_id}-${r.EVENT_TS || r.event_ts}`,
        ts: r.EVENT_TS || r.event_ts,
        eventName: r.EVENT_NAME || r.event_name,
        status: normalizeStatus(r.STATUS || r.status),
        action: r.LIVE_ACTION_ID || r.live_action_id || '',
        order: r.LIVE_ORDER_ID || r.live_order_id || '',
      }))
    }
    return (selectedChain?.events || []).map((r) => ({
      key: r.event_key,
      ts: r.event_ts,
      eventName: r.event_name || r.title,
      status: normalizeStatus(r.status || r.severity),
      action: r.live_action_id || '',
      order: r.live_order_id || '',
    }))
  }, [detail, selectedChain])

  const stats = useMemo(() => {
    const trainingCount = events.filter((e) => e.event_type === 'TRAINING_EVENT').length
    const decisionCount = events.filter((e) => e.event_type === 'DECISION_EVENT').length
    const highCount = events.filter((e) => (e.severity || '').toLowerCase() === 'high').length
    const newsInfluencedCount = events.filter((e) => e.news_influence_used).length
    return { trainingCount, decisionCount, highCount, newsInfluencedCount, total: events.length }
  }, [events])

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
                      <span>{c.taxonomy_category || e.event_type || 'GENERAL'}</span>
                      <span>events: {c.event_count ?? (c.events?.length || 0)}</span>
                      <span>run: {c.run_id || e.run_id || '—'}</span>
                    </div>
                  </button>
                )
              })}
            </div>
          </div>

          <div className="ledger-column ledger-timeline-column">
            <h2>Timeline</h2>
            <p className="ledger-chain-summary">{summarizeChain(selected, detail)}</p>
            {detailLoading ? <LoadingState /> : null}
            {!detailLoading && timelineRows.length === 0 ? (
              <p className="ledger-note">No timeline events found for this chain.</p>
            ) : (
              <ol className="ledger-timeline">
                {timelineRows.map((r) => (
                  <li key={r.key} className="ledger-timeline-item">
                    <div className="ledger-timeline-top">
                      <span className="ledger-ts">{fmtTs(r.ts)}</span>
                      <span className={severityClass(r.status === 'FAIL' ? 'high' : r.status === 'BLOCKED' ? 'medium' : 'info')}>
                        {r.status}
                      </span>
                    </div>
                    <div className="ledger-timeline-name">{prettyEventName(r.eventName)}</div>
                    <div className="ledger-meta">
                      <span>action: {r.action || '—'}</span>
                      <span>order: {r.order || '—'}</span>
                    </div>
                  </li>
                ))}
              </ol>
            )}
          </div>

          <aside className="ledger-column ledger-context-column">
            <h2>Why This Happened</h2>
            {!selected ? (
              <p className="ledger-note">Select a chain to view context.</p>
            ) : (
              <>
                <div className="ledger-impact-grid">
                  <div><span>Run</span><b>{selected.run_id || '—'}</b></div>
                  <div><span>Event</span><b>{prettyEventName(selected.event_name || selected.title)}</b></div>
                  <div><span>Trusted delta</span><b>{selected.impact?.trusted_delta ?? '—'}</b></div>
                  <div><span>Executed</span><b>{detail?.summary?.executed_proposals ?? selected.impact?.executed_count ?? 0}</b></div>
                  <div><span>Live orders</span><b>{detail?.summary?.live_order_count ?? selected.impact?.live_order_count ?? 0}</b></div>
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
                  <h3>Influenced proposals (latest)</h3>
                  <div className="ledger-table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>ID</th>
                          <th>Symbol</th>
                          <th>Status</th>
                          <th>Weight</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(detail?.proposals || []).slice(0, 8).map((p) => (
                          <tr key={p.PROPOSAL_ID || p.proposal_id}>
                            <td>{p.PROPOSAL_ID || p.proposal_id}</td>
                            <td>{p.SYMBOL || p.symbol}</td>
                            <td>{p.STATUS || p.status}</td>
                            <td>{fmtPct(p.TARGET_WEIGHT || p.target_weight)}</td>
                          </tr>
                        ))}
                        {(detail?.proposals || []).length === 0 ? (
                          <tr><td colSpan={4}>No proposals found for this run.</td></tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
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
