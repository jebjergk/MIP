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

export default function LearningLedger() {
  const [events, setEvents] = useState([])
  const [chains, setChains] = useState([])
  const [selected, setSelected] = useState(null)
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
      setSelected((chainRows[0]?.events && chainRows[0].events[0]) || rows[0] || null)
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
    if (!selected?.run_id) {
      setDetail(null)
      return
    }
    let cancelled = false
    const loadDetail = async () => {
      setDetailLoading(true)
      try {
        const params = new URLSearchParams({ run_id: String(selected.run_id) })
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
        <div className="ledger-stat-card"><span>Total events</span><b>{stats.total}</b></div>
        <div className="ledger-stat-card"><span>Causal chains</span><b>{chains.length}</b></div>
        <div className="ledger-stat-card"><span>Training events</span><b>{stats.trainingCount}</b></div>
        <div className="ledger-stat-card"><span>Decision events</span><b>{stats.decisionCount}</b></div>
        <div className="ledger-stat-card"><span>News-influenced</span><b>{stats.newsInfluencedCount}</b></div>
        <div className="ledger-stat-card"><span>High severity</span><b>{stats.highCount}</b></div>
      </section>
      <section className="ledger-source-banner">
        <span>Feed source:</span>
        <b>{feedSource === 'canonical_ledger' ? 'Canonical immutable ledger' : 'Derived fallback (deploy ledger SQL to activate canonical mode)'}</b>
      </section>

      {effectiveness ? (
        <section className="ledger-effectiveness">
          <h3>Resulting Impact (30d)</h3>
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
        <section className="ledger-grid">
          <div className="ledger-feed">
            {chains.length > 0 ? chains.map((c) => {
              const e = (c.events && c.events[c.events.length - 1]) || {}
              return (
              <button
                type="button"
                key={c.chain_key}
                className={`ledger-event ${selected?.event_key === e.event_key ? 'ledger-event-selected' : ''}`}
                onClick={() => setSelected(e)}
              >
                <div className="ledger-event-top">
                  <span className={severityClass(e.severity)}>{(e.severity || 'info').toUpperCase()}</span>
                  <span className="ledger-ts">{fmtTs(c.latest_event_ts || e.event_ts)}</span>
                </div>
                <h3>{c.latest_title || e.title}</h3>
                <p>{c.latest_summary || e.summary}</p>
                <div className="ledger-meta">
                  <span>{c.taxonomy_category || e.event_type}</span>
                  <span>chain events: {c.event_count ?? (c.events?.length || 0)}</span>
                  <span>run: {c.run_id || e.run_id || '—'}</span>
                </div>
              </button>
            )}) : events.map((e) => (
              <button
                type="button"
                key={e.event_key}
                className={`ledger-event ${selected?.event_key === e.event_key ? 'ledger-event-selected' : ''}`}
                onClick={() => setSelected(e)}
              >
                <div className="ledger-event-top">
                  <span className={severityClass(e.severity)}>{(e.severity || 'info').toUpperCase()}</span>
                  <span className="ledger-ts">{fmtTs(e.event_ts)}</span>
                </div>
                <h3>{e.title}</h3>
                <p>{e.summary}</p>
                <div className="ledger-meta">
                  <span>{e.event_type}</span>
                  <span>run: {e.run_id || '—'}</span>
                  <span>portfolio: {e.portfolio_id ?? '—'}</span>
                </div>
              </button>
            ))}
          </div>

          <aside className="ledger-detail">
            {!selected ? (
              <p>Select an event to view causality details.</p>
            ) : (
              <>
                <h2>Audit Drill-down</h2>
                <p className="ledger-detail-subtitle">{selected.title}</p>
                <div className="ledger-impact-grid">
                  <div><span>Run</span><b>{selected.run_id || '—'}</b></div>
                  <div><span>Event</span><b>{selected.event_name || '—'}</b></div>
                  <div><span>Executed</span><b>{selected.impact?.executed_count ?? '—'}</b></div>
                  <div><span>Live orders</span><b>{selected.impact?.live_order_count ?? '—'}</b></div>
                  <div><span>Avg target weight</span><b>{fmtPct(selected.impact?.avg_target_weight, 1)}</b></div>
                  <div><span>Trusted delta</span><b>{selected.impact?.trusted_delta ?? '—'}</b></div>
                </div>

                {detailLoading ? <LoadingState /> : null}

                {!detailLoading && detail ? (
                  <div className="ledger-detail-sections">
                    <section>
                      <h3>Causality Summary</h3>
                      <ul>
                        <li>Audit events: {detail.summary?.audit_event_count ?? 0}</li>
                        <li>Proposals: {detail.summary?.proposal_count ?? 0}</li>
                        <li>Live actions: {detail.summary?.live_action_count ?? 0}</li>
                        <li>Live orders: {detail.summary?.live_order_count ?? 0}</li>
                        <li>Filled/partial: {detail.summary?.filled_or_partial_orders ?? 0}</li>
                      </ul>
                    </section>
                    <section>
                      <h3>Training Snapshot Link</h3>
                      {detail.training_snapshot ? (
                        <ul>
                          <li>Snapshot as_of: {fmtTs(detail.training_snapshot.AS_OF_TS || detail.training_snapshot.as_of_ts)}</li>
                          <li>Facts hash: {detail.training_snapshot.SOURCE_FACTS_HASH || detail.training_snapshot.source_facts_hash || '—'}</li>
                        </ul>
                      ) : (
                        <p>No global training snapshot tied to this run.</p>
                      )}
                    </section>
                    <section>
                      <h3>Influence Delta (Canonical)</h3>
                      {detail.ledger_event ? (
                        <pre className="ledger-json">
                          {JSON.stringify(detail.ledger_event.INFLUENCE_DELTA || detail.ledger_event.influence_delta || {}, null, 2)}
                        </pre>
                      ) : (
                        <p>No canonical influence delta found for this selected event.</p>
                      )}
                    </section>
                    <section>
                      <h3>Causal Chain (Canonical)</h3>
                      {(detail.causal_chain || []).length ? (
                        <div className="ledger-table-wrap">
                          <table>
                            <thead>
                              <tr>
                                <th>Time</th>
                                <th>Event</th>
                                <th>Status</th>
                                <th>Action</th>
                                <th>Order</th>
                              </tr>
                            </thead>
                            <tbody>
                              {(detail.causal_chain || []).slice(-30).map((r) => (
                                <tr key={`${r.LEDGER_ID || r.ledger_id}`}>
                                  <td>{fmtTs(r.EVENT_TS || r.event_ts)}</td>
                                  <td>{r.EVENT_NAME || r.event_name}</td>
                                  <td>{r.STATUS || r.status}</td>
                                  <td>{r.LIVE_ACTION_ID || r.live_action_id || '—'}</td>
                                  <td>{r.LIVE_ORDER_ID || r.live_order_id || '—'}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <p>No canonical chain records found for this selection.</p>
                      )}
                    </section>
                    <section>
                      <h3>Influenced Proposals (latest)</h3>
                      <div className="ledger-table-wrap">
                        <table>
                          <thead>
                            <tr>
                              <th>ID</th>
                              <th>Symbol</th>
                              <th>Status</th>
                              <th>Target Weight</th>
                            </tr>
                          </thead>
                          <tbody>
                            {(detail.proposals || []).slice(0, 12).map((p) => (
                              <tr key={p.PROPOSAL_ID || p.proposal_id}>
                                <td>{p.PROPOSAL_ID || p.proposal_id}</td>
                                <td>{p.SYMBOL || p.symbol}</td>
                                <td>{p.STATUS || p.status}</td>
                                <td>{fmtPct(p.TARGET_WEIGHT || p.target_weight)}</td>
                              </tr>
                            ))}
                            {(detail.proposals || []).length === 0 ? (
                              <tr><td colSpan={4}>No proposals for this run.</td></tr>
                            ) : null}
                          </tbody>
                        </table>
                      </div>
                    </section>
                  </div>
                ) : null}
              </>
            )}
          </aside>
        </section>
      )}
    </div>
  )
}
