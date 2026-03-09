import { useCallback, useEffect, useRef, useState } from 'react'
import { API_BASE } from '../App'
import './LivePortfolioActivity.css'

function fmtTs(ts) {
  if (!ts) return '—'
  try {
    return new Date(ts).toLocaleString()
  } catch {
    return ts
  }
}

function fmtNum(v, digits = 2) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

function fmtPct(v) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return `${(n * 100).toFixed(2)}%`
}

function fmtMaybePending(v, formatter) {
  if (v == null) return 'Pending'
  return formatter(v)
}

function stateClass(value) {
  const v = String(value || '').toUpperCase()
  if (v === 'FRESH' || v === 'CLEAR' || v === 'FILLED') return 'ok'
  if (v === 'AGING' || v === 'WARNING' || v === 'PARTIAL_FILL') return 'warn'
  if (v === 'STALE' || v === 'BLOCKED' || v === 'REJECTED' || v === 'CANCELED') return 'bad'
  return 'neutral'
}

export default function LivePortfolioActivity() {
  const [overview, setOverview] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState('')
  const [streamActionId, setStreamActionId] = useState('')
  const [streamStatus, setStreamStatus] = useState('')
  const [streamLogs, setStreamLogs] = useState([])
  const streamRef = useRef(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const resp = await fetch(`${API_BASE}/live/activity/overview`)
      if (!resp.ok) throw new Error(`Failed to load activity overview (${resp.status})`)
      const data = await resp.json()
      setOverview(data)
    } catch (e) {
      setError(e.message || 'Failed to load live activity.')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
  }, [load])

  useEffect(() => {
    return () => {
      if (streamRef.current) {
        streamRef.current.close()
        streamRef.current = null
      }
    }
  }, [])

  const refreshBroker = useCallback(async () => {
    setBusy('refresh')
    setError('')
    try {
      const resp = await fetch(`${API_BASE}/live/snapshot/refresh`, { method: 'POST' })
      if (!resp.ok) throw new Error(`Broker refresh failed (${resp.status})`)
      await load()
    } catch (e) {
      setError(e.message || 'Broker refresh failed.')
    } finally {
      setBusy('')
    }
  }, [load])

  const openCommitteeStream = useCallback((actionId) => {
    if (streamRef.current) {
      streamRef.current.close()
      streamRef.current = null
    }
    setStreamActionId(actionId)
    setStreamStatus('Connecting...')
    setStreamLogs([{ type: 'system', summary: 'Starting committee stream...' }])
    const es = new EventSource(
      `${API_BASE}/live/trades/actions/${actionId}/committee/live-prompt?actor=committee_orchestrator&model=claude-3-5-sonnet`,
    )
    streamRef.current = es

    es.addEventListener('start', (evt) => {
      setStreamStatus('Running...')
      try {
        const data = JSON.parse(evt.data)
        setStreamLogs((prev) => [...prev, { type: 'start', ...data }])
      } catch {
        setStreamLogs((prev) => [...prev, { type: 'start', summary: 'Committee run started.' }])
      }
    })
    es.addEventListener('agent_turn', (evt) => {
      try {
        setStreamLogs((prev) => [...prev, JSON.parse(evt.data)])
      } catch {
        // Ignore malformed frame
      }
    })
    es.addEventListener('role_summary', (evt) => {
      try {
        setStreamLogs((prev) => [...prev, JSON.parse(evt.data)])
      } catch {
        // Ignore malformed frame
      }
    })
    es.addEventListener('final', (evt) => {
      try {
        const data = JSON.parse(evt.data)
        setStreamLogs((prev) => [...prev, { type: 'final', ...data }])
      } catch {
        // Ignore malformed frame
      }
      setStreamStatus('Completed')
      es.close()
      streamRef.current = null
    })
    es.addEventListener('error', (evt) => {
      let detail = 'Stream stopped.'
      try {
        if (evt?.data) {
          const data = JSON.parse(evt.data)
          detail = data?.message || detail
        }
      } catch {
        // ignore parse errors
      }
      setStreamLogs((prev) => [...prev, { type: 'error', summary: detail }])
      setStreamStatus('Stopped')
      es.close()
      streamRef.current = null
    })
  }, [])

  const approveAndSubmit = useCallback(async (actionId) => {
    setBusy(actionId)
    setError('')
    try {
      const resp = await fetch(`${API_BASE}/live/decisions/${actionId}/approve-and-submit`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          force_refresh_1m: true,
          committee_recheck_before_submit: true,
        }),
      })
      if (!resp.ok) {
        let msg = `Submit failed (${resp.status})`
        try {
          const j = await resp.json()
          if (j?.detail?.reason_codes?.length) {
            msg = `${j.detail.message || 'Submit blocked'}: ${j.detail.reason_codes.join(', ')}`
          }
        } catch {
          // Keep fallback error message
        }
        throw new Error(msg)
      }
      await load()
    } catch (e) {
      setError(e.message || 'Submit failed.')
    } finally {
      setBusy('')
    }
  }, [load])

  const kpis = overview?.account_kpis || {}
  const pending = overview?.pending_decisions || []
  const openPositions = overview?.open_positions || []
  const orders = overview?.orders || []
  const executions = overview?.executions || []
  const readiness = overview?.readiness || {}
  const outsideHours = readiness.market_open === false

  return (
    <div className="page lpa-page">
      <div className="lpa-header">
        <div>
          <h2>Live Portfolio Activity</h2>
          <p>Broker-truth operations console for the linked IBKR portfolio.</p>
        </div>
        <button className="lpa-btn" disabled={busy === 'refresh'} onClick={refreshBroker}>
          {busy === 'refresh' ? 'Refreshing...' : 'Refresh From IB'}
        </button>
      </div>

      {error ? <div className="lpa-error">{error}</div> : null}
      {loading ? <div>Loading live portfolio activity...</div> : null}

      {!loading && (
        <>
          <div className="lpa-kpis">
            <div className="lpa-kpi"><span>Account</span><b>{overview?.portfolio?.ibkr_account_id || '—'}</b></div>
            <div className="lpa-kpi"><span>Equity / NAV</span><b>{fmtNum(kpis.equity_nav_eur, 2)}</b></div>
            <div className="lpa-kpi"><span>Cash</span><b>{fmtNum(kpis.cash_eur, 2)}</b></div>
            <div className="lpa-kpi"><span>Gross Exposure</span><b>{fmtNum(kpis.gross_exposure_eur, 2)}</b></div>
            <div className="lpa-kpi"><span>Open Positions</span><b>{kpis.open_positions_count ?? 0}</b></div>
            <div className="lpa-kpi"><span>Open Orders</span><b>{kpis.open_orders_count ?? 0}</b></div>
            <div className="lpa-kpi"><span>Snapshot</span><b>{fmtTs(kpis.snapshot_ts)}</b></div>
            <div className={`lpa-kpi lpa-kpi--${stateClass(readiness.snapshot_state)}`}>
              <span>Freshness</span><b>{readiness.snapshot_state || '—'}</b>
            </div>
            <div className={`lpa-kpi lpa-kpi--${stateClass(readiness.drift_state)}`}>
              <span>Drift</span><b>{readiness.drift_state || '—'}</b>
            </div>
          </div>

          <section className="lpa-section">
            <h3>Pending Decisions</h3>
            <div className="lpa-subtle">
              Decisions not yet broker-opened. Submit path keeps approval/revalidation/execution lifecycle traceable.
            </div>
            {outsideHours ? <div className="lpa-subtle">Outside operating hours: actions are disabled.</div> : null}
            <div className="lpa-table-wrap">
              <table className="lpa-table lpa-table--pending">
                <thead>
                  <tr>
                    <th>Decision</th>
                    <th>Status</th>
                    <th>Sizing Transparency</th>
                    <th>Reason / Next Step</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {pending.length === 0 && (
                    <tr><td colSpan={5}>No pending decisions.</td></tr>
                  )}
                  {pending.map((d) => (
                    <tr key={d.action_id}>
                      <td>
                        <div><b>{d.symbol}</b> ({d.side})</div>
                        <div>Action: {d.action_id}</div>
                        <div>Committee: {d.committee_verdict || '—'}</div>
                      </td>
                      <td>
                        <div>{d.status || '—'}</div>
                        <div>Compliance: {d.compliance_status || '—'}</div>
                        <div>Protected: {d.protection?.state || 'NONE'}</div>
                      </td>
                      <td>
                        <div className="lpa-kv-list">
                          <div className="lpa-kv"><span>Qty preview</span><b>{fmtMaybePending(d.sizing?.final_qty_preview, (n) => fmtNum(n, 0))}</b></div>
                          <div className="lpa-kv"><span>Proposed qty</span><b>{fmtMaybePending(d.sizing?.proposed_qty, (n) => fmtNum(n, 0))}</b></div>
                          <div className="lpa-kv"><span>Price</span><b>{fmtMaybePending(d.sizing?.proposed_price, (n) => fmtNum(n, 4))}</b></div>
                          <div className="lpa-kv"><span>Notional</span><b>{fmtMaybePending(d.sizing?.estimated_notional_eur, (n) => fmtNum(n, 2))}</b></div>
                          <div className="lpa-kv"><span>Position %</span><b>{fmtMaybePending(d.sizing?.estimated_position_pct, fmtPct)}</b></div>
                          <div className="lpa-kv"><span>Committee factor</span><b>{fmtMaybePending(d.sizing?.committee_size_factor, (n) => fmtNum(n, 2))}</b></div>
                          <div className="lpa-kv"><span>Cap factor</span><b>{fmtMaybePending(d.sizing?.training_size_cap_factor, (n) => fmtNum(n, 2))}</b></div>
                          <div className="lpa-kv"><span>Open factor</span><b>{fmtMaybePending(d.sizing?.target_open_condition_factor, (n) => fmtNum(n, 2))}</b></div>
                        </div>
                        {d.sizing?.availability_reason ? (
                          <div className="lpa-subtle">{d.sizing.availability_reason}</div>
                        ) : null}
                      </td>
                      <td>
                        <div>{(d.reason_codes || []).join(', ') || '—'}</div>
                        <div className="lpa-subtle">Next: {d.required_next_step || '—'}</div>
                      </td>
                      <td>
                        <div className="lpa-actions">
                        <button
                          className="lpa-btn"
                          disabled={busy === d.action_id || !d.submission_allowed || outsideHours}
                          onClick={() => approveAndSubmit(d.action_id)}
                        >
                          {busy === d.action_id ? 'Submitting...' : 'Approve + Submit'}
                        </button>
                        <button
                          className="lpa-btn lpa-btn-secondary"
                          disabled={outsideHours}
                          onClick={() => openCommitteeStream(d.action_id)}
                        >
                          Committee revalidation
                        </button>
                        {!d.submission_allowed ? (
                          <div className="lpa-subtle">Blocked until gates are clear.</div>
                        ) : null}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {streamActionId ? (
              <div className="lpa-stream">
                <div>
                  <b>Live Committee Stream</b> for {streamActionId} ({streamStatus || 'Idle'})
                </div>
                {(streamLogs || []).length === 0 ? (
                  <div className="lpa-subtle">No events yet.</div>
                ) : (
                  (streamLogs || []).map((entry, idx) => (
                    <div key={`${streamActionId}_${idx}`} className="lpa-stream-line">
                      {entry.round ? `[R${entry.round}] ` : ''}{entry.role || entry.type || 'event'}: {entry.output?.summary || entry.summary || JSON.stringify(entry.joint_decision || entry.verdict || entry)}
                    </div>
                  ))
                )}
              </div>
            ) : null}
          </section>

          <section className="lpa-section">
            <h3>Open Positions (IBKR Truth)</h3>
            <div className="lpa-table-wrap">
              <table className="lpa-table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Qty</th>
                    <th>Avg Cost</th>
                    <th>Market Value</th>
                    <th>Unrealized P&L</th>
                  </tr>
                </thead>
                <tbody>
                  {openPositions.length === 0 && <tr><td colSpan={5}>No open broker positions.</td></tr>}
                  {openPositions.map((p, idx) => (
                    <tr key={`${p.SYMBOL || 'SYM'}_${idx}`}>
                      <td>{p.SYMBOL || '—'}</td>
                      <td>{fmtNum(p.POSITION_QTY, 0)}</td>
                      <td>{fmtNum(p.AVG_COST, 4)}</td>
                      <td>{fmtNum(p.MARKET_VALUE, 2)}</td>
                      <td>{fmtNum(p.UNREALIZED_PNL, 2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="lpa-section">
            <h3>Orders (Broker Lifecycle)</h3>
            <div className="lpa-table-wrap">
              <table className="lpa-table">
                <thead>
                  <tr>
                    <th>Order</th>
                    <th>Status</th>
                    <th>Qty</th>
                    <th>Price</th>
                    <th>Timestamps</th>
                  </tr>
                </thead>
                <tbody>
                  {orders.length === 0 && <tr><td colSpan={5}>No broker orders.</td></tr>}
                  {orders.map((o) => (
                    <tr key={o.ORDER_ID}>
                      <td>
                        <div><b>{o.SYMBOL}</b> ({o.SIDE})</div>
                        <div>Order: {o.ORDER_ID}</div>
                        <div>Broker ID: {o.BROKER_ORDER_ID || '—'}</div>
                        <div>Action: {o.ACTION_ID || '—'}</div>
                      </td>
                      <td className={`lpa-status lpa-status--${stateClass(o.STATUS)}`}>{o.STATUS || '—'}</td>
                      <td>
                        <div>Ordered: {fmtNum(o.QTY_ORDERED, 0)}</div>
                        <div>Filled: {fmtNum(o.QTY_FILLED, 0)}</div>
                      </td>
                      <td>
                        <div>Limit: {fmtNum(o.LIMIT_PRICE, 4)}</div>
                        <div>Avg fill: {fmtNum(o.AVG_FILL_PRICE, 4)}</div>
                      </td>
                      <td>
                        <div>Submitted: {fmtTs(o.SUBMITTED_AT)}</div>
                        <div>Updated: {fmtTs(o.LAST_UPDATED_AT || o.CREATED_AT)}</div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="lpa-section">
            <h3>Executions / Trades</h3>
            <div className="lpa-table-wrap">
              <table className="lpa-table">
                <thead>
                  <tr>
                    <th>Execution</th>
                    <th>Status</th>
                    <th>Filled Qty</th>
                    <th>Fill Price</th>
                    <th>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {executions.length === 0 && <tr><td colSpan={5}>No executions yet.</td></tr>}
                  {executions.map((e) => (
                    <tr key={`${e.order_id}_${e.execution_ts || 'ts'}`}>
                      <td>
                        <div><b>{e.symbol}</b> ({e.side})</div>
                        <div>Order: {e.order_id}</div>
                        <div>Action: {e.action_id || '—'}</div>
                      </td>
                      <td className={`lpa-status lpa-status--${stateClass(e.status)}`}>{e.status || '—'}</td>
                      <td>{fmtNum(e.qty_filled, 0)}</td>
                      <td>{fmtNum(e.avg_fill_price, 4)}</td>
                      <td>{fmtTs(e.execution_ts)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}
    </div>
  )
}
