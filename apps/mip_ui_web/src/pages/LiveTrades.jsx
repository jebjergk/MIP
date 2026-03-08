import { Fragment, useCallback, useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../App'
import './LiveTrades.css'

function fmtTs(ts) {
  if (!ts) return '—'
  try {
    return new Date(ts).toLocaleString()
  } catch {
    return ts
  }
}

function fmtNum(v, digits = 4) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toFixed(digits)
}

export default function LiveTrades() {
  const [actions, setActions] = useState([])
  const [orders, setOrders] = useState([])
  const [liveConfigs, setLiveConfigs] = useState([])
  const [proposalInbox, setProposalInbox] = useState([])
  const [proposalInboxMeta, setProposalInboxMeta] = useState(null)
  const [loading, setLoading] = useState(true)
  const [busyId, setBusyId] = useState(null)
  const [error, setError] = useState('')
  const [latestNav, setLatestNav] = useState(null)
  const [earlyExit, setEarlyExit] = useState(null)
  const [driftStatus, setDriftStatus] = useState(null)
  const [complianceActor, setComplianceActor] = useState('compliance_user')
  const [committeeActor, setCommitteeActor] = useState('committee_orchestrator')
  const [committeeModel, setCommitteeModel] = useState('mistral-large2')
  const [intentSubmitActor, setIntentSubmitActor] = useState('intent_submitter')
  const [intentApproveActor, setIntentApproveActor] = useState('intent_approver')
  const [pmActor, setPmActor] = useState('portfolio_manager')
  const [executionActor, setExecutionActor] = useState('execution_operator')
  const [orderActor, setOrderActor] = useState('broker_sync_operator')
  const [bridgeLivePortfolioId, setBridgeLivePortfolioId] = useState('1')
  const [bridgeRunId, setBridgeRunId] = useState('')
  const [bridgeResult, setBridgeResult] = useState(null)
  const [committeeByAction, setCommitteeByAction] = useState({})
  const [expandedActionId, setExpandedActionId] = useState(null)
  const [livePromptActionId, setLivePromptActionId] = useState(null)
  const [livePromptStage, setLivePromptStage] = useState(null)
  const [livePromptLogs, setLivePromptLogs] = useState([])
  const [livePromptStatus, setLivePromptStatus] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const [actionsResp, ordersResp, snapshotResp, earlyExitResp, driftResp, cfgResp] = await Promise.all([
        fetch(`${API_BASE}/live/trades/actions?pending_only=true&limit=300`),
        fetch(`${API_BASE}/live/trades/orders?limit=300`),
        fetch(`${API_BASE}/live/snapshot/latest`),
        fetch(`${API_BASE}/live/early-exit/status?limit=10`),
        fetch(`${API_BASE}/live/drift/status`),
        fetch(`${API_BASE}/live/portfolio-config`),
      ])
      if (!actionsResp.ok) throw new Error(`Failed to load actions (${actionsResp.status})`)
      if (!ordersResp.ok) throw new Error(`Failed to load orders (${ordersResp.status})`)
      if (!snapshotResp.ok) throw new Error(`Failed to load snapshot (${snapshotResp.status})`)
      if (!earlyExitResp.ok) throw new Error(`Failed to load early-exit monitor (${earlyExitResp.status})`)
      if (!driftResp.ok) throw new Error(`Failed to load drift status (${driftResp.status})`)
      const actionsJson = await actionsResp.json()
      const ordersJson = await ordersResp.json()
      const snapJson = await snapshotResp.json()
      const earlyExitJson = await earlyExitResp.json()
      const driftJson = await driftResp.json()
      const cfgJson = cfgResp.ok ? await cfgResp.json() : { configs: [] }
      setActions(actionsJson.actions || [])
      setOrders(ordersJson.orders || [])
      setLatestNav(snapJson.latest_nav || null)
      setEarlyExit(earlyExitJson || null)
      setDriftStatus(driftJson || null)
      const cfgs = Array.isArray(cfgJson?.configs) ? cfgJson.configs : []
      setLiveConfigs(cfgs)
      if ((!bridgeLivePortfolioId || bridgeLivePortfolioId === '1') && cfgs.length > 0) {
        setBridgeLivePortfolioId(String(cfgs[0].PORTFOLIO_ID))
      }
    } catch (e) {
      setError(e.message || 'Failed to load live trades data.')
    } finally {
      setLoading(false)
    }
  }, [bridgeLivePortfolioId])

  useEffect(() => {
    load()
  }, [load])

  const loadProposalInbox = useCallback(async () => {
    try {
      const qp = new URLSearchParams({
        live_portfolio_id: String(Number(bridgeLivePortfolioId) || 1),
        latest_batch_only: 'true',
        limit: '120',
      })
      if (bridgeRunId.trim()) qp.set('run_id', bridgeRunId.trim())
      const resp = await fetch(`${API_BASE}/live/trades/proposal-candidates?${qp.toString()}`)
      if (!resp.ok) throw new Error(`Failed to load proposal inbox (${resp.status})`)
      const data = await resp.json()
      setProposalInbox(Array.isArray(data?.candidates) ? data.candidates : [])
      setProposalInboxMeta(data || null)
    } catch (e) {
      setProposalInbox([])
      setProposalInboxMeta({ error: e.message || 'Failed to load proposal inbox.' })
    }
  }, [bridgeLivePortfolioId, bridgeRunId])

  useEffect(() => {
    loadProposalInbox()
  }, [loadProposalInbox])

  const loadCommittee = useCallback(async (actionId) => {
    try {
      const resp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/committee`)
      if (!resp.ok) throw new Error(`Failed committee fetch (${resp.status})`)
      const data = await resp.json()
      setCommitteeByAction((prev) => ({ ...prev, [actionId]: data }))
    } catch (e) {
      setCommitteeByAction((prev) => ({ ...prev, [actionId]: { error: e.message || 'Failed to load committee reasoning.' } }))
    }
  }, [])

  const toggleCommitteeDetails = useCallback(async (actionId) => {
    if (expandedActionId === actionId) {
      setExpandedActionId(null)
      return
    }
    setExpandedActionId(actionId)
    if (!committeeByAction[actionId]) {
      await loadCommittee(actionId)
    }
  }, [committeeByAction, expandedActionId, loadCommittee])

  const startLivePrompt = useCallback((actionId, stage) => {
    setLivePromptActionId(actionId)
    setLivePromptStage(stage)
    setLivePromptLogs([])
    setLivePromptStatus('Connecting...')
    const base =
      stage === 'revalidate'
        ? `${API_BASE}/live/trades/actions/${actionId}/revalidate/live-prompt`
        : `${API_BASE}/live/trades/actions/${actionId}/committee/live-prompt?actor=${encodeURIComponent(committeeActor)}&model=${encodeURIComponent(committeeModel)}`
    const es = new EventSource(base)
    es.addEventListener('start', () => setLivePromptStatus('Running...'))
    es.addEventListener('agent_turn', (evt) => {
      try {
        const data = JSON.parse(evt.data)
        setLivePromptLogs((prev) => [...prev, data])
      } catch {
        // ignore malformed frames
      }
    })
    es.addEventListener('role_summary', (evt) => {
      try {
        const data = JSON.parse(evt.data)
        setLivePromptLogs((prev) => [...prev, data])
      } catch {
        // ignore malformed frames
      }
    })
    es.addEventListener('joint_decision', (evt) => {
      try {
        const data = JSON.parse(evt.data)
        setLivePromptLogs((prev) => [...prev, { type: 'joint_decision', ...data }])
      } catch {
        // ignore malformed frames
      }
    })
    es.addEventListener('final', (evt) => {
      try {
        const data = JSON.parse(evt.data)
        setLivePromptLogs((prev) => [...prev, { type: 'final', ...data }])
      } catch {
        // ignore malformed frames
      }
      setLivePromptStatus('Completed')
      es.close()
    })
    es.addEventListener('error', (evt) => {
      try {
        if (evt?.data) {
          const data = JSON.parse(evt.data)
          setLivePromptLogs((prev) => [...prev, { type: 'error', ...data }])
        }
      } catch {
        // ignore malformed frames
      }
      setLivePromptStatus('Stopped')
      es.close()
    })
  }, [committeeActor, committeeModel])

  const pendingCompliance = useMemo(
    () => actions.filter((a) => a.STATUS === 'PM_ACCEPTED' || a.COMPLIANCE_STATUS === 'PENDING'),
    [actions]
  )

  const runAction = useCallback(async (actionId, endpoint, body = null) => {
    setBusyId(actionId)
    setError('')
    try {
      const resp = await fetch(`${API_BASE}${endpoint}`, {
        method: 'POST',
        headers: body ? { 'Content-Type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
      })
      if (!resp.ok) {
        let msg = ''
        try {
          const j = await resp.json()
          if (j?.detail?.reason_codes && Array.isArray(j.detail.reason_codes)) {
            msg = `${j.detail.message || 'Action blocked'}: ${j.detail.reason_codes.join(', ')}`
          } else {
            msg = j?.detail ? JSON.stringify(j.detail) : ''
          }
        } catch {
          msg = await resp.text()
        }
        throw new Error(msg || `Action failed (${resp.status})`)
      }
      await load()
    } catch (e) {
      setError(e.message || 'Action failed.')
    } finally {
      setBusyId(null)
    }
  }, [load])

  const refreshSnapshot = useCallback(async () => {
    setBusyId('snapshot')
    setError('')
    try {
      const resp = await fetch(`${API_BASE}/live/snapshot/refresh`, { method: 'POST' })
      if (!resp.ok) throw new Error(`Snapshot refresh failed (${resp.status})`)
      await load()
    } catch (e) {
      setError(e.message || 'Snapshot refresh failed.')
    } finally {
      setBusyId(null)
    }
  }, [load])

  const runEarlyExitMonitor = useCallback(async () => {
    setBusyId('early-exit')
    setError('')
    try {
      const resp = await fetch(`${API_BASE}/live/early-exit/run`, { method: 'POST' })
      if (!resp.ok) {
        const msg = await resp.text()
        throw new Error(msg || `Early-exit monitor failed (${resp.status})`)
      }
      await load()
    } catch (e) {
      setError(e.message || 'Early-exit monitor failed.')
    } finally {
      setBusyId(null)
    }
  }, [load])

  const importFromResearchProposals = useCallback(async () => {
    setBusyId('bridge')
    setError('')
    setBridgeResult(null)
    try {
      const payload = {
        live_portfolio_id: Number(bridgeLivePortfolioId),
        source_portfolio_id: null,
        run_id: bridgeRunId.trim() ? bridgeRunId.trim() : null,
        limit: 200,
        latest_batch_only: true,
      }
      const resp = await fetch(`${API_BASE}/live/trades/actions/import-proposals`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!resp.ok) {
        const msg = await resp.text()
        throw new Error(msg || `Import failed (${resp.status})`)
      }
      const data = await resp.json()
      setBridgeResult(data)
      await Promise.all([load(), loadProposalInbox()])
    } catch (e) {
      setError(e.message || 'Failed to import proposals.')
    } finally {
      setBusyId(null)
    }
  }, [bridgeLivePortfolioId, bridgeRunId, load, loadProposalInbox])

  const runOrderStatus = useCallback(async (order, status) => {
    const orderBusyId = `order:${order.ORDER_ID}:${status}`
    setBusyId(orderBusyId)
    setError('')
    try {
      let qtyFilled = null
      if (status === 'PARTIAL_FILL') {
        const ordered = Number(order.QTY_ORDERED || 0)
        if (ordered > 0) qtyFilled = Math.max(1, Math.floor(ordered * 0.5))
      } else if (status === 'FILLED') {
        qtyFilled = Number(order.QTY_ORDERED || 0) || null
      }
      const payload = {
        actor: orderActor,
        status,
        qty_filled: qtyFilled,
        avg_fill_price: order.LIMIT_PRICE != null ? Number(order.LIMIT_PRICE) : null,
      }
      const resp = await fetch(`${API_BASE}/live/trades/orders/${order.ORDER_ID}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!resp.ok) {
        const msg = await resp.text()
        throw new Error(msg || `Order status update failed (${resp.status})`)
      }
      await load()
    } catch (e) {
      setError(e.message || 'Order status update failed.')
    } finally {
      setBusyId(null)
    }
  }, [load, orderActor])

  return (
    <div className="page live-trades-page">
      <div className="live-trades-header">
        <div>
          <h2>Live Trades</h2>
          <p>Live approval queue and broker-linked execution controls.</p>
        </div>
        <button className="lt-btn" disabled={busyId === 'snapshot'} onClick={refreshSnapshot}>
          {busyId === 'snapshot' ? 'Refreshing...' : 'Refresh IBKR Snapshot'}
        </button>
      </div>

      <div className="lt-create-card">
        <h3>How To Read This Page</h3>
        <div className="lt-summary">
          <b>Refresh IBKR Snapshot</b> pulls latest account/cash/positions/open-orders from IB Gateway into MIP snapshot tables (read-only sync).
        </div>
        <div className="lt-summary">
          <b>Live Portfolio ID</b> is an internal MIP execution container. It is not an IB field.
        </div>
        <div className="lt-summary">
          <b>IBKR Account</b> shown in header is the broker truth source used for live state.
        </div>
      </div>

      {latestNav && (
        <div className="lt-nav-card">
          <span>Account: <b>{latestNav.IBKR_ACCOUNT_ID || '—'}</b></span>
          <span>NAV (EUR): <b>{fmtNum(latestNav.NET_LIQUIDATION_EUR, 2)}</b></span>
          <span>Total Cash (EUR): <b>{fmtNum(latestNav.TOTAL_CASH_EUR, 2)}</b></span>
          <span>Snapshot: <b>{fmtTs(latestNav.SNAPSHOT_TS)}</b></span>
        </div>
      )}

      <div className="lt-create-card">
        <h3>Hourly Early-Exit Monitor (60m)</h3>
        <div className="lt-summary">
          Enabled: <b>{earlyExit?.enabled ? 'Yes' : 'No'}</b> | Interval: <b>{earlyExit?.interval_minutes ?? '—'}m</b>
          {' '}| Latest status: <b>{earlyExit?.latest?.status || '—'}</b>
        </div>
        <div className="lt-summary">
          Latest run: <b>{earlyExit?.latest?.run_id || '—'}</b> at <b>{fmtTs(earlyExit?.latest?.event_ts)}</b>
          {' '}| Exits executed: <b>{earlyExit?.latest?.exits_executed ?? 0}</b>
        </div>
        <button className="lt-btn" disabled={busyId === 'early-exit'} onClick={runEarlyExitMonitor}>
          {busyId === 'early-exit' ? 'Running...' : 'Run Early-Exit Monitor'}
        </button>
      </div>

      <div className="lt-create-card">
        <h3>Broker Drift Guard</h3>
        <div className="lt-summary">
          Drift status: <b>{driftStatus?.drift_status || '—'}</b> | Unresolved drift count: <b>{driftStatus?.unresolved_drift_count ?? '—'}</b>
        </div>
        <div className="lt-summary">
          Latest NAV snapshot age (sec): <b>{driftStatus?.snapshot_age_sec ?? '—'}</b>
        </div>
      </div>

      <div className="lt-actors-card">
        <label>
          PM actor
          <input value={pmActor} readOnly disabled />
        </label>
        <label>
          Compliance actor
          <input value={complianceActor} readOnly disabled />
        </label>
        <label>
          Committee actor
          <input value={committeeActor} readOnly disabled />
        </label>
        <label>
          Committee model
          <input value={committeeModel} readOnly disabled />
        </label>
        <label>
          Intent submit actor
          <input value={intentSubmitActor} readOnly disabled />
        </label>
        <label>
          Intent approve actor
          <input value={intentApproveActor} readOnly disabled />
        </label>
        <label>
          Execution actor
          <input value={executionActor} readOnly disabled />
        </label>
        <label>
          Order status actor
          <input value={orderActor} readOnly disabled />
        </label>
      </div>
      <div className="lt-summary">
        Actor identities are fixed defaults for auditability and to avoid accidental workflow drift from UI edits.
      </div>

      <div className="lt-create-card">
        <h3>Research Proposal Inbox</h3>
        <div className="lt-summary">
          This is read-only source data from `ORDER_PROPOSALS`. It does not execute anything.
        </div>
        <div className="lt-summary">
          Scope: <b>{proposalInboxMeta?.scope || '—'}</b>
          {' '}| Latest batch day: <b>{proposalInboxMeta?.latest_batch_date || '—'}</b>
          {' '}| Rows shown: <b>{proposalInbox.length}</b>
        </div>
        {proposalInboxMeta?.error ? (
          <div className="lt-error">{proposalInboxMeta.error}</div>
        ) : null}
        <div className="lt-table-wrap">
          <table className="lt-table">
            <thead>
              <tr>
                <th>Proposed</th>
                <th>Proposal</th>
                <th>Research Source</th>
                <th>Status</th>
                <th>Queued?</th>
              </tr>
            </thead>
            <tbody>
              {proposalInbox.length === 0 && (
                <tr>
                  <td colSpan={5}>No proposal candidates in current scope.</td>
                </tr>
              )}
              {proposalInbox.map((p) => (
                <tr key={p.PROPOSAL_ID}>
                  <td>{fmtTs(p.PROPOSED_AT)}</td>
                  <td>
                    <div><b>{p.SYMBOL}</b> ({p.SIDE})</div>
                    <div>Proposal #{p.PROPOSAL_ID}</div>
                  </td>
                  <td>
                    <div>Portfolio {p.PORTFOLIO_ID}</div>
                    <div>Run: {p.RUN_ID_VARCHAR || '—'}</div>
                  </td>
                  <td>{p.STATUS || '—'}</td>
                  <td>{p.ALREADY_QUEUED ? 'Yes' : 'No'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="lt-create-card">
        <h3>Queue Proposals to Live Review</h3>
        <div className="lt-summary">
          Queued candidates start in <b>RESEARCH_IMPORTED</b> and are non-executable until PM accept,
          compliance approval, and revalidation pass.
        </div>
        <div className="lt-summary">
          Queue action reads the latest proposal batch by default.
          Use optional <b>run_id</b> to narrow to a specific generation batch.
        </div>
        <div className="lt-summary">
          Queueing only copies candidate proposals into the live approval queue.
          Simulation portfolios continue their normal autonomous learning/trading path unchanged.
        </div>
        <div className="lt-form-row">
          <select
            value={bridgeLivePortfolioId}
            onChange={(e) => setBridgeLivePortfolioId(e.target.value)}
            title="Target live execution container"
          >
            {liveConfigs.length === 0 ? (
              <option value="">No live configs found</option>
            ) : (
              liveConfigs.map((cfg) => (
                <option key={String(cfg.PORTFOLIO_ID)} value={String(cfg.PORTFOLIO_ID)}>
                  Live #{cfg.PORTFOLIO_ID} | {cfg.IBKR_ACCOUNT_ID || 'No IBKR'} | {cfg.ADAPTER_MODE || 'PAPER'}
                </option>
              ))
            )}
          </select>
          <input
            value={bridgeRunId}
            onChange={(e) => setBridgeRunId(e.target.value)}
            placeholder="Optional run_id filter"
          />
          <button className="lt-btn" onClick={importFromResearchProposals} disabled={busyId === 'bridge'}>
            {busyId === 'bridge' ? 'Queueing...' : 'Queue Proposals'}
          </button>
        </div>
        {bridgeResult ? (
          <div className="lt-summary">
            Queued {bridgeResult.imported_count} / {bridgeResult.candidate_count} candidate proposals
            (skipped existing: {bridgeResult.skipped_existing_count}, invalid: {bridgeResult.skipped_invalid_count}).
            {' '}Scope: <b>{bridgeResult.source_scope || bridgeResult.source_origin || '—'}</b>.
            {' '}Distinct symbols: <b>{bridgeResult.distinct_symbol_count ?? '—'}</b>.
          </div>
        ) : null}
      </div>

      {error ? <div className="lt-error">{error}</div> : null}
      {loading ? <div>Loading live trade approvals...</div> : null}

      <div className="lt-summary">
        Pending compliance approvals: <b>{pendingCompliance.length}</b> | Total pending actions: <b>{actions.length}</b>
      </div>

      <div className="lt-table-wrap">
        <table className="lt-table">
          <thead>
            <tr>
              <th>Created</th>
              <th>Action</th>
              <th>State</th>
              <th>Proposed</th>
              <th>Revalidation</th>
              <th>Compliance</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {actions.length === 0 && (
              <tr>
                <td colSpan={7}>No pending actions.</td>
              </tr>
            )}
            {actions.map((a) => (
              <Fragment key={a.ACTION_ID}>
              <tr>
                <td>{fmtTs(a.CREATED_AT)}</td>
                <td>
                  <div><b>{a.SYMBOL}</b> ({a.SIDE})</div>
                  <div>Live Portfolio #{a.PORTFOLIO_ID}</div>
                  <div>Action: {a.ACTION_ID}</div>
                </td>
                <td>
                  <div>{a.STATUS}</div>
                  <div>Committee: {a.COMMITTEE_STATUS || '—'} ({a.COMMITTEE_VERDICT || '—'})</div>
                  <div>
                    Training: {(a.TRAINING_QUALIFICATION_SNAPSHOT?.maturity_stage || a.TRAINING_QUALIFICATION_SNAPSHOT?.MATURITY_STAGE || '—')}
                    {' '}| Eligible: {a.TRAINING_LIVE_ELIGIBLE == null ? '—' : (a.TRAINING_LIVE_ELIGIBLE ? 'Yes' : 'No')}
                  </div>
                  <div>
                    Rank: {a.TRAINING_RANK_IMPACT || '—'} | Size cap: {a.TRAINING_SIZE_CAP_FACTOR == null ? '—' : fmtNum(a.TRAINING_SIZE_CAP_FACTOR, 2)}
                  </div>
                  <div>
                    Target bands: C {fmtNum(a.TARGET_EXPECTATION_SNAPSHOT?.bands?.conservative ?? a.TARGET_EXPECTATION_SNAPSHOT?.BANDS?.CONSERVATIVE, 3)}
                    {' '}| B {fmtNum(a.TARGET_EXPECTATION_SNAPSHOT?.bands?.base ?? a.TARGET_EXPECTATION_SNAPSHOT?.BANDS?.BASE, 3)}
                    {' '}| S {fmtNum(a.TARGET_EXPECTATION_SNAPSHOT?.bands?.strong ?? a.TARGET_EXPECTATION_SNAPSHOT?.BANDS?.STRONG, 3)}
                  </div>
                  <div>
                    News: {a.NEWS_CONTEXT_STATE || a.NEWS_CONTEXT_SNAPSHOT?.context_state || a.NEWS_CONTEXT_SNAPSHOT?.CONTEXT_STATE || '—'}
                    {' '}| Freshness: {a.NEWS_FRESHNESS_BUCKET || a.NEWS_CONTEXT_SNAPSHOT?.freshness_bucket || a.NEWS_CONTEXT_SNAPSHOT?.FRESHNESS_BUCKET || '—'}
                  </div>
                  <div>
                    News shock: {a.NEWS_EVENT_SHOCK_FLAG == null ? '—' : (a.NEWS_EVENT_SHOCK_FLAG ? 'Yes' : 'No')}
                  </div>
                  <div>{a.COMPLIANCE_STATUS || '—'}</div>
                  <div>{Array.isArray(a.REASON_CODES) && a.REASON_CODES.length ? a.REASON_CODES.join(', ') : '—'}</div>
                </td>
                <td>
                  <div>Qty: {fmtNum(a.PROPOSED_QTY, 0)}</div>
                  <div>Px: {fmtNum(a.PROPOSED_PRICE, 4)}</div>
                  <div>Valid until: {fmtTs(a.VALIDITY_WINDOW_END)}</div>
                </td>
                <td>
                  <div>Source: {a.EXECUTION_PRICE_SOURCE || '—'}</div>
                  <div>Px: {fmtNum(a.REVALIDATION_PRICE, 4)}</div>
                  <div>Dev: {a.PRICE_DEVIATION_PCT != null ? `${fmtNum(a.PRICE_DEVIATION_PCT * 100, 2)}%` : '—'}</div>
                  <div>Guard: {a.PRICE_GUARD_RESULT || '—'}</div>
                  <div>Open factor: {a.TARGET_OPEN_CONDITION_FACTOR == null ? '—' : fmtNum(a.TARGET_OPEN_CONDITION_FACTOR, 2)}</div>
                </td>
                <td>
                  <div>By: {a.COMPLIANCE_APPROVED_BY || '—'}</div>
                  <div>At: {fmtTs(a.COMPLIANCE_DECISION_TS)}</div>
                  <div>Ref: {a.COMPLIANCE_REFERENCE_ID || '—'}</div>
                  <div>Intent submit: {a.INTENT_SUBMITTED_BY || '—'} @ {fmtTs(a.INTENT_SUBMITTED_TS)}</div>
                  <div>Intent approve: {a.INTENT_APPROVED_BY || '—'} @ {fmtTs(a.INTENT_APPROVED_TS)}</div>
                </td>
                <td>
                  <div className="lt-actions">
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID || !['RESEARCH_IMPORTED', 'PROPOSED', 'PM_ACCEPTED', 'COMPLIANCE_APPROVED'].includes(a.STATUS)}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/committee/run`, {
                        actor: committeeActor,
                        model: committeeModel,
                        force_rerun: false,
                      })}
                    >
                      Run Committee
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID}
                      onClick={() => toggleCommitteeDetails(a.ACTION_ID)}
                    >
                      {expandedActionId === a.ACTION_ID ? 'Hide Reasoning' : 'View Reasoning'}
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID}
                      onClick={() => startLivePrompt(a.ACTION_ID, 'committee')}
                    >
                      Live Committee Prompt
                    </button>
                    <button
                      className="lt-btn"
                      disabled={
                        busyId === a.ACTION_ID
                        || !['RESEARCH_IMPORTED', 'PROPOSED'].includes(a.STATUS)
                        || ((a.COMMITTEE_REQUIRED ?? true) && a.COMMITTEE_STATUS !== 'COMPLETED')
                      }
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/pm-accept`, { actor: pmActor })}
                    >
                      PM Accept
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID || a.STATUS !== 'PM_ACCEPTED'}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/compliance`, {
                        actor: complianceActor,
                        decision: 'APPROVE',
                      })}
                    >
                      Approve
                    </button>
                    <button
                      className="lt-btn lt-btn-danger"
                      disabled={busyId === a.ACTION_ID || a.STATUS !== 'PM_ACCEPTED'}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/compliance`, {
                        actor: complianceActor,
                        decision: 'DENY',
                      })}
                    >
                      Deny
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID || a.STATUS !== 'COMPLIANCE_APPROVED'}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/intent-submit`, {
                        actor: intentSubmitActor,
                        reference_id: `UI_INTENT_${Date.now()}`,
                      })}
                    >
                      Submit Intent
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID || a.STATUS !== 'INTENT_SUBMITTED'}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/intent-approve`, {
                        actor: intentApproveActor,
                      })}
                    >
                      Approve Intent
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID || !['INTENT_APPROVED', 'REVALIDATED_FAIL', 'REVALIDATED_PASS'].includes(a.STATUS)}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/revalidate`)}
                    >
                      Revalidate
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID || !['INTENT_APPROVED', 'REVALIDATED_FAIL', 'REVALIDATED_PASS'].includes(a.STATUS)}
                      onClick={() => startLivePrompt(a.ACTION_ID, 'revalidate')}
                    >
                      Live Revalidate Prompt
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID || a.STATUS !== 'REVALIDATED_PASS' || a.COMPLIANCE_STATUS !== 'APPROVE'}
                      onClick={() =>
                        runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/execute`, {
                          actor: executionActor,
                          attempt_n: 1,
                        })
                      }
                    >
                      Execute (Paper)
                    </button>
                  </div>
                </td>
              </tr>
              {expandedActionId === a.ACTION_ID ? (
                <tr key={`${a.ACTION_ID}__details`}>
                  <td colSpan={7}>
                    <div className="lt-summary"><b>Committee & Revalidation Details</b></div>
                    {committeeByAction[a.ACTION_ID]?.error ? (
                      <div className="lt-error">{committeeByAction[a.ACTION_ID].error}</div>
                    ) : (
                      <>
                        <div className="lt-summary">
                          Committee recommendation: <b>{committeeByAction[a.ACTION_ID]?.verdict?.RECOMMENDATION || '—'}</b>
                          {' '}| blocked: <b>{committeeByAction[a.ACTION_ID]?.verdict?.IS_BLOCKED == null ? '—' : (committeeByAction[a.ACTION_ID]?.verdict?.IS_BLOCKED ? 'Yes' : 'No')}</b>
                          {' '}| confidence: <b>{committeeByAction[a.ACTION_ID]?.verdict?.CONFIDENCE ?? '—'}</b>
                        </div>
                        <div className="lt-summary">
                          Joint decision: enter <b>{
                            committeeByAction[a.ACTION_ID]?.verdict?.VERDICT_JSON?.joint_decision?.should_enter == null
                              ? '—'
                              : (committeeByAction[a.ACTION_ID]?.verdict?.VERDICT_JSON?.joint_decision?.should_enter ? 'Yes' : 'No')
                          }</b>
                          {' '}| size <b>{committeeByAction[a.ACTION_ID]?.verdict?.VERDICT_JSON?.joint_decision?.position_size_factor ?? '—'}</b>
                          {' '}| target <b>{committeeByAction[a.ACTION_ID]?.verdict?.VERDICT_JSON?.joint_decision?.realistic_target_return ?? '—'}</b>
                          {' '}| hold bars <b>{committeeByAction[a.ACTION_ID]?.verdict?.VERDICT_JSON?.joint_decision?.hold_bars ?? '—'}</b>
                          {' '}| early-exit target <b>{committeeByAction[a.ACTION_ID]?.verdict?.VERDICT_JSON?.joint_decision?.acceptable_early_exit_target_return ?? '—'}</b>
                        </div>
                        <div className="lt-summary">
                          Revalidation outcome: <b>{a.REVALIDATION_OUTCOME || '—'}</b>
                          {' '}| policy: <b>{a.REVALIDATION_POLICY_VERSION || '—'}</b>
                          {' '}| data source: <b>{a.REVALIDATION_DATA_SOURCE || '—'}</b>
                        </div>
                        <div className="lt-summary">
                          Reason codes: <b>{Array.isArray(a.REASON_CODES) && a.REASON_CODES.length ? a.REASON_CODES.join(', ') : '—'}</b>
                        </div>
                        <div className="lt-summary">
                          Role summaries:
                          {' '}
                          {(committeeByAction[a.ACTION_ID]?.role_outputs || []).length === 0 ? 'No committee run recorded yet.' : ''}
                        </div>
                        {(committeeByAction[a.ACTION_ID]?.role_outputs || []).map((r) => (
                          <div key={`${a.ACTION_ID}_${r.ROLE_NAME}`} className="lt-summary">
                            <b>{r.ROLE_NAME}</b>: {r.STANCE || '—'} | conf {r.CONFIDENCE ?? '—'} | {r.SUMMARY || '—'}
                          </div>
                        ))}
                        {livePromptActionId === a.ACTION_ID ? (
                          <div className="lt-summary">
                            <b>Live {livePromptStage} prompt:</b> {livePromptStatus || '—'}
                            {(livePromptLogs || []).map((x, idx) => (
                              <div key={`${a.ACTION_ID}_lp_${idx}`}>
                                {x.round ? `[R${x.round}] ` : ''}{x.role || x.type || 'event'}: {x.output?.summary || x.summary || JSON.stringify(x.joint_decision || x.verdict || x)}
                              </div>
                            ))}
                          </div>
                        ) : null}
                      </>
                    )}
                  </td>
                </tr>
              ) : null}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>

      <div className="lt-summary">
        Live orders tracked: <b>{orders.length}</b>
      </div>

      <div className="lt-table-wrap">
        <table className="lt-table">
          <thead>
            <tr>
              <th>Updated</th>
              <th>Order</th>
              <th>Status</th>
              <th>Quantity</th>
              <th>Price</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {orders.length === 0 && (
              <tr>
                <td colSpan={6}>No live orders yet.</td>
              </tr>
            )}
            {orders.map((o) => (
              <tr key={o.ORDER_ID}>
                <td>{fmtTs(o.LAST_UPDATED_AT || o.CREATED_AT)}</td>
                <td>
                  <div><b>{o.SYMBOL}</b> ({o.SIDE})</div>
                  <div>Order: {o.ORDER_ID}</div>
                  <div>Action: {o.ACTION_ID}</div>
                </td>
                <td>{o.STATUS || '—'}</td>
                <td>
                  <div>Ordered: {fmtNum(o.QTY_ORDERED, 0)}</div>
                  <div>Filled: {fmtNum(o.QTY_FILLED, 0)}</div>
                </td>
                <td>
                  <div>Limit: {fmtNum(o.LIMIT_PRICE, 4)}</div>
                  <div>Avg fill: {fmtNum(o.AVG_FILL_PRICE, 4)}</div>
                </td>
                <td>
                  <div className="lt-actions">
                    <button
                      className="lt-btn"
                      disabled={busyId === `order:${o.ORDER_ID}:PARTIAL_FILL` || ['FILLED', 'CANCELED', 'REJECTED'].includes(o.STATUS)}
                      onClick={() => runOrderStatus(o, 'PARTIAL_FILL')}
                    >
                      Partial Fill
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === `order:${o.ORDER_ID}:FILLED` || ['FILLED', 'CANCELED', 'REJECTED'].includes(o.STATUS)}
                      onClick={() => runOrderStatus(o, 'FILLED')}
                    >
                      Fill
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === `order:${o.ORDER_ID}:CANCELED` || ['FILLED', 'CANCELED', 'REJECTED'].includes(o.STATUS)}
                      onClick={() => runOrderStatus(o, 'CANCELED')}
                    >
                      Cancel
                    </button>
                    <button
                      className="lt-btn lt-btn-danger"
                      disabled={busyId === `order:${o.ORDER_ID}:REJECTED` || ['FILLED', 'CANCELED', 'REJECTED'].includes(o.STATUS)}
                      onClick={() => runOrderStatus(o, 'REJECTED')}
                    >
                      Reject
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
