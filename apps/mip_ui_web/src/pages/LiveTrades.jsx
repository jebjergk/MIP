import { useCallback, useEffect, useMemo, useState } from 'react'
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
  const [loading, setLoading] = useState(true)
  const [busyId, setBusyId] = useState(null)
  const [error, setError] = useState('')
  const [latestNav, setLatestNav] = useState(null)
  const [complianceActor, setComplianceActor] = useState('compliance_user')
  const [pmActor, setPmActor] = useState('portfolio_manager')
  const [createForm, setCreateForm] = useState({
    portfolio_id: 1,
    symbol: '',
    side: 'BUY',
    proposed_qty: 0,
    proposed_price: '',
  })

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const [actionsResp, snapshotResp] = await Promise.all([
        fetch(`${API_BASE}/live/trades/actions?pending_only=true&limit=300`),
        fetch(`${API_BASE}/live/snapshot/latest`),
      ])
      if (!actionsResp.ok) throw new Error(`Failed to load actions (${actionsResp.status})`)
      if (!snapshotResp.ok) throw new Error(`Failed to load snapshot (${snapshotResp.status})`)
      const actionsJson = await actionsResp.json()
      const snapJson = await snapshotResp.json()
      setActions(actionsJson.actions || [])
      setLatestNav(snapJson.latest_nav || null)
    } catch (e) {
      setError(e.message || 'Failed to load live trades data.')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
  }, [load])

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
        const msg = await resp.text()
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

  const createAction = useCallback(async () => {
    setBusyId('create')
    setError('')
    try {
      const payload = {
        portfolio_id: Number(createForm.portfolio_id),
        symbol: createForm.symbol.trim().toUpperCase(),
        side: createForm.side,
        proposed_qty: Number(createForm.proposed_qty),
        proposed_price: createForm.proposed_price === '' ? null : Number(createForm.proposed_price),
      }
      const resp = await fetch(`${API_BASE}/live/trades/actions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!resp.ok) {
        const msg = await resp.text()
        throw new Error(msg || `Create failed (${resp.status})`)
      }
      setCreateForm((v) => ({ ...v, symbol: '', proposed_qty: 0, proposed_price: '' }))
      await load()
    } catch (e) {
      setError(e.message || 'Failed to create action.')
    } finally {
      setBusyId(null)
    }
  }, [createForm, load])

  return (
    <div className="page live-trades-page">
      <div className="live-trades-header">
        <div>
          <h2>Live Trades</h2>
          <p>Compliance ledger for pending entry approvals and revalidation.</p>
        </div>
        <button className="lt-btn" disabled={busyId === 'snapshot'} onClick={refreshSnapshot}>
          {busyId === 'snapshot' ? 'Refreshing...' : 'Refresh IBKR Snapshot'}
        </button>
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
        <h3>Create Trade Intent</h3>
        <div className="lt-form-row">
          <input
            value={createForm.portfolio_id}
            onChange={(e) => setCreateForm((v) => ({ ...v, portfolio_id: e.target.value }))}
            placeholder="Portfolio ID"
          />
          <input
            value={createForm.symbol}
            onChange={(e) => setCreateForm((v) => ({ ...v, symbol: e.target.value }))}
            placeholder="Symbol"
          />
          <select
            value={createForm.side}
            onChange={(e) => setCreateForm((v) => ({ ...v, side: e.target.value }))}
          >
            <option value="BUY">BUY</option>
            <option value="SELL">SELL</option>
          </select>
          <input
            value={createForm.proposed_qty}
            onChange={(e) => setCreateForm((v) => ({ ...v, proposed_qty: e.target.value }))}
            placeholder="Quantity"
          />
          <input
            value={createForm.proposed_price}
            onChange={(e) => setCreateForm((v) => ({ ...v, proposed_price: e.target.value }))}
            placeholder="Proposed Price"
          />
          <button className="lt-btn" onClick={createAction} disabled={busyId === 'create'}>
            {busyId === 'create' ? 'Creating...' : 'Create'}
          </button>
        </div>
      </div>

      <div className="lt-actors-card">
        <label>
          PM actor
          <input value={pmActor} onChange={(e) => setPmActor(e.target.value)} />
        </label>
        <label>
          Compliance actor
          <input value={complianceActor} onChange={(e) => setComplianceActor(e.target.value)} />
        </label>
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
              <tr key={a.ACTION_ID}>
                <td>{fmtTs(a.CREATED_AT)}</td>
                <td>
                  <div><b>{a.SYMBOL}</b> ({a.SIDE})</div>
                  <div>Portfolio {a.PORTFOLIO_ID}</div>
                </td>
                <td>
                  <div>{a.STATUS}</div>
                  <div>{a.COMPLIANCE_STATUS || '—'}</div>
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
                </td>
                <td>
                  <div>By: {a.COMPLIANCE_APPROVED_BY || '—'}</div>
                  <div>At: {fmtTs(a.COMPLIANCE_DECISION_TS)}</div>
                  <div>Ref: {a.COMPLIANCE_REFERENCE_ID || '—'}</div>
                </td>
                <td>
                  <div className="lt-actions">
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/pm-accept`, { actor: pmActor })}
                    >
                      PM Accept
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/compliance`, {
                        actor: complianceActor,
                        decision: 'APPROVE',
                      })}
                    >
                      Approve
                    </button>
                    <button
                      className="lt-btn lt-btn-danger"
                      disabled={busyId === a.ACTION_ID}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/compliance`, {
                        actor: complianceActor,
                        decision: 'DENY',
                      })}
                    >
                      Deny
                    </button>
                    <button
                      className="lt-btn"
                      disabled={busyId === a.ACTION_ID}
                      onClick={() => runAction(a.ACTION_ID, `/live/trades/actions/${a.ACTION_ID}/revalidate`)}
                    >
                      Revalidate
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
