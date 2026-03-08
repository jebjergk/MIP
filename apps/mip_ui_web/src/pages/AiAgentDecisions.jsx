import { useCallback, useEffect, useState } from 'react'
import { API_BASE } from '../App'
import './AiAgentDecisions.css'

function fmtTs(ts) {
  if (!ts) return '—'
  try { return new Date(ts).toLocaleString() } catch { return ts }
}

function fmtNum(v, d = 4) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toFixed(d)
}

export default function AiAgentDecisions() {
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [runId, setRunId] = useState('')
  const [status, setStatus] = useState('')
  const [selected, setSelected] = useState(null)
  const [detail, setDetail] = useState(null)
  const [detailError, setDetailError] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const qs = new URLSearchParams({ limit: '200' })
      if (runId.trim()) qs.set('run_id', runId.trim())
      if (status) qs.set('status', status)
      const resp = await fetch(`${API_BASE}/decisions/sim-agent-decisions?${qs.toString()}`)
      if (!resp.ok) throw new Error(`Failed to load AI decisions (${resp.status})`)
      const data = await resp.json()
      setRows(Array.isArray(data?.decisions) ? data.decisions : [])
    } catch (e) {
      setError(e.message || 'Failed to load AI decisions.')
      setRows([])
    } finally {
      setLoading(false)
    }
  }, [runId, status])

  useEffect(() => { load() }, [load])

  const loadDetail = useCallback(async (proposalId) => {
    setSelected(proposalId)
    setDetail(null)
    setDetailError('')
    try {
      const resp = await fetch(`${API_BASE}/decisions/sim-agent-decisions/${proposalId}`)
      if (!resp.ok) throw new Error(`Failed to load decision detail (${resp.status})`)
      const data = await resp.json()
      setDetail(data)
    } catch (e) {
      setDetailError(e.message || 'Failed to load detail.')
    }
  }, [])

  return (
    <div className="page ai-agent-decisions-page">
      <div className="aad-header">
        <div>
          <h2>AI Agent Decisions</h2>
          <p>Simulation committee decisions with joint outcomes and transcript.</p>
        </div>
      </div>

      <div className="aad-filters">
        <input
          value={runId}
          onChange={(e) => setRunId(e.target.value)}
          placeholder="Optional run_id filter"
        />
        <select value={status} onChange={(e) => setStatus(e.target.value)}>
          <option value="">All statuses</option>
          <option value="PROPOSED">PROPOSED</option>
          <option value="APPROVED">APPROVED</option>
          <option value="REJECTED">REJECTED</option>
          <option value="EXECUTED">EXECUTED</option>
        </select>
        <button className="aad-btn" onClick={load}>Refresh</button>
      </div>

      {error ? <div className="aad-error">{error}</div> : null}
      {loading ? <div>Loading AI agent decisions...</div> : null}

      <div className="aad-layout">
        <div className="aad-table-wrap">
          <table className="aad-table">
            <thead>
              <tr>
                <th>Proposed</th>
                <th>Symbol</th>
                <th>Status</th>
                <th>Joint Decision</th>
                <th>Summary</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr><td colSpan={6}>No simulation AI decisions found.</td></tr>
              )}
              {rows.map((r) => (
                <tr key={r.proposal_id} className={selected === r.proposal_id ? 'is-selected' : ''}>
                  <td>{fmtTs(r.proposed_at)}</td>
                  <td>
                    <div><b>{r.symbol}</b> ({r.side})</div>
                    <div>Proposal #{r.proposal_id}</div>
                    <div>Portfolio {r.portfolio_id}</div>
                  </td>
                  <td>{r.status || '—'}</td>
                  <td>
                    <div>Enter: {r.joint_decision?.should_enter == null ? '—' : (r.joint_decision.should_enter ? 'Yes' : 'No')}</div>
                    <div>Size: {fmtNum(r.joint_decision?.size_factor, 2)}</div>
                    <div>Target: {fmtNum(r.joint_decision?.target_return, 3)}</div>
                    <div>Hold bars: {r.joint_decision?.hold_bars ?? '—'}</div>
                    <div>Early-exit: {fmtNum(r.joint_decision?.early_exit_target_return, 3)}</div>
                  </td>
                  <td>{r.summary || '—'}</td>
                  <td>
                    <button className="aad-btn" onClick={() => loadDetail(r.proposal_id)}>View</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="aad-detail">
          <h3>Conversation Transcript</h3>
          {!selected ? <p>Select a row to view transcript.</p> : null}
          {detailError ? <div className="aad-error">{detailError}</div> : null}
          {detail ? (
            <>
              <div className="aad-meta">
                <div><b>Proposal:</b> #{detail.proposal_id}</div>
                <div><b>Run:</b> {detail.run_id || '—'}</div>
                <div><b>Status:</b> {detail.status || '—'}</div>
              </div>
              <div className="aad-meta">
                <div><b>Joint decision:</b></div>
                <div>Enter: {detail.joint_decision?.should_enter == null ? '—' : (detail.joint_decision.should_enter ? 'Yes' : 'No')}</div>
                <div>Size: {fmtNum(detail.joint_decision?.size_factor, 2)}</div>
                <div>Target: {fmtNum(detail.joint_decision?.target_return, 3)}</div>
                <div>Hold bars: {detail.joint_decision?.hold_bars ?? '—'}</div>
                <div>Early-exit target: {fmtNum(detail.joint_decision?.early_exit_target_return, 3)}</div>
              </div>
              <div className="aad-transcript">
                {(detail.agent_dialogue || []).length === 0 ? (
                  <div className="aad-line">No transcript payload recorded for this proposal.</div>
                ) : (
                  detail.agent_dialogue.map((m, idx) => (
                    <div key={`${detail.proposal_id}_${idx}`} className="aad-line">
                      <b>{m?.role || `Agent ${idx + 1}`}:</b> {m?.message || '—'}
                    </div>
                  ))
                )}
              </div>
            </>
          ) : null}
        </div>
      </div>
    </div>
  )
}
