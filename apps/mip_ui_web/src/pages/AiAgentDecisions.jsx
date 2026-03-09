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

function parseMaybeJson(v) {
  if (!v) return null
  if (typeof v === 'object') return v
  if (typeof v === 'string') {
    try { return JSON.parse(v) } catch { return null }
  }
  return null
}

export default function AiAgentDecisions() {
  const [mode, setMode] = useState('simulation') // simulation | live
  const [liveLatestPerSymbol, setLiveLatestPerSymbol] = useState(true)
  const [simLatestPerSymbol, setSimLatestPerSymbol] = useState(true)
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [info, setInfo] = useState('')
  const [simRunId, setSimRunId] = useState('')
  const [simStatus, setSimStatus] = useState('')
  const [liveStatus, setLiveStatus] = useState('')
  const [selected, setSelected] = useState(null)
  const [detail, setDetail] = useState(null)
  const [detailError, setDetailError] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    setInfo('')
    try {
      if (mode === 'simulation') {
        const qs = new URLSearchParams({ limit: '200' })
        if (simRunId.trim()) qs.set('run_id', simRunId.trim())
        if (simStatus) qs.set('status', simStatus)
        qs.set('committee_only', 'true')
        let resp = await fetch(`${API_BASE}/decisions/sim-agent-decisions?${qs.toString()}`)
        if (!resp.ok) throw new Error(`Failed to load simulation AI decisions (${resp.status})`)
        let data = await resp.json()
        let decisions = Array.isArray(data?.decisions) ? data.decisions : []
        if (decisions.length === 0) {
          const fallbackQs = new URLSearchParams({ limit: '200' })
          if (simRunId.trim()) fallbackQs.set('run_id', simRunId.trim())
          if (simStatus) fallbackQs.set('status', simStatus)
          resp = await fetch(`${API_BASE}/decisions/sim-agent-decisions?${fallbackQs.toString()}`)
          if (!resp.ok) throw new Error(`Failed to load simulation fallback decisions (${resp.status})`)
          data = await resp.json()
          decisions = Array.isArray(data?.decisions) ? data.decisions : []
          if (decisions.length > 0) {
            setInfo('No simulation committee outputs found yet; showing latest simulation proposals instead.')
          }
        }
        if (simLatestPerSymbol) {
          const perSymbol = new Map()
          decisions.forEach((r) => {
            const sym = String(r?.symbol || '').toUpperCase()
            const pid = String(r?.portfolio_id ?? '')
            if (!sym) return
            const key = `${pid}|${sym}`
            const prev = perSymbol.get(key)
            const prevTs = prev?.proposed_at ? new Date(prev.proposed_at).getTime() : -1
            const nextTs = r?.proposed_at ? new Date(r.proposed_at).getTime() : -1
            if (!prev || nextTs >= prevTs) perSymbol.set(key, r)
          })
          decisions = Array.from(perSymbol.values()).sort((a, b) => new Date(b.proposed_at || 0) - new Date(a.proposed_at || 0))
        }
        setRows(decisions)
      } else {
        const qs = new URLSearchParams({ pending_only: 'false', limit: '300' })
        const resp = await fetch(`${API_BASE}/live/trades/actions?${qs.toString()}`)
        if (!resp.ok) throw new Error(`Failed to load live AI decisions (${resp.status})`)
        const data = await resp.json()
        const actions = Array.isArray(data?.actions) ? data.actions : []
        let mapped = actions
          .filter((a) => !liveStatus || String(a.STATUS || '').toUpperCase() === liveStatus)
          .map((a) => ({
            action_id: a.ACTION_ID,
            portfolio_id: a.PORTFOLIO_ID,
            symbol: a.SYMBOL,
            market_type: a.ASSET_CLASS,
            side: a.SIDE,
            status: a.STATUS,
            proposed_at: a.CREATED_AT,
            committee_status: a.COMMITTEE_STATUS,
            committee_verdict: a.COMMITTEE_VERDICT,
            committee_summary: a.COMMITTEE_SUMMARY,
            committee_joint_decision: parseMaybeJson(a.COMMITTEE_JOINT_DECISION),
            revalidation_outcome: a.REVALIDATION_OUTCOME,
            reason_codes: Array.isArray(a.REASON_CODES) ? a.REASON_CODES : [],
          }))
        if (liveLatestPerSymbol) {
          const perSymbol = new Map()
          mapped.forEach((r) => {
            const key = String(r.symbol || '').toUpperCase()
            if (!key) return
            const prev = perSymbol.get(key)
            const prevTs = prev?.proposed_at ? new Date(prev.proposed_at).getTime() : -1
            const nextTs = r?.proposed_at ? new Date(r.proposed_at).getTime() : -1
            if (!prev || nextTs >= prevTs) perSymbol.set(key, r)
          })
          mapped = Array.from(perSymbol.values()).sort((a, b) => new Date(b.proposed_at || 0) - new Date(a.proposed_at || 0))
        }
        setRows(mapped)
      }
    } catch (e) {
      setError(e.message || 'Failed to load AI decisions.')
      setRows([])
    } finally {
      setLoading(false)
    }
  }, [mode, simRunId, simStatus, liveStatus, liveLatestPerSymbol, simLatestPerSymbol])

  useEffect(() => {
    setSelected(null)
    setDetail(null)
    setDetailError('')
    load()
  }, [load])

  const loadDetail = useCallback(async (id) => {
    setSelected(id)
    setDetail(null)
    setDetailError('')
    try {
      if (mode === 'simulation') {
        const resp = await fetch(`${API_BASE}/decisions/sim-agent-decisions/${id}`)
        if (!resp.ok) throw new Error(`Failed to load decision detail (${resp.status})`)
        const data = await resp.json()
        setDetail({ mode: 'simulation', ...data })
      } else {
        const base = rows.find((r) => r.action_id === id)
        const committeeResp = await fetch(`${API_BASE}/live/trades/actions/${id}/committee`)
        const committee = committeeResp.ok ? await committeeResp.json() : { role_outputs: [], verdict: null }
        setDetail({
          mode: 'live',
          ...base,
          committee,
        })
      }
    } catch (e) {
      setDetailError(e.message || 'Failed to load detail.')
    }
  }, [mode, rows])

  return (
    <div className="page ai-agent-decisions-page">
      <div className="aad-header">
        <div>
          <h2>AI Agent Decisions</h2>
          <p>Committee outcomes and transcripts across simulation and live workflows.</p>
        </div>
      </div>

      <div className="aad-tabs">
        <button
          className={`aad-tab ${mode === 'simulation' ? 'is-active' : ''}`}
          onClick={() => setMode('simulation')}
        >
          Simulation Decisions
        </button>
        <button
          className={`aad-tab ${mode === 'live' ? 'is-active' : ''}`}
          onClick={() => setMode('live')}
        >
          Live Decisions
        </button>
      </div>

      <div className="aad-filters">
        <input
          value={simRunId}
          onChange={(e) => setSimRunId(e.target.value)}
          placeholder={mode === 'simulation' ? 'Optional run_id filter' : 'run_id not used in live list'}
          disabled={mode !== 'simulation'}
        />
        <select
          value={mode === 'simulation' ? simStatus : liveStatus}
          onChange={(e) => (mode === 'simulation' ? setSimStatus(e.target.value) : setLiveStatus(e.target.value))}
        >
          <option value="">All statuses</option>
          {mode === 'simulation' ? (
            <>
              <option value="PROPOSED">PROPOSED</option>
              <option value="APPROVED">APPROVED</option>
              <option value="REJECTED">REJECTED</option>
              <option value="EXECUTED">EXECUTED</option>
            </>
          ) : (
            <>
              <option value="PENDING_OPEN_VALIDATION">PENDING_OPEN_VALIDATION</option>
              <option value="OPEN_BLOCKED">OPEN_BLOCKED</option>
              <option value="OPEN_CAUTION">OPEN_CAUTION</option>
              <option value="OPEN_ELIGIBLE">OPEN_ELIGIBLE</option>
              <option value="PENDING_OPEN_STABILITY_REVIEW">PENDING_OPEN_STABILITY_REVIEW</option>
              <option value="READY_FOR_APPROVAL_FLOW">READY_FOR_APPROVAL_FLOW</option>
              <option value="RESEARCH_IMPORTED">RESEARCH_IMPORTED</option>
              <option value="PROPOSED">PROPOSED</option>
              <option value="APPROVED">APPROVED</option>
              <option value="PM_ACCEPTED">PM_ACCEPTED</option>
              <option value="COMPLIANCE_APPROVED">COMPLIANCE_APPROVED</option>
              <option value="INTENT_SUBMITTED">INTENT_SUBMITTED</option>
              <option value="INTENT_APPROVED">INTENT_APPROVED</option>
              <option value="REVALIDATED_PASS">REVALIDATED_PASS</option>
              <option value="REVALIDATED_FAIL">REVALIDATED_FAIL</option>
              <option value="REJECTED">REJECTED</option>
              <option value="EXECUTED">EXECUTED</option>
            </>
          )}
        </select>
        {mode === 'live' ? (
          <label style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
            <input
              type="checkbox"
              checked={liveLatestPerSymbol}
              onChange={(e) => setLiveLatestPerSymbol(e.target.checked)}
            />
            Latest action per symbol
          </label>
        ) : (
          <>
            <label style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
              <input
                type="checkbox"
                checked={simLatestPerSymbol}
                onChange={(e) => setSimLatestPerSymbol(e.target.checked)}
              />
              Latest proposal per symbol+portfolio
            </label>
          </>
        )}
        <button className="aad-btn" onClick={load}>Refresh</button>
      </div>

      {error ? <div className="aad-error">{error}</div> : null}
      {info ? <div className="aad-error" style={{ background: '#f0f9ff', borderColor: '#bfdbfe', color: '#1e3a8a' }}>{info}</div> : null}
      {loading ? <div>Loading {mode} AI agent decisions...</div> : null}

      <div className="aad-layout">
        <div className="aad-table-wrap">
          <table className="aad-table">
            <thead>
              <tr>
                <th>Proposed</th>
                <th>{mode === 'simulation' ? 'Proposal' : 'Action'}</th>
                <th>Status</th>
                <th>Joint Decision</th>
                <th>Summary</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr><td colSpan={6}>No {mode} AI decisions found.</td></tr>
              )}
              {rows.map((r) => (
                <tr
                  key={mode === 'simulation' ? r.proposal_id : r.action_id}
                  className={selected === (mode === 'simulation' ? r.proposal_id : r.action_id) ? 'is-selected' : ''}
                >
                  <td>{fmtTs(r.proposed_at)}</td>
                  <td>
                    <div><b>{r.symbol}</b> ({r.side})</div>
                    {mode === 'simulation' ? <div>Proposal #{r.proposal_id}</div> : <div>Action #{r.action_id}</div>}
                    <div>{mode === 'simulation' ? 'Sim' : 'Live'} Portfolio {r.portfolio_id}</div>
                  </td>
                  <td>{r.status || '—'}</td>
                  <td>
                    {mode === 'simulation' ? (
                      <>
                        <div>Enter: {r.joint_decision?.should_enter == null ? '—' : (r.joint_decision.should_enter ? 'Yes' : 'No')}</div>
                        <div>Size: {fmtNum(r.joint_decision?.size_factor, 2)}</div>
                        <div>Target: {fmtNum(r.joint_decision?.target_return, 3)}</div>
                        <div>Hold bars: {r.joint_decision?.hold_bars ?? '—'}</div>
                        <div>Early-exit: {fmtNum(r.joint_decision?.early_exit_target_return, 3)}</div>
                      </>
                    ) : (
                      <>
                        <div>Committee: {r.committee_status || '—'}</div>
                        <div>Verdict: {r.committee_verdict || '—'}</div>
                        <div>Size: {fmtNum(r.committee_joint_decision?.position_size_factor, 2)}</div>
                        <div>Target: {fmtNum(r.committee_joint_decision?.realistic_target_return, 3)}</div>
                        <div>Hold bars: {r.committee_joint_decision?.hold_bars ?? '—'}</div>
                        <div>Early-exit: {fmtNum(r.committee_joint_decision?.acceptable_early_exit_target_return, 3)}</div>
                        <div>Revalidation: {r.revalidation_outcome || '—'}</div>
                      </>
                    )}
                  </td>
                  <td>
                    {mode === 'simulation'
                      ? (r.summary || (r.has_sim_committee ? '—' : 'No sim committee output yet'))
                      : (r.committee_summary || r.reason_codes?.join(', ') || '—')}
                  </td>
                  <td>
                    <button
                      className="aad-btn"
                      onClick={() => loadDetail(mode === 'simulation' ? r.proposal_id : r.action_id)}
                    >
                      View
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="aad-detail">
          <h3>{mode === 'simulation' ? 'Simulation Transcript' : 'Live Committee Transcript'}</h3>
          {!selected ? <p>Select a row to view transcript.</p> : null}
          {detailError ? <div className="aad-error">{detailError}</div> : null}
          {detail ? (
            <>
              <div className="aad-meta">
                <div><b>{mode === 'simulation' ? 'Proposal' : 'Action'}:</b> #{mode === 'simulation' ? detail.proposal_id : detail.action_id}</div>
                <div><b>Run:</b> {detail.run_id || detail.committee?.run?.RUN_ID || '—'}</div>
                <div><b>Status:</b> {detail.status || '—'}</div>
              </div>
              <div className="aad-meta">
                <div><b>Joint decision:</b></div>
                {mode === 'simulation' ? (
                  <>
                    <div>Enter: {detail.joint_decision?.should_enter == null ? '—' : (detail.joint_decision.should_enter ? 'Yes' : 'No')}</div>
                    <div>Size: {fmtNum(detail.joint_decision?.size_factor, 2)}</div>
                    <div>Target: {fmtNum(detail.joint_decision?.target_return, 3)}</div>
                    <div>Hold bars: {detail.joint_decision?.hold_bars ?? '—'}</div>
                    <div>Early-exit target: {fmtNum(detail.joint_decision?.early_exit_target_return, 3)}</div>
                  </>
                ) : (
                  <>
                    <div>Recommendation: {detail.committee?.verdict?.RECOMMENDATION || '—'}</div>
                    <div>Size factor: {fmtNum(detail.committee?.verdict?.SIZE_FACTOR, 2)}</div>
                    <div>Confidence: {fmtNum(detail.committee?.verdict?.CONFIDENCE, 2)}</div>
                    <div>Blocked: {detail.committee?.verdict?.IS_BLOCKED == null ? '—' : (detail.committee?.verdict?.IS_BLOCKED ? 'Yes' : 'No')}</div>
                    <div>Target: {fmtNum(parseMaybeJson(detail.committee?.verdict?.VERDICT_JSON)?.verdict?.joint_decision?.realistic_target_return, 3)}</div>
                    <div>Hold bars: {parseMaybeJson(detail.committee?.verdict?.VERDICT_JSON)?.verdict?.joint_decision?.hold_bars ?? '—'}</div>
                    <div>Early-exit target: {fmtNum(parseMaybeJson(detail.committee?.verdict?.VERDICT_JSON)?.verdict?.joint_decision?.acceptable_early_exit_target_return, 3)}</div>
                  </>
                )}
              </div>
              <div className="aad-transcript">
                {(mode === 'simulation' ? (detail.agent_dialogue || []) : (detail.committee?.role_outputs || [])).length === 0 ? (
                  <div className="aad-line">No transcript payload recorded.</div>
                ) : (
                  mode === 'simulation'
                    ? detail.agent_dialogue.map((m, idx) => (
                        <div key={`${detail.proposal_id}_${idx}`} className="aad-line">
                          <b>{m?.role || `Agent ${idx + 1}`}:</b> {m?.message || '—'}
                        </div>
                      ))
                    : detail.committee.role_outputs.map((m, idx) => (
                        <div key={`${detail.action_id}_${idx}`} className="aad-line">
                          <b>{m?.ROLE_NAME || `Agent ${idx + 1}`}:</b> {m?.SUMMARY || '—'}
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
