import { Fragment, useCallback, useEffect, useRef, useState } from 'react'
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

function fmtSigned(v, digits = 2) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  const abs = Math.abs(n).toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })
  if (n > 0) return `+${abs}`
  if (n < 0) return `-${abs}`
  return abs
}

function fmtAge(ts) {
  if (!ts) return '—'
  const dt = new Date(ts)
  if (Number.isNaN(dt.getTime())) return '—'
  const mins = Math.floor((Date.now() - dt.getTime()) / 60000)
  if (mins < 60) return `${Math.max(mins, 0)}m`
  const hrs = Math.floor(mins / 60)
  if (hrs < 48) return `${hrs}h`
  return `${Math.floor(hrs / 24)}d`
}

function isNewDecision(ts) {
  if (!ts) return false
  const dt = new Date(ts)
  if (Number.isNaN(dt.getTime())) return false
  return (Date.now() - dt.getTime()) <= 24 * 60 * 60 * 1000
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

function isStaleRevalidationState(decision) {
  const status = String(decision?.status || '').toUpperCase()
  const staleRelevantStatuses = new Set([
    'INTENT_APPROVED',
    'REVALIDATED_FAIL',
    'REVALIDATED_PASS',
    'EXECUTION_REQUESTED',
  ])
  if (!staleRelevantStatuses.has(status)) return false
  const reasons = Array.isArray(decision?.reason_codes) ? decision.reason_codes.map((r) => String(r || '').toUpperCase()) : []
  const staleSignals = new Set([
    'FIRST_SESSION_REALISM_1M_STALE',
    'EXECUTION_CLICK_REVALIDATION_STALE',
    'SNAPSHOT_STALE',
    'ACTION_EXPIRED',
    'MISSING_REVALIDATION',
    'FIRST_SESSION_REALISM_REVALIDATION_NOT_LATEST',
  ])
  return reasons.some((r) => staleSignals.has(r))
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 120000) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await fetch(url, { ...options, signal: controller.signal })
  } finally {
    clearTimeout(timer)
  }
}

function MiniSparkline({ points = [], color = '#1565c0' }) {
  const width = 220
  const height = 48
  const pad = 4
  const values = (points || []).map((v) => Number(v)).filter((v) => Number.isFinite(v))
  if (values.length < 2) return <div className="lpa-subtle">Not enough points yet.</div>
  const min = Math.min(...values)
  const max = Math.max(...values)
  const spread = max - min || 1
  const xStep = (width - pad * 2) / (values.length - 1)
  const path = values.map((v, i) => {
    const x = pad + i * xStep
    const y = height - pad - ((v - min) / spread) * (height - pad * 2)
    return `${i === 0 ? 'M' : 'L'}${x},${y}`
  }).join(' ')
  return (
    <svg className="lpa-sparkline" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden>
      <path d={path} fill="none" stroke={color} strokeWidth="2" />
    </svg>
  )
}

function pickBetterProtection(current, candidate) {
  if (!current) return candidate
  const score = (p) => {
    const state = String(p?.state || '').toUpperCase()
    const active = Boolean(p?.activeAtBroker)
    if (active && state === 'FULL') return 4
    if (active && state === 'PARTIAL') return 3
    if (!active && state === 'FULL') return 2
    if (!active && state === 'PARTIAL') return 1
    return 0
  }
  return score(candidate) > score(current) ? candidate : current
}

export default function LivePortfolioActivity() {
  const [overview, setOverview] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState('')
  const [ordersLookbackDays, setOrdersLookbackDays] = useState(30)
  const [ordersLimit, setOrdersLimit] = useState(120)
  const [executionsLimit, setExecutionsLimit] = useState(60)
  const [snapshotLookbackDays, setSnapshotLookbackDays] = useState(14)
  const [streamActionId, setStreamActionId] = useState('')
  const [streamStatus, setStreamStatus] = useState('')
  const [streamLogs, setStreamLogs] = useState([])
  const [activeStreamActionId, setActiveStreamActionId] = useState('')
  const [readyPulseActionId, setReadyPulseActionId] = useState('')
  const [liveLineTarget, setLiveLineTarget] = useState('')
  const [liveLineDisplay, setLiveLineDisplay] = useState('')
  const streamRef = useRef(null)
  const streamPaneRef = useRef(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const params = new URLSearchParams({
        order_lookback_days: String(ordersLookbackDays),
        order_limit: String(ordersLimit),
        execution_limit: String(executionsLimit),
        snapshot_lookback_days: String(snapshotLookbackDays),
      })
      const resp = await fetch(`${API_BASE}/live/activity/overview?${params.toString()}`)
      if (!resp.ok) throw new Error(`Failed to load activity overview (${resp.status})`)
      const data = await resp.json()
      setOverview(data)
    } catch (e) {
      setError(e.message || 'Failed to load live activity.')
    } finally {
      setLoading(false)
    }
  }, [ordersLookbackDays, ordersLimit, executionsLimit, snapshotLookbackDays])

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

  useEffect(() => {
    if (!streamPaneRef.current) return
    streamPaneRef.current.scrollTop = streamPaneRef.current.scrollHeight
  }, [streamLogs, streamStatus])

  useEffect(() => {
    if (!liveLineTarget) {
      setLiveLineDisplay('')
      return undefined
    }
    let idx = 0
    setLiveLineDisplay('')
    const timer = setInterval(() => {
      idx += 1
      setLiveLineDisplay(liveLineTarget.slice(0, idx))
      if (idx >= liveLineTarget.length) {
        clearInterval(timer)
      }
    }, 12)
    return () => clearInterval(timer)
  }, [liveLineTarget])

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

  const finalizeCommitteeRevalidation = useCallback(async (actionId, verdict) => {
    setBusy(`committee:${actionId}`)
    setError('')
    try {
      const resp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/committee/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          actor: 'committee_orchestrator',
          model: 'claude-3-5-sonnet',
          verdict: verdict || {},
        }),
      })
      if (!resp.ok) {
        let msg = `Committee revalidation failed (${resp.status})`
        try {
          const j = await resp.json()
          if (j?.detail?.reason_codes?.length) {
            msg = `${j.detail.message || 'Committee revalidation blocked'}: ${j.detail.reason_codes.join(', ')}`
          } else if (j?.detail) {
            msg = typeof j.detail === 'string' ? j.detail : JSON.stringify(j.detail)
          }
        } catch {
          // fallback message
        }
        throw new Error(msg)
      }
      const applyData = await resp.json()
      const nextStatus = String(applyData?.action_status || '').toUpperCase()
      const canRunApproveFlow = ['READY_FOR_APPROVAL_FLOW', 'PM_ACCEPTED', 'COMPLIANCE_APPROVED', 'INTENT_SUBMITTED'].includes(nextStatus)
      if (canRunApproveFlow) {
        setStreamStatus('Advancing approval flow...')
        setLiveLineTarget('Committee complete. Advancing PM/Compliance/Intent approvals...')
        const approveResp = await fetch(`${API_BASE}/live/decisions/${actionId}/approve-flow`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({}),
        })
        if (!approveResp.ok) {
          let msg = `Approve flow failed (${approveResp.status})`
          try {
            const j = await approveResp.json()
            if (j?.detail?.reason_codes?.length) {
              msg = `${j.detail.message || 'Approve flow blocked'}: ${j.detail.reason_codes.join(', ')}`
            } else if (j?.detail) {
              msg = typeof j.detail === 'string' ? j.detail : JSON.stringify(j.detail)
            }
          } catch {
            // fallback
          }
          throw new Error(msg)
        }
      }
      const canRunRevalidate = ['INTENT_APPROVED', 'REVALIDATED_FAIL', 'REVALIDATED_PASS'].includes(nextStatus) || canRunApproveFlow
      if (canRunRevalidate) {
        setStreamStatus('Revalidating 1m freshness...')
        setLiveLineTarget('Applying committee result and forcing 1m-bar revalidation...')
        const revalResp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/revalidate`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ force_refresh_1m: true }),
        })
        if (!revalResp.ok) {
          let msg = `Revalidation failed (${revalResp.status})`
          try {
            const j = await revalResp.json()
            if (j?.detail?.reason_codes?.length) {
              msg = `${j.detail.message || 'Revalidation blocked'}: ${j.detail.reason_codes.join(', ')}`
            } else if (j?.detail) {
              msg = typeof j.detail === 'string' ? j.detail : JSON.stringify(j.detail)
            }
          } catch {
            // keep fallback
          }
          throw new Error(msg)
        }
        setLiveLineTarget('Revalidation complete. If gates are clear, decision is ready to submit.')
      } else {
        setLiveLineTarget('Committee updated. No further revalidation step available for this status yet.')
      }
      await load()
      setStreamStatus('Completed')
      setReadyPulseActionId(actionId)
      setTimeout(() => setReadyPulseActionId(''), 20000)
      setActiveStreamActionId('')
    } catch (e) {
      setError(e.message || 'Committee revalidation failed.')
      setStreamStatus('Stopped')
      setActiveStreamActionId('')
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
    setActiveStreamActionId(actionId)
    setStreamStatus('Connecting...')
    setStreamLogs([{ type: 'system', summary: 'Starting committee stream...' }])
    setLiveLineTarget('Starting committee stream...')
    const es = new EventSource(
      `${API_BASE}/live/trades/actions/${actionId}/committee/live-prompt?actor=committee_orchestrator&model=claude-3-5-sonnet`,
    )
    streamRef.current = es

    es.addEventListener('start', (evt) => {
      setStreamStatus('Running...')
      try {
        const data = JSON.parse(evt.data)
        setStreamLogs((prev) => [...prev, { type: 'start', ...data }])
        setLiveLineTarget(`start: ${JSON.stringify(data)}`)
      } catch {
        setStreamLogs((prev) => [...prev, { type: 'start', summary: 'Committee run started.' }])
        setLiveLineTarget('Committee run started.')
      }
    })
    es.addEventListener('agent_turn', (evt) => {
      try {
        const data = JSON.parse(evt.data)
        setStreamLogs((prev) => [...prev, data])
        setLiveLineTarget(`${data.role || data.type || 'agent'}: ${data.output?.summary || data.summary || '...'}`)
      } catch {
        // Ignore malformed frame
      }
    })
    es.addEventListener('role_summary', (evt) => {
      try {
        const data = JSON.parse(evt.data)
        setStreamLogs((prev) => [...prev, data])
        setLiveLineTarget(`${data.role || 'role'}: ${data.summary || '...'}`)
      } catch {
        // Ignore malformed frame
      }
    })
    es.addEventListener('heartbeat', () => {
      setLiveLineTarget('Agents are thinking...')
    })
    es.addEventListener('final', (evt) => {
      let verdictPayload = {}
      try {
        const data = JSON.parse(evt.data)
        verdictPayload = data?.verdict || {}
        setStreamLogs((prev) => [...prev, { type: 'final', ...data }])
        setLiveLineTarget(`final: ${JSON.stringify(data?.joint_decision || data?.verdict || {})}`)
      } catch {
        // Ignore malformed frame
      }
      setStreamStatus('Finalizing...')
      es.close()
      streamRef.current = null
      void finalizeCommitteeRevalidation(actionId, verdictPayload)
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
      setLiveLineTarget(`error: ${detail}`)
      setStreamStatus('Stopped')
      setActiveStreamActionId('')
      es.close()
      streamRef.current = null
    })
  }, [finalizeCommitteeRevalidation])

  const submitOnly = useCallback(async (actionId) => {
    setBusy(`submit:${actionId}`)
    setError('')
    try {
      const resp = await fetchWithTimeout(`${API_BASE}/live/decisions/${actionId}/submit-only`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      }, 180000)
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
      if (e?.name === 'AbortError') {
        setError('Submit request timed out. The backend may still be processing; click Refresh From IB to reconcile latest state.')
      } else {
        setError(e.message || 'Submit failed.')
      }
    } finally {
      setBusy('')
    }
  }, [load])

  const rejectStale = useCallback(async (actionId) => {
    setBusy(`reject:${actionId}`)
    setError('')
    try {
      const resp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/reject-stale`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ actor: 'portfolio_manager' }),
      })
      if (!resp.ok) {
        let msg = `Reject stale failed (${resp.status})`
        try {
          const j = await resp.json()
          if (j?.detail?.reason_codes?.length) {
            msg = `${j.detail.message || 'Reject stale blocked'}: ${j.detail.reason_codes.join(', ')}`
          }
        } catch {
          // fallback
        }
        throw new Error(msg)
      }
      await load()
    } catch (e) {
      setError(e.message || 'Reject stale failed.')
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
  const navTrend = overview?.activity_trends?.nav || []
  const positionTrend = overview?.activity_trends?.positions || []
  const outsideHours = readiness.market_open === false
  const posCount = openPositions.length
  const totalMarketValue = openPositions.reduce((sum, p) => sum + Number(p?.MARKET_VALUE || 0), 0)
  const totalUnrealized = openPositions.reduce((sum, p) => sum + Number(p?.UNREALIZED_PNL || 0), 0)
  const winners = openPositions.filter((p) => Number(p?.UNREALIZED_PNL || 0) > 0).length
  const losers = openPositions.filter((p) => Number(p?.UNREALIZED_PNL || 0) < 0).length
  const tradeNotional = executions.reduce((sum, e) => {
    const q = Number(e?.qty_filled || 0)
    const px = Number(e?.avg_fill_price || 0)
    if (!Number.isFinite(q) || !Number.isFinite(px)) return sum
    return sum + Math.abs(q * px)
  }, 0)
  const trendWindowDays = Number(overview?.account_kpis?.trend_window_days || snapshotLookbackDays)
  const navChangeAbs = overview?.account_kpis?.trend_nav_change_abs
  const navChangePct = overview?.account_kpis?.trend_nav_change_pct
  const trendUnrealizedChange = overview?.account_kpis?.trend_unrealized_change_abs
  const protectionBySymbol = orders.reduce((acc, o) => {
    const symbol = String(o?.SYMBOL || '').toUpperCase()
    if (!symbol) return acc
    const prot = o?.PROTECTION || {}
    const tp = prot?.take_profit || null
    const sl = prot?.stop_loss || null
    const activeAtBroker = Boolean(tp?.broker_truth_active || sl?.broker_truth_active)
    const summary = {
      state: String(prot?.state || 'NONE').toUpperCase(),
      activeAtBroker,
      tpStatus: tp?.status || null,
      tpPrice: tp?.limit_price,
      slStatus: sl?.status || null,
      slPrice: sl?.limit_price,
      updatedAt: o?.LAST_UPDATED_AT || o?.CREATED_AT || null,
    }
    const existing = acc.get(symbol)
    const better = pickBetterProtection(existing, summary)
    if (better === existing && existing && summary.updatedAt && existing.updatedAt && summary.updatedAt > existing.updatedAt) {
      acc.set(symbol, summary)
      return acc
    }
    acc.set(symbol, better)
    return acc
  }, new Map())

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
            <div className="lpa-visuals-head">
              <div>
                <h3>Snapshot Trends</h3>
                <div className="lpa-subtle">Useful visuals from stored IB snapshots (refresh creates a new point).</div>
              </div>
              <label className="lpa-control">
                <span>Trend Window</span>
                <select value={snapshotLookbackDays} onChange={(e) => setSnapshotLookbackDays(Number(e.target.value))}>
                  <option value={1}>1 day</option>
                  <option value={7}>7 days</option>
                  <option value={14}>14 days</option>
                  <option value={30}>30 days</option>
                  <option value={90}>90 days</option>
                </select>
              </label>
            </div>
            <div className="lpa-spark-grid">
              <div className="lpa-spark-card">
                <div className="lpa-spark-title">NAV</div>
                <div className="lpa-spark-value">{fmtNum(kpis.equity_nav_eur, 2)}</div>
                <div className={`lpa-spark-delta ${(Number(navChangeAbs || 0) >= 0) ? 'lpa-pos' : 'lpa-neg'}`}>
                  {fmtSigned(navChangeAbs, 2)} ({fmtPct(navChangePct)})
                </div>
                <MiniSparkline points={navTrend.map((p) => p.nav_eur)} color="#1565c0" />
                <div className="lpa-subtle">{trendWindowDays}d window</div>
              </div>
              <div className="lpa-spark-card">
                <div className="lpa-spark-title">Unrealized P&L</div>
                <div className={`lpa-spark-value ${totalUnrealized >= 0 ? 'lpa-pos' : 'lpa-neg'}`}>{fmtSigned(totalUnrealized, 2)}</div>
                <div className={`lpa-spark-delta ${(Number(trendUnrealizedChange || 0) >= 0) ? 'lpa-pos' : 'lpa-neg'}`}>
                  {fmtSigned(trendUnrealizedChange, 2)}
                </div>
                <MiniSparkline points={positionTrend.map((p) => p.total_unrealized_pnl)} color="#2e7d32" />
                <div className="lpa-subtle">{trendWindowDays}d change</div>
              </div>
              <div className="lpa-spark-card">
                <div className="lpa-spark-title">Gross Exposure</div>
                <div className="lpa-spark-value">{fmtNum(kpis.gross_exposure_eur, 2)}</div>
                <div className="lpa-spark-delta lpa-subtle">Cash: {fmtNum(kpis.cash_eur, 2)}</div>
                <MiniSparkline points={navTrend.map((p) => p.gross_exposure_eur)} color="#6a1b9a" />
                <div className="lpa-subtle">Open positions: {kpis.open_positions_count ?? 0}</div>
              </div>
            </div>
          </section>

          <section className="lpa-section">
            <h3>Pending Decisions</h3>
            <div className="lpa-subtle">
              Decisions not yet broker-opened. Workflow: Committee Revalidation, then Submit.
            </div>
            {outsideHours ? <div className="lpa-subtle">Market is closed. Submit sends DAY orders that IB queues for next session.</div> : null}
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
                    <Fragment key={d.action_id}>
                    {(() => {
                      const statusUpper = String(d.status || '').toUpperCase()
                      const canSubmit = statusUpper === 'REVALIDATED_PASS'
                      const canRunCommittee = [
                        'OPEN_ELIGIBLE',
                        'OPEN_CAUTION',
                        'PENDING_OPEN_STABILITY_REVIEW',
                        'READY_FOR_APPROVAL_FLOW',
                        'PM_ACCEPTED',
                        'COMPLIANCE_APPROVED',
                        'INTENT_SUBMITTED',
                        'INTENT_APPROVED',
                        'REVALIDATED_FAIL',
                        'REVALIDATED_PASS',
                      ].includes(statusUpper)
                      return (
                    <tr className={isStaleRevalidationState(d) ? 'lpa-row-stale' : ''}>
                      <td>
                        <div><b>{d.symbol}</b> ({d.side})</div>
                        <div>Action: {d.action_id}</div>
                        <div>Created: {fmtTs(d.timestamps?.created_at)} ({fmtAge(d.timestamps?.created_at)} ago)</div>
                        {isNewDecision(d.timestamps?.created_at) ? <div className="lpa-subtle">NEW</div> : null}
                        <div>Committee: {d.committee_verdict || '—'}</div>
                      </td>
                      <td>
                        <div>{d.status || '—'}</div>
                        <div>Compliance: {d.compliance_status || '—'}</div>
                        <div>Committee run: {d.committee_run_id || '—'}</div>
                        <div>Committee at: {fmtTs(d.committee_completed_ts)}</div>
                        <div>Protected: {d.protection?.state || 'NONE'}</div>
                        <div>Plan: {d.protection?.planned ? 'TP/SL expected' : 'No bracket planned'}</div>
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
                        {isStaleRevalidationState(d) ? (
                          <div className="lpa-warning-inline">
                            Revalidation expired - run Committee revalidation before submit.
                          </div>
                        ) : null}
                        <button
                          className="lpa-btn"
                          disabled={busy === `submit:${d.action_id}` || !canSubmit || isStaleRevalidationState(d)}
                          onClick={() => submitOnly(d.action_id)}
                        >
                          {busy === `submit:${d.action_id}` ? 'Submitting...' : 'Submit'}
                        </button>
                        <button
                          className="lpa-btn lpa-btn-secondary"
                          disabled={busy === `committee:${d.action_id}` || activeStreamActionId === d.action_id || !canRunCommittee}
                          onClick={() => {
                            openCommitteeStream(d.action_id)
                          }}
                        >
                          {busy === `committee:${d.action_id}` || activeStreamActionId === d.action_id ? 'Running...' : 'Committee revalidation'}
                        </button>
                        <button
                          className="lpa-btn lpa-btn-secondary"
                          disabled={busy === `reject:${d.action_id}`}
                          onClick={() => rejectStale(d.action_id)}
                        >
                          {busy === `reject:${d.action_id}` ? 'Rejecting...' : 'Reject stale'}
                        </button>
                        {readyPulseActionId === d.action_id ? (
                          <div className="lpa-ready-chip">Ready to submit</div>
                        ) : null}
                        {!canSubmit ? (
                          <div className="lpa-subtle">Run Committee revalidation. If committee says go, Submit will be enabled.</div>
                        ) : null}
                        {!canRunCommittee && statusUpper === 'OPEN_BLOCKED' ? (
                          <div className="lpa-subtle">Blocked by opening guard. Use Reject stale to clear, or wait for fresher opening data.</div>
                        ) : null}
                        </div>
                      </td>
                    </tr>
                      )
                    })()}
                    {streamActionId === d.action_id ? (
                      <tr>
                        <td colSpan={5}>
                          <div className="lpa-stream">
                            <div>
                              <b>Live Committee Stream</b> for {streamActionId} ({streamStatus || 'Idle'})
                            </div>
                            <div className="lpa-live-line">{liveLineDisplay}<span className="lpa-caret">|</span></div>
                            <div ref={streamPaneRef} className="lpa-stream-body">
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
                          </div>
                        </td>
                      </tr>
                    ) : null}
                    </Fragment>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="lpa-section">
            <h3>Orders (Broker Lifecycle)</h3>
            <div className="lpa-controls">
              <label className="lpa-control">
                <span>Orders Lookback</span>
                <select value={ordersLookbackDays} onChange={(e) => setOrdersLookbackDays(Number(e.target.value))}>
                  <option value={7}>7d</option>
                  <option value={30}>30d</option>
                  <option value={90}>90d</option>
                  <option value={180}>180d</option>
                </select>
              </label>
              <label className="lpa-control">
                <span>Order Rows</span>
                <select value={ordersLimit} onChange={(e) => setOrdersLimit(Number(e.target.value))}>
                  <option value={60}>60</option>
                  <option value={120}>120</option>
                  <option value={250}>250</option>
                  <option value={500}>500</option>
                </select>
              </label>
              <label className="lpa-control">
                <span>Trade Rows</span>
                <select value={executionsLimit} onChange={(e) => setExecutionsLimit(Number(e.target.value))}>
                  <option value={30}>30</option>
                  <option value={60}>60</option>
                  <option value={120}>120</option>
                  <option value={250}>250</option>
                </select>
              </label>
            </div>
            <div className="lpa-table-wrap">
              <table className="lpa-table">
                <thead>
                  <tr>
                    <th>Order</th>
                    <th>Status</th>
                    <th>Qty</th>
                    <th>Price</th>
                    <th>Protection</th>
                    <th>Timestamps</th>
                  </tr>
                </thead>
                <tbody>
                  {orders.length === 0 && <tr><td colSpan={6}>No broker orders.</td></tr>}
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
                        <div><b>{o.PROTECTION?.state || 'NONE'}</b></div>
                        <div>Parent: {o.PROTECTION?.parent?.status || '—'}</div>
                        <div>TP: {o.PROTECTION?.take_profit?.status || '—'}</div>
                        <div>SL: {o.PROTECTION?.stop_loss?.status || '—'}</div>
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
            <div className="lpa-split">
              <div className="lpa-panel lpa-panel--positions">
                <h3>Open Positions (IBKR Truth)</h3>
                <div className="lpa-subtle">Snapshot-stored broker truth. Updated on refresh.</div>
                <div className="lpa-mini-kpis">
                  <div><span>Positions</span><b>{posCount}</b></div>
                  <div><span>Mkt Value</span><b>{fmtNum(totalMarketValue, 2)}</b></div>
                  <div><span>Unrealized P&L</span><b className={totalUnrealized >= 0 ? 'lpa-pos' : 'lpa-neg'}>{fmtSigned(totalUnrealized, 2)}</b></div>
                  <div><span>Winners / Losers</span><b>{winners} / {losers}</b></div>
                </div>
                <div className="lpa-table-wrap">
                  <table className="lpa-table">
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th>Qty</th>
                        <th>Avg Cost</th>
                        <th>Mkt Value</th>
                        <th>P&L</th>
                        <th>Exit Setup</th>
                      </tr>
                    </thead>
                    <tbody>
                      {openPositions.length === 0 && <tr><td colSpan={6}>No open broker positions.</td></tr>}
                      {openPositions.map((p, idx) => {
                        const symbol = String(p.SYMBOL || '').toUpperCase()
                        const exit = protectionBySymbol.get(symbol)
                        const hasExit = exit && exit.state !== 'NONE'
                        return (
                          <tr key={`${p.SYMBOL || 'SYM'}_${idx}`}>
                            <td>
                              <div><b>{p.SYMBOL || '—'}</b></div>
                              <div className="lpa-subtle">{p.SECURITY_TYPE || '—'}</div>
                            </td>
                            <td>{fmtNum(p.POSITION_QTY, 0)}</td>
                            <td>{fmtNum(p.AVG_COST, 4)}</td>
                            <td>{fmtNum(p.MARKET_VALUE, 2)}</td>
                            <td className={Number(p.UNREALIZED_PNL || 0) >= 0 ? 'lpa-pos' : 'lpa-neg'}>{fmtSigned(p.UNREALIZED_PNL, 2)}</td>
                            <td>
                              {!hasExit ? (
                                <div className="lpa-subtle">No TP/SL linked in latest order bundle.</div>
                              ) : (
                                <div className="lpa-exit-setup">
                                  <div>
                                    <span className={`lpa-protect-chip lpa-protect-chip--${exit.activeAtBroker ? 'armed' : 'idle'}`}>
                                      {exit.activeAtBroker ? 'Armed at IB' : 'Not armed'}
                                    </span>
                                  </div>
                                  <div>State: <b>{exit.state}</b></div>
                                  <div>TP: {exit.tpStatus || '—'} {exit.tpPrice != null ? `@ ${fmtNum(exit.tpPrice, 4)}` : ''}</div>
                                  <div>SL: {exit.slStatus || '—'} {exit.slPrice != null ? `@ ${fmtNum(exit.slPrice, 4)}` : ''}</div>
                                </div>
                              )}
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="lpa-panel lpa-panel--trades">
                <h3>Trades</h3>
                <div className="lpa-subtle">Executed fills from broker lifecycle.</div>
                <div className="lpa-mini-kpis">
                  <div><span>Lookback</span><b>{executions.length} total</b></div>
                  <div><span>Notional</span><b>{fmtNum(tradeNotional, 2)}</b></div>
                </div>
                <div className="lpa-table-wrap">
                  <table className="lpa-table">
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Qty</th>
                        <th>Notional</th>
                        <th>P&L</th>
                      </tr>
                    </thead>
                    <tbody>
                      {executions.length === 0 && <tr><td colSpan={5}>No executions yet.</td></tr>}
                      {executions.map((e) => {
                        const side = String(e.side || '').toUpperCase()
                        const qty = Number(e.qty_filled || 0)
                        const px = Number(e.avg_fill_price || 0)
                        const notional = Number.isFinite(qty) && Number.isFinite(px) ? Math.abs(qty * px) : null
                        return (
                          <tr key={`${e.order_id}_${e.execution_ts || 'ts'}`}>
                            <td>
                              <div><b>{e.symbol}</b></div>
                              <div className="lpa-subtle">{fmtTs(e.execution_ts)}</div>
                            </td>
                            <td><span className={`lpa-side-chip lpa-side-chip--${side === 'BUY' ? 'buy' : 'sell'}`}>{side || '—'}</span></td>
                            <td>{fmtNum(e.qty_filled, 0)}</td>
                            <td>{fmtNum(notional, 2)}</td>
                            <td>—</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </section>
        </>
      )}
    </div>
  )
}
