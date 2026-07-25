import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../config/apiBase'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import { usePortfolio } from '../context/PortfolioContext'
import './LivePortfolioActivity.css'
import { useAskMipPageRuntime } from '../hooks/useAskMipPageRuntime'
import LpaCommittee2Exhibits from './LpaCommittee2Exhibits'
import {
  normalizeShadowBoardResponse,
  isShadowStatusTerminal,
} from '../components/committee/shadowResponse'

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

const LPA_FLOW_STEPS = [
  { id: 'review', label: 'Review' },
  { id: 'outcome', label: 'Outcome' },
  { id: 'price', label: 'Price check' },
  { id: 'submit', label: 'Submit' },
]

const LPA_POSITIVE_AUTHORITY = new Set(['AGENTIC_APPROVE', 'AGENTIC_APPROVE_REDUCED'])
const LPA_BLOCKING_AUTHORITY = new Set([
  'AGENTIC_WAIT_RECLAIM',
  'AGENTIC_DEFER',
  'AGENTIC_REJECT',
  'AGENTIC_DEGRADED_NO_AUTHORITY',
  'AGENTIC_FAILED_NO_AUTHORITY',
])

function computeLpaFlowStep({
  isStructuralEntry,
  statusUpper,
  canSubmit,
  shadowIsTerminal,
  shadowRunning,
  authorityStatus,
  isOperatorCommitted,
}) {
  if (!isStructuralEntry) return null
  if (canSubmit) return 'submit'
  const auth = String(authorityStatus || '').toUpperCase()
  const positiveCommitted = isOperatorCommitted && LPA_POSITIVE_AUTHORITY.has(auth)
  // OPEN_BLOCKED = execution/opening guard — committee may already be done.
  if (statusUpper === 'OPEN_BLOCKED' && positiveCommitted) return 'opening'
  if (positiveCommitted && statusUpper !== 'REVALIDATED_PASS') {
    if (['INTENT_APPROVED', 'REVALIDATED_FAIL'].includes(statusUpper)) return 'price'
    // Pre-approval statuses: opening guard must clear before price check.
    if (statusUpper !== 'OPEN_BLOCKED') return 'opening'
  }
  if (shadowIsTerminal) {
    if (LPA_BLOCKING_AUTHORITY.has(auth)) return 'outcome'
    if (LPA_POSITIVE_AUTHORITY.has(auth) && !isOperatorCommitted) return 'outcome'
    if (positiveCommitted) return statusUpper === 'OPEN_BLOCKED' ? 'opening' : 'price'
    return 'outcome'
  }
  if (shadowRunning || !shadowIsTerminal) return 'review'
  return 'review'
}

function lpaFlowStepsForRow(statusUpper) {
  if (String(statusUpper || '').toUpperCase() === 'OPEN_BLOCKED') {
    return [
      { id: 'review', label: 'Review' },
      { id: 'outcome', label: 'Outcome' },
      { id: 'opening', label: 'Opening guard' },
      { id: 'price', label: 'Price check' },
      { id: 'submit', label: 'Submit' },
    ]
  }
  return LPA_FLOW_STEPS
}

function LpaStepIndicator({ currentStepId, statusUpper }) {
  if (!currentStepId) return null
  const steps = lpaFlowStepsForRow(statusUpper)
  const idx = steps.findIndex((s) => s.id === currentStepId)
  return (
    <div className="lpa-flow-steps" aria-label="Submission progress">
      {steps.map((step, i) => {
        const done = idx > i
        const active = step.id === currentStepId
        return (
          <Fragment key={step.id}>
            {i > 0 ? <span className={`lpa-flow-arrow${done || active ? ' lpa-flow-arrow--lit' : ''}`} aria-hidden>→</span> : null}
            <span
              className={`lpa-flow-step${done ? ' lpa-flow-step--done' : ''}${active ? ' lpa-flow-step--active' : ''}`}
            >
              {step.label}
            </span>
          </Fragment>
        )
      })}
    </div>
  )
}

function lpaOutcomeMessage(authorityStatus) {
  const s = String(authorityStatus || '').toUpperCase()
  if (s === 'AGENTIC_WAIT_RECLAIM') {
    return {
      title: 'Wait / Reclaim',
      body: 'Entry is not ready — the committee wants a better reclaim. Re-run review when price improves, or Reject this action.',
    }
  }
  if (s === 'AGENTIC_DEFER') {
    return {
      title: 'Deferred',
      body: 'The committee deferred this entry. Re-run review when conditions improve, or Reject.',
    }
  }
  if (s === 'AGENTIC_REJECT') {
    return {
      title: 'Rejected',
      body: 'The committee rejected this entry. Submit will not enable — use Reject stale to clear the row.',
    }
  }
  if (s === 'AGENTIC_DEGRADED_NO_AUTHORITY' || s === 'AGENTIC_FAILED_NO_AUTHORITY') {
    return {
      title: 'Review unavailable',
      body: 'The Agentic Committee did not produce a usable verdict. Re-run review or Reject.',
    }
  }
  return null
}

function fmtMaybePending(v, formatter) {
  if (v == null) return 'Pending'
  return formatter(v)
}

function explainReasonCode(code) {
  const c = String(code || '').toUpperCase()
  const map = {
    OPEN_MARKET_CLOSED: 'Market is closed right now',
    OPEN_SNAPSHOT_MISSING: 'No fresh opening snapshot yet',
    OPEN_LIVE_GUARD_FAILED: 'Opening safety checks failed',
    OPEN_GAP_UNAVAILABLE: 'Opening gap data is unavailable',
    SNAPSHOT_STALE: 'Latest market snapshot is stale',
    ACTION_EXPIRED: 'This action has expired and needs refresh',
    DRIFT_UNRESOLVED: 'Broker and local state need reconciliation before trading',
    UNRESOLVED_DRIFT_LOG_PRESENT: 'Broker and local state need reconciliation before trading',
    BROKER_EXECUTION_UNMAPPED: 'Recent broker fills are not linked locally yet. Reconcile first.',
    BROKER_RECONCILIATION_REQUIRED: 'Recent broker fills are not linked locally yet. Reconcile first.',
    EXECUTION_CLICK_REVALIDATION_STALE: 'Decision is stale. Re-run revalidation before submitting.',
    PRICE_GUARD_FAIL: 'Latest price check failed. Re-run revalidation before submitting.',
    EXIT_REVALIDATION_MARKET_BYPASS: 'Exit uses a market order; price guard vs reference quote was skipped.',
    REVALIDATION_PRICE_FROM_IBKR_DIRECT: 'Reference price taken from live IBKR 1m refresh (informational).',
    EXIT_REVALIDATION_STALE_BAR_BYPASS: 'Exit revalidation allowed with an older bar (market exit).',
    IBKR_TRUTH_MISSING_ORDER_ACK:
      'After submit, snapshot showed neither your open order nor the expected position. Refresh IB, retry, or check exec vs snapshot ports.',
    PRIOR_SUBMIT_BROKER_UNCONFIRMED:
      'A prior submit may have reached IB but was not confirmed here—do not repeat the same attempt; refresh IB then use a new attempt if needed.',
    LIVE_IDEMPOTENT_SUBMIT_ALREADY_RECORDED: 'This execution attempt already has order rows; duplicate IB submit is blocked.',
    EXIT_NO_LONG_POSITION_AT_BROKER: 'Broker shows no long shares to sell-to-close—refresh IB; your exit may have already filled.',
    EXIT_NO_SHORT_POSITION_AT_BROKER: 'Broker shows no short to cover—refresh IB before retrying.',
    COMPLIANCE_NOT_APPROVED: 'Decision is not approved for execution yet.',
    STRUCTURAL_AGENTIC_REVIEWED: 'Agentic intelligence review applied to this structural entry.',
    AGENTIC_AUTHORITY_AGENTIC_APPROVE: 'Agentic board: Approved.',
    AGENTIC_AUTHORITY_AGENTIC_APPROVE_REDUCED: 'Agentic board: Approved (reduced size).',
    AGENTIC_AUTHORITY_AGENTIC_FAILED_NO_AUTHORITY: 'Agentic review unavailable — run Intelligence Review again.',
    AGENTIC_AUTHORITY_STALE: 'Agentic authority is stale — re-commit after a fresh review.',
    AGENTIC_SIZE_POSTURE_REDUCED: 'Agentic sizing posture: reduced.',
    STRUCT_SUBMIT_CONTRACT_INCOMPLETE: 'Executable protection contract incomplete — re-apply agentic review.',
    LIVE_RISK_REWARD_TOO_LOW: 'Live bracket R/R below minimum — protection targets need refresh.',
  }
  return map[c] || c.replaceAll('_', ' ')
}

function isAgenticAuthoritySyncReasonCode(code) {
  const c = String(code || '').toUpperCase()
  return (
    c.startsWith('AGENTIC_AUTHORITY_') ||
    c === 'STRUCTURAL_AGENTIC_REVIEWED' ||
    c === 'AGENTIC_SIZE_POSTURE_REDUCED'
  )
}

/** Hide superseded agentic tags when the live gate says authority is current. */
function displayReasonCodes(decision) {
  const codes = Array.isArray(decision?.reason_codes) ? decision.reason_codes : []
  const gate = decision?.agentic_authority_gate
  if (!gate?.gate_enabled || gate.gate_ok !== true) return codes
  const status = String(gate.authority_status || '').toUpperCase()
  const expected = status ? `AGENTIC_AUTHORITY_${status}` : ''
  return codes.filter((code) => {
    const c = String(code || '').toUpperCase()
    if (!isAgenticAuthoritySyncReasonCode(c)) return true
    if (c === 'AGENTIC_AUTHORITY_STALE') return false
    if (c.startsWith('AGENTIC_AUTHORITY_') && expected && c !== expected) return false
    return true
  })
}

function summarizeReasonCodes(reasonCodes) {
  const items = Array.isArray(reasonCodes) ? reasonCodes.map((c) => explainReasonCode(c)) : []
  const unique = [...new Set(items.filter(Boolean))]
  return unique.length ? unique.join(' ') : ''
}

function hasPriceGuardFailReason(reasonCodes) {
  if (!Array.isArray(reasonCodes)) return false
  return reasonCodes.some((c) => String(c || '').toUpperCase() === 'PRICE_GUARD_FAIL')
}

function messageFromApiFailure(payload, fallback) {
  const detail = payload?.detail
  if (typeof detail === 'string' && detail.trim()) return detail
  const reasonCodes = detail?.reason_codes
  if (Array.isArray(reasonCodes) && reasonCodes.length > 0) {
    const reasonMsg = summarizeReasonCodes(reasonCodes)
    if (reasonMsg) return reasonMsg
  }
  if (detail?.message && typeof detail.message === 'string') return detail.message
  return fallback
}

function stateClass(value) {
  const v = String(value || '').toUpperCase()
  if (v === 'FRESH' || v === 'CLEAR' || v === 'FILLED') return 'ok'
  if (v === 'AGING' || v === 'WARNING' || v === 'PARTIAL_FILL') return 'warn'
  if (v === 'STALE' || v === 'BLOCKED' || v === 'REJECTED' || v === 'CANCELED') return 'bad'
  return 'neutral'
}

function formatExecutionSideLabel(exec) {
  const side = String(exec?.side || '').toUpperCase()
  const context = String(exec?.execution_context || '').toUpperCase()
  if (context === 'CLOSE_SHORT') return 'BUY (COVER)'
  if (context === 'CLOSE_LONG') return 'SELL (CLOSE)'
  if (context === 'OPEN_OR_ADD_LONG') return 'BUY (OPEN)'
  if (context === 'OPEN_OR_ADD_SHORT') return 'SELL (SHORT)'
  const intent = String(exec?.action_intent || '').toUpperCase()
  if (intent === 'EXIT' && side === 'BUY') return 'BUY (COVER)'
  if (intent === 'EXIT' && side === 'SELL') return 'SELL (CLOSE)'
  if (intent === 'ENTRY' && side === 'BUY') return 'BUY (OPEN)'
  if (intent === 'ENTRY' && side === 'SELL') return 'SELL (SHORT)'
  return side || '—'
}

/** For P&L column copy when value is missing */
function executionPnlContext(exec) {
  const c = String(exec?.execution_context || '').toUpperCase()
  if (c === 'OPEN_OR_ADD_LONG' || c === 'OPEN_OR_ADD_SHORT') return 'open'
  if (c === 'CLOSE_LONG' || c === 'CLOSE_SHORT') return 'close'
  return 'unknown'
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
  useAskMipPageRuntime('live_portfolio_activity', ['live_orders', 'live_positions', 'live_fills'], {
    session_mode: 'live',
  })
  const { formatSymbolLabel } = useSymbolMeta()
  const {
    portfolios,
    selectedPortfolioId,
    setSelectedPortfolioId,
    selectedPortfolio,
    loading: portfolioLoading,
  } = usePortfolio()
  const [overview, setOverview] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const feedbackRef = useRef(null)
  const [busy, setBusy] = useState('')
  const [ordersLookbackDays, setOrdersLookbackDays] = useState(30)
  const [ordersLimit, setOrdersLimit] = useState(120)
  const [ordersView, setOrdersView] = useState('active')
  const [executionsLimit, setExecutionsLimit] = useState(60)
  const [snapshotLookbackDays, setSnapshotLookbackDays] = useState(14)
  const [streamActionId, setStreamActionId] = useState('')
  const [streamStatus, setStreamStatus] = useState('')
  const [streamLogs, setStreamLogs] = useState([])
  const [sessionProbe, setSessionProbe] = useState(null)
  const [sessionProbeLoading, setSessionProbeLoading] = useState(false)
  const [showRiskPanel, setShowRiskPanel] = useState(false)
  const [activeStreamActionId, setActiveStreamActionId] = useState('')
  const [readyPulseActionId, setReadyPulseActionId] = useState('')
  const [liveLineTarget, setLiveLineTarget] = useState('')
  const [liveLineDisplay, setLiveLineDisplay] = useState('')
  const streamRef = useRef(null)
  const streamPaneRef = useRef(null)
  /** Tracks whether the open stream is structural (Committee 2.0) for finalize copy. */
  const committeeStreamContextRef = useRef({ structural: false })
  /** Last Committee 2.0 orchestrate result per action (structural ENTRY). */
  const [c20OrchestrateByAction, setC20OrchestrateByAction] = useState({})
  /** Inline proof exhibits expanded per structural entry action. */
  const [c20ExpandedByAction, setC20ExpandedByAction] = useState({})
  const [c20AgenticVisibleByAction, setC20AgenticVisibleByAction] = useState({})
  // Stage 3: deterministic baseline panel is collapsible / visually secondary.
  // Defaults to collapsed; auto-expanded while loading or on error so the
  // operator never loses visibility on a failure mode.
  const [c20BaselineExpandedByAction, setC20BaselineExpandedByAction] = useState({})
  /**
   * Stage 2: bounded shadow-board poll per structural ENTRY action.
   * Headline (agentic / shadow chair verdict) is the primary intelligence
   * readout; deterministic baseline remains the materialization source
   * until Stage 4. Polling is strictly bounded (max 5 attempts @ 1.5s)
   * and never blocks operator flow.
   */
  const [shadowBoardByAction, setShadowBoardByAction] = useState({})
  const shadowPollersRef = useRef({})

  /**
   * Stage 4c: latest agentic-authority row per action.
   *
   * Read from GET /live/trades/actions/{action_id}/agentic-authority. Used to
   * render the authority chip + "Apply Agentic Review" / "Re-commit (stale)"
   * buttons on the Shadow Chair Verdict headline.
   *
   * Stage 4c contract: this is observation-only. The chip and button never
   * gate Submit — Submit gating still flows through REVALIDATED_PASS +
   * submission_allowed. Stage 4d will start gating on OPERATOR_COMMITTED rows.
   *
   * Shape per action:
   *   { loading, error, authority, gate_eligible, display, fetchedAt }
   * authority is the row from V_AGENTIC_AUTHORITY_LATEST or null.
   */
  const [agenticAuthorityByAction, setAgenticAuthorityByAction] = useState({})
  const [agenticCommitBusyByAction, setAgenticCommitBusyByAction] = useState({})
  const agenticAuthorityFetchInFlightRef = useRef({})
  const autoPriceCheckDoneRef = useRef({})

  useEffect(() => {
    return () => {
      const pollers = shadowPollersRef.current || {}
      Object.values(pollers).forEach((ctx) => {
        if (!ctx) return
        ctx.cancelled = true
        if (ctx.timer) clearTimeout(ctx.timer)
      })
      shadowPollersRef.current = {}
    }
  }, [])

  const load = useCallback(async (opts = {}) => {
    if (!selectedPortfolioId) return   // guard: wait for portfolio selection
    const silent = Boolean(opts.silent)
    if (!silent) {
      setLoading(true)
      setError('')
      setNotice('')
    }
    try {
      const params = new URLSearchParams({
        portfolio_id: String(selectedPortfolioId),
        order_lookback_days: String(ordersLookbackDays),
        order_limit: String(ordersLimit),
        execution_limit: String(executionsLimit),
        snapshot_lookback_days: String(snapshotLookbackDays),
        include_legacy: 'false',
      })
      const resp = await fetch(`${API_BASE}/live/activity/overview?${params.toString()}`)
      if (!resp.ok) throw new Error('Could not load live activity. Please refresh.')
      const data = await resp.json()
      setOverview(data)
    } catch (e) {
      setError(e.message || 'Failed to load live activity.')
    } finally {
      if (!silent) setLoading(false)
    }
  }, [selectedPortfolioId, ordersLookbackDays, ordersLimit, executionsLimit, snapshotLookbackDays])

  useEffect(() => {
    load()
  }, [load])

  useEffect(() => {
    if (!selectedPortfolioId) {
      setSessionProbe(null)
      return
    }
    let cancelled = false
    setSessionProbeLoading(true)
    fetch(`${API_BASE}/live/ibkr/session-probe?portfolio_id=${selectedPortfolioId}`)
      .then((r) => r.json())
      .then((data) => { if (!cancelled) setSessionProbe(data) })
      .catch(() => {
        if (!cancelled) setSessionProbe({ status: 'PROBE_ERROR', connected: false, detected_accounts: [] })
      })
      .finally(() => { if (!cancelled) setSessionProbeLoading(false) })
    return () => { cancelled = true }
  }, [selectedPortfolioId])

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
    setNotice('')
    try {
      const qs = selectedPortfolioId ? `?portfolio_id=${selectedPortfolioId}` : ''
      const resp = await fetch(`${API_BASE}/live/snapshot/refresh${qs}`, { method: 'POST' })
      if (!resp.ok) throw new Error(`Broker refresh failed (${resp.status})`)
      const refreshData = await resp.json().catch(() => null)
      const imp = refreshData?.structural_import
      if (imp?.imported_count > 0) {
        setNotice(
          `Broker synced — imported ${imp.imported_count} structural proposal(s) into pending decisions.`,
        )
      } else if (imp?.attempted && imp?.ok === false) {
        const msg = imp?.error?.message || imp?.error || 'proposal import failed'
        setNotice(`Broker synced but structural import failed: ${String(msg).slice(0, 240)}`)
      }
      await load()
    } catch (e) {
      setError(e.message || 'Broker refresh failed.')
    } finally {
      setBusy('')
    }
  }, [load, selectedPortfolioId])

  const cancelSingleOrder = useCallback(async (order) => {
    const orderId = order?.ORDER_ID
    if (!orderId) return
    if (!window.confirm(`Cancel order ${orderId} (${order?.SYMBOL || '—'} ${order?.SIDE || '—'})?`)) return
    setBusy(`cancelOrder:${orderId}`)
    setError('')
    setNotice('')
    try {
      const resp = await fetch(`${API_BASE}/live/orders/${orderId}/cancel`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          actor: 'portfolio_manager',
          dry_run: false,
          include_local_sync: true,
        }),
      })
      if (!resp.ok) {
        const body = await resp.json().catch(() => null)
        throw new Error(messageFromApiFailure(body, 'Cancel could not be completed right now.'))
      }
      setNotice(`Cancel submitted for order ${orderId}.`)
      await load()
    } catch (e) {
      setError(e.message || 'Cancel order failed.')
    } finally {
      setBusy('')
    }
  }, [load])

  const advanceLiveActionAfterCommitteeApply = useCallback(
    async (actionId, applyData, opts = {}) => {
      const isStructuralC20Flow = Boolean(opts.isStructuralC20Flow)
      let nextStatus = String(applyData?.action_status || '').toUpperCase()

      if (nextStatus === 'OPEN_BLOCKED') {
        setStreamStatus('Rechecking opening guard...')
        setLiveLineTarget('Agentic verdict committed — rechecking opening guard before price check...')
        const openingResp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/opening/validate`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ force_refresh_1m: true }),
        })
        const openingBody = await openingResp.json().catch(() => null)
        if (!openingResp.ok) {
          throw new Error(messageFromApiFailure(openingBody, 'Opening guard recheck is currently blocked.'))
        }
        nextStatus = String(openingBody?.status || '').toUpperCase()
        if (nextStatus === 'OPEN_BLOCKED') {
          await load({ silent: true })
          setLiveLineTarget('Opening guard still blocked — retry when the market is open.')
          setStreamStatus('Opening guard blocked')
          setActiveStreamActionId('')
          return
        }
      }

      const canRunApproveFlow = [
        'READY_FOR_APPROVAL_FLOW',
        'PM_ACCEPTED',
        'COMPLIANCE_APPROVED',
        'INTENT_SUBMITTED',
        'OPEN_ELIGIBLE',
        'OPEN_CAUTION',
        'PENDING_OPEN_STABILITY_REVIEW',
        'PENDING_OPEN_VALIDATION',
      ].includes(nextStatus)
      if (canRunApproveFlow) {
        setStreamStatus('Advancing approval flow...')
        setLiveLineTarget(
          isStructuralC20Flow
            ? 'Intelligence Review verdict applied. Advancing PM/Compliance/Intent approvals...'
            : 'Committee complete. Advancing PM/Compliance/Intent approvals...',
        )
        const approveResp = await fetch(`${API_BASE}/live/decisions/${actionId}/approve-flow`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({}),
        })
        if (!approveResp.ok) {
          const body = await approveResp.json().catch(() => null)
          throw new Error(messageFromApiFailure(body, 'Approval flow is currently blocked.'))
        }
      }
      const canRunRevalidate =
        ['INTENT_APPROVED', 'REVALIDATED_FAIL', 'REVALIDATED_PASS'].includes(nextStatus) || canRunApproveFlow
      if (canRunRevalidate) {
        setStreamStatus('Revalidating 1m freshness...')
        setLiveLineTarget('Applying committee result and forcing 1m-bar revalidation...')
        const revalResp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/revalidate`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ force_refresh_1m: true }),
        })
        if (!revalResp.ok) {
          const body = await revalResp.json().catch(() => null)
          throw new Error(messageFromApiFailure(body, 'Revalidation is currently blocked.'))
        }
        setLiveLineTarget('Revalidation complete. If gates are clear, decision is ready to submit.')
      } else {
        setLiveLineTarget(
          isStructuralC20Flow
            ? 'Intelligence Review verdict updated. No further revalidation step available for this status yet.'
            : 'Committee updated. No further revalidation step available for this status yet.',
        )
      }
      await load({ silent: true })
      setStreamStatus('Completed')
      setReadyPulseActionId(actionId)
      setTimeout(() => setReadyPulseActionId(''), 20000)
      setActiveStreamActionId('')
    },
    [load],
  )

  /**
   * Stage 2: bounded shadow-board poll for the Shadow Chair Verdict
   * headline on a structural ENTRY action.
   *
   * Strictly bounded:
   *   - Max 5 attempts at ~1.5s = ~7.5s wall clock.
   *   - Stops immediately on terminal status (COMPLETE / DEGRADED / FAILED).
   *   - Stops on max attempts (marks status = 'TIMEOUT').
   *   - Stops on cancel (new poll started for same action, or component unmount).
   *
   * All fetch errors are swallowed into UI state. The headline degrades
   * gracefully; it never throws and never blocks submission.
   */
  const startBoundedShadowPoll = useCallback((actionId, hearingId) => {
    if (!actionId || !hearingId) return

    const prior = shadowPollersRef.current[actionId]
    if (prior) {
      prior.cancelled = true
      if (prior.timer) clearTimeout(prior.timer)
    }

    const ctx = { cancelled: false, attempts: 0, timer: null }
    shadowPollersRef.current[actionId] = ctx

    // Two-phase bounded poll (still has a hard ceiling — no continuous
    // background loops). The shadow chair under pack v2.0.0 + sonnet-4.6
    // typically completes in ~100–180s, so the original 5×1.5s=7.5s
    // window always fell through to TIMEOUT and froze the headline at
    // "still running" forever.
    //
    //   - Fast phase: 5 × 1500ms (= 7.5s) catches early stance / specialist
    //     bubbles and any unusually fast chair.
    //   - Slow phase: 6 × 30000ms (= 180s) covers the chair-completion
    //     window without hammering the API while specialists deliberate.
    //   Total ceiling: 11 fetches over ~187s. Cancels immediately on
    //   the first terminal status (COMPLETE / DEGRADED / FAILED).
    //
    // Belt-and-braces: when the user expands proof exhibits, that
    // panel's own long-running fetch pushes the latest normalized
    // payload back into shadowBoardByAction via onShadowSessionLoaded
    // (see LpaCommittee2Exhibits mount below). So even if a chair runs
    // longer than 187s, opening exhibits resolves the headline.
    const FAST_ATTEMPTS = 5
    const FAST_INTERVAL_MS = 1500
    const SLOW_ATTEMPTS = 6
    const SLOW_INTERVAL_MS = 30000
    const MAX_ATTEMPTS = FAST_ATTEMPTS + SLOW_ATTEMPTS

    setShadowBoardByAction((prev) => ({
      ...prev,
      [actionId]: {
        polling: true,
        hearingId,
        status: 'RUNNING',
        stance: null,
        confidence: null,
        thesisHealth: null,
        primaryReasonCode: null,
        whyNotOpposite: null,
        degraded: false,
        degradedReason: null,
        chair: null,
        error: null,
        attempts: 0,
      },
    }))

    const tick = async () => {
      if (ctx.cancelled) return
      ctx.attempts += 1

      try {
        const r = await fetch(
          `${API_BASE}/committee/hearing/${encodeURIComponent(hearingId)}/shadow-board`,
        )
        if (ctx.cancelled) return
        if (r.ok) {
          const j = await r.json().catch(() => null)
          if (j) {
            const normalized = normalizeShadowBoardResponse(j)
            const terminal = isShadowStatusTerminal(normalized.status)
            const stillPolling = !terminal && ctx.attempts < MAX_ATTEMPTS
            setShadowBoardByAction((prev) => {
              const cur = prev[actionId] || {}
              const sessionChanged = Boolean(
                normalized.sessionId
                && cur.sessionId
                && normalized.sessionId !== cur.sessionId,
              )
              const freshRun = sessionChanged || (
                String(normalized.status || '').toUpperCase() === 'RUNNING'
                && !isShadowStatusTerminal(cur.status)
                && ctx.attempts <= 1
              )
              // Never downgrade a stance/confidence we already have on the
              // same sealed session. Do NOT carry a prior session's verdict
              // into a new RUNNING review.
              const nextStance = freshRun
                ? (normalized.stance ?? null)
                : (
                  normalized.stance != null && normalized.stance !== ''
                    ? normalized.stance
                    : cur.stance
                )
              const nextConfidence = freshRun
                ? (normalized.confidence ?? null)
                : (
                  normalized.confidence != null
                    ? normalized.confidence
                    : cur.confidence
                )
              // Once terminal is observed, lock it in — later fetches that
              // happen to return a non-terminal status (rare; would only
              // occur on a race) must not overwrite it.
              const nextStatus = isShadowStatusTerminal(cur.status)
                ? cur.status
                : normalized.status
              return {
                ...prev,
                [actionId]: {
                  ...cur,
                  ...normalized,
                  stance: nextStance,
                  confidence: nextConfidence,
                  status: nextStatus,
                  hearingId,
                  polling: stillPolling,
                  error: null,
                  attempts: ctx.attempts,
                  chair: freshRun ? null : (normalized.chair ?? cur.chair ?? null),
                  raw: normalized.raw ?? cur.raw ?? null,
                },
              }
            })
            if (terminal) {
              ctx.cancelled = true
              return
            }
          }
        }
        // 404 / non-OK / parse failure: keep polling silently until max attempts.
      } catch (_e) {
        // Network error swallowed — never crash the row.
      }

      if (ctx.attempts >= MAX_ATTEMPTS) {
        setShadowBoardByAction((prev) => {
          const cur = prev[actionId] || {}
          const finalStatus = cur.status && cur.status !== 'RUNNING' ? cur.status : 'TIMEOUT'
          return {
            ...prev,
            [actionId]: { ...cur, polling: false, status: finalStatus, attempts: ctx.attempts },
          }
        })
        ctx.cancelled = true
        return
      }

      if (!ctx.cancelled) {
        const nextInterval = ctx.attempts >= FAST_ATTEMPTS ? SLOW_INTERVAL_MS : FAST_INTERVAL_MS
        ctx.timer = setTimeout(tick, nextInterval)
      }
    }

    tick()
  }, [])

  /**
   * Stage 2: when the proof-exhibits panel mounts and fetches the full
   * shadow-board payload (its internal long-running poll), it pushes the
   * normalized result back here so the LPA headline state stays in sync
   * with whatever the operator is actually looking at on screen — even
   * if our bounded poll exited earlier with status=TIMEOUT/RUNNING.
   *
   * Stage 2 contract: this only updates display state. It cannot affect
   * submit gating (REVALIDATED_PASS), materialization, or the
   * deterministic baseline.
   */
  const handleExhibitsShadowLoaded = useCallback((actionId, normalized) => {
    if (!actionId || !normalized) return
    setShadowBoardByAction((prev) => {
      const cur = prev[actionId] || {}
      // Don't downgrade a terminal status we already have to UNAVAILABLE
      // just because a transient progress fetch came back empty.
      if (normalized.status === 'UNAVAILABLE' && isShadowStatusTerminal(cur.status)) {
        return prev
      }
      // Don't downgrade a stance/confidence we already have just because a
      // transient progress fetch came back empty. The exhibits panel polls
      // its own way and can briefly return an UNAVAILABLE/RUNNING payload
      // without stance even though we already saw COMPLETE with a stance.
      // Without this guard the Shadow Chair Verdict headline visibly
      // "disappears" / drops back to placeholder after showing the verdict.
      const newSession = Boolean(
        normalized.sessionId
        && cur.sessionId
        && normalized.sessionId !== cur.sessionId,
      )
      const preserveStance =
        !newSession
        && cur.stance != null
        && (normalized.stance == null || normalized.stance === '')
      const preserveConfidence =
        !newSession
        && cur.confidence != null
        && (normalized.confidence == null)
      return {
        ...prev,
        [actionId]: {
          ...cur,
          ...normalized,
          stance: preserveStance ? cur.stance : normalized.stance,
          confidence: preserveConfidence ? cur.confidence : normalized.confidence,
          chair: newSession ? null : (normalized.chair ?? cur.chair ?? null),
          // If we already saw a terminal status, keep it. Otherwise take what
          // the exhibits push tells us.
          status: isShadowStatusTerminal(cur.status) ? cur.status : normalized.status,
          hearingId: cur.hearingId || normalized.hearingId,
          polling: cur.polling && !isShadowStatusTerminal(normalized.status),
          error: null,
        },
      }
    })
  }, [])

  /**
   * Stage 4c — fetch the latest agentic authority row for an action.
   *
   * Called automatically when the bounded shadow poll reaches a terminal
   * status (so the Preview chip can show up alongside the verdict) and
   * after a successful commit. Safe to call any time; the endpoint always
   * returns 200 (with authority:null when no row exists yet).
   *
   * Never raises. On error we just stash {error} so the UI can render a
   * subdued failure note without unmounting the headline.
   */
  const fetchAgenticAuthority = useCallback(async (actionId) => {
    if (!actionId) return null
    // Coalesce duplicate in-flight fetches.
    if (agenticAuthorityFetchInFlightRef.current[actionId]) return null
    agenticAuthorityFetchInFlightRef.current[actionId] = true
    setAgenticAuthorityByAction((prev) => ({
      ...prev,
      [actionId]: { ...(prev[actionId] || {}), loading: true, error: null },
    }))
    try {
      const r = await fetch(
        `${API_BASE}/live/trades/actions/${encodeURIComponent(actionId)}/agentic-authority`,
      )
      const body = await r.json().catch(() => null)
      if (!r.ok) {
        setAgenticAuthorityByAction((prev) => ({
          ...prev,
          [actionId]: {
            ...(prev[actionId] || {}),
            loading: false,
            error: body?.detail?.error_code || `HTTP_${r.status}`,
          },
        }))
        return null
      }
      const payload = body || {}
      setAgenticAuthorityByAction((prev) => ({
        ...prev,
        [actionId]: {
          loading: false,
          error: null,
          authority: payload.authority || null,
          gate_eligible: Boolean(payload.gate_eligible),
          display: payload.display || null,
          gate_evaluation: payload.gate_evaluation || null,
          fetchedAt: new Date().toISOString(),
        },
      }))
      return payload
    } catch (e) {
      setAgenticAuthorityByAction((prev) => ({
        ...prev,
        [actionId]: {
          ...(prev[actionId] || {}),
          loading: false,
          error: e?.message || 'fetch_failed',
        },
      }))
      return null
    } finally {
      delete agenticAuthorityFetchInFlightRef.current[actionId]
    }
  }, [])

  /**
   * Stage 4c — commit the latest shadow verdict as OPERATOR_COMMITTED
   * authority. Writes one row into MIP.APP.AGENTIC_REVALIDATION_AUTHORITY.
   *
   * Submit gating is NOT affected in Stage 4c. The commit only changes
   * `V_AGENTIC_AUTHORITY_LATEST`'s newest row to AUTHORITY_MODE=
   * OPERATOR_COMMITTED, which Stage 4d will gate on.
   *
   * The shadow_session_id + hearing_id come from `shadowBoardByAction`,
   * populated by the bounded poll / exhibits panel.
   */
  const commitAgenticAuthority = useCallback(
    async (actionId) => {
      if (!actionId) return
      const shadow = shadowBoardByAction[actionId] || null
      const sessionId = shadow?.sessionId
      const hearingId = shadow?.hearingId
      if (!sessionId || !hearingId) {
        setAgenticAuthorityByAction((prev) => ({
          ...prev,
          [actionId]: {
            ...(prev[actionId] || {}),
            error: 'NO_SESSION_OR_HEARING',
          },
        }))
        return
      }
      setAgenticCommitBusyByAction((prev) => ({ ...prev, [actionId]: true }))
      try {
        const r = await fetch(
          `${API_BASE}/live/trades/actions/${encodeURIComponent(actionId)}/agentic-authority/commit`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              shadow_session_id: sessionId,
              hearing_id: hearingId,
              actor: 'lpa_operator',
            }),
          },
        )
        const body = await r.json().catch(() => null)
        if (!r.ok) {
          const code =
            body?.detail?.error_code ||
            body?.error_code ||
            `HTTP_${r.status}`
          setAgenticAuthorityByAction((prev) => ({
            ...prev,
            [actionId]: {
              ...(prev[actionId] || {}),
              error: code,
              lastCommitMessage: body?.detail?.message || null,
            },
          }))
          return
        }
        // Synthesize the authority row from the commit response so the chip
        // updates immediately without a second roundtrip. The shape mirrors
        // V_AGENTIC_AUTHORITY_LATEST columns.
        const auth = {
          AUTHORITY_ID: body.authority_id,
          ACTION_ID: body.action_id,
          AUTHORITY_MODE: body.authority_mode,
          AUTHORITY_STATUS: body.authority_status,
          AUTHORITY_REASON_CODE: body.authority_reason_code,
          AUTHORITY_CONFIDENCE: body.authority_confidence,
          SHADOW_STANCE_RAW: body.shadow_stance_raw,
          DETERMINISTIC_BASELINE_STANCE: body.deterministic_baseline_stance,
          DISAGREES_WITH_BASELINE: body.disagrees_with_baseline,
          IS_STALE: body.is_stale,
          STALE_REASON: body.stale_reason,
          PACK_VERSION: body.pack_version,
          PACK_VERSION_OK: body.pack_version_ok,
          SESSION_AGE_MINUTES: body.session_age_minutes,
          COMMITTED_BY: 'lpa_operator',
          CREATED_AT: new Date().toISOString(),
        }
        setAgenticAuthorityByAction((prev) => ({
          ...prev,
          [actionId]: {
            loading: false,
            error: null,
            authority: auth,
            gate_eligible: Boolean(body.gate_eligible),
            display: body.display || null,
            gate_evaluation: body.gate_evaluation || null,
            lastCommitMessage: null,
            fetchedAt: new Date().toISOString(),
          },
        }))
        const advancedStatus = String(
          body?.auto_advance?.status
          || body?.agentic_materializer?.status
          || '',
        ).toUpperCase()
        if (advancedStatus !== 'REVALIDATED_PASS') {
          await advanceLiveActionAfterCommitteeApply(
            actionId,
            { action_status: advancedStatus || 'READY_FOR_APPROVAL_FLOW' },
            { isStructuralC20Flow: true },
          )
        } else {
          await load({ silent: true })
        }
      } catch (e) {
        setAgenticAuthorityByAction((prev) => ({
          ...prev,
          [actionId]: {
            ...(prev[actionId] || {}),
            error: e?.message || 'commit_failed',
          },
        }))
      } finally {
        setAgenticCommitBusyByAction((prev) => ({ ...prev, [actionId]: false }))
      }
    },
    [shadowBoardByAction, advanceLiveActionAfterCommitteeApply, load],
  )

  /**
   * Stage 4c + Phase 5C — refetch the latest authority row whenever the
   * shadow board reaches a terminal status OR when its (terminal) session id
   * changes (a fresh revalidation produces a new session). Previously this
   * effect short-circuited when *any* prior auth state existed, which left a
   * stale "Not available · conf 0.00" chip on screen after re-validations
   * because the new IS_LATEST row was never fetched.
   *
   * No-op for actions that haven't reached a terminal status yet.
   */
  const lastAuthoritySessionRef = useRef({})
  useEffect(() => {
    Object.entries(shadowBoardByAction).forEach(([actionId, state]) => {
      if (!state) return
      if (!isShadowStatusTerminal(state.status)) return
      const sessionId = state.sessionId || ''
      // Skip in-flight fetches.
      const auth = agenticAuthorityByAction[actionId]
      if (auth?.loading) return
      // Refetch when the session id changes (revalidation produced a new
      // shadow session) OR when we have never fetched for this action.
      const lastSession = lastAuthoritySessionRef.current[actionId]
      if (lastSession === sessionId && auth && (auth.authority || auth.error)) return
      lastAuthoritySessionRef.current[actionId] = sessionId
      fetchAgenticAuthority(actionId)
    })
  }, [shadowBoardByAction, agenticAuthorityByAction, fetchAgenticAuthority])

  const runCommittee2Orchestrate = useCallback(
    async (actionId, opts = {}) => {
      const forceFreshShadow = Boolean(opts.forceFreshShadow)
      setBusy(`c2orch:${actionId}`)
      setError('')
      setNotice('')
      const progressMsgs = ['Refreshing evidence dossier…', 'Running Agentic Committee…', 'Applying agentic verdict…']
      let rot = 0
      let progressTick = null
      setC20AgenticVisibleByAction((prev) => ({ ...prev, [actionId]: true }))
      setShadowBoardByAction((prev) => ({
        ...prev,
        [actionId]: {
          hearingId: prev[actionId]?.hearingId || null,
          sessionId: null,
          stance: null,
          confidence: null,
          status: 'RUNNING',
          polling: true,
          degraded: false,
          degradedReason: null,
        },
      }))
      setC20OrchestrateByAction((prev) => ({
        ...prev,
        [actionId]: {
          ...(prev[actionId] || {}),
          loading: true,
          error: null,
          progressMsg: progressMsgs[0],
          shadowReused: null,
        },
      }))
      progressTick = setInterval(() => {
        rot = (rot + 1) % progressMsgs.length
        setC20OrchestrateByAction((prev) => ({
          ...prev,
          [actionId]: { ...(prev[actionId] || {}), progressMsg: progressMsgs[rot], loading: true },
        }))
      }, 750)
      try {
        const resp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/committee2/orchestrate`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            force_rebuild_hearing: false,
            force_fresh_shadow: forceFreshShadow,
          }),
        })
        const body = await resp.json().catch(() => null)
        if (!resp.ok) {
          throw new Error(messageFromApiFailure(body, 'Agentic Review orchestration failed.'))
        }
        if (progressTick) {
          clearInterval(progressTick)
          progressTick = null
        }
        const shadowReused = body?.shadow_reused ?? body?.inline_hearing?.shadow_reused ?? null
        setC20OrchestrateByAction((prev) => ({
          ...prev,
          [actionId]: {
            loading: true,
            error: null,
            lastResult: body,
            lastAt: Date.now(),
            progressMsg: shadowReused
              ? 'Prior review reused — polling Agentic Committee…'
              : 'Fresh review running — polling Agentic Committee…',
            shadowReused,
          },
        }))
        const _shadowHearingId = body?.hearing_id || body?.inline_hearing?.hearing_id || null
        if (_shadowHearingId) {
          startBoundedShadowPoll(actionId, _shadowHearingId)
        }
        await advanceLiveActionAfterCommitteeApply(actionId, body, { isStructuralC20Flow: true })
        setC20OrchestrateByAction((prev) => ({
          ...prev,
          [actionId]: { ...(prev[actionId] || {}), loading: false, progressMsg: null },
        }))
        setNotice(
          body?.idempotent_replay
            ? `Agentic Review replay OK for ${actionId} (already materialized).`
            : shadowReused
              ? `Agentic Review refreshed for ${actionId} (prior session reused — same market snapshot).`
              : `Agentic Review started for ${actionId}.`,
        )
      } catch (e) {
        const msg = e.message || 'Agentic Review orchestration failed.'
        setC20OrchestrateByAction((prev) => ({
          ...prev,
          [actionId]: {
            // Preserve lastResult (especially hearing_id) when a downstream
            // step throws — e.g. /committee/apply or /revalidate failing with
            // IBKR_BAR_STALE_ENTRY_BLOCKED / OUTSIDE_EXTENDED_TRADING_WINDOW.
            // Orchestrate itself already succeeded and the bounded shadow poll
            // is running; without this merge the Shadow Chair Verdict headline
            // disappears because the render gate at the top of the inline
            // shadow block reads c20State.lastResult?.hearing_id.
            ...(prev[actionId] || {}),
            loading: false,
            error: msg,
            lastAt: Date.now(),
            progressMsg: null,
          },
        }))
        setError(msg)
      } finally {
        if (progressTick) clearInterval(progressTick)
        setBusy('')
      }
    },
    [advanceLiveActionAfterCommitteeApply, startBoundedShadowPoll],
  )

  const finalizeCommitteeRevalidation = useCallback(async (actionId, verdict) => {
    const syncC20 = Boolean(committeeStreamContextRef.current?.structural)
    const executionOnlyExit = Boolean(committeeStreamContextRef.current?.executionOnlyExit)
    setBusy(`committee:${actionId}`)
    setError('')
    setNotice('')
    try {
      const resp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/committee/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          actor: 'committee_orchestrator',
          model: 'claude-4-sonnet',
          verdict: verdict || {},
        }),
      })
      if (!resp.ok) {
        const body = await resp.json().catch(() => null)
        throw new Error(
          messageFromApiFailure(
            body,
            syncC20 && executionOnlyExit
              ? 'Execution replay is currently unavailable.'
              : syncC20
                ? 'Intelligence Review sync is currently unavailable.'
                : 'Committee revalidation is currently unavailable.',
          ),
        )
      }
      const applyData = await resp.json()
      await advanceLiveActionAfterCommitteeApply(actionId, applyData, { isStructuralC20Flow: syncC20 })
    } catch (e) {
      setError(
        e.message ||
          (committeeStreamContextRef.current?.structural &&
          committeeStreamContextRef.current?.executionOnlyExit
            ? 'Execution replay failed.'
            : committeeStreamContextRef.current?.structural
              ? 'Intelligence Review sync failed.'
              : 'Committee revalidation failed.'),
      )
      setStreamStatus('Stopped')
      setActiveStreamActionId('')
    } finally {
      setBusy('')
    }
  }, [advanceLiveActionAfterCommitteeApply])

  const openCommitteeStream = useCallback((actionId, opts = {}) => {
    const syncC20 = Boolean(opts.structural)
    const executionOnlyExit = Boolean(opts.executionOnlyExit)
    committeeStreamContextRef.current = { structural: syncC20, executionOnlyExit }
    if (streamRef.current) {
      streamRef.current.close()
      streamRef.current = null
    }
    setStreamActionId(actionId)
    setActiveStreamActionId(actionId)
    setStreamStatus('Connecting...')
    setStreamLogs([
      {
        type: 'system',
        summary: executionOnlyExit
          ? 'Starting execution replay stream...'
          : syncC20
            ? 'Starting Intelligence Review sync stream...'
            : 'Starting committee stream...',
      },
    ])
    setLiveLineTarget(
      executionOnlyExit
        ? 'Starting execution replay...'
        : syncC20
          ? 'Starting Intelligence Review sync...'
          : 'Starting committee stream...',
    )
    const es = new EventSource(
      `${API_BASE}/live/trades/actions/${actionId}/committee/live-prompt?actor=committee_orchestrator&model=claude-4-sonnet`,
    )
    streamRef.current = es

    es.addEventListener('start', (evt) => {
      setStreamStatus('Running...')
      try {
        const data = JSON.parse(evt.data)
        setStreamLogs((prev) => [...prev, { type: 'start', ...data }])
        setLiveLineTarget(`start: ${JSON.stringify(data)}`)
      } catch {
        setStreamLogs((prev) => [
          ...prev,
          {
            type: 'start',
            summary: executionOnlyExit
              ? 'Execution replay started.'
              : syncC20
                ? 'Intelligence Review sync started.'
                : 'Committee run started.',
          },
        ])
        setLiveLineTarget(
          executionOnlyExit
            ? 'Execution replay started.'
            : syncC20
              ? 'Intelligence Review sync started.'
              : 'Committee run started.',
        )
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

  const runRevalidateForSubmit = useCallback(async (actionId) => {
    setBusy(`revalidate:${actionId}`)
    setError('')
    setNotice('')
    try {
      const revalResp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/revalidate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ force_refresh_1m: true }),
      })
      const body = await revalResp.json().catch(() => null)
      if (!revalResp.ok) {
        throw new Error(messageFromApiFailure(body, 'Revalidation is currently blocked.'))
      }
      const nextStatus = String(body?.status || '').toUpperCase()
      setNotice(
        nextStatus === 'REVALIDATED_PASS'
          ? `Revalidation passed for ${actionId}. Submit is now enabled if all gates are clear.`
          : `Revalidation finished with status ${nextStatus || 'unknown'}.`,
      )
      await load({ silent: true })
      if (nextStatus === 'REVALIDATED_PASS') {
        setReadyPulseActionId(actionId)
        setTimeout(() => setReadyPulseActionId(''), 20000)
      }
    } catch (e) {
      setError(e.message || 'Revalidation failed.')
    } finally {
      setBusy('')
    }
  }, [load])

  const runOpeningValidation = useCallback(async (actionId) => {
    setBusy(`opening:${actionId}`)
    setError('')
    setNotice('')
    try {
      const resp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/opening/validate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ force_refresh_1m: true }),
      })
      const body = await resp.json().catch(() => null)
      if (!resp.ok) {
        throw new Error(messageFromApiFailure(body, 'Opening guard recheck failed.'))
      }
      const nextStatus = String(body?.status || '').toUpperCase()
      if (nextStatus === 'OPEN_BLOCKED') {
        const opening = body?.opening_validation || {}
        const reasons = Array.isArray(opening.reasons) ? opening.reasons : (body?.reason_codes || [])
        const refresh = opening?.refresh?.bars || {}
        const refreshStatus = String(refresh?.status || 'NOT_ATTEMPTED').toUpperCase()
        const reasonText = reasons.length
          ? reasons.map((r) => explainReasonCode(r)).join('; ')
          : 'market may be closed or snapshot stale'
        const refreshNote = refreshStatus === 'FAIL'
          ? ' IBKR 1m refresh failed — check live gateway (portfolio 2 uses port 7496, not paper 7497).'
          : refreshStatus === 'SUCCESS'
            ? ' Latest 1m bar was fetched from IBKR.'
            : ''
        setNotice(`Still blocked — ${reasonText}.${refreshNote}`)
        await load({ silent: true })
        return
      }
      const chainStatus = String(body?.agentic_materializer?.status || nextStatus).toUpperCase()
      setNotice(`Opening guard passed (${chainStatus || nextStatus}). Advancing approval and price check…`)
      await advanceLiveActionAfterCommitteeApply(actionId, { action_status: chainStatus }, { isStructuralC20Flow: true })
    } catch (e) {
      setError(e.message || 'Opening guard recheck failed.')
    } finally {
      setBusy('')
    }
  }, [load, advanceLiveActionAfterCommitteeApply])

  // Phase 5 UX: after auto-commit (or manual confirm) on APPROVE, chain opening
  // guard (if needed), approval flow, and price check.
  // Prefer overview's agentic_authority_gate (always loaded with pending rows);
  // fall back to the separate authority fetch when shadow poll populated it.
  useEffect(() => {
    const APPROVAL_CHAIN_STATUSES = [
      'READY_FOR_APPROVAL_FLOW',
      'PM_ACCEPTED',
      'COMPLIANCE_APPROVED',
      'INTENT_SUBMITTED',
      'OPEN_ELIGIBLE',
      'OPEN_CAUTION',
      'PENDING_OPEN_STABILITY_REVIEW',
      'PENDING_OPEN_VALIDATION',
    ]
    const REVALIDATE_CHAIN_STATUSES = ['INTENT_APPROVED', 'REVALIDATED_FAIL']

    const rows = overview?.pending_decisions || []
    rows.forEach((d) => {
      const isEntry = Boolean(d.structural) && String(d.action_intent || '').toUpperCase() !== 'EXIT'
      if (!isEntry) return
      const auth = agenticAuthorityByAction[d.action_id]?.authority
      const gate = d.agentic_authority_gate || {}
      const mode = String(auth?.AUTHORITY_MODE || gate.authority_mode || '').toUpperCase()
      const authorityStatus = String(auth?.AUTHORITY_STATUS || gate.authority_status || '').toUpperCase()
      const isStale = Boolean(auth?.IS_STALE ?? gate.is_stale)
      const authorityId = auth?.AUTHORITY_ID || gate.authority_id || authorityStatus
      if (mode !== 'OPERATOR_COMMITTED' || isStale) return
      if (!LPA_POSITIVE_AUTHORITY.has(authorityStatus)) return
      const actionStatus = String(d.status || '').toUpperCase()

      if (actionStatus === 'OPEN_BLOCKED') {
        const openingKey = `${d.action_id}:opening:${authorityId}`
        if (autoPriceCheckDoneRef.current[openingKey]) return
        autoPriceCheckDoneRef.current[openingKey] = true
        void runOpeningValidation(d.action_id).catch(() => {
          delete autoPriceCheckDoneRef.current[openingKey]
        })
        return
      }

      if (APPROVAL_CHAIN_STATUSES.includes(actionStatus)) {
        const chainKey = `${d.action_id}:approve:${authorityId}`
        if (autoPriceCheckDoneRef.current[chainKey]) return
        autoPriceCheckDoneRef.current[chainKey] = true
        void advanceLiveActionAfterCommitteeApply(
          d.action_id,
          { action_status: actionStatus },
          { isStructuralC20Flow: true },
        ).catch(() => {
          delete autoPriceCheckDoneRef.current[chainKey]
        })
        return
      }

      if (!REVALIDATE_CHAIN_STATUSES.includes(actionStatus)) return
      const shadow = shadowBoardByAction[d.action_id]
      const key = `${d.action_id}:${shadow?.sessionId || auth?.SHADOW_SESSION_ID || gate.authority_id || ''}`
      if (autoPriceCheckDoneRef.current[key]) return
      autoPriceCheckDoneRef.current[key] = true
      void runRevalidateForSubmit(d.action_id)
    })
  }, [overview, agenticAuthorityByAction, shadowBoardByAction, runRevalidateForSubmit, advanceLiveActionAfterCommitteeApply, runOpeningValidation])

  const submitOnly = useCallback(async (actionId) => {
    // Phase 3A: real-money confirmation gate (temporary UI guard).
    // NOTE: window.confirm is NOT sufficient for the first real-money pilot.
    // This must be replaced with a modal confirmation step with explicit
    // operator acknowledgement and a typed account confirmation before any
    // real-money execution is enabled.
    if (selectedPortfolio?.ibkr_account_mode === 'REAL') {
      const confirmed = window.confirm(
        'REAL MONEY ORDER — This will submit a live order against your real IBKR account.\n\n' +
        `Account: ${selectedPortfolio.ibkr_account_id || '(unknown)'}\n` +
        'Real money will be at risk. Verify the proposal details carefully.\n\n' +
        'Click OK to proceed or Cancel to abort.'
      )
      if (!confirmed) {
        setNotice('Submit cancelled by operator.')
        return
      }
    }

    setBusy(`submit:${actionId}`)
    setError('')
    setNotice('')
    try {
      // Send client-side context assertions so the backend can validate before broker submit.
      const assertions = {
        ...(selectedPortfolio?.portfolio_id != null    ? { portfolio_id:         Number(selectedPortfolio.portfolio_id) }    : {}),
        ...(selectedPortfolio?.ibkr_account_id        ? { ibkr_account_id:       selectedPortfolio.ibkr_account_id }         : {}),
        ...(selectedPortfolio?.broker_name            ? { broker_name:           selectedPortfolio.broker_name }             : {}),
        ...(selectedPortfolio?.broker_universe_type   ? { broker_universe_type:  selectedPortfolio.broker_universe_type }    : {}),
      }
      const resp = await fetchWithTimeout(`${API_BASE}/live/decisions/${actionId}/submit-only`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(assertions),
      }, 180000)
      if (!resp.ok) {
        const body = await resp.json().catch(() => null)
        throw new Error(messageFromApiFailure(body, 'Submit is currently blocked.'))
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
  }, [load, selectedPortfolio])

  const rejectStale = useCallback(async (actionId) => {
    setBusy(`reject:${actionId}`)
    setError('')
    setNotice('')
    try {
      const resp = await fetch(`${API_BASE}/live/trades/actions/${actionId}/reject-stale`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ actor: 'portfolio_manager' }),
      })
      if (!resp.ok) {
        const body = await resp.json().catch(() => null)
        throw new Error(messageFromApiFailure(body, 'Could not clear stale decision right now.'))
      }
      await load()
    } catch (e) {
      setError(e.message || 'Reject stale failed.')
    } finally {
      setBusy('')
    }
  }, [load])

  const scrollFeedbackIntoView = useCallback(() => {
    requestAnimationFrame(() => {
      feedbackRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    })
  }, [])

  const createExitAction = useCallback(async (positionRow) => {
    const symbol = String(positionRow?.SYMBOL || '').toUpperCase().trim()
    const portfolioId = selectedPortfolioId
    const positionQty = Number(positionRow?.POSITION_QTY || 0)
    const isShort = positionQty < 0
    const exitSideLabel = isShort ? 'BUY (cover short)' : 'SELL (close long)'
    if (!symbol || !portfolioId) {
      setNotice('')
      setError('Cannot create exit action: missing symbol or live portfolio id.')
      scrollFeedbackIntoView()
      return
    }
    const confirmed = window.confirm(
      isShort
        ? `Create exit to cover short ${symbol}? (IB will receive a BUY to close ~${Math.abs(positionQty)} shares.)`
        : `Create SELL exit to close long ${symbol}? (~${Math.abs(positionQty)} shares.)`,
    )
    if (!confirmed) {
      setError('')
      setNotice('You cancelled the confirmation dialog — no request was sent to the server.')
      scrollFeedbackIntoView()
      return
    }
    const busyKey = `exit:${symbol}`
    setBusy(busyKey)
    setError('')
    setNotice('')
    try {
      const resp = await fetch(`${API_BASE}/live/positions/exit-action`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          portfolio_id: Number(portfolioId),
          symbol,
          reason: 'Manual sell from Live Portfolio Activity open-positions table',
          auto_submit: true,
        }),
      })
      const rawText = await resp.text()
      let body = null
      try {
        body = rawText ? JSON.parse(rawText) : null
      } catch {
        body = null
      }
      if (!resp.ok) {
        throw new Error(messageFromApiFailure(body, rawText?.slice(0, 200) || 'Could not create exit decision right now.'))
      }
      const data = body && typeof body === 'object' ? body : {}
      const actionId = data?.action_id || 'new'
      const autoStatus = data?.auto_submit?.status || data?.status || ''
      if (data?.idempotent_replay) {
        setNotice(
          `Exit workflow already in progress for ${symbol} (action ${actionId}, status ${data?.status || '—'}). ` +
            'Scroll to Pending Decisions and use Submit, or Reject stale if stuck. Refresh From IB only updates data—it does not trade.',
        )
      } else {
        setNotice(`${exitSideLabel} started for ${symbol} (action ${actionId}). Latest status: ${autoStatus || '—'}. Check Pending Decisions if Submit did not run.`)
      }
      scrollFeedbackIntoView()
      await load()
    } catch (e) {
      setNotice('')
      setError(e.message || 'Create exit action failed.')
      scrollFeedbackIntoView()
    } finally {
      setBusy('')
    }
  }, [load, selectedPortfolioId, scrollFeedbackIntoView])

  const kpis = overview?.account_kpis || {}
  const pending = overview?.pending_decisions || []
  const openPositions = overview?.open_positions || []
  const orders = overview?.orders || []
  const executions = overview?.executions || []
  const archivedOrderStatuses = new Set([
    'CANCELED',
    'CANCELLED',
    'REJECTED',
    'FILLED',
    'NOT_ACTIVE_AT_BROKER',
    'UNCONFIRMED_AT_BROKER',
  ])
  const displayedOrders = orders.filter((o) => {
    const status = String(o?.STATUS || '').toUpperCase()
    const isArchived = archivedOrderStatuses.has(status)
    if (ordersView === 'archived') return isArchived
    if (ordersView === 'active') return !isArchived
    return true
  })
  const archivedOrdersCount = orders.filter((o) => archivedOrderStatuses.has(String(o?.STATUS || '').toUpperCase())).length
  const activeOrdersCount = Math.max(orders.length - archivedOrdersCount, 0)

  const pendingExitBySymbol = pending.reduce((acc, d) => {
    const intent = String(d?.action_intent || '').toUpperCase()
    if (intent !== 'EXIT') return acc
    const symbol = String(d?.symbol || '').toUpperCase()
    if (!symbol) return acc
    acc.set(symbol, d)
    return acc
  }, new Map())

  const reconV2 = overview?.reconciliation_v2 || {}
  const readiness = overview?.readiness || {}
  const driftStateDisplay = readiness?.drift_state_effective || readiness?.drift_state
  const navTrend = overview?.activity_trends?.nav || []
  const positionTrend = overview?.activity_trends?.positions || []
  const reconciliationRequired = String(readiness?.reconciliation_state || '').toUpperCase() === 'REQUIRED'
  const unmappedExecutionCount = Number(readiness?.unmapped_execution_count || 0)
  const unmappedOnFlatSymbols = Number(readiness?.unmapped_on_flat_symbols_count || 0)
  const unmappedSymbols = Array.isArray(readiness?.unmapped_execution_symbols) ? readiness.unmapped_execution_symbols : []
  const missingCloseFillCount = Number(readiness?.missing_close_fill_count || 0)
  const missingCloseFillSymbols = Array.isArray(readiness?.missing_close_fill_symbols)
    ? readiness.missing_close_fill_symbols
    : []
  const outsideHours = readiness.market_open === false
  const posCount = openPositions.length
  const totalMarketValue = openPositions.reduce((sum, p) => sum + Number(p?.MARKET_VALUE || 0), 0)
  const totalUnrealized = openPositions.reduce((sum, p) => sum + Number(p?.UNREALIZED_PNL || 0), 0)
  const winners = openPositions.filter((p) => Number(p?.UNREALIZED_PNL || 0) > 0).length
  const losers = openPositions.filter((p) => Number(p?.UNREALIZED_PNL || 0) < 0).length

  // True when the selected portfolio has IS_EXECUTION_ENABLED=false (e.g. real account in read-only mode).
  // Applied to Submit, Exit, and Approve-Flow buttons for defence-in-depth; backend gates remain authoritative.
  const executionDisabled = selectedPortfolio ? !selectedPortfolio.is_execution_enabled : true

  // Broker read is blocked when the probe hasn't resolved or shows a session mismatch/error.
  // Refresh From IB is read-only and can proceed on MATCH or MULTIPLE_ACCOUNTS even when execution is off.
  const brokerReadBlocked =
    sessionProbeLoading ||
    !sessionProbe ||
    ['ACCOUNT_MISMATCH', 'NOT_CONNECTED', 'PROBE_ERROR', 'CONFIG_NOT_FOUND'].includes(sessionProbe.status)

  // Broker execution is blocked when reading is blocked OR execution is disabled on the portfolio.
  const brokerExecutionBlocked = brokerReadBlocked || executionDisabled

  const executionsChrono = useMemo(() => {
    const list = Array.isArray(executions) ? [...executions] : []
    const ts = (e) => {
      const v = e?.execution_ts
      if (v == null) return 0
      const t = new Date(v).getTime()
      return Number.isFinite(t) ? t : 0
    }
    list.sort((a, b) => ts(b) - ts(a))
    return list
  }, [executions])

  const tradeNotional = executionsChrono.reduce((sum, e) => {
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
    const trail = prot?.trailing_stop || null
    const protectiveStop = sl || trail
    const activeAtBroker = Boolean(
      tp?.broker_truth_active ||
      sl?.broker_truth_active ||
      trail?.broker_truth_active,
    )
    const summary = {
      state: String(prot?.state || 'NONE').toUpperCase(),
      activeAtBroker,
      tpStatus: tp?.status || null,
      tpPrice: tp?.limit_price,
      slStatus: protectiveStop?.status || null,
      slPrice: protectiveStop?.limit_price,
      slKind: trail ? 'TRAIL' : (sl ? 'STOP' : null),
      trailStyle: trail?.trail_style || null,
      trailPercent: trail?.trail_percent ?? null,
      trailAmount: trail?.trail_amount ?? null,
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
        <button className="lpa-btn" disabled={busy === 'refresh' || brokerReadBlocked} onClick={refreshBroker}>
          {busy === 'refresh' ? 'Refreshing...' : 'Refresh From IB'}
        </button>
      </div>

      {/* Portfolio selector / mode banner — rendered above all action controls */}
      <div className="lpa-portfolio-bar">
        {portfolioLoading ? (
          <span className="lpa-portfolio-bar__loading">Loading portfolio config…</span>
        ) : portfolios.length > 1 ? (
          <>
            <label className="lpa-portfolio-bar__label" htmlFor="lpa-portfolio-select">Portfolio:</label>
            <select
              id="lpa-portfolio-select"
              className="lpa-portfolio-bar__select"
              value={selectedPortfolioId ?? ''}
              onChange={(e) => setSelectedPortfolioId(e.target.value ? Number(e.target.value) : null)}
            >
              {portfolios.map((p) => (
                <option key={p.portfolio_id} value={p.portfolio_id}>
                  {p.name || `Portfolio ${p.portfolio_id}`}
                  {p.ibkr_account_id ? ` (${p.ibkr_account_id})` : ''}
                </option>
              ))}
            </select>
          </>
        ) : selectedPortfolio ? (
          <span className="lpa-portfolio-bar__name">
            {selectedPortfolio.name || `Portfolio ${selectedPortfolio.portfolio_id}`}
            {selectedPortfolio.ibkr_account_id ? ` · ${selectedPortfolio.ibkr_account_id}` : ''}
          </span>
        ) : null}
        {selectedPortfolio ? (
          <span
            className={`lpa-portfolio-bar__mode lpa-portfolio-bar__mode--${(selectedPortfolio.ibkr_account_mode || 'unknown').toLowerCase()}`}
            title={
              selectedPortfolio.ibkr_account_mode === 'PAPER'
                ? 'Paper trading — no real money at risk'
                : selectedPortfolio.ibkr_account_mode === 'REAL'
                  ? 'REAL MONEY — execution is live'
                  : 'Account mode unknown'
            }
          >
            {selectedPortfolio.ibkr_account_mode === 'PAPER'
              ? 'PAPER'
              : selectedPortfolio.ibkr_account_mode === 'REAL'
                ? 'REAL MONEY'
                : 'MODE UNKNOWN'}
          </span>
        ) : null}
        {selectedPortfolio && !selectedPortfolio.is_execution_enabled ? (
          <span className="lpa-portfolio-bar__exec-disabled" title="Execution is disabled for this portfolio in LIVE_PORTFOLIO_CONFIG">
            EXECUTION DISABLED
          </span>
        ) : null}
      </div>

      {/* Session compatibility banner — shown when probe has resolved */}
      {selectedPortfolioId && !portfolioLoading && (
        <div
          className={
            `lpa-session-bar ` +
            (sessionProbeLoading
              ? 'lpa-session-bar--loading'
              : !sessionProbe || ['NOT_CONNECTED', 'PROBE_ERROR'].includes(sessionProbe?.status)
                ? 'lpa-session-bar--error'
                : sessionProbe?.status === 'ACCOUNT_MISMATCH' || sessionProbe?.status === 'CONFIG_NOT_FOUND'
                  ? 'lpa-session-bar--mismatch'
                  : 'lpa-session-bar--match')
          }
        >
          {sessionProbeLoading ? (
            <span>Checking IBKR session…</span>
          ) : !sessionProbe || sessionProbe.status === 'PROBE_ERROR' ? (
            <span>Session probe failed — cannot verify IBKR connection. Broker operations are disabled.</span>
          ) : sessionProbe.status === 'NOT_CONNECTED' ? (
            <span>IBKR session not reachable. Start TWS/Gateway for this portfolio, then reload. Broker operations are disabled.</span>
          ) : sessionProbe.status === 'CONFIG_NOT_FOUND' ? (
            <span>No portfolio config found — cannot resolve IBKR connection. Broker operations are disabled.</span>
          ) : sessionProbe.status === 'ACCOUNT_MISMATCH' ? (
            <span>
              Session mismatch — connected session exposes{' '}
              <strong>{(sessionProbe.detected_accounts || []).join(', ') || '(none)'}</strong>
              {selectedPortfolio?.ibkr_account_id ? <>, expected <strong>{selectedPortfolio.ibkr_account_id}</strong></> : null}.
              {' '}Start the matching TWS/Gateway or select the matching portfolio.
            </span>
          ) : sessionProbe.status === 'MULTIPLE_ACCOUNTS' ? (
            <span>
              Session OK — <strong>{selectedPortfolio?.ibkr_account_id}</strong> present
              (session also exposes: {(sessionProbe.detected_accounts || []).filter((a) => a !== selectedPortfolio?.ibkr_account_id).join(', ')}).
            </span>
          ) : (
            <span>
              Session OK — <strong>{(sessionProbe.detected_accounts || []).join(', ') || selectedPortfolio?.ibkr_account_id}</strong> connected.
            </span>
          )}
        </div>
      )}

      {/* Phase 3A risk limits panel — collapsed by default, expands on click */}
      {selectedPortfolio && (
        <div className="lpa-risk-panel">
          <button
            type="button"
            className="lpa-risk-panel__toggle"
            onClick={() => setShowRiskPanel((v) => !v)}
            aria-expanded={showRiskPanel}
          >
            {selectedPortfolio.ibkr_account_mode === 'REAL' ? (
              <span className="lpa-risk-panel__real-chip">REAL MONEY</span>
            ) : null}
            Risk Limits
            <span className="lpa-risk-panel__caret">{showRiskPanel ? '▲' : '▼'}</span>
          </button>
          {showRiskPanel && (
            <div className="lpa-risk-panel__body">
              <dl className="lpa-risk-panel__dl">
                <dt>Max Positions</dt>
                <dd>{selectedPortfolio.max_positions ?? '—'}</dd>
                <dt>Max Position %</dt>
                <dd>{selectedPortfolio.max_position_pct != null ? `${(selectedPortfolio.max_position_pct * 100).toFixed(1)}%` : '—'}</dd>
                <dt>Cash Buffer %</dt>
                <dd>{selectedPortfolio.cash_buffer_pct != null ? `${(selectedPortfolio.cash_buffer_pct * 100).toFixed(1)}%` : '—'}</dd>
                <dt>Drawdown Stop</dt>
                <dd className={selectedPortfolio.drawdown_stop_pct != null ? 'lpa-risk-panel__val--warn' : ''}>
                  {selectedPortfolio.drawdown_stop_pct != null ? `${(selectedPortfolio.drawdown_stop_pct * 100).toFixed(1)}% max drawdown` : '—'}
                </dd>
                <dt>Max Slippage %</dt>
                <dd className={selectedPortfolio.max_slippage_pct != null ? 'lpa-risk-panel__val--warn' : ''}>
                  {selectedPortfolio.max_slippage_pct != null ? `${(selectedPortfolio.max_slippage_pct * 100).toFixed(2)}%` : '—'}
                </dd>
                {selectedPortfolio.ibkr_account_mode === 'REAL' && (
                  <>
                    <dt>Short Selling</dt>
                    <dd className={!selectedPortfolio.allow_short_selling ? 'lpa-risk-panel__val--blocked' : 'lpa-risk-panel__val--ok'}>
                      {selectedPortfolio.allow_short_selling ? 'Allowed' : 'Blocked'}
                    </dd>
                    <dt>Trailing Stop</dt>
                    <dd className={!selectedPortfolio.trail_enabled ? 'lpa-risk-panel__val--warn' : 'lpa-risk-panel__val--ok'}>
                      {selectedPortfolio.trail_enabled ? 'Enabled' : 'Pending certification'}
                    </dd>
                  </>
                )}
              </dl>
            </div>
          )}
        </div>
      )}


      <div ref={feedbackRef} className="lpa-feedback-region">
        {error ? <div className="lpa-error" role="alert">{error}</div> : null}
        {notice ? <div className="lpa-notice-banner" role="status">{notice}</div> : null}
      </div>
      {!selectedPortfolioId && !portfolioLoading ? (
        <div className="lpa-notice-inline" role="status">No portfolio selected — select a portfolio above to load live activity.</div>
      ) : null}
      {loading ? <div>Loading live portfolio activity...</div> : null}

      {!loading && (
        <>
          {reconciliationRequired ? (
            <div className="lpa-warning-inline">
              Broker reconciliation required before new submissions. Recent broker fills are present but not fully linked locally
              ({unmappedExecutionCount}{unmappedSymbols.length ? ` across ${unmappedSymbols.join(', ')}` : ''}).
              Please use Refresh From IB and reconcile before trading.
            </div>
          ) : null}
          {!reconciliationRequired && unmappedOnFlatSymbols > 0 ? (
            <div className="lpa-notice-inline" role="status">
              IB shows no open position for symbols that still have unmapped broker fills in the lookback window (
              {unmappedOnFlatSymbols} execution{unmappedOnFlatSymbols === 1 ? '' : 's'}). Those are treated as historical
              lineage gaps and do not block trading. If you add a new position in IB without MIP, unmapped fills on an
              open symbol will block again until lineage is aligned.
            </div>
          ) : null}
          {missingCloseFillCount > 0 ? (
            <div className="lpa-warning-inline" role="alert">
              {missingCloseFillCount} closed position{missingCloseFillCount === 1 ? '' : 's'} in IB have no SELL fill
              recorded in MIP for this lookback
              {missingCloseFillSymbols.length ? ` (${missingCloseFillSymbols.join(', ')})` : ''}.
              Use <b>Refresh From IB</b> first. Rows marked <b>Missing fill</b> in Trades are placeholders — verify
              prices and P&amp;L in IBKR Activity.
            </div>
          ) : null}
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
            <div className={`lpa-kpi lpa-kpi--${stateClass(driftStateDisplay)}`}>
              <span>Drift</span><b>{driftStateDisplay || '—'}</b>
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
              Decisions not yet broker-opened. Structural <strong>entry</strong>: <strong>Run Agentic Review</strong>, then <strong>Check price for Submit</strong>, then Submit.
              Structural <strong>exit</strong>: legacy SSE replay. Other intents: committee revalidation stream, then Submit.
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
                  {pending.map((d) => {
                    const isStructuralC20Row = Boolean(d.structural)
                    const isStructuralEntryRow =
                      isStructuralC20Row && String(d.action_intent || '').toUpperCase() !== 'EXIT'
                    const isStructuralExitRow =
                      isStructuralC20Row && String(d.action_intent || '').toUpperCase() === 'EXIT'
                    return (
                    <Fragment key={d.action_id}>
                    {(() => {
                      const statusUpper = String(d.status || '').toUpperCase()
                      const isStructuralC20 = isStructuralC20Row
                      const isStructuralEntry = isStructuralEntryRow
                      const isStructuralExit = isStructuralC20 && !isStructuralEntry
                      const c20State = c20OrchestrateByAction[d.action_id] || {}
                      const c20Expanded = Boolean(c20ExpandedByAction[d.action_id])
                      const c20AgenticVisible = Boolean(c20AgenticVisibleByAction[d.action_id])
                      const c20BaselineUserExpanded = Boolean(c20BaselineExpandedByAction[d.action_id])
                      const c20BaselineExpanded = c20BaselineUserExpanded
                      const canSubmit = statusUpper === 'REVALIDATED_PASS' && Boolean(d.submission_allowed)
                      // Phase 5C: structural ENTRY actions whose underlying
                      // proposal is from a superseded board run cannot ever be
                      // submitted. Re-running Intelligence Review on them just
                      // refreshes the evidence dossier and the Agentic
                      // Committee but cannot unstick the supersedure gate — so
                      // disable revalidation entirely. Only "Reject stale"
                      // remains meaningful for these rows.
                      const proposalIsStale = (
                        isStructuralEntry
                        && d.proposal_freshness
                        && String(d.proposal_freshness).toUpperCase() !== 'CURRENT'
                      )
                      const canRunRevalidateForSubmit = !proposalIsStale && [
                        'READY_FOR_APPROVAL_FLOW',
                        'PM_ACCEPTED',
                        'COMPLIANCE_APPROVED',
                        'INTENT_SUBMITTED',
                        'INTENT_APPROVED',
                        'REVALIDATED_FAIL',
                      ].includes(statusUpper)
                      const agenticGate = d.agentic_authority_gate || {}
                      const agenticGateOk = Boolean(agenticGate.gate_ok)
                      const authorityStatusForRow = String(
                        agenticAuthorityByAction[d.action_id]?.authority?.AUTHORITY_STATUS
                        || agenticGate.authority_status
                        || '',
                      ).toUpperCase()
                      const operatorCommittedPositive = (
                        String(
                          agenticAuthorityByAction[d.action_id]?.authority?.AUTHORITY_MODE
                          || agenticGate.authority_mode
                          || '',
                        ).toUpperCase() === 'OPERATOR_COMMITTED'
                        && !(agenticAuthorityByAction[d.action_id]?.authority?.IS_STALE ?? agenticGate.is_stale)
                        && LPA_POSITIVE_AUTHORITY.has(authorityStatusForRow)
                      )
                      const canRunOpeningRecheck = (
                        !proposalIsStale
                        && statusUpper === 'OPEN_BLOCKED'
                        && (operatorCommittedPositive || agenticGateOk)
                      )
                      const canRunCommittee = !proposalIsStale && [
                        'RESEARCH_IMPORTED',
                        'PROPOSED',
                        'PENDING_OPEN_VALIDATION',
                        'OPEN_BLOCKED',
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
                      const authorityModeForRow = String(
                        agenticAuthorityByAction[d.action_id]?.authority?.AUTHORITY_MODE
                        || agenticGate.authority_mode
                        || '',
                      ).toUpperCase()
                      const operatorCommittedForRow = (
                        authorityModeForRow === 'OPERATOR_COMMITTED'
                        && !(agenticAuthorityByAction[d.action_id]?.authority?.IS_STALE ?? agenticGate.is_stale)
                      )
                      return (
                    <Fragment>
                    <tr className={isStaleRevalidationState(d) ? 'lpa-row-stale' : ''}>
                      <td>
                        <div><b>{formatSymbolLabel(d.symbol, d.market_type)}</b> ({d.side})</div>
                        <div>Intent: {d.action_intent || 'ENTRY'}{d.exit_type ? ` (${d.exit_type})` : ''}</div>
                        {d.live_intent_kind ? (
                          <div>
                            <span className="lpa-badge lpa-badge--intent-kind">{d.live_intent_kind}</span>
                          </div>
                        ) : null}
                        {d.structural ? (
                          <div className="lpa-structural-badge">
                            <span className="lpa-badge lpa-badge--structural">{d.structural.setup_family}</span>
                            <span className={`lpa-badge lpa-badge--${d.structural.direction === 'SHORT' ? 'short' : 'long'}`}>{d.structural.direction}</span>
                            {d.structural.trail_style ? <span className="lpa-badge lpa-badge--trail">{d.structural.trail_style}</span> : null}
                          </div>
                        ) : null}
                        {d.structural?.entry_zone_low != null ? (
                          <div className="lpa-subtle">Entry zone: {fmtNum(d.structural.entry_zone_low, 2)} – {fmtNum(d.structural.entry_zone_high, 2)} | Invalidation: {fmtNum(d.structural.invalidation_level, 2)}</div>
                        ) : null}
                        {d.structural?.setup_narrative ? (
                          <div className="lpa-subtle lpa-narrative">{d.structural.setup_narrative}</div>
                        ) : null}
                        {isStructuralEntry && d.proposal_id != null ? (
                          <div className="lpa-phase4-inline">
                            <Link
                              to={`/symbol-tracker?symbol=${encodeURIComponent(String(d.symbol || '').trim())}&proposal_id=${encodeURIComponent(String(d.proposal_id))}`}
                            >
                              Symbol Tracker — Phase 4 overlays & board read
                            </Link>
                            {d.structural?.board_dossier_id != null ? (
                              <span className="lpa-phase4-inline-meta">
                                Dossier #{d.structural.board_dossier_id}
                              </span>
                            ) : null}
                          </div>
                        ) : null}
                        <div>Action: {d.action_id}</div>
                        <div>Created: {fmtTs(d.timestamps?.created_at)} ({fmtAge(d.timestamps?.created_at)} ago)</div>
                        {isNewDecision(d.timestamps?.created_at) ? <div className="lpa-subtle">NEW</div> : null}
                        {isStructuralEntry &&
                        Array.isArray(d.superseded_pending) &&
                        d.superseded_pending.length > 0 ? (
                          <div className="lpa-subtle">
                            Superseded pending (read-only):{' '}
                            {d.superseded_pending
                              .map((s) => `${s.proposal_id ?? '—'} / ${s.action_id ?? '—'}`)
                              .join('; ')}
                          </div>
                        ) : null}
                        {isStructuralEntry &&
                        (c20State.lastResult ||
                          c20State.error ||
                          c20State.loading ||
                          shadowBoardByAction[d.action_id]) ? (
                          <>
                            {(() => {
                              // Shadow Chair Verdict headline render gate.
                              //
                              // The headline is anchored to shadow board state
                              // (bounded poll output) — NOT to c20State — so it
                              // survives any downstream throw in
                              // advanceLiveActionAfterCommitteeApply (e.g.
                              // /revalidate failing IBKR_BAR_STALE_ENTRY_BLOCKED
                              // or OUTSIDE_EXTENDED_TRADING_WINDOW).
                              //
                              // c20State.lastResult is used ONLY to compute the
                              // optional Δ disagrees-with-baseline chip. If it
                              // is missing, the headline still shows stance /
                              // confidence / placeholder without the chip.
                              const shadow = shadowBoardByAction[d.action_id] || null
                              // Phase 5C: when the underlying proposal is from a
                              // superseded board run, no Agentic Committee verdict
                              // can ever produce a Submit-able state. Render a
                              // prominent banner that replaces the agentic panel
                              // entirely, so the operator sees the dead-end before
                              // running any more validations.
                              if (d.proposal_freshness && String(d.proposal_freshness).toUpperCase() !== 'CURRENT') {
                                const rejectBusy = busy === `reject:${d.action_id}`
                                return (
                                  <div className="lpa-c2-stale-proposal-banner" role="alert">
                                    <div className="lpa-c2-stale-proposal-head">
                                      <span className="lpa-c2-stale-proposal-chip">Stale proposal</span>
                                      <strong>This action is dead. Submit will never enable.</strong>
                                    </div>
                                    <p>
                                      The underlying proposal is from a board run that has been
                                      superseded by a newer one ({String(d.proposal_freshness).replace(/_/g, ' ').toLowerCase()}).
                                      Re-validating it does nothing useful — the agentic verdict can
                                      only act on the current board run's proposals. The only way
                                      forward is to reject this row and trade a proposal from the
                                      latest board run instead.
                                    </p>
                                    <div className="lpa-c2-stale-proposal-actions">
                                      <button
                                        type="button"
                                        className="lpa-btn lpa-btn-danger"
                                        disabled={rejectBusy}
                                        onClick={() => rejectStale(d.action_id)}
                                        title="Mark this superseded action as rejected and clear it from the active LPA list."
                                      >
                                        {rejectBusy ? 'Rejecting…' : `Reject this ${d.symbol || 'action'}`}
                                      </button>
                                      <span className="lpa-subtle">
                                        Then look in the LPA list for a proposal from the newest
                                        board run (no red banner) and validate that one.
                                      </span>
                                    </div>
                                  </div>
                                )
                              }
                              if (!shadow) return null
                              const baselineStance = String(c20State.lastResult?.stance || '').toUpperCase()
                              const shadowStanceRaw = shadow.stance ? String(shadow.stance).toUpperCase() : ''
                              const status = String(shadow.status || 'RUNNING').toUpperCase()
                              const showStance = Boolean(shadowStanceRaw) && status !== 'FAILED'
                              const showDisagree =
                                baselineStance && shadowStanceRaw && baselineStance !== shadowStanceRaw
                              const placeholder =
                                status === 'TIMEOUT'
                                  ? 'Agentic Committee still running — check exhibits in a few seconds'
                                  : status === 'FAILED'
                                    ? 'Agentic Committee unavailable — Submit blocked until review succeeds'
                                    : status === 'UNAVAILABLE'
                                      ? 'Agentic Committee starting…'
                                      : shadow.polling || status === 'RUNNING'
                                        ? 'Agentic Committee running…'
                                        : 'Agentic Committee not yet available — Submit blocked until review completes'

                              // Stage 4c — authority chip + Apply button.
                              const authState = agenticAuthorityByAction[d.action_id] || null
                              const authority = authState?.authority || null
                              const authorityMode = String(authority?.AUTHORITY_MODE || '').toUpperCase()
                              const authorityStatus = String(authority?.AUTHORITY_STATUS || '').toUpperCase()
                              const authorityIsStale = Boolean(authority?.IS_STALE)
                              const authorityConfidence = authority?.AUTHORITY_CONFIDENCE
                              const shadowIsTerminal = isShadowStatusTerminal(status)
                              const POSITIVE = new Set(['AGENTIC_APPROVE', 'AGENTIC_APPROVE_REDUCED'])
                              const BLOCK_LIKE = new Set([
                                'AGENTIC_WAIT_RECLAIM',
                                'AGENTIC_DEFER',
                                'AGENTIC_REJECT',
                                'AGENTIC_DEGRADED_NO_AUTHORITY',
                                'AGENTIC_FAILED_NO_AUTHORITY',
                              ])
                              const cssToneByStatus = {
                                AGENTIC_APPROVE:                 'lpa-authority-approve',
                                AGENTIC_APPROVE_REDUCED:         'lpa-authority-approve-reduced',
                                AGENTIC_WAIT_RECLAIM:            'lpa-authority-block',
                                AGENTIC_DEFER:                   'lpa-authority-block',
                                AGENTIC_REJECT:                  'lpa-authority-reject',
                                AGENTIC_DEGRADED_NO_AUTHORITY:   'lpa-authority-degraded',
                                AGENTIC_FAILED_NO_AUTHORITY:     'lpa-authority-degraded',
                              }
                              const labelByStatus = {
                                AGENTIC_APPROVE:                 'Approved',
                                AGENTIC_APPROVE_REDUCED:         'Approve (reduced size)',
                                AGENTIC_WAIT_RECLAIM:            'Wait / Reclaim',
                                AGENTIC_DEFER:                   'Defer',
                                AGENTIC_REJECT:                  'Reject',
                                AGENTIC_DEGRADED_NO_AUTHORITY:   'Degraded — no authority',
                                AGENTIC_FAILED_NO_AUTHORITY:     'Not available',
                              }
                              const authorityChipLabel = labelByStatus[authorityStatus] || '—'
                              const authorityChipTone =
                                cssToneByStatus[authorityStatus] || 'lpa-authority-degraded'
                              const isOperatorCommitted =
                                authorityMode === 'OPERATOR_COMMITTED' && !authorityIsStale
                              const isOperatorStale =
                                authorityMode === 'OPERATOR_COMMITTED' && authorityIsStale
                              const isPreviewAutoAudit =
                                authorityMode === 'AUTO_AUDIT' && !authorityIsStale
                              const showApplyButton =
                                shadowIsTerminal &&
                                shadow.sessionId &&
                                shadow.hearingId &&
                                !isOperatorCommitted &&
                                !BLOCK_LIKE.has(authorityStatus) &&
                                POSITIVE.has(authorityStatus) &&
                                isPreviewAutoAudit
                              const showRecommitButton =
                                shadowIsTerminal &&
                                shadow.sessionId &&
                                shadow.hearingId &&
                                isOperatorStale
                              const commitBusy = Boolean(agenticCommitBusyByAction[d.action_id])
                              const gateEval = authState?.gate_evaluation || null
                              const gateEnabled = Boolean(gateEval?.gate_enabled)
                              const gateOk = gateEval ? Boolean(gateEval.gate_ok) : null
                              const gateTooltip = gateEval?.tooltip || null
                              const wouldGateBlockSubmit = BLOCK_LIKE.has(authorityStatus)
                              const gateActuallyBlocks = gateEnabled && gateOk === false
                              const outcomeMsg = lpaOutcomeMessage(authorityStatus)
                                || (shadowIsTerminal && shadowStanceRaw === 'DENY' && (authorityStatus === 'AGENTIC_FAILED_NO_AUTHORITY' || authorityStatus === 'AGENTIC_DEGRADED_NO_AUTHORITY')
                                  ? {
                                      title: 'Rejected',
                                      body: 'The committee denied this entry (one specialist failed, but the chair ruling stands). Submit will not enable — use Reject stale to clear the row.',
                                    }
                                  : null)
                              const flowStepId = computeLpaFlowStep({
                                isStructuralEntry: true,
                                statusUpper,
                                canSubmit,
                                shadowIsTerminal,
                                shadowRunning: shadow.polling || status === 'RUNNING',
                                authorityStatus,
                                isOperatorCommitted,
                              })

                              return (
                                <div className="lpa-c2-shadow-headline">
                                  <LpaStepIndicator currentStepId={flowStepId} statusUpper={statusUpper} />
                                  <div className="lpa-c2-shadow-headline-head">
                                    <span className="lpa-c2-shadow-headline-title">Agentic Committee Verdict</span>
                                    <span className="lpa-c2-shadow-headline-agentic">Primary</span>
                                  </div>
                                  {c20State.shadowReused === true ? (
                                    <div className="lpa-c2-reused-badge">
                                      Same market snapshot — prior review reused
                                    </div>
                                  ) : c20State.loading && c20State.shadowReused === false ? (
                                    <div className="lpa-c2-fresh-badge">Fresh review running…</div>
                                  ) : null}
                                  <div className="lpa-c2-shadow-headline-sub lpa-subtle">
                                    {d.required_next_step || 'Run Agentic Review to begin'}
                                  </div>
                                  {showStance ? (
                                    <div className="lpa-c2-shadow-headline-row">
                                      <span className="lpa-c2-shadow-headline-pill lpa-c2-shadow-headline-pill--stance">
                                        {shadowStanceRaw.replace(/_/g, ' ')}
                                      </span>
                                      <span className="lpa-c2-shadow-headline-pill">
                                        conf{' '}
                                        {shadow.confidence != null ? fmtNum(shadow.confidence, 2) : '—'}
                                      </span>
                                      {showDisagree ? (
                                        <span
                                          className="lpa-c2-shadow-headline-disagree"
                                          title={`Agentic stance ${shadowStanceRaw.replace(/_/g, ' ')} differs from historical evidence snapshot ${baselineStance.replace(/_/g, ' ')} — informational only.`}
                                        >
                                          Δ Disagrees with baseline
                                        </span>
                                      ) : null}
                                      {shadow.degraded || status === 'DEGRADED' ? (
                                        <span
                                          className="lpa-c2-shadow-headline-degraded"
                                          title={shadow.degradedReason || 'Agentic review completed in degraded mode.'}
                                        >
                                          Degraded
                                        </span>
                                      ) : null}
                                    </div>
                                  ) : (
                                    <div className="lpa-c2-shadow-headline-placeholder lpa-subtle">
                                      {placeholder}
                                    </div>
                                  )}

                                  {/* Stage 4c — agentic authority chip + actions. */}
                                  {shadowIsTerminal && authority ? (
                                    <div className="lpa-authority-row">
                                      <span
                                        className={`lpa-authority-chip ${authorityChipTone}${
                                          isOperatorStale ? ' lpa-authority-chip--stale' : ''
                                        }`}
                                        title={`Authority reason: ${
                                          authority.AUTHORITY_REASON_CODE || '—'
                                        }`}
                                      >
                                        Agentic: {authorityChipLabel}
                                        {authorityConfidence != null ? (
                                          <span className="lpa-authority-chip-conf">
                                            · conf {fmtNum(authorityConfidence, 2)}
                                          </span>
                                        ) : null}
                                      </span>
                                      {isOperatorCommitted ? (
                                        (() => {
                                          // Phase 5C: distinguish auto-committed
                                          // (system promoted AUTO_AUDIT for clean
                                          // APPROVE / APPROVE_REDUCED) from
                                          // operator-clicked commits so the
                                          // operator can see whether they need to
                                          // intervene.
                                          const committedBy = String(authority.COMMITTED_BY || '')
                                          const isAutoCommit = committedBy.startsWith('system_auto_commit')
                                          return (
                                            <span
                                              className="lpa-authority-badge lpa-authority-badge--committed"
                                              title={
                                                isAutoCommit
                                                  ? `Auto-committed by the system after the Agentic Committee returned a clean APPROVE / APPROVE_REDUCED verdict (${committedBy}) at ${fmtTs(authority.CREATED_AT)}. No operator click required.`
                                                  : `Committed by ${committedBy || '—'} at ${fmtTs(authority.CREATED_AT)}`
                                              }
                                            >
                                              {isAutoCommit ? 'Verdict recorded (auto)' : 'Verdict recorded'}
                                            </span>
                                          )
                                        })()
                                      ) : isPreviewAutoAudit ? (
                                        <span
                                          className="lpa-authority-badge lpa-authority-badge--preview"
                                          title="Auto-commit is off — confirm approval to proceed to price check."
                                        >
                                          Awaiting confirm
                                        </span>
                                      ) : null}
                                      {authorityIsStale ? (
                                        <span
                                          className="lpa-authority-badge lpa-authority-badge--stale"
                                          title={authority.STALE_REASON || 'Authority row is stale relative to the current hearing evidence pack.'}
                                        >
                                          Stale
                                        </span>
                                      ) : null}
                                      {authority.DISAGREES_WITH_BASELINE === true ? (
                                        <span
                                          className="lpa-authority-badge lpa-authority-badge--delta"
                                          title={`Agentic ${authority.SHADOW_STANCE_RAW || '—'} differs from historical evidence snapshot ${authority.DETERMINISTIC_BASELINE_STANCE || '—'}.`}
                                        >
                                          Δ vs baseline
                                        </span>
                                      ) : null}
                                      {gateActuallyBlocks ? (
                                        <span
                                          className="lpa-authority-badge lpa-authority-badge--blocking"
                                          title={
                                            gateTooltip ||
                                            'Agentic authority gate is blocking Submit for this action.'
                                          }
                                        >
                                          Blocks submit
                                        </span>
                                      ) : wouldGateBlockSubmit && !gateEnabled ? (
                                        <span
                                          className="lpa-authority-badge lpa-authority-badge--future-gate"
                                          title="This authority status would block Submit when the Agentic Committee gate is enabled. The gate is currently disabled in APP_CONFIG."
                                        >
                                          Would block submit
                                        </span>
                                      ) : null}
                                      {showApplyButton ? (
                                        <button
                                          type="button"
                                          className="lpa-authority-btn"
                                          disabled={commitBusy}
                                          onClick={() => commitAgenticAuthority(d.action_id)}
                                          title="Auto-commit is disabled — confirm the committee approval to proceed to price check."
                                        >
                                          {commitBusy ? 'Confirming…' : 'Confirm approval'}
                                        </button>
                                      ) : null}
                                      {showRecommitButton ? (
                                        <button
                                          type="button"
                                          className="lpa-authority-btn lpa-authority-btn--recommit"
                                          disabled={commitBusy}
                                          onClick={() => commitAgenticAuthority(d.action_id)}
                                          title="The committed authority is stale relative to a newer hearing evidence pack. Re-commit to refresh."
                                        >
                                          {commitBusy ? 'Re-committing…' : 'Re-commit (stale)'}
                                        </button>
                                      ) : null}
                                    </div>
                                  ) : null}
                                  {shadowIsTerminal && outcomeMsg ? (
                                    <div className="lpa-outcome-card" role="status">
                                      <strong>{outcomeMsg.title}</strong>
                                      <p className="lpa-subtle">{outcomeMsg.body}</p>
                                    </div>
                                  ) : null}
                                  {statusUpper === 'OPEN_BLOCKED' && isOperatorCommitted && POSITIVE.has(authorityStatus) ? (
                                    <div className="lpa-open-blocked-banner" role="alert">
                                      <strong>Submit blocked — opening guard (market closed or snapshot stale)</strong>
                                      <p className="lpa-subtle">
                                        The Agentic Committee verdict is already recorded (see Verdict recorded below).
                                        You do not need to commit anything else. When the market opens, click{' '}
                                        <strong>Recheck opening guard</strong> in the action column.
                                      </p>
                                    </div>
                                  ) : null}
                                  {isOperatorCommitted && POSITIVE.has(authorityStatus) && statusUpper !== 'REVALIDATED_PASS' && statusUpper !== 'OPEN_BLOCKED' ? (
                                    <div className="lpa-c2-approved-hint lpa-subtle">
                                      Committee approved — click Check price for Submit when ready.
                                    </div>
                                  ) : null}
                                  {shadowIsTerminal && !authority && authState?.loading ? (
                                    <div className="lpa-authority-row lpa-subtle">
                                      Loading agentic authority…
                                    </div>
                                  ) : null}
                                  {authState?.error ? (
                                    <div className="lpa-authority-row lpa-authority-row--err">
                                      Agentic authority: {authState.error}
                                      {authState.lastCommitMessage ? (
                                        <span className="lpa-subtle">
                                          {' '}
                                          — {authState.lastCommitMessage}
                                        </span>
                                      ) : null}
                                    </div>
                                  ) : null}
                                </div>
                              )
                            })()}
                          {/* Phase 5C: skip the entire secondary evidence-dossier
                              collapsible for actions whose proposal is from a
                              superseded board run. The red banner above is the
                              only thing the operator should see for those rows
                              — anything else just invites another wasted click. */}
                          {proposalIsStale ? null : (
                          <div
                            className={`lpa-c2-panel lpa-c2-panel--secondary${
                              c20State.error ? ' lpa-c2-panel--err' : ''
                            }${c20BaselineExpanded ? ' lpa-c2-panel--expanded' : ' lpa-c2-panel--collapsed'}`}
                          >
                            <button
                              type="button"
                              className="lpa-c2-panel-titlebar"
                              aria-expanded={c20BaselineExpanded}
                              onClick={() =>
                                setC20BaselineExpandedByAction((prev) => ({
                                  ...prev,
                                  [d.action_id]: !c20BaselineUserExpanded,
                                }))
                              }
                              title={
                                c20BaselineExpanded
                                  ? 'Collapse evidence dossier (Agentic Committee Verdict above is authoritative)'
                                  : 'Expand evidence dossier (read-only diagnostic; agentic verdict above is authoritative)'
                              }
                            >
                              <span className="lpa-c2-panel-toggle" aria-hidden>
                                {c20BaselineExpanded ? '▼' : '▶'}
                              </span>
                              <span className="lpa-c2-panel-title">Evidence dossier (read-only)</span>
                              {/* Phase 5B detection: the agentic-only orchestrate path returns
                                  stance/confidence/recommendation all empty because the
                                  deterministic chair never runs. The old summary line
                                  "—  ·  conf —  ·  —" looked like a broken deterministic
                                  verdict, which is what users were calling "old committee
                                  UX". Show an evidence-only summary instead, pointing
                                  upward to the Agentic Committee Verdict. */}
                              {!c20BaselineExpanded && c20State.lastResult ? (
                                (!c20State.lastResult.stance && !c20State.lastResult.recommendation) ? (
                                  <span className="lpa-c2-panel-summary lpa-subtle">
                                    evidence refreshed · agentic verdict above
                                  </span>
                                ) : (
                                  <span className="lpa-c2-panel-summary lpa-subtle">
                                    {String(c20State.lastResult.stance ?? '—').replace(/_/g, ' ')} ·
                                    conf{' '}
                                    {c20State.lastResult.confidence != null
                                      ? fmtNum(c20State.lastResult.confidence, 2)
                                      : '—'}{' '}
                                    · {String(c20State.lastResult.recommendation || '—')}
                                    {c20State.lastResult.blocked ? ' (blocked)' : ''}
                                    {c20State.lastResult.idempotent_replay ? ' · replay' : ''}
                                  </span>
                                )
                              ) : null}
                              {!c20BaselineExpanded && !c20State.lastResult && c20State.loading ? (
                                <span className="lpa-c2-panel-summary lpa-subtle">running…</span>
                              ) : null}
                            </button>
                            {c20BaselineExpanded ? (
                              <div className="lpa-c2-panel-body">
                                <div className="lpa-c2-panel-subtitle lpa-subtle">
                                  Evidence dossier (read-only diagnostic) · the Agentic Committee Verdict above is the authoritative source.
                                </div>
                                {c20State.loading && c20State.progressMsg ? (
                                  <div className="lpa-c2-progress-inline">{c20State.progressMsg}</div>
                                ) : null}
                                {c20State.error ? (
                                  <div>{c20State.error}</div>
                                ) : (
                                  <>
                                    {/* Phase 5B: when stance and recommendation are both empty,
                                        no deterministic chair executed for this hearing — these
                                        lines would render as "— / —" and "Verdict: —" which is
                                        exactly what made the panel look like a broken old
                                        committee. Hide them and show an evidence-only status. */}
                                    {(!c20State.lastResult?.stance && !c20State.lastResult?.recommendation) ? (
                                      <div className="lpa-subtle">
                                        Evidence dossier refreshed for the hearing snapshot. The Agentic Committee
                                        above runs against this dossier and produces the only authoritative verdict.
                                      </div>
                                    ) : (
                                      <>
                                        <div>
                                          Stance / confidence: {String(c20State.lastResult?.stance ?? '—')} /{' '}
                                          {c20State.lastResult?.confidence != null
                                            ? fmtNum(c20State.lastResult.confidence, 2)
                                            : '—'}
                                        </div>
                                        <div>
                                          Verdict: {String(c20State.lastResult?.recommendation || '—')}
                                          {c20State.lastResult?.blocked ? ' (blocked)' : ''}
                                          {c20State.lastResult?.idempotent_replay ? ' · replay' : ''}
                                        </div>
                                      </>
                                    )}
                                    {Array.isArray(c20State.lastResult?.reason_codes) &&
                                    c20State.lastResult.reason_codes.length > 0 ? (
                                      <div className="lpa-subtle">
                                        {c20State.lastResult.reason_codes.slice(0, 6).join(', ')}
                                      </div>
                                    ) : null}
                                    {c20State.lastResult?.inline_hearing ? (
                                      <button
                                        type="button"
                                        className="lpa-c2-expand-btn lpa-c2-expand-btn--diagnostic"
                                        onClick={() =>
                                          setC20ExpandedByAction((prev) => ({
                                            ...prev,
                                            [d.action_id]: !prev[d.action_id],
                                          }))
                                        }
                                        title="Open the full evidence dossier — specialist outputs + Agentic Committee boardroom. Read-only diagnostic."
                                      >
                                        {c20Expanded
                                          ? '▲ Hide evidence dossier'
                                          : '▼ Show evidence dossier'}
                                      </button>
                                    ) : null}
                                    {c20State.lastResult?.hearing_id && d.action_id && d.proposal_id != null ? (
                                      <div className="lpa-c2-panel-links">
                                        <Link
                                          to={`/structural-committee/${encodeURIComponent(c20State.lastResult.hearing_id)}?action_id=${encodeURIComponent(
                                            String(d.action_id),
                                          )}&proposal_id=${encodeURIComponent(String(d.proposal_id))}`}
                                          title="Open the historical hearing evidence dossier (read-only)"
                                        >
                                          Open evidence dossier
                                        </Link>
                                      </div>
                                    ) : null}
                                  </>
                                )}
                              </div>
                            ) : null}
                          </div>
                          )}
                          </>
                        ) : null}
                        {/* Phase 5B: LIVE_ACTIONS.COMMITTEE_VERDICT is the last verdict
                            written to this action's row. For actions revalidated under
                            the agentic-only path it carries the Agentic Committee
                            outcome (or remains as the historical verdict until the
                            operator commits). Surface it as "Last verdict on file"
                            with a tooltip so it doesn't read as a fresh deterministic
                            committee output. */}
                        {d.committee_verdict ? (
                          <div className="lpa-subtle" title="Last verdict written to LIVE_ACTIONS for this action. Operator authority comes from the Agentic Committee Verdict, not from this field.">
                            Last verdict on file: {d.committee_verdict}
                          </div>
                        ) : null}
                        {d.structural?.freshness_assessment ? <div className="lpa-subtle">Freshness: {d.structural.freshness_assessment} | Hold: {d.structural.hold_character || '—'}</div> : null}
                      </td>
                      <td>
                        <div>{d.status || '—'}</div>
                        <div>Compliance: {d.compliance_status || '—'}</div>
                        <div>Committee run: {d.committee_run_id || '—'}</div>
                        <div>Committee at: {fmtTs(d.committee_completed_ts)}</div>
                        {d.committee_decision ? (
                          <div className="lpa-committee-decision">
                            <div>
                              <span className={`lpa-badge ${d.committee_decision.should_enter === true ? 'lpa-badge--long' : d.committee_decision.should_enter === false ? 'lpa-badge--short' : 'lpa-badge--structural'}`}>
                                {d.committee_decision.should_enter === true ? 'APPROVED' : d.committee_decision.should_enter === false ? 'DENIED' : 'PENDING'}
                              </span>
                            </div>
                            {d.committee_decision.risk_notes ? <div className="lpa-subtle lpa-narrative">{d.committee_decision.risk_notes}</div> : null}
                            {d.committee_decision.realistic_target_return != null ? <div className="lpa-subtle">Target: {fmtPct(d.committee_decision.realistic_target_return)}</div> : null}
                            {d.committee_decision.stop_loss_pct != null ? <div className="lpa-subtle">Stop: {fmtPct(d.committee_decision.stop_loss_pct)}</div> : null}
                          </div>
                        ) : null}
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
                        {(() => {
                          const shownReasons = displayReasonCodes(d)
                          return shownReasons.length > 0 ? (
                          <ul className="lpa-reason-list">
                            {shownReasons.map((code, idx) => (
                              <li key={`${d.action_id}:reason:${idx}`}>
                                <span className="lpa-reason-human">{explainReasonCode(code)}</span>
                              </li>
                            ))}
                          </ul>
                        ) : (
                          <div>—</div>
                        )
                        })()}
                        {hasPriceGuardFailReason(d.reason_codes) &&
                        d.price_guard != null &&
                        d.price_guard.price_deviation_pct != null ? (
                          <div className="lpa-subtle lpa-price-guard-hint">
                            Price guard: {fmtNum(d.price_guard.price_deviation_pct * 100, 2)}% deviation vs proposed
                            (full pass ≤{fmtNum((d.price_guard.pass_max_pct ?? 0.02) * 100, 0)}%, reduced{' '}
                            {fmtNum((d.price_guard.pass_max_pct ?? 0.02) * 100, 0)}–
                            {fmtNum((d.price_guard.reduced_max_pct ?? 0.04) * 100, 0)}%, fail &gt;
                            {fmtNum((d.price_guard.reduced_max_pct ?? 0.04) * 100, 0)}%).
                            {d.price_guard.revalidation_price != null
                              ? ` Reference close at last revalidation: ${fmtNum(d.price_guard.revalidation_price, 4)}.`
                              : ''}
                          </div>
                        ) : null}
                        <div className="lpa-subtle lpa-next-step">Next: {d.required_next_step || '—'}</div>
                      </td>
                      <td>
                        <div className="lpa-actions">
                        {isStaleRevalidationState(d) ? (
                          <div className="lpa-warning-inline">
                            Safety revalidation expired —{' '}
                            {isStructuralEntry
                              ? 'run Agentic Review'
                              : isStructuralExit
                                ? 'replay execution verdict (SSE)'
                                : isStructuralC20
                                  ? 'sync Intelligence Review'
                                  : 'run committee revalidation'}{' '}
                            before submit.
                          </div>
                        ) : null}
                        {isStructuralEntry && !proposalIsStale ? (
                          <LpaStepIndicator
                            currentStepId={computeLpaFlowStep({
                              isStructuralEntry: true,
                              statusUpper,
                              canSubmit,
                              shadowIsTerminal: isShadowStatusTerminal(
                                String(shadowBoardByAction[d.action_id]?.status || '').toUpperCase(),
                              ),
                              shadowRunning: (
                                shadowBoardByAction[d.action_id]?.polling
                                || String(shadowBoardByAction[d.action_id]?.status || '').toUpperCase() === 'RUNNING'
                              ),
                              authorityStatus: authorityStatusForRow,
                              isOperatorCommitted: operatorCommittedForRow,
                            })}
                            statusUpper={statusUpper}
                          />
                        ) : null}
                        {!canSubmit && Array.isArray(d.submission_gate_hints) && d.submission_gate_hints.length > 0 ? (
                          <ul className="lpa-reason-list lpa-subtle">
                            {d.submission_gate_hints.map((hint, hi) => (
                              <li key={`${d.action_id}:hint:${hi}`}>{hint}</li>
                            ))}
                          </ul>
                        ) : null}
                        {!canSubmit && statusUpper !== 'REVALIDATED_PASS' && !isStructuralEntry ? (
                          <div className="lpa-subtle">
                            Submit to IBKR is enabled only when status is REVALIDATED_PASS (
                            {isStructuralExit
                              ? 'replay execution verdict if needed'
                              : isStructuralC20
                                ? 'sync Intelligence Review if needed'
                                : 'run committee revalidation if needed'}
                            ).
                          </div>
                        ) : null}
                        {!canSubmit && isStructuralEntry && !proposalIsStale ? (
                          <div className="lpa-subtle">{d.required_next_step || 'Follow the steps above.'}</div>
                        ) : null}
                        <button
                          className="lpa-btn"
                          disabled={busy === `submit:${d.action_id}` || !canSubmit || isStaleRevalidationState(d) || brokerExecutionBlocked}
                          onClick={() => submitOnly(d.action_id)}
                          title={brokerExecutionBlocked ? (executionDisabled ? 'Execution disabled for this portfolio' : 'Session mismatch — start the correct TWS/Gateway') : undefined}
                        >
                          {busy === `submit:${d.action_id}` ? 'Submitting...' : 'Submit'}
                        </button>
                        {selectedPortfolio?.ibkr_account_mode === 'REAL' ? (
                          <span className="lpa-real-money-chip" title="Orders submitted here will execute against a real-money IBKR account.">
                            REAL MONEY
                          </span>
                        ) : null}
                        {brokerExecutionBlocked ? (
                          <div className="lpa-subtle" style={{ color: '#e65100', fontWeight: 600 }}>
                            {executionDisabled ? 'Execution disabled — read-only portfolio' : 'Session mismatch — start correct TWS/Gateway'}
                          </div>
                        ) : null}
                        {isStructuralEntry ? (
                          proposalIsStale ? null : (
                          <>
                            <button
                              type="button"
                              className="lpa-btn"
                              disabled={busy === `c2orch:${d.action_id}` || !canRunCommittee}
                              onClick={() => runCommittee2Orchestrate(d.action_id)}
                            >
                              {busy === `c2orch:${d.action_id}`
                                ? 'Running…'
                                : c20State.lastResult
                                  ? 'Re-run Agentic Review'
                                  : 'Run Agentic Review'}
                            </button>
                            {(c20State.shadowReused === true
                              || authorityStatusForRow === 'AGENTIC_WAIT_RECLAIM') ? (
                              <button
                                type="button"
                                className="lpa-btn lpa-btn-secondary"
                                disabled={busy === `c2orch:${d.action_id}` || !canRunCommittee}
                                onClick={() => runCommittee2Orchestrate(d.action_id, { forceFreshShadow: true })}
                                title="Force a new Agentic Committee run even when the market snapshot is unchanged."
                              >
                                {busy === `c2orch:${d.action_id}` ? 'Running…' : 'Force fresh review'}
                              </button>
                            ) : null}
                            {canRunOpeningRecheck ? (
                              <button
                                type="button"
                                className="lpa-btn lpa-btn-secondary"
                                disabled={busy === `opening:${d.action_id}`}
                                onClick={() => runOpeningValidation(d.action_id)}
                                title="Re-run the opening sanity gate (market hours, snapshot freshness). Committee verdict is already done."
                              >
                                {busy === `opening:${d.action_id}` ? 'Rechecking…' : 'Recheck opening guard'}
                              </button>
                            ) : null}
                            {canRunRevalidateForSubmit ? (
                              <button
                                type="button"
                                className="lpa-btn lpa-btn-secondary"
                                disabled={busy === `revalidate:${d.action_id}`}
                                onClick={() => runRevalidateForSubmit(d.action_id)}
                                title="Price-check against latest market bar and enable Submit when gates pass."
                              >
                                {busy === `revalidate:${d.action_id}` ? 'Checking…' : 'Check price for Submit'}
                              </button>
                            ) : null}
                          </>
                          )
                        ) : (
                          <button
                            type="button"
                            className="lpa-btn lpa-btn-secondary"
                            disabled={
                              busy === `committee:${d.action_id}` || activeStreamActionId === d.action_id || !canRunCommittee
                            }
                            onClick={() => {
                              openCommitteeStream(d.action_id, {
                                structural: isStructuralC20,
                                executionOnlyExit: isStructuralExit,
                              })
                            }}
                          >
                            {busy === `committee:${d.action_id}` || activeStreamActionId === d.action_id
                              ? isStructuralC20
                                ? isStructuralExit
                                  ? 'Replaying…'
                                  : 'Syncing…'
                                : 'Running...'
                              : isStructuralC20
                                ? isStructuralExit
                                  ? 'Replay execution verdict'
                                  : 'Sync Intelligence Review'
                                : 'Committee revalidation'}
                          </button>
                        )}
                        {/* Phase 5C: for stale-proposal structural ENTRY rows the
                            Reject button now lives inside the red banner where it
                            is closer to the explanation; suppress the duplicate
                            sidebar button so the action toolbar isn't redundant. */}
                        {isStructuralEntry && proposalIsStale ? null : (
                          <button
                            className="lpa-btn lpa-btn-secondary"
                            disabled={busy === `reject:${d.action_id}`}
                            onClick={() => rejectStale(d.action_id)}
                          >
                            {busy === `reject:${d.action_id}` ? 'Rejecting...' : 'Reject stale'}
                          </button>
                        )}
                        {readyPulseActionId === d.action_id ? (
                          <div className="lpa-ready-chip">Ready to submit</div>
                        ) : null}
                        {!canSubmit ? (
                          <div className="lpa-subtle">
                            {d.execution_hard_blocked
                              ? 'Submit blocked by risk limits shown in reason codes. Adjust sizing/config or rerun committee.'
                              : isStructuralEntry && proposalIsStale
                                ? 'Submit is permanently blocked for this row — the proposal is from a superseded board run. Use Reject this action above.'
                                : isStructuralEntry
                                  ? d.required_next_step || 'Follow Review → Outcome → Price check → Submit.'
                                  : isStructuralExit
                                    ? 'Replay execution verdict (SSE) materializes the execution-only structural exit check. Submit enables when REVALIDATED_PASS.'
                                    : isStructuralC20
                                      ? 'Sync Intelligence Review after Hearing Room commit. If the verdict allows execution, Submit will be enabled.'
                                      : 'Run committee revalidation. If committee says go, Submit will be enabled.'}
                          </div>
                        ) : null}
                        {!canRunCommittee && statusUpper === 'OPEN_BLOCKED' && !operatorCommittedPositive ? (
                          <div className="lpa-subtle">
                            Blocked by opening guard. Run Agentic Review when ready, or Reject stale to clear.
                          </div>
                        ) : null}
                        </div>
                      </td>
                    </tr>
                    {isStructuralEntry &&
                    (c20AgenticVisible || c20Expanded) &&
                    !proposalIsStale &&
                    (c20State.loading || c20State.lastResult?.inline_hearing) ? (
                      // Note: deliberately do NOT gate on !c20State.error.
                      // If a downstream chain step (e.g. /revalidate failing
                      // IBKR_BAR_STALE_ENTRY_BLOCKED or
                      // OUTSIDE_EXTENDED_TRADING_WINDOW) throws, the catch
                      // handler sets c20State.error — but the agentic
                      // shadow board is already running in the background
                      // and the exhibits panel must stay mounted to show
                      // live specialist/conflict/chair bubbles as they
                      // land. Unmounting on error was the cause of the
                      // "agentic board vanished after ~10s" symptom.
                      //
                      // Phase 5C: also gated on !proposalIsStale so the big
                      // exhibits panel never renders for actions whose
                      // proposal is from a superseded board run — those
                      // rows show ONLY the red "stale proposal" banner so
                      // the operator does not get stuck running validations
                      // in circles.
                      <tr className="lpa-c2-expand-row">
                        <td colSpan={5}>
                          <LpaCommittee2Exhibits
                            inline={c20State.lastResult?.inline_hearing || null}
                            loading={Boolean(c20State.loading)}
                            progressMsg={c20State.progressMsg || null}
                            shadowReused={c20State.shadowReused === true}
                            showEvidenceColumn={c20Expanded}
                            onForceFreshReview={() => runCommittee2Orchestrate(d.action_id, { forceFreshShadow: true })}
                            hearingHref={
                              c20State.lastResult?.hearing_id && d.proposal_id != null
                                ? `/structural-committee/${encodeURIComponent(c20State.lastResult.hearing_id)}?action_id=${encodeURIComponent(String(d.action_id))}&proposal_id=${encodeURIComponent(String(d.proposal_id))}`
                                : null
                            }
                            onShadowSessionLoaded={(normalized) =>
                              handleExhibitsShadowLoaded(d.action_id, normalized)
                            }
                          />
                        </td>
                      </tr>
                    ) : null}
                    </Fragment>
                      )
                    })()}
                    {streamActionId === d.action_id &&
                    (!isStructuralEntryRow || activeStreamActionId === d.action_id) ? (
                      <tr>
                        <td colSpan={5}>
                          <div className="lpa-stream">
                            <div>
                              <b>
                                {d.structural
                                  ? isStructuralExitRow
                                    ? 'Structural execution replay'
                                    : 'Intelligence Review sync'
                                  : 'Live committee stream'}
                              </b>{' '}
                              for{' '}
                              {streamActionId} ({streamStatus || 'Idle'})
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
                  )
                  })}
                </tbody>
              </table>
            </div>
          </section>

          <section className="lpa-section">
            <h3>Orders (Broker Lifecycle)</h3>
            <div className="lpa-subtle">
              Row status reflects the latest IB open-order snapshot: if your MIP row is still PreSubmitted/PendingSubmit/etc. but that broker order id is not in IB&apos;s open orders, the UI shows NOT_ACTIVE_AT_BROKER (see Archived). Refresh From IB first.
            </div>
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
              <div className="lpa-control">
                <span>Orders View</span>
                <div className="lpa-toggle">
                  <button
                    className={`lpa-toggle-btn ${ordersView === 'active' ? 'is-active' : ''}`}
                    onClick={() => setOrdersView('active')}
                    type="button"
                  >
                    Active ({activeOrdersCount})
                  </button>
                  <button
                    className={`lpa-toggle-btn ${ordersView === 'archived' ? 'is-active' : ''}`}
                    onClick={() => setOrdersView('archived')}
                    type="button"
                  >
                    Archived ({archivedOrdersCount})
                  </button>
                  <button
                    className={`lpa-toggle-btn ${ordersView === 'all' ? 'is-active' : ''}`}
                    onClick={() => setOrdersView('all')}
                    type="button"
                  >
                    All ({orders.length})
                  </button>
                </div>
              </div>
            </div>
            <div className="lpa-table-wrap">
              <table className="lpa-table lpa-table--orders">
                <thead>
                  <tr>
                    <th>Order</th>
                    <th>Status</th>
                    <th>Qty</th>
                    <th>Price</th>
                    <th>Protection</th>
                    <th>Timestamps</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {displayedOrders.length === 0 && <tr><td colSpan={7}>No orders in this view.</td></tr>}
                  {displayedOrders.map((o) => (
                    <tr key={o.ORDER_ID}>
                      <td>
                        <div><b>{formatSymbolLabel(o.SYMBOL, o.MARKET_TYPE || o.ASSET_CLASS)}</b> ({o.SIDE})</div>
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
                        {o.ORDER_ROLE ? (
                          <div>
                            <span className={`lpa-badge lpa-badge--${o.ORDER_ROLE === 'ENTRY' ? 'entry' : o.ORDER_ROLE === 'TRAILING_STOP' ? 'trail' : 'protect'}`}>{o.ORDER_ROLE}</span>
                            {o.PROTECTION_TYPE ? <span className="lpa-subtle"> ({o.PROTECTION_TYPE})</span> : null}
                          </div>
                        ) : (
                          <div><b>{o.PROTECTION?.state || 'NONE'}</b></div>
                        )}
                        {o.TRAIL_ACTIVATED ? (
                          <div className="lpa-trail-active">Trail Active{o.BROKER_TRAIL_STATE ? ` (${o.BROKER_TRAIL_STATE})` : ''}</div>
                        ) : o.TRAIL_STYLE ? (
                          <div className="lpa-subtle">Trail pending ({o.TRAIL_STYLE})</div>
                        ) : null}
                        {o.STOP_PRICE != null ? <div>Stop: {fmtNum(o.STOP_PRICE, 2)}</div> : null}
                        {o.TRAIL_AMOUNT != null ? <div>Trail amt: {fmtNum(o.TRAIL_AMOUNT, 2)}</div> : null}
                        {o.TRAIL_PERCENT != null ? <div>Trail %: {fmtNum(o.TRAIL_PERCENT, 1)}%</div> : null}
                        {o.OCA_GROUP ? <div className="lpa-subtle">OCA: {o.OCA_GROUP}</div> : null}
                        {!o.ORDER_ROLE ? (
                          <>
                            <div>Parent: {o.PROTECTION?.parent?.status || '—'}</div>
                            <div>TP: {o.PROTECTION?.take_profit?.status || '—'}</div>
                            <div>SL: {o.PROTECTION?.stop_loss?.status || '—'}</div>
                          </>
                        ) : null}
                      </td>
                      <td>
                        <div>Submitted: {fmtTs(o.SUBMITTED_AT)}</div>
                        <div>Updated: {fmtTs(o.LAST_UPDATED_AT || o.CREATED_AT)}</div>
                      </td>
                      <td>
                        {(() => {
                          const statusUpper = String(o.STATUS || '').toUpperCase()
                          const canCancel =
                            statusUpper !== 'NOT_ACTIVE_AT_BROKER' &&
                            ['SUBMITTED', 'ACKNOWLEDGED', 'PENDINGSUBMIT', 'PRESUBMITTED', 'PARTIAL_FILL', 'PARTIALLYFILLED'].includes(statusUpper)
                          return (
                            <button
                              className="lpa-btn lpa-btn-secondary lpa-btn-compact"
                              disabled={!canCancel || busy === `cancelOrder:${o.ORDER_ID}`}
                              onClick={() => cancelSingleOrder(o)}
                            >
                              {busy === `cancelOrder:${o.ORDER_ID}` ? 'Canceling...' : 'Cancel'}
                            </button>
                          )
                        })()}
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
                  <table className="lpa-table lpa-table--positions-trades">
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th>Qty</th>
                        <th>Avg Cost</th>
                        <th>Mkt Value</th>
                        <th>P&L</th>
                        <th>Broker Alignment</th>
                        <th>Exit Setup</th>
                      </tr>
                    </thead>
                    <tbody>
                      {openPositions.length === 0 && <tr><td colSpan={7}>No open broker positions.</td></tr>}
                      {openPositions.map((p, idx) => {
                        const symbol = String(p.SYMBOL || '').toUpperCase()
                        const positionQty = Number(p.POSITION_QTY || 0)
                        const isShort = positionQty < 0
                        const exit = protectionBySymbol.get(symbol)
                        const hasExit = exit && exit.state !== 'NONE'
                        const pendingExit = pendingExitBySymbol.get(symbol)
                        const hasPendingExit = Boolean(pendingExit)
                        return (
                          <tr key={`${p.SYMBOL || 'SYM'}_${idx}`}>
                            <td>
                              <div><b>{formatSymbolLabel(p.SYMBOL || '—', p.MARKET_TYPE || p.SECURITY_TYPE)}</b></div>
                              <div className="lpa-subtle">{p.SECURITY_TYPE || '—'}</div>
                            </td>
                            <td>{fmtNum(p.POSITION_QTY, 0)}</td>
                            <td>{fmtNum(p.AVG_COST, 4)}</td>
                            <td>{fmtNum(p.MARKET_VALUE, 2)}</td>
                            <td className={Number(p.UNREALIZED_PNL || 0) >= 0 ? 'lpa-pos' : 'lpa-neg'}>{fmtSigned(p.UNREALIZED_PNL, 2)}</td>
                            <td>
                              {(() => {
                                const recon = reconV2[symbol]
                                if (!recon) return <span className="lpa-subtle">—</span>
                                const st = String(recon.status || '').toUpperCase()
                                const badgeClass = st === 'ALIGNED' ? 'ok' : st === 'MISMATCH' ? 'warn' : st === 'ORPHAN' ? 'bad' : 'neutral'
                                return (
                                  <>
                                    <span className={`lpa-recon-badge lpa-recon-badge--${badgeClass}`}>{st}</span>
                                    {recon.position_aligned === false ? <div className="lpa-subtle">Position mismatch</div> : null}
                                    {recon.protection_aligned === false ? <div className="lpa-subtle">Protection mismatch</div> : null}
                                    {Array.isArray(recon.flags) && recon.flags.length > 0 ? (
                                      <div className="lpa-subtle">{recon.flags.join(', ')}</div>
                                    ) : null}
                                    {Array.isArray(recon.orphan_orders) && recon.orphan_orders.length > 0 ? (
                                      <div className="lpa-subtle">{recon.orphan_orders.length} orphan order{recon.orphan_orders.length > 1 ? 's' : ''}</div>
                                    ) : null}
                                  </>
                                )
                              })()}
                            </td>
                            <td>
                              <div className="lpa-exit-setup">
                                <div className="lpa-position-exit-top">
                                  {hasExit ? (
                                    <span className={`lpa-protect-chip lpa-protect-chip--${exit.activeAtBroker ? 'armed' : 'idle'}`}>
                                      {exit.activeAtBroker ? 'Armed at IB' : 'Not armed'}
                                    </span>
                                  ) : (
                                    <span className="lpa-subtle">No active TP/SL</span>
                                  )}
                                    <button
                                    type="button"
                                    className="lpa-btn lpa-btn-secondary lpa-btn-compact lpa-position-sell-btn"
                                    disabled={busy === `exit:${symbol}` || brokerExecutionBlocked}
                                    title={brokerExecutionBlocked ? (executionDisabled ? 'Execution disabled for this portfolio' : 'Session mismatch — start correct TWS/Gateway') : (hasPendingExit ? 'An exit is already in the workflow — use Pending Decisions.' : isShort ? 'Places a BUY at IB to cover the short (same as close short).' : 'Places a SELL at IB to close the long.')}
                                    onClick={() => {
                                      if (hasPendingExit) {
                                        setError('')
                                        setNotice(
                                          `Exit already queued for ${symbol} (action ${pendingExit?.action_id || '—'}). ` +
                                            'Scroll up to the blue banner or open Pending Decisions — use Submit or Reject stale. ' +
                                            'This button is intentionally not sending a duplicate exit.',
                                        )
                                        scrollFeedbackIntoView()
                                        return
                                      }
                                      createExitAction(p)
                                    }}
                                  >
                                    {busy === `exit:${symbol}`
                                      ? 'Creating...'
                                      : (hasPendingExit ? 'Exit queued' : (isShort ? 'Cover short' : 'Sell'))}
                                  </button>
                                </div>
                                {isShort ? (
                                  <div className="lpa-subtle">
                                    Negative qty = short. Cover sends <b>BUY</b> to IB. &quot;Not armed&quot; is TP/SL state, not this button.
                                  </div>
                                ) : null}
                                {hasExit ? (
                                  <>
                                    <div>State: <b>{exit.state}</b></div>
                                    <div>TP: {exit.tpStatus || '—'} {exit.tpPrice != null ? `@ ${fmtNum(exit.tpPrice, 4)}` : ''}</div>
                                    <div>
                                      {exit.slKind === 'TRAIL' ? 'TRAIL' : 'SL'}: {exit.slStatus || '—'}
                                      {exit.slKind === 'TRAIL' ? (
                                        exit.trailPercent != null
                                          ? ` @ ${fmtNum(exit.trailPercent, 1)}%`
                                          : (exit.trailAmount != null ? ` @ ${fmtNum(exit.trailAmount, 2)}` : '')
                                      ) : (
                                        exit.slPrice != null ? ` @ ${fmtNum(exit.slPrice, 4)}` : ''
                                      )}
                                    </div>
                                  </>
                                ) : (
                                  <div className="lpa-subtle">No TP/SL linked in latest order bundle.</div>
                                )}
                              </div>
                              {hasPendingExit ? <div className="lpa-subtle">Pending action: {pendingExit?.action_id}</div> : null}
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
                <div className="lpa-subtle">
                  All fills in lookback (opens and closes). <b>Closing</b> legs can show per-fill P&amp;L (IBKR when available, else a MIP estimate from snapshot avg cost).
                  Account-level realized profit lives in <b>IBKR</b>—do not sum this column.
                </div>
                <div className="lpa-mini-kpis">
                  <div><span>Lookback</span><b>{executionsChrono.length} fills</b></div>
                  <div><span>Notional</span><b>{fmtNum(tradeNotional, 2)}</b></div>
                </div>
                <div className="lpa-table-wrap">
                  <table className="lpa-table lpa-table--positions-trades">
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Qty</th>
                        <th>Notional</th>
                        <th>Commission</th>
                        <th title="Per execution fill. Do not add down the column for total account P&amp;L.">P&amp;L (per fill)</th>
                      </tr>
                    </thead>
                    <tbody>
                      {executionsChrono.length === 0 && <tr><td colSpan={6}>No executions yet.</td></tr>}
                      {executionsChrono.map((e) => {
                        const side = String(e.side || '').toUpperCase()
                        const sideLabel = e.missing_fill ? 'SELL (MISSING FILL)' : formatExecutionSideLabel(e)
                        const pnlCtx = executionPnlContext(e)
                        const isMissingFill = Boolean(e.missing_fill) || String(e.status || '').toUpperCase() === 'MISSING_FILL'
                        const qty = Number(e.qty_filled || 0)
                        const px = Number(e.avg_fill_price || 0)
                        const notional = Number.isFinite(qty) && Number.isFinite(px) ? Math.abs(qty * px) : null
                        const realizedPnl = (e.realized_pnl == null) ? null : Number(e.realized_pnl)
                        const commission = e.commission != null ? Number(e.commission) : null
                        const isEst = Boolean(e.realized_pnl_is_estimate)
                        const pnlTitle = isEst
                          ? 'MIP estimate from snapshot position basis. Can mis-classify legs (e.g. short vs long). Do not sum rows for total profit—use IBKR realized P&L.'
                          : 'Realized P&L from IB execution payload for this fill (if provided). Still per-fill only; verify totals in IBKR.'
                        const pnlNearZero =
                          realizedPnl != null && Number.isFinite(realizedPnl) && Math.abs(realizedPnl) < 1e-8
                        const favorable = realizedPnl != null && realizedPnl > 0
                        const unfavorable = realizedPnl != null && realizedPnl < 0
                        return (
                          <tr key={`${e.order_id}_${e.execution_ts || 'ts'}`} className={isMissingFill ? 'lpa-row--missing-fill' : undefined}>
                            <td>
                              <div><b>{formatSymbolLabel(e.symbol, e.market_type)}</b></div>
                              <div className="lpa-subtle">{isMissingFill ? 'Close detected — fill not synced' : fmtTs(e.execution_ts)}</div>
                            </td>
                            <td><span className={`lpa-side-chip lpa-side-chip--${side === 'BUY' ? 'buy' : 'sell'}${isMissingFill ? ' lpa-side-chip--missing' : ''}`}>{sideLabel}</span></td>
                            <td>{fmtNum(e.qty_filled, 0)}</td>
                            <td>{fmtNum(notional, 2)}</td>
                            <td className="lpa-subtle">{commission != null ? fmtNum(commission, 2) : '—'}</td>
                            <td title={pnlTitle}>
                              {isMissingFill ? (
                                <div className="lpa-pnl-stack lpa-pnl-stack--na">
                                  <span className="lpa-subtle">Missing fill</span>
                                  <span className="lpa-pnl-fill-hint">{e.missing_fill_reason || 'Refresh From IB or check IBKR Activity for this close.'}</span>
                                </div>
                              ) : realizedPnl == null ? (
                                <div className="lpa-pnl-stack lpa-pnl-stack--na">
                                  <span className="lpa-subtle">—</span>
                                  <span className="lpa-pnl-fill-hint">
                                    {pnlCtx === 'open'
                                      ? 'Open / add: realized P&L is booked when you close'
                                      : pnlCtx === 'close'
                                        ? 'No figure — check IBKR Activity / Trades for this fill'
                                        : 'IBKR or snapshot basis not available for this row'}
                                  </span>
                                </div>
                              ) : (
                                <div className="lpa-pnl-stack">
                                  <div
                                    className={
                                      isEst
                                        ? `lpa-pnl-main--est ${favorable ? 'lpa-pnl-est-pos' : 'lpa-pnl-est-neg'}`
                                        : `lpa-pnl-main--ib ${
                                            pnlNearZero ? 'lpa-pnl-ib-zero' : favorable ? 'lpa-pos' : 'lpa-neg'
                                          }`
                                    }
                                  >
                                    {isEst ? '~' : ''}{fmtSigned(realizedPnl, 2)}
                                  </div>
                                  <span className={`lpa-pnl-source-tag ${isEst ? '' : 'lpa-pnl-source-tag--ib'}`}>
                                    {isEst ? 'Estimated' : 'IBKR'}
                                  </span>
                                  <span className="lpa-pnl-fill-hint">
                                    {pnlNearZero && isEst
                                      ? 'About breakeven (approx.)'
                                      : pnlNearZero && !isEst
                                        ? 'Breakeven on this fill (IBKR)'
                                        : favorable
                                          ? 'Favorable on this fill'
                                          : unfavorable
                                            ? 'Unfavorable on this fill'
                                            : ''}
                                    {isEst && !pnlNearZero ? ' (approx.)' : ''}
                                  </span>
                                </div>
                              )}
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
                <div className="lpa-trades-footnote">
                  <strong>How to get a useful read:</strong>{' '}
                  <span className="lpa-trades-footnote__list">
                    (1) Use the <strong>Side</strong> chip—<em>Close / Cover</em> rows are where profit or loss on that leg is meaningful.
                    (2) <strong>Estimated</strong> ≈ (exit price − avg cost from our position snapshots) × qty, before fees—good for directionally
                    “better or worse than my basis,” not an audit.
                    (3) <strong>IBKR</strong> on a row means we took realized P&amp;L (or breakeven with fees) from the bridge for that fill.
                    (4) Partial fills split one order across several rows—<strong>never add</strong> the P&amp;L column for a period total.
                    (5) For real totals: <strong>IBKR</strong> → Activity / Trades (realized) or account reports—this table is a fill ledger with hints.
                  </span>
                </div>
              </div>
            </div>
          </section>
        </>
      )}
    </div>
  )
}
