/**
 * Cockpit — primary daily operator landing page (live/structural era).
 *
 * Single GET to /api/cockpit/overview returns the entire payload.
 * The page is composed of focused subcomponents under ./cockpit/:
 *
 *   CockpitStatusStrip          — broker / market / pipeline / intraday
 *   LivePortfolioOverviewCard   — single canonical NAV / cash / positions
 *   MarketPulseCompact          — STOCK-only breadth + tone label
 *   PositionHealthSummaryTable  — current live positions only
 *   PriorityReviewBlock         — capped exception lists
 *   IntradayAlertSummaryBlock   — overlay_status + counts
 *   ShadowDisagreementSummary   — aggregate shadow vs real counts
 *
 * Legacy committee/news/readiness/digest blocks and the duplicate
 * portfolio cards have been removed from the cockpit per the
 * structural/live re-anchor. Daily IB Job buttons are demoted into a
 * small <details> drawer at the bottom; ops can still trigger them
 * without polluting the operator-first layout.
 */
import { useCallback, useEffect, useState } from 'react'
import { API_BASE } from '../config/apiBase'
import LoadingState from '../components/LoadingState'
import { useAskMipPageRuntime } from '../hooks/useAskMipPageRuntime'

import CockpitStatusStrip from './cockpit/CockpitStatusStrip'
import LivePortfolioOverviewCard from './cockpit/LivePortfolioOverviewCard'
import MarketPulseCompact from './cockpit/MarketPulseCompact'
import PositionHealthSummaryTable from './cockpit/PositionHealthSummaryTable'
import PriorityReviewBlock from './cockpit/PriorityReviewBlock'
import IntradayAlertSummaryBlock from './cockpit/IntradayAlertSummaryBlock'
import ShadowDisagreementSummary from './cockpit/ShadowDisagreementSummary'

import './Cockpit.css'
import './cockpit/Cockpit.css'


function formatTs(ts) {
  if (!ts) return '\u2014'
  try {
    const s = String(ts)
    const norm = /^\d{4}-\d{2}-\d{2}T/.test(s) && !/(?:Z|[+-]\d{2}:\d{2})$/i.test(s) ? `${s}Z` : s
    return new Date(norm).toLocaleString(undefined, {
      month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit',
    })
  } catch { return String(ts) }
}


export default function Cockpit() {
  useAskMipPageRuntime('cockpit', ['cockpit_overview', 'cockpit_position_health'])

  const [overview, setOverview] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  // Demoted ops drawer state (full IB daily job + partial RTH variant).
  const [opsRunning, setOpsRunning] = useState(false)
  const [opsMsg, setOpsMsg] = useState({ type: '', text: '' })

  const loadOverview = useCallback(async () => {
    setError('')
    try {
      const resp = await fetch(`${API_BASE}/cockpit/overview`)
      const data = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(data?.detail || `Cockpit overview failed (${resp.status})`)
      }
      setOverview(data)
    } catch (e) {
      setError(e?.message || 'Failed to load cockpit overview.')
      setOverview(null)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadOverview()
  }, [loadOverview])

  // Best-effort broker snapshot refresh on demand. We intentionally do
  // NOT block initial render on this — the overview already reflects
  // the most recent persisted snapshot, and waiting for a synchronous
  // IBKR roundtrip just to render the page is exactly what the
  // refactor avoids.
  const handleRefresh = useCallback(async () => {
    try {
      await fetch(`${API_BASE}/live/snapshot/refresh`, { method: 'POST' })
    } catch {
      // non-fatal
    }
    await loadOverview()
  }, [loadOverview])

  const runIbJob = useCallback(async (synthIntraday) => {
    const prompt = synthIntraday
      ? 'Run partial daily pipeline using today\u2019s session-so-far (1m \u2192 1440m, equities RTH TRADES, FX MIDPOINT)?'
      : 'Run full IB daily job now? This ingests bars and runs the daily pipeline.'
    if (!window.confirm(prompt)) return
    setOpsRunning(true)
    setOpsMsg({ type: '', text: '' })
    try {
      const qs = new URLSearchParams()
      qs.set('dry_run', 'false')
      qs.set('skip_ingest', 'false')
      qs.set('run_pipeline', 'true')
      if (synthIntraday) qs.set('synth_intraday_daily', 'true')
      const resp = await fetch(`${API_BASE}/manage/ib/daily-job/run?${qs.toString()}`, {
        method: 'POST',
      })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        const d = payload?.detail
        const errText = typeof d === 'string'
          ? d
          : d?.message || (typeof d === 'object' && d != null ? JSON.stringify(d) : null)
        throw new Error(errText || `IB daily job failed (${resp.status})`)
      }
      setOpsMsg({ type: 'ok', text: 'IB job completed and daily pipeline triggered.' })
      await loadOverview()
    } catch (e) {
      setOpsMsg({ type: 'error', text: e?.message || 'IB daily job failed.' })
    } finally {
      setOpsRunning(false)
    }
  }, [loadOverview])

  if (loading) {
    return (
      <div className="ck-co-page">
        <h1>Cockpit</h1>
        <LoadingState message="Loading cockpit overview..." />
      </div>
    )
  }

  if (error || !overview) {
    return (
      <div className="ck-co-page">
        <h1>Cockpit</h1>
        <div className="ck-co-error-banner">
          {error || 'No cockpit data available.'}
        </div>
        <button className="ck-op-btn" type="button" onClick={loadOverview}>
          Retry
        </button>
      </div>
    )
  }

  const status = overview.status
  const note = status?.note

  return (
    <div className="ck-co-page">
      <div className="ck-co-page-header">
        <h1>Cockpit</h1>
        <span className="ck-co-page-sub">As of {formatTs(overview.as_of_ts)}</span>
        <span style={{ marginLeft: 'auto' }}>
          <button className="ck-op-btn" type="button" onClick={handleRefresh}>
            Refresh
          </button>
        </span>
      </div>

      {note ? <div className="ck-co-warn-banner">{note}</div> : null}

      <CockpitStatusStrip status={status} />

      <div className="ck-co-top-row">
        <LivePortfolioOverviewCard data={overview.live_portfolio_overview} />
        <MarketPulseCompact data={overview.market_pulse} />
      </div>

      <PositionHealthSummaryTable rows={overview.position_health_summary_rows} />

      <PriorityReviewBlock data={overview.priority_review} />

      <div className="ck-co-top-row">
        <IntradayAlertSummaryBlock data={overview.intraday_summary} />
        <ShadowDisagreementSummary data={overview.shadow_summary} />
      </div>

      <details className="ck-co-ops">
        <summary>Operations (IB daily job)</summary>
        <div className="ck-co-ops-actions">
          <button
            className="ck-op-btn"
            type="button"
            onClick={() => runIbJob(false)}
            disabled={opsRunning}
          >
            {opsRunning ? 'Running...' : 'Run IB Daily Job'}
          </button>
          <button
            className="ck-op-btn"
            type="button"
            onClick={() => runIbJob(true)}
            disabled={opsRunning}
            title="Aggregate today's RTH 1m \u2192 1440m and run the daily pipeline now."
          >
            {opsRunning ? 'Running...' : 'Pipeline (today RTH \u2192 daily)'}
          </button>
        </div>
        {opsMsg.text ? (
          <div className={`ck-co-ops-msg ck-co-ops-msg--${opsMsg.type === 'ok' ? 'ok' : 'error'}`}>
            {opsMsg.text}
          </div>
        ) : null}
      </details>
    </div>
  )
}
