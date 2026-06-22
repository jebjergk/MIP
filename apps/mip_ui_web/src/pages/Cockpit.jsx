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
import { useCallback, useEffect, useRef, useState } from 'react'
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
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')

  // Operations drawer: shared running flag so both buttons disable together.
  const [opsRunning, setOpsRunning] = useState(false)
  const [opsMsg, setOpsMsg] = useState({ type: '', text: '' })
  // Agentic search result (shown separately from the market-update result).
  const [agenticResult, setAgenticResult] = useState(null)

  // Track an in-flight overview fetch so auto-polls don't trample a
  // user-triggered manual refresh and vice-versa.
  const inFlightRef = useRef(false)

  const loadOverview = useCallback(async ({ forceRefresh = false } = {}) => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    setError('')
    try {
      // Always append a cache-buster so no proxy/browser layer can ever
      // hand back a stale payload. forceRefresh additionally tells the
      // backend to bypass its 60s intraday-overlay cache.
      const params = new URLSearchParams()
      if (forceRefresh) params.set('force_refresh', 'true')
      params.set('_t', Date.now().toString())
      const url = `${API_BASE}/cockpit/overview?${params.toString()}`
      const resp = await fetch(url, { cache: 'no-store' })
      const data = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(data?.detail || `Cockpit overview failed (${resp.status})`)
      }
      setOverview(data)
    } catch (e) {
      setError(e?.message || 'Failed to load cockpit overview.')
      setOverview((prev) => prev || null)
    } finally {
      setLoading(false)
      inFlightRef.current = false
    }
  }, [])

  useEffect(() => {
    loadOverview()
  }, [loadOverview])

  // Auto-poll every 90s while the tab is visible so the intraday card,
  // trade proposals, status freshness timestamps, and counts stay
  // current without the user having to click Refresh. We pass
  // forceRefresh so the server bypasses its 60s intraday-overlay cache
  // and TWS is queried fresh each tick (90s > a 15m bar's worth of
  // staleness budget anyway).
  useEffect(() => {
    const POLL_MS = 90_000
    const tick = () => {
      if (typeof document !== 'undefined' && document.hidden) return
      loadOverview({ forceRefresh: true })
    }
    const id = setInterval(tick, POLL_MS)
    return () => clearInterval(id)
  }, [loadOverview])

  // Manual refresh: drop the broker snapshot cache, drop the intraday
  // overlay cache, then pull a fresh overview with `force_refresh=true`
  // so the next intraday read goes straight to TWS. Visual loading
  // state is essential — without it the button looked dead because
  // the network call returned cached data instantly.
  const handleRefresh = useCallback(async () => {
    if (refreshing) return
    setRefreshing(true)
    try {
      await Promise.allSettled([
        fetch(`${API_BASE}/live/snapshot/refresh`, { method: 'POST' }),
        fetch(`${API_BASE}/cockpit/overview/refresh-cache`, { method: 'POST' }),
      ])
    } finally {
      try {
        await loadOverview({ forceRefresh: true })
      } finally {
        setRefreshing(false)
      }
    }
  }, [loadOverview, refreshing])

  const runDailyMarketUpdate = useCallback(async (synthIntraday) => {
    const prompt = synthIntraday
      ? 'Run Daily Market Update using today\u2019s session-so-far (1m \u2192 1440m, equities RTH TRADES, FX MIDPOINT)?\n\nThis refreshes market bars, returns, features, patterns, and structural state.\nThe agentic opportunity search will NOT run.'
      : 'Run Daily Market Update?\n\nThis ingests bars and runs the daily pipeline (returns, features, patterns, structural state).\nThe agentic opportunity search will NOT run automatically.'
    if (!window.confirm(prompt)) return
    setOpsRunning(true)
    setOpsMsg({ type: '', text: '' })
    setAgenticResult(null)
    try {
      const qs = new URLSearchParams()
      qs.set('dry_run', 'false')
      qs.set('skip_ingest', 'false')
      qs.set('run_pipeline', 'true')
      qs.set('run_proposal_board', 'false')
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
        throw new Error(errText || `Daily market update failed (${resp.status})`)
      }
      const partial = payload?.ingest_partial_failure ? ' (partial ingest)' : ''
      setOpsMsg({
        type: 'ok',
        text: `Daily market update completed${partial}. Market data, returns, features, and structural analysis refreshed. Agentic opportunity search was NOT run.`,
      })
      await loadOverview()
    } catch (e) {
      setOpsMsg({ type: 'error', text: e?.message || 'Daily market update failed.' })
    } finally {
      setOpsRunning(false)
    }
  }, [loadOverview])

  const runAgenticSearch = useCallback(async () => {
    const confirmed = window.confirm(
      'Search for New Trade Proposals\n\n' +
      'This runs the Phase 4 Cortex AI agent panel (~5 candidates \u00d7 7 agent sessions).\n' +
      'AI credits will be consumed. Market data must be current.\n\n' +
      'No trade will be automatically executed. Proposals appear in LPA as PENDING review items.\n\n' +
      'Proceed?'
    )
    if (!confirmed) return
    setOpsRunning(true)
    setOpsMsg({ type: '', text: '' })
    setAgenticResult(null)
    try {
      const resp = await fetch(`${API_BASE}/manage/proposal-board/run`, { method: 'POST' })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        const d = payload?.detail
        const errText = typeof d === 'string'
          ? d
          : d?.message || (typeof d === 'object' && d != null ? JSON.stringify(d) : null)
        throw new Error(errText || `Agentic search failed (${resp.status})`)
      }
      const boardResult = payload?.proposal_board_result || {}
      const proposals = boardResult?.proposals_published ?? boardResult?.published_count ?? '?'
      const candidates = boardResult?.candidates_evaluated ?? boardResult?.candidates ?? '?'
      const lpaImported = payload?.lpa_import_total_imported ?? 0
      setAgenticResult({
        type: 'ok',
        proposals,
        candidates,
        lpaImported,
        tradeAutoExecuted: payload?.trade_auto_executed === true,
        runId: payload?.cockpit_run_id,
      })
    } catch (e) {
      setAgenticResult({ type: 'error', text: e?.message || 'Agentic opportunity search failed.' })
    } finally {
      setOpsRunning(false)
    }
  }, [])

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
          <button
            className="ck-op-btn"
            type="button"
            onClick={handleRefresh}
            disabled={refreshing}
            title="Force a fresh broker + intraday read"
          >
            {refreshing ? 'Refreshing...' : 'Refresh'}
          </button>
        </span>
      </div>

      {note ? <div className="ck-co-warn-banner">{note}</div> : null}

      <CockpitStatusStrip status={status} />

      <div className="ck-co-top-row">
        <LivePortfolioOverviewCard
          data={overview.live_portfolio_overview}
          proposals={overview.trade_proposals}
        />
        <MarketPulseCompact data={overview.market_pulse} />
      </div>

      <PositionHealthSummaryTable rows={overview.position_health_summary_rows} />

      <PriorityReviewBlock data={overview.priority_review} />

      <div className="ck-co-top-row">
        <IntradayAlertSummaryBlock data={overview.intraday_summary} />
        <ShadowDisagreementSummary data={overview.shadow_summary} />
      </div>

      <details className="ck-co-ops">
        <summary>Operations</summary>

        <div className="ck-co-ops-section">
          <div className="ck-co-ops-section-title">Routine Market Maintenance</div>
          <div className="ck-co-ops-actions">
            <button
              className="ck-op-btn"
              type="button"
              onClick={() => runDailyMarketUpdate(false)}
              disabled={opsRunning}
              title="Ingest daily bars, refresh returns/features/patterns/structural state. Does NOT run agentic search."
            >
              {opsRunning ? 'Running...' : 'Run Daily Market Update'}
            </button>
            <button
              className="ck-op-btn"
              type="button"
              onClick={() => runDailyMarketUpdate(true)}
              disabled={opsRunning}
              title="Aggregate today\u2019s RTH 1m \u2192 1440m and run the daily pipeline. Does NOT run agentic search."
            >
              {opsRunning ? 'Running...' : 'Update from Today\u2019s Session'}
            </button>
          </div>
          {opsMsg.text ? (
            <div className={`ck-co-ops-msg ck-co-ops-msg--${opsMsg.type === 'ok' ? 'ok' : 'error'}`}>
              {opsMsg.text}
            </div>
          ) : null}
        </div>

        <div className="ck-co-ops-divider" />

        <div className="ck-co-ops-section">
          <div className="ck-co-ops-section-title">Agentic Opportunity Search</div>
          <div className="ck-co-ops-section-desc">
            Runs Phase 4 Cortex AI agents to find new trade proposals. Costs AI credits.
            Run &quot;Daily Market Update&quot; first to ensure fresh data.
          </div>
          <div className="ck-co-ops-actions">
            <button
              className="ck-op-btn ck-op-btn--agentic"
              type="button"
              onClick={runAgenticSearch}
              disabled={opsRunning}
              title="Runs Phase 4 Cortex AI agents. Requires fresh market data. No trade auto-executed."
            >
              {opsRunning ? 'Running...' : 'Search for New Trade Proposals'}
            </button>
          </div>
          {agenticResult ? (
            agenticResult.type === 'error' ? (
              <div className="ck-co-ops-msg ck-co-ops-msg--error">
                {agenticResult.text}
              </div>
            ) : (
              <div className="ck-co-ops-msg ck-co-ops-msg--ok">
                Agentic search complete.
                {' '}Candidates evaluated: {agenticResult.candidates}.
                {' '}Proposals published: {agenticResult.proposals}.
                {' '}Imported to LPA: {agenticResult.lpaImported}.
                {' '}No trade auto-executed.
                {agenticResult.runId ? ` [Run ${agenticResult.runId.slice(0, 8)}]` : ''}
              </div>
            )
          ) : null}
        </div>
      </details>
    </div>
  )
}
