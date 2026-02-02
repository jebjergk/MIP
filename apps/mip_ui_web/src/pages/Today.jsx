import { useState, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import InfoTooltip from '../components/InfoTooltip'
import LoadingState from '../components/LoadingState'
import { useExplainMode } from '../context/ExplainModeContext'
import { getGlossaryEntry } from '../data/glossary'
import './Today.css'

const SCOPE_TODAY = 'today'
const SCOPE_RISK = 'risk_gate'
const SCOPE_TS = 'training_status'
const SCOPE_PERF = 'performance'

function get(obj, k) {
  return obj?.[k] ?? obj?.[k?.toUpperCase?.()] ?? null
}

export default function Today() {
  const [searchParams] = useSearchParams()
  const portfolioId = searchParams.get('portfolio_id') ? parseInt(searchParams.get('portfolio_id'), 10) : null
  const { explainMode } = useExplainMode()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    const url = portfolioId != null
      ? `${API_BASE}/today?portfolio_id=${encodeURIComponent(portfolioId)}`
      : `${API_BASE}/today`
    fetch(url)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText || 'Failed to load'))))
      .then((d) => { if (!cancelled) setData(d) })
      .catch((e) => { if (!cancelled) setError(e.message) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [portfolioId])

  if (loading) {
    return (
      <>
        <h1>Today</h1>
        <LoadingState message="Loading system status and today’s view…" />
      </>
    )
  }

  if (error) {
    return (
      <>
        <h1>Today</h1>
        <ErrorState message={error} />
      </>
    )
  }

  const status = data?.status ?? {}
  const portfolio = data?.portfolio ?? null
  const brief = data?.brief ?? null
  const insights = Array.isArray(data?.insights) ? data.insights : []
  const snowflakeOk = status.snowflake_ok === true
  const statusMessage = status.message ?? (snowflakeOk ? 'OK' : 'Data backend not reachable')

  return (
    <>
      <h1>Today</h1>
      {explainMode && (
        <p className="today-intro">
          One view: system status, portfolio situation, and today’s candidates from evaluated history. Research guidance only—no auto-trading.
          <InfoTooltip scope={SCOPE_TODAY} entryKey="today_overview" variant="short" />
        </p>
      )}

      {/* Top: status one-liner (banner is in nav) */}
      <section className="today-status-line" aria-label="System status">
        <span className={`today-status-dot ${snowflakeOk ? 'today-status-dot--ok' : 'today-status-dot--degraded'}`} aria-hidden="true" />
        <span className="today-status-message">{statusMessage}</span>
        <InfoTooltip scope="ui" entryKey="system_status" variant="short" />
      </section>

      {!snowflakeOk && (
        <div className="today-degraded-notice">
          <p>Snowflake is unreachable. Portfolio, brief, and insights are unavailable until the connection is restored.</p>
        </div>
      )}

      <div className="today-grid">
        {/* Left: Portfolio situation */}
        <section className="today-portfolio" aria-label="Portfolio situation">
          <h2>Portfolio situation</h2>
          {portfolioId == null ? (
            <EmptyState
              title="No portfolio selected"
              explanation="Add ?portfolio_id=1 to the URL to see risk state, KPIs, and recent events for that portfolio."
              reasons={['Use /today?portfolio_id=1 (or your portfolio ID) to load portfolio section.']}
            />
          ) : !portfolio ? (
            <EmptyState
              title="No portfolio data"
              explanation="Risk state and events could not be loaded for this portfolio."
              reasons={['Portfolio may not exist.', 'Snowflake may be unreachable.']}
            />
          ) : (
            <>
              <div className="today-card today-card-risk">
                <h3>Risk gate status <InfoTooltip scope={SCOPE_RISK} entryKey="entries_blocked" variant="short" /></h3>
                {(() => {
                  const gate = (portfolio.risk_gate && portfolio.risk_gate[0]) || (portfolio.risk_state && portfolio.risk_state[0])
                  const entriesBlocked = gate?.ENTRIES_BLOCKED ?? gate?.entries_blocked ?? false
                  const stopReason = gate?.STOP_REASON ?? gate?.stop_reason ?? gate?.BLOCK_REASON ?? gate?.block_reason
                  const summary = entriesBlocked
                    ? 'Entries blocked but exits allowed.'
                    : 'Trading allowed.'
                  return (
                    <>
                      <p className="today-risk-summary"><strong>{summary}</strong></p>
                      {entriesBlocked && <InfoTooltip scope={SCOPE_RISK} entryKey="allow_exits_only" variant="short" />}
                      {stopReason != null && String(stopReason) !== '' && (
                        <p className="today-risk-reason">Reason: {String(stopReason)}</p>
                      )}
                    </>
                  )
                })()}
              </div>
              <div className="today-card today-card-events">
                <h3>Recent events <InfoTooltip scope={SCOPE_TODAY} entryKey="recent_events" variant="short" /></h3>
                {portfolio.run_events?.length > 0 ? (
                  <ul className="today-events-list">
                    {portfolio.run_events.slice(0, 10).map((ev, i) => (
                      <li key={i}>
                        Run {get(ev, 'RUN_ID') ?? get(ev, 'run_id')} — {get(ev, 'STOP_REASON') ?? get(ev, 'stop_reason') ?? '—'}
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="today-empty-inline">No recent run events.</p>
                )}
              </div>
            </>
          )}
        </section>

        {/* Right: Today's insights */}
        <section className="today-insights" aria-label="Today's insights">
          <h2>Today’s insights <InfoTooltip scope={SCOPE_TODAY} entryKey="candidate" variant="short" /></h2>
          {insights.length === 0 ? (
            <EmptyState
              title="No candidates to show"
              explanation="Candidates are symbol/pattern pairs from today’s eligible signals, ranked by maturity and outcome history. None met the filter (e.g. maturity stage not INSUFFICIENT, minimum recs)."
              reasons={['No eligible signals for today (INTERVAL_MINUTES=1440).', 'All candidates filtered out (maturity or sample).', 'Pipeline has not run yet.']}
            />
          ) : (
            <ul className="today-insights-list">
              {insights.map((item, i) => (
                <li key={i} className="today-insight-card">
                  <div className="today-insight-header">
                    <span className="today-insight-rank">#{i + 1}</span>
                    <span className="today-insight-symbol">{item.symbol ?? '—'}</span>
                    <span className="today-insight-pattern">pattern {item.pattern_id ?? '—'}</span>
                    <span className="today-insight-market">{item.market_type ?? '—'}</span>
                    <span
                      className={`today-insight-stage today-insight-stage--${(item.maturity_stage ?? '').toLowerCase().replace('_', '-')}`}
                      title={explainMode ? getGlossaryEntry(SCOPE_TS, item.maturity_stage === 'INSUFFICIENT' ? 'stage_insufficient' : item.maturity_stage === 'CONFIDENT' ? 'stage_confident' : 'maturity_stage')?.short : undefined}
                    >
                      {item.maturity_stage ?? '—'}
                    </span>
                    <InfoTooltip scope={SCOPE_TS} entryKey="maturity_stage" variant="short" />
                    <span className="today-insight-score" title={explainMode ? getGlossaryEntry(SCOPE_TODAY, 'today_score')?.short : undefined}>
                      Today score: {item.today_score != null ? Number(item.today_score).toFixed(2) : '—'}
                    </span>
                    <InfoTooltip scope={SCOPE_TODAY} entryKey="today_score" variant="short" />
                  </div>
                  <div className="today-insight-maturity-bar">
                    <div
                      className="today-insight-maturity-fill"
                      style={{ width: `${Math.min(100, Math.max(0, item.maturity_score ?? 0))}%` }}
                      role="progressbar"
                      aria-valuenow={item.maturity_score ?? 0}
                      aria-valuemin={0}
                      aria-valuemax={100}
                    />
                  </div>
                  <p className="today-insight-what" title={explainMode ? getGlossaryEntry(SCOPE_PERF, 'mean_outcome')?.short : undefined}>
                    What history suggests: maturity score {item.maturity_score ?? '—'}; outcomes at 5-bar horizon in performance summary.
                    <InfoTooltip scope={SCOPE_PERF} entryKey="mean_outcome" variant="short" />
                  </p>
                  <p className="today-insight-why" title={explainMode ? getGlossaryEntry(SCOPE_TODAY, 'why_this_is_shown')?.short : undefined}>
                    Why it’s shown: {item.why_this_is_here ?? '—'}
                    <InfoTooltip scope={SCOPE_TODAY} entryKey="why_this_is_shown" variant="short" />
                  </p>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>

      {/* Bottom: collapsible Latest brief */}
      <section className="today-brief" aria-label="Latest brief">
        <h2>Latest brief <InfoTooltip scope={SCOPE_TODAY} entryKey="latest_brief" variant="short" /></h2>
        {portfolioId == null ? (
          <p className="today-empty-inline">Select a portfolio (?portfolio_id=1) to see the latest brief.</p>
        ) : !brief ? (
          <EmptyState
            title="No brief"
            explanation="No morning brief found for this portfolio."
            reasons={['Pipeline has not written a brief yet.', 'Agent run may not have completed.']}
          />
        ) : (
          <details className="today-brief-details">
            <summary>Brief as of {brief.as_of_ts ?? '—'} (run {brief.pipeline_run_id ?? '—'})</summary>
            {brief.brief_json != null ? (
              <pre className="today-brief-json">{typeof brief.brief_json === 'string' ? brief.brief_json : JSON.stringify(brief.brief_json, null, 2)}</pre>
            ) : (
              <p className="today-empty-inline">No brief JSON.</p>
            )}
          </details>
        )}
      </section>
    </>
  )
}
