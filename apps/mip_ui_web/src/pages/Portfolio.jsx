import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, ReferenceLine, Cell } from 'recharts'
import { API_BASE } from '../App'
import InfoTooltip from '../components/InfoTooltip'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import EpisodeCard from '../components/EpisodeCard'
import PortfolioMiniGridCharts from '../components/PortfolioMiniGridCharts'
import { useExplainMode } from '../context/ExplainModeContext'
import { useExplainCenter, useExplainSection } from '../context/ExplainCenterContext'
import { useFreshness, relativeTime } from '../context/FreshnessContext'
import { getGlossaryEntry, getGlossaryEntryByDotKey } from '../data/glossary'
import { PORTFOLIO_EXPLAIN_CONTEXT, RISK_GATE_EXPLAIN_CONTEXT } from '../data/explainContexts'
import './Portfolio.css'

export default function Portfolio() {
  const { portfolioId } = useParams()
  const { explainMode } = useExplainMode()
  const { latestRunId, latestRunTs, isStale } = useFreshness()
  const statusBadgeTitle = explainMode ? getGlossaryEntryByDotKey('ui.status_badge')?.long : undefined
  const [portfolios, setPortfolios] = useState([])
  const [portfolio, setPortfolio] = useState(null)
  const [snapshot, setSnapshot] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [lookbackDays, setLookbackDays] = useState(30)
  const [showRestartModal, setShowRestartModal] = useState(false)
  const [episodes, setEpisodes] = useState([])
  const [timeline, setTimeline] = useState(null)
  const [activeAnalytics, setActiveAnalytics] = useState(null)
  const [proposerDiagnostics, setProposerDiagnostics] = useState(null)
  const { setContext } = useExplainCenter()
  const openExplainRiskGate = useExplainSection(RISK_GATE_EXPLAIN_CONTEXT)

  useEffect(() => {
    setContext(PORTFOLIO_EXPLAIN_CONTEXT)
  }, [setContext])

  useEffect(() => {
    let cancelled = false
    async function load() {
      setLoading(true)
      setError(null)
      try {
        const listRes = await fetch(`${API_BASE}/portfolios`)
        if (!listRes.ok) throw new Error(listRes.statusText)
        const list = await listRes.json()
        if (cancelled) return
        setPortfolios(list)
        if (portfolioId) {
          const headerRes = await fetch(`${API_BASE}/portfolios/${portfolioId}`)
          if (!headerRes.ok) throw new Error(headerRes.statusText)
          const port = await headerRes.json()
          if (cancelled) return
          setPortfolio(port)
          const runId = port.LAST_SIMULATION_RUN_ID ?? null
          const params = new URLSearchParams()
          if (runId) params.set('run_id', runId)
          params.set('lookback_days', String(lookbackDays))
          const snapUrl = `${API_BASE}/portfolios/${portfolioId}/snapshot?${params.toString()}`
          const snapRes = await fetch(snapUrl)
          if (!snapRes.ok) throw new Error(snapRes.statusText)
          const snapData = await snapRes.json()
          if (!cancelled) setSnapshot(snapData)
          const epRes = await fetch(`${API_BASE}/portfolios/${portfolioId}/episodes`)
          if (epRes.ok) {
            const epList = await epRes.json()
            if (!cancelled) setEpisodes(Array.isArray(epList) ? epList : [])
          } else if (!cancelled) setEpisodes([])
          const tlRes = await fetch(`${API_BASE}/portfolios/${portfolioId}/timeline`)
          if (tlRes.ok) {
            const tlData = await tlRes.json()
            if (!cancelled) setTimeline(tlData)
          } else if (!cancelled) setTimeline(null)
          const activeEp = snapData?.active_episode
          const activeEpisodeId = activeEp?.episode_id ?? activeEp?.EPISODE_ID
          if (activeEpisodeId && !cancelled) {
            fetch(`${API_BASE}/portfolios/${portfolioId}/episodes/${activeEpisodeId}`)
              .then(r => r.ok ? r.json() : null)
              .then(data => { if (!cancelled) setActiveAnalytics(data) })
              .catch(() => { if (!cancelled) setActiveAnalytics(null) })
          } else if (!cancelled) setActiveAnalytics(null)
          // Fetch proposer diagnostics for this portfolio
          fetch(`${API_BASE}/market-timeline/diagnostics?portfolio_id=${portfolioId}`)
            .then(r => r.ok ? r.json() : null)
            .then(data => { if (!cancelled) setProposerDiagnostics(data) })
            .catch(() => { if (!cancelled) setProposerDiagnostics(null) })
        } else {
          setPortfolio(null)
          setSnapshot(null)
          setEpisodes([])
          setTimeline(null)
          setActiveAnalytics(null)
        }
      } catch (e) {
        if (!cancelled) setError(e.message)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => { cancelled = true }
  }, [portfolioId, lookbackDays])

  if (loading) {
    return (
      <>
        <h1>{portfolioId ? 'Portfolio' : 'Portfolios'}</h1>
        <LoadingState />
      </>
    )
  }
  if (error) {
    return (
      <>
        <h1>{portfolioId ? 'Portfolio' : 'Portfolios'}</h1>
        {portfolioId && <p><Link to="/portfolios">← Back to list</Link></p>}
        <ErrorState message={error} />
      </>
    )
  }


  if (portfolioId && portfolio) {
    const activeEpisode = snapshot?.active_episode
    const episodeLabel = activeEpisode
      ? `Episode ${activeEpisode.episode_id ?? activeEpisode.EPISODE_ID ?? '—'} since ${(activeEpisode.start_ts ?? activeEpisode.START_TS ?? '').slice(0, 10)}`
      : null

    const activeEp = snapshot?.active_episode
    const activeSince = (activeEp?.start_ts ?? activeEp?.START_TS ?? '').slice(0, 10)
    const activeProfileId = activeEp?.profile_id ?? activeEp?.PROFILE_ID
    
    // Item 2 fix: Use episode-scoped risk state for consistency with drawdown chart
    // This ensures the risk label matches what the episode drawdown chart shows
    const episodeRiskState = snapshot?.episode_risk_state
    const riskLabel = episodeRiskState?.risk_label ?? snapshot?.risk_gate?.risk_label ?? 'NORMAL'
    const episodeDrawdownBreached = episodeRiskState?.episode_drawdown_breached ?? false
    
    // Derive gate state from episode-scoped data when available
    let gateState = 'SAFE'
    if (episodeRiskState?.entries_blocked) {
      gateState = 'STOPPED'
    } else if (episodeDrawdownBreached || riskLabel === 'CAUTION') {
      gateState = 'CAUTION'
    } else if (riskLabel === 'DEFENSIVE') {
      gateState = 'STOPPED'
    }
    
    // Explain sentence: show episode-specific info if drawdown is high
    let explainSentence = snapshot?.risk_gate?.reason_text ?? 'Portfolio is within safe limits.'
    if (episodeRiskState && episodeDrawdownBreached && gateState !== 'STOPPED') {
      const ddPct = episodeRiskState.episode_drawdown_pct?.toFixed(1) ?? '?'
      const stopPct = episodeRiskState.episode_drawdown_stop_pct?.toFixed(1) ?? '?'
      explainSentence = `Episode drawdown at ${ddPct}% (threshold: ${stopPct}%). Approaching risk limit.`
    }
    
    const activeHeaderLine = activeEp && activeSince
      ? `Active since ${activeSince} · Profile ${activeProfileId ?? '—'} · ${gateState}`
      : null
    
    // Freshness check (Item 3 fix: use market dates, not run IDs)
    // portfolio_simulated_through_ts: latest market date the portfolio has data for
    // pipeline_last_run_at: when the pipeline last ran successfully
    // latest_available_bar_date: most recent trading day with data available
    const portfolioSimulatedThrough = snapshot?.portfolio_simulated_through_ts
    const pipelineLastRunAt = snapshot?.pipeline_last_run_at
    const latestAvailableBarDate = snapshot?.latest_available_bar_date
    
    // Format the simulated-through date for display (just the date part)
    const simulatedThroughDate = portfolioSimulatedThrough 
      ? portfolioSimulatedThrough.slice(0, 10)
      : null
    
    // Format pipeline run time (just the time part for same-day, or relative)
    const pipelineRunTime = pipelineLastRunAt
      ? new Date(pipelineLastRunAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      : null
    
    // Determine if truly stale: portfolio simulated-through date < latest available bar date
    // Only stale if there's a newer market date we haven't simulated through yet
    const isTrulyStale = simulatedThroughDate && latestAvailableBarDate
      ? simulatedThroughDate < latestAvailableBarDate
      : false
    
    const hasFreshnessData = portfolioSimulatedThrough || pipelineLastRunAt

    return (
      <>
        <h1>Portfolio: {portfolio.NAME}</h1>
        {episodeLabel && <p className="portfolio-episode-header">{episodeLabel}</p>}
        
        {/* Freshness header (Item 3: informational, not misleading) */}
        <div className="portfolio-freshness-header">
          {isTrulyStale ? (
            <>
              <span className="freshness-badge freshness-stale">PENDING UPDATE</span>
              <span className="freshness-text">
                Simulated through {simulatedThroughDate} · New market date available ({latestAvailableBarDate})
              </span>
            </>
          ) : hasFreshnessData ? (
            <>
              <span className="freshness-badge freshness-current">CURRENT</span>
              <span className="freshness-text">
                {simulatedThroughDate && `Simulated through ${simulatedThroughDate}`}
                {simulatedThroughDate && pipelineRunTime && ' · '}
                {pipelineRunTime && `Pipeline ran at ${pipelineRunTime}`}
              </span>
            </>
          ) : null}
        </div>
        
        <p><Link to="/portfolios">← Back to list</Link></p>

        {activeEp && (
          <section className="portfolio-active-period" aria-label="Active period dashboard">
            {(activeHeaderLine || explainSentence) && (
              <>
                {activeHeaderLine && <p className="mini-grid-header-line">{activeHeaderLine}</p>}
                {explainSentence && <p className="mini-grid-explain">{explainSentence}</p>}
              </>
            )}
            {activeAnalytics ? (
              <PortfolioMiniGridCharts
                titlePrefix=""
                dateRange={{ start_ts: activeEp.start_ts ?? activeEp.START_TS, end_ts: null }}
                series={{
                  equity: activeAnalytics.equity_series ?? [],
                  drawdown: activeAnalytics.drawdown_series ?? [],
                  tradesPerDay: activeAnalytics.trades_per_day ?? [],
                  regime: activeAnalytics.regime_per_day ?? [],
                }}
                thresholds={activeAnalytics.thresholds ?? {}}
                events={activeAnalytics.events ?? []}
              />
            ) : (
              <p className="portfolio-active-period-loading">Loading analytics…</p>
            )}
          </section>
        )}

        <h2>Header</h2>
        <div className="kpi-cards">
          {portfolio.STARTING_CASH != null && (
            <div className="kpi-card">
              <span className="kpi-label">Starting cash <InfoTooltip scope="portfolio" key="starting_cash" variant="short" /></span>
              <span className="kpi-value">{Number(portfolio.STARTING_CASH).toLocaleString()}</span>
            </div>
          )}
          {snapshot?.cards?.cash_and_exposure?.cash != null && (
            <div className="kpi-card">
              <span className="kpi-label">Current cash</span>
              <span className="kpi-value">{Number(snapshot.cards.cash_and_exposure.cash).toLocaleString(undefined, { minimumFractionDigits: 2 })}</span>
            </div>
          )}
          {portfolio.FINAL_EQUITY != null && (
            <div className="kpi-card">
              <span className="kpi-label">Final equity <InfoTooltip scope="portfolio" key="final_equity" variant="short" /></span>
              <span className="kpi-value">{Number(portfolio.FINAL_EQUITY).toLocaleString()}</span>
            </div>
          )}
          {portfolio.TOTAL_RETURN != null && (
            <div className="kpi-card">
              <span className="kpi-label">Total return <InfoTooltip scope="portfolio" key="total_return" variant="short" /></span>
              <span className="kpi-value">{Number(portfolio.TOTAL_RETURN * 100).toFixed(2)}%</span>
            </div>
          )}
          {portfolio.MAX_DRAWDOWN != null && (
            <div className="kpi-card">
              <span className="kpi-label">Max drawdown <InfoTooltip scope="portfolio" key="max_drawdown" variant="short" /></span>
              <span className="kpi-value">{Number(portfolio.MAX_DRAWDOWN * 100).toFixed(2)}%</span>
            </div>
          )}
          {portfolio.WIN_DAYS != null && (
            <div className="kpi-card">
              <span className="kpi-label">Win days <InfoTooltip scope="portfolio" key="win_days" variant="short" /></span>
              <span className="kpi-value">{portfolio.WIN_DAYS}</span>
            </div>
          )}
          {portfolio.LOSS_DAYS != null && (
            <div className="kpi-card">
              <span className="kpi-label">Loss days <InfoTooltip scope="portfolio" key="loss_days" variant="short" /></span>
              <span className="kpi-value">{portfolio.LOSS_DAYS}</span>
            </div>
          )}
          {portfolio.STATUS && (
            <div className="kpi-card">
              <span className="kpi-label">Status <InfoTooltip scope="portfolio" key="status" variant="short" /></span>
              <span className="kpi-value status-badge" title={statusBadgeTitle}>{portfolio.STATUS}</span>
            </div>
          )}
        </div>
        {snapshot && (() => {
          const cards = snapshot.cards || {}
          const cashExposure = cards.cash_and_exposure
          const openPositions = cards.open_positions ?? snapshot.positions ?? []
          const closedThisBarPositions = cards.closed_this_bar_positions ?? snapshot.closed_this_bar_positions ?? []
          const tradesList = Array.isArray(snapshot.trades) ? snapshot.trades : (cards.recent_trades ?? [])
          const tradesTotal = snapshot.trades_total ?? cards.trades_total ?? 0
          const lastTradeTs = snapshot.last_trade_ts ?? cards.last_trade_ts ?? null
          const riskGate = snapshot.risk_gate || {}
          const riskStrategy = snapshot.risk_strategy || null
          const riskLabel = riskGate.risk_label || 'NORMAL'
          const mode = riskGate.mode || 'ALLOW_ENTRIES'
          const entriesAllowed = riskGate.entries_allowed !== false
          const exitsAllowed = riskGate.exits_allowed !== false
          const reasonText = riskGate.reason_text ?? 'Portfolio is within safe limits.'
          const whatToDoNow = Array.isArray(riskGate.what_to_do_now) ? riskGate.what_to_do_now : []

          return (
            <>
              <h2>Portfolio Overview</h2>
              <p className="portfolio-overview-intro">Where do we stand right now? Snapshot as of last simulation run.</p>
              <p className="portfolio-overview-semantics">
                Positions are current holdings (snapshot); trades are execution history (events).
                <InfoTooltip scope="positions" entryKey="snapshot" variant="short" />
                <InfoTooltip scope="trades" entryKey="event" variant="short" />
              </p>

              <section className="portfolio-cards" aria-label="Portfolio snapshot cards">
                {/* Cash & Exposure */}
                <div className="portfolio-card portfolio-card-cash-exposure">
                  <h3 className="portfolio-card-title">Cash & Exposure <InfoTooltip scope="portfolio" key="total_equity" variant="short" /></h3>
                  {cashExposure ? (
                    <dl className="portfolio-card-dl">
                      <dt>Cash <InfoTooltip scope="portfolio" key="cash" variant="short" /></dt>
                      <dd>{cashExposure.cash != null ? Number(cashExposure.cash).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</dd>
                      <dt>Exposure <InfoTooltip scope="portfolio" key="exposure" variant="short" /></dt>
                      <dd>{cashExposure.exposure != null ? Number(cashExposure.exposure).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</dd>
                      <dt>Total equity <InfoTooltip scope="portfolio" key="total_equity" variant="short" /></dt>
                      <dd>{cashExposure.total_equity != null ? Number(cashExposure.total_equity).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</dd>
                      {cashExposure.as_of_ts && <dd className="portfolio-card-meta">As of {String(cashExposure.as_of_ts).slice(0, 19)}</dd>}
                    </dl>
                  ) : (
                    <p className="portfolio-card-empty">No daily data yet. Run pipeline to see cash and exposure.</p>
                  )}
                </div>

                {/* Open Positions — sorted by hold-until bar ascending (next to close first) */}
                <div id="portfolio-positions" className="portfolio-card portfolio-card-positions">
                  <h3 className="portfolio-card-title">Open Positions <InfoTooltip scope="positions" entryKey="symbol" variant="short" /></h3>
                  {(cards.as_of_ts != null || cards.current_bar_index != null) && (
                    <p className="portfolio-positions-as-of" title="Snapshot date and current bar index (daily bars)">
                      {cards.as_of_ts != null && <span>As of {String(cards.as_of_ts).slice(0, 10)}</span>}
                      {cards.current_bar_index != null && <span>{cards.as_of_ts != null ? ' · ' : ''}Bar {cards.current_bar_index}</span>}
                    </p>
                  )}
                  {Array.isArray(openPositions) && openPositions.length > 0 ? (
                    <div className="portfolio-card-table-wrap">
                      <table className="portfolio-card-table">
                        <thead>
                          <tr>
                            <th>Symbol <InfoTooltip scope="positions" entryKey="symbol" variant="short" /></th>
                            <th>Side <InfoTooltip scope="positions" entryKey="side" variant="short" /></th>
                            <th>Quantity <InfoTooltip scope="positions" entryKey="quantity" variant="short" /></th>
                            <th>Cost basis <InfoTooltip scope="positions" entryKey="cost_basis" variant="short" /></th>
                            <th title="Bar index when this position is scheduled to close (next to close first)">Hold until (bar)</th>
                            <th title="Calendar date when this position is scheduled to close">Hold until (date)</th>
                          </tr>
                        </thead>
                        <tbody>
                          {[...openPositions]
                            .sort((a, b) => {
                              const ha = a.HOLD_UNTIL_INDEX ?? a.hold_until_index ?? 0
                              const hb = b.HOLD_UNTIL_INDEX ?? b.hold_until_index ?? 0
                              return Number(ha) - Number(hb)
                            })
                            .slice(0, 20)
                            .map((pos, i) => (
                              <tr key={i}>
                                <td>{pos.SYMBOL ?? pos.symbol}</td>
                                <td>{pos.side_label ?? pos.side ?? '—'}</td>
                                <td>{pos.QUANTITY ?? pos.quantity}</td>
                                <td>{pos.COST_BASIS != null ? Number(pos.COST_BASIS ?? pos.cost_basis).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</td>
                                <td>{pos.HOLD_UNTIL_INDEX ?? pos.hold_until_index ?? '—'}</td>
                                <td>{pos.hold_until_ts ? String(pos.hold_until_ts).slice(0, 10) : '—'}</td>
                              </tr>
                            ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <EmptyState
                      title="No open positions"
                      action="Run pipeline or wait for next run."
                      explanation="This portfolio has no open positions right now. Positions are the assets (e.g. stocks) the strategy currently holds."
                      reasons={['The run has not opened any positions yet.', 'Data may be from a time before any trades.', 'The risk gate may be blocking new entries.']}
                    />
                  )}
                </div>

                {/* Recent Trades (events) */}
                <div className="portfolio-card portfolio-card-trades">
                  <h3 className="portfolio-card-title">Trades (events) <InfoTooltip scope="trades" entryKey="event" variant="short" /></h3>
                  <div className="portfolio-trades-lookback">
                    <label>
                      Lookback
                      <select
                        value={lookbackDays}
                        onChange={(e) => setLookbackDays(Number(e.target.value))}
                        aria-label="Trades lookback days"
                      >
                        <option value={1}>1 day</option>
                        <option value={7}>7 days</option>
                        <option value={30}>30 days</option>
                        <option value={-1}>All</option>
                      </select>
                    </label>
                    {tradesTotal > 0 && (
                      <span className="portfolio-trades-total">{tradesTotal} total</span>
                    )}
                  </div>
                  {tradesList.length > 0 ? (
                    <div className="portfolio-card-table-wrap">
                      <table className="portfolio-card-table">
                        <thead>
                          <tr>
                            <th>Symbol <InfoTooltip scope="trades" entryKey="symbol" variant="short" /></th>
                            <th>Side <InfoTooltip scope="trades" entryKey="side" variant="short" /></th>
                            <th>Quantity <InfoTooltip scope="trades" entryKey="quantity" variant="short" /></th>
                            <th>Price <InfoTooltip scope="trades" entryKey="price" variant="short" /></th>
                            <th>Notional <InfoTooltip scope="trades" entryKey="notional" variant="short" /></th>
                          </tr>
                        </thead>
                        <tbody>
                          {tradesList.slice(0, 20).map((t, i) => {
                              const price = t.PRICE ?? t.price
                              const qty = t.QUANTITY ?? t.quantity
                              const notional = (price != null && qty != null)
                                ? Number(price) * Number(qty)
                                : (t.NOTIONAL ?? t.notional)
                              return (
                                <tr key={i} className={t.from_last_run ? 'portfolio-trade-from-last-run' : undefined} title={t.from_last_run ? 'From latest run' : undefined}>
                                  <td>{t.SYMBOL ?? t.symbol}</td>
                                  <td>{t.SIDE ?? t.side}</td>
                                  <td>{t.QUANTITY ?? t.quantity}</td>
                                  <td>{price != null ? Number(price).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</td>
                                  <td>{notional != null ? Number(notional).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</td>
                                </tr>
                              )
                            })}
                        </tbody>
                      </table>
                    </div>
                  ) : tradesTotal > 0 ? (
                    <p className="portfolio-trades-no-range">
                      No trades in this range. Last trade: {lastTradeTs != null ? String(lastTradeTs).slice(0, 19) : '—'}
                    </p>
                  ) : (
                    <EmptyState
                      title="No trades"
                      action="Run pipeline or wait for next run."
                      explanation="Trades are execution history (events). None have been recorded for this portfolio yet."
                      reasons={['The run may not have executed any trades yet.', 'Data may be from before the first trade.', 'Score or threshold filters may have excluded all signals.', 'The risk gate may be blocking new entries.']}
                    />
                  )}
                </div>

                {/* Risk Gate panel */}
                <div className="portfolio-card portfolio-card-risk-gate">
                  <h3 className="portfolio-card-title risk-gate-headline" title={explainMode ? (getGlossaryEntry('risk_gate', 'mode')?.short ?? '') : undefined}>
                    Risk Gate:{' '}
                    <span className="risk-gate-mode risk-gate-mode--current">
                      {riskLabel === 'NORMAL' && '✅ Normal'}
                      {riskLabel === 'CAUTION' && '⚠️ Caution'}
                      {riskLabel === 'DEFENSIVE' && '🛑 Defensive'}
                      {riskLabel !== 'NORMAL' && riskLabel !== 'CAUTION' && riskLabel !== 'DEFENSIVE' && '✅ Normal'}
                    </span>
                    {explainMode && <InfoTooltip scope="risk_gate" entryKey="mode" variant="short" />}
                  </h3>
                  {riskStrategy && (
                    <div className="risk-strategy-block">
                      <p className="risk-strategy-summary">{riskStrategy.summary}</p>
                      {Array.isArray(riskStrategy.rules) && riskStrategy.rules.length > 0 && (
                        <div className="risk-strategy-rules" role="list">
                          {riskStrategy.rules.map((r) => (
                            <div key={r.key} className="risk-strategy-rule" role="listitem">
                              <span className="risk-strategy-rule-label" title={r.tooltip ?? undefined}>{r.label}</span>
                              <span className="risk-strategy-rule-value">{r.value}</span>
                            </div>
                          ))}
                        </div>
                      )}
                      {riskStrategy.state && (
                        <div className="risk-strategy-state">
                          <span className={`risk-strategy-chip risk-strategy-chip--${(riskStrategy.state.state_label || 'SAFE').toLowerCase()}`}>
                            {riskStrategy.state.state_label ?? 'SAFE'}
                          </span>
                          {riskStrategy.state.reason_text && (
                            <span className="risk-strategy-reason">{riskStrategy.state.reason_text}</span>
                          )}
                        </div>
                      )}
                    </div>
                  )}
                  <p className="risk-gate-subtext">
                    {mode === 'ALLOW_EXITS_ONLY' ? 'Exits only' : 'Entries allowed'}
                  </p>
                  <dl className="portfolio-card-dl risk-gate-matrix">
                    <dt>Open new positions <InfoTooltip scope="risk_gate" entryKey="entries_allowed" variant="short" /></dt>
                    <dd>{entriesAllowed ? 'Allowed' : 'Blocked'}</dd>
                    <dt>Close/reduce positions <InfoTooltip scope="risk_gate" entryKey="exits_allowed" variant="short" /></dt>
                    <dd>Allowed</dd>
                  </dl>
                  <p className="risk-gate-reason" title={explainMode ? (getGlossaryEntry('risk_gate', 'reason_text')?.short ?? '') : undefined}>
                    {reasonText}
                    {explainMode && <InfoTooltip scope="risk_gate" entryKey="reason_text" variant="short" />}
                  </p>
                  {whatToDoNow.length > 0 && (
                    <div className="risk-gate-what-to-do" title={explainMode ? (getGlossaryEntry('risk_gate', 'what_to_do_now')?.short ?? '') : undefined}>
                      <span className="risk-gate-what-to-do-label">What to do now</span>
                      {explainMode && <InfoTooltip scope="risk_gate" entryKey="what_to_do_now" variant="short" />}
                      <ul className="risk-gate-bullets">
                        {whatToDoNow.map((item, i) => (
                          <li key={i}>{item}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                  <div className="risk-gate-actions">
                    <Link to="/suggestions" className="risk-gate-link">Open Suggestions</Link>
                    <a href="#portfolio-positions" className="risk-gate-link">View Positions</a>
                    <button type="button" className="risk-gate-link risk-gate-link--button" onClick={() => setShowRestartModal(true)}>How to restart episode</button>
                  </div>
                </div>
                {showRestartModal && (
                  <div className="risk-gate-modal-overlay" role="dialog" aria-modal="true" aria-labelledby="restart-modal-title">
                    <div className="risk-gate-modal">
                      <h4 id="restart-modal-title">How to restart a portfolio episode</h4>
                      <p>Run the script:</p>
                      <code className="risk-gate-modal-script">MIP/SQL/scripts/restart_portfolio_episode.sql</code>
                      <p>See <strong>Restarting a portfolio episode</strong> in the Runbook (docs/ux/73_UX_RUNBOOK.md) for full steps.</p>
                      <button type="button" className="risk-gate-modal-close" onClick={() => setShowRestartModal(false)}>Close</button>
                    </div>
                  </div>
                )}

                {/* Proposer Diagnostics: why no proposals? */}
                {proposerDiagnostics && (
                  <div className="portfolio-card portfolio-card-proposer-diagnostics">
                    <h3 className="portfolio-card-title">Proposer Diagnostics</h3>
                    <p className="proposer-diagnostics-subtitle">
                      Why proposals may be zero or low
                    </p>
                    
                    {/* Current state summary */}
                    <div className="proposer-diagnostics-summary">
                      <div className="proposer-stat">
                        <span className="proposer-stat-label">Raw Signals (latest bar)</span>
                        <span className={`proposer-stat-value ${proposerDiagnostics.current_state?.candidate_count_raw > 0 ? 'positive' : 'zero'}`}>
                          {proposerDiagnostics.current_state?.candidate_count_raw ?? 0}
                        </span>
                      </div>
                      <div className="proposer-stat">
                        <span className="proposer-stat-label">Trusted Signals</span>
                        <span className={`proposer-stat-value ${proposerDiagnostics.current_state?.candidate_count_trusted > 0 ? 'positive' : 'zero'}`}>
                          {proposerDiagnostics.current_state?.candidate_count_trusted ?? 0}
                        </span>
                      </div>
                      <div className="proposer-stat">
                        <span className="proposer-stat-label">Trusted Patterns</span>
                        <span className={`proposer-stat-value ${proposerDiagnostics.current_state?.trusted_pattern_count > 0 ? 'positive' : 'zero'}`}>
                          {proposerDiagnostics.current_state?.trusted_pattern_count ?? 0}
                        </span>
                      </div>
                      <div className="proposer-stat">
                        <span className="proposer-stat-label">Rec TS = Bar TS?</span>
                        <span className={`proposer-stat-value ${proposerDiagnostics.current_state?.rec_ts_matches_bar_ts ? 'positive' : 'warning'}`}>
                          {proposerDiagnostics.current_state?.rec_ts_matches_bar_ts ? 'Yes' : 'No'}
                        </span>
                      </div>
                    </div>
                    
                    {/* Latest proposer event */}
                    {proposerDiagnostics.proposer_events?.length > 0 && (() => {
                      const latest = proposerDiagnostics.proposer_events[0]
                      return (
                        <div className="proposer-latest-event">
                          <h4>Latest Proposer Run</h4>
                          <dl className="proposer-event-details">
                            <dt>Status</dt>
                            <dd className={latest.status === 'SUCCESS' ? 'positive' : 'neutral'}>{latest.status}</dd>
                            <dt>Raw candidates</dt>
                            <dd>{latest.candidate_count_raw ?? 0}</dd>
                            <dt>Trusted candidates</dt>
                            <dd>{latest.candidate_count_trusted ?? 0}</dd>
                            <dt>Proposals inserted</dt>
                            <dd>{latest.rows_affected ?? 0}</dd>
                            {latest.no_candidates_reason && (
                              <>
                                <dt>Reason for zero</dt>
                                <dd className="reason-code">{latest.no_candidates_reason.replace(/_/g, ' ')}</dd>
                              </>
                            )}
                            {latest.entries_blocked && (
                              <>
                                <dt>Entries blocked</dt>
                                <dd className="warning">{latest.stop_reason || 'Yes'}</dd>
                              </>
                            )}
                          </dl>
                          {latest.diagnostics && (
                            <div className="proposer-diagnostics-extra">
                              <span className="extra-label">Bar TS:</span> {latest.diagnostics.latest_bar_ts?.slice(0, 10) ?? '—'} | 
                              <span className="extra-label"> Rec TS:</span> {latest.diagnostics.latest_rec_ts?.slice(0, 10) ?? '—'}
                            </div>
                          )}
                        </div>
                      )
                    })()}
                    
                    {/* Recommendation freshness by market type */}
                    {proposerDiagnostics.rec_freshness_by_market_type?.length > 0 && (
                      <div className="proposer-rec-freshness">
                        <h4>Recommendation Freshness by Market Type</h4>
                        <table className="proposer-freshness-table">
                          <thead>
                            <tr>
                              <th>Market Type</th>
                              <th>Count</th>
                              <th>Latest TS</th>
                            </tr>
                          </thead>
                          <tbody>
                            {proposerDiagnostics.rec_freshness_by_market_type.map((r, i) => (
                              <tr key={i}>
                                <td>{r.MARKET_TYPE || r.market_type}</td>
                                <td>{r.CNT || r.cnt}</td>
                                <td>{(r.MAX_TS || r.max_ts)?.slice?.(0, 10) || r.MAX_TS || r.max_ts}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                    
                    <p className="proposer-diagnostics-hint">
                      <Link to="/market-timeline">View full Market Timeline →</Link>
                    </p>
                  </div>
                )}
              </section>

              {/* Cumulative Performance: paid out + realized P&L across episodes */}
              {(timeline && (timeline.per_episode?.length > 0 || (timeline.cumulative_series?.length > 0))) && (
                <section className="portfolio-cumulative-performance" aria-label="Cumulative performance">
                  <h2>Cumulative Performance</h2>
                  <p className="cumulative-intro">Your investment journey across all episodes. Click a bar to jump to that episode.</p>
                  
                  {/* KPI tiles row */}
                  <div className="cumulative-kpi-row">
                    <div className="cumulative-kpi-tile cumulative-kpi-tile--paidout">
                      <span className="cumulative-kpi-icon">💰</span>
                      <span className="cumulative-kpi-label">Total Paid Out</span>
                      <span className="cumulative-kpi-value">
                        €{Number(timeline.total_paid_out_amount ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </span>
                      <span className="cumulative-kpi-hint">Profits withdrawn at episode ends</span>
                    </div>
                    <div className="cumulative-kpi-tile cumulative-kpi-tile--pnl">
                      <span className="cumulative-kpi-icon">📈</span>
                      <span className="cumulative-kpi-label">Total Realized P&L</span>
                      <span className={`cumulative-kpi-value ${(timeline.total_realized_pnl ?? 0) >= 0 ? 'positive' : 'negative'}`}>
                        €{Number(timeline.total_realized_pnl ?? 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </span>
                      <span className="cumulative-kpi-hint">Cumulative profit/loss across episodes</span>
                    </div>
                    <div className="cumulative-kpi-tile cumulative-kpi-tile--episodes">
                      <span className="cumulative-kpi-icon">🔄</span>
                      <span className="cumulative-kpi-label">Episodes</span>
                      <span className="cumulative-kpi-value">{timeline.episode_count ?? timeline.per_episode?.length ?? 0}</span>
                      <span className="cumulative-kpi-hint">Profile generations</span>
                    </div>
                  </div>

                  {timeline.cumulative_series?.length > 0 && (
                    <div className="portfolio-cumulative-chart">
                      <h4 className="chart-section-title">Cumulative Growth Over Time</h4>
                      <ResponsiveContainer width="100%" height={220}>
                        <LineChart
                          data={timeline.cumulative_series.map((p) => ({
                            ts: p.ts,
                            paid_out: p.cum_distributed_amount,
                            realized_pnl: p.cum_realized_pnl,
                          }))}
                          margin={{ top: 8, right: 8, left: 0, bottom: 4 }}
                        >
                          <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                          <XAxis dataKey="ts" tick={{ fontSize: 10 }} tickFormatter={(v) => String(v).slice(0, 10)} />
                          <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => (Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 }))} width={48} />
                          <Tooltip
                            formatter={(v, name) => [`€${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2 })}`, name]}
                            labelFormatter={(v) => String(v).slice(0, 10)}
                          />
                          <ReferenceLine y={0} stroke="#666" strokeDasharray="2 2" />
                          <Line type="monotone" dataKey="paid_out" stroke="#2e7d32" strokeWidth={2} dot={false} name="Paid out" />
                          <Line type="monotone" dataKey="realized_pnl" stroke="#1565c0" strokeWidth={2} dot={false} name="Realized P&L" />
                        </LineChart>
                      </ResponsiveContainer>
                      <p className="portfolio-cumulative-chart-legend">
                        <span className="legend-dot" style={{ background: '#2e7d32' }} /> Paid out
                        <span className="legend-dot" style={{ background: '#1565c0' }} /> Realized P&L
                      </p>
                    </div>
                  )}
                  {timeline.per_episode?.length > 0 && (
                    <div className="portfolio-cumulative-bars">
                      <h4 className="chart-section-title">P&L by Episode</h4>
                      <ResponsiveContainer width="100%" height={140}>
                        <BarChart
                          data={timeline.per_episode.map((ep) => ({
                            episode_id: ep.episode_id,
                            name: `Ep. ${ep.episode_id}`,
                            pnl: ep.realized_pnl ?? 0,
                            lifecycle: ep.lifecycle_label ?? (ep.status === 'ACTIVE' ? 'Active' : 'Ended'),
                            status: ep.status,
                          }))}
                          margin={{ top: 4, right: 8, left: 0, bottom: 4 }}
                        >
                          <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                          <XAxis dataKey="name" tick={{ fontSize: 9 }} />
                          <YAxis tick={{ fontSize: 10 }} width={48} tickFormatter={(v) => Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 })} />
                          <Tooltip
                            formatter={(v, name, props) => [`€${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2 })}`, props.payload.lifecycle]}
                            labelFormatter={(v) => v}
                          />
                          <ReferenceLine y={0} stroke="#666" />
                          <Bar
                            dataKey="pnl"
                            name="P&L"
                            onClick={(data) => {
                              const epId = data?.episode_id
                              if (epId) {
                                const el = document.getElementById(`episode-card-${epId}`)
                                if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' })
                              }
                            }}
                            cursor="pointer"
                          >
                            {timeline.per_episode.map((ep) => {
                              const pnl = ep.realized_pnl ?? 0
                              const isActive = ep.status === 'ACTIVE'
                              let fill = pnl >= 0 ? '#2e7d32' : '#c62828'
                              if (isActive) fill = '#1565c0'
                              return <Cell key={ep.episode_id} fill={fill} />
                            })}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                      <p className="bar-chart-hint">Click a bar to scroll to that episode card below.</p>
                    </div>
                  )}
                </section>
              )}

              {episodes.length > 0 && (
                <section className="episode-list-section" aria-label="Episode timeline">
                  <h2>Episodes</h2>
                  <p className="portfolio-overview-intro">Profile generations and resets. Expand a card to see equity, drawdown, trades per day, and risk regime.</p>
                  <div className="episode-list">
                    {episodes.map((ep) => {
                      const epId = ep.episode_id ?? ep.EPISODE_ID
                      return (
                        <div key={epId} id={`episode-card-${epId}`}>
                          <EpisodeCard
                            episode={ep}
                            portfolioId={Number(portfolioId)}
                            isActive={ep.status === 'ACTIVE' || ep.STATUS === 'ACTIVE'}
                          />
                        </div>
                      )
                    })}
                  </div>
                </section>
              )}
            </>
          )
        })()}
      </>
    )
  }

  if (portfolios.length === 0) {
    return (
      <>
        <h1>Portfolios</h1>
        <EmptyState
          title="No portfolios yet"
          action={<>Run pipeline, then <Link to="/runs">check Runs</Link>.</>}
          explanation="Portfolios are created by the pipeline. Run the daily pipeline, then check Audit Viewer for runs."
          reasons={['Pipeline has not run yet.', 'No portfolios have been created in MIP.APP.PORTFOLIO.']}
        />
      </>
    )
  }

  return (
    <>
      <h1>Portfolio Control Tower</h1>
      <p className="control-tower-intro">
        Monitor all portfolios at a glance. <strong>Gate</strong> = risk regime (entries allowed?). <strong>Health</strong> = data freshness.
      </p>
      <div className="control-tower-table-wrap">
        <table className="control-tower-table">
          <thead>
            <tr>
              <th>Name</th>
              <th className="col-id">ID</th>
              <th className="col-gate" title="Risk regime: SAFE = entries allowed, CAUTION = approaching threshold, STOPPED = entries blocked">Gate</th>
              <th className="col-health" title="Data freshness: OK = recent run, STALE = older than 24h, BROKEN = very old or failed">Health</th>
              <th className="col-equity" title="Latest total equity from last simulation">Equity</th>
              <th className="col-paidout" title="Cumulative profits withdrawn across all episodes">Paid Out</th>
              <th className="col-episode">Active Episode</th>
              <th className="col-status">Status</th>
            </tr>
          </thead>
          <tbody>
            {portfolios.map((p) => {
              const gateState = (p.GATE_STATE ?? p.gate_state ?? 'SAFE').toUpperCase()
              const gateTooltip = p.gate_tooltip ?? (gateState === 'STOPPED' ? 'Entries blocked' : gateState === 'CAUTION' ? 'Approaching threshold' : 'Entries allowed')
              const healthState = (p.health_state ?? 'OK').toUpperCase()
              const healthReason = p.health_reason ?? ''
              const ep = p.active_episode
              const episodeLabel = ep
                ? `#${ep.episode_id ?? '—'} since ${(ep.start_ts ?? '').toString().slice(0, 10)}`
                : '—'
              const equity = p.latest_equity ?? p.FINAL_EQUITY ?? p.final_equity ?? 0
              const paidOut = p.total_paid_out ?? p.TOTAL_PAID_OUT ?? 0
              const status = p.STATUS ?? p.status ?? 'ACTIVE'
              
              return (
                <tr
                  key={p.PORTFOLIO_ID ?? p.portfolio_id}
                  className={`control-tower-row control-tower-row--gate-${gateState.toLowerCase()}`}
                >
                  <td className="col-name">
                    <Link to={`/portfolios/${p.PORTFOLIO_ID ?? p.portfolio_id}`}>{p.NAME ?? p.name}</Link>
                  </td>
                  <td className="col-id">{p.PORTFOLIO_ID ?? p.portfolio_id}</td>
                  <td className="col-gate">
                    <span
                      className={`traffic-light traffic-light--${gateState.toLowerCase()}`}
                      title={gateTooltip}
                    />
                    <span className="traffic-label">{gateState}</span>
                  </td>
                  <td className="col-health">
                    <span
                      className={`traffic-light traffic-light--${healthState.toLowerCase()}`}
                      title={healthReason}
                    />
                    <span className="traffic-label">{healthState}</span>
                  </td>
                  <td className="col-equity">
                    €{Number(equity).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}
                  </td>
                  <td className="col-paidout">
                    {paidOut > 0 ? (
                      <span className="paidout-value">€{Number(paidOut).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}</span>
                    ) : (
                      <span className="paidout-zero">—</span>
                    )}
                  </td>
                  <td className="col-episode">{episodeLabel}</td>
                  <td className="col-status">
                    <span className={`status-chip status-chip--${status.toLowerCase()}`}>{status}</span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </>
  )
}
