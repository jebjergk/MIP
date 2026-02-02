import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { API_BASE } from '../App'
import InfoTooltip from '../components/InfoTooltip'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import { useExplainMode } from '../context/ExplainModeContext'
import { useExplainCenter, useExplainSection } from '../context/ExplainCenterContext'
import { getGlossaryEntry, getGlossaryEntryByDotKey } from '../data/glossary'
import { PORTFOLIO_EXPLAIN_CONTEXT, RISK_GATE_EXPLAIN_CONTEXT } from '../data/explainContexts'
import './Portfolio.css'

export default function Portfolio() {
  const { portfolioId } = useParams()
  const { explainMode } = useExplainMode()
  const statusBadgeTitle = explainMode ? getGlossaryEntryByDotKey('ui.status_badge')?.long : undefined
  const [portfolios, setPortfolios] = useState([])
  const [portfolio, setPortfolio] = useState(null)
  const [snapshot, setSnapshot] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [lookbackDays, setLookbackDays] = useState(30)
  const [showRestartModal, setShowRestartModal] = useState(false)
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
          if (!cancelled) setSnapshot(await snapRes.json())
        } else {
          setPortfolio(null)
          setSnapshot(null)
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
    return (
      <>
        <h1>Portfolio: {portfolio.NAME}</h1>
        <p><Link to="/portfolios">← Back to list</Link></p>
        <h2>Header</h2>
        <div className="kpi-cards">
          {portfolio.STARTING_CASH != null && (
            <div className="kpi-card">
              <span className="kpi-label">Starting cash <InfoTooltip scope="portfolio" key="starting_cash" variant="short" /></span>
              <span className="kpi-value">{Number(portfolio.STARTING_CASH).toLocaleString()}</span>
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
          const tradesList = Array.isArray(snapshot.trades) ? snapshot.trades : (cards.recent_trades ?? [])
          const tradesTotal = snapshot.trades_total ?? cards.trades_total ?? 0
          const lastTradeTs = snapshot.last_trade_ts ?? cards.last_trade_ts ?? null
          const riskGate = snapshot.risk_gate || {}
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

                {/* Open Positions */}
                <div id="portfolio-positions" className="portfolio-card portfolio-card-positions">
                  <h3 className="portfolio-card-title">Open Positions <InfoTooltip scope="positions" entryKey="symbol" variant="short" /></h3>
                  {Array.isArray(openPositions) && openPositions.length > 0 ? (
                    <div className="portfolio-card-table-wrap">
                      <table className="portfolio-card-table">
                        <thead>
                          <tr>
                            <th>Symbol <InfoTooltip scope="positions" entryKey="symbol" variant="short" /></th>
                            <th>Side <InfoTooltip scope="positions" entryKey="side" variant="short" /></th>
                            <th>Quantity <InfoTooltip scope="positions" entryKey="quantity" variant="short" /></th>
                            <th>Cost basis <InfoTooltip scope="positions" entryKey="cost_basis" variant="short" /></th>
                          </tr>
                        </thead>
                        <tbody>
                          {openPositions.slice(0, 20).map((pos, i) => (
                            <tr key={i}>
                              <td>{pos.SYMBOL ?? pos.symbol}</td>
                              <td>{pos.side_label ?? pos.side ?? '—'}</td>
                              <td>{pos.QUANTITY ?? pos.quantity}</td>
                              <td>{pos.COST_BASIS != null ? Number(pos.COST_BASIS ?? pos.cost_basis).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</td>
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
                          {tradesList.slice(0, 20).map((t, i) => (
                            <tr key={i}>
                              <td>{t.SYMBOL ?? t.symbol}</td>
                              <td>{t.SIDE ?? t.side}</td>
                              <td>{t.QUANTITY ?? t.quantity}</td>
                              <td>{t.PRICE != null ? Number(t.PRICE ?? t.price).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</td>
                              <td>{t.NOTIONAL != null ? Number(t.NOTIONAL ?? t.notional).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</td>
                            </tr>
                          ))}
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
                    <span className={riskLabel === 'NORMAL' ? 'risk-gate-mode risk-gate-mode--current' : 'risk-gate-mode'}>✅ Normal</span>
                    {' / '}
                    <span className={riskLabel === 'CAUTION' ? 'risk-gate-mode risk-gate-mode--current' : 'risk-gate-mode'}>⚠️ Caution</span>
                    {' / '}
                    <span className={riskLabel === 'DEFENSIVE' ? 'risk-gate-mode risk-gate-mode--current' : 'risk-gate-mode'}>🛑 Defensive</span>
                    {explainMode && <InfoTooltip scope="risk_gate" entryKey="mode" variant="short" />}
                  </h3>
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
              </section>
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
      <h1>Portfolios</h1>
      <table className="runs-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>ID</th>
            <th>Status <InfoTooltip scope="portfolio" key="status" variant="short" /></th>
          </tr>
        </thead>
        <tbody>
          {portfolios.map((p) => (
            <tr key={p.PORTFOLIO_ID}>
              <td><Link to={`/portfolios/${p.PORTFOLIO_ID}`}>{p.NAME}</Link></td>
              <td>{p.PORTFOLIO_ID}</td>
              <td><span className="status-badge" title={statusBadgeTitle}>{p.STATUS}</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}
