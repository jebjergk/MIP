import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { API_BASE } from '../App'
import InfoTooltip from '../components/InfoTooltip'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import { useExplainMode } from '../context/ExplainModeContext'
import { getGlossaryEntry, getGlossaryEntryByDotKey } from '../data/glossary'

export default function Portfolio() {
  const { portfolioId } = useParams()
  const { explainMode } = useExplainMode()
  const statusBadgeTitle = explainMode ? getGlossaryEntryByDotKey('ui.status_badge')?.long : undefined
  const [portfolios, setPortfolios] = useState([])
  const [portfolio, setPortfolio] = useState(null)
  const [snapshot, setSnapshot] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

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
          const [headerRes, snapRes] = await Promise.all([
            fetch(`${API_BASE}/portfolios/${portfolioId}`),
            fetch(`${API_BASE}/portfolios/${portfolioId}/snapshot`),
          ])
          if (!headerRes.ok) throw new Error(headerRes.statusText)
          if (!snapRes.ok) throw new Error(snapRes.statusText)
          setPortfolio(await headerRes.json())
          setSnapshot(await snapRes.json())
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
  }, [portfolioId])

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
        {snapshot && (
          <>
            <h2>Snapshot</h2>
            <p>Positions: {snapshot.positions?.length ?? 0} · Trades: {snapshot.trades?.length ?? 0} · Daily: {snapshot.daily?.length ?? 0} · KPIs: {snapshot.kpis?.length ?? 0}</p>
            <details>
              <summary>Positions</summary>
              {Array.isArray(snapshot.positions) && snapshot.positions.length > 0 && (
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Symbol <InfoTooltip key="positions.symbol" variant="short" /></th>
                      <th>Quantity <InfoTooltip key="positions.quantity" variant="short" /></th>
                      <th>Cost basis <InfoTooltip key="positions.cost_basis" variant="short" /></th>
                    </tr>
                  </thead>
                  <tbody>
                    {snapshot.positions.slice(0, 20).map((pos, i) => (
                      <tr key={i}>
                        <td>{pos.SYMBOL ?? pos.symbol}</td>
                        <td>{pos.QUANTITY ?? pos.quantity}</td>
                        <td>{pos.COST_BASIS ?? pos.cost_basis}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              {(!snapshot.positions || snapshot.positions.length === 0) && (
                <EmptyState
                  title="No positions"
                  action="Run pipeline or wait for next run."
                  explanation="This portfolio has no open positions right now. Positions are the assets (e.g. stocks) the strategy currently holds."
                  reasons={['The run has not opened any positions yet.', 'Data may be from a time before any trades.', 'The risk gate may be blocking new entries.']}
                />
              )}
            </details>
            <details>
              <summary>Trades</summary>
              {Array.isArray(snapshot.trades) && snapshot.trades.length > 0 && (
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Symbol <InfoTooltip key="trades.symbol" variant="short" /></th>
                      <th>Side <InfoTooltip key="trades.side" variant="short" /></th>
                      <th>Quantity <InfoTooltip key="trades.quantity" variant="short" /></th>
                      <th>Price <InfoTooltip key="trades.price" variant="short" /></th>
                      <th>Notional <InfoTooltip key="trades.notional" variant="short" /></th>
                    </tr>
                  </thead>
                  <tbody>
                    {snapshot.trades.slice(0, 10).map((t, i) => (
                      <tr key={i}>
                        <td>{t.SYMBOL ?? t.symbol}</td>
                        <td>{t.SIDE ?? t.side}</td>
                        <td>{t.QUANTITY ?? t.quantity}</td>
                        <td>{t.PRICE ?? t.price}</td>
                        <td>{t.NOTIONAL ?? t.notional}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              {(!snapshot.trades || snapshot.trades.length === 0) && (
                <EmptyState
                  title="No trades"
                  action="Run pipeline or wait for next run."
                  explanation="No trades are shown for this snapshot. Trades are the buy/sell actions the strategy has executed."
                  reasons={['The run may not have executed any trades yet.', 'Data may be from before the first trade.', 'Score or threshold filters may have excluded all signals.', 'The risk gate may be blocking new entries.']}
                />
              )}
            </details>
            <details>
              <summary>Risk gate <InfoTooltip scope="risk_gate" key="entries_blocked" variant="short" /></summary>
              {snapshot.risk_gate && (
                <div className="risk-gate-summary">
                  <p><strong>Entries blocked</strong> <InfoTooltip scope="risk_gate" key="entries_blocked" variant="short" />: {String(snapshot.risk_gate.ENTRIES_BLOCKED ?? snapshot.risk_gate.entries_blocked ?? '—')}</p>
                  <p><strong>Stop reason</strong> <InfoTooltip scope="risk_gate" key="stop_reason" variant="short" />: {String(snapshot.risk_gate.STOP_REASON ?? snapshot.risk_gate.stop_reason ?? '—')}</p>
                </div>
              )}
              <pre>{JSON.stringify(snapshot.risk_gate, null, 2)}</pre>
            </details>
          </>
        )}
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
