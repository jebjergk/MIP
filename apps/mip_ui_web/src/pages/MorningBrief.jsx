import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import { useExplainCenter } from '../context/ExplainCenterContext'
import { usePortfolios } from '../context/PortfolioContext'
import { MORNING_BRIEF_EXPLAIN_CONTEXT } from '../data/explainContexts'
import './MorningBrief.css'

// Status badge component
function StatusBadge({ status }) {
  const statusConfig = {
    SAFE: { className: 'status-safe', icon: '✓', label: 'Safe' },
    CAUTION: { className: 'status-caution', icon: '⚠', label: 'Caution' },
    STOPPED: { className: 'status-stopped', icon: '⛔', label: 'Stopped' },
  }
  const config = statusConfig[status] || statusConfig.SAFE
  return (
    <span className={`status-badge ${config.className}`}>
      <span className="status-icon">{config.icon}</span>
      {config.label}
    </span>
  )
}

// Delta indicator component
function DeltaIndicator({ value, format = 'number', invert = false }) {
  if (value == null) return <span className="delta-indicator neutral">—</span>
  const isPositive = invert ? value < 0 : value > 0
  const isNegative = invert ? value > 0 : value < 0
  const className = isPositive ? 'positive' : isNegative ? 'negative' : 'neutral'
  const arrow = isPositive ? '↑' : isNegative ? '↓' : ''

  let displayValue
  if (format === 'percent') {
    displayValue = `${(value * 100).toFixed(2)}%`
  } else if (format === 'currency') {
    displayValue = `$${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
  } else {
    displayValue = typeof value === 'number' ? value.toLocaleString() : value
  }

  return (
    <span className={`delta-indicator ${className}`}>
      {arrow} {displayValue}
    </span>
  )
}

// Executed Trades Modal
function ExecutedTradesModal({ trades, source, portfolioId, onClose }) {
  useEffect(() => {
    const handleEsc = (e) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', handleEsc)
    return () => document.removeEventListener('keydown', handleEsc)
  }, [onClose])

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal-content executed-trades-modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>Executed Trades ({trades.length})</h2>
          <button type="button" className="modal-close" onClick={onClose} aria-label="Close">×</button>
        </div>
        <p className="modal-source">Source: {source}</p>
        <div className="modal-body">
          {trades.length === 0 ? (
            <p className="no-trades">No executed trades in this brief.</p>
          ) : (
            <table className="trades-table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Symbol</th>
                  <th>Side</th>
                  <th>Qty</th>
                  <th>Price</th>
                  <th>Notional</th>
                  <th>Score</th>
                </tr>
              </thead>
              <tbody>
                {trades.map((trade, idx) => (
                  <tr key={trade.trade_id || idx}>
                    <td>{trade.trade_ts ? new Date(trade.trade_ts).toLocaleString() : '—'}</td>
                    <td className="trade-symbol">{trade.symbol}</td>
                    <td className={`trade-side ${(trade.side || '').toLowerCase()}`}>{trade.side}</td>
                    <td>{trade.quantity != null ? trade.quantity.toLocaleString() : '—'}</td>
                    <td>{trade.price != null ? `$${trade.price.toLocaleString()}` : '—'}</td>
                    <td>{trade.notional != null ? `$${trade.notional.toLocaleString()}` : '—'}</td>
                    <td>{trade.score != null ? trade.score.toFixed(2) : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        <div className="modal-footer">
          <Link 
            to={`/portfolios/${portfolioId}`} 
            className="view-all-link"
            onClick={onClose}
          >
            View all trades in Portfolio →
          </Link>
        </div>
      </div>
    </div>
  )
}

// Stale badge component
function StaleBadge({ reason }) {
  return (
    <span className="stale-badge" title={reason || 'Brief is from an older pipeline run'}>
      STALE
    </span>
  )
}

// Current badge component  
function CurrentBadge() {
  return (
    <span className="current-badge" title="Brief is from the latest pipeline run">
      CURRENT
    </span>
  )
}

// Reset warning banner
function ResetWarningBanner({ warning }) {
  return (
    <div className="reset-warning-banner" role="alert">
      <span className="warning-icon">⚠️</span>
      <span className="warning-text">{warning}</span>
    </div>
  )
}

// Executive Summary Section
function ExecutiveSummary({ brief, onShowTrades }) {
  const { summary, as_of_ts, created_at, pipeline_run_id, is_stale, stale_reason, is_before_reset, reset_warning } = brief
  const hasExecutedTrades = summary.executed_count > 0
  const executedTradesNote = summary.executed_trades_note

  return (
    <section className="brief-card executive-summary" aria-label="Executive Summary">
      <div className="summary-title-row">
        <h2>Executive Summary</h2>
        {is_stale ? <StaleBadge reason={stale_reason} /> : <CurrentBadge />}
      </div>
      
      {is_before_reset && reset_warning && (
        <ResetWarningBanner warning={reset_warning} />
      )}

      <div className="summary-header">
        <StatusBadge status={summary.status} />
        <span className="entries-badge">
          Entries: {summary.entries_allowed ? (
            <span className="entries-yes">Allowed</span>
          ) : (
            <span className="entries-no">Blocked</span>
          )}
        </span>
        <span className="suggestions-count">
          {summary.new_suggestions_today} suggestion{summary.new_suggestions_today !== 1 ? 's' : ''} today
        </span>
      </div>
      <p className="summary-explanation">{summary.explanation}</p>
      <div className="summary-meta">
        <span className="meta-item">
          <strong>As of (market date):</strong> {as_of_ts ? new Date(as_of_ts).toLocaleString() : '—'}
        </span>
        <span className="meta-item">
          <strong>Generated at:</strong> {created_at ? new Date(created_at).toLocaleString() : '—'}
        </span>
        <span className="meta-item">
          <strong>Run ID:</strong> <code>{pipeline_run_id ? (pipeline_run_id.length > 12 ? `${pipeline_run_id.slice(0, 8)}...` : pipeline_run_id) : '—'}</code>
        </span>
        {hasExecutedTrades ? (
          <button 
            type="button" 
            className="meta-item executed-link"
            onClick={onShowTrades}
            title={executedTradesNote || "Click to see executed trades"}
          >
            <strong>Executed:</strong> {summary.executed_count} trade{summary.executed_count !== 1 ? 's' : ''}
            {executedTradesNote && <span className="executed-note"> ({executedTradesNote})</span>}
          </button>
        ) : (
          <span className="meta-item">
            <strong>Executed:</strong> {executedTradesNote ? `— (${executedTradesNote})` : '0 trades'}
          </span>
        )}
      </div>
    </section>
  )
}

// Build URL for Signals Explorer page with filters
function buildSignalsUrl(opportunity, portfolioId, asOfTs, pipelineRunId) {
  const params = new URLSearchParams()
  if (portfolioId) params.set('portfolioId', String(portfolioId))
  if (asOfTs) params.set('asOf', asOfTs)
  if (pipelineRunId) params.set('pipelineRunId', pipelineRunId)
  if (opportunity.market_type) params.set('market_type', opportunity.market_type)
  if (opportunity.symbol && opportunity.symbol !== '—') params.set('symbol', opportunity.symbol)
  if (opportunity.pattern_id) params.set('pattern_id', String(opportunity.pattern_id))
  if (opportunity.horizon_bars) params.set('horizon_bars', String(opportunity.horizon_bars))
  if (opportunity.trust_label) params.set('trust_label', opportunity.trust_label)
  params.set('from', 'brief')
  const query = params.toString()
  return query ? `/signals?${query}` : '/signals'
}

// Opportunity Card component
function OpportunityCard({ opportunity, portfolioId, asOfTs, pipelineRunId }) {
  const { symbol, side, market_type, horizon_bars, interval_minutes, confidence, why, pattern_id } = opportunity
  const signalsUrl = buildSignalsUrl(opportunity, portfolioId, asOfTs, pipelineRunId)
  const displaySymbol = symbol && symbol !== '—' ? symbol : `Pattern ${pattern_id}`
  
  return (
    <div className="opportunity-card">
      <div className="opp-header">
        <span className="opp-symbol-title">{displaySymbol} — {side}</span>
        <span className={`opp-confidence confidence-${confidence.toLowerCase()}`}>{confidence}</span>
      </div>
      <div className="opp-details">
        <span className="opp-market">{market_type}</span>
        {pattern_id && <span className="opp-pattern">Pattern {pattern_id}</span>}
        {horizon_bars && <span className="opp-horizon">{horizon_bars} bar{horizon_bars !== 1 ? 's' : ''} horizon</span>}
        {interval_minutes && <span className="opp-interval">{interval_minutes}min</span>}
      </div>
      <p className="opp-why">{why}</p>
      <div className="opp-actions">
        <Link to={signalsUrl} className="opp-link">
          View in Signals →
        </Link>
      </div>
    </div>
  )
}

// Opportunities Section
function OpportunitiesSection({ opportunities, portfolioId, asOfTs, pipelineRunId }) {
  if (!opportunities || opportunities.length === 0) {
    return (
      <section className="brief-card opportunities-section" aria-label="Opportunities">
        <h2>Opportunities</h2>
        <div className="empty-opportunities">
          <p className="empty-title">No suggestions today</p>
          <p className="empty-explanation">
            Likely reasons: no new bars, no eligible signals, or risk gate blocked entries.
          </p>
        </div>
      </section>
    )
  }

  return (
    <section className="brief-card opportunities-section" aria-label="Opportunities">
      <h2>Opportunities ({opportunities.length})</h2>
      <div className="opportunities-grid">
        {opportunities.map((opp, idx) => (
          <OpportunityCard 
            key={opp.pattern_id || idx} 
            opportunity={opp} 
            portfolioId={portfolioId}
            asOfTs={asOfTs}
            pipelineRunId={pipelineRunId}
          />
        ))}
      </div>
    </section>
  )
}

// Risk & Guardrails Section
function RiskSection({ risk }) {
  const { current_state, thresholds, actions } = risk
  const profileMissing = !thresholds.profile_name
  return (
    <section className="brief-card risk-section" aria-label="Risk & Guardrails">
      <h2>Risk &amp; Guardrails</h2>
      <div className="risk-grid">
        <div className="risk-current">
          <h3>Current State</h3>
          <dl className="risk-dl">
            <dt>Risk Status</dt>
            <dd>
              <span className={`risk-status-indicator ${current_state.risk_status?.toLowerCase() || 'ok'}`}>
                {current_state.risk_status || 'OK'}
              </span>
            </dd>
            <dt>Max Drawdown</dt>
            <dd>{current_state.max_drawdown_pct}</dd>
            <dt>Open Positions</dt>
            <dd>{current_state.open_positions ?? '—'}</dd>
            <dt>Entries Blocked</dt>
            <dd>{current_state.entries_blocked ? 'Yes' : 'No'}</dd>
          </dl>
        </div>
        <div className={`risk-thresholds ${profileMissing ? 'profile-missing' : ''}`}>
          <h3>
            Profile Thresholds
            {thresholds.profile_name && (
              <span className="profile-name-badge">{thresholds.profile_name}</span>
            )}
          </h3>
          {profileMissing && (
            <p className="profile-warning">Profile not configured for this portfolio.</p>
          )}
          <dl className="risk-dl">
            <dt>Drawdown Stop</dt>
            <dd>{thresholds.drawdown_stop_label || '—'}</dd>
            <dt>Max Positions</dt>
            <dd>{thresholds.max_positions ?? '—'}</dd>
            <dt>Max Position Size</dt>
            <dd>{thresholds.max_position_pct_label || '—'}</dd>
            <dt>Bust Threshold</dt>
            <dd>{thresholds.bust_equity_pct_label || '—'}</dd>
          </dl>
        </div>
      </div>
      <div className="risk-actions">
        <h3>What to do now</h3>
        <ul className="action-list">
          {actions.map((action, idx) => (
            <li key={idx}>{action}</li>
          ))}
        </ul>
      </div>
    </section>
  )
}

// Deltas Section
function DeltasSection({ deltas }) {
  if (!deltas.has_prior_brief) {
    return (
      <section className="brief-card deltas-section" aria-label="Changes Since Last Brief">
        <h2>Changes Since Last Brief</h2>
        <div className="no-prior-brief">
          <p>No prior brief to compare.</p>
        </div>
      </section>
    )
  }

  return (
    <section className="brief-card deltas-section" aria-label="Changes Since Last Brief">
      <h2>Changes Since Last Brief</h2>
      {deltas.prior_as_of_ts && (
        <p className="prior-timestamp">
          Compared to: {new Date(deltas.prior_as_of_ts).toLocaleString()}
        </p>
      )}
      <div className="deltas-grid">
        <div className="delta-item">
          <span className="delta-label">Total Equity</span>
          <span className="delta-curr">
            {deltas.equity?.curr != null ? `$${deltas.equity.curr.toLocaleString()}` : '—'}
          </span>
          <DeltaIndicator value={deltas.equity?.delta} format="currency" />
        </div>
        <div className="delta-item">
          <span className="delta-label">Total Return</span>
          <span className="delta-curr">
            {deltas.total_return?.curr != null ? `${(deltas.total_return.curr * 100).toFixed(2)}%` : '—'}
          </span>
          <DeltaIndicator value={deltas.total_return?.delta} format="percent" />
        </div>
        <div className="delta-item">
          <span className="delta-label">Max Drawdown</span>
          <span className="delta-curr">
            {deltas.max_drawdown?.curr != null ? `${(deltas.max_drawdown.curr * 100).toFixed(2)}%` : '—'}
          </span>
          <DeltaIndicator value={deltas.max_drawdown?.delta} format="percent" invert />
        </div>
        <div className="delta-item">
          <span className="delta-label">Open Positions</span>
          <span className="delta-curr">{deltas.open_positions?.curr ?? '—'}</span>
          <DeltaIndicator value={deltas.open_positions?.delta} />
        </div>
      </div>
      {(deltas.trusted_signals?.added?.length > 0 || deltas.trusted_signals?.removed?.length > 0) && (
        <div className="signal-changes">
          <h3>Signal Changes</h3>
          {deltas.trusted_signals?.added?.length > 0 && (
            <div className="signals-added">
              <strong>Added ({deltas.trusted_signals.added.length}):</strong>
              <ul>
                {deltas.trusted_signals.added.slice(0, 5).map((sig, idx) => (
                  <li key={idx}>{sig.pattern_id || JSON.stringify(sig)}</li>
                ))}
                {deltas.trusted_signals.added.length > 5 && (
                  <li className="more">+{deltas.trusted_signals.added.length - 5} more</li>
                )}
              </ul>
            </div>
          )}
          {deltas.trusted_signals?.removed?.length > 0 && (
            <div className="signals-removed">
              <strong>Removed ({deltas.trusted_signals.removed.length}):</strong>
              <ul>
                {deltas.trusted_signals.removed.slice(0, 5).map((sig, idx) => (
                  <li key={idx}>{sig.pattern_id || JSON.stringify(sig)}</li>
                ))}
                {deltas.trusted_signals.removed.length > 5 && (
                  <li className="more">+{deltas.trusted_signals.removed.length - 5} more</li>
                )}
              </ul>
            </div>
          )}
        </div>
      )}
    </section>
  )
}

// Raw JSON Panel (collapsed by default)
function RawJsonPanel({ rawJson }) {
  return (
    <details className="raw-json-panel">
      <summary>Full Brief JSON (Debug)</summary>
      <pre className="raw-json-pre">{JSON.stringify(rawJson, null, 2)}</pre>
    </details>
  )
}

export default function MorningBrief() {
  const { portfolios, defaultPortfolioId, loading: portfoliosLoading, error: portfoliosError } = usePortfolios()
  const [portfolioId, setPortfolioId] = useState('')
  const [briefResponse, setBriefResponse] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [showTradesModal, setShowTradesModal] = useState(false)
  const { setContext } = useExplainCenter()

  useEffect(() => {
    setContext(MORNING_BRIEF_EXPLAIN_CONTEXT)
  }, [setContext])

  // Default selected portfolio to first active when list is ready
  useEffect(() => {
    if (portfoliosLoading || defaultPortfolioId == null || portfolioId !== '') return
    setPortfolioId(String(defaultPortfolioId))
  }, [defaultPortfolioId, portfoliosLoading, portfolioId])

  const loadBrief = async () => {
    const id = portfolioId.trim()
    if (!id) return
    setLoading(true)
    setError(null)
    setBriefResponse(null)
    try {
      const res = await fetch(`${API_BASE}/briefs/latest?portfolio_id=${id}`)
      if (!res.ok) throw new Error(res.statusText)
      const data = await res.json()
      setBriefResponse(data)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  const found = briefResponse?.found === true
  const brief = found ? briefResponse : null

  if (portfoliosLoading) {
    return (
      <>
        <h1>Morning Brief</h1>
        <LoadingState />
      </>
    )
  }
  if ((error || portfoliosError) && !portfolioId) {
    return (
      <>
        <h1>Morning Brief</h1>
        <ErrorState message={error || portfoliosError} />
      </>
    )
  }

  if (portfolios.length === 0 && !error) {
    return (
      <>
        <h1>Morning Brief</h1>
        <EmptyState
          title="No portfolios yet"
          action={<>Run pipeline, then <Link to="/portfolios">pick a portfolio</Link>.</>}
          explanation="Briefs are per portfolio. Load portfolios first by running the pipeline."
          reasons={['Pipeline has not run yet.', 'No portfolios in MIP.APP.PORTFOLIO.']}
        />
      </>
    )
  }

  return (
    <>
      <h1>Morning Brief</h1>
      <p className="page-description">
        Your daily portfolio summary with opportunities, risk status, and performance changes.
      </p>
      <div className="brief-controls">
        <label>
          Portfolio:{' '}
          <select
            value={portfolioId}
            onChange={(e) => setPortfolioId(e.target.value)}
          >
            <option value="">Select…</option>
            {portfolios.map((p) => {
              const id = p.PORTFOLIO_ID ?? p.portfolio_id
              const name = p.NAME ?? p.name
              return (
                <option key={id} value={id}>
                  {name != null ? `${name} (${id})` : id}
                </option>
              )
            })}
          </select>
        </label>
        <button type="button" onClick={loadBrief} disabled={!portfolioId || loading}>
          {loading ? 'Loading…' : 'Load Brief'}
        </button>
      </div>

      {error && <ErrorState message={error} />}

      {!portfolioId && !loading && (
        <EmptyState
          title="Select a portfolio"
          action="Choose a portfolio above and click Load Brief."
          explanation="Briefs are generated per portfolio by the daily pipeline."
        />
      )}

      {portfolioId && !loading && !error && briefResponse == null && (
        <EmptyState
          title="No brief loaded"
          action="Click Load Brief above to fetch the latest brief for this portfolio."
          explanation="Briefs are generated per portfolio by the daily pipeline."
        />
      )}

      {portfolioId && !loading && !error && briefResponse?.found === false && (
        <EmptyState
          title="No brief exists yet for this portfolio"
          action={<>Run the daily pipeline for this portfolio, then load again.</>}
          explanation={briefResponse?.message ?? 'Briefs are written when the pipeline runs and writes morning briefs for each portfolio.'}
          reasons={['Pipeline has not run yet for this portfolio.', 'No brief row in MIP.AGENT_OUT.MORNING_BRIEF.']}
        />
      )}

      {brief && (
        <div className="brief-content">
          <ExecutiveSummary 
            brief={brief} 
            onShowTrades={() => setShowTradesModal(true)}
          />
          <OpportunitiesSection 
            opportunities={brief.opportunities} 
            portfolioId={brief.portfolio_id}
            asOfTs={brief.as_of_ts}
            pipelineRunId={brief.pipeline_run_id}
          />
          <RiskSection risk={brief.risk} />
          <DeltasSection deltas={brief.deltas} />
          <RawJsonPanel rawJson={brief.raw_json} />
        </div>
      )}

      {showTradesModal && brief && (
        <ExecutedTradesModal
          trades={brief.summary.executed_trades_preview || []}
          source={brief.summary.executed_trades_source || 'Portfolio trades'}
          portfolioId={brief.portfolio_id}
          onClose={() => setShowTradesModal(false)}
        />
      )}
    </>
  )
}
