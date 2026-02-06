import { useState, useEffect, useCallback } from 'react'
import { Link, useSearchParams, useNavigate } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import { useExplainCenter } from '../context/ExplainCenterContext'
import { SIGNALS_EXPLAIN_CONTEXT } from '../data/explainContexts'
import './Signals.css'

// Filter badge component
function FilterBadge({ label, value, onClear }) {
  if (!value) return null
  return (
    <span className="filter-badge">
      <span className="filter-label">{label}:</span>
      <span className="filter-value">{value}</span>
      {onClear && (
        <button type="button" className="filter-clear" onClick={onClear} aria-label={`Clear ${label} filter`}>
          ×
        </button>
      )}
    </span>
  )
}

// Fallback banner component
function FallbackBanner({ reason, onClearFilters, onUseLatestRun, onBackToBrief }) {
  return (
    <div className="signals-fallback-banner" role="alert">
      <div className="fallback-icon">⚠️</div>
      <div className="fallback-content">
        <p className="fallback-reason">{reason}</p>
        <div className="fallback-actions">
          <button type="button" className="fallback-btn" onClick={onClearFilters}>
            Clear all filters
          </button>
          <button type="button" className="fallback-btn" onClick={onUseLatestRun}>
            Use latest run
          </button>
          <Link to="/brief" className="fallback-btn" onClick={onBackToBrief}>
            Back to Morning Brief
          </Link>
        </div>
      </div>
    </div>
  )
}

// From Brief banner component
function FromBriefBanner({ portfolioId, asOfTs, pipelineRunId, onClearFilters }) {
  return (
    <div className="signals-from-brief-banner" role="status">
      <span className="banner-icon">📋</span>
      <span className="banner-text">
        Filtered from Morning Brief
        {portfolioId && ` (Portfolio ${portfolioId})`}
        {asOfTs && ` • As of ${new Date(asOfTs).toLocaleDateString()}`}
      </span>
      <button type="button" className="banner-clear-btn" onClick={onClearFilters}>
        Clear filters
      </button>
    </div>
  )
}

// Signal row component
function SignalRow({ signal }) {
  const trustClass = (signal.TRUST_LABEL || signal.trust_label || '').toLowerCase()
  const isEligible = signal.IS_ELIGIBLE ?? signal.is_eligible
  
  return (
    <tr className={`signal-row ${isEligible ? '' : 'signal-ineligible'}`}>
      <td className="signal-symbol">{signal.SYMBOL || signal.symbol}</td>
      <td className="signal-market">{signal.MARKET_TYPE || signal.market_type}</td>
      <td className="signal-pattern">{signal.PATTERN_ID || signal.pattern_id}</td>
      <td className="signal-score">{(signal.SCORE || signal.score)?.toFixed(2) ?? '—'}</td>
      <td className={`signal-trust trust-${trustClass}`}>
        {signal.TRUST_LABEL || signal.trust_label || '—'}
      </td>
      <td className="signal-action">{signal.RECOMMENDED_ACTION || signal.recommended_action || '—'}</td>
      <td className="signal-eligible">
        {isEligible ? '✓' : (signal.GATING_REASON || signal.gating_reason || 'No')}
      </td>
      <td className="signal-ts">
        {(signal.SIGNAL_TS || signal.signal_ts) 
          ? new Date(signal.SIGNAL_TS || signal.signal_ts).toLocaleString() 
          : '—'}
      </td>
    </tr>
  )
}

export default function Signals() {
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const { setContext } = useExplainCenter()
  
  const [signalsData, setSignalsData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  
  // Set explain context on mount
  useEffect(() => {
    setContext(SIGNALS_EXPLAIN_CONTEXT)
  }, [setContext])
  
  // Parse filters from URL
  const filters = {
    symbol: searchParams.get('symbol'),
    marketType: searchParams.get('market_type'),
    patternId: searchParams.get('pattern_id'),
    horizonBars: searchParams.get('horizon_bars'),
    runId: searchParams.get('pipelineRunId') || searchParams.get('run_id'),
    asOfTs: searchParams.get('asOf') || searchParams.get('as_of_ts'),
    trustLabel: searchParams.get('trust_label'),
    fromBrief: searchParams.get('from') === 'brief',
    portfolioId: searchParams.get('portfolioId'),
  }
  
  const hasFilters = Object.values(filters).some(v => v && v !== 'brief')
  
  // Fetch signals
  const fetchSignals = useCallback(async () => {
    setLoading(true)
    setError(null)
    
    try {
      const params = new URLSearchParams()
      if (filters.symbol) params.set('symbol', filters.symbol)
      if (filters.marketType) params.set('market_type', filters.marketType)
      if (filters.patternId) params.set('pattern_id', filters.patternId)
      if (filters.horizonBars) params.set('horizon_bars', filters.horizonBars)
      if (filters.runId) params.set('run_id', filters.runId)
      if (filters.asOfTs) params.set('as_of_ts', filters.asOfTs)
      if (filters.trustLabel) params.set('trust_label', filters.trustLabel)
      params.set('include_fallback', 'true')
      params.set('limit', '100')
      
      const res = await fetch(`${API_BASE}/signals?${params}`)
      if (!res.ok) throw new Error(res.statusText)
      const data = await res.json()
      setSignalsData(data)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [filters.symbol, filters.marketType, filters.patternId, filters.horizonBars, filters.runId, filters.asOfTs, filters.trustLabel])
  
  useEffect(() => {
    fetchSignals()
  }, [fetchSignals])
  
  // Clear all filters
  const clearFilters = useCallback(() => {
    setSearchParams({})
  }, [setSearchParams])
  
  // Clear single filter
  const clearFilter = useCallback((key) => {
    const newParams = new URLSearchParams(searchParams)
    newParams.delete(key)
    // Also clear 'from' if we're clearing filters
    if (key !== 'from') newParams.delete('from')
    setSearchParams(newParams)
  }, [searchParams, setSearchParams])
  
  // Use latest run (remove run_id filter)
  const useLatestRun = useCallback(() => {
    const newParams = new URLSearchParams(searchParams)
    newParams.delete('pipelineRunId')
    newParams.delete('run_id')
    newParams.delete('asOf')
    newParams.delete('as_of_ts')
    setSearchParams(newParams)
  }, [searchParams, setSearchParams])
  
  if (loading) {
    return (
      <>
        <h1>Signals Explorer</h1>
        <LoadingState />
      </>
    )
  }
  
  if (error) {
    return (
      <>
        <h1>Signals Explorer</h1>
        <ErrorState message={error} />
      </>
    )
  }
  
  const signals = signalsData?.signals || []
  const count = signalsData?.count || 0
  const fallbackUsed = signalsData?.fallback_used
  const fallbackReason = signalsData?.fallback_reason
  const queryType = signalsData?.query_type
  
  return (
    <>
      <h1>Signals Explorer</h1>
      <p className="page-description">
        Browse actual signal and recommendation rows. Filter by symbol, pattern, trust level, and more.
      </p>
      
      {/* From Brief banner */}
      {filters.fromBrief && (
        <FromBriefBanner
          portfolioId={filters.portfolioId}
          asOfTs={filters.asOfTs}
          pipelineRunId={filters.runId}
          onClearFilters={clearFilters}
        />
      )}
      
      {/* Fallback banner */}
      {fallbackUsed && fallbackReason && (
        <FallbackBanner
          reason={fallbackReason}
          onClearFilters={clearFilters}
          onUseLatestRun={useLatestRun}
          onBackToBrief={() => navigate('/brief')}
        />
      )}
      
      {/* Active filters */}
      {hasFilters && (
        <div className="signals-active-filters">
          <span className="filters-label">Active filters:</span>
          <FilterBadge label="Symbol" value={filters.symbol} onClear={() => clearFilter('symbol')} />
          <FilterBadge label="Market" value={filters.marketType} onClear={() => clearFilter('market_type')} />
          <FilterBadge label="Pattern" value={filters.patternId} onClear={() => clearFilter('pattern_id')} />
          <FilterBadge label="Trust" value={filters.trustLabel} onClear={() => clearFilter('trust_label')} />
          <FilterBadge label="Run ID" value={filters.runId?.slice(0, 8)} onClear={() => clearFilter('pipelineRunId')} />
          <FilterBadge label="As of" value={filters.asOfTs ? new Date(filters.asOfTs).toLocaleDateString() : null} onClear={() => clearFilter('asOf')} />
          <button type="button" className="clear-all-btn" onClick={clearFilters}>
            Clear all
          </button>
        </div>
      )}
      
      {/* Results count */}
      <div className="signals-count">
        {count > 0 ? (
          <span>Showing {count} signal{count !== 1 ? 's' : ''}</span>
        ) : (
          <span>No signals found</span>
        )}
        {queryType && queryType !== 'primary' && queryType !== 'no_results' && (
          <span className="query-type-badge">{queryType.replace(/_/g, ' ')}</span>
        )}
      </div>
      
      {/* Results table or empty state */}
      {count === 0 ? (
        <EmptyState
          title="No signals found"
          action={
            hasFilters ? (
              <button type="button" onClick={clearFilters}>Clear all filters</button>
            ) : (
              <Link to="/brief">Go to Morning Brief</Link>
            )
          }
          explanation={
            hasFilters
              ? "No signals match your current filters. Try clearing some filters or using a different time window."
              : "No signals available. Run the pipeline to generate recommendations."
          }
          reasons={[
            "The brief may be stale (from an older pipeline run).",
            "The signal may have been filtered out by trust rules.",
            "Try clearing the run ID or date filters.",
          ]}
        />
      ) : (
        <div className="signals-table-container">
          <table className="signals-table">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Market</th>
                <th>Pattern</th>
                <th>Score</th>
                <th>Trust</th>
                <th>Action</th>
                <th>Eligible</th>
                <th>Signal Time</th>
              </tr>
            </thead>
            <tbody>
              {signals.map((signal, idx) => (
                <SignalRow 
                  key={signal.RECOMMENDATION_ID || signal.recommendation_id || idx} 
                  signal={signal} 
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
      
      {/* Navigation links */}
      <div className="signals-nav">
        <Link to="/brief" className="nav-link">← Morning Brief</Link>
        <Link to="/suggestions" className="nav-link">Suggestions →</Link>
      </div>
    </>
  )
}
