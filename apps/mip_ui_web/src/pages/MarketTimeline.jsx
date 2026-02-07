import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import MarketTimelineDetail from '../components/MarketTimelineDetail'
import { useExplainCenter } from '../context/ExplainCenterContext'
import { MARKET_TIMELINE_EXPLAIN_CONTEXT } from '../data/explainContexts'
import './MarketTimeline.css'

/**
 * Market Timeline: End-to-end symbol observability page.
 * 
 * Shows a grid of symbols with signal/proposal/trade counts,
 * with inline expansion to view OHLC chart + event overlays + decision narrative.
 */
export default function MarketTimeline() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const { setContext } = useExplainCenter()
  
  // Filters
  const [portfolioId, setPortfolioId] = useState('')
  const [marketTypeFilter, setMarketTypeFilter] = useState('')
  const [windowBars, setWindowBars] = useState(30)
  
  // Inline expansion
  const [expandedSymbol, setExpandedSymbol] = useState(null) // {symbol, market_type}
  const detailCacheRef = useRef({}) // Cache for detail data
  
  // Set explain context on mount
  useEffect(() => {
    setContext(MARKET_TIMELINE_EXPLAIN_CONTEXT)
  }, [setContext])
  
  // Fetch overview data
  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    
    const params = new URLSearchParams()
    params.set('window_bars', String(windowBars))
    if (portfolioId) params.set('portfolio_id', portfolioId)
    if (marketTypeFilter) params.set('market_type', marketTypeFilter)
    
    fetch(`${API_BASE}/market-timeline/overview?${params.toString()}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((d) => {
        if (!cancelled) setData(d)
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [portfolioId, marketTypeFilter, windowBars])
  
  // Get unique market types for filter dropdown
  const marketTypes = useMemo(() => {
    if (!data?.symbols) return []
    const set = new Set(data.symbols.map((s) => s.market_type).filter(Boolean))
    return Array.from(set).sort()
  }, [data])
  
  // Toggle inline expansion
  const toggleExpand = useCallback((symbol, marketType) => {
    const key = `${marketType}-${symbol}`
    if (expandedSymbol && expandedSymbol.symbol === symbol && expandedSymbol.market_type === marketType) {
      setExpandedSymbol(null)
    } else {
      setExpandedSymbol({ symbol, market_type: marketType, key })
    }
  }, [expandedSymbol])
  
  // Handle keyboard navigation
  const handleTileKeyDown = useCallback((e, symbol, marketType) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      toggleExpand(symbol, marketType)
    } else if (e.key === 'Escape' && expandedSymbol) {
      setExpandedSymbol(null)
    }
  }, [toggleExpand, expandedSymbol])
  
  // Cache detail data
  const setDetailCache = useCallback((key, data) => {
    detailCacheRef.current[key] = data
  }, [])
  
  if (loading) {
    return (
      <div className="market-timeline-page">
        <h1>Market Timeline</h1>
        <LoadingState />
      </div>
    )
  }
  
  if (error) {
    return (
      <div className="market-timeline-page">
        <h1>Market Timeline</h1>
        <ErrorState message={error} />
      </div>
    )
  }
  
  const symbols = data?.symbols ?? []
  const window = data?.window ?? {}
  
  return (
    <div className="market-timeline-page">
      <h1>Market Timeline</h1>
      <p className="market-timeline-subtitle">
        End-to-end observability: signals → proposals → trades per symbol.
        Click a symbol to see the OHLC chart with event overlays and decision narrative.
      </p>
      
      {/* Filters */}
      <div className="market-timeline-filters">
        <label>
          Portfolio:
          <select value={portfolioId} onChange={(e) => setPortfolioId(e.target.value)}>
            <option value="">All</option>
            <option value="1">Portfolio 1</option>
            <option value="2">Portfolio 2</option>
          </select>
        </label>
        <label>
          Market:
          <select value={marketTypeFilter} onChange={(e) => setMarketTypeFilter(e.target.value)}>
            <option value="">All</option>
            {marketTypes.map((m) => (
              <option key={m} value={m}>{m}</option>
            ))}
          </select>
        </label>
        <label>
          Window:
          <select value={windowBars} onChange={(e) => setWindowBars(Number(e.target.value))}>
            <option value={30}>30 bars</option>
            <option value={60}>60 bars</option>
            <option value={90}>90 bars</option>
            <option value={180}>180 bars</option>
          </select>
        </label>
      </div>
      
      {/* Window info */}
      {window.start_ts && (
        <p className="market-timeline-window-info">
          Showing {window.bars} bars from {window.start_ts?.slice(0, 10)} to {window.latest_ts?.slice(0, 10)}
        </p>
      )}
      
      {/* Legend */}
      <div className="market-timeline-legend">
        <span className="legend-item"><span className="legend-signal">S</span> Signals</span>
        <span className="legend-item"><span className="legend-proposal">P</span> Proposals</span>
        <span className="legend-item"><span className="legend-trade">T</span> Trades</span>
      </div>
      
      {symbols.length === 0 ? (
        <EmptyState title="No symbols found" message="No market data in the selected window." />
      ) : (
        <div className="market-timeline-grid">
          {symbols.map((sym) => {
            const key = `${sym.market_type}-${sym.symbol}`
            const isExpanded = expandedSymbol?.key === key
            const hasSignals = sym.signal_count > 0
            const hasProposals = sym.proposal_count > 0
            const hasTrades = sym.trade_count > 0
            const hasTodayProposals = sym.today_proposal_count > 0
            const cachedDetail = detailCacheRef.current[key]
            
            // Determine tile status class
            let statusClass = 'tile-inactive'
            if (hasTrades) statusClass = 'tile-executed'
            else if (hasProposals) statusClass = 'tile-proposed'
            else if (hasSignals) statusClass = 'tile-signals-only'
            
            // Add highlight class for today's actionable proposals
            const todayHighlight = hasTodayProposals ? 'tile-today-actionable' : ''
            
            return (
              <React.Fragment key={key}>
                <div
                  className={`market-timeline-tile ${statusClass} ${todayHighlight} ${isExpanded ? 'tile-expanded' : ''}`}
                  onClick={() => toggleExpand(sym.symbol, sym.market_type)}
                  onKeyDown={(e) => handleTileKeyDown(e, sym.symbol, sym.market_type)}
                  tabIndex={0}
                  role="button"
                  aria-expanded={isExpanded}
                  aria-label={`${sym.symbol} ${sym.market_type}. Signals: ${sym.signal_count}, Proposals: ${sym.proposal_count}, Trades: ${sym.trade_count}. Press Enter to ${isExpanded ? 'collapse' : 'expand'}.`}
                >
                  <div className="tile-header">
                    <span className="tile-symbol">{sym.symbol}</span>
                    <span className="tile-market-type">{sym.market_type}</span>
                  </div>
                  
                  <div className="tile-badges">
                    {hasTodayProposals && (
                      <span className="tile-action-badge">ACTION</span>
                    )}
                    {sym.trust_label && (
                      <span className={`tile-trust-badge trust-${sym.trust_label?.toLowerCase()}`}>
                        {sym.trust_label}
                      </span>
                    )}
                  </div>
                  
                  <div className="tile-counts">
                    <span className={`count-badge count-signal ${hasSignals ? 'has-count' : ''}`}>
                      S:{sym.signal_count}
                    </span>
                    <span className={`count-badge count-proposal ${hasProposals ? 'has-count' : ''}`}>
                      P:{sym.proposal_count}
                    </span>
                    <span className={`count-badge count-trade ${hasTrades ? 'has-count' : ''}`}>
                      T:{sym.trade_count}
                    </span>
                  </div>
                  
                  <div className="tile-expand-icon">
                    <span className={isExpanded ? 'icon-open' : ''}>▶</span>
                  </div>
                </div>
                
                {isExpanded && (
                  <div className="market-timeline-detail-row">
                    <MarketTimelineDetail
                      symbol={sym.symbol}
                      marketType={sym.market_type}
                      portfolioId={portfolioId || null}
                      windowBars={windowBars * 2} // Show more detail in expanded view
                      cachedData={cachedDetail}
                      onDataLoaded={(data) => setDetailCache(key, data)}
                      onClose={() => setExpandedSymbol(null)}
                    />
                  </div>
                )}
              </React.Fragment>
            )
          })}
        </div>
      )}
      
      {/* Summary stats */}
      {symbols.length > 0 && (
        <div className="market-timeline-summary">
          <p>
            <strong>{symbols.length}</strong> symbols · 
            <strong> {symbols.reduce((acc, s) => acc + s.signal_count, 0)}</strong> signals · 
            <strong> {symbols.reduce((acc, s) => acc + s.proposal_count, 0)}</strong> proposals · 
            <strong> {symbols.reduce((acc, s) => acc + s.trade_count, 0)}</strong> trades
          </p>
        </div>
      )}
    </div>
  )
}
