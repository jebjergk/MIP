import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import MarketTimelineDetail from '../components/MarketTimelineDetail'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import './MarketTimeline.css'

/**
 * Market Timeline: End-to-end symbol observability page.
 * 
 * Shows a grid of symbols with signal/proposal/trade counts,
 * with inline expansion to view OHLC chart + event overlays + decision narrative.
 */
export default function MarketTimeline() {
  const { formatSymbolLabel } = useSymbolMeta()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [portfolios, setPortfolios] = useState([])
  // Filters
  const [portfolioId, setPortfolioId] = useState('')
  const [marketTypeFilter, setMarketTypeFilter] = useState('')
  const [windowBars, setWindowBars] = useState(30)
  
  // Inline expansion
  const [expandedSymbol, setExpandedSymbol] = useState(null) // {symbol, market_type}
  const detailCacheRef = useRef({}) // Cache for detail data
  
  // Fetch portfolio list once
  useEffect(() => {
    fetch(`${API_BASE}/live/portfolio-config`)
      .then((r) => r.ok ? r.json() : [])
      .then((d) => setPortfolios(Array.isArray(d) ? d : []))
      .catch(() => setPortfolios([]))
  }, [])

  // Fetch overview data
  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    detailCacheRef.current = {}
    
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
        End-to-end observability: signals → proposals → broker-truth trades per symbol.
        Click a symbol to see the OHLC chart with event overlays and decision narrative.
      </p>
      
      {/* Filters */}
      <div className="market-timeline-filters">
        <label>
          Portfolio:
          <select value={portfolioId} onChange={(e) => setPortfolioId(e.target.value)}>
            <option value="">All</option>
            {portfolios.map((p) => (
              <option key={p.PORTFOLIO_ID} value={p.PORTFOLIO_ID}>
                {(p.IBKR_ACCOUNT_ID ? `${p.IBKR_ACCOUNT_ID} · ` : '') + (p.PORTFOLIO_ID != null ? `Portfolio ${p.PORTFOLIO_ID}` : 'Portfolio')}
              </option>
            ))}
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
        <span className="legend-item market-timeline-legend-note">T = live + sim fills (window total)</span>
      </div>
      
      {symbols.length === 0 ? (
        <EmptyState title="No symbols found" message="No market data in the selected window." />
      ) : (
        <div className="market-timeline-grid">
          {symbols.map((sym) => {
            const key = `${sym.market_type}-${sym.symbol}`
            const displaySymbol = formatSymbolLabel(sym.symbol, sym.market_type)
            const isExpanded = expandedSymbol?.key === key
            const hasSignals = sym.signal_count > 0
            const hasProposals = sym.proposal_count > 0
            const hasTrades = sym.trade_count > 0
            const latestBarSignalCount = sym.latest_bar_signal_count
            const latestBarProposalCount = sym.latest_bar_proposal_count
            const latestBarTradeCount = sym.latest_bar_trade_count
            const hasLatestSignals = Number(latestBarSignalCount || 0) > 0
            const hasLatestProposals = Number(latestBarProposalCount || 0) > 0
            const hasLatestTrades = Number(latestBarTradeCount || 0) > 0
            const displaySignalCount = Number(sym.signal_count || 0)
            const displayProposalCount = Number(sym.proposal_count || 0)
            const displayTradeCount = Number(sym.trade_count || 0)
            const hasDisplaySignals = displaySignalCount > 0
            const hasDisplayProposals = displayProposalCount > 0
            const hasDisplayTrades = displayTradeCount > 0
            const hasActionableProposals = (sym.actionable_proposal_count ?? sym.today_proposal_count ?? 0) > 0
            const cachedDetail = detailCacheRef.current[key]
            
            // Determine tile status class from latest-bar activity only.
            // Fall back to window-based counts only when latest-bar fields are absent.
            let statusClass = 'tile-inactive'
            if (hasLatestTrades) statusClass = 'tile-executed'
            else if (hasLatestProposals) statusClass = 'tile-proposed'
            else if (hasLatestSignals) statusClass = 'tile-signals-only'
            else if (hasTrades) statusClass = 'tile-executed'
            else if (hasProposals) statusClass = 'tile-proposed'
            else if (hasSignals) statusClass = 'tile-signals-only'
            
            // Keep actionable highlight until the next daily bar batch arrives.
            const todayHighlight = hasActionableProposals ? 'tile-today-actionable' : ''
            
            return (
              <React.Fragment key={key}>
                <div
                  className={`market-timeline-tile ${statusClass} ${todayHighlight} ${isExpanded ? 'tile-expanded' : ''}`}
                  onClick={() => toggleExpand(sym.symbol, sym.market_type)}
                  onKeyDown={(e) => handleTileKeyDown(e, sym.symbol, sym.market_type)}
                  tabIndex={0}
                  role="button"
                  aria-expanded={isExpanded}
                  aria-label={`${displaySymbol} ${sym.market_type}. Signals: ${sym.signal_count}, Proposals: ${sym.proposal_count}, Trades: ${sym.trade_count}. Press Enter to ${isExpanded ? 'collapse' : 'expand'}.`}
                >
                  <div className="tile-header">
                    <span className="tile-symbol">{displaySymbol}</span>
                    <span className="tile-market-type">{sym.market_type}</span>
                  </div>
                  
                  <div className="tile-badges">
                    {hasActionableProposals && (
                      <span className="tile-action-badge">ACTION</span>
                    )}
                    {sym.trust_label && (
                      <span className={`tile-trust-badge trust-${sym.trust_label?.toLowerCase()}`}>
                        {sym.trust_label}
                      </span>
                    )}
                  </div>
                  
                  <div className="tile-counts">
                    <span
                      className={`count-badge count-signal ${hasDisplaySignals ? 'has-count' : ''}`}
                      title={`Window signals: ${displaySignalCount} (latest bar: ${Number(latestBarSignalCount || 0)})`}
                    >
                      S:{displaySignalCount}
                    </span>
                    <span
                      className={`count-badge count-proposal ${hasDisplayProposals ? 'has-count' : ''}`}
                      title={`Window proposals: ${displayProposalCount} (latest bar: ${Number(latestBarProposalCount || 0)})`}
                    >
                      P:{displayProposalCount}
                    </span>
                    <span
                      className={`count-badge count-trade ${hasDisplayTrades ? 'has-count' : ''}`}
                      title={`Window trades: ${displayTradeCount} (latest bar: ${Number(latestBarTradeCount || 0)})`}
                    >
                      T:{displayTradeCount}
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
            {symbols.some((s) => s.live_trade_count != null) ? (
              <> {' '}(<strong>{symbols.reduce((acc, s) => acc + (s.live_trade_count || 0), 0)}</strong> live)</>
            ) : null}
          </p>
        </div>
      )}
    </div>
  )
}
