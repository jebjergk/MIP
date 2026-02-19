import React, { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  ReferenceArea,
  Scatter,
} from 'recharts'
import { API_BASE } from '../App'
import './MarketTimelineDetail.css'

/**
 * Custom tooltip for the OHLC chart
 */
function ChartTooltip({ active, payload, label }) {
  if (!active || !payload || !payload.length) return null
  
  const data = payload[0]?.payload
  if (!data) return null
  
  return (
    <div className="mtd-tooltip">
      <div className="mtd-tooltip-date">{label}</div>
      {data.open != null && (
        <div className="mtd-tooltip-row">
          <span>Open:</span> <span>{data.open?.toFixed(2)}</span>
        </div>
      )}
      {data.high != null && (
        <div className="mtd-tooltip-row">
          <span>High:</span> <span>{data.high?.toFixed(2)}</span>
        </div>
      )}
      {data.low != null && (
        <div className="mtd-tooltip-row">
          <span>Low:</span> <span>{data.low?.toFixed(2)}</span>
        </div>
      )}
      {data.close != null && (
        <div className="mtd-tooltip-row">
          <span>Close:</span> <span>{data.close?.toFixed(2)}</span>
        </div>
      )}
      {data.events && data.events.length > 0 && (
        <div className="mtd-tooltip-events">
          {data.events.map((e, i) => (
            <div key={i} className={`mtd-tooltip-event event-${e.type?.toLowerCase()}`}>
              {e.type}: {e.side || ''} {e.type === 'TRADE' && e.quantity ? `${e.quantity} @ ${e.price?.toFixed(2)}` : ''}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

/**
 * Loading skeleton for the detail panel
 */
function DetailSkeleton() {
  return (
    <div className="mtd-skeleton">
      <div className="mtd-skeleton-header"></div>
      <div className="mtd-skeleton-chart"></div>
      <div className="mtd-skeleton-narrative">
        <div className="mtd-skeleton-line"></div>
        <div className="mtd-skeleton-line"></div>
        <div className="mtd-skeleton-line short"></div>
      </div>
    </div>
  )
}

/**
 * Tree-style Signal Chain view.
 * Each signal branches into multiple proposals (one per portfolio),
 * each proposal may lead to a BUY trade, then a SELL trade.
 */
function SignalChainTree({ chains }) {
  if (!chains?.length) return null

  const active = chains.filter((c) => c.status !== 'SIGNAL_ONLY')
  const orphanCount = chains.length - active.length
  const sorted = [...active].sort((a, b) => (b.signal?.ts || '').localeCompare(a.signal?.ts || ''))

  const fmtDate = (ts) => {
    if (!ts) return '—'
    const d = ts.slice(0, 10)
    const t = ts.slice(11, 16)
    return t && t !== '00:00' ? `${d} ${t}` : d
  }
  const fmtPrice = (v) => v != null ? '$' + Number(v).toFixed(2) : ''
  const fmtPnl = (v) => {
    if (v == null) return null
    const n = Number(v)
    const sign = n >= 0 ? '+' : ''
    return `${sign}${fmtPrice(Math.abs(n))}`
  }

  const branches = (chain) => chain.branches || []

  return (
    <div className="mtd-tree">
      <h5>Signal Chains ({active.length}){orphanCount > 0 && <span className="mtd-tree-orphan-note"> · {orphanCount} signal-only</span>}</h5>
      {sorted.map((chain, ci) => (
        <div key={ci} className="mtd-tree-chain">
          {/* Level 0: Signal */}
          <div className="mtd-tree-node mtd-tree-l0">
            <span className="mtd-tree-dot signal" />
            <span className="mtd-tree-type">Signal</span>
            <span className="mtd-tree-date">{fmtDate(chain.signal?.ts)}</span>
            <span className="mtd-tree-detail">Pattern {chain.signal?.pattern_id}</span>
            {chain.signal?.score != null && (
              <span className="mtd-tree-detail">Score {chain.signal.score.toFixed(4)}</span>
            )}
          </div>

          {/* Branches: one per proposal/portfolio */}
          {branches(chain).map((branch, bi) => (
            <div key={bi} className="mtd-tree-branch-group">
              {/* Level 1: Proposal */}
              <div className="mtd-tree-node mtd-tree-l1">
                <span className="mtd-tree-branch" />
                <span className="mtd-tree-dot proposal" />
                <span className="mtd-tree-type">{branch.proposal?.side} Proposal</span>
                <span className="mtd-tree-date">{fmtDate(branch.proposal?.proposed_at || branch.proposal?.ts)}</span>
                {branch.proposal?.portfolio_id && (
                  <Link to={`/portfolios/${branch.proposal.portfolio_id}`} className="mtd-portfolio-link">
                    Portfolio {branch.proposal.portfolio_id}
                  </Link>
                )}
                {branch.proposal?.target_weight != null && (
                  <span className="mtd-tree-detail">Weight {branch.proposal.target_weight.toFixed(2)}</span>
                )}
                {branch.status === 'REJECTED' && (
                  <span className="mtd-tree-badge rejected">Rejected</span>
                )}
                {branch.status === 'PROPOSED' && (
                  <span className="mtd-tree-badge proposed">Pending</span>
                )}
              </div>

              {/* Level 2: BUY Trade */}
              {branch.buy && (
                <div className="mtd-tree-node mtd-tree-l2">
                  <span className="mtd-tree-branch" />
                  <span className="mtd-tree-dot trade" />
                  <span className="mtd-tree-type">BUY</span>
                  <span className="mtd-tree-date">{fmtDate(branch.buy.ts)}</span>
                  <span className="mtd-tree-detail">
                    {branch.buy.quantity} × {fmtPrice(branch.buy.price)}
                  </span>
                  {branch.buy.notional != null && (
                    <span className="mtd-tree-detail">(${Number(branch.buy.notional).toFixed(0)})</span>
                  )}
                  {branch.buy.portfolio_id && (
                    <Link to={`/portfolios/${branch.buy.portfolio_id}`} className="mtd-portfolio-link">
                      Portfolio {branch.buy.portfolio_id}
                    </Link>
                  )}
                  {!branch.sell && branch.status === 'OPEN' && (
                    <span className="mtd-tree-badge open">Open</span>
                  )}
                </div>
              )}

              {/* Level 3: SELL Trade */}
              {branch.sell && (
                <div className="mtd-tree-node mtd-tree-l3">
                  <span className="mtd-tree-branch" />
                  <span className="mtd-tree-dot sell" />
                  <span className="mtd-tree-type">SELL</span>
                  <span className="mtd-tree-date">{fmtDate(branch.sell.ts)}</span>
                  <span className="mtd-tree-detail">
                    {branch.sell.quantity} × {fmtPrice(branch.sell.price)}
                  </span>
                  {branch.sell.realized_pnl != null && (
                    <span className={`mtd-tree-pnl ${Number(branch.sell.realized_pnl) >= 0 ? 'positive' : 'negative'}`}>
                      {fmtPnl(branch.sell.realized_pnl)}
                    </span>
                  )}
                  <span className="mtd-tree-badge closed">Closed</span>
                </div>
              )}
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}

/**
 * Market Timeline Detail: OHLC chart with event overlays and decision narrative.
 */
export default function MarketTimelineDetail({
  symbol,
  marketType,
  portfolioId,
  windowBars = 60,
  cachedData,
  onDataLoaded,
  onClose,
}) {
  const [data, setData] = useState(cachedData || null)
  const [loading, setLoading] = useState(!cachedData)
  const [error, setError] = useState(null)
  
  useEffect(() => {
    if (cachedData) {
      setData(cachedData)
      setLoading(false)
      return
    }
    
    let cancelled = false
    setLoading(true)
    setError(null)
    
    const params = new URLSearchParams()
    params.set('symbol', symbol)
    params.set('market_type', marketType)
    params.set('window_bars', String(windowBars))
    if (portfolioId) params.set('portfolio_id', portfolioId)
    
    fetch(`${API_BASE}/market-timeline/detail?${params.toString()}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((d) => {
        if (!cancelled) {
          setData(d)
          if (onDataLoaded) onDataLoaded(d)
        }
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    
    return () => { cancelled = true }
  }, [symbol, marketType, portfolioId, windowBars, cachedData, onDataLoaded])
  
  if (loading) {
    return <DetailSkeleton />
  }
  
  if (error) {
    return (
      <div className="mtd-error">
        <p>Error loading details: {error}</p>
        <button onClick={onClose}>Close</button>
      </div>
    )
  }
  
  if (!data) {
    return null
  }
  
  // Prepare chart data: merge OHLC with events
  const ohlc = data.ohlc || []
  const events = data.events || []
  const chains = data.chains || []
  const narrative = data.narrative || {}
  const counts = data.counts || {}
  
  // Create a map of events by date
  const eventsByDate = {}
  events.forEach((e) => {
    const date = e.ts?.slice(0, 10)
    if (!eventsByDate[date]) eventsByDate[date] = []
    eventsByDate[date].push(e)
  })
  
  // Merge events into OHLC data
  const chartData = ohlc.map((bar) => {
    const date = bar.ts?.slice(0, 10)
    const barEvents = eventsByDate[date] || []
    
    // Calculate markers for overlay — space out when multiple types on same day
    const hasSignal = barEvents.some((e) => e.type === 'SIGNAL')
    const hasProposal = barEvents.some((e) => e.type === 'PROPOSAL')
    const hasTrade = barEvents.some((e) => e.type === 'TRADE')
    
    return {
      ...bar,
      date,
      events: barEvents,
      signalMarker: hasSignal ? bar.low * (hasProposal ? 0.982 : 0.99) : null,
      proposalMarker: hasProposal ? bar.low * 0.99 : null,
      tradeMarker: hasTrade ? bar.high * 1.005 : null,
      // For candlestick-like rendering
      range: bar.high && bar.low ? [bar.low, bar.high] : null,
    }
  })
  
  // Determine decision status styling
  const decisionStatus = narrative.decision_status || 'SKIPPED'
  const decisionClass = decisionStatus === 'EXECUTED' ? 'status-executed' :
                        decisionStatus === 'PROPOSED' ? 'status-proposed' : 'status-skipped'
  
  return (
    <div className="mtd-container">
      {/* Header */}
      <div className="mtd-header">
        <div className="mtd-header-left">
          <h3>{symbol} <span className="mtd-market-type">{marketType}</span></h3>
          <span className={`mtd-decision-badge ${decisionClass}`}>
            {decisionStatus}
          </span>
        </div>
        <button className="mtd-close-btn" onClick={onClose} aria-label="Close">×</button>
      </div>
      
      {/* Counts summary */}
      <div className="mtd-counts-bar">
        <span className="mtd-count mtd-count-signal">
          <strong>{counts.signals || 0}</strong> signals
        </span>
        <span className="mtd-count-arrow">→</span>
        <span className="mtd-count mtd-count-proposal">
          <strong>{counts.proposals || 0}</strong> proposals
        </span>
        <span className="mtd-count-arrow">→</span>
        <span className="mtd-count mtd-count-trade">
          <strong>{counts.trades || 0}</strong> trades
        </span>
      </div>
      
      {/* Chart */}
      <div className="mtd-chart-container">
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={chartData} margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
            <XAxis
              dataKey="date"
              tick={{ fontSize: 10 }}
              tickFormatter={(v) => v?.slice(5)} // Show MM-DD
            />
            <YAxis
              domain={['auto', 'auto']}
              tick={{ fontSize: 10 }}
              tickFormatter={(v) => v?.toFixed(2)}
            />
            <Tooltip content={<ChartTooltip />} />
            
            {/* High-Low range as thin grey lines showing daily price range */}
            <Line
              type="monotone"
              dataKey="high"
              stroke="#bbb"
              strokeWidth={1}
              strokeDasharray="2 2"
              dot={false}
              name="Daily High"
              legendType="none"
            />
            <Line
              type="monotone"
              dataKey="low"
              stroke="#bbb"
              strokeWidth={1}
              strokeDasharray="2 2"
              dot={false}
              name="Daily Low"
              legendType="none"
            />
            
            {/* Close price as main blue line */}
            <Line
              type="monotone"
              dataKey="close"
              stroke="#1976d2"
              strokeWidth={2}
              dot={false}
              name="Close Price"
            />
            
            {/* Proposal markers - orange circles below price */}
            <Scatter
              dataKey="proposalMarker"
              fill="#ff9800"
              name="Proposal"
            >
              {chartData.map((entry, index) => (
                entry.proposalMarker != null ? (
                  <circle key={`proposal-${index}`} r={7} fill="#ff9800" stroke="#e65100" strokeWidth={2} />
                ) : null
              ))}
            </Scatter>
            
            {/* Signal markers - blue circles below proposals (rendered after so visible on top) */}
            <Scatter
              dataKey="signalMarker"
              fill="#2196f3"
              name="Signal"
            >
              {chartData.map((entry, index) => (
                entry.signalMarker != null ? (
                  <circle key={`signal-${index}`} r={6} fill="#2196f3" stroke="#1565c0" strokeWidth={2} />
                ) : null
              ))}
            </Scatter>
            
            {/* Trade markers - green circles above price */}
            <Scatter
              dataKey="tradeMarker"
              fill="#4caf50"
              name="Trade"
            >
              {chartData.map((entry, index) => (
                entry.tradeMarker != null ? (
                  <circle key={`trade-${index}`} r={8} fill="#4caf50" stroke="#2e7d32" strokeWidth={2} />
                ) : null
              ))}
            </Scatter>
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      
      {/* Chart legend - explains what each element means */}
      <div className="mtd-chart-legend">
        <div className="legend-section legend-price">
          <span className="legend-title">Price:</span>
          <span className="legend-item"><span className="legend-line blue"></span> Close price</span>
          <span className="legend-item"><span className="legend-line grey dashed"></span> High/Low range</span>
        </div>
        <div className="legend-section legend-events">
          <span className="legend-title">Events:</span>
          <span className="legend-item"><span className="legend-dot signal"></span> Signal (pattern fired)</span>
          <span className="legend-item"><span className="legend-dot proposal"></span> Proposal (order suggested)</span>
          <span className="legend-item"><span className="legend-dot trade"></span> Trade (executed)</span>
        </div>
      </div>
      
      {/* Narrative */}
      <div className="mtd-narrative">
        <h4>Decision Narrative</h4>
        <div className="mtd-narrative-bullets">
          {narrative.bullets?.map((bullet, i) => (
            <p key={i} className="mtd-bullet">• {bullet}</p>
          ))}
        </div>
        
        {narrative.reasons?.length > 0 && (
          <div className="mtd-reasons">
            <h5>Reason Details</h5>
            {narrative.reasons.map((reason, i) => (
              <div key={i} className="mtd-reason">
                <span className="mtd-reason-code">{reason.code}</span>
                <span className="mtd-reason-title">{reason.title}</span>
                {reason.evidence && Object.keys(reason.evidence).length > 0 && (
                  <span className="mtd-reason-evidence">
                    {JSON.stringify(reason.evidence)}
                  </span>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
      
      {/* Trust summary */}
      {data.trust_summary?.length > 0 && (
        <div className="mtd-trust-summary">
          <h5>Trust Status by Pattern</h5>
          <div className="mtd-trust-grid">
            {data.trust_summary.map((t, i) => (
              <div key={i} className={`mtd-trust-item trust-${t.trust_label?.toLowerCase()}`}>
                <span className="mtd-trust-pattern">Pattern {t.pattern_id}</span>
                <span className="mtd-trust-label">{t.trust_label}</span>
                {t.n_success != null && (
                  <span className="mtd-trust-detail">
                    {t.n_success} outcomes · {(t.coverage_rate * 100)?.toFixed(0)}% coverage
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
      
      {/* Signal chain tree view */}
      {chains.length > 0 && <SignalChainTree chains={chains} />}
    </div>
  )
}
