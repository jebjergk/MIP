import React, { useState, useEffect } from 'react'
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
    
    // Calculate markers for overlay
    const hasSignal = barEvents.some((e) => e.type === 'SIGNAL')
    const hasProposal = barEvents.some((e) => e.type === 'PROPOSAL')
    const hasTrade = barEvents.some((e) => e.type === 'TRADE')
    
    return {
      ...bar,
      date,
      events: barEvents,
      signalMarker: hasSignal ? bar.low * 0.995 : null,
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
            
            {/* High-Low range as area */}
            <Line
              type="monotone"
              dataKey="high"
              stroke="#ccc"
              strokeWidth={1}
              dot={false}
              name="High"
            />
            <Line
              type="monotone"
              dataKey="low"
              stroke="#ccc"
              strokeWidth={1}
              dot={false}
              name="Low"
            />
            
            {/* Close price as main line */}
            <Line
              type="monotone"
              dataKey="close"
              stroke="#1976d2"
              strokeWidth={2}
              dot={false}
              name="Close"
            />
            
            {/* Signal markers */}
            <Scatter
              dataKey="signalMarker"
              fill="#2196f3"
              shape="triangle"
              name="Signal"
            />
            
            {/* Proposal markers */}
            <Scatter
              dataKey="proposalMarker"
              fill="#ff9800"
              shape="diamond"
              name="Proposal"
            />
            
            {/* Trade markers */}
            <Scatter
              dataKey="tradeMarker"
              fill="#4caf50"
              shape="star"
              name="Trade"
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      
      {/* Chart legend */}
      <div className="mtd-chart-legend">
        <span className="legend-item"><span className="legend-dot signal"></span> Signal</span>
        <span className="legend-item"><span className="legend-dot proposal"></span> Proposal</span>
        <span className="legend-item"><span className="legend-dot trade"></span> Trade</span>
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
      
      {/* Events table (recent) */}
      {events.length > 0 && (
        <div className="mtd-events-table">
          <h5>Recent Events ({events.length})</h5>
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Type</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>
              {events.slice(-20).reverse().map((e, i) => (
                <tr key={i} className={`event-row event-${e.type?.toLowerCase()}`}>
                  <td>{e.ts?.slice(0, 10)}</td>
                  <td><span className={`event-badge ${e.type?.toLowerCase()}`}>{e.type}</span></td>
                  <td>
                    {e.type === 'SIGNAL' && `Pattern ${e.pattern_id}, Score: ${e.score?.toFixed(4) || '—'}`}
                    {e.type === 'PROPOSAL' && `${e.side} · Weight: ${e.target_weight?.toFixed(2) || '—'} · Status: ${e.status}`}
                    {e.type === 'TRADE' && `${e.side} ${e.quantity} @ ${e.price?.toFixed(2)} ($${e.notional?.toFixed(0)})`}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
