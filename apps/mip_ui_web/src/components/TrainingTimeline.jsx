import { useState, useEffect, useMemo } from 'react'
// Force reload - Feb 6 2026 - dual Y-axis with avg return line
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Dot,
} from 'recharts'
import { API_BASE } from '../App'
import LoadingState from './LoadingState'
import EmptyState from './EmptyState'
import ErrorState from './ErrorState'
import './TrainingTimeline.css'

/**
 * Custom dot renderer for event markers
 */
function EventDot(props) {
  const { cx, cy, payload } = props
  if (!payload?.event) return null

  const eventColors = {
    FIRST_OUTCOME: '#1565c0',
    MIN_SIGNALS_REACHED: '#7b1fa2',
    ENTERED_WATCH: '#f57c00',
    ENTERED_TRUSTED: '#2e7d32',
    DROPPED_FROM_TRUSTED: '#c62828',
    MISS_STREAK: '#c62828',
  }

  const color = eventColors[payload.event] || '#666'

  return (
    <g>
      <circle cx={cx} cy={cy} r={6} fill={color} stroke="#fff" strokeWidth={2} />
    </g>
  )
}

/**
 * Custom tooltip for the timeline chart
 */
function TimelineTooltip({ active, payload, label }) {
  if (!active || !payload || !payload.length) return null
  const data = payload[0]?.payload
  if (!data) return null

  const stateColors = {
    TRUSTED: '#2e7d32',
    WATCH: '#f57c00',
    UNTRUSTED: '#9e9e9e',
  }

  return (
    <div className="timeline-tooltip">
      <p className="timeline-tooltip-date">{data.ts ? String(data.ts).slice(0, 10) : '—'}</p>
      <p className="timeline-tooltip-row">
        <span>Hit rate:</span>
        <strong>{data.rolling_hit_rate != null ? `${(data.rolling_hit_rate * 100).toFixed(1)}%` : '—'}</strong>
      </p>
      <p className="timeline-tooltip-row">
        <span>Evaluated:</span>
        <strong>{data.evaluated_count ?? '—'}</strong>
      </p>
      <p className="timeline-tooltip-row">
        <span>Avg return:</span>
        <strong>{data.rolling_avg_return != null ? `${(data.rolling_avg_return * 100).toFixed(3)}%` : '—'}</strong>
      </p>
      <p className="timeline-tooltip-state" style={{ color: stateColors[data.state] || '#666' }}>
        {data.state ?? 'UNKNOWN'}
      </p>
      {data.event && (
        <p className="timeline-tooltip-event">{data.event.replace(/_/g, ' ')}</p>
      )}
    </div>
  )
}

/**
 * TrainingTimeline component - shows confidence over time for a symbol
 */
export default function TrainingTimeline({
  symbol,
  marketType,
  patternId = 1,
  horizonBars = 5,
  onClose,
}) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!symbol || !marketType) {
      setLoading(false)
      return
    }

    let cancelled = false
    setLoading(true)
    setError(null)

    const params = new URLSearchParams({
      symbol,
      market_type: marketType,
      pattern_id: String(patternId),
      horizon_bars: String(horizonBars),
    })

    fetch(`${API_BASE}/training/timeline?${params}`)
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

    return () => {
      cancelled = true
    }
  }, [symbol, marketType, patternId, horizonBars])

  // Transform series data for recharts
  const chartData = useMemo(() => {
    if (!data?.series) return []
    return data.series.map((pt) => ({
      ...pt,
      // Convert to percentage for display
      hit_rate_pct: pt.rolling_hit_rate != null ? pt.rolling_hit_rate * 100 : null,
      avg_return_pct: pt.rolling_avg_return != null ? pt.rolling_avg_return * 100 : null,
      date_label: pt.ts ? String(pt.ts).slice(0, 10) : '',
    }))
  }, [data])
  
  // Calculate Y-axis domain for avg return (auto-scale with some padding)
  const avgReturnDomain = useMemo(() => {
    if (!chartData.length) return [-1, 1]
    const returns = chartData.map(d => d.avg_return_pct).filter(v => v != null)
    if (!returns.length) return [-1, 1]
    const min = Math.min(...returns)
    const max = Math.max(...returns)
    const padding = Math.max(0.5, (max - min) * 0.2)
    return [Math.floor((min - padding) * 10) / 10, Math.ceil((max + padding) * 10) / 10]
  }, [chartData])

  const thresholdPct = data?.thresholds?.min_hit_rate != null
    ? data.thresholds.min_hit_rate * 100
    : 55

  if (loading) {
    return (
      <div className="training-timeline-panel">
        <div className="training-timeline-header">
          <h3>Confidence over time (evidence accumulation)</h3>
          {onClose && <button className="timeline-close-btn" onClick={onClose}>&times;</button>}
        </div>
        <LoadingState />
      </div>
    )
  }

  if (error) {
    return (
      <div className="training-timeline-panel">
        <div className="training-timeline-header">
          <h3>Confidence over time</h3>
          {onClose && <button className="timeline-close-btn" onClick={onClose}>&times;</button>}
        </div>
        <ErrorState message={error} />
      </div>
    )
  }

  if (!data?.series?.length) {
    return (
      <div className="training-timeline-panel">
        <div className="training-timeline-header">
          <h3>Confidence over time (evidence accumulation)</h3>
          {onClose && <button className="timeline-close-btn" onClick={onClose}>&times;</button>}
        </div>
        <EmptyState
          title="No evaluated outcomes yet"
          action="Still observing — outcomes will appear as signals mature."
          explanation="Training timeline shows how confidence evolves as outcomes are evaluated."
          reasons={data?.narrative || ['No evaluated outcomes for this symbol yet.']}
        />
      </div>
    )
  }

  return (
    <div className="training-timeline-panel">
      <div className="training-timeline-header">
        <div>
          <h3>Confidence over time (evidence accumulation)</h3>
          <p className="training-timeline-subtitle">
            This is derived from evaluated outcomes, not a model weight.
          </p>
        </div>
        {onClose && <button className="timeline-close-btn" onClick={onClose}>&times;</button>}
      </div>

      {/* Pattern Trust Status - THE KEY INFO */}
      {data.pattern_trust && (
        <div className={`pattern-trust-card ${data.pattern_trust.is_trusted ? 'trusted' : 'not-trusted'}`}>
          <div className="pattern-trust-header">
            <span className={`pattern-trust-badge ${data.pattern_trust.is_trusted ? 'trusted' : 'not-trusted'}`}>
              {data.pattern_trust.is_trusted ? '✓ PATTERN TRUSTED' : '✗ PATTERN NOT TRUSTED'}
            </span>
            <span className="pattern-trust-label">
              (Pattern {patternId} across all symbols)
            </span>
          </div>
          <div className="pattern-trust-stats">
            <span>
              <strong>{data.pattern_trust.n_signals || 0}</strong> outcomes
            </span>
            {data.pattern_trust.hit_rate != null && (
              <span>
                <strong>{(data.pattern_trust.hit_rate * 100).toFixed(1)}%</strong> hit rate
              </span>
            )}
            {data.pattern_trust.avg_return != null && (
              <span>
                <strong>{(data.pattern_trust.avg_return * 100).toFixed(3)}%</strong> avg return
              </span>
            )}
            {data.pattern_trust.confidence && (
              <span className={`confidence-badge ${data.pattern_trust.confidence.toLowerCase()}`}>
                {data.pattern_trust.confidence} confidence
              </span>
            )}
          </div>
          <p className="pattern-trust-reason">
            {data.pattern_trust.is_trusted 
              ? 'Signals from this pattern CAN be traded because it meets thresholds across all symbols.'
              : `Not tradeable: ${data.pattern_trust.reason}`
            }
          </p>
        </div>
      )}

      {/* Narrative bullets - Symbol specific */}
      {data.narrative && data.narrative.length > 0 && (
        <div className="training-journey-card">
          <h4>Training Journey (This Symbol Only)</h4>
          <ul className="training-journey-bullets">
            {data.narrative.map((bullet, i) => (
              <li key={i}>{bullet}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Timeline chart */}
      <div className="training-timeline-chart">
        <ResponsiveContainer width="100%" height={280}>
          <LineChart
            data={chartData}
            margin={{ top: 8, right: 50, left: 0, bottom: 4 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
            <XAxis
              dataKey="date_label"
              tick={{ fontSize: 10 }}
              tickFormatter={(v) => v.slice(5)}
              interval="preserveStartEnd"
            />
            {/* Left Y-axis for Hit Rate (0-100%) */}
            <YAxis
              yAxisId="left"
              domain={[0, 100]}
              tick={{ fontSize: 10 }}
              tickFormatter={(v) => `${v}%`}
              width={44}
            />
            {/* Right Y-axis for Avg Return (auto-scaled) */}
            <YAxis
              yAxisId="right"
              orientation="right"
              domain={avgReturnDomain}
              tick={{ fontSize: 10 }}
              tickFormatter={(v) => `${v.toFixed(1)}%`}
              width={44}
            />
            <Tooltip content={<TimelineTooltip />} />
            
            {/* Trusted threshold line (hit rate) */}
            <ReferenceLine
              yAxisId="left"
              y={thresholdPct}
              stroke="#2e7d32"
              strokeDasharray="5 5"
              strokeWidth={1.5}
            />

            {/* 50% reference line (hit rate) */}
            <ReferenceLine
              yAxisId="left"
              y={50}
              stroke="#9e9e9e"
              strokeDasharray="2 2"
              strokeWidth={1}
            />
            
            {/* 0% reference line (avg return) */}
            <ReferenceLine
              yAxisId="right"
              y={0}
              stroke="#ff9800"
              strokeDasharray="2 2"
              strokeWidth={1}
            />

            {/* Hit rate line */}
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="hit_rate_pct"
              stroke="#1565c0"
              strokeWidth={2}
              dot={<EventDot />}
              activeDot={{ r: 4, fill: '#1565c0' }}
              connectNulls
              name="Rolling Hit Rate"
            />
            
            {/* Avg return line */}
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="avg_return_pct"
              stroke="#ff9800"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#ff9800' }}
              connectNulls
              name="Rolling Avg Return"
            />
          </LineChart>
        </ResponsiveContainer>

        {/* Legend */}
        <div className="timeline-legend">
          <span className="legend-item">
            <span className="legend-line" style={{ background: '#1565c0' }} />
            Rolling hit rate (left axis)
          </span>
          <span className="legend-item">
            <span className="legend-line" style={{ background: '#ff9800' }} />
            Rolling avg return (right axis)
          </span>
          <span className="legend-item">
            <span className="legend-line legend-line--dashed" style={{ borderColor: '#2e7d32' }} />
            Trusted threshold ({thresholdPct}%)
          </span>
        </div>
        <div className="timeline-legend timeline-legend--events">
          <span className="legend-label">Event markers:</span>
          <span className="legend-item">
            <span className="legend-dot" style={{ background: '#1565c0' }} />
            First outcome
          </span>
          <span className="legend-item">
            <span className="legend-dot" style={{ background: '#7b1fa2' }} />
            Min signals reached
          </span>
          <span className="legend-item">
            <span className="legend-dot" style={{ background: '#2e7d32' }} />
            Entered trusted
          </span>
          <span className="legend-item">
            <span className="legend-dot" style={{ background: '#f57c00' }} />
            Entered watch
          </span>
          <span className="legend-item">
            <span className="legend-dot" style={{ background: '#c62828' }} />
            Dropped / miss streak
          </span>
        </div>
      </div>

      {/* How it works explanation */}
      <details className="training-explainer">
        <summary>How pattern trust works</summary>
        <div className="training-explainer-content">
          <div className="explainer-diagram">
            <div className="explainer-level">
              <span className="explainer-icon">🎯</span>
              <div>
                <strong>Pattern Level</strong> (used for trading decisions)
                <p>Pattern {patternId} is evaluated across ALL symbols. If the combined performance meets thresholds, signals from ANY symbol using this pattern can be traded.</p>
              </div>
            </div>
            <div className="explainer-arrow">↓</div>
            <div className="explainer-level">
              <span className="explainer-icon">📊</span>
              <div>
                <strong>Symbol Level</strong> (shown in chart above)
                <p>{symbol} specifically has {data?.series?.length || 0} evaluated outcomes. This may be fewer than needed for individual trust, but the pattern overall may still be trusted.</p>
              </div>
            </div>
          </div>
          <p className="explainer-example">
            <strong>Example:</strong> Pattern 2 might have 50 outcomes across 10 symbols (5 per symbol average). 
            Even though each symbol alone has &lt;40 outcomes, the pattern total of 50 meets the threshold.
          </p>
        </div>
      </details>

      {/* Pending evaluations indicator */}
      {data.pending_evaluations?.count > 0 && (
        <div className="pending-evaluations-card">
          <span className="pending-icon">⏳</span>
          <div className="pending-text">
            <strong>{data.pending_evaluations.count} signal{data.pending_evaluations.count !== 1 ? 's' : ''} pending evaluation</strong>
            <p>
              Recent signals from {data.pending_evaluations.oldest?.slice(0, 10)} to {data.pending_evaluations.newest?.slice(0, 10)} 
              are waiting for {horizonBars} more bars of market data before outcomes can be measured.
            </p>
          </div>
        </div>
      )}

      {/* Meta info */}
      <div className="training-timeline-meta">
        <span>Symbol: {data.symbol}</span>
        <span>Pattern: {data.pattern_id}</span>
        <span>Horizon: {data.horizon_bars} bars</span>
        <span>Evaluated: {data.series.length}</span>
        {data.pending_evaluations?.count > 0 && (
          <span className="pending-count">Pending: {data.pending_evaluations.count}</span>
        )}
      </div>
    </div>
  )
}
