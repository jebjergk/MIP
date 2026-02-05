import { useState, useEffect, useMemo } from 'react'
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
      date_label: pt.ts ? String(pt.ts).slice(0, 10) : '',
    }))
  }, [data])

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

      {/* Narrative bullets */}
      {data.narrative && data.narrative.length > 0 && (
        <div className="training-journey-card">
          <h4>Training Journey</h4>
          <ul className="training-journey-bullets">
            {data.narrative.map((bullet, i) => (
              <li key={i}>{bullet}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Timeline chart */}
      <div className="training-timeline-chart">
        <ResponsiveContainer width="100%" height={240}>
          <LineChart
            data={chartData}
            margin={{ top: 8, right: 16, left: 0, bottom: 4 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
            <XAxis
              dataKey="date_label"
              tick={{ fontSize: 10 }}
              tickFormatter={(v) => v.slice(5)}
              interval="preserveStartEnd"
            />
            <YAxis
              domain={[0, 100]}
              tick={{ fontSize: 10 }}
              tickFormatter={(v) => `${v}%`}
              width={44}
            />
            <Tooltip content={<TimelineTooltip />} />
            
            {/* Trusted threshold line */}
            <ReferenceLine
              y={thresholdPct}
              stroke="#2e7d32"
              strokeDasharray="5 5"
              strokeWidth={1.5}
              label={{
                value: `Trusted (${thresholdPct}%)`,
                position: 'right',
                fill: '#2e7d32',
                fontSize: 10,
              }}
            />

            {/* 50% reference line */}
            <ReferenceLine
              y={50}
              stroke="#9e9e9e"
              strokeDasharray="2 2"
              strokeWidth={1}
            />

            {/* Hit rate line */}
            <Line
              type="monotone"
              dataKey="hit_rate_pct"
              stroke="#1565c0"
              strokeWidth={2}
              dot={<EventDot />}
              activeDot={{ r: 4, fill: '#1565c0' }}
              connectNulls
              name="Rolling Hit Rate"
            />
          </LineChart>
        </ResponsiveContainer>

        {/* Legend */}
        <div className="timeline-legend">
          <span className="legend-item">
            <span className="legend-line" style={{ background: '#1565c0' }} />
            Rolling hit rate
          </span>
          <span className="legend-item">
            <span className="legend-line legend-line--dashed" style={{ borderColor: '#2e7d32' }} />
            Trusted threshold
          </span>
          <span className="legend-item legend-item--events">
            <span className="legend-dot" style={{ background: '#2e7d32' }} />
            Key events
          </span>
        </div>
      </div>

      {/* UX coherence note */}
      <p className="training-timeline-note">
        Confidence increases as outcomes are evaluated. Opportunities may be shown even when evidence is still insufficient to act.
      </p>

      {/* Meta info */}
      <div className="training-timeline-meta">
        <span>Symbol: {data.symbol}</span>
        <span>Pattern: {data.pattern_id}</span>
        <span>Horizon: {data.horizon_bars} bars</span>
        <span>Points: {data.series.length}</span>
      </div>
    </div>
  )
}
