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
} from 'recharts'
import { API_BASE } from '../App'
import EmptyState from './EmptyState'
import ErrorState from './ErrorState'
import './TrainingTimelineInline.css'

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
function TimelineTooltip({ active, payload }) {
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
 * Loading skeleton for inline timeline
 */
function InlineLoadingSkeleton() {
  return (
    <div className="timeline-inline-skeleton">
      <div className="skeleton-header">
        <div className="skeleton-title" />
        <div className="skeleton-subtitle" />
      </div>
      <div className="skeleton-journey">
        <div className="skeleton-line skeleton-line--short" />
        <div className="skeleton-line" />
        <div className="skeleton-line skeleton-line--medium" />
      </div>
      <div className="skeleton-chart" />
      <div className="skeleton-meta">
        <div className="skeleton-tag" />
        <div className="skeleton-tag" />
        <div className="skeleton-tag" />
      </div>
    </div>
  )
}

/**
 * TrainingTimelineInline component - inline expandable version
 * Supports caching via cachedData prop and onDataLoaded callback
 */
export default function TrainingTimelineInline({
  symbol,
  marketType,
  patternId = 1,
  horizonBars = 5,
  cachedData,
  onDataLoaded,
  onClose,
}) {
  const [data, setData] = useState(cachedData || null)
  const [loading, setLoading] = useState(!cachedData)
  const [error, setError] = useState(null)

  useEffect(() => {
    // If we have cached data, use it immediately
    if (cachedData) {
      setData(cachedData)
      setLoading(false)
      return
    }

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
        if (!cancelled) {
          setData(d)
          // Notify parent to cache the data
          if (onDataLoaded) {
            onDataLoaded(d)
          }
        }
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
  }, [symbol, marketType, patternId, horizonBars, cachedData, onDataLoaded])

  // Transform series data for recharts
  const chartData = useMemo(() => {
    if (!data?.series) return []
    return data.series.map((pt) => ({
      ...pt,
      hit_rate_pct: pt.rolling_hit_rate != null ? pt.rolling_hit_rate * 100 : null,
      avg_return_pct: pt.rolling_avg_return != null ? pt.rolling_avg_return * 100 : null,
      date_label: pt.ts ? String(pt.ts).slice(0, 10) : '',
    }))
  }, [data])

  // Calculate Y-axis domain for avg return (auto-scale with padding)
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
      <div className="training-timeline-inline">
        <InlineLoadingSkeleton />
      </div>
    )
  }

  if (error) {
    return (
      <div className="training-timeline-inline">
        <div className="training-timeline-inline-header">
          <h4>Confidence over time</h4>
          {onClose && (
            <button className="timeline-inline-close" onClick={onClose} aria-label="Collapse details">
              &times;
            </button>
          )}
        </div>
        <ErrorState message={error} />
      </div>
    )
  }

  if (!data?.series?.length) {
    return (
      <div className="training-timeline-inline">
        <div className="training-timeline-inline-header">
          <h4>Confidence over time (evidence accumulation)</h4>
          {onClose && (
            <button className="timeline-inline-close" onClick={onClose} aria-label="Collapse details">
              &times;
            </button>
          )}
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
    <div className="training-timeline-inline">
      <div className="training-timeline-inline-header">
        <div>
          <h4>Confidence over time (evidence accumulation)</h4>
          <p className="training-timeline-inline-subtitle">
            This is derived from evaluated outcomes, not a model weight.
          </p>
        </div>
        {onClose && (
          <button className="timeline-inline-close" onClick={onClose} aria-label="Collapse details">
            &times;
          </button>
        )}
      </div>

      <div className="training-timeline-inline-content">
        {/* Left side: Narrative + Meta */}
        <div className="training-timeline-inline-left">
          {/* Narrative bullets */}
          {data.narrative && data.narrative.length > 0 && (
            <div className="training-journey-card-inline">
              <h5>Training Journey</h5>
              <ul className="training-journey-bullets-inline">
                {data.narrative.map((bullet, i) => (
                  <li key={i}>{bullet}</li>
                ))}
              </ul>
            </div>
          )}

          {/* Meta info */}
          <div className="training-timeline-meta-inline">
            <span>Symbol: <strong>{data.symbol}</strong></span>
            <span>Pattern: <strong>{data.pattern_id}</strong></span>
            <span>Horizon: <strong>{data.horizon_bars} bars</strong></span>
            <span>Points: <strong>{data.series.length}</strong></span>
          </div>
        </div>

        {/* Right side: Chart */}
        <div className="training-timeline-inline-right">
          <div className="training-timeline-chart-inline">
            <ResponsiveContainer width="100%" height={200}>
              <LineChart
                data={chartData}
                margin={{ top: 8, right: 40, left: 0, bottom: 4 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                <XAxis
                  dataKey="date_label"
                  tick={{ fontSize: 9 }}
                  tickFormatter={(v) => v.slice(5)}
                  interval="preserveStartEnd"
                />
                {/* Left Y-axis for Hit Rate (0-100%) */}
                <YAxis
                  yAxisId="left"
                  domain={[0, 100]}
                  tick={{ fontSize: 9 }}
                  tickFormatter={(v) => `${v}%`}
                  width={36}
                />
                {/* Right Y-axis for Avg Return (auto-scaled) */}
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  domain={avgReturnDomain}
                  tick={{ fontSize: 9 }}
                  tickFormatter={(v) => `${v?.toFixed?.(1) ?? v}%`}
                  width={36}
                />
                <Tooltip content={<TimelineTooltip />} />
                
                {/* Trusted threshold line */}
                <ReferenceLine
                  yAxisId="left"
                  y={thresholdPct}
                  stroke="#2e7d32"
                  strokeDasharray="5 5"
                  strokeWidth={1.5}
                />

                {/* 50% reference line */}
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
            <div className="timeline-legend-inline">
              <span className="legend-item-inline">
                <span className="legend-line-inline" style={{ background: '#1565c0' }} />
                Hit rate (left)
              </span>
              <span className="legend-item-inline">
                <span className="legend-line-inline" style={{ background: '#ff9800' }} />
                Avg return (right)
              </span>
              <span className="legend-item-inline">
                <span className="legend-line-inline legend-line-inline--dashed" style={{ borderColor: '#2e7d32' }} />
                Trusted threshold
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* UX coherence note */}
      <p className="training-timeline-note-inline">
        Confidence increases as outcomes are evaluated. Opportunities may be shown even when evidence is still insufficient to act.
      </p>
    </div>
  )
}
