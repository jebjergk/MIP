import { useState, useEffect, useMemo } from 'react'
import {
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { API_BASE } from '../App'
import './IntradaySignalChart.css'

const PATTERN_COLORS = {
  ORB: '#1565c0',
  PULLBACK_CONTINUATION: '#7b1fa2',
  MEAN_REVERSION: '#e65100',
}

function fmtPrice(v, isFX) {
  if (v == null) return '—'
  return isFX ? Number(v).toFixed(4) : `$${Number(v).toFixed(2)}`
}

function SignalTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null

  const ts = d.ts ? new Date(d.ts) : null
  const timeStr = ts
    ? ts.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
    : '—'
  const fx = d._isFX

  return (
    <div className="isc-tooltip">
      <p className="isc-tooltip-time">{timeStr}</p>
      {d.CLOSE != null && (
        <p className="isc-tooltip-row">
          <span>Close:</span> <strong>{fmtPrice(d.CLOSE, fx)}</strong>
        </p>
      )}
      {d.HIGH != null && d.LOW != null && (
        <p className="isc-tooltip-row">
          <span>Range:</span> <strong>{fmtPrice(d.LOW, fx)} – {fmtPrice(d.HIGH, fx)}</strong>
        </p>
      )}
      {d.VOLUME != null && d.VOLUME > 0 && (
        <p className="isc-tooltip-row">
          <span>Volume:</span> <strong>{Number(d.VOLUME).toLocaleString()}</strong>
        </p>
      )}
      {d.signals && d.signals.length > 0 && (
        <div className="isc-tooltip-signals">
          {d.signals.map((sig, i) => {
            const dir = sig.DETAILS?.direction || '—'
            const color = PATTERN_COLORS[sig.PATTERN_TYPE] || '#666'
            return (
              <div key={i} className="isc-tooltip-signal" style={{ borderLeftColor: color }}>
                <strong>{sig.PATTERN_NAME || sig.PATTERN_TYPE}</strong>
                <span className={`isc-dir isc-dir--${dir.toLowerCase()}`}>{dir}</span>
                {sig.SCORE != null && <span>Score: {(Number(sig.SCORE) * 100).toFixed(2)}%</span>}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

function SignalMarkerDot(props) {
  const { cx, cy, payload } = props
  if (!payload?.signals?.length || cx == null || cy == null) return null

  return payload.signals.map((sig, i) => {
    const color = PATTERN_COLORS[sig.PATTERN_TYPE] || '#666'
    const dir = sig.DETAILS?.direction
    const yOffset = dir === 'BEARISH' ? -12 - i * 10 : 12 + i * 10

    return (
      <g key={i}>
        <circle
          cx={cx}
          cy={cy + yOffset}
          r={5}
          fill={color}
          stroke="#fff"
          strokeWidth={1.5}
        />
        <text
          x={cx}
          y={cy + yOffset}
          textAnchor="middle"
          dominantBaseline="central"
          fill="#fff"
          fontSize={7}
          fontWeight="bold"
        >
          {dir === 'BEARISH' ? '▼' : '▲'}
        </text>
      </g>
    )
  })
}

export default function IntradaySignalChart() {
  const [symbol, setSymbol] = useState('')
  const [marketType, setMarketType] = useState('')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [availableSymbols, setAvailableSymbols] = useState([])
  const [days, setDays] = useState(5)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    fetch(`${API_BASE}/training/intraday/signal-chart?symbol=AAPL&market_type=STOCK&days=5`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (cancelled || !d) return
        setAvailableSymbols(d.available_symbols || [])
        if (d.available_symbols?.length > 0 && !symbol) {
          setSymbol(d.available_symbols[0].SYMBOL)
          setMarketType(d.available_symbols[0].MARKET_TYPE)
        }
        setData(d)
        setLoading(false)
      })
      .catch(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    if (!symbol || !marketType) return
    let cancelled = false
    setLoading(true)
    fetch(`${API_BASE}/training/intraday/signal-chart?symbol=${encodeURIComponent(symbol)}&market_type=${encodeURIComponent(marketType)}&days=${days}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (cancelled || !d) return
        setData(d)
        if (d.available_symbols?.length) setAvailableSymbols(d.available_symbols)
        setLoading(false)
      })
      .catch(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [symbol, marketType, days])

  const chartData = useMemo(() => {
    if (!data?.bars) return []
    const signalMap = {}
    for (const sig of (data.signals || [])) {
      const key = sig.TS
      if (!signalMap[key]) signalMap[key] = []
      signalMap[key].push(sig)
    }

    return data.bars.map(bar => {
      const ts = bar.TS
      return {
        ts,
        _isFX: data.market_type === 'FX',
        OPEN: bar.OPEN != null ? Number(bar.OPEN) : null,
        HIGH: bar.HIGH != null ? Number(bar.HIGH) : null,
        LOW: bar.LOW != null ? Number(bar.LOW) : null,
        CLOSE: bar.CLOSE != null ? Number(bar.CLOSE) : null,
        VOLUME: bar.VOLUME != null ? Number(bar.VOLUME) : null,
        signals: signalMap[ts] || [],
        hasSignal: !!signalMap[ts],
        label: ts ? new Date(ts).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '',
      }
    })
  }, [data])

  const signalSummary = useMemo(() => {
    if (!data?.signals) return {}
    const summary = {}
    for (const sig of data.signals) {
      const key = sig.PATTERN_TYPE || 'UNKNOWN'
      if (!summary[key]) summary[key] = { count: 0, name: sig.PATTERN_NAME || key }
      summary[key].count++
    }
    return summary
  }, [data])

  const priceDomain = useMemo(() => {
    if (!chartData.length) return ['auto', 'auto']
    const prices = chartData.flatMap(d => [d.HIGH, d.LOW]).filter(v => v != null)
    if (!prices.length) return ['auto', 'auto']
    const min = Math.min(...prices)
    const max = Math.max(...prices)
    const pad = Math.max((max - min) * 0.08, 0.001)
    const scale = marketType === 'FX' ? 10000 : 100
    return [Math.floor((min - pad) * scale) / scale, Math.ceil((max + pad) * scale) / scale]
  }, [chartData, marketType])

  const isFX = marketType === 'FX'

  const handleSymbolChange = (e) => {
    const val = e.target.value
    const parts = val.split('|')
    setSymbol(parts[0])
    setMarketType(parts[1] || 'STOCK')
  }

  return (
    <div className="isc-section">
      <div className="isc-header">
        <div />
        <div className="isc-controls">
          <select
            className="isc-select"
            value={`${symbol}|${marketType}`}
            onChange={handleSymbolChange}
          >
            {availableSymbols.length === 0 && <option>No signals yet</option>}
            {availableSymbols.map((s, i) => (
              <option key={i} value={`${s.SYMBOL}|${s.MARKET_TYPE}`}>
                {s.SYMBOL} ({s.MARKET_TYPE})
              </option>
            ))}
          </select>
          <select className="isc-select isc-select--small" value={days} onChange={e => setDays(Number(e.target.value))}>
            <option value={3}>3 days</option>
            <option value={5}>5 days</option>
            <option value={10}>10 days</option>
          </select>
        </div>
      </div>

      {loading && <div className="isc-loading">Loading chart...</div>}

      {!loading && chartData.length === 0 && (
        <p className="id-empty">No bar data for {symbol}. Signals will appear after the pipeline ingests bars and detects patterns.</p>
      )}

      {!loading && chartData.length > 0 && (
        <>
          <div className="isc-signal-badges">
            {Object.entries(signalSummary).map(([type, info]) => (
              <span
                key={type}
                className="isc-badge"
                style={{ borderColor: PATTERN_COLORS[type] || '#666', color: PATTERN_COLORS[type] || '#666' }}
              >
                <span className="isc-badge-dot" style={{ background: PATTERN_COLORS[type] || '#666' }} />
                {info.name}: {info.count}
              </span>
            ))}
            {Object.keys(signalSummary).length === 0 && (
              <span className="isc-badge isc-badge--muted">No signals detected yet</span>
            )}
          </div>

          <div className="isc-chart-wrap">
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 16, right: 12, left: 0, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                <XAxis
                  dataKey="label"
                  tick={{ fontSize: 9 }}
                  interval="preserveStartEnd"
                  minTickGap={60}
                />
                <YAxis
                  domain={priceDomain}
                  tick={{ fontSize: 9 }}
                  tickFormatter={v => isFX ? Number(v).toFixed(4) : `$${v}`}
                  width={isFX ? 64 : 52}
                />
                <Tooltip content={<SignalTooltip />} />

                <Line
                  type="monotone"
                  dataKey="CLOSE"
                  stroke="#37474f"
                  strokeWidth={1.5}
                  dot={<SignalMarkerDot />}
                  activeDot={{ r: 3, fill: '#37474f' }}
                  connectNulls
                  name="Close"
                />
              </ComposedChart>
            </ResponsiveContainer>
          </div>

          <div className="isc-legend">
            {Object.entries(PATTERN_COLORS).map(([type, color]) => (
              <span key={type} className="isc-legend-item">
                <span className="isc-legend-dot" style={{ background: color }} />
                {type.replace(/_/g, ' ')}
              </span>
            ))}
            <span className="isc-legend-item">
              <span className="isc-legend-arrow">▲</span> Bullish
            </span>
            <span className="isc-legend-item">
              <span className="isc-legend-arrow isc-legend-arrow--bear">▼</span> Bearish
            </span>
          </div>
        </>
      )}
    </div>
  )
}
