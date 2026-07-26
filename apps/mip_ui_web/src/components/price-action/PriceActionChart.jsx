import { useMemo } from 'react'
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceArea,
  ReferenceDot,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { buildChartData } from './priceActionModel'

function formatPrice(value) {
  const number = Number(value)
  if (!Number.isFinite(number)) return '—'
  return number >= 100 ? number.toFixed(2) : number.toFixed(3)
}

function CandlestickShape({ x, y, width, height, payload }) {
  if ([x, y, width, height].some((value) => value == null) || !payload) return null
  const { open, close, high, low } = payload
  if ([open, close, high, low].some((value) => value == null)) return null
  const centre = x + width / 2
  const bottom = y + Math.max(height, 1)
  const range = Math.max(high - low, 1e-12)
  const openY = y + ((high - open) / range) * (bottom - y)
  const closeY = y + ((high - close) / range) * (bottom - y)
  const bodyTop = Math.min(openY, closeY)
  const bodyHeight = Math.max(Math.abs(openY - closeY), 1)
  const bodyWidth = Math.max(Math.min(width * 0.76, 8), 3)
  const colour = close >= open ? '#0f9d83' : '#db5a5a'

  return (
    <g>
      <line x1={centre} x2={centre} y1={y} y2={bottom} stroke="#64748b" strokeWidth={1} />
      <rect
        x={centre - bodyWidth / 2}
        y={bodyTop}
        width={bodyWidth}
        height={bodyHeight}
        fill={colour}
        stroke={colour}
      />
    </g>
  )
}

function SwingShape({ cx, cy, payload }) {
  if (cx == null || cy == null) return null
  const isLow = /^(HL|LL|LOW)/i.test(payload?.label || '')
  return (
    <g>
      <circle cx={cx} cy={cy} r={4} fill="#334155" stroke="#fff" strokeWidth={1} />
      <text
        x={cx + 6}
        y={cy + (isLow ? 12 : -6)}
        fill="#334155"
        fontSize={9}
        fontWeight={700}
      >
        {payload?.label}
      </text>
    </g>
  )
}

function NoteShape({ cx, cy }) {
  if (cx == null || cy == null) return null
  return (
    <g>
      <circle cx={cx} cy={cy} r={7} fill="#7c3aed" stroke="#fff" strokeWidth={1.5} />
      <text x={cx} y={cy + 3} textAnchor="middle" fill="#fff" fontSize={9} fontWeight={800}>M</text>
    </g>
  )
}

function ChartTooltip({ active, payload, label, annotationsByDate }) {
  if (!active || !payload?.length) return null
  const bar = payload[0]?.payload
  const annotations = annotationsByDate.get(label) || []
  return (
    <div className="paa-chart-tooltip">
      <strong>{label}</strong>
      {bar?.open != null && (
        <div>O {formatPrice(bar.open)} · H {formatPrice(bar.high)} · L {formatPrice(bar.low)} · C {formatPrice(bar.close)}</div>
      )}
      {bar?.ema20 != null && <div>EMA20 {formatPrice(bar.ema20)}</div>}
      {annotations.map((annotation) => (
        <div className="paa-chart-tooltip__annotation" key={annotation.id}>
          <span>{annotation.label || annotation.text}</span>
          <em>{annotation.provenance}</em>
        </div>
      ))}
    </div>
  )
}

function AnnotationDetails({ swings, zones, ranges, notes }) {
  const items = [
    ...swings.map((item) => ({ ...item, kind: 'Swing', value: `${item.date} · ${formatPrice(item.price)}` })),
    ...zones.map((item) => ({ ...item, kind: 'Zone', value: `${formatPrice(item.low)}–${formatPrice(item.high)}` })),
    ...ranges.map((item) => ({ ...item, kind: 'Range', value: `${formatPrice(item.low)}–${formatPrice(item.high)}` })),
    ...notes.map((item) => ({ ...item, kind: 'Note', label: item.text, value: item.date || 'Methodologist read' })),
  ]
  if (!items.length) return null

  return (
    <details className="paa-annotation-details">
      <summary>Annotation details and provenance ({items.length})</summary>
      <div className="paa-annotation-list">
        {items.map((item) => (
          <div className="paa-annotation-row" key={`${item.kind}-${item.id}`}>
            <span className="paa-annotation-kind">{item.kind}</span>
            <span>
              <strong>{item.label}</strong>
              <small>{item.value}{item.detail ? ` · ${item.detail}` : ''}</small>
            </span>
            <code>{item.provenance}</code>
          </div>
        ))}
      </div>
    </details>
  )
}

export default function PriceActionChart({ analysis }) {
  const chartData = useMemo(() => buildChartData(analysis.candles), [analysis.candles])
  const dates = useMemo(() => chartData.map((bar) => bar.date), [chartData])
  const firstDate = dates[0]
  const lastDate = dates[dates.length - 1]
  const dateSet = useMemo(() => new Set(dates), [dates])

  const snapDate = (date, fallback) => {
    if (!date) return fallback
    if (dateSet.has(date)) return date
    return dates.reduce((closest, candidate) => (
      Math.abs(new Date(candidate) - new Date(date)) < Math.abs(new Date(closest) - new Date(date))
        ? candidate
        : closest
    ), fallback)
  }

  const swings = analysis.swings
    .map((swing) => ({ ...swing, date: snapDate(swing.date, firstDate) }))
    .filter((swing) => swing.date)
  const notes = analysis.notes
    .filter((note) => note.date && note.price !== null)
    .map((note) => ({ ...note, date: snapDate(note.date, firstDate) }))
  const currentPrice = analysis.currentPrice ?? chartData[chartData.length - 1]?.close
  const allPrices = chartData.flatMap((bar) => [bar.low, bar.high, bar.ema20]).filter(Number.isFinite)
  analysis.zones.forEach((zone) => allPrices.push(zone.low, zone.high))
  analysis.ranges.forEach((range) => allPrices.push(range.low, range.high))
  if (Number.isFinite(currentPrice)) allPrices.push(currentPrice)
  const minPrice = Math.min(...allPrices)
  const maxPrice = Math.max(...allPrices)
  const padding = Math.max((maxPrice - minPrice) * 0.06, maxPrice * 0.003)

  const annotationsByDate = useMemo(() => {
    const map = new Map()
    ;[...swings, ...notes].forEach((item) => {
      if (!map.has(item.date)) map.set(item.date, [])
      map.get(item.date).push(item)
    })
    return map
  }, [swings, notes])

  if (!chartData.length) {
    return <div className="paa-empty-chart">No daily candles were returned for this analysis.</div>
  }

  return (
    <section className="paa-card paa-chart-card">
      <div className="paa-section-heading">
        <div>
          <span className="paa-eyebrow">Detected geometry</span>
          <h2>Annotated daily price action</h2>
        </div>
        <div className="paa-chart-legend" aria-label="Chart legend">
          <span><i className="paa-legend-support" /> Support</span>
          <span><i className="paa-legend-resistance" /> Resistance</span>
          <span><i className="paa-legend-range" /> Range</span>
          <span><i className="paa-legend-ema" /> EMA20</span>
        </div>
      </div>

      <div className="paa-chart">
        <ResponsiveContainer width="100%" height={450}>
          <ComposedChart data={chartData} margin={{ top: 12, right: 22, bottom: 4, left: 4 }}>
            <CartesianGrid stroke="#e8edf4" strokeDasharray="3 4" />
            <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#64748b' }} interval="preserveStartEnd" />
            <YAxis
              domain={[minPrice - padding, maxPrice + padding]}
              width={66}
              tick={{ fontSize: 10, fill: '#64748b' }}
              tickFormatter={formatPrice}
            />
            <Tooltip content={<ChartTooltip annotationsByDate={annotationsByDate} />} />

            {analysis.ranges.map((range) => (
              <ReferenceArea
                key={range.id}
                x1={snapDate(range.startDate, firstDate)}
                x2={snapDate(range.endDate, lastDate)}
                y1={range.low}
                y2={range.high}
                fill="rgba(100,116,139,.14)"
                stroke="#64748b"
                strokeDasharray="5 4"
              />
            ))}
            {analysis.zones.map((zone) => {
              const resistance = zone.type === 'resistance'
              return (
                <ReferenceArea
                  key={zone.id}
                  x1={snapDate(zone.startDate, firstDate)}
                  x2={snapDate(zone.endDate, lastDate)}
                  y1={zone.low}
                  y2={zone.high}
                  fill={resistance ? 'rgba(219,90,90,.12)' : 'rgba(15,157,131,.12)'}
                  stroke={resistance ? '#db5a5a' : '#0f9d83'}
                  strokeOpacity={0.55}
                />
              )
            })}

            <Bar dataKey="wick" barSize={7} shape={<CandlestickShape />} isAnimationActive={false} />
            <Line type="monotone" dataKey="ema20" stroke="#d97706" strokeWidth={1.7} dot={false} isAnimationActive={false} />
            {Number.isFinite(currentPrice) && (
              <ReferenceLine
                y={currentPrice}
                stroke="#2563eb"
                strokeDasharray="4 3"
                label={{ value: `Current ${formatPrice(currentPrice)}`, position: 'insideTopRight', fill: '#2563eb', fontSize: 10 }}
              />
            )}
            {swings.map((swing) => (
              <ReferenceDot
                key={swing.id}
                x={swing.date}
                y={swing.price}
                r={0}
                ifOverflow="visible"
                shape={(props) => <SwingShape {...props} payload={swing} />}
              />
            ))}
            {notes.map((note) => (
              <ReferenceDot
                key={note.id}
                x={note.date}
                y={note.price}
                r={0}
                ifOverflow="visible"
                shape={(props) => <NoteShape {...props} />}
              />
            ))}
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <AnnotationDetails
        swings={analysis.swings}
        zones={analysis.zones}
        ranges={analysis.ranges}
        notes={analysis.notes}
      />
    </section>
  )
}
