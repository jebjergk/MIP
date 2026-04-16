import React, { useMemo } from 'react'
import {
  ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Scatter, ReferenceLine,
  ReferenceArea, Cell,
} from 'recharts'

const STATE_COLORS = {
  BREAKOUT_EXPANSION: '#26a69a',
  TREND_UP: '#66bb6a',
  PULLBACK_IN_UPTREND: '#a5d6a7',
  RANGE_BOUND: '#9e9e9e',
  RANGE_CONTRACTION: '#bdbdbd',
  PULLBACK_IN_DOWNTREND: '#ef9a9a',
  TREND_DOWN: '#ef5350',
  BREAKDOWN: '#c62828',
  RECOVERY: '#42a5f5',
}

const LEVEL_COLORS = {
  SUPPORT: '#26a69a',
  RESISTANCE: '#ef5350',
  SUPPORT_ZONE: 'rgba(38,166,154,0.10)',
  RESISTANCE_ZONE: 'rgba(239,83,80,0.10)',
}

function fmtDate(d) {
  if (!d) return ''
  return String(d).slice(0, 10)
}

function CandlestickShape(props) {
  const { x, y, width, height, payload } = props
  if (x == null || y == null || width == null || height == null || !payload) return null
  const { open, high, low, close } = payload
  if ([high, low, open, close].some(v => v == null)) return null

  const cx = x + width / 2
  const wickTop = y
  const wickBottom = y + Math.max(height, 1)
  const valueRange = high - low
  const isUp = close >= open
  const color = isUp ? '#26a69a' : '#ef5350'

  let openY = wickBottom, closeY = wickBottom
  if (Math.abs(valueRange) > 1e-12) {
    openY = wickTop + ((high - open) / valueRange) * (wickBottom - wickTop)
    closeY = wickTop + ((high - close) / valueRange) * (wickBottom - wickTop)
  }
  const bodyTop = Math.min(openY, closeY)
  const bodyHeight = Math.max(Math.abs(openY - closeY), 1)
  const bodyWidth = Math.max(Math.min(width * 0.78, 8), 4)
  const bodyLeft = cx - bodyWidth / 2

  return (
    <g>
      <line x1={cx} x2={cx} y1={wickTop} y2={wickBottom} stroke="#888" strokeWidth={1} shapeRendering="crispEdges" />
      <rect x={bodyLeft} y={bodyTop} width={bodyWidth} height={bodyHeight} fill={color} stroke={color} strokeWidth={1} shapeRendering="crispEdges" />
    </g>
  )
}

function SetupMarkerShape(props) {
  const { cx, cy, payload } = props
  if (cx == null || cy == null || !payload) return null
  const dir = (payload.DIRECTION || payload.direction || '').toUpperCase()
  const isProposal = payload.BECAME_PROPOSAL || payload.became_proposal
  const isLong = dir === 'LONG'
  const fill = isProposal ? '#f9a825' : (isLong ? '#1a73e8' : '#d93025')
  const size = 5
  const points = isLong
    ? `${cx},${cy - size} ${cx - size},${cy + size} ${cx + size},${cy + size}`
    : `${cx},${cy + size} ${cx - size},${cy - size} ${cx + size},${cy - size}`
  return <polygon points={points} fill={fill} stroke="#fff" strokeWidth={1} />
}

function ProposalMarkerShape(props) {
  const { cx, cy } = props
  if (cx == null || cy == null) return null
  return <circle cx={cx} cy={cy} r={5} fill="#f9a825" stroke="#fff" strokeWidth={1.5} />
}

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload || !payload.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 4, padding: '6px 10px', fontSize: '0.8rem', maxWidth: 260 }}>
      <div style={{ fontWeight: 600 }}>{label}</div>
      {d.open != null && <div>O: {d.open?.toFixed(2)} H: {d.high?.toFixed(2)} L: {d.low?.toFixed(2)} C: {d.close?.toFixed(2)}</div>}
      {d.STRUCTURAL_STATE && <div style={{ color: '#6c757d' }}>State: {d.STRUCTURAL_STATE.replace(/_/g, ' ')}</div>}
      {d.setupEvents && d.setupEvents.length > 0 && (
        <div style={{ marginTop: 4, borderTop: '1px solid #eee', paddingTop: 4 }}>
          {d.setupEvents.map((s, i) => (
            <div key={i} style={{ fontSize: '0.75rem', color: '#333' }}>
              {s.SETUP_FAMILY?.replace(/_/g, ' ')} ({s.DIRECTION})
              {s.BECAME_PROPOSAL ? ' → Proposal' : ''}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function StlChart({
  bars, levels, setups, proposals, overlays,
  chartMode, selectedSetupId, onSelectSetup, get,
}) {
  const chartData = useMemo(() => {
    if (!bars.length) return []

    const setupsByDate = {}
    setups.forEach(s => {
      const d = fmtDate(get(s, 'SETUP_DATE'))
      if (!setupsByDate[d]) setupsByDate[d] = []
      setupsByDate[d].push(s)
    })

    const proposalsByDate = {}
    proposals.forEach(p => {
      const d = fmtDate(get(p, 'PROPOSAL_CREATED_AT') || get(p, 'SETUP_DATE'))
      if (!proposalsByDate[d]) proposalsByDate[d] = []
      proposalsByDate[d].push(p)
    })

    return bars.map(b => {
      const date = fmtDate(get(b, 'BAR_DATE'))
      const open = Number(get(b, 'OPEN'))
      const high = Number(get(b, 'HIGH'))
      const low = Number(get(b, 'LOW'))
      const close = Number(get(b, 'CLOSE'))
      const barSetups = setupsByDate[date] || []
      const barProposals = proposalsByDate[date] || []

      return {
        date,
        open, high, low, close,
        wick: [low, high],
        STRUCTURAL_STATE: get(b, 'STRUCTURAL_STATE'),
        VOL_REGIME: get(b, 'VOL_REGIME'),
        TREND_REGIME: get(b, 'TREND_REGIME'),
        stateColor: STATE_COLORS[get(b, 'STRUCTURAL_STATE')] || '#e0e0e0',
        stateVal: 1,
        setupMarker: barSetups.length > 0 ? low - (high - low) * 0.08 : null,
        setupEvents: barSetups,
        proposalMarker: barProposals.length > 0 ? high + (high - low) * 0.08 : null,
        proposalEvents: barProposals,
      }
    })
  }, [bars, setups, proposals, get])

  const selectedSetup = useMemo(() => {
    if (!selectedSetupId) return null
    return setups.find(s => get(s, 'SETUP_EVENT_ID') === selectedSetupId)
  }, [setups, selectedSetupId, get])

  const visibleLevels = useMemo(() => {
    if (!overlays.levels && !selectedSetup) return []
    if (!levels.length) return []

    const seen = new Map()
    levels.forEach(l => {
      const key = `${get(l, 'LEVEL_TYPE')}|${get(l, 'LEVEL_PRICE')}`
      const existing = seen.get(key)
      if (!existing || (get(l, 'AS_OF_DATE') || '') > (get(existing, 'AS_OF_DATE') || '')) {
        seen.set(key, l)
      }
    })
    return [...seen.values()]
  }, [levels, overlays.levels, selectedSetup, get])

  if (!chartData.length) return null

  const yValues = chartData.flatMap(d => [d.high, d.low].filter(v => v != null && !isNaN(v)))
  const yMin = Math.min(...yValues) * 0.995
  const yMax = Math.max(...yValues) * 1.005

  return (
    <div className="stl-chart-wrap">
      <div className="stl-chart-legend">
        <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#1a73e8' }} /> Long setup</span>
        <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#d93025' }} /> Short setup</span>
        <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#f9a825', borderRadius: '50%' }} /> Proposal</span>
        {overlays.levels && <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#26a69a' }} /> Support</span>}
        {overlays.levels && <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#ef5350' }} /> Resistance</span>}
      </div>

      <ResponsiveContainer width="100%" height={420}>
        <ComposedChart data={chartData} margin={{ top: 10, right: 10, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="date" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
          <YAxis domain={[yMin, yMax]} tick={{ fontSize: 10 }} width={60} />
          <Tooltip content={<ChartTooltip />} />

          {/* S/R levels — horizontal lines, historically faithful */}
          {(overlays.levels || selectedSetup) && visibleLevels.map(l => {
            const price = Number(get(l, 'LEVEL_PRICE'))
            const lType = (get(l, 'LEVEL_TYPE') || '').toUpperCase()
            const color = lType.includes('RESIST') ? '#ef5350' : '#26a69a'
            return (
              <ReferenceLine
                key={get(l, 'LEVEL_ID') || `${lType}-${price}`}
                y={price}
                stroke={color}
                strokeDasharray="4 3"
                strokeOpacity={0.6}
              />
            )
          })}

          {/* S/R zones */}
          {overlays.zones && visibleLevels.filter(l => get(l, 'LEVEL_LOW') && get(l, 'LEVEL_HIGH')).map(l => {
            const lType = (get(l, 'LEVEL_TYPE') || '').toUpperCase()
            const fill = lType.includes('RESIST') ? LEVEL_COLORS.RESISTANCE_ZONE : LEVEL_COLORS.SUPPORT_ZONE
            return (
              <ReferenceArea
                key={`zone-${get(l, 'LEVEL_ID')}`}
                y1={Number(get(l, 'LEVEL_LOW'))}
                y2={Number(get(l, 'LEVEL_HIGH'))}
                fill={fill}
                strokeOpacity={0}
              />
            )
          })}

          {/* Selected setup: entry zone + invalidation */}
          {selectedSetup && overlays.entryZones !== false && (
            <ReferenceArea
              y1={Number(get(selectedSetup, 'ENTRY_ZONE_LOW'))}
              y2={Number(get(selectedSetup, 'ENTRY_ZONE_HIGH'))}
              fill="rgba(26,115,232,0.12)"
              strokeOpacity={0}
            />
          )}
          {selectedSetup && (
            <ReferenceLine
              y={Number(get(selectedSetup, 'PRICE_INVALIDATION_LEVEL'))}
              stroke="#d93025"
              strokeDasharray="6 3"
              strokeWidth={1.5}
              label={{ value: 'Invalidation', position: 'right', fontSize: 10, fill: '#d93025' }}
            />
          )}

          {/* Price series */}
          {chartMode === 'candle' ? (
            <Bar dataKey="wick" barSize={6} shape={<CandlestickShape />} isAnimationActive={false} />
          ) : (
            <>
              <Line type="monotone" dataKey="close" stroke="#3366cc" strokeWidth={1.5} dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="high" stroke="#ccc" strokeWidth={0.5} dot={false} strokeDasharray="2 2" isAnimationActive={false} />
              <Line type="monotone" dataKey="low" stroke="#ccc" strokeWidth={0.5} dot={false} strokeDasharray="2 2" isAnimationActive={false} />
            </>
          )}

          {/* Setup markers */}
          {overlays.setupMarkers && (
            <Scatter
              dataKey="setupMarker"
              shape={<SetupMarkerShape />}
              isAnimationActive={false}
              onClick={(data) => {
                const evts = data?.setupEvents || data?.payload?.setupEvents
                if (evts && evts.length > 0) {
                  const id = get(evts[0], 'SETUP_EVENT_ID')
                  if (id) onSelectSetup(id)
                }
              }}
            />
          )}

          {/* Proposal markers */}
          {overlays.proposalMarkers && (
            <Scatter dataKey="proposalMarker" shape={<ProposalMarkerShape />} isAnimationActive={false} />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {/* State strip */}
      {overlays.stateStrip && (
        <div className="stl-state-strip">
          <ResponsiveContainer width="100%" height={28}>
            <ComposedChart data={chartData} margin={{ top: 0, right: 10, bottom: 0, left: 0 }}>
              <XAxis dataKey="date" hide />
              <Tooltip
                content={({ active, payload }) => {
                  if (!active || !payload || !payload.length) return null
                  const d = payload[0]?.payload
                  return d ? (
                    <div style={{ background: '#fff', border: '1px solid #eee', borderRadius: 3, padding: '3px 8px', fontSize: '0.75rem' }}>
                      {d.date}: {(d.STRUCTURAL_STATE || '—').replace(/_/g, ' ')}
                    </div>
                  ) : null
                }}
              />
              <Bar dataKey="stateVal" isAnimationActive={false} barSize={4}>
                {chartData.map((d, i) => (
                  <Cell key={i} fill={d.stateColor} />
                ))}
              </Bar>
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Regime strip */}
      {overlays.regimeStrip && (
        <div style={{ fontSize: '0.72rem', color: '#6c757d', padding: '2px 8px', display: 'flex', gap: 12 }}>
          <span>Latest regime: {chartData.length > 0 ? (chartData[chartData.length - 1].VOL_REGIME || '—').replace(/_/g, ' ') : '—'}</span>
          <span>Trend: {chartData.length > 0 ? (chartData[chartData.length - 1].TREND_REGIME || '—').replace(/_/g, ' ') : '—'}</span>
        </div>
      )}
    </div>
  )
}
