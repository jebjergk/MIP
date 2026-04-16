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
  PULLBACK_IN_TREND: '#a5d6a7',
  RANGE_BOUND: '#9e9e9e',
  RANGE_CONTRACTION: '#bdbdbd',
  PULLBACK_IN_DOWNTREND: '#ef9a9a',
  TREND_DOWN: '#ef5350',
  BREAKDOWN: '#c62828',
  RECOVERY: '#42a5f5',
  REVERSAL_FORMING: '#7e57c2',
  FAILED_MOVE: '#ff7043',
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

function fmtPrice(v) {
  if (v == null || isNaN(v)) return ''
  if (v >= 100) return v.toFixed(0)
  if (v >= 10) return v.toFixed(1)
  return v.toFixed(2)
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
  const dir = (payload.setupDir || '').toUpperCase()
  const hasProposal = payload.setupHasProposal
  const isLong = dir === 'LONG'
  const fill = hasProposal ? '#f9a825' : (isLong ? '#1a73e8' : '#d93025')
  const size = 5
  const points = isLong
    ? `${cx},${cy - size} ${cx - size},${cy + size} ${cx + size},${cy + size}`
    : `${cx},${cy + size} ${cx - size},${cy - size} ${cx + size},${cy - size}`
  return <polygon points={points} fill={fill} stroke="#fff" strokeWidth={1} />
}

function ProposalMarkerShape(props) {
  const { cx, cy } = props
  if (cx == null || cy == null) return null
  return (
    <g>
      <circle cx={cx} cy={cy} r={6} fill="#f9a825" stroke="#fff" strokeWidth={1.5} />
      <text x={cx} y={cy + 1} textAnchor="middle" dominantBaseline="middle" fontSize={8} fill="#fff" fontWeight="bold">P</text>
    </g>
  )
}

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload || !payload.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 4, padding: '6px 10px', fontSize: '0.8rem', maxWidth: 260 }}>
      <div style={{ fontWeight: 600 }}>{label}</div>
      {d.open != null && <div>O: {fmtPrice(d.open)} H: {fmtPrice(d.high)} L: {fmtPrice(d.low)} C: {fmtPrice(d.close)}</div>}
      {d.STRUCTURAL_STATE && <div style={{ color: '#6c757d' }}>State: {d.STRUCTURAL_STATE.replace(/_/g, ' ')}</div>}
      {d.setupEvents && d.setupEvents.length > 0 && (
        <div style={{ marginTop: 4, borderTop: '1px solid #eee', paddingTop: 4 }}>
          {d.setupEvents.map((s, i) => (
            <div key={i} style={{ fontSize: '0.75rem', color: '#333' }}>
              {(s.SETUP_FAMILY || s.setup_family || '').replace(/_/g, ' ')} ({s.DIRECTION || s.direction})
              {(s.BECAME_PROPOSAL || s.became_proposal) ? ' → Proposal' : ''}
            </div>
          ))}
        </div>
      )}
      {d.proposalEvents && d.proposalEvents.length > 0 && (
        <div style={{ marginTop: 4, color: '#e37400', fontWeight: 600, fontSize: '0.75rem' }}>
          Proposal: {(d.proposalEvents[0].SETUP_FAMILY || d.proposalEvents[0].setup_family || '').replace(/_/g, ' ')}
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

    const lastBarDate = fmtDate(get(bars[bars.length - 1], 'BAR_DATE'))

    // Attach orphan proposals (created after last bar) to the last bar
    proposals.forEach(p => {
      const d = fmtDate(get(p, 'PROPOSAL_CREATED_AT') || get(p, 'SETUP_DATE'))
      if (d > lastBarDate) {
        if (!proposalsByDate[lastBarDate]) proposalsByDate[lastBarDate] = []
        if (!proposalsByDate[lastBarDate].includes(p)) proposalsByDate[lastBarDate].push(p)
      }
    })

    return bars.map(b => {
      const date = fmtDate(get(b, 'BAR_DATE'))
      const open = Number(get(b, 'OPEN'))
      const high = Number(get(b, 'HIGH'))
      const low = Number(get(b, 'LOW'))
      const close = Number(get(b, 'CLOSE'))
      const barSetups = setupsByDate[date] || []
      const barProposals = proposalsByDate[date] || []

      // Pull direction from the first setup so the marker shape can read it
      const firstSetup = barSetups[0]
      const setupDir = firstSetup ? (get(firstSetup, 'DIRECTION') || '') : ''
      const setupHasProposal = barSetups.some(s => get(s, 'BECAME_PROPOSAL'))

      return {
        date,
        open, high, low, close,
        wick: [low, high],
        STRUCTURAL_STATE: get(b, 'STRUCTURAL_STATE'),
        VOL_REGIME: get(b, 'VOL_REGIME'),
        TREND_REGIME: get(b, 'TREND_REGIME'),
        stateColor: STATE_COLORS[get(b, 'STRUCTURAL_STATE')] || '#e0e0e0',
        stateVal: 1,
        setupMarker: barSetups.length > 0 ? (setupDir === 'LONG' ? low - (high - low) * 0.12 : high + (high - low) * 0.12) : null,
        setupDir,
        setupHasProposal,
        setupEvents: barSetups,
        proposalMarker: barProposals.length > 0 ? high + (high - low) * 0.18 : null,
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
      const price = Number(get(l, 'LEVEL_PRICE') || 0)
      const rounded = Math.round(price * 10) / 10
      const key = `${get(l, 'LEVEL_TYPE')}|${rounded}`
      const existing = seen.get(key)
      const sig = Number(get(l, 'LEVEL_SIGNIFICANCE') || 0)
      if (!existing || sig > Number(get(existing, 'LEVEL_SIGNIFICANCE') || 0)) {
        seen.set(key, l)
      }
    })

    return [...seen.values()]
      .sort((a, b) => Number(get(b, 'LEVEL_SIGNIFICANCE') || 0) - Number(get(a, 'LEVEL_SIGNIFICANCE') || 0))
      .slice(0, 12)
  }, [levels, overlays.levels, selectedSetup, get])

  // Entry zones: show for active setups when toggle is on, or for selected setup always
  const entryZoneSetups = useMemo(() => {
    const result = []
    if (overlays.entryZones) {
      setups.forEach(s => {
        const status = (get(s, 'SETUP_STATUS') || '').toUpperCase()
        if (status === 'DETECTED' || status === 'ELIGIBLE') {
          const lo = Number(get(s, 'ENTRY_ZONE_LOW'))
          const hi = Number(get(s, 'ENTRY_ZONE_HIGH'))
          if (lo && hi && lo !== hi) result.push(s)
        }
      })
    }
    if (selectedSetup && !result.find(s => get(s, 'SETUP_EVENT_ID') === selectedSetupId)) {
      result.push(selectedSetup)
    }
    return result
  }, [setups, overlays.entryZones, selectedSetup, selectedSetupId, get])

  // Invalidation lines: show for active setups when toggle on, or for selected setup
  const invalidationSetups = useMemo(() => {
    const result = []
    if (overlays.invalidationLines) {
      setups.forEach(s => {
        const status = (get(s, 'SETUP_STATUS') || '').toUpperCase()
        if (status === 'DETECTED' || status === 'ELIGIBLE') {
          const inv = Number(get(s, 'PRICE_INVALIDATION_LEVEL'))
          if (inv) result.push(s)
        }
      })
    }
    if (selectedSetup && !result.find(s => get(s, 'SETUP_EVENT_ID') === selectedSetupId)) {
      const inv = Number(get(selectedSetup, 'PRICE_INVALIDATION_LEVEL'))
      if (inv) result.push(selectedSetup)
    }
    return result
  }, [setups, overlays.invalidationLines, selectedSetup, selectedSetupId, get])

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
          <YAxis
            domain={[yMin, yMax]}
            tick={{ fontSize: 10 }}
            width={60}
            tickFormatter={fmtPrice}
          />
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

          {/* Entry zones for active setups or selected setup */}
          {entryZoneSetups.map(s => {
            const lo = Number(get(s, 'ENTRY_ZONE_LOW'))
            const hi = Number(get(s, 'ENTRY_ZONE_HIGH'))
            const isSelected = get(s, 'SETUP_EVENT_ID') === selectedSetupId
            if (!lo || !hi || lo === hi) return null
            return (
              <ReferenceArea
                key={`ez-${get(s, 'SETUP_EVENT_ID')}`}
                y1={Math.min(lo, hi)}
                y2={Math.max(lo, hi)}
                fill={isSelected ? 'rgba(26,115,232,0.18)' : 'rgba(26,115,232,0.08)'}
                stroke={isSelected ? '#1a73e8' : 'none'}
                strokeDasharray={isSelected ? '4 2' : ''}
                strokeOpacity={0.5}
              />
            )
          })}

          {/* Invalidation lines for active setups or selected setup */}
          {invalidationSetups.map(s => {
            const inv = Number(get(s, 'PRICE_INVALIDATION_LEVEL'))
            const isSelected = get(s, 'SETUP_EVENT_ID') === selectedSetupId
            if (!inv) return null
            return (
              <ReferenceLine
                key={`inv-${get(s, 'SETUP_EVENT_ID')}`}
                y={inv}
                stroke="#d93025"
                strokeDasharray="6 3"
                strokeWidth={isSelected ? 1.5 : 0.8}
                strokeOpacity={isSelected ? 1 : 0.4}
                label={isSelected ? { value: 'Invalidation', position: 'right', fontSize: 10, fill: '#d93025' } : undefined}
              />
            )
          })}

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
