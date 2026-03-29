import {
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  ResponsiveContainer,
  Tooltip,
} from 'recharts'

function bandPalette(finalRecommendation) {
  const b = String(finalRecommendation || 'STAY_COURSE').toUpperCase()
  if (b === 'EXIT_NOW') return { stroke: '#ef4444', fill: '#ef4444' }
  if (b === 'PREPARE_EXIT') return { stroke: '#f97316', fill: '#f97316' }
  if (b === 'WATCH_CLOSELY') return { stroke: '#ca8a04', fill: '#eab308' }
  return { stroke: '#14b8a6', fill: '#0ea5e9' }
}

function RadarTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const row = payload[0]?.payload
  if (!row) return null
  return (
    <div
      style={{
        background: '#1e293b',
        border: '1px solid #334155',
        borderRadius: 8,
        padding: '8px 10px',
        fontSize: 11,
        color: '#f1f5f9',
        maxWidth: 240,
      }}
    >
      <div style={{ fontWeight: 700, marginBottom: 4 }}>{row.metric}</div>
      <div style={{ color: '#94a3b8', fontVariantNumeric: 'tabular-nums' }}>Score: {row.score}</div>
    </div>
  )
}

function angleAxisTick(props) {
  const { x, y, payload, textAnchor } = props
  if (x == null || y == null || !payload) return null
  return (
    <text
      x={x}
      y={y}
      dy={6}
      textAnchor={textAnchor || 'middle'}
      fill="#94a3b8"
      fontSize={9}
      fontWeight={700}
      letterSpacing="0.04em"
    >
      {payload.value}
    </text>
  )
}

/**
 * Fixed-size radar: short spoke labels outside, ideal hexagon at 100%, fill by recommendation band.
 */
export default function LicPositionRadar({ data, finalRecommendation, hint }) {
  const { stroke, fill } = bandPalette(finalRecommendation)

  return (
    <div className="lic-position-radar-wrap">
      {hint ? (
        <div className="lic-radar-hint" title={hint}>
          {hint}
        </div>
      ) : null}
      <div className="lic-position-radar-chart" aria-label="Position health radar">
        <ResponsiveContainer width="100%" height="100%">
          <RadarChart
            cx="50%"
            cy="50%"
            outerRadius="68%"
            data={data}
            margin={{ top: 14, right: 20, bottom: 14, left: 20 }}
          >
            <PolarGrid stroke="#475569" strokeOpacity={0.45} />
            <PolarAngleAxis dataKey="shortLabel" tickLine={false} tick={angleAxisTick} />
            <PolarRadiusAxis angle={90} domain={[0, 100]} tick={false} axisLine={false} />
            <Tooltip content={<RadarTooltip />} />
            <Radar
              name="Ideal"
              dataKey="ideal"
              stroke="#64748b"
              fill="#64748b"
              fillOpacity={0.08}
              strokeOpacity={0.5}
              strokeWidth={1}
              strokeDasharray="5 4"
              dot={false}
              isAnimationActive={false}
            />
            <Radar
              name="Position"
              dataKey="score"
              stroke={stroke}
              fill={fill}
              fillOpacity={0.5}
              strokeWidth={2.35}
              dot={false}
              isAnimationActive
              animationDuration={450}
            />
          </RadarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
