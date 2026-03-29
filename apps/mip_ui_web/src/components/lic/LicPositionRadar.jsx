import {
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  ResponsiveContainer,
  Tooltip,
} from 'recharts'

function bandStroke(finalRecommendation) {
  const b = String(finalRecommendation || 'STAY_COURSE').toUpperCase()
  if (b === 'EXIT_NOW') return '#f87171'
  if (b === 'PREPARE_EXIT') return '#fbbf24'
  if (b === 'WATCH_CLOSELY') return '#38bdf8'
  return '#0ea5e9'
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

/**
 * Fixed-size position health radar; no axis text on spokes (tooltip only).
 */
export default function LicPositionRadar({ data, finalRecommendation }) {
  const stroke = bandStroke(finalRecommendation)
  const fill = stroke

  return (
    <div className="lic-position-radar-chart" aria-label="Position health radar">
      <ResponsiveContainer width="100%" height="100%">
        <RadarChart cx="50%" cy="50%" outerRadius="72%" data={data} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
          <PolarGrid stroke="#334155" strokeOpacity={0.6} />
          <PolarAngleAxis dataKey="metric" tick={false} />
          <PolarRadiusAxis angle={90} domain={[0, 100]} tick={false} axisLine={false} />
          <Tooltip content={<RadarTooltip />} />
          <Radar
            name="Position"
            dataKey="score"
            stroke={stroke}
            fill={fill}
            fillOpacity={0.32}
            strokeWidth={1.6}
            dot={false}
            isAnimationActive
            animationDuration={450}
          />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  )
}
