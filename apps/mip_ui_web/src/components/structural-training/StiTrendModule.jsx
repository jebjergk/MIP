import React, { useState } from 'react'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from 'recharts'
import InfoTooltip from '../InfoTooltip'

const SCOPE = 'structural_training'

const METRICS = [
  { key: 'MEANINGFUL_HIT_RATE', label: 'Meaningful Hit Rate', color: '#1565c0', fmt: pctFmt },
  { key: 'PATH_SURVIVAL_RATE', label: 'Path Survival', color: '#198754', fmt: pctFmt },
  { key: 'AVG_MFE_MAE_RATIO', label: 'MFE/MAE Ratio', color: '#fd7e14', fmt: ratioFmt },
]

function pctFmt(v) { return v != null ? `${(v * 100).toFixed(1)}%` : '—' }
function ratioFmt(v) { return v != null ? Number(v).toFixed(2) : '—' }

export default function StiTrendModule({ data, family }) {
  const [primaryMetric, setPrimaryMetric] = useState(0)

  if (!data?.length) return <p style={{ color: '#6c757d', fontSize: '0.85rem' }}>No trend data available for this family. Trends require at least 3 setups per 60-day window.</p>

  const g = (r, k) => r[k] ?? r[k.toUpperCase()] ?? r[k.toLowerCase()]

  const chartData = data.map(d => ({
    period: String(g(d, 'PERIOD_END') || '').slice(0, 10),
    mhr: g(d, 'MEANINGFUL_HIT_RATE'),
    ps: g(d, 'PATH_SURVIVAL_RATE'),
    ratio: g(d, 'AVG_MFE_MAE_RATIO'),
    n: g(d, 'N_SETUPS'),
  }))

  const primary = METRICS[primaryMetric]

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        Trend Over Time
        <InfoTooltip scope={SCOPE} entryKey="trend_over_time" variant="short" />
      </div>

      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.5rem' }}>
        {METRICS.map((m, i) => (
          <button
            key={m.key}
            className={`sti-detail-tab${i === primaryMetric ? ' sti-detail-tab--active' : ''}`}
            onClick={() => setPrimaryMetric(i)}
            style={{ fontSize: '0.75rem', padding: '0.3rem 0.6rem' }}
          >
            {m.label}
          </button>
        ))}
      </div>

      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={chartData} margin={{ top: 5, right: 15, bottom: 5, left: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e9ecef" />
          <XAxis dataKey="period" tick={{ fontSize: 11 }} />
          <YAxis
            tick={{ fontSize: 11 }}
            domain={primary.key === 'AVG_MFE_MAE_RATIO' ? [0, 'auto'] : [0, 1]}
            tickFormatter={primary.fmt}
          />
          <Tooltip
            formatter={(v, name) => {
              const m = METRICS.find(x => x.label === name)
              return [m ? m.fmt(v) : v, name]
            }}
            labelFormatter={l => `Period ending: ${l}`}
          />
          <Legend />
          {primaryMetric === 0 && <Line type="monotone" dataKey="mhr" name="Meaningful Hit Rate" stroke="#1565c0" dot={{ r: 3 }} strokeWidth={2} />}
          {primaryMetric === 1 && <Line type="monotone" dataKey="ps" name="Path Survival" stroke="#198754" dot={{ r: 3 }} strokeWidth={2} />}
          {primaryMetric === 2 && <Line type="monotone" dataKey="ratio" name="MFE/MAE Ratio" stroke="#fd7e14" dot={{ r: 3 }} strokeWidth={2} />}
        </LineChart>
      </ResponsiveContainer>

      <p style={{ fontSize: '0.78rem', color: '#6c757d', marginTop: '0.3rem' }}>
        Each point covers a rolling 60-day window. Points require at least 3 setups to appear.
      </p>
    </div>
  )
}
