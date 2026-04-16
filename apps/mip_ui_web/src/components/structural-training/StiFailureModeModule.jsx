import React, { useMemo } from 'react'
import InfoTooltip from '../InfoTooltip'

const SCOPE = 'structural_training'

const FAILURE_COLORS = {
  SUCCESS: '#198754',
  IMMEDIATE_FAILURE: '#dc3545',
  STRUCTURAL_BREAK: '#a11d33',
  WICK_FAILURE: '#fd7e14',
  REVERSAL_AFTER_CONFIRMATION: '#ffc107',
  LATE_FADE: '#adb5bd',
  NO_MOVE: '#dee2e6',
}

const FAILURE_DESCRIPTIONS = {
  SUCCESS: 'Setup reached its meaningful move target.',
  IMMEDIATE_FAILURE: 'Price hit invalidation within 1-2 bars — the setup failed immediately.',
  STRUCTURAL_BREAK: 'Structural invalidation was hit without the setup ever confirming directionally.',
  WICK_FAILURE: 'Invalidation was hit, but the setup had initially moved in the right direction before reversing.',
  REVERSAL_AFTER_CONFIRMATION: 'Setup confirmed directionally, then reversed and hit invalidation.',
  LATE_FADE: 'Price moved in the right direction but never reached the meaningful move threshold.',
  NO_MOVE: 'Price did not move meaningfully in either direction during the evaluation window.',
}

export default function StiFailureModeModule({ data, bestWindow }) {
  const filtered = useMemo(() => {
    if (!data?.length) return []
    const bw = bestWindow ?? 20
    return data.filter(d => {
      const ew = d.EVAL_WINDOW ?? d.eval_window
      return ew === bw || ew == null
    })
  }, [data, bestWindow])

  const total = filtered.reduce((s, d) => s + Number(d.CNT ?? d.cnt ?? 0), 0)

  if (!filtered.length) return <p style={{ color: '#6c757d', fontSize: '0.85rem' }}>No failure mode data available.</p>

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        Failure Mode Distribution
        <InfoTooltip scope={SCOPE} entryKey="failure_mode" variant="short" />
      </div>

      {/* Stacked bar */}
      <div className="sti-failure-bar">
        {filtered.map(d => {
          const mode = d.FAILURE_MODE ?? d.failure_mode
          const cnt = Number(d.CNT ?? d.cnt ?? 0)
          const pct = total > 0 ? (cnt / total) * 100 : 0
          return (
            <div
              key={mode}
              className="sti-failure-segment"
              style={{ flex: pct, background: FAILURE_COLORS[mode] || '#6c757d' }}
              title={`${mode}: ${cnt} (${pct.toFixed(1)}%)\n${FAILURE_DESCRIPTIONS[mode] || ''}`}
            />
          )
        })}
      </div>

      {/* Legend */}
      <div className="sti-failure-legend">
        {filtered.map(d => {
          const mode = d.FAILURE_MODE ?? d.failure_mode
          const cnt = Number(d.CNT ?? d.cnt ?? 0)
          const pctVal = Number(d.PCT ?? d.pct ?? 0)
          return (
            <span key={mode} title={FAILURE_DESCRIPTIONS[mode]}>
              <span className="sti-failure-legend-dot" style={{ background: FAILURE_COLORS[mode] || '#6c757d' }} />
              {mode.replace(/_/g, ' ')}: {cnt} ({pctVal.toFixed(1)}%)
            </span>
          )
        })}
      </div>

      <p style={{ fontSize: '0.82rem', color: '#555', marginTop: '0.5rem' }}>
        {failureNarrative(filtered, total)}
      </p>
    </div>
  )
}

function failureNarrative(data, total) {
  if (!data.length || total === 0) return ''
  const successRow = data.find(d => (d.FAILURE_MODE ?? d.failure_mode) === 'SUCCESS')
  const successPct = successRow ? (Number(successRow.CNT ?? successRow.cnt ?? 0) / total * 100) : 0
  const topFailure = data.filter(d => (d.FAILURE_MODE ?? d.failure_mode) !== 'SUCCESS').sort((a, b) => Number(b.CNT ?? b.cnt ?? 0) - Number(a.CNT ?? a.cnt ?? 0))[0]
  const parts = [`${successPct.toFixed(0)}% of setups succeeded.`]
  if (topFailure) {
    const mode = (topFailure.FAILURE_MODE ?? (topFailure.failure_mode || '')).replace(/_/g, ' ').toLowerCase()
    parts.push(`Most common failure: ${mode}.`)
  }
  return parts.join(' ')
}
