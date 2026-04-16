import React from 'react'
import InfoTooltip from '../InfoTooltip'

const SCOPE = 'structural_training'

export default function StiStateRegimeModule({ row, get }) {
  const dominantState = get(row, 'DOMINANT_STATE')
  const regimeFit = Number(get(row, 'REGIME_FIT_GOOD_PCT') ?? 0)
  const riskClass = get(row, 'DOMINANT_RISK_CLASS')

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        State & Regime Compatibility
        <InfoTooltip scope={SCOPE} entryKey="state_compatibility" variant="short" />
      </div>

      <div className="sti-metric-grid" style={{ marginBottom: '0.8rem' }}>
        <div className="sti-metric-card">
          <div className="sti-metric-label">Dominant State <InfoTooltip scope={SCOPE} entryKey="structural_state" variant="short" /></div>
          <div className="sti-metric-value" style={{ fontSize: '0.88rem' }}>
            <span className="sti-badge" style={{ background: '#f5e6c8', color: '#7d5a00' }}>{dominantState || '—'}</span>
          </div>
        </div>

        <div className="sti-metric-card">
          <div className="sti-metric-label">Regime Fit (GOOD %) <InfoTooltip scope={SCOPE} entryKey="regime_fit" variant="short" /></div>
          <div className="sti-metric-value">{(regimeFit * 100).toFixed(1)}%</div>
          <div className="sti-metric-bar">
            <div
              className="sti-metric-bar-fill"
              style={{
                width: `${regimeFit * 100}%`,
                background: regimeFit >= 0.5 ? '#198754' : regimeFit >= 0.3 ? '#b8860b' : '#dc3545',
              }}
            />
          </div>
        </div>

        <div className="sti-metric-card">
          <div className="sti-metric-label">Risk Class</div>
          <div className="sti-metric-value" style={{ fontSize: '0.88rem' }}>
            {riskClass ? <span className={`sti-badge sti-risk-${riskClass.toLowerCase()}`}>{riskClass}</span> : '—'}
          </div>
        </div>
      </div>

      <p style={{ fontSize: '0.82rem', color: '#555' }}>
        {regimeNarrative(dominantState, regimeFit)}
      </p>
    </div>
  )
}

function regimeNarrative(state, fit) {
  const parts = []
  if (state) parts.push(`This family most often appears in the ${state.replace(/_/g, ' ')} structural state.`)
  if (fit >= 0.5) parts.push('Strong regime compatibility — over half of detections occur in favorable environments.')
  else if (fit >= 0.3) parts.push('Moderate regime compatibility.')
  else parts.push('Low regime compatibility — most detections occur in neutral or unfavorable environments.')
  return parts.join(' ')
}
