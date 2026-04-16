import React from 'react'
import InfoTooltip from '../InfoTooltip'

const SCOPE = 'structural_training'

const STAGE_THRESHOLDS = [
  { min: 0, label: 'Insufficient', color: '#dc3545' },
  { min: 10, label: 'Emerging', color: '#fd7e14' },
  { min: 20, label: 'Researching', color: '#ffc107' },
  { min: 30, label: 'Provisional', color: '#0d6efd' },
  { min: 40, label: 'Trusted', color: '#198754' },
]

export default function StiMaturityModule({ row, get }) {
  const n = Number(get(row, 'N_SETUPS_TOTAL') ?? 0)
  const maturity = get(row, 'MATURITY_LABEL') || 'UNKNOWN'
  const pct = Math.min(100, get(row, 'MATURITY_PCT') ?? 0)
  const trust = get(row, 'TRUST_LABEL') || 'UNKNOWN'

  const stage = STAGE_THRESHOLDS.slice().reverse().find(s => n >= s.min) || STAGE_THRESHOLDS[0]

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        Training Maturity
        <InfoTooltip scope={SCOPE} entryKey="maturity" variant="short" />
      </div>

      <div className="sti-metric-grid">
        <div className="sti-metric-card">
          <div className="sti-metric-label">Sample Size</div>
          <div className="sti-metric-value">{n.toLocaleString()}</div>
          <div className="sti-metric-bar">
            <div className="sti-metric-bar-fill" style={{ width: `${pct}%`, background: stage.color }} />
          </div>
        </div>

        <div className="sti-metric-card">
          <div className="sti-metric-label">Maturity Stage</div>
          <div className="sti-metric-value" style={{ color: stage.color, fontSize: '0.92rem' }}>{maturity}</div>
        </div>

        <div className="sti-metric-card">
          <div className="sti-metric-label">Trust Status</div>
          <div className="sti-metric-value" style={{ fontSize: '0.92rem' }}>
            <span className={`sti-badge sti-trust-pill sti-trust-${trust.toLowerCase()}`}>{trust}</span>
          </div>
        </div>
      </div>

      <p style={{ fontSize: '0.82rem', color: '#555', marginTop: '0.5rem' }}>
        {maturityNarrative(n, maturity, trust)}
      </p>
    </div>
  )
}

function maturityNarrative(n, maturity, trust) {
  if (n < 10) return `Only ${n} setups observed so far. Not enough evidence to evaluate quality. More data needed.`
  if (n < 20) return `${n} setups observed. Early signal — patterns are emerging but sample is still thin.`
  if (n < 40) return `${n} setups evaluated. Building evidence — trust determination is approaching but may shift.`
  if (trust === 'TRUSTED') return `${n} setups evaluated. Strong evidence base with stable trust status.`
  if (trust === 'REJECTED') return `${n} setups evaluated but quality metrics fell below thresholds. This family may need structural refinement.`
  return `${n} setups evaluated. Evidence is substantial. Trust label: ${trust}.`
}
