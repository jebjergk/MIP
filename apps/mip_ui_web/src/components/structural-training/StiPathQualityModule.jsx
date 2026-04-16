import React from 'react'
import InfoTooltip from '../InfoTooltip'

const SCOPE = 'structural_training'

function MetricCard({ label, value, tip, barPct, barColor }) {
  return (
    <div className="sti-metric-card">
      <div className="sti-metric-label">
        {label}
        {tip && <InfoTooltip scope={SCOPE} entryKey={tip} variant="short" />}
      </div>
      <div className="sti-metric-value">{value}</div>
      {barPct != null && (
        <div className="sti-metric-bar">
          <div className="sti-metric-bar-fill" style={{ width: `${Math.min(100, Math.max(0, barPct))}%`, background: barColor || '#1565c0' }} />
        </div>
      )}
    </div>
  )
}

function pct(v) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return `${(Number(v) * 100).toFixed(1)}%`
}

function ratio(v) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return Number(v).toFixed(2)
}

function bars(v) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return `${Number(v).toFixed(1)} bars`
}

function qualityColor(v, goodThresh, okThresh) {
  if (v == null) return '#6c757d'
  if (v >= goodThresh) return '#198754'
  if (v >= okThresh) return '#b8860b'
  return '#dc3545'
}

export default function StiPathQualityModule({ row, get, detail }) {
  const mhr = Number(get(row, 'MEANINGFUL_HIT_RATE'))
  const ps = Number(get(row, 'PATH_SURVIVAL_HIT_RATE'))
  const mfeMae = Number(get(row, 'MFE_MAE_RATIO'))
  const barsToThreshold = get(row, 'AVG_BARS_TO_THRESHOLD')
  const adverseBefore = detail ? (detail.PCT_ADVERSE_BEFORE_FAVORABLE ?? detail.pct_adverse_before_favorable) : get(row, 'PCT_ADVERSE_BEFORE_FAVORABLE')
  const gapRisk = detail ? (detail.GAP_RISK_CONTRIBUTION ?? detail.gap_risk_contribution) : get(row, 'GAP_RISK_CONTRIBUTION')

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        Path Quality
        <InfoTooltip scope={SCOPE} entryKey="path_quality" variant="short" />
      </div>

      <div className="sti-metric-grid">
        <MetricCard
          label="Meaningful Hit Rate"
          tip="meaningful_hit_rate"
          value={pct(mhr)}
          barPct={mhr * 100}
          barColor={qualityColor(mhr, 0.40, 0.30)}
        />
        <MetricCard
          label="Path Survival"
          tip="path_survival"
          value={pct(ps)}
          barPct={ps * 100}
          barColor={qualityColor(ps, 0.35, 0.25)}
        />
        <MetricCard
          label="MFE/MAE Ratio"
          tip="mfe_mae_ratio"
          value={ratio(mfeMae)}
          barPct={Math.min(100, (mfeMae / 3) * 100)}
          barColor={qualityColor(mfeMae, 1.5, 1.2)}
        />
        <MetricCard
          label="Avg Bars to Threshold"
          tip="bars_to_threshold"
          value={bars(barsToThreshold)}
        />
        <MetricCard
          label="Adverse Before Favorable"
          tip="adverse_before_favorable"
          value={pct(adverseBefore)}
          barPct={Number(adverseBefore) * 100}
          barColor={Number(adverseBefore) > 0.5 ? '#dc3545' : '#198754'}
        />
        <MetricCard
          label="Gap Risk Contribution"
          tip="gap_risk"
          value={pct(gapRisk)}
          barPct={Number(gapRisk) * 100}
          barColor={Number(gapRisk) > 0.3 ? '#dc3545' : '#6c757d'}
        />
      </div>

      <p style={{ fontSize: '0.82rem', color: '#555', marginTop: '0.6rem' }}>
        {pathNarrative(mhr, ps, mfeMae)}
      </p>
    </div>
  )
}

function pathNarrative(mhr, ps, ratio) {
  if (Number.isNaN(mhr)) return 'Insufficient data to assess path quality.'
  const parts = []
  if (mhr >= 0.4 && ps >= 0.35) parts.push('Strong path quality — meaningful moves reached frequently with good survival.')
  else if (mhr >= 0.3) parts.push('Moderate hit rate.')
  else parts.push('Low meaningful hit rate — most setups do not reach the target move.')

  if (ratio >= 1.5) parts.push('Favorable risk-reward ratio.')
  else if (ratio >= 1.0) parts.push('Marginal risk-reward.')
  else if (!Number.isNaN(ratio)) parts.push('Unfavorable risk-reward — adverse moves tend to exceed favorable ones.')

  return parts.join(' ')
}
