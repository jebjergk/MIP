import React from 'react'
import InfoTooltip from '../InfoTooltip'

const SCOPE = 'structural_training'

export default function StiProposalReadiness({ row, get }) {
  const trust = get(row, 'TRUST_LABEL') || 'UNKNOWN'
  const n = Number(get(row, 'N_SETUPS_TOTAL') ?? 0)
  const mhr = Number(get(row, 'MEANINGFUL_HIT_RATE') ?? 0)
  const mfeMae = Number(get(row, 'MFE_MAE_RATIO') ?? 0)
  const trail = get(row, 'TRAIL_STRATEGY_RECOMMENDATION')
  const exit = get(row, 'EXIT_STYLE_RECOMMENDATION')
  const isReady = get(row, 'IS_PROPOSAL_READY')

  const gates = [
    {
      label: 'Trust status is TRUSTED or PROVISIONAL',
      pass: trust === 'TRUSTED' || trust === 'PROVISIONAL',
      detail: `Current: ${trust}`,
    },
    {
      label: 'Sample size >= 20 setups',
      pass: n >= 20,
      detail: `Current: ${n}`,
    },
    {
      label: 'Meaningful hit rate >= 30%',
      pass: mhr >= 0.30,
      detail: `Current: ${(mhr * 100).toFixed(1)}%`,
    },
    {
      label: 'MFE/MAE ratio >= 1.2',
      pass: mfeMae >= 1.2,
      detail: `Current: ${mfeMae.toFixed(2)}`,
    },
    {
      label: 'Risk policy assigned and active',
      pass: isReady,
      detail: isReady ? 'Active' : 'Missing or inactive',
    },
    {
      label: 'Trailing style assigned',
      pass: !!trail,
      detail: trail || 'None',
    },
    {
      label: 'Exit style assigned',
      pass: !!exit,
      detail: exit || 'None',
    },
  ]

  const allPass = gates.every(g => g.pass)

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        Proposal Readiness
        <InfoTooltip scope={SCOPE} entryKey="proposal_readiness" variant="short" />
      </div>

      {allPass && (
        <div style={{ padding: '0.5rem 0.75rem', background: '#d4edda', borderRadius: 6, marginBottom: '0.6rem', fontWeight: 600, color: '#155724', fontSize: '0.88rem' }}>
          Proposal-Ready — all quality gates passed.
        </div>
      )}

      {!allPass && (
        <div style={{ padding: '0.5rem 0.75rem', background: '#fff3cd', borderRadius: 6, marginBottom: '0.6rem', fontWeight: 600, color: '#856404', fontSize: '0.88rem' }}>
          Not Proposal-Ready — {gates.filter(g => !g.pass).length} gate(s) unmet.
        </div>
      )}

      <ul className="sti-readiness-list">
        {gates.map((g, i) => (
          <li key={i} className={`sti-readiness-item ${g.pass ? 'sti-readiness-pass' : 'sti-readiness-fail'}`}>
            <span className="sti-readiness-icon">{g.pass ? '\u2713' : '\u2717'}</span>
            <span>{g.label}</span>
            <span style={{ fontSize: '0.78rem', color: '#6c757d', marginLeft: 'auto' }}>{g.detail}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}
