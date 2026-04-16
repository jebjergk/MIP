import React from 'react'
import InfoTooltip from '../InfoTooltip'

const SCOPE = 'structural_training'

function pct(v) {
  if (v == null) return '—'
  return `${(Number(v) * 100).toFixed(2)}%`
}

export default function StiExamplesModule({ data }) {
  if (!data?.length) return <p style={{ color: '#6c757d', fontSize: '0.85rem' }}>No example setups available.</p>

  const g = (r, k) => r[k] ?? r[k.toUpperCase()] ?? r[k.toLowerCase()]

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        Recent Example Setups
        <InfoTooltip scope={SCOPE} entryKey="example_setups" variant="short" />
      </div>

      <div className="sti-examples-grid">
        {data.slice(0, 15).map((ex, i) => (
          <div key={g(ex, 'SETUP_EVENT_ID') ?? i} className="sti-example-card">
            <div className="sti-example-header">
              <span className="sti-example-symbol">{g(ex, 'SYMBOL')}</span>
              <span className="sti-example-date">{String(g(ex, 'SETUP_DATE') || '').slice(0, 10)}</span>
              <span className={`sti-badge sti-dir-${(g(ex, 'DIRECTION') || '').toLowerCase()}`} style={{ marginLeft: 'auto', fontSize: '0.68rem' }}>
                {g(ex, 'DIRECTION')}
              </span>
            </div>
            <div className="sti-example-metrics">
              <span title="Structural state at detection">State: {g(ex, 'STRUCTURAL_STATE') || '—'}</span>
              <span title="Regime compatibility">{g(ex, 'REGIME_COMPAT') || '—'}</span>
              <span title="Max favorable excursion">MFE: {pct(g(ex, 'MFE_PCT'))}</span>
              <span title="Max adverse excursion">MAE: {pct(g(ex, 'MAE_PCT'))}</span>
              <span title="Failure mode classification">{(g(ex, 'FAILURE_MODE') || '—').replace(/_/g, ' ')}</span>
              {g(ex, 'BECAME_PROPOSAL') && (
                <span className="sti-proposal-ready" title="This setup became a trade proposal">Proposed</span>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
