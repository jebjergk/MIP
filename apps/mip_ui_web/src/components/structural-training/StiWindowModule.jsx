import React from 'react'
import InfoTooltip from '../InfoTooltip'

const SCOPE = 'structural_training'

function pct(v) {
  if (v == null) return '—'
  return `${(Number(v) * 100).toFixed(1)}%`
}

function ratio(v) {
  if (v == null) return '—'
  return Number(v).toFixed(2)
}

function num(v) {
  if (v == null) return '—'
  return Number(v).toFixed(1)
}

export default function StiWindowModule({ windows }) {
  if (!windows || !windows.length) return <p style={{ color: '#6c757d', fontSize: '0.85rem' }}>No window data available.</p>

  const g = (w, k) => w[k] ?? w[k.toUpperCase()] ?? w[k.toLowerCase()]
  const bestWin = g(windows[0], 'BEST_WINDOW')

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        Window Behavior (5 / 10 / 20 bars)
        <InfoTooltip scope={SCOPE} entryKey="best_window" variant="short" />
      </div>

      <table className="sti-window-table">
        <thead>
          <tr>
            <th>Metric</th>
            {windows.map(w => {
              const ew = g(w, 'EVAL_WINDOW')
              return <th key={ew} className={ew === bestWin ? 'sti-window-best' : ''}>{ew} bars{ew === bestWin ? ' *' : ''}</th>
            })}
          </tr>
        </thead>
        <tbody>
          <Row label="Meaningful Hit Rate" windows={windows} field="MEANINGFUL_HIT_RATE" fmt={pct} bestWin={bestWin} g={g} />
          <Row label="Directional Hit Rate" windows={windows} field="DIRECTIONAL_HIT_RATE" fmt={pct} bestWin={bestWin} g={g} />
          <Row label="Path Survival" windows={windows} field="PATH_SURVIVAL_HIT_RATE" fmt={pct} bestWin={bestWin} g={g} />
          <Row label="MFE/MAE Ratio" windows={windows} field="MFE_MAE_RATIO" fmt={ratio} bestWin={bestWin} g={g} />
          <Row label="Avg MFE %" windows={windows} field="AVG_MFE" fmt={pct} bestWin={bestWin} g={g} />
          <Row label="Avg MAE %" windows={windows} field="AVG_MAE" fmt={pct} bestWin={bestWin} g={g} />
          <Row label="Bars to Threshold" windows={windows} field="AVG_BARS_TO_THRESHOLD" fmt={num} bestWin={bestWin} g={g} />
          <Row label="Trail: Structural" windows={windows} field="AVG_TRAIL_STRUCTURAL" fmt={pct} bestWin={bestWin} g={g} />
          <Row label="Trail: Progress" windows={windows} field="AVG_TRAIL_PROGRESS" fmt={pct} bestWin={bestWin} g={g} />
          <Row label="Trail: Hybrid" windows={windows} field="AVG_TRAIL_HYBRID" fmt={pct} bestWin={bestWin} g={g} />
        </tbody>
      </table>

      <p style={{ fontSize: '0.75rem', color: '#6c757d', marginTop: '0.4rem' }}>* Best evaluation window based on meaningful hit rate.</p>
    </div>
  )
}

function Row({ label, windows, field, fmt, bestWin, g }) {
  return (
    <tr>
      <td style={{ textAlign: 'left', fontWeight: 500, fontSize: '0.8rem' }}>{label}</td>
      {windows.map(w => {
        const ew = g(w, 'EVAL_WINDOW')
        return <td key={ew} className={ew === bestWin ? 'sti-window-best' : ''}>{fmt(g(w, field))}</td>
      })}
    </tr>
  )
}
