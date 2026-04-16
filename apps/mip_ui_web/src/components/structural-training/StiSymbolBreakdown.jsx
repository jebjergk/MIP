import React, { useState, useEffect } from 'react'
import { API_BASE } from '../../config/apiBase'
import InfoTooltip from '../InfoTooltip'
import LoadingState from '../LoadingState'

const SCOPE = 'structural_training'

function pct(v) {
  if (v == null) return '—'
  return `${(Number(v) * 100).toFixed(1)}%`
}

function ratio(v) {
  if (v == null) return '—'
  return Number(v).toFixed(2)
}

function diffArrow(diff) {
  if (diff == null || Number.isNaN(Number(diff))) return ''
  const d = Number(diff)
  if (Math.abs(d) < 0.05) return ''
  return d > 0 ? ' \u25B2' : ' \u25BC'
}

function strengthBadge(s) {
  const classes = {
    ALIGNED: 'sti-regime-neutral',
    STRONGER: 'sti-regime-good',
    WEAKER: 'sti-regime-poor',
    INSUFFICIENT: 'sti-trust-unknown',
  }
  return classes[s] || 'sti-trust-unknown'
}

export default function StiSymbolBreakdown({ family, marketType, direction }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    setData(null)
    const params = new URLSearchParams({ setup_family: family, market_type: marketType })
    if (direction) params.set('direction', direction)
    fetch(`${API_BASE}/structural-training/symbol-breakdown?${params}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
      .then(d => setData(d.rows ?? []))
      .catch(() => setData([]))
      .finally(() => setLoading(false))
  }, [family, marketType, direction])

  if (loading) return <LoadingState />

  const g = (r, k) => r[k] ?? r[k.toUpperCase()] ?? r[k.toLowerCase()]

  if (!data?.length) return <p style={{ color: '#6c757d', fontSize: '0.85rem' }}>No symbol-level data available.</p>

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        Symbol Breakdown
        <InfoTooltip scope={SCOPE} entryKey="symbol_breakdown" variant="short" />
      </div>

      <div className="sti-table-wrap" style={{ maxHeight: 340 }}>
        <table className="sti-table">
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Dir.</th>
              <th className="sti-col-right">n</th>
              <th className="sti-col-right">Symbol MHR</th>
              <th className="sti-col-right">Family MHR</th>
              <th className="sti-col-right">Diff</th>
              <th className="sti-col-right">MFE/MAE</th>
              <th>Evidence</th>
            </tr>
          </thead>
          <tbody>
            {data.map((row, i) => {
              const sym = g(row, 'SYMBOL')
              const evidence = g(row, 'EVIDENCE_STRENGTH') || 'UNKNOWN'
              const diff = g(row, 'VS_FAMILY_MHR_DIFF')
              return (
                <tr key={`${sym}-${i}`}>
                  <td className="sti-family-name">{sym}</td>
                  <td><span className={`sti-badge sti-dir-${(g(row, 'DIRECTION') || '').toLowerCase()}`}>{g(row, 'DIRECTION')}</span></td>
                  <td className="sti-col-right">{g(row, 'N_SETUPS')}</td>
                  <td className="sti-col-right">{pct(g(row, 'SYMBOL_MHR'))}</td>
                  <td className="sti-col-right">{pct(g(row, 'FAMILY_MHR'))}</td>
                  <td className={`sti-col-right ${diff > 0.05 ? 'sti-val-good' : diff < -0.05 ? 'sti-val-weak' : ''}`}>
                    {pct(diff)}{diffArrow(diff)}
                  </td>
                  <td className="sti-col-right">{ratio(g(row, 'SYMBOL_MFE_MAE'))}</td>
                  <td><span className={`sti-badge ${strengthBadge(evidence)}`}>{evidence}</span></td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
