import React, { useState, useMemo, useCallback } from 'react'
import InfoTooltip from '../InfoTooltip'
import StiDetailPanel from './StiDetailPanel'

const SCOPE = 'structural_training'

const COLUMNS = [
  { key: 'SETUP_FAMILY', label: 'Setup Family', tip: 'setup_family' },
  { key: 'DIRECTION', label: 'Dir.', tip: 'direction' },
  { key: 'MARKET_TYPE', label: 'Market' },
  { key: 'BEST_WINDOW', label: 'Win', tip: 'best_window', right: true },
  { key: 'TRUST_LABEL', label: 'Trust', tip: 'trust_label' },
  { key: 'MATURITY_LABEL', label: 'Maturity', tip: 'maturity' },
  { key: 'N_SETUPS_TOTAL', label: 'n', right: true },
  { key: 'MEANINGFUL_HIT_RATE', label: 'MHR', tip: 'meaningful_hit_rate', right: true, pct: true },
  { key: 'PATH_SURVIVAL_HIT_RATE', label: 'Path Surv.', tip: 'path_survival', right: true, pct: true },
  { key: 'MFE_MAE_RATIO', label: 'MFE/MAE', tip: 'mfe_mae_ratio', right: true, ratio: true },
  { key: 'REGIME_FIT_GOOD_PCT', label: 'Regime Fit', tip: 'regime_fit', right: true, pct: true },
  { key: 'DOMINANT_RISK_CLASS', label: 'Risk', tip: 'risk_class' },
  { key: 'IS_PROPOSAL_READY', label: 'Proposal', tip: 'proposal_readiness' },
]

function mhrClass(v) {
  if (v == null) return ''
  if (v >= 0.40) return 'sti-val-good'
  if (v >= 0.30) return 'sti-val-ok'
  return 'sti-val-weak'
}

function ratioClass(v) {
  if (v == null) return ''
  if (v >= 1.5) return 'sti-val-good'
  if (v >= 1.2) return 'sti-val-ok'
  return 'sti-val-weak'
}

function trustBadgeClass(t) {
  if (!t) return 'sti-trust-unknown'
  const k = t.toLowerCase()
  return `sti-trust-${k}`
}

function riskBadgeClass(r) {
  if (!r) return ''
  return `sti-risk-${r.toLowerCase()}`
}

function formatPct(v) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return `${(Number(v) * 100).toFixed(1)}%`
}

function formatRatio(v) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return Number(v).toFixed(2)
}

function formatFamily(f) {
  if (!f) return '—'
  return f.replace(/_/g, ' ')
}

export default function StiLeaderboard({ rows, expandedKey, onToggle, get }) {
  const [sortCol, setSortCol] = useState(null)
  const [sortAsc, setSortAsc] = useState(false)

  const handleSort = useCallback(key => {
    if (sortCol === key) {
      setSortAsc(prev => !prev)
    } else {
      setSortCol(key)
      setSortAsc(false)
    }
  }, [sortCol])

  const sorted = useMemo(() => {
    if (!sortCol) return rows
    const copy = [...rows]
    copy.sort((a, b) => {
      let va = get(a, sortCol)
      let vb = get(b, sortCol)
      if (va == null) return 1
      if (vb == null) return -1
      if (typeof va === 'string') {
        const cmp = va.localeCompare(vb)
        return sortAsc ? cmp : -cmp
      }
      if (typeof va === 'boolean') {
        va = va ? 1 : 0
        vb = vb ? 1 : 0
      }
      return sortAsc ? va - vb : vb - va
    })
    return copy
  }, [rows, sortCol, sortAsc, get])

  const rk = r => `${get(r, 'SETUP_FAMILY')}|${get(r, 'MARKET_TYPE')}|${get(r, 'DIRECTION')}`

  return (
    <div className="sti-table-wrap">
      <table className="sti-table">
        <thead>
          <tr>
            <th className="sti-expand-cell" aria-label="Expand" />
            {COLUMNS.map(col => (
              <th
                key={col.key}
                className={col.right ? 'sti-col-right' : ''}
                onClick={() => handleSort(col.key)}
              >
                {col.label}
                {col.tip && <InfoTooltip scope={SCOPE} entryKey={col.tip} variant="short" />}
                <SortArrow active={sortCol === col.key} asc={sortAsc} />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map(row => {
            const key = rk(row)
            const expanded = expandedKey === key
            return (
              <React.Fragment key={key}>
                <tr
                  className={`sti-row${expanded ? ' sti-row--expanded' : ''}`}
                  onClick={() => onToggle(key)}
                  tabIndex={0}
                  role="button"
                  aria-expanded={expanded}
                  onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onToggle(key) } }}
                >
                  <td className="sti-expand-cell">
                    <span className={`sti-expand-icon${expanded ? ' sti-expand-icon--open' : ''}`}>&#9658;</span>
                  </td>
                  <td className="sti-family-name">{formatFamily(get(row, 'SETUP_FAMILY'))}</td>
                  <td><span className={`sti-badge sti-dir-${(get(row, 'DIRECTION') || '').toLowerCase()}`}>{get(row, 'DIRECTION')}</span></td>
                  <td>{get(row, 'MARKET_TYPE')}</td>
                  <td className="sti-col-right">{get(row, 'BEST_WINDOW')}</td>
                  <td><span className={`sti-badge sti-trust-pill ${trustBadgeClass(get(row, 'TRUST_LABEL'))}`}>{get(row, 'TRUST_LABEL')}</span></td>
                  <td>
                    <div className="sti-maturity-bar-wrap">
                      <div className="sti-maturity-bar"><div className="sti-maturity-bar-fill" style={{ width: `${Math.min(100, get(row, 'MATURITY_PCT') ?? 0)}%` }} /></div>
                      <span className="sti-maturity-label">{get(row, 'MATURITY_LABEL')}</span>
                    </div>
                  </td>
                  <td className="sti-col-right">{get(row, 'N_SETUPS_TOTAL') ?? '—'}</td>
                  <td className={`sti-col-right ${mhrClass(get(row, 'MEANINGFUL_HIT_RATE'))}`}>{formatPct(get(row, 'MEANINGFUL_HIT_RATE'))}</td>
                  <td className={`sti-col-right ${mhrClass(get(row, 'PATH_SURVIVAL_HIT_RATE'))}`}>{formatPct(get(row, 'PATH_SURVIVAL_HIT_RATE'))}</td>
                  <td className={`sti-col-right ${ratioClass(get(row, 'MFE_MAE_RATIO'))}`}>{formatRatio(get(row, 'MFE_MAE_RATIO'))}</td>
                  <td className="sti-col-right">{formatPct(get(row, 'REGIME_FIT_GOOD_PCT'))}</td>
                  <td>{get(row, 'DOMINANT_RISK_CLASS') ? <span className={`sti-badge ${riskBadgeClass(get(row, 'DOMINANT_RISK_CLASS'))}`}>{get(row, 'DOMINANT_RISK_CLASS')}</span> : '—'}</td>
                  <td className="sti-col-center">{get(row, 'IS_PROPOSAL_READY') ? <span className="sti-proposal-ready">&#10003;</span> : <span className="sti-proposal-not">—</span>}</td>
                </tr>
                {expanded && (
                  <tr className="sti-detail-row">
                    <td colSpan={COLUMNS.length + 1} className="sti-detail-cell">
                      <StiDetailPanel
                        row={row}
                        get={get}
                        onClose={() => onToggle(key)}
                      />
                    </td>
                  </tr>
                )}
              </React.Fragment>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function SortArrow({ active, asc }) {
  if (!active) return <span className="sti-sort-arrow" />
  return <span className="sti-sort-arrow sti-sort-arrow--active">{asc ? ' \u25B2' : ' \u25BC'}</span>
}
