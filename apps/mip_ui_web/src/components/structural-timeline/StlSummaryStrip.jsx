import React from 'react'

export default function StlSummaryStrip({ data, get }) {
  if (!data || !Object.keys(data).length) return null

  const v = (k) => get(data, k)
  const kpi = (label, value, small) => (
    <div className="stl-kpi" key={label}>
      <div className="stl-kpi-label">{label}</div>
      <div className={`stl-kpi-value${small ? ' stl-kpi-value--small' : ''}`}>{value ?? '—'}</div>
    </div>
  )

  return (
    <div className="stl-summary-strip">
      {kpi('Setups', v('TOTAL_SETUPS'))}
      {kpi('Eligible', v('ELIGIBLE_SETUPS'))}
      {kpi('Proposals', v('PROPOSALS_CREATED'))}
      {kpi('Trades', v('TRADES_EXECUTED'))}
      {kpi('Long / Short', `${v('LONG_SETUPS') ?? 0} / ${v('SHORT_SETUPS') ?? 0}`)}
      {kpi('Current State', v('DOMINANT_STATE')?.replace(/_/g, ' '), true)}
      {kpi('Vol Regime', v('VOL_REGIME')?.replace(/_/g, ' '), true)}
      {kpi('Trend', v('TREND_REGIME')?.replace(/_/g, ' '), true)}
      {v('CURRENT_THESIS') && kpi('Active Thesis', v('CURRENT_THESIS')?.replace(/_/g, ' '), true)}
      {v('STRONGEST_FAMILY') && kpi('Strongest Family', v('STRONGEST_FAMILY')?.replace(/_/g, ' '), true)}
    </div>
  )
}
