import React from 'react'

function Card({ label, value, small }) {
  return (
    <div className="sti-summary-card">
      <span className="sti-summary-label">{label}</span>
      <span className={`sti-summary-value${small ? ' sti-summary-value--small' : ''}`}>{value ?? '—'}</span>
    </div>
  )
}

export default function StiSummaryStrip({ data }) {
  if (!data) return null
  const g = (k) => data[k] ?? data[k.toUpperCase()] ?? data[k.toLowerCase()]
  return (
    <div className="sti-summary-strip">
      <Card label="Active Families" value={g('TOTAL_FAMILIES_ACTIVE')} />
      <Card label="Trusted" value={g('TRUSTED_COMBOS')} />
      <Card label="Provisional" value={g('PROVISIONAL_COMBOS')} />
      <Card label="Research" value={g('RESEARCH_COMBOS')} />
      <Card label="Rejected" value={g('REJECTED_COMBOS')} />
      <Card label="Total Setups" value={Number(g('TOTAL_HISTORICAL_SETUPS') ?? 0).toLocaleString()} />
      <Card label="Proposal Eligible" value={g('PROPOSAL_ELIGIBLE_FAMILIES')} />
      <Card label="Strongest" value={formatFamily(g('STRONGEST_FAMILY'))} small />
      <Card label="Weakest" value={formatFamily(g('WEAKEST_FAMILY'))} small />
    </div>
  )
}

function formatFamily(f) {
  if (!f) return '—'
  return f.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()).toLowerCase().replace(/\b\w/g, c => c.toUpperCase())
}
