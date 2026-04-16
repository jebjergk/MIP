import React from 'react'

export default function StiFilterBar({
  marketType, setMarketType,
  direction, setDirection,
  trustLabel, setTrustLabel,
  setupFamily, setSetupFamily,
  proposalOnly, setProposalOnly,
  showRejected, setShowRejected,
  options,
}) {
  return (
    <section className="sti-filters" aria-label="Filters">
      <div className="sti-filter-group">
        <label htmlFor="sti-market">Market</label>
        <select id="sti-market" value={marketType} onChange={e => setMarketType(e.target.value)}>
          <option value="">All</option>
          {options.marketTypes.map(v => <option key={v} value={v}>{v}</option>)}
        </select>
      </div>

      <div className="sti-filter-group">
        <label htmlFor="sti-dir">Direction</label>
        <select id="sti-dir" value={direction} onChange={e => setDirection(e.target.value)}>
          <option value="">All</option>
          {options.directions.map(v => <option key={v} value={v}>{v}</option>)}
        </select>
      </div>

      <div className="sti-filter-group">
        <label htmlFor="sti-trust">Trust</label>
        <select id="sti-trust" value={trustLabel} onChange={e => setTrustLabel(e.target.value)}>
          <option value="">All</option>
          {options.trustLabels.map(v => <option key={v} value={v}>{v}</option>)}
        </select>
      </div>

      <div className="sti-filter-group">
        <label htmlFor="sti-family">Family</label>
        <select id="sti-family" value={setupFamily} onChange={e => setSetupFamily(e.target.value)}>
          <option value="">All</option>
          {options.setupFamilies.map(v => <option key={v} value={v}>{formatFamily(v)}</option>)}
        </select>
      </div>

      <label className="sti-toggle">
        <input type="checkbox" checked={proposalOnly} onChange={e => setProposalOnly(e.target.checked)} />
        Proposal-ready only
      </label>

      <label className="sti-toggle">
        <input type="checkbox" checked={showRejected} onChange={e => setShowRejected(e.target.checked)} />
        Show rejected
      </label>
    </section>
  )
}

function formatFamily(f) {
  if (!f) return ''
  return f.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()).toLowerCase().replace(/\b\w/g, c => c.toUpperCase())
}
