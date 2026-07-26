function formatCost(cost) {
  if (cost == null || cost === '') return null
  if (typeof cost === 'object') {
    const value = cost.cost_usd ?? cost.usd ?? cost.amount ?? cost.estimated_cost_usd
    const status = cost.status ?? cost.cost_status
    const formatted = value == null ? null : formatCost(value)
    return [status, formatted].filter(Boolean).join(' · ') || null
  }
  if (typeof cost === 'number') return `$${cost.toFixed(cost < 0.1 ? 4 : 2)}`
  const numeric = Number(cost)
  return Number.isFinite(numeric) ? `$${numeric.toFixed(numeric < 0.1 ? 4 : 2)}` : String(cost)
}

function FactorList({ title, items, tone }) {
  if (!items.length) return null
  return (
    <section className={`paa-factor-list paa-factor-list--${tone}`}>
      <h3>{title}</h3>
      <ul>
        {items.map((item, index) => <li key={`${index}-${item}`}>{item}</li>)}
      </ul>
    </section>
  )
}

function PlainExplanation({ value }) {
  if (!value) return null
  if (typeof value === 'string') return <p>{value}</p>
  return (
    <>
      <p>{value.summary}</p>
      <p><strong>What the chart is doing:</strong> {value.what_the_chart_is_doing}</p>
      <p><strong>Why it matters:</strong> {value.why_it_matters}</p>
      <p><strong>What to wait for:</strong> {value.what_to_wait_for}</p>
      <p><strong>Risk:</strong> {value.simple_risk_warning}</p>
    </>
  )
}

function ExpertRead({ value }) {
  if (!value) return <p>No methodologist narrative was returned.</p>
  if (typeof value === 'string') return <p>{value}</p>
  const sections = [
    ['Market context', value.market_context],
    ['Trend state', value.trend_state],
    ['Current pattern', value.current_pattern],
    ['Support / resistance', value.support_resistance_read],
    ['Long location quality', value.long_location_quality],
    ['Continuation vs failure risk', value.continuation_vs_failure_risk],
    ['Confirmation needed', value.confirmation_needed],
    ['Invalidation logic', value.invalidation_logic],
  ]
  return (
    <div className="paa-expert-read">
      {sections.filter(([, text]) => text).map(([title, text]) => (
        <p key={title}><strong>{title}:</strong> {text}</p>
      ))}
    </div>
  )
}

export default function PriceActionRead({ analysis }) {
  const cost = formatCost(analysis.cost)
  return (
    <>
      <section className="paa-card paa-verdict-card">
        <div className="paa-verdict-topline">
          <div>
            <span className="paa-eyebrow">Long-only verdict</span>
            <h2>{analysis.verdict}</h2>
          </div>
          <div className="paa-badges">
            <span className={`paa-badge paa-badge--${analysis.status.toLowerCase()}`}>{analysis.status}</span>
            {cost && <span className="paa-badge paa-badge--cost">Cost {cost}</span>}
            <span className="paa-badge paa-badge--side">LONG</span>
          </div>
        </div>
        {analysis.simpleExplanation && (
          <div className="paa-simple-read">
            <strong>Simple explanation</strong>
            <PlainExplanation value={analysis.simpleExplanation} />
          </div>
        )}
      </section>

      <section className="paa-card paa-methodologist-card">
        <div className="paa-section-heading">
          <div>
            <span className="paa-eyebrow">Expert interpretation</span>
            <h2>Price Action Methodologist read</h2>
          </div>
          <code>methodologist_interpretation</code>
        </div>
        <ExpertRead value={analysis.expertRead} />
      </section>

      {(analysis.supportingFactors.length > 0 || analysis.warningFactors.length > 0) && (
        <div className="paa-factor-grid">
          <FactorList title="Supporting factors" items={analysis.supportingFactors} tone="support" />
          <FactorList title="Warning factors" items={analysis.warningFactors} tone="warning" />
        </div>
      )}

      {analysis.concepts.length > 0 && (
        <section className="paa-card paa-concepts">
          <span className="paa-eyebrow">Knowledge used</span>
          <h2>Concepts in this read</h2>
          <div className="paa-concept-chips">
            {analysis.concepts.map((concept) => <span key={concept}>{concept}</span>)}
          </div>
        </section>
      )}
    </>
  )
}
