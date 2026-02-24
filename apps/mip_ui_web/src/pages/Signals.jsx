import { useState, useEffect, useCallback } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import './Signals.css'

/* ── helpers ──────────────────────────────────────────────────────── */

function formatJson(val) {
  if (val == null) return 'null'
  if (typeof val === 'string') {
    try { return JSON.stringify(JSON.parse(val), null, 2) }
    catch { return val }
  }
  return JSON.stringify(val, null, 2)
}

function fmtPct(v)  {
  if (v == null) return '—'
  const pct = v * 100
  if (Math.abs(pct) < 0.1) return `${pct.toFixed(2)}%`
  return `${pct.toFixed(1)}%`
}
function fmtNum(v, d = 2) { return v != null ? Number(v).toFixed(d) : '—' }

const OUTCOME_CFG = {
  TRADED:               { icon: '✔', label: 'Traded',                cls: 'de-pill--traded'  },
  REJECTED_BY_TRUST:    { icon: '✖', label: 'Rejected · Trust',      cls: 'de-pill--rejected' },
  REJECTED_BY_RISK:     { icon: '✖', label: 'Rejected · Risk',       cls: 'de-pill--rejected-risk' },
  REJECTED_BY_CAPACITY: { icon: '✖', label: 'Rejected · Capacity',   cls: 'de-pill--rejected-risk' },
  ELIGIBLE_NOT_SELECTED:{ icon: '○', label: 'Eligible · Not Selected',cls: 'de-pill--eligible' },
}

const TRUST_CLS = {
  TRUSTED:   'de-trust--trusted',
  WATCH:     'de-trust--watch',
  UNTRUSTED: 'de-trust--untrusted',
}

/* ── small components ─────────────────────────────────────────────── */

function OutcomePill({ outcome }) {
  const c = OUTCOME_CFG[outcome] || { icon: '?', label: outcome, cls: '' }
  return (
    <span className={`de-outcome-pill ${c.cls}`}>
      <span className="de-pill-icon">{c.icon}</span>
      {c.label}
    </span>
  )
}

function TrustPill({ label }) {
  const cls = TRUST_CLS[(label || '').toUpperCase()] || ''
  return <span className={`de-trust-pill ${cls}`}>{label || '—'}</span>
}

function FilterBadge({ label, value, onClear }) {
  if (!value) return null
  return (
    <span className="de-filter-badge">
      <span className="de-filter-badge-label">{label}:</span>
      <span className="de-filter-badge-value">{value}</span>
      {onClear && (
        <button type="button" className="de-filter-badge-x" onClick={onClear} aria-label={`Clear ${label}`}>×</button>
      )}
    </span>
  )
}

/* ── Summary Banner ───────────────────────────────────────────────── */

function SummaryBanner({ summary, scope }) {
  if (!summary) return null
  const rejected = (summary.rejected_by_trust || 0) + (summary.rejected_by_risk || 0) + (summary.rejected_by_capacity || 0)
  return (
    <div className="de-summary">
      {scope && <div className="de-summary-scope">{scope}</div>}
      <div className="de-summary-cards">
        <div className="de-card de-card--total">
          <span className="de-card-count">{summary.total}</span>
          <span className="de-card-label">Total Signals</span>
        </div>
        <div className="de-card de-card--traded">
          <span className="de-card-count">{summary.traded}</span>
          <span className="de-card-label">Traded</span>
        </div>
        <div className="de-card de-card--rejected">
          <span className="de-card-count">{rejected}</span>
          <span className="de-card-label">Rejected</span>
        </div>
        <div className="de-card de-card--eligible">
          <span className="de-card-count">{summary.eligible_not_selected}</span>
          <span className="de-card-label">Eligible · Not Selected</span>
        </div>
      </div>
    </div>
  )
}

/* ── Filter Bar ───────────────────────────────────────────────────── */

function FilterBar({ filters, onChange, options }) {
  return (
    <div className="de-filters">
      <div className="de-filter-group">
        <label className="de-filter-label">Symbol</label>
        <input
          type="text"
          className="de-filter-input"
          placeholder="e.g. AAPL"
          value={filters.symbol || ''}
          onChange={e => onChange('symbol', e.target.value || null)}
        />
      </div>

      <div className="de-filter-group">
        <label className="de-filter-label">Market</label>
        <select className="de-filter-select" value={filters.marketType || ''} onChange={e => onChange('market_type', e.target.value || null)}>
          <option value="">All</option>
          {(options.markets || []).map(m => <option key={m} value={m}>{m}</option>)}
        </select>
      </div>

      <div className="de-filter-group">
        <label className="de-filter-label">Pattern</label>
        <select className="de-filter-select" value={filters.patternId || ''} onChange={e => onChange('pattern_id', e.target.value || null)}>
          <option value="">All</option>
          {(options.patterns || []).map(p => <option key={p} value={p}>{p}</option>)}
        </select>
      </div>

      <div className="de-filter-group">
        <label className="de-filter-label">Trust</label>
        <select className="de-filter-select" value={filters.trustLabel || ''} onChange={e => onChange('trust_label', e.target.value || null)}>
          <option value="">All</option>
          <option value="TRUSTED">Trusted</option>
          <option value="WATCH">Watch</option>
          <option value="UNTRUSTED">Untrusted</option>
        </select>
      </div>

      <div className="de-filter-group">
        <label className="de-filter-label">Outcome</label>
        <select className="de-filter-select" value={filters.outcome || ''} onChange={e => onChange('outcome', e.target.value || null)}>
          <option value="">All</option>
          <option value="TRADED">Traded</option>
          <option value="REJECTED_BY_TRUST">Rejected · Trust</option>
          <option value="ELIGIBLE_NOT_SELECTED">Eligible · Not Selected</option>
        </select>
      </div>
    </div>
  )
}

/* ── Decision Trace (expanded panel) ──────────────────────────────── */

function DecisionTrace({ decision }) {
  const [showJson, setShowJson] = useState(false)
  const trace   = decision.decision_trace || []
  const metrics = decision.metrics || {}
  const hasMetrics = Object.keys(metrics).length > 0

  return (
    <div className="de-trace">
      {/* Horizontal signal process */}
      <div className="de-trace-flow">
        {trace.map((step, i) => (
          <div key={i} className="de-flow-item-wrap">
            <div className={`de-flow-item ${step.passed ? 'de-step--pass' : 'de-step--fail'}`}>
              <span className="de-step-circle">{step.passed ? '✓' : '✗'}</span>
              <div className="de-step-body">
                <span className="de-step-label">{step.label}</span>
                {step.detail && <span className="de-step-detail">{step.detail}</span>}
              </div>
            </div>
            {i < trace.length - 1 && <span className="de-flow-arrow" aria-hidden="true">→</span>}
          </div>
        ))}
      </div>

      {/* Supporting cards under process row */}
      <div className="de-trace-panels">
        {hasMetrics && (
          <div className="de-trace-metrics">
            <h4 className="de-metrics-title">Supporting Metrics</h4>
            <div className="de-metrics-grid">
              {Object.entries(metrics).map(([k, v]) => (
                <div key={k} className="de-metric">
                  <span className="de-metric-label">{k}</span>
                  <span className="de-metric-value">
                    {typeof v === 'number'
                      ? (k.includes('Rate') || k.includes('Return') || k.includes('Coverage') || k.includes('Corr')
                          ? fmtPct(v) : fmtNum(v, k.includes('Score') ? 2 : 0))
                      : String(v ?? '—')}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {decision.trade_info && (
          <div className="de-trace-trade">
            <h4 className="de-metrics-title">Trade Details</h4>
            <div className="de-metrics-grid">
              <div className="de-metric">
                <span className="de-metric-label">Portfolios</span>
                <span className="de-metric-value">
                  {Array.isArray(decision.trade_info.PORTFOLIO_IDS) && decision.trade_info.PORTFOLIO_IDS.length > 0
                    ? decision.trade_info.PORTFOLIO_IDS.join(', ')
                    : (decision.trade_info.PORTFOLIO_ID ?? '—')}
                </span>
              </div>
              <div className="de-metric">
                <span className="de-metric-label">Portfolio Count</span>
                <span className="de-metric-value">{fmtNum(decision.trade_info.PORTFOLIO_COUNT, 0)}</span>
              </div>
              <div className="de-metric">
                <span className="de-metric-label">Linked Trades</span>
                <span className="de-metric-value">{fmtNum(decision.trade_info.TRADE_COUNT, 0)}</span>
              </div>
              <div className="de-metric">
                <span className="de-metric-label">Side</span>
                <span className="de-metric-value">{decision.trade_info.SIDE}</span>
              </div>
              <div className="de-metric">
                <span className="de-metric-label">Price</span>
                <span className="de-metric-value">{fmtNum(decision.trade_info.PRICE, 4)}</span>
              </div>
              <div className="de-metric">
                <span className="de-metric-label">Quantity</span>
                <span className="de-metric-value">{fmtNum(decision.trade_info.QUANTITY, 4)}</span>
              </div>
              <div className="de-metric">
                <span className="de-metric-label">Notional</span>
                <span className="de-metric-value">{fmtNum(decision.trade_info.NOTIONAL, 2)}</span>
              </div>
              <div className="de-metric">
                <span className="de-metric-label">Total Quantity</span>
                <span className="de-metric-value">{fmtNum(decision.trade_info.TOTAL_QUANTITY, 4)}</span>
              </div>
              <div className="de-metric">
                <span className="de-metric-label">Total Notional</span>
                <span className="de-metric-value">{fmtNum(decision.trade_info.TOTAL_NOTIONAL, 2)}</span>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Advanced JSON */}
      <button type="button" className="de-json-toggle" onClick={() => setShowJson(v => !v)}>
        {showJson ? '▾ Hide' : '▸ Show'} raw policy JSON
      </button>
      {showJson && (
        <pre className="de-raw-json">{formatJson(decision.GATING_REASON)}</pre>
      )}
    </div>
  )
}

/* ── Decision Row ─────────────────────────────────────────────────── */

function DecisionRow({ decision, expanded, onToggle }) {
  const d = decision
  return (
    <>
      <tr className={`de-row ${expanded ? 'de-row--expanded' : ''}`} onClick={onToggle}>
        <td className="de-col-symbol">{d.SYMBOL}</td>
        <td className="de-col-pattern">
          <span className="de-pattern-id">{d.PATTERN_ID ?? '—'}</span>
          {d.MARKET_TYPE && <span className="de-market-tag">{d.MARKET_TYPE}</span>}
        </td>
        <td className="de-col-outcome"><OutcomePill outcome={d.outcome} /></td>
        <td className="de-col-trust"><TrustPill label={d.TRUST_LABEL} /></td>
        <td className="de-col-score">{fmtNum(d.SCORE)}</td>
        <td className="de-col-time">
          {d.SIGNAL_TS ? new Date(d.SIGNAL_TS).toLocaleString() : '—'}
        </td>
        <td className="de-col-why">{d.why_summary || '—'}</td>
        <td className="de-col-expand">
          <span className={`de-chevron ${expanded ? 'de-chevron--open' : ''}`}>▸</span>
        </td>
      </tr>
      {expanded && (
        <tr className="de-trace-row">
          <td colSpan={8}>
            <DecisionTrace decision={d} />
          </td>
        </tr>
      )}
    </>
  )
}

/* ── From-Cockpit Banner ──────────────────────────────────────────── */

function FromBriefBanner({ portfolioId, asOfTs, onClear }) {
  return (
    <div className="de-brief-banner" role="status">
      <span className="de-brief-icon">📋</span>
      <span className="de-brief-text">
        Filtered from Cockpit
        {portfolioId && ` (Portfolio ${portfolioId})`}
        {asOfTs && ` · As of ${new Date(asOfTs).toLocaleDateString()}`}
      </span>
      <button type="button" className="de-brief-clear" onClick={onClear}>Clear filters</button>
    </div>
  )
}

/* ── Main Page ────────────────────────────────────────────────────── */

export default function Signals() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)
  const [expandedId, setExpandedId] = useState(null)

  const filters = {
    symbol:      searchParams.get('symbol'),
    marketType:  searchParams.get('market_type'),
    patternId:   searchParams.get('pattern_id'),
    trustLabel:  searchParams.get('trust_label'),
    outcome:     searchParams.get('outcome'),
    runId:       searchParams.get('pipelineRunId') || searchParams.get('run_id'),
    asOfTs:      searchParams.get('asOf') || searchParams.get('as_of_ts'),
    fromBrief:   searchParams.get('from') === 'brief',
    portfolioId: searchParams.get('portfolioId'),
  }

  const hasFilters = Object.entries(filters).some(
    ([k, v]) => v && k !== 'fromBrief',
  )

  /* fetch decisions */
  const fetchDecisions = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const p = new URLSearchParams()
      if (filters.symbol)    p.set('symbol', filters.symbol)
      if (filters.marketType)p.set('market_type', filters.marketType)
      if (filters.patternId) p.set('pattern_id', filters.patternId)
      if (filters.trustLabel)p.set('trust_label', filters.trustLabel)
      if (filters.outcome)   p.set('outcome', filters.outcome)
      if (filters.runId)     p.set('run_id', filters.runId)
      if (filters.asOfTs)    p.set('as_of_ts', filters.asOfTs)
      if (filters.portfolioId) p.set('portfolio_id', filters.portfolioId)
      p.set('include_fallback', 'true')
      p.set('limit', '200')

      const res = await fetch(`${API_BASE}/signals/decisions?${p}`)
      if (!res.ok) throw new Error(res.statusText)
      setData(await res.json())
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [
    filters.symbol, filters.marketType, filters.patternId,
    filters.trustLabel, filters.outcome, filters.runId,
    filters.asOfTs, filters.portfolioId,
  ])

  useEffect(() => { fetchDecisions() }, [fetchDecisions])

  /* filter actions */
  const clearFilters = useCallback(() => setSearchParams({}), [setSearchParams])

  const setFilter = useCallback((key, value) => {
    const next = new URLSearchParams(searchParams)
    if (value) { next.set(key, value) } else { next.delete(key) }
    next.delete('from')
    setSearchParams(next)
  }, [searchParams, setSearchParams])

  const clearFilter = useCallback((key) => {
    const next = new URLSearchParams(searchParams)
    next.delete(key)
    next.delete('from')
    setSearchParams(next)
  }, [searchParams, setSearchParams])

  /* scope description */
  const scopeLabel = [
    filters.asOfTs && `Date: ${new Date(filters.asOfTs).toLocaleDateString()}`,
    filters.runId && `Run: ${filters.runId.slice(0, 8)}…`,
    filters.portfolioId && `Portfolio ${filters.portfolioId}`,
  ].filter(Boolean).join(' · ') || 'Today'

  /* ── render ──────────────────────────────────────────────────────── */

  if (loading) {
    return (
      <>
        <h1>Decision Explorer</h1>
        <p className="de-subtitle">Explain why trades happened (or didn't).</p>
        <LoadingState />
      </>
    )
  }

  if (error) {
    return (
      <>
        <h1>Decision Explorer</h1>
        <p className="de-subtitle">Explain why trades happened (or didn't).</p>
        <ErrorState message={error} />
      </>
    )
  }

  const decisions     = data?.decisions || []
  const summary       = data?.summary
  const filterOptions = data?.filter_options || {}

  return (
    <>
      <h1>Decision Explorer</h1>
      <p className="de-subtitle">Explain why trades happened (or didn't).</p>

      {/* From-Cockpit banner */}
      {filters.fromBrief && (
        <FromBriefBanner
          portfolioId={filters.portfolioId}
          asOfTs={filters.asOfTs}
          onClear={clearFilters}
        />
      )}

      {/* Executive Summary */}
      <SummaryBanner summary={summary} scope={scopeLabel} />

      {/* Filters */}
      <FilterBar
        filters={filters}
        onChange={setFilter}
        options={filterOptions}
      />

      {/* Active filter badges */}
      {hasFilters && (
        <div className="de-active-filters">
          <span className="de-active-label">Active:</span>
          <FilterBadge label="Symbol"  value={filters.symbol}    onClear={() => clearFilter('symbol')} />
          <FilterBadge label="Market"  value={filters.marketType} onClear={() => clearFilter('market_type')} />
          <FilterBadge label="Pattern" value={filters.patternId}  onClear={() => clearFilter('pattern_id')} />
          <FilterBadge label="Trust"   value={filters.trustLabel} onClear={() => clearFilter('trust_label')} />
          <FilterBadge label="Outcome" value={filters.outcome?.replace(/_/g, ' ')} onClear={() => clearFilter('outcome')} />
          <FilterBadge label="Run"     value={filters.runId?.slice(0, 8)} onClear={() => clearFilter('pipelineRunId')} />
          <FilterBadge
            label="Date"
            value={filters.asOfTs ? new Date(filters.asOfTs).toLocaleDateString() : null}
            onClear={() => clearFilter('asOf')}
          />
          <button type="button" className="de-clear-all" onClick={clearFilters}>Clear all</button>
        </div>
      )}

      {/* Results count */}
      <div className="de-count">
        {decisions.length > 0
          ? <span>Showing {decisions.length} decision{decisions.length !== 1 ? 's' : ''}{data?.total > decisions.length ? ` of ${data.total} total` : ''}</span>
          : <span>No decisions found</span>}
      </div>

      {/* Table or empty state */}
      {decisions.length === 0 ? (
        <EmptyState
          title="No decisions found"
          action={
            hasFilters
              ? <button type="button" onClick={clearFilters}>Clear all filters</button>
              : <Link to="/cockpit">Go to Cockpit</Link>
          }
          explanation={
            hasFilters
              ? 'No decisions match your filters. Try clearing some or using a different date.'
              : 'No signals available. Run the pipeline to generate recommendations.'
          }
          reasons={[
            'The pipeline may not have run today.',
            'Signals may have been filtered out by trust rules.',
            'Try clearing the run ID or date filters.',
          ]}
        />
      ) : (
        <div className="de-table-wrap">
          <table className="de-table">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Pattern</th>
                <th>Outcome</th>
                <th>Trust</th>
                <th>Signal Score</th>
                <th>Signal Time</th>
                <th>Why</th>
                <th className="de-th-expand" aria-label="Expand" />
              </tr>
            </thead>
            <tbody>
              {decisions.map((d, idx) => (
                <DecisionRow
                  key={d.RECOMMENDATION_ID || idx}
                  decision={d}
                  expanded={expandedId === (d.RECOMMENDATION_ID || idx)}
                  onToggle={() =>
                    setExpandedId(prev =>
                      prev === (d.RECOMMENDATION_ID || idx) ? null : (d.RECOMMENDATION_ID || idx),
                    )
                  }
                />
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Navigation */}
      <div className="de-nav">
        <Link to="/cockpit" className="de-nav-link">← Cockpit</Link>
        <Link to="/suggestions" className="de-nav-link">Suggestions →</Link>
      </div>
    </>
  )
}
