import { useState, useEffect, useCallback } from 'react'
import { API_BASE } from '../App'
import { usePortfolios } from '../context/PortfolioContext'
import LoadingState from '../components/LoadingState'
import EmptyState from '../components/EmptyState'
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend, ReferenceLine,
  AreaChart, Area, BarChart, Bar, Cell,
} from 'recharts'
import './ParallelWorlds.css'

/* ── Helpers ─────────────────────────────────────────── */

function formatMoney(val) {
  if (val == null) return '\u2014'
  return new Intl.NumberFormat('en-US', {
    style: 'currency', currency: 'USD',
    minimumFractionDigits: 0, maximumFractionDigits: 2,
  }).format(val)
}

function formatPct(val, decimals = 2) {
  if (val == null) return '\u2014'
  return `${(val * 100).toFixed(decimals)}%`
}

function formatDate(ts) {
  if (!ts) return '\u2014'
  try {
    const d = new Date(ts)
    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
  } catch { return String(ts) }
}

function deltaColor(val) {
  if (val == null || val === 0) return 'pw-delta--neutral'
  return val > 0 ? 'pw-delta--positive' : 'pw-delta--negative'
}

const SCENARIO_COLORS = [
  '#6366f1', '#f59e0b', '#10b981', '#ef4444',
  '#8b5cf6', '#ec4899', '#14b8a6', '#f97316',
]

/* ── Narrative Card ──────────────────────────────────── */

function NarrativeCard({ narrative, isAi }) {
  if (!narrative || !narrative.found) return null
  const n = narrative.narrative || {}

  return (
    <section className="pw-card pw-narrative">
      <div className="pw-card-header">
        <h3>Parallel Worlds Analysis</h3>
        <span className={`pw-badge ${isAi ? 'pw-badge--ai' : 'pw-badge--fallback'}`}>
          {isAi ? 'AI' : 'Fallback'}
        </span>
      </div>
      {n.headline && <p className="pw-narrative-headline">{n.headline}</p>}

      {n.gate_analysis && (
        <div className="pw-narrative-section">
          <h4>Gate Analysis</h4>
          <p>{n.gate_analysis}</p>
        </div>
      )}

      {n.what_if_summary && n.what_if_summary.length > 0 && (
        <div className="pw-narrative-section">
          <h4>What-If Insights</h4>
          <ul>
            {n.what_if_summary.map((b, i) => <li key={i}>{b}</li>)}
          </ul>
        </div>
      )}

      {n.regret_trend && (
        <div className="pw-narrative-section">
          <h4>Regret Trend</h4>
          <p>{n.regret_trend}</p>
        </div>
      )}

      {n.recommendation && (
        <div className="pw-narrative-section pw-narrative-recommendation">
          <h4>Consideration</h4>
          <p>{n.recommendation}</p>
        </div>
      )}
    </section>
  )
}

/* ── Type Labels ─────────────────────────────────────── */

const TYPE_LABELS = {
  THRESHOLD: 'Signal Filter',
  SIZING: 'Position Size',
  TIMING: 'Entry Timing',
  BASELINE: 'Baseline',
}

/* ── Gate Humanizer ──────────────────────────────────── */

function humanizeGate(gate) {
  const g = gate.gate?.toUpperCase() || ''
  const status = (gate.status || '').toUpperCase()

  // Baseline / DO_NOTHING
  if (g === 'BASELINE_SKIP' || g === 'BASELINE') {
    return gate.reason || 'This scenario skips all trade entries (stay in cash).'
  }

  // Timing
  if (g === 'TIMING') {
    const delay = gate.delay_bars || 1
    const trades = gate.trades_affected ?? '?'
    const pnl = gate.pnl_impact
    let sentence = `Delayed entry by ${delay} bar${delay > 1 ? 's' : ''}.`
    if (trades !== '?' && trades > 0) sentence += ` ${trades} trade${trades > 1 ? 's' : ''} affected.`
    else if (trades === 0) sentence += ' No trades were affected.'
    if (pnl != null && pnl !== 0) sentence += ` PnL impact: ${formatMoney(pnl)}.`
    return sentence
  }

  // Sizing
  if (g === 'SIZING') {
    const orig = gate.original_max_position_pct
    const adj = gate.adjusted_max_position_pct
    const mult = gate.multiplier
    const pnl = gate.pnl_delta
    let sentence = ''
    if (mult != null) sentence += `Position size multiplier: ${mult}x.`
    if (orig != null && adj != null) sentence += ` Max position moved from ${(orig * 100).toFixed(1)}% to ${(adj * 100).toFixed(1)}%.`
    if (pnl != null && pnl !== 0) sentence += ` PnL delta: ${formatMoney(pnl)}.`
    else if (pnl === 0) sentence += ' No PnL change.'
    return sentence.trim() || 'Position sizing was adjusted.'
  }

  // Threshold
  if (g === 'THRESHOLD') {
    const eligible = gate.newly_eligible ?? 0
    const excluded = gate.newly_excluded ?? 0
    const pnl = gate.estimated_new_pnl
    const zd = gate.zscore_delta
    const rd = gate.return_delta
    let sentence = ''
    if (zd != null && zd !== 0) sentence += `Z-score threshold shifted by ${zd > 0 ? '+' : ''}${zd}. `
    if (rd != null && rd !== 0) sentence += `Return threshold shifted by ${rd > 0 ? '+' : ''}${(rd * 100).toFixed(2)}%. `
    if (eligible > 0) sentence += `${eligible} new signal${eligible > 1 ? 's' : ''} became eligible. `
    if (excluded > 0) sentence += `${excluded} signal${excluded > 1 ? 's' : ''} excluded. `
    if (eligible === 0 && excluded === 0) sentence += 'No signals changed eligibility. '
    if (pnl != null && pnl !== 0) sentence += `Estimated PnL: ${formatMoney(pnl)}.`
    return sentence.trim() || 'Signal filter thresholds were adjusted.'
  }

  // Trust / Risk / Capacity (INFO gates)
  if (g === 'TRUST' || g === 'RISK' || g === 'CAPACITY') {
    if (gate.note) return gate.note
    if (status === 'PASSED') return `${g} gate passed — no restrictions.`
    if (status === 'BLOCKED') return `${g} gate blocked this trade.`
    if (status === 'INFO') return `${g} gate: informational only (applied normally).`
    return `${g} gate: ${status.toLowerCase()}.`
  }

  // Fallback — return the raw reason or a generic description
  if (gate.reason) return gate.reason
  return `${g} gate: ${status.toLowerCase()}.`
}

/* ── Scenario Table ──────────────────────────────────── */

function ScenarioTable({ results, expandedRow, setExpandedRow, confidenceMap }) {
  if (!results || !results.scenarios) return null

  return (
    <section className="pw-card">
      <div className="pw-card-header">
        <h3>Scenario Comparison</h3>
        <span className="pw-subtitle">
          Actual PnL: <strong>{formatMoney(results.actual?.pnl)}</strong> | Equity: <strong>{formatMoney(results.actual?.equity)}</strong>
        </span>
      </div>
      <p className="pw-help-text">
        Each row is an alternative universe: <em>"What if we had used different rules?"</em>
        Green numbers mean that scenario would have done better than what actually happened.
        Red means your actual approach was better. Click the arrow to see why each scenario differed.
      </p>
      <div className="pw-table-wrap">
        <table className="pw-table">
          <thead>
            <tr>
              <th>What If We Had...</th>
              <th>Category</th>
              <th className="pw-num" title="Counterfactual PnL — what the daily profit/loss would have been under this scenario">Scenario PnL</th>
              <th className="pw-num" title="Scenario PnL minus Actual PnL. Positive = scenario was better.">vs Actual</th>
              <th className="pw-num" title="Difference in total portfolio equity">Equity Impact</th>
              <th className="pw-num" title="Number of trades the scenario would have taken"># Trades</th>
              <th title="Signal confidence based on win-rate and consistency">Signal</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {results.scenarios.map((s, i) => (
              <>
                <tr key={s.scenario_id} className={s.outperformed ? 'pw-row--outperformed' : ''}>
                  <td className="pw-scenario-name">{s.display_name || s.scenario_name}</td>
                  <td><span className={`pw-type pw-type--${(s.scenario_type || '').toLowerCase()}`}>{TYPE_LABELS[s.scenario_type] || s.scenario_type}</span></td>
                  <td className="pw-num">{formatMoney(s.cf_pnl)}</td>
                  <td className={`pw-num ${deltaColor(s.pnl_delta)}`}>{formatMoney(s.pnl_delta)}</td>
                  <td className={`pw-num ${deltaColor(s.equity_delta)}`}>{formatMoney(s.equity_delta)}</td>
                  <td className="pw-num">{s.cf_trades}</td>
                  <td>{(() => {
                    const conf = confidenceMap?.[s.scenario_name]
                    if (!conf) return null
                    const cls = `pw-conf-badge pw-conf--${conf.confidence_class?.toLowerCase() || 'noise'}`
                    return <span className={cls} title={conf.confidence_reason}>{CONFIDENCE_LABELS[conf.confidence_class] || conf.confidence_class}</span>
                  })()}</td>
                  <td>
                    <button
                      className="pw-expand-btn"
                      onClick={() => setExpandedRow(expandedRow === i ? null : i)}
                      aria-label="Toggle details"
                    >
                      {expandedRow === i ? '\u25B2' : '\u25BC'}
                    </button>
                  </td>
                </tr>
                {expandedRow === i && (
                  <tr key={`${s.scenario_id}-detail`} className="pw-detail-row">
                    <td colSpan={8}>
                      <div className="pw-detail">
                        <h4>Decision Trace</h4>
                        <p className="pw-muted pw-detail-help">This shows which decision gates passed, blocked, or modified trades in this scenario.</p>
                        {s.decision_trace && s.decision_trace.length > 0 ? (
                          <div className="pw-trace-grid">
                            {s.decision_trace.map((gate, gi) => (
                              <div key={gi} className={`pw-gate pw-gate--${(gate.status || '').toLowerCase()}`}>
                                <span className="pw-gate-name">{gate.gate}</span>
                                <span className="pw-gate-status">{gate.status}</span>
                                <p className="pw-gate-explanation">{humanizeGate(gate)}</p>
                              </div>
                            ))}
                          </div>
                        ) : <p className="pw-muted">No decision trace available.</p>}
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

/* ── Equity Curves Chart ─────────────────────────────── */

function EquityCurvesChart({ curves }) {
  if (!curves || curves.length === 0) return null

  // Merge all curves into a single dataset by date
  const dateMap = {}
  curves.forEach((c, ci) => {
    (c.points || []).forEach(p => {
      const key = p.as_of_ts?.split('T')[0] || p.as_of_ts
      if (!dateMap[key]) dateMap[key] = { date: key }
      dateMap[key][c.scenario_name] = p.equity
    })
  })
  const data = Object.values(dateMap).sort((a, b) => a.date.localeCompare(b.date))

  if (data.length === 0) return null

  return (
    <section className="pw-card">
      <div className="pw-card-header"><h3>Equity Curves</h3></div>
      <p className="pw-help-text">
        The solid line is your actual portfolio equity over time.
        Dashed lines show how each scenario would have tracked.
        If a dashed line stays above the solid line, that approach would have compounded better.
      </p>
      <div className="pw-chart-wrap">
        <ResponsiveContainer width="100%" height={340}>
          <LineChart data={data} margin={{ top: 10, right: 20, left: 10, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" opacity={0.15} />
            <XAxis dataKey="date" tickFormatter={formatDate} fontSize={11} />
            <YAxis
              tickFormatter={v => `$${(v / 1000).toFixed(0)}k`}
              fontSize={11}
              domain={['dataMin - 100', 'dataMax + 100']}
            />
            <Tooltip
              formatter={(v, name) => [formatMoney(v), name]}
              labelFormatter={formatDate}
            />
            <Legend />
            {curves.map((c, i) => (
              <Line
                key={c.scenario_name}
                type="monotone"
                dataKey={c.scenario_name}
                stroke={c.scenario_name === 'ACTUAL' ? '#3b82f6' : SCENARIO_COLORS[i % SCENARIO_COLORS.length]}
                strokeWidth={c.scenario_name === 'ACTUAL' ? 2.5 : 1.5}
                dot={false}
                strokeDasharray={c.scenario_name === 'ACTUAL' ? undefined : '5 3'}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  )
}

/* ── Policy Health Card ──────────────────────────────── */

const HEALTH_CONFIG = {
  HEALTHY: { label: 'Healthy', cls: 'pw-health--healthy', icon: '\u2705' },
  WATCH: { label: 'Watch', cls: 'pw-health--watch', icon: '\uD83D\uDC41' },
  MONITOR: { label: 'Monitor', cls: 'pw-health--monitor', icon: '\uD83D\uDD0D' },
  REVIEW_SUGGESTED: { label: 'Review Suggested', cls: 'pw-health--review', icon: '\u26A0\uFE0F' },
  NEEDS_ATTENTION: { label: 'Needs Attention', cls: 'pw-health--attention', icon: '\uD83D\uDEA8' },
}

function PolicyHealthCard({ diagnostics }) {
  if (!diagnostics || !diagnostics.found) return null

  const health = HEALTH_CONFIG[diagnostics.policy_health] || HEALTH_CONFIG.HEALTHY

  return (
    <section className="pw-card pw-policy-card">
      <div className="pw-card-header">
        <h3>Policy Health</h3>
        <span className={`pw-health-badge ${health.cls}`}>
          {health.icon} {health.label}
        </span>
      </div>
      <p className="pw-help-text">
        An at-a-glance view of whether your current trading rules are optimal.
        Based on how many alternative scenarios consistently outperform your approach.
      </p>

      <div className="pw-policy-grid">
        {/* Stability gauge */}
        <div className="pw-policy-block">
          <div className="pw-policy-block-label">Stability</div>
          <div className="pw-policy-gauge">
            <div className="pw-policy-gauge-bar" style={{ width: `${diagnostics.stability_score || 0}%` }} />
          </div>
          <div className="pw-policy-gauge-meta">
            <span>{diagnostics.stability_label}</span>
            <span>{diagnostics.stability_score}/100</span>
          </div>
        </div>

        {/* Signal breakdown */}
        <div className="pw-policy-block">
          <div className="pw-policy-block-label">Signal Breakdown</div>
          <div className="pw-policy-signals">
            {diagnostics.strong_signals > 0 && <span className="pw-conf-badge pw-conf--strong">Strong: {diagnostics.strong_signals}</span>}
            {diagnostics.emerging_signals > 0 && <span className="pw-conf-badge pw-conf--emerging">Emerging: {diagnostics.emerging_signals}</span>}
            {diagnostics.weak_signals > 0 && <span className="pw-conf-badge pw-conf--weak">Weak: {diagnostics.weak_signals}</span>}
            <span className="pw-conf-badge pw-conf--noise">Noise: {diagnostics.noise_signals}</span>
          </div>
        </div>

        {/* Dominant driver */}
        {diagnostics.dominant_driver_label && (
          <div className="pw-policy-block">
            <div className="pw-policy-block-label">Biggest Regret Area</div>
            <div className="pw-policy-driver">
              <span className={`pw-type pw-type--${(diagnostics.dominant_driver_type || '').toLowerCase()}`}>{diagnostics.dominant_driver_label}</span>
              {diagnostics.dominant_driver_regret > 0 && (
                <span className="pw-policy-driver-regret">{formatMoney(diagnostics.dominant_driver_regret)} cumulative regret</span>
              )}
            </div>
          </div>
        )}

        {/* Top recommendation */}
        {diagnostics.top_recommendation && (
          <div className="pw-policy-block">
            <div className="pw-policy-block-label">Top Candidate</div>
            <div className="pw-policy-rec">
              <strong>{diagnostics.top_recommendation}</strong>
              {diagnostics.top_recommendation_type && (
                <span className={`pw-type pw-type--${(diagnostics.top_recommendation_type || '').toLowerCase()}`}>{TYPE_LABELS[diagnostics.top_recommendation_type] || diagnostics.top_recommendation_type}</span>
              )}
            </div>
          </div>
        )}
      </div>

      <p className="pw-policy-reason">{diagnostics.policy_health_reason}</p>
    </section>
  )
}

/* ── Confidence Labels ────────────────────────────────── */

const CONFIDENCE_LABELS = {
  STRONG: 'Strong',
  EMERGING: 'Emerging',
  WEAK: 'Weak',
  NOISE: 'Noise',
}

const STRENGTH_LABELS = {
  STRONG_SIGNAL: 'Strong Signal',
  CANDIDATE: 'Candidate',
  EXPERIMENTAL: 'Experimental',
  NOT_ACTIONABLE: 'Not Actionable',
}

/* ── Confidence Panel ────────────────────────────────── */

function ConfidencePanel({ confidenceData }) {
  if (!confidenceData || confidenceData.length === 0) return null

  const strong = confidenceData.filter(c => c.confidence_class === 'STRONG')
  const emerging = confidenceData.filter(c => c.confidence_class === 'EMERGING')
  const weak = confidenceData.filter(c => c.confidence_class === 'WEAK')
  const noise = confidenceData.filter(c => c.confidence_class === 'NOISE')

  return (
    <section className="pw-card">
      <div className="pw-card-header"><h3>Signal Confidence</h3></div>
      <p className="pw-help-text">
        How reliable is each scenario's signal? Based on win-rate, cumulative impact, and consistency.
        <em> Strong </em> = consistent outperformance over many days. <em> Noise </em> = no meaningful pattern.
      </p>
      <div className="pw-confidence-grid">
        {[
          { label: 'Strong', items: strong, cls: 'pw-conf--strong' },
          { label: 'Emerging', items: emerging, cls: 'pw-conf--emerging' },
          { label: 'Weak', items: weak, cls: 'pw-conf--weak' },
          { label: 'Noise', items: noise, cls: 'pw-conf--noise' },
        ].filter(g => g.items.length > 0).map(group => (
          <div key={group.label} className={`pw-conf-group ${group.cls}`}>
            <div className="pw-conf-group-header">
              <span className={`pw-conf-badge ${group.cls}`}>{group.label}</span>
              <span className="pw-conf-count">{group.items.length} scenario{group.items.length !== 1 ? 's' : ''}</span>
            </div>
            {group.items.map(c => (
              <div key={c.scenario_id} className="pw-conf-item">
                <span className="pw-conf-name">{c.scenario_display_name || c.scenario_name}</span>
                <span className="pw-conf-reason">{c.confidence_reason}</span>
                <div className="pw-conf-metrics">
                  <span>Win rate: <strong>{c.outperform_pct}%</strong></span>
                  <span>Cumulative: <strong className={deltaColor(c.cumulative_delta)}>{formatMoney(c.cumulative_delta)}</strong></span>
                  <span>{c.total_days} days</span>
                </div>
              </div>
            ))}
          </div>
        ))}
      </div>
    </section>
  )
}

/* ── Regret Attribution ──────────────────────────────── */

function RegretAttribution({ attribution }) {
  if (!attribution || !attribution.data || attribution.data.length === 0) return null
  const dominant = attribution.dominant_driver

  return (
    <section className="pw-card">
      <div className="pw-card-header"><h3>Regret Attribution</h3></div>
      <p className="pw-help-text">
        Where is the biggest gap between actual results and what <em>could</em> have been?
        The dominant driver is the category of rule changes that would have made the most difference.
      </p>
      {dominant && (
        <div className="pw-attribution-dominant">
          <span className="pw-attribution-dominant-label">Dominant regret driver:</span>
          <span className={`pw-type pw-type--${(dominant.scenario_type || '').toLowerCase()}`}>{dominant.type_label}</span>
          {dominant.best_scenario_display_name && (
            <span className="pw-attribution-best">Best: {dominant.best_scenario_display_name}</span>
          )}
        </div>
      )}
      <div className="pw-attribution-grid">
        {attribution.data.map(a => (
          <div key={a.scenario_type} className={`pw-attribution-card ${a.is_dominant_driver ? 'pw-attribution-card--dominant' : ''}`}>
            <div className="pw-attribution-card-header">
              <span className={`pw-type pw-type--${(a.scenario_type || '').toLowerCase()}`}>{a.type_label}</span>
              <span className="pw-attribution-rank">#{a.regret_rank}</span>
            </div>
            <div className="pw-attribution-stats">
              <div className="pw-attribution-stat">
                <span className="pw-attribution-stat-label">Avg win rate</span>
                <span className="pw-attribution-stat-value">{a.avg_outperform_pct}%</span>
              </div>
              <div className="pw-attribution-stat">
                <span className="pw-attribution-stat-label">Cumulative regret</span>
                <span className="pw-attribution-stat-value">{formatMoney(a.total_cumulative_regret)}</span>
              </div>
              <div className="pw-attribution-stat">
                <span className="pw-attribution-stat-label">Best delta</span>
                <span className={`pw-attribution-stat-value ${deltaColor(a.max_cumulative_delta)}`}>{formatMoney(a.max_cumulative_delta)}</span>
              </div>
            </div>
            <div className="pw-attribution-best-scenario">
              Best: <strong>{a.best_scenario_display_name || a.best_scenario_name}</strong>
              <span className={`pw-conf-badge pw-conf--${(a.best_confidence_class || 'noise').toLowerCase()}`}>{CONFIDENCE_LABELS[a.best_confidence_class] || a.best_confidence_class}</span>
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}

/* ── Regret Heatmap ──────────────────────────────────── */

function RegretHeatmap({ regretData }) {
  if (!regretData || regretData.length === 0) return null

  // Group by scenario — prefer display name
  const scenarios = {}
  const dates = new Set()
  regretData.forEach(r => {
    const name = r.scenario_display_name || r.SCENARIO_DISPLAY_NAME || r.scenario_name || r.SCENARIO_NAME
    const date = (r.as_of_ts || r.AS_OF_TS || '').split('T')[0]
    const delta = r.pnl_delta ?? r.PNL_DELTA ?? 0
    dates.add(date)
    if (!scenarios[name]) scenarios[name] = {}
    scenarios[name][date] = delta
  })

  const sortedDates = [...dates].sort()
  const scenarioNames = Object.keys(scenarios).sort()

  if (scenarioNames.length === 0 || sortedDates.length === 0) return null

  // Compute max abs delta for color scaling
  const allDeltas = regretData.map(r => Math.abs(r.pnl_delta ?? r.PNL_DELTA ?? 0))
  const maxDelta = Math.max(...allDeltas, 1)

  function cellColor(val) {
    if (val == null || val === 0) return '#f8f9fa'
    const intensity = Math.min(Math.abs(val) / maxDelta, 1)
    if (val > 0) return `rgba(16, 185, 129, ${0.15 + intensity * 0.7})`
    return `rgba(239, 68, 68, ${0.15 + intensity * 0.7})`
  }

  return (
    <section className="pw-card">
      <div className="pw-card-header"><h3>Regret Heatmap</h3></div>
      <p className="pw-help-text">
        Each cell shows how much better (green) or worse (red) a scenario would have been on that day.
        $0 means no difference. A row that is consistently green suggests that rule change might genuinely improve results.
      </p>
      <div className="pw-heatmap-wrap">
        <table className="pw-heatmap">
          <thead>
            <tr>
              <th>Scenario</th>
              {sortedDates.map(d => <th key={d}>{formatDate(d)}</th>)}
            </tr>
          </thead>
          <tbody>
            {scenarioNames.map(name => (
              <tr key={name}>
                <td className="pw-heatmap-label">{name}</td>
                {sortedDates.map(d => {
                  const val = scenarios[name]?.[d]
                  return (
                    <td
                      key={d}
                      className="pw-heatmap-cell"
                      style={{ backgroundColor: cellColor(val) }}
                      title={`${name} on ${d}: ${formatMoney(val)}`}
                    >
                      {val != null ? formatMoney(val) : ''}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

/* ── Tuning Surface Chart ────────────────────────────── */

const FAMILY_LABELS = {
  ZSCORE_SWEEP: 'Z-Score Threshold',
  RETURN_SWEEP: 'Return Threshold',
  SIZING_SWEEP: 'Position Sizing',
  TIMING_SWEEP: 'Entry Timing',
}

const FAMILY_X_LABELS = {
  ZSCORE_SWEEP: 'Z-Score Delta',
  RETURN_SWEEP: 'Return Delta (%)',
  SIZING_SWEEP: 'Position Size Multiplier',
  TIMING_SWEEP: 'Entry Delay (bars)',
}

function TuningSurfaceChart({ familyData }) {
  if (!familyData || !familyData.points || familyData.points.length === 0) return null

  const family = familyData.family
  const points = familyData.points.map(p => ({
    ...p,
    param: family === 'RETURN_SWEEP' ? (p.param_value * 100) : p.param_value,
    pnl_delta: p.total_pnl_delta || 0,
    display: p.display_name,
  }))

  const optimal = points.find(p => p.is_optimal)
  const safeTweak = points.find(p => p.is_minimal_safe_tweak)
  const current = points.find(p => p.is_current_setting)

  return (
    <section className="pw-card pw-surface-card">
      <div className="pw-card-header">
        <h3>{FAMILY_LABELS[family] || family} Surface</h3>
        <div className="pw-surface-legend">
          {optimal && <span className="pw-surface-legend-item pw-surface-legend--optimal">Optimal</span>}
          {safeTweak && <span className="pw-surface-legend-item pw-surface-legend--safe">Safe Tweak</span>}
          <span className="pw-surface-legend-item pw-surface-legend--current">Current</span>
        </div>
      </div>
      <p className="pw-help-text">
        Each point shows the cumulative PnL impact of a different parameter setting.
        <em> Green</em> = better than current, <em>Red</em> = worse.
        The higher the bar, the more that setting would have helped.
      </p>
      <div className="pw-chart-wrap">
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={points} margin={{ top: 20, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" opacity={0.15} />
            <XAxis
              dataKey="display"
              fontSize={10}
              angle={-30}
              textAnchor="end"
              height={60}
              interval={0}
            />
            <YAxis
              tickFormatter={v => `$${v}`}
              fontSize={11}
              label={{ value: 'Cumulative PnL Delta ($)', angle: -90, position: 'insideLeft', fontSize: 10, fill: '#6c757d' }}
            />
            <ReferenceLine y={0} stroke="#adb5bd" strokeDasharray="3 3" />
            <Tooltip
              formatter={(v, name) => [formatMoney(v), 'PnL Delta']}
              labelFormatter={label => label}
              contentStyle={{ fontSize: '0.82rem' }}
            />
            <Bar dataKey="pnl_delta" radius={[4, 4, 0, 0]}>
              {points.map((p, i) => {
                let fill = '#e9ecef'
                if (p.pnl_delta > 0) fill = '#198754'
                else if (p.pnl_delta < 0) fill = '#dc3545'
                if (p.is_current_setting) fill = '#0d6efd'
                if (p.is_optimal) fill = '#198754'
                return <Cell key={i} fill={fill} stroke={p.is_optimal ? '#0f5132' : p.is_minimal_safe_tweak ? '#fd7e14' : 'none'} strokeWidth={p.is_optimal || p.is_minimal_safe_tweak ? 2 : 0} />
              })}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      {/* Markers legend below chart */}
      <div className="pw-surface-markers">
        {optimal && (
          <div className="pw-surface-marker">
            <span className="pw-surface-dot pw-surface-dot--optimal" />
            <span>Optimal: <strong>{optimal.display}</strong> ({formatMoney(optimal.pnl_delta)}, {optimal.win_rate_pct}% win rate, {optimal.observation_days} days)</span>
          </div>
        )}
        {safeTweak && (
          <div className="pw-surface-marker">
            <span className="pw-surface-dot pw-surface-dot--safe" />
            <span>Minimal safe tweak: <strong>{safeTweak.display}</strong> ({formatMoney(safeTweak.pnl_delta)})</span>
          </div>
        )}
        {current && (
          <div className="pw-surface-marker">
            <span className="pw-surface-dot pw-surface-dot--current" />
            <span>Current setting: <strong>{current.display}</strong></span>
          </div>
        )}
      </div>
    </section>
  )
}

/* ── Regime Sensitivity Chart ────────────────────────── */

function RegimeSensitivityChart({ regimeData, family }) {
  if (!regimeData || regimeData.length === 0) return null

  const filtered = regimeData.filter(r => !family || r.sweep_family === family)
  if (filtered.length === 0) return null

  // Pivot: one row per scenario, columns for each regime
  const byScenario = {}
  filtered.forEach(r => {
    const key = r.display_name || r.scenario_id
    if (!byScenario[key]) byScenario[key] = { name: key, is_fragile: r.is_regime_fragile }
    byScenario[key][r.regime] = r.regime_win_rate_pct || 0
  })
  const data = Object.values(byScenario)
  if (data.length === 0) return null

  return (
    <section className="pw-card">
      <div className="pw-card-header"><h3>Regime Sensitivity</h3></div>
      <p className="pw-help-text">
        How each parameter setting performs across different market conditions.
        <em> Fragile</em> scenarios only work in one regime — use with caution.
      </p>
      <div className="pw-chart-wrap">
        <ResponsiveContainer width="100%" height={Math.max(200, data.length * 32 + 60)}>
          <BarChart data={data} layout="vertical" margin={{ top: 10, right: 20, left: 100, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" opacity={0.15} />
            <XAxis type="number" domain={[0, 100]} tickFormatter={v => `${v}%`} fontSize={11} />
            <YAxis type="category" dataKey="name" fontSize={10} width={90} />
            <Tooltip formatter={(v) => [`${v}%`, '']} contentStyle={{ fontSize: '0.82rem' }} />
            <Legend />
            <Bar dataKey="QUIET" fill="#6c757d" name="Quiet" barSize={8} />
            <Bar dataKey="NORMAL" fill="#0d6efd" name="Normal" barSize={8} />
            <Bar dataKey="VOLATILE" fill="#fd7e14" name="Volatile" barSize={8} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </section>
  )
}

/* ── Safety Checklist ────────────────────────────────── */

function SafetyChecklist({ checks }) {
  if (!checks || checks.length === 0) return <p className="pw-muted">No safety data.</p>
  return (
    <ul className="pw-safety-checklist">
      {checks.map((c, i) => (
        <li key={i} className={`pw-safety-item ${c.passed ? 'pw-safety--pass' : 'pw-safety--fail'}`}>
          <span className="pw-safety-icon">{c.passed ? '\u2705' : '\u274C'}</span>
          <span className="pw-safety-label">{c.explanation || c.check_name}</span>
          <span className="pw-safety-value">
            {c.actual_value}{c.threshold ? ` (need: ${c.threshold})` : ''}
          </span>
        </li>
      ))}
    </ul>
  )
}

/* ── Recommendation Cards ────────────────────────────── */

const CONF_BADGE_MAP = {
  STRONG:   { cls: 'pw-conf--strong',   label: 'Strong' },
  EMERGING: { cls: 'pw-conf--emerging',  label: 'Emerging' },
  WEAK:     { cls: 'pw-conf--weak',      label: 'Weak' },
  NOISE:    { cls: 'pw-conf--noise',     label: 'Noise' },
}

function RecommendationCards({ recommendations, onLoadSafety, safetyCache, domainFilter }) {
  const [expandedRec, setExpandedRec] = useState(null)

  const filtered = (recommendations || []).filter(r =>
    !domainFilter || r.domain === domainFilter
  )

  if (filtered.length === 0) {
    return (
      <section className="pw-card">
        <div className="pw-card-header"><h3>Recommendations</h3></div>
        <p className="pw-muted" style={{ padding: '1rem' }}>
          No recommendations available yet. More sweep data is needed to generate reliable suggestions.
        </p>
      </section>
    )
  }

  const handleExpand = (recId) => {
    if (expandedRec === recId) {
      setExpandedRec(null)
    } else {
      setExpandedRec(recId)
      if (onLoadSafety && !safetyCache?.[recId]) {
        onLoadSafety(recId)
      }
    }
  }

  return (
    <section className="pw-card">
      <div className="pw-card-header"><h3>Top Recommendations</h3></div>
      <div className="pw-rec-list">
        {filtered.map(rec => {
          const isExpanded = expandedRec === rec.rec_id
          const confBadge = CONF_BADGE_MAP[rec.confidence_class] || CONF_BADGE_MAP.NOISE
          const isAggressive = rec.recommendation_type === 'AGGRESSIVE'

          return (
            <div key={rec.rec_id}
              className={`pw-rec-card ${isAggressive ? 'pw-rec-card--aggressive' : 'pw-rec-card--conservative'}`}
            >
              <div className="pw-rec-header" onClick={() => handleExpand(rec.rec_id)}>
                <div className="pw-rec-header-left">
                  <span className={`pw-rec-type-pill ${isAggressive ? 'pw-rec-type--aggressive' : 'pw-rec-type--conservative'}`}>
                    {rec.type_label || rec.recommendation_type}
                  </span>
                  <span className="pw-rec-family">{rec.family_label || rec.sweep_family}</span>
                  <span className={`pw-conf-badge ${confBadge.cls}`}>{confBadge.label}</span>
                </div>
                <div className="pw-rec-header-right">
                  <div className="pw-rec-delta">
                    <span className="pw-rec-delta-value">{formatMoney(rec.expected_cumulative_delta)}</span>
                    <span className="pw-rec-delta-label">cumulative</span>
                  </div>
                  <span className={`pw-rec-safety-badge ${rec.safety_status === 'READY_FOR_REVIEW' ? 'pw-rec-safety--ready' : 'pw-rec-safety--notready'}`}>
                    {rec.safety_status === 'READY_FOR_REVIEW' ? 'Ready for Review' : 'Not Ready'}
                  </span>
                  <span className="pw-rec-expand">{isExpanded ? '\u25B2' : '\u25BC'}</span>
                </div>
              </div>

              {/* Summary row */}
              <div className="pw-rec-summary">
                <span>{rec.parameter_name}: <strong>{rec.current_value}</strong> → <strong>{rec.recommended_value}</strong></span>
                <span>{rec.win_rate_pct}% win rate</span>
                <span>{rec.observation_days} days observed</span>
                {rec.regime_fragile && <span className="pw-rec-fragile-badge">Regime Fragile</span>}
              </div>

              {/* Expanded details */}
              {isExpanded && (
                <div className="pw-rec-details">
                  <div className="pw-rec-detail-grid">
                    <div className="pw-rec-detail-item">
                      <span className="pw-rec-detail-label">Expected daily delta</span>
                      <span className="pw-rec-detail-value">{formatMoney(rec.expected_daily_delta)}</span>
                    </div>
                    <div className="pw-rec-detail-item">
                      <span className="pw-rec-detail-label">Domain</span>
                      <span className="pw-rec-detail-value">{rec.domain_label || rec.domain}</span>
                    </div>
                    <div className="pw-rec-detail-item">
                      <span className="pw-rec-detail-label">Confidence</span>
                      <span className="pw-rec-detail-value">{rec.confidence_reason}</span>
                    </div>
                    {rec.regime_detail && (
                      <div className="pw-rec-detail-item">
                        <span className="pw-rec-detail-label">Regime Note</span>
                        <span className="pw-rec-detail-value">{rec.regime_detail}</span>
                      </div>
                    )}
                    {rec.rollback_note && (
                      <div className="pw-rec-detail-item">
                        <span className="pw-rec-detail-label">Rollback Plan</span>
                        <span className="pw-rec-detail-value">{rec.rollback_note}</span>
                      </div>
                    )}
                  </div>

                  <h4 className="pw-rec-section-title">Safety Checks</h4>
                  <SafetyChecklist checks={safetyCache?.[rec.rec_id]} />

                  {/* Future governance buttons */}
                  <div className="pw-approval-row">
                    <button className="pw-approval-btn pw-approval--approve" disabled title="Governance workflow coming soon">Approve</button>
                    <button className="pw-approval-btn pw-approval--reject" disabled title="Governance workflow coming soon">Reject</button>
                    <button className="pw-approval-btn pw-approval--dismiss" disabled title="Governance workflow coming soon">Dismiss</button>
                  </div>
                </div>
              )}
            </div>
          )
        })}
      </div>
    </section>
  )
}

/* ── Signal Tuning Tab ───────────────────────────────── */

function SignalTuningTab({ pid, surfaceData, regimeData, recommendations, onLoadSafety, safetyCache }) {
  const zscoreSurface = surfaceData?.families?.find(f => f.family === 'ZSCORE_SWEEP')
  const returnSurface = surfaceData?.families?.find(f => f.family === 'RETURN_SWEEP')

  return (
    <>
      <div className="pw-disclaimer">
        Analysis only — these are suggestions based on historical replay, not instructions.
        All changes require human review and approval.
      </div>
      <RecommendationCards
        recommendations={recommendations}
        domainFilter="SIGNAL"
        onLoadSafety={onLoadSafety}
        safetyCache={safetyCache}
      />
      {!zscoreSurface && !returnSurface && (
        <EmptyState
          title="No signal tuning data yet"
          explanation="Run a parameter sweep to see signal tuning surfaces."
          reasons={['The sweep may not have run yet.', 'PW_SWEEP_ENABLED may be false in APP_CONFIG.']}
        />
      )}
      {zscoreSurface && <TuningSurfaceChart familyData={zscoreSurface} />}
      {returnSurface && <TuningSurfaceChart familyData={returnSurface} />}
      {regimeData && regimeData.length > 0 && (
        <RegimeSensitivityChart regimeData={regimeData.filter(r => r.sweep_family?.includes('ZSCORE') || r.sweep_family?.includes('RETURN'))} />
      )}
    </>
  )
}

/* ── Portfolio Tuning Tab ────────────────────────────── */

function PortfolioTuningTab({ pid, surfaceData, regimeData, recommendations, onLoadSafety, safetyCache }) {
  const sizingSurface = surfaceData?.families?.find(f => f.family === 'SIZING_SWEEP')
  const timingSurface = surfaceData?.families?.find(f => f.family === 'TIMING_SWEEP')

  return (
    <>
      <div className="pw-disclaimer">
        Analysis only — these are suggestions based on historical replay, not instructions.
        All changes require human review and approval.
      </div>
      <RecommendationCards
        recommendations={recommendations}
        domainFilter="PORTFOLIO"
        onLoadSafety={onLoadSafety}
        safetyCache={safetyCache}
      />
      {!sizingSurface && !timingSurface && (
        <EmptyState
          title="No portfolio tuning data yet"
          explanation="Run a parameter sweep to see portfolio tuning surfaces."
          reasons={['The sweep may not have run yet.', 'PW_SWEEP_ENABLED may be false in APP_CONFIG.']}
        />
      )}
      {sizingSurface && <TuningSurfaceChart familyData={sizingSurface} />}
      {timingSurface && <TuningSurfaceChart familyData={timingSurface} />}
      {regimeData && regimeData.length > 0 && (
        <RegimeSensitivityChart regimeData={regimeData.filter(r => r.sweep_family?.includes('SIZING') || r.sweep_family?.includes('TIMING'))} />
      )}
    </>
  )
}

/* ── Main Page ───────────────────────────────────────── */

export default function ParallelWorlds() {
  const { portfolios } = usePortfolios()
  const [selectedPortfolio, setSelectedPortfolio] = useState(null)
  const [results, setResults] = useState(null)
  const [narrative, setNarrative] = useState(null)
  const [curves, setCurves] = useState(null)
  const [regret, setRegret] = useState(null)
  const [confidence, setConfidence] = useState(null)
  const [attribution, setAttribution] = useState(null)
  const [diagnostics, setDiagnostics] = useState(null)
  const [surfaceData, setSurfaceData] = useState(null)
  const [regimeData, setRegimeData] = useState(null)
  const [recommendations, setRecommendations] = useState(null)
  const [safetyCache, setSafetyCache] = useState({})
  const [activeTab, setActiveTab] = useState('overview')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [expandedRow, setExpandedRow] = useState(null)

  // Auto-select first portfolio
  useEffect(() => {
    if (portfolios && portfolios.length > 0 && !selectedPortfolio) {
      const first = portfolios[0]
      setSelectedPortfolio(first.portfolio_id || first.PORTFOLIO_ID)
    }
  }, [portfolios, selectedPortfolio])

  const loadData = useCallback(async (pid) => {
    if (!pid) return
    setLoading(true)
    setError(null)
    try {
      const [resResult, resNarrative, resCurves, resRegret, resConfidence, resAttribution, resDiagnostics, resSurface, resRegime, resRecs] = await Promise.all([
        fetch(`${API_BASE}/parallel-worlds/results?portfolio_id=${pid}`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/narrative?portfolio_id=${pid}`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/equity-curves?portfolio_id=${pid}`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/regret?portfolio_id=${pid}&days=20`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/confidence?portfolio_id=${pid}`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/regret-attribution?portfolio_id=${pid}`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/policy-diagnostics?portfolio_id=${pid}`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/tuning-surface?portfolio_id=${pid}`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/parallel-worlds/regime-sensitivity?portfolio_id=${pid}`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/parallel-worlds/recommendations?portfolio_id=${pid}`).then(r => r.json()).catch(() => null),
      ])
      setResults(resResult)
      setNarrative(resNarrative)
      setCurves(resCurves)
      setRegret(resRegret)
      setConfidence(resConfidence)
      setAttribution(resAttribution)
      setDiagnostics(resDiagnostics)
      setSurfaceData(resSurface)
      setRegimeData(resRegime)
      setRecommendations(resRecs)
      setSafetyCache({})
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    if (selectedPortfolio) loadData(selectedPortfolio)
  }, [selectedPortfolio, loadData])

  const loadSafetyChecks = useCallback(async (recId) => {
    try {
      const res = await fetch(`${API_BASE}/parallel-worlds/safety-checks?rec_id=${recId}`).then(r => r.json())
      setSafetyCache(prev => ({ ...prev, [recId]: res?.checks || [] }))
    } catch { /* ignore */ }
  }, [])

  return (
    <div className="pw-page">
      <header className="pw-header">
        <div className="pw-header-left">
          <h2 className="pw-title">Parallel Worlds</h2>
          <p className="pw-subtitle">Counterfactual analysis &amp; policy tuning lab</p>
        </div>
        <div className="pw-header-right">
          <select
            className="pw-select"
            value={selectedPortfolio || ''}
            onChange={e => setSelectedPortfolio(Number(e.target.value))}
          >
            <option value="">Select portfolio...</option>
            {(portfolios || []).map(p => {
              const pid = p.portfolio_id || p.PORTFOLIO_ID
              const name = p.name || p.NAME
              return <option key={pid} value={pid}>{name} (#{pid})</option>
            })}
          </select>
        </div>
      </header>

      {/* Tab navigation */}
      <nav className="pw-tab-bar">
        {[
          { id: 'overview', label: 'Overview' },
          { id: 'signal-tuning', label: 'Signal Tuning' },
          { id: 'portfolio-tuning', label: 'Portfolio Tuning' },
        ].map(tab => (
          <button
            key={tab.id}
            className={`pw-tab ${activeTab === tab.id ? 'pw-tab--active' : ''}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      {loading && <LoadingState message="Loading parallel worlds..." />}
      {error && <div className="pw-error">Error: {error}</div>}

      {/* Overview Tab */}
      {!loading && !error && activeTab === 'overview' && (
        <>
          {results && !results.found && (
            <EmptyState
              title="No parallel-worlds data yet"
              explanation="Run the parallel worlds simulation to see counterfactual comparisons."
              reasons={['The daily pipeline may not have run yet.', 'Parallel Worlds may not be enabled in APP_CONFIG.']}
            />
          )}
          {results && results.found && (
            <>
              <PolicyHealthCard diagnostics={diagnostics} />
              <NarrativeCard narrative={narrative} isAi={narrative?.is_ai_narrative} />
              <ScenarioTable
                results={results}
                expandedRow={expandedRow}
                setExpandedRow={setExpandedRow}
                confidenceMap={
                  confidence?.data
                    ? Object.fromEntries(confidence.data.map(c => [c.scenario_name, c]))
                    : {}
                }
              />
              <ConfidencePanel confidenceData={confidence?.data} />
              <EquityCurvesChart curves={curves?.curves} />
              <RegretAttribution attribution={attribution} />
              <RegretHeatmap regretData={regret?.data} />
            </>
          )}
        </>
      )}

      {/* Signal Tuning Tab */}
      {!loading && !error && activeTab === 'signal-tuning' && (
        <SignalTuningTab
          pid={selectedPortfolio}
          surfaceData={surfaceData}
          regimeData={regimeData?.data}
          recommendations={recommendations?.data}
          onLoadSafety={loadSafetyChecks}
          safetyCache={safetyCache}
        />
      )}

      {/* Portfolio Tuning Tab */}
      {!loading && !error && activeTab === 'portfolio-tuning' && (
        <PortfolioTuningTab
          pid={selectedPortfolio}
          surfaceData={surfaceData}
          regimeData={regimeData?.data}
          recommendations={recommendations?.data}
          onLoadSafety={loadSafetyChecks}
          safetyCache={safetyCache}
        />
      )}
    </div>
  )
}
