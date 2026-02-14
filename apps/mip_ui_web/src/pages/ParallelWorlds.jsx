import { useState, useEffect, useCallback } from 'react'
import { API_BASE } from '../App'
import { usePortfolios } from '../context/PortfolioContext'
import LoadingState from '../components/LoadingState'
import EmptyState from '../components/EmptyState'
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend, ReferenceLine,
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

/* ── Scenario Table ──────────────────────────────────── */

function ScenarioTable({ results, expandedRow, setExpandedRow }) {
  if (!results || !results.scenarios) return null

  return (
    <section className="pw-card">
      <div className="pw-card-header">
        <h3>Scenario Comparison</h3>
        <span className="pw-subtitle">
          Actual PnL: <strong>{formatMoney(results.actual?.pnl)}</strong> | Equity: <strong>{formatMoney(results.actual?.equity)}</strong>
        </span>
      </div>
      <div className="pw-table-wrap">
        <table className="pw-table">
          <thead>
            <tr>
              <th>Scenario</th>
              <th>Type</th>
              <th className="pw-num">CF PnL</th>
              <th className="pw-num">PnL Delta</th>
              <th className="pw-num">Equity Delta</th>
              <th className="pw-num">Trades</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {results.scenarios.map((s, i) => (
              <>
                <tr key={s.scenario_id} className={s.outperformed ? 'pw-row--outperformed' : ''}>
                  <td className="pw-scenario-name">{s.scenario_name}</td>
                  <td><span className={`pw-type pw-type--${(s.scenario_type || '').toLowerCase()}`}>{s.scenario_type}</span></td>
                  <td className="pw-num">{formatMoney(s.cf_pnl)}</td>
                  <td className={`pw-num ${deltaColor(s.pnl_delta)}`}>{formatMoney(s.pnl_delta)}</td>
                  <td className={`pw-num ${deltaColor(s.equity_delta)}`}>{formatMoney(s.equity_delta)}</td>
                  <td className="pw-num">{s.cf_trades}</td>
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
                    <td colSpan={7}>
                      <div className="pw-detail">
                        <h4>Decision Trace</h4>
                        {s.decision_trace && s.decision_trace.length > 0 ? (
                          <div className="pw-trace-grid">
                            {s.decision_trace.map((gate, gi) => (
                              <div key={gi} className={`pw-gate pw-gate--${(gate.status || '').toLowerCase()}`}>
                                <span className="pw-gate-name">{gate.gate}</span>
                                <span className="pw-gate-status">{gate.status}</span>
                                {Object.entries(gate)
                                  .filter(([k]) => !['gate', 'status'].includes(k))
                                  .map(([k, v]) => (
                                    <div key={k} className="pw-gate-detail">
                                      <span className="pw-gate-key">{k.replace(/_/g, ' ')}</span>
                                      <span className="pw-gate-val">{typeof v === 'number' ? v.toFixed(4) : String(v)}</span>
                                    </div>
                                  ))
                                }
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

/* ── Regret Heatmap ──────────────────────────────────── */

function RegretHeatmap({ regretData }) {
  if (!regretData || regretData.length === 0) return null

  // Group by scenario
  const scenarios = {}
  const dates = new Set()
  regretData.forEach(r => {
    const name = r.scenario_name || r.SCENARIO_NAME
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
    if (val == null || val === 0) return 'var(--pw-heatmap-neutral)'
    const intensity = Math.min(Math.abs(val) / maxDelta, 1)
    if (val > 0) return `rgba(16, 185, 129, ${0.15 + intensity * 0.7})`
    return `rgba(239, 68, 68, ${0.15 + intensity * 0.7})`
  }

  return (
    <section className="pw-card">
      <div className="pw-card-header"><h3>Regret Heatmap</h3></div>
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

/* ── Main Page ───────────────────────────────────────── */

export default function ParallelWorlds() {
  const { portfolios } = usePortfolios()
  const [selectedPortfolio, setSelectedPortfolio] = useState(null)
  const [results, setResults] = useState(null)
  const [narrative, setNarrative] = useState(null)
  const [curves, setCurves] = useState(null)
  const [regret, setRegret] = useState(null)
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
      const [resResult, resNarrative, resCurves, resRegret] = await Promise.all([
        fetch(`${API_BASE}/parallel-worlds/results?portfolio_id=${pid}`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/narrative?portfolio_id=${pid}`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/equity-curves?portfolio_id=${pid}`).then(r => r.json()),
        fetch(`${API_BASE}/parallel-worlds/regret?portfolio_id=${pid}&days=20`).then(r => r.json()),
      ])
      setResults(resResult)
      setNarrative(resNarrative)
      setCurves(resCurves)
      setRegret(resRegret)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    if (selectedPortfolio) loadData(selectedPortfolio)
  }, [selectedPortfolio, loadData])

  return (
    <div className="pw-page">
      <header className="pw-header">
        <div className="pw-header-left">
          <h2 className="pw-title">Parallel Worlds</h2>
          <p className="pw-subtitle">Counterfactual analysis — what could have been</p>
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

      {loading && <LoadingState message="Loading parallel worlds..." />}
      {error && <div className="pw-error">Error: {error}</div>}

      {!loading && !error && results && !results.found && (
        <EmptyState
          title="No parallel-worlds data yet"
          explanation="Run the parallel worlds simulation to see counterfactual comparisons."
          reasons={['The daily pipeline may not have run yet.', 'Parallel Worlds may not be enabled in APP_CONFIG.']}
        />
      )}

      {!loading && !error && results && results.found && (
        <>
          <NarrativeCard narrative={narrative} isAi={narrative?.is_ai_narrative} />
          <ScenarioTable results={results} expandedRow={expandedRow} setExpandedRow={setExpandedRow} />
          <EquityCurvesChart curves={curves?.curves} />
          <RegretHeatmap regretData={regret?.data} />
        </>
      )}
    </div>
  )
}
