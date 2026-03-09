import { useEffect, useMemo, useState } from 'react'
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  BarChart,
  Bar,
  Cell,
  Legend,
} from 'recharts'
import { API_BASE } from '../App'
import LoadingState from '../components/LoadingState'
import ErrorState from '../components/ErrorState'
import './PerformanceDashboard.css'

function fmtPct(v, d = 1) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return `${(Number(v) * 100).toFixed(d)}%`
}

function fmtMoney(v) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(Number(v))
}

function fmtNum(v, d = 2) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return Number(v).toFixed(d)
}

function KpiCard({ label, value, hint }) {
  return (
    <article className="perf-kpi-card">
      <span className="perf-kpi-label">{label}</span>
      <strong className="perf-kpi-value">{value}</strong>
      {hint ? <span className="perf-kpi-hint">{hint}</span> : null}
    </article>
  )
}

export default function PerformanceDashboard() {
  const [lookbackDays, setLookbackDays] = useState(90)
  const [tab, setTab] = useState('executive')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`${API_BASE}/performance-dashboard/overview?lookback_days=${lookbackDays}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((payload) => {
        if (!cancelled) setData(payload)
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [lookbackDays])

  const kpis = data?.executive?.kpis || {}
  const trends = data?.executive?.trends || {}
  const diag = data?.diagnostics || {}
  const intelligence = data?.executive?.intelligence_impact || {}

  const selectivityData = useMemo(() => {
    return (trends.selectivity_trend || []).map((r) => ({
      period: String(r.PERIOD || '').slice(0, 10),
      candidates: Number(r.CANDIDATES_CREATED || 0),
      blocked: Math.max(0, Number(r.CANDIDATES_CREATED || 0) - Number(r.COMMITTEE_PASSED || 0)),
      approved: Number(r.PM_ACCEPTED || 0),
      executed: Number(r.EXECUTED || 0),
    }))
  }, [trends.selectivity_trend])

  const funnelData = useMemo(() => {
    const f = diag.decision_funnel || {}
    return [
      { stage: 'Signals', value: Number(f.signals || 0) },
      { stage: 'Proposals', value: Number(f.proposals || 0) },
      { stage: 'PM Accepted', value: Number(f.pm_accepted || 0) },
      { stage: 'Compliance', value: Number(f.compliance_approved || 0) },
      { stage: 'Executed', value: Number(f.executed || 0) },
      { stage: 'Successful', value: Number(f.successful_outcomes || 0) },
    ]
  }, [diag.decision_funnel])

  if (loading) {
    return (
      <>
        <h1>MIP Performance Dashboard</h1>
        <LoadingState message="Loading performance truth layer..." />
      </>
    )
  }

  if (error) {
    return (
      <>
        <h1>MIP Performance Dashboard</h1>
        <ErrorState message={error} />
      </>
    )
  }

  return (
    <div className="perf-page">
      <div className="perf-header">
        <h1>MIP Performance Dashboard</h1>
        <div className="perf-controls">
          <label>
            Lookback
            <select value={lookbackDays} onChange={(e) => setLookbackDays(Number(e.target.value))}>
              <option value={90}>90 days</option>
              <option value={180}>180 days</option>
              <option value={365}>365 days</option>
            </select>
          </label>
        </div>
      </div>

      <div className="perf-tabs">
        <button type="button" className={tab === 'executive' ? 'active' : ''} onClick={() => setTab('executive')}>
          Executive Overview
        </button>
        <button type="button" className={tab === 'diagnostics' ? 'active' : ''} onClick={() => setTab('diagnostics')}>
          Diagnostics & Attribution
        </button>
      </div>

      {tab === 'executive' && (
        <>
          <section className="perf-kpi-grid">
            <KpiCard label="MIP Performance Score" value={fmtNum(kpis.mip_performance_score, 1)} />
            <KpiCard label="Period P&L" value={fmtMoney(kpis.period_realized_pnl)} />
            <KpiCard label="Win Rate" value={fmtPct(kpis.win_rate)} />
            <KpiCard label="Avg Return / Trade" value={fmtNum(kpis.avg_return_per_trade)} />
            <KpiCard label="Max Drawdown" value={fmtPct(kpis.max_drawdown)} />
            <KpiCard label="Decision Quality Score" value={fmtNum(kpis.decision_quality_score, 1)} />
            <KpiCard label="Monthly Cost" value={fmtMoney(kpis.monthly_cost_usd)} hint={`Snowflake ${fmtMoney(kpis.monthly_snowflake_cost_usd)} + Fixed ${fmtMoney(kpis.monthly_fixed_tools_cost_usd)}`} />
            <KpiCard label="Cost Efficiency" value={fmtNum(kpis.cost_efficiency_ratio, 3)} hint="P&L per $1 cost" />
          </section>

          <section className="perf-charts-grid">
            <article className="perf-panel">
              <h3>Equity Curve</h3>
              <ResponsiveContainer width="100%" height={260}>
                <LineChart data={(trends.equity_curve || []).map((r) => ({ day: String(r.DAY || '').slice(0, 10), equity: Number(r.TOTAL_EQUITY || 0) }))}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="day" hide />
                  <YAxis />
                  <Tooltip />
                  <Line type="monotone" dataKey="equity" stroke="#2e7d32" dot={false} strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </article>

            <article className="perf-panel">
              <h3>Monthly Cost Trend</h3>
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={trends.monthly_cost_trend || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="period_month" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="snowflake_cost_usd" stackId="cost" fill="#1565c0" name="Snowflake" />
                  <Bar dataKey="fixed_tools_usd" stackId="cost" fill="#7b1fa2" name="Fixed tools ($80)" />
                </BarChart>
              </ResponsiveContainer>
            </article>

            <article className="perf-panel">
              <h3>Decision Quality Trend</h3>
              <ResponsiveContainer width="100%" height={260}>
                <LineChart data={(trends.decision_quality_trend || []).map((r) => ({ period: String(r.PERIOD || '').slice(0, 10), expectancy: Number(r.EXPECTANCY || 0), positive: Number(r.PCT_POSITIVE || 0) * 100 }))}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="period" />
                  <YAxis yAxisId="left" />
                  <YAxis yAxisId="right" orientation="right" />
                  <Tooltip />
                  <Line yAxisId="left" type="monotone" dataKey="expectancy" stroke="#ef6c00" dot={false} name="Expectancy" />
                  <Line yAxisId="right" type="monotone" dataKey="positive" stroke="#00897b" dot={false} name="% Positive" />
                </LineChart>
              </ResponsiveContainer>
            </article>

            <article className="perf-panel">
              <h3>Selectivity Trend</h3>
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={selectivityData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="period" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="candidates" fill="#546e7a" />
                  <Bar dataKey="blocked" fill="#c62828" />
                  <Bar dataKey="approved" fill="#1565c0" />
                  <Bar dataKey="executed" fill="#2e7d32" />
                </BarChart>
              </ResponsiveContainer>
            </article>
          </section>

          <section className="perf-impact-grid">
            {['committee', 'parallel_worlds', 'training', 'news'].map((k) => (
              <article key={k} className="perf-panel">
                <h3>{k.replace('_', ' ').replace(/\b\w/g, (m) => m.toUpperCase())} Impact</h3>
                <p>Influence rate: {fmtPct(intelligence[k]?.influence_rate)}</p>
                {'avg_pnl_delta' in (intelligence[k] || {}) ? <p>Directional value: {fmtNum(intelligence[k]?.avg_pnl_delta)}</p> : null}
                {'blocked_new_entries' in (intelligence[k] || {}) ? <p>Blocked entries: {intelligence[k]?.blocked_new_entries ?? 0}</p> : null}
                {'blocked_count' in (intelligence[k] || {}) ? <p>Blocked decisions: {intelligence[k]?.blocked_count ?? 0}</p> : null}
                <p>Status: <strong>{intelligence[k]?.status || 'under_review'}</strong></p>
              </article>
            ))}
          </section>

          <section className="perf-verdict">
            <h3>Executive Verdict</h3>
            <p>{data?.executive?.verdict || 'No verdict available yet.'}</p>
          </section>
        </>
      )}

      {tab === 'diagnostics' && (
        <section className="perf-diagnostics-grid">
          <article className="perf-panel">
            <h3>Decision Funnel</h3>
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={funnelData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" />
                <YAxis type="category" dataKey="stage" width={95} />
                <Tooltip />
                <Bar dataKey="value">
                  {funnelData.map((_, idx) => <Cell key={idx} fill={idx < 2 ? '#607d8b' : idx < 4 ? '#1565c0' : '#2e7d32'} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </article>

          <article className="perf-panel">
            <h3>Committee Effectiveness</h3>
            <p>Total decisions: {diag?.committee_effectiveness?.overall?.TOTAL_DECISIONS ?? 0}</p>
            <p>Influenced: {diag?.committee_effectiveness?.overall?.COMMITTEE_INFLUENCED ?? 0}</p>
            <p>Blocked: {diag?.committee_effectiveness?.overall?.BLOCKED_BY_COMMITTEE ?? 0}</p>
            <p>Resized: {diag?.committee_effectiveness?.overall?.RESIZED_BY_COMMITTEE ?? 0}</p>
          </article>

          <article className="perf-panel">
            <h3>Role Contribution</h3>
            <div className="perf-simple-table">
              {(diag?.committee_effectiveness?.role_contribution || []).slice(0, 8).map((r) => (
                <div key={r.ROLE_NAME} className="perf-row">
                  <span>{r.ROLE_NAME}</span>
                  <strong>{r.INFLUENCE_COUNT}</strong>
                </div>
              ))}
            </div>
          </article>

          <article className="perf-panel">
            <h3>Parallel Worlds</h3>
            <p>Rows: {diag?.parallel_worlds_effectiveness?.PW_SCENARIO_ROWS ?? 0}</p>
            <p>Outperform rate: {fmtPct(diag?.parallel_worlds_effectiveness?.PW_OUTPERFORM_RATE)}</p>
            <p>Avg P&L delta: {fmtNum(diag?.parallel_worlds_effectiveness?.PW_AVG_PNL_DELTA)}</p>
          </article>

          <article className="perf-panel">
            <h3>Training Influence</h3>
            <p>Training events: {diag?.training_influence_effectiveness?.TRAINING_EVENTS ?? 0}</p>
            <p>Avg trusted count: {fmtNum(diag?.training_influence_effectiveness?.AVG_TRUSTED_COUNT)}</p>
          </article>

          <article className="perf-panel">
            <h3>News Influence</h3>
            <p>News-scoped decisions: {diag?.news_influence_effectiveness?.NEWS_DECISIONS ?? 0}</p>
            <p>Blocked by news: {diag?.news_influence_effectiveness?.NEWS_BLOCK_COUNT ?? 0}</p>
            <p>Avg news adjustment: {fmtNum(diag?.news_influence_effectiveness?.AVG_NEWS_ADJ_MAGNITUDE, 3)}</p>
          </article>

          <article className="perf-panel perf-panel-wide">
            <h3>Target Realism Analysis</h3>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={(diag?.target_realism_analysis || []).slice(0, 12)}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="SYMBOL" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="DEFAULT_TARGET" fill="#8e24aa" name="Default target" />
                <Bar dataKey="REALISTIC_TARGET" fill="#1565c0" name="Realistic target" />
                <Bar dataKey="REALIZED_RETURN_PROXY" fill="#2e7d32" name="Realized return proxy" />
              </BarChart>
            </ResponsiveContainer>
          </article>

          <article className="perf-panel">
            <h3>Cost Attribution</h3>
            <p>Current month total: {fmtMoney(diag?.cost_attribution?.current_month_total_usd)}</p>
            <p>Cost / executed trade: {fmtMoney(diag?.cost_attribution?.cost_per_executed_trade_usd)}</p>
            <p>Cost / profitable trade: {fmtMoney(diag?.cost_attribution?.cost_per_profitable_trade_usd)}</p>
            <p>Cost / committee review: {fmtMoney(diag?.cost_attribution?.cost_per_committee_review_usd)}</p>
          </article>

          <article className="perf-panel">
            <h3>Learning-to-Decision Ledger</h3>
            <p>Ledger events: {diag?.learning_to_decision_ledger?.LEDGER_EVENTS ?? 0}</p>
            <p>Runs touched: {diag?.learning_to_decision_ledger?.RUNS_TOUCHED ?? 0}</p>
            <p>Training events: {diag?.learning_to_decision_ledger?.TRAINING_EVENTS ?? 0}</p>
            <p>News influenced events: {diag?.learning_to_decision_ledger?.NEWS_INFLUENCED_EVENTS ?? 0}</p>
          </article>
        </section>
      )}
    </div>
  )
}
