import { useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { API_BASE } from '../../App'
import LoadingState from '../../components/LoadingState'
import ErrorState from '../../components/ErrorState'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { EvidenceBadge, fmtNum, IntradayHeader, HelpTip } from './IntradayTrainingCommon'
import './IntradayTraining.css'

export default function IntradayPatternDetailPage() {
  const { patternId } = useParams()
  const navigate = useNavigate()
  const [patternList, setPatternList] = useState([])
  const [detail, setDetail] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  const selectedPatternId = Number(patternId || 501)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    Promise.all([
      fetch(`${API_BASE}/intraday/patterns?limit=200`).then((r) => (r.ok ? r.json() : Promise.reject(new Error('Failed pattern list')))),
      fetch(`${API_BASE}/intraday/pattern/${selectedPatternId}`).then((r) => (r.ok ? r.json() : Promise.reject(new Error('Failed pattern detail')))),
    ])
      .then(([list, patternDetail]) => {
        if (cancelled) return
        setPatternList(list?.rows ?? [])
        setDetail(patternDetail)
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [selectedPatternId])

  const fallbackSeries = useMemo(() => {
    const rows = detail?.fallback_mix_series ?? []
    return rows.map((row) => {
      const total = Number(row.TOTAL_ROWS || 0)
      const safe = total > 0 ? total : 1
      return {
        date: row.CALCULATED_DATE,
        exactPct: (Number(row.EXACT_ROWS || 0) / safe) * 100,
        regimePct: (Number(row.REGIME_ROWS || 0) / safe) * 100,
        globalPct: (Number(row.GLOBAL_ROWS || 0) / safe) * 100,
      }
    })
  }, [detail])

  if (loading) return <LoadingState />
  if (error) return <ErrorState message={error} />

  const summary = detail?.summary ?? {}
  const globalFallback = Number(summary.GLOBAL_ROWS || 0) > Number(summary.EXACT_ROWS || 0)

  return (
    <div className="it-page">
      <IntradayHeader
        title="Pattern Detail"
        subtitle="Deep dive into one intraday pattern: evidence depth, fallback behavior, and signal concentration."
      />

      <div className="it-card">
        <label htmlFor="pattern-picker">
          Pattern selector <HelpTip text="Switch between patterns to inspect evidence quality and fallback behavior." />
        </label>
        <select
          id="pattern-picker"
          value={selectedPatternId}
          onChange={(e) => navigate(`/intraday/pattern/${e.target.value}`)}
        >
          {patternList.map((row) => (
            <option key={row.PATTERN_ID} value={row.PATTERN_ID}>
              {row.PATTERN_ID} - {row.PATTERN_NAME || row.PATTERN_TYPE || 'Pattern'}
            </option>
          ))}
        </select>
      </div>

      <div className="it-grid">
        <div className="it-card">
          <div className="it-kpi-value">{summary.PATTERN_NAME || summary.PATTERN_ID}</div>
          <div className="it-kpi-label">Pattern <HelpTip text="Human-readable pattern name from the registry." /></div>
        </div>
        <div className="it-card">
          <div className="it-kpi-value">{fmtNum(summary.AVG_EVIDENCE_N, 1)}</div>
          <div className="it-kpi-label">Avg evidence N <HelpTip text="Average N_SIGNALS across trust rows in selected snapshot." /></div>
        </div>
        <div className="it-card">
          <div className="it-kpi-value">{fmtNum(summary.AVG_HIT_RATE, 3)}</div>
          <div className="it-kpi-label">Avg hit rate <HelpTip text="Average state-conditioned hit rate for this pattern snapshot." /></div>
        </div>
        <div className="it-card">
          <EvidenceBadge fallbackLevel={globalFallback ? 'GLOBAL' : 'EXACT'} evidenceN={summary.AVG_EVIDENCE_N} />
          <div className="it-kpi-label">Evidence status <HelpTip text="Flagged when GLOBAL fallback dominates or evidence is thin." /></div>
        </div>
      </div>

      <div className="it-card">
        <h3>Fallback Mix (Pattern) <HelpTip text="Shows whether this pattern is moving from global fallback to exact-state trust." /></h3>
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={fallbackSeries}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" />
            <YAxis />
            <Tooltip />
            <Line dataKey="exactPct" stroke="#2e7d32" dot={false} />
            <Line dataKey="regimePct" stroke="#ef6c00" dot={false} />
            <Line dataKey="globalPct" stroke="#c62828" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="it-card">
        <h3>Signals Per Day <HelpTip text="Signal flow for this pattern over time." /></h3>
        <ResponsiveContainer width="100%" height={240}>
          <BarChart data={detail?.signals_per_day ?? []}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="SIGNAL_DATE" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="SIGNALS_TOTAL" fill="#1d4f8c" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="it-card">
        <h3>Per-Symbol Distribution <HelpTip text="Checks concentration risk; severe dominance by one symbol is a warning." /></h3>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={detail?.symbol_distribution ?? []} layout="vertical" margin={{ left: 30 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" />
            <YAxis type="category" dataKey="SYMBOL" width={70} />
            <Tooltip />
            <Bar dataKey="SIGNALS_TOTAL" fill="#4e84be" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
