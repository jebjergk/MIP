import { useState, useEffect } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  ReferenceLine,
} from 'recharts'
import { API_BASE } from '../App'
import LoadingState from './LoadingState'
import './EpisodeCard.css'

const GATE_LABELS = { SAFE: 'SAFE', CAUTION: 'CAUTION', STOPPED: 'STOPPED' }
const GATE_COLORS = { SAFE: '#2e7d32', CAUTION: '#f9a825', STOPPED: '#c62828' }

function formatTs(ts) {
  if (!ts) return '—'
  const s = String(ts)
  return s.slice(0, 10)
}

function EquityChart({ data, startEquity, bustPct, events, thresholds }) {
  if (!Array.isArray(data) || data.length === 0) return <p className="episode-chart-empty">No equity data</p>
  const bustLine = thresholds?.bust_threshold_pct != null && thresholds?.start_equity != null
    ? (thresholds.start_equity * (thresholds.bust_threshold_pct / 100)) : null
  return (
    <ResponsiveContainer width="100%" height={160}>
      <LineChart data={data} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
        <XAxis dataKey="ts" tick={{ fontSize: 10 }} tickFormatter={(v) => formatTs(v)} />
        <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => (v / 1000).toFixed(0) + 'k'} width={36} />
        <Tooltip formatter={(v) => [Number(v).toLocaleString(undefined, { minimumFractionDigits: 2 }), 'Equity']} labelFormatter={formatTs} />
        {bustLine != null && <ReferenceLine y={bustLine} stroke="#c62828" strokeDasharray="2 2" />}
        <Line type="monotone" dataKey="equity" stroke="#1565c0" strokeWidth={2} dot={false} name="Equity" />
      </LineChart>
    </ResponsiveContainer>
  )
}

function DrawdownChart({ data, drawdownStopPct, firstBreachTs }) {
  if (!Array.isArray(data) || data.length === 0) return <p className="episode-chart-empty">No drawdown data</p>
  const refVal = drawdownStopPct != null ? -Math.abs(Number(drawdownStopPct)) : null
  return (
    <ResponsiveContainer width="100%" height={160}>
      <LineChart data={data} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
        <XAxis dataKey="ts" tick={{ fontSize: 10 }} tickFormatter={(v) => formatTs(v)} />
        <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => v + '%'} width={40} domain={['auto', 0]} />
        <Tooltip formatter={(v) => [v != null ? Number(v).toFixed(2) + '%' : '—', 'Drawdown']} labelFormatter={formatTs} />
        {refVal != null && <ReferenceLine y={refVal} stroke="#c62828" strokeDasharray="2 2" />}
        <Line type="monotone" dataKey="drawdown_pct" stroke="#6a1b9a" strokeWidth={2} dot={false} name="Drawdown %" />
      </LineChart>
    </ResponsiveContainer>
  )
}

function TradesPerDayChart({ data }) {
  if (!Array.isArray(data) || data.length === 0) return <p className="episode-chart-empty">No trades in episode</p>
  return (
    <ResponsiveContainer width="100%" height={160}>
      <BarChart data={data} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
        <XAxis dataKey="ts" tick={{ fontSize: 10 }} tickFormatter={(v) => formatTs(v)} />
        <YAxis tick={{ fontSize: 10 }} width={28} />
        <Tooltip formatter={(v) => [v, 'Trades']} labelFormatter={formatTs} />
        <Bar dataKey="trades_count" fill="#2e7d32" radius={[2, 2, 0, 0]} name="Trades" />
      </BarChart>
    </ResponsiveContainer>
  )
}

function RiskRegimeStrip({ data }) {
  if (!Array.isArray(data) || data.length === 0) return <p className="episode-chart-empty">No regime data</p>
  const segments = []
  let prev = null
  for (let i = 0; i < data.length; i++) {
    const g = data[i].gate_state || 'SAFE'
    if (g !== prev) {
      segments.push({ start: i, end: i, state: g })
      prev = g
    } else {
      segments[segments.length - 1].end = i
    }
  }
  return (
    <div className="episode-regime-strip" title="Risk state by day (SAFE / STOPPED)">
      <div className="episode-regime-strip-inner">
        {segments.map((seg, i) => (
          <div
            key={i}
            className="episode-regime-segment"
            style={{
              width: `${((seg.end - seg.start + 1) / data.length) * 100}%`,
              backgroundColor: GATE_COLORS[seg.state] || '#9e9e9e',
            }}
            title={`${formatTs(data[seg.start].ts)} – ${GATE_LABELS[seg.state] || seg.state}`}
          />
        ))}
      </div>
      <div className="episode-regime-legend">
        <span style={{ color: GATE_COLORS.SAFE }}>SAFE</span>
        <span style={{ color: GATE_COLORS.STOPPED }}>STOPPED</span>
      </div>
    </div>
  )
}

export default function EpisodeCard({ episode, portfolioId, isActive }) {
  const [expanded, setExpanded] = useState(false)
  const [detail, setDetail] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const episodeId = episode?.episode_id ?? episode?.EPISODE_ID
  const startTs = episode?.start_ts ?? episode?.START_TS ?? ''
  const endTs = episode?.end_ts ?? episode?.END_TS ?? null
  const status = episode?.status ?? episode?.STATUS ?? '—'
  const endReason = episode?.end_reason ?? episode?.END_REASON ?? null
  const profileName = episode?.profile_name ?? episode?.PROFILE_NAME ?? '—'
  const totalReturn = episode?.total_return ?? episode?.total_return
  const maxDrawdown = episode?.max_drawdown ?? episode?.MAX_DRAWDOWN
  const tradesCount = episode?.trades_count ?? episode?.trades_count ?? 0
  const winDays = episode?.win_days ?? episode?.win_days
  const lossDays = episode?.loss_days ?? episode?.loss_days
  const gateChip = isActive ? (episode?.risk_status ?? 'SAFE') : (endReason === 'DRAWDOWN_STOP' || endReason === 'MANUAL_RESET' ? 'STOPPED' : 'SAFE')

  useEffect(() => {
    if (!expanded || !portfolioId || !episodeId || detail != null) return
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`${API_BASE}/portfolios/${portfolioId}/episodes/${episodeId}`)
      .then((res) => {
        if (!res.ok) throw new Error(res.statusText)
        return res.json()
      })
      .then((d) => {
        if (!cancelled) {
          setDetail(d)
          setLoading(false)
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setError(e.message)
          setLoading(false)
        }
      })
    return () => { cancelled = true }
  }, [expanded, portfolioId, episodeId, detail])

  const explainLine = endReason
    ? `Ended: ${endReason === 'MANUAL_RESET' ? 'Manual reset' : endReason === 'DRAWDOWN_STOP' ? 'Drawdown stop' : endReason}. Profile: ${profileName}.`
    : `Active. Profile: ${profileName}.`

  return (
    <div className={`episode-card ${expanded ? 'episode-card--expanded' : ''}`}>
      <button
        type="button"
        className="episode-card-header"
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
      >
        <span className="episode-card-id">Episode {episodeId}</span>
        <span className="episode-card-dates" title="Date range">
          {formatTs(startTs)} → {endTs ? formatTs(endTs) : 'now'}
        </span>
        <span className={`episode-card-chip episode-card-chip--${(status || '').toLowerCase()}`}>
          {status || '—'}
        </span>
        <span className={`episode-card-chip episode-card-chip--gate episode-card-chip--${String(gateChip || 'safe').toLowerCase()}`} title="Gate state">
          {GATE_LABELS[gateChip] || gateChip}
        </span>
        <span className="episode-card-profile">{profileName}</span>
      </button>
      <p className="episode-card-explain">{explainLine}</p>
      <dl className="episode-card-stats">
        <dt>Start equity</dt>
        <dd>{episode?.start_equity != null ? Number(episode.start_equity).toLocaleString(undefined, { minimumFractionDigits: 2 }) : (detail?.start_equity != null ? Number(detail.start_equity).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—')}</dd>
        <dt>End equity</dt>
        <dd>{detail?.end_equity != null ? Number(detail.end_equity).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</dd>
        <dt>Total return</dt>
        <dd>{totalReturn != null ? (Number(totalReturn) * 100).toFixed(2) + '%' : '—'}</dd>
        <dt>Max drawdown</dt>
        <dd>{maxDrawdown != null ? Number(maxDrawdown).toFixed(2) + '%' : '—'}</dd>
        <dt>Trades</dt>
        <dd>{tradesCount}</dd>
        <dt>Win / Loss days</dt>
        <dd>{winDays != null && lossDays != null ? `${winDays} / ${lossDays}` : '—'}</dd>
        <dt>Peak open symbols</dt>
        <dd>{detail?.peak_open_symbols ?? episode?.peak_open_symbols ?? '—'}</dd>
      </dl>
      {expanded && (
        <div className="episode-card-charts">
          {loading && <LoadingState />}
          {error && <p className="episode-chart-error">{error}</p>}
          {detail && !loading && !error && (
            <div className="episode-charts-grid">
              <div className="episode-chart-cell" title="Equity over time; horizontal line = bust threshold if set">
                <h4 className="episode-chart-title">Equity</h4>
                <EquityChart
                  data={detail.equity_series || []}
                  startEquity={detail.thresholds?.start_equity}
                  bustPct={detail.thresholds?.bust_threshold_pct}
                  events={detail.events}
                  thresholds={detail.thresholds}
                />
              </div>
              <div className="episode-chart-cell" title="Drawdown %; line = drawdown stop threshold">
                <h4 className="episode-chart-title">Drawdown</h4>
                <DrawdownChart
                  data={detail.drawdown_series || []}
                  drawdownStopPct={detail.thresholds?.drawdown_stop_pct}
                  firstBreachTs={detail.events?.find((e) => e.type === 'drawdown_stop_triggered')?.ts}
                />
              </div>
              <div className="episode-chart-cell" title="Trades per day">
                <h4 className="episode-chart-title">Trades per day</h4>
                <TradesPerDayChart data={detail.trades_per_day || []} />
              </div>
              <div className="episode-chart-cell" title="Risk regime by day (SAFE / STOPPED)">
                <h4 className="episode-chart-title">Risk regime</h4>
                <RiskRegimeStrip data={detail.regime_per_day || []} />
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
