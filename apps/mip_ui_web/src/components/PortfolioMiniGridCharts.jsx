/**
 * Reusable 2x2 mini chart grid: Equity, Drawdown, Trades/day, Risk regime strip.
 * Used for Active Period (top of Portfolio page) and for each Episode Card.
 */
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
import './PortfolioMiniGridCharts.css'

export const GATE_LABELS = { SAFE: 'SAFE', CAUTION: 'CAUTION', STOPPED: 'STOPPED' }
export const GATE_COLORS = { SAFE: '#2e7d32', CAUTION: '#f9a825', STOPPED: '#c62828' }

function formatTs(ts) {
  if (!ts) return '—'
  return String(ts).slice(0, 10)
}

function ChartTitle({ title, tooltip }) {
  return (
    <h4 className="mini-grid-chart-title" title={tooltip}>
      {title}
      {tooltip && <span className="mini-grid-chart-q" aria-label={tooltip}>?</span>}
    </h4>
  )
}

function EquityChart({ data, thresholds, events }) {
  if (!Array.isArray(data) || data.length === 0) return <p className="mini-grid-empty">No equity data</p>
  const bustLine = thresholds?.bust_threshold_pct != null && thresholds?.start_equity != null
    ? (thresholds.start_equity * (thresholds.bust_threshold_pct / 100)) : null
  const hasCash = data.some(d => d.cash != null)
  return (
    <ResponsiveContainer width="100%" height={160}>
      <LineChart data={data} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
        <XAxis dataKey="ts" tick={{ fontSize: 10 }} tickFormatter={formatTs} />
        <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => (v / 1000).toFixed(0) + 'k'} width={36} />
        <Tooltip
          formatter={(v, name) => [Number(v).toLocaleString(undefined, { minimumFractionDigits: 2 }), name]}
          labelFormatter={formatTs}
        />
        {bustLine != null && <ReferenceLine y={bustLine} stroke="#c62828" strokeDasharray="2 2" />}
        <Line type="monotone" dataKey="equity" stroke="#1565c0" strokeWidth={2} dot={false} name="Total equity" />
        {hasCash && <Line type="monotone" dataKey="cash" stroke="#2e7d32" strokeWidth={1.5} dot={false} strokeDasharray="4 3" name="Cash" />}
      </LineChart>
    </ResponsiveContainer>
  )
}

function DrawdownChart({ data, drawdownStopPct }) {
  if (!Array.isArray(data) || data.length === 0) return <p className="mini-grid-empty">No drawdown data</p>
  const stopVal = drawdownStopPct != null ? -Math.abs(Number(drawdownStopPct)) : null
  const maxDD = Math.min(...data.map(d => d.drawdown_pct ?? 0))
  const domainTop = stopVal != null ? Math.min(maxDD, stopVal) - 1 : maxDD - 1
  return (
    <ResponsiveContainer width="100%" height={160}>
      <LineChart data={data} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
        <XAxis dataKey="ts" tick={{ fontSize: 10 }} tickFormatter={formatTs} />
        <YAxis
          tick={{ fontSize: 10 }}
          tickFormatter={(v) => v + '%'}
          width={40}
          domain={[domainTop, 0]}
          reversed
        />
        <Tooltip formatter={(v) => [v != null ? Number(v).toFixed(2) + '%' : '—', 'Drawdown']} labelFormatter={formatTs} />
        {stopVal != null && (
          <ReferenceLine y={stopVal} stroke="#c62828" strokeDasharray="4 3" label={{ value: 'Stop', position: 'right', fontSize: 9, fill: '#c62828' }} />
        )}
        {maxDD < -0.01 && (
          <ReferenceLine y={maxDD} stroke="#6a1b9a" strokeDasharray="3 3" label={{ value: `Max ${maxDD.toFixed(1)}%`, position: 'right', fontSize: 9, fill: '#6a1b9a' }} />
        )}
        <Line type="monotone" dataKey="drawdown_pct" stroke="#6a1b9a" strokeWidth={2} dot={false} name="Drawdown %" />
      </LineChart>
    </ResponsiveContainer>
  )
}

function TradesPerDayChart({ data }) {
  const dataKey = data?.[0] != null && ('day' in data[0]) ? 'day' : 'ts'
  if (!Array.isArray(data) || data.length === 0) return <p className="mini-grid-empty">No trades in period</p>
  const hasBreakdown = data.some(d => d.buy_count != null || d.sell_count != null)
  if (hasBreakdown) {
    return (
      <ResponsiveContainer width="100%" height={160}>
        <BarChart data={data} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
          <XAxis dataKey={dataKey} tick={{ fontSize: 10 }} tickFormatter={formatTs} />
          <YAxis tick={{ fontSize: 10 }} width={28} allowDecimals={false} />
          <Tooltip labelFormatter={formatTs} />
          <Bar dataKey="buy_count" stackId="trades" fill="#1565c0" name="Buy" radius={[0, 0, 0, 0]} />
          <Bar dataKey="sell_count" stackId="trades" fill="#c62828" name="Sell" radius={[2, 2, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    )
  }
  const countKey = data?.[0] != null && ('trades_count' in data[0]) ? 'trades_count' : 'tradesCount'
  return (
    <ResponsiveContainer width="100%" height={160}>
      <BarChart data={data} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
        <XAxis dataKey={dataKey} tick={{ fontSize: 10 }} tickFormatter={formatTs} />
        <YAxis tick={{ fontSize: 10 }} width={28} />
        <Tooltip formatter={(v) => [v, 'Trades']} labelFormatter={formatTs} />
        <Bar dataKey={countKey} fill="#2e7d32" radius={[2, 2, 0, 0]} name="Trades" />
      </BarChart>
    </ResponsiveContainer>
  )
}

function RiskRegimeStrip({ data }) {
  const tsKey = data?.[0] != null && ('day' in data[0]) ? 'day' : 'ts'
  const stateKey = data?.[0] != null && ('gate_state' in data[0]) ? 'gate_state' : 'gateState'
  if (!Array.isArray(data) || data.length === 0) return <p className="mini-grid-empty">No regime data</p>
  const segments = []
  let prev = null
  for (let i = 0; i < data.length; i++) {
    const g = data[i][stateKey] || data[i].gate_state || 'SAFE'
    if (g !== prev) {
      segments.push({ start: i, end: i, state: g })
      prev = g
    } else {
      segments[segments.length - 1].end = i
    }
  }
  return (
    <div className="mini-grid-regime-wrap" title="Risk state by day (SAFE / CAUTION / STOPPED)">
      <div className="mini-grid-regime-strip">
        {segments.map((seg, i) => (
          <div
            key={i}
            className="mini-grid-regime-segment"
            style={{
              width: `${((seg.end - seg.start + 1) / data.length) * 100}%`,
              backgroundColor: GATE_COLORS[seg.state] || '#9e9e9e',
            }}
            title={`${formatTs(data[seg.start][tsKey] ?? data[seg.start].ts)} – ${GATE_LABELS[seg.state] || seg.state}`}
          />
        ))}
      </div>
      <div className="mini-grid-regime-legend">
        <span style={{ color: GATE_COLORS.SAFE }}>SAFE</span>
        <span style={{ color: GATE_COLORS.CAUTION }}>CAUTION</span>
        <span style={{ color: GATE_COLORS.STOPPED }}>STOPPED</span>
      </div>
    </div>
  )
}

/**
 * Props:
 * - titlePrefix: string (e.g. "Active Period" or "Episode #12")
 * - dateRange: { start_ts, end_ts } (optional)
 * - series: { equity, drawdown, tradesPerDay, regime } — arrays of { ts, equity }, { ts, drawdown_pct }, etc.
 * - thresholds: { drawdown_stop_pct, bust_threshold_pct?, start_equity? }
 * - events: [{ ts, type, label? }] (optional, for future markers)
 * - headerLine: string (e.g. "Active since 2025-01-15 · Profile 2 · SAFE")
 * - explainSentence: string (e.g. "Safety brakes are off; you can open new positions.")
 */
export default function PortfolioMiniGridCharts({
  titlePrefix,
  dateRange = {},
  series = {},
  thresholds = {},
  events = [],
  headerLine,
  explainSentence,
}) {
  const equity = series.equity ?? []
  const drawdown = series.drawdown ?? []
  const tradesPerDay = series.tradesPerDay ?? []
  const regime = series.regime ?? []
  const startTs = dateRange.start_ts ?? dateRange.startTs
  const endTs = dateRange.end_ts ?? dateRange.endTs

  return (
    <div className="portfolio-mini-grid">
      {(headerLine || titlePrefix || explainSentence) && (
        <div className="mini-grid-header">
          {headerLine && <p className="mini-grid-header-line">{headerLine}</p>}
          {!headerLine && titlePrefix && (
            <h3 className="mini-grid-title">
              {titlePrefix}
              {(startTs || endTs) && (
                <span className="mini-grid-dates">
                  {' · '}{formatTs(startTs)} → {endTs ? formatTs(endTs) : 'now'}
                </span>
              )}
            </h3>
          )}
          {explainSentence && <p className="mini-grid-explain">{explainSentence}</p>}
        </div>
      )}
      <div className="mini-grid-grid">
        <div className="mini-grid-cell">
          <ChartTitle title="Equity & Cash" tooltip="Total equity (solid blue) and available cash (dashed green) over time. Dashed red line = bust threshold if set." />
          <EquityChart data={equity} thresholds={thresholds} events={events} />
        </div>
        <div className="mini-grid-cell">
          <ChartTitle title="Drawdown" tooltip="Drawdown %. Horizontal line = drawdown stop threshold; breach marks entries blocked." />
          <DrawdownChart data={drawdown} drawdownStopPct={thresholds.drawdown_stop_pct} />
        </div>
        <div className="mini-grid-cell">
          <ChartTitle title="Trades per day" tooltip="Trades executed each day — blue = buys, red = sells." />
          <TradesPerDayChart data={tradesPerDay} />
        </div>
        <div className="mini-grid-cell">
          <ChartTitle title="Risk regime" tooltip="Risk state by day: SAFE (green), CAUTION (amber), STOPPED (red)." />
          <RiskRegimeStrip data={regime} />
        </div>
      </div>
    </div>
  )
}
