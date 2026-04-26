/**
 * Inline trade-intelligence panel for a position row on the cockpit.
 *
 * Layout:
 *   ┌────────────────────────────────────────┬──────────────────────┐
 *   │ Combined chart                         │ Trade facts          │
 *   │  - daily-since-entry → today's 15m     │  qty, avg cost,      │
 *   │  - vertical divider at session open    │  current, TP, SL,    │
 *   │  - reference lines: entry / TP / SL    │  P&L, held, distance │
 *   ├────────────────────────────────────────┼──────────────────────┤
 *   │ Thesis (one line) + expectation        │ Recommendation       │
 *   │ Plan / Now / On plan framing           │ + subtle shadow line │
 *   └────────────────────────────────────────┴──────────────────────┘
 *
 * Recharts is already a frontend dep. Series is the backend's
 * `trade_chart_series` (pre-merged daily + intraday); `session_open_ts`
 * marks the boundary so we draw a single divider rather than render
 * two disjoint charts.
 */
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  ReferenceArea,
  CartesianGrid,
} from 'recharts'

const EM = '\u2014'

function fmtPct(v, dp = 2) {
  if (v == null) return EM
  const n = Number(v)
  if (!Number.isFinite(n)) return EM
  return `${n >= 0 ? '+' : ''}${(n * 100).toFixed(dp)}%`
}

function fmtMoney(v, withSign = false) {
  if (v == null) return EM
  const n = Number(v)
  if (!Number.isFinite(n)) return EM
  const abs = Math.abs(n)
  const formatted = `$${abs.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
  if (withSign) {
    if (n < 0) return `-${formatted}`
    if (n > 0) return `+${formatted}`
  }
  return n < 0 ? `-${formatted}` : formatted
}

function fmtPrice(v) {
  if (v == null) return EM
  const n = Number(v)
  if (!Number.isFinite(n)) return EM
  return n.toFixed(2)
}

function fmtDistance(v) {
  if (v == null) return EM
  const n = Number(v)
  if (!Number.isFinite(n)) return EM
  return `${n >= 0 ? '+' : ''}${n.toFixed(1)}%`
}

function CombinedTradeChart({ row }) {
  const series = Array.isArray(row.trade_chart_series) ? row.trade_chart_series : []
  if (series.length === 0) {
    return (
      <div className="ck-co-chart-empty">
        No price history available for this position yet.
      </div>
    )
  }

  const sessionOpen = row.session_open_ts || null
  const data = series
    .filter(p => p && p.close != null)
    .map(p => ({ ts: p.ts, label: p.label, close: Number(p.close), kind: p.kind }))

  const intradayPoints = data.filter(d => d.kind === 'INTRADAY')
  const intradayStart = intradayPoints.length > 0 ? intradayPoints[0].ts : null
  const intradayEnd = intradayPoints.length > 0 ? intradayPoints[intradayPoints.length - 1].ts : null

  const refs = []
  if (row.avg_cost != null) {
    refs.push({ y: Number(row.avg_cost), color: '#0d6efd', dash: '4 3', label: 'Entry' })
  }
  if (row.tp_price != null) {
    refs.push({ y: Number(row.tp_price), color: '#198754', dash: '4 3', label: 'TP' })
  }
  if (row.sl_price != null) {
    const sl_label = row.sl_is_dynamic ? 'SL ~' : 'SL'
    refs.push({ y: Number(row.sl_price), color: '#dc3545', dash: '4 3', label: sl_label })
  }
  if (row.invalidation_level != null && row.sl_price == null) {
    refs.push({ y: Number(row.invalidation_level), color: '#dc3545', dash: '2 4', label: 'Inv' })
  }
  if (row.last_price != null) {
    refs.push({ y: Number(row.last_price), color: '#212529', dash: '1 2', label: 'Now' })
  }

  return (
    <ResponsiveContainer width="100%" height={240}>
      <LineChart data={data} margin={{ top: 8, right: 56, left: 8, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#eef0f3" />
        <XAxis
          dataKey="ts"
          tick={({ x, y, payload }) => {
            const point = data.find(d => d.ts === payload.value)
            const label = point ? point.label : ''
            return (
              <text x={x} y={y + 10} textAnchor="middle" fontSize={10} fill="#6c757d">
                {label}
              </text>
            )
          }}
          interval="preserveStartEnd"
          minTickGap={32}
        />
        <YAxis tick={{ fontSize: 10, fill: '#6c757d' }} domain={['auto', 'auto']} width={48} />
        <Tooltip
          formatter={(v) => fmtPrice(v)}
          labelFormatter={(l) => {
            const point = data.find(d => d.ts === l)
            return point ? `${point.kind === 'INTRADAY' ? 'Today ' : ''}${point.label}` : l
          }}
          contentStyle={{ fontSize: 11 }}
        />
        {intradayStart && intradayEnd && (
          <ReferenceArea
            x1={intradayStart}
            x2={intradayEnd}
            fill="#0d6efd"
            fillOpacity={0.04}
            ifOverflow="extendDomain"
          />
        )}
        {sessionOpen && (
          <ReferenceLine
            x={sessionOpen}
            stroke="#0d6efd"
            strokeDasharray="3 3"
            label={{ value: 'Today', position: 'top', fill: '#0d6efd', fontSize: 10 }}
            ifOverflow="extendDomain"
          />
        )}
        {refs.map((r, i) => (
          <ReferenceLine
            key={i}
            y={r.y}
            stroke={r.color}
            strokeDasharray={r.dash}
            label={{ value: r.label, position: 'right', fill: r.color, fontSize: 10 }}
          />
        ))}
        <Line
          type="monotone"
          dataKey="close"
          stroke="#212529"
          dot={false}
          strokeWidth={1.5}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}

function FactRow({ label, value, valueClass }) {
  return (
    <div className="ck-co-fact">
      <span className="ck-co-fact-label">{label}</span>
      <span className={`ck-co-fact-value${valueClass ? ` ${valueClass}` : ''}`}>{value}</span>
    </div>
  )
}

function TradeFacts({ row }) {
  const slLabel = row.sl_label || (row.sl_is_dynamic ? 'Trailing stop' : 'Stop')
  return (
    <div className="ck-co-facts">
      <FactRow label="Quantity" value={row.quantity != null ? Math.abs(row.quantity) : EM} />
      <FactRow label="Avg cost" value={fmtPrice(row.avg_cost)} />
      <FactRow label="Last" value={fmtPrice(row.last_price ?? row.current_price)} />
      <FactRow label="Take profit" value={fmtPrice(row.tp_price)} />
      <FactRow
        label={slLabel}
        value={
          row.sl_price != null
            ? `${fmtPrice(row.sl_price)}${row.sl_is_dynamic ? ' (approx.)' : ''}`
            : EM
        }
      />
      <FactRow label="Invalidation" value={fmtPrice(row.invalidation_level)} />
      <FactRow
        label="Unrealized"
        value={`${fmtPct(row.unrealized_pnl_pct)} · ${fmtMoney(row.unrealized_pnl, true)}`}
        valueClass={row.unrealized_pnl != null && row.unrealized_pnl < 0 ? 'ck-co-pnl--down' : (row.unrealized_pnl != null && row.unrealized_pnl > 0 ? 'ck-co-pnl--up' : '')}
      />
      <FactRow label="Held" value={row.days_held != null ? `${row.days_held}d` : EM} />
      <FactRow label="Cushion" value={fmtDistance(row.distance_to_invalidation_pct)} />
    </div>
  )
}

function FramingBlock({ framing }) {
  if (!framing) return null
  return (
    <dl className="ck-co-framing">
      <div className="ck-co-framing-row">
        <dt>Plan</dt><dd>{framing.plan || EM}</dd>
      </div>
      <div className="ck-co-framing-row">
        <dt>Now</dt><dd>{framing.now || EM}</dd>
      </div>
      <div className="ck-co-framing-row">
        <dt>On plan?</dt><dd>{framing.on_plan || EM}</dd>
      </div>
      <div className="ck-co-framing-row ck-co-framing-row--advice">
        <dt>Advice</dt><dd>{framing.advice || EM}</dd>
      </div>
    </dl>
  )
}

function ThesisBlock({ row }) {
  if (!row.thesis_line && !row.expectation_line) {
    return (
      <div className="ck-co-thesis ck-co-thesis--missing">
        No structured thesis recorded for this trade.
      </div>
    )
  }
  return (
    <div className="ck-co-thesis">
      {row.thesis_line && <div className="ck-co-thesis-line"><strong>Thesis:</strong> {row.thesis_line}</div>}
      {row.expectation_line && <div className="ck-co-thesis-line"><strong>Expectation:</strong> {row.expectation_line}</div>}
    </div>
  )
}

function ShadowLine({ row }) {
  const label = row.shadow_relation_label || row.shadow_verdict_label || 'Shadow: —'
  const summary = row.shadow_summary_text || ''
  const level = String(row.shadow_relation_level || 'neutral').toLowerCase()
  return (
    <div className={`ck-co-shadow-line ck-co-shadow-line--${level}`}>
      <span className="ck-co-shadow-line-tag">{label}</span>
      {summary ? <span className="ck-co-shadow-line-text"> · {summary}</span> : null}
    </div>
  )
}

export default function PositionRowExpanded({ row }) {
  if (!row) return null
  return (
    <div className="ck-co-expand">
      <div className="ck-co-expand-grid">
        <section className="ck-co-trade-chart">
          <div className="ck-co-chart-header">
            <span className="ck-co-chart-title">{row.symbol} · since entry → today</span>
            <span className="ck-co-chart-sub">
              Held {row.days_held ?? EM}d · entered {row.entry_date || EM}
            </span>
          </div>
          <CombinedTradeChart row={row} />
        </section>
        <section className="ck-co-trade-facts">
          <div className="ck-co-trade-facts-header">Trade facts</div>
          <TradeFacts row={row} />
        </section>
        <section className="ck-co-trade-thesis">
          <ThesisBlock row={row} />
          <FramingBlock framing={row.recommendation_framing} />
          <ShadowLine row={row} />
        </section>
      </div>
    </div>
  )
}
