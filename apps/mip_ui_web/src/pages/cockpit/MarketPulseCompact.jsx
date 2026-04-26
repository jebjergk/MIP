/**
 * Compact Market Pulse block for the cockpit (STOCK-only).
 *
 * Embedded contextual information — never a launcher. Restores the
 * visual market overview after the previous regression that reduced
 * Market Pulse to a stat card. Now shows:
 *   - tone label + breadth + avg return
 *   - lookback index sparkline (equal-weight, last ~30d)
 *   - top-3 and bottom-3 mover sparklines for quick read
 *
 * All charts are intentionally tiny (height ≤ 50px) so the block
 * stays compact and operator-first; this is not a research view.
 */
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  LineChart,
  Line,
  Tooltip,
  YAxis,
} from 'recharts'

const EM = '\u2014'

function formatPct(val, decimals = 1) {
  if (val == null) return EM
  const n = Number(val)
  if (!Number.isFinite(n)) return EM
  return `${n >= 0 ? '+' : ''}${n.toFixed(decimals)}%`
}

function toneClass(label) {
  const s = String(label || '').toLowerCase()
  if (s === 'supportive') return 'ok'
  if (s === 'weak') return 'warn'
  if (s === 'no data') return 'neutral'
  return 'info'
}

function IndexSparkline({ series }) {
  if (!series || series.length === 0) {
    return <div className="ck-co-sparkline-empty">No history</div>
  }
  const data = series.map(p => ({ ts: p.ts, v: Number(p.index_return_pct) }))
  const last = data[data.length - 1]?.v ?? 0
  const fillColor = last >= 0 ? '#198754' : '#dc3545'
  const strokeColor = fillColor
  return (
    <ResponsiveContainer width="100%" height={50}>
      <AreaChart data={data} margin={{ top: 2, right: 0, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="ck-co-mp-grad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={fillColor} stopOpacity={0.25} />
            <stop offset="100%" stopColor={fillColor} stopOpacity={0} />
          </linearGradient>
        </defs>
        <YAxis hide domain={['auto', 'auto']} />
        <Tooltip
          formatter={(v) => `${Number(v).toFixed(2)}%`}
          labelFormatter={() => ''}
          contentStyle={{ fontSize: 11 }}
        />
        <Area
          type="monotone"
          dataKey="v"
          stroke={strokeColor}
          strokeWidth={1.4}
          fill="url(#ck-co-mp-grad)"
          isAnimationActive={false}
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}

function MoverSpark({ mover, kind }) {
  const data = (mover?.sparkline || []).map(p => ({ ts: p.ts, c: Number(p.close) }))
  const positive = (mover?.day_return_pct ?? 0) >= 0
  const color = positive ? '#198754' : '#dc3545'
  return (
    <div className={`ck-co-mover ck-co-mover--${kind}`}>
      <div className="ck-co-mover-head">
        <span className="ck-co-mover-symbol">{mover?.symbol || EM}</span>
        <span className={`ck-co-mover-pct${positive ? ' ck-co-positive' : ' ck-co-negative'}`}>
          {formatPct(mover?.day_return_pct, 1)}
        </span>
      </div>
      <div className="ck-co-mover-spark">
        {data.length === 0 ? (
          <div className="ck-co-sparkline-empty ck-co-sparkline-empty--xs">{EM}</div>
        ) : (
          <ResponsiveContainer width="100%" height={28}>
            <LineChart data={data} margin={{ top: 2, right: 0, left: 0, bottom: 0 }}>
              <YAxis hide domain={['auto', 'auto']} />
              <Line
                type="monotone"
                dataKey="c"
                stroke={color}
                strokeWidth={1.2}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

export default function MarketPulseCompact({ data }) {
  if (!data || !data.available) {
    return (
      <div className="ck-co-card ck-co-card--market">
        <h2 className="ck-co-card-title">Market Pulse (STOCK)</h2>
        <p className="ck-co-empty">
          {data?.error ? `Market data unavailable: ${data.error}` : 'No market data yet.'}
        </p>
      </div>
    )
  }
  const breadthDenom = Number(data.breadth_total || 0)
  const breadthNumer = Number(data.breadth_up || 0)
  const breadthLabel = breadthDenom > 0
    ? `${breadthNumer} of ${breadthDenom} up`
    : EM
  const tone = toneClass(data.pulse_label)
  const topMovers = Array.isArray(data.top_movers) ? data.top_movers : []
  const bottomMovers = Array.isArray(data.bottom_movers) ? data.bottom_movers : []

  return (
    <div className="ck-co-card ck-co-card--market">
      <div className="ck-co-card-header">
        <h2 className="ck-co-card-title">Market Pulse (STOCK)</h2>
        <span className={`ck-co-pulse-tag ck-co-pulse-tag--${tone}`}>
          {data.pulse_label || 'Mixed'}
        </span>
      </div>
      <div className="ck-co-mp-headline">
        <div className="ck-co-mp-stat">
          <div className="ck-co-kpi-label">Breadth</div>
          <div className="ck-co-kpi-value">{breadthLabel}</div>
        </div>
        <div className="ck-co-mp-stat">
          <div className="ck-co-kpi-label">Avg return</div>
          <div className="ck-co-kpi-value">{formatPct(data.avg_return_pct, 2)}</div>
        </div>
        <div className="ck-co-mp-index">
          <div className="ck-co-kpi-label">Index (30d)</div>
          <IndexSparkline series={data.index_series} />
        </div>
      </div>
      <div className="ck-co-mp-movers">
        <div className="ck-co-mp-movers-col">
          <div className="ck-co-mp-movers-title">Top</div>
          <div className="ck-co-mp-movers-list">
            {topMovers.length === 0 ? (
              <div className="ck-co-empty">No movers yet.</div>
            ) : (
              topMovers.map((m, i) => <MoverSpark key={`t-${i}`} mover={m} kind="up" />)
            )}
          </div>
        </div>
        <div className="ck-co-mp-movers-col">
          <div className="ck-co-mp-movers-title">Bottom</div>
          <div className="ck-co-mp-movers-list">
            {bottomMovers.length === 0 ? (
              <div className="ck-co-empty">No movers yet.</div>
            ) : (
              bottomMovers.map((m, i) => <MoverSpark key={`b-${i}`} mover={m} kind="down" />)
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
