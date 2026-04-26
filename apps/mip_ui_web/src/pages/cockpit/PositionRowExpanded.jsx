/**
 * Inline expansion panel for a Position Health row on the cockpit.
 *
 * Shows the operator everything they need to make a judgment without
 * leaving the cockpit:
 *   - current-day 15m chart (line of bar closes)
 *   - daily-since-entry sparkline
 *   - Real / Shadow / Intraday / Invalidation summaries
 *   - plain-English recommendation
 *
 * Charts use Recharts (already a dep). Both series are tolerant of
 * missing/empty data — when the overlay is UNAVAILABLE / MARKET_CLOSED
 * the 15m chart renders an explanatory placeholder rather than an
 * empty axis.
 */
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  CartesianGrid,
} from 'recharts'

const EM = '\u2014'

function fmtPct(v, dp = 2) {
  if (v == null) return EM
  const n = Number(v)
  if (!Number.isFinite(n)) return EM
  return `${n >= 0 ? '+' : ''}${(n * 100).toFixed(dp)}%`
}

function fmtPrice(v) {
  if (v == null) return EM
  const n = Number(v)
  if (!Number.isFinite(n)) return EM
  return n.toFixed(2)
}

function fmtBarTime(ts) {
  if (!ts) return ''
  // bars come back as ISO; show HH:MM only
  const d = new Date(ts)
  if (Number.isNaN(d.getTime())) return String(ts).slice(11, 16)
  return d.toTimeString().slice(0, 5)
}

function IntradayChart({ bars, todayOpen, lastPrice }) {
  if (!bars || bars.length === 0) {
    return (
      <div className="ck-co-chart-empty">
        No current-day intraday bars available.
      </div>
    )
  }
  const data = bars
    .map(b => ({
      t: fmtBarTime(b.ts),
      close: b.close == null ? null : Number(b.close),
    }))
    .filter(d => d.close != null)

  return (
    <ResponsiveContainer width="100%" height={160}>
      <LineChart data={data} margin={{ top: 6, right: 12, left: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#eef0f3" />
        <XAxis dataKey="t" tick={{ fontSize: 10, fill: '#6c757d' }} interval="preserveStartEnd" minTickGap={32} />
        <YAxis tick={{ fontSize: 10, fill: '#6c757d' }} domain={['auto', 'auto']} width={42} />
        <Tooltip
          formatter={v => fmtPrice(v)}
          labelFormatter={l => `Time ${l}`}
          contentStyle={{ fontSize: 11 }}
        />
        {todayOpen != null && (
          <ReferenceLine
            y={Number(todayOpen)}
            stroke="#adb5bd"
            strokeDasharray="4 3"
            label={{ value: 'Open', position: 'right', fill: '#6c757d', fontSize: 10 }}
          />
        )}
        <Line type="monotone" dataKey="close" stroke="#0d6efd" dot={false} strokeWidth={1.6} isAnimationActive={false} />
      </LineChart>
    </ResponsiveContainer>
  )
}

function DailySinceEntryChart({ daily, entryDate }) {
  if (!daily || daily.length === 0) {
    return (
      <div className="ck-co-chart-empty">
        No daily-since-entry history available.
      </div>
    )
  }
  const data = daily.map(d => ({ d: d.date, close: Number(d.close) })).filter(x => Number.isFinite(x.close))

  return (
    <ResponsiveContainer width="100%" height={120}>
      <LineChart data={data} margin={{ top: 6, right: 12, left: 4, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#eef0f3" />
        <XAxis dataKey="d" tick={{ fontSize: 10, fill: '#6c757d' }} interval="preserveStartEnd" minTickGap={40} />
        <YAxis tick={{ fontSize: 10, fill: '#6c757d' }} domain={['auto', 'auto']} width={42} />
        <Tooltip
          formatter={v => fmtPrice(v)}
          labelFormatter={l => `Date ${l}`}
          contentStyle={{ fontSize: 11 }}
        />
        {entryDate && (
          <ReferenceLine
            x={String(entryDate).slice(0, 10)}
            stroke="#198754"
            strokeDasharray="4 3"
            label={{ value: 'Entry', position: 'top', fill: '#198754', fontSize: 10 }}
          />
        )}
        <Line type="monotone" dataKey="close" stroke="#212529" dot={false} strokeWidth={1.4} isAnimationActive={false} />
      </LineChart>
    </ResponsiveContainer>
  )
}

function SummaryBlock({ label, body, level }) {
  const cls = level ? ` ck-co-summary--${level}` : ''
  return (
    <div className={`ck-co-summary${cls}`}>
      <div className="ck-co-summary-label">{label}</div>
      <div className="ck-co-summary-body">{body || EM}</div>
    </div>
  )
}

export default function PositionRowExpanded({ row }) {
  if (!row) return null
  const todayPctText = fmtPct(row.today_change_pct, 2)
  const todayHeader =
    row.last_price != null
      ? `Last ${fmtPrice(row.last_price)} · ${todayPctText} from open`
      : todayPctText

  return (
    <div className="ck-co-expand">
      <div className="ck-co-expand-grid">
        <section className="ck-co-expand-charts">
          <div className="ck-co-chart-block">
            <div className="ck-co-chart-header">
              <span className="ck-co-chart-title">Today (15m)</span>
              <span className="ck-co-chart-sub">{todayHeader}</span>
            </div>
            <IntradayChart
              bars={row.intraday_bars}
              todayOpen={row.today_open}
              lastPrice={row.last_price}
            />
          </div>
          <div className="ck-co-chart-block">
            <div className="ck-co-chart-header">
              <span className="ck-co-chart-title">Since entry (daily)</span>
              <span className="ck-co-chart-sub">
                Held {row.days_held ?? EM} days · entered {row.entry_date || EM}
              </span>
            </div>
            <DailySinceEntryChart
              daily={row.daily_since_entry}
              entryDate={row.entry_date}
            />
          </div>
        </section>
        <section className="ck-co-expand-summaries">
          <SummaryBlock label="Real" body={row.real_summary_text} />
          <SummaryBlock label="Shadow" body={row.shadow_summary_text} />
          <SummaryBlock label="Today" body={row.intraday_summary_text} />
          <SummaryBlock label="Invalidation" body={row.invalidation_summary_text} />
          <SummaryBlock
            label="Recommendation"
            body={row.recommendation_text}
            level="recommendation"
          />
        </section>
      </div>
    </div>
  )
}
