/**
 * Compact Trade Proposals subsection embedded inside the Live Portfolio
 * card.
 *
 * Renders deterministic structural proposals as a stacked column of
 * compact rows. The panel is **pre-entry monitoring**: each row shows
 *   - a live "Now" price (IBKR snapshot tick, refreshed each cockpit
 *     poll), or a per-proposal "Live unavailable" banner when the
 *     intraday fetch failed / market is closed,
 *   - prev-close as a small secondary footer (always present),
 *   - a mini chart that combines a daily backbone with today's
 *     intraday tail, separated by a vertical "today" divider so the
 *     operator can see in one glance whether price is approaching the
 *     entry zone right now.
 *
 * The deeper-research surface lives at /structural-market-timeline.
 */
import { Line, LineChart, ReferenceArea, ReferenceLine, ResponsiveContainer, YAxis } from 'recharts'
import { Link } from 'react-router-dom'

function fmtPrice(v) {
  if (v == null) return '\u2014'
  const n = Number(v)
  if (!Number.isFinite(n)) return '\u2014'
  return n.toFixed(2)
}

function fmtZone(low, high) {
  if (low == null || high == null) return '\u2014'
  return `${fmtPrice(low)} \u2013 ${fmtPrice(high)}`
}

function fmtPct(decimal) {
  if (decimal == null) return null
  const n = Number(decimal) * 100
  if (!Number.isFinite(n)) return null
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toFixed(1)}%`
}

function fmtCloseDate(iso) {
  if (!iso) return null
  const m = String(iso).match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (!m) return null
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
  const mo = months[parseInt(m[2], 10) - 1] || m[2]
  return `${mo} ${parseInt(m[3], 10)}`
}

/** Format an ISO timestamp into a short HH:MM (local-ish; the server
 *  already returns the IBKR exchange-local clock string). */
function fmtQuoteClock(iso) {
  if (!iso) return null
  const m = String(iso).match(/(\d{2}):(\d{2})/)
  if (!m) return null
  return `${m[1]}:${m[2]}`
}

function zoneTone(code) {
  switch (code) {
    case 'IN_ZONE':           return 'ok'
    case 'NEAR_ZONE':         return 'info'
    case 'INVALIDATED':       return 'critical'
    case 'TOO_FAR':           return 'neutral'
    case 'ABOVE_ENTRY':
    case 'BELOW_ENTRY':       return 'warn'
    case 'LIVE_UNAVAILABLE':  return 'neutral'
    default:                  return 'neutral'
  }
}

function readinessTone(code) {
  switch (code) {
    case 'READY_NOW':         return 'ok'
    case 'NEAR_READY':        return 'info'
    case 'WAIT':              return 'warn'
    case 'DO_NOT_ENTER':      return 'critical'
    case 'LIVE_UNAVAILABLE':  return 'neutral'
    default:                  return 'neutral'
  }
}

// Priority is an INTENTIONALLY-SEPARATE concept from entry readiness:
//   * priority  = comparative ranking strength of this proposal vs the
//                 rest of today's slate (computed from the same composite
//                 the proposal SP uses).
//   * readiness = whether the proposal is actionable RIGHT NOW (live
//                 price vs entry zone + committee stance).
// A #1 proposal can be WAIT, and a #5 proposal can be READY_NOW. The two
// badges are read independently.
function priorityTone(band) {
  switch (band) {
    case 'HIGH':   return 'ok'
    case 'MEDIUM': return 'info'
    case 'LOW':    return 'neutral'
    default:       return 'neutral'
  }
}


// --- Phase 4 thesis-health surfacing ---------------------------------------
//
// Tones map to existing badge CSS palettes:
//   constructive = green/ok       (long thesis intact, e.g. PROPOSE_LONG)
//   warning      = amber/warn     (long thesis degraded but alive)
//   bearish      = rose/danger    (fresh short signal forming)
//   neutral      = grey           (not actionable, e.g. NO_TRADE)
//
// IMPORTANT semantic notes:
//   * WATCH_LONG_FAILURE is a *long-thesis warning*, not a fresh short.
//     Tone is 'warning', not 'bearish' — operator sees "long is in
//     trouble, watch for failure" rather than "go short".
//   * SHORT_REJECTED is a neutral non-event ("don't short here"), not
//     a bullish call.
//   * LONG_DEGRADED_BUT_ALIVE / SHORT_DEGRADED_BUT_ALIVE are health
//     states attached to the prior thesis; both render amber.
const THESIS_HEALTH_TONE = {
  PROPOSE_LONG:          'ok',
  PROPOSE_SHORT:         'bearish',
  WATCH_LONG:            'info',
  WATCH_SHORT:           'bearish',
  WAIT_FOR_CONFIRMATION: 'neutral',
  WATCH_LONG_FAILURE:    'warn',
  WATCH_SHORT_FAILURE:   'warn',
  NO_TRADE:              'neutral',
}

const HEALTH_LABEL_TONE = {
  LONG_CONFIRMED:           'ok',
  SHORT_CONFIRMED:          'bearish',
  LONG_DEGRADED_BUT_ALIVE:  'warn',
  SHORT_DEGRADED_BUT_ALIVE: 'warn',
  LONG_REJECTED:            'warn',
  SHORT_REJECTED:           'neutral',
  UNKNOWN:                  'neutral',
}

const FINAL_ACTION_LABEL = {
  PROPOSE_LONG:          'Propose long',
  PROPOSE_SHORT:         'Propose short',
  WATCH_LONG:            'Watch long',
  WATCH_SHORT:           'Watch short',
  WAIT_FOR_CONFIRMATION: 'Wait for confirmation',
  WATCH_LONG_FAILURE:    'Watch long failure',
  WATCH_SHORT_FAILURE:   'Watch short failure',
  NO_TRADE:              'No trade',
}

const FINAL_ACTION_TOOLTIP = {
  WATCH_LONG_FAILURE:
    'Prior long thesis is degrading but a confirmed short is not yet dominant. ' +
    'Treat as a long-thesis health warning, not a short signal.',
  WAIT_FOR_CONFIRMATION:
    'Setup is still forming. Wait for confirming follow-through before acting.',
  WATCH_SHORT:
    'Fresh short evidence dominant: support failure or breakdown confirmation. ' +
    'A new short proposal may be forming.',
  WATCH_SHORT_FAILURE:
    'Prior short thesis is degrading. Watch for invalidation, not a new short.',
}

function thesisHealthLabel(health) {
  if (!health) return null
  return String(health).replace(/_/g, ' ').toLowerCase().replace(/^./, (c) => c.toUpperCase())
}

function isDegradedLongAction(action) {
  return action === 'WATCH_LONG_FAILURE'
}

function isBearishWatchAction(action) {
  return action === 'WATCH_SHORT' || action === 'WATCH_SHORT_FAILURE'
}

function fmtAsOfDate(iso) {
  if (!iso) return null
  const m = String(iso).match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (!m) return iso
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
  const mo = months[parseInt(m[2], 10) - 1] || m[2]
  return `${mo} ${parseInt(m[3], 10)}`
}

/** Days between an ISO date and today, or null if not parseable. */
function daysSince(isoDate) {
  if (!isoDate) return null
  const m = String(isoDate).match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (!m) return null
  const dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]))
  if (Number.isNaN(dt.getTime())) return null
  const today = new Date()
  const todayUtc = Date.UTC(today.getFullYear(), today.getMonth(), today.getDate())
  return Math.max(0, Math.floor((todayUtc - dt.getTime()) / 86400000))
}

function fmtConfidence(v) {
  if (v == null) return null
  const n = Number(v)
  if (!Number.isFinite(n)) return null
  return n.toFixed(2)
}


function ThesisHealthBadge({ health }) {
  if (!health) return null
  const action = health.latest_board_action || null
  if (!action) return null
  const tone = THESIS_HEALTH_TONE[action] || 'neutral'
  const label = FINAL_ACTION_LABEL[action] || action.replace(/_/g, ' ')
  const tooltip = FINAL_ACTION_TOOLTIP[action] || (health.latest_final_thesis || null)
  const linkageNote =
    health.linkage === 'SYMBOL_LATEST_ONLY' ? ' (symbol-level)' : ''
  return (
    <span
      className={`ck-co-tp-thesis ck-co-tp-thesis--${tone}`}
      title={tooltip || undefined}
      aria-label={tooltip || undefined}
    >
      <span className="ck-co-tp-thesis-label">{label}</span>
      {linkageNote ? (
        <span className="ck-co-tp-thesis-linkage">{linkageNote}</span>
      ) : null}
    </span>
  )
}


function BoardAgeNote({ health }) {
  if (!health) return null
  const asOf = health.as_of_date
  if (!asOf) return null
  const days = daysSince(asOf)
  let suffix = ''
  if (days === 0) suffix = ' · today'
  else if (days === 1) suffix = ' · 1d ago'
  else if (days != null) suffix = ` · ${days}d ago`
  const stale = days != null && days >= 2
  return (
    <span
      className={`ck-co-tp-board-age${stale ? ' ck-co-tp-board-age--stale' : ''}`}
      title={stale ? `Board verdict is ${days} days old.` : undefined}
    >
      {`as_of ${fmtAsOfDate(asOf)}${suffix}`}
    </span>
  )
}


function AgenticHealthDetail({ health }) {
  if (!health) return null
  const healthLabel = thesisHealthLabel(health.latest_thesis_health)
  const healthTone = HEALTH_LABEL_TONE[health.latest_thesis_health] || 'neutral'
  const continuation = health.continuation_quality
  const overhead = health.resistance_overhead_risk
  const cluster = health.recent_cluster
  const rangePct = health.current_range_position_pct
  const brokenLevel = health.broken_resistance_level
  const brokenConf = health.broken_resistance_confidence
  const ns = health.nearest_support
  const nr = health.nearest_resistance
  const thesis = health.latest_final_thesis
  return (
    <details className="ck-co-tp-health-detail">
      <summary className="ck-co-tp-health-summary">
        Agentic thesis health
        {healthLabel ? (
          <span className={`ck-co-tp-thesis-state ck-co-tp-thesis-state--${healthTone}`}>
            {healthLabel}
          </span>
        ) : null}
      </summary>
      <div className="ck-co-tp-health-body">
        {thesis ? (
          <p className="ck-co-tp-health-thesis">{thesis}</p>
        ) : null}
        <dl className="ck-co-tp-health-grid">
          {continuation ? (
            <>
              <dt>Continuation</dt>
              <dd>{continuation.replace(/_/g, ' ').toLowerCase()}</dd>
            </>
          ) : null}
          {overhead ? (
            <>
              <dt>Resistance overhead</dt>
              <dd>{overhead.replace(/_/g, ' ').toLowerCase()}</dd>
            </>
          ) : null}
          {cluster ? (
            <>
              <dt>Recent cluster</dt>
              <dd>{cluster.replace(/_/g, ' ').toLowerCase()}</dd>
            </>
          ) : null}
          {rangePct != null ? (
            <>
              <dt>Range position</dt>
              <dd>{Number(rangePct).toFixed(1)}%</dd>
            </>
          ) : null}
          {brokenLevel != null ? (
            <>
              <dt>Broken R{'\u2192'}S</dt>
              <dd>
                {fmtPrice(brokenLevel)}
                {brokenConf != null ? (
                  <span className="ck-co-tp-health-conf"> · conf {fmtConfidence(brokenConf)}</span>
                ) : null}
              </dd>
            </>
          ) : null}
          {ns != null ? (
            <>
              <dt>Nearest support</dt>
              <dd>{fmtPrice(ns)}</dd>
            </>
          ) : null}
          {nr != null ? (
            <>
              <dt>Nearest resistance</dt>
              <dd>{fmtPrice(nr)}</dd>
            </>
          ) : null}
        </dl>
      </div>
    </details>
  )
}

function stanceLabel(stance) {
  if (!stance) return null
  const s = String(stance).toUpperCase()
  switch (s) {
    case 'APPROVE':         return 'Approve'
    case 'APPROVE_REDUCED': return 'Approve (reduced)'
    case 'DEFER':           return 'Defer'
    case 'DENY':            return 'Deny'
    case 'WAIT_RECLAIM':    return 'Wait for reclaim'
    default:                return s.replace(/_/g, ' ').toLowerCase()
  }
}


/**
 * Build a Y domain that always shows the entry zone band prominently
 * even when invalidation sits far away (which would otherwise crush
 * the price action to a hairline). The invalidation level is honoured
 * only when it is within ~30% of the price midpoint.
 */
function computeYDomain({ closes, zoneLow, zoneHigh, invalidation, currentPrice }) {
  const candidates = closes.slice()
  if (currentPrice != null && Number.isFinite(currentPrice)) candidates.push(currentPrice)
  const priceLo = Math.min(...candidates)
  const priceHi = Math.max(...candidates)
  let lo = priceLo
  let hi = priceHi
  if (zoneLow != null) lo = Math.min(lo, zoneLow)
  if (zoneHigh != null) hi = Math.max(hi, zoneHigh)

  const span = Math.max(hi - lo, 1e-6)
  const mid = (priceLo + priceHi) / 2
  let drawInvalidation = false
  if (invalidation != null && Number.isFinite(invalidation) && mid > 0) {
    const dist = Math.abs(invalidation - mid) / mid
    if (dist <= 0.30) {
      lo = Math.min(lo, invalidation)
      hi = Math.max(hi, invalidation)
      drawInvalidation = true
    }
  }
  const pad = Math.max((hi - lo) * 0.10, span * 0.05)
  return { domain: [lo - pad, hi + pad], drawInvalidation }
}

function MiniChart({ proposal }) {
  const series = proposal.mini_chart_series || []
  if (series.length < 2) {
    return <div className="ck-co-tp-spark-empty">no chart yet</div>
  }

  // Find the boundary where the intraday tail begins (first INTRADAY
  // point). All points before are daily backbone; from this index on
  // it's today's session.
  const dividerIdx = series.findIndex((p) => p.kind === 'INTRADAY')
  const hasIntraday = dividerIdx >= 0
  const lastIdx = series.length - 1

  // Build per-row data with two parallel keys (`daily`, `intraday`) so
  // Recharts can render them as two visually distinct lines. We bridge
  // the join (last DAILY index also gets `intraday=close`) so there's
  // no visual gap.
  const data = series.map((p, i) => {
    const row = { i, ts: p.ts, close: p.close }
    if (p.kind === 'INTRADAY') {
      row.intraday = p.close
    } else {
      row.daily = p.close
    }
    return row
  })
  if (hasIntraday && dividerIdx > 0) {
    // Bridge the join.
    data[dividerIdx - 1].intraday = data[dividerIdx - 1].daily
  }

  const closes = data.map((d) => d.close).filter((v) => Number.isFinite(v))
  if (closes.length < 2) {
    return <div className="ck-co-tp-spark-empty">no chart yet</div>
  }

  const { domain, drawInvalidation } = computeYDomain({
    closes,
    zoneLow: proposal.entry_zone_low,
    zoneHigh: proposal.entry_zone_high,
    invalidation: proposal.invalidation_level,
    currentPrice: proposal.current_price,
  })

  const dirLong = (proposal.direction || '').toUpperCase() === 'LONG'
  const zoneFill = dirLong ? '#198754' : '#dc3545'
  const dailyColor = '#adb5bd'   // muted backbone
  const intradayColor = '#1f2933' // crisp today line

  const inZone =
    proposal.zone_status === 'IN_ZONE' ||
    proposal.zone_status === 'NEAR_ZONE'

  const renderLastIntradayDot = (props) => {
    if (props.index !== lastIdx) return null
    const { cx, cy } = props
    const fill = inZone ? '#0d6efd' : '#1f2933'
    return (
      <circle cx={cx} cy={cy} r={3.4} fill={fill} stroke="#fff" strokeWidth={1.2} />
    )
  }
  // When there's no intraday tail, put the dot on the last daily point
  // instead so the latest reference price is still marked.
  const renderLastDailyDot = (props) => {
    if (hasIntraday) return null
    if (props.index !== lastIdx) return null
    const { cx, cy } = props
    return (
      <circle cx={cx} cy={cy} r={3.0} fill="#6c757d" stroke="#fff" strokeWidth={1} />
    )
  }

  return (
    <div className="ck-co-tp-spark">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 6, right: 8, bottom: 4, left: 6 }}>
          <YAxis hide domain={domain} type="number" allowDecimals />
          {proposal.entry_zone_low != null && proposal.entry_zone_high != null ? (
            <ReferenceArea
              y1={proposal.entry_zone_low}
              y2={proposal.entry_zone_high}
              fill={zoneFill}
              fillOpacity={0.22}
              stroke={zoneFill}
              strokeOpacity={0.45}
              strokeDasharray="2 3"
            />
          ) : null}
          {drawInvalidation && proposal.invalidation_level != null ? (
            <ReferenceLine
              y={proposal.invalidation_level}
              stroke="#b02a37"
              strokeDasharray="3 3"
              strokeOpacity={0.7}
            />
          ) : null}
          {hasIntraday && dividerIdx > 0 ? (
            <ReferenceLine
              x={dividerIdx}
              stroke="#868e96"
              strokeDasharray="1 3"
              strokeOpacity={0.6}
            />
          ) : null}
          <Line
            type="monotone"
            dataKey="daily"
            stroke={dailyColor}
            strokeWidth={1.2}
            dot={renderLastDailyDot}
            isAnimationActive={false}
            connectNulls={false}
          />
          {hasIntraday ? (
            <Line
              type="monotone"
              dataKey="intraday"
              stroke={intradayColor}
              strokeWidth={1.8}
              dot={renderLastIntradayDot}
              isAnimationActive={false}
              connectNulls={false}
            />
          ) : null}
        </LineChart>
      </ResponsiveContainer>
      <span className="sr-only">
        {`Mini chart for ${proposal.symbol}, current ${fmtPrice(proposal.current_price)}, zone ${fmtZone(proposal.entry_zone_low, proposal.entry_zone_high)}.`}
      </span>
    </div>
  )
}


function NowLine({ proposal }) {
  const live = proposal.current_price != null
  if (!live) {
    return (
      <div className="ck-co-tp-now ck-co-tp-now--unavailable">
        <span className="ck-co-tp-now-label">Live unavailable</span>
        <span
          className="ck-co-tp-now-hint"
          title={'No tick data \u2014 TWS may be down, or the market is closed/pre-open.'}
        >
          {'\u2014'}
        </span>
      </div>
    )
  }
  const sourceLabel =
    proposal.current_price_source === 'LIVE_TICK'
      ? 'live tick'
      : proposal.current_price_source === 'INTRADAY_BAR'
        ? '15m bar'
        : null
  const clock = fmtQuoteClock(proposal.current_price_ts)
  return (
    <div className="ck-co-tp-now">
      <span className="ck-co-tp-now-label">Now</span>
      <span className="ck-co-tp-now-value">{fmtPrice(proposal.current_price)}</span>
      {sourceLabel ? (
        <span className="ck-co-tp-now-source" title={`Source: ${sourceLabel}${clock ? ` at ${clock}` : ''}`}>
          {sourceLabel}{clock ? ` ${clock}` : ''}
        </span>
      ) : null}
    </div>
  )
}


function ProposalRow({ proposal }) {
  const distancePct = fmtPct(proposal.distance_to_zone_pct)
  const stance = stanceLabel(proposal.committee_stance)
  const liveAvailable = proposal.current_price != null
  const hasPriority = proposal.priority_rank != null && proposal.priority_band != null
  // Phase 4 thesis-health surfacing — UI-only, never affects trading.
  const ph4 = proposal.phase4_health || null
  const ph4Action = ph4?.latest_board_action || null
  const rowToneClass = isDegradedLongAction(ph4Action)
    ? ' ck-co-tp-row--degraded'
    : isBearishWatchAction(ph4Action)
      ? ' ck-co-tp-row--bearish-watch'
      : ''
  // Tooltip explains *why* this proposal ranks where it does. The score is
  // included so an operator can compare two proposals at a glance.
  const priorityTooltip = hasPriority
    ? `Priority #${proposal.priority_rank} (${proposal.priority_band_label}) — ${proposal.priority_reason_label}` +
      (proposal.composite_score != null ? ` · score ${proposal.composite_score.toFixed(3)}` : '')
    : null
  // Phase 2 Sprint 4 / option 4a — non-blocking same-symbol warning.
  // Surfaces when the active slate carries more than one PROPOSED row
  // on this SYMBOL so the operator notices and can pick. The board
  // itself is unchanged; we just make the multiplicity visible.
  const sameSymbolOther = proposal.same_symbol_other_active && proposal.same_symbol_other_count > 0
  const sameSymbolDirs = Array.isArray(proposal.same_symbol_other_directions)
    ? proposal.same_symbol_other_directions
    : []
  const sameSymbolTooltip = sameSymbolOther
    ? `${proposal.same_symbol_other_count} other active proposal${proposal.same_symbol_other_count === 1 ? '' : 's'} on ${proposal.symbol}` +
      (sameSymbolDirs.length > 0 ? ` (${sameSymbolDirs.join(', ')})` : '') +
      ' — pick one or compare on the structural market timeline before acting.'
    : null
  return (
    <div className={`ck-co-tp-row${rowToneClass}`}>
      <div className="ck-co-tp-info">
        <div className="ck-co-tp-headline">
          {hasPriority ? (
            <span
              className={`ck-co-tp-priority ck-co-tp-priority--${priorityTone(proposal.priority_band)}`}
              title={priorityTooltip}
              aria-label={priorityTooltip}
            >
              <span className="ck-co-tp-priority-rank">#{proposal.priority_rank}</span>
              <span className="ck-co-tp-priority-band">{proposal.priority_band_label}</span>
            </span>
          ) : null}
          <span className="ck-co-tp-symbol">{proposal.symbol}</span>
          <span className={`ck-co-tp-direction ck-co-tp-direction--${(proposal.direction || '').toLowerCase()}`}>
            {proposal.direction || '\u2014'}
          </span>
          {stance ? (
            <span className="ck-co-tp-stance" title={proposal.committee_stance_source ? `from ${proposal.committee_stance_source}` : ''}>
              {stance}
            </span>
          ) : (
            <span className="ck-co-tp-stance ck-co-tp-stance--missing">No verdict yet</span>
          )}
          <ThesisHealthBadge health={ph4} />
          {sameSymbolOther ? (
            <span
              className="ck-co-tp-samesym"
              title={sameSymbolTooltip}
              aria-label={sameSymbolTooltip}
            >
              +{proposal.same_symbol_other_count} on {proposal.symbol}
            </span>
          ) : null}
          <BoardAgeNote health={ph4} />
        </div>

        {hasPriority ? (
          <div className="ck-co-tp-line ck-co-tp-line--reason" title={priorityTooltip}>
            <span className="ck-co-tp-line-label">Why ranked</span>
            <span className="ck-co-tp-reason-text">{proposal.priority_reason_label}</span>
          </div>
        ) : null}

        <div className="ck-co-tp-line">
          <span className="ck-co-tp-line-label">Zone</span>
          <span>{fmtZone(proposal.entry_zone_low, proposal.entry_zone_high)}</span>
          <span className="ck-co-tp-line-sep">{'\u00b7'}</span>
          <NowLine proposal={proposal} />
        </div>

        <div className="ck-co-tp-line ck-co-tp-line--secondary">
          <span
            className="ck-co-tp-line-label"
            title="Most recent daily close (not a live tick)"
          >
            Prev close
          </span>
          <span>{fmtPrice(proposal.last_close)}</span>
          {proposal.last_close_date ? (
            <span className="ck-co-tp-line-stale">
              ({fmtCloseDate(proposal.last_close_date)})
            </span>
          ) : null}
        </div>

        {liveAvailable ? (
          <div className="ck-co-tp-badges">
            <span className={`ck-co-tp-badge ck-co-tp-badge--${zoneTone(proposal.zone_status)}`}>
              {proposal.zone_status_label}
              {distancePct && proposal.zone_status !== 'IN_ZONE' && proposal.zone_status !== 'INVALIDATED' ? (
                <span className="ck-co-tp-badge-sub"> ({distancePct})</span>
              ) : null}
            </span>
            <span className={`ck-co-tp-badge ck-co-tp-badge--${readinessTone(proposal.entry_readiness)}`}>
              {proposal.entry_readiness_label}
            </span>
          </div>
        ) : (
          <div className="ck-co-tp-banner">
            Live data unavailable {'\u2014'} zone status will update on the next refresh.
          </div>
        )}
        <AgenticHealthDetail health={ph4} />
      </div>
      <MiniChart proposal={proposal} />
    </div>
  )
}


function overlaySubLabel(status) {
  switch (status) {
    case 'OK':            return null   // healthy → no extra label needed
    case 'PARTIAL':       return 'live partial'
    case 'MARKET_CLOSED': return 'market closed'
    case 'UNAVAILABLE':   return 'live unavailable'
    default:              return null
  }
}


export default function TradeProposalsPanel({ data }) {
  if (!data) return null

  const total = data.total_count || 0
  const proposals = data.proposals || []
  const moreCount = Math.max(0, total - proposals.length)
  const overlayStatus = data.intraday_overlay_status || 'UNAVAILABLE'
  const overlaySub = overlaySubLabel(overlayStatus)

  if (!data.available) {
    return (
      <section className="ck-co-tp-section">
        <header className="ck-co-tp-header">
          <h3 className="ck-co-tp-title">Trade proposals</h3>
          <span className="ck-co-tp-sub ck-co-tp-sub--warn">unavailable</span>
        </header>
        {data.note ? <p className="ck-co-tp-note">{data.note}</p> : null}
      </section>
    )
  }

  return (
    <section className="ck-co-tp-section">
      <header className="ck-co-tp-header">
        <h3 className="ck-co-tp-title">Trade proposals</h3>
        <span className="ck-co-tp-sub">
          {proposals.length === 0 && total === 0 ? 'none today' : `${proposals.length} actionable`}
          {moreCount > 0 ? ` \u00b7 +${moreCount} more` : ''}
          {overlaySub ? ` \u00b7 ${overlaySub}` : ''}
        </span>
      </header>

      {/* When the live overlay is degraded, surface the actual reason
          (timeout, TWS disconnected, sub missing, etc.) so the operator
          knows why the panel is showing prev close only. */}
      {overlayStatus !== 'OK' && data.note ? (
        <p className="ck-co-tp-note ck-co-tp-note--warn" title={data.note}>
          {data.note}
        </p>
      ) : null}

      {proposals.length === 0 ? (
        <p className="ck-co-tp-note">
          {data.note || 'No actionable proposals right now.'}
        </p>
      ) : (
        <div className="ck-co-tp-list">
          {proposals.map((p) => (
            <Link
              key={p.proposal_id}
              to={p.detail_route || '#'}
              className="ck-co-tp-row-link"
              title={`Inspect ${p.symbol} on the structural market timeline`}
            >
              <ProposalRow proposal={p} />
            </Link>
          ))}
        </div>
      )}
    </section>
  )
}
