/**
 * Phase 4 agentic chair read — thesis health, structural evidence, zones.
 * Shared by LPA board explanation and Structural Market Timeline.
 */

import './Phase4ChairSection.css'

function pretty(s) {
  if (s == null) return '\u2014'
  return String(s).replace(/_/g, ' ').toLowerCase()
}

const PH4_ACTION_LABEL = {
  PROPOSE_LONG:          'Propose long',
  PROPOSE_SHORT:         'Propose short',
  WATCH_LONG:            'Watch long',
  WATCH_SHORT:           'Watch short',
  WAIT_FOR_CONFIRMATION: 'Wait for confirmation',
  WATCH_LONG_FAILURE:    'Watch long failure',
  WATCH_SHORT_FAILURE:   'Watch short failure',
  NO_TRADE:              'No trade',
}

const PH4_ACTION_TONE = {
  PROPOSE_LONG:          'ok',
  PROPOSE_SHORT:         'bearish',
  WATCH_LONG:            'info',
  WATCH_SHORT:           'bearish',
  WAIT_FOR_CONFIRMATION: 'neutral',
  WATCH_LONG_FAILURE:    'warn',
  WATCH_SHORT_FAILURE:   'warn',
  NO_TRADE:              'neutral',
}

const DEGRADED_THESIS_HEALTH = new Set([
  'LONG_DEGRADED_BUT_ALIVE',
  'SHORT_DEGRADED_BUT_ALIVE',
  'LONG_REJECTED',
])

const DEGRADED_FINAL_ACTION = new Set([
  'WATCH_LONG_FAILURE',
  'WATCH_SHORT_FAILURE',
])

function fmtPh4Date(iso) {
  if (!iso) return null
  const m = String(iso).match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (!m) return iso
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
  return `${months[parseInt(m[2], 10) - 1] || m[2]} ${parseInt(m[3], 10)}`
}

function ph4DaysSince(iso) {
  if (!iso) return null
  const m = String(iso).match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (!m) return null
  const dt = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]))
  if (Number.isNaN(dt.getTime())) return null
  const today = new Date()
  const todayUtc = Date.UTC(today.getFullYear(), today.getMonth(), today.getDate())
  return Math.max(0, Math.floor((todayUtc - dt.getTime()) / 86400000))
}

function fmtPriceLite(v) {
  if (v == null) return '\u2014'
  const n = Number(v)
  if (!Number.isFinite(n)) return '\u2014'
  return n.toFixed(2)
}

export default function Phase4ChairSection({ phase4Chair, phase4LatestHealth }) {
  const latest = phase4LatestHealth || phase4Chair
  if (!latest) return null

  const action = latest.final_action
  const actionLabel = PH4_ACTION_LABEL[action] || (action ? action.replace(/_/g, ' ').toLowerCase() : null)
  const actionTone = PH4_ACTION_TONE[action] || 'neutral'
  const thesisHealth = latest.thesis_health
  const isDegraded =
    DEGRADED_FINAL_ACTION.has(action) ||
    DEGRADED_THESIS_HEALTH.has(thesisHealth)

  const linkage = latest.linkage
  const asOf = latest.as_of_date
  const daysOld = ph4DaysSince(asOf)
  const ageSuffix = daysOld === 0
    ? 'today'
    : daysOld === 1
      ? '1d ago'
      : daysOld != null ? `${daysOld}d ago` : null
  const stale = daysOld != null && daysOld >= 2

  const priorRef = latest.prior_thesis_reference
  const priorRefId = priorRef && typeof priorRef === 'object'
    ? (priorRef.proposal_id ?? priorRef.PROPOSAL_ID)
    : null
  const priorRefDirection = priorRef && typeof priorRef === 'object'
    ? (priorRef.direction ?? priorRef.DIRECTION)
    : null
  const priorRefAction = priorRef && typeof priorRef === 'object'
    ? (priorRef.original_action ?? priorRef.original_final_action ?? priorRef.ACTION)
    : null

  const struct = latest.structural_timeline_summary || {}
  const candle = latest.candle_psychology || {}
  const ac = latest.actionability_context || {}
  const zones = latest.zones || {}
  const brokenR = zones.broken_resistance_as_support
  const nearestSupport = zones.nearest_support
  const nearestResistance = zones.nearest_resistance

  const showBreadcrumb =
    phase4LatestHealth && phase4Chair && phase4LatestHealth !== phase4Chair

  return (
    <div className="lpa-c2-bx-ph4">
      <div className="lpa-c2-bx-ph4-head">
        <span className="lpa-c2-bx-card-subhead">Agentic thesis (Phase 4)</span>
        {actionLabel ? (
          <span className={`lpa-c2-bx-ph4-action lpa-c2-bx-ph4-action--${actionTone}`}>
            {actionLabel}
          </span>
        ) : null}
        {thesisHealth ? (
          <span className={`lpa-c2-bx-ph4-health lpa-c2-bx-ph4-health--${actionTone}`}>
            {pretty(thesisHealth)}
          </span>
        ) : null}
        {linkage === 'SYMBOL_LATEST_ONLY' ? (
          <span className="lpa-c2-bx-ph4-linkage" title="No verdict directly cites this proposal — showing the latest symbol-level verdict.">
            symbol-level
          </span>
        ) : null}
        {asOf ? (
          <span className={`lpa-c2-bx-ph4-age${stale ? ' lpa-c2-bx-ph4-age--stale' : ''}`}>
            as_of {fmtPh4Date(asOf)}{ageSuffix ? ` · ${ageSuffix}` : ''}
          </span>
        ) : null}
      </div>

      {isDegraded ? (
        <div className="lpa-c2-bx-ph4-callout">
          {action === 'WATCH_LONG_FAILURE'
            ? 'Long thesis is degrading. Treat as a long-thesis health warning, not a fresh short signal.'
            : action === 'WATCH_SHORT_FAILURE'
              ? 'Short thesis is degrading. Watch for invalidation.'
              : thesisHealth === 'LONG_DEGRADED_BUT_ALIVE'
                ? 'Prior long thesis is contested but not yet rejected.'
                : thesisHealth === 'SHORT_DEGRADED_BUT_ALIVE'
                  ? 'Prior short thesis is contested but not yet rejected.'
                  : 'Prior thesis has been rejected by latest evidence.'}
        </div>
      ) : null}

      {priorRefId ? (
        <div className="lpa-c2-bx-ph4-prior" title="Prior thesis lineage referenced by this verdict">
          <span className="lpa-c2-bx-ph4-prior-label">Prior thesis</span>
          <span>#{priorRefId}</span>
          {priorRefDirection ? <span className="lpa-c2-bx-ph4-prior-dir">{String(priorRefDirection).toUpperCase()}</span> : null}
          {priorRefAction ? <span className="lpa-c2-bx-ph4-prior-action">{pretty(priorRefAction)}</span> : null}
        </div>
      ) : null}

      {latest.final_thesis ? (
        <p className="lpa-c2-bx-ph4-thesis">{latest.final_thesis}</p>
      ) : null}

      <div className="lpa-c2-bx-ph4-grid">
        <div className="lpa-c2-bx-ph4-section">
          <div className="lpa-c2-bx-ph4-section-label">Structural timeline</div>
          <dl className="lpa-c2-bx-ph4-dl">
            {struct.trend_shape_class ? (
              <>
                <dt>Trend shape</dt>
                <dd>{pretty(struct.trend_shape_class)}</dd>
              </>
            ) : null}
            {struct.current_range_position_pct != null ? (
              <>
                <dt>Range position</dt>
                <dd>{Number(struct.current_range_position_pct).toFixed(1)}%</dd>
              </>
            ) : null}
            {ac.continuation_quality ? (
              <>
                <dt>Continuation</dt>
                <dd>{pretty(ac.continuation_quality)}</dd>
              </>
            ) : null}
          </dl>
        </div>

        <div className="lpa-c2-bx-ph4-section">
          <div className="lpa-c2-bx-ph4-section-label">Candle psychology</div>
          <dl className="lpa-c2-bx-ph4-dl">
            {candle.recent_cluster ? (
              <>
                <dt>Recent cluster</dt>
                <dd>{pretty(candle.recent_cluster)}</dd>
              </>
            ) : (
              <>
                <dt>Recent cluster</dt>
                <dd className="lpa-c2-muted">none labeled</dd>
              </>
            )}
            {ac.resistance_overhead_risk ? (
              <>
                <dt>Resistance overhead</dt>
                <dd>{pretty(ac.resistance_overhead_risk)}</dd>
              </>
            ) : null}
          </dl>
        </div>

        <div className="lpa-c2-bx-ph4-section">
          <div className="lpa-c2-bx-ph4-section-label">Zones</div>
          <dl className="lpa-c2-bx-ph4-dl">
            {brokenR && brokenR.level_price != null ? (
              <>
                <dt>Broken R{'\u2192'}S</dt>
                <dd>
                  {fmtPriceLite(brokenR.level_price)}
                  {brokenR.confidence != null ? (
                    <span className="lpa-c2-bx-ph4-conf"> · conf {Number(brokenR.confidence).toFixed(2)}</span>
                  ) : null}
                </dd>
              </>
            ) : null}
            {nearestSupport != null ? (
              <>
                <dt>Nearest support</dt>
                <dd>{fmtPriceLite(nearestSupport)}</dd>
              </>
            ) : null}
            {nearestResistance != null ? (
              <>
                <dt>Nearest resistance</dt>
                <dd>{fmtPriceLite(nearestResistance)}</dd>
              </>
            ) : null}
          </dl>
        </div>

        <div className="lpa-c2-bx-ph4-section">
          <div className="lpa-c2-bx-ph4-section-label">What changes the verdict</div>
          <dl className="lpa-c2-bx-ph4-dl">
            {latest.why_not_opposite ? (
              <>
                <dt>Why not opposite</dt>
                <dd>{latest.why_not_opposite}</dd>
              </>
            ) : null}
            {latest.why_not_no_trade ? (
              <>
                <dt>Why not no-trade</dt>
                <dd>{latest.why_not_no_trade}</dd>
              </>
            ) : null}
            {latest.risk_treatment ? (
              <>
                <dt>Risk treatment</dt>
                <dd>{latest.risk_treatment}</dd>
              </>
            ) : null}
          </dl>
        </div>
      </div>

      {showBreadcrumb ? (
        <div className="lpa-c2-bx-ph4-breadcrumb" title="Chair output that produced this published proposal at proposal time">
          <span className="lpa-c2-bx-ph4-breadcrumb-label">Originally proposed</span>
          <span>
            {PH4_ACTION_LABEL[phase4Chair.final_action] || pretty(phase4Chair.final_action)}
            {phase4Chair.as_of_date ? ` · ${fmtPh4Date(phase4Chair.as_of_date)}` : ''}
          </span>
        </div>
      ) : null}
    </div>
  )
}
