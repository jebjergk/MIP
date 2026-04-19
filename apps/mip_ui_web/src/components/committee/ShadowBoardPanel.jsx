/**
 * ShadowBoardPanel — Shadow Board Phase 1 display component.
 *
 * Renders the full shadow board session (right-hand panel in the dual-board layout).
 * Display-only: no commit, no trade execution, no real board data shown.
 *
 * Props:
 *   shadowPayload  — result from GET /committee/hearing/{id}/shadow-board (or null)
 *   shadowLoading  — boolean
 *   shadowError    — string | null
 *   onRun          — function to trigger POST .../shadow-board/run
 *   runLoading     — boolean (run in progress)
 */
import './ShadowBoardPanel.css'

const STANCE_ORDER = ['DENY', 'DEFER', 'WAIT_RECLAIM', 'APPROVE_REDUCED', 'APPROVE']

function stanceClass(stance) {
  if (!stance) return 'sbp-stance--unknown'
  return `sbp-stance--${stance.toLowerCase().replace(/_/g, '-')}`
}

function ShadowStanceBadge({ stance, size = 'md' }) {
  if (!stance) return <span className="sbp-stance sbp-stance--unknown sbp-stance--sm">—</span>
  const s = String(stance).toUpperCase()
  return (
    <span className={`sbp-stance ${stanceClass(s)} sbp-stance--${size}`}>
      {s.replace(/_/g, ' ')}
    </span>
  )
}

function SeverityPip({ severity }) {
  const cls = severity === 'CRITICAL' ? 'sbp-pip--critical'
    : severity === 'MAJOR' ? 'sbp-pip--major'
    : 'sbp-pip--minor'
  return <span className={`sbp-pip ${cls}`} title={severity} />
}

function SpecialistCard({ pos }) {
  const role = pos?.role || '—'
  const stance = pos?.stance || '—'
  const conf = pos?.confidence != null ? Number(pos.confidence).toFixed(2) : '—'
  const isDegraded = Boolean(pos?.degraded)

  return (
    <div className={`sbp-specialist-card ${isDegraded ? 'sbp-specialist-card--degraded' : ''}`}>
      <div className="sbp-specialist-header">
        <span className="sbp-specialist-role">{role.replace(/_/g, ' ')}</span>
        <ShadowStanceBadge stance={stance} size="sm" />
        <span className="sbp-specialist-conf">conf {conf}</span>
      </div>
      {isDegraded ? (
        <p className="sbp-degraded-note">{pos?.degraded_reason || 'Agent did not respond'}</p>
      ) : (
        <p className="sbp-specialist-rationale">{pos?.rationale || ''}</p>
      )}
    </div>
  )
}

function ConflictStrip({ conflicts }) {
  if (!conflicts?.length) return null
  return (
    <div className="sbp-conflict-strip">
      <h4 className="sbp-section-label">Conflicts ({conflicts.length})</h4>
      {conflicts.map((c, i) => (
        <div key={i} className="sbp-conflict-row">
          <SeverityPip severity={c.severity} />
          <span className="sbp-conflict-roles">
            {c.role_a?.replace(/_/g, ' ')} [{c.stance_a}] vs {c.role_b?.replace(/_/g, ' ')} [{c.stance_b}]
          </span>
          <span className="sbp-conflict-severity">{c.severity}</span>
        </div>
      ))}
    </div>
  )
}

function ChallengeRevisionStrip({ challenge, revisions }) {
  if (!challenge && (!revisions || !revisions.length)) return null
  return (
    <div className="sbp-challenge-strip">
      {challenge && (
        <div className="sbp-challenge-block">
          <h4 className="sbp-section-label">
            Challenge — {challenge.challenger_role?.replace(/_/g, ' ')} → {challenge.target_role?.replace(/_/g, ' ')}
          </h4>
          {challenge.degraded ? (
            <p className="sbp-degraded-note">Challenge agent failed</p>
          ) : (
            <p className="sbp-challenge-text">{challenge.challenge_text}</p>
          )}
        </div>
      )}
      {revisions?.length > 0 && revisions.map((rev, i) => (
        <div key={i} className="sbp-revision-block">
          <h4 className="sbp-section-label">
            Revision — {rev.role?.replace(/_/g, ' ')}
            {rev.stance_changed && (
              <span className="sbp-revision-change">
                {rev.original_stance} → {rev.revised_stance}
              </span>
            )}
          </h4>
          {rev.degraded ? (
            <p className="sbp-degraded-note">Revision agent failed</p>
          ) : (
            <p className="sbp-revision-note">{rev.revision_note}</p>
          )}
        </div>
      ))}
    </div>
  )
}

function ChairCard({ chair, shadowStance, shadowConfidence }) {
  if (!chair) return (
    <div className="sbp-chair-card sbp-chair-card--missing">
      <p className="sbp-muted">Shadow chair ruling not available.</p>
    </div>
  )

  const isDegraded = Boolean(chair.degraded)
  const trade = chair.shadow_trade || {}
  const supports = Array.isArray(chair.top_supports) ? chair.top_supports : []
  const tensions = Array.isArray(chair.top_tensions) ? chair.top_tensions : []

  return (
    <div className={`sbp-chair-card ${isDegraded ? 'sbp-chair-card--degraded' : ''}`}>
      <div className="sbp-chair-header">
        <span className="sbp-chair-label">Shadow Chair Ruling</span>
        <ShadowStanceBadge stance={shadowStance} size="lg" />
        <span className="sbp-chair-conf">conf {shadowConfidence != null ? Number(shadowConfidence).toFixed(2) : '—'}</span>
      </div>

      {chair.plurality_basis && (
        <p className="sbp-chair-basis">{chair.plurality_basis}</p>
      )}

      {chair.conflict_resolution && (
        <div className="sbp-chair-resolution">
          <span className="sbp-section-label">Conflict resolution</span>
          <p>{chair.conflict_resolution}</p>
        </div>
      )}

      {(supports.length > 0 || tensions.length > 0) && (
        <div className="sbp-chair-supports-tensions">
          {supports.length > 0 && (
            <div className="sbp-supports">
              <span className="sbp-section-label sbp-section-label--green">Supports</span>
              <ul>{supports.map((s, i) => <li key={i}>{s}</li>)}</ul>
            </div>
          )}
          {tensions.length > 0 && (
            <div className="sbp-tensions">
              <span className="sbp-section-label sbp-section-label--amber">Tensions</span>
              <ul>{tensions.map((t, i) => <li key={i}>{t}</li>)}</ul>
            </div>
          )}
        </div>
      )}

      {trade.entry_zone && (
        <div className="sbp-shadow-trade">
          <span className="sbp-section-label">Shadow trade construction (advisory only)</span>
          <dl className="sbp-trade-dl">
            <dt>Entry zone</dt><dd>{trade.entry_zone}</dd>
            <dt>Size posture</dt><dd>{trade.size_posture}</dd>
            <dt>Trail posture</dt><dd>{trade.trail_posture}</dd>
            {trade.key_condition && <><dt>Condition</dt><dd>{trade.key_condition}</dd></>}
          </dl>
          <p className="sbp-advisory-note">Advisory only. No execution. Zero live authority.</p>
        </div>
      )}

      {isDegraded && (
        <p className="sbp-degraded-note">Chair agent degraded: {chair.degraded_reason}</p>
      )}
    </div>
  )
}

function ShadowBoardSkeleton() {
  return (
    <div className="sbp-skeleton">
      <div className="sbp-skeleton-header" />
      <div className="sbp-skeleton-row" />
      <div className="sbp-skeleton-row sbp-skeleton-row--short" />
      <div className="sbp-skeleton-grid">
        {[1, 2, 3, 4, 5, 6].map(i => (
          <div key={i} className="sbp-skeleton-card" />
        ))}
      </div>
      <div className="sbp-skeleton-row" />
    </div>
  )
}

export default function ShadowBoardPanel({ shadowPayload, shadowLoading, shadowError, onRun, runLoading }) {
  const positions = Array.isArray(shadowPayload?.positions) ? shadowPayload.positions : []
  const conflicts = Array.isArray(shadowPayload?.conflicts) ? shadowPayload.conflicts : []
  const challenge = shadowPayload?.challenge || null
  const revisions = Array.isArray(shadowPayload?.revisions) ? shadowPayload.revisions : []
  const chair = shadowPayload?.chair || null
  const status = shadowPayload?.status
  const isDegraded = Boolean(shadowPayload?.degraded)

  return (
    <aside className="sbp-root">
      <div className="sbp-header">
        <div className="sbp-header-title">
          <span className="sbp-label-chip">SHADOW BOARD</span>
          <span className="sbp-header-subtitle">Independent · Advisory Only · Zero Authority</span>
        </div>
        {shadowPayload && (
          <div className="sbp-header-meta">
            {isDegraded && <span className="sbp-degraded-chip">DEGRADED</span>}
            <span className="sbp-status-chip sbp-status-chip--{(status || 'unknown').toLowerCase()}">{status || '—'}</span>
            {shadowPayload.run_ms != null && (
              <span className="sbp-muted">{(shadowPayload.run_ms / 1000).toFixed(1)}s</span>
            )}
          </div>
        )}
        <button
          type="button"
          className="sbp-run-btn"
          onClick={onRun}
          disabled={runLoading || shadowLoading}
          title="Re-run shadow board session"
        >
          {runLoading ? 'Running…' : shadowPayload ? 'Re-run' : 'Run shadow board'}
        </button>
      </div>

      {(shadowLoading || runLoading) && !shadowPayload && <ShadowBoardSkeleton />}

      {shadowError && !shadowPayload && (
        <div className="sbp-error">
          <p className="sbp-error-msg">{shadowError}</p>
          <p className="sbp-muted">Shadow board unavailable. Real board is unaffected.</p>
        </div>
      )}

      {shadowPayload && (
        <div className="sbp-body">
          {/* Final stance summary */}
          <div className="sbp-verdict-strip">
            <ShadowStanceBadge stance={shadowPayload.shadow_stance} size="lg" />
            <span className="sbp-verdict-conf">
              Confidence {shadowPayload.shadow_confidence != null
                ? Number(shadowPayload.shadow_confidence).toFixed(2)
                : '—'}
            </span>
            <span className="sbp-verdict-session">Session {String(shadowPayload.session_id || '').slice(0, 8)}</span>
          </div>

          {isDegraded && shadowPayload.degraded_reason && (
            <div className="sbp-degraded-banner">
              <strong>Degraded run:</strong> {shadowPayload.degraded_reason}
            </div>
          )}

          {/* Specialist grid */}
          <section className="sbp-section">
            <h3 className="sbp-section-title">Specialist positions ({positions.length})</h3>
            <div className="sbp-specialist-grid">
              {positions.map((pos, i) => (
                <SpecialistCard key={pos?.role || i} pos={pos} />
              ))}
            </div>
          </section>

          {/* Conflicts */}
          <ConflictStrip conflicts={conflicts} />

          {/* Challenge + revision */}
          <ChallengeRevisionStrip challenge={challenge} revisions={revisions} />

          {/* Chair ruling */}
          <section className="sbp-section">
            <h3 className="sbp-section-title">Chair ruling</h3>
            <ChairCard
              chair={chair}
              shadowStance={shadowPayload.shadow_stance}
              shadowConfidence={shadowPayload.shadow_confidence}
            />
          </section>

          <footer className="sbp-disclaimer">
            Shadow Board — no trade authority. Never interacts with COMMITTEE_FINAL_DECISION.
            Model: claude-4-sonnet via Snowflake Cortex Agents.
          </footer>
        </div>
      )}

      {!shadowPayload && !shadowLoading && !runLoading && !shadowError && (
        <div className="sbp-empty">
          <p className="sbp-muted">No shadow session yet. Click <strong>Run shadow board</strong> to start.</p>
        </div>
      )}
    </aside>
  )
}
