/**
 * BoardExplanationPanel
 *
 * Collapsed-by-default disclosure rendered under the LPA Committee 2
 * exhibits header. When opened, it shows the full agentic-board audit
 * trail for the current proposal:
 *
 *   - the four specialist verdicts (verdict, primary/secondary reason
 *     code, confidence, mode = cortex vs deterministic_fallback),
 *   - a chair disagreement summary (support / concern / reject counts,
 *     avg confidence, warning flags) — pulled from the chair's
 *     COMPARATIVE_REASONING_JSON so the math matches what the chair
 *     itself synthesized,
 *   - the chair's own structured rationale (final_rationale +
 *     why_selected_or_rejected from Sprint 2's templated chair),
 *   - the BOARD_RUN_ID + model_config / prompt_version /
 *     policy_version so an operator can trace this row to a specific
 *     board run.
 *
 * Strictly read-only — no overrides, no write paths, no operator-
 * facing actions in this sprint. The API contract is `available:
 * bool`; when `available=false`, we render an inline notice instead
 * of the panel content (not an error toast).
 */
import { useEffect, useState } from 'react'
import { API_BASE } from '../../config/apiBase'
import Phase4ChairSection from './Phase4ChairSection'

function fmtConfidence(v) {
  if (v == null) return '\u2014'
  const n = Number(v)
  if (!Number.isFinite(n)) return '\u2014'
  return n.toFixed(2)
}

function pretty(s) {
  if (s == null) return '\u2014'
  return String(s).replace(/_/g, ' ').toLowerCase()
}

function verdictTone(v) {
  switch (String(v || '').toLowerCase()) {
    case 'approve':
      return 'ok'
    case 'weak':
    case 'mixed':
    case 'watch':
    case 'constrained':
      return 'warn'
    case 'reject':
      return 'critical'
    default:
      return 'neutral'
  }
}

function chairVerdictTone(v) {
  switch (String(v || '').toUpperCase()) {
    case 'APPROVE':
      return 'ok'
    case 'APPROVE_REDUCED':
      return 'info'
    case 'WATCH':
    case 'DEFER':
      return 'warn'
    case 'REJECT':
    case 'BLOCKED':
      return 'critical'
    default:
      return 'neutral'
  }
}

function modeBadge(mode) {
  if (!mode) return null
  const m = String(mode).toLowerCase()
  if (m === 'cortex') return { label: 'Cortex', tone: 'cortex' }
  if (m === 'deterministic_fallback') return { label: 'Fallback', tone: 'fallback' }
  return { label: m, tone: 'neutral' }
}

const AGENT_LABELS = {
  STRUCTURE_AGENT: 'Structure',
  OPPORTUNITY_QUALITY_AGENT: 'Opportunity quality',
  HISTORICAL_EVIDENCE_AGENT: 'Historical evidence',
  RISK_EXECUTION_FEASIBILITY_AGENT: 'Risk / execution',
}

function agentLabel(name) {
  if (!name) return '\u2014'
  if (AGENT_LABELS[name]) return AGENT_LABELS[name]
  return name
    .replace(/_AGENT$/i, '')
    .replace(/_/g, ' ')
    .toLowerCase()
    .replace(/^./, (c) => c.toUpperCase())
}

function SpecialistRow({ specialist }) {
  const conf = fmtConfidence(specialist.confidence)
  const verdict = specialist.verdict
  const tone = verdictTone(verdict)
  const mode = modeBadge(specialist.mode)
  const flags = Array.isArray(specialist.concern_flags) ? specialist.concern_flags : []
  return (
    <div className="lpa-c2-bx-spec-row">
      <div className="lpa-c2-bx-spec-head">
        <span className="lpa-c2-bx-spec-name">{agentLabel(specialist.agent_name)}</span>
        <span className={`lpa-c2-bx-verdict lpa-c2-bx-verdict--${tone}`}>{pretty(verdict)}</span>
        <span className="lpa-c2-bx-conf" title="Specialist self-rated confidence">
          conf {conf}
        </span>
        {mode ? (
          <span
            className={`lpa-c2-bx-mode lpa-c2-bx-mode--${mode.tone}`}
            title={
              mode.tone === 'fallback'
                ? 'Cortex output failed validation — deterministic specialist served as the fallback safety net.'
                : mode.tone === 'cortex'
                  ? 'Cortex-backed specialist (snowflake.cortex.complete).'
                  : ''
            }
          >
            {mode.label}
          </span>
        ) : null}
      </div>
      <div className="lpa-c2-bx-spec-codes">
        <span className="lpa-c2-bx-code-pri" title="Primary reason code">
          {specialist.primary_reason_code || '\u2014'}
        </span>
        {specialist.secondary_reason_code ? (
          <span className="lpa-c2-bx-code-sec" title="Secondary reason code">
            {specialist.secondary_reason_code}
          </span>
        ) : null}
        {flags.length > 0 ? (
          <span className="lpa-c2-bx-flags" title="Concern flags raised by this specialist">
            {flags.slice(0, 4).join(' \u00b7 ')}
            {flags.length > 4 ? ` \u00b7 +${flags.length - 4}` : ''}
          </span>
        ) : null}
      </div>
      {specialist.rationale_text ? (
        <p className="lpa-c2-bx-spec-rat" title={specialist.rationale_text}>
          {specialist.rationale_text}
        </p>
      ) : null}
    </div>
  )
}

function DisagreementSummary({ disagreement }) {
  if (!disagreement) return null
  const { support_count, concern_count, reject_count, avg_confidence, warning_flags } = disagreement
  const flags = Array.isArray(warning_flags) ? warning_flags : []
  const showAny =
    support_count != null ||
    concern_count != null ||
    reject_count != null ||
    avg_confidence != null ||
    flags.length > 0
  if (!showAny) return null
  return (
    <div className="lpa-c2-bx-disagree">
      <div className="lpa-c2-bx-disagree-counts">
        <span className="lpa-c2-bx-disagree-cell lpa-c2-bx-disagree-cell--ok">
          <strong>{support_count ?? '\u2014'}</strong>
          <span>support</span>
        </span>
        <span className="lpa-c2-bx-disagree-cell lpa-c2-bx-disagree-cell--warn">
          <strong>{concern_count ?? '\u2014'}</strong>
          <span>concern</span>
        </span>
        <span className="lpa-c2-bx-disagree-cell lpa-c2-bx-disagree-cell--critical">
          <strong>{reject_count ?? '\u2014'}</strong>
          <span>reject</span>
        </span>
        <span className="lpa-c2-bx-disagree-cell">
          <strong>{fmtConfidence(avg_confidence)}</strong>
          <span>avg conf</span>
        </span>
      </div>
      {flags.length > 0 ? (
        <div className="lpa-c2-bx-disagree-flags" title="Warning flags surfaced into chair view">
          <span className="lpa-c2-bx-disagree-flags-label">Warnings</span>
          <span>{flags.join(' \u00b7 ')}</span>
        </div>
      ) : null}
    </div>
  )
}

function ChairBlock({ chair }) {
  if (!chair) return null
  const tone = chairVerdictTone(chair.final_verdict)
  const cmp = chair.comparative_reasoning && typeof chair.comparative_reasoning === 'object'
    ? chair.comparative_reasoning
    : {}
  const tplVersion = cmp.chair_template_version
  const modeLine = cmp.mode_line
  return (
    <div className="lpa-c2-bx-chair">
      <div className="lpa-c2-bx-chair-head">
        <span className="lpa-c2-bx-card-subhead">Chair synthesis</span>
        <span className={`lpa-c2-bx-verdict lpa-c2-bx-verdict--${tone}`}>{pretty(chair.final_verdict)}</span>
        {chair.primary_reason_code ? (
          <span className="lpa-c2-bx-code-pri">{chair.primary_reason_code}</span>
        ) : null}
        {chair.secondary_reason_code ? (
          <span className="lpa-c2-bx-code-sec">{chair.secondary_reason_code}</span>
        ) : null}
      </div>
      {chair.why_selected_or_rejected ? (
        <p className="lpa-c2-bx-chair-why">
          <span className="lpa-c2-bx-chair-why-label">Why this verdict</span>
          {chair.why_selected_or_rejected}
        </p>
      ) : null}
      {chair.final_rationale ? (
        <p className="lpa-c2-bx-chair-rat">{chair.final_rationale}</p>
      ) : null}
      {tplVersion || modeLine ? (
        <div className="lpa-c2-bx-chair-foot" title="Chair template lineage">
          {modeLine ? <span>{modeLine}</span> : null}
          {tplVersion ? <span className="lpa-c2-bx-runmeta-chip">{tplVersion}</span> : null}
        </div>
      ) : null}
    </div>
  )
}

function RunMeta({ run, boardRunId }) {
  const cfg = run?.model_config && typeof run.model_config === 'object' ? run.model_config : null
  const mode = cfg?.mode
  // Sprint 1 / Sprint 2 use `specialist_model`; older code paths use `model`.
  const model = cfg?.specialist_model || cfg?.model
  const chairMode = cfg?.chair_mode
  const promptV = run?.prompt_version
  const policyV = run?.policy_version
  return (
    <div className="lpa-c2-bx-runmeta" title="Board run lineage for this proposal">
      <span className="lpa-c2-bx-runmeta-label">Run</span>
      <span className="lpa-c2-bx-runmeta-id" title={boardRunId || ''}>
        {boardRunId ? `${String(boardRunId).slice(0, 8)}\u2026` : '\u2014'}
      </span>
      {mode ? <span className="lpa-c2-bx-runmeta-chip">{mode}</span> : null}
      {model ? <span className="lpa-c2-bx-runmeta-chip">{model}</span> : null}
      {chairMode ? <span className="lpa-c2-bx-runmeta-chip">chair {chairMode}</span> : null}
      {promptV ? <span className="lpa-c2-bx-runmeta-chip">prompt {promptV}</span> : null}
      {policyV ? <span className="lpa-c2-bx-runmeta-chip">policy {policyV}</span> : null}
    </div>
  )
}

export default function BoardExplanationPanel({ proposalId }) {
  const [open, setOpen] = useState(false)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [hasFetched, setHasFetched] = useState(false)

  useEffect(() => {
    setData(null)
    setError(null)
    setHasFetched(false)
  }, [proposalId])

  // Fetch immediately on mount / proposal change — Phase 4 agentic read
  // must be visible without expanding the legacy Phase 3 audit trail.
  useEffect(() => {
    if (proposalId == null || hasFetched) return undefined
    let cancelled = false
    const run = async () => {
      setLoading(true)
      setError(null)
      try {
        const r = await fetch(
          `${API_BASE}/committee/proposal/${encodeURIComponent(proposalId)}/board-explanation`,
        )
        if (cancelled) return
        if (!r.ok) {
          setError(`Server returned ${r.status}.`)
          setHasFetched(true)
          return
        }
        const j = await r.json()
        if (cancelled) return
        setData(j)
        setHasFetched(true)
      } catch (e) {
        if (cancelled) return
        setError(String(e?.message || e))
        setHasFetched(true)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    run()
    return () => {
      cancelled = true
    }
  }, [proposalId, hasFetched])

  if (proposalId == null) return null

  const available = data?.available === true
  const note = data?.note
  const specialists = Array.isArray(data?.specialists) ? data.specialists : []
  const phase4Chair = data?.phase4_chair || null
  const phase4LatestHealth = data?.phase4_latest_health || null
  const hasPhase4 = Boolean(phase4Chair || phase4LatestHealth)
  const hasSpecialists = specialists.length > 0
  const hasChairBlock = data?.chair && (
    data.chair.final_verdict || data.chair.final_rationale || data.chair.why_selected_or_rejected
  )

  return (
    <section className="lpa-c2-card lpa-c2-bx-card">
      <div className="lpa-c2-bx-ph4-masthead">
        {loading ? (
          <p className="lpa-c2-muted">Loading agentic board read…</p>
        ) : error ? (
          <p className="lpa-c2-muted">Could not load board data: {error}</p>
        ) : !available ? (
          <p className="lpa-c2-muted">{note || 'Board explanation is not available for this proposal.'}</p>
        ) : (
          <>
            <div className="lpa-c2-bx-contract" role="status">
              {(data?.proposal_lifecycle_status || data?.status) ? (
                <span
                  className="lpa-c2-bx-contract-pill lpa-c2-bx-contract-pill--life"
                  title="Proposal row status in STRUCTURAL_TRADE_PROPOSALS"
                >
                  Lifecycle: {pretty(data.proposal_lifecycle_status || data.status)}
                </span>
              ) : null}
              {data?.operational_state ? (
                <span
                  className="lpa-c2-bx-contract-pill lpa-c2-bx-contract-pill--op"
                  title={
                    'API-derived operator state from Phase 4 + execution linkage. ' +
                    'Live invalidation is applied on the cockpit trade-proposals path; this panel does not synthesize it.'
                  }
                >
                  Operator: {pretty(data.operational_state)}
                </span>
              ) : null}
            </div>
            {hasPhase4 ? (
              <Phase4ChairSection
                phase4Chair={phase4Chair}
                phase4LatestHealth={phase4LatestHealth}
              />
            ) : (
              <p className="lpa-c2-muted">
                No Phase 4 chair payload returned for this proposal (check board lineage / STOCK-only guard).
              </p>
            )}
          </>
        )}
      </div>

      <button
        type="button"
        className={`lpa-c2-bx-toggle ${open ? 'is-open' : ''}`}
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        aria-controls={`lpa-c2-bx-body-${proposalId}`}
      >
        <span className="lpa-c2-bx-toggle-caret" aria-hidden>
          {open ? '\u25BE' : '\u25B8'}
        </span>
        <span className="lpa-c2-bx-toggle-label">Board audit trail</span>
        <span className="lpa-c2-bx-toggle-sub">
          Phase 3 specialists · disagreement · templated chair
        </span>
      </button>
      {open ? (
        <div id={`lpa-c2-bx-body-${proposalId}`} className="lpa-c2-bx-body">
          {loading ? (
            <p className="lpa-c2-muted">Loading…</p>
          ) : error ? (
            <p className="lpa-c2-muted">Could not load: {error}</p>
          ) : !available ? (
            <p className="lpa-c2-muted">{note || 'Not available.'}</p>
          ) : (
            <>
              <RunMeta run={data.run} boardRunId={data.board_run_id} />
              {hasSpecialists ? (
                <>
                  <div className="lpa-c2-divider" />
                  <div className="lpa-c2-bx-card-subhead">Specialists</div>
                  <div className="lpa-c2-bx-spec-list">
                    {specialists.map((s) => (
                      <SpecialistRow key={s.agent_name || Math.random()} specialist={s} />
                    ))}
                  </div>
                  <div className="lpa-c2-divider" />
                  <div className="lpa-c2-bx-card-subhead">Disagreement summary</div>
                  <DisagreementSummary disagreement={data.disagreement} />
                </>
              ) : (
                <p className="lpa-c2-muted">No Phase 3 specialist outcomes (agentic Phase 4 proposal).</p>
              )}
              {hasChairBlock ? (
                <>
                  <div className="lpa-c2-divider" />
                  <ChairBlock chair={data.chair} />
                </>
              ) : null}
            </>
          )}
        </div>
      ) : null}
    </section>
  )
}
