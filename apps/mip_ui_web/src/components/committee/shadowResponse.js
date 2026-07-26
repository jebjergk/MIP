/**
 * Shared normalizer for the GET /committee/hearing/{hearing_id}/shadow-board
 * response. Used by:
 *   - the LPA Shadow Chair Verdict headline (bounded poll in LivePortfolioActivity.jsx)
 *   - the proof-exhibits dual-board panel (LpaCommittee2Exhibits.jsx)
 *
 * The endpoint returns a payload assembled by fetch_shadow_session() in
 * MIP/apps/mip_ui_api/app/committee/shadow_board.py. Two read paths exist:
 *   - session-level fields (set by the orchestrator at seal time)
 *   - chair-level fallback (set when SHADOW_CHAIR_RULING lands but the
 *     session row hasn't been updated yet)
 * The normalizer collapses both into a single, stable shape so the
 * headline never has to branch on whichever path happens to be populated.
 *
 * Stage 2 contract: this is read-only. Nothing here can affect submit
 * gating (REVALIDATED_PASS), materialization, or the deterministic
 * baseline.
 */

const TERMINAL_STATUSES = new Set(['COMPLETE', 'DEGRADED', 'FAILED'])
const KNOWN_STATUSES = new Set(['COMPLETE', 'RUNNING', 'DEGRADED', 'FAILED', 'TIMEOUT', 'UNAVAILABLE'])

function emptyNormalized(raw) {
  return {
    status: 'UNAVAILABLE',
    stance: null,
    confidence: null,
    thesisHealth: null,
    primaryReasonCode: null,
    whyNotOpposite: null,
    degraded: false,
    degradedReason: null,
    sessionId: null,
    hearingId: null,
    chair: null,
    literatureSupport: { enabled: false, status: 'DISABLED' },
    methodologistEffect: { used: false, effect: 'NO_MATERIAL_EFFECT', summary: '' },
    raw: raw ?? null,
  }
}

function coerceConfidence(v) {
  if (v == null) return null
  if (typeof v === 'number') return Number.isFinite(v) ? v : null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

/**
 * Normalize a /shadow-board response (or progress-only payload) into the
 * shape the LPA headline state expects.
 *
 * Accepts:
 *  - null / undefined / non-object   → UNAVAILABLE sentinel
 *  - full payload from fetch_shadow_session()
 *  - lightweight payload from fetch_shadow_progress() (?include_progress=1)
 */
export function normalizeShadowBoardResponse(json) {
  if (!json || typeof json !== 'object') return emptyNormalized(json)

  const statusRaw = String(json.status ?? '').toUpperCase()
  const status = KNOWN_STATUSES.has(statusRaw) ? statusRaw : (statusRaw || 'UNAVAILABLE')
  const isRunning = status === 'RUNNING'

  const chair = json.chair && typeof json.chair === 'object' ? json.chair : null
  const literatureSupport = json.literature_support && typeof json.literature_support === 'object'
    ? json.literature_support
    : { enabled: false, status: 'DISABLED' }
  const methodologistEffect = chair?.methodologist_effect
    ?? literatureSupport?.methodologist_effect
    ?? { used: false, effect: 'NO_MATERIAL_EFFECT', summary: '' }

  const stanceRaw = isRunning
    ? (json.shadow_stance ?? null)
    : (json.shadow_stance ?? chair?.shadow_stance ?? null)
  const stance = stanceRaw && typeof stanceRaw === 'string' ? stanceRaw.toUpperCase() : null

  const confidence = isRunning
    ? coerceConfidence(json.shadow_confidence)
    : coerceConfidence(json.shadow_confidence ?? chair?.shadow_confidence)

  // Optional Phase 4-style chair fields. Not in today's shadow chair JSON
  // schema but exposed in the normalizer so any future enrichment of
  // SHADOW_CHAIR_RULING flows straight through without callers re-wiring.
  const thesisHealth = chair?.thesis_health ?? json.thesis_health ?? null
  const primaryReasonCode = chair?.primary_reason_code ?? json.primary_reason_code ?? null
  const whyNotOpposite = chair?.why_not_opposite ?? json.why_not_opposite ?? chair?.plurality_basis ?? null

  const degraded = Boolean(json.degraded ?? chair?.degraded ?? false)
  const degradedReason = json.degraded_reason ?? null

  return {
    status,
    stance,
    confidence,
    thesisHealth,
    primaryReasonCode,
    whyNotOpposite,
    degraded,
    degradedReason,
    sessionId: json.session_id ?? null,
    hearingId: json.hearing_id ?? null,
    chair: isRunning ? null : chair,
    literatureSupport,
    methodologistEffect,
    raw: json,
  }
}

export function isShadowStatusTerminal(status) {
  return TERMINAL_STATUSES.has(String(status || '').toUpperCase())
}
