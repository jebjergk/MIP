/**
 * ShadowBoardPanel — Agentic Committee boardroom
 *
 * A live, conversational deliberation view that animates the six specialist
 * agents arguing through a frozen evidence pack and the chair's closing
 * verdict. This is the AUTHORITATIVE review surface as of Stage 4 — the
 * operator commits its verdict via the Agentic Authority endpoint, and
 * Submit eligibility flows directly from that commit. The component name
 * (`ShadowBoardPanel`) and underlying `/shadow-board` payload route are
 * kept for now to avoid a backend-wide rename; user-facing strings have
 * been migrated to the "Agentic Committee" name.
 *
 * Props (unchanged from legacy panel for drop-in compatibility):
 *   shadowPayload, shadowLoading, shadowError, onRun, runLoading,
 *   evidenceHash, snapshotId, runningProgress, showManualRun
 */
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import './ShadowBoardPanel.css'

// ---------------------------------------------------------------------------
// Persona system
// ---------------------------------------------------------------------------
// Each role gets a personified identity so the deliberation reads like a
// boardroom transcript rather than a dashboard.

const PERSONAS = {
  ENTRY_GEOMETRY: {
    name: 'The Cartographer',
    abbr: 'EG',
    role: 'Entry Geometry',
    tone: 'maps the zone',
    accent: '#a78bfa',
    accentSoft: 'rgba(167, 139, 250, 0.16)',
  },
  PATH_TRADEABILITY: {
    name: 'The Trail Reader',
    abbr: 'PT',
    role: 'Path Tradeability',
    tone: 'walks the trail',
    accent: '#34d399',
    accentSoft: 'rgba(52, 211, 153, 0.16)',
  },
  PROTECTION_EXIT: {
    name: 'The Sentinel',
    abbr: 'PE',
    role: 'Protection / Exit',
    tone: 'guards the edge',
    accent: '#60a5fa',
    accentSoft: 'rgba(96, 165, 250, 0.16)',
  },
  REGIME: {
    name: 'The Meteorologist',
    abbr: 'RG',
    role: 'Regime',
    tone: 'reads the weather',
    accent: '#22d3ee',
    accentSoft: 'rgba(34, 211, 238, 0.16)',
  },
  STRUCTURAL_THESIS: {
    name: 'The Architect',
    abbr: 'ST',
    role: 'Structural Thesis',
    tone: 'inspects the frame',
    accent: '#fbbf24',
    accentSoft: 'rgba(251, 191, 36, 0.16)',
  },
  SYMBOL_BEHAVIOR: {
    name: 'The Behaviorist',
    abbr: 'SB',
    role: 'Symbol Behavior',
    tone: 'studies the pattern',
    accent: '#f472b6',
    accentSoft: 'rgba(244, 114, 182, 0.16)',
  },
}

const CHAIR_PERSONA = {
  name: 'The Conductor',
  abbr: '✦',
  role: 'Chair',
  tone: 'synthesizes the verdict',
  accent: '#e5c07b',
  accentSoft: 'rgba(229, 192, 123, 0.18)',
}

const FALLBACK_PERSONA = {
  name: 'Specialist',
  abbr: '?',
  role: 'Unknown role',
  tone: 'considers the evidence',
  accent: '#94a3b8',
  accentSoft: 'rgba(148, 163, 184, 0.16)',
}

function personaFor(role) {
  if (!role) return FALLBACK_PERSONA
  return PERSONAS[String(role).toUpperCase()] || FALLBACK_PERSONA
}

// ---------------------------------------------------------------------------
// Stance system
// ---------------------------------------------------------------------------

const STANCE_INDEX = {
  DENY: 0,
  DEFER: 1,
  WAIT_RECLAIM: 2,
  APPROVE_REDUCED: 3,
  APPROVE: 4,
}

const STANCE_LABEL = {
  DENY: 'DENY',
  DEFER: 'DEFER',
  WAIT_RECLAIM: 'WAIT RECLAIM',
  APPROVE_REDUCED: 'APPROVE REDUCED',
  APPROVE: 'APPROVE',
}

function stanceClass(stance) {
  if (!stance) return 'sbp-stance--unknown'
  return `sbp-stance--${String(stance).toLowerCase().replace(/_/g, '-')}`
}

function stancePolarityClass(stance) {
  const idx = STANCE_INDEX[String(stance || '').toUpperCase()]
  if (idx === undefined) return 'sbp-pol--neutral'
  if (idx >= 3) return 'sbp-pol--positive'
  if (idx <= 1) return 'sbp-pol--negative'
  return 'sbp-pol--cautious'
}

function ShadowStanceBadge({ stance, size = 'md' }) {
  if (!stance) {
    return <span className="sbp-stance sbp-stance--unknown sbp-stance--sm">—</span>
  }
  const s = String(stance).toUpperCase()
  return (
    <span className={`sbp-stance ${stanceClass(s)} sbp-stance--${size}`}>
      {STANCE_LABEL[s] || s.replace(/_/g, ' ')}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Confidence donut — animated SVG ring
// ---------------------------------------------------------------------------

function ConfidenceDonut({ value, size = 38, strokeWidth = 4, color }) {
  const v = Math.max(0, Math.min(1, Number(value) || 0))
  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const dash = circumference * v
  const accent = color || '#7dd3fc'
  return (
    <span className="sbp-donut" title={`Confidence ${v.toFixed(2)}`}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} className="sbp-donut-svg">
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="rgba(255,255,255,0.08)"
          strokeWidth={strokeWidth}
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={accent}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
          strokeDasharray={`${dash} ${circumference - dash}`}
          transform={`rotate(-90 ${size / 2} ${size / 2})`}
          className="sbp-donut-fg"
          style={{ '--donut-target': dash, '--donut-circ': circumference }}
        />
      </svg>
      <span className="sbp-donut-label">{v.toFixed(2)}</span>
    </span>
  )
}

// ---------------------------------------------------------------------------
// Stance scale bar — horizontal 5-segment indicator
// ---------------------------------------------------------------------------

function StanceScale({ stance, accent }) {
  const idx = STANCE_INDEX[String(stance || '').toUpperCase()]
  if (idx === undefined) return null
  const segments = ['DENY', 'DEFER', 'WAIT_RECLAIM', 'APPROVE_REDUCED', 'APPROVE']
  return (
    <div className="sbp-scale" title={`Stance scale: ${STANCE_LABEL[segments[idx]]}`}>
      {segments.map((s, i) => (
        <span
          key={s}
          className={`sbp-scale-seg ${i === idx ? 'sbp-scale-seg--active' : ''}`}
          style={i === idx ? { background: accent || '#7dd3fc' } : undefined}
        />
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Persona avatar
// ---------------------------------------------------------------------------

function PersonaAvatar({ persona, ringPulse = false, size = 'md' }) {
  return (
    <span
      className={`sbp-avatar sbp-avatar--${size} ${ringPulse ? 'sbp-avatar--pulse' : ''}`}
      style={{
        '--avatar-accent': persona.accent,
        '--avatar-accent-soft': persona.accentSoft,
      }}
      title={`${persona.name} — ${persona.role}`}
    >
      <span className="sbp-avatar-ring" aria-hidden />
      <span className="sbp-avatar-letters">{persona.abbr}</span>
    </span>
  )
}

// ---------------------------------------------------------------------------
// Evidence chip extraction
// ---------------------------------------------------------------------------
// Best-effort parser that pulls the citation-y fragments out of the rationale
// text + evidence_used array so each bubble shows visible "receipts" instead
// of one wall of prose.

const _NUM_PATTERNS = [
  /\b(?:ABF|MFE\/MAE|MHR|LFE|RR|cushion|zone(?:\s+mid)?|invalidation|trail|drawdown)\s*[:=≈~-]?\s*[+-]?\d+(?:\.\d+)?\s*%?/gi,
  /\b\d+(?:\.\d+)?\s*%/g,
  /\b(?:trust|trusted|verified|provisional|untrusted)\b/gi,
]

function extractChips(pos) {
  const chips = []
  const seen = new Set()
  const push = (label) => {
    const norm = String(label).trim().replace(/\s+/g, ' ')
    if (!norm || norm.length > 48) return
    const key = norm.toLowerCase()
    if (seen.has(key)) return
    seen.add(key)
    chips.push(norm)
  }
  // Prefer explicit evidence_used, but cap to keep bubbles tight
  if (Array.isArray(pos?.evidence_used)) {
    for (const ev of pos.evidence_used) {
      if (chips.length >= 5) break
      push(ev)
    }
  }
  // Top up by scanning the rationale for citation-y fragments
  const text = String(pos?.rationale || '')
  for (const pat of _NUM_PATTERNS) {
    if (chips.length >= 5) break
    const matches = text.match(pat) || []
    for (const m of matches) {
      if (chips.length >= 5) break
      push(m)
    }
  }
  return chips.slice(0, 5)
}

function EvidenceChips({ chips, polarity }) {
  if (!chips?.length) return null
  return (
    <div className={`sbp-chips ${polarity}`}>
      {chips.map((chip, i) => (
        <span key={`${chip}-${i}`} className="sbp-chip" title={chip}>
          {chip}
        </span>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Specialist message bubble
// ---------------------------------------------------------------------------

function SpecialistBubble({ pos, index = 0, hasReply = false }) {
  const persona = personaFor(pos?.role)
  const stance = String(pos?.stance || '').toUpperCase()
  const conf = pos?.confidence
  const isDegraded = Boolean(pos?.degraded)
  // Backend pre-seeds every specialist with a placeholder row so all 6
  // avatars appear immediately. Those rows carry degraded_reason
  // "awaiting_specialist" and should render as a live "thinking" bubble
  // (animated dots, no harsh degraded styling), not as a failure.
  const isThinking = isDegraded && String(pos?.degraded_reason || '') === 'awaiting_specialist'
  const polarity = isThinking ? 'sbp-bubble--neutral' : stancePolarityClass(stance)
  const chips = useMemo(() => (isThinking ? [] : extractChips(pos)), [pos, isThinking])

  const classes = [
    'sbp-bubble',
    polarity,
    isThinking ? 'sbp-bubble--thinking' : '',
    !isThinking && isDegraded ? 'sbp-bubble--degraded' : '',
    hasReply ? 'sbp-bubble--has-reply' : '',
  ].filter(Boolean).join(' ')

  return (
    <article
      className={classes}
      style={{ '--bubble-accent': persona.accent, '--bubble-accent-soft': persona.accentSoft, '--bubble-delay': `${index * 90}ms` }}
    >
      <div className="sbp-bubble-gutter">
        <PersonaAvatar persona={persona} />
        {hasReply && <span className="sbp-bubble-thread-line" aria-hidden />}
      </div>

      <div className="sbp-bubble-body">
        <header className="sbp-bubble-header">
          <span className="sbp-bubble-name">{persona.name}</span>
          <span className="sbp-bubble-role">· {persona.role}</span>
          <span className="sbp-bubble-spacer" />
          {isThinking ? (
            <span className="sbp-thinking-chip" aria-label="Specialist deliberating">
              deliberating
              <span className="sbp-thinking-dots" aria-hidden>
                <span /><span /><span />
              </span>
            </span>
          ) : (
            <>
              <ShadowStanceBadge stance={stance} size="sm" />
              <ConfidenceDonut value={conf} color={persona.accent} />
            </>
          )}
        </header>

        {!isThinking && <StanceScale stance={stance} accent={persona.accent} />}

        {isThinking ? (
          <div className="sbp-thinking-skeleton" aria-hidden>
            <span className="sbp-skel-line sbp-skel-line--w-90" />
            <span className="sbp-skel-line sbp-skel-line--w-75" />
            <span className="sbp-skel-line sbp-skel-line--w-60" />
          </div>
        ) : isDegraded ? (
          <p className="sbp-degraded-note">
            <span className="sbp-degraded-glyph" aria-hidden>!</span>
            {pos?.degraded_reason || 'This specialist did not respond — treated as DEFER for the chair.'}
          </p>
        ) : (
          <p className="sbp-bubble-text">{pos?.rationale || <em className="sbp-muted">No rationale recorded.</em>}</p>
        )}

        {!isThinking && <EvidenceChips chips={chips} polarity={polarity} />}
      </div>
    </article>
  )
}

// ---------------------------------------------------------------------------
// Conflict marker — appears between two bubbles
// ---------------------------------------------------------------------------

function ConflictMarker({ conflict }) {
  if (!conflict) return null
  const a = personaFor(conflict.role_a)
  const b = personaFor(conflict.role_b)
  const sev = String(conflict.severity || 'MINOR').toUpperCase()
  return (
    <div
      className={`sbp-conflict-marker sbp-conflict-marker--${sev.toLowerCase()}`}
      title={`${sev} conflict: ${a.name} (${conflict.stance_a}) vs ${b.name} (${conflict.stance_b})`}
    >
      <span className="sbp-conflict-glyph" aria-hidden>✕</span>
      <span className="sbp-conflict-pair">
        <span className="sbp-conflict-side">
          <span className="sbp-conflict-name" style={{ color: a.accent }}>{a.name}</span>
          <ShadowStanceBadge stance={conflict.stance_a} size="sm" />
        </span>
        <span className="sbp-conflict-vs">disagrees with</span>
        <span className="sbp-conflict-side">
          <span className="sbp-conflict-name" style={{ color: b.accent }}>{b.name}</span>
          <ShadowStanceBadge stance={conflict.stance_b} size="sm" />
        </span>
      </span>
      <span className={`sbp-conflict-sev sbp-conflict-sev--${sev.toLowerCase()}`}>{sev}</span>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Challenge bubble — threaded reply from challenger -> target
// ---------------------------------------------------------------------------

function ChallengeBubble({ challenge, index = 0 }) {
  if (!challenge) return null
  const challenger = personaFor(challenge.challenger_role)
  const target = personaFor(challenge.target_role)
  return (
    <article
      className="sbp-bubble sbp-bubble--challenge"
      style={{ '--bubble-accent': challenger.accent, '--bubble-accent-soft': challenger.accentSoft, '--bubble-delay': `${index * 90}ms` }}
    >
      <div className="sbp-bubble-gutter">
        <PersonaAvatar persona={challenger} size="sm" />
      </div>
      <div className="sbp-bubble-body">
        <header className="sbp-bubble-header">
          <span className="sbp-bubble-name">{challenger.name}</span>
          <span className="sbp-bubble-role">challenges</span>
          <span className="sbp-mention" style={{ color: target.accent, borderColor: target.accent }}>
            @{target.abbr}
          </span>
          <span className="sbp-bubble-spacer" />
          <span className="sbp-bubble-tag sbp-bubble-tag--amber">CHALLENGE</span>
        </header>
        {challenge.degraded ? (
          <p className="sbp-degraded-note">
            <span className="sbp-degraded-glyph" aria-hidden>!</span>
            Challenge agent did not respond.
          </p>
        ) : (
          <p className="sbp-bubble-text sbp-bubble-text--quote">{challenge.challenge_text}</p>
        )}
      </div>
    </article>
  )
}

// ---------------------------------------------------------------------------
// Revision bubble — specialist revising under challenge
// ---------------------------------------------------------------------------

function RevisionBubble({ revision, index = 0 }) {
  if (!revision) return null
  const persona = personaFor(revision.role || revision.role_name)
  const moved = Boolean(revision.stance_changed)
  return (
    <article
      className={`sbp-bubble sbp-bubble--revision ${moved ? 'sbp-bubble--revision-moved' : ''}`}
      style={{ '--bubble-accent': persona.accent, '--bubble-accent-soft': persona.accentSoft, '--bubble-delay': `${index * 90}ms` }}
    >
      <div className="sbp-bubble-gutter">
        <PersonaAvatar persona={persona} size="sm" />
      </div>
      <div className="sbp-bubble-body">
        <header className="sbp-bubble-header">
          <span className="sbp-bubble-name">{persona.name}</span>
          <span className="sbp-bubble-role">revises</span>
          <span className="sbp-bubble-spacer" />
          {moved ? (
            <span className="sbp-stance-arrow" title={`${revision.original_stance} → ${revision.revised_stance}`}>
              <ShadowStanceBadge stance={revision.original_stance} size="sm" />
              <span className="sbp-arrow-glyph" aria-hidden>→</span>
              <ShadowStanceBadge stance={revision.revised_stance} size="sm" />
            </span>
          ) : (
            <span className="sbp-bubble-tag sbp-bubble-tag--neutral">STANCE HELD · {STANCE_LABEL[String(revision.revised_stance || '').toUpperCase()] || revision.revised_stance}</span>
          )}
        </header>
        {revision.degraded ? (
          <p className="sbp-degraded-note">
            <span className="sbp-degraded-glyph" aria-hidden>!</span>
            Revision agent did not respond.
          </p>
        ) : (
          <p className="sbp-bubble-text sbp-bubble-text--quote">{revision.revision_note}</p>
        )}
      </div>
    </article>
  )
}

// ---------------------------------------------------------------------------
// Chair finale bubble
// ---------------------------------------------------------------------------

function ChairBubble({ chair, shadowStance, shadowConfidence, totalSpecialists, index = 0 }) {
  const conf = shadowConfidence != null ? Number(shadowConfidence) : null
  const stance = String(shadowStance || chair?.shadow_stance || '').toUpperCase()
  const polarity = stancePolarityClass(stance)
  const trade = chair?.shadow_trade || {}
  const supports = Array.isArray(chair?.top_supports) ? chair.top_supports : []
  const tensions = Array.isArray(chair?.top_tensions) ? chair.top_tensions : []
  const isDegraded = Boolean(chair?.degraded)

  return (
    <article
      className={`sbp-bubble sbp-bubble--chair ${polarity} ${isDegraded ? 'sbp-bubble--degraded' : ''}`}
      style={{ '--bubble-accent': CHAIR_PERSONA.accent, '--bubble-accent-soft': CHAIR_PERSONA.accentSoft, '--bubble-delay': `${index * 90}ms` }}
    >
      <div className="sbp-bubble-gutter">
        <PersonaAvatar persona={CHAIR_PERSONA} size="lg" ringPulse />
      </div>
      <div className="sbp-bubble-body">
        <header className="sbp-bubble-header sbp-bubble-header--chair">
          <span className="sbp-chair-marquee">CHAIR RULING</span>
          <span className="sbp-bubble-spacer" />
          <ShadowStanceBadge stance={stance} size="lg" />
          <ConfidenceDonut value={conf} color={CHAIR_PERSONA.accent} size={48} strokeWidth={5} />
        </header>

        {chair?.plurality_basis && (
          <p className="sbp-chair-basis">{chair.plurality_basis}</p>
        )}

        {chair?.conflict_resolution && (
          <p className="sbp-bubble-text sbp-bubble-text--quote">
            <span className="sbp-quote-label">Resolution:</span> {chair.conflict_resolution}
          </p>
        )}

        {(supports.length > 0 || tensions.length > 0) && (
          <div className="sbp-chair-columns">
            {supports.length > 0 && (
              <div className="sbp-chair-col">
                <span className="sbp-section-label sbp-section-label--green">Supports</span>
                <ul className="sbp-chair-list sbp-chair-list--supports">
                  {supports.map((s, i) => <li key={i}>{s}</li>)}
                </ul>
              </div>
            )}
            {tensions.length > 0 && (
              <div className="sbp-chair-col">
                <span className="sbp-section-label sbp-section-label--amber">Tensions</span>
                <ul className="sbp-chair-list sbp-chair-list--tensions">
                  {tensions.map((t, i) => <li key={i}>{t}</li>)}
                </ul>
              </div>
            )}
          </div>
        )}

        {(trade.entry_zone || trade.size_posture || trade.trail_posture) && (
          <div className="sbp-shadow-trade-card">
            <span className="sbp-section-label">Shadow trade construction · advisory only</span>
            <dl className="sbp-trade-dl">
              {trade.entry_zone && (<><dt>Entry zone</dt><dd>{trade.entry_zone}</dd></>)}
              {trade.size_posture && (<><dt>Size posture</dt><dd>{trade.size_posture}</dd></>)}
              {trade.trail_posture && (<><dt>Trail posture</dt><dd>{trade.trail_posture}</dd></>)}
              {trade.key_condition && (<><dt>Condition</dt><dd>{trade.key_condition}</dd></>)}
            </dl>
            <p className="sbp-advisory-note">No execution. Zero live authority.</p>
          </div>
        )}

        {isDegraded && (
          <p className="sbp-degraded-note">
            <span className="sbp-degraded-glyph" aria-hidden>!</span>
            Chair degraded: {chair?.degraded_reason || 'unknown'}
          </p>
        )}
      </div>
    </article>
  )
}

// ---------------------------------------------------------------------------
// Stage rail — vertical progress
// ---------------------------------------------------------------------------

const STAGE_LABELS = [
  { key: 'pack', label: 'Evidence pack' },
  { key: 'specialists', label: 'Specialists' },
  { key: 'conflicts', label: 'Conflicts' },
  { key: 'challenge', label: 'Challenge' },
  { key: 'revision', label: 'Revision' },
  { key: 'chair', label: 'Chair' },
  { key: 'persist', label: 'Sealed' },
]

function StageRail({ stageReached, isRunning, isComplete }) {
  const total = STAGE_LABELS.length
  const cur = Math.max(0, Math.min(total - 1, Number(stageReached) || 0))
  return (
    <ol className="sbp-rail" role="list" aria-label="Deliberation stages">
      {STAGE_LABELS.map((stage, i) => {
        const done = isComplete || i < cur
        const active = isRunning && i === cur
        return (
          <li
            key={stage.key}
            className={`sbp-rail-item ${done ? 'sbp-rail-item--done' : ''} ${active ? 'sbp-rail-item--active' : ''}`}
          >
            <span className="sbp-rail-dot" aria-hidden>
              {done ? '✓' : i + 1}
            </span>
            <span className="sbp-rail-label">{stage.label}</span>
          </li>
        )
      })}
    </ol>
  )
}

// ---------------------------------------------------------------------------
// Typing indicator — shown for specialist slots whose agent hasn't replied yet
// ---------------------------------------------------------------------------

function TypingPlaceholder({ stage, isRunning }) {
  if (!isRunning) return null
  const labels = {
    pack: 'Building evidence pack',
    specialists: 'Specialists deliberating',
    conflicts: 'Detecting conflicts',
    challenge: 'Challenge round',
    revision: 'Revision round',
    chair: 'Chair drafting verdict',
    persist: 'Sealing session',
  }
  return (
    <div className="sbp-typing">
      <span className="sbp-typing-dot" />
      <span className="sbp-typing-dot" />
      <span className="sbp-typing-dot" />
      <span className="sbp-typing-label">{labels[stage] || 'Working…'}</span>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Final verdict ribbon — across the top once chair has ruled
// ---------------------------------------------------------------------------

function VerdictRibbon({ stance, confidence, sessionId, runMs }) {
  if (!stance) return null
  const polarity = stancePolarityClass(stance)
  return (
    <div className={`sbp-verdict-ribbon ${polarity}`}>
      <div className="sbp-verdict-ribbon-left">
        <span className="sbp-verdict-label">Committee verdict</span>
        <ShadowStanceBadge stance={stance} size="lg" />
      </div>
      <div className="sbp-verdict-ribbon-right">
        <ConfidenceDonut value={confidence} color={CHAIR_PERSONA.accent} size={42} strokeWidth={5} />
        <div className="sbp-verdict-meta">
          {runMs != null && <span>{(runMs / 1000).toFixed(1)}s</span>}
        </div>
      </div>
    </div>
  )
}

function OutcomeSummary({ chair, shadowStance, shadowConfidence }) {
  const stance = String(shadowStance || chair?.shadow_stance || '').toUpperCase()
  const conf = shadowConfidence != null ? Number(shadowConfidence) : null
  const supports = (Array.isArray(chair?.top_supports) ? chair.top_supports : []).slice(0, 3)
  const tensions = (Array.isArray(chair?.top_tensions) ? chair.top_tensions : []).slice(0, 3)
  const resolution = chair?.conflict_resolution || chair?.plurality_basis || null
  if (!stance) return null
  return (
    <div className={`sbp-outcome-summary ${stancePolarityClass(stance)}`}>
      <div className="sbp-outcome-summary-head">
        <ShadowStanceBadge stance={stance} size="lg" />
        {conf != null ? (
          <span className="sbp-outcome-conf">Confidence {Math.round(conf * 100)}%</span>
        ) : null}
      </div>
      {resolution ? <p className="sbp-outcome-resolution">{resolution}</p> : null}
      {(supports.length > 0 || tensions.length > 0) && (
        <div className="sbp-outcome-columns">
          {supports.length > 0 ? (
            <ul className="sbp-outcome-list">
              {supports.map((s, i) => <li key={`s-${i}`}>{s}</li>)}
            </ul>
          ) : null}
          {tensions.length > 0 ? (
            <ul className="sbp-outcome-list sbp-outcome-list--tension">
              {tensions.map((t, i) => <li key={`t-${i}`}>{t}</li>)}
            </ul>
          ) : null}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Snapshot footer (kept from legacy)
// ---------------------------------------------------------------------------

function SnapshotBindFooter({ evidenceHash, snapshotId, sessionId }) {
  if (!evidenceHash && !snapshotId && !sessionId) return null
  const shortHash = evidenceHash ? `${String(evidenceHash).slice(0, 12)}…` : null
  return (
    <div className="sbp-snapshot-bind" title="Frozen evidence snapshot binding for this Agentic Committee session">
      <span className="sbp-snapshot-bind-label">Bound to</span>
      {snapshotId != null && <span className="sbp-snapshot-bind-chip">snapshot #{snapshotId}</span>}
      {shortHash && <span className="sbp-snapshot-bind-chip sbp-snapshot-bind-chip--mono">pack {shortHash}</span>}
      {sessionId && (
        <span className="sbp-snapshot-bind-chip sbp-snapshot-bind-chip--mono">
          session {String(sessionId).slice(0, 8)}
        </span>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Live ticker (run elapsed) — runs while RUNNING
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ChatFeed — auto-scrolling chat-prompt-style container
// ---------------------------------------------------------------------------
// The deliberation feed can grow to ~10+ bubbles. Embedding it directly in
// the page would force the host page to scroll several screen-heights as
// new content arrives — the user explicitly does NOT want that.
//
// This wrapper:
//   1. Bounds the feed to a fixed visual height so the page stays short.
//   2. Auto-scrolls to the bottom whenever new content lands, exactly like
//      a chat client (Claude / ChatGPT / Slack).
//   3. Honours the user if they manually scroll up to read an earlier
//      bubble — auto-scroll pauses and a small "↓ Jump to latest" pill
//      appears so they can re-engage when they're ready.
//
// `chatSignals` is an array of values that, when changed, should trigger
// an auto-scroll. We pass things like timeline length and stage_reached.

function ChatFeed({ children, isRunning, chatSignals }) {
  const scrollerRef = useRef(null)
  const [autoStick, setAutoStick] = useState(true)
  const [showJumpBtn, setShowJumpBtn] = useState(false)

  // Did the user scroll away from the bottom? Use a 32px tolerance so a
  // tiny rounding offset doesn't disengage auto-stick.
  const handleScroll = () => {
    const el = scrollerRef.current
    if (!el) return
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight
    const atBottom = distanceFromBottom < 32
    setAutoStick(atBottom)
    setShowJumpBtn(!atBottom)
  }

  // Scroll-to-bottom on new content (after layout, before paint).
  useLayoutEffect(() => {
    const el = scrollerRef.current
    if (!el) return
    if (autoStick) {
      el.scrollTop = el.scrollHeight
    }
    // We intentionally re-run when chatSignals changes (count of bubbles,
    // stage_reached, chair-arrived-flag) so the feed pins as content
    // materialises during the live run.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, chatSignals)

  // Re-arm auto-stick when a new run begins.
  useEffect(() => {
    if (isRunning) {
      setAutoStick(true)
      setShowJumpBtn(false)
    }
  }, [isRunning])

  const jumpToLatest = () => {
    const el = scrollerRef.current
    if (!el) return
    el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' })
    setAutoStick(true)
    setShowJumpBtn(false)
  }

  return (
    <div className="sbp-feed-shell">
      <div
        className="sbp-feed sbp-feed--chat"
        ref={scrollerRef}
        onScroll={handleScroll}
        aria-live={isRunning ? 'polite' : 'off'}
      >
        {children}
      </div>
      {showJumpBtn && (
        <button
          type="button"
          className="sbp-jump-latest"
          onClick={jumpToLatest}
          aria-label="Jump to latest message"
        >
          ↓ Jump to latest
        </button>
      )}
    </div>
  )
}

function useElapsedSeconds(active, startedAtMs) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    if (!active) return undefined
    const id = setInterval(() => setNow(Date.now()), 500)
    return () => clearInterval(id)
  }, [active])
  if (!active || !startedAtMs) return null
  return Math.max(0, Math.floor((now - startedAtMs) / 1000))
}

// ---------------------------------------------------------------------------
// Main panel
// ---------------------------------------------------------------------------

export default function ShadowBoardPanel({
  shadowPayload,
  shadowLoading,
  shadowError,
  onRun,
  runLoading,
  evidenceHash,
  snapshotId,
  runningProgress,
  showManualRun = false,
}) {
  const positions = Array.isArray(shadowPayload?.positions) ? shadowPayload.positions : []
  const conflicts = Array.isArray(shadowPayload?.conflicts) ? shadowPayload.conflicts : []
  const challenge = shadowPayload?.challenge || null
  const revisions = Array.isArray(shadowPayload?.revisions) ? shadowPayload.revisions : []
  const chair = shadowPayload?.chair || null
  const status = shadowPayload?.status || runningProgress?.status
  const isDegraded = Boolean(shadowPayload?.degraded)
  const isRunning = String(status || '').toUpperCase() === 'RUNNING'
  const isComplete = String(status || '').toUpperCase() === 'COMPLETE'
  const stageReached = runningProgress?.stage_reached ?? shadowPayload?.stage_reached ?? 0
  const stageKey = STAGE_LABELS[Math.max(0, Math.min(STAGE_LABELS.length - 1, stageReached))]?.key
  const effectiveSessionId = shadowPayload?.session_id || runningProgress?.session_id
  const effectiveHash = shadowPayload?.evidence_pack_hash || runningProgress?.evidence_pack_hash || evidenceHash
  const effectiveSnapshotId = shadowPayload?.snapshot_id ?? runningProgress?.snapshot_id ?? snapshotId
  const startedAtMs = runningProgress?.started_at_ms || null
  const liveElapsed = useElapsedSeconds(isRunning, startedAtMs)
  const [showLiveDeliberation, setShowLiveDeliberation] = useState(false)
  const [showFullTranscript, setShowFullTranscript] = useState(false)
  const [showTechnicalDetails, setShowTechnicalDetails] = useState(false)

  useEffect(() => {
    if (isRunning) {
      setShowLiveDeliberation(false)
      setShowFullTranscript(false)
    }
  }, [isRunning, effectiveSessionId])

  // Build a chronological event timeline so conflicts land *between* the
  // bubbles they relate to and challenge/revision land where they fit.
  const timeline = useMemo(() => {
    const events = []
    // Specialist bubbles first, in payload order — also tag whether each
    // specialist will receive a downstream reply (challenge or revision)
    // so we can draw a thread line in the gutter.
    const replyTargets = new Set()
    if (challenge?.target_role) replyTargets.add(String(challenge.target_role).toUpperCase())
    for (const rev of revisions || []) {
      const r = rev?.role || rev?.role_name
      if (r) replyTargets.add(String(r).toUpperCase())
    }
    const positionBlocks = positions.map((pos, i) => ({
      kind: 'position',
      pos,
      index: i,
      hasReply: replyTargets.has(String(pos?.role || '').toUpperCase()),
    }))
    events.push(...positionBlocks)
    // After specialists, surface the most-significant conflict
    if (conflicts?.length) {
      const sorted = [...conflicts].sort((a, b) => {
        const sevRank = (s) => (s === 'CRITICAL' ? 3 : s === 'MAJOR' ? 2 : 1)
        return sevRank(b?.severity) - sevRank(a?.severity)
      })
      const primary = sorted[0]
      if (primary) events.push({ kind: 'conflict', conflict: primary })
    }
    if (challenge) events.push({ kind: 'challenge', challenge })
    for (const rev of revisions || []) {
      events.push({ kind: 'revision', revision: rev })
    }
    return events
  }, [positions, conflicts, challenge, revisions])

  const showDeliberationFeed = isRunning
    ? (showLiveDeliberation || stageReached >= 1)
    : Boolean(showFullTranscript)
  const showStageSection = isRunning || (isComplete && showFullTranscript)

  return (
    <aside className="sbp-root">
      <div className="sbp-header">
        <div className="sbp-header-title">
          <span className="sbp-label-chip">AGENTIC COMMITTEE</span>
          <span className="sbp-header-subtitle">Primary review for this trade</span>
        </div>
        {(shadowPayload || runningProgress) && (
          <div className="sbp-header-meta">
            {isDegraded && <span className="sbp-degraded-chip">DEGRADED</span>}
            <span className={`sbp-status-chip sbp-status-chip--${String(status || 'unknown').toLowerCase()}`}>
              {isRunning && liveElapsed != null ? `LIVE · ${liveElapsed}s` : (status || '—')}
            </span>
            {!isRunning && shadowPayload?.run_ms != null && (
              <span className="sbp-muted">{(shadowPayload.run_ms / 1000).toFixed(1)}s</span>
            )}
          </div>
        )}
        {showManualRun && (
          <button
            type="button"
            className="sbp-run-btn"
            onClick={onRun}
            disabled={runLoading || shadowLoading}
            title="Diagnostics: re-run agentic committee session"
          >
            {runLoading ? 'Running…' : shadowPayload ? 'Re-run' : 'Run Agentic Committee'}
          </button>
        )}
      </div>

      {/* Final verdict ribbon — only after chair has ruled */}
      {(isComplete || (chair && shadowPayload?.shadow_stance)) && (
        <VerdictRibbon
          stance={shadowPayload?.shadow_stance}
          confidence={shadowPayload?.shadow_confidence}
          sessionId={effectiveSessionId}
          runMs={shadowPayload?.run_ms}
        />
      )}

      {isComplete && chair && !showFullTranscript ? (
        <>
          <OutcomeSummary
            chair={chair}
            shadowStance={shadowPayload?.shadow_stance}
            shadowConfidence={shadowPayload?.shadow_confidence}
          />
          <button
            type="button"
            className="sbp-expand-btn"
            onClick={() => setShowFullTranscript(true)}
          >
            Show full deliberation transcript
          </button>
        </>
      ) : null}

      {showStageSection ? (
      <div className="sbp-stage">
        <StageRail stageReached={stageReached} isRunning={isRunning} isComplete={isComplete} />

        {isRunning && !showDeliberationFeed ? (
          <div className="sbp-running-summary">
            <TypingPlaceholder stage={stageKey} isRunning={isRunning} />
            <button
              type="button"
              className="sbp-expand-btn sbp-expand-btn--inline"
              onClick={() => setShowLiveDeliberation(true)}
            >
              Watch live deliberation
            </button>
          </div>
        ) : null}

        {showDeliberationFeed ? (
        <ChatFeed
          isRunning={isRunning}
          chatSignals={[
            timeline.length,
            chair ? 1 : 0,
            isRunning ? stageReached : -1,
          ]}
        >
          {/* While running with no payload yet, show typing placeholder */}
          {isRunning && !shadowPayload && (
            <TypingPlaceholder stage={stageKey} isRunning={isRunning} />
          )}

          {/* Skeleton when truly empty + loading */}
          {(shadowLoading || runLoading) && !shadowPayload && !isRunning && (
            <div className="sbp-skeleton">
              <div className="sbp-skeleton-row" />
              <div className="sbp-skeleton-row sbp-skeleton-row--short" />
              <div className="sbp-skeleton-row" />
            </div>
          )}

          {/* Error */}
          {shadowError && !shadowPayload && (
            <div className="sbp-error">
              <p className="sbp-error-msg">{shadowError}</p>
              <p className="sbp-muted">Agentic Committee unavailable. Submit will be blocked until the review succeeds.</p>
            </div>
          )}

          {/* Payload available — render the timeline */}
          {shadowPayload && (
            <>
              {isDegraded && shadowPayload.degraded_reason && (
                <div className="sbp-degraded-banner">
                  <strong>Degraded run:</strong> {shadowPayload.degraded_reason}
                </div>
              )}

              {timeline.map((event, i) => {
                if (event.kind === 'position') {
                  return (
                    <SpecialistBubble
                      key={`p-${event.pos?.role || i}`}
                      pos={event.pos}
                      index={event.index}
                      hasReply={event.hasReply}
                    />
                  )
                }
                if (event.kind === 'conflict') {
                  return <ConflictMarker key={`c-${i}`} conflict={event.conflict} />
                }
                if (event.kind === 'challenge') {
                  return <ChallengeBubble key={`ch-${i}`} challenge={event.challenge} index={i} />
                }
                if (event.kind === 'revision') {
                  return <RevisionBubble key={`rv-${i}`} revision={event.revision} index={i} />
                }
                return null
              })}

              {/* Chair finale — always last */}
              {chair && (
                <ChairBubble
                  chair={chair}
                  shadowStance={shadowPayload.shadow_stance}
                  shadowConfidence={shadowPayload.shadow_confidence}
                  totalSpecialists={positions.length}
                  index={timeline.length}
                />
              )}

              {/* Mid-run typing indicator below last bubble */}
              {isRunning && (
                <TypingPlaceholder stage={stageKey} isRunning={isRunning} />
              )}
            </>
          )}

          {/* Empty + idle */}
          {!shadowPayload && !shadowLoading && !runLoading && !shadowError && !isRunning && (
            <div className="sbp-empty">
              <p className="sbp-muted">
                The boardroom is dark. {showManualRun ? 'Click Run to convene the specialists.' : 'No session has started yet.'}
              </p>
            </div>
          )}
        </ChatFeed>
        ) : null}
      </div>
      ) : isRunning ? (
        <div className="sbp-running-summary sbp-running-summary--solo">
          <StageRail stageReached={stageReached} isRunning={isRunning} isComplete={isComplete} />
          <TypingPlaceholder stage={stageKey} isRunning={isRunning} />
          <button
            type="button"
            className="sbp-expand-btn sbp-expand-btn--inline"
            onClick={() => setShowLiveDeliberation(true)}
          >
            Watch live deliberation
          </button>
        </div>
      ) : null}

      {isComplete && showFullTranscript ? (
        <button
          type="button"
          className="sbp-expand-btn sbp-expand-btn--collapse"
          onClick={() => setShowFullTranscript(false)}
        >
          Hide deliberation transcript
        </button>
      ) : null}

      {showTechnicalDetails ? (
        <SnapshotBindFooter
          evidenceHash={effectiveHash}
          snapshotId={effectiveSnapshotId}
          sessionId={effectiveSessionId}
        />
      ) : (
        <button
          type="button"
          className="sbp-expand-btn sbp-expand-btn--technical"
          onClick={() => setShowTechnicalDetails(true)}
        >
          Technical details
        </button>
      )}
      <footer className="sbp-disclaimer">
        Agentic Committee — six specialists and one chair on a frozen snapshot.
      </footer>
    </aside>
  )
}
