import { Link } from 'react-router-dom'
import { useEffect, useMemo, useRef, useState } from 'react'
import IntradaySubstantiationMapCard from '../components/IntradaySubstantiationMapCard'
import LivePoliticianDisclosureContextCard from '../components/LivePoliticianDisclosureContextCard'
import PublicDisclosureContextCard from '../components/PublicDisclosureContextCard'
import ShadowBoardPanel from '../components/committee/ShadowBoardPanel'
import { normalizeShadowBoardResponse } from '../components/committee/shadowResponse'
import { API_BASE } from '../config/apiBase'

function fmtNum(v, digits = 2) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

function fmtShortDate(s) {
  if (s == null) return '—'
  const t = String(s).slice(0, 10)
  return t || '—'
}

/** Bar date as a readable label (evidence anchor). */
function fmtBarDateHuman(s) {
  if (s == null || s === '') return '—'
  const raw = String(s).slice(0, 10)
  const d = new Date(`${raw}T12:00:00`)
  if (Number.isNaN(d.getTime())) return raw
  return d.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' })
}

/** Relative “Updated …” from API timestamp. */
function humanRelativeUpdated(iso) {
  if (iso == null || iso === '') return null
  const t = new Date(iso)
  if (Number.isNaN(t.getTime())) return null
  const sec = Math.max(0, Math.floor((Date.now() - t.getTime()) / 1000))
  if (sec < 45) return 'Updated just now'
  if (sec < 3600) return `Updated ${Math.floor(sec / 60)}m ago`
  if (sec < 86400) return `Updated ${Math.floor(sec / 3600)}h ago`
  if (sec < 86400 * 7) return `Updated ${Math.floor(sec / 86400)}d ago`
  return `Updated ${t.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })}`
}

/** Calmer summary line for fingerprint (no “V1 baseline” tone). */
function humanizeFingerprintSummary(text) {
  if (text == null || typeof text !== 'string') return ''
  let t = text
    .replace(/\bV1\b/gi, '')
    .replace(/\bbaseline\b/gi, 'default read')
    .replace(/\s+/g, ' ')
    .trim()
  if (/^Limited fingerprint for\s+/i.test(t)) {
    t = t.replace(
      /^Limited fingerprint for\s+(\S+)\s+—\s*only setup family, trust label, and live vol are wired;\s*add path\/geometry numerics for a fuller read\.?/i,
      'Snapshot is light on path and geometry for $1 — trust, setup, and live vol still apply.',
    )
    t = t.replace(/^Limited fingerprint for\s+(\S+)\s+—\s*/i, 'Snapshot detail is limited for $1 — ')
  }
  return t
}

/** Badge label for trait panel. */
function fingerprintBadgeLabel(badge) {
  const b = String(badge || '').toUpperCase()
  if (b === 'THIN') return 'Sparse traits'
  if (b === 'POOR' || b === 'OK' || b === 'MIXED') return badge
  return badge || '—'
}

/** One bullet → short trait chip text. */
function traitChipFromBullet(line) {
  if (line == null) return ''
  const s = String(line).trim()
  if (s.length > 72) return `${s.slice(0, 69)}…`
  return s
}

function ExecutionShapingPanel({ shaping }) {
  const s = shaping && typeof shaping === 'object' && !Array.isArray(shaping) ? shaping : {}
  const stance = s.stance != null ? String(s.stance).replace(/_/g, ' ') : '—'
  const size = s.size_posture != null ? String(s.size_posture) : '—'
  const trail = s.trail_posture != null ? String(s.trail_posture) : '—'
  const notes = s.notes != null ? String(s.notes) : ''
  return (
    <div className="lpa-c2-exec-panel">
      <div className="lpa-c2-exec-row">
        <span className="lpa-c2-exec-k">Stance</span>
        <span className="lpa-c2-exec-v">{stance}</span>
      </div>
      <div className="lpa-c2-exec-row">
        <span className="lpa-c2-exec-k">Size posture</span>
        <span className="lpa-c2-exec-v">{size}</span>
      </div>
      <div className="lpa-c2-exec-row">
        <span className="lpa-c2-exec-k">Trail posture</span>
        <span className="lpa-c2-exec-v">{trail}</span>
      </div>
      {notes ? (
        <p className="lpa-c2-exec-notes" title={notes}>
          {notes}
        </p>
      ) : null}
    </div>
  )
}

/** Horizontal evidence meter: zone band, invalidation tick, last price marker. */
function GeometryEvidenceMeter({ zoneLow, zoneHigh, price, inv, breached }) {
  const zl = Number(zoneLow)
  const zh = Number(zoneHigh)
  const px = Number(price)
  const iv = inv != null ? Number(inv) : NaN
  if (![zl, zh, px].every((x) => Number.isFinite(x)) || zh <= zl) {
    return (
      <p className="lpa-c2-muted lpa-c2-geo-fallback">
        Scale view needs a valid entry zone and last price together. When one is missing, use the numeric summary below —
        nothing is wrong with the hearing.
      </p>
    )
  }
  const candidates = [zl, zh, px]
  if (Number.isFinite(iv)) candidates.push(iv)
  let lo = Math.min(...candidates)
  let hi = Math.max(...candidates)
  const pad = (hi - lo) * 0.06 || Math.abs(px) * 0.002 || 0.01
  lo -= pad
  hi += pad
  const span = hi - lo || 1
  const pct = (v) => ((v - lo) / span) * 100
  const zoneL = pct(zl)
  const zoneW = Math.max(pct(zh) - zoneL, 0.8)
  const priceP = Math.min(100, Math.max(0, pct(px)))
  const invP = Number.isFinite(iv) ? Math.min(100, Math.max(0, pct(iv))) : null
  return (
    <div className="lpa-c2-geo-meter" aria-label="Price versus proposal zone and invalidation">
      <div className="lpa-c2-geo-track">
        <div className="lpa-c2-geo-zone" style={{ left: `${zoneL}%`, width: `${zoneW}%` }} title="Entry zone" />
        {invP != null ? (
          <div
            className={`lpa-c2-geo-marker lpa-c2-geo-marker--inv ${breached ? 'is-breach' : ''}`}
            style={{ left: `${invP}%` }}
            title={`Invalidation ${fmtNum(iv, 4)}`}
          />
        ) : null}
        <div className="lpa-c2-geo-marker lpa-c2-geo-marker--px" style={{ left: `${priceP}%` }} title={`Last ${fmtNum(px, 4)}`} />
      </div>
      <div className="lpa-c2-geo-legend">
        <span>
          <i className="lpa-c2-swatch lpa-c2-swatch--zone" /> Zone
        </span>
        <span>
          <i className="lpa-c2-swatch lpa-c2-swatch--px" /> Last close
        </span>
        {invP != null ? (
          <span>
            <i className="lpa-c2-swatch lpa-c2-swatch--inv" /> Inv
          </span>
        ) : null}
      </div>
    </div>
  )
}

/** Labeled close chart: only when there is meaningful variation; otherwise explanatory copy. */
function CloseHistoryChart({ trace, gradId }) {
  const rows = Array.isArray(trace) ? trace : []
  const pts = rows
    .map((t) => ({
      close: Number(t?.close),
      date: t?.bar_date != null ? String(t.bar_date) : '',
    }))
    .filter((t) => Number.isFinite(t.close))

  if (pts.length < 2) {
    return (
      <div className="lpa-c2-chart-fallback">
        <span className="lpa-c2-chart-icon" aria-hidden>
          📉
        </span>
        <p>Need at least two daily closes in evidence to draw a path. Numeric zone stats above still apply.</p>
      </div>
    )
  }

  const values = pts.map((p) => p.close)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const avg = values.reduce((a, b) => a + b, 0) / values.length
  const spread = max - min
  const flat = spread <= Math.max(1e-9, Math.abs(avg) * 0.0004)

  const width = 280
  const height = 72
  const padX = 36
  const padY = 14
  const innerW = width - padX * 2
  const innerH = height - padY * 2

  if (flat) {
    return (
      <div className="lpa-c2-chart-fallback">
        <span className="lpa-c2-chart-icon" aria-hidden>
          ⏥
        </span>
        <p>
          Closes in this window are effectively flat ({fmtNum(min, 4)}–{fmtNum(max, 4)}). No slope to read — use the
          zone meter and stance instead.
        </p>
      </div>
    )
  }

  const normY = (v) => padY + innerH - ((v - min) / spread) * innerH
  const normX = (i) => padX + (innerW * i) / (pts.length - 1)

  let dLine = ''
  pts.forEach((p, i) => {
    const x = normX(i)
    const y = normY(p.close)
    dLine += `${i === 0 ? 'M' : 'L'}${x},${y} `
  })
  const lastX = normX(pts.length - 1)
  const firstX = normX(0)
  const baseY = padY + innerH
  const dArea = `${dLine} L ${lastX},${baseY} L ${firstX},${baseY} Z`

  return (
    <div className="lpa-c2-chart-wrap">
      <div className="lpa-c2-chart-cap">Daily closes (evidence window)</div>
      <svg className="lpa-c2-chart-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Close price path">
        <defs>
          <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#5c6bc0" stopOpacity="0.35" />
            <stop offset="100%" stopColor="#5c6bc0" stopOpacity="0.02" />
          </linearGradient>
        </defs>
        <line x1={padX} y1={baseY} x2={width - padX} y2={baseY} className="lpa-c2-chart-baseline" />
        <path d={dArea} fill={`url(#${gradId})`} />
        <path d={dLine} fill="none" stroke="#3949ab" strokeWidth="2" strokeLinejoin="round" />
        {pts.map((p, i) => (
          <circle key={i} cx={normX(i)} cy={normY(p.close)} r={i === pts.length - 1 ? 4 : 2.5} className="lpa-c2-chart-dot" />
        ))}
        <text x={padX} y={height - 2} className="lpa-c2-chart-axis">
          {fmtShortDate(pts[0].date)}
        </text>
        <text x={width - padX} y={height - 2} textAnchor="end" className="lpa-c2-chart-axis">
          {fmtShortDate(pts[pts.length - 1].date)}
        </text>
        <text x={padX} y={11} className="lpa-c2-chart-axis">
          {fmtNum(max, 4)} hi
        </text>
        <text x={padX} y={padY + innerH - 2} className="lpa-c2-chart-axis">
          {fmtNum(min, 4)} lo
        </text>
      </svg>
    </div>
  )
}

function WipTerminal({ progressMsg }) {
  const steps = [
    { key: 'hearing', label: 'Refresh evidence dossier from latest bars' },
    { key: 'bind', label: 'Run Agentic Committee against evidence dossier' },
    { key: 'live', label: 'Apply Agentic Committee verdict to LIVE action' },
  ]
  const pm = progressMsg || ''
  // Phase 5B-aware progress mapping. Backward compatible with the legacy
  // "Refreshing hearing… / Binding final decision… / Materializing LIVE…"
  // phrasing in case any caller still emits the old strings.
  const activeIdx = /Applying agentic verdict|Materializing|revalidation|approvals|Verdict received/i.test(pm)
    ? 2
    : /Running Agentic Committee|Binding|final decision/i.test(pm)
      ? 1
      : 0
  return (
    <div className="lpa-c2-wip" role="status" aria-live="polite">
      <div className="lpa-c2-wip-glow" aria-hidden />
      <header className="lpa-c2-wip-head">
        <span className="lpa-c2-wip-badge">In progress</span>
        <h4 className="lpa-c2-wip-title">Intelligence Review Evidence</h4>
        <p className="lpa-c2-wip-sub">Evidence is being compiled — hang tight.</p>
      </header>
      <div className="lpa-c2-wip-current">
        <span className="lpa-c2-wip-pulse" aria-hidden />
        <span>{progressMsg || 'Starting request…'}</span>
      </div>
      <ol className="lpa-c2-wip-steps">
        {steps.map((s, i) => (
          <li key={s.key} className={`lpa-c2-wip-step ${i <= activeIdx ? 'is-done' : ''} ${i === activeIdx ? 'is-active' : ''}`}>
            <span className="lpa-c2-wip-step-ix">{i < activeIdx ? '✓' : i === activeIdx ? '◉' : '○'}</span>
            {s.label}
          </li>
        ))}
      </ol>
      <p className="lpa-c2-wip-foot">Exhibits will appear step-by-step when the server responds — the page stays put.</p>
    </div>
  )
}

function Reveal({ show, children, className = '' }) {
  return <div className={`lpa-c2-reveal ${show ? 'is-visible' : ''} ${className}`.trim()}>{children}</div>
}

/**
 * Inline proof exhibits + WIP terminal for Committee 2.0 orchestrate.
 */
export default function LpaCommittee2Exhibits({
  inline,
  hearingHref,
  progressMsg,
  loading,
  onShadowSessionLoaded,
  shadowReused = false,
  showEvidenceColumn = false,
  onForceFreshReview,
}) {
  const chartGradId = useMemo(() => `c2fx_${Math.random().toString(36).slice(2, 11)}`, [])
  const [revealStep, setRevealStep] = useState(0)
  // Comparative-priority context for this proposal vs the rest of the
  // active slate — backed by /committee/proposal/{id}/priority-context.
  // Intentionally orthogonal to stance/confidence: priority answers
  // "is this the strongest idea right now?", stance answers
  // "should we take it?". Failures keep the page rendering without the pill.
  const [priorityCtx, setPriorityCtx] = useState(null)
  const revealKey = useMemo(
    () =>
      inline
        ? `${inline.hearing_id || ''}:${inline.hearing_updated_at || inline.hearing_ts || ''}:${inline.stance || ''}`
        : '',
    [inline],
  )

  useEffect(() => {
    const pid = inline?.proposal_id
    if (pid == null) {
      setPriorityCtx(null)
      return undefined
    }
    let cancelled = false
    const run = async () => {
      try {
        const r = await fetch(`${API_BASE}/committee/proposal/${encodeURIComponent(pid)}/priority-context`)
        if (cancelled || !r.ok) return
        const j = await r.json()
        if (!cancelled && j && j.available) setPriorityCtx(j)
        else if (!cancelled) setPriorityCtx(null)
      } catch (_e) {
        if (!cancelled) setPriorityCtx(null)
      }
    }
    run()
    return () => {
      cancelled = true
    }
  }, [inline?.proposal_id])

  // ----------------------------------------------------------------------
  // Phase 1 dual-hearing: simultaneous shadow-board polling.
  //
  // The LPA orchestrate response already carries shadow_session_id +
  // evidence_pack_hash (kicked off in parallel by the backend). We:
  //   1. Poll the lightweight `?include_progress=1` endpoint every ~2s while
  //      the session is still RUNNING (renders a live progress strip).
  //   2. Once status flips to COMPLETE / DEGRADED / FAILED, fetch the full
  //      session payload one time to render the full panel.
  //
  // NOTE: hooks must be declared before any early returns to keep hook order
  // stable across renders (otherwise React throws "Rendered more hooks than
  // during the previous render" and unmounts the entire tree → blank page).
  // ----------------------------------------------------------------------
  const hearingId = inline?.hearing_id || null
  const inlineEvidenceHash = inline?.evidence_pack_hash || null
  const inlineShadowStatus = inline?.shadow_status || null
  const inlineShadowSession = inline?.shadow_session_id || null

  const [shadowProgress, setShadowProgress] = useState(null)
  const [shadowPayload, setShadowPayload] = useState(null)
  const [shadowError, setShadowError] = useState(null)
  const [shadowLoading, setShadowLoading] = useState(false)
  const fullFetchedFor = useRef(null)
  const pollGenerationRef = useRef(0)
  const pollStatusRef = useRef('RUNNING')

  useEffect(() => {
    if (!hearingId) {
      setShadowProgress(null)
      setShadowPayload(null)
      setShadowError(null)
      setShadowLoading(false)
      fullFetchedFor.current = null
      return undefined
    }

    const generation = pollGenerationRef.current + 1
    pollGenerationRef.current = generation

    let cancelled = false
    let timer = null
    setShadowPayload(null)
    setShadowError(null)
    setShadowLoading(true)
    fullFetchedFor.current = null

    // Seed progress with whatever orchestrate told us up-front.
    const seededStatus = String(inlineShadowStatus || 'RUNNING').toUpperCase()
    pollStatusRef.current = seededStatus
    if (inlineShadowSession || inlineShadowStatus) {
      setShadowProgress({
        session_id: inlineShadowSession,
        status: inlineShadowStatus || 'RUNNING',
        stage_reached: 0,
        evidence_pack_hash: inlineEvidenceHash,
      })
    } else {
      setShadowProgress({
        status: 'RUNNING',
        stage_reached: 0,
        evidence_pack_hash: inlineEvidenceHash,
      })
    }

    const hashQuery = inlineEvidenceHash
      ? `&evidence_pack_hash=${encodeURIComponent(inlineEvidenceHash)}`
      : ''

    const fetchProgress = async () => {
      try {
        const r = await fetch(
          `${API_BASE}/committee/hearing/${encodeURIComponent(hearingId)}/shadow-board?include_progress=1${hashQuery}`,
        )
        if (cancelled || pollGenerationRef.current !== generation) return
        if (r.status === 404) {
          return
        }
        if (r.status === 503) {
          setShadowError('Agentic Committee disabled')
          return
        }
        if (!r.ok) {
          setShadowError(`Agentic Committee poll failed (${r.status})`)
          return
        }
        const j = await r.json()
        if (cancelled || pollGenerationRef.current !== generation || !j) return
        pollStatusRef.current = String(j.status || '').toUpperCase()
        setShadowProgress(j)

        const status = pollStatusRef.current
        const stageReached = Number(j.stage_reached ?? 0)
        const isTerminal = status === 'COMPLETE' || status === 'DEGRADED' || status === 'FAILED'
        const isRunningNow = status === 'RUNNING'
        const shouldFetchFull = isTerminal || stageReached >= 1
        if (shouldFetchFull) {
          try {
            const full = await fetch(
              `${API_BASE}/committee/hearing/${encodeURIComponent(hearingId)}/shadow-board${inlineEvidenceHash ? `?evidence_pack_hash=${encodeURIComponent(inlineEvidenceHash)}` : ''}`,
            )
            if (full.ok) {
              const fj = await full.json()
              if (cancelled || pollGenerationRef.current !== generation) return
              if (isRunningNow && fj?.stale_session) {
                return
              }
              setShadowPayload(fj)
              if (isTerminal) fullFetchedFor.current = j.session_id
            }
          } catch (_e) { /* ignore */ }
        }
      } catch (_e) {
      } finally {
        if (!cancelled && pollGenerationRef.current === generation) setShadowLoading(false)
      }
    }

    fetchProgress()

    const tick = () => {
      const status = pollStatusRef.current
      if (status === 'COMPLETE' || status === 'DEGRADED' || status === 'FAILED') {
        return
      }
      fetchProgress().finally(() => {
        if (!cancelled && pollGenerationRef.current === generation) {
          const next = pollStatusRef.current
          if (next === 'COMPLETE' || next === 'DEGRADED' || next === 'FAILED') {
            return
          }
          timer = setTimeout(tick, 1200)
        }
      })
    }

    timer = setTimeout(tick, 1200)
    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hearingId, inlineShadowSession, inlineEvidenceHash])

  // Stage 2: when the exhibits panel has fetched the (possibly final) shadow
  // payload, normalize it and push it back to the LPA row so the Shadow
  // Chair Verdict headline stays in sync with what's actually rendered
  // here in the exhibits. Without this, the LPA bounded poll can exit at
  // TIMEOUT while the exhibits show the completed board.
  // Read-only / advisory — no effect on submit gating or materialization.
  //
  // The callback is held in a ref so a new inline arrow from the parent
  // (recreated on every parent render) does NOT retrigger this effect.
  // Otherwise we'd race: parent re-render → new prop fn → effect fires →
  // calls back → parent setState → parent re-render → loop.
  const onShadowSessionLoadedRef = useRef(onShadowSessionLoaded)
  useEffect(() => {
    onShadowSessionLoadedRef.current = onShadowSessionLoaded
  }, [onShadowSessionLoaded])
  useEffect(() => {
    const cb = onShadowSessionLoadedRef.current
    if (!cb) return
    // Prefer the full payload; fall back to the progress payload so the
    // headline still surfaces RUNNING/DEGRADED transitions while the
    // chair is mid-flight.
    const source = shadowPayload || shadowProgress
    if (!source) return
    const normalized = normalizeShadowBoardResponse(source)
    if (normalized.status === 'UNAVAILABLE' && !normalized.stance) return
    cb(normalized)
  }, [shadowPayload, shadowProgress])

  useEffect(() => {
    if (!inline) {
      setRevealStep(0)
      return undefined
    }
    const shadowStatus = String(shadowProgress?.status || inlineShadowStatus || '').toUpperCase()
    const shadowRunning = shadowStatus === 'RUNNING' || loading
    if (shadowRunning && !showEvidenceColumn) {
      setRevealStep(0)
      return undefined
    }
    setRevealStep(0)
    const hasIntraday = inline.exhibit_intraday_substantiation_map != null
    const hasDisclosure =
      inline.exhibit_public_disclosure_context != null || inline.exhibit_live_politician_disclosure_context != null
    const intradayOffset = hasIntraday ? 1 : 0
    const disclosureOffset = hasDisclosure ? 1 : 0
    const maxStep = 8 + intradayOffset + disclosureOffset + 1
    let n = 0
    const tick = setInterval(() => {
      n += 1
      setRevealStep((s) => Math.min(s + 1, maxStep))
      if (n >= maxStep) clearInterval(tick)
    }, 380)
    return () => clearInterval(tick)
  }, [revealKey, inline, shadowProgress?.status, inlineShadowStatus, loading, showEvidenceColumn])

  if (loading && !inline) {
    return <WipTerminal progressMsg={progressMsg} />
  }

  if (!inline) return null

  const gh = inline.exhibit_geometry_hero || {}
  const pq = inline.exhibit_path_quality || {}
  const rg = inline.exhibit_regime_continuity || {}
  const pr = inline.exhibit_protection || {}
  const fp = inline.exhibit_symbol_fingerprint || {}
  const chair = inline.chair_board || {}
  // Phase 5B: when the agentic-only orchestrate writes the hearing, every
  // verdict-flavored field (stance / confidence / chair.stance / chair
  // supports + tensions) is NULL on COMMITTEE_HEARING by design. Detect that
  // state so the "Chair board" panel and stance/conf masthead pills don't
  // render as empty placeholders that look like a still-loading deterministic
  // verdict — that's what made revalidations appear to "run the deterministic
  // committee first" when in fact no chair ever executes.
  const isEvidenceOnlyHearing = (
    !inline.stance
    && !chair.stance
    && (chair.top_supports || []).length === 0
    && (chair.top_tensions || []).length === 0
  )
  const strip = Array.isArray(inline.what_changed_strip) ? inline.what_changed_strip : []
  const trace = gh.post_proposal_path_trace
  const pdcExhibit = inline.exhibit_public_disclosure_context
  const livePdcExhibit = inline.exhibit_live_politician_disclosure_context
  const hasDisclosureExhibit = pdcExhibit != null || livePdcExhibit != null
  const hasIntradayExhibit = inline.exhibit_intraday_substantiation_map != null
  const intradayOffset = hasIntradayExhibit ? 1 : 0
  const disclosureOffset = hasDisclosureExhibit ? 1 : 0
  const stepPath = 2 + intradayOffset
  const stepReg = 3 + intradayOffset
  const stepFp = 4 + intradayOffset
  const stepDisclosure = 5 + intradayOffset
  const stripStep = 5 + intradayOffset + disclosureOffset
  const chairStep = 6 + intradayOffset + disclosureOffset
  const shadowStep = 7 + intradayOffset + disclosureOffset
  const linkStep = 8 + intradayOffset + disclosureOffset

  const effectiveShadowStatus = String(shadowProgress?.status || inlineShadowStatus || '').toUpperCase()
  const shadowRunning = effectiveShadowStatus === 'RUNNING' || loading
  const showEvidence = showEvidenceColumn || !shadowRunning

  const agenticPanel = (
    <div className="lpa-c2-dual-shadow">
      <div className="lpa-c2-dual-banner lpa-c2-dual-banner--shadow">
        <span className="lpa-c2-dual-chip lpa-c2-dual-chip--shadow">AGENTIC COMMITTEE</span>
        <span className="lpa-c2-dual-banner-text">Primary review for this trade</span>
      </div>
      {shadowReused ? (
        <div className="lpa-c2-reused-row">
          <span className="lpa-c2-reused-badge">Same market snapshot — prior review reused</span>
          {onForceFreshReview ? (
            <button type="button" className="lpa-c2-force-fresh-btn" onClick={onForceFreshReview}>
              Force fresh review
            </button>
          ) : null}
        </div>
      ) : shadowRunning && !shadowReused ? (
        <div className="lpa-c2-fresh-badge">Fresh review running…</div>
      ) : null}
      <ShadowBoardPanel
        shadowPayload={shadowPayload}
        shadowLoading={shadowLoading && !shadowPayload && !shadowProgress}
        shadowError={shadowError}
        runningProgress={shadowProgress}
        evidenceHash={inlineEvidenceHash}
        showManualRun={false}
      />
    </div>
  )

  return (
    <div className="lpa-c2-dual lpa-c2-dual--agentic-first">
      {agenticPanel}
      {!showEvidence && shadowRunning ? (
        <p className="lpa-c2-evidence-collapsed lpa-subtle">
          Evidence dossier is collapsed while the Agentic Committee runs. Use &quot;Show evidence dossier&quot; above to expand.
        </p>
      ) : null}
      {showEvidence ? (
    <div className="lpa-c2-dual-real">
      <div className="lpa-c2-dual-diagnostic-header">
        <span className="lpa-c2-dual-diagnostic-chip">Evidence trail</span>
        <span className="lpa-c2-dual-diagnostic-text">
          Read-only investigation view — the Agentic Committee above is the verdict source.
        </span>
      </div>
      <div className="lpa-c2-dual-banner lpa-c2-dual-banner--real">
        <span className="lpa-c2-dual-chip">EVIDENCE SNAPSHOT</span>
        <span className="lpa-c2-dual-banner-text">Frozen evidence dossier supporting the Agentic Committee review</span>
      </div>
      <div className="lpa-c2-exhibits">
      {loading && progressMsg ? (
        <div className="lpa-c2-inline-wip">
          <span className="lpa-c2-wip-pulse" aria-hidden />
          {progressMsg}
        </div>
      ) : null}

      <Reveal show={revealStep >= 0} className="lpa-c2-masthead">
        <div className="lpa-c2-masthead-row">
          <div>
            {isEvidenceOnlyHearing ? (
              <span
                className="lpa-c2-pill lpa-c2-pill--ghost"
                title="Phase 5B: this dossier carries evidence only. The Agentic Committee on the right is the verdict source."
              >
                Evidence dossier — verdict from Agentic Committee →
              </span>
            ) : (
              <>
                <span className="lpa-c2-pill lpa-c2-pill--stance">{String(inline.stance || '—').replace(/_/g, ' ')}</span>
                <span className="lpa-c2-pill">conf {fmtNum(inline.confidence, 2)}</span>
              </>
            )}
            <span className="lpa-c2-pill lpa-c2-pill--ghost">{inline.symbol || '—'}</span>
            {priorityCtx && priorityCtx.in_slate ? (
              <span
                className={`lpa-c2-pill lpa-c2-pill--priority lpa-c2-pill--priority-${String(priorityCtx.priority_band || 'low').toLowerCase()}`}
                title={`Priority #${priorityCtx.priority_rank} of ${priorityCtx.total} (${priorityCtx.priority_band_label}) — ${priorityCtx.priority_reason_label}${priorityCtx.composite_score != null ? ` · score ${priorityCtx.composite_score.toFixed(3)}` : ''}`}
              >
                Priority #{priorityCtx.priority_rank} / {priorityCtx.total} · {priorityCtx.priority_band_label}
              </span>
            ) : null}
            {/* Phase 2 Sprint 4 / option 4a — non-blocking same-symbol warning.
                Surfaces when the active slate carries another PROPOSED row on
                this SYMBOL. Board behavior is unchanged; we just make the
                competing setup visible to the operator. */}
            {priorityCtx && priorityCtx.same_symbol_other_active && priorityCtx.same_symbol_other_count > 0 ? (
              <span
                className="lpa-c2-pill lpa-c2-pill--samesym"
                title={`${priorityCtx.same_symbol_other_count} other active proposal${
                  priorityCtx.same_symbol_other_count === 1 ? '' : 's'
                } on ${inline.symbol || ''}${
                  Array.isArray(priorityCtx.same_symbol_other_directions) && priorityCtx.same_symbol_other_directions.length > 0
                    ? ` (${priorityCtx.same_symbol_other_directions.join(', ')})`
                    : ''
                } — pick one or compare on the structural market timeline before acting.`}
              >
                +{priorityCtx.same_symbol_other_count} on {inline.symbol || 'symbol'}
              </span>
            ) : null}
          </div>
        </div>
        {priorityCtx && priorityCtx.in_slate ? (
          <div className="lpa-c2-priority-reason" title="Why this proposal ranks here">
            <span className="lpa-c2-priority-reason-label">Why ranked</span>
            <span className="lpa-c2-priority-reason-text">{priorityCtx.priority_reason_label}</span>
          </div>
        ) : null}
        <div className="lpa-c2-freshness-bar">
          <div className="lpa-c2-freshness-primary">
            <strong className="lpa-c2-freshness-updated">
              {humanRelativeUpdated(inline.hearing_updated_at || inline.hearing_ts) || 'Updated time not available'}
            </strong>
            <span className="lpa-c2-freshness-barline">
              Bar date <span className="lpa-c2-freshness-barvalue">{fmtBarDateHuman(inline.evidence_bar_date)}</span>
            </span>
          </div>
          {inline.stale_hint ? (
            <span className="lpa-c2-stale-badge" title={inline.stale_hint}>
              Stale — consider refresh
            </span>
          ) : null}
        </div>
      </Reveal>

      <div className="lpa-c2-exhibits-cols">
        <div className="lpa-c2-col">
          <Reveal show={revealStep >= 1} className="lpa-c2-card">
            <div className="lpa-c2-card-head">
              <span className="lpa-c2-card-icon" aria-hidden>
                ◈
              </span>
              Geometry vs proposal
            </div>
            <GeometryEvidenceMeter
              zoneLow={gh.zone_low}
              zoneHigh={gh.zone_high}
              price={gh.latest_price}
              inv={gh.invalidation_level}
              breached={gh.invalidation_breached}
            />
            <div className="lpa-c2-hero-stats">
              <span>
                Zone {fmtNum(gh.zone_low, 4)} – {fmtNum(gh.zone_high, 4)}
              </span>
              <span>Last {fmtNum(gh.latest_price, 4)}</span>
              <span>Δ mid {fmtNum(gh.zone_distance_pct, 2)}%</span>
            </div>
            <div className="lpa-c2-hero-inv">
              Inv {fmtNum(gh.invalidation_level, 4)} {gh.invalidation_breached ? <strong> breached</strong> : ' holding'}
            </div>
            <CloseHistoryChart trace={trace} gradId={chartGradId} />
          </Reveal>

          {hasIntradayExhibit ? (
            <Reveal show={revealStep >= 2} className="lpa-c2-card lpa-c2-card--ism">
              <IntradaySubstantiationMapCard exhibit={inline.exhibit_intraday_substantiation_map} variant="lpa" />
            </Reveal>
          ) : null}

          <Reveal show={revealStep >= stepPath} className="lpa-c2-card">
            <div className="lpa-c2-card-head">
              <span className="lpa-c2-card-icon" aria-hidden>
                ≋
              </span>
              Path quality
            </div>
            <div className="lpa-c2-path-meters">
              <div className="lpa-c2-meter">
                <span className="lpa-c2-meter-label">Adverse-before-favorable</span>
                <div className="lpa-c2-meter-bar">
                  <div
                    className="lpa-c2-meter-fill"
                    style={{ width: `${Math.min(100, (Number(pq.pct_adverse_before_favorable) || 0) * 100)}%` }}
                  />
                </div>
                <strong>{fmtNum(pq.pct_adverse_before_favorable, 3)}</strong>
              </div>
              <div className="lpa-c2-meter">
                <span className="lpa-c2-meter-label">MHR</span>
                <div className="lpa-c2-meter-bar">
                  <div
                    className="lpa-c2-meter-fill lpa-c2-meter-fill--alt"
                    style={{ width: `${Math.min(100, (Number(pq.mhr) || 0) * 100)}%` }}
                  />
                </div>
                <strong>{fmtNum(pq.mhr, 3)}</strong>
              </div>
              <div className="lpa-c2-path-bucket">
                <span className="lpa-c2-meter-label">Bucket</span>
                <span className="lpa-c2-bucket-pill">{pq.path_quality_label || '—'}</span>
              </div>
            </div>
            {pq.interpretation ? <p className="lpa-c2-interpret">{pq.interpretation}</p> : null}
          </Reveal>
        </div>

        <div className="lpa-c2-col">
          <Reveal show={revealStep >= 3} className="lpa-c2-card">
            <div className="lpa-c2-card-head">
              <span className="lpa-c2-card-icon" aria-hidden>
                ⇄
              </span>
              Regime continuity
            </div>
            <div className="lpa-c2-regime-flow">
              <div className="lpa-c2-regime-card">
                <span className="lpa-c2-micro-label">Proposal</span>
                <div className="lpa-c2-regime-value">{rg.proposal_regime || '—'}</div>
                <div className="lpa-c2-subtle">{rg.structure_proposal || '—'}</div>
              </div>
              <div className="lpa-c2-regime-ribbon" aria-hidden>
                <span />
              </div>
              <div className="lpa-c2-regime-card">
                <span className="lpa-c2-micro-label">Now</span>
                <div className="lpa-c2-regime-value">
                  {rg.trend_now || '—'} / {rg.vol_now || '—'}
                </div>
                <div className="lpa-c2-subtle">{rg.structure_now || '—'}</div>
              </div>
            </div>
            <div className={`lpa-c2-continuity lpa-c2-continuity--${String(rg.continuity_verdict || 'na').toLowerCase()}`}>
              <strong>{rg.continuity_verdict || '—'}</strong>
              {rg.continuity_detail ? <span>{rg.continuity_detail}</span> : null}
            </div>

            <div className="lpa-c2-divider" />

            <div className="lpa-c2-card-subhead">Protection</div>
            <div className="lpa-c2-protect-grid">
              <div className="lpa-c2-protect-stat">
                <span className="lpa-c2-micro-label">Cushion to inv.</span>
                <strong>{fmtNum(pr.cushion_pct, 2)}%</strong>
              </div>
              <div className="lpa-c2-protect-stat">
                <span className="lpa-c2-micro-label">Trail / size</span>
                <strong>
                  {pr.trail_posture || '—'} · {pr.size_posture || '—'}
                </strong>
              </div>
            </div>
          </Reveal>

          <Reveal show={revealStep >= stepFp} className="lpa-c2-card">
            <div className="lpa-c2-card-head">
              <span className="lpa-c2-card-icon" aria-hidden>
                ✦
              </span>
              Symbol fingerprint
            </div>
            <div className="lpa-c2-fp-badge">{fp.badge || '—'}</div>
            {fp.one_liner ? <p className="lpa-c2-fp-one">{fp.one_liner}</p> : null}
            <ul className="lpa-c2-fp-bullets">
              {(fp.bullets || []).slice(0, 6).map((b, i) => (
                <li key={i}>{b}</li>
              ))}
            </ul>
          </Reveal>
        </div>
      </div>

      {hasDisclosureExhibit ? (
        <Reveal show={revealStep >= stepDisclosure} className="lpa-c2-card lpa-c2-card--pdc">
          <div className="lpa-c2-disclosure-pair">
            {pdcExhibit != null ? <PublicDisclosureContextCard exhibit={pdcExhibit} variant="lpa" /> : null}
            {livePdcExhibit != null ? (
              <LivePoliticianDisclosureContextCard exhibit={livePdcExhibit} variant="lpa" />
            ) : null}
          </div>
        </Reveal>
      ) : null}

      <Reveal show={revealStep >= stripStep} className="lpa-c2-card lpa-c2-card--strip">
        <div className="lpa-c2-card-head">Since proposal</div>
        {strip.length > 0 ? (
          <div className="lpa-c2-strip-chips">
            {strip.map((s, i) => (
              <span key={i} className="lpa-c2-chip" title={s}>
                {s}
              </span>
            ))}
          </div>
        ) : (
          <p className="lpa-c2-muted">No delta strip lines on this hearing.</p>
        )}
      </Reveal>

      {/* Phase 5B: when the orchestrate path is the agentic-only refresh,
          the deterministic chair is not executed and CH.STANCE / CONFIDENCE /
          CHAIR_OUTPUT_JSON are all NULL on purpose. Hiding the "Chair board"
          card prevents the empty pills + empty Supports/Tensions lists from
          looking like a half-loaded deterministic verdict. The Agentic
          Committee panel on the right is the only verdict source. */}
      {isEvidenceOnlyHearing ? null : (
        <Reveal show={revealStep >= chairStep} className="lpa-c2-chair">
          <div className="lpa-c2-card-head">
            <span className="lpa-c2-card-icon" aria-hidden>
              ⚖
            </span>
            Chair board
          </div>
          <div className="lpa-c2-chair-head">
            <strong>{String(chair.stance || inline.stance || '—').replace(/_/g, ' ')}</strong>
            <span>confidence {fmtNum(chair.confidence ?? inline.confidence, 2)}</span>
          </div>
          <div className="lpa-c2-chair-zones">
            <div>
              <span className="lpa-c2-zone-label">Supports</span>
              <ul>
                {(chair.top_supports || []).map((s, i) => (
                  <li key={i}>{s}</li>
                ))}
              </ul>
            </div>
            <div>
              <span className="lpa-c2-zone-label">Tensions</span>
              <ul>
                {(chair.top_tensions || []).map((s, i) => (
                  <li key={i}>{s}</li>
                ))}
              </ul>
            </div>
            <div>
              <span className="lpa-c2-zone-label">Execution shaping</span>
              <ExecutionShapingPanel shaping={chair.execution_shaping} />
            </div>
          </div>
        </Reveal>
      )}

      <Reveal show={revealStep >= linkStep} className="lpa-c2-full-link">
        {hearingHref ? (
          <Link to={hearingHref} title="Open historical evidence dossier (read-only)">
            Open evidence dossier →
          </Link>
        ) : null}
      </Reveal>
        </div>
      </div>
      ) : null}
    </div>
  )
}
