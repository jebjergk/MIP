import { Link } from 'react-router-dom'
import { useEffect, useMemo, useState } from 'react'
import IntradaySubstantiationMapCard from '../components/IntradaySubstantiationMapCard'
import LivePoliticianDisclosureContextCard from '../components/LivePoliticianDisclosureContextCard'
import PublicDisclosureContextCard from '../components/PublicDisclosureContextCard'

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
    { key: 'hearing', label: 'Refresh deterministic hearing from latest bars' },
    { key: 'bind', label: 'Bind final decision to this LIVE action' },
    { key: 'live', label: 'Materialize verdict into LIVE committee tables' },
  ]
  const pm = progressMsg || ''
  const activeIdx = /revalidation|approvals|Verdict received|Materializing/i.test(pm)
    ? 2
    : /Binding|final decision/i.test(pm)
      ? 1
      : 0
  return (
    <div className="lpa-c2-wip" role="status" aria-live="polite">
      <div className="lpa-c2-wip-glow" aria-hidden />
      <header className="lpa-c2-wip-head">
        <span className="lpa-c2-wip-badge">In progress</span>
        <h4 className="lpa-c2-wip-title">Committee 2.0 orchestration</h4>
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
export default function LpaCommittee2Exhibits({ inline, hearingHref, progressMsg, loading }) {
  const chartGradId = useMemo(() => `c2fx_${Math.random().toString(36).slice(2, 11)}`, [])
  const [revealStep, setRevealStep] = useState(0)
  const revealKey = useMemo(
    () =>
      inline
        ? `${inline.hearing_id || ''}:${inline.hearing_updated_at || inline.hearing_ts || ''}:${inline.stance || ''}`
        : '',
    [inline],
  )

  useEffect(() => {
    if (!inline) {
      setRevealStep(0)
      return undefined
    }
    setRevealStep(0)
    const hasIntraday = inline.exhibit_intraday_substantiation_map != null
    const hasDisclosure =
      inline.exhibit_public_disclosure_context != null || inline.exhibit_live_politician_disclosure_context != null
    const intradayOffset = hasIntraday ? 1 : 0
    const disclosureOffset = hasDisclosure ? 1 : 0
    const maxStep = 7 + intradayOffset + disclosureOffset + 1
    let n = 0
    const tick = setInterval(() => {
      n += 1
      setRevealStep((s) => Math.min(s + 1, maxStep))
      if (n >= maxStep) clearInterval(tick)
    }, 380)
    return () => clearInterval(tick)
  }, [revealKey, inline])

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
  const linkStep = 7 + intradayOffset + disclosureOffset

  return (
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
            <span className="lpa-c2-pill lpa-c2-pill--stance">{String(inline.stance || '—').replace(/_/g, ' ')}</span>
            <span className="lpa-c2-pill">conf {fmtNum(inline.confidence, 2)}</span>
            <span className="lpa-c2-pill lpa-c2-pill--ghost">{inline.symbol || '—'}</span>
          </div>
        </div>
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

      <Reveal show={revealStep >= linkStep} className="lpa-c2-full-link">
        {hearingHref ? <Link to={hearingHref}>Open full hearing →</Link> : null}
      </Reveal>
    </div>
  )
}
