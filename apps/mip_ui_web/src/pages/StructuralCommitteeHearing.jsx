import { useCallback, useEffect, useState } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'
import { API_BASE } from '../config/apiBase'
import LivePoliticianDisclosureContextCard from '../components/LivePoliticianDisclosureContextCard'
import PublicDisclosureContextCard from '../components/PublicDisclosureContextCard'
import './StructuralCommitteeHearing.css'

function confFixed(v, digits = 2) {
  const n = Number(v)
  return Number.isFinite(n) ? n.toFixed(digits) : Number(0).toFixed(digits)
}

function StanceBadge({ stance }) {
  if (!stance) return null
  const s = String(stance)
  const cls = `sch-stance sch-stance--${s.toLowerCase()}`
  return <span className={cls}>{s.replace(/_/g, ' ')}</span>
}

export default function StructuralCommitteeHearing() {
  const { hearingId } = useParams()
  const [searchParams] = useSearchParams()
  const proposalId = searchParams.get('proposal_id')
  const actionIdFromUrl = searchParams.get('action_id')
  const navigate = useNavigate()

  const [payload, setPayload] = useState(null)
  const [prevSnapshot, setPrevSnapshot] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)
  const [commitMsg, setCommitMsg] = useState(null)
  const [actionIdInput, setActionIdInput] = useState(() => (actionIdFromUrl || '').trim())
  const [showAdvancedBind, setShowAdvancedBind] = useState(false)

  const loadByHearing = useCallback(async (hid) => {
    setLoading(true)
    setError(null)
    try {
      const r = await fetch(`${API_BASE}/committee/hearing/${encodeURIComponent(hid)}`)
      const j = await r.json()
      if (!r.ok) throw new Error(j.detail ? JSON.stringify(j.detail) : r.statusText)
      setPayload(j)
    } catch (e) {
      setError(e.message || String(e))
      setPayload(null)
    } finally {
      setLoading(false)
    }
  }, [])

  const openFromProposal = useCallback(async (pid) => {
    setLoading(true)
    setError(null)
    try {
      const r = await fetch(`${API_BASE}/committee/hearing/open`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ proposal_id: Number(pid), force_rebuild: false }),
      })
      const j = await r.json()
      if (!r.ok) throw new Error(j.detail ? JSON.stringify(j.detail) : r.statusText)
      navigate(`/structural-committee/${j.hearing_id}`, { replace: true })
      setPayload(j)
    } catch (e) {
      setError(e.message || String(e))
      setPayload(null)
    } finally {
      setLoading(false)
    }
  }, [navigate])

  useEffect(() => {
    if (hearingId) {
      loadByHearing(hearingId)
      return
    }
    if (proposalId) {
      openFromProposal(proposalId)
      return
    }
    setLoading(false)
  }, [hearingId, proposalId, loadByHearing, openFromProposal])

  useEffect(() => {
    if (actionIdFromUrl) setActionIdInput((actionIdFromUrl || '').trim())
  }, [actionIdFromUrl])

  const refresh = async () => {
    if (!hearingId) return
    setPrevSnapshot(payload)
    setLoading(true)
    setError(null)
    try {
      const r = await fetch(`${API_BASE}/committee/hearing/${encodeURIComponent(hearingId)}/refresh`, {
        method: 'POST',
      })
      const j = await r.json()
      if (!r.ok) throw new Error(j.detail ? JSON.stringify(j.detail) : r.statusText)
      setPayload(j)
    } catch (e) {
      setError(e.message || String(e))
    } finally {
      setLoading(false)
    }
  }

  const boundActionId = (actionIdFromUrl || '').trim()

  const commit = async () => {
    if (!hearingId) return
    setCommitMsg(null)
    const aid = showAdvancedBind
      ? (actionIdInput || '').trim() || boundActionId
      : boundActionId || (actionIdInput || '').trim()
    if (!aid) {
      setCommitMsg(
        'Commit skipped: set Live action ID (required to link COMMITTEE_FINAL_DECISION for LPA).',
      )
      return
    }
    try {
      const r = await fetch(`${API_BASE}/committee/hearing/${encodeURIComponent(hearingId)}/commit`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ note: 'UI commit', action_id: aid }),
      })
      const j = await r.json()
      if (!r.ok) throw new Error(j.detail ? JSON.stringify(j.detail) : r.statusText)
      setCommitMsg(j.already_committed ? 'Already committed (idempotent).' : 'Final decision stored.')
    } catch (e) {
      setCommitMsg(`Commit failed: ${e.message}`)
    }
  }

  if (!hearingId && !proposalId) {
    return (
      <div className="sch-wrap sch-wrap--landing">
        <header className="sch-header sch-header--premium">
          <div>
            <h1>Committee 2.0</h1>
            <p className="sch-sub">
              Structural entry hearings are opened from a proposal. This page is empty until you open or load a hearing.
            </p>
          </div>
        </header>
        <section className="sch-panel sch-landing-card">
          <h2>How to open a hearing</h2>
          <ul className="sch-landing-list">
            <li>
              <Link to="/structural-timeline">Structural Market Timeline</Link> — use <strong>Open hearing</strong> on a
              proposal.
            </li>
            <li>
              Or open directly:{' '}
              <code className="sch-inline-code">
                /structural-committee?proposal_id=&lt;id&gt;&amp;action_id=&lt;LIVE_ACTION_ID&gt;
              </code>{' '}
              (action ID binds the final decision for Live Portfolio Activity sync).
            </li>
          </ul>
          <p className="sch-muted sch-landing-foot">
            If Committee 2.0 is disabled in Snowflake (<code>COMMITTEE2_ENABLED</code>), the API returns 503 and this
            room cannot load.
          </p>
        </section>
      </div>
    )
  }

  if (loading && !payload) {
    return (
      <div className="sch-wrap">
        <p className="sch-muted">Loading hearing…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="sch-wrap">
        <p className="sch-error">{error}</p>
      </div>
    )
  }

  const snap = payload?.snapshot_panel || {}
  const chair = payload?.chair || {}
  const deltas = Array.isArray(payload?.deltas) ? payload.deltas : []
  const roles = Array.isArray(payload?.roles) ? payload.roles : []
  const arts = Array.isArray(payload?.artifacts) ? payload.artifacts : []
  const ev = payload?.hearing_evidence || {}

  return (
    <div className="sch-wrap">
      <header className="sch-header sch-header--premium">
        <div>
          <h1>Committee 2.0 — Hearing</h1>
          <p className="sch-sub">
            {payload?.proposal?.symbol} {payload?.proposal?.direction} · Proposal {payload?.proposal_id} · Hearing{' '}
            <code>{payload?.hearing_id}</code>
          </p>
        </div>
        <div className="sch-header-actions">
          <StanceBadge stance={payload?.stance} />
          <span className="sch-conf">Conf {(Number(payload?.confidence) || 0).toFixed(2)}</span>
          <button type="button" className="sch-btn" onClick={refresh} disabled={loading}>
            Refresh
          </button>
          <button type="button" className="sch-btn sch-btn--primary" onClick={commit} disabled={!hearingId}>
            Commit final
          </button>
        </div>
      </header>
      {commitMsg && <p className={`sch-note ${commitMsg.startsWith('Commit skipped') ? 'sch-note--warn' : ''}`}>{commitMsg}</p>}

      <section className="sch-panel sch-live-bind">
        <h2>Live action binding</h2>
        {boundActionId ? (
          <>
            <p className="sch-sub">
              Using <strong>action_id</strong> from the link. <strong>Commit final</strong> binds this hearing to that
              pending LIVE row for LPA.
            </p>
            <div className="sch-bind-chips">
              <span className="sch-chip">action_id: {boundActionId}</span>
              {proposalId ? <span className="sch-chip">proposal_id: {proposalId}</span> : null}
            </div>
            {!showAdvancedBind ? (
              <button type="button" className="sch-btn sch-btn--ghost" onClick={() => setShowAdvancedBind(true)}>
                Advanced — change action_id
              </button>
            ) : null}
          </>
        ) : (
          <p className="sch-sub">
            Paste the pending <strong>LIVE_ACTIONS.ACTION_ID</strong> for this proposal before <strong>Commit final</strong>{' '}
            so Live Portfolio Activity can load this decision.
          </p>
        )}
        {(showAdvancedBind || !boundActionId) && (
          <label className="sch-field">
            <span className="sch-field-label">action_id {boundActionId ? '(override)' : ''}</span>
            <input
              type="text"
              className="sch-input"
              value={actionIdInput}
              onChange={(e) => setActionIdInput(e.target.value)}
              placeholder="e.g. from Pending Decisions row"
              autoComplete="off"
            />
          </label>
        )}
      </section>

      <div className="sch-grid">
        <section className="sch-panel">
          <h2>Proposal snapshot</h2>
          <dl className="sch-dl">
            <dt>Setup</dt><dd>{snap.SETUP_FAMILY}</dd>
            <dt>Trust</dt><dd>{snap.TRUST_LABEL}</dd>
            <dt>Regime (proposal)</dt><dd>{snap.REGIME_STATE}</dd>
            <dt>Structure (proposal)</dt><dd>{snap.STRUCTURAL_STATE}</dd>
            <dt>Trail</dt><dd>{snap.TRAILING_STYLE}</dd>
          </dl>
        </section>
        <section className="sch-panel sch-panel--live">
          <h2>Live evidence</h2>
          <dl className="sch-dl">
            <dt>Latest price</dt><dd title="hearing.latest_price">{ev.latest_price}</dd>
            <dt>Structure now</dt><dd title="hearing.structural_state_now">{ev.structural_state_now}</dd>
            <dt>Trend regime</dt><dd title="hearing.trend_regime_now">{ev.trend_regime_now}</dd>
            <dt>Invalidation</dt><dd title="snapshot.INVALIDATION_JSON">{JSON.stringify(ev.invalidation_level)} {ev.invalidation_breached ? '⚠ breached' : ''}</dd>
          </dl>
        </section>
      </div>

      {payload?.exhibit_public_disclosure_context || payload?.exhibit_live_politician_disclosure_context ? (
        <div className="sch-disclosure-row">
          {payload?.exhibit_public_disclosure_context ? (
            <PublicDisclosureContextCard exhibit={payload.exhibit_public_disclosure_context} variant="hearing" />
          ) : null}
          {payload?.exhibit_live_politician_disclosure_context ? (
            <LivePoliticianDisclosureContextCard
              exhibit={payload.exhibit_live_politician_disclosure_context}
              variant="hearing"
            />
          ) : null}
        </div>
      ) : null}

      <section className="sch-panel sch-delta-strip">
        <h2>Delta strip</h2>
        <div className="sch-delta-row">
          {deltas.map((d, i) => (
            <div key={i} className="sch-delta-chip" title={d.detail}>
              <span className="sch-delta-cat">{d.category}</span>
              <span className="sch-delta-sum">{d.summary}</span>
            </div>
          ))}
        </div>
      </section>

      <section className="sch-panel">
        <h2>Specialist board</h2>
        <div className="sch-role-grid">
          {roles.map((r) => (
            <article key={r.role_name} className="sch-role-card">
              <div className="sch-role-head">
                <span className="sch-role-title">{String(r.role_name).replace(/_/g, ' ')}</span>
                <span className="sch-role-badge">{r.output?.stance_badge}</span>
              </div>
              <p className="sch-role-line">{r.output?.one_liner}</p>
              <ul className="sch-role-bullets">
                {(r.output?.bullets || []).map((b, j) => (
                  <li key={j}>{b}</li>
                ))}
              </ul>
              <div className="sch-art">
                {arts.find((a) => a.artifact_kind === 'GEOMETRY_METER' && r.role_name === 'ENTRY_GEOMETRY')?.payload
                  ? <MiniGeometry art={arts.find((a) => a.artifact_kind === 'GEOMETRY_METER')} />
                  : null}
                {r.role_name === 'STRUCTURAL_THESIS' && (
                  <MiniStructure art={arts.find((a) => a.artifact_kind === 'STRUCTURE_MAP')} />
                )}
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className="sch-panel sch-chair">
        <h2>Chair</h2>
        <StanceBadge stance={chair.stance || payload?.stance} />
        <p className="sch-chair-line">Confidence {confFixed(chair.confidence ?? payload?.confidence ?? 0)}</p>
        <div className="sch-chair-cols">
          <div>
            <h3>Supports</h3>
            <ul>{(chair.top_supports || []).map((s, i) => <li key={i}>{s}</li>)}</ul>
          </div>
          <div>
            <h3>Tensions</h3>
            <ul>{(chair.top_tensions || []).map((s, i) => <li key={i}>{s}</li>)}</ul>
          </div>
        </div>
        <div className="sch-exec">
          <h3>Execution shaping</h3>
          <pre className="sch-pre">{JSON.stringify(chair.execution_shaping || {}, null, 2)}</pre>
        </div>
        <div className="sch-changed">
          <h3>What changed</h3>
          <ul>{(chair.what_changed_since_proposal || []).map((s, i) => <li key={i}>{s}</li>)}</ul>
        </div>
      </section>

      {prevSnapshot && payload && (
        <section className="sch-panel sch-compare">
          <h2>Compare to previous refresh (session)</h2>
          <p className="sch-muted">Stance was {prevSnapshot.stance}; now {payload.stance}</p>
        </section>
      )}
    </div>
  )
}

function MiniGeometry({ art }) {
  if (!art?.payload) return null
  const p = art.payload
  return (
    <div className="sch-mini sch-mini--geom" title="GEOMETRY_METER">
      <div className="sch-meter-bar">
        <span style={{ width: `${Math.min(100, (p.dist_pct || 0) * 4)}%` }} />
      </div>
      <span className="sch-mini-cap">{p.label}</span>
    </div>
  )
}

function MiniStructure({ art }) {
  if (!art?.payload) return null
  const p = art.payload
  return (
    <div className="sch-mini sch-mini--struct" title="STRUCTURE_MAP">
      <span>{p.snapshot_state} → {p.now_state}</span>
      <span className="sch-mini-cap">{p.thesis}</span>
    </div>
  )
}
