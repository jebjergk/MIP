import { useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../config/apiBase'
import LoadingState from '../components/LoadingState'
import ErrorState from '../components/ErrorState'
import './CommitteePerformance.css'

/* ------------------------------------------------------------------ */
/* Committee Bake-off (Performance)                                    */
/* Read-only sidecar comparing Committee 2.0 (REAL) vs Shadow Board    */
/* on shared opportunities. Source: GET /api/committee-performance/*.  */
/* ------------------------------------------------------------------ */

function fmtPct(v, d = 1) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return `${(Number(v) * 100).toFixed(d)}%`
}

function fmtRet(v, d = 2) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  const num = Number(v) * 100
  const sign = num > 0 ? '+' : ''
  return `${sign}${num.toFixed(d)}%`
}

function fmtTs(v) {
  if (!v) return '—'
  try {
    return new Date(v).toISOString().slice(0, 16).replace('T', ' ')
  } catch {
    return v
  }
}

function stanceClass(raw, normalized) {
  if (normalized === 'ENTER') return 'enter'
  const r = (raw || '').toUpperCase()
  if (r === 'DENY' || r === 'REJECT') return 'deny'
  if (r === 'DEFER' || r === 'TABLE') return 'defer'
  return 'cash'
}

function StanceBadge({ raw, normalized }) {
  if (!raw) return <span className="cbo-empty-inline">—</span>
  return (
    <span className={`cbo-stance ${stanceClass(raw, normalized)}`}>
      <strong>{raw}</strong>
      <span className="normalized">({normalized || 'CASH'})</span>
    </span>
  )
}

function ConfigBadge({ status, reason }) {
  if (!status) return null
  const cls = (status || '').toLowerCase()
  return (
    <span className={`cbo-config ${cls}`} title={reason || status}>{status}</span>
  )
}

function OutcomeBadge({ outcome, ret, excluded, excludedReason }) {
  if (!outcome) return <span className="cbo-empty-inline">—</span>
  let toneCls = 'flat'
  if (ret != null) {
    const n = Number(ret)
    if (n > 0) toneCls = 'win'
    else if (n < 0) toneCls = 'loss'
  }
  return (
    <span className={`cbo-outcome ${toneCls}`}>
      <strong>{outcome}</strong>
      <span>{fmtRet(ret)}</span>
      {excluded ? <span className="cbo-excluded-tag" title={excludedReason || ''}>excluded</span> : null}
    </span>
  )
}

function KpiCard({ label, value, hint }) {
  return (
    <article className="cbo-kpi-card">
      <span className="cbo-kpi-label">{label}</span>
      <strong className="cbo-kpi-value">{value}</strong>
      {hint ? <span className="cbo-kpi-hint">{hint}</span> : null}
    </article>
  )
}

const RECOMMENDATION_CLASS = {
  PREFER_REAL: 'prefer-real',
  PREFER_SHADOW: 'prefer-shadow',
  TIE: 'tie',
  INSUFFICIENT_DATA: 'insufficient',
}

/* Plain-English headline answering "is shadow helping, hurting, or inconclusive?" */
const RECOMMENDATION_HEADLINE = {
  PREFER_REAL:       { tone: 'helping-real',  text: 'Shadow is hurting — real has done better month-to-date.' },
  PREFER_SHADOW:     { tone: 'helping-shadow', text: 'Shadow is helping — its picks have done better than real.' },
  TIE:               { tone: 'tie',            text: 'Inconclusive — real and shadow are roughly tied this month.' },
  INSUFFICIENT_DATA: { tone: 'insufficient',   text: 'Inconclusive — not enough scored opportunities yet.' },
}

/* Plain-English mapping for COMPARISON_LABEL values produced by the bake-off SP. */
const COMPARISON_LABEL_PLAIN = {
  AGREE:                   'Both agreed',
  BOTH_CASH:               'Both stayed out',
  REAL_WIN__SHADOW_WIN:    'Both entered and won',
  REAL_LOSS__SHADOW_LOSS:  'Both entered and lost',
  REAL_WIN__SHADOW_LOSS:   'Real won; shadow lost',
  REAL_LOSS__SHADOW_WIN:   'Real lost; shadow won',
  REAL_WIN__SHADOW_CASH:   'Real won; shadow stayed out',
  REAL_LOSS__SHADOW_CASH:  'Real lost; shadow stayed out',
  REAL_CASH__SHADOW_WIN:   'Real stayed out; shadow won',
  REAL_CASH__SHADOW_LOSS:  'Real stayed out; shadow lost',
  UNCOMPARABLE:            'No fair comparison yet',
  NULL:                    'No fair comparison yet',
}

function plainComparisonLabel(raw) {
  if (!raw) return COMPARISON_LABEL_PLAIN.NULL
  const key = String(raw).toUpperCase()
  return COMPARISON_LABEL_PLAIN[key] || key.replace(/__/g, ' / ').replace(/_/g, ' ').toLowerCase()
}

export default function CommitteePerformance() {
  const [scorecard, setScorecard] = useState(null)
  const [mtd, setMtd] = useState(null)
  const [labels, setLabels] = useState([])
  const [opportunities, setOpportunities] = useState([])
  const [opportunitiesLoading, setOpportunitiesLoading] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [filter, setFilter] = useState('all')
  const [labelFilter, setLabelFilter] = useState('')
  const [detail, setDetail] = useState(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState(null)

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      setLoading(true)
      setError(null)
      try {
        const [sRes, mRes, lRes] = await Promise.all([
          fetch(`${API_BASE}/committee-performance/scorecard`),
          fetch(`${API_BASE}/committee-performance/mtd`),
          fetch(`${API_BASE}/committee-performance/labels`),
        ])
        if (!sRes.ok) throw new Error(`scorecard ${sRes.status}`)
        if (!mRes.ok) throw new Error(`mtd ${mRes.status}`)
        if (!lRes.ok) throw new Error(`labels ${lRes.status}`)
        const [s, m, l] = await Promise.all([sRes.json(), mRes.json(), lRes.json()])
        if (!cancelled) {
          setScorecard(s || {})
          setMtd(m || {})
          setLabels(l?.rows || [])
        }
      } catch (e) {
        if (!cancelled) setError(e.message || String(e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      setOpportunitiesLoading(true)
      try {
        const params = new URLSearchParams()
        if (filter === 'disagreements') params.set('disagreements_only', 'true')
        if (filter === 'excluded') params.set('excluded_only', 'true')
        if (labelFilter) params.set('comparison_label', labelFilter)
        const res = await fetch(`${API_BASE}/committee-performance/opportunities?${params}`)
        if (!res.ok) throw new Error(`opportunities ${res.status}`)
        const data = await res.json()
        if (!cancelled) setOpportunities(data?.rows || [])
      } catch (e) {
        if (!cancelled) setError(e.message || String(e))
      } finally {
        if (!cancelled) setOpportunitiesLoading(false)
      }
    }
    load()
    return () => { cancelled = true }
  }, [filter, labelFilter])

  const openDetail = async (proposalId) => {
    setDetail({ proposal_id: proposalId, boards: [] })
    setDetailLoading(true)
    setDetailError(null)
    try {
      const res = await fetch(`${API_BASE}/committee-performance/opportunity/${proposalId}`)
      if (!res.ok) throw new Error(`opportunity ${proposalId}: ${res.status}`)
      setDetail(await res.json())
    } catch (e) {
      setDetailError(e.message || String(e))
    } finally {
      setDetailLoading(false)
    }
  }

  const closeDetail = () => {
    setDetail(null)
    setDetailError(null)
  }

  const recommendation = mtd?.MTD_RECOMMENDATION || 'INSUFFICIENT_DATA'
  const recClass = RECOMMENDATION_CLASS[recommendation] || 'insufficient'

  const totalOpps = scorecard?.TOTAL_OPPORTUNITIES ?? 0
  const scored = scorecard?.SCORED_OPPORTUNITIES ?? 0
  const excluded = scorecard?.EXCLUDED_OPPORTUNITIES ?? 0

  const summarySafe = (key) => scorecard?.[key]

  const tableBody = useMemo(() => {
    if (opportunitiesLoading) return null
    if (opportunities.length === 0) {
      return (
        <tr>
          <td colSpan={10} className="cbo-empty">
            No opportunities for the current filter.
          </td>
        </tr>
      )
    }
    return opportunities.map((o) => {
      const realExcluded = !!o.REAL_EXCLUDED
      const shadowExcluded = !!o.SHADOW_EXCLUDED
      const rowExcluded = realExcluded || shadowExcluded
      return (
        <tr key={`${o.PROPOSAL_ID}_${o.SYMBOL || ''}`} className={rowExcluded ? 'excluded' : ''}>
          <td style={{ whiteSpace: 'nowrap' }}>{fmtTs(o.FIRST_DECISION_TS)}</td>
          <td>
            <span className="cbo-symbol">{o.SYMBOL || '—'}</span>
            {o.DIRECTION ? <span className="cbo-direction">{o.DIRECTION}</span> : null}
          </td>
          <td><StanceBadge raw={o.REAL_RAW_STANCE} normalized={o.REAL_ACTION} /></td>
          <td><ConfigBadge status={o.REAL_CONFIG_STATUS} reason={o.REAL_CONFIG_REASON} /></td>
          <td>
            <OutcomeBadge
              outcome={o.REAL_OUTCOME}
              ret={o.REAL_RETURN}
              excluded={realExcluded}
              excludedReason={o.REAL_EXCLUDED_REASON}
            />
          </td>
          <td><StanceBadge raw={o.SHADOW_RAW_STANCE} normalized={o.SHADOW_ACTION} /></td>
          <td><ConfigBadge status={o.SHADOW_CONFIG_STATUS} reason={o.SHADOW_CONFIG_REASON} /></td>
          <td>
            <OutcomeBadge
              outcome={o.SHADOW_OUTCOME}
              ret={o.SHADOW_RETURN}
              excluded={shadowExcluded}
              excludedReason={o.SHADOW_EXCLUDED_REASON}
            />
          </td>
          <td>
            <span
              className="cbo-comparison"
              title={o.COMPARISON_LABEL || ''}
            >
              {plainComparisonLabel(o.COMPARISON_LABEL)}
            </span>
          </td>
          <td>
            <button type="button" className="cbo-detail-btn" onClick={() => openDetail(o.PROPOSAL_ID)}>
              Details
            </button>
          </td>
        </tr>
      )
    })
  }, [opportunities, opportunitiesLoading])

  if (loading) return <LoadingState message="Loading committee bake-off..." />
  if (error) return <ErrorState message={error} />

  return (
    <div className="cbo-page">
      <header className="cbo-header">
        <h1>Committee Bake-off <span className="cbo-historical-tag">(historical)</span></h1>
        <p>
          <b>Historical analytics from the pre-Stage-4 era.</b>{' '}
          As of Stage 4 the Agentic Committee is the only authoritative review path; this bake-off
          remains for retrospective comparison of the legacy deterministic committee (labeled "Real
          Board" below) versus the agentic specialists (labeled "Shadow Board" below) on shared
          historical opportunities. Diagnostics only — does not change live execution, the proposal
          pipeline, or IBKR.
        </p>
      </header>

      <section className={`cbo-mtd ${recClass}`}>
        <div className="cbo-mtd-main">
          <div className="cbo-mtd-label">Is shadow helping, hurting, or inconclusive?</div>
          <div className={`cbo-mtd-headline ${RECOMMENDATION_HEADLINE[recommendation]?.tone || 'insufficient'}`}>
            {RECOMMENDATION_HEADLINE[recommendation]?.text || RECOMMENDATION_HEADLINE.INSUFFICIENT_DATA.text}
          </div>
          <div className="cbo-mtd-sub">
            Month-to-date · real avg {fmtRet(mtd?.REAL_AVG_RETURN)} · shadow avg {fmtRet(mtd?.SHADOW_AVG_RETURN)} ·
            difference {fmtRet(mtd?.AVG_RETURN_DELTA_REAL_MINUS_SHADOW)} · raw signal {recommendation.toLowerCase().replace(/_/g, ' ')}
          </div>
        </div>
        <KpiCard label="Total opportunities" value={totalOpps} />
        <KpiCard
          label="Scored"
          value={scored}
          hint={excluded ? `${excluded} excluded` : 'all included'}
        />
        <KpiCard
          label="Disagreements"
          value={summarySafe('DISAGREE_NORMALIZED_COUNT') ?? 0}
          hint={`${summarySafe('DISAGREE_RAW_COUNT') ?? 0} raw-stance`}
        />
      </section>

      <section className="cbo-summary-grid">
        <article className="cbo-panel">
          <h3>Real Board</h3>
          <div className="cbo-panel-row">
            Entered: <strong>{summarySafe('REAL_ENTER_COUNT') ?? 0}</strong> ·{' '}
            Wins: <strong className="cbo-pos">{summarySafe('REAL_WIN_COUNT') ?? 0}</strong> ·{' '}
            Losses: <strong className="cbo-neg">{summarySafe('REAL_LOSS_COUNT') ?? 0}</strong>
          </div>
          <div className="cbo-panel-row">
            Hit rate: <strong>{fmtPct(summarySafe('REAL_HIT_RATE'))}</strong> ·{' '}
            Avg return: <strong>{fmtRet(summarySafe('REAL_AVG_RETURN'))}</strong>
          </div>
        </article>
        <article className="cbo-panel">
          <h3>Shadow Board</h3>
          <div className="cbo-panel-row">
            Entered: <strong>{summarySafe('SHADOW_ENTER_COUNT') ?? 0}</strong> ·{' '}
            Wins: <strong className="cbo-pos">{summarySafe('SHADOW_WIN_COUNT') ?? 0}</strong> ·{' '}
            Losses: <strong className="cbo-neg">{summarySafe('SHADOW_LOSS_COUNT') ?? 0}</strong>
          </div>
          <div className="cbo-panel-row">
            Hit rate: <strong>{fmtPct(summarySafe('SHADOW_HIT_RATE'))}</strong> ·{' '}
            Avg return: <strong>{fmtRet(summarySafe('SHADOW_AVG_RETURN'))}</strong>
          </div>
        </article>
        <article className="cbo-panel">
          <h3>Comparison labels</h3>
          {labels.length === 0 ? (
            <div className="cbo-panel-row">No labels yet.</div>
          ) : (
            <table className="cbo-label-table">
              <tbody>
                {labels.map((l) => {
                  const name = l.COMPARISON_LABEL || 'NULL'
                  const active = labelFilter === name
                  return (
                    <tr key={name}>
                      <td>
                        <button
                          type="button"
                          className={`cbo-label-btn ${active ? 'active' : ''}`}
                          onClick={() => setLabelFilter(active ? '' : name)}
                          title={name}
                        >
                          {plainComparisonLabel(name)}
                        </button>
                      </td>
                      <td style={{ textAlign: 'right' }}>{l.N}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}
          {labelFilter ? (
            <button className="cbo-label-clear" type="button" onClick={() => setLabelFilter('')}>
              Clear label filter
            </button>
          ) : null}
        </article>
      </section>

      <section className="cbo-filter-row">
        <strong>Per-opportunity:</strong>
        {[
          { id: 'all', label: 'All' },
          { id: 'disagreements', label: 'Disagreements only' },
          { id: 'excluded', label: 'Excluded only' },
        ].map((f) => (
          <button
            key={f.id}
            type="button"
            className={`cbo-chip ${filter === f.id ? 'active' : ''}`}
            onClick={() => setFilter(f.id)}
          >
            {f.label}
          </button>
        ))}
        <span className="cbo-row-count">{opportunities.length} row(s)</span>
      </section>

      <section className="cbo-table-wrap">
        <table className="cbo-table">
          <thead>
            <tr>
              <th>Decision TS</th>
              <th>Symbol</th>
              <th>Real stance</th>
              <th>Real config</th>
              <th>Real outcome</th>
              <th>Shadow stance</th>
              <th>Shadow config</th>
              <th>Shadow outcome</th>
              <th>Comparison</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {opportunitiesLoading ? (
              <tr>
                <td colSpan={10} className="cbo-empty">Loading...</td>
              </tr>
            ) : (
              tableBody
            )}
          </tbody>
        </table>
      </section>

      {detail ? (
        <div className="cbo-modal-backdrop" onClick={closeDetail}>
          <div className="cbo-modal" onClick={(e) => e.stopPropagation()}>
            <div className="cbo-modal-head">
              <h2>Proposal {detail.proposal_id} detail</h2>
              <button type="button" className="cbo-modal-close" onClick={closeDetail}>Close</button>
            </div>
            {detailLoading ? <LoadingState message="Loading detail..." /> : null}
            {detailError ? <ErrorState message={detailError} /> : null}
            {!detailLoading && !detailError ? (
              <div className="cbo-board-grid">
                {(detail.boards || []).map((b) => (
                  <article key={b.BOARD_KIND} className="cbo-board-card">
                    <h3>{b.BOARD_KIND}</h3>
                    <div className="cbo-board-line">
                      Stance: <StanceBadge raw={b.RAW_STANCE} normalized={b.NORMALIZED_ACTION} />
                    </div>
                    <div className="cbo-board-line">
                      Latched: {b.LATCH_REASON} · Decision {fmtTs(b.DECISION_TS)}
                    </div>
                    <div className="cbo-board-line">Source: {b.LATCH_SOURCE_TABLE}</div>
                    <div className="cbo-board-line">
                      Config: <ConfigBadge status={b.CONFIG_STATUS} reason={b.CONFIG_STATUS_REASON} />
                    </div>
                    {b.CONFIG_STATUS_REASON ? (
                      <div className="cbo-board-reason">{b.CONFIG_STATUS_REASON}</div>
                    ) : null}
                    <div className="cbo-board-line">
                      Outcome: <OutcomeBadge outcome={b.RAW_OUTCOME} ret={b.REALIZED_RETURN_PCT} />
                    </div>
                    {b.SCORING_EXCLUDED_FLAG ? (
                      <div className="cbo-board-reason">
                        Excluded from scoring: {b.SCORING_EXCLUDED_REASON}
                      </div>
                    ) : null}
                    <details>
                      <summary style={{ cursor: 'pointer', fontSize: '0.78rem', marginTop: '0.5rem' }}>
                        EVAL_CONFIG_JSON
                      </summary>
                      <pre className="cbo-json">{JSON.stringify(b.EVAL_CONFIG_JSON, null, 2)}</pre>
                    </details>
                  </article>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  )
}
