import { Fragment, useCallback, useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../config/apiBase'
import LoadingState from '../components/LoadingState'
import ErrorState from '../components/ErrorState'
import './PositionHealth.css'

/* ------------------------------------------------------------------ */
/* Daily Position Health (advisory)                                    */
/* Read-only view of per-position real verdict (deterministic) vs the  */
/* shadow Cortex health review. Neither alters real positions.         */
/* Source: GET /api/position-health/comparison/latest                   */
/*         GET /api/position-health/lifecycle                           */
/*         GET /api/position-health/history/{position_episode_key}     */
/*         POST /api/position-health/run-shadow                        */
/* ------------------------------------------------------------------ */

function fmtDate(v) {
  if (!v) return '—'
  try {
    return new Date(v).toISOString().slice(0, 10)
  } catch {
    return String(v)
  }
}

function fmtTs(v) {
  if (!v) return '—'
  try {
    return new Date(v).toISOString().slice(0, 16).replace('T', ' ')
  } catch {
    return String(v)
  }
}

function fmtPctSigned(v, d = 2) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  const n = Number(v)
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toFixed(d)}%`
}

function fmtPctAbs(v, d = 2) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return `${Number(v).toFixed(d)}%`
}

function fmtNumber(v, d = 0) {
  if (v == null || Number.isNaN(Number(v))) return '—'
  return Number(v).toLocaleString(undefined, {
    minimumFractionDigits: d,
    maximumFractionDigits: d,
  })
}

function todayIsoLocal() {
  const d = new Date()
  const tz = d.getTimezoneOffset() * 60000
  return new Date(d.getTime() - tz).toISOString().slice(0, 10)
}

const VERDICT_CLASS = {
  KEEP: 'keep',
  WATCH: 'watch',
  EXIT_REVIEW: 'exit',
}

function VerdictBadge({ verdict, severity }) {
  if (!verdict) return <span className="ph-empty-inline">—</span>
  const cls = VERDICT_CLASS[String(verdict).toUpperCase()] || 'neutral'
  return (
    <span className={`ph-verdict ph-verdict--${cls}`}>
      <strong>{verdict}</strong>
      {severity ? <span className="ph-sev">{severity}</span> : null}
    </span>
  )
}

const AGREEMENT_CLASS = {
  AGREE: 'agree',
  NO_SHADOW: 'pending',
  SHADOW_MORE_BEARISH: 'shadow-bearish',
  REAL_MORE_BEARISH: 'real-bearish',
  DIFFERENT_BUT_BOTH_NON_EXIT: 'differ',
}

function AgreementBadge({ label }) {
  if (!label) return <span className="ph-empty-inline">—</span>
  const cls = AGREEMENT_CLASS[label] || 'differ'
  const display = String(label).replace(/_/g, ' ').toLowerCase()
  return <span className={`ph-agreement ph-agreement--${cls}`}>{display}</span>
}

const SHADOW_RUN_CLASS = {
  OK: 'ok',
  PARSE_ERROR: 'warn',
  API_ERROR: 'bad',
  SKIPPED_NO_PAYLOAD: 'neutral',
  PENDING: 'neutral',
}

function ShadowRunBadge({ status }) {
  if (!status) return <span className="ph-empty-inline">—</span>
  const cls = SHADOW_RUN_CLASS[String(status).toUpperCase()] || 'neutral'
  return <span className={`ph-shadow-run ph-shadow-run--${cls}`}>{status}</span>
}

/* Plain-English mapping for the most common deterministic reason codes.
 * The full code is preserved in the expanded detail row.
 */
const REASON_PHRASES = {
  THESIS_FAILED: 'Original thesis no longer holds',
  THESIS_WEAKENING: 'Thesis is weakening',
  STRUCTURE_BROKEN: 'Structure broke down',
  STOP_PROXIMITY: 'Price is close to the protective stop',
  INVALIDATION_PROXIMITY: 'Price is close to invalidation',
  REGIME_ADVERSE: 'Regime turned against the trade',
  PATH_DETERIORATING: 'Path quality is deteriorating',
  PROFIT_HOLDING: 'Trade is on track and profitable',
  ON_TRACK: 'Developing as expected',
  STRENGTHENING: 'Setup is strengthening',
  HEALTHY_HOLD: 'Healthy hold',
  NO_DETERIORATION: 'No deterioration detected',
}

function humanizeCode(code) {
  if (!code) return ''
  const c = String(code).toUpperCase()
  if (REASON_PHRASES[c]) return REASON_PHRASES[c]
  return c.replace(/_/g, ' ').toLowerCase().replace(/^\w/, (s) => s.toUpperCase())
}

/* Operator-friendly one-liner for the main row. Prefer the deterministic real
 * WHY_SUMMARY (already plain English from the SP). Fall back to a humanized
 * primary reason code, then to a generic "no concerns" line for KEEP.
 */
function plainEnglishReason(row) {
  const summary = (row.REAL_WHY_SUMMARY || '').trim()
  if (summary) return summary
  const code = humanizeCode(row.REAL_PRIMARY_REASON_CODE)
  if (code) return code
  const verdict = String(row.REAL_VERDICT || '').toUpperCase()
  if (verdict === 'KEEP') return 'No concerns'
  if (verdict === 'WATCH') return 'Watching for further signal'
  if (verdict === 'EXIT_REVIEW') return 'Flagged for exit review'
  return '—'
}

/* Single attention indicator: one short label + tone. The most operator-relevant
 * concern wins. Anything not flagged gets null (no chip rendered).
 */
function attentionFor(row) {
  const real = String(row.REAL_VERDICT || '').toUpperCase()
  const shadow = String(row.SHADOW_VERDICT || '').toUpperCase()
  const bias = String(row.SHADOW_ACTION_BIAS || '').toUpperCase()
  const agreement = String(row.AGREEMENT_LABEL || '').toUpperCase()
  const runStatus = String(row.SHADOW_RUN_STATUS || '').toUpperCase()

  if (real === 'EXIT_REVIEW' && shadow === 'EXIT_REVIEW') {
    return { label: 'Both flag exit', tone: 'exit', detail: 'Real and shadow both want exit review.' }
  }
  if (real === 'EXIT_REVIEW') {
    return { label: 'Exit review', tone: 'exit', detail: 'Real verdict is EXIT_REVIEW.' }
  }
  if (shadow === 'EXIT_REVIEW' && bias === 'EXIT_NOW') {
    return { label: 'Shadow says exit', tone: 'shadow-bearish', detail: 'Shadow wants out today; real is still holding.' }
  }
  if (agreement === 'SHADOW_MORE_BEARISH') {
    return { label: 'Shadow more cautious', tone: 'shadow-bearish', detail: 'Shadow is more bearish than real.' }
  }
  if (agreement === 'REAL_MORE_BEARISH') {
    return { label: 'Real more cautious', tone: 'real-bearish', detail: 'Real is more bearish than shadow.' }
  }
  if (real === 'WATCH' && shadow === 'WATCH') {
    return { label: 'Both watching', tone: 'watch', detail: 'Both real and shadow are in watch.' }
  }
  if (real === 'WATCH') {
    return { label: 'Watching', tone: 'watch', detail: 'Real verdict is WATCH.' }
  }
  if (runStatus === 'PARSE_ERROR' || runStatus === 'API_ERROR') {
    return { label: 'Shadow run error', tone: 'warn', detail: row.SHADOW_RUN_ERROR || `Shadow run status: ${runStatus}` }
  }
  if (agreement === 'NO_SHADOW') {
    return { label: 'Pending shadow', tone: 'neutral', detail: 'Shadow review has not run for this position yet.' }
  }
  return null
}

function KpiCard({ label, value, hint, tone }) {
  return (
    <article className={`ph-kpi-card${tone ? ` ph-kpi-card--${tone}` : ''}`}>
      <span className="ph-kpi-label">{label}</span>
      <strong className="ph-kpi-value">{value}</strong>
      {hint ? <span className="ph-kpi-hint">{hint}</span> : null}
    </article>
  )
}

export default function PositionHealth() {
  const [rows, setRows] = useState([])
  const [lifecycle, setLifecycle] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [filter, setFilter] = useState('all')
  const [portfolioFilter, setPortfolioFilter] = useState('')
  const [expanded, setExpanded] = useState({})
  const [historyByKey, setHistoryByKey] = useState({})
  const [historyLoading, setHistoryLoading] = useState({})
  const [running, setRunning] = useState(false)
  const [runMessage, setRunMessage] = useState('')
  const [runError, setRunError] = useState('')
  const [runDate, setRunDate] = useState(todayIsoLocal())

  const loadAll = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [cmpRes, lifeRes] = await Promise.all([
        fetch(`${API_BASE}/position-health/comparison/latest`),
        fetch(`${API_BASE}/position-health/lifecycle`),
      ])
      if (!cmpRes.ok) throw new Error(`comparison ${cmpRes.status}`)
      if (!lifeRes.ok) throw new Error(`lifecycle ${lifeRes.status}`)
      const [cmpData, lifeData] = await Promise.all([cmpRes.json(), lifeRes.json()])
      setRows(Array.isArray(cmpData) ? cmpData : [])
      setLifecycle(Array.isArray(lifeData) ? lifeData : [])
    } catch (e) {
      setError(e.message || String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { loadAll() }, [loadAll])

  const portfolioOptions = useMemo(() => {
    const ids = new Set()
    rows.forEach((r) => { if (r.PORTFOLIO_ID != null) ids.add(String(r.PORTFOLIO_ID)) })
    return Array.from(ids).sort((a, b) => Number(a) - Number(b))
  }, [rows])

  const filteredRows = useMemo(() => {
    return rows.filter((r) => {
      if (portfolioFilter && String(r.PORTFOLIO_ID) !== portfolioFilter) return false
      const verdict = String(r.REAL_VERDICT || '').toUpperCase()
      const agreement = String(r.AGREEMENT_LABEL || '').toUpperCase()
      if (filter === 'exit_review' && verdict !== 'EXIT_REVIEW') return false
      if (filter === 'watch' && verdict !== 'WATCH') return false
      if (filter === 'disagreements') {
        if (!agreement || agreement === 'AGREE' || agreement === 'NO_SHADOW') return false
      }
      if (filter === 'shadow_exit') {
        const shadow = String(r.SHADOW_VERDICT || '').toUpperCase()
        const bias = String(r.SHADOW_ACTION_BIAS || '').toUpperCase()
        if (shadow !== 'EXIT_REVIEW' || bias !== 'EXIT_NOW') return false
      }
      return true
    })
  }, [rows, filter, portfolioFilter])

  const summary = useMemo(() => {
    let keep = 0, watch = 0, exitReview = 0
    let shadowExitNow = 0, disagreements = 0, noShadow = 0
    let shadowOk = 0, shadowError = 0
    filteredRows.forEach((r) => {
      const v = String(r.REAL_VERDICT || '').toUpperCase()
      if (v === 'KEEP') keep += 1
      else if (v === 'WATCH') watch += 1
      else if (v === 'EXIT_REVIEW') exitReview += 1

      const a = String(r.AGREEMENT_LABEL || '').toUpperCase()
      if (a === 'NO_SHADOW') noShadow += 1
      else if (a && a !== 'AGREE') disagreements += 1

      const shadow = String(r.SHADOW_VERDICT || '').toUpperCase()
      const bias = String(r.SHADOW_ACTION_BIAS || '').toUpperCase()
      if (shadow === 'EXIT_REVIEW' && bias === 'EXIT_NOW') shadowExitNow += 1

      const status = String(r.SHADOW_RUN_STATUS || '').toUpperCase()
      if (status === 'OK') shadowOk += 1
      else if (status === 'PARSE_ERROR' || status === 'API_ERROR') shadowError += 1
    })
    return {
      total: filteredRows.length,
      keep, watch, exitReview,
      shadowExitNow, disagreements, noShadow,
      shadowOk, shadowError,
    }
  }, [filteredRows])

  const lifecycleSummary = useMemo(() => {
    let open = 0, simExited = 0
    let avgReturn = null
    const exitedReturns = []
    lifecycle.forEach((r) => {
      const status = String(r.SHADOW_STATUS || '').toUpperCase()
      if (status === 'OPEN') open += 1
      if (status === 'SIM_EXITED') {
        simExited += 1
        const v = Number(r.REALIZED_RETURN_SHADOW_PCT)
        if (Number.isFinite(v)) exitedReturns.push(v)
      }
    })
    if (exitedReturns.length > 0) {
      avgReturn = exitedReturns.reduce((s, v) => s + v, 0) / exitedReturns.length
    }
    return { open, simExited, avgReturn }
  }, [lifecycle])

  const toggleExpand = useCallback(async (key) => {
    if (!key) return
    setExpanded((prev) => ({ ...prev, [key]: !prev[key] }))
    if (historyByKey[key] || historyLoading[key]) return
    setHistoryLoading((prev) => ({ ...prev, [key]: true }))
    try {
      const res = await fetch(`${API_BASE}/position-health/history/${encodeURIComponent(key)}`)
      if (!res.ok) throw new Error(`history ${res.status}`)
      const data = await res.json()
      setHistoryByKey((prev) => ({ ...prev, [key]: data || { real: [], shadow: [] } }))
    } catch (e) {
      setHistoryByKey((prev) => ({ ...prev, [key]: { error: e.message || String(e), real: [], shadow: [] } }))
    } finally {
      setHistoryLoading((prev) => ({ ...prev, [key]: false }))
    }
  }, [historyByKey, historyLoading])

  const runShadow = useCallback(async () => {
    setRunning(true)
    setRunMessage('')
    setRunError('')
    try {
      const res = await fetch(`${API_BASE}/position-health/run-shadow`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ as_of_date: runDate, force: true }),
      })
      const text = await res.text()
      let payload = null
      try { payload = text ? JSON.parse(text) : null } catch { /* ignore */ }
      if (!res.ok) {
        const detail = payload?.detail || text || res.statusText
        throw new Error(typeof detail === 'string' ? detail : JSON.stringify(detail))
      }
      const positions = payload?.positions_total ?? '?'
      const ok = payload?.reviews_success ?? '?'
      const parseErr = payload?.reviews_parse_error ?? 0
      const apiErr = payload?.reviews_api_error ?? 0
      const status = payload?.status || 'DONE'
      const errSuffix = parseErr || apiErr ? ` (${parseErr} parse, ${apiErr} api errors)` : ''
      setRunMessage(`${status} for ${runDate} - positions: ${positions}, ok: ${ok}${errSuffix}.`)
      await loadAll()
    } catch (e) {
      setRunError(e.message || String(e))
    } finally {
      setRunning(false)
    }
  }, [runDate, loadAll])

  if (loading) return <LoadingState message="Loading position health..." />
  if (error) return <ErrorState message={error} />

  return (
    <div className="ph-page">
      <header className="ph-header">
        <h1>Position Health</h1>
        <p>
          Daily check on every open position. <b>Real</b> is the deterministic rule-based verdict;{' '}
          <b>shadow</b> is the Cortex review of the original thesis. Both are advisory — nothing
          here alters positions or fires orders. This view will move into Live Portfolio and
          Cockpit; it lives under Diagnostics for now.
        </p>
      </header>

      <section className="ph-run-bar">
        <div className="ph-run-bar-left">
          <label className="ph-run-label">
            <span>Run shadow review for</span>
            <input
              type="date"
              value={runDate}
              onChange={(e) => setRunDate(e.target.value)}
              disabled={running}
            />
          </label>
          <button
            type="button"
            className="ph-btn ph-btn-primary"
            onClick={runShadow}
            disabled={running || !runDate}
            title="Re-runs the Cortex health-review agent for every open position on this business date."
          >
            {running ? 'Running...' : 'Run shadow review'}
          </button>
          <button
            type="button"
            className="ph-btn"
            onClick={loadAll}
            disabled={running}
          >
            Refresh
          </button>
        </div>
        <div className="ph-run-bar-right">
          {runMessage ? <span className="ph-run-msg ok">{runMessage}</span> : null}
          {runError ? <span className="ph-run-msg bad">{runError}</span> : null}
        </div>
      </section>

      <section className="ph-kpi-grid">
        <KpiCard label="Open positions tracked" value={summary.total} />
        <KpiCard label="Real KEEP"        value={summary.keep}        tone="keep" />
        <KpiCard label="Real WATCH"       value={summary.watch}       tone="watch" />
        <KpiCard label="Real EXIT_REVIEW" value={summary.exitReview}  tone="exit" />
        <KpiCard
          label="Shadow EXIT_NOW"
          value={summary.shadowExitNow}
          hint="shadow wants out today"
          tone="exit"
        />
        <KpiCard
          label="Real vs shadow disagreements"
          value={summary.disagreements}
          hint={summary.noShadow ? `${summary.noShadow} pending shadow` : 'all shadows reported'}
          tone="differ"
        />
        <KpiCard
          label="Shadow run health"
          value={summary.shadowOk}
          hint={summary.shadowError ? `${summary.shadowError} errors` : 'no errors'}
        />
      </section>

      <section className="ph-kpi-grid ph-kpi-grid--lifecycle">
        <KpiCard label="Shadow lifecycle - OPEN"        value={lifecycleSummary.open} />
        <KpiCard label="Shadow lifecycle - SIM_EXITED" value={lifecycleSummary.simExited} />
        <KpiCard
          label="Avg sim-exit return"
          value={lifecycleSummary.avgReturn != null ? fmtPctSigned(lifecycleSummary.avgReturn) : '—'}
          hint="hypothetical only - no orders fired"
        />
      </section>

      <section className="ph-filter-row">
        <strong>View:</strong>
        {[
          { id: 'all', label: 'All' },
          { id: 'exit_review', label: 'Real EXIT_REVIEW' },
          { id: 'watch', label: 'Real WATCH' },
          { id: 'disagreements', label: 'Disagreements' },
          { id: 'shadow_exit', label: 'Shadow wants exit' },
        ].map((f) => (
          <button
            key={f.id}
            type="button"
            className={`ph-chip ${filter === f.id ? 'active' : ''}`}
            onClick={() => setFilter(f.id)}
          >
            {f.label}
          </button>
        ))}
        {portfolioOptions.length > 1 ? (
          <label className="ph-portfolio-filter">
            <span>Portfolio</span>
            <select
              value={portfolioFilter}
              onChange={(e) => setPortfolioFilter(e.target.value)}
            >
              <option value="">All</option>
              {portfolioOptions.map((p) => (
                <option key={p} value={p}>#{p}</option>
              ))}
            </select>
          </label>
        ) : null}
        <span className="ph-row-count">{filteredRows.length} row(s)</span>
      </section>

      <section className="ph-table-wrap">
        <table className="ph-table">
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Held</th>
              <th>P&amp;L</th>
              <th>Real</th>
              <th>Shadow</th>
              <th>Why</th>
              <th>Attention</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {filteredRows.length === 0 ? (
              <tr><td colSpan={8} className="ph-empty">No positions for the current filter.</td></tr>
            ) : (
              filteredRows.map((r) => {
                const key = r.POSITION_EPISODE_KEY
                const isOpen = Boolean(expanded[key])
                const hist = historyByKey[key]
                const reason = plainEnglishReason(r)
                const attention = attentionFor(r)
                return (
                  <Fragment key={key}>
                    <tr>
                      <td>
                        <div className="ph-symbol">{r.SYMBOL || '—'}</div>
                        <div className="ph-side">
                          {r.SIDE || ''}{r.PORTFOLIO_ID != null ? ` · pf #${r.PORTFOLIO_ID}` : ''}
                        </div>
                      </td>
                      <td>{fmtNumber(r.DAYS_HELD, 0)}d</td>
                      <td className={Number(r.UNREALIZED_PNL_PCT) >= 0 ? 'ph-pos' : 'ph-neg'}>
                        {fmtPctSigned(r.UNREALIZED_PNL_PCT)}
                      </td>
                      <td>
                        <VerdictBadge verdict={r.REAL_VERDICT} severity={r.REAL_SEVERITY} />
                      </td>
                      <td>
                        <VerdictBadge verdict={r.SHADOW_VERDICT} severity={r.SHADOW_SEVERITY} />
                      </td>
                      <td className="ph-why-cell" title={reason}>{reason}</td>
                      <td>
                        {attention ? (
                          <span
                            className={`ph-attention ph-attention--${attention.tone}`}
                            title={attention.detail}
                          >
                            {attention.label}
                          </span>
                        ) : (
                          <span className="ph-empty-inline">—</span>
                        )}
                      </td>
                      <td>
                        <button
                          type="button"
                          className="ph-detail-btn"
                          onClick={() => toggleExpand(key)}
                        >
                          {isOpen ? 'Hide' : 'Detail'}
                        </button>
                      </td>
                    </tr>
                    {isOpen ? (
                      <tr className="ph-detail-row">
                        <td colSpan={8}>
                          <div className="ph-detail-meta">
                            <span><b>As-of:</b> {fmtDate(r.AS_OF_DATE)}</span>
                            <span><b>Real health:</b> {r.REAL_HEALTH_STATE || '—'}</span>
                            <span><b>Shadow bias:</b> {r.SHADOW_ACTION_BIAS || '—'}</span>
                            <span><b>Agreement:</b> <AgreementBadge label={r.AGREEMENT_LABEL} /></span>
                            <span><b>Shadow run:</b> <ShadowRunBadge status={r.SHADOW_RUN_STATUS} /></span>
                            {r.SHADOW_RUN_TS ? <span className="ph-subtle">at {fmtTs(r.SHADOW_RUN_TS)}</span> : null}
                            <span><b>Distance to invalidation:</b> {fmtPctAbs(r.DISTANCE_TO_INVALIDATION_PCT)}</span>
                          </div>
                          <div className="ph-dim-line ph-detail-dim">
                            Dimensions —
                            thesis:{r.REAL_THESIS_INTEGRITY || '—'} ·
                            path:{r.REAL_PATH_QUALITY || '—'} ·
                            regime:{r.REAL_REGIME_ALIGNMENT || '—'} ·
                            fragility:{r.REAL_FRAGILITY || '—'}
                          </div>
                          <div className="ph-detail-grid">
                            <div className="ph-detail-card">
                              <h4>Real summaries</h4>
                              <div className="ph-detail-line"><b>Observation:</b> {r.REAL_OBSERVATION_SUMMARY || '—'}</div>
                              <div className="ph-detail-line"><b>Verdict:</b> {r.REAL_VERDICT_SUMMARY || '—'}</div>
                              <div className="ph-detail-line"><b>Why:</b> {r.REAL_WHY_SUMMARY || '—'}</div>
                              <div className="ph-detail-line ph-subtle">
                                Baseline: {r.REAL_BASELINE_QUALITY || '—'} ·
                                Reason code: {r.REAL_PRIMARY_REASON_CODE || '—'}
                              </div>
                            </div>
                            <div className="ph-detail-card">
                              <h4>Shadow summaries</h4>
                              <div className="ph-detail-line"><b>Observation:</b> {r.SHADOW_OBSERVATION_SUMMARY || '—'}</div>
                              <div className="ph-detail-line"><b>Verdict:</b> {r.SHADOW_VERDICT_SUMMARY || '—'}</div>
                              <div className="ph-detail-line"><b>Why:</b> {r.SHADOW_WHY_SUMMARY || '—'}</div>
                              <div className="ph-detail-line ph-subtle">
                                Thesis status: {r.SHADOW_THESIS_STATUS || '—'}
                              </div>
                              {r.SHADOW_RATIONALE_TEXT ? (
                                <details>
                                  <summary style={{ cursor: 'pointer', fontSize: '0.78rem', marginTop: '0.4rem' }}>
                                    Full rationale
                                  </summary>
                                  <pre className="ph-rationale">{r.SHADOW_RATIONALE_TEXT}</pre>
                                </details>
                              ) : null}
                              {r.SHADOW_RUN_ERROR ? (
                                <div className="ph-detail-line ph-error-line">
                                  <b>Run error:</b> {r.SHADOW_RUN_ERROR}
                                </div>
                              ) : null}
                            </div>
                            <div className="ph-detail-card">
                              <h4>Lifecycle (shadow bake-off)</h4>
                              <div className="ph-detail-line">Status: <b>{r.SHADOW_STATUS || '—'}</b></div>
                              <div className="ph-detail-line">Sim exit date: {fmtDate(r.SHADOW_EXIT_DATE)}</div>
                              <div className="ph-detail-line">Sim exit price: {r.SHADOW_EXIT_PRICE != null ? fmtNumber(r.SHADOW_EXIT_PRICE, 4) : '—'}</div>
                              <div className="ph-detail-line">
                                Sim return: {r.REALIZED_RETURN_SHADOW_PCT != null ? fmtPctSigned(r.REALIZED_RETURN_SHADOW_PCT) : '—'}
                                {r.DAYS_HELD_SHADOW != null ? ` over ${r.DAYS_HELD_SHADOW}d` : ''}
                              </div>
                              <div className="ph-detail-line ph-subtle">{r.SHADOW_EXIT_TRIGGER || ''}</div>
                            </div>
                            <div className="ph-detail-card ph-detail-card--wide">
                              <h4>History (real vs shadow)</h4>
                              {historyLoading[key] ? (
                                <div className="ph-subtle">Loading history...</div>
                              ) : hist?.error ? (
                                <div className="ph-error-line">{hist.error}</div>
                              ) : (
                                <HistoryTable real={hist?.real || []} shadow={hist?.shadow || []} />
                              )}
                            </div>
                          </div>
                        </td>
                      </tr>
                    ) : null}
                  </Fragment>
                )
              })
            )}
          </tbody>
        </table>
      </section>
    </div>
  )
}

function HistoryTable({ real, shadow }) {
  const dates = useMemo(() => {
    const set = new Set()
    real.forEach((r) => set.add(String(r.AS_OF_DATE)))
    shadow.forEach((r) => set.add(String(r.AS_OF_DATE)))
    return Array.from(set).sort()
  }, [real, shadow])

  const realByDate = useMemo(() => {
    const map = new Map()
    real.forEach((r) => map.set(String(r.AS_OF_DATE), r))
    return map
  }, [real])

  const shadowByDate = useMemo(() => {
    const map = new Map()
    shadow.forEach((r) => map.set(String(r.AS_OF_DATE), r))
    return map
  }, [shadow])

  if (dates.length === 0) {
    return <div className="ph-subtle">No history yet for this position.</div>
  }

  return (
    <div className="ph-history-wrap">
      <table className="ph-history-table">
        <thead>
          <tr>
            <th>As-of</th>
            <th>Real</th>
            <th>Health</th>
            <th>Shadow</th>
            <th>Bias</th>
            <th>Shadow run</th>
          </tr>
        </thead>
        <tbody>
          {dates.map((d) => {
            const rv = realByDate.get(d) || {}
            const sv = shadowByDate.get(d) || {}
            return (
              <tr key={d}>
                <td>{fmtDate(d)}</td>
                <td><VerdictBadge verdict={rv.VERDICT} severity={rv.SEVERITY} /></td>
                <td>{rv.HEALTH_STATE || '—'}</td>
                <td><VerdictBadge verdict={sv.SHADOW_VERDICT} severity={sv.SHADOW_SEVERITY} /></td>
                <td>{sv.SHADOW_ACTION_BIAS || '—'}</td>
                <td><ShadowRunBadge status={sv.SHADOW_RUN_STATUS} /></td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
