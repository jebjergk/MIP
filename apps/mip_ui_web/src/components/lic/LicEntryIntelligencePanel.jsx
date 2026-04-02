import { useMemo } from 'react'

function safeText(v, fallback = '—') {
  if (v == null || v === '') return fallback
  if (typeof v === 'string' || typeof v === 'number') return String(v)
  return fallback
}

function fmtProb(p) {
  if (p == null || p === '') return '—'
  const x = Number(p)
  if (!Number.isFinite(x)) return '—'
  return `${(x * 100).toFixed(1)}%`
}

function fmtAvgR(v) {
  if (v == null || v === '') return '—'
  const x = Number(v)
  if (!Number.isFinite(x)) return '—'
  return `${(x * 100).toFixed(2)}%`
}

/**
 * Entry intelligence + committee + optional closeout — fed only from LIC bootstrap (no polling).
 */
export default function LicEntryIntelligencePanel({ lifecycle }) {
  const body = useMemo(() => {
    if (!lifecycle || typeof lifecycle !== 'object') {
      return { mode: 'none' }
    }
    if (!lifecycle.has_entry_intel_link) {
      return { mode: 'unlinked', reason: lifecycle.unavailable_reason }
    }
    return { mode: 'full', data: lifecycle }
  }, [lifecycle])

  if (body.mode === 'none') {
    return null
  }

  if (body.mode === 'unlinked') {
    return (
      <section className="lic-ei" aria-label="Entry analysis">
        <h4 className="lic-ei-title">Entry analysis</h4>
        <p className="lic-ei-muted">
          {safeText(body.reason, 'No linked pre-trade analysis for this position. Reload Snowflake bootstrap after a new entry is linked.')}
        </p>
      </section>
    )
  }

  const d = body.data
  const ea = d.entry_analysis || {}
  const ss = d.similar_setups || {}
  const cv = d.committee_vs_baseline || {}
  const oc = d.outcome
  const baselineWeak = ea.has_actionable_baseline === false

  return (
    <section className="lic-ei" aria-label="Entry analysis">
      <h4 className="lic-ei-title">Entry analysis</h4>
      <p className="lic-ei-lead">
        Pre-trade snapshot and committee context (loaded once with this page — refresh via &quot;Reload bootstrap&quot;).
      </p>

      <div className="lic-ei-grid">
        <div className="lic-ei-card">
          <h5 className="lic-ei-card-title">Pre-trade recommendation</h5>
          {baselineWeak ? (
            <p className="lic-ei-warn">Baseline unavailable or legacy — committee may have decided without a full analysis row.</p>
          ) : null}
          <dl className="lic-ei-dl">
            <div>
              <dt>Recommended action</dt>
              <dd>{safeText(ea.recommended_action)}</dd>
            </div>
            <div>
              <dt>Size band</dt>
              <dd>{safeText(ea.size_band)}</dd>
            </div>
            <div>
              <dt title="How strong the historical sample supports the call">Conviction</dt>
              <dd>{safeText(ea.confidence_band)} — {safeText(ea.confidence_note)}</dd>
            </div>
            <div>
              <dt title="How often similar setups showed meaningful drawdowns">Downside risk (history)</dt>
              <dd>{safeText(ea.downside_risk_band)} — {safeText(ea.downside_note)}</dd>
            </div>
            <div>
              <dt>Net edge (after cost floor)</dt>
              <dd>{ea.net_edge_display != null ? ea.net_edge_display : safeText(ea.expected_value_net)}</dd>
            </div>
          </dl>
          {ea.alpha_summary_text ? <p className="lic-ei-one-liner">{safeText(ea.alpha_summary_text)}</p> : null}
        </div>

        <div className="lic-ei-card">
          <h5 className="lic-ei-card-title">Similar setups (history)</h5>
          <p className="lic-ei-narrative">{safeText(ss.narrative)}</p>
          <p className="lic-ei-muted">
            Sample: <strong>{safeText(ss.sample_strength)}</strong> ({ss.sample_size != null ? `${ss.sample_size} rows` : '—'})
            {ss.sample_strength_note ? ` — ${ss.sample_strength_note}` : ''}
          </p>
          <div className="lic-ei-chips">
            <span className="lic-ei-chip">Up {fmtProb(ss.upside_probability)}</span>
            <span className="lic-ei-chip">Flat {fmtProb(ss.base_probability)}</span>
            <span className="lic-ei-chip">Down {fmtProb(ss.downside_probability)}</span>
          </div>
          <dl className="lic-ei-dl lic-ei-dl--compact">
            <div>
              <dt>Avg if up</dt>
              <dd>{fmtAvgR(ss.avg_up)}</dd>
            </div>
            <div>
              <dt>Avg if flat</dt>
              <dd>{fmtAvgR(ss.avg_base)}</dd>
            </div>
            <div>
              <dt>Avg if down</dt>
              <dd>{fmtAvgR(ss.avg_down)}</dd>
            </div>
          </dl>
        </div>

        <div className="lic-ei-card">
          <h5 className="lic-ei-card-title">Committee vs baseline</h5>
          <p className="lic-ei-committee-headline">{safeText(cv.headline)}</p>
          <p className="lic-ei-muted">{safeText(cv.detail)}</p>
          {cv.committee_recommendation ? (
            <p className="lic-ei-muted">
              Committee vote: <strong>{safeText(cv.committee_recommendation)}</strong>
            </p>
          ) : null}
          <p className="lic-ei-muted">{safeText(cv.justification_phrase)}</p>
          {cv.consensus_note ? <p className="lic-ei-note">{safeText(cv.consensus_note)}</p> : null}
        </div>

        <div className="lic-ei-card">
          <h5 className="lic-ei-card-title">Outcome</h5>
          {!oc ? (
            <p className="lic-ei-muted">
              Still open — outcome vs pre-trade expectation appears here after the exit is confirmed at the broker.
            </p>
          ) : (
            <>
              <dl className="lic-ei-dl">
                <div>
                  <dt title="Economic result on the position">Realized result</dt>
                  <dd>{safeText(oc.realized_outcome_label)}</dd>
                </div>
                <div>
                  <dt title="Compared to what the analysis implied at entry">vs expectation</dt>
                  <dd>{safeText(oc.alignment_label)}</dd>
                </div>
                <div>
                  <dt>Exit type</dt>
                  <dd>{safeText(oc.exit_type)}</dd>
                </div>
                <div>
                  <dt>Realized return</dt>
                  <dd>{oc.realized_return_display != null ? oc.realized_return_display : '—'}</dd>
                </div>
              </dl>
              <p className="lic-ei-muted lic-ei-small">{safeText(oc.alignment_explainer)}</p>
              {oc.summary_line ? <p className="lic-ei-one-liner">{safeText(oc.summary_line)}</p> : null}
            </>
          )}
        </div>
      </div>

      <details className="lic-ei-details">
        <summary>Technical details (audit)</summary>
        <dl className="lic-ei-dl lic-ei-dl--debug">
          <div>
            <dt>Entry action</dt>
            <dd><code>{safeText(d.entry_action_id)}</code></dd>
          </div>
          <div>
            <dt>Snapshot</dt>
            <dd><code>{safeText(d.debug?.snapshot_id)}</code></dd>
          </div>
          <div>
            <dt>Source version</dt>
            <dd>{safeText(d.debug?.eis_source_version)}</dd>
          </div>
          <div>
            <dt>Snapshot version no.</dt>
            <dd>{safeText(d.debug?.eis_version)}</dd>
          </div>
          <div>
            <dt>Override class</dt>
            <dd><code>{safeText(d.debug?.alpha_override_class_raw)}</code></dd>
          </div>
          <div>
            <dt>Alignment rule</dt>
            <dd><code>{safeText(d.debug?.comparison_rule_version)}</code></dd>
          </div>
          <div>
            <dt>Reason codes</dt>
            <dd>{Array.isArray(d.debug?.alignment_reason_codes) ? d.debug.alignment_reason_codes.join(', ') : '—'}</dd>
          </div>
          <div>
            <dt>Committee run</dt>
            <dd><code>{safeText(d.debug?.committee_run_id)}</code></dd>
          </div>
        </dl>
      </details>
    </section>
  )
}
