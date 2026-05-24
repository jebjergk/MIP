import React from 'react'
import LoadingState from '../LoadingState'

function pct(v) { return v != null ? `${(Number(v) * 100).toFixed(1)}%` : '—' }
function num(v, d = 2) { return v != null ? Number(v).toFixed(d) : '—' }
function badge(label, cls) { return <span className={`stl-badge stl-badge-${cls}`}>{label}</span> }

function trustBadge(label) {
  const l = (label || '').toUpperCase()
  const cls = l === 'TRUSTED' ? 'trusted' : l === 'PROVISIONAL' ? 'provisional' : l === 'RESEARCH' ? 'research' : l === 'REJECTED' ? 'rejected' : 'unknown'
  return badge(l || 'UNKNOWN', cls)
}

// PROPOSAL_SKIP_REASON comes from V_STRUCTURAL_TIMELINE_SETUPS and reflects a
// setup-family trust / regime gate — NOT the agentic board's execution policy.
// We map the raw code to a human-readable label so users do not confuse
// RESEARCH_ONLY (setup trust) with SHORT_LIVE_DISABLED (execution policy).
const SKIP_REASON_LABELS = {
  RESEARCH_ONLY: 'Family trust = RESEARCH (not TRUSTED/PROVISIONAL)',
  FAMILY_TRUST_RESEARCH: 'Family trust = RESEARCH (not TRUSTED/PROVISIONAL)',
  INSUFFICIENT_TRUST: 'Trust label unknown',
  NO_ACTIVE_POLICY: 'No active risk policy for family',
  REJECTED_TRUST: 'Family rejected by trust model',
  WEAK_REGIME: 'Regime compatibility POOR',
}

function skipReasonLabel(code) {
  if (!code) return null
  return SKIP_REASON_LABELS[code] || code.replace(/_/g, ' ')
}

function execPolicyBadge(status) {
  const s = (status || '').toUpperCase()
  if (!s || s === 'EXECUTABLE') return badge('EXECUTABLE', 'success')
  return badge(s, 'fail')
}

function dirBadge(dir) {
  const d = (dir || '').toUpperCase()
  return badge(d, d === 'LONG' ? 'long' : 'short')
}

function Row({ label, value }) {
  return (
    <>
      <div className="stl-detail-label">{label}</div>
      <div className="stl-detail-value">{value ?? '—'}</div>
    </>
  )
}

export default function StlSetupDetail({ detail, loading, onClose, get }) {
  if (loading) return <div className="stl-detail"><LoadingState /></div>
  if (!detail || !detail.setup) return null

  const s = detail.setup
  const v = (k) => get(s, k)
  const cmp = detail.symbol_vs_family || {}

  const narrative = v('SETUP_NARRATIVE') || v('RATIONALE_TEXT')
  const skipReason = v('PROPOSAL_SKIP_REASON')

  return (
    <div className="stl-detail">
      <div className="stl-detail-header">
        <h3>
          {(v('SETUP_FAMILY') || '').replace(/_/g, ' ')} — {v('SYMBOL')} {dirBadge(v('DIRECTION'))}
        </h3>
        <button className="stl-detail-close" onClick={onClose}>&times;</button>
      </div>

      {/* Narrative */}
      {narrative && (
        <div className="stl-detail-narrative">{narrative}</div>
      )}

      <div className="stl-detail-sections">
        {/* Identity */}
        <div className="stl-detail-section">
          <h4>Identity</h4>
          <div className="stl-detail-grid">
            <Row label="Symbol" value={v('SYMBOL')} />
            <Row label="Date" value={String(v('SETUP_DATE') || '').slice(0, 10)} />
            <Row label="Family" value={(v('SETUP_FAMILY') || '').replace(/_/g, ' ')} />
            <Row label="Direction" value={dirBadge(v('DIRECTION'))} />
            <Row label="Market Type" value={v('MARKET_TYPE')} />
            <Row label="Status" value={v('SETUP_STATUS')} />
            <Row label="Trust Label" value={trustBadge(v('TRUST_LABEL'))} />
            <Row label="Proposal Ready" value={v('IS_PROPOSAL_READY') ? badge('Yes', 'success') : badge('No', 'fail')} />
          </div>
        </div>

        {/* Structural Evidence */}
        <div className="stl-detail-section">
          <h4>Structural Evidence</h4>
          <div className="stl-detail-grid">
            <Row label="Level Type" value={v('LEVEL_TYPE')} />
            <Row label="Level Price" value={`$${num(v('LEVEL_PRICE'))}`} />
            <Row label="Level Significance" value={num(v('LEVEL_SIGNIFICANCE'))} />
            <Row label="Structure Confidence" value={num(v('STRUCTURE_CONFIDENCE'))} />
            <Row label="Wick Score" value={num(v('WICK_CONFIRMATION_SCORE'))} />
            <Row label="3-Bar Score" value={num(v('THREE_BAR_CONFIRMATION_SCORE'))} />
            <Row label="Trend Context" value={num(v('TREND_CONTEXT_SCORE'))} />
            <Row label="State" value={(v('STRUCTURAL_STATE') || '').replace(/_/g, ' ')} />
            <Row label="Regime Compat" value={v('REGIME_COMPAT')} />
          </div>
        </div>

        {/* Risk / Management */}
        <div className="stl-detail-section">
          <h4>Risk &amp; Management</h4>
          <div className="stl-detail-grid">
            <Row label="Entry Zone" value={`$${num(v('ENTRY_ZONE_LOW'))} – $${num(v('ENTRY_ZONE_HIGH'))}`} />
            <Row label="Invalidation" value={`$${num(v('PRICE_INVALIDATION_LEVEL'))}`} />
            <Row label="Invalidation Rule" value={v('INVALIDATION_RULE')} />
            <Row label="Trail Style" value={v('TRAIL_STYLE')} />
            <Row label="Risk Class" value={v('RISK_CLASS')} />
            <Row label="Eligible Since" value={String(v('ELIGIBLE_SINCE') || '').slice(0, 10) || '—'} />
            <Row label="Expiry" value={String(v('EXPIRY_DATE') || '').slice(0, 10) || '—'} />
            <Row label="Bars Since Detection" value={v('BARS_SINCE_DETECTION')} />
          </div>
        </div>

        {/* Outcome */}
        <div className="stl-detail-section">
          <h4>Outcome</h4>
          <div className="stl-detail-grid">
            <Row label="Eval Status" value={v('EVAL_STATUS') || 'Pending'} />
            <Row label="MFE" value={v('MFE_PCT') != null ? `${num(v('MFE_PCT'))}%` : '—'} />
            <Row label="MAE" value={v('MAE_PCT') != null ? `${num(v('MAE_PCT'))}%` : '—'} />
            <Row label="MFE/MAE Ratio" value={num(v('MFE_MAE_RATIO'))} />
            <Row label="Meaningful Move" value={v('MEANINGFUL_MOVE_SUCCESS') === true ? badge('Yes', 'success') : v('MEANINGFUL_MOVE_SUCCESS') === false ? badge('No', 'fail') : '—'} />
            <Row label="Invalidation Hit" value={v('INVALIDATION_HIT') === true ? badge('Yes', 'fail') : v('INVALIDATION_HIT') === false ? badge('No', 'success') : '—'} />
            <Row label="Failure Mode" value={(v('FAILURE_MODE') || '').replace(/_/g, ' ') || '—'} />
            <Row label="Became Proposal" value={v('BECAME_PROPOSAL') ? badge('Yes', 'success') : badge('No', 'fail')} />
            {skipReason && <Row label="Skip Reason" value={skipReasonLabel(skipReason)} />}
          </div>
          {skipReason && (skipReason === 'RESEARCH_ONLY' || skipReason === 'FAMILY_TRUST_RESEARCH') && (
            <div className="stl-detail-note" style={{ fontSize: '0.8rem', marginTop: 8, color: '#6b7280' }}>
              This is a setup-family trust gate, not a short execution block. Live short
              execution policy is evaluated separately at the agentic board / LPA layer.
            </div>
          )}
        </div>

        {/* Symbol vs Pooled Family */}
        {Object.keys(cmp).length > 0 && (
          <div className="stl-detail-section">
            <h4>Symbol vs Family</h4>
            <table className="stl-comparison-table">
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Symbol</th>
                  <th>Family</th>
                  <th>Verdict</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>MHR</td>
                  <td>{pct(get(cmp, 'SYMBOL_MHR'))}</td>
                  <td>{pct(get(cmp, 'FAMILY_MHR'))}</td>
                  <td className={evidenceClass(get(cmp, 'EVIDENCE_STRENGTH'))}>{(get(cmp, 'EVIDENCE_STRENGTH') || '').replace(/_/g, ' ')}</td>
                </tr>
                <tr>
                  <td>MFE/MAE</td>
                  <td>{num(get(cmp, 'SYMBOL_MFE_MAE'))}</td>
                  <td>{num(get(cmp, 'FAMILY_MFE_MAE'))}</td>
                  <td>{num(get(cmp, 'VS_FAMILY_MHR_DIFF'), 3)}</td>
                </tr>
                <tr>
                  <td>Sample</td>
                  <td>{get(cmp, 'N_SETUPS') ?? '—'}</td>
                  <td colSpan={2}>—</td>
                </tr>
              </tbody>
            </table>
          </div>
        )}

        {/* Proposal details */}
        {v('BECAME_PROPOSAL') && v('RATIONALE_TEXT') && (
          <div className="stl-detail-section">
            <h4>Proposal Rationale</h4>
            <div style={{ fontSize: '0.85rem', lineHeight: 1.5 }}>{v('RATIONALE_TEXT')}</div>
            <div className="stl-detail-grid" style={{ marginTop: 8 }}>
              <Row label="Proposal ID" value={v('PROPOSAL_ID')} />
              <Row label="Proposal Status" value={v('PROPOSAL_STATUS')} />
              <Row label="Execution Policy" value={execPolicyBadge(v('EXECUTION_POLICY_STATUS'))} />
              {v('EXECUTION_POLICY_REASON') && (
                <Row label="Policy Reason" value={(v('EXECUTION_POLICY_REASON') || '').replace(/_/g, ' ')} />
              )}
              {v('IS_RESEARCH_ONLY') === true && (
                <Row label="Research Only" value={badge('Yes', 'fail')} />
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function evidenceClass(strength) {
  const s = (strength || '').toUpperCase()
  if (s === 'STRONGER') return 'stl-cmp-stronger'
  if (s === 'WEAKER') return 'stl-cmp-weaker'
  return 'stl-cmp-aligned'
}
