import { buildCommitteeLearningNote } from './committeeLearningNote.js'
import {
  alignmentBucket,
  alignmentPlainLanguage,
  committeeSentence,
  formatCommitteeNormalized,
  formatDateTime,
  formatMoney,
  formatReturnFraction,
  isReconAttention,
  labelPwRegretDriver,
  reconPlainSummary,
} from './formatters.js'

function Card({ title, children, className = '' }) {
  return (
    <section className={`pw-card ${className}`.trim()}>
      <div className="pw-card-header">
        <h3>{title}</h3>
      </div>
      {children}
    </section>
  )
}

export default function TradeReviewDetail({ row, meta }) {
  if (!row) {
    return (
      <div className="pw-card">
        <p className="tr-empty-hint">Select a closed trade in the table to open the case file.</p>
      </div>
    )
  }

  const note = buildCommitteeLearningNote(row, meta)
  const sym = row.symbol ?? row.SYMBOL ?? '—'
  const ab = alignmentBucket(row.alignment_class, row.outcome_class)
  const hasEis = row.has_eis === true || row.has_eis === 1
  const pwDisclaimer =
    meta?.pw_date_matching ||
    'Portfolio-day Parallel Worlds context on exit date — not a trade-path replay.'
  const reconDisclaimer =
    meta?.reconciliation_detail ||
    'Reconciliation reflects current symbol state, not guaranteed closeout-time state.'

  return (
    <div className="tr-detail-stack">
      <Card title="Trade snapshot">
        <p><strong>{sym}</strong></p>
        <p className="tr-card__sub">
          Entry {formatDateTime(row.entry_ts ?? row.ENTRY_TS)} · Exit {formatDateTime(row.exit_ts ?? row.EXIT_TS)}
        </p>
        <p>
          Realized return <strong>{formatReturnFraction(row.realized_return ?? row.REALIZED_RETURN)}</strong>
          {' · '}
          PnL <strong>{formatMoney(row.realized_pnl ?? row.REALIZED_PNL)}</strong>
        </p>
        <p>
          <span className="tr-badge-soft">{row.alignment_class ?? '—'}</span>
          {' '}
          <span className="tr-badge-soft">{row.outcome_class ?? '—'}</span>
        </p>
        <p className="tr-card__sub">{reconPlainSummary(row)}</p>
      </Card>

      <Card title="Entry expectation">
        {hasEis ? (
          <>
            <p>
              Expected return (net):{' '}
              <strong>{row.expected_return != null ? formatReturnFraction(Number(row.expected_return)) : '—'}</strong>
            </p>
            <p className="tr-card__sub">{row.expectation_summary || 'No summary text on file.'}</p>
          </>
        ) : (
          <p>No entry expectation snapshot (EIS) was linked for this closeout.</p>
        )}
      </Card>

      <Card title="Committee vs alpha">
        <p>{committeeSentence(row.committee_action_normalized, row.committee_action_raw)}</p>
        <p className="tr-card__sub">
          Raw recommendation: <code>{row.committee_action_raw ?? '—'}</code>
          {' · '}
          Normalized: {formatCommitteeNormalized(row.committee_action_normalized)}
        </p>
        <p className="tr-card__sub">
          Override class: {row.override_class ?? '—'} (committee stance relative to alpha baseline, when recorded).
        </p>
      </Card>

      <Card title="Realized outcome">
        <p>{alignmentPlainLanguage(ab)}</p>
        <p className="tr-card__sub">
          Alignment class: {row.alignment_class ?? '—'} · Outcome class: {row.outcome_class ?? '—'}
        </p>
      </Card>

      <Card title="Parallel Worlds context">
        <p className="tr-card__sub">{pwDisclaimer}</p>
        {row.best_pw_scenario_name ? (
          <>
            <p>
              Best scenario (portfolio-day): <strong>{row.best_pw_scenario_name}</strong>
            </p>
            <p className="tr-card__sub">
              Scenario return: {formatReturnFraction(row.best_pw_scenario_return)} · vs-actual delta (PnL):{' '}
              {formatMoney(row.best_pw_vs_actual_delta)}
            </p>
            <p>
              Regret amount: {formatMoney(row.pw_regret_amount)} · Driver:{' '}
              {labelPwRegretDriver(row.pw_regret_driver) ?? row.pw_regret_driver ?? '—'}
            </p>
          </>
        ) : (
          <p>No portfolio-day Parallel Worlds comparison was available for this exit date.</p>
        )}
      </Card>

      <Card title="Reconciliation / integrity" className="tr-card--muted">
        <p className="tr-card__sub">{reconDisclaimer}</p>
        <p>
          Class: <span className="tr-badge-soft">{row.reconciliation_class ?? '—'}</span>
          {' · '}
          Entry intel link: {row.has_entry_intel_link === true || row.has_entry_intel_link === 1 ? 'Yes' : 'No'}
        </p>
        <p className="tr-card__sub">State as of: {formatDateTime(row.recon_state_as_of_ts)}</p>
        {isReconAttention(row.reconciliation_class) ? (
          <p className="tr-card__sub">Review integrity before leaning on {'P&L'} interpretation.</p>
        ) : null}
      </Card>

      <section
        className={`pw-card tr-learnings${note.takeaway_type === 'caution' ? ' tr-learnings--caution' : ''}`}
      >
        <div className="pw-card-header">
          <h3>Committee learning note</h3>
        </div>
        <p>{note.body_text}</p>
        <div
          className={`tr-learnings__takeaway tr-learnings__takeaway--${
            note.takeaway_type === 'caution' ? 'caution' : 'pattern'
          }`}
        >
          {note.takeaway_text}
        </div>
      </section>
    </div>
  )
}

