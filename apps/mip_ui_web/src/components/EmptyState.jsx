import { useExplainMode } from '../context/ExplainModeContext'
import './EmptyState.css'

/**
 * Reusable empty-state block: plain-language explanation + likely reasons.
 * Used in: Portfolio (no positions/trades), Signals (no eligible signals), Proposals (no proposals).
 * When Explain mode is OFF, shows only the short title (no helper callout).
 *
 * Example for Signals page: <EmptyState title="No eligible signals" explanation="..."
 *   reasons={['No signals passed the score or trust threshold.', 'Data may not be fresh yet.', 'Risk gate may be blocking.']} />
 * @param {React.ReactNode} [action] - Next action (e.g. Link, "Run pipeline")
 * Example for Proposals page: <EmptyState title="No proposals" explanation="..."
 *   reasons={['No orders were suggested for this run.', 'Signals may have been filtered out.', 'Risk or threshold rules may apply.']} />
 */
export default function EmptyState({
  title,
  explanation,
  reasons = [],
  action,
  className = '',
}) {
  const { explainMode } = useExplainMode()

  return (
    <div className={`empty-state ${className}`.trim()}>
      <p className="empty-state-title">{title}</p>
      {action && <p className="empty-state-action">{action}</p>}
      {explainMode && (
        <>
          {explanation && <p className="empty-state-explanation">{explanation}</p>}
          {reasons.length > 0 && (
            <p className="empty-state-reasons">
              <strong>Common reasons:</strong> {reasons.join(' ')}
            </p>
          )}
        </>
      )}
    </div>
  )
}
