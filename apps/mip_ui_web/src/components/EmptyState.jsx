import './EmptyState.css'

/**
 * Reusable empty-state block: plain-language explanation + likely reasons.
 * Always shows all content (no Explain Mode dependency).
 */
export default function EmptyState({
  title,
  explanation,
  reasons = [],
  action,
  className = '',
}) {
  return (
    <div className={`empty-state ${className}`.trim()}>
      <p className="empty-state-title">{title}</p>
      {action && <p className="empty-state-action">{action}</p>}
      {explanation && <p className="empty-state-explanation">{explanation}</p>}
      {reasons.length > 0 && (
        <p className="empty-state-reasons">
          <strong>Common reasons:</strong> {reasons.join(' ')}
        </p>
      )}
    </div>
  )
}
