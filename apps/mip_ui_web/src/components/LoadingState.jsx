import './LoadingState.css'

/**
 * Reusable loading state: shows a clear "Loading…" block so the page never looks blank.
 * Use while fetching data (portfolios, runs, training status, etc.).
 *
 * @param {string} [message] - Optional message (default: "Loading…")
 * @param {string} [className] - Optional extra class names
 */
export default function LoadingState({ message = 'Loading…', className = '' }) {
  return (
    <div className={`loading-state ${className}`.trim()} role="status" aria-live="polite">
      <p className="loading-state-message">{message}</p>
    </div>
  )
}
