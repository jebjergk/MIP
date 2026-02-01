import { Link } from 'react-router-dom'
import './ErrorState.css'

/**
 * Error state: clear message + "Open /debug" link for diagnostics.
 * Used when API calls fail (network, 404, 500, etc.).
 */
export default function ErrorState({ message }) {
  return (
    <div className="error-state">
      <p className="error-state-message">{message}</p>
      <p className="error-state-action">
        <Link to="/debug">Open /debug</Link> to see which endpoints fail.
      </p>
    </div>
  )
}
