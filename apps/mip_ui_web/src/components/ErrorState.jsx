import { Link } from 'react-router-dom'
import './ErrorState.css'

/**
 * Error state: clear message + consistent "Open Debug" link so every page
 * offers a path forward when Snowflake or the API is down.
 * Used when API calls fail (network, 404, 500, etc.).
 */
export default function ErrorState({ message }) {
  return (
    <div className="error-state">
      <p className="error-state-message">{message}</p>
      <p className="error-state-action">
        <Link to="/debug" className="error-state-link">Open Debug</Link>
        {' '}to see which endpoints are failing and get diagnostics.
      </p>
    </div>
  )
}
