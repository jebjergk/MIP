/**
 * Compact shadow vs real summary block.
 *
 * Aggregate counts only. Per spec, the cockpit must NOT embed full
 * Bake-off analysis; users should follow the link for diagnostics.
 */
import { Link } from 'react-router-dom'

export default function ShadowDisagreementSummary({ data }) {
  if (!data) return null
  const total =
    (data.harsher_count || 0) +
    (data.softer_count || 0) +
    (data.no_shadow_count || 0) +
    (data.exit_now_count || 0)
  if (total === 0) return null

  return (
    <div className="ck-co-card ck-co-card--shadow">
      <div className="ck-co-card-header">
        <h2 className="ck-co-card-title">Shadow vs Real</h2>
        <Link to="/diagnostics/bake-off" className="ck-co-link">
          Open Bake-off &rarr;
        </Link>
      </div>
      <div className="ck-co-shadow-grid">
        <div className="ck-co-shadow-cell ck-co-shadow-cell--critical">
          <div className="ck-co-kpi-value">{data.exit_now_count || 0}</div>
          <div className="ck-co-kpi-label">Exit-now bias</div>
        </div>
        <div className="ck-co-shadow-cell ck-co-shadow-cell--warning">
          <div className="ck-co-kpi-value">{data.harsher_count || 0}</div>
          <div className="ck-co-kpi-label">Harsher than real</div>
        </div>
        <div className="ck-co-shadow-cell">
          <div className="ck-co-kpi-value">{data.softer_count || 0}</div>
          <div className="ck-co-kpi-label">Softer than real</div>
        </div>
        <div className="ck-co-shadow-cell ck-co-shadow-cell--neutral">
          <div className="ck-co-kpi-value">{data.no_shadow_count || 0}</div>
          <div className="ck-co-kpi-label">No shadow yet</div>
        </div>
      </div>
    </div>
  )
}
