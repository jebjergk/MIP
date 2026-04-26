/**
 * Priority Review block — surfaces high-signal exception items.
 *
 * Each list is capped at 5 items by the backend (PRIORITY_REVIEW_MAX_ROWS)
 * and includes a `more_count` field so the UI shows "+N more" with a
 * link to the full Position Health page when the underlying set is
 * larger than the cap.
 */
import { Link } from 'react-router-dom'

function CappedList({ title, capped, fallbackRoute, tone = 'info' }) {
  const items = capped?.items || []
  const more = Number(capped?.more_count || 0)
  if (!items.length && more === 0) return null
  return (
    <div className={`ck-co-pr-list ck-co-pr-list--${tone}`}>
      <div className="ck-co-pr-list-title">{title} ({capped?.total_count ?? items.length})</div>
      <ul className="ck-co-pr-list-items">
        {items.map(it => (
          <li key={it.position_episode_key}>
            <Link to={it.detail_route} className="ck-co-link">
              <strong>{it.symbol}</strong>
            </Link>{' '}
            <span className="ck-co-pr-why">{it.why_text}</span>
            {it.attention_label ? (
              <span className="ck-co-pr-chip"> · {it.attention_label}</span>
            ) : null}
          </li>
        ))}
      </ul>
      {more > 0 ? (
        <div className="ck-co-pr-more">
          <Link to={fallbackRoute || '/position-health'} className="ck-co-link">
            +{more} more &rarr;
          </Link>
        </div>
      ) : null}
    </div>
  )
}

export default function PriorityReviewBlock({ data }) {
  if (!data) return null
  const fallback = data.detail_index_route || '/position-health'
  const allEmpty =
    !data.real_exit_review_rows?.total_count &&
    !data.shadow_exit_now_rows?.total_count &&
    !data.disagreement_rows?.total_count &&
    !data.intraday_review_now_rows?.total_count &&
    !data.intraday_sell_now_rows?.total_count &&
    !data.pending_shadow_rows?.total_count

  return (
    <div className="ck-co-card ck-co-card--pr">
      <div className="ck-co-card-header">
        <h2 className="ck-co-card-title">Priority Review</h2>
        <Link to={fallback} className="ck-co-link">
          Open full Position Health &rarr;
        </Link>
      </div>
      {allEmpty ? (
        <p className="ck-co-empty">No items need priority review.</p>
      ) : (
        <div className="ck-co-pr-grid">
          <CappedList
            title="Intraday: Sell now"
            capped={data.intraday_sell_now_rows}
            fallbackRoute={fallback}
            tone="critical"
          />
          <CappedList
            title="Intraday: Review now"
            capped={data.intraday_review_now_rows}
            fallbackRoute={fallback}
            tone="warning"
          />
          <CappedList
            title="Real: Exit review"
            capped={data.real_exit_review_rows}
            fallbackRoute={fallback}
            tone="warning"
          />
          <CappedList
            title="Shadow: Exit now bias"
            capped={data.shadow_exit_now_rows}
            fallbackRoute={fallback}
            tone="warning"
          />
          <CappedList
            title="Real / Shadow disagreement"
            capped={data.disagreement_rows}
            fallbackRoute={fallback}
            tone="info"
          />
          <CappedList
            title="Pending shadow"
            capped={data.pending_shadow_rows}
            fallbackRoute={fallback}
            tone="info"
          />
        </div>
      )}
    </div>
  )
}
