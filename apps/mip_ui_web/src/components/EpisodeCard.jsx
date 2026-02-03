import { useState, useEffect } from 'react'
import { API_BASE } from '../App'
import LoadingState from './LoadingState'
import PortfolioMiniGridCharts, { GATE_LABELS } from './PortfolioMiniGridCharts'
import './EpisodeCard.css'

function formatTs(ts) {
  if (!ts) return '—'
  const s = String(ts)
  return s.slice(0, 10)
}

function formatDuration(startTs, endTs) {
  if (!startTs) return '—'
  const start = new Date(startTs)
  const end = endTs ? new Date(endTs) : new Date()
  const days = Math.ceil((end - start) / (1000 * 60 * 60 * 24))
  if (days < 1) return '< 1 day'
  if (days === 1) return '1 day'
  return `${days} days`
}

// Lifecycle badge component
function LifecycleBadge({ endReason, status, distributionAmount }) {
  if (status === 'ACTIVE') {
    return <span className="lifecycle-badge lifecycle-badge--active">● Active</span>
  }
  if (endReason === 'PROFIT_TARGET_HIT') {
    return (
      <span className="lifecycle-badge lifecycle-badge--crystallized" title="Profit target hit, gains crystallized">
        ✓ Crystallized
      </span>
    )
  }
  if (endReason === 'DRAWDOWN_STOP') {
    return (
      <span className="lifecycle-badge lifecycle-badge--stopped" title="Drawdown stop triggered">
        ⚠ Stopped
      </span>
    )
  }
  if (endReason === 'MANUAL_RESET') {
    return (
      <span className="lifecycle-badge lifecycle-badge--reset" title="Manually reset by user">
        ↺ Reset
      </span>
    )
  }
  if (endReason === 'BUST') {
    return (
      <span className="lifecycle-badge lifecycle-badge--bust" title="Bust threshold hit">
        ✕ Bust
      </span>
    )
  }
  return <span className="lifecycle-badge lifecycle-badge--ended">Ended</span>
}

export default function EpisodeCard({ episode, portfolioId, isActive }) {
  const [expanded, setExpanded] = useState(false)
  const [detail, setDetail] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const episodeId = episode?.episode_id ?? episode?.EPISODE_ID
  const startTs = episode?.start_ts ?? episode?.START_TS ?? ''
  const endTs = episode?.end_ts ?? episode?.END_TS ?? null
  const status = episode?.status ?? episode?.STATUS ?? '—'
  const endReason = episode?.end_reason ?? episode?.END_REASON ?? null
  const profileName = episode?.profile_name ?? episode?.PROFILE_NAME ?? '—'
  const totalReturn = episode?.total_return ?? episode?.total_return
  const maxDrawdown = episode?.max_drawdown ?? episode?.MAX_DRAWDOWN
  const tradesCount = episode?.trades_count ?? episode?.trades_count ?? 0
  const winDays = episode?.win_days ?? episode?.win_days
  const lossDays = episode?.loss_days ?? episode?.loss_days
  const gateChip = isActive ? (episode?.risk_status ?? 'SAFE') : (endReason === 'DRAWDOWN_STOP' || endReason === 'MANUAL_RESET' ? 'STOPPED' : 'SAFE')
  const distributionAmount = episode?.distribution_amount ?? episode?.DISTRIBUTION_AMOUNT
  const hasPaidOut = distributionAmount != null && Number(distributionAmount) > 0

  useEffect(() => {
    if (!expanded || !portfolioId || !episodeId || detail != null) return
    let cancelled = false
    setLoading(true)
    setError(null)
    fetch(`${API_BASE}/portfolios/${portfolioId}/episodes/${episodeId}`)
      .then((res) => {
        if (!res.ok) throw new Error(res.statusText)
        return res.json()
      })
      .then((d) => {
        if (!cancelled) {
          setDetail(d)
          setLoading(false)
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setError(e.message)
          setLoading(false)
        }
      })
    return () => { cancelled = true }
  }, [expanded, portfolioId, episodeId, detail])

  const duration = formatDuration(startTs, endTs)
  
  // Build story/narrative text
  let storyText = ''
  if (!endReason) {
    storyText = `Active episode running for ${duration}. Profile: ${profileName}.`
  } else if (endReason === 'PROFIT_TARGET_HIT') {
    storyText = hasPaidOut
      ? `Ran for ${duration}. Profit target achieved — €${Number(distributionAmount).toLocaleString(undefined, { minimumFractionDigits: 2 })} crystallized and withdrawn.`
      : `Ran for ${duration}. Profit target achieved.`
  } else if (endReason === 'DRAWDOWN_STOP') {
    storyText = `Ran for ${duration}. Drawdown protection triggered — entries blocked to prevent further losses.`
  } else if (endReason === 'MANUAL_RESET') {
    storyText = `Ran for ${duration}. Manually reset by user.`
  } else if (endReason === 'BUST') {
    storyText = `Ran for ${duration}. Bust threshold hit — portfolio liquidated.`
  } else {
    storyText = `Ran for ${duration}. Ended: ${endReason}. Profile: ${profileName}.`
  }

  return (
    <div className={`episode-card ${expanded ? 'episode-card--expanded' : ''} ${isActive ? 'episode-card--active' : ''}`}>
      <button
        type="button"
        className="episode-card-header"
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
      >
        <span className="episode-card-id">Episode {episodeId}</span>
        <span className="episode-card-dates" title="Date range">
          {formatTs(startTs)} → {endTs ? formatTs(endTs) : 'now'}
        </span>
        <LifecycleBadge endReason={endReason} status={status} distributionAmount={distributionAmount} />
        {hasPaidOut && (
          <span className="episode-card-chip episode-card-chip--paidout" title="Profits withdrawn at end of episode">
            💰 €{Number(distributionAmount).toLocaleString(undefined, { minimumFractionDigits: 2 })}
          </span>
        )}
        <span className="episode-card-profile">{profileName}</span>
        <span className="episode-card-expand-icon">{expanded ? '▼' : '▶'}</span>
      </button>
      <p className="episode-card-story">{storyText}</p>
      <dl className="episode-card-stats">
        <dt>Start equity</dt>
        <dd>{episode?.start_equity != null ? Number(episode.start_equity).toLocaleString(undefined, { minimumFractionDigits: 2 }) : (detail?.start_equity != null ? Number(detail.start_equity).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—')}</dd>
        <dt>End equity</dt>
        <dd>{detail?.end_equity != null ? Number(detail.end_equity).toLocaleString(undefined, { minimumFractionDigits: 2 }) : '—'}</dd>
        <dt>Total return</dt>
        <dd>{totalReturn != null ? (Number(totalReturn) * 100).toFixed(2) + '%' : '—'}</dd>
        <dt>Max drawdown</dt>
        <dd>{maxDrawdown != null ? Number(maxDrawdown).toFixed(2) + '%' : '—'}</dd>
        <dt>Trades</dt>
        <dd>{tradesCount}</dd>
        <dt>Win / Loss days</dt>
        <dd>{winDays != null && lossDays != null ? `${winDays} / ${lossDays}` : '—'}</dd>
        <dt>Peak open symbols</dt>
        <dd>{detail?.peak_open_symbols ?? episode?.peak_open_symbols ?? '—'}</dd>
      </dl>
      {expanded && (
        <div className="episode-card-charts">
          {loading && <LoadingState />}
          {error && <p className="episode-chart-error">{error}</p>}
          {detail && !loading && !error && (
            <PortfolioMiniGridCharts
              titlePrefix={`Episode #${episodeId}`}
              dateRange={{ start_ts: detail.start_ts, end_ts: detail.end_ts }}
              series={{
                equity: detail.equity_series ?? [],
                drawdown: detail.drawdown_series ?? [],
                tradesPerDay: detail.trades_per_day ?? [],
                regime: detail.regime_per_day ?? [],
              }}
              thresholds={detail.thresholds ?? {}}
              events={detail.events ?? []}
            />
          )}
        </div>
      )}
    </div>
  )
}
