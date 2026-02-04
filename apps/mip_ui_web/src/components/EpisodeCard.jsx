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

  const reasonText = !endReason
    ? 'Active'
    : endReason === 'MANUAL_RESET'
      ? 'Manual reset'
      : endReason === 'DRAWDOWN_STOP'
        ? 'Drawdown stop'
        : endReason === 'PROFIT_TARGET_HIT'
          ? (hasPaidOut ? `Profit crystallized; paid out €${Number(distributionAmount).toLocaleString(undefined, { minimumFractionDigits: 2 })}` : 'Profit target hit')
          : endReason
  const explainLine = endReason ? `Ended: ${reasonText}. Profile: ${profileName}.` : `Active. Profile: ${profileName}.`

  return (
    <div className={`episode-card ${expanded ? 'episode-card--expanded' : ''}`}>
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
        <span className={`episode-card-chip episode-card-chip--${(status || '').toLowerCase()}`}>
          {status || '—'}
        </span>
        <span className={`episode-card-chip episode-card-chip--gate episode-card-chip--${String(gateChip || 'safe').toLowerCase()}`} title="Gate state">
          {GATE_LABELS[gateChip] || gateChip}
        </span>
        {hasPaidOut && (
          <span className="episode-card-chip episode-card-chip--paidout" title="Profits withdrawn at end of episode">
            Paid out: €{Number(distributionAmount).toLocaleString(undefined, { minimumFractionDigits: 2 })}
          </span>
        )}
        <span className="episode-card-profile">{profileName}</span>
      </button>
      <p className="episode-card-explain">{explainLine}</p>
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
