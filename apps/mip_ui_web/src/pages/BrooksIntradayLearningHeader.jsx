import { formatLearningMoney } from './brooksTradeLearningG1'

export default function BrooksIntradayLearningHeader({
  header,
  summary,
  noTrade,
  symbol,
  tradingDate,
  onOpenTechnicalView,
}) {
  const title = header?.title || (noTrade && symbol && tradingDate
    ? `${symbol} · ${tradingDate}`
    : 'Learning review')
  const shortTitle = title
    .replace('Monday', 'Mon')
    .replace('Tuesday', 'Tue')
    .replace('Wednesday', 'Wed')
    .replace('Thursday', 'Thu')
    .replace('Friday', 'Fri')
    .replace('Saturday', 'Sat')
    .replace('Sunday', 'Sun')

  return (
    <header className="bil-learning-header-strip">
      <div className="bil-learning-header-strip-main">
        <span className="bil-learning-strip-title">{shortTitle}</span>
        {noTrade ? (
          <>
            <span className="bil-learning-strip-sep">|</span>
            <span className="bil-learning-strip-no-trade">No simulated trade</span>
          </>
        ) : null}
        {summary && summary.entry_price != null ? (
          <>
            <span className="bil-learning-strip-sep">|</span>
            <span className={`bil-learning-strip-pnl ${summary.realized_pnl >= 0 ? 'bil-learning-pnl--pos' : 'bil-learning-pnl--neg'}`}>
              {formatLearningMoney(summary.realized_pnl)}
            </span>
            <span className="bil-learning-strip-sep">|</span>
            <span className="bil-learning-strip-stat">Entry ${summary.entry_price?.toFixed(2)}</span>
            <span className="bil-learning-strip-sep">|</span>
            <span className="bil-learning-strip-stat">Exit ${summary.exit_price?.toFixed(2)}</span>
            <span className="bil-learning-strip-sep">|</span>
            <span className="bil-learning-strip-stat">Qty {summary.quantity}</span>
            <span className="bil-learning-strip-sep">|</span>
            <span className="bil-learning-strip-stat">{summary.duration_label}</span>
            <span className="bil-learning-strip-sep">|</span>
            <span className="bil-learning-strip-stat">{summary.exit_reason_plain}</span>
          </>
        ) : summary && summary.realized_pnl != null && summary.adviser_calls == null ? (
          <>
            <span className="bil-learning-strip-sep">|</span>
            <span className={`bil-learning-strip-pnl ${summary.realized_pnl >= 0 ? 'bil-learning-pnl--pos' : 'bil-learning-pnl--neg'}`}>
              {formatLearningMoney(summary.realized_pnl)}
            </span>
          </>
        ) : null}
      </div>
      <button type="button" className="bil-learning-strip-tech" onClick={() => onOpenTechnicalView?.()}>
        Open technical trade review
      </button>
    </header>
  )
}
