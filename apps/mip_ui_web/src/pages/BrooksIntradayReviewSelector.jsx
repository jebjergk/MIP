import {
  REVIEW_MODE_SESSIONS,
  REVIEW_MODE_TRADES,
  chainKey,
  findChainInRun,
  findRunInCatalog,
  findV1Session,
  formatRunWeekLabel,
  formatShortDate,
  formatTradeOptionLabel,
  obsoleteChainSortedLast,
  selectionFromV1Session,
  selectionStatusLine,
  v1SessionKey,
} from './brooksLabReviewSelection'

function ReviewField({ label, children }) {
  return (
    <label className="bil-review-field">
      <span className="bil-review-field-label">{label}</span>
      {children}
    </label>
  )
}

export default function BrooksIntradayReviewSelector({
  catalog,
  selection,
  onSelectionChange,
  hideLegacyChains = false,
}) {
  const run = findRunInCatalog(catalog, selection?.runId)
  const chains = obsoleteChainSortedLast(run?.available_chains || [])
  const chain = run
    ? findChainInRun(run, selection?.contextAttemptId, selection?.simulationAttemptId)
    : null
  const trades = chain?.available_trades || []
  const sessions = run?.available_symbol_sessions || []
  const v1Sessions = catalog?.v1_validation_sessions || []
  const status = selectionStatusLine(selection, catalog)
  const reviewMode = selection?.reviewMode || REVIEW_MODE_TRADES

  const patch = (partial) => {
    onSelectionChange?.({ ...selection, ...partial, barTs: null })
  }

  const onRunChange = (runId) => {
    const nextRun = findRunInCatalog(catalog, runId)
    if (!nextRun) return
    if (hideLegacyChains) {
      const v1List = v1Sessions.length ? v1Sessions : []
      const match = v1List.find(
        (s) => s.run_id === runId || !s.run_id,
      ) || v1List[0]
      if (match) {
        onSelectionChange?.(selectionFromV1Session(match, runId))
        return
      }
      const sess = nextRun.available_symbol_sessions?.[0]
      onSelectionChange?.({
        runId,
        contextAttemptId: null,
        simulationAttemptId: null,
        reviewMode: REVIEW_MODE_SESSIONS,
        tradeId: null,
        symbol: sess?.symbol || nextRun.symbols?.[0],
        tradingDate: sess?.trading_date,
        barTs: null,
        adviserFoundationOnly: true,
      })
      return
    }
    const nextChain = (nextRun.available_chains || []).find((c) => c.canonical_pm_certification && !c.disabled)
      || (nextRun.available_chains || []).find((c) => c.official)
      || (nextRun.available_chains || []).find((c) => !c.disabled)
    const nextTrades = nextChain?.available_trades || []
    const firstTrade = nextTrades[0]
    onSelectionChange?.({
      runId,
      contextAttemptId: nextChain?.context_attempt_id,
      simulationAttemptId: nextChain?.simulation_attempt_id,
      reviewMode: REVIEW_MODE_TRADES,
      tradeId: firstTrade?.trade_id || null,
      symbol: firstTrade?.symbol || nextRun.symbols?.[0],
      tradingDate: firstTrade?.trading_date || nextRun.available_symbol_sessions?.[0]?.trading_date,
      barTs: null,
    })
  }

  const onChainChange = (value) => {
    const ch = chains.find((c) => chainKey(c) === value)
    if (!ch || ch.disabled) return
    const nextTrades = ch.available_trades || []
    const firstTrade = nextTrades[0]
    onSelectionChange?.({
      ...selection,
      contextAttemptId: ch.context_attempt_id,
      simulationAttemptId: ch.simulation_attempt_id,
      tradeId: firstTrade?.trade_id || null,
      symbol: firstTrade?.symbol || selection?.symbol,
      tradingDate: firstTrade?.trading_date || selection?.tradingDate,
      barTs: null,
    })
  }

  const onTradeChange = (tradeId) => {
    const trade = trades.find((t) => t.trade_id === tradeId)
    if (!trade) return
    onSelectionChange?.({
      ...selection,
      reviewMode: REVIEW_MODE_TRADES,
      tradeId: trade.trade_id,
      symbol: trade.symbol,
      tradingDate: trade.trading_date,
      barTs: null,
    })
  }

  const onReviewModeChange = (mode) => {
    if (mode === REVIEW_MODE_SESSIONS) {
      const sess = sessions.find(
        (s) => s.symbol === selection?.symbol && s.trading_date === selection?.tradingDate,
      ) || sessions[0]
      onSelectionChange?.({
        ...selection,
        reviewMode: REVIEW_MODE_SESSIONS,
        tradeId: null,
        symbol: sess?.symbol || selection?.symbol,
        tradingDate: sess?.trading_date || selection?.tradingDate,
        barTs: null,
      })
    } else {
      const first = trades[0]
      onSelectionChange?.({
        ...selection,
        reviewMode: REVIEW_MODE_TRADES,
        tradeId: first?.trade_id || null,
        symbol: first?.symbol || selection?.symbol,
        tradingDate: first?.trading_date || selection?.tradingDate,
        barTs: null,
      })
    }
  }

  const onV1SessionChange = (key) => {
    const sess = v1Sessions.find((s) => v1SessionKey(s) === key)
    if (!sess) return
    onSelectionChange?.(selectionFromV1Session(sess, selection?.runId || sess.run_id))
  }

  const selectedV1Key = (() => {
    const v1 = findV1Session(catalog, selection?.symbol, selection?.tradingDate)
    return v1 ? v1SessionKey(v1) : (v1Sessions[0] ? v1SessionKey(v1Sessions[0]) : '')
  })()

  const selectedTrade = trades.find((t) => t.trade_id === selection?.tradeId)

  return (
    <div className="bil-review-selector-wrap">
      {!hideLegacyChains ? (
      <div className="bil-review-mode-row">
        <span className="bil-review-field-label">Review mode</span>
        <select
          value={reviewMode}
          onChange={(e) => onReviewModeChange(e.target.value)}
          aria-label="Review mode"
        >
          <option value={REVIEW_MODE_TRADES}>Trades</option>
          <option value={REVIEW_MODE_SESSIONS}>All symbol sessions</option>
        </select>
      </div>
      ) : null}

      <div className="bil-review-selector">
        {hideLegacyChains && v1Sessions.length ? (
          <ReviewField label="Session">
            <select
              value={selectedV1Key}
              onChange={(e) => onV1SessionChange(e.target.value)}
              aria-label="V1.0 validation session"
            >
              {v1Sessions.map((s) => (
                <option key={v1SessionKey(s)} value={v1SessionKey(s)}>
                  {s.selector_label || s.label || `${s.symbol} · ${s.trading_date}`}
                </option>
              ))}
            </select>
          </ReviewField>
        ) : (
        <>
        <ReviewField label="Run">
          <select
            value={selection?.runId || ''}
            onChange={(e) => onRunChange(e.target.value)}
            aria-label="Run"
          >
            {(catalog?.runs || []).map((r) => (
              <option key={r.run_id} value={r.run_id}>
                {formatRunWeekLabel(r)}
                {!r.has_learning_view_data ? ' · No Learning data' : ''}
                {r.completed_trade_count != null ? ` · ${r.completed_trade_count} trades` : ''}
              </option>
            ))}
          </select>
        </ReviewField>

        {!hideLegacyChains ? (
          <ReviewField label="Review">
            <select
              value={chain ? chainKey(chain) : ''}
              onChange={(e) => onChainChange(e.target.value)}
              aria-label="Review chain"
            >
              {chains.map((c) => (
                <option
                  key={chainKey(c)}
                  value={chainKey(c)}
                  disabled={c.disabled}
                >
                  {c.primary_label}
                  {c.obsolete_certification ? ' (disabled)' : ''}
                </option>
              ))}
            </select>
          </ReviewField>
        ) : null}

        {!hideLegacyChains && reviewMode === REVIEW_MODE_TRADES ? (
          <ReviewField label="Trade">
            <select
              value={selection?.tradeId || ''}
              onChange={(e) => onTradeChange(e.target.value)}
              aria-label="Trade"
              disabled={!trades.length}
            >
              {!trades.length ? (
                <option value="">No trades</option>
              ) : (
                trades.map((t) => (
                  <option key={t.trade_id} value={t.trade_id}>
                    {formatTradeOptionLabel(t)}
                  </option>
                ))
              )}
            </select>
          </ReviewField>
        ) : !hideLegacyChains && reviewMode === REVIEW_MODE_SESSIONS ? (
          <>
            <ReviewField label="Symbol">
              <select
                value={selection?.symbol || ''}
                onChange={(e) => patch({ symbol: e.target.value, tradeId: null })}
                aria-label="Symbol"
              >
                {(run?.symbols || []).map((sym) => (
                  <option key={sym} value={sym}>{sym}</option>
                ))}
              </select>
            </ReviewField>
            <ReviewField label="Date">
              <select
                value={selection?.tradingDate || ''}
                onChange={(e) => patch({ tradingDate: e.target.value, tradeId: null })}
                aria-label="Trading date"
              >
                {[...new Set(sessions.map((s) => s.trading_date))].sort().map((td) => (
                  <option key={td} value={td}>{formatShortDate(td)}</option>
                ))}
              </select>
            </ReviewField>
          </>
        ) : null}
        </>
        )}
      </div>

      <p className="bil-review-status-line" aria-live="polite">
        <span>{status.reviewing}</span>
        <span className="bil-review-status-sep">·</span>
        <span>{status.run}</span>
        <span className="bil-review-status-sep">·</span>
        <span>{status.chain}</span>
        {reviewMode === REVIEW_MODE_TRADES && selectedTrade ? (
          <>
            <span className="bil-review-status-sep">·</span>
            <span className="bil-review-status-compact">{formatTradeOptionLabel(selectedTrade)}</span>
          </>
        ) : null}
      </p>


      {!trades.length && reviewMode === REVIEW_MODE_TRADES && chain ? (
        <p className="bil-note bil-review-empty-hint">
          No trades were generated for this review chain. Choose “All symbol sessions” to inspect why.
        </p>
      ) : null}
      {run && !run.has_learning_view_data ? (
        <p className="bil-note bil-review-empty-hint">
          This run is paused or does not yet have completed Learning View data.
        </p>
      ) : null}
    </div>
  )
}
