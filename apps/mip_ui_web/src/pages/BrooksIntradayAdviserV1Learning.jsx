import { useMemo, useState } from 'react'
import BrooksIntradayLearningChart from './BrooksIntradayLearningChart'
import {
  findEvidenceForBar,
  findGridRow,
  formatOhlc,
  formatSessionSummaryLine,
  formatTradeRow,
} from './brooksAdviserV1Learning'
import { normalizeBarTs } from './brooksLearningBarSelection'

const TABS = [
  { id: 'grid', label: 'Observation Grid' },
  { id: 'timeline', label: 'Timeline' },
  { id: 'evidence', label: 'Brooks Evidence' },
  { id: 'trades', label: 'Trades' },
]

export default function BrooksIntradayAdviserV1Learning({
  payload,
  selectedBarTs,
  onSelectBar,
  registerRowRef,
  registerCandleRef,
  chartHeight = 420,
  allBars,
  chartScrollRef,
  gridScrollRef,
}) {
  const [activeTab, setActiveTab] = useState('grid')
  const [expandedCardId, setExpandedCardId] = useState(null)
  const [reasoningOpen, setReasoningOpen] = useState(false)
  const [expandedNoteRow, setExpandedNoteRow] = useState(null)
  const [selectedTradeId, setSelectedTradeId] = useState(null)

  const grid = payload?.observation_grid || []
  const summary = payload?.session_summary || {}
  const selectedRow = findGridRow(payload, selectedBarTs)
  const evidence = findEvidenceForBar(payload, selectedBarTs)
  const trades = payload?.completed_trades || []
  const timeline = payload?.adviser_timeline || []

  const highlightRange = useMemo(() => {
    const t = trades.find((x) => x.trade_id === selectedTradeId)
    if (!t?.entry_ts || !t?.exit_ts) return null
    return { fromTs: normalizeBarTs(t.entry_ts), toTs: normalizeBarTs(t.exit_ts) }
  }, [trades, selectedTradeId])

  const overlays = {
    ...(payload?.chart?.overlays || {}),
    adviser_mode: true,
  }

  const onTradeSelect = (trade) => {
    setSelectedTradeId(trade.trade_id)
    if (trade.entry_ts) onSelectBar?.(trade.entry_ts)
  }

  return (
    <div className="bil-adviser-v1-layout">
      <div className="bil-adviser-v1-summary-compact" aria-label="Session summary">
        <strong>{payload?.symbol}</strong>
        {' · '}
        {payload?.trading_date}
        {' · '}
        {formatSessionSummaryLine(summary)}
      </div>

      <div className="bil-adviser-v1-main">
        <div className="bil-adviser-v1-chart-pane">
          <div className="bil-learning-chart-scroll bil-adviser-v1-chart-scroll" ref={chartScrollRef}>
            <BrooksIntradayLearningChart
              bars={allBars}
              overlays={overlays}
              selectedTs={selectedBarTs}
              onSelectBar={onSelectBar}
              registerCandleRef={registerCandleRef}
              chartHeight={chartHeight}
              adviserChart
              highlightRange={highlightRange}
            />
          </div>
        </div>

        <aside className="bil-adviser-v1-context-pane" aria-label="Adviser context">
          <h2 className="bil-learning-section-label">Adviser context</h2>
          {selectedRow ? (
            <dl className="bil-adviser-v1-dl">
              <dt>Intraday regime</dt>
              <dd>{selectedRow.regime || '—'}</dd>
              <dt>Always-in bias</dt>
              <dd>{selectedRow.always_in_bias || '—'}</dd>
              <dt>Brooks market state</dt>
              <dd className="bil-adviser-pre">{selectedRow.brooks_market_state || evidence?.brooks_market_state || '—'}</dd>
              <dt>Setup</dt>
              <dd>
                {evidence?.setup_id || selectedRow.setup_state || '—'}
                {evidence?.setup_family ? ` · ${evidence.setup_family}` : ''}
              </dd>
              <dt>Current thesis</dt>
              <dd className="bil-adviser-pre">{selectedRow.current_thesis || evidence?.current_thesis || '—'}</dd>
            </dl>
          ) : (
            <p className="bil-note">Select a bar on the chart or grid.</p>
          )}
        </aside>
      </div>

      <div className="bil-adviser-v1-lower">
        <div className="bil-adviser-v1-tabs" role="tablist">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              type="button"
              role="tab"
              aria-selected={activeTab === tab.id}
              className={activeTab === tab.id ? 'bil-adviser-v1-tab--active' : 'bil-adviser-v1-tab'}
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="bil-adviser-v1-tab-panel" role="tabpanel">
          {activeTab === 'grid' ? (
            <div className="bil-adviser-v1-grid-wrap" ref={gridScrollRef}>
              <table className="bil-learning-grid bil-adviser-v1-grid bil-adviser-v1-grid--compact">
                <thead>
                  <tr>
                    <th>ET</th>
                    <th>OHLC</th>
                    <th>Regime</th>
                    <th>Wake</th>
                    <th>Action</th>
                    <th>Setup</th>
                    <th>Confirmation</th>
                    <th>Invalidation</th>
                    <th>Position</th>
                    <th aria-label="Note" />
                  </tr>
                </thead>
                <tbody>
                  {grid.map((row) => {
                    const rowKey = normalizeBarTs(row.bar_ts)
                    const sel = selectedBarTs && rowKey === normalizeBarTs(selectedBarTs)
                    const noteOpen = expandedNoteRow === row.bar_ts
                    return (
                      <tr
                        key={row.bar_ts}
                        ref={(el) => registerRowRef(row.bar_ts, el)}
                        className={sel ? 'bil-learning-grid-row--selected' : undefined}
                        onClick={() => onSelectBar(row.bar_ts)}
                      >
                        <td>{row.time_et}</td>
                        <td>{formatOhlc(row.ohlc)}</td>
                        <td>{row.regime || '—'}</td>
                        <td>{row.wake_reason || ''}</td>
                        <td>{row.adviser_action || '—'}</td>
                        <td>{row.setup_state}</td>
                        <td>{row.confirmation_state}</td>
                        <td>{row.invalidation_state}</td>
                        <td>{row.position_state}</td>
                        <td>
                          {row.short_explanation ? (
                            <button
                              type="button"
                              className="bil-adviser-note-toggle"
                              onClick={(e) => {
                                e.stopPropagation()
                                setExpandedNoteRow(noteOpen ? null : row.bar_ts)
                              }}
                            >
                              {noteOpen ? '▾' : '▸'}
                            </button>
                          ) : null}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          ) : null}

          {activeTab === 'timeline' ? (
            <ul className="bil-adviser-timeline-list">
              {timeline.map((ev) => {
                const key = normalizeBarTs(ev.bar_ts)
                const sel = normalizeBarTs(selectedBarTs) === key
                return (
                  <li key={`${ev.call_number}-${ev.bar_ts}`}>
                    <button
                      type="button"
                      className={sel ? 'bil-adviser-timeline-row--selected' : 'bil-adviser-timeline-row'}
                      onClick={() => onSelectBar(ev.bar_ts)}
                    >
                      <span>{ev.bar_ts_et}</span>
                      <span>{ev.wake_reason}</span>
                      <span>{ev.action}</span>
                    </button>
                  </li>
                )
              })}
            </ul>
          ) : null}

          {activeTab === 'evidence' ? (
            <div className="bil-adviser-evidence-compact">
              {evidence ? (
                <>
                  <p className="bil-note">
                    {evidence.bar_ts_et}
                    {' ET · '}
                    {evidence.wake_reason}
                    {' · '}
                    {evidence.action}
                  </p>
                  <p className="bil-adviser-retrieval-once">
                    <strong>Retrieval query for this Adviser call</strong>
                    <span className="bil-adviser-pre">{evidence.retrieval_query || '—'}</span>
                  </p>
                  <ul className="bil-adviser-card-list">
                    {(evidence.cards || []).map((c) => {
                      const open = expandedCardId === c.card_id
                      const rel = c.execution_relevance || c.adviser_class || ''
                      return (
                        <li key={c.card_id} className="bil-adviser-card-row">
                          <button
                            type="button"
                            className="bil-adviser-card-row-head"
                            onClick={() => setExpandedCardId(open ? null : c.card_id)}
                          >
                            <span className="bil-adviser-card-title">{c.concept_name || c.card_id}</span>
                            <span className="bil-adviser-card-rel">{rel}</span>
                          </button>
                          {open && c.display_text ? (
                            <pre className="bil-adviser-pre bil-adviser-card-detail">{c.display_text}</pre>
                          ) : null}
                        </li>
                      )
                    })}
                  </ul>
                  <button
                    type="button"
                    className="bil-adviser-reasoning-toggle"
                    onClick={() => setReasoningOpen(!reasoningOpen)}
                  >
                    Adviser reasoning
                    {reasoningOpen ? ' ▾' : ' ▸'}
                  </button>
                  {reasoningOpen ? (
                    <pre className="bil-adviser-pre bil-adviser-reasoning">{evidence.brooks_reasoning_summary || '—'}</pre>
                  ) : null}
                </>
              ) : (
                <p className="bil-note">Select an Adviser call bar to inspect Brooks evidence.</p>
              )}
            </div>
          ) : null}

          {activeTab === 'trades' ? (
            <div className="bil-adviser-trades-tab">
              {trades.length ? (
                <ul className="bil-adviser-trades-list">
                  {trades.map((t) => (
                    <li key={t.trade_id}>
                      <button
                        type="button"
                        className={selectedTradeId === t.trade_id ? 'bil-adviser-trade-row--selected' : 'bil-adviser-trade-row'}
                        onClick={() => onTradeSelect(t)}
                      >
                        {formatTradeRow(t)}
                      </button>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="bil-note">No trades in this session.</p>
              )}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  )
}
