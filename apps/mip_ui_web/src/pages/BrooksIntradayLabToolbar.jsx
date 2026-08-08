import {
  findV1Session,
  selectionFromV1Session,
  selectionStatusLine,
  v1SessionKey,
} from './brooksLabReviewSelection'
import BrooksIntradayReviewSelector from './BrooksIntradayReviewSelector'

export default function BrooksIntradayLabToolbar({
  catalog,
  selection,
  view,
  onViewChange,
  onSelectionChange,
  onNewValidation,
  adviserFoundationOnly,
}) {
  const v1Sessions = catalog?.v1_validation_sessions || []
  const status = selectionStatusLine(selection, catalog)

  const onV1SessionChange = (key) => {
    const sess = v1Sessions.find((s) => v1SessionKey(s) === key)
    if (!sess) return
    onSelectionChange?.(selectionFromV1Session(sess, selection?.runId || sess.run_id))
  }

  const selectedV1Key = (() => {
    const v1 = findV1Session(catalog, selection?.symbol, selection?.tradingDate, selection?.contextAttemptId)
    return v1 ? v1SessionKey(v1) : (v1Sessions[0] ? v1SessionKey(v1Sessions[0]) : '')
  })()

  return (
    <header className="bil-lab-toolbar" role="banner">
      <div className="bil-lab-toolbar-brand">Brooks Intraday Lab</div>
      <div className="bil-lab-toolbar-tabs" role="tablist" aria-label="Lab view">
        <button
          type="button"
          role="tab"
          aria-selected={view === 'learning'}
          className={view === 'learning' ? 'bil-lab-toolbar-tab--active' : 'bil-lab-toolbar-tab'}
          onClick={() => onViewChange?.('learning')}
        >
          Learning View
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={view === 'technical'}
          className={view === 'technical' ? 'bil-lab-toolbar-tab--active' : 'bil-lab-toolbar-tab'}
          onClick={() => onViewChange?.('technical')}
        >
          Technical View
        </button>
      </div>
      <div className="bil-lab-toolbar-session">
        {adviserFoundationOnly && v1Sessions.length ? (
          <label className="bil-lab-toolbar-session-label">
            <span className="bil-review-field-label">Session</span>
            <select
              value={selectedV1Key}
              onChange={(e) => onV1SessionChange(e.target.value)}
              aria-label="Validation session"
            >
              {v1Sessions.map((s) => (
                <option key={v1SessionKey(s)} value={v1SessionKey(s)}>
                  {s.selector_label || s.label}
                </option>
              ))}
            </select>
          </label>
        ) : null}
        {!adviserFoundationOnly ? (
          <BrooksIntradayReviewSelector
            catalog={catalog}
            selection={selection}
            onSelectionChange={onSelectionChange}
            hideLegacyChains={false}
            compact
          />
        ) : null}
      </div>
      {adviserFoundationOnly ? (
        <button type="button" className="bil-lab-toolbar-validate" onClick={onNewValidation}>
          New Validation
        </button>
      ) : null}
      <p className="bil-lab-toolbar-status" aria-live="polite">
        {status.reviewing}
        {' · '}
        {status.run}
      </p>
    </header>
  )
}
