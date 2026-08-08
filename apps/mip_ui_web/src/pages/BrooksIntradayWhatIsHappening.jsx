import { useMemo, useState } from 'react'
import { buildEvidenceGroups, formatEvidenceGroupsForDisplay } from './brooksLearningEvidenceGroups'
import { explainEvidenceTerm } from './brooksLearningEvidenceGlossary'
import {
  buildCompactStory,
  formatCompactPositionRow,
} from './brooksLearningPanelCompact'

function CompactStoryBlock({ compact, stopTimeline }) {
  return (
    <div className="bil-wh-compact-story">
      {compact.market ? (
        <p className="bil-wh-row">
          <span className="bil-wh-label">Market</span>
          <span className="bil-wh-text">{compact.market}</span>
        </p>
      ) : null}
      {compact.system ? (
        <p className="bil-wh-row">
          <span className="bil-wh-label">System</span>
          <span className="bil-wh-text">{compact.system}</span>
        </p>
      ) : null}
      {compact.decision ? (
        <p className="bil-wh-row">
          <span className="bil-wh-label">Decision</span>
          <span className="bil-wh-text bil-wh-text--strong">{compact.decision}</span>
        </p>
      ) : null}
      {compact.risk ? (
        <p className="bil-wh-row">
          <span className="bil-wh-label">Risk</span>
          <span className="bil-wh-text">{compact.risk}</span>
        </p>
      ) : null}
      {compact.exitWhy ? (
        <p className="bil-wh-row">
          <span className="bil-wh-label">Exit</span>
          <span className="bil-wh-text">{compact.exitWhy}</span>
        </p>
      ) : null}
      {stopTimeline?.length ? (
        <p className="bil-wh-row bil-wh-row--timeline">
          <span className="bil-wh-label">Stop timing</span>
          <span className="bil-wh-text">
            {stopTimeline.map((step) => step.label).join(' → ')}
          </span>
        </p>
      ) : null}
    </div>
  )
}

function EvidenceCompact({ whyMattered, evidenceGroups }) {
  return (
    <div className="bil-wh-evidence-compact">
      {whyMattered ? (
        <div className="bil-wh-ev-block">
          <div className="bil-wh-ev-block-title">Why this mattered</div>
          <p className="bil-wh-ev-block-body">{whyMattered}</p>
        </div>
      ) : null}
      {evidenceGroups.map((sec) => (
        <div key={sec.title} className="bil-wh-ev-block">
          <div className="bil-wh-ev-block-title">{sec.title}</div>
          <div className="bil-wh-ev-chips">
            {sec.items.map((item, i) => {
              const plain = typeof item === 'string' ? item : item.plain
              const code = typeof item === 'string' ? null : item.code
              const hint = explainEvidenceTerm(code, plain)
              return (
                <span
                  key={`${sec.title}-${i}`}
                  className="bil-wh-ev-chip"
                  title={hint || plain}
                >
                  {plain}
                </span>
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}

export default function BrooksIntradayWhatIsHappening({ narrative, payloadSymbol }) {
  const [evidenceOpen, setEvidenceOpen] = useState(true)

  const evidenceBundle = useMemo(
    () => (narrative ? buildEvidenceGroups(narrative) : null),
    [narrative],
  )
  const evidenceGroups = useMemo(
    () => (evidenceBundle ? formatEvidenceGroupsForDisplay(evidenceBundle) : []),
    [evidenceBundle],
  )
  const whyMattered = evidenceBundle?.whyThisMattered

  const compact = useMemo(
    () => (narrative ? buildCompactStory(narrative.story, narrative.headline) : null),
    [narrative],
  )
  const positionLine = useMemo(
    () => formatCompactPositionRow(narrative?.position_at_bar),
    [narrative?.position_at_bar],
  )

  if (!narrative) {
    return (
      <section className="bil-wh-panel bil-wh-panel--compact" aria-label="What is happening">
        <h2 className="bil-wh-title">What is happening?</h2>
        <p className="bil-wh-placeholder">Select a candle or grid row to see the story for that moment.</p>
      </section>
    )
  }

  if (narrative.technical?.symbol && payloadSymbol
    && narrative.technical.symbol.toUpperCase() !== payloadSymbol.toUpperCase()) {
    return (
      <section className="bil-wh-panel bil-wh-panel--compact" aria-label="What is happening">
        <h2 className="bil-wh-title">What is happening?</h2>
        <p className="bil-wh-placeholder">No review data for this symbol at this bar.</p>
      </section>
    )
  }

  return (
    <section className="bil-wh-panel bil-wh-panel--compact" aria-label="What is happening">
      <header className="bil-wh-head-compact">
        <h2 className="bil-wh-title">What is happening?</h2>
        <p className="bil-wh-lede">
          {narrative.time_ny}
          {' — '}
          {narrative.headline}
        </p>
        <div className="bil-wh-panel-toggle" role="group" aria-label="Panel sections">
          <span className="bil-wh-panel-toggle-label bil-wh-panel-toggle-label--active">Story</span>
          <button
            type="button"
            className={evidenceOpen ? 'bil-wh-panel-toggle-btn bil-wh-panel-toggle-btn--active' : 'bil-wh-panel-toggle-btn'}
            onClick={() => setEvidenceOpen((v) => !v)}
            aria-pressed={evidenceOpen}
          >
            {evidenceOpen ? 'Hide evidence' : 'Show evidence'}
          </button>
        </div>
      </header>

      <div className="bil-wh-body-scroll">
        <CompactStoryBlock compact={compact} stopTimeline={compact?.stopTimeline} />

        {positionLine ? (
          <p className="bil-wh-position-line">{positionLine}</p>
        ) : null}

        {evidenceOpen ? (
          <EvidenceCompact whyMattered={whyMattered} evidenceGroups={evidenceGroups} />
        ) : null}

        <details className="bil-wh-disclosure bil-wh-disclosure--technical">
          <summary>Technical</summary>
          <pre className="bil-wh-technical">{JSON.stringify(narrative.technical, null, 2)}</pre>
        </details>
      </div>
    </section>
  )
}
