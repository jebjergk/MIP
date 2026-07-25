import React, { useState, useMemo } from 'react'

const INTERESTING_TYPES = new Set([
  'STATE_CHANGED', 'SETUP_DETECTED', 'SETUP_ELIGIBLE', 'PROPOSAL_CREATED',
])

const ALL_TYPES = [
  'PROPOSAL_CREATED', 'SETUP_ELIGIBLE', 'SETUP_DETECTED', 'STATE_CHANGED',
  'SETUP_INVALIDATED', 'SETUP_EXPIRED', 'SETUP_STALE',
  'LEVEL_DETECTED', 'SETUP_STATUS_CHANGE',
]

const MAX_VISIBLE = 80

export default function StlEventRail({ events, onSelectSetup, selectedSetupId, get }) {
  const [typeFilter, setTypeFilter] = useState('')
  const [showNoise, setShowNoise] = useState(false)
  const [showAll, setShowAll] = useState(false)

  const typeCounts = useMemo(() => {
    const counts = {}
    events.forEach(e => {
      const t = get(e, 'EVENT_TYPE')
      counts[t] = (counts[t] || 0) + 1
    })
    return counts
  }, [events, get])

  const interestingCount = useMemo(() =>
    events.filter(e => INTERESTING_TYPES.has(get(e, 'EVENT_TYPE'))).length
  , [events, get])

  const filtered = useMemo(() => {
    let list = events
    if (typeFilter) {
      list = list.filter(e => get(e, 'EVENT_TYPE') === typeFilter)
    } else if (!showNoise) {
      list = list.filter(e => INTERESTING_TYPES.has(get(e, 'EVENT_TYPE')))
    }
    return list
  }, [events, typeFilter, showNoise, get])

  const visible = showAll ? filtered : filtered.slice(0, MAX_VISIBLE)

  if (!events.length) return null

  const noiseCount = events.length - interestingCount

  return (
    <div className="stl-rail">
      <h3>
        Event Timeline
        <span style={{ fontWeight: 400, fontSize: '0.82rem', color: '#6c757d', marginLeft: 8 }}>
          {interestingCount} key events
          {noiseCount > 0 && !showNoise && !typeFilter && (
            <> · {noiseCount} lifecycle changes hidden</>
          )}
        </span>
      </h3>

      <div className="stl-rail-filters">
        <span
          className={`stl-overlay-chip${!typeFilter && !showNoise ? ' stl-overlay-chip--on' : ''}`}
          onClick={() => { setTypeFilter(''); setShowNoise(false) }}
        >Key events</span>
        <span
          className={`stl-overlay-chip${!typeFilter && showNoise ? ' stl-overlay-chip--on' : ''}`}
          onClick={() => { setTypeFilter(''); setShowNoise(true) }}
        >All ({events.length})</span>
        <span className="stl-rail-sep" />
        {ALL_TYPES.filter(t => typeCounts[t]).map(t => (
          <span
            key={t}
            className={`stl-overlay-chip${typeFilter === t ? ' stl-overlay-chip--on' : ''}`}
            onClick={() => setTypeFilter(typeFilter === t ? '' : t)}
          >
            {t.replace(/_/g, ' ').toLowerCase()} ({typeCounts[t]})
          </span>
        ))}
      </div>

      <div className="stl-rail-list" style={{ maxHeight: 320, overflowY: 'auto' }}>
        {visible.length === 0 && (
          <div className="stl-rail-empty">No matching events</div>
        )}
        {visible.map((e, i) => {
          const etype = get(e, 'EVENT_TYPE') || ''
          const setupId = get(e, 'SETUP_EVENT_ID')
          const hasSetup = setupId && (etype.startsWith('SETUP_') || etype === 'PROPOSAL_CREATED')
          const isActive = hasSetup && setupId === selectedSetupId
          return (
            <div
              key={i}
              className={`stl-rail-row${isActive ? ' stl-rail-row--active' : ''}`}
              onClick={() => hasSetup && onSelectSetup(setupId)}
              style={hasSetup ? { cursor: 'pointer' } : { cursor: 'default' }}
            >
              <span className="stl-rail-date">{String(get(e, 'EVENT_DATE') || '').slice(0, 10)}</span>
              <span className={`stl-rail-badge stl-badge-${etype}`}>
                {etype.replace(/_/g, ' ')}
              </span>
              <span className="stl-rail-desc">{get(e, 'EVENT_DESCRIPTION')}</span>
              {get(e, 'PROPOSAL_ID') ? (
                <span className="stl-rail-family">#{get(e, 'PROPOSAL_ID')}</span>
              ) : null}
              {get(e, 'SETUP_FAMILY') && (
                <span className="stl-rail-family">{get(e, 'SETUP_FAMILY').replace(/_/g, ' ')}</span>
              )}
            </div>
          )
        })}
      </div>

      {filtered.length > MAX_VISIBLE && !showAll && (
        <button className="stl-range-btn" style={{ marginTop: 6 }} onClick={() => setShowAll(true)}>
          Show all {filtered.length} events
        </button>
      )}
    </div>
  )
}
