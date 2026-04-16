import React, { useState, useMemo } from 'react'

const EVENT_TYPES = [
  'STATE_CHANGED', 'SETUP_DETECTED', 'SETUP_ELIGIBLE',
  'SETUP_INVALIDATED', 'SETUP_EXPIRED', 'SETUP_STALE',
  'PROPOSAL_CREATED', 'LEVEL_DETECTED', 'SETUP_STATUS_CHANGE',
]

const MAX_VISIBLE = 80

export default function StlEventRail({ events, onSelectSetup, get }) {
  const [typeFilter, setTypeFilter] = useState('')
  const [showAll, setShowAll] = useState(false)

  const filtered = useMemo(() => {
    let list = events
    if (typeFilter) list = list.filter(e => get(e, 'EVENT_TYPE') === typeFilter)
    return list
  }, [events, typeFilter, get])

  const visible = showAll ? filtered : filtered.slice(0, MAX_VISIBLE)

  const typeCounts = useMemo(() => {
    const counts = {}
    events.forEach(e => {
      const t = get(e, 'EVENT_TYPE')
      counts[t] = (counts[t] || 0) + 1
    })
    return counts
  }, [events, get])

  if (!events.length) return null

  return (
    <div className="stl-rail">
      <h3>Event Timeline ({filtered.length})</h3>

      <div className="stl-rail-filters">
        <span
          className={`stl-overlay-chip${!typeFilter ? ' stl-overlay-chip--on' : ''}`}
          onClick={() => setTypeFilter('')}
        >All</span>
        {EVENT_TYPES.filter(t => typeCounts[t]).map(t => (
          <span
            key={t}
            className={`stl-overlay-chip${typeFilter === t ? ' stl-overlay-chip--on' : ''}`}
            onClick={() => setTypeFilter(typeFilter === t ? '' : t)}
          >
            {t.replace(/_/g, ' ').toLowerCase()} ({typeCounts[t]})
          </span>
        ))}
      </div>

      <div className="stl-rail-list">
        {visible.map((e, i) => {
          const etype = get(e, 'EVENT_TYPE') || ''
          const setupId = get(e, 'SETUP_EVENT_ID')
          const hasSetup = setupId && etype.startsWith('SETUP_')
          return (
            <div
              key={i}
              className="stl-rail-row"
              onClick={() => hasSetup && onSelectSetup(setupId)}
              style={hasSetup ? { cursor: 'pointer' } : { cursor: 'default' }}
            >
              <span className="stl-rail-date">{String(get(e, 'EVENT_DATE') || '').slice(0, 10)}</span>
              <span className={`stl-rail-badge stl-badge-${etype}`}>
                {etype.replace(/_/g, ' ')}
              </span>
              <span className="stl-rail-desc">{get(e, 'EVENT_DESCRIPTION')}</span>
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
