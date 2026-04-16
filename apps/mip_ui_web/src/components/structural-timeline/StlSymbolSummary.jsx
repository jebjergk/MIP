import React, { useMemo } from 'react'

export default function StlSymbolSummary({ setups, summary, get }) {
  const stats = useMemo(() => {
    if (!setups || !setups.length) return null

    const familyCounts = {}
    const failureCounts = {}
    const stateCounts = {}
    let detected = 0, eligible = 0, proposed = 0, traded = 0

    setups.forEach(s => {
      const fam = get(s, 'SETUP_FAMILY') || 'Unknown'
      const dir = get(s, 'DIRECTION') || ''
      const key = `${fam}|${dir}`
      familyCounts[key] = (familyCounts[key] || 0) + 1

      const fm = get(s, 'FAILURE_MODE')
      if (fm) failureCounts[fm] = (failureCounts[fm] || 0) + 1

      const st = get(s, 'STRUCTURAL_STATE')
      if (st) stateCounts[st] = (stateCounts[st] || 0) + 1

      detected++
      if (get(s, 'SETUP_STATUS') === 'ELIGIBLE' || get(s, 'ELIGIBLE_SINCE')) eligible++
      if (get(s, 'BECAME_PROPOSAL')) { proposed++; traded++ }
    })

    const sortedFamilies = Object.entries(familyCounts)
      .map(([k, c]) => { const [fam, dir] = k.split('|'); return { fam, dir, count: c } })
      .sort((a, b) => b.count - a.count)

    const sortedFailures = Object.entries(failureCounts)
      .map(([mode, count]) => ({ mode, count }))
      .sort((a, b) => b.count - a.count)

    const sortedStates = Object.entries(stateCounts)
      .map(([state, count]) => ({ state, count }))
      .sort((a, b) => b.count - a.count)
    const totalStateBars = sortedStates.reduce((s, v) => s + v.count, 0)

    return { sortedFamilies, sortedFailures, sortedStates, totalStateBars, detected, eligible, proposed, traded }
  }, [setups, get])

  if (!stats) return null

  const funnelTotal = Math.max(stats.detected, 1)
  const funnelData = [
    { label: 'Detected', value: stats.detected, color: '#9e9e9e' },
    { label: 'Eligible', value: stats.eligible, color: '#42a5f5' },
    { label: 'Proposed', value: stats.proposed, color: '#f9a825' },
  ]

  const stateColors = {
    BREAKOUT_EXPANSION: '#26a69a', TREND_UP: '#66bb6a', PULLBACK_IN_UPTREND: '#a5d6a7',
    RANGE_BOUND: '#9e9e9e', RANGE_CONTRACTION: '#bdbdbd',
    PULLBACK_IN_DOWNTREND: '#ef9a9a', TREND_DOWN: '#ef5350', BREAKDOWN: '#c62828', RECOVERY: '#42a5f5',
  }

  return (
    <div className="stl-sym-summary">
      {/* Family mix */}
      <div className="stl-sym-card">
        <h4>Setup Family Mix</h4>
        <table className="stl-mini-table">
          <tbody>
            {stats.sortedFamilies.slice(0, 8).map(f => (
              <tr key={`${f.fam}-${f.dir}`}>
                <td>{f.fam.replace(/_/g, ' ')} ({f.dir})</td>
                <td>{f.count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Proposal funnel */}
      <div className="stl-sym-card">
        <h4>Proposal Funnel</h4>
        <div className="stl-funnel-bar">
          {funnelData.map(f => {
            const w = Math.max((f.value / funnelTotal) * 100, f.value > 0 ? 8 : 0)
            return (
              <div key={f.label} className="stl-funnel-seg" style={{ width: `${w}%`, background: f.color }}>
                {f.value > 0 ? f.value : ''}
              </div>
            )
          })}
        </div>
        <div style={{ fontSize: '0.75rem', color: '#6c757d', display: 'flex', gap: 12 }}>
          {funnelData.map(f => (
            <span key={f.label}>{f.label}: {f.value}</span>
          ))}
        </div>
      </div>

      {/* State distribution */}
      <div className="stl-sym-card">
        <h4>State Distribution</h4>
        <div className="stl-funnel-bar">
          {stats.sortedStates.map(s => {
            const w = (s.count / stats.totalStateBars) * 100
            return (
              <div
                key={s.state}
                className="stl-funnel-seg"
                style={{ width: `${w}%`, background: stateColors[s.state] || '#bdbdbd' }}
                title={`${s.state.replace(/_/g, ' ')}: ${s.count}`}
              />
            )
          })}
        </div>
        <table className="stl-mini-table">
          <tbody>
            {stats.sortedStates.slice(0, 6).map(s => (
              <tr key={s.state}>
                <td>{s.state.replace(/_/g, ' ')}</td>
                <td>{s.count} ({((s.count / stats.totalStateBars) * 100).toFixed(0)}%)</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Failure modes */}
      {stats.sortedFailures.length > 0 && (
        <div className="stl-sym-card">
          <h4>Failure Modes</h4>
          <table className="stl-mini-table">
            <tbody>
              {stats.sortedFailures.slice(0, 6).map(f => (
                <tr key={f.mode}>
                  <td>{(f.mode || '').replace(/_/g, ' ')}</td>
                  <td>{f.count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
