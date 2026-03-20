import { useEffect, useState } from 'react'
import { API_BASE } from '../App'

export default function AskCoverageDashboardPanel() {
  const [coverage, setCoverage] = useState([])
  const [unknown, setUnknown] = useState([])

  useEffect(() => {
    let mounted = true
    Promise.all([
      fetch(`${API_BASE}/ask/telemetry/coverage?limit=14`).then((r) => (r.ok ? r.json() : { items: [] })),
      fetch(`${API_BASE}/ask/telemetry/unknown-terms?limit=10`).then((r) => (r.ok ? r.json() : { items: [] })),
    ]).then(([coverageData, unknownData]) => {
      if (!mounted) return
      setCoverage(coverageData.items || [])
      setUnknown(unknownData.items || [])
    })
    return () => {
      mounted = false
    }
  }, [])

  return (
    <section style={{ marginTop: 24 }}>
      <h3>Ask MIP Coverage Snapshot</h3>
      <div className="perf-simple-table">
        {(coverage || []).slice(0, 7).map((row) => (
          <div className="perf-row" key={`${row.DAY}`}>
            <span>{String(row.DAY || '').slice(0, 10)}</span>
            <strong>{row.TOTAL_QUERIES || 0} queries</strong>
          </div>
        ))}
      </div>
      <h4 style={{ marginTop: 12 }}>Top Unknown Terms</h4>
      <ul>
        {(unknown || []).slice(0, 10).map((row) => (
          <li key={row.UNKNOWN_TERM}>
            {row.UNKNOWN_TERM}: {row.ASK_COUNT}
          </li>
        ))}
      </ul>
    </section>
  )
}
