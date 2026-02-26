import { useEffect, useState } from 'react'
import { API_BASE } from '../../App'
import LoadingState from '../../components/LoadingState'
import ErrorState from '../../components/ErrorState'
import { EvidenceBadge, fmtNum, IntradayHeader, HelpTip } from './IntradayTrainingCommon'
import './IntradayTraining.css'

function terrainColor(score) {
  const v = Number(score || 0)
  if (v >= 1.25) return '#dff6e5'
  if (v >= 0.5) return '#ecf7ff'
  if (v >= 0) return '#f8f9fb'
  if (v >= -1) return '#fff2e0'
  return '#fde6e6'
}

export default function IntradayTerrainExplorerPage() {
  const [topRows, setTopRows] = useState([])
  const [heatmapRows, setHeatmapRows] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    Promise.all([
      fetch(`${API_BASE}/intraday/terrain/top?limit=50`).then((r) => (r.ok ? r.json() : Promise.reject(new Error('Failed terrain top')))),
      fetch(`${API_BASE}/intraday/terrain/heatmap`).then((r) => (r.ok ? r.json() : Promise.reject(new Error('Failed terrain heatmap')))),
    ])
      .then(([top, heat]) => {
        if (cancelled) return
        setTopRows(top?.rows ?? [])
        setHeatmapRows(heat?.rows ?? [])
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [])

  if (loading) return <LoadingState />
  if (error) return <ErrorState message={error} />

  return (
    <div className="it-page">
      <IntradayHeader
        title="Terrain Explorer"
        subtitle="Explore where expected edge is highest and where uncertainty still dominates."
      />

      <div className="it-card">
        <h3>Top Terrain Opportunities <HelpTip text="Highest terrain score rows from deterministic latest snapshot <= as_of." /></h3>
        <div className="it-table-wrap">
          <table className="it-table">
            <thead>
              <tr>
                <th>Pattern</th>
                <th>Symbol</th>
                <th>State</th>
                <th>Score</th>
                <th>Edge</th>
                <th>Uncertainty</th>
                <th>Suitability</th>
                <th>Evidence</th>
              </tr>
            </thead>
            <tbody>
              {topRows.map((row, idx) => (
                <tr key={`${row.PATTERN_ID}-${row.SYMBOL}-${idx}`}>
                  <td>{row.PATTERN_ID}</td>
                  <td>{row.SYMBOL}</td>
                  <td>{row.STATE_BUCKET_ID}</td>
                  <td>{fmtNum(row.TERRAIN_SCORE, 3)}</td>
                  <td>{fmtNum(row.EDGE, 4)}</td>
                  <td>{fmtNum(row.UNCERTAINTY, 4)}</td>
                  <td>{fmtNum(row.SUITABILITY, 4)}</td>
                  <td><EvidenceBadge fallbackLevel={row.N_SIGNALS < 20 ? 'GLOBAL' : 'EXACT'} evidenceN={row.N_SIGNALS} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="it-card">
        <h3>Terrain Heatmap <HelpTip text="Average terrain score by pattern and state bucket. Darker green = better." /></h3>
        <div className="it-heatmap">
          {heatmapRows.map((row, idx) => (
            <div
              key={`${row.PATTERN_ID}-${row.STATE_BUCKET_ID}-${idx}`}
              className="it-heat-cell"
              style={{ background: terrainColor(row.TERRAIN_SCORE_AVG) }}
            >
              <div><strong>P{row.PATTERN_ID}</strong> / S{row.STATE_BUCKET_ID}</div>
              <div>Avg: {fmtNum(row.TERRAIN_SCORE_AVG, 3)}</div>
              <div>Std: {fmtNum(row.TERRAIN_SCORE_STDDEV, 3)}</div>
              <div>N: {fmtNum(row.CELL_COUNT, 0)}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
