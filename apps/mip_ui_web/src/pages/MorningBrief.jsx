import { useState, useEffect } from 'react'
import { API_BASE } from '../App'
import { useExplainMode } from '../context/ExplainModeContext'
import { getGlossaryEntry } from '../data/glossary'

export default function MorningBrief() {
  const { explainMode } = useExplainMode()
  const statusBadgeTitle = explainMode ? getGlossaryEntry('ui', 'status_badge')?.long : undefined
  const [portfolios, setPortfolios] = useState([])
  const [portfolioId, setPortfolioId] = useState('')
  const [brief, setBrief] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch(`${API_BASE}/portfolios`)
      .then((r) => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
      .then((list) => { if (!cancelled) setPortfolios(list) })
      .catch((e) => { if (!cancelled) setError(e.message) })
    return () => { cancelled = true }
  }, [])

  const loadBrief = async () => {
    const id = portfolioId.trim()
    if (!id) return
    setLoading(true)
    setError(null)
    setBrief(null)
    try {
      const res = await fetch(`${API_BASE}/briefs/latest?portfolio_id=${id}`)
      if (!res.ok) throw new Error(res.statusText)
      const data = await res.json()
      setBrief(data)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <>
      <h1>Morning Brief</h1>
      <p>Latest morning brief per portfolio.</p>
      <p>
        <label>
          Portfolio ID:{' '}
          <select
            value={portfolioId}
            onChange={(e) => setPortfolioId(e.target.value)}
          >
            <option value="">Select…</option>
            {portfolios.map((p) => (
              <option key={p.PORTFOLIO_ID} value={p.PORTFOLIO_ID}>
                {p.NAME} ({p.PORTFOLIO_ID})
              </option>
            ))}
          </select>
        </label>
        {' '}
        <button type="button" onClick={loadBrief} disabled={!portfolioId || loading}>
          {loading ? 'Loading…' : 'Load latest brief'}
        </button>
      </p>
      {error && <p>Error: {error}</p>}
      {brief && (
        <>
          <h2>Latest brief {brief.STATUS != null && <><span className="status-badge" title={statusBadgeTitle}>{brief.STATUS}</span></>}</h2>
          <pre>{JSON.stringify(brief, null, 2)}</pre>
        </>
      )}
    </>
  )
}
