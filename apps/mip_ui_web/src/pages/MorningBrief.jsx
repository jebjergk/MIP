import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import { useExplainMode } from '../context/ExplainModeContext'
import { getGlossaryEntry } from '../data/glossary'

export default function MorningBrief() {
  const { explainMode } = useExplainMode()
  const statusBadgeTitle = explainMode ? getGlossaryEntry('ui', 'status_badge')?.long : undefined
  const [portfolios, setPortfolios] = useState([])
  const [portfoliosLoading, setPortfoliosLoading] = useState(true)
  const [portfolioId, setPortfolioId] = useState('')
  const [brief, setBrief] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setPortfoliosLoading(true)
    fetch(`${API_BASE}/portfolios`)
      .then((r) => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
      .then((list) => { if (!cancelled) setPortfolios(list) })
      .catch((e) => { if (!cancelled) setError(e.message) })
      .finally(() => { if (!cancelled) setPortfoliosLoading(false) })
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

  if (portfoliosLoading) return <p>Loading…</p>
  if (error && !portfolioId) {
    return (
      <>
        <h1>Morning Brief</h1>
        <ErrorState message={error} />
      </>
    )
  }

  if (portfolios.length === 0 && !error) {
    return (
      <>
        <h1>Morning Brief</h1>
        <EmptyState
          title="No portfolios yet"
          action={<>Run pipeline, then <Link to="/portfolios">pick a portfolio</Link>.</>}
          explanation="Briefs are per portfolio. Load portfolios first by running the pipeline."
          reasons={['Pipeline has not run yet.', 'No portfolios in MIP.APP.PORTFOLIO.']}
        />
      </>
    )
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
      {error && <ErrorState message={error} />}
      {!brief && !error && portfolioId && (
        <EmptyState
          title="No brief loaded"
          action="Click Load latest brief above."
          explanation="Select a portfolio and load to fetch its latest morning brief."
        />
      )}
      {brief && (
        <>
          <h2>Latest brief {brief.STATUS != null && <><span className="status-badge" title={statusBadgeTitle}>{brief.STATUS}</span></>}</h2>
          <pre>{JSON.stringify(brief, null, 2)}</pre>
        </>
      )}
    </>
  )
}
