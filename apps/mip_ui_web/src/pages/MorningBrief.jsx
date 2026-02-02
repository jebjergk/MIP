import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import InfoTooltip from '../components/InfoTooltip'
import LoadingState from '../components/LoadingState'
import { useExplainCenter } from '../context/ExplainCenterContext'
import { MORNING_BRIEF_EXPLAIN_CONTEXT } from '../data/explainContexts'
import './MorningBrief.css'

export default function MorningBrief() {
  const [portfolios, setPortfolios] = useState([])
  const [portfoliosLoading, setPortfoliosLoading] = useState(true)
  const [portfolioId, setPortfolioId] = useState('')
  const [briefResponse, setBriefResponse] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const { setContext } = useExplainCenter()

  useEffect(() => {
    setContext(MORNING_BRIEF_EXPLAIN_CONTEXT)
  }, [setContext])

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
    setBriefResponse(null)
    try {
      const res = await fetch(`${API_BASE}/briefs/latest?portfolio_id=${id}`)
      if (!res.ok) throw new Error(res.statusText)
      const data = await res.json()
      setBriefResponse(data)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  const found = briefResponse?.found === true
  const brief = found ? briefResponse : null

  if (portfoliosLoading) {
    return (
      <>
        <h1>Morning Brief</h1>
        <LoadingState />
      </>
    )
  }
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
      {!portfolioId && !loading && (
        <EmptyState
          title="Select a portfolio"
          action="Choose a portfolio above and click Load latest brief."
          explanation="Briefs are generated per portfolio by the daily pipeline."
        />
      )}
      {portfolioId && !loading && !error && briefResponse == null && (
        <EmptyState
          title="No brief loaded"
          action="Click Load latest brief above to fetch the latest brief for this portfolio."
          explanation="Briefs are generated per portfolio by the daily pipeline."
        />
      )}
      {portfolioId && !loading && !error && briefResponse?.found === false && (
        <EmptyState
          title="No brief exists yet for this portfolio"
          action={<>Run the daily pipeline for this portfolio, then load again.</>}
          explanation={briefResponse?.message ?? 'Briefs are written when the pipeline runs and writes morning briefs for each portfolio.'}
          reasons={['Pipeline has not run yet for this portfolio.', 'No brief row in MIP.AGENT_OUT.MORNING_BRIEF.']}
        />
      )}
      {brief && (
        <>
          <section className="brief-summary-card" aria-label="Brief summary">
            <h2>Brief Summary</h2>
            <dl className="brief-summary-dl">
              <dt>As-of <InfoTooltip scope="brief" key="as_of_ts" variant="short" /></dt>
              <dd>{brief.as_of_ts ?? '—'}</dd>
              <dt>Pipeline run id <InfoTooltip scope="brief" key="pipeline_run_id" variant="short" /></dt>
              <dd><code>{brief.pipeline_run_id ?? '—'}</code></dd>
              <dt>Agent name</dt>
              <dd>{brief.agent_name ?? '—'}</dd>
            </dl>
            <details className="brief-json-details">
              <summary>Full brief JSON</summary>
              <pre className="brief-json-pre">{JSON.stringify(brief.brief_json ?? brief, null, 2)}</pre>
            </details>
          </section>
        </>
      )}
    </>
  )
}
