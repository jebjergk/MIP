import { useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../App'
import LoadingState from '../components/LoadingState'
import ErrorState from '../components/ErrorState'
import EmptyState from '../components/EmptyState'
import './NewsIntelligence.css'

function fmtMins(v) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  if (n < 60) return `${Math.round(n)}m`
  const h = Math.floor(n / 60)
  const m = Math.round(n % 60)
  return m ? `${h}h ${m}m` : `${h}h`
}

function fmtNum(v, digits = 2) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toFixed(digits)
}

function fmtTs(ts) {
  if (!ts) return '—'
  try {
    return new Date(ts).toLocaleString()
  } catch {
    return ts
  }
}

function fmtSigned(v, digits = 3) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  const s = n > 0 ? '+' : ''
  return `${s}${n.toFixed(digits)}`
}

export default function NewsIntelligence() {
  const [data, setData] = useState(null)
  const [portfolios, setPortfolios] = useState([])
  const [portfolioId, setPortfolioId] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetch(`${API_BASE}/portfolios`)
      .then((r) => (r.ok ? r.json() : Promise.resolve([])))
      .then((rows) => setPortfolios(Array.isArray(rows) ? rows : []))
      .catch(() => setPortfolios([]))
  }, [])

  useEffect(() => {
    const params = new URLSearchParams()
    if (portfolioId) params.set('portfolio_id', portfolioId)
    setLoading(true)
    setError(null)
    fetch(`${API_BASE}/news/intelligence?${params.toString()}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((payload) => setData(payload))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [portfolioId])

  const cards = useMemo(() => data?.symbol_cards || [], [data])
  const bullets = useMemo(() => data?.summary_bullets || [], [data])
  const topHeadlines = useMemo(() => data?.market_context?.top_headlines || [], [data])
  const overlay = data?.portfolio_overlay || {}
  const mc = data?.market_context || {}
  const di = data?.decision_impact || {}
  const impacts = di?.top_impacts || []

  return (
    <div className="page news-intel-page">
      <div className="news-intel-header">
        <div>
          <h1>News Intelligence</h1>
          <p className="news-intel-subtitle">
            Deterministic, evidence-backed summaries from stored news features.
          </p>
        </div>
        <div className="news-intel-controls">
          <label>
            Portfolio scope
            <select value={portfolioId} onChange={(e) => setPortfolioId(e.target.value)}>
              <option value="">All open portfolios</option>
              {portfolios.map((p) => (
                <option key={p.PORTFOLIO_ID || p.portfolio_id} value={p.PORTFOLIO_ID || p.portfolio_id}>
                  {p.NAME || p.name || `Portfolio ${p.PORTFOLIO_ID || p.portfolio_id}`}
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>

      {loading && <LoadingState message="Loading news intelligence..." />}
      {error && <ErrorState message={error} />}

      {!loading && !error && !data && (
        <EmptyState message="No news intelligence snapshot available." />
      )}

      {!loading && !error && data && (
        <>
          <div className="news-intel-meta">
            <span>Generated: {fmtTs(data.generated_at)}</span>
            <span>Mode: {data?.narrative_contract?.mode || 'deterministic'}</span>
            <span>LLM used: {data?.narrative_contract?.llm_used ? 'yes' : 'no'}</span>
          </div>

          <div className="news-intel-kpis">
            <article><h3>Symbols With News</h3><p>{mc.symbols_with_news ?? 0}/{mc.symbols_total ?? 0}</p></article>
            <article><h3>Stale Symbols</h3><p>{mc.stale_symbols ?? 0}</p></article>
            <article><h3>HOT Symbols</h3><p>{mc.hot_symbols ?? 0}</p></article>
            <article><h3>Avg Snapshot Age</h3><p>{fmtMins(mc.avg_snapshot_age_minutes)}</p></article>
            <article><h3>Exposure At Risk</h3><p>{fmtNum(overlay.risk_market_value_pct, 1)}%</p></article>
          </div>

          <section className="news-intel-section">
            <h2>Reader Summary</h2>
            <ul className="news-intel-bullets">
              {bullets.map((b, i) => <li key={i}>{b}</li>)}
            </ul>
          </section>

          <section className="news-intel-section">
            <h2>Top Headlines</h2>
            {topHeadlines.length === 0 ? (
              <EmptyState message="No headline evidence available in current snapshot." />
            ) : (
              <ul className="news-intel-headlines">
                {topHeadlines.map((h, i) => (
                  <li key={`${h.symbol}-${i}`}>
                    <span className="news-intel-headline-symbol">{h.symbol}</span>
                    {h.url ? (
                      <a href={h.url} target="_blank" rel="noreferrer">{h.title}</a>
                    ) : (
                      <span>{h.title}</span>
                    )}
                  </li>
                ))}
              </ul>
            )}
          </section>

          <section className="news-intel-section">
            <h2>Symbol Cards</h2>
            <div className="news-intel-table-wrap">
              <table className="news-intel-table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Type</th>
                    <th>Badge</th>
                    <th>Count</th>
                    <th>Age</th>
                    <th>Stale</th>
                    <th>Uncertainty</th>
                    <th>Novelty</th>
                    <th>Burst</th>
                  </tr>
                </thead>
                <tbody>
                  {cards.map((c) => (
                    <tr key={`${c.symbol}-${c.market_type}`}>
                      <td>{c.symbol}</td>
                      <td>{c.market_type}</td>
                      <td>{c.news_badge || '—'}</td>
                      <td>{c.news_count ?? 0}</td>
                      <td>{fmtMins(c.news_snapshot_age_minutes)}</td>
                      <td>{c.news_is_stale ? 'Yes' : 'No'}</td>
                      <td>{c.uncertainty_flag ? 'Yes' : 'No'}</td>
                      <td>{fmtNum(c.novelty_score, 3)}</td>
                      <td>{fmtNum(c.burst_score, 3)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="news-intel-section">
            <h2>Decision Impact</h2>
            <p className="news-intel-note">
              Evidence from proposal payloads. {di.note || ''}
            </p>
            <div className="news-intel-kpis news-intel-kpis-impact">
              <article><h3>Proposals Scoped</h3><p>{di.proposals_scoped ?? 0}</p></article>
              <article><h3>With News Context</h3><p>{di.proposals_with_news_context ?? 0}</p></article>
              <article><h3>With News Adj</h3><p>{di.proposals_with_news_score_adj ?? 0}</p></article>
              <article><h3>Avg News Adj</h3><p>{fmtSigned(di.avg_news_score_adj, 3)}</p></article>
              <article><h3>Blocked New Entry</h3><p>{di.blocked_new_entry_count ?? 0}</p></article>
            </div>
            {impacts.length === 0 ? (
              <EmptyState message="No proposal-level decision impact rows yet." />
            ) : (
              <div className="news-intel-table-wrap">
                <table className="news-intel-table">
                  <thead>
                    <tr>
                      <th>Symbol</th>
                      <th>Status</th>
                      <th>News Adj</th>
                      <th>Blocked</th>
                      <th>Stale</th>
                      <th>Age</th>
                      <th>Badge</th>
                      <th>Reasons</th>
                    </tr>
                  </thead>
                  <tbody>
                    {impacts.map((r) => (
                      <tr key={r.proposal_id}>
                        <td>{r.symbol}</td>
                        <td>{r.status}</td>
                        <td className={(r.news_score_adj || 0) < 0 ? 'news-intel-neg' : (r.news_score_adj || 0) > 0 ? 'news-intel-pos' : ''}>
                          {fmtSigned(r.news_score_adj, 3)}
                        </td>
                        <td>{r.news_block_new_entry ? 'Yes' : 'No'}</td>
                        <td>{r.news_is_stale ? 'Yes' : 'No'}</td>
                        <td>{fmtMins(r.news_snapshot_age_minutes)}</td>
                        <td>{r.news_badge || '—'}</td>
                        <td>{Array.isArray(r.reasons) && r.reasons.length ? r.reasons.join(' | ') : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </>
      )}
    </div>
  )
}
