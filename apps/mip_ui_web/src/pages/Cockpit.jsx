import { useState, useEffect, useMemo } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import LoadingState from '../components/LoadingState'
import { usePortfolios } from '../context/PortfolioContext'
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  BarChart, Bar, Cell, CartesianGrid, ReferenceLine,
} from 'recharts'
import './Cockpit.css'

/* ── Helpers ─────────────────────────────────────────── */

function formatTs(ts) {
  if (!ts) return '\u2014'
  try {
    const d = new Date(ts)
    return d.toLocaleString(undefined, {
      month: 'short', day: 'numeric', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    })
  } catch { return String(ts) }
}

function minutesAgo(ts) {
  if (!ts) return null
  try { return Math.round((Date.now() - new Date(ts).getTime()) / 60000) }
  catch { return null }
}

function formatPct(val, decimals = 2) {
  if (val == null) return '\u2014'
  return `${(val * 100).toFixed(decimals)}%`
}

function formatMoney(val) {
  if (val == null) return '\u2014'
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 0, maximumFractionDigits: 0 }).format(val)
}

/* ── Attention Indicator ──────────────────────────────── */

function AttentionDot({ level, pulse }) {
  // level: 'critical' | 'warning' | 'positive' | 'info' | 'neutral'
  return (
    <span className={`ck-attention ck-attention--${level || 'neutral'} ${pulse ? 'ck-attention--pulse' : ''}`} />
  )
}

/* ── Badges ───────────────────────────────────────────── */

function AiBadge({ isAi, modelInfo }) {
  return isAi ? (
    <span className="ck-badge ck-badge--ai" title={`Model: ${modelInfo}`}>Cortex AI</span>
  ) : (
    <span className="ck-badge ck-badge--fallback">Deterministic</span>
  )
}

function FreshnessBadge({ createdAt }) {
  const mins = minutesAgo(createdAt)
  if (mins === null) return null
  const fresh = mins < 120
  return (
    <span className={`ck-badge ${fresh ? 'ck-badge--fresh' : 'ck-badge--stale'}`}>
      {fresh ? 'Fresh' : 'Stale'} ({mins < 60 ? `${mins}m ago` : `${Math.round(mins / 60)}h ago`})
    </span>
  )
}

function DirectionBadge({ direction }) {
  const map = {
    UP: { label: 'Markets Up', cls: 'ck-dir--up', icon: '\u25B2' },
    DOWN: { label: 'Markets Down', cls: 'ck-dir--down', icon: '\u25BC' },
    MIXED: { label: 'Mixed', cls: 'ck-dir--mixed', icon: '\u25C6' },
    NO_DATA: { label: 'No Data', cls: 'ck-dir--neutral', icon: '\u2014' },
  }
  const d = map[direction] || map.NO_DATA
  return <span className={`ck-direction-badge ${d.cls}`}>{d.icon} {d.label}</span>
}

function DetectorPills({ detectors }) {
  if (!detectors?.length) return null
  const fired = detectors.filter((d) => d.fired)
  if (!fired.length) return null
  return (
    <div className="ck-detectors">
      {fired.map((d, i) => {
        const sev = (d.severity || 'low').toLowerCase()
        return (
          <span key={i} className={`ck-detector ck-detector--${sev}`} title={JSON.stringify(d.detail, null, 2)}>
            {(d.detector || '').replace(/_/g, ' ')}
          </span>
        )
      })}
    </div>
  )
}

/* ── Story Expander Component ──────────────────────────── */

function StoryCard({ attention, headline, summary, badges, children, defaultOpen, accent }) {
  const [open, setOpen] = useState(defaultOpen || false)

  return (
    <div className={`ck-story ${open ? 'ck-story--open' : ''} ${accent ? `ck-story--${accent}` : ''}`}>
      <button className="ck-story-header" onClick={() => setOpen(!open)} type="button">
        <div className="ck-story-indicator">
          <AttentionDot level={attention} pulse={attention === 'critical'} />
        </div>
        <div className="ck-story-headline-area">
          <h3 className="ck-story-headline">{headline}</h3>
          <p className="ck-story-summary">{summary}</p>
        </div>
        <div className="ck-story-meta">
          {badges}
          <span className={`ck-story-chevron ${open ? 'ck-story-chevron--open' : ''}`}>&#x25B8;</span>
        </div>
      </button>
      {open && (
        <div className="ck-story-body">
          {children}
        </div>
      )}
    </div>
  )
}

/* ── Digest Sections ──────────────────────────────────── */

function DigestSection({ title, icon, bullets, variant }) {
  if (!bullets || bullets.length === 0) return null
  return (
    <div className={`ck-section ck-section--${variant || 'default'}`}>
      <h4 className="ck-section-title">
        <span className="ck-section-icon">{icon}</span>
        {title}
      </h4>
      <ul className="ck-bullets">
        {bullets.map((b, i) => (
          <li key={i} className="ck-bullet">{typeof b === 'string' ? b : JSON.stringify(b)}</li>
        ))}
      </ul>
    </div>
  )
}

function DrillLinks({ whereToLook, links }) {
  const allLinks = []
  if (whereToLook?.length) {
    whereToLook.forEach((item) => {
      if (item?.route && item?.label) allLinks.push({ to: item.route, label: item.label })
    })
  }
  if (links) {
    ['signals', 'training', 'portfolio', 'market_timeline', 'digest'].forEach((key) => {
      if (links[key] && !allLinks.find((l) => l.to === links[key])) {
        const labels = { signals: 'Signals', training: 'Training', portfolio: 'Portfolio', market_timeline: 'Timeline', digest: 'Digest' }
        allLinks.push({ to: links[key], label: labels[key] || key })
      }
    })
  }
  if (!allLinks.length) return null
  return (
    <div className="ck-drill-links">
      {allLinks.map((l, i) => (
        <Link key={i} to={l.to} className="ck-drill-link">{l.label} &rarr;</Link>
      ))}
    </div>
  )
}

/* ── Market Return Bar Chart ──────────────────────────── */

function MarketReturnChart({ symbols }) {
  if (!symbols?.length) return null

  const data = symbols
    .filter((s) => s.day_return != null)
    .map((s) => ({
      symbol: s.symbol,
      return_pct: +(s.day_return * 100).toFixed(2),
    }))
    .sort((a, b) => b.return_pct - a.return_pct)

  return (
    <div className="ck-market-chart">
      <h4 className="ck-chart-title">Daily Returns by Symbol</h4>
      <ResponsiveContainer width="100%" height={Math.max(280, data.length * 24)}>
        <BarChart data={data} layout="vertical" margin={{ left: 50, right: 20, top: 5, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis type="number" tickFormatter={(v) => `${v}%`} fontSize={11} />
          <YAxis type="category" dataKey="symbol" width={50} fontSize={11} tick={{ fill: '#495057' }} />
          <Tooltip formatter={(v) => [`${v}%`, 'Return']} />
          <ReferenceLine x={0} stroke="#adb5bd" />
          <Bar dataKey="return_pct" radius={[0, 3, 3, 0]}>
            {data.map((entry, i) => (
              <Cell key={i} fill={entry.return_pct >= 0 ? '#198754' : '#dc3545'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

/* ── Market Index Line Chart ──────────────────────────── */

function MarketIndexChart({ indexSeries }) {
  if (!indexSeries?.length) return null

  const data = indexSeries.map((d) => ({
    ts: new Date(d.ts).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }),
    value: d.index_return_pct,
  }))

  const lastVal = data[data.length - 1]?.value ?? 0

  return (
    <div className="ck-market-chart">
      <h4 className="ck-chart-title">
        Equal-Weight Universe Index ({lastVal >= 0 ? '+' : ''}{lastVal.toFixed(1)}% over period)
      </h4>
      <ResponsiveContainer width="100%" height={200}>
        <AreaChart data={data} margin={{ left: 10, right: 10, top: 5, bottom: 5 }}>
          <defs>
            <linearGradient id="indexGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={lastVal >= 0 ? '#198754' : '#dc3545'} stopOpacity={0.3} />
              <stop offset="95%" stopColor={lastVal >= 0 ? '#198754' : '#dc3545'} stopOpacity={0.05} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="ts" fontSize={10} tick={{ fill: '#6c757d' }} />
          <YAxis tickFormatter={(v) => `${v}%`} fontSize={10} tick={{ fill: '#6c757d' }} />
          <Tooltip formatter={(v) => [`${v.toFixed(2)}%`, 'Index Return']} />
          <ReferenceLine y={0} stroke="#adb5bd" strokeDasharray="3 3" />
          <Area
            type="monotone"
            dataKey="value"
            stroke={lastVal >= 0 ? '#198754' : '#dc3545'}
            fill="url(#indexGrad)"
            strokeWidth={2}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

/* ── Top Movers Row ───────────────────────────────────── */

function TopMovers({ symbols }) {
  if (!symbols?.length) return null
  const top3 = symbols.filter(s => s.day_return != null).slice(0, 3)
  const bottom3 = symbols.filter(s => s.day_return != null).slice(-3).reverse()

  return (
    <div className="ck-movers">
      <div className="ck-movers-group">
        <h4 className="ck-movers-label ck-movers-label--up">Top Gainers</h4>
        {top3.map((s, i) => (
          <div key={i} className="ck-mover ck-mover--up">
            <span className="ck-mover-symbol">{s.symbol}</span>
            <span className="ck-mover-return">+{(s.day_return * 100).toFixed(2)}%</span>
          </div>
        ))}
      </div>
      <div className="ck-movers-group">
        <h4 className="ck-movers-label ck-movers-label--down">Top Losers</h4>
        {bottom3.map((s, i) => (
          <div key={i} className="ck-mover ck-mover--down">
            <span className="ck-mover-symbol">{s.symbol}</span>
            <span className="ck-mover-return">{(s.day_return * 100).toFixed(2)}%</span>
          </div>
        ))}
      </div>
    </div>
  )
}

/* ── Stage Pill ───────────────────────────────────────── */

function StagePill({ stage }) {
  const cls = (stage || '').toLowerCase().replace('_', '-')
  return <span className={`ck-stage-pill ck-stage-pill--${cls}`}>{stage}</span>
}

/* ── Upcoming Symbols Detail ─────────────────────────── */

function UpcomingSymbolsDetail({ trainingData }) {
  const snapshot = trainingData?.snapshot || {}
  const nearMiss = snapshot.near_miss_symbols || []
  const topConfident = snapshot.top_confident_symbols || []

  if (!nearMiss.length && !topConfident.length) {
    return <p className="ck-empty">No near-miss or upcoming symbol data available yet.</p>
  }

  return (
    <>
      {nearMiss.length > 0 && (
        <div className="ck-upcoming-list">
          {nearMiss.slice(0, 6).map((sym, i) => {
            const gap = sym.gap_to_next ?? '?'
            const score = sym.maturity_score ?? 0
            const stage = sym.maturity_stage || '?'
            const nextStage = stage === 'INSUFFICIENT' ? 'WARMING_UP' : stage === 'WARMING_UP' ? 'LEARNING' : stage === 'LEARNING' ? 'CONFIDENT' : 'NEXT'
            return (
              <div key={i} className="ck-upcoming-item">
                <div className="ck-upcoming-header">
                  <span className="ck-upcoming-rank">#{i + 1}</span>
                  <Link to={`/training?symbol=${sym.symbol}&market_type=${sym.market_type}`} className="ck-upcoming-symbol">{sym.symbol}</Link>
                  <span className="ck-upcoming-market">{sym.market_type}</span>
                  <StagePill stage={stage} />
                </div>
                <div className="ck-upcoming-bar-wrap">
                  <div className="ck-upcoming-bar" style={{ width: `${Math.min(100, Math.max(0, score))}%` }} />
                </div>
                <div className="ck-upcoming-detail">
                  <span>Score: {score}/100</span>
                  <span className="ck-upcoming-gap">Gap to {nextStage}: {gap} pts</span>
                </div>
              </div>
            )
          })}
        </div>
      )}
      {topConfident.length > 0 && (
        <>
          <h4 className="ck-subsection-title">Trade-Ready (CONFIDENT)</h4>
          <div className="ck-upcoming-list">
            {topConfident.slice(0, 4).map((sym, i) => (
              <div key={i} className="ck-upcoming-item ck-upcoming-item--confident">
                <div className="ck-upcoming-header">
                  <Link to={`/training?symbol=${sym.symbol}&market_type=${sym.market_type}`} className="ck-upcoming-symbol">{sym.symbol}</Link>
                  <span className="ck-upcoming-market">{sym.market_type}</span>
                  <StagePill stage="CONFIDENT" />
                  <span className="ck-upcoming-score">Score: {sym.maturity_score}/100</span>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </>
  )
}

/* ── Main Cockpit Page ───────────────────────────────── */

export default function Cockpit() {
  const [searchParams, setSearchParams] = useSearchParams()
  const portfolioIdParam = searchParams.get('portfolio_id')
  const portfolioId = portfolioIdParam ? parseInt(portfolioIdParam, 10) : null
  const { portfolios, defaultPortfolioId, loading: portfoliosLoading } = usePortfolios()

  const [digestGlobal, setDigestGlobal] = useState(null)
  const [digestPortfolio, setDigestPortfolio] = useState(null)
  const [trainingGlobal, setTrainingGlobal] = useState(null)
  const [todayData, setTodayData] = useState(null)
  const [marketPulse, setMarketPulse] = useState(null)
  const [loading, setLoading] = useState(true)

  // Default portfolio
  useEffect(() => {
    if (portfolioIdParam != null || portfoliosLoading || defaultPortfolioId == null) return
    setSearchParams({ portfolio_id: String(defaultPortfolioId) }, { replace: true })
  }, [defaultPortfolioId, portfolioIdParam, portfoliosLoading, setSearchParams])

  // Parallel fetch all data sources
  useEffect(() => {
    if (portfoliosLoading) return
    let cancelled = false
    setLoading(true)

    const fetches = [
      fetch(`${API_BASE}/digest/latest?scope=GLOBAL`).then(r => r.ok ? r.json() : null).catch(() => null),
      portfolioId
        ? fetch(`${API_BASE}/digest/latest?portfolio_id=${portfolioId}`).then(r => r.ok ? r.json() : null).catch(() => null)
        : Promise.resolve(null),
      fetch(`${API_BASE}/training/digest/latest`).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/today${portfolioId ? `?portfolio_id=${portfolioId}` : ''}`).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/market/pulse`).then(r => r.ok ? r.json() : null).catch(() => null),
    ]

    Promise.all(fetches).then(([dg, dp, tg, td, mp]) => {
      if (cancelled) return
      setDigestGlobal(dg)
      setDigestPortfolio(dp)
      setTrainingGlobal(tg)
      setTodayData(td)
      setMarketPulse(mp)
      setLoading(false)
    })

    return () => { cancelled = true }
  }, [portfolioId, portfoliosLoading])

  const insights = todayData?.insights || []
  const aggregate = marketPulse?.aggregate || {}
  const marketSymbols = marketPulse?.symbols || []
  const indexSeries = marketPulse?.index_series || []

  // Compute attention levels
  const portfolioAttention = useMemo(() => {
    if (!digestPortfolio?.found) return 'neutral'
    const detectors = digestPortfolio.snapshot?.detectors || []
    const highFired = detectors.some(d => d.fired && d.severity === 'HIGH')
    const medFired = detectors.some(d => d.fired && d.severity === 'MEDIUM')
    if (highFired) return 'critical'
    if (medFired) return 'warning'
    return 'info'
  }, [digestPortfolio])

  const globalAttention = useMemo(() => {
    if (!digestGlobal?.found) return 'neutral'
    const detectors = digestGlobal.snapshot?.detectors || []
    const highFired = detectors.some(d => d.fired && d.severity === 'HIGH')
    const medFired = detectors.some(d => d.fired && d.severity === 'MEDIUM')
    if (highFired) return 'critical'
    if (medFired) return 'warning'
    return 'info'
  }, [digestGlobal])

  const marketAttention = useMemo(() => {
    if (!aggregate.direction) return 'neutral'
    if (aggregate.direction === 'DOWN') return 'warning'
    if (aggregate.direction === 'UP') return 'positive'
    return 'info'
  }, [aggregate])

  const signalAttention = useMemo(() => {
    if (insights.length === 0) return 'neutral'
    if (insights.length >= 3) return 'positive'
    return 'info'
  }, [insights])

  const trainingAttention = useMemo(() => {
    const snapshot = trainingGlobal?.snapshot || {}
    const nearMiss = snapshot.near_miss_symbols || []
    if (nearMiss.length > 0) return 'info'
    return 'neutral'
  }, [trainingGlobal])

  // Build portfolio headline + summary
  const portfolioHeadline = digestPortfolio?.found
    ? (digestPortfolio.narrative?.headline || 'Portfolio Intelligence')
    : 'Portfolio Intelligence'

  const portfolioSummary = useMemo(() => {
    if (!digestPortfolio?.found) return 'Select a portfolio above to see intelligence.'
    const wc = digestPortfolio.narrative?.what_changed || []
    const wm = digestPortfolio.narrative?.what_matters || []
    if (wc.length > 0) return wc[0]
    if (wm.length > 0) return wm[0]
    return 'Portfolio digest generated. Expand for full details.'
  }, [digestPortfolio])

  // Build global headline + summary
  const globalHeadline = digestGlobal?.found
    ? (digestGlobal.narrative?.headline || 'System Overview')
    : 'System Overview'

  const globalSummary = useMemo(() => {
    if (!digestGlobal?.found) return 'No global digest yet. Run the pipeline to generate.'
    const wc = digestGlobal.narrative?.what_changed || []
    const wm = digestGlobal.narrative?.what_matters || []
    if (wc.length > 0) return wc[0]
    if (wm.length > 0) return wm[0]
    return 'System digest generated. Expand for full details.'
  }, [digestGlobal])

  // Market headline + summary
  const marketHeadline = useMemo(() => {
    if (!aggregate.direction || aggregate.direction === 'NO_DATA') return 'Market Pulse \u2014 Awaiting Data'
    const dir = aggregate.direction === 'UP' ? 'mostly up' : aggregate.direction === 'DOWN' ? 'mostly down' : 'mixed'
    return `Market Pulse \u2014 ${aggregate.up_count} of ${aggregate.total_symbols} symbols gained today`
  }, [aggregate])

  const marketSummary = useMemo(() => {
    if (!marketSymbols.length) return 'No market data available.'
    const top = marketSymbols[0]
    const bottom = marketSymbols[marketSymbols.length - 1]
    const avgPct = aggregate.avg_return_pct ?? 0
    return `Average return ${avgPct >= 0 ? '+' : ''}${avgPct}%. Top: ${top?.symbol} (+${((top?.day_return || 0) * 100).toFixed(1)}%), Bottom: ${bottom?.symbol} (${((bottom?.day_return || 0) * 100).toFixed(1)}%).`
  }, [marketSymbols, aggregate])

  // Signal headline
  const signalHeadline = insights.length > 0
    ? `${insights.length} Signal Candidate${insights.length > 1 ? 's' : ''} Today`
    : 'Signal Candidates \u2014 None Today'

  const signalSummary = useMemo(() => {
    if (insights.length === 0) return 'No eligible signals found today.'
    const top = insights[0]
    return `Top candidate: ${top.symbol} (${top.maturity_stage}, score ${top.maturity_score}). ${top.why_this_is_here || ''}`
  }, [insights])

  // Training headline
  const trainingNarrative = trainingGlobal?.narrative || {}
  const trainingHeadline = trainingNarrative.headline || 'Training Progress'
  const trainingSummary = useMemo(() => {
    const snapshot = trainingGlobal?.snapshot || {}
    const nearMiss = snapshot.near_miss_symbols || []
    const topConf = snapshot.top_confident_symbols || []
    if (nearMiss.length > 0) {
      return `${nearMiss.length} symbol${nearMiss.length > 1 ? 's' : ''} approaching next stage. ${topConf.length} trade-ready.`
    }
    if (topConf.length > 0) return `${topConf.length} symbol${topConf.length > 1 ? 's' : ''} trade-ready (CONFIDENT).`
    return 'No near-miss symbols. Expand for training details.'
  }, [trainingGlobal])

  if (loading) {
    return (
      <>
        <h1>Cockpit</h1>
        <LoadingState message="Loading cockpit data..." />
      </>
    )
  }

  return (
    <div className="ck-page">
      <div className="ck-page-header">
        <h1>Cockpit</h1>
        {portfolios.length > 0 && (
          <label className="ck-portfolio-picker">
            Portfolio:
            <select
              value={portfolioId != null ? String(portfolioId) : ''}
              onChange={(e) => {
                const v = e.target.value
                setSearchParams(v ? { portfolio_id: v } : {})
              }}
            >
              <option value="">&mdash;</option>
              {portfolios.map((p) => {
                const id = p.PORTFOLIO_ID ?? p.portfolio_id
                return <option key={id} value={String(id)}>{p.NAME ?? p.name ?? id}</option>
              })}
            </select>
          </label>
        )}
      </div>

      {/* ═══ Two-Column News Layout ═══ */}
      <div className="ck-news-grid">

        {/* ── LEFT COLUMN: Portfolio Stories ── */}
        <div className="ck-news-column">
          <div className="ck-column-label">Portfolio</div>

          {/* Story: Portfolio Digest */}
          <StoryCard
            attention={portfolioAttention}
            headline={portfolioHeadline}
            summary={portfolioSummary}
            accent="portfolio"
            badges={
              <>
                {digestPortfolio?.found && <AiBadge isAi={digestPortfolio.is_ai_narrative} modelInfo={digestPortfolio.model_info} />}
                {digestPortfolio?.found && digestPortfolio.snapshot?.episode?.episode_id && (
                  <span className="ck-badge ck-badge--episode">
                    Ep {digestPortfolio.snapshot.episode.total_episodes || 1}
                  </span>
                )}
              </>
            }
          >
            {portfolioId == null ? (
              <p className="ck-empty">Select a portfolio above.</p>
            ) : digestPortfolio?.found ? (
              <>
                <DetectorPills detectors={digestPortfolio.snapshot?.detectors} />
                <DigestSection title="What Changed" icon="&#x1F504;" bullets={digestPortfolio.narrative?.what_changed} variant="changed" />
                <DigestSection title="What Matters" icon="&#x26A0;&#xFE0F;" bullets={digestPortfolio.narrative?.what_matters} variant="matters" />
                <DigestSection title="Waiting For" icon="&#x23F3;" bullets={digestPortfolio.narrative?.waiting_for} variant="waiting" />
                <DrillLinks whereToLook={digestPortfolio.narrative?.where_to_look} links={digestPortfolio.links} />
              </>
            ) : (
              <EmptyState title="No portfolio digest yet" action="Run the pipeline to generate." />
            )}
          </StoryCard>

          {/* Story: Portfolio KPIs (from todayData) */}
          {todayData?.portfolio && (
            <StoryCard
              attention={
                todayData.portfolio.risk_gate?.[0]?.ENTRIES_BLOCKED ? 'critical' :
                todayData.portfolio.risk_gate?.[0]?.RISK_STATUS === 'WARN' ? 'warning' : 'positive'
              }
              headline="Portfolio Risk & Performance"
              summary={(() => {
                const kpi = todayData.portfolio.kpis?.[0]
                if (!kpi) return 'No KPI data available for this portfolio.'
                const equity = kpi.FINAL_EQUITY || kpi.final_equity
                const ret = kpi.TOTAL_RETURN || kpi.total_return
                return `Equity: ${formatMoney(equity)}. Return: ${formatPct(ret)}.`
              })()}
              accent="portfolio"
            >
              {todayData.portfolio.kpis?.length > 0 && (
                <div className="ck-kpi-grid">
                  {todayData.portfolio.kpis.slice(0, 1).map((kpi, i) => {
                    const equity = kpi.FINAL_EQUITY || kpi.final_equity
                    const ret = kpi.TOTAL_RETURN || kpi.total_return
                    const dd = kpi.MAX_DRAWDOWN || kpi.max_drawdown
                    return (
                      <div key={i} className="ck-kpi-row">
                        <div className="ck-kpi-item">
                          <span className="ck-kpi-label">Equity</span>
                          <span className="ck-kpi-value">{formatMoney(equity)}</span>
                        </div>
                        <div className="ck-kpi-item">
                          <span className="ck-kpi-label">Return</span>
                          <span className={`ck-kpi-value ${ret > 0 ? 'ck-kpi--positive' : ret < 0 ? 'ck-kpi--negative' : ''}`}>{formatPct(ret)}</span>
                        </div>
                        <div className="ck-kpi-item">
                          <span className="ck-kpi-label">Max DD</span>
                          <span className="ck-kpi-value ck-kpi--negative">{formatPct(dd)}</span>
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
              {todayData.portfolio.run_events?.length > 0 && (
                <div className="ck-run-events">
                  <h4 className="ck-subsection-title">Recent Pipeline Events</h4>
                  <ul className="ck-bullets">
                    {todayData.portfolio.run_events.slice(0, 5).map((ev, i) => {
                      const name = ev.EVENT_NAME || ev.event_name || 'Event'
                      const status = ev.STATUS || ev.status || ''
                      return <li key={i} className="ck-bullet">{name}: {status}</li>
                    })}
                  </ul>
                </div>
              )}
            </StoryCard>
          )}
        </div>

        {/* ── RIGHT COLUMN: Market, System, Signals, Training ── */}
        <div className="ck-news-column">
          <div className="ck-column-label">Market & System</div>

          {/* Story: Market Pulse (NEW) */}
          <StoryCard
            attention={marketAttention}
            headline={marketHeadline}
            summary={marketSummary}
            accent="market"
            defaultOpen={false}
            badges={<DirectionBadge direction={aggregate.direction} />}
          >
            {marketPulse ? (
              <>
                {/* KPI strip */}
                <div className="ck-market-kpi-strip">
                  <div className="ck-market-kpi">
                    <span className="ck-market-kpi-val ck-kpi--positive">{aggregate.up_count}</span>
                    <span className="ck-market-kpi-label">Up</span>
                  </div>
                  <div className="ck-market-kpi">
                    <span className="ck-market-kpi-val ck-kpi--negative">{aggregate.down_count}</span>
                    <span className="ck-market-kpi-label">Down</span>
                  </div>
                  <div className="ck-market-kpi">
                    <span className="ck-market-kpi-val">{aggregate.flat_count}</span>
                    <span className="ck-market-kpi-label">Flat</span>
                  </div>
                  <div className="ck-market-kpi">
                    <span className={`ck-market-kpi-val ${aggregate.avg_return_pct >= 0 ? 'ck-kpi--positive' : 'ck-kpi--negative'}`}>
                      {aggregate.avg_return_pct >= 0 ? '+' : ''}{aggregate.avg_return_pct}%
                    </span>
                    <span className="ck-market-kpi-label">Avg Return</span>
                  </div>
                  <div className="ck-market-kpi">
                    <span className="ck-market-kpi-val">{aggregate.breadth_pct}%</span>
                    <span className="ck-market-kpi-label">Breadth</span>
                  </div>
                </div>

                <TopMovers symbols={marketSymbols} />
                <MarketIndexChart indexSeries={indexSeries} />
                <MarketReturnChart symbols={marketSymbols} />
              </>
            ) : (
              <EmptyState title="No market data" action="Market data will appear after ingestion runs." />
            )}
          </StoryCard>

          {/* Story: System Overview */}
          <StoryCard
            attention={globalAttention}
            headline={globalHeadline}
            summary={globalSummary}
            accent="system"
            badges={
              <>
                <span className="ck-badge ck-badge--scope">Global</span>
                {digestGlobal?.found && <AiBadge isAi={digestGlobal.is_ai_narrative} modelInfo={digestGlobal.model_info} />}
                {digestGlobal?.found && <FreshnessBadge createdAt={digestGlobal.snapshot_created_at} />}
              </>
            }
          >
            {digestGlobal?.found ? (
              <>
                <DetectorPills detectors={digestGlobal.snapshot?.detectors} />
                <DigestSection title="What Changed" icon="&#x1F504;" bullets={digestGlobal.narrative?.what_changed} variant="changed" />
                <DigestSection title="What Matters" icon="&#x26A0;&#xFE0F;" bullets={digestGlobal.narrative?.what_matters} variant="matters" />
                <DigestSection title="Waiting For" icon="&#x23F3;" bullets={digestGlobal.narrative?.waiting_for} variant="waiting" />
                <DrillLinks whereToLook={digestGlobal.narrative?.where_to_look} links={digestGlobal.links} />
              </>
            ) : (
              <EmptyState title="No global digest yet" action="Run the pipeline to generate." />
            )}
          </StoryCard>

          {/* Story: Signal Candidates */}
          <StoryCard
            attention={signalAttention}
            headline={signalHeadline}
            summary={signalSummary}
            accent="signal"
          >
            {insights.length > 0 ? (
              <div className="ck-insights-grid">
                {insights.slice(0, 8).map((item, i) => (
                  <div key={i} className="ck-insight-mini">
                    <div className="ck-insight-mini-header">
                      <span className="ck-insight-mini-rank">#{i + 1}</span>
                      <span className="ck-insight-mini-symbol">{item.symbol}</span>
                      <StagePill stage={item.maturity_stage} />
                      <span className="ck-insight-mini-score">{item.maturity_score}/100</span>
                    </div>
                    <div className="ck-upcoming-bar-wrap">
                      <div className="ck-upcoming-bar" style={{ width: `${Math.min(100, Math.max(0, item.maturity_score ?? 0))}%` }} />
                    </div>
                    <p className="ck-insight-mini-why">{item.why_this_is_here || '\u2014'}</p>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState title="No signal candidates today" action="Signals appear when eligible symbols have sufficient maturity." />
            )}
          </StoryCard>

          {/* Story: Training Progress / Upcoming Symbols */}
          <StoryCard
            attention={trainingAttention}
            headline={trainingHeadline}
            summary={trainingSummary}
            accent="training"
          >
            {trainingGlobal?.found ? (
              <>
                <DigestSection title="What Changed" icon="&#x1F504;" bullets={trainingNarrative.what_changed} variant="changed" />
                <DigestSection title="What Matters" icon="&#x26A0;&#xFE0F;" bullets={trainingNarrative.what_matters} variant="matters" />
                <DigestSection title="Waiting For" icon="&#x23F3;" bullets={trainingNarrative.waiting_for} variant="waiting" />
                <UpcomingSymbolsDetail trainingData={trainingGlobal} />
              </>
            ) : (
              <EmptyState title="No training digest yet" action="Run the pipeline to generate." />
            )}
          </StoryCard>
        </div>
      </div>
    </div>
  )
}
