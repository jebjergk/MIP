import { useState, useEffect, useMemo, useCallback } from 'react'
import { Link } from 'react-router-dom'
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

function _get(row, ...keys) {
  for (const k of keys) {
    if (row[k] != null) return row[k]
    if (row[k.toLowerCase?.()] != null) return row[k.toLowerCase()]
  }
  return null
}

/* ── Attention Indicator ──────────────────────────────── */

function AttentionDot({ level, pulse }) {
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

function GateBadge({ gateState }) {
  const map = {
    SAFE: { cls: 'ck-gate--safe', label: 'Safe' },
    CAUTION: { cls: 'ck-gate--caution', label: 'Caution' },
    STOPPED: { cls: 'ck-gate--stopped', label: 'Stopped' },
  }
  const d = map[gateState] || map.SAFE
  return <span className={`ck-gate-badge ${d.cls}`}>{d.label}</span>
}

function HealthBadge({ healthState }) {
  const map = {
    OK: { cls: 'ck-health--ok', label: 'Healthy' },
    NEW: { cls: 'ck-health--new', label: 'New' },
    STALE: { cls: 'ck-health--stale', label: 'Stale' },
    BROKEN: { cls: 'ck-health--broken', label: 'Broken' },
  }
  const d = map[healthState] || map.OK
  return <span className={`ck-health-badge ${d.cls}`}>{d.label}</span>
}

function NasdaqBadge() {
  return (
    <span className="ck-nasdaq-badge" title="Data sourced from Nasdaq-listed symbols">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style={{ verticalAlign: '-2px', marginRight: '3px' }}>
        <rect x="1" y="1" width="22" height="22" rx="4" fill="#0996C7" />
        <path d="M7 17V7l10 10V7" stroke="#fff" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
      Nasdaq
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

function StoryCard({ attention, headline, summary, badges, children, defaultOpen, accent, onOpen }) {
  const [open, setOpen] = useState(defaultOpen || false)

  const handleToggle = useCallback(() => {
    const next = !open
    setOpen(next)
    if (next && onOpen) onOpen()
  }, [open, onOpen])

  return (
    <div className={`ck-story ${open ? 'ck-story--open' : ''} ${accent ? `ck-story--${accent}` : ''}`}>
      <button className="ck-story-header" onClick={handleToggle} type="button">
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

/* ── Intraday Top Movers Row ──────────────────────────── */

function IntradayTopMovers({ symbols }) {
  if (!symbols?.length) return null
  const withReturn = symbols.filter(s => s.session_return != null)
  const top3 = withReturn.slice(0, 3)
  const bottom3 = withReturn.slice(-3).reverse()
  if (!top3.length && !bottom3.length) return null

  return (
    <div className="ck-movers">
      <div className="ck-movers-group">
        <h4 className="ck-movers-label ck-movers-label--up">Top Gainers</h4>
        {top3.map((s, i) => (
          <div key={i} className="ck-mover ck-mover--up">
            <span className="ck-mover-symbol">{s.symbol}</span>
            <span className="ck-mover-return">+{(s.session_return * 100).toFixed(2)}%</span>
          </div>
        ))}
      </div>
      <div className="ck-movers-group">
        <h4 className="ck-movers-label ck-movers-label--down">Top Losers</h4>
        {bottom3.map((s, i) => (
          <div key={i} className="ck-mover ck-mover--down">
            <span className="ck-mover-symbol">{s.symbol}</span>
            <span className="ck-mover-return">{(s.session_return * 100).toFixed(2)}%</span>
          </div>
        ))}
      </div>
    </div>
  )
}

/* ── Intraday Return Bar Chart ───────────────────────── */

function IntradayReturnChart({ symbols }) {
  if (!symbols?.length) return null
  const data = symbols
    .filter(s => s.session_return != null)
    .map(s => ({ symbol: s.symbol, return_pct: +(s.session_return * 100).toFixed(2) }))
    .sort((a, b) => b.return_pct - a.return_pct)
  if (!data.length) return null

  return (
    <div className="ck-market-chart">
      <h4 className="ck-chart-title">Session Returns by Symbol</h4>
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

/* ── Intraday Session Badge ──────────────────────────── */

function SessionBadge({ isToday }) {
  return (
    <span className={`ck-badge ${isToday ? 'ck-badge--fresh' : 'ck-badge--stale'}`}>
      {isToday ? 'Live Session' : 'Previous Session'}
    </span>
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

/* ── Portfolio Story Card (lazy-loads digest on expand) ── */

function PortfolioStory({ portfolio }) {
  const pid = _get(portfolio, 'PORTFOLIO_ID', 'portfolio_id')
  const name = _get(portfolio, 'NAME', 'name') || `Portfolio ${pid}`
  const status = (_get(portfolio, 'STATUS', 'status') || 'ACTIVE').toUpperCase()
  const gateState = (_get(portfolio, 'GATE_STATE', 'gate_state') || 'SAFE').toUpperCase()
  const healthState = _get(portfolio, 'health_state') || 'OK'
  const equity = _get(portfolio, 'latest_equity', 'FINAL_EQUITY', 'final_equity') || 0
  const totalReturn = _get(portfolio, 'TOTAL_RETURN', 'total_return')
  const maxDrawdown = _get(portfolio, 'MAX_DRAWDOWN', 'max_drawdown')
  const totalPaidOut = _get(portfolio, 'total_paid_out', 'TOTAL_PAID_OUT') || 0
  const gateTooltip = _get(portfolio, 'gate_tooltip') || ''

  const [digest, setDigest] = useState(null)
  const [digestLoading, setDigestLoading] = useState(false)
  const [digestFetched, setDigestFetched] = useState(false)

  // Attention level based on gate state and health
  const attention = useMemo(() => {
    if (gateState === 'STOPPED' || healthState === 'BROKEN') return 'critical'
    if (gateState === 'CAUTION' || healthState === 'STALE') return 'warning'
    if (status !== 'ACTIVE') return 'neutral'
    if (totalReturn != null && totalReturn > 0) return 'positive'
    return 'info'
  }, [gateState, healthState, status, totalReturn])

  // Headline: portfolio name + quick status
  const headline = useMemo(() => {
    const retStr = totalReturn != null ? ` \u2014 ${totalReturn >= 0 ? '+' : ''}${(totalReturn * 100).toFixed(1)}%` : ''
    return `${name}${retStr}`
  }, [name, totalReturn])

  // Summary: equity, gate, key stat
  const summary = useMemo(() => {
    const parts = [`Equity: ${formatMoney(equity)}`]
    if (gateState !== 'SAFE') parts.push(`Gate: ${gateState}`)
    if (totalPaidOut > 0) parts.push(`Paid out: ${formatMoney(totalPaidOut)}`)
    if (maxDrawdown != null) parts.push(`Max DD: ${(maxDrawdown * 100).toFixed(1)}%`)
    return parts.join('  \u00B7  ')
  }, [equity, gateState, totalPaidOut, maxDrawdown])

  // Lazy-fetch digest on expand
  const handleOpen = useCallback(() => {
    if (digestFetched || digestLoading) return
    setDigestLoading(true)
    fetch(`${API_BASE}/digest/latest?portfolio_id=${pid}`)
      .then(r => r.ok ? r.json() : null)
      .catch(() => null)
      .then(data => {
        setDigest(data)
        setDigestLoading(false)
        setDigestFetched(true)
      })
  }, [pid, digestFetched, digestLoading])

  return (
    <StoryCard
      attention={attention}
      headline={headline}
      summary={summary}
      accent="portfolio"
      onOpen={handleOpen}
      badges={
        <>
          <GateBadge gateState={gateState} />
          <HealthBadge healthState={healthState} />
        </>
      }
    >
      {/* KPI strip */}
      <div className="ck-kpi-row">
        <div className="ck-kpi-item">
          <span className="ck-kpi-label">Equity</span>
          <span className="ck-kpi-value">{formatMoney(equity)}</span>
        </div>
        {totalReturn != null && (
          <div className="ck-kpi-item">
            <span className="ck-kpi-label">Return</span>
            <span className={`ck-kpi-value ${totalReturn > 0 ? 'ck-kpi--positive' : totalReturn < 0 ? 'ck-kpi--negative' : ''}`}>
              {formatPct(totalReturn)}
            </span>
          </div>
        )}
        {maxDrawdown != null && (
          <div className="ck-kpi-item">
            <span className="ck-kpi-label">Max DD</span>
            <span className="ck-kpi-value ck-kpi--negative">{formatPct(maxDrawdown)}</span>
          </div>
        )}
        {totalPaidOut > 0 && (
          <div className="ck-kpi-item">
            <span className="ck-kpi-label">Paid Out</span>
            <span className="ck-kpi-value ck-kpi--positive">{formatMoney(totalPaidOut)}</span>
          </div>
        )}
      </div>

      {gateTooltip && <p className="ck-gate-explanation">{gateTooltip}</p>}

      {/* Digest content (lazy loaded) */}
      {digestLoading && <p className="ck-loading-inline">Loading digest...</p>}

      {digest?.found && (
        <div className="ck-digest-detail">
          <p className="ck-headline">{digest.narrative?.headline || 'Portfolio digest available'}</p>
          {digest.is_ai_narrative && (
            <div style={{ marginBottom: '0.4rem' }}>
              <AiBadge isAi={digest.is_ai_narrative} modelInfo={digest.model_info} />
            </div>
          )}
          <DetectorPills detectors={digest.snapshot?.detectors} />
          <DigestSection title="What Changed" icon="&#x1F504;" bullets={digest.narrative?.what_changed} variant="changed" />
          <DigestSection title="What Matters" icon="&#x26A0;&#xFE0F;" bullets={digest.narrative?.what_matters} variant="matters" />
          <DigestSection title="Waiting For" icon="&#x23F3;" bullets={digest.narrative?.waiting_for} variant="waiting" />
          <DrillLinks whereToLook={digest.narrative?.where_to_look} links={digest.links} />
        </div>
      )}

      {digestFetched && !digest?.found && (
        <p className="ck-empty">No digest yet for this portfolio. Run the pipeline to generate.</p>
      )}

      {/* Link to full portfolio page */}
      <div className="ck-drill-links" style={{ marginTop: '0.5rem' }}>
        <Link to={`/portfolios/${pid}`} className="ck-drill-link">Full Portfolio &rarr;</Link>
        <Link to={`/suggestions?portfolio_id=${pid}`} className="ck-drill-link">Suggestions &rarr;</Link>
      </div>
    </StoryCard>
  )
}

/* ── Main Cockpit Page ───────────────────────────────── */

export default function Cockpit() {
  const { portfolios, loading: portfoliosLoading } = usePortfolios()

  const [digestGlobal, setDigestGlobal] = useState(null)
  const [trainingGlobal, setTrainingGlobal] = useState(null)
  const [todayData, setTodayData] = useState(null)
  const [marketPulse, setMarketPulse] = useState(null)
  const [intradayPulse, setIntradayPulse] = useState(null)
  const [loading, setLoading] = useState(true)

  // Parallel fetch non-portfolio data sources
  useEffect(() => {
    if (portfoliosLoading) return
    let cancelled = false
    setLoading(true)

    const fetches = [
      fetch(`${API_BASE}/digest/latest?scope=GLOBAL`).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/training/digest/latest`).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/today`).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/market/pulse`).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/market/pulse/intraday`).then(r => r.ok ? r.json() : null).catch(() => null),
    ]

    Promise.all(fetches).then(([dg, tg, td, mp, ip]) => {
      if (cancelled) return
      setDigestGlobal(dg)
      setTrainingGlobal(tg)
      setTodayData(td)
      setMarketPulse(mp)
      setIntradayPulse(ip)
      setLoading(false)
    })

    return () => { cancelled = true }
  }, [portfoliosLoading])

  const insights = todayData?.insights || []
  const aggregate = marketPulse?.aggregate || {}
  const marketSymbols = marketPulse?.symbols || []
  const indexSeries = marketPulse?.index_series || []

  const intradayAgg = intradayPulse?.aggregate || {}
  const intradaySymbols = intradayPulse?.symbols || []
  const intradaySessionDate = intradayPulse?.session_date
  const intradayIsToday = intradayPulse?.is_today

  // Attention levels for right-column stories
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

  // Global digest headline + summary
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
    return `Market Pulse \u2014 ${aggregate.up_count} of ${aggregate.total_symbols} symbols gained today`
  }, [aggregate])

  const marketSummary = useMemo(() => {
    if (!marketSymbols.length) return 'No market data available.'
    const top = marketSymbols[0]
    const bottom = marketSymbols[marketSymbols.length - 1]
    const avgPct = aggregate.avg_return_pct ?? 0
    return `Average return ${avgPct >= 0 ? '+' : ''}${avgPct}%. Top: ${top?.symbol} (+${((top?.day_return || 0) * 100).toFixed(1)}%), Bottom: ${bottom?.symbol} (${((bottom?.day_return || 0) * 100).toFixed(1)}%).`
  }, [marketSymbols, aggregate])

  // Intraday market pulse attention / headline / summary
  const intradayAttention = useMemo(() => {
    if (!intradayAgg.direction || intradayAgg.direction === 'NO_DATA') return 'neutral'
    if (intradayAgg.direction === 'DOWN') return 'warning'
    if (intradayAgg.direction === 'UP') return 'positive'
    return 'info'
  }, [intradayAgg])

  const intradayDateLabel = useMemo(() => {
    if (!intradaySessionDate) return ''
    try {
      return new Date(intradaySessionDate + 'T12:00:00').toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' })
    } catch { return intradaySessionDate }
  }, [intradaySessionDate])

  const intradayHeadline = useMemo(() => {
    if (!intradayAgg.direction || intradayAgg.direction === 'NO_DATA') return 'Intraday Pulse \u2014 Awaiting Data'
    return `Intraday Pulse \u2014 ${intradayAgg.up_count} of ${intradayAgg.total_symbols} symbols up this session`
  }, [intradayAgg])

  const intradaySummary = useMemo(() => {
    if (!intradaySymbols.length) return 'No intraday data available.'
    const withReturn = intradaySymbols.filter(s => s.session_return != null)
    if (!withReturn.length) return 'Session data loading.'
    const top = withReturn[0]
    const bottom = withReturn[withReturn.length - 1]
    const avgPct = intradayAgg.avg_return_pct ?? 0
    return `${intradayDateLabel} \u2014 Avg return ${avgPct >= 0 ? '+' : ''}${avgPct}%. Top: ${top?.symbol} (+${((top?.session_return || 0) * 100).toFixed(1)}%), Bottom: ${bottom?.symbol} (${((bottom?.session_return || 0) * 100).toFixed(1)}%).`
  }, [intradaySymbols, intradayAgg, intradayDateLabel])

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

  // Split portfolios: active first, then others
  const activePortfolios = useMemo(() => {
    return portfolios.filter(p => (_get(p, 'STATUS', 'status') || '').toUpperCase() === 'ACTIVE')
  }, [portfolios])

  const otherPortfolios = useMemo(() => {
    return portfolios.filter(p => (_get(p, 'STATUS', 'status') || '').toUpperCase() !== 'ACTIVE')
  }, [portfolios])

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
      </div>

      {/* ═══ Two-Column News Layout ═══ */}
      <div className="ck-news-grid">

        {/* ── LEFT COLUMN: All Portfolios ── */}
        <div className="ck-news-column">
          <div className="ck-column-label">Portfolios</div>

          {activePortfolios.length === 0 && otherPortfolios.length === 0 && (
            <div className="ck-story ck-story--portfolio">
              <div className="ck-story-body" style={{ padding: '1rem' }}>
                <EmptyState title="No portfolios" action="Create a portfolio to get started." />
              </div>
            </div>
          )}

          {activePortfolios.map((p) => (
            <PortfolioStory key={_get(p, 'PORTFOLIO_ID', 'portfolio_id')} portfolio={p} />
          ))}

          {otherPortfolios.length > 0 && (
            <>
              <div className="ck-column-label" style={{ marginTop: '0.5rem' }}>Inactive / Ended</div>
              {otherPortfolios.map((p) => (
                <PortfolioStory key={_get(p, 'PORTFOLIO_ID', 'portfolio_id')} portfolio={p} />
              ))}
            </>
          )}
        </div>

        {/* ── RIGHT COLUMN: Market, System, Signals, Training ── */}
        <div className="ck-news-column">
          <div className="ck-column-label">Market & System</div>

          {/* Story: Market Pulse */}
          <StoryCard
            attention={marketAttention}
            headline={marketHeadline}
            summary={marketSummary}
            accent="market"
            defaultOpen={false}
            badges={<><NasdaqBadge /><DirectionBadge direction={aggregate.direction} /></>}
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

          {/* Story: Intraday Market Pulse */}
          <StoryCard
            attention={intradayAttention}
            headline={intradayHeadline}
            summary={intradaySummary}
            accent="market"
            defaultOpen={false}
            badges={
              <>
                {intradayPulse && <SessionBadge isToday={intradayIsToday} />}
                <DirectionBadge direction={intradayAgg.direction} />
              </>
            }
          >
            {intradayPulse && intradayAgg.direction !== 'NO_DATA' ? (
              <>
                <div className="ck-market-kpi-strip">
                  <div className="ck-market-kpi">
                    <span className="ck-market-kpi-val ck-kpi--positive">{intradayAgg.up_count}</span>
                    <span className="ck-market-kpi-label">Up</span>
                  </div>
                  <div className="ck-market-kpi">
                    <span className="ck-market-kpi-val ck-kpi--negative">{intradayAgg.down_count}</span>
                    <span className="ck-market-kpi-label">Down</span>
                  </div>
                  <div className="ck-market-kpi">
                    <span className="ck-market-kpi-val">{intradayAgg.flat_count}</span>
                    <span className="ck-market-kpi-label">Flat</span>
                  </div>
                  <div className="ck-market-kpi">
                    <span className={`ck-market-kpi-val ${intradayAgg.avg_return_pct >= 0 ? 'ck-kpi--positive' : 'ck-kpi--negative'}`}>
                      {intradayAgg.avg_return_pct >= 0 ? '+' : ''}{intradayAgg.avg_return_pct}%
                    </span>
                    <span className="ck-market-kpi-label">Avg Return</span>
                  </div>
                  <div className="ck-market-kpi">
                    <span className="ck-market-kpi-val">{intradayAgg.breadth_pct}%</span>
                    <span className="ck-market-kpi-label">Breadth</span>
                  </div>
                  <div className="ck-market-kpi">
                    <span className="ck-market-kpi-val">{intradayAgg.total_symbols}</span>
                    <span className="ck-market-kpi-label">Symbols</span>
                  </div>
                </div>

                <IntradayTopMovers symbols={intradaySymbols} />
                <IntradayReturnChart symbols={intradaySymbols} />
              </>
            ) : (
              <EmptyState title="No intraday data" action="Intraday pulse will appear after the pipeline ingests 15-min bars." />
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
