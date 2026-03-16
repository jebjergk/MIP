import { useState, useEffect, useMemo, useCallback } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE } from '../App'
import EmptyState from '../components/EmptyState'
import LoadingState from '../components/LoadingState'
import { usePortfolios } from '../context/PortfolioContext'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  BarChart, Bar, Cell, CartesianGrid, ReferenceLine,
} from 'recharts'
import './Cockpit.css'

/* ── Helpers ─────────────────────────────────────────── */

function formatTs(ts) {
  if (!ts) return '\u2014'
  try {
    const d = new Date(normalizeIsoTs(ts))
    return d.toLocaleString(undefined, {
      month: 'short', day: 'numeric', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    })
  } catch { return String(ts) }
}

function normalizeIsoTs(ts) {
  if (typeof ts !== 'string') return ts
  const s = ts.trim()
  if (!s) return ts
  const hasExplicitZone = /(?:Z|[+-]\d{2}:\d{2})$/i.test(s)
  const looksIso = /^\d{4}-\d{2}-\d{2}T/.test(s)
  if (looksIso && !hasExplicitZone) return `${s}Z`
  return s
}

function minutesAgo(ts) {
  if (!ts) return null
  try { return Math.round((Date.now() - new Date(normalizeIsoTs(ts)).getTime()) / 60000) }
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

function formatSignedMoney(val) {
  if (val == null) return '\u2014'
  const n = Number(val)
  if (!Number.isFinite(n)) return '\u2014'
  const abs = formatMoney(Math.abs(n))
  if (n > 0) return `+${abs}`
  if (n < 0) return `-${abs}`
  return abs
}

function formatQty(val) {
  if (val == null) return '\u2014'
  const n = Number(val)
  if (!Number.isFinite(n)) return '\u2014'
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

function formatAgeShort(ts) {
  const mins = minutesAgo(ts)
  if (mins == null) return '\u2014'
  if (mins < 60) return `${Math.max(mins, 0)}m ago`
  const hours = Math.round(mins / 60)
  if (hours < 48) return `${hours}h ago`
  return `${Math.round(hours / 24)}d ago`
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

function GateBadge({ gateState, reasonCode }) {
  const map = {
    SAFE: { cls: 'ck-gate--safe', label: 'Safe' },
    CAUTION: { cls: 'ck-gate--caution', label: 'Caution' },
    STOPPED: { cls: 'ck-gate--stopped', label: 'Stopped' },
  }
  const d = map[gateState] || map.SAFE
  const showReason = gateState !== 'SAFE' && reasonCode
  return <span className={`ck-gate-badge ${d.cls}`}>{showReason ? `${d.label}: ${reasonCode}` : d.label}</span>
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
    ['decision_console', 'training', 'portfolio', 'market_timeline', 'digest'].forEach((key) => {
      if (links[key] && !allLinks.find((l) => l.to === links[key])) {
        const labels = { decision_console: 'AI Decisions', training: 'Training', portfolio: 'Portfolio', market_timeline: 'Timeline', digest: 'Digest' }
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
    .map((s) => ({
      symbol: s.symbol,
      return_pct: +(((s.day_return ?? 0) * 100).toFixed(2)),
      has_return: s.day_return != null,
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
              <Cell key={i} fill={!entry.has_return ? '#adb5bd' : (entry.return_pct >= 0 ? '#198754' : '#dc3545')} />
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
  const { formatSymbolLabel } = useSymbolMeta()
  if (!symbols?.length) return null
  const top3 = symbols.filter(s => s.day_return != null).slice(0, 3)
  const bottom3 = symbols.filter(s => s.day_return != null).slice(-3).reverse()

  return (
    <div className="ck-movers">
      <div className="ck-movers-group">
        <h4 className="ck-movers-label ck-movers-label--up">Top Gainers</h4>
        {top3.map((s, i) => (
          <div key={i} className="ck-mover ck-mover--up">
            <span className="ck-mover-symbol">{formatSymbolLabel(s.symbol, s.market_type)}</span>
            <span className="ck-mover-return">+{(s.day_return * 100).toFixed(2)}%</span>
          </div>
        ))}
      </div>
      <div className="ck-movers-group">
        <h4 className="ck-movers-label ck-movers-label--down">Top Losers</h4>
        {bottom3.map((s, i) => (
          <div key={i} className="ck-mover ck-mover--down">
            <span className="ck-mover-symbol">{formatSymbolLabel(s.symbol, s.market_type)}</span>
            <span className="ck-mover-return">{(s.day_return * 100).toFixed(2)}%</span>
          </div>
        ))}
      </div>
    </div>
  )
}

/* ── News Intelligence Overview ───────────────────────── */

function NewsIntelligenceOverview({ overview }) {
  const { formatSymbolLabel } = useSymbolMeta()
  if (!overview?.found) return <p className="ck-empty">No news intelligence overview available.</p>

  const bullets = overview.summary_bullets || []
  const headlines = overview.key_headlines || []
  const impacted = overview.impacted_symbols || []
  const metrics = overview.metrics || {}

  return (
    <div className="ck-news-overview">
      <p className="ck-headline">{overview.executive_summary || 'Latest news intelligence summary is available.'}</p>

      <div className="ck-market-kpi-strip">
        <div className="ck-market-kpi">
          <span className="ck-market-kpi-val">{metrics.symbols_with_news ?? 0}/{metrics.symbols_total ?? 0}</span>
          <span className="ck-market-kpi-label">Coverage</span>
        </div>
        <div className="ck-market-kpi">
          <span className="ck-market-kpi-val ck-kpi--negative">{metrics.hot_symbols ?? 0}</span>
          <span className="ck-market-kpi-label">HOT</span>
        </div>
        <div className="ck-market-kpi">
          <span className="ck-market-kpi-val ck-kpi--negative">{metrics.stale_symbols ?? 0}</span>
          <span className="ck-market-kpi-label">Stale</span>
        </div>
        <div className="ck-market-kpi">
          <span className={`ck-market-kpi-val ${(metrics.risk_market_value_pct ?? 0) >= 20 ? 'ck-kpi--negative' : ''}`}>
            {metrics.risk_market_value_pct ?? 0}%
          </span>
          <span className="ck-market-kpi-label">At Risk</span>
        </div>
      </div>

      <DigestSection title="Committee Summary" icon="&#x1F4F0;" bullets={bullets} variant="matters" />
      <DigestSection title="Committee Hint" icon="&#x1F9ED;" bullets={[overview.committee_hint]} variant="waiting" />

      {headlines.length > 0 && (
        <div className="ck-news-headlines">
          <h4 className="ck-chart-title">Top Headlines</h4>
          <ul className="ck-news-headline-list">
            {headlines.map((h, i) => (
              <li key={`${h.symbol}-${i}`} className="ck-news-headline-item">
                <div className="ck-news-headline-main">
                  <span
                    className={`ck-news-headline-icon ck-news-headline-icon--${(h.tone || 'NO_EFFECT').toLowerCase()}`}
                    title={h.effect_label || 'No clear effect'}
                    aria-label={h.effect_label || 'No clear effect'}
                  >
                    {h.icon || '😐'}
                  </span>
                  <span className="ck-news-headline-title">
                    <strong>{formatSymbolLabel(h.symbol)}</strong> {h.title}
                  </span>
                </div>
                <div className="ck-news-headline-sub">
                  <span className="ck-news-headline-effect">{h.effect_label || 'No clear directional edge'}</span>
                  <span className="ck-news-headline-note">{h.committee_note || 'Use as context for committee review.'}</span>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}

      {impacted.length > 0 && (
        <div className="ck-news-symbol-chips">
          {impacted.map((sym) => <span key={sym} className="ck-news-symbol-chip">{formatSymbolLabel(sym)}</span>)}
        </div>
      )}

      <div className="ck-drill-links">
        <Link to="/news-intelligence" className="ck-drill-link">Open News Intelligence &rarr;</Link>
        <Link to="/decision-console" className="ck-drill-link">Open AI Agent Decisions &rarr;</Link>
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
  const { formatSymbolLabel } = useSymbolMeta()
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
                  <Link to={`/training?symbol=${sym.symbol}&market_type=${sym.market_type}`} className="ck-upcoming-symbol">{formatSymbolLabel(sym.symbol, sym.market_type)}</Link>
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
                  <Link to={`/training?symbol=${sym.symbol}&market_type=${sym.market_type}`} className="ck-upcoming-symbol">{formatSymbolLabel(sym.symbol, sym.market_type)}</Link>
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

function DailyReadinessOverview({ readiness }) {
  const { formatSymbolLabel } = useSymbolMeta()
  if (!readiness?.found) {
    return <p className="ck-empty">Daily readiness data not available yet.</p>
  }

  const counts = readiness?.counts || {}
  const byMarket = Array.isArray(readiness?.training_by_market_type) ? readiness.training_by_market_type : []
  const proposals = Array.isArray(readiness?.proposals_preview) ? readiness.proposals_preview : []
  const proposalGroups = useMemo(() => {
    const groups = []
    const byDay = new Map()
    proposals.forEach((p) => {
      const ts = normalizeIsoTs(p?.proposed_at)
      const d = ts ? new Date(ts) : null
      const dayKey = d && !Number.isNaN(d.getTime())
        ? `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
        : 'Unknown Date'
      const dayLabel = d && !Number.isNaN(d.getTime())
        ? d.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' })
        : 'Unknown Date'
      if (!byDay.has(dayKey)) {
        byDay.set(dayKey, { key: dayKey, label: dayLabel, items: [] })
        groups.push(byDay.get(dayKey))
      }
      byDay.get(dayKey).items.push(p)
    })
    return groups
  }, [proposals])

  const assessmentClass = (a) => {
    const v = String(a || '').toUpperCase()
    if (v === 'STRONG') return 'ck-health--ok'
    if (v === 'WATCH') return 'ck-health--new'
    if (v === 'LOW_EVIDENCE') return 'ck-health--stale'
    return 'ck-health--broken'
  }

  return (
    <div className="ck-readiness">
      <div className="ck-market-kpi-strip">
        <div className="ck-market-kpi">
          <span className="ck-market-kpi-val">{counts.signals_generated ?? 0}</span>
          <span className="ck-market-kpi-label">Signals (last run)</span>
        </div>
        <div className="ck-market-kpi">
          <span className="ck-market-kpi-val">{counts.signals_eligible ?? 0}</span>
          <span className="ck-market-kpi-label">Eligible</span>
        </div>
        <div className="ck-market-kpi">
          <span className="ck-market-kpi-val">{counts.proposals_generated ?? 0}</span>
          <span className="ck-market-kpi-label">Proposals</span>
        </div>
      </div>

      <h4 className="ck-chart-title">Training & trust by market type</h4>
      {byMarket.length === 0 ? (
        <p className="ck-empty">No market-type training rows available.</p>
      ) : (
        <div className="ck-insights-grid">
          {byMarket.map((row) => (
            <div key={row.market_type} className="ck-insight-mini">
              <div className="ck-insight-mini-header">
                <span className="ck-insight-mini-symbol">{row.market_type}</span>
                <span className="ck-insight-mini-score">
                  Signals {row.signals_generated_last_run ?? 0} · Proposals {row.proposals_generated_last_run ?? 0}
                </span>
              </div>
              <p className="ck-insight-mini-why">
                Trusted {row.trusted_count ?? 0} · Watch {row.watch_count ?? 0} · Untrusted {row.untrusted_count ?? 0}
              </p>
            </div>
          ))}
        </div>
      )}

      <h4 className="ck-chart-title" style={{ marginTop: '0.7rem' }}>
        Proposal details (latest {proposals.length} of 10)
      </h4>
      {proposals.length === 0 ? (
        <p className="ck-empty">No proposals produced in the latest run.</p>
      ) : (
        proposalGroups.map((g) => (
          <div key={g.key} className="ck-proposal-group">
            <h5 className="ck-proposal-group-title">{g.label}</h5>
            <ul className="ck-live-list">
              {g.items.map((p) => (
                <li key={p.proposal_id || `${p.symbol}_${p.proposed_at}`} className="ck-live-list-item">
                  <strong>{formatSymbolLabel(p.symbol, p.market_type)}</strong> {p.side} ({p.market_type}) · wt {p.target_weight != null ? `${(Number(p.target_weight) * 100).toFixed(1)}%` : '\u2014'}
                  {' '}· {p.status || 'PROPOSED'}
                  <span className={`ck-health-badge ${assessmentClass(p.committee_assessment)}`} style={{ marginLeft: '0.35rem' }}>
                    {p.committee_assessment || 'N/A'}
                  </span>
                  <span className="ck-live-subline">
                    Hist hit {(Number(p.historical_hit_rate || 0) * 100).toFixed(1)}% · avg ret {(Number(p.historical_mean_return || 0) * 100).toFixed(2)}%
                    · hold {p.suggested_hold_bars ?? '\u2014'} bars · n={p.evidence_samples ?? 0}
                  </span>
                  {p.committee_reason ? <span className="ck-live-subline">Why: {p.committee_reason}</span> : null}
                </li>
              ))}
            </ul>
          </div>
        ))
      )}

      <div className="ck-drill-links">
        <Link to="/training" className="ck-drill-link">Training &rarr;</Link>
        <Link to="/decision-console" className="ck-drill-link">AI Agent Decisions &rarr;</Link>
      </div>
    </div>
  )
}

/* ── Portfolio Story Card (lazy-loads digest on expand) ── */

function PortfolioStory({ portfolio }) {
  const { formatSymbolLabel } = useSymbolMeta()
  const pid = _get(portfolio, 'PORTFOLIO_ID', 'portfolio_id')
  const isLiveCard = Boolean(_get(portfolio, 'IS_LIVE_CARD', 'is_live_card'))
  const name = _get(portfolio, 'NAME', 'name') || `Portfolio ${pid}`
  const status = (_get(portfolio, 'STATUS', 'status') || 'ACTIVE').toUpperCase()
  const gateState = (_get(portfolio, 'GATE_STATE', 'gate_state') || 'SAFE').toUpperCase()
  const healthState = _get(portfolio, 'health_state') || 'OK'
  const lastDayCloseEquity = _get(portfolio, 'last_day_close_equity', 'latest_equity', 'FINAL_EQUITY', 'final_equity') || 0
  const currentEquity = _get(portfolio, 'current_equity')
  const totalReturn = _get(portfolio, 'TOTAL_RETURN', 'total_return')
  const maxDrawdown = _get(portfolio, 'MAX_DRAWDOWN', 'max_drawdown')
  const totalPaidOut = _get(portfolio, 'total_paid_out', 'TOTAL_PAID_OUT') || 0
  const gateTooltip = _get(portfolio, 'gate_tooltip') || ''
  const gateReasonCode = _get(portfolio, 'GATE_REASON_CODE', 'gate_reason_code')
  const liveOverview = _get(portfolio, 'LIVE_OVERVIEW', 'live_overview')
  const liveSnapshotTs = liveOverview?.account_kpis?.snapshot_ts
  const liveSnapshotState = String(liveOverview?.readiness?.snapshot_state || '').toUpperCase()
  const pendingDecisions = Array.isArray(liveOverview?.pending_decisions) ? liveOverview.pending_decisions : []
  const openPositions = Array.isArray(liveOverview?.open_positions) ? liveOverview.open_positions : []
  const recentTrades = useMemo(() => {
    const execs = Array.isArray(liveOverview?.executions) ? [...liveOverview.executions] : []
    execs.sort((a, b) => {
      const aTs = new Date(_get(a, 'execution_ts', 'EXECUTION_TS') || 0).getTime()
      const bTs = new Date(_get(b, 'execution_ts', 'EXECUTION_TS') || 0).getTime()
      return bTs - aTs
    })
    return execs.slice(0, 10)
  }, [liveOverview])

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
    const parts = [`Last day close equity: ${formatMoney(lastDayCloseEquity)}`]
    if (currentEquity != null) parts.push(`Current equity: ${formatMoney(currentEquity)}`)
    if (gateState !== 'SAFE') parts.push(`Gate: ${gateState}`)
    if (totalPaidOut > 0) parts.push(`Paid out: ${formatMoney(totalPaidOut)}`)
    if (maxDrawdown != null) parts.push(`Max DD: ${(maxDrawdown * 100).toFixed(1)}%`)
    return parts.join('  \u00B7  ')
  }, [lastDayCloseEquity, currentEquity, gateState, totalPaidOut, maxDrawdown])

  // Lazy-fetch digest on expand
  const handleOpen = useCallback(() => {
    if (isLiveCard) return
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
  }, [pid, digestFetched, digestLoading, isLiveCard])

  return (
    <StoryCard
      attention={attention}
      headline={headline}
      summary={summary}
      accent="portfolio"
      onOpen={isLiveCard ? undefined : handleOpen}
      badges={
        <>
          <GateBadge gateState={gateState} reasonCode={gateReasonCode} />
          <HealthBadge healthState={healthState} />
        </>
      }
    >
      {/* KPI strip */}
      <div className="ck-kpi-row">
        <div className="ck-kpi-item">
          <span className="ck-kpi-label">Last day close equity</span>
          <span className="ck-kpi-value">{formatMoney(lastDayCloseEquity)}</span>
        </div>
        {currentEquity != null && (
          <div className="ck-kpi-item">
            <span className="ck-kpi-label">Current equity</span>
            <span className="ck-kpi-value">{formatMoney(currentEquity)}</span>
          </div>
        )}
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

      {isLiveCard && (
        <div className="ck-live-snapshot">
          <div className="ck-live-meta">
            <span>Snapshot from latest refresh: <strong>{formatTs(liveSnapshotTs)}</strong></span>
            <span className={`ck-live-snapshot-state ck-live-snapshot-state--${liveSnapshotState.toLowerCase() || 'unknown'}`}>
              {liveSnapshotState || 'UNKNOWN'} ({formatAgeShort(liveSnapshotTs)})
            </span>
          </div>

          <div className="ck-live-grid">
            <div className="ck-live-block">
              <h4 className="ck-chart-title">Pending decisions ({pendingDecisions.length})</h4>
              {pendingDecisions.length === 0 ? (
                <p className="ck-empty">No pending decisions right now.</p>
              ) : (
                <ul className="ck-live-list">
                  {pendingDecisions.slice(0, 4).map((d) => {
                    const actionId = _get(d, 'action_id', 'ACTION_ID') || '—'
                    const symbol = _get(d, 'symbol', 'SYMBOL') || '—'
                    const marketType = _get(d, 'market_type', 'MARKET_TYPE', 'asset_class', 'ASSET_CLASS')
                    const side = String(_get(d, 'side', 'SIDE') || '—').toUpperCase()
                    const status = _get(d, 'status', 'STATUS') || '—'
                    const nextStep = _get(d, 'required_next_step', 'REQUIRED_NEXT_STEP')
                    const createdAt = _get(d, 'created_at', 'CREATED_AT', 'timestamps')?.created_at || _get(d, 'created_at', 'CREATED_AT')
                    return (
                      <li key={actionId} className="ck-live-list-item">
                        <strong>{formatSymbolLabel(symbol, marketType)}</strong> {side} · {status} · {formatAgeShort(createdAt)}
                        {nextStep ? <span className="ck-live-subline">Next: {String(nextStep).replaceAll('_', ' ')}</span> : null}
                      </li>
                    )
                  })}
                </ul>
              )}
            </div>

            <div className="ck-live-block">
              <h4 className="ck-chart-title">Open positions ({openPositions.length})</h4>
              {openPositions.length === 0 ? (
                <p className="ck-empty">No open positions.</p>
              ) : (
                <ul className="ck-live-list">
                  {openPositions.slice(0, 5).map((p, idx) => {
                    const symbol = _get(p, 'symbol', 'SYMBOL') || '—'
                    const marketType = _get(p, 'market_type', 'MARKET_TYPE', 'asset_class', 'ASSET_CLASS')
                    const qty = _get(p, 'position_qty', 'POSITION_QTY')
                    const pnl = _get(p, 'unrealized_pnl', 'UNREALIZED_PNL')
                    return (
                      <li key={`${symbol}_${idx}`} className="ck-live-list-item">
                        <strong>{formatSymbolLabel(symbol, marketType)}</strong> · Qty {formatQty(qty)} · P&L{' '}
                        <span className={Number(pnl || 0) >= 0 ? 'ck-kpi--positive' : 'ck-kpi--negative'}>
                          {formatSignedMoney(pnl)}
                        </span>
                      </li>
                    )
                  })}
                </ul>
              )}
            </div>
          </div>

          <div className="ck-live-block ck-live-block--trades">
            <h4 className="ck-chart-title">Latest trades (last {recentTrades.length} of 10)</h4>
            {recentTrades.length === 0 ? (
              <p className="ck-empty">No recent executions yet.</p>
            ) : (
              <ul className="ck-live-list">
                {recentTrades.map((t, idx) => {
                  const symbol = _get(t, 'symbol', 'SYMBOL') || '—'
                  const marketType = _get(t, 'market_type', 'MARKET_TYPE', 'asset_class', 'ASSET_CLASS')
                  const side = String(_get(t, 'side', 'SIDE') || '—').toUpperCase()
                  const qty = _get(t, 'qty_filled', 'QTY_FILLED')
                  const price = _get(t, 'avg_fill_price', 'AVG_FILL_PRICE')
                  const ts = _get(t, 'execution_ts', 'EXECUTION_TS')
                  return (
                    <li key={`${symbol}_${ts || idx}`} className="ck-live-list-item">
                      <strong>{formatSymbolLabel(symbol, marketType)}</strong> {side} {formatQty(qty)} @ {price != null ? Number(price).toFixed(4) : '\u2014'}
                      <span className="ck-live-subline">{formatTs(ts)}</span>
                    </li>
                  )
                })}
              </ul>
            )}
          </div>
        </div>
      )}

      {/* Digest content (lazy loaded) */}
      {!isLiveCard && digestLoading && <p className="ck-loading-inline">Loading digest...</p>}

      {!isLiveCard && digest?.found && (
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

      {!isLiveCard && digestFetched && !digest?.found && (
        <p className="ck-empty">No digest yet for this portfolio. Run the pipeline to generate.</p>
      )}

      {/* Link to full portfolio page */}
      <div className="ck-drill-links" style={{ marginTop: '0.5rem' }}>
        {isLiveCard ? (
          <>
            <Link to="/live-portfolio-activity" className="ck-drill-link">Live Portfolio Activity &rarr;</Link>
            <Link to="/decision-console" className="ck-drill-link">AI Agent Decisions &rarr;</Link>
          </>
        ) : (
          <>
            <Link to="/live-portfolio-activity" className="ck-drill-link">Live Portfolio Activity &rarr;</Link>
            <Link to="/decision-console" className="ck-drill-link">AI Agent Decisions &rarr;</Link>
          </>
        )}
      </div>
    </StoryCard>
  )
}

/* ── Main Cockpit Page ───────────────────────────────── */

export default function Cockpit() {
  const { loading: portfoliosLoading } = usePortfolios()
  const { formatSymbolLabel } = useSymbolMeta()

  const [todayData, setTodayData] = useState(null)
  const [marketPulse, setMarketPulse] = useState(null)
  const [newsOverview, setNewsOverview] = useState(null)
  const [liveOverview, setLiveOverview] = useState(null)
  const [liveOverviewError, setLiveOverviewError] = useState('')
  const [loading, setLoading] = useState(true)
  const [ibJobRunning, setIbJobRunning] = useState(false)
  const [ibJobNotice, setIbJobNotice] = useState({ type: '', text: '' })
  const [ibDailyHealth, setIbDailyHealth] = useState(null)
  const [ibHealthCheckedAt, setIbHealthCheckedAt] = useState(null)
  const [ibHealthLoading, setIbHealthLoading] = useState(false)

  const loadLiveOverview = useCallback(async () => {
    try {
      const resp = await fetch(`${API_BASE}/live/activity/overview`)
      const data = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(data?.detail || `Live overview load failed (${resp.status})`)
      }
      setLiveOverview(data || null)
      setLiveOverviewError('')
      return true
    } catch (e) {
      setLiveOverview(null)
      setLiveOverviewError(e?.message || 'Live overview load failed')
      return false
    }
  }, [])

  const runSnapshotRefresh = useCallback(async () => {
    const resp = await fetch(`${API_BASE}/live/snapshot/refresh`, { method: 'POST' })
    const payload = await resp.json().catch(() => ({}))
    if (!resp.ok) {
      throw new Error(payload?.detail || `Snapshot refresh failed (${resp.status})`)
    }
    return payload
  }, [])

  const loadIbDailyHealth = useCallback(async () => {
    setIbHealthLoading(true)
    try {
      const resp = await fetch(`${API_BASE}/manage/ib/daily-job/health`)
      const data = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(data?.detail || `Failed to load IB health (${resp.status})`)
      }
      setIbDailyHealth(data)
    } catch (e) {
      setIbDailyHealth({ status: 'ERROR', error: e?.message || 'Failed to load IB health.' })
    } finally {
      setIbHealthCheckedAt(new Date().toISOString())
      setIbHealthLoading(false)
    }
  }, [])

  // Parallel fetch non-portfolio data sources
  useEffect(() => {
    if (portfoliosLoading) return
    let cancelled = false
    setLoading(true)
    ;(async () => {
      try {
        // Automation-first: refresh broker snapshot before cockpit data render.
        try {
          await runSnapshotRefresh()
        } catch {
          // Non-fatal: still render cockpit with best available data.
        }

        const fetches = [
          fetch(`${API_BASE}/today`).then(r => r.ok ? r.json() : null).catch(() => null),
          fetch(`${API_BASE}/market/pulse`).then(r => r.ok ? r.json() : null).catch(() => null),
          fetch(`${API_BASE}/news/intelligence/overview`).then(r => r.ok ? r.json() : null).catch(() => null),
          loadLiveOverview(),
          loadIbDailyHealth(),
        ]
        const [td, mp, no] = await Promise.all(fetches)
        if (cancelled) return
        setTodayData(td)
        setMarketPulse(mp)
        setNewsOverview(no)
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()

    return () => { cancelled = true }
  }, [portfoliosLoading, loadLiveOverview, loadIbDailyHealth, runSnapshotRefresh])

  const insights = todayData?.insights || []
  const aggregate = marketPulse?.aggregate || {}
  const marketSymbols = marketPulse?.symbols || []
  const indexSeries = marketPulse?.index_series || []

  // Attention levels for right-column stories
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

  const newsAttention = useMemo(() => {
    const tone = newsOverview?.tone
    if (!tone) return 'neutral'
    if (tone === 'HIGH_RISK') return 'critical'
    if (tone === 'CAUTION') return 'warning'
    return 'info'
  }, [newsOverview])

  const dailyReadiness = todayData?.daily_readiness || null
  const readinessAttention = useMemo(() => {
    if (!dailyReadiness?.found) return 'neutral'
    const proposals = Number(dailyReadiness?.counts?.proposals_generated ?? 0)
    if (proposals > 0) return 'positive'
    const signals = Number(dailyReadiness?.counts?.signals_generated ?? 0)
    return signals > 0 ? 'info' : 'neutral'
  }, [dailyReadiness])

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
    const topLabel = top ? formatSymbolLabel(top.symbol, top.market_type) : '—'
    const bottomLabel = bottom ? formatSymbolLabel(bottom.symbol, bottom.market_type) : '—'
    return `Average return ${avgPct >= 0 ? '+' : ''}${avgPct}%. Top: ${topLabel} (+${((top?.day_return || 0) * 100).toFixed(1)}%), Bottom: ${bottomLabel} (${((bottom?.day_return || 0) * 100).toFixed(1)}%).`
  }, [marketSymbols, aggregate, formatSymbolLabel])

  // Signal headline
  const signalHeadline = insights.length > 0
    ? `${insights.length} Committee Candidate${insights.length > 1 ? 's' : ''} Today`
    : 'Committee Candidates \u2014 None Today'

  const signalSummary = useMemo(() => {
    if (insights.length === 0) return 'No committee-ready candidates found today.'
    const top = insights[0]
    return `Top candidate: ${formatSymbolLabel(top.symbol, top.market_type)} (${top.maturity_stage}, score ${top.maturity_score}). ${top.why_this_is_here || ''}`
  }, [insights, formatSymbolLabel])

  const newsHeadline = useMemo(() => {
    if (!newsOverview?.found) return 'News Intelligence — Awaiting Data'
    const hot = newsOverview?.metrics?.hot_symbols ?? 0
    const stale = newsOverview?.metrics?.stale_symbols ?? 0
    return `News Intelligence — ${hot} HOT, ${stale} stale symbols`
  }, [newsOverview])

  const newsSummary = useMemo(() => {
    if (!newsOverview?.found) return 'No news intelligence overview available yet.'
    return newsOverview.executive_summary || 'Committee-focused headline summary is available.'
  }, [newsOverview])

  const readinessHeadline = useMemo(() => {
    if (!dailyReadiness?.found) return 'Daily Trade Readiness — Awaiting Data'
    const c = dailyReadiness?.counts || {}
    return `Daily Trade Readiness — ${c.signals_generated ?? 0} signals, ${c.proposals_generated ?? 0} proposals`
  }, [dailyReadiness])

  const readinessSummary = useMemo(() => {
    if (!dailyReadiness?.found) return 'No readiness summary yet. Run the daily pipeline.'
    const run = dailyReadiness?.last_run || {}
    const c = dailyReadiness?.counts || {}
    return `Run ${String(run.run_id || '').slice(0, 12)} (${run.status || 'UNKNOWN'}) · Signals ${c.signals_generated ?? 0}, eligible ${c.signals_eligible ?? 0}, proposals ${c.proposals_generated ?? 0}.`
  }, [dailyReadiness])

  const liveCardPortfolio = useMemo(() => {
    const p = liveOverview?.portfolio || {}
    const pid = Number(p?.portfolio_id)
    if (!Number.isFinite(pid)) return null
    const k = liveOverview?.account_kpis || {}
    const readiness = liveOverview?.readiness || {}
    const reasons = Array.isArray(readiness?.blocking_reasons) ? readiness.blocking_reasons : []
    const reasonText = reasons.length ? reasons.join(', ') : 'No blocking reasons.'
    const primaryReasonCode = reasons[0] || ''
    const snapshotState = String(readiness?.snapshot_state || '').toUpperCase()
    const driftState = String(readiness?.drift_state || '').toUpperCase()
    const healthState = snapshotState === 'FRESH' || snapshotState === 'AGING' || snapshotState === 'READY'
      ? 'OK'
      : (snapshotState === 'STALE' ? 'STALE' : (k?.snapshot_ts ? 'STALE' : 'BROKEN'))
    const gateState = readiness?.actionable ? 'SAFE' : (driftState === 'BLOCKED' ? 'STOPPED' : 'CAUTION')
    return {
      IS_LIVE_CARD: true,
      PORTFOLIO_ID: pid,
      NAME: `Live Portfolio #${pid}`,
      STATUS: p?.is_active === false ? 'INACTIVE' : 'ACTIVE',
      GATE_STATE: gateState,
      GATE_REASON: reasonText,
      GATE_REASON_CODE: primaryReasonCode,
      gate_tooltip: reasonText,
      health_state: healthState,
      last_day_close_equity: k?.equity_nav_eur ?? null,
      current_equity: k?.equity_nav_eur ?? null,
      FINAL_EQUITY: k?.equity_nav_eur ?? null,
      TOTAL_RETURN: null,
      MAX_DRAWDOWN: null,
      TOTAL_PAID_OUT: 0,
      LIVE_OVERVIEW: liveOverview,
    }
  }, [liveOverview])

  const cockpitPortfolios = useMemo(() => {
    return liveCardPortfolio ? [liveCardPortfolio] : []
  }, [liveCardPortfolio])

  // Split portfolios: active first, then others (live-only scope)
  const activePortfolios = useMemo(() => {
    return cockpitPortfolios.filter(p => (_get(p, 'STATUS', 'status') || '').toUpperCase() === 'ACTIVE')
  }, [cockpitPortfolios])

  const otherPortfolios = useMemo(() => {
    return cockpitPortfolios.filter(p => (_get(p, 'STATUS', 'status') || '').toUpperCase() !== 'ACTIVE')
  }, [cockpitPortfolios])

  const gatedPortfoliosCount = useMemo(() => (
    cockpitPortfolios.filter((p) => {
      const gate = (_get(p, 'GATE_STATE', 'gate_state') || 'SAFE').toUpperCase()
      return gate !== 'SAFE'
    }).length
  ), [cockpitPortfolios])

  const ibFreshness = useMemo(() => {
    if (ibHealthLoading && !ibDailyHealth) {
      return { text: 'Checking daily data status...', tone: 'neutral' }
    }
    if (!ibDailyHealth) {
      return { text: 'Daily data status unavailable right now.', tone: 'neutral' }
    }
    if (ibDailyHealth.status === 'ERROR') {
      return {
        text: 'Daily data status unavailable. Open Debug if this persists.',
        tone: 'error',
      }
    }
    const c = ibDailyHealth.coverage || {}
    const barsDate = c.latest_daily_bar_date || null
    const lag = c.bars_lag_days
    const barSymbols = Number(c.bar_symbols_on_latest_date ?? 0)
    const universeSymbols = Number(c.universe_symbols ?? 0)
    const missingSymbols = Number(c.missing_symbols_on_latest_date ?? 0)
    const pipelineDate = ibDailyHealth?.pipeline?.latest_effective_to_date || null
    const checkedAt = ibHealthCheckedAt ? formatTs(ibHealthCheckedAt) : '—'
    const hasGaps = universeSymbols > 0 && missingSymbols > 0
    const lagged = typeof lag === 'number' ? lag > 0 : false
    const pipelineBehind = Boolean(barsDate && pipelineDate && pipelineDate < barsDate)
    const shouldRun = hasGaps || lagged || pipelineBehind

    if (shouldRun) {
      return {
        text: `Run IB Daily Job now. Data is not ready yet (${barSymbols}/${universeSymbols} symbols loaded${barsDate ? ` for ${barsDate}` : ''}${pipelineDate ? `; pipeline currently at ${pipelineDate}` : ''}). Checked ${checkedAt}.`,
        tone: 'warn',
      }
    }

    return {
      text: `No action needed. Daily data is complete${barsDate ? ` for ${barsDate}` : ''}. Checked ${checkedAt}.`,
      tone: 'ok',
    }
  }, [ibDailyHealth, ibHealthCheckedAt, ibHealthLoading])

  const runIbDailyJob = useCallback(async () => {
    const confirmed = window.confirm('Run full IB daily job now? This ingests bars and executes catch-up replay.')
    if (!confirmed) return
    setIbJobRunning(true)
    setIbJobNotice({ type: '', text: '' })
    try {
      const qs = new URLSearchParams()
      qs.set('dry_run', 'false')
      qs.set('skip_ingest', 'false')
      qs.set('run_pipeline', 'true')
      const resp = await fetch(`${API_BASE}/manage/ib/daily-job/run?${qs.toString()}`, {
        method: 'POST',
      })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(payload?.detail?.message || payload?.detail || `IB daily job failed (${resp.status})`)
      }
      setIbJobNotice({ type: 'ok', text: 'IB Daily Job completed and daily pipeline triggered.' })
      await loadIbDailyHealth()
    } catch (e) {
      setIbJobNotice({ type: 'error', text: e?.message || 'IB Daily Job failed.' })
      await loadIbDailyHealth()
    } finally {
      setIbJobRunning(false)
    }
  }, [loadIbDailyHealth])

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
        <div className="ck-header-actions">
          <button className="ck-op-btn" type="button" onClick={runIbDailyJob} disabled={ibJobRunning}>
            {ibJobRunning ? 'Running IB Daily Job...' : 'Run IB Daily Job'}
          </button>
        </div>
      </div>
      {ibJobNotice.text ? (
        <div className={`ck-op-msg ${ibJobNotice.type === 'ok' ? 'ck-op-msg--ok' : 'ck-op-msg--error'}`}>
          {ibJobNotice.text}
        </div>
      ) : null}
      {liveOverviewError ? (
        <div className="ck-op-msg ck-op-msg--error">
          Live scope warning: {liveOverviewError}
        </div>
      ) : null}
      <div className={`ck-refresh-line ck-refresh-line--${ibFreshness.tone}`}>
        {ibFreshness.text}
      </div>
      <div className="ck-summary-strip">
        <div className="ck-summary-card">
          <span className="ck-summary-label">Active portfolios</span>
          <span className="ck-summary-value">{activePortfolios.length}</span>
        </div>
        <div className="ck-summary-card">
          <span className="ck-summary-label">Gated portfolios</span>
          <span className={`ck-summary-value ${gatedPortfoliosCount > 0 ? 'ck-kpi--negative' : 'ck-kpi--positive'}`}>
            {gatedPortfoliosCount}
          </span>
        </div>
        <div className="ck-summary-card">
          <span className="ck-summary-label">Committee candidates</span>
          <span className="ck-summary-value">{insights.length}</span>
        </div>
        <div className="ck-summary-card">
          <span className="ck-summary-label">Market breadth</span>
          <span className="ck-summary-value">{aggregate.breadth_pct ?? '—'}%</span>
        </div>
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

        {/* ── RIGHT COLUMN: Market, System, Committee, Training ── */}
        <div className="ck-news-column">
          <div className="ck-column-label">Market, System & Committee</div>

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

          {/* Story: Committee Candidates */}
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
                      <span className="ck-insight-mini-symbol">{formatSymbolLabel(item.symbol, item.market_type)}</span>
                      <StagePill stage={item.maturity_stage} />
                      <span className="ck-insight-mini-score">{item.maturity_score}/100</span>
                    </div>
                    <div className="ck-upcoming-bar-wrap">
                      <div className="ck-upcoming-bar" style={{ width: `${Math.min(100, Math.max(0, item.maturity_score ?? 0))}%` }} />
                    </div>
                    <p className="ck-insight-mini-why">{item.why_this_is_here || '\u2014'}</p>
                  </div>
                ))}
                <div className="ck-drill-links">
                  <Link to="/decision-console" className="ck-drill-link">Open AI Agent Decisions &rarr;</Link>
                </div>
              </div>
            ) : (
              <EmptyState title="No committee candidates today" action="Candidates appear when symbols have sufficient maturity and pass policy checks." />
            )}
          </StoryCard>

          {/* Story: News Intelligence */}
          <StoryCard
            attention={newsAttention}
            headline={newsHeadline}
            summary={newsSummary}
            accent="system"
            badges={
              <>
                <span className="ck-badge ck-badge--scope">News</span>
                {newsOverview?.found && <AiBadge isAi={newsOverview?.is_ai_generated} modelInfo={newsOverview?.model_info} />}
                {newsOverview?.found && <FreshnessBadge createdAt={newsOverview?.generated_at} />}
              </>
            }
          >
            <NewsIntelligenceOverview overview={newsOverview} />
          </StoryCard>

          {/* Story: Daily Trade Readiness */}
          <StoryCard
            attention={readinessAttention}
            headline={readinessHeadline}
            summary={readinessSummary}
            accent="training"
          >
            <DailyReadinessOverview readiness={dailyReadiness} />
          </StoryCard>
        </div>
      </div>
    </div>
  )
}
