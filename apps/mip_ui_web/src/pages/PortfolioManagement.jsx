import { useState, useEffect, useCallback } from 'react'
import {
  LineChart, Line, AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ReferenceLine, Cell, Legend,
} from 'recharts'
import { API_BASE } from '../App'
import useVisibleInterval from '../hooks/useVisibleInterval'
import './PortfolioManagement.css'

const TABS = [
  { id: 'portfolios', label: 'Portfolios' },
  { id: 'profiles', label: 'Profiles' },
  { id: 'lifecycle', label: 'Lifecycle Timeline' },
  { id: 'story', label: 'Portfolio Story' },
]

const fmt$ = (v) => v == null ? '—' : '$' + Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
const fmtPct = (v) => v == null ? '—' : (Number(v) * 100).toFixed(2) + '%'
const fmtDate = (v) => {
  if (!v) return '—'
  try { return new Date(v).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' }) }
  catch { return v }
}
const fmtDateTime = (v) => {
  if (!v) return '—'
  try { return new Date(v).toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) }
  catch { return v }
}

// ─── Main Component ──────────────────────────────────────────────────────────

export default function PortfolioManagement() {
  const [tab, setTab] = useState('portfolios')
  const [portfolios, setPortfolios] = useState([])
  const [profiles, setProfiles] = useState([])
  const [loading, setLoading] = useState(true)
  const [feedback, setFeedback] = useState(null)

  // Pipeline lock state -- disables all write actions when pipeline is running
  const [pipelineRunning, setPipelineRunning] = useState(false)

  // Lifecycle tab state
  const [selectedPortfolioId, setSelectedPortfolioId] = useState(null)
  const [lifecycle, setLifecycle] = useState(null)
  const [lifecycleLoading, setLifecycleLoading] = useState(false)

  // Narrative tab state
  const [narrative, setNarrative] = useState(null)
  const [narrativeLoading, setNarrativeLoading] = useState(false)
  const [narrativeGenerating, setNarrativeGenerating] = useState(false)

  // Modal state
  const [modal, setModal] = useState(null) // { type: 'portfolio'|'profile'|'cash', data?: ... }

  // ── Poll pipeline status (pauses when tab hidden) ──
  useVisibleInterval(
    useCallback(() => {
      fetch(`${API_BASE}/status`)
        .then(r => r.ok ? r.json() : {})
        .then(d => setPipelineRunning(!!d.pipeline_running))
        .catch(() => {})
    }, []),
    15_000,
  )

  // ── Load portfolios + profiles ──
  const loadData = useCallback(async () => {
    setLoading(true)
    try {
      const [pRes, prRes] = await Promise.all([
        fetch(`${API_BASE}/portfolios`).then(r => r.ok ? r.json() : []),
        fetch(`${API_BASE}/manage/profiles`).then(r => r.ok ? r.json() : { profiles: [] }),
      ])
      setPortfolios(Array.isArray(pRes) ? pRes : [])
      setProfiles(prRes.profiles || [])
      // Auto-select first portfolio
      const pList = Array.isArray(pRes) ? pRes : []
      if (pList.length > 0 && !selectedPortfolioId) {
        setSelectedPortfolioId(pList[0].PORTFOLIO_ID || pList[0].portfolio_id)
      }
    } catch (e) {
      console.error('Failed to load management data', e)
    } finally {
      setLoading(false)
    }
  }, [selectedPortfolioId])

  useEffect(() => { loadData() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Load lifecycle events for selected portfolio ──
  useEffect(() => {
    if (!selectedPortfolioId || (tab !== 'lifecycle' && tab !== 'story')) return
    if (tab === 'lifecycle') {
      setLifecycleLoading(true)
      fetch(`${API_BASE}/manage/portfolios/${selectedPortfolioId}/lifecycle`)
        .then(r => r.ok ? r.json() : null)
        .then(d => setLifecycle(d))
        .catch(() => setLifecycle(null))
        .finally(() => setLifecycleLoading(false))
    }
    if (tab === 'story') {
      setNarrativeLoading(true)
      fetch(`${API_BASE}/manage/portfolios/${selectedPortfolioId}/narrative`)
        .then(r => r.ok ? r.json() : null)
        .then(d => {
          if (d && d.narrative) {
            // Cached narrative exists, show it
            setNarrative(d)
            setNarrativeLoading(false)
          } else {
            // No cached narrative -- auto-generate
            setNarrativeLoading(false)
            setNarrativeGenerating(true)
            fetch(`${API_BASE}/manage/portfolios/${selectedPortfolioId}/narrative`, { method: 'POST' })
              .then(() => fetch(`${API_BASE}/manage/portfolios/${selectedPortfolioId}/narrative`))
              .then(r => r.ok ? r.json() : null)
              .then(d2 => setNarrative(d2))
              .catch(() => setNarrative(null))
              .finally(() => setNarrativeGenerating(false))
          }
        })
        .catch(() => { setNarrative(null); setNarrativeLoading(false) })
    }
  }, [selectedPortfolioId, tab])

  // ── Feedback auto-clear ──
  useEffect(() => {
    if (feedback) {
      const t = setTimeout(() => setFeedback(null), 5000)
      return () => clearTimeout(t)
    }
  }, [feedback])

  // ── API call helper ──
  const apiCall = async (method, url, body) => {
    try {
      const opts = { method, headers: { 'Content-Type': 'application/json' } }
      if (body) opts.body = JSON.stringify(body)
      const res = await fetch(`${API_BASE}${url}`, opts)
      let data
      const text = await res.text()
      try { data = JSON.parse(text) } catch { data = { detail: text } }
      if (!res.ok) {
        setFeedback({ type: 'error', message: data.detail || `Request failed (${res.status})` })
        return null
      }
      setFeedback({ type: 'success', message: data.action ? `${data.action} successfully` : 'Success' })
      await loadData()
      return data
    } catch (e) {
      setFeedback({ type: 'error', message: e.message })
      return null
    }
  }

  // ── Generate narrative ──
  const generateNarrative = async () => {
    if (!selectedPortfolioId) return
    setNarrativeGenerating(true)
    try {
      const res = await fetch(`${API_BASE}/manage/portfolios/${selectedPortfolioId}/narrative`, { method: 'POST' })
      let resData
      const resText = await res.text()
      try { resData = JSON.parse(resText) } catch { resData = { detail: resText } }
      if (res.ok) {
        // Reload narrative
        const narRes = await fetch(`${API_BASE}/manage/portfolios/${selectedPortfolioId}/narrative`)
        if (narRes.ok) setNarrative(await narRes.json())
        setFeedback({ type: 'success', message: 'Portfolio story generated successfully' })
      } else {
        setFeedback({ type: 'error', message: resData.detail || `Failed to generate narrative (${res.status})` })
      }
    } catch (e) {
      setFeedback({ type: 'error', message: e.message })
    } finally {
      setNarrativeGenerating(false)
    }
  }

  const pid = (p) => p.PORTFOLIO_ID || p.portfolio_id
  const pname = (p) => p.NAME || p.name || p.PORTFOLIO_NAME || p.portfolio_name

  if (loading) return <div className="mgmt-loading">Loading management data...</div>

  return (
    <div>
      <h1>Portfolio Management</h1>

      {pipelineRunning && (
        <div className="mgmt-pipeline-lock">
          Pipeline is currently running &mdash; editing is disabled until the run completes.
        </div>
      )}

      {feedback && (
        <div className={feedback.type === 'error' ? 'mgmt-error' : 'mgmt-success'}>
          {feedback.message}
        </div>
      )}

      {/* Tabs */}
      <div className="mgmt-tabs">
        {TABS.map(t => (
          <button
            key={t.id}
            className={`mgmt-tab ${tab === t.id ? 'mgmt-tab--active' : ''}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === 'portfolios' && (
        <PortfoliosTab
          portfolios={portfolios}
          profiles={profiles}
          onEdit={(p) => setModal({ type: 'portfolio', data: p })}
          onCreate={() => setModal({ type: 'portfolio', data: null })}
          onCash={(p) => setModal({ type: 'cash', data: p })}
          onAttachProfile={(p) => setModal({ type: 'attach', data: p })}
          disabled={pipelineRunning}
        />
      )}

      {tab === 'profiles' && (
        <ProfilesTab
          profiles={profiles}
          onEdit={(p) => setModal({ type: 'profile', data: p })}
          onCreate={() => setModal({ type: 'profile', data: null })}
          disabled={pipelineRunning}
        />
      )}

      {tab === 'lifecycle' && (
        <LifecycleTab
          portfolios={portfolios}
          selectedPortfolioId={selectedPortfolioId}
          onSelectPortfolio={setSelectedPortfolioId}
          lifecycle={lifecycle}
          loading={lifecycleLoading}
        />
      )}

      {tab === 'story' && (
        <StoryTab
          portfolios={portfolios}
          selectedPortfolioId={selectedPortfolioId}
          onSelectPortfolio={setSelectedPortfolioId}
          narrative={narrative}
          loading={narrativeLoading}
          generating={narrativeGenerating}
          onGenerate={generateNarrative}
        />
      )}

      {/* Modals */}
      {modal?.type === 'portfolio' && (
        <PortfolioModal
          data={modal.data}
          profiles={profiles}
          onClose={() => setModal(null)}
          onSave={(body) => {
            const isCreate = !modal.data
            apiCall(isCreate ? 'POST' : 'PUT',
              isCreate ? '/manage/portfolios' : `/manage/portfolios/${pid(modal.data)}`,
              body
            ).then(r => { if (r) setModal(null) })
          }}
        />
      )}

      {modal?.type === 'profile' && (
        <ProfileModal
          data={modal.data}
          onClose={() => setModal(null)}
          onSave={(body) => {
            const isCreate = !modal.data
            apiCall(isCreate ? 'POST' : 'PUT',
              isCreate ? '/manage/profiles' : `/manage/profiles/${modal.data.PROFILE_ID}`,
              body
            ).then(r => { if (r) setModal(null) })
          }}
        />
      )}

      {modal?.type === 'cash' && (
        <CashEventModal
          portfolio={modal.data}
          onClose={() => setModal(null)}
          onSave={(body) => {
            apiCall('POST', `/manage/portfolios/${pid(modal.data)}/cash`, body)
              .then(r => { if (r) setModal(null) })
          }}
        />
      )}

      {modal?.type === 'attach' && (
        <AttachProfileModal
          portfolio={modal.data}
          profiles={profiles}
          onClose={() => setModal(null)}
          onSave={(profileId) => {
            apiCall('PUT', `/manage/portfolios/${pid(modal.data)}/profile`, { profile_id: profileId })
              .then(r => { if (r) setModal(null) })
          }}
        />
      )}
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// Tab 1: Portfolios
// ═══════════════════════════════════════════════════════════════════════════════

function PortfoliosTab({ portfolios, profiles, onEdit, onCreate, onCash, onAttachProfile, disabled }) {
  const profileMap = {}
  profiles.forEach(p => { profileMap[p.PROFILE_ID] = p.NAME })

  return (
    <div>
      <div className="mgmt-section-header">
        <h2>Portfolios</h2>
        <button className="mgmt-btn mgmt-btn-primary" onClick={onCreate} disabled={disabled}>+ Create Portfolio</button>
      </div>
      <table className="mgmt-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Profile</th>
            <th>Starting Cash</th>
            <th>Final Equity</th>
            <th>Return</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {portfolios.map(p => {
            const id = p.PORTFOLIO_ID || p.portfolio_id
            const profileId = p.PROFILE_ID || p.profile_id
            return (
              <tr key={id}>
                <td>{id}</td>
                <td><strong>{p.NAME || p.name}</strong></td>
                <td>{profileMap[profileId] || profileId || '—'}</td>
                <td>{fmt$(p.STARTING_CASH || p.starting_cash)}</td>
                <td>{fmt$(p.FINAL_EQUITY || p.final_equity)}</td>
                <td>{fmtPct(p.TOTAL_RETURN || p.total_return)}</td>
                <td>
                  <span className={`status-badge ${(p.STATUS || p.status) === 'ACTIVE' ? '' : 'status-badge--warn'}`}>
                    {p.STATUS || p.status}
                  </span>
                </td>
                <td>
                  <div style={{ display: 'flex', gap: '0.3rem', flexWrap: 'wrap' }}>
                    <button className="mgmt-btn mgmt-btn-secondary mgmt-btn-sm" onClick={() => onEdit(p)} disabled={disabled}>Edit</button>
                    <button className="mgmt-btn mgmt-btn-success mgmt-btn-sm" onClick={() => onCash(p)} disabled={disabled}>Cash</button>
                    <button className="mgmt-btn mgmt-btn-primary mgmt-btn-sm" onClick={() => onAttachProfile(p)} disabled={disabled}>Profile</button>
                  </div>
                </td>
              </tr>
            )
          })}
          {portfolios.length === 0 && (
            <tr><td colSpan={8} style={{ textAlign: 'center', color: '#888', padding: '2rem' }}>No portfolios yet. Create your first one.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// Tab 2: Profiles
// ═══════════════════════════════════════════════════════════════════════════════

function ProfilesTab({ profiles, onEdit, onCreate, disabled }) {
  return (
    <div>
      <div className="mgmt-section-header">
        <h2>Portfolio Profiles</h2>
        <button className="mgmt-btn mgmt-btn-primary" onClick={onCreate} disabled={disabled}>+ Create Profile</button>
      </div>
      <table className="mgmt-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Max Pos</th>
            <th>Max Pos %</th>
            <th>Bust %</th>
            <th>Bust Action</th>
            <th>DD Stop %</th>
            <th>Crystallize</th>
            <th>Used By</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {profiles.map(p => (
            <tr key={p.PROFILE_ID}>
              <td>{p.PROFILE_ID}</td>
              <td><strong>{p.NAME}</strong></td>
              <td>{p.MAX_POSITIONS ?? '—'}</td>
              <td>{p.MAX_POSITION_PCT != null ? fmtPct(p.MAX_POSITION_PCT) : '—'}</td>
              <td>{p.BUST_EQUITY_PCT != null ? fmtPct(p.BUST_EQUITY_PCT) : '—'}</td>
              <td>{p.BUST_ACTION || '—'}</td>
              <td>{p.DRAWDOWN_STOP_PCT != null ? fmtPct(p.DRAWDOWN_STOP_PCT) : '—'}</td>
              <td>
                {p.CRYSTALLIZE_ENABLED ? (
                  <span style={{ color: '#198754', fontWeight: 600 }}>
                    {fmtPct(p.PROFIT_TARGET_PCT)} ({p.CRYSTALLIZE_MODE})
                  </span>
                ) : <span style={{ color: '#888' }}>Off</span>}
              </td>
              <td>{p.PORTFOLIO_COUNT || 0} portfolio{(p.PORTFOLIO_COUNT || 0) !== 1 ? 's' : ''}</td>
              <td>
                <button className="mgmt-btn mgmt-btn-secondary mgmt-btn-sm" onClick={() => onEdit(p)} disabled={disabled}>Edit</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// Tab 3: Lifecycle Timeline + Charts
// ═══════════════════════════════════════════════════════════════════════════════

function LifecycleTab({ portfolios, selectedPortfolioId, onSelectPortfolio, lifecycle, loading }) {
  const events = lifecycle?.events || []
  const dailySeries = lifecycle?.daily_series || []
  const pid = (p) => p.PORTFOLIO_ID || p.portfolio_id

  // Prepare chart data from events (ordered ASC)
  const chartData = events.map(e => ({
    ts: fmtDate(e.EVENT_TS),
    type: e.EVENT_TYPE,
    equity: Number(e.EQUITY_AFTER) || 0,
    cash: Number(e.CASH_AFTER) || 0,
    pnl: Number(e.CUMULATIVE_PNL) || 0,
    amount: Number(e.AMOUNT) || 0,
    label: e.EVENT_LABEL,
  }))

  // Daily data for Cash vs Equity (real intraday divergence)
  const dailyChartData = dailySeries.map(d => ({
    ts: fmtDate(d.ts),
    equity: d.equity ?? 0,
    cash: d.cash ?? 0,
  }))

  // Cash flow data (only money events)
  const cashFlowData = events
    .filter(e => ['CREATE', 'DEPOSIT', 'WITHDRAW', 'CRYSTALLIZE'].includes(e.EVENT_TYPE))
    .map(e => ({
      ts: fmtDate(e.EVENT_TS),
      type: e.EVENT_TYPE,
      amount: e.EVENT_TYPE === 'WITHDRAW' || e.EVENT_TYPE === 'CRYSTALLIZE'
        ? -(Number(e.AMOUNT) || 0) : (Number(e.AMOUNT) || 0),
      netContributed: Number(e.NET_CONTRIBUTED) || 0,
      label: e.EVENT_LABEL,
    }))

  return (
    <div>
      <PortfolioSelector
        portfolios={portfolios}
        selectedId={selectedPortfolioId}
        onChange={onSelectPortfolio}
      />

      {loading && <div className="mgmt-loading">Loading lifecycle events...</div>}

      {!loading && events.length === 0 && (
        <div className="mgmt-card" style={{ textAlign: 'center', color: '#888', padding: '3rem' }}>
          No lifecycle events recorded yet. Create a portfolio or perform a cash event to begin tracking.
        </div>
      )}

      {!loading && events.length > 0 && (
        <>
          {/* Charts */}
          <div className="mgmt-charts-grid">
            {/* Lifetime Equity Curve */}
            <div className="mgmt-chart-card">
              <h4>Lifetime Equity</h4>
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                  <XAxis dataKey="ts" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} tickFormatter={v => '$' + (v / 1000).toFixed(1) + 'k'} />
                  <Tooltip
                    formatter={(v) => [fmt$(v), 'Equity']}
                    labelFormatter={(l) => l}
                  />
                  <Line type="monotone" dataKey="equity" stroke="#0066cc" strokeWidth={2} dot={{ r: 4 }} />
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Cumulative P&L */}
            <div className="mgmt-chart-card">
              <h4>Cumulative Lifetime P&L</h4>
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                  <XAxis dataKey="ts" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} tickFormatter={v => '$' + v.toLocaleString()} />
                  <Tooltip
                    formatter={(v) => [fmt$(v), 'P&L']}
                    labelFormatter={(l) => l}
                  />
                  <ReferenceLine y={0} stroke="#999" strokeDasharray="3 3" />
                  <defs>
                    <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#198754" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#198754" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <Area type="monotone" dataKey="pnl" stroke="#198754" fill="url(#pnlGrad)" strokeWidth={2} dot={{ r: 4 }} />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            {/* Cash Flow Waterfall */}
            <div className="mgmt-chart-card">
              <h4>Cash Flow Events</h4>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={cashFlowData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                  <XAxis dataKey="ts" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} tickFormatter={v => '$' + v.toLocaleString()} />
                  <Tooltip
                    formatter={(v, name) => [fmt$(Math.abs(v)), name === 'amount' ? 'Cash Flow' : 'Net Contributed']}
                    labelFormatter={(l, payload) => payload?.[0]?.payload?.label || l}
                  />
                  <ReferenceLine y={0} stroke="#999" />
                  <Bar dataKey="amount" name="Cash Flow">
                    {cashFlowData.map((entry, i) => (
                      <Cell key={i} fill={entry.amount >= 0 ? '#198754' : '#dc3545'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Cash + Equity Over Time — uses daily series for real divergence */}
            <div className="mgmt-chart-card">
              <h4>Cash vs Equity</h4>
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={dailyChartData.length > 0 ? dailyChartData : chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                  <XAxis dataKey="ts" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} tickFormatter={v => '$' + (v / 1000).toFixed(1) + 'k'} />
                  <Tooltip formatter={(v) => [fmt$(v)]} />
                  <Legend />
                  <Line type="monotone" dataKey="cash" stroke="#ff9800" strokeWidth={2} dot={false} name="Cash" />
                  <Line type="monotone" dataKey="equity" stroke="#0066cc" strokeWidth={2} dot={false} name="Total Equity" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Event Timeline */}
          <h3>Event Timeline</h3>
          <div className="mgmt-timeline">
            {events.map(e => (
              <div key={e.EVENT_ID} className="mgmt-timeline-event">
                <div className={`mgmt-timeline-dot mgmt-timeline-dot--${(e.EVENT_TYPE || '').toLowerCase()}`} />
                <div className="mgmt-timeline-header">
                  <span className="mgmt-timeline-type">{e.EVENT_LABEL || e.EVENT_TYPE}</span>
                  <span className="mgmt-timeline-ts">{fmtDateTime(e.EVENT_TS)}</span>
                </div>
                {e.AMOUNT != null && e.AMOUNT > 0 && !['EPISODE_START'].includes(e.EVENT_TYPE) && (() => {
                  const isPositive = ['DEPOSIT', 'CREATE', 'EPISODE_END'].includes(e.EVENT_TYPE)
                  return (
                    <span className={`mgmt-timeline-amount ${isPositive ? 'mgmt-timeline-amount--positive' : 'mgmt-timeline-amount--negative'}`}>
                      {isPositive ? '+' : '-'}{fmt$(e.AMOUNT)}
                    </span>
                  )
                })()}
                <div className="mgmt-timeline-snapshots">
                  <span>Cash: {fmt$(e.CASH_AFTER)}</span>
                  <span>Equity: {fmt$(e.EQUITY_AFTER)}</span>
                  <span>Lifetime P&L: {fmt$(e.CUMULATIVE_PNL)}</span>
                </div>
                {e.NOTES && <div className="mgmt-timeline-notes">{e.NOTES}</div>}
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// Tab 4: Portfolio Story (AI Narrative)
// ═══════════════════════════════════════════════════════════════════════════════

function StoryTab({ portfolios, selectedPortfolioId, onSelectPortfolio, narrative, loading, generating, onGenerate }) {
  const narData = narrative?.narrative
  const narJson = narData?.NARRATIVE_JSON || narData?.narrative_json

  return (
    <div>
      <PortfolioSelector
        portfolios={portfolios}
        selectedId={selectedPortfolioId}
        onChange={onSelectPortfolio}
      />

      <div className="mgmt-section-header">
        <h2>Portfolio Story</h2>
        {narData && (
          <button
            className="mgmt-btn mgmt-btn-secondary mgmt-btn-sm"
            onClick={onGenerate}
            disabled={generating}
          >
            {generating ? 'Regenerating...' : 'Regenerate'}
          </button>
        )}
      </div>

      {loading && <div className="mgmt-loading">Loading narrative...</div>}
      {generating && <div className="mgmt-loading">Generating portfolio story with AI... This may take 10-20 seconds.</div>}

      {!loading && !generating && !narData && (
        <div className="mgmt-card" style={{ textAlign: 'center', color: '#888', padding: '3rem' }}>
          No portfolio story available. The AI narrative will be generated automatically.
        </div>
      )}

      {!loading && !generating && narData && (
        <div className="mgmt-narrative-card">
          {/* Headline */}
          {narJson?.headline && (
            <div className="mgmt-narrative-headline">{narJson.headline}</div>
          )}

          {/* Main narrative */}
          <div className="mgmt-narrative-text">
            {narJson?.narrative
              ? narJson.narrative.split('\n\n').map((para, i) => <p key={i}>{para}</p>)
              : <p>{narData.NARRATIVE_TEXT || narData.narrative_text || 'No narrative text available.'}</p>
            }
          </div>

          {/* Key moments */}
          {narJson?.key_moments?.length > 0 && (
            <div className="mgmt-narrative-moments">
              <h4>Key Moments</h4>
              <ul>
                {narJson.key_moments.map((m, i) => <li key={i}>{m}</li>)}
              </ul>
            </div>
          )}

          {/* Outlook */}
          {narJson?.outlook && (
            <div className="mgmt-narrative-outlook">
              <strong>Outlook:</strong> {narJson.outlook}
            </div>
          )}

          {/* Meta */}
          <div className="mgmt-narrative-meta">
            <span>Model: {narData.MODEL_INFO || narData.model_info || '—'}</span>
            <span>Generated: {fmtDateTime(narData.CREATED_AT || narData.created_at)}</span>
          </div>
        </div>
      )}
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// Shared: Portfolio Selector
// ═══════════════════════════════════════════════════════════════════════════════

function PortfolioSelector({ portfolios, selectedId, onChange }) {
  return (
    <div className="mgmt-portfolio-selector">
      <label>Portfolio:</label>
      <select
        value={selectedId || ''}
        onChange={e => onChange(Number(e.target.value))}
      >
        {portfolios.map(p => {
          const id = p.PORTFOLIO_ID || p.portfolio_id
          return <option key={id} value={id}>{p.NAME || p.name} (#{id})</option>
        })}
      </select>
    </div>
  )
}


// ═══════════════════════════════════════════════════════════════════════════════
// Modals
// ═══════════════════════════════════════════════════════════════════════════════

function PortfolioModal({ data, profiles, onClose, onSave }) {
  const isCreate = !data
  const [form, setForm] = useState({
    name: data?.NAME || data?.name || '',
    base_currency: data?.BASE_CURRENCY || data?.base_currency || 'USD',
    starting_cash: data?.STARTING_CASH || data?.starting_cash || '',
    profile_id: data?.PROFILE_ID || data?.profile_id || (profiles[0]?.PROFILE_ID || ''),
    notes: data?.NOTES || data?.notes || '',
  })

  const handleSave = () => {
    const body = isCreate
      ? { ...form, starting_cash: Number(form.starting_cash), profile_id: Number(form.profile_id) }
      : { name: form.name, base_currency: form.base_currency, notes: form.notes }
    onSave(body)
  }

  return (
    <div className="mgmt-modal-backdrop" onClick={onClose}>
      <div className="mgmt-modal" onClick={e => e.stopPropagation()}>
        <h3>{isCreate ? 'Create New Portfolio' : `Edit Portfolio: ${data?.NAME || data?.name}`}</h3>
        <div className="mgmt-form">
          <div className="mgmt-field">
            <label>Name</label>
            <input value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} />
          </div>
          <div className="mgmt-field">
            <label>Currency</label>
            <select value={form.base_currency} onChange={e => setForm({ ...form, base_currency: e.target.value })}>
              <option value="USD">USD</option>
              <option value="EUR">EUR</option>
              <option value="GBP">GBP</option>
            </select>
          </div>
          {isCreate && (
            <>
              <div className="mgmt-field">
                <label>Starting Cash</label>
                <input type="number" min="1" step="100" value={form.starting_cash}
                  onChange={e => setForm({ ...form, starting_cash: e.target.value })} />
              </div>
              <div className="mgmt-field">
                <label>Risk Profile</label>
                <select value={form.profile_id} onChange={e => setForm({ ...form, profile_id: e.target.value })}>
                  {profiles.map(p => (
                    <option key={p.PROFILE_ID} value={p.PROFILE_ID}>{p.NAME}</option>
                  ))}
                </select>
              </div>
            </>
          )}
          <div className="mgmt-field mgmt-form-full">
            <label>Notes</label>
            <textarea value={form.notes} onChange={e => setForm({ ...form, notes: e.target.value })} rows={2} />
          </div>
        </div>
        <div className="mgmt-btn-group">
          <button className="mgmt-btn mgmt-btn-primary" onClick={handleSave}>
            {isCreate ? 'Create Portfolio' : 'Save Changes'}
          </button>
          <button className="mgmt-btn mgmt-btn-secondary" onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}


function CashEventModal({ portfolio, onClose, onSave }) {
  const [eventType, setEventType] = useState('DEPOSIT')
  const [amount, setAmount] = useState('')
  const [notes, setNotes] = useState('')

  const name = portfolio?.NAME || portfolio?.name

  return (
    <div className="mgmt-modal-backdrop" onClick={onClose}>
      <div className="mgmt-modal" onClick={e => e.stopPropagation()}>
        <h3>Cash Event: {name}</h3>
        <p style={{ color: '#666', fontSize: '0.9rem', margin: '0 0 1rem 0' }}>
          Register a deposit or withdrawal. Your lifetime P&L tracking will remain intact —
          the system adjusts the cost basis so gains/losses are always calculated correctly.
        </p>
        <div className="mgmt-form">
          <div className="mgmt-field">
            <label>Type</label>
            <select value={eventType} onChange={e => setEventType(e.target.value)}>
              <option value="DEPOSIT">Deposit (add cash)</option>
              <option value="WITHDRAW">Withdraw (remove cash)</option>
            </select>
          </div>
          <div className="mgmt-field">
            <label>Amount</label>
            <input type="number" min="0.01" step="100" value={amount}
              onChange={e => setAmount(e.target.value)} placeholder="1000.00" />
          </div>
          <div className="mgmt-field mgmt-form-full">
            <label>Notes (optional)</label>
            <textarea value={notes} onChange={e => setNotes(e.target.value)} rows={2}
              placeholder="e.g. Monthly savings contribution" />
          </div>
        </div>
        <div className="mgmt-btn-group">
          <button
            className={`mgmt-btn ${eventType === 'DEPOSIT' ? 'mgmt-btn-success' : 'mgmt-btn-danger'}`}
            onClick={() => onSave({ event_type: eventType, amount: Number(amount), notes: notes || undefined })}
            disabled={!amount || Number(amount) <= 0}
          >
            {eventType === 'DEPOSIT' ? `Deposit ${fmt$(amount)}` : `Withdraw ${fmt$(amount)}`}
          </button>
          <button className="mgmt-btn mgmt-btn-secondary" onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}


function AttachProfileModal({ portfolio, profiles, onClose, onSave }) {
  const currentProfileId = portfolio?.PROFILE_ID || portfolio?.profile_id
  const [selected, setSelected] = useState(currentProfileId || (profiles[0]?.PROFILE_ID || ''))
  const name = portfolio?.NAME || portfolio?.name

  return (
    <div className="mgmt-modal-backdrop" onClick={onClose}>
      <div className="mgmt-modal" onClick={e => e.stopPropagation()}>
        <h3>Attach Profile: {name}</h3>
        <p style={{ color: '#666', fontSize: '0.9rem', margin: '0 0 1rem 0' }}>
          Changing the profile will end the current episode and start a new one with the selected profile.
          Episode results will be preserved.
        </p>
        <div className="mgmt-field" style={{ maxWidth: 300 }}>
          <label>Risk Profile</label>
          <select value={selected} onChange={e => setSelected(Number(e.target.value))}>
            {profiles.map(p => (
              <option key={p.PROFILE_ID} value={p.PROFILE_ID}>
                {p.NAME} {p.PROFILE_ID === currentProfileId ? '(current)' : ''}
              </option>
            ))}
          </select>
        </div>
        <div className="mgmt-btn-group">
          <button
            className="mgmt-btn mgmt-btn-primary"
            onClick={() => onSave(selected)}
            disabled={selected === currentProfileId}
          >
            {selected === currentProfileId ? 'Already attached' : 'Attach Profile'}
          </button>
          <button className="mgmt-btn mgmt-btn-secondary" onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}


function ProfileModal({ data, onClose, onSave }) {
  const isCreate = !data
  const [form, setForm] = useState({
    name: data?.NAME || '',
    max_positions: data?.MAX_POSITIONS ?? '',
    max_position_pct: data?.MAX_POSITION_PCT != null ? (data.MAX_POSITION_PCT * 100) : '',
    bust_equity_pct: data?.BUST_EQUITY_PCT != null ? (data.BUST_EQUITY_PCT * 100) : '',
    bust_action: data?.BUST_ACTION || 'ALLOW_EXITS_ONLY',
    drawdown_stop_pct: data?.DRAWDOWN_STOP_PCT != null ? (data.DRAWDOWN_STOP_PCT * 100) : '',
    crystallize_enabled: data?.CRYSTALLIZE_ENABLED || false,
    profit_target_pct: data?.PROFIT_TARGET_PCT != null ? (data.PROFIT_TARGET_PCT * 100) : '',
    crystallize_mode: data?.CRYSTALLIZE_MODE || 'WITHDRAW_PROFITS',
    cooldown_days: data?.COOLDOWN_DAYS ?? '',
    max_episode_days: data?.MAX_EPISODE_DAYS ?? '',
    take_profit_on: data?.TAKE_PROFIT_ON || 'EOD',
    description: data?.DESCRIPTION || '',
  })

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  const handleSave = () => {
    const body = {
      name: form.name || undefined,
      max_positions: form.max_positions !== '' ? Number(form.max_positions) : undefined,
      max_position_pct: form.max_position_pct !== '' ? Number(form.max_position_pct) / 100 : undefined,
      bust_equity_pct: form.bust_equity_pct !== '' ? Number(form.bust_equity_pct) / 100 : undefined,
      bust_action: form.bust_action || undefined,
      drawdown_stop_pct: form.drawdown_stop_pct !== '' ? Number(form.drawdown_stop_pct) / 100 : undefined,
      crystallize_enabled: form.crystallize_enabled,
      profit_target_pct: form.profit_target_pct !== '' ? Number(form.profit_target_pct) / 100 : undefined,
      crystallize_mode: form.crystallize_mode || undefined,
      cooldown_days: form.cooldown_days !== '' ? Number(form.cooldown_days) : undefined,
      max_episode_days: form.max_episode_days !== '' ? Number(form.max_episode_days) : undefined,
      take_profit_on: form.take_profit_on || undefined,
      description: form.description || undefined,
    }
    onSave(body)
  }

  return (
    <div className="mgmt-modal-backdrop" onClick={onClose}>
      <div className="mgmt-modal" onClick={e => e.stopPropagation()}>
        <h3>{isCreate ? 'Create New Profile' : `Edit Profile: ${data?.NAME}`}</h3>
        <div className="mgmt-form">
          <div className="mgmt-field">
            <label>Name</label>
            <input value={form.name} onChange={e => set('name', e.target.value)}
              placeholder="e.g. MODERATE_RISK" />
          </div>
          <div className="mgmt-field">
            <label>Max Positions</label>
            <input type="number" min="1" value={form.max_positions}
              onChange={e => set('max_positions', e.target.value)} />
          </div>
          <div className="mgmt-field">
            <label>Max Position %</label>
            <input type="number" min="0" max="100" step="0.5" value={form.max_position_pct}
              onChange={e => set('max_position_pct', e.target.value)} placeholder="e.g. 8 for 8%" />
          </div>
          <div className="mgmt-field">
            <label>Bust Equity %</label>
            <input type="number" min="0" max="100" step="1" value={form.bust_equity_pct}
              onChange={e => set('bust_equity_pct', e.target.value)} placeholder="e.g. 50 for 50%" />
          </div>
          <div className="mgmt-field">
            <label>Bust Action</label>
            <select value={form.bust_action} onChange={e => set('bust_action', e.target.value)}>
              <option value="ALLOW_EXITS_ONLY">Allow Exits Only</option>
              <option value="LIQUIDATE_NEXT_BAR">Liquidate Next Bar</option>
              <option value="LIQUIDATE_IMMEDIATE">Liquidate Immediate</option>
            </select>
          </div>
          <div className="mgmt-field">
            <label>Drawdown Stop %</label>
            <input type="number" min="0" max="100" step="1" value={form.drawdown_stop_pct}
              onChange={e => set('drawdown_stop_pct', e.target.value)} placeholder="e.g. 15 for 15%" />
          </div>
          <div className="mgmt-field mgmt-form-full">
            <label>Description</label>
            <textarea value={form.description} onChange={e => set('description', e.target.value)} rows={2} />
          </div>
        </div>

        {/* Crystallization section */}
        <details className="mgmt-crystallize-section" open={form.crystallize_enabled}>
          <summary>Crystallization Settings</summary>
          <div className="mgmt-crystallize-fields">
            <div className="mgmt-field">
              <label>Enabled</label>
              <div className="mgmt-toggle">
                <input type="checkbox" checked={form.crystallize_enabled}
                  onChange={e => set('crystallize_enabled', e.target.checked)} />
                <span style={{ fontSize: '0.85rem' }}>{form.crystallize_enabled ? 'On' : 'Off'}</span>
              </div>
            </div>
            <div className="mgmt-field">
              <label>Profit Target %</label>
              <input type="number" min="0" step="0.5" value={form.profit_target_pct}
                onChange={e => set('profit_target_pct', e.target.value)} placeholder="e.g. 10 for 10%" />
            </div>
            <div className="mgmt-field">
              <label>Mode</label>
              <select value={form.crystallize_mode} onChange={e => set('crystallize_mode', e.target.value)}>
                <option value="WITHDRAW_PROFITS">Withdraw Profits</option>
                <option value="REBASE">Rebase (compound)</option>
              </select>
            </div>
            <div className="mgmt-field">
              <label>Cooldown Days</label>
              <input type="number" min="0" value={form.cooldown_days}
                onChange={e => set('cooldown_days', e.target.value)} />
            </div>
            <div className="mgmt-field">
              <label>Max Episode Days</label>
              <input type="number" min="0" value={form.max_episode_days}
                onChange={e => set('max_episode_days', e.target.value)} />
            </div>
            <div className="mgmt-field">
              <label>Take Profit On</label>
              <select value={form.take_profit_on} onChange={e => set('take_profit_on', e.target.value)}>
                <option value="EOD">End of Day</option>
                <option value="INTRADAY">Intraday</option>
              </select>
            </div>
          </div>
        </details>

        <div className="mgmt-btn-group">
          <button className="mgmt-btn mgmt-btn-primary" onClick={handleSave}>
            {isCreate ? 'Create Profile' : 'Save Changes'}
          </button>
          <button className="mgmt-btn mgmt-btn-secondary" onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}
