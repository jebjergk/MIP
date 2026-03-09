import { useEffect, useRef, useState } from 'react'
import { NavLink, Outlet, useLocation } from 'react-router-dom'
import StatusBanner from './StatusBanner'
import LiveHeader from './LiveHeader'
import AskMipFab from './AskMipFab'
import AskMipPanel from './AskMipPanel'
import './AppLayout.css'

const SIDEBAR_WIDTH = 240
const NAV_GROUPS = [
  {
    label: 'Dashboard',
    items: [
      { to: '/cockpit', icon: '\uD83D\uDCCA', label: 'Cockpit' },
      { to: '/home',    icon: '\uD83C\uDFE0', label: 'Home' },
      { to: '/performance-dashboard', icon: '\uD83D\uDCC9', label: 'Performance' },
    ],
  },
  {
    label: 'Portfolio',
    items: [
      { to: '/manage',     icon: '\uD83D\uDCCB', label: 'Management' },
      { to: '/portfolios', icon: '\uD83D\uDCC8', label: 'Activity' },
      { to: '/live-portfolio-config', icon: '\u2699\uFE0F', label: 'Live Portfolio Link' },
    ],
  },
  {
    label: 'Research',
    items: [
      { to: '/training',        icon: '\uD83C\uDFAF', label: 'Training Status' },
      { to: '/intraday/dashboard', icon: '\u23F1\uFE0F', label: 'Intraday Training' },
      { to: '/learning-ledger', icon: '\uD83E\uDDEE', label: 'Learning Ledger' },
      { to: '/market-timeline', icon: '\uD83D\uDCC5', label: 'Market Timeline' },
      { to: '/parallel-worlds', icon: '\uD83C\uDF10', label: 'Parallel Worlds' },
    ],
  },
  {
    label: 'Operations',
    items: [
      { to: '/live-portfolio-activity', icon: '\uD83D\uDCCA', label: 'Live Portfolio Activity' },
      { to: '/live-trades', icon: '\uD83E\uDDFE', label: 'Live Trades' },
      { to: '/decision-console', icon: '\u26A1', label: 'AI Agent Decisions' },
      { to: '/news-intelligence', icon: '\uD83D\uDCF0', label: 'News Intelligence' },
      { to: '/runs',  icon: '\u25B6\uFE0F', label: 'Runs (Audit)' },
      { to: '/debug', icon: '\uD83D\uDD27', label: 'Debug' },
    ],
  },
]

export default function AppLayout() {
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [askMipOpen, setAskMipOpen] = useState(false)
  const [hasNewsAlert, setHasNewsAlert] = useState(false)
  const [newsTicker, setNewsTicker] = useState('')
  const latestNewsTsRef = useRef(null)
  const { pathname } = useLocation()

  const closeSidebar = () => setSidebarOpen(false)

  useEffect(() => {
    let cancelled = false
    const SEEN_KEY = 'mip.newsIntelligence.lastSeenGeneratedAt'

    const checkNews = async () => {
      try {
        const resp = await fetch('/api/news/intelligence')
        if (!resp.ok) return
        const data = await resp.json()
        const generatedAt = data?.generated_at || null
        const hotSymbols = Number(data?.market_context?.hot_symbols || 0)
        const topHeadlines = Array.isArray(data?.market_context?.top_headlines)
          ? data.market_context.top_headlines
          : []
        const topHotHeadline = topHeadlines.find((h) => String(h?.badge || '').toUpperCase() === 'HOT') || topHeadlines[0]
        const tickerText = topHotHeadline?.title
          ? `${topHotHeadline?.symbol || 'NEWS'}: ${String(topHotHeadline.title).slice(0, 54)}${String(topHotHeadline.title).length > 54 ? '…' : ''}`
          : ''
        latestNewsTsRef.current = generatedAt
        const isDecisionRelevant = hotSymbols > 0 && topHeadlines.length > 0
        const seenTs = localStorage.getItem(SEEN_KEY)
        const hasUnseen = Boolean(generatedAt) && (!seenTs || generatedAt > seenTs)
        if (!cancelled) {
          setHasNewsAlert(isDecisionRelevant && hasUnseen)
          setNewsTicker(tickerText)
        }
      } catch {
        if (!cancelled) {
          setHasNewsAlert(false)
          setNewsTicker('')
        }
      }
    }

    checkNews()
    const t = setInterval(checkNews, 5 * 60 * 1000)
    return () => {
      cancelled = true
      clearInterval(t)
    }
  }, [])

  useEffect(() => {
    const SEEN_KEY = 'mip.newsIntelligence.lastSeenGeneratedAt'
    if (!pathname.startsWith('/news-intelligence')) return
    const latest = latestNewsTsRef.current
    if (latest) localStorage.setItem(SEEN_KEY, latest)
    setHasNewsAlert(false)
    setNewsTicker('')
  }, [pathname])

  return (
    <div className="app-layout">
      <button
        type="button"
        className="app-layout-sidebar-toggle"
        onClick={() => setSidebarOpen((o) => !o)}
        aria-label={sidebarOpen ? 'Close menu' : 'Open menu'}
        aria-expanded={sidebarOpen}
      >
        <span className="app-layout-hamburger" aria-hidden="true">
          <span />
          <span />
          <span />
        </span>
      </button>

      <aside
        className={`app-layout-sidebar ${sidebarOpen ? 'app-layout-sidebar--open' : ''}`}
        style={{ width: SIDEBAR_WIDTH }}
        aria-label="Main navigation"
      >
        <div className="app-layout-sidebar-inner">
          <div className="app-layout-brand">
            <h1 className="app-layout-title">
              <NavLink to="/" onClick={closeSidebar} className="app-layout-title-link">
                <span className="app-layout-title-accent" />
                MIP
              </NavLink>
            </h1>
            <span className="app-layout-subtitle">Market Intelligence Platform</span>
          </div>
          <nav className="app-layout-nav">
            {NAV_GROUPS.map((group) => (
              <div key={group.label} className="app-layout-nav-group">
                <span className="app-layout-nav-group-label">{group.label}</span>
                {group.items.map(({ to, icon, label }) => {
                  const isNews = to === '/news-intelligence'
                  return (
                    <NavLink
                      key={to}
                      to={to}
                      end={to !== '/portfolios' && to !== '/runs'}
                      className={({ isActive }) =>
                        `app-layout-nav-link ${isActive ? 'app-layout-nav-link--active' : ''}`
                      }
                      onClick={closeSidebar}
                    >
                      <span className="app-layout-nav-icon" aria-hidden="true">{icon}</span>
                      <span className="app-layout-nav-label-wrap">
                        <span className="app-layout-nav-label">
                          {label}
                          {isNews && hasNewsAlert ? <span className="app-layout-news-pill">HOT</span> : null}
                        </span>
                        {isNews && hasNewsAlert && newsTicker ? (
                          <span className="app-layout-news-ticker">{newsTicker}</span>
                        ) : null}
                      </span>
                    </NavLink>
                  )
                })}
              </div>
            ))}
            <div className="app-layout-nav-footer">
              <NavLink
                to="/guide"
                className={({ isActive }) =>
                  `app-layout-nav-link app-layout-nav-link--guide ${isActive ? 'app-layout-nav-link--active' : ''}`
                }
                onClick={closeSidebar}
              >
                User Guide
              </NavLink>
            </div>
          </nav>
        </div>
      </aside>

      {sidebarOpen && (
        <div
          className="app-layout-sidebar-backdrop"
          onClick={closeSidebar}
          onKeyDown={(e) => e.key === 'Escape' && closeSidebar()}
          role="button"
          tabIndex={0}
          aria-label="Close menu"
        />
      )}

      <div className="app-layout-main" style={{ marginLeft: SIDEBAR_WIDTH }}>
        <header className="app-layout-topbar" role="banner">
          <div className="app-layout-topbar-left">
            <StatusBanner />
            <LiveHeader />
          </div>
        </header>
        <main className="app-layout-content page" role="main">
          <Outlet />
        </main>
      </div>

      {/* Ask MIP — floating button + slide-over panel */}
      <AskMipFab open={askMipOpen} onClick={() => setAskMipOpen((v) => !v)} />
      <AskMipPanel open={askMipOpen} onClose={() => setAskMipOpen(false)} pathname={pathname} />
    </div>
  )
}
