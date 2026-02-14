import { useState } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import StatusBanner from './StatusBanner'
import LiveHeader from './LiveHeader'
import ExplainModeToggle from './ExplainModeToggle'
import ExplainDrawer from './ExplainDrawer'
import { useExplainMode } from '../context/ExplainModeContext'
import { useExplainCenter } from '../context/ExplainCenterContext'
import './AppLayout.css'

const SIDEBAR_WIDTH = 240
const NAV_GROUPS = [
  {
    label: 'Dashboard',
    items: [
      { to: '/cockpit', icon: '\uD83D\uDCCA', label: 'Cockpit' },
      { to: '/home',    icon: '\uD83C\uDFE0', label: 'Home' },
    ],
  },
  {
    label: 'Portfolio',
    items: [
      { to: '/manage',     icon: '\uD83D\uDCCB', label: 'Management' },
      { to: '/portfolios', icon: '\uD83D\uDCC8', label: 'Activity' },
    ],
  },
  {
    label: 'Research',
    items: [
      { to: '/training',        icon: '\uD83C\uDFAF', label: 'Training Status' },
      { to: '/suggestions',     icon: '\uD83D\uDCA1', label: 'Suggestions' },
      { to: '/signals',         icon: '\uD83D\uDCE1', label: 'Signals' },
      { to: '/market-timeline', icon: '\uD83D\uDCC5', label: 'Market Timeline' },
      { to: '/parallel-worlds', icon: '\uD83C\uDF10', label: 'Parallel Worlds' },
    ],
  },
  {
    label: 'Operations',
    items: [
      { to: '/runs',  icon: '\u25B6\uFE0F', label: 'Runs (Audit)' },
      { to: '/debug', icon: '\uD83D\uDD27', label: 'Debug' },
    ],
  },
]

export default function AppLayout() {
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const { explainMode } = useExplainMode()
  const { open: openExplainDrawer } = useExplainCenter()

  const closeSidebar = () => setSidebarOpen(false)

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
                {group.items.map(({ to, icon, label }) => (
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
                    {label}
                  </NavLink>
                ))}
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
          <div className="app-layout-topbar-right">
            {explainMode && (
              <button
                type="button"
                className="explain-center-btn"
                onClick={openExplainDrawer}
                aria-label="Open Explain Center"
              >
                Explain
              </button>
            )}
            <ExplainModeToggle />
          </div>
        </header>
        <main className="app-layout-content page" role="main">
          <Outlet />
        </main>
      </div>
      <ExplainDrawer />
    </div>
  )
}
