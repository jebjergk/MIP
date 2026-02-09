import { useState } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import StatusBanner from './StatusBanner'
import LiveHeader from './LiveHeader'
import ExplainModeToggle from './ExplainModeToggle'
import ExplainDrawer from './ExplainDrawer'
import { useExplainMode } from '../context/ExplainModeContext'
import { useExplainCenter } from '../context/ExplainCenterContext'
import './AppLayout.css'

const SIDEBAR_WIDTH = 220
const NAV_ITEMS = [
  { to: '/cockpit', label: 'Cockpit' },
  { to: '/home', label: 'Home' },
  { to: '/manage', label: 'Portfolio Management' },
  { to: '/portfolios', label: 'Portfolio Activity' },
  { to: '/training', label: 'Training Status' },
  { to: '/suggestions', label: 'Suggestions' },
  { to: '/signals', label: 'Signals' },
  { to: '/market-timeline', label: 'Market Timeline' },
  { to: '/runs', label: 'Runs (Audit)' },
  { to: '/debug', label: 'Debug' },
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
          <h1 className="app-layout-title">
            <NavLink to="/" onClick={closeSidebar} className="app-layout-title-link">
              MIP
            </NavLink>
          </h1>
          <nav className="app-layout-nav">
            {NAV_ITEMS.map(({ to, label }) => (
              <NavLink
                key={to}
                to={to}
                end={to !== '/portfolios' && to !== '/runs'}
                className={({ isActive }) =>
                  `app-layout-nav-link ${isActive ? 'app-layout-nav-link--active' : ''}`
                }
                onClick={closeSidebar}
              >
                {label}
              </NavLink>
            ))}
            <div className="app-layout-nav-divider" />
            <NavLink
              to="/guide"
              className={({ isActive }) =>
                `app-layout-nav-link app-layout-nav-link--guide ${isActive ? 'app-layout-nav-link--active' : ''}`
              }
              onClick={closeSidebar}
            >
              User Guide
            </NavLink>
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
