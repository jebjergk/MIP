import { Routes, Route, Navigate } from 'react-router-dom'
import Cockpit from './pages/Cockpit'
import Home from './pages/Home'
import Portfolio from './pages/Portfolio'
import AuditViewer from './pages/AuditViewer'
import TrainingStatus from './pages/TrainingStatus'
import Suggestions from './pages/Suggestions'
import Signals from './pages/Signals'
import Debug from './pages/Debug'
import MarketTimeline from './pages/MarketTimeline'
import PortfolioManagement from './pages/PortfolioManagement'
import UserGuide from './pages/UserGuide'
import AppLayout from './components/AppLayout'

const API_BASE = '/api'

export { API_BASE }

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route path="/" element={<Navigate to="/cockpit" replace />} />
        <Route path="/cockpit" element={<Cockpit />} />
        <Route path="/home" element={<Home />} />
        <Route path="/portfolios" element={<Portfolio />} />
        <Route path="/portfolios/:portfolioId" element={<Portfolio />} />
        <Route path="/runs" element={<AuditViewer />} />
        <Route path="/runs/:runId" element={<AuditViewer />} />
        <Route path="/training" element={<TrainingStatus />} />
        <Route path="/suggestions" element={<Suggestions />} />
        <Route path="/signals" element={<Signals />} />
        <Route path="/market-timeline" element={<MarketTimeline />} />
        <Route path="/manage" element={<PortfolioManagement />} />
        <Route path="/debug" element={<Debug />} />
        <Route path="/guide" element={<UserGuide />} />
        {/* Redirects for old routes */}
        <Route path="/today" element={<Navigate to="/cockpit" replace />} />
        <Route path="/brief" element={<Navigate to="/cockpit" replace />} />
        <Route path="/digest" element={<Navigate to="/cockpit" replace />} />
        <Route path="/portfolios/:portfolioId/digest" element={<Navigate to="/cockpit" replace />} />
      </Route>
    </Routes>
  )
}
