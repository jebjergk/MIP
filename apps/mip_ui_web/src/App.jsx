import { Routes, Route, Link } from 'react-router-dom'
import Home from './pages/Home'
import Portfolio from './pages/Portfolio'
import AuditViewer from './pages/AuditViewer'
import MorningBrief from './pages/MorningBrief'
import TrainingStatus from './pages/TrainingStatus'
import Suggestions from './pages/Suggestions'
import ExplainModeToggle from './components/ExplainModeToggle'
import StatusBanner from './components/StatusBanner'

const API_BASE = '/api'

export { API_BASE }

export default function App() {
  return (
    <>
      <nav>
        <StatusBanner />
        <Link to="/">Home</Link>
        <Link to="/portfolios">Portfolios</Link>
        <Link to="/audit">Audit Viewer</Link>
        <Link to="/brief">Morning Brief</Link>
        <Link to="/training">Training Status</Link>
        <ExplainModeToggle />
      </nav>
      <main className="page">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/portfolios" element={<Portfolio />} />
          <Route path="/portfolios/:portfolioId" element={<Portfolio />} />
          <Route path="/audit" element={<AuditViewer />} />
          <Route path="/audit/:runId" element={<AuditViewer />} />
          <Route path="/brief" element={<MorningBrief />} />
          <Route path="/training" element={<TrainingStatus />} />
          <Route path="/suggestions" element={<Suggestions />} />
        </Routes>
      </main>
    </>
  )
}
