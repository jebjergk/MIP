import { Routes, Route, Navigate } from 'react-router-dom'
import Today from './pages/Today'
import Home from './pages/Home'
import Portfolio from './pages/Portfolio'
import AuditViewer from './pages/AuditViewer'
import MorningBrief from './pages/MorningBrief'
import TrainingStatus from './pages/TrainingStatus'
import Suggestions from './pages/Suggestions'
import Signals from './pages/Signals'
import Debug from './pages/Debug'
import AppLayout from './components/AppLayout'

const API_BASE = '/api'

export { API_BASE }

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route path="/" element={<Navigate to="/today" replace />} />
        <Route path="/today" element={<Today />} />
        <Route path="/home" element={<Home />} />
        <Route path="/portfolios" element={<Portfolio />} />
        <Route path="/portfolios/:portfolioId" element={<Portfolio />} />
        <Route path="/runs" element={<AuditViewer />} />
        <Route path="/runs/:runId" element={<AuditViewer />} />
        <Route path="/brief" element={<MorningBrief />} />
        <Route path="/training" element={<TrainingStatus />} />
        <Route path="/suggestions" element={<Suggestions />} />
        <Route path="/signals" element={<Signals />} />
        <Route path="/debug" element={<Debug />} />
      </Route>
    </Routes>
  )
}
