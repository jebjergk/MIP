import { Routes, Route, Navigate } from 'react-router-dom'
import Cockpit from './pages/Cockpit'
import Home from './pages/Home'
import Portfolio from './pages/Portfolio'
import AuditViewer from './pages/AuditViewer'
import TrainingStatus from './pages/TrainingStatus'
import Debug from './pages/Debug'
import MarketTimeline from './pages/MarketTimeline'
import PortfolioManagement from './pages/PortfolioManagement'
import UserGuide from './pages/UserGuide'
import ParallelWorlds from './pages/ParallelWorlds'
import AiAgentDecisions from './pages/AiAgentDecisions'
import DecisionConsole from './pages/DecisionConsole'
import NewsIntelligence from './pages/NewsIntelligence'
import LivePortfolioActivity from './pages/LivePortfolioActivity'
import LivePortfolioConfig from './pages/LivePortfolioConfig'
import LearningLedger from './pages/LearningLedger'
import PerformanceDashboard from './pages/PerformanceDashboard'
import AppLayout from './components/AppLayout'
import IntradayDashboardPage from './pages/intraday/IntradayDashboardPage'
import IntradayPatternDetailPage from './pages/intraday/IntradayPatternDetailPage'
import IntradayTerrainExplorerPage from './pages/intraday/IntradayTerrainExplorerPage'
import IntradayPipelineHealthPage from './pages/intraday/IntradayPipelineHealthPage'

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
        <Route path="/market-timeline" element={<MarketTimeline />} />
        <Route path="/manage" element={<PortfolioManagement />} />
        <Route path="/parallel-worlds" element={<ParallelWorlds />} />
        <Route path="/learning-ledger" element={<LearningLedger />} />
        <Route path="/performance-dashboard" element={<PerformanceDashboard />} />
        <Route path="/decision-console" element={<AiAgentDecisions />} />
        <Route path="/intraday/early-exit" element={<DecisionConsole />} />
        <Route path="/live-portfolio-activity" element={<LivePortfolioActivity />} />
        <Route path="/live-portfolio-config" element={<LivePortfolioConfig />} />
        <Route path="/news-intelligence" element={<NewsIntelligence />} />
        <Route path="/intraday/dashboard" element={<IntradayDashboardPage />} />
        <Route path="/intraday/pattern/:patternId" element={<IntradayPatternDetailPage />} />
        <Route path="/intraday/terrain" element={<IntradayTerrainExplorerPage />} />
        <Route path="/intraday/health" element={<IntradayPipelineHealthPage />} />
        <Route path="/debug" element={<Debug />} />
        <Route path="/guide" element={<UserGuide />} />
        {/* Redirects for old routes */}
        <Route path="/suggestions" element={<Navigate to="/cockpit" replace />} />
        <Route path="/signals" element={<Navigate to="/decision-console" replace />} />
        <Route path="/today" element={<Navigate to="/cockpit" replace />} />
        <Route path="/brief" element={<Navigate to="/cockpit" replace />} />
        <Route path="/digest" element={<Navigate to="/cockpit" replace />} />
        <Route path="/portfolios/:portfolioId/digest" element={<Navigate to="/cockpit" replace />} />
      </Route>
    </Routes>
  )
}
