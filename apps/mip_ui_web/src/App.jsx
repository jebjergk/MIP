import { useEffect } from 'react'
import { Routes, Route, Navigate, useLocation, matchPath } from 'react-router-dom'
import Cockpit from './pages/Cockpit'
import Home from './pages/Home'
import AuditViewer from './pages/AuditViewer'
import TrainingStatus from './pages/TrainingStatus'
import Debug from './pages/Debug'
import MarketTimeline from './pages/MarketTimeline'
import SymbolTracker from './pages/SymbolTracker'
import LiveIntelligenceCockpit from './pages/LiveIntelligenceCockpit'
import UserGuide from './pages/UserGuide'
import ParallelWorlds from './pages/ParallelWorlds'
import AiAgentDecisions from './pages/AiAgentDecisions'
import DecisionConsole from './pages/DecisionConsole'
import NewsIntelligence from './pages/NewsIntelligence'
import LivePortfolioActivity from './pages/LivePortfolioActivity'
import LivePortfolioConfig from './pages/LivePortfolioConfig'
import PositionHealth from './pages/PositionHealth'
import LearningLedger from './pages/LearningLedger'
import PerformanceDashboard from './pages/PerformanceDashboard'
import GlossaryAdminPage from './pages/GlossaryAdminPage'
import StructuralTrainingIntelligence from './pages/StructuralTrainingIntelligence'
import StructuralMarketTimeline from './pages/StructuralMarketTimeline'
import StructuralCommitteeHearing from './pages/StructuralCommitteeHearing'
import CommitteePerformance from './pages/CommitteePerformance'
import PriceActionAnalyser from './pages/PriceActionAnalyser'
import BrooksIntradayLab from './pages/BrooksIntradayLab'
import AppLayout from './components/AppLayout'
import { AskMipRuntimeProvider } from './context/AskMipRuntimeContext'
import IntradayDashboardPage from './pages/intraday/IntradayDashboardPage'
import IntradayPatternDetailPage from './pages/intraday/IntradayPatternDetailPage'
import IntradayTerrainExplorerPage from './pages/intraday/IntradayTerrainExplorerPage'
import IntradayPipelineHealthPage from './pages/intraday/IntradayPipelineHealthPage'

export { API_BASE } from './config/apiBase'

function pageTitleForPath(pathname) {
  const titleByPattern = [
    { pattern: '/cockpit', title: 'Cockpit' },
    { pattern: '/home', title: 'Home' },
    { pattern: '/portfolios', title: 'Portfolios' },
    { pattern: '/portfolios/:portfolioId', title: 'Portfolio' },
    { pattern: '/runs', title: 'Runs' },
    { pattern: '/runs/:runId', title: 'Run Details' },
    { pattern: '/structural-training', title: 'Structural Training Intelligence' },
    { pattern: '/training', title: 'Legacy Training Status' },
    { pattern: '/structural-timeline', title: 'Structural Market Timeline' },
    { pattern: '/price-action-analyser', title: 'Price Action Analyser' },
    { pattern: '/research/brooks-intraday', title: 'Brooks Intraday Lab' },
    { pattern: '/structural-committee/:hearingId', title: 'Hearing Replay — Evidence Snapshot (historical)' },
    { pattern: '/structural-committee', title: 'Hearing Replay — Evidence Snapshot (historical)' },
    { pattern: '/committee-performance', title: 'Committee Bake-off' },
    { pattern: '/market-timeline', title: 'Legacy Market Timeline' },
    { pattern: '/symbol-tracker', title: 'Living Chart' },
    { pattern: '/living-chart', title: 'Living Chart' },
    { pattern: '/live-intelligence', title: 'Live Intelligence Cockpit' },
    { pattern: '/manage', title: 'Portfolio Management' },
    { pattern: '/parallel-worlds', title: 'Parallel Worlds' },
    { pattern: '/learning-ledger', title: 'Learning Ledger' },
    { pattern: '/performance-dashboard', title: 'Performance Dashboard' },
    { pattern: '/decision-console', title: 'Decision Console' },
    { pattern: '/intraday/early-exit', title: 'Intraday Early Exit' },
    { pattern: '/live-portfolio-activity', title: 'Live Portfolio Activity' },
    { pattern: '/live-portfolio-config', title: 'Live Portfolio Config' },
    { pattern: '/position-health', title: 'Position Health' },
    { pattern: '/news-intelligence', title: 'News Intelligence' },
    { pattern: '/intraday/dashboard', title: 'Intraday Dashboard' },
    { pattern: '/intraday/pattern/:patternId', title: 'Intraday Pattern' },
    { pattern: '/intraday/terrain', title: 'Intraday Terrain Explorer' },
    { pattern: '/intraday/health', title: 'Intraday Pipeline Health' },
    { pattern: '/debug', title: 'Debug' },
    { pattern: '/guide', title: 'User Guide' },
    { pattern: '/ask-glossary', title: 'Ask Glossary Admin' },
  ]

  for (const entry of titleByPattern) {
    const match = matchPath({ path: entry.pattern, end: true }, pathname)
    if (!match) continue
    if (entry.pattern === '/portfolios/:portfolioId' && match.params.portfolioId) {
      return `Portfolio ${match.params.portfolioId}`
    }
    if (entry.pattern === '/runs/:runId' && match.params.runId) {
      return `Run ${match.params.runId}`
    }
    if (entry.pattern === '/intraday/pattern/:patternId' && match.params.patternId) {
      return `Pattern ${match.params.patternId}`
    }
    if (entry.pattern === '/structural-committee/:hearingId' && match.params.hearingId) {
      return `Hearing Replay ${String(match.params.hearingId).slice(0, 8)}…`
    }
    return entry.title
  }

  return 'MIP UI'
}

export default function App() {
  const { pathname } = useLocation()

  useEffect(() => {
    const pageTitle = pageTitleForPath(pathname)
    document.title = `${pageTitle} | MIP UI`
  }, [pathname])

  return (
    <Routes>
      <Route element={<AskMipRuntimeProvider><AppLayout /></AskMipRuntimeProvider>}>
        <Route path="/" element={<Navigate to="/cockpit" replace />} />
        <Route path="/cockpit" element={<Cockpit />} />
        <Route path="/home" element={<Home />} />
        <Route path="/portfolios" element={<Navigate to="/cockpit" replace />} />
        <Route path="/portfolios/:portfolioId" element={<Navigate to="/cockpit" replace />} />
        <Route path="/runs" element={<AuditViewer />} />
        <Route path="/runs/:runId" element={<AuditViewer />} />
        <Route path="/structural-training" element={<StructuralTrainingIntelligence />} />
        <Route path="/training" element={<TrainingStatus />} />
        <Route path="/structural-timeline" element={<StructuralMarketTimeline />} />
        <Route path="/price-action-analyser" element={<PriceActionAnalyser />} />
        <Route path="/research/brooks-intraday" element={<BrooksIntradayLab />} />
        <Route path="/structural-committee" element={<StructuralCommitteeHearing />} />
        <Route path="/structural-committee/:hearingId" element={<StructuralCommitteeHearing />} />
        <Route path="/committee-performance" element={<CommitteePerformance />} />
        <Route path="/market-timeline" element={<MarketTimeline />} />
        <Route path="/symbol-tracker" element={<SymbolTracker />} />
        <Route path="/living-chart" element={<SymbolTracker />} />
        <Route path="/live-intelligence" element={<LiveIntelligenceCockpit />} />
        <Route path="/manage" element={<Navigate to="/cockpit" replace />} />
        <Route path="/parallel-worlds" element={<ParallelWorlds />} />
        <Route path="/learning-ledger" element={<LearningLedger />} />
        <Route path="/performance-dashboard" element={<PerformanceDashboard />} />
        <Route path="/decision-console" element={<AiAgentDecisions />} />
        <Route path="/intraday/early-exit" element={<DecisionConsole />} />
        <Route path="/live-portfolio-activity" element={<LivePortfolioActivity />} />
        <Route path="/live-portfolio-config" element={<LivePortfolioConfig />} />
        <Route path="/position-health" element={<PositionHealth />} />
        <Route path="/news-intelligence" element={<NewsIntelligence />} />
        <Route path="/intraday/dashboard" element={<IntradayDashboardPage />} />
        <Route path="/intraday/pattern/:patternId" element={<IntradayPatternDetailPage />} />
        <Route path="/intraday/terrain" element={<IntradayTerrainExplorerPage />} />
        <Route path="/intraday/health" element={<IntradayPipelineHealthPage />} />
        <Route path="/debug" element={<Debug />} />
        <Route path="/guide" element={<UserGuide />} />
        <Route path="/ask-glossary" element={<GlossaryAdminPage />} />
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
