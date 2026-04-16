import React, { useState, useEffect } from 'react'
import { API_BASE } from '../../config/apiBase'
import LoadingState from '../LoadingState'
import StiIdentityCard from './StiIdentityCard'
import StiMaturityModule from './StiMaturityModule'
import StiPathQualityModule from './StiPathQualityModule'
import StiWindowModule from './StiWindowModule'
import StiFailureModeModule from './StiFailureModeModule'
import StiStateRegimeModule from './StiStateRegimeModule'
import StiTrendModule from './StiTrendModule'
import StiExamplesModule from './StiExamplesModule'
import StiProposalReadiness from './StiProposalReadiness'
import StiSymbolBreakdown from './StiSymbolBreakdown'

const TABS = [
  { id: 'identity', label: 'Overview' },
  { id: 'path', label: 'Path Quality' },
  { id: 'windows', label: 'Windows' },
  { id: 'failures', label: 'Failure Modes' },
  { id: 'regime', label: 'State & Regime' },
  { id: 'trend', label: 'Trend' },
  { id: 'symbols', label: 'Symbols' },
  { id: 'examples', label: 'Examples' },
  { id: 'readiness', label: 'Proposal Readiness' },
]

export default function StiDetailPanel({ row, get, onClose }) {
  const [activeTab, setActiveTab] = useState('identity')
  const [detail, setDetail] = useState(null)
  const [trend, setTrend] = useState(null)
  const [examples, setExamples] = useState(null)
  const [loading, setLoading] = useState(true)

  const family = get(row, 'SETUP_FAMILY')
  const market = get(row, 'MARKET_TYPE')
  const dir = get(row, 'DIRECTION')

  // Load detail + failure dist on mount
  useEffect(() => {
    setLoading(true)
    setDetail(null)
    setTrend(null)
    setExamples(null)
    setActiveTab('identity')

    fetch(`${API_BASE}/structural-training/detail?setup_family=${encodeURIComponent(family)}&market_type=${encodeURIComponent(market)}`)
      .then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
      .then(d => setDetail(d))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [family, market])

  // Lazy-load trend when tab selected
  useEffect(() => {
    if (activeTab === 'trend' && !trend) {
      fetch(`${API_BASE}/structural-training/trend?setup_family=${encodeURIComponent(family)}&market_type=${encodeURIComponent(market)}&direction=${encodeURIComponent(dir)}`)
        .then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
        .then(d => setTrend(d))
        .catch(() => {})
    }
  }, [activeTab, trend, family, market, dir])

  // Lazy-load examples when tab selected
  useEffect(() => {
    if (activeTab === 'examples' && !examples) {
      fetch(`${API_BASE}/structural-training/examples?setup_family=${encodeURIComponent(family)}&market_type=${encodeURIComponent(market)}&direction=${encodeURIComponent(dir)}&limit=15`)
        .then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText)))
        .then(d => setExamples(d))
        .catch(() => {})
    }
  }, [activeTab, examples, family, market, dir])

  if (loading) return <div className="sti-detail-panel"><LoadingState /></div>
  if (!detail) return null

  const bestWindow = detail.windows?.find(w => (w.BEST_WINDOW ?? w.best_window) === (w.EVAL_WINDOW ?? w.eval_window))
  const failureDist = (detail.failure_distribution ?? []).filter(f => {
    const fd = f.DIRECTION ?? f.direction
    return !fd || fd === dir
  })

  return (
    <div className="sti-detail-panel">
      <div className="sti-detail-tabs">
        {TABS.map(t => (
          <button
            key={t.id}
            className={`sti-detail-tab${activeTab === t.id ? ' sti-detail-tab--active' : ''}`}
            onClick={() => setActiveTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {activeTab === 'identity' && (
        <>
          <StiIdentityCard row={row} get={get} />
          <StiMaturityModule row={row} get={get} />
        </>
      )}
      {activeTab === 'path' && <StiPathQualityModule row={row} get={get} detail={bestWindow} />}
      {activeTab === 'windows' && <StiWindowModule windows={detail.windows ?? []} />}
      {activeTab === 'failures' && <StiFailureModeModule data={failureDist} bestWindow={get(row, 'BEST_WINDOW')} />}
      {activeTab === 'regime' && <StiStateRegimeModule row={row} get={get} />}
      {activeTab === 'trend' && <StiTrendModule data={trend?.series} family={family} />}
      {activeTab === 'symbols' && <StiSymbolBreakdown family={family} marketType={market} direction={dir} />}
      {activeTab === 'examples' && <StiExamplesModule data={examples?.rows} />}
      {activeTab === 'readiness' && <StiProposalReadiness row={row} get={get} />}
    </div>
  )
}
