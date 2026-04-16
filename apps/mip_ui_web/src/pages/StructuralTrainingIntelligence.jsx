import React, { useState, useEffect, useMemo, useCallback } from 'react'
import { useLocation, useSearchParams } from 'react-router-dom'
import { API_BASE } from '../config/apiBase'
import { useAskMipRuntime } from '../context/AskMipRuntimeContext'
import InfoTooltip from '../components/InfoTooltip'
import LoadingState from '../components/LoadingState'
import ErrorState from '../components/ErrorState'
import EmptyState from '../components/EmptyState'
import StiSummaryStrip from '../components/structural-training/StiSummaryStrip'
import StiFilterBar from '../components/structural-training/StiFilterBar'
import StiLeaderboard from '../components/structural-training/StiLeaderboard'
import './StructuralTrainingIntelligence.css'

const SCOPE = 'structural_training'

export default function StructuralTrainingIntelligence() {
  const { pathname } = useLocation()
  const { mergeAskMipRuntime } = useAskMipRuntime()
  const [searchParams, setSearchParams] = useSearchParams()

  const [summary, setSummary] = useState(null)
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [expandedKey, setExpandedKey] = useState(null)

  // Filters (synced with URL)
  const [marketType, setMarketType] = useState(searchParams.get('market_type') || '')
  const [direction, setDirection] = useState(searchParams.get('direction') || '')
  const [trustLabel, setTrustLabel] = useState(searchParams.get('trust_label') || '')
  const [setupFamily, setSetupFamily] = useState(searchParams.get('setup_family') || '')
  const [proposalOnly, setProposalOnly] = useState(searchParams.get('proposal_only') === 'true')
  const [showRejected, setShowRejected] = useState(searchParams.get('show_rejected') === 'true')

  // Sync filters → URL
  useEffect(() => {
    const p = {}
    if (marketType) p.market_type = marketType
    if (direction) p.direction = direction
    if (trustLabel) p.trust_label = trustLabel
    if (setupFamily) p.setup_family = setupFamily
    if (proposalOnly) p.proposal_only = 'true'
    if (showRejected) p.show_rejected = 'true'
    setSearchParams(p, { replace: true })
  }, [marketType, direction, trustLabel, setupFamily, proposalOnly, showRejected, setSearchParams])

  // Load summary + leaderboard
  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    Promise.all([
      fetch(`${API_BASE}/structural-training/summary`).then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText))),
      fetch(`${API_BASE}/structural-training/leaderboard`).then(r => r.ok ? r.json() : Promise.reject(new Error(r.statusText))),
    ])
      .then(([s, lb]) => {
        if (!cancelled) {
          setSummary(s)
          setRows(lb.rows ?? [])
        }
      })
      .catch(e => { if (!cancelled) setError(e.message) })
      .finally(() => { if (!cancelled) setLoading(false) })

    return () => { cancelled = true }
  }, [])

  const get = useCallback((r, k) => r[k] ?? r[k.toUpperCase()] ?? r[k.toLowerCase()], [])

  // Client-side filter
  const filteredRows = useMemo(() => {
    return rows.filter(r => {
      if (marketType && get(r, 'MARKET_TYPE') !== marketType) return false
      if (direction && get(r, 'DIRECTION') !== direction) return false
      if (trustLabel && get(r, 'TRUST_LABEL') !== trustLabel) return false
      if (setupFamily && get(r, 'SETUP_FAMILY') !== setupFamily) return false
      if (proposalOnly && !get(r, 'IS_PROPOSAL_READY')) return false
      if (!showRejected && get(r, 'TRUST_LABEL') === 'REJECTED') return false
      return true
    })
  }, [rows, marketType, direction, trustLabel, setupFamily, proposalOnly, showRejected, get])

  // Distinct values for filter dropdowns
  const filterOptions = useMemo(() => ({
    marketTypes: [...new Set(rows.map(r => get(r, 'MARKET_TYPE')).filter(Boolean))].sort(),
    directions: [...new Set(rows.map(r => get(r, 'DIRECTION')).filter(Boolean))].sort(),
    trustLabels: [...new Set(rows.map(r => get(r, 'TRUST_LABEL')).filter(Boolean))].sort(),
    setupFamilies: [...new Set(rows.map(r => get(r, 'SETUP_FAMILY')).filter(Boolean))].sort(),
  }), [rows, get])

  // Collapse expanded row if filtered away
  useEffect(() => {
    if (expandedKey && !filteredRows.some(r => rowKey(r, get) === expandedKey)) {
      setExpandedKey(null)
    }
  }, [filteredRows, expandedKey, get])

  // AskMip runtime context
  useEffect(() => {
    mergeAskMipRuntime({
      page_id: 'structural_training',
      page_route: pathname,
      session_mode: 'research',
      active_filters: { marketType, direction, trustLabel, setupFamily, proposalOnly },
      visible_widget_ids: ['sti_leaderboard'],
      selected_widget_id: expandedKey ? 'sti_leaderboard' : null,
      current_kpi_snapshot: {
        filtered_rows: filteredRows.length,
        total_rows: rows.length,
      },
    })
    return () => {
      mergeAskMipRuntime({ page_id: null, active_filters: {}, visible_widget_ids: [], selected_widget_id: null, current_kpi_snapshot: null })
    }
  }, [pathname, filteredRows.length, rows.length, marketType, direction, trustLabel, setupFamily, proposalOnly, expandedKey, mergeAskMipRuntime])

  const toggleRow = useCallback(key => {
    setExpandedKey(prev => prev === key ? null : key)
  }, [])

  // Esc to collapse
  useEffect(() => {
    const h = e => { if (e.key === 'Escape' && expandedKey) setExpandedKey(null) }
    document.addEventListener('keydown', h)
    return () => document.removeEventListener('keydown', h)
  }, [expandedKey])

  if (loading) return <><h1>Structural Training Intelligence</h1><LoadingState /></>
  if (error) return <><h1>Structural Training Intelligence</h1><ErrorState message={error} /></>

  return (
    <>
      <h1>Structural Training Intelligence</h1>
      <p className="sti-intro">
        Understand how the structural strategy engine is learning by setup family, path behavior, state, regime, and proposal readiness.
      </p>

      {summary && <StiSummaryStrip data={summary} />}

      <StiFilterBar
        marketType={marketType} setMarketType={setMarketType}
        direction={direction} setDirection={setDirection}
        trustLabel={trustLabel} setTrustLabel={setTrustLabel}
        setupFamily={setupFamily} setSetupFamily={setSetupFamily}
        proposalOnly={proposalOnly} setProposalOnly={setProposalOnly}
        showRejected={showRejected} setShowRejected={setShowRejected}
        options={filterOptions}
      />

      <StiLeaderboard
        rows={filteredRows}
        expandedKey={expandedKey}
        onToggle={toggleRow}
        get={get}
      />

      {filteredRows.length === 0 && (
        <EmptyState
          title={rows.length === 0 ? 'No structural setup families found' : 'No rows match the current filters'}
          action={rows.length === 0 ? 'Run the structural pipeline to populate data.' : 'Adjust filters above.'}
        />
      )}
    </>
  )
}

function rowKey(r, get) {
  return `${get(r, 'SETUP_FAMILY')}|${get(r, 'MARKET_TYPE')}|${get(r, 'DIRECTION')}`
}
