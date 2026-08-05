import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { API_BASE } from '../config/apiBase'
import {
  applyReviewChainChange,
  completeLocatorNavigation,
  createLocatorNavigationIntent,
  initialLabNavigationState,
  reduceLabNavigation,
} from './brooksLabNavigation'
import './BrooksIntradayLab.css'

const API = `${API_BASE}/research/brooks-intraday`
const DEFAULT_SYMBOLS = ['AAPL', 'AMZN', 'JPM', 'MCD']
const PLAYBACK_SPEEDS = ['manual', '1bps', '2bps', '5bps', 'instant_eod']

function formatMoney(val) {
  if (val == null || Number.isNaN(Number(val))) return '—'
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
  }).format(Number(val))
}

function statusClass(status) {
  if (!status) return 'bil-status'
  const s = String(status).toUpperCase()
  if (s === 'RUNNING') return 'bil-status bil-status--running'
  if (s === 'PAUSED') return 'bil-status bil-status--paused'
  if (s === 'STOPPED' || s === 'ERROR') return 'bil-status bil-status--stopped'
  return 'bil-status'
}

function markerLabels(terms, categories) {
  const names = new Set((terms || []).map((t) => (typeof t === 'string' ? t : t.term)))
  const out = []
  const showStructure = categories?.STRUCTURE !== false
  const showBarFacts = categories?.BAR_FACTS === true
  if (showBarFacts) {
    if (names.has('DOJI')) out.push('DOJI')
    if (names.has('INSIDE_BAR')) out.push('IB')
    if (names.has('OUTSIDE_BAR')) out.push('OB')
  }
  if (showStructure) {
    if (names.has('POSSIBLE_BREAKOUT_BAR')) out.push('BO?')
    if (names.has('POSSIBLE_FOLLOW_THROUGH_BAR')) out.push('FT?')
  }
  if (categories?.BAR_FACTS && categories?.STRUCTURE) {
    if (names.has('HIGHER_HIGH')) out.push('HH')
    if (names.has('HIGHER_LOW')) out.push('HL')
    if (names.has('LOWER_HIGH')) out.push('LH')
    if (names.has('LOWER_LOW')) out.push('LL')
  }
  return out.join(' ')
}

function contextMarkerLabels(ctxRow, categories) {
  if (categories?.ACTIONS === false || !ctxRow) return ''
  const pj = ctxRow.payload_json || {}
  const flags = pj.marker_flags_json || []
  const abbr = {
    support_test: 'ST',
    support_hold: 'SH',
    reclaim_test: 'RT',
    reclaim_confirmed: 'RC',
    setup_developing: 'SD',
    entry_armed: 'EA',
    consider_entry: 'CE',
    do_not_chase: 'DNC',
    thesis_weakened: 'TW',
    thesis_invalidated: 'TI',
  }
  return flags.map((f) => abbr[f] || f.slice(0, 3)).join(' ')
}

function patternMarkerLabels(patterns, categories) {
  if (categories?.PATTERNS === false) return ''
  return (patterns || [])
    .map((p) => {
      const fam = (p.pattern_family || '').replace(/_/g, ' ')
      const lc = p.lifecycle || ''
      if (lc === 'CONFIRMED') return `[${fam.slice(0, 8)}]`
      if (lc === 'DEVELOPING') return `(${fam.slice(0, 6)})`
      if (lc === 'FAILED') return `x${fam.slice(0, 4)}`
      return `${fam.slice(0, 5)}?`
    })
    .join(' ')
}

function formatTerms(row) {
  const terms = row.brooks_obs_json || []
  return terms.map((t) => t.term).filter(Boolean).join(', ')
}

function parseBrooksLocator(locator) {
  const text = String(locator || '').trim()
  const out = {
    run_id: null,
    symbol: null,
    bar_ts: null,
    context_attempt_id: null,
    simulation_attempt_id: null,
  }
  if (!text.startsWith('brooks-lab/')) return out
  const body = text.slice('brooks-lab/'.length)
  const [pathPart, queryPart = ''] = body.split('?')
  const path = pathPart.split('#')[0]
  const slash = path.indexOf('/')
  if (slash < 0) return out
  out.run_id = path.slice(0, slash) || null
  const rest = path.slice(slash + 1)
  const at = rest.indexOf('@')
  if (at >= 0) {
    out.symbol = rest.slice(0, at) || null
    out.bar_ts = rest.slice(at + 1).replace(' ', 'T').slice(0, 19) || null
  }
  if (queryPart) {
    const params = new URLSearchParams(queryPart)
    out.context_attempt_id = params.get('context_attempt_id')
    out.simulation_attempt_id = params.get('simulation_attempt_id')
  }
  return out
}

function formatTradeEventMoney(ev) {
  const d = ev?.detail || {}
  if (ev?.kind === 'TRADE_ENTRY' && d.entry_price != null) {
    return `entry ${formatMoney(d.entry_price)}`
  }
  if (ev?.kind === 'TRADE_EXIT') {
    const parts = []
    if (d.exit_price != null) parts.push(`exit ${formatMoney(d.exit_price)}`)
    if (d.realized_pnl != null) parts.push(`P/L ${formatMoney(d.realized_pnl)}`)
    return parts.join(' · ')
  }
  return ''
}

export default function BrooksIntradayLab() {
  const [metaBanner, setMetaBanner] = useState('SIMULATION ONLY — NO REAL ORDERS OR PORTFOLIO CONNECTION')
  const [weekStart, setWeekStart] = useState('')
  const [symbolsText, setSymbolsText] = useState(DEFAULT_SYMBOLS.join(', '))
  const [playbackSpeed, setPlaybackSpeed] = useState('manual')
  const [run, setRun] = useState(null)
  const [activeSymbol, setActiveSymbol] = useState(DEFAULT_SYMBOLS[0])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [frozenDossier, setFrozenDossier] = useState(null)
  const [matrixDate, setMatrixDate] = useState('')
  const [missingTsModal, setMissingTsModal] = useState(null)
  const [replayState, setReplayState] = useState(null)
  const [visibleBars, setVisibleBars] = useState([])
  const [observations, setObservations] = useState([])
  const [reviewFilter, setReviewFilter] = useState('')
  const [selectedBarTs, setSelectedBarTs] = useState(null)
  const [selectedPattern, setSelectedPattern] = useState(null)
  const [patternDetail, setPatternDetail] = useState(null)
  const [markerCategories, setMarkerCategories] = useState({
    BAR_FACTS: false,
    STRUCTURE: true,
    PATTERNS: true,
    ACTIONS: true,
  })
  const [chartLayers, setChartLayers] = useState({
    DAILY_LEVELS: true,
    OBJECTIVE_FACTS: false,
    STRUCTURE: true,
    PATTERNS: true,
    CONTEXT: true,
    ACTIONS: true,
    SIMULATED_TRADES: true,
  })
  const [reviewMode, setReviewMode] = useState('full')
  const [learningRun, setLearningRun] = useState(null)
  const [learningSymbol, setLearningSymbol] = useState(null)
  const [certification, setCertification] = useState(null)
  const [showCertModal, setShowCertModal] = useState(false)
  const [learningFilters, setLearningFilters] = useState({
    action_changed_only: false,
    meaningful_pattern_no_entry: false,
    transition_events_only: false,
  })
  const [patternsCatalog, setPatternsCatalog] = useState([])
  const [contextRows, setContextRows] = useState([])
  const gridRowRefs = useRef({})
  const [loadRunId, setLoadRunId] = useState('4eababc8-88fe-4ebc-b47c-b65f6ec10c74')
  const [experimentOverview, setExperimentOverview] = useState(null)
  const [experimentComparison, setExperimentComparison] = useState(null)
  const [experimentAggregate, setExperimentAggregate] = useState(null)
  const [experimentTrades, setExperimentTrades] = useState([])
  const [tradeFilterWin, setTradeFilterWin] = useState('')
  const [phase9, setPhase9] = useState(null)
  const [phase9Busy, setPhase9Busy] = useState(false)
  const [loadedRunBanner, setLoadedRunBanner] = useState('')
  // Session-only review chain override (never persisted; resets to official on reload / new run load)
  const [reviewChainOverride, setReviewChainOverride] = useState(null)
  const [reviewChainOptions, setReviewChainOptions] = useState(null)
  const [reviewTrades, setReviewTrades] = useState([])
  const [selectedTradeMgmt, setSelectedTradeMgmt] = useState(null)
  const [tradeMgmtReview, setTradeMgmtReview] = useState(null)
  const [tradeMgmtLoading, setTradeMgmtLoading] = useState(false)
  const reviewSummaryRef = useRef(null)
  const provenanceRef = useRef(null)
  // One-shot locator scroll intent — never revived by data refreshes / re-renders.
  const labNavRef = useRef(initialLabNavigationState())

  const refreshPhase9 = useCallback(async () => {
    try {
      const st = await fetch(`${API}/experiments/phase9/status`).then((r) => r.json())
      setPhase9(st)
      if (st.phase9_complete && st.aggregate_report) {
        setExperimentAggregate(st.aggregate_report)
      }
      if (st.comparison) {
        setExperimentComparison(st.comparison)
      }
    } catch {
      /* panel optional until API up */
    }
  }, [])

  const phase9Post = useCallback(async (path) => {
    setPhase9Busy(true)
    try {
      const res = await fetch(`${API}/experiments/phase9/${path}`, { method: 'POST' })
      let body = null
      const text = await res.text()
      if (text) {
        try {
          body = JSON.parse(text)
        } catch {
          body = {
            ok: false,
            tws: {
              readiness: 'UNKNOWN_ERROR',
              connected: false,
              message: text.slice(0, 200) || `HTTP ${res.status}`,
              recoverable: true,
            },
          }
        }
      } else {
        body = { ok: false, tws: { readiness: 'UNKNOWN_ERROR', connected: false, message: `HTTP ${res.status}`, recoverable: true } }
      }
      if (body?.status) setPhase9(body.status)
      else await refreshPhase9()
      if (body?.ok === false && body?.message) {
        window.alert(body.message)
      }
      return body
    } finally {
      setPhase9Busy(false)
    }
  }, [refreshPhase9])

  useEffect(() => {
    refreshPhase9()
  }, [refreshPhase9])

  useEffect(() => {
    const active = phase9?.overall_status === 'RUNNING' || phase9?.overall_status === 'WAITING_FOR_TWS'
    if (!active) return undefined
    const t = setInterval(refreshPhase9, 4000)
    return () => clearInterval(t)
  }, [phase9?.overall_status, refreshPhase9])

  useEffect(() => {
    let cancelled = false
    const loadExperiments = async () => {
      try {
        const [ov, cmp, agg, tr] = await Promise.all([
          fetch(`${API}/experiments/validation-runs`).then((r) => r.json()),
          fetch(`${API}/experiments/comparison`).then((r) => r.json()),
          fetch(`${API}/experiments/aggregate-report`).then((r) => r.json()),
          fetch(`${API}/experiments/trades`).then((r) => r.json()),
        ])
        if (cancelled) return
        setExperimentOverview(ov)
        setExperimentComparison(cmp)
        setExperimentAggregate(agg)
        setExperimentTrades(tr.trades || [])
      } catch {
        /* optional panel */
      }
    }
    loadExperiments()
    return () => { cancelled = true }
  }, [])

  const refreshExperimentTrades = useCallback(async () => {
    const params = new URLSearchParams()
    if (tradeFilterWin === 'win') params.set('win_only', 'true')
    if (tradeFilterWin === 'loss') params.set('win_only', 'false')
    // When a run is loaded, scope Trade browser to that run's selected review chain.
    if (run?.run_id) {
      params.set('run_id', run.run_id)
      if (reviewChainOverride?.context_attempt_id && reviewChainOverride?.simulation_attempt_id) {
        params.set('context_attempt_id', reviewChainOverride.context_attempt_id)
        params.set('simulation_attempt_id', reviewChainOverride.simulation_attempt_id)
      }
    }
    const qs = params.toString() ? `?${params.toString()}` : ''
    const tr = await fetch(`${API}/experiments/trades${qs}`).then((r) => r.json()).catch(() => ({}))
    setExperimentTrades(tr.trades || [])
  }, [tradeFilterWin, run?.run_id, reviewChainOverride])

  useEffect(() => {
    refreshExperimentTrades()
  }, [refreshExperimentTrades])

  const resetReviewStateForRun = useCallback((payload) => {
    const symbols = payload?.symbols || []
    const matrix = payload?.preparation?.matrix || []
    const dates = [...new Set(matrix.map((r) => r.trading_date).filter(Boolean))].sort()
    setSelectedBarTs(null)
    setSelectedPattern(null)
    setPatternDetail(null)
    setMatrixDate(dates[0] || '')
    setReviewChainOverride(null)
    setReviewChainOptions(null)
    setReviewTrades([])
    setSelectedTradeMgmt(null)
    setTradeMgmtReview(null)
    labNavRef.current = reduceLabNavigation(labNavRef.current, { type: 'PAGE_RELOAD' })
    if (symbols.length && !symbols.includes(activeSymbol)) {
      setActiveSymbol(symbols[0])
    }
  }, [activeSymbol])

  const loadExistingRun = async () => {
    const id = loadRunId.trim()
    if (!id) return
    setLoading(true)
    setError('')
    try {
      const resp = await fetch(`${API}/runs/${id}`)
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(payload?.detail || `Failed to load run (${resp.status})`)
      }
      setRun(payload)
      setLoadRunId(id)
      resetReviewStateForRun(payload)
      const weekStart = payload?.selected_week_start || payload?.configuration?.selected_week_start
      const weekEnd = payload?.selected_week_end || payload?.configuration?.selected_week_end
      setLoadedRunBanner(
        weekStart
          ? `Loaded validation week ${String(weekStart).slice(0, 10)}${weekEnd ? ` to ${String(weekEnd).slice(0, 10)}` : ''} · Run ${id}`
          : `Loaded run ${id}`,
      )
      const chainsResp = await fetch(`${API}/runs/${id}/review-chains`)
      const chains = await chainsResp.json().catch(() => ({}))
      if (chainsResp.ok) setReviewChainOptions(chains)
      await refreshReplay(id, null)
      if (reviewSummaryRef.current?.scrollIntoView) {
        reviewSummaryRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' })
      }
    } catch (err) {
      setError(err?.message || 'Could not load run.')
    } finally {
      setLoading(false)
    }
  }

  const openTradeLocator = async (locator) => {
    const loc = parseBrooksLocator(locator)
    if (!loc.run_id || !loc.symbol) return
    const intent = createLocatorNavigationIntent(loc)
    if (!intent) return
    labNavRef.current = reduceLabNavigation(labNavRef.current, {
      type: 'LOCATOR_CLICKED',
      intent,
    })
    const override = labNavRef.current.reviewChainOverride
    setLoading(true)
    setError('')
    try {
      if (run?.run_id !== loc.run_id) {
        const resp = await fetch(`${API}/runs/${loc.run_id}`)
        const payload = await resp.json().catch(() => ({}))
        if (!resp.ok) {
          throw new Error(payload?.detail || `Failed to load run (${resp.status})`)
        }
        setRun(payload)
        setLoadRunId(loc.run_id)
        const matrix = payload?.preparation?.matrix || []
        const dates = [...new Set(matrix.map((r) => r.trading_date).filter(Boolean))].sort()
        setSelectedPattern(null)
        setPatternDetail(null)
        setMatrixDate(loc.bar_ts ? String(loc.bar_ts).slice(0, 10) : (dates[0] || ''))
        const weekStart = payload?.selected_week_start || payload?.configuration?.selected_week_start
        const weekEnd = payload?.selected_week_end || payload?.configuration?.selected_week_end
        setLoadedRunBanner(
          weekStart
            ? `Loaded validation week ${String(weekStart).slice(0, 10)}${weekEnd ? ` to ${String(weekEnd).slice(0, 10)}` : ''} · Run ${loc.run_id}`
            : `Loaded run ${loc.run_id}`,
        )
        const chainsResp = await fetch(`${API}/runs/${loc.run_id}/review-chains`)
        const chains = await chainsResp.json().catch(() => ({}))
        if (chainsResp.ok) setReviewChainOptions(chains)
      }
      setReviewChainOverride(override)
      setActiveSymbol(loc.symbol)
      if (loc.bar_ts) {
        setSelectedBarTs(loc.bar_ts)
        setMatrixDate(String(loc.bar_ts).slice(0, 10))
      } else {
        setSelectedBarTs(null)
      }
      const mode = reviewMode === 'replay' ? 'replay' : 'full'
      await refreshReplay(loc.run_id, override)
      await fetchLearning(loc.run_id, loc.symbol, mode, override)
      // Exactly one scroll for this explicit locator click; consume intent so refreshes cannot resroll.
      const { state, scrolled, decision } = completeLocatorNavigation(labNavRef.current)
      labNavRef.current = reduceLabNavigation(state, { type: 'DATA_REFRESHED' })
      if (scrolled && decision.target?.bar_ts) {
        const key = String(decision.target.bar_ts).replace(' ', 'T').slice(0, 19)
        requestAnimationFrame(() => {
          const el = gridRowRefs.current[key]
          if (el?.scrollIntoView) {
            el.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
          } else if (reviewSummaryRef.current?.scrollIntoView) {
            reviewSummaryRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' })
          }
        })
      }
    } catch (err) {
      labNavRef.current = reduceLabNavigation(labNavRef.current, { type: 'CLEAR_SELECTION' })
      setError(err?.message || 'Could not open trade locator.')
    } finally {
      setLoading(false)
    }
  }

  const fetchLearning = useCallback(async (runId, sym, mode, override = undefined) => {
    if (!runId || !sym) return
    const td = matrixDate ? `&trading_date=${encodeURIComponent(matrixDate)}` : ''
    const f = learningFilters
    const fq = [
      f.action_changed_only ? 'action_changed_only=true' : '',
      f.meaningful_pattern_no_entry ? 'meaningful_pattern_no_entry=true' : '',
      f.transition_events_only ? 'transition_events_only=true' : '',
    ].filter(Boolean).join('&')
    const activeOverride = override === undefined ? reviewChainOverride : override
    const chainQs = (activeOverride?.context_attempt_id && activeOverride?.simulation_attempt_id)
      ? (
        `&context_attempt_id=${encodeURIComponent(activeOverride.context_attempt_id)}` +
        `&simulation_attempt_id=${encodeURIComponent(activeOverride.simulation_attempt_id)}`
      )
      : ''
    const timelineQs = chainQs ? `?${chainQs.slice(1)}` : ''
    const symQs = `mode=${encodeURIComponent(mode)}&offset=0&limit=500${td}${fq ? `&${fq}` : ''}${chainQs}`
    const [lvResp, lsResp, certResp, tlResp] = await Promise.all([
      fetch(`${API}/runs/${runId}/learning-view?mode=${encodeURIComponent(mode)}&active_symbol=${encodeURIComponent(sym)}${td ? `&trading_date=${encodeURIComponent(matrixDate)}` : ''}${chainQs}`),
      fetch(`${API}/runs/${runId}/symbols/${encodeURIComponent(sym)}/learning-view?${symQs}`),
      fetch(`${API}/runs/${runId}/certification-summary`),
      fetch(`${API}/runs/${runId}/account-timeline${timelineQs}`),
    ])
    const lv = await lvResp.json().catch(() => ({}))
    const ls = await lsResp.json().catch(() => ({}))
    const cert = await certResp.json().catch(() => ({}))
    const tl = await tlResp.json().catch(() => ({}))
    if (!lvResp.ok && lv?.detail) {
      setError(typeof lv.detail === 'string' ? lv.detail : 'Learning view failed')
    }
    if (lvResp.ok) setLearningRun(lv)
    if (lsResp.ok) {
      setLearningSymbol(ls)
      setPatternsCatalog(ls.patterns || [])
      setContextRows(
        (ls.grid_rows || []).map((r) => ({
          bar_ts: r.bar_ts,
          state_after: r.state_after,
          selected_action: r.selected_action,
          thesis_effect: r.thesis_effect,
          payload_json: r.payload_json,
          explanation: r.explanation,
        })),
      )
      setObservations(
        (ls.grid_rows || []).map((r) => ({
          bar_ts: r.bar_ts,
          brooks_obs_json: r.objective_terms,
          derived_metrics_json: { direction: r.direction },
          pattern_snapshot_json: r.active_patterns,
          explanation: r.explanation_sections?.objective_facts,
        })),
      )
      if (ls.chart_bars?.length) {
        setVisibleBars(ls.chart_bars)
      }
    }
    if (certResp.ok) setCertification(cert)
    if (tlResp.ok) {
      setReviewTrades(
        (tl.events || []).filter((e) => e.kind === 'TRADE_ENTRY' || e.kind === 'TRADE_EXIT'),
      )
    } else {
      setReviewTrades([])
    }
  }, [learningFilters, matrixDate, reviewChainOverride])

  const refreshReplay = useCallback(async (runId, override = undefined) => {
    if (!runId) return
    try {
      const rsResp = await fetch(`${API}/runs/${runId}/replay-state`)
      const rs = await rsResp.json().catch(() => ({}))
      if (rsResp.ok) setReplayState(rs)
      const sym = activeSymbol
      const mode = reviewMode === 'replay' ? 'replay' : 'full'
      await fetchLearning(runId, sym, mode, override)
      const vbResp = await fetch(`${API}/runs/${runId}/visible-bars/${sym}`)
      const vb = await vbResp.json().catch(() => ({}))
      if (vbResp.ok && !(learningSymbol?.chart_bars?.length)) {
        setVisibleBars(vb.bars || [])
      }
    } catch {
      /* ignore polling errors */
    }
  }, [activeSymbol, fetchLearning, reviewMode, learningSymbol?.chart_bars?.length])

  const contextByTs = useMemo(() => {
    const m = {}
    for (const row of contextRows) {
      m[String(row.bar_ts || '').slice(0, 19)] = row
    }
    return m
  }, [contextRows])

  const latestContext = useMemo(() => {
    if (!contextRows.length) return null
    return contextRows[contextRows.length - 1]
  }, [contextRows])

  const symbols = useMemo(() => {
    const parts = symbolsText.split(/[,\s]+/).map((s) => s.trim().toUpperCase()).filter(Boolean)
    return parts.length ? parts : DEFAULT_SYMBOLS
  }, [symbolsText])

  const activeAttemptChain = useMemo(() => {
    if (reviewChainOverride?.context_attempt_id && reviewChainOverride?.simulation_attempt_id) {
      return reviewChainOverride
    }
    const chain = learningRun?.provenance?.attempt_chain
    if (chain?.context_attempt_id && chain?.simulation_attempt_id) {
      return {
        context_attempt_id: chain.context_attempt_id,
        simulation_attempt_id: chain.simulation_attempt_id,
      }
    }
    const t = experimentTrades[0]
    if (t?.context_attempt_id && t?.simulation_attempt_id) {
      return {
        context_attempt_id: t.context_attempt_id,
        simulation_attempt_id: t.simulation_attempt_id,
      }
    }
    return null
  }, [reviewChainOverride, learningRun, experimentTrades])

  const loadTradeManagementReview = useCallback(async (trade) => {
    if (!run?.run_id || !trade?.trade_id) return
    const ctxId = trade.context_attempt_id || activeAttemptChain?.context_attempt_id
    const simId = trade.simulation_attempt_id || activeAttemptChain?.simulation_attempt_id
    if (!ctxId || !simId) {
      setTradeMgmtReview({ error: 'Select a run with a resolved context/simulation attempt chain.' })
      return
    }
    setSelectedTradeMgmt(trade)
    setTradeMgmtLoading(true)
    setTradeMgmtReview(null)
    try {
      const qs = new URLSearchParams({
        context_attempt_id: ctxId,
        simulation_attempt_id: simId,
      })
      const resp = await fetch(
        `${API}/runs/${run.run_id}/sim-trades/${encodeURIComponent(trade.trade_id)}/management-review?${qs}`,
      )
      const body = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        setTradeMgmtReview({ error: body?.detail || 'Trade management review failed' })
      } else {
        setTradeMgmtReview(body)
      }
    } catch (err) {
      setTradeMgmtReview({ error: err?.message || 'Trade management review failed' })
    } finally {
      setTradeMgmtLoading(false)
    }
  }, [run?.run_id, activeAttemptChain])

  const refreshRun = useCallback(async (runId) => {
    const resp = await fetch(`${API}/runs/${runId}`)
    const payload = await resp.json().catch(() => ({}))
    if (!resp.ok) {
      throw new Error(payload?.detail || `Failed to load run (${resp.status})`)
    }
    setRun(payload)
    if (payload?.symbols?.length && !payload.symbols.includes(activeSymbol)) {
      setActiveSymbol(payload.symbols[0])
    }
    await refreshReplay(runId)
  }, [activeSymbol, refreshReplay])

  useEffect(() => {
    let cancelled = false
    fetch(`${API}/meta`)
      .then((r) => r.json())
      .then((data) => {
        if (!cancelled && data?.simulation_banner) setMetaBanner(data.simulation_banner)
      })
      .catch(() => {})
    return () => { cancelled = true }
  }, [])

  const selectBar = useCallback((barTs, patternSnap) => {
    // Explicit user bar click — scroll once here, never from data-refresh effects.
    setSelectedBarTs(barTs)
    setSelectedPattern(patternSnap || null)
    if (patternSnap?.pattern_instance_id) {
      const full = patternsCatalog.find((p) => p.pattern_instance_id === patternSnap.pattern_instance_id)
      setPatternDetail(full || patternSnap)
    } else {
      setPatternDetail(null)
    }
    const key = String(barTs || '').slice(0, 19)
    const el = gridRowRefs.current[key]
    if (el?.scrollIntoView) el.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
  }, [patternsCatalog])

  const applyReviewChainSelection = useCallback((override) => {
    const { state } = applyReviewChainChange(labNavRef.current, override)
    labNavRef.current = state
    setSelectedBarTs(null)
    setSelectedPattern(null)
    setPatternDetail(null)
    setReviewChainOverride(override)
  }, [])

  const runPatternReset = async () => {
    if (!run?.run_id) return
    setLoading(true)
    setError('')
    try {
      const resp = await fetch(`${API}/runs/${run.run_id}/reset-pattern-replay`, { method: 'POST' })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) throw new Error(payload?.detail?.message || payload?.detail || 'Pattern reset failed')
      await refreshRun(run.run_id)
    } catch (err) {
      setError(err?.message || 'Pattern reset failed.')
    } finally {
      setLoading(false)
    }
  }

  const createRun = async () => {
    setLoading(true)
    setError('')
    try {
      if (symbols.length !== 4) {
        throw new Error('Configure exactly four pilot symbols for v0.1.')
      }
      const body = {
        mode: 'HISTORICAL_REPLAY',
        symbols,
        starting_cash: 1000,
      }
      if (weekStart) body.week_start = weekStart
      const resp = await fetch(`${API}/runs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(parseApiError(payload, `Create run failed (${resp.status})`))
      }
      setRun(payload.run)
      setActiveSymbol(payload.run?.symbols?.[0] || DEFAULT_SYMBOLS[0])
    } catch (err) {
      setError(err?.message || 'Unable to create run.')
    } finally {
      setLoading(false)
    }
  }

  const runAction = async (action) => {
    if (!run?.run_id) return
    setLoading(true)
    setError('')
    try {
      const resp = await fetch(`${API}/runs/${run.run_id}/${action}`, { method: 'POST' })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(parseApiError(payload, `${action} failed (${resp.status})`))
      }
      await refreshRun(run.run_id)
    } catch (err) {
      setError(err?.message || `Action ${action} failed.`)
    } finally {
      setLoading(false)
    }
  }

  const preparation = run?.preparation
  const matrixRows = preparation?.matrix || []

  const loadDossier = useCallback(async (runId, symbol, tradingDate) => {
    if (!runId || !symbol || !tradingDate) {
      setFrozenDossier(null)
      return
    }
    const resp = await fetch(`${API}/runs/${runId}/dossiers/${symbol}/${tradingDate}`)
    const payload = await resp.json().catch(() => ({}))
    if (!resp.ok) {
      setFrozenDossier({ error: payload?.detail?.message || 'Dossier not available.' })
      return
    }
    setFrozenDossier(payload.dossier)
  }, [])

  useEffect(() => {
    if (!run?.run_id || !activeSymbol) return
    const dates = matrixRows.map((r) => r.trading_date).filter(Boolean)
    const td = matrixDate || dates[0]
    if (td) loadDossier(run.run_id, activeSymbol, td)
  }, [run?.run_id, activeSymbol, matrixDate, matrixRows, loadDossier])

  const reconstructDossiers = async () => {
    if (!run?.run_id) return
    setLoading(true)
    setError('')
    try {
      const resp = await fetch(`${API}/runs/${run.run_id}/reconstruct-dossiers`, { method: 'POST' })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(parseApiError(payload, `Reconstruct failed (${resp.status})`))
      }
      await refreshRun(run.run_id)
    } catch (err) {
      setError(err?.message || 'Reconstruct failed.')
    } finally {
      setLoading(false)
    }
  }

  const prepareRun = async () => {
    if (!run?.run_id) return
    setLoading(true)
    setError('')
    try {
      const resp = await fetch(`${API}/runs/${run.run_id}/prepare`, { method: 'POST' })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        const msg = payload?.detail?.message || payload?.detail || `Prepare failed (${resp.status})`
        throw new Error(typeof msg === 'string' ? msg : JSON.stringify(msg))
      }
      await refreshRun(run.run_id)
    } catch (err) {
      setError(err?.message || 'Prepare failed.')
    } finally {
      setLoading(false)
    }
  }

  const parseApiError = (payload, fallback) => {
    if (payload?.detail?.message) return payload.detail.message
    if (typeof payload?.detail === 'string') return payload.detail
    return fallback
  }

  const readiness = run?.readiness || preparation?.readiness
  const replayBlocked = readiness?.replay_readiness !== 'READY'

  const acquireBars = async (retryFailed = false) => {
    if (!run?.run_id) return
    setLoading(true)
    setError('')
    try {
      const qs = retryFailed ? '?retry_failed=true' : ''
      const resp = await fetch(`${API}/runs/${run.run_id}/acquire-historical-bars${qs}`, { method: 'POST' })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(parseApiError(payload, `Acquire failed (${resp.status})`))
      }
      await refreshRun(run.run_id)
    } catch (err) {
      setError(err?.message || 'Acquire failed.')
    } finally {
      setLoading(false)
    }
  }

  const dryRunAcquire = async () => {
    if (!run?.run_id) return
    setLoading(true)
    setError('')
    try {
      const resp = await fetch(`${API}/runs/${run.run_id}/acquire-historical-bars`)
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(parseApiError(payload, `Acquire plan failed (${resp.status})`))
      }
      const n = payload.sessions_to_request ?? payload.plan?.length ?? 0
      setError('')
      window.alert(`Dry run: ${n} symbol/session(s) would be requested from IB historical data.`)
    } catch (err) {
      setError(err?.message || 'Acquire plan failed.')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (!run?.run_id) return
    // While Phase 9 orchestration is active, poll only the lightweight status endpoint.
    const phase9Active = phase9?.overall_status === 'RUNNING' || phase9?.overall_status === 'WAITING_FOR_TWS'
    refreshReplay(run.run_id)
    if (phase9Active) return undefined
    const id = setInterval(() => refreshReplay(run.run_id), 4000)
    return () => clearInterval(id)
  }, [run?.run_id, activeSymbol, refreshReplay, reviewMode, learningFilters, phase9?.overall_status, reviewChainOverride])

  const selectedGridRow = useMemo(() => {
    if (!learningSymbol?.grid_rows?.length || !selectedBarTs) return null
    const key = String(selectedBarTs).slice(0, 19)
    return learningSymbol.grid_rows.find((r) => String(r.bar_ts).slice(0, 19) === key) || null
  }, [learningSymbol, selectedBarTs])

  const chartBarsForDisplay = useMemo(() => {
    if (learningSymbol?.chart_bars?.length) return learningSymbol.chart_bars
    return visibleBars
  }, [learningSymbol, visibleBars])

  const accountSummary = learningRun?.account_summary

  const nextBar = async () => {
    if (!run?.run_id) return
    setLoading(true)
    setError('')
    try {
      const resp = await fetch(`${API}/runs/${run.run_id}/next-bar`, { method: 'POST' })
      const payload = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        throw new Error(parseApiError(payload, `Next bar failed (${resp.status})`))
      }
      await refreshRun(run.run_id)
    } catch (err) {
      setError(err?.message || 'Next bar failed.')
    } finally {
      setLoading(false)
    }
  }

  const levelDistanceWarning = useMemo(() => {
    if (!frozenDossier?.invalidation_level || !frozenDossier?.latest_close) return null
    const inv = Number(frozenDossier.invalidation_level)
    const close = Number(frozenDossier.latest_close)
    if (!inv || !close) return null
    const pct = Math.abs(inv - close) / close
    if (pct < 0.15) return null
    return `Daily thesis invalidation is ${(pct * 100).toFixed(0)}% below the latest known close. This may be valid as a broad daily structural invalidation but should not automatically become an intraday simulated trade stop.`
  }, [frozenDossier])

  const startDisabledReason = run && replayBlocked
    ? `${preparation?.counts?.dossiers_compiled ?? 0} dossiers are ready, but ${preparation?.counts?.bar_sessions_complete ?? 0} of ${preparation?.counts?.bar_sessions_expected ?? 20} historical sessions contain a complete validated 5-minute RTH dataset.`
    : null

  const symbolRows = useMemo(() => {
    if (learningRun?.symbol_overviews?.length) {
      return learningRun.symbol_overviews.map((o) => ({
        symbol: o.symbol,
        paa_verdict: o.paa_verdict,
        brooks_state: o.current_state,
        latest_finding: (o.active_meaningful_patterns || []).map((p) => p.pattern_family).join(', ') || '—',
        latest_action: o.latest_advisory_action,
        position_status: o.simulated_position ? 'Open' : 'Flat',
        thesis_status: o.thesis_effect,
        blocked_by_other_position: false,
      }))
    }
    return run?.symbol_snapshots || []
  }, [learningRun, run])

  return (
    <div className="bil-page">
      <div className="bil-sim-banner" role="status">
        {metaBanner}
      </div>

      <header className="bil-header">
        <div>
          <span className="bil-kicker">Research module · historical replay</span>
          <h1>Brooks Intraday Lab</h1>
          <p>
            Replay one historical week on 5-minute RTH bars for four symbols with a frozen daily PAA
            thesis, deterministic Brooks observations, and an isolated $1,000 simulated account.
            No connection to live portfolios, IBKR orders, LPA, or proposal execution.
          </p>
        </div>
        <span className="bil-research-tag">Simulation only · no execution authority</span>
      </header>

      <section className="bil-config" aria-labelledby="bil-config-heading">
        <h2 id="bil-config-heading">Run configuration</h2>
        <div className="bil-config-grid">
          <label>
            Historical week (Monday)
            <input
              type="date"
              value={weekStart}
              onChange={(e) => setWeekStart(e.target.value)}
            />
          </label>
          <label>
            Pilot symbols (comma-separated, 4)
            <input
              type="text"
              value={symbolsText}
              onChange={(e) => setSymbolsText(e.target.value)}
              spellCheck={false}
            />
          </label>
          <label>
            Playback speed
            <select value={playbackSpeed} onChange={(e) => setPlaybackSpeed(e.target.value)}>
              {PLAYBACK_SPEEDS.map((sp) => (
                <option key={sp} value={sp}>{sp}</option>
              ))}
            </select>
          </label>
        </div>
        <div className="bil-actions">
          <label className="bil-load-run">
            Existing run ID
            <input
              type="text"
              value={loadRunId}
              onChange={(e) => setLoadRunId(e.target.value)}
              spellCheck={false}
              placeholder="uuid"
            />
          </label>
          <button type="button" className="primary" disabled={loading || !loadRunId.trim()} onClick={loadExistingRun}>
            Load run
          </button>
          <button type="button" disabled={loading} onClick={createRun}>
            Create new run
          </button>
          <button
            type="button"
            disabled={!run || loading || replayBlocked}
            title={startDisabledReason || ''}
            onClick={() => runAction('start')}
          >
            Start
          </button>
          <button type="button" disabled={!run || loading} onClick={() => runAction('pause')}>
            Pause
          </button>
          <button type="button" disabled={!run || loading} onClick={() => runAction('resume')}>
            Resume
          </button>
          <button type="button" disabled={!run || loading} onClick={() => runAction('stop')}>
            Stop
          </button>
          <button type="button" disabled={!run || loading} onClick={() => runAction('reset')}>
            Reset
          </button>
          <button type="button" disabled={!run || loading} onClick={runPatternReset}>
            Reset pattern replay (Phase 5)
          </button>
          <button type="button" disabled={!run || loading} onClick={reconstructDossiers}>
            Reconstruct historical dossiers
          </button>
          <button type="button" className="primary" disabled={!run || loading} onClick={prepareRun}>
            Prepare dossiers
          </button>
          <button type="button" disabled={!run || loading} onClick={dryRunAcquire}>
            Preview acquire plan
          </button>
          <button type="button" disabled={!run || loading} onClick={() => acquireBars(false)}>
            Acquire missing historical bars
          </button>
          <button type="button" disabled={!run || loading} onClick={() => acquireBars(true)}>
            Retry failed sessions
          </button>
          <button type="button" disabled={!run || loading || run?.status === 'COMPLETED'} onClick={nextBar}>
            Next bar
          </button>
        </div>
        {readiness ? (
          <div className={replayBlocked ? 'bil-replay-banner bil-replay-banner--blocked' : 'bil-replay-banner bil-replay-banner--ready'}>
            <strong>{replayBlocked ? 'Replay blocked' : 'Replay ready'}</strong>
            <span>
              Dossiers: {readiness.dossier_readiness} · Historical data: {readiness.historical_bar_readiness} · Replay: {readiness.replay_readiness}
            </span>
            {replayBlocked && startDisabledReason ? <p>{startDisabledReason}</p> : null}
          </div>
        ) : null}
        {preparation ? (
          <div className="bil-note">
            Preparation: {run?.preparation_status} — dossiers {preparation.counts?.dossiers_compiled}/
            {preparation.counts?.dossiers_expected}, bars complete {preparation.counts?.bar_sessions_complete}/
            {preparation.counts?.bar_sessions_expected}
            {preparation.historical_bar_source ? ` · bar source: ${preparation.historical_bar_source}` : ''}
          </div>
        ) : null}
        {replayState?.phase5_pattern_attempt_id ? (
          <div className="bil-note bil-review-baseline">
            Phase 5 pattern attempt:{' '}
            <code>{replayState.phase5_pattern_attempt_id.slice(0, 8)}…</code>
          </div>
        ) : null}
        {replayState?.phase4_review_baseline_attempt_id ? (
          <div className="bil-note bil-review-baseline">
            Phase 4 review baseline:{' '}
            <code>{replayState.phase4_review_baseline_attempt_id.slice(0, 8)}…</code>
            {' · '}
            Observations use this completed attempt for chart/grid review.
          </div>
        ) : null}
        {replayState ? (
          <div className="bil-note bil-replay-progress">
            Step {replayState.completed_steps ?? 0} of {replayState.total_steps ?? 390}
            {' · '}
            Observations: {replayState.observations_total ?? 0} / {replayState.observations_expected ?? 1560}
            {run?.replay_timestamp ? ` · NY: ${run.replay_timestamp}` : ''}
          </div>
        ) : null}
        {error ? <p className="bil-error">{error}</p> : null}
        <p className="bil-note">
          Phase 5 adds stateful pattern instances on top of the Phase 4 baseline. Actions remain OBSERVE / WAIT only.
        </p>
      </section>

      {run ? (
        <>
          <section className="bil-session" aria-labelledby="bil-session-heading">
            <h2 id="bil-session-heading">Session</h2>
            <dl className="bil-session-grid">
              <div>
                <dt>Run ID</dt>
                <dd>{run.run_id?.slice(0, 8)}…</dd>
              </div>
              <div>
                <dt>Mode</dt>
                <dd>{run.mode}</dd>
              </div>
              <div>
                <dt>Week</dt>
                <dd>{run.selected_week_start || '—'}</dd>
              </div>
              <div>
                <dt>Ruleset</dt>
                <dd>{run.ruleset_version}</dd>
              </div>
              <div>
                <dt>Cash</dt>
                <dd>{formatMoney(run.current_cash)}</dd>
              </div>
              <div>
                <dt>Starting cash</dt>
                <dd>{formatMoney(run.starting_cash)}</dd>
              </div>
              <div>
                <dt>Open position</dt>
                <dd>{run.open_position_symbol || 'None'}</dd>
              </div>
              <div>
                <dt>Realized P/L</dt>
                <dd>{formatMoney(run.realized_pnl)}</dd>
              </div>
              <div>
                <dt>Unrealized P/L</dt>
                <dd>{formatMoney(run.unrealized_pnl)}</dd>
              </div>
              <div>
                <dt>Replay time</dt>
                <dd>{run.replay_timestamp || '—'}</dd>
              </div>
              <div>
                <dt>Status</dt>
                <dd><span className={statusClass(run.status)}>{run.status}</span></dd>
              </div>
            </dl>
          </section>

          {learningRun?.provenance ? (
            <section className="bil-provenance" aria-labelledby="bil-provenance-heading" ref={provenanceRef}>
              <h2 id="bil-provenance-heading">Attempt provenance (Phase 8)</h2>
              {learningRun.provenance.review_only_banner ? (
                <p className="bil-review-override-banner" role="status">
                  {learningRun.provenance.review_only_banner}
                </p>
              ) : null}
              {learningRun.provenance.dossier_provenance?.reconstructed_badge ? (
                <p className="bil-recon-badge" role="status">
                  <strong>{learningRun.provenance.dossier_provenance.reconstructed_badge}</strong>
                </p>
              ) : null}
              <dl className="bil-session-grid bil-provenance-grid">
                <div><dt>Run</dt><dd><code>{learningRun.provenance.run_id}</code></dd></div>
                <div>
                  <dt>Context ruleset</dt>
                  <dd><code>{learningRun.provenance.attempt_chain?.context_ruleset || '—'}</code></dd>
                </div>
                <div>
                  <dt>Context attempt</dt>
                  <dd><code>{learningRun.provenance.attempt_chain?.context_attempt_id || '—'}</code></dd>
                </div>
                <div>
                  <dt>Simulation attempt</dt>
                  <dd><code>{learningRun.provenance.attempt_chain?.simulation_attempt_id || '—'}</code></dd>
                </div>
                <div>
                  <dt>Review chain</dt>
                  <dd>{learningRun.provenance.review_override ? 'Alternate (session-only)' : 'Official pinned'}</dd>
                </div>
                {Object.entries(learningRun.provenance.attempt_chain || {})
                  .filter(([k]) => !['context_ruleset', 'context_attempt_id', 'simulation_attempt_id', 'review_override', 'review_only'].includes(k))
                  .map(([k, v]) => (
                    <div key={k}><dt>{k}</dt><dd><code>{String(v ?? '—')}</code></dd></div>
                  ))}
                <div><dt>Bar freeze hash</dt><dd>{learningRun.provenance.bar_dataset_freeze_hash?.slice(0, 16) || '—'}…</dd></div>
                <div><dt>Dossier hash</dt><dd>{learningRun.provenance.dossier_provenance?.dossier_aggregate_hash?.slice(0, 16) || '—'}…</dd></div>
              </dl>
              <div className="bil-review-controls">
                {reviewChainOptions ? (
                  <label>
                    Review attempt chain{' '}
                    <select
                      value={
                        reviewChainOverride
                          ? `${reviewChainOverride.context_attempt_id}|${reviewChainOverride.simulation_attempt_id}`
                          : 'official'
                      }
                      onChange={(e) => {
                        const val = e.target.value
                        if (val === 'official') {
                          applyReviewChainSelection(null)
                          return
                        }
                        const [ctx, sim] = val.split('|')
                        const alt = (reviewChainOptions.alternatives || []).find(
                          (a) => a.context_attempt_id === ctx && a.simulation_attempt_id === sim,
                        )
                        applyReviewChainSelection(
                          alt
                            ? {
                              context_attempt_id: alt.context_attempt_id,
                              simulation_attempt_id: alt.simulation_attempt_id,
                              context_ruleset: alt.context_ruleset,
                            }
                            : { context_attempt_id: ctx, simulation_attempt_id: sim },
                        )
                      }}
                    >
                      <option value="official">{reviewChainOptions.official?.label || 'Official pinned chain'}</option>
                      {(reviewChainOptions.alternatives || []).map((a) => (
                        <option
                          key={`${a.context_attempt_id}|${a.simulation_attempt_id}`}
                          value={`${a.context_attempt_id}|${a.simulation_attempt_id}`}
                        >
                          {a.label}
                        </option>
                      ))}
                    </select>
                  </label>
                ) : null}
                <label>
                  Review mode{' '}
                  <select value={reviewMode} onChange={(e) => setReviewMode(e.target.value)}>
                    <option value="full">Completed run (all 390 bars/symbol)</option>
                    <option value="replay">Step replay (revealed bars only)</option>
                  </select>
                </label>
                <button type="button" className="bil-link-btn" onClick={() => setShowCertModal(true)}>
                  SIMULATION ENGINE CERTIFICATION — SYNTHETIC FIXTURES
                </button>
              </div>
              <p className="bil-note">
                Attempt-chain selection is review-only for this browser session. It does not change official run pins, approve, or activate alternate attempts. Reloading the page returns to the official pinned chain.
              </p>
            </section>
          ) : null}

          {accountSummary ? (
            <section className="bil-session" aria-labelledby="bil-account-heading">
              <h2 id="bil-account-heading">Simulated account (historical attempt)</h2>
              <dl className="bil-session-grid">
                <div><dt>Starting cash</dt><dd>{formatMoney(accountSummary.starting_cash)}</dd></div>
                <div><dt>Ending cash</dt><dd>{formatMoney(accountSummary.ending_cash)}</dd></div>
                <div><dt>Trades</dt><dd>{accountSummary.trades ?? 0}</dd></div>
                <div><dt>Realised P/L</dt><dd>{formatMoney(accountSummary.realized_pnl)}</dd></div>
                <div><dt>Wins / losses</dt><dd>{accountSummary.wins ?? 0} / {accountSummary.losses ?? 0}</dd></div>
              </dl>
              {accountSummary.zero_trade_explanation ? (
                <p className="bil-note">{accountSummary.zero_trade_explanation}</p>
              ) : null}
              {accountSummary.pilot_week_detail ? (
                <p className="bil-note">{accountSummary.pilot_week_detail}</p>
              ) : null}
              {reviewTrades.length ? (
                <div className="bil-review-trades">
                  <h3>Trade events (selected chain)</h3>
                  <ul className="bil-timeline-list">
                    {reviewTrades.map((ev, i) => {
                      const money = formatTradeEventMoney(ev)
                      return (
                        <li key={`${ev.kind}-${ev.ts}-${i}`}>
                          <code>{ev.kind}</code> {ev.symbol} · {String(ev.ts || '').slice(0, 19)}
                          {money ? ` · ${money}` : ''}
                        </li>
                      )
                    })}
                  </ul>
                </div>
              ) : null}
            </section>
          ) : null}

          <section className="bil-overview bil-experiment" aria-labelledby="bil-experiment-heading">
            <h2 id="bil-experiment-heading">Phase 9 — Unseen-week validation</h2>
            <p className="bil-note">
              Baseline engineering week 2026-07-20 (<code>4eababc8…</code>) is not modified.
              Ruleset freeze: <strong>{phase9?.freeze_id || experimentOverview?.freeze_id || 'BROOKS_EXPERIMENT_RULESET_FREEZE_V1'}</strong>.
              Historical IB data only — no live orders.
            </p>

            {phase9 ? (
              <div className="bil-p9-control">
                <div className="bil-p9-summary">
                  <p>
                    <strong>Phase 9 progress:</strong> {phase9.weeks_completed_count ?? 0} of {phase9.weeks_total ?? 3} weeks complete
                    {phase9.phase9_complete ? ' — Phase 9 complete' : null}
                  </p>
                  <p>
                    <span className={statusClass(phase9.overall_status)}>{phase9.overall_status}</span>
                    {phase9.next_pending_week ? (
                      <> · Next week: <strong>{phase9.next_pending_week}</strong></>
                    ) : null}
                  </p>
                  {phase9.current_stage || phase9.stage_progress ? (
                    <p className="bil-note">
                      {phase9.progress?.label || phase9.current_stage}
                      {phase9.stage_progress?.updated_at_utc
                        ? ` · Last progress: ${String(phase9.stage_progress.updated_at_utc).slice(11, 19)}`
                        : null}
                      {phase9.heartbeat_at ? ` · Heartbeat: ${String(phase9.heartbeat_at).slice(11, 19)}` : null}
                    </p>
                  ) : null}
                  {phase9.interrupted_prior_run ? (
                    <p className="bil-p9-warn">A prior run was interrupted. Progress was preserved — use Resume current week.</p>
                  ) : null}
                  {phase9.overall_status === 'WAITING_FOR_TWS' ? (
                    <p className="bil-p9-warn">
                      Open and log in to TWS or IB Gateway, enable API access, then select Check TWS connection or Resume.
                    </p>
                  ) : null}
                  {phase9.last_error?.message ? (
                    <p className="bil-p9-error" role="alert">
                      {phase9.last_error.message}
                      {phase9.last_error.recoverable ? ' (recoverable)' : ''}
                    </p>
                  ) : null}
                  <p className="bil-note bil-p9-next">{phase9.next_automatic_action}</p>
                </div>

                <div className="bil-p9-tws">
                  <h3>TWS / API status</h3>
                  <dl className="bil-dl-compact">
                    <div><dt>Readiness</dt><dd>{phase9.tws?.readiness || '—'}</dd></div>
                    <div><dt>Host</dt><dd>{phase9.tws?.host}:{phase9.tws?.port}</dd></div>
                    <div><dt>Historical client ID</dt><dd>{phase9.tws?.historical_client_id ?? '—'}</dd></div>
                    <div><dt>Server version</dt><dd>{phase9.tws?.server_version ?? '—'}</dd></div>
                    <div><dt>Account</dt><dd>{(phase9.tws?.managed_accounts_masked || []).join(', ') || '—'}</dd></div>
                    <div><dt>Last probe (UTC)</dt><dd>{phase9.tws?.last_successful_probe_at_utc || phase9.tws?.checked_at_utc || '—'}</dd></div>
                  </dl>
                  <button type="button" className="bil-btn" disabled={phase9Busy} onClick={() => phase9Post('check-tws')}>
                    Check TWS connection
                  </button>
                </div>

                <div className="bil-p9-actions">
                  <button
                    type="button"
                    className="bil-btn bil-btn-primary"
                    disabled={phase9Busy || phase9.phase9_complete || phase9.overall_status === 'RUNNING'}
                    onClick={() => phase9Post('run-next')}
                  >
                    Run next validation week
                  </button>
                  <button
                    type="button"
                    className="bil-btn"
                    disabled={phase9Busy || !['PAUSED', 'FAILED_RECOVERABLE', 'WAITING_FOR_TWS', 'READY'].includes(phase9.overall_status)}
                    onClick={() => phase9Post('resume')}
                  >
                    Resume current week
                  </button>
                  <button
                    type="button"
                    className="bil-btn"
                    disabled={phase9Busy || phase9.phase9_complete}
                    onClick={() => phase9Post('run-remaining')}
                  >
                    Run all remaining weeks
                  </button>
                  <button
                    type="button"
                    className="bil-btn"
                    disabled={phase9Busy || phase9.overall_status !== 'RUNNING'}
                    onClick={() => phase9Post('pause')}
                  >
                    Pause after current safe checkpoint
                  </button>
                  <button
                    type="button"
                    className="bil-btn"
                    disabled={phase9Busy || phase9.overall_status !== 'FAILED_RECOVERABLE'}
                    onClick={() => phase9Post('retry')}
                  >
                    Retry failed stage
                  </button>
                  <button
                    type="button"
                    className="bil-btn bil-btn-muted"
                    disabled={phase9Busy || phase9.overall_status === 'RUNNING'}
                    onClick={() => phase9Post('cancel-pending')}
                  >
                    Cancel pending work
                  </button>
                </div>

                <div className="bil-p9-weeks">
                  <h3>Validation weeks</h3>
                  <div className="bil-p9-week-grid">
                    {(phase9.weeks || []).map((w) => (
                      <article key={w.week_start} className="bil-p9-week-card">
                        <header>
                          <strong>{w.week_start}</strong> – {w.week_end}
                          <span className={statusClass(w.week_status)}>{w.week_status}</span>
                        </header>
                        <dl>
                          <div><dt>Run</dt><dd>{w.run_id ? <code>{String(w.run_id).slice(0, 8)}…</code> : '—'}</dd></div>
                          <div><dt>Bars</dt><dd>{w.bar_sessions_complete ?? 0} / {w.bar_sessions_expected ?? 20}</dd></div>
                          <div><dt>Dossiers</dt><dd>{w.dossiers_complete ?? 0} / {w.dossiers_expected ?? 20}</dd></div>
                          <div><dt>Stage</dt><dd>{w.current_stage || w.week_status || '—'}</dd></div>
                          <div><dt>Trades</dt><dd>{w.trades ?? '—'}</dd></div>
                          <div><dt>Ending cash</dt><dd>{formatMoney(w.ending_cash)}</dd></div>
                        </dl>
                        {w.current_session?.symbol ? (
                          <p className="bil-note">
                            Current session: {w.current_session.symbol} {w.current_session.trading_date || ''}
                          </p>
                        ) : null}
                        {w.review_link || w.run_id ? (
                          <button
                            type="button"
                            className="bil-link-btn"
                            onClick={async () => {
                              if (!w.run_id) return
                              setLoadRunId(w.run_id)
                              setLoading(true)
                              setError('')
                              try {
                                const resp = await fetch(`${API}/runs/${w.run_id}`)
                                const payload = await resp.json().catch(() => ({}))
                                if (!resp.ok) {
                                  throw new Error(payload?.detail || `Failed to load run (${resp.status})`)
                                }
                                setRun(payload)
                                resetReviewStateForRun(payload)
                                setLoadedRunBanner(
                                  `Loaded validation week ${w.week_start} to ${w.week_end} · Run ${w.run_id}`,
                                )
                                await refreshReplay(w.run_id)
                                if (reviewSummaryRef.current?.scrollIntoView) {
                                  reviewSummaryRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' })
                                }
                              } catch (err) {
                                setError(err?.message || 'Could not load validation week.')
                              } finally {
                                setLoading(false)
                              }
                            }}
                          >
                            Review week
                          </button>
                        ) : null}
                      </article>
                    ))}
                  </div>
                </div>

                <div className="bil-p9-activity">
                  <h3>Activity</h3>
                  <ul className="bil-p9-activity-list">
                    {[...(phase9.activity || [])]
                      .sort((a, b) => String(a.event_timestamp_utc || '').localeCompare(String(b.event_timestamp_utc || '')))
                      .slice(-40)
                      .map((a, i) => (
                        <li key={`${a.event_timestamp_utc || a.time}-${i}`} className={a.severity === 'ERROR' ? 'bil-p9-act-err' : ''}>
                          <time dateTime={a.event_timestamp_utc || undefined}>
                            {a.event_timestamp_utc ? String(a.event_timestamp_utc).slice(11, 19) : (a.time || '')}
                          </time>{' '}
                          {a.message}
                        </li>
                      ))}
                  </ul>
                </div>
              </div>
            ) : (
              <p className="bil-note">Loading experiment control…</p>
            )}

            {experimentOverview?.validation_runs?.length ? (
              <div className="bil-table-wrap">
                <table className="bil-table">
                  <thead>
                    <tr>
                      <th>Week</th>
                      <th>Run</th>
                      <th>Status</th>
                      <th>Ending cash</th>
                      <th>Trades</th>
                      <th>P/L</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {experimentOverview.validation_runs.map((row) => (
                      <tr key={row.run_id}>
                        <td>{row.week_start}</td>
                        <td><code>{String(row.run_id).slice(0, 8)}…</code></td>
                        <td>{row.status}</td>
                        <td>{formatMoney(row.ending_cash)}</td>
                        <td>{row.trades ?? '—'}</td>
                        <td>{formatMoney(row.realized_pnl)}</td>
                        <td>
                          <button
                            type="button"
                            className="bil-link-btn"
                            onClick={async () => {
                              setLoadRunId(row.run_id)
                              setLoading(true)
                              try {
                                await refreshRun(row.run_id)
                              } finally {
                                setLoading(false)
                              }
                            }}
                          >
                            Review
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="bil-note">Validation run details appear here as weeks complete.</p>
            )}
            {experimentComparison?.account_by_week?.length ? (
              <div className="bil-note">
                <strong>Cross-week accounts</strong>
                <ul>
                  {experimentComparison.account_by_week.map((a) => (
                    <li key={a.run_id}>
                      {a.week_start || a.run_id?.slice(0, 8)}: {formatMoney(a.ending_cash)} · {a.trades ?? 0} trades
                    </li>
                  ))}
                </ul>
                {experimentComparison.conversion_metrics ? (
                  <p>
                    CONSIDER_ENTRY events: {experimentComparison.conversion_metrics.consider_entry_events_total ?? 0}
                    {' · '}
                    Trades: {experimentComparison.conversion_metrics.trades_total ?? 0}
                  </p>
                ) : null}
              </div>
            ) : null}
            {experimentAggregate?.weeks_tested != null ? (
              <p className="bil-note">
                Aggregate (exploratory): {experimentAggregate.weeks_tested} weeks · {experimentAggregate.total_simulated_trades} trades ·
                gross P/L {formatMoney(experimentAggregate.gross_pnl)} · no-trade weeks {experimentAggregate.no_trade_weeks}
              </p>
            ) : null}
            <h3 style={{ fontSize: '0.95rem' }}>Trade browser</h3>
            <p className="bil-note">
              {run?.run_id
                ? (reviewChainOverride
                  ? 'Showing trades for the selected review-only attempt chain (not official pins).'
                  : 'Showing trades for this run’s official pinned simulation attempt.')
                : 'Showing trades across validation runs (official pins).'}
              {' '}Click a locator to open the symbol workspace on that chain.
            </p>
            <label className="bil-note">
              Filter{' '}
              <select value={tradeFilterWin} onChange={(e) => setTradeFilterWin(e.target.value)}>
                <option value="">All</option>
                <option value="win">Wins</option>
                <option value="loss">Losses</option>
              </select>
            </label>
            <div className="bil-table-wrap bil-obs-grid">
              <table className="bil-table">
                <thead>
                  <tr>
                    <th>Week</th>
                    <th>Symbol</th>
                    <th>Entry</th>
                    <th>Exit reason</th>
                    <th>P/L</th>
                    <th>Mgmt</th>
                    <th>Locator</th>
                  </tr>
                </thead>
                <tbody>
                  {experimentTrades.length ? experimentTrades.map((t) => (
                    <tr
                      key={`${t.ui_locator}-${t.entry_timestamp}`}
                      className={`clickable${selectedTradeMgmt?.trade_id === t.trade_id ? ' selected' : ''}`}
                      onClick={() => openTradeLocator(t.ui_locator)}
                      title="Open this trade in the symbol workspace"
                    >
                      <td>{t.week}</td>
                      <td>{t.symbol}</td>
                      <td>{String(t.entry_timestamp || '').slice(0, 19)}</td>
                      <td>{t.exit_reason}</td>
                      <td>{formatMoney(t.realized_pnl)}</td>
                      <td>
                        <button
                          type="button"
                          className="bil-link-btn"
                          onClick={(e) => {
                            e.stopPropagation()
                            loadTradeManagementReview(t)
                          }}
                        >
                          Review
                        </button>
                      </td>
                      <td><code>{t.ui_locator}</code></td>
                    </tr>
                  )) : (
                    <tr>
                      <td colSpan={7} className="bil-note">No trades for the selected review chain.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
            {selectedTradeMgmt ? (
              <section className="bil-trade-mgmt-review" aria-labelledby="bil-trade-mgmt-heading">
                <h3 id="bil-trade-mgmt-heading">Trade management review (read-only)</h3>
                {tradeMgmtLoading ? (
                  <p className="bil-note">Loading…</p>
                ) : tradeMgmtReview?.error ? (
                  <p className="bil-note">{tradeMgmtReview.error}</p>
                ) : tradeMgmtReview ? (
                  <>
                    <dl className="bil-dl-compact">
                      <div><dt>Symbol</dt><dd>{tradeMgmtReview.symbol}</dd></div>
                      <div><dt>Entry</dt><dd>{String(tradeMgmtReview.entry_ts || '').slice(0, 19)} @ {tradeMgmtReview.entry_price}</dd></div>
                      <div><dt>Quantity</dt><dd>{tradeMgmtReview.quantity ?? '—'}</dd></div>
                      <div><dt>Initial stop</dt><dd>{tradeMgmtReview.initial_stop?.display ?? '—'}</dd></div>
                      <div><dt>Current stop</dt><dd>{tradeMgmtReview.current_stop?.display ?? '—'}</dd></div>
                      <div><dt>MFE</dt><dd>{tradeMgmtReview.mfe ?? '—'}</dd></div>
                      <div><dt>MAE</dt><dd>{tradeMgmtReview.mae ?? '—'}</dd></div>
                      <div><dt>Peak unrealized P/L</dt><dd>
                        {tradeMgmtReview.max_unrealized_pnl != null
                          ? `${formatMoney(tradeMgmtReview.max_unrealized_pnl)} @ ${String(tradeMgmtReview.max_unrealized_pnl_ts || '').slice(0, 19)} (high ${tradeMgmtReview.max_unrealized_pnl_price})`
                          : '—'}
                      </dd></div>
                      <div><dt>Exit</dt><dd>{String(tradeMgmtReview.exit_ts || '').slice(0, 19)} @ {tradeMgmtReview.exit_price}</dd></div>
                      <div><dt>Exit reason</dt><dd>{tradeMgmtReview.exit_reason}</dd></div>
                      <div><dt>Realized P/L</dt><dd>{formatMoney(tradeMgmtReview.realized_pnl)}</dd></div>
                    </dl>
                    <p className="bil-note"><strong>Active exit rules</strong> ({tradeMgmtReview.active_exit_rules?.simulation_ruleset_version})</p>
                    <ul className="bil-timeline-list">
                      {(tradeMgmtReview.active_exit_rules?.exit_rules || []).map((line) => (
                        <li key={line}>{line}</li>
                      ))}
                    </ul>
                  </>
                ) : null}
              </section>
            ) : null}
          </section>

          <section className="bil-overview" aria-labelledby="bil-overview-heading" ref={reviewSummaryRef}>
            <h2 id="bil-overview-heading">Four-symbol overview</h2>
            {loadedRunBanner ? (
              <p className="bil-note bil-loaded-banner" role="status">{loadedRunBanner}</p>
            ) : null}
            <div className="bil-table-wrap">
              <table className="bil-table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>PAA verdict</th>
                    <th>Brooks state</th>
                    <th>Latest finding</th>
                    <th>Action</th>
                    <th>Position</th>
                    <th>Thesis</th>
                    <th>Blocked</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolRows.map((row) => (
                    <tr
                      key={row.symbol}
                      className={`clickable ${row.symbol === activeSymbol ? 'selected' : ''}`}
                      onClick={() => setActiveSymbol(row.symbol)}
                    >
                      <td>{row.symbol}</td>
                      <td>{row.paa_verdict || 'Not available'}</td>
                      <td>{row.brooks_state}</td>
                      <td>{row.latest_finding || '—'}</td>
                      <td>{row.latest_action}</td>
                      <td>{row.position_status}</td>
                      <td>{row.thesis_status}</td>
                      <td>{row.blocked_by_other_position ? 'Yes' : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          {matrixRows.length ? (
            <section className="bil-overview" aria-labelledby="bil-matrix-heading">
              <h2 id="bil-matrix-heading">Preparation matrix ({matrixRows.length} cells)</h2>
              <div className="bil-table-wrap">
                <table className="bil-table">
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Symbol</th>
                      <th>PAA</th>
                      <th>Analysis ID</th>
                      <th>Dossier</th>
                      <th>Origin</th>
                      <th>Obs</th>
                      <th>Sim</th>
                      <th>Bars (act/exp)</th>
                      <th>Bar status</th>
                      <th>Missing ts</th>
                      <th>Source</th>
                      <th>Retrieval</th>
                      <th>Freeze</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {matrixRows.map((row) => {
                      const key = `${row.trading_date}-${row.symbol}`
                      const selected = row.symbol === activeSymbol && row.trading_date === matrixDate
                      return (
                        <tr
                          key={key}
                          className={`clickable ${selected ? 'selected' : ''}`}
                          onClick={() => {
                            setActiveSymbol(row.symbol)
                            setMatrixDate(row.trading_date)
                          }}
                        >
                          <td>{row.trading_date}</td>
                          <td>{row.symbol}</td>
                          <td>{row.paa_status || '—'}</td>
                          <td>{row.paa_analysis_id ? `${String(row.paa_analysis_id).slice(0, 8)}…` : '—'}</td>
                          <td>{row.dossier_status || '—'}</td>
                          <td>{row.dossier_origin === 'HISTORICAL_RECONSTRUCTION' ? 'Reconstructed' : row.dossier_origin || '—'}</td>
                          <td>{row.observation_ready ? 'Yes' : 'No'}</td>
                          <td>{row.simulation_ready ? 'Yes' : 'No'}</td>
                          <td>{row.bar_count ?? '—'}{row.bar_expected != null ? ` / ${row.bar_expected}` : ''}</td>
                          <td>{row.bar_data_status || '—'}</td>
                          <td>{row.missing_timestamps_count ?? '—'}</td>
                          <td>{row.bar_source || '—'}</td>
                          <td>{row.retrieval_status || '—'}</td>
                          <td>{row.data_frozen ? 'Yes' : '—'}</td>
                          <td>
                            {(row.missing_timestamps_count ?? 0) > 0 ? (
                              <button
                                type="button"
                                className="bil-link-btn"
                                onClick={(e) => {
                                  e.stopPropagation()
                                  setMissingTsModal(row)
                                }}
                              >
                                View
                              </button>
                            ) : '—'}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          <section className="bil-workspace" aria-labelledby="bil-workspace-heading">
            <h2 id="bil-workspace-heading">Symbol workspace — {activeSymbol}</h2>
            <div className="bil-symbol-tabs" role="tablist">
              {(run.symbols || DEFAULT_SYMBOLS).map((sym) => (
                <button
                  key={sym}
                  type="button"
                  role="tab"
                  aria-selected={sym === activeSymbol}
                  className={sym === activeSymbol ? 'active' : ''}
                  onClick={() => setActiveSymbol(sym)}
                >
                  {sym}
                </button>
              ))}
            </div>
            <div className="bil-workspace-panels">
              <div>
                <h3 style={{ margin: '0 0 0.5rem', fontSize: '0.95rem' }}>
                  5-minute chart
                  {learningSymbol?.grid_total != null ? ` · ${learningSymbol.grid_total} bars` : ''}
                </h3>
                <div className="bil-marker-toggles bil-note">
                  {['DAILY_LEVELS', 'OBJECTIVE_FACTS', 'STRUCTURE', 'PATTERNS', 'CONTEXT', 'ACTIONS', 'SIMULATED_TRADES'].map((cat) => (
                    <label key={cat} style={{ marginRight: '0.75rem' }}>
                      <input
                        type="checkbox"
                        checked={!!chartLayers[cat]}
                        onChange={(e) => setChartLayers((m) => ({ ...m, [cat]: e.target.checked }))}
                      />
                      {cat.replace(/_/g, ' ')}
                    </label>
                  ))}
                </div>
                <div className="bil-mini-chart">
                  {chartBarsForDisplay.length ? (
                    <svg viewBox="0 0 400 120" className="bil-mini-chart-svg" role="img" aria-label="5m bars">
                      {chartBarsForDisplay.map((b, i) => {
                        const w = 400 / Math.max(chartBarsForDisplay.length, 1)
                        const prices = chartBarsForDisplay.flatMap((x) => [x.high, x.low])
                        const minP = Math.min(...prices)
                        const maxP = Math.max(...prices)
                        const scale = (p) => 110 - ((p - minP) / (maxP - minP || 1)) * 100
                        const x = i * w + w * 0.2
                        const yH = scale(b.high)
                        const yL = scale(b.low)
                        const yO = scale(b.open)
                        const yC = scale(b.close)
                        const up = b.close >= b.open
                        const tsKey = String(b.ts_utc || '').slice(0, 19)
                        const ctx = contextByTs[tsKey]
                        const pats = b.active_patterns || []
                        const obsTerms = b.objective_terms || []
                        const factMarkers = chartLayers.OBJECTIVE_FACTS ? markerLabels(obsTerms, { BAR_FACTS: true, STRUCTURE: chartLayers.STRUCTURE }) : markerLabels(obsTerms, { BAR_FACTS: false, STRUCTURE: chartLayers.STRUCTURE })
                        const patMarkers = chartLayers.PATTERNS ? patternMarkerLabels(pats, { PATTERNS: true }) : ''
                        const actMarkers = chartLayers.ACTIONS
                          ? (b.transition_markers || []).map((f) => contextMarkerLabels({ payload_json: { marker_flags_json: [f] } }, { ACTIONS: true })).join(' ')
                          : chartLayers.CONTEXT
                            ? contextMarkerLabels(ctx, { ACTIONS: true })
                            : ''
                        const simMark = chartLayers.SIMULATED_TRADES && b.simulation_effect && b.simulation_effect !== '—' ? 'SIM' : ''
                        const markers = [simMark, actMarkers, patMarkers, factMarkers].filter(Boolean).join(' ')
                        const highlighted = selectedBarTs && String(b.ts_utc || '').startsWith(String(selectedBarTs).slice(0, 19))
                        return (
                          <g
                            key={`${b.ts_utc}-${i}`}
                            opacity={highlighted ? 1 : 0.92}
                            style={{ cursor: 'pointer' }}
                            onClick={() => selectBar(b.ts_utc, pats[0] || null)}
                          >
                            <line x1={x + w * 0.3} y1={yH} x2={x + w * 0.3} y2={yL} stroke={up ? '#16a34a' : '#dc2626'} strokeWidth={highlighted ? 2 : 1} />
                            <rect x={x} y={Math.min(yO, yC)} width={w * 0.6} height={Math.max(1, Math.abs(yC - yO))} fill={up ? '#16a34a' : '#dc2626'} />
                            {markers ? (
                              <text x={x} y={yH - 4} fontSize="6" fill="#374151">{markers}</text>
                            ) : null}
                          </g>
                        )
                      })}
                    </svg>
                  ) : (
                    <div className="bil-placeholder">No bars revealed yet — Start replay and use Next bar, or switch to full-week review.</div>
                  )}
                </div>
                {chartLayers.DAILY_LEVELS ? (
                  <div className="bil-note bil-levels-summary" role="region" aria-label="Daily levels">
                    {(learningSymbol?.daily_levels?.levels || []).length ? (
                      <ul className="bil-levels-list">
                        {learningSymbol.daily_levels.levels.map((lv) => (
                          <li key={lv.name}>
                            <strong>{lv.name}:</strong>{' '}
                            {lv.value != null ? lv.value : 'Not available'}
                            <span className="bil-level-meta">
                              {' '}· Source: {lv.source || learningSymbol.daily_levels.source || 'frozen daily dossier'}
                              {lv.trading_date || learningSymbol.daily_levels.trading_date
                                ? ` · Trading date: ${lv.trading_date || learningSymbol.daily_levels.trading_date}`
                                : ''}
                            </span>
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p>
                        Levels: Support Not available · Resistance Not available · Reclaim Not available · DNC Not available
                        · daily thesis inv Not available · intraday setup inv Not available
                      </p>
                    )}
                  </div>
                ) : null}
              </div>
              <div>
                <h3 style={{ margin: '0 0 0.5rem', fontSize: '0.95rem' }}>
                  Rolling observation grid
                  {learningSymbol?.grid_total != null ? ` (${learningSymbol.grid_total} rows)` : ''}
                </h3>
                <div className="bil-note bil-grid-filters">
                  <label>
                    <input
                      type="checkbox"
                      checked={learningFilters.action_changed_only}
                      onChange={(e) => setLearningFilters((f) => ({ ...f, action_changed_only: e.target.checked }))}
                    />
                    Show only bars where the action changed
                  </label>
                  <label>
                    <input
                      type="checkbox"
                      checked={learningFilters.meaningful_pattern_no_entry}
                      onChange={(e) => setLearningFilters((f) => ({ ...f, meaningful_pattern_no_entry: e.target.checked }))}
                    />
                    Meaningful long-pattern evidence but no entry progression
                  </label>
                  <label>
                    <input
                      type="checkbox"
                      checked={learningFilters.transition_events_only}
                      onChange={(e) => setLearningFilters((f) => ({ ...f, transition_events_only: e.target.checked }))}
                    />
                    Transition events only
                  </label>
                  <label className="bil-note">
                    Legacy filter{' '}
                    <select value={reviewFilter} onChange={(e) => setReviewFilter(e.target.value)}>
                    <option value="">All bars</option>
                    <option value="inside_bar">Inside bars</option>
                    <option value="outside_bar">Outside bars</option>
                    <option value="doji">Dojis</option>
                    <option value="large_bar">Large bars</option>
                    <option value="possible_breakout">Possible breakouts</option>
                    <option value="possible_follow_through">Possible follow-through</option>
                    <option value="hh_hl">HH + HL</option>
                    <option value="lh_ll">LH + LL</option>
                    <option value="pattern:H1">H1 candidates</option>
                    <option value="pattern:H2">H2 candidates</option>
                    <option value="pattern:DOUBLE_BOTTOM">Double bottom</option>
                    <option value="pattern:MICRO_DOUBLE">Micro double bottom</option>
                    <option value="pattern:WEDGE">Wedge</option>
                    <option value="pattern:TWO_LEG">Two-legged pullback</option>
                    <option value="pattern:STRUCTURAL_BREAKOUT">Breakout pullback</option>
                    <option value="pattern:FAILED_BREAKOUT">Failed breakout</option>
                    <option value="pattern:MICRO_CHANNEL">Micro channel</option>
                    <option value="pattern:CLIMAX">Climax</option>
                  </select>
                  </label>
                </div>
                <div className="bil-table-wrap bil-obs-grid bil-obs-grid--full">
                  <table className="bil-table">
                    <thead>
                      <tr>
                        <th>NY time</th>
                        <th>Berlin</th>
                        <th>UTC</th>
                        <th>OHLC</th>
                        <th>Objective</th>
                        <th>Patterns</th>
                        <th>Thesis</th>
                        <th>State before</th>
                        <th>State after</th>
                        <th>Action</th>
                        <th>Blockers</th>
                        <th>Simulation</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(learningSymbol?.grid_rows || observations).map((row) => {
                        const ts = row.bar_ts || row.bar_ts_utc
                        const ohlcv = row.ohlcv || {}
                        const pats = row.active_patterns || row.pattern_snapshot_json || []
                        const patStr = pats.map((p) => `${p.pattern_family}:${p.lifecycle}`).join('; ')
                        const sel = selectedBarTs && String(ts).slice(0, 19) === String(selectedBarTs).slice(0, 19)
                        const blockers = (row.blocker_display || row.blockers || row.payload_json?.blockers_json || []).join(', ')
                        return (
                          <tr
                            key={`${ts}-${row.symbol || activeSymbol}`}
                            ref={(el) => { gridRowRefs.current[String(ts).slice(0, 19)] = el }}
                            className={sel ? 'bil-obs-row-selected' : ''}
                            onClick={() => selectBar(ts, pats[0] || null)}
                            style={{ cursor: 'pointer' }}
                            title={row.explanation}
                          >
                            <td>{String(row.bar_ts_ny || ts || '').slice(0, 19)}</td>
                            <td>{row.bar_ts_berlin || '—'}</td>
                            <td>{String(ts || '').slice(0, 19)}</td>
                            <td>
                              {ohlcv.close != null
                                ? `${ohlcv.open?.toFixed?.(2) ?? ohlcv.open}/${ohlcv.close?.toFixed?.(2) ?? ohlcv.close}`
                                : '—'}
                            </td>
                            <td>{row.objective_summary || formatTerms(row)}</td>
                            <td>{patStr || '—'}</td>
                            <td>{row.thesis_effect || '—'}</td>
                            <td>{row.state_before || '—'}</td>
                            <td>{row.state_after || '—'}</td>
                            <td>{row.selected_action || '—'}</td>
                            <td title={blockers}>{blockers || '—'}</td>
                            <td>{row.simulation_effect || '—'}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
                {learningSymbol?.state_transitions?.length ? (
                  <div className="bil-timeline bil-note" role="region" aria-label="State transitions">
                    <strong>State transitions</strong>
                    <ul className="bil-timeline-list">
                      {learningSymbol.state_transitions.map((t) => (
                        <li key={`${t.bar_ts}-${t.state_after}`}>
                          <button
                            type="button"
                            className="bil-link-btn"
                            onClick={() => selectBar(t.bar_ts, null)}
                          >
                            {t.time_ny_short || '—'} {t.state_after} → {t.selected_action}
                          </button>
                        </li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                {selectedGridRow?.explanation_sections ? (
                  <div className="bil-explanation-panel bil-note" role="region" aria-label="Bar explanation">
                    <strong>Explanation</strong>
                    <p><em>Objective facts:</em> {selectedGridRow.explanation_sections.objective_facts || '—'}</p>
                    <p><em>Advisory:</em> {selectedGridRow.explanation_sections.advisory_conclusion?.explanation || '—'}</p>
                    <p><em>Simulation:</em> {selectedGridRow.explanation_sections.simulation_result || '—'}</p>
                    {selectedGridRow.explanation_sections.zero_trade_note ? (
                      <p>{selectedGridRow.explanation_sections.zero_trade_note}</p>
                    ) : null}
                  </div>
                ) : null}
                {selectedGridRow?.blocker_diagnostics ? (
                  <div className="bil-explanation-panel bil-note" role="region" aria-label="Blocker diagnostics">
                    <strong>Blocker diagnostics</strong>
                    <p>
                      Stored: {selectedGridRow.blocker_diagnostics.stored_blocker || 'Not available'}
                      {' · '}
                      Display: {selectedGridRow.blocker_diagnostics.display_blocker || 'Not available'}
                      {' · '}
                      Room class: {selectedGridRow.blocker_diagnostics.room_class || 'Not available'}
                    </p>
                    <p>
                      Close: {selectedGridRow.blocker_diagnostics.decision_close ?? 'Not available'}
                      {' · '}
                      Resistance: {selectedGridRow.blocker_diagnostics.resistance ?? 'Not available'}
                      {' · '}
                      Distance:{' '}
                      {selectedGridRow.blocker_diagnostics.distance != null
                        ? `${Number(selectedGridRow.blocker_diagnostics.distance).toFixed(2)} (${Number(selectedGridRow.blocker_diagnostics.distance_pct || 0).toFixed(2)}%)`
                        : 'Not available'}
                    </p>
                    <p>
                      Session range: {selectedGridRow.blocker_diagnostics.session_range != null
                        ? Number(selectedGridRow.blocker_diagnostics.session_range).toFixed(4)
                        : 'Not available'}
                      {' · '}
                      Room fraction:{' '}
                      {selectedGridRow.blocker_diagnostics.room_fraction != null
                        ? Number(selectedGridRow.blocker_diagnostics.room_fraction).toFixed(4)
                        : 'Not available'}
                      {' · '}
                      Min acceptable: {selectedGridRow.blocker_diagnostics.min_room_acceptable_fraction}
                      {' · '}
                      Min ample: {selectedGridRow.blocker_diagnostics.min_room_ample_fraction ?? 0.55}
                    </p>
                  </div>
                ) : null}
                {patternDetail ? (
                  <div className="bil-pattern-detail bil-note" role="region" aria-label="Pattern detail">
                    <strong>{patternDetail.pattern_family || selectedPattern?.pattern_family}</strong>
                    {' · '}
                    {patternDetail.lifecycle_status || selectedPattern?.lifecycle}
                    <p style={{ margin: '0.35rem 0' }}>{patternDetail.explanation || '—'}</p>
                    {patternDetail.lifecycle_history_json ? (
                      <pre style={{ fontSize: '0.75rem', maxHeight: '120px', overflow: 'auto' }}>
                        {JSON.stringify(patternDetail.lifecycle_history_json, null, 2)}
                      </pre>
                    ) : null}
                  </div>
                ) : null}
              </div>
            </div>
            <div style={{ marginTop: '0.75rem' }}>
              <h3 style={{ margin: '0 0 0.5rem', fontSize: '0.95rem' }}>Frozen PAA dossier</h3>
              {frozenDossier?.dossier_origin === 'HISTORICAL_RECONSTRUCTION' ? (
                <div className="bil-recon-badge" role="status">
                  <strong>RECONSTRUCTED HISTORICAL DOSSIER</strong>
                  <p>Historical reconstruction — PAA did not run on this date.</p>
                  <p className="bil-note">
                    This dossier was reconstructed after the fact using only data available before the simulated session.
                    It is not an original historical PAA audit record.
                  </p>
                  <p><strong>Latest daily bar used:</strong> {frozenDossier.latest_daily_bar_used || '—'}</p>
                  <p><strong>Point-in-time cutoff:</strong> {frozenDossier.daily_data_cutoff_date || '—'}</p>
                  <p><strong>Reconstruction:</strong> {frozenDossier.reconstruction_version || '—'} · {frozenDossier.reconstruction_id?.slice(0, 12)}…</p>
                  <p><strong>Daily bar hash:</strong> {frozenDossier.source_daily_bar_hash?.slice(0, 16)}…</p>
                </div>
              ) : null}
              {frozenDossier?.error ? (
                <p className="bil-error">{frozenDossier.error}</p>
              ) : frozenDossier ? (
                <div className="bil-dossier-panel">
                  <p><strong>Verdict:</strong> {frozenDossier.paa_verdict} ({frozenDossier.paa_confidence})</p>
                  <p><strong>Trend / location:</strong> {frozenDossier.daily_trend || '—'} · {frozenDossier.location || '—'}</p>
                  <p><strong>Summary:</strong> {frozenDossier.methodologist_summary || '—'}</p>
                  <p><strong>Invalidation:</strong> {frozenDossier.invalidation_level ?? '—'}
                    {frozenDossier.derived_levels?.invalidation_level?.source_type
                      ? ` (${frozenDossier.derived_levels.invalidation_level.source_type})` : ''}
                  </p>
                  <p><strong>Reclaim:</strong> {frozenDossier.reclaim_level ?? '—'}</p>
                  <p><strong>Do not chase:</strong> {frozenDossier.do_not_chase_level ?? '—'}</p>
                  <p><strong>Observation ready:</strong> {frozenDossier.observation_ready ? 'Yes' : 'No'}
                    {' · '}
                    <strong>Simulation ready:</strong> {frozenDossier.trade_simulation_ready ? 'Yes' : 'No'}
                  </p>
                  {latestContext ? (
                    <div className="bil-note" style={{ marginTop: '0.75rem', padding: '0.5rem', border: '1px solid #e5e7eb' }}>
                      <p><strong>Phase 6 context ({activeSymbol}):</strong> {latestContext.state_after} → {latestContext.selected_action}</p>
                      <p><strong>Thesis effect:</strong> {latestContext.thesis_effect}</p>
                      <p><strong>Explanation:</strong> {latestContext.explanation}</p>
                      <p><strong>Setup invalidation:</strong> {latestContext.payload_json?.intraday_setup_invalidation ?? '—'}
                        {' · '}
                        <strong>Daily thesis inv:</strong> {latestContext.payload_json?.daily_thesis_invalidation ?? frozenDossier.invalidation_level ?? '—'}
                      </p>
                    </div>
                  ) : null}
                  {(frozenDossier.validation_messages || []).map((m) => (
                    <p key={m} className="bil-note">{m}</p>
                  ))}
                  {levelDistanceWarning ? (
                    <p className="bil-note bil-level-warning">{levelDistanceWarning}</p>
                  ) : null}
                  <p className="bil-note">
                    PAA {frozenDossier.paa_analysis_id?.slice(0, 8)}… · {frozenDossier.compiler_version} · {frozenDossier.dossier_version}
                  </p>
                </div>
              ) : (
                <div className="bil-placeholder">Select a matrix row after preparation to view the frozen dossier.</div>
              )}
            </div>
          </section>
        </>
      ) : null}
      {showCertModal && certification ? (
        <div className="bil-modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="bil-cert-title">
          <div className="bil-modal bil-modal--wide">
            <h3 id="bil-cert-title">{certification.label}</h3>
            <p className="bil-note">
              {certification.scenarios_passed}/{certification.scenarios_total} scenarios passed
              {certification.all_pass ? ' · ALL PASS' : ''}
            </p>
            <p className="bil-note">Fixture hash: <code>{certification.fixture_sequence_hash?.slice(0, 24)}…</code></p>
            <p className="bil-note">Historical simulation attempt: <code>{certification.historical_simulation_attempt_id}</code></p>
            <p className="bil-note">{certification.persistence_isolation}</p>
            <ul className="bil-note">
              {(certification.must_not || []).map((m) => (
                <li key={m}>Must not: {m}</li>
              ))}
            </ul>
            <button type="button" className="primary" onClick={() => setShowCertModal(false)}>Close</button>
          </div>
        </div>
      ) : null}
      {missingTsModal ? (
        <div className="bil-modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="bil-missing-ts-title">
          <div className="bil-modal">
            <h3 id="bil-missing-ts-title">
              Missing timestamps — {missingTsModal.symbol} · {missingTsModal.trading_date}
            </h3>
            <ul className="bil-missing-ts-list">
              {(missingTsModal.missing_timestamps || []).map((ts) => (
                <li key={ts}>{ts}</li>
              ))}
            </ul>
            {(missingTsModal.missing_timestamps || []).length === 0 ? (
              <p className="bil-note">No missing timestamp list on file (count: {missingTsModal.missing_timestamps_count ?? 0}).</p>
            ) : null}
            <button type="button" className="primary" onClick={() => setMissingTsModal(null)}>Close</button>
          </div>
        </div>
      ) : null}
    </div>
  )
}
