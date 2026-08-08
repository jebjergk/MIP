import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { API_BASE } from '../config/apiBase'
import BrooksIntradayLearningChart from './BrooksIntradayLearningChart'
import BrooksIntradayLearningHeader from './BrooksIntradayLearningHeader'
import BrooksIntradayAdviserV1Learning from './BrooksIntradayAdviserV1Learning'
import BrooksIntradayWhatIsHappening from './BrooksIntradayWhatIsHappening'
import {
  adjacentBarTimestamp,
  barSessionMeta,
  isBarInVisibleWindow,
  resolveChartBars,
} from './brooksLearningSessionNav'
import {
  barTsFromSearchParams,
  findNarrativeForBar,
  normalizeBarTs,
  scrollChartBarIntoView,
  scrollRowIntoView,
  syncBarTsToUrl,
} from './brooksLearningBarSelection'
import {
  tradeLearningG1Url,
} from './brooksTradeLearningG1'
import {
  ADVISER_EMPTY_STATE_DEFAULT,
} from './brooksLabDiagnosticMode'
import './BrooksIntradayLab.css'

const API = `${API_BASE}/research/brooks-intraday`

function chartHeightForViewport() {
  if (typeof window === 'undefined') return 580
  return window.innerWidth < 768 ? Math.max(360, Math.min(420, window.innerHeight * 0.45)) : 580
}

export default function BrooksIntradayLearningView({
  review,
  selectedBarTs: controlledBarTs,
  onSelectedBarTsChange,
  onOpenTechnicalView,
  compactV1 = false,
}) {
  const [searchParams] = useSearchParams()
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [chartFit, setChartFit] = useState('trade')
  const [chartHeight, setChartHeight] = useState(chartHeightForViewport)
  const [selectedBarTs, setSelectedBarTsState] = useState(() => controlledBarTs
    || barTsFromSearchParams(
      typeof window !== 'undefined' ? new URLSearchParams(window.location.search) : null,
    ))

  const reviewKey = review
    ? `${review.runId}|${review.contextAttemptId}|${review.simulationAttemptId}|${review.symbol}|${review.tradingDate}|${review.tradeId || ''}|${review.reviewMode || 'trades'}`
    : ''

  const gridScrollRef = useRef(null)
  const chartScrollRef = useRef(null)
  const rowRefs = useRef({})
  const candleRefs = useRef({})
  const workspaceRef = useRef(null)

  const params = review || {}
  const adviserNotRun = !params.contextAttemptId && !params.simulationAttemptId

  const isV1 = Boolean(payload?.adviser_v1_learning)
  const allBars = payload?.chart?.bars || []
  const summary = isV1 ? payload?.session_summary : payload?.trade_summary

  const selectBar = useCallback((ts, { expandSession = false } = {}) => {
    const key = normalizeBarTs(ts)
    if (!key) return
    if (expandSession || (chartFit === 'trade' && summary && !isBarInVisibleWindow(allBars, 'trade', summary, key))) {
      setChartFit('session')
    }
    setSelectedBarTsState(key)
    if (onSelectedBarTsChange) onSelectedBarTsChange(key)
    else syncBarTsToUrl(key)
  }, [allBars, chartFit, summary, onSelectedBarTsChange])

  useEffect(() => {
    const onResize = () => setChartHeight(chartHeightForViewport())
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])

  const load = useCallback(async () => {
    if (!params.contextAttemptId || !params.simulationAttemptId) {
      setPayload(null)
      setError('')
      setLoading(false)
      return
    }
    setLoading(true)
    setError('')
    try {
      const url = tradeLearningG1Url(API, {
        runId: params.runId,
        contextAttemptId: params.contextAttemptId,
        simulationAttemptId: params.simulationAttemptId,
        symbol: params.symbol,
        tradingDate: params.tradingDate,
      })
      const resp = await fetch(url)
      if (!resp.ok) {
        const text = await resp.text()
        throw new Error(text || `HTTP ${resp.status}`)
      }
      setPayload(await resp.json())
    } catch (e) {
      setPayload(null)
      setError(e.message || 'Failed to load learning review')
    } finally {
      setLoading(false)
    }
  }, [params.runId, params.contextAttemptId, params.simulationAttemptId, params.symbol, params.tradingDate])

  useEffect(() => {
    setPayload(null)
    setSelectedBarTsState(controlledBarTs || null)
    setChartFit('trade')
  }, [reviewKey])

  useEffect(() => {
    if (controlledBarTs != null && controlledBarTs !== selectedBarTs) {
      setSelectedBarTsState(controlledBarTs)
    }
  }, [controlledBarTs, selectedBarTs])

  useEffect(() => {
    load()
  }, [load])

  useEffect(() => {
    if (onSelectedBarTsChange) return
    const fromUrl = barTsFromSearchParams(searchParams)
    if (fromUrl && fromUrl !== selectedBarTs) {
      selectBar(fromUrl, { expandSession: true })
    }
  }, [searchParams, selectedBarTs, selectBar, onSelectedBarTsChange])

  useEffect(() => {
    if (!payload || selectedBarTs) return
    if (isV1) {
      const firstCall = (payload.adviser_evidence_by_call || [])[0]
      if (firstCall?.bar_ts) selectBar(firstCall.bar_ts, { expandSession: true })
      return
    }
    const entry = payload.trade_summary?.entry_ts
    if (entry) selectBar(entry)
  }, [payload, selectedBarTs, selectBar, isV1])

  const { bars: visibleBars, windowLabel, autoExpanded } = useMemo(
    () => resolveChartBars(allBars, chartFit, summary, selectedBarTs),
    [allBars, chartFit, summary, selectedBarTs],
  )

  const selectedKey = normalizeBarTs(selectedBarTs)
  const sessionMeta = useMemo(
    () => (selectedKey ? barSessionMeta(allBars, selectedKey) : null),
    [allBars, selectedKey],
  )

  const narrative = useMemo(
    () => findNarrativeForBar(payload, selectedKey),
    [payload, selectedKey],
  )

  const stepBar = useCallback((delta) => {
    const next = adjacentBarTimestamp(allBars, selectedKey, delta)
    if (next) selectBar(next)
  }, [allBars, selectedKey, selectBar])

  useEffect(() => {
    const onKey = (e) => {
      if (!payload || e.target?.tagName === 'INPUT' || e.target?.tagName === 'TEXTAREA') return
      if (e.key === 'ArrowLeft') {
        e.preventDefault()
        stepBar(-1)
      } else if (e.key === 'ArrowRight') {
        e.preventDefault()
        stepBar(1)
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [payload, stepBar])

  useEffect(() => {
    if (!selectedKey) return
    const id = requestAnimationFrame(() => {
      scrollRowIntoView(rowRefs.current[selectedKey], gridScrollRef.current)
      scrollChartBarIntoView(candleRefs.current[selectedKey], chartScrollRef.current)
    })
    return () => cancelAnimationFrame(id)
  }, [selectedKey, payload, chartFit, visibleBars.length, autoExpanded])

  const registerRowRef = useCallback((ts, el) => {
    const key = normalizeBarTs(ts)
    if (key) rowRefs.current[key] = el
  }, [])

  const registerCandleRef = useCallback((ts, el) => {
    const key = normalizeBarTs(ts)
    if (key) candleRefs.current[key] = el
  }, [])

  const header = payload?.header
  const grid = isV1 ? (payload?.observation_grid || []) : (payload?.educational_grid || [])
  const overlays = payload?.chart?.overlays

  const canStepPrev = adjacentBarTimestamp(allBars, selectedKey, -1) != null
  const canStepNext = adjacentBarTimestamp(allBars, selectedKey, 1) != null

  return (
    <div className={`bil-learning-view bil-learning-view--g22${compactV1 ? ' bil-learning-view--v1-compact' : ''}`} ref={workspaceRef}>
      {!compactV1 ? (
      <BrooksIntradayLearningHeader
        header={header}
        summary={summary}
        noTrade={false}
        symbol={params.symbol}
        tradingDate={params.tradingDate}
        onOpenTechnicalView={onOpenTechnicalView}
      />
      ) : null}

      {adviserNotRun && !loading ? (
        <p className="bil-note bil-adviser-empty" role="status">{ADVISER_EMPTY_STATE_DEFAULT}</p>
      ) : null}

      {loading ? <p className="bil-note">Loading learning review…</p> : null}
      {error ? (
        <div className="bil-error">
          <p>{error}</p>
          <p className="bil-note">Load run <code>{params.runId}</code> on the Technical View, then return here.</p>
          <button type="button" onClick={load}>Retry</button>
        </div>
      ) : null}

      {!loading && !error && !adviserNotRun && payload ? (
        isV1 ? (
          <BrooksIntradayAdviserV1Learning
            payload={payload}
            selectedBarTs={selectedKey}
            onSelectBar={(ts) => selectBar(ts, { expandSession: true })}
            registerRowRef={registerRowRef}
            registerCandleRef={registerCandleRef}
            chartHeight={chartHeight}
            visibleBars={visibleBars}
            allBars={allBars}
            chartScrollRef={chartScrollRef}
            gridScrollRef={gridScrollRef}
          />
        ) : (
        <>
          <div className="bil-learning-workspace">
            <div className="bil-learning-workspace-chart">
              <div className="bil-learning-chart-toolbar">
                <h2 className="bil-learning-section-label">5-minute session chart</h2>
                <div className="bil-learning-chart-controls">
                  <button
                    type="button"
                    className={chartFit === 'session' ? 'bil-chart-fit--active' : undefined}
                    onClick={() => setChartFit('session')}
                  >
                    Fit session
                  </button>
                  <button
                    type="button"
                    className={chartFit === 'trade' ? 'bil-chart-fit--active' : undefined}
                    onClick={() => setChartFit('trade')}
                  >
                    Fit trade
                  </button>
                  <button type="button" onClick={() => setChartFit('trade')} title="Reset to fit trade">
                    Reset view
                  </button>
                </div>
              </div>
              <p className="bil-note bil-learning-window-label">
                {windowLabel}
                {autoExpanded ? ' · expanded to show selected bar' : ''}
                {' · '}
                {visibleBars.length}
                {' bars shown'}
              </p>
              <div className="bil-learning-bar-nav">
                <button type="button" disabled={!canStepPrev} onClick={() => stepBar(-1)}>Previous bar</button>
                <button type="button" disabled={!canStepNext} onClick={() => stepBar(1)}>Next bar</button>
                <button
                  type="button"
                  onClick={() => summary?.entry_ts && selectBar(summary.entry_ts)}
                >
                  Jump to entry
                </button>
                <button
                  type="button"
                  onClick={() => summary?.exit_ts && selectBar(summary.exit_ts)}
                >
                  Jump to exit
                </button>
                {sessionMeta ? (
                  <span className="bil-learning-bar-pos">
                    Bar
                    {' '}
                    {sessionMeta.index}
                    {' of '}
                    {sessionMeta.total}
                    {' · '}
                    {sessionMeta.timeNy}
                    {' NY'}
                  </span>
                ) : null}
              </div>
              <div className="bil-learning-chart-scroll" ref={chartScrollRef}>
                <BrooksIntradayLearningChart
                  bars={visibleBars}
                  overlays={overlays}
                  selectedTs={selectedKey}
                  onSelectBar={(ts) => selectBar(ts, { expandSession: false })}
                  registerCandleRef={registerCandleRef}
                  chartHeight={chartHeight}
                  tradeSummary={summary}
                />
              </div>
            </div>

            <BrooksIntradayWhatIsHappening
              narrative={narrative}
              payloadSymbol={payload.symbol}
            />
          </div>

          <section className="bil-learning-grid-section">
            <h2 className="bil-learning-section-label">Session walkthrough</h2>
            <div className="bil-learning-grid-scroll" ref={gridScrollRef}>
              <table className="bil-learning-grid">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Market story</th>
                    <th>Important signal</th>
                    <th>System view</th>
                    <th>Action</th>
                    <th>Position</th>
                    <th>Stop</th>
                    <th>Why</th>
                  </tr>
                </thead>
                <tbody>
                  {grid.map((row) => {
                    const rowKey = normalizeBarTs(row.bar_ts)
                    const sel = selectedKey && rowKey === selectedKey
                    return (
                      <tr
                        key={row.bar_ts}
                        ref={(el) => registerRowRef(row.bar_ts, el)}
                        className={sel ? 'bil-learning-grid-row--selected' : undefined}
                        onClick={() => selectBar(row.bar_ts, { expandSession: true })}
                      >
                        <td>{row.time_ny || row.bar_ts_ny?.slice(11, 16)}</td>
                        <td>{row.market_story}</td>
                        <td>{row.important_signal}</td>
                        <td>{row.system_view}</td>
                        <td>{row.action}</td>
                        <td>{row.position}</td>
                        <td>{row.stop}</td>
                        <td>{row.why}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </section>

          <details className="bil-learning-tech-details">
            <summary>Technical details</summary>
            <pre>{JSON.stringify(payload.technical_details, null, 2)}</pre>
          </details>
        </>
        )
      ) : null}
    </div>
  )
}
