import { useState, useEffect, useCallback } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { API_BASE } from '../App'
import InfoTooltip from '../components/InfoTooltip'
import EmptyState from '../components/EmptyState'
import ErrorState from '../components/ErrorState'
import LoadingState from '../components/LoadingState'
import { getGlossaryEntry } from '../data/glossary'
import './AuditViewer.css'

const RUNS_PER_PAGE = 15

function StatusBadge({ status, showTooltip = false }) {
  const statusBadgeTitle = showTooltip ? getGlossaryEntry('ui', 'status_badge')?.long : undefined

  const statusClass = status?.toUpperCase()?.includes('FAIL') || status?.toUpperCase()?.includes('ERROR')
    ? 'status-badge--error'
    : status?.toUpperCase()?.includes('SKIP') || status?.toUpperCase()?.includes('RUNNING')
    ? 'status-badge--warning'
    : status?.toUpperCase()?.includes('SUCCESS')
    ? 'status-badge--success'
    : ''

  return (
    <span className={`status-badge ${statusClass}`} title={statusBadgeTitle}>
      {status ?? '—'}
    </span>
  )
}

function CopyButton({ text, label = 'Copy' }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch (err) {
      console.error('Failed to copy:', err)
    }
  }, [text])

  return (
    <button
      className="copy-button"
      onClick={handleCopy}
      title={copied ? 'Copied!' : `Copy ${label}`}
    >
      {copied ? '✓ Copied' : label}
    </button>
  )
}

function RunFilters({ filters, setFilters, onSearch, pipelineMode }) {
  const statusOptions = pipelineMode === 'intraday'
    ? [
        { value: '', label: 'All statuses' },
        { value: 'FAIL', label: 'Failed' },
        { value: 'SUCCESS', label: 'Success' },
        { value: 'PARTIAL', label: 'Partial' },
        { value: 'SKIPPED_DISABLED', label: 'Skipped (disabled)' },
      ]
    : [
        { value: '', label: 'All statuses' },
        { value: 'FAIL', label: 'Failed' },
        { value: 'SUCCESS', label: 'Success' },
        { value: 'SUCCESS_WITH_SKIPS', label: 'Success with skips' },
        { value: 'RUNNING', label: 'Running' },
      ]

  return (
    <div className="run-filters">
      <div className="run-filters__row">
        <select
          value={filters.status}
          onChange={(e) => setFilters({ ...filters, status: e.target.value })}
          className="run-filters__select"
          title="Filter runs by their final status"
        >
          {statusOptions.map((o) => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>
      </div>
      <div className="run-filters__row">
        <input
          type="date"
          value={filters.fromDate}
          onChange={(e) => setFilters({ ...filters, fromDate: e.target.value })}
          className="run-filters__date"
          title="Filter runs started after this date"
        />
        <input
          type="date"
          value={filters.toDate}
          onChange={(e) => setFilters({ ...filters, toDate: e.target.value })}
          className="run-filters__date"
          title="Filter runs started before this date"
        />
      </div>
      <button onClick={onSearch} className="run-filters__search">
        Apply Filters
      </button>
    </div>
  )
}

function RunListItem({ run, isSelected, onClick, pipelineMode }) {
  const formatTime = (ts) => {
    if (!ts) return '—'
    const d = new Date(ts)
    return d.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })
  }

  return (
    <div
      className={`run-list-item ${isSelected ? 'run-list-item--selected' : ''} ${run.has_errors ? 'run-list-item--error' : ''}`}
      onClick={onClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onClick(); } }}
      title="Click to view run details"
    >
      <div className="run-list-item__header">
        <StatusBadge status={run.status} showTooltip />
        <span className="run-list-item__time">{formatTime(run.started_at)}</span>
      </div>
      <div className="run-list-item__id" title={run.run_id}>
        {run.run_id?.slice(0, 8)}...
      </div>
      {pipelineMode === 'intraday' && (
        <div className="run-list-item__metrics">
          {run.bars_ingested != null && <span>{run.bars_ingested} bars</span>}
          {run.signals_generated != null && <span>{run.signals_generated} signals</span>}
        </div>
      )}
      {run.summary_hint && (
        <div className="run-list-item__hint">{run.summary_hint}</div>
      )}
      {run.error_count > 0 && (
        <div className="run-list-item__errors">
          {run.error_count} error{run.error_count > 1 ? 's' : ''}
        </div>
      )}
    </div>
  )
}

function Pagination({ page, totalPages, onPageChange }) {
  if (totalPages <= 1) return null
  return (
    <div className="run-pagination">
      <button
        className="run-pagination__btn"
        disabled={page <= 1}
        onClick={() => onPageChange(page - 1)}
      >
        Prev
      </button>
      <span className="run-pagination__info">{page} / {totalPages}</span>
      <button
        className="run-pagination__btn"
        disabled={page >= totalPages}
        onClick={() => onPageChange(page + 1)}
      >
        Next
      </button>
    </div>
  )
}

function StepTimeline({ steps, selectedStep, onSelectStep }) {
  if (!steps || steps.length === 0) {
    return <p className="step-timeline__empty">No steps recorded.</p>
  }

  return (
    <div className="step-timeline" title="Pipeline execution steps in order">
      {steps.map((step, i) => {
        const isSelected = selectedStep === i
        const isFailed = step.status?.toUpperCase()?.includes('FAIL') || step.status?.toUpperCase()?.includes('ERROR')
        const isSkipped = step.status?.toUpperCase()?.includes('SKIP')

        return (
          <div
            key={i}
            className={`step-timeline__item ${isSelected ? 'step-timeline__item--selected' : ''} ${isFailed ? 'step-timeline__item--failed' : ''} ${isSkipped ? 'step-timeline__item--skipped' : ''}`}
            onClick={() => onSelectStep(i)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelectStep(i); } }}
          >
            <div className="step-timeline__indicator">
              {isFailed ? '✗' : isSkipped ? '○' : '✓'}
            </div>
            <div className="step-timeline__content">
              <div className="step-timeline__name">{step.step_name || step.event_name}</div>
              <div className="step-timeline__meta">
                <StatusBadge status={step.status} />
                {step.duration_ms != null && (
                  <span className="step-timeline__duration">{(step.duration_ms / 1000).toFixed(1)}s</span>
                )}
                {step.portfolio_id != null && (
                  <span className="step-timeline__portfolio">Portfolio {step.portfolio_id}</span>
                )}
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}

function StepDetail({ step }) {
  if (!step) {
    return (
      <div className="step-detail step-detail--empty">
        <p>Select a step to view details</p>
      </div>
    )
  }

  const isFailed = step.status?.toUpperCase()?.includes('FAIL') || step.status?.toUpperCase()?.includes('ERROR')

  return (
    <div className={`step-detail ${isFailed ? 'step-detail--failed' : ''}`}>
      <h4>{step.step_name || step.event_name}</h4>
      <dl className="step-detail__dl">
        <dt title="Final status of this step">Status</dt>
        <dd><StatusBadge status={step.status} /></dd>

        {step.duration_ms != null && (
          <>
            <dt title="Time taken to execute this step">Duration</dt>
            <dd>{(step.duration_ms / 1000).toFixed(2)}s ({step.duration_ms}ms)</dd>
          </>
        )}

        {step.rows_affected != null && (
          <>
            <dt title="Number of database rows affected">Rows Affected</dt>
            <dd>{step.rows_affected}</dd>
          </>
        )}

        {step.portfolio_id != null && (
          <>
            <dt>Portfolio ID</dt>
            <dd>{step.portfolio_id}</dd>
          </>
        )}

        {step.started_at && (
          <>
            <dt>Started</dt>
            <dd>{step.started_at}</dd>
          </>
        )}

        {step.completed_at && (
          <>
            <dt>Completed</dt>
            <dd>{step.completed_at}</dd>
          </>
        )}
      </dl>

      {isFailed && step.error_message && (
        <div className="step-detail__error">
          <h5>Error Details</h5>
          <div className="step-detail__error-message">
            <code>{step.error_message}</code>
            <CopyButton text={step.error_message} label="Copy Error" />
          </div>
          {step.error_sqlstate && (
            <div className="step-detail__error-meta">
              <span>SQLSTATE: <code>{step.error_sqlstate}</code></span>
              <CopyButton text={step.error_sqlstate} label="Copy" />
            </div>
          )}
          {step.error_query_id && (
            <div className="step-detail__error-meta">
              <span>Query ID: <code>{step.error_query_id}</code></span>
              <CopyButton text={step.error_query_id} label="Copy" />
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function ErrorPanel({ errors, debugSql }) {
  const [showDebugSql, setShowDebugSql] = useState(false)
  const [selectedDebugQuery, setSelectedDebugQuery] = useState('all_events')

  if (!errors || errors.length === 0) {
    return null
  }

  return (
    <div className="error-panel">
      <h3 className="error-panel__title">
        Errors ({errors.length})
        <InfoTooltip scope="audit" entry="error_details" variant="short" />
      </h3>

      {errors.map((error, i) => (
        <div key={i} className="error-panel__item">
          <div className="error-panel__header">
            <span className="error-panel__event">{error.event_name}</span>
            <span className="error-panel__time">{error.event_ts}</span>
          </div>
          <div className="error-panel__message">
            <code>{error.error_message}</code>
            <CopyButton text={error.error_message} label="Copy" />
          </div>
          <div className="error-panel__meta">
            {error.error_sqlstate && (
              <span title="SQL error state code from Snowflake">
                SQLSTATE: <code>{error.error_sqlstate}</code>
              </span>
            )}
            {error.error_query_id && (
              <span title="Snowflake query ID - use to look up in QUERY_HISTORY">
                Query ID: <code>{error.error_query_id}</code>
                <CopyButton text={error.error_query_id} label="Copy" />
              </span>
            )}
          </div>
        </div>
      ))}

      {debugSql && (
        <div className="error-panel__debug">
          <button
            className="error-panel__debug-toggle"
            onClick={() => setShowDebugSql(!showDebugSql)}
          >
            {showDebugSql ? '▼ Hide Debug SQL' : '▶ Show Debug SQL'}
          </button>

          {showDebugSql && (
            <div className="error-panel__debug-content">
              <div className="error-panel__debug-tabs">
                {Object.keys(debugSql).map((key) => (
                  <button
                    key={key}
                    className={`error-panel__debug-tab ${selectedDebugQuery === key ? 'error-panel__debug-tab--active' : ''}`}
                    onClick={() => setSelectedDebugQuery(key)}
                  >
                    {key.replace(/_/g, ' ')}
                  </button>
                ))}
              </div>
              <div className="error-panel__debug-sql">
                <pre>{debugSql[selectedDebugQuery]}</pre>
                <CopyButton text={debugSql[selectedDebugQuery]} label="Copy SQL" />
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function RunSummaryCards({ runDetail }) {
  if (!runDetail) return null

  const timeline = runDetail.timeline || []
  const firstEvent = timeline[0]
  const lastEvent = timeline[timeline.length - 1]

  const startedAt = firstEvent?.EVENT_TS
  const completedAt = lastEvent?.EVENT_TS
  const status = lastEvent?.STATUS
  const totalDurationMs = runDetail.total_duration_ms

  let asOfTs = null
  for (const event of timeline) {
    const details = event.DETAILS
    if (details?.effective_to_ts) {
      asOfTs = details.effective_to_ts
      break
    }
  }

  const portfolioIds = new Set()
  for (const step of runDetail.steps || []) {
    if (step.portfolio_id) {
      portfolioIds.add(step.portfolio_id)
    }
  }

  return (
    <div className="run-summary-cards">
      <div className="run-summary-card" title="Final status of the pipeline run">
        <div className="run-summary-card__label">Status</div>
        <div className="run-summary-card__value">
          <StatusBadge status={status} showTooltip />
        </div>
      </div>

      <div className="run-summary-card" title="Total time from start to finish">
        <div className="run-summary-card__label">Duration</div>
        <div className="run-summary-card__value">
          {totalDurationMs != null ? `${(totalDurationMs / 1000).toFixed(1)}s` : '—'}
        </div>
      </div>

      <div className="run-summary-card" title="Market data timestamp the pipeline processed up to">
        <div className="run-summary-card__label">As-of</div>
        <div className="run-summary-card__value run-summary-card__value--small">
          {asOfTs ? new Date(asOfTs).toLocaleString() : '—'}
        </div>
      </div>

      <div className="run-summary-card" title="Number of portfolios processed">
        <div className="run-summary-card__label">Portfolios</div>
        <div className="run-summary-card__value">
          {portfolioIds.size || '—'}
        </div>
      </div>

      {runDetail.error_count > 0 && (
        <div className="run-summary-card run-summary-card--error" title="Number of failed steps">
          <div className="run-summary-card__label">Errors</div>
          <div className="run-summary-card__value">{runDetail.error_count}</div>
        </div>
      )}
    </div>
  )
}

function IntradaySummaryCards({ runDetail }) {
  if (!runDetail) return null

  const fmtTs = (v) => {
    if (!v) return '—'
    const d = new Date(v)
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  }

  return (
    <div className="run-summary-cards">
      <div className="run-summary-card" title="Final status">
        <div className="run-summary-card__label">Status</div>
        <div className="run-summary-card__value">
          <StatusBadge status={runDetail.status} showTooltip />
        </div>
      </div>
      <div className="run-summary-card" title="Total duration">
        <div className="run-summary-card__label">Duration</div>
        <div className="run-summary-card__value">
          {runDetail.total_duration_ms != null ? `${(runDetail.total_duration_ms / 1000).toFixed(1)}s` : '—'}
        </div>
      </div>
      <div className="run-summary-card" title="Bars ingested in this run">
        <div className="run-summary-card__label">Bars Ingested</div>
        <div className="run-summary-card__value">{runDetail.bars_ingested ?? '—'}</div>
      </div>
      <div className="run-summary-card" title="Signals generated by pattern detectors">
        <div className="run-summary-card__label">Signals</div>
        <div className="run-summary-card__value">{runDetail.signals_generated ?? '—'}</div>
      </div>
      <div className="run-summary-card" title="Outcomes evaluated">
        <div className="run-summary-card__label">Outcomes</div>
        <div className="run-summary-card__value">{runDetail.outcomes_evaluated ?? '—'}</div>
      </div>
      <div className="run-summary-card" title="Symbols processed">
        <div className="run-summary-card__label">Symbols</div>
        <div className="run-summary-card__value">{runDetail.symbols_processed ?? '—'}</div>
      </div>
      <div className="run-summary-card" title="Compute time in seconds">
        <div className="run-summary-card__label">Compute</div>
        <div className="run-summary-card__value">
          {runDetail.compute_seconds != null ? `${Number(runDetail.compute_seconds).toFixed(1)}s` : '—'}
        </div>
      </div>
      {runDetail.has_errors && (
        <div className="run-summary-card run-summary-card--error" title="Run failed">
          <div className="run-summary-card__label">Error</div>
          <div className="run-summary-card__value">1</div>
        </div>
      )}
    </div>
  )
}

function IntradayRunDetail({ runDetail }) {
  if (!runDetail) return null

  const fmtTs = (v) => {
    if (!v) return '—'
    const d = new Date(v)
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' })
  }

  const details = runDetail.details || {}
  const config = details.config || {}
  const ingestResult = details.ingest_result || details.ingest || {}
  const signalResult = details.signal_result || details.signals || {}

  return (
    <div className="intraday-run-detail">
      <section className="intraday-detail-section">
        <h4>Run Info</h4>
        <dl className="intraday-detail-dl">
          <dt>Started</dt>
          <dd>{fmtTs(runDetail.started_at)}</dd>
          <dt>Completed</dt>
          <dd>{fmtTs(runDetail.completed_at)}</dd>
          <dt>Interval</dt>
          <dd>{runDetail.interval_minutes ?? '—'}m</dd>
          <dt>Daily Context</dt>
          <dd>{runDetail.daily_context_used ? 'Yes' : 'No'}</dd>
        </dl>
      </section>

      {Object.keys(config).length > 0 && (
        <section className="intraday-detail-section">
          <h4>Configuration</h4>
          <dl className="intraday-detail-dl">
            {Object.entries(config).map(([k, v]) => (
              <span key={k}>
                <dt>{k}</dt>
                <dd>{String(v)}</dd>
              </span>
            ))}
          </dl>
        </section>
      )}

      {runDetail.error_message && (
        <div className="error-panel" style={{ marginTop: '1rem' }}>
          <h3 className="error-panel__title">Error</h3>
          <div className="error-panel__item">
            <div className="error-panel__message">
              <code>{runDetail.error_message}</code>
              <CopyButton text={runDetail.error_message} label="Copy" />
            </div>
          </div>
        </div>
      )}

      {Object.keys(details).length > 0 && (
        <details className="raw-timeline-section" style={{ marginTop: '1rem' }}>
          <summary>Raw Details (JSON)</summary>
          <div className="error-panel__debug-sql" style={{ marginTop: '0.5rem' }}>
            <pre>{JSON.stringify(details, null, 2)}</pre>
            <CopyButton text={JSON.stringify(details, null, 2)} label="Copy JSON" />
          </div>
        </details>
      )}
    </div>
  )
}


export default function AuditViewer() {
  const { runId: urlRunId } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()

  const [pipelineMode, setPipelineMode] = useState(searchParams.get('pipeline') || 'daily')
  const [runs, setRuns] = useState([])
  const [selectedRunId, setSelectedRunId] = useState(urlRunId || null)
  const [runDetail, setRunDetail] = useState(null)
  const [selectedStep, setSelectedStep] = useState(null)
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)
  const [error, setError] = useState(null)
  const [page, setPage] = useState(1)
  const [filters, setFilters] = useState({
    status: searchParams.get('status') || '',
    fromDate: searchParams.get('from') || '',
    toDate: searchParams.get('to') || '',
  })

  const totalPages = Math.max(1, Math.ceil(runs.length / RUNS_PER_PAGE))
  const pagedRuns = runs.slice((page - 1) * RUNS_PER_PAGE, page * RUNS_PER_PAGE)

  const loadRuns = useCallback(async (mode) => {
    setLoading(true)
    setError(null)
    try {
      const params = new URLSearchParams()
      params.set('limit', '200')
      if (filters.status) params.set('status', filters.status)
      if (filters.fromDate) params.set('from_ts', new Date(filters.fromDate).toISOString())
      if (filters.toDate) params.set('to_ts', new Date(filters.toDate).toISOString())

      const endpoint = mode === 'intraday' ? `${API_BASE}/runs/intraday` : `${API_BASE}/runs`
      const res = await fetch(`${endpoint}?${params}`)
      if (!res.ok) throw new Error(res.statusText)
      const data = await res.json()
      setRuns(data)
      setPage(1)

      if (data.length > 0 && !urlRunId) {
        setSelectedRunId(data[0].run_id)
      }
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [filters, urlRunId])

  const loadRunDetail = useCallback(async (runId, mode) => {
    if (!runId) {
      setRunDetail(null)
      return
    }
    setDetailLoading(true)
    try {
      const endpoint = mode === 'intraday'
        ? `${API_BASE}/runs/intraday/${encodeURIComponent(runId)}`
        : `${API_BASE}/runs/${encodeURIComponent(runId)}`
      const res = await fetch(endpoint)
      if (!res.ok) throw new Error(res.statusText)
      const data = await res.json()
      setRunDetail(data)
      setSelectedStep(null)

      if (mode === 'daily' && data.failed_step) {
        const failedIndex = (data.steps || []).findIndex(
          s => s.event_ts === data.failed_step.event_ts && s.step_name === data.failed_step.step_name
        )
        if (failedIndex >= 0) {
          setSelectedStep(failedIndex)
        }
      }
    } catch (e) {
      console.error('Failed to load run detail:', e)
    } finally {
      setDetailLoading(false)
    }
  }, [])

  useEffect(() => {
    loadRuns(pipelineMode)
  }, [pipelineMode]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    loadRunDetail(selectedRunId, pipelineMode)
  }, [selectedRunId, pipelineMode, loadRunDetail])

  useEffect(() => {
    if (selectedRunId && selectedRunId !== urlRunId) {
      navigate(`/runs/${encodeURIComponent(selectedRunId)}?pipeline=${pipelineMode}`, { replace: true })
    }
  }, [selectedRunId, urlRunId, pipelineMode, navigate])

  const handleSearch = () => {
    const params = new URLSearchParams()
    params.set('pipeline', pipelineMode)
    if (filters.status) params.set('status', filters.status)
    if (filters.fromDate) params.set('from', filters.fromDate)
    if (filters.toDate) params.set('to', filters.toDate)
    setSearchParams(params)
    loadRuns(pipelineMode)
  }

  const handleModeSwitch = (mode) => {
    setPipelineMode(mode)
    setSelectedRunId(null)
    setRunDetail(null)
    setSelectedStep(null)
    setFilters({ status: '', fromDate: '', toDate: '' })
  }

  const handleSelectRun = (runId) => {
    setSelectedRunId(runId)
  }

  if (loading && runs.length === 0) {
    return (
      <div className="run-explorer">
        <h1>Run Explorer</h1>
        <LoadingState />
      </div>
    )
  }

  if (error && runs.length === 0) {
    return (
      <div className="run-explorer">
        <h1>Run Explorer</h1>
        <ErrorState message={error} />
      </div>
    )
  }

  return (
    <div className="run-explorer">
      <h1>Run Explorer <InfoTooltip scope="audit" entry="run_explorer" variant="short" /></h1>

      <div className="run-pipeline-toggle">
        <button
          className={`run-pipeline-btn ${pipelineMode === 'daily' ? 'run-pipeline-btn--active' : ''}`}
          onClick={() => handleModeSwitch('daily')}
        >
          Daily Pipeline
        </button>
        <button
          className={`run-pipeline-btn ${pipelineMode === 'intraday' ? 'run-pipeline-btn--active' : ''}`}
          onClick={() => handleModeSwitch('intraday')}
        >
          Intraday Pipeline
        </button>
      </div>

      <div className="run-explorer__layout">
        {/* Left pane: Run list with filters and pagination */}
        <div className="run-explorer__left">
          <RunFilters
            filters={filters}
            setFilters={setFilters}
            onSearch={handleSearch}
            pipelineMode={pipelineMode}
          />

          <div className="run-list">
            {pagedRuns.length === 0 ? (
              <EmptyState
                title="No runs found"
                action={pipelineMode === 'intraday' ? 'The intraday pipeline runs hourly during market hours.' : 'Adjust filters or run the pipeline.'}
                explanation={pipelineMode === 'intraday'
                  ? 'Intraday pipeline runs appear here after TASK_RUN_INTRADAY_PIPELINE executes.'
                  : 'Pipeline runs appear here after SP_RUN_DAILY_PIPELINE executes.'}
                reasons={['No runs match filters.', 'Pipeline has not run yet.']}
              />
            ) : (
              pagedRuns.map((run) => (
                <RunListItem
                  key={run.run_id}
                  run={run}
                  isSelected={selectedRunId === run.run_id}
                  onClick={() => handleSelectRun(run.run_id)}
                  pipelineMode={pipelineMode}
                />
              ))
            )}
          </div>

          <Pagination page={page} totalPages={totalPages} onPageChange={setPage} />
          <div className="run-list-count">{runs.length} run{runs.length !== 1 ? 's' : ''} total</div>
        </div>

        {/* Right pane: Run detail */}
        <div className="run-explorer__right">
          {detailLoading ? (
            <LoadingState />
          ) : !runDetail ? (
            <div className="run-explorer__empty">
              <p>Select a run to view details</p>
            </div>
          ) : pipelineMode === 'intraday' ? (
            <>
              <div className="run-detail-header">
                <h2 title={selectedRunId}>
                  Intraday Run: {selectedRunId?.slice(0, 12)}...
                  <CopyButton text={selectedRunId} label="Copy ID" />
                </h2>
              </div>
              <IntradaySummaryCards runDetail={runDetail} />
              <IntradayRunDetail runDetail={runDetail} />
            </>
          ) : (
            <>
              <div className="run-detail-header">
                <h2 title={selectedRunId}>
                  Run: {selectedRunId?.slice(0, 12)}...
                  <CopyButton text={selectedRunId} label="Copy ID" />
                </h2>
              </div>
              <RunSummaryCards runDetail={runDetail} />

              {runDetail.run_summary && (
                <section className="run-summary-narrative">
                  <h3>{runDetail.run_summary.headline}</h3>
                  <dl>
                    <dt>What happened</dt>
                    <dd>{runDetail.run_summary.what_happened}</dd>
                    <dt>Why</dt>
                    <dd>{runDetail.run_summary.why}</dd>
                    <dt>Impact</dt>
                    <dd>{runDetail.run_summary.impact}</dd>
                    {runDetail.run_summary.next_check && (
                      <>
                        <dt>Next check</dt>
                        <dd>{runDetail.run_summary.next_check}</dd>
                      </>
                    )}
                  </dl>
                </section>
              )}

              <ErrorPanel
                errors={runDetail.errors}
                debugSql={runDetail.debug_sql}
              />

              <section className="run-steps-section">
                <h3>
                  Step Timeline
                  <InfoTooltip scope="audit" entry="step_timeline" variant="short" />
                </h3>
                <div className="run-steps-layout">
                  <StepTimeline
                    steps={runDetail.steps}
                    selectedStep={selectedStep}
                    onSelectStep={setSelectedStep}
                  />
                  <StepDetail
                    step={selectedStep != null ? runDetail.steps[selectedStep] : null}
                  />
                </div>
              </section>

              <details className="raw-timeline-section">
                <summary>Raw Timeline ({runDetail.timeline?.length || 0} events)</summary>
                <div className="raw-timeline-wrap">
                  <table className="raw-timeline-table">
                    <thead>
                      <tr>
                        <th>Time</th>
                        <th>Type</th>
                        <th>Event</th>
                        <th>Status</th>
                        <th>Duration</th>
                        <th>Error</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(runDetail.timeline || []).slice(0, 100).map((row, i) => {
                        const isFailed = row.STATUS?.toUpperCase()?.includes('FAIL') || row.STATUS?.toUpperCase()?.includes('ERROR')
                        return (
                          <tr key={i} className={isFailed ? 'raw-timeline-row--failed' : ''}>
                            <td>{row.EVENT_TS}</td>
                            <td>{row.EVENT_TYPE}</td>
                            <td>{row.EVENT_NAME}</td>
                            <td><StatusBadge status={row.STATUS} /></td>
                            <td>{row.DURATION_MS != null ? `${row.DURATION_MS}ms` : '—'}</td>
                            <td className="raw-timeline-error">
                              {row.ERROR_MESSAGE ? (
                                <span title={row.ERROR_MESSAGE}>
                                  {row.ERROR_MESSAGE.slice(0, 50)}{row.ERROR_MESSAGE.length > 50 ? '...' : ''}
                                </span>
                              ) : '—'}
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                  {(runDetail.timeline?.length || 0) > 100 && (
                    <p className="raw-timeline-more">Showing first 100 of {runDetail.timeline.length} events.</p>
                  )}
                </div>
              </details>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
