import { useCallback, useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../config/apiBase'
import {
  findV1Session,
  selectionFromV1Session,
  v1SessionKey,
} from './brooksLabReviewSelection'

const API = `${API_BASE}/research/brooks-intraday`
const PILOT = ['AAPL', 'AMZN', 'JPM', 'MCD']

export default function BrooksIntradayValidationLauncher({
  open,
  onClose,
  onCompleted,
  defaultRunId,
}) {
  const [symbol, setSymbol] = useState('MCD')
  const [tradingDate, setTradingDate] = useState('2026-07-14')
  const [barsOk, setBarsOk] = useState(null)
  const [barsCount, setBarsCount] = useState(null)
  const [alreadyValidated, setAlreadyValidated] = useState(false)
  const [existingSessionLabel, setExistingSessionLabel] = useState('')
  const [checking, setChecking] = useState(false)
  const [running, setRunning] = useState(false)
  const [runStartedAt, setRunStartedAt] = useState(null)
  const [runElapsedSec, setRunElapsedSec] = useState(0)
  const [error, setError] = useState('')
  const [config, setConfig] = useState(null)

  useEffect(() => {
    if (!running || runStartedAt == null) return undefined
    const tick = () => setRunElapsedSec(Math.floor((Date.now() - runStartedAt) / 1000))
    tick()
    const id = window.setInterval(tick, 1000)
    return () => window.clearInterval(id)
  }, [running, runStartedAt])

  const runProgressHint = useMemo(() => {
    const n = barsCount || 78
    if (runElapsedSec < 45) return `Starting adviser session (${n} RTH bars)…`
    if (runElapsedSec < 120) return 'Running bar-by-bar adviser + literature retrieval…'
    if (runElapsedSec < 300) return 'Still processing — full sessions often take several minutes.'
    return 'Almost there — finishing simulation and persisting results…'
  }, [barsCount, runElapsedSec])

  const formatElapsed = (sec) => {
    const m = Math.floor(sec / 60)
    const s = sec % 60
    return m > 0 ? `${m}:${String(s).padStart(2, '0')}` : `${s}s`
  }

  useEffect(() => {
    if (!open) return
    setError('')
    fetch(`${API}/validation/v1/config`)
      .then((r) => r.json())
      .then(setConfig)
      .catch(() => setConfig(null))
  }, [open])

  useEffect(() => {
    if (!open || !symbol || !tradingDate) {
      setBarsOk(null)
      setAlreadyValidated(false)
      setExistingSessionLabel('')
      return
    }
    let cancelled = false
    setChecking(true)
    const q = new URLSearchParams({ symbol, trading_date: tradingDate })
    fetch(`${API}/validation/v1/bars-check?${q}`)
      .then((r) => r.json())
      .then((d) => {
        if (cancelled) return
        setBarsOk(Boolean(d.ok))
        setBarsCount(d.rth_bar_count ?? null)
        setAlreadyValidated(Boolean(d.already_validated))
        setExistingSessionLabel(d.existing_session?.selector_label || '')
      })
      .catch(() => {
        if (!cancelled) setBarsOk(false)
      })
      .finally(() => {
        if (!cancelled) setChecking(false)
      })
    return () => { cancelled = true }
  }, [open, symbol, tradingDate])

  const runValidation = useCallback(async () => {
    setRunning(true)
    setRunStartedAt(Date.now())
    setRunElapsedSec(0)
    setError('')
    const recoverIfPersisted = async () => {
      const q = new URLSearchParams({ symbol, trading_date: tradingDate })
      const checkResp = await fetch(`${API}/validation/v1/bars-check?${q}`)
      const check = await checkResp.json().catch(() => ({}))
      const existing = check.existing_session
      if (check.already_validated && existing?.adviser_attempt_id) {
        onCompleted?.({
          symbol: check.symbol || symbol,
          trading_date: check.trading_date || tradingDate,
          adviser_attempt_id: existing.adviser_attempt_id,
          simulation_attempt_id: existing.simulation_attempt_id,
          run_id: defaultRunId,
        })
        onClose?.()
        return true
      }
      return false
    }
    try {
      const resp = await fetch(`${API}/validation/v1/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          symbol,
          trading_date: tradingDate,
          run_id: defaultRunId || undefined,
        }),
      })
      const data = await resp.json().catch(() => ({}))
      if (!resp.ok) {
        const detail = data.detail || data.message
        throw new Error(typeof detail === 'string' ? detail : `HTTP ${resp.status}`)
      }
      onCompleted?.(data)
      onClose?.()
    } catch (e) {
      const msg = e?.message || ''
      const network = msg === 'Failed to fetch' || e?.name === 'TypeError'
      if (network) {
        try {
          if (await recoverIfPersisted()) return
        } catch {
          /* ignore recovery errors */
        }
        setError(
          'Network interrupted while waiting for validation (sessions can take several minutes). '
          + 'If the run finished, pick the session from the dropdown or run bars-check again.',
        )
      } else {
        setError(msg || 'Validation run failed')
      }
    } finally {
      setRunning(false)
      setRunStartedAt(null)
    }
  }, [symbol, tradingDate, defaultRunId, onCompleted, onClose])

  if (!open) return null

  const frozen = config || {
    adviser_version: 'BROOKS_INTRADAY_ADVISER_V1_0',
    starting_cash_usd: 1000,
  }

  return (
    <div
      className="bil-validation-backdrop"
      role="presentation"
      onClick={running ? undefined : onClose}
    >
      <div
        className="bil-validation-dialog"
        role="dialog"
        aria-labelledby="bil-validation-title"
        aria-busy={running}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id="bil-validation-title">New Validation (V1.0)</h2>
        <p className="bil-note">Historical session replay using frozen Adviser V1.0 — no config tuning.</p>

        <label className="bil-review-field">
          <span className="bil-review-field-label">Symbol</span>
          <select value={symbol} onChange={(e) => setSymbol(e.target.value)}>
            {PILOT.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </label>

        <label className="bil-review-field">
          <span className="bil-review-field-label">Trading date</span>
          <input
            type="date"
            value={tradingDate}
            onChange={(e) => setTradingDate(e.target.value)}
          />
        </label>

        <p className="bil-note" role="status">
          {checking ? 'Checking RTH bars…' : null}
          {!checking && alreadyValidated ? (
            <>
              Already validated:
              {' '}
              {existingSessionLabel || `${symbol} · ${tradingDate}`}
              . Open it from the Session dropdown — re-runs are blocked for this symbol/day.
            </>
          ) : null}
          {!checking && !alreadyValidated && barsOk === true ? `5-minute RTH data OK (${barsCount ?? '?' } bars).` : null}
          {!checking && barsOk === false ? 'No RTH bar data for this symbol/date.' : null}
        </p>

        <dl className="bil-validation-frozen">
          <dt>Adviser</dt>
          <dd>{frozen.adviser_version}</dd>
          <dt>Cash</dt>
          <dd>
            $
            {Number(frozen.starting_cash_usd || 1000).toFixed(0)}
            {' · whole shares · long only · one position · 5-minute RTH'}
          </dd>
        </dl>

        {error ? <p className="bil-error">{error}</p> : null}

        {running ? (
          <div className="bil-validation-progress" role="status" aria-live="polite">
            <div className="bil-validation-progress-track">
              <div className="bil-validation-progress-bar" />
            </div>
            <p className="bil-validation-progress-title">
              Validation in progress
              {' · '}
              {formatElapsed(runElapsedSec)}
            </p>
            <p className="bil-note bil-validation-progress-hint">{runProgressHint}</p>
            <p className="bil-note bil-validation-progress-note">
              Leave this window open until complete. Cancel is disabled while the run is active.
            </p>
          </div>
        ) : null}

        <div className="bil-validation-actions">
          <button type="button" onClick={onClose} disabled={running}>Cancel</button>
          <button
            type="button"
            className="primary"
            disabled={running || checking || !barsOk || alreadyValidated}
            onClick={runValidation}
          >
            {running ? 'Running…' : 'Run Validation'}
          </button>
        </div>
      </div>
    </div>
  )
}

export function selectionAfterValidation(catalog, result) {
  const sessions = catalog?.v1_validation_sessions || []
  const match = sessions.find(
    (s) => s.adviser_attempt_id === result.adviser_attempt_id,
  ) || {
    symbol: result.symbol,
    trading_date: result.trading_date,
    adviser_attempt_id: result.adviser_attempt_id,
    simulation_attempt_id: result.simulation_attempt_id,
    run_id: result.run_id,
  }
  return selectionFromV1Session(match, result.run_id)
}

export { findV1Session, v1SessionKey }
