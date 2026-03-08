import { useCallback, useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../App'
import './LivePortfolioConfig.css'

function pctToRatio(v) {
  if (v === '' || v == null) return null
  const n = Number(v)
  if (!Number.isFinite(n)) return null
  return n / 100
}

function ratioToPct(v) {
  if (v == null) return ''
  const n = Number(v) * 100
  if (!Number.isFinite(n)) return ''
  return String(n)
}

export default function LivePortfolioConfig() {
  const [configs, setConfigs] = useState([])
  const [selectedPortfolioId, setSelectedPortfolioId] = useState('')
  const [status, setStatus] = useState({ loading: true, error: '', ok: '' })
  const [guard, setGuard] = useState(null)
  const [smokeResult, setSmokeResult] = useState(null)
  const [form, setForm] = useState({
    sim_portfolio_id: '',
    ibkr_account_id: '',
    adapter_mode: 'PAPER',
    base_currency: 'EUR',
    max_positions: '',
    max_position_pct: '',
    cash_buffer_pct: '',
    max_slippage_pct: '',
    drawdown_stop_pct: '',
    bust_pct: '',
    validity_window_sec: '',
    quote_freshness_threshold_sec: '',
    snapshot_freshness_threshold_sec: '',
    cooldown_bars: '',
    is_active: true,
  })

  const loadConfigs = useCallback(async () => {
    setStatus({ loading: true, error: '', ok: '' })
    try {
      const resp = await fetch(`${API_BASE}/live/portfolio-config`)
      if (!resp.ok) throw new Error(`Failed to load configs (${resp.status})`)
      const data = await resp.json()
      const rows = data.configs || []
      setConfigs(rows)
      if (!selectedPortfolioId && rows.length > 0) {
        setSelectedPortfolioId(String(rows[0].PORTFOLIO_ID))
      }
      setStatus({ loading: false, error: '', ok: '' })
    } catch (e) {
      setStatus({ loading: false, error: e.message || 'Failed to load configs.', ok: '' })
    }
  }, [selectedPortfolioId])

  useEffect(() => {
    loadConfigs()
  }, [loadConfigs])

  const selectedConfig = useMemo(
    () => configs.find((c) => String(c.PORTFOLIO_ID) === String(selectedPortfolioId)) || null,
    [configs, selectedPortfolioId]
  )

  const loadGuard = useCallback(async () => {
    if (!selectedPortfolioId) {
      setGuard(null)
      return
    }
    try {
      const resp = await fetch(`${API_BASE}/live/activation/guard?portfolio_id=${encodeURIComponent(selectedPortfolioId)}`)
      if (!resp.ok) throw new Error(`Guard check failed (${resp.status})`)
      const data = await resp.json()
      setGuard(data)
    } catch (e) {
      setGuard({ error: e.message || 'Failed to load guard.' })
    }
  }, [selectedPortfolioId])

  useEffect(() => {
    if (!selectedConfig) return
    setForm({
      sim_portfolio_id: selectedConfig.SIM_PORTFOLIO_ID ?? '',
      ibkr_account_id: selectedConfig.IBKR_ACCOUNT_ID ?? '',
      adapter_mode: selectedConfig.ADAPTER_MODE ?? 'PAPER',
      base_currency: selectedConfig.BASE_CURRENCY ?? 'EUR',
      max_positions: selectedConfig.MAX_POSITIONS ?? '',
      max_position_pct: ratioToPct(selectedConfig.MAX_POSITION_PCT),
      cash_buffer_pct: ratioToPct(selectedConfig.CASH_BUFFER_PCT),
      max_slippage_pct: ratioToPct(selectedConfig.MAX_SLIPPAGE_PCT),
      drawdown_stop_pct: ratioToPct(selectedConfig.DRAWDOWN_STOP_PCT),
      bust_pct: ratioToPct(selectedConfig.BUST_PCT),
      validity_window_sec: selectedConfig.VALIDITY_WINDOW_SEC ?? '',
      quote_freshness_threshold_sec: selectedConfig.QUOTE_FRESHNESS_THRESHOLD_SEC ?? '',
      snapshot_freshness_threshold_sec: selectedConfig.SNAPSHOT_FRESHNESS_THRESHOLD_SEC ?? '',
      cooldown_bars: selectedConfig.COOLDOWN_BARS ?? '',
      is_active: selectedConfig.IS_ACTIVE ?? true,
    })
  }, [selectedConfig])

  useEffect(() => {
    loadGuard()
  }, [loadGuard])

  const onSave = useCallback(async () => {
    if (!selectedPortfolioId) {
      setStatus((s) => ({ ...s, error: 'Portfolio ID is required.' }))
      return
    }
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const payload = {
        sim_portfolio_id: form.sim_portfolio_id === '' ? null : Number(form.sim_portfolio_id),
        ibkr_account_id: form.ibkr_account_id || null,
        adapter_mode: form.adapter_mode || null,
        base_currency: form.base_currency || null,
        max_positions: form.max_positions === '' ? null : Number(form.max_positions),
        max_position_pct: pctToRatio(form.max_position_pct),
        cash_buffer_pct: pctToRatio(form.cash_buffer_pct),
        max_slippage_pct: pctToRatio(form.max_slippage_pct),
        drawdown_stop_pct: pctToRatio(form.drawdown_stop_pct),
        bust_pct: pctToRatio(form.bust_pct),
        validity_window_sec: form.validity_window_sec === '' ? null : Number(form.validity_window_sec),
        quote_freshness_threshold_sec: form.quote_freshness_threshold_sec === '' ? null : Number(form.quote_freshness_threshold_sec),
        snapshot_freshness_threshold_sec: form.snapshot_freshness_threshold_sec === '' ? null : Number(form.snapshot_freshness_threshold_sec),
        cooldown_bars: form.cooldown_bars === '' ? null : Number(form.cooldown_bars),
        is_active: Boolean(form.is_active),
      }
      const resp = await fetch(`${API_BASE}/live/portfolio-config/${selectedPortfolioId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!resp.ok) {
        const txt = await resp.text()
        throw new Error(txt || `Save failed (${resp.status})`)
      }
      await loadConfigs()
      await loadGuard()
      setStatus({ loading: false, error: '', ok: 'Saved live portfolio config.' })
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to save config.', ok: '' }))
    }
  }, [form, loadConfigs, loadGuard, selectedPortfolioId])

  const runPhaseGateSmoke = useCallback(async () => {
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    setSmokeResult(null)
    try {
      const resp = await fetch(`${API_BASE}/live/smoke/phase-gate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phase: 'phase6_7', include_db_checks: true, include_write_checks: false }),
      })
      if (!resp.ok) {
        const txt = await resp.text()
        throw new Error(txt || `Phase smoke failed (${resp.status})`)
      }
      const data = await resp.json()
      setSmokeResult(data)
      setStatus((s) => ({ ...s, ok: data.ok ? 'Phase-gate smoke passed.' : 'Phase-gate smoke reported failures.' }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to run phase-gate smoke.' }))
    }
  }, [])

  const enableLive = useCallback(async (force = false) => {
    if (!selectedPortfolioId) return
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const resp = await fetch(`${API_BASE}/live/activation/enable`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ portfolio_id: Number(selectedPortfolioId), actor: 'ui_operator', force }),
      })
      if (!resp.ok) {
        const txt = await resp.text()
        throw new Error(txt || `Enable failed (${resp.status})`)
      }
      await loadConfigs()
      await loadGuard()
      setStatus((s) => ({ ...s, ok: force ? 'LIVE mode force-enabled.' : 'LIVE mode enabled.' }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to enable LIVE mode.' }))
    }
  }, [selectedPortfolioId, loadConfigs, loadGuard])

  const disableLive = useCallback(async () => {
    if (!selectedPortfolioId) return
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const resp = await fetch(`${API_BASE}/live/activation/disable`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ portfolio_id: Number(selectedPortfolioId), actor: 'ui_operator', reason: 'manual rollback' }),
      })
      if (!resp.ok) {
        const txt = await resp.text()
        throw new Error(txt || `Disable failed (${resp.status})`)
      }
      await loadConfigs()
      await loadGuard()
      setStatus((s) => ({ ...s, ok: 'LIVE mode disabled (rollback to PAPER).' }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to disable LIVE mode.' }))
    }
  }, [selectedPortfolioId, loadConfigs, loadGuard])

  return (
    <div className="page lpc-page">
      <div className="lpc-header">
        <h2>Live Portfolio Config</h2>
        <button className="lpc-btn" onClick={loadConfigs} disabled={status.loading}>
          {status.loading ? 'Loading...' : 'Refresh'}
        </button>
      </div>

      <div className="lpc-guide">
        <h3>What This Page Represents</h3>
        <p>
          <b>Live Portfolio ID</b> is an internal MIP execution container ID, not an IB field.
          It groups approvals, revalidation, risk limits, and audit events for one live workflow.
        </p>
        <ul>
          <li><b>IBKR Account ID</b>: your real broker account (e.g. DU...)</li>
          <li><b>SIM Portfolio ID</b>: optional research twin that provides proposals</li>
          <li><b>Cash/positions truth</b>: always mirrored from IB snapshots, not from simulation tables</li>
          <li><b>Risk fields here</b>: pre-trade execution guards enforced by MIP before submit</li>
        </ul>
      </div>

      <div className="lpc-top-row">
        <label>
          Live Portfolio ID (MIP internal)
          <input
            value={selectedPortfolioId}
            onChange={(e) => setSelectedPortfolioId(e.target.value)}
            placeholder="e.g. 1"
          />
        </label>
        <select value={selectedPortfolioId} onChange={(e) => setSelectedPortfolioId(e.target.value)}>
          <option value="">Select existing config</option>
          {configs.map((c) => (
            <option key={c.PORTFOLIO_ID} value={String(c.PORTFOLIO_ID)}>
              {c.PORTFOLIO_ID} - {c.IBKR_ACCOUNT_ID}
            </option>
          ))}
        </select>
      </div>

      {status.error ? <div className="lpc-msg lpc-msg-error">{status.error}</div> : null}
      {status.ok ? <div className="lpc-msg lpc-msg-ok">{status.ok}</div> : null}

      <div className="lpc-guide">
        <h3>Activation Guard</h3>
        {guard?.error ? (
          <p>{guard.error}</p>
        ) : (
          <>
            <p>
              Eligible: <b>{guard?.eligible ? 'Yes' : 'No'}</b>
              {' '}| Drift status: <b>{guard?.checks?.drift_status ?? '—'}</b>
              {' '}| Snapshot age sec: <b>{guard?.checks?.snapshot_age_sec ?? '—'}</b>
            </p>
            {Array.isArray(guard?.reasons) && guard.reasons.length > 0 ? (
              <ul>
                {guard.reasons.map((r) => (<li key={r}>{r}</li>))}
              </ul>
            ) : (
              <p>No blocking reasons.</p>
            )}
          </>
        )}
      </div>

      <div className="lpc-grid">
        <label>SIM Portfolio ID (research twin)<input value={form.sim_portfolio_id} onChange={(e) => setForm((v) => ({ ...v, sim_portfolio_id: e.target.value }))} /></label>
        <label>IBKR Account ID<input value={form.ibkr_account_id} onChange={(e) => setForm((v) => ({ ...v, ibkr_account_id: e.target.value }))} /></label>
        <label>Adapter Mode
          <select value={form.adapter_mode} onChange={(e) => setForm((v) => ({ ...v, adapter_mode: e.target.value }))}>
            <option value="PAPER">PAPER</option>
            <option value="LIVE">LIVE</option>
          </select>
        </label>
        <label>Base Currency<input value={form.base_currency} onChange={(e) => setForm((v) => ({ ...v, base_currency: e.target.value.toUpperCase() }))} /></label>
        <label>Max Positions<input value={form.max_positions} onChange={(e) => setForm((v) => ({ ...v, max_positions: e.target.value }))} /></label>
        <label>Max Position %<input value={form.max_position_pct} onChange={(e) => setForm((v) => ({ ...v, max_position_pct: e.target.value }))} /></label>
        <label>Cash Buffer %<input value={form.cash_buffer_pct} onChange={(e) => setForm((v) => ({ ...v, cash_buffer_pct: e.target.value }))} /></label>
        <label>Max Slippage %<input value={form.max_slippage_pct} onChange={(e) => setForm((v) => ({ ...v, max_slippage_pct: e.target.value }))} /></label>
        <label>Drawdown Stop %<input value={form.drawdown_stop_pct} onChange={(e) => setForm((v) => ({ ...v, drawdown_stop_pct: e.target.value }))} /></label>
        <label>Bust %<input value={form.bust_pct} onChange={(e) => setForm((v) => ({ ...v, bust_pct: e.target.value }))} /></label>
        <label>Validity Window Sec<input value={form.validity_window_sec} onChange={(e) => setForm((v) => ({ ...v, validity_window_sec: e.target.value }))} /></label>
        <label>Quote Freshness Sec<input value={form.quote_freshness_threshold_sec} onChange={(e) => setForm((v) => ({ ...v, quote_freshness_threshold_sec: e.target.value }))} /></label>
        <label>Snapshot Freshness Sec<input value={form.snapshot_freshness_threshold_sec} onChange={(e) => setForm((v) => ({ ...v, snapshot_freshness_threshold_sec: e.target.value }))} /></label>
        <label>Cooldown Bars<input value={form.cooldown_bars} onChange={(e) => setForm((v) => ({ ...v, cooldown_bars: e.target.value }))} /></label>
        <label className="lpc-checkbox">Active
          <input
            type="checkbox"
            checked={Boolean(form.is_active)}
            onChange={(e) => setForm((v) => ({ ...v, is_active: e.target.checked }))}
          />
        </label>
      </div>

      <div className="lpc-actions">
        <button className="lpc-btn lpc-btn-primary" onClick={onSave} disabled={!selectedPortfolioId || status.loading}>
          Save Config
        </button>
        <button className="lpc-btn" onClick={() => enableLive(false)} disabled={!selectedPortfolioId || status.loading}>
          Enable LIVE (Guarded)
        </button>
        <button className="lpc-btn" onClick={() => enableLive(true)} disabled={!selectedPortfolioId || status.loading}>
          Force Enable LIVE
        </button>
        <button className="lpc-btn" onClick={disableLive} disabled={!selectedPortfolioId || status.loading}>
          Disable LIVE (Rollback)
        </button>
        <button className="lpc-btn" onClick={runPhaseGateSmoke} disabled={status.loading}>
          Run Phase-Gate Smoke
        </button>
      </div>

      {smokeResult ? (
        <div className="lpc-guide">
          <h3>Latest Smoke Result</h3>
          <p>Overall: <b>{smokeResult.ok ? 'PASS' : 'FAIL'}</b> | Checks: <b>{(smokeResult.checks || []).length}</b></p>
        </div>
      ) : null}
    </div>
  )
}
