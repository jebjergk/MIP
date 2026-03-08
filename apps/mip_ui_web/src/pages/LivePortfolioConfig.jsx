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

function normalizePortfolioId(v) {
  return String(v ?? '').replace(/[^\d]/g, '')
}

function statusClass(status) {
  if (status === 'ok') return 'lpc-status-ok'
  if (status === 'bad') return 'lpc-status-bad'
  return 'lpc-status-na'
}

export default function LivePortfolioConfig() {
  const [configs, setConfigs] = useState([])
  const [simPortfolios, setSimPortfolios] = useState([])
  const [selectedPortfolioId, setSelectedPortfolioId] = useState('')
  const [editorMode, setEditorMode] = useState('edit')
  const [draftPortfolioId, setDraftPortfolioId] = useState('')
  const [showAdvancedGuards, setShowAdvancedGuards] = useState(false)
  const [status, setStatus] = useState({ loading: true, error: '', ok: '' })
  const [guard, setGuard] = useState(null)
  const [smokeResult, setSmokeResult] = useState(null)
  const [lastRefreshAt, setLastRefreshAt] = useState('')
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
    setStatus((s) => ({ ...s, loading: true, error: '' }))
    try {
      const [cfgResp, simResp] = await Promise.all([
        fetch(`${API_BASE}/live/portfolio-config`),
        fetch(`${API_BASE}/portfolios`),
      ])
      if (!cfgResp.ok) throw new Error(`Failed to load configs (${cfgResp.status})`)
      const data = await cfgResp.json()
      const rows = data.configs || []
      setConfigs(rows)
      if (simResp.ok) {
        const simRows = await simResp.json()
        setSimPortfolios(Array.isArray(simRows) ? simRows : [])
      } else {
        setSimPortfolios([])
      }
      setLastRefreshAt(new Date().toLocaleString())
      if (!selectedPortfolioId && rows.length > 0) {
        setSelectedPortfolioId(String(rows[0].PORTFOLIO_ID))
      }
      setStatus((s) => ({ ...s, loading: false, error: '' }))
    } catch (e) {
      setStatus((s) => ({ ...s, loading: false, error: e.message || 'Failed to load configs.' }))
    }
  }, [selectedPortfolioId])

  useEffect(() => {
    loadConfigs()
  }, [loadConfigs])

  const selectedConfig = useMemo(
    () => (editorMode === 'create'
      ? null
      : configs.find((c) => String(c.PORTFOLIO_ID) === String(selectedPortfolioId)) || null),
    [configs, editorMode, selectedPortfolioId]
  )
  const normalizedSelectedPortfolioId = useMemo(() => normalizePortfolioId(selectedPortfolioId), [selectedPortfolioId])
  const normalizedDraftPortfolioId = useMemo(() => normalizePortfolioId(draftPortfolioId), [draftPortfolioId])
  const activePortfolioId = editorMode === 'create' ? normalizedDraftPortfolioId : normalizedSelectedPortfolioId

  const loadGuard = useCallback(async () => {
    if (!activePortfolioId || editorMode === 'create') {
      setGuard(null)
      return
    }
    try {
      const resp = await fetch(`${API_BASE}/live/activation/guard?portfolio_id=${encodeURIComponent(activePortfolioId)}`)
      if (!resp.ok) throw new Error(`Guard check failed (${resp.status})`)
      const data = await resp.json()
      setGuard(data)
    } catch (e) {
      setGuard({ error: e.message || 'Failed to load guard.' })
    }
  }, [activePortfolioId, editorMode])

  const refreshAll = useCallback(async () => {
    await loadConfigs()
    await loadGuard()
    setStatus((s) => ({ ...s, ok: 'Refreshed config list and guard status.' }))
  }, [loadConfigs, loadGuard])

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

  const buildPayload = useCallback(() => ({
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
  }), [form])

  const createNewConfig = useCallback(async () => {
    if (editorMode === 'create') {
      setStatus((s) => ({ ...s, ok: `Already in create mode for Live Config #${normalizedDraftPortfolioId || 'new'}. Click Save Config to persist.` }))
      return
    }
    const nextId = Math.max(0, ...configs.map((c) => Number(c.PORTFOLIO_ID) || 0)) + 1
    setEditorMode('create')
    setDraftPortfolioId(String(nextId))
    setGuard(null)
    setShowAdvancedGuards(false)
    setForm((prev) => ({
      ...prev,
      sim_portfolio_id: prev.sim_portfolio_id || '',
      ibkr_account_id: prev.ibkr_account_id || '',
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
    }))
    setStatus((s) => ({
      ...s,
      error: '',
      ok: `Create mode started for Live Config #${nextId}. Nothing is saved until you click Save Config.`,
    }))
  }, [configs, editorMode, normalizedDraftPortfolioId])

  const cancelCreateMode = useCallback(() => {
    setEditorMode('edit')
    setDraftPortfolioId('')
    setShowAdvancedGuards(false)
    setStatus((s) => ({ ...s, error: '', ok: 'Create mode cancelled. No new config was saved.' }))
  }, [])

  const onSave = useCallback(async () => {
    const portfolioIdNum = Number(activePortfolioId)
    if (!activePortfolioId || !Number.isInteger(portfolioIdNum) || portfolioIdNum <= 0) {
      setStatus((s) => ({ ...s, error: 'Select an existing Live Config first, or click Create New Live Config.', ok: '' }))
      return
    }
    if (editorMode === 'create' && !form.ibkr_account_id) {
      setStatus((s) => ({ ...s, error: 'IBKR Account ID is required when creating a new config.', ok: '' }))
      return
    }
    if (editorMode === 'create' && !form.sim_portfolio_id) {
      setStatus((s) => ({ ...s, error: 'Select a SIM Portfolio before saving a new live config.', ok: '' }))
      return
    }
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const payload = buildPayload()
      const resp = await fetch(`${API_BASE}/live/portfolio-config/${portfolioIdNum}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!resp.ok) {
        const txt = await resp.text()
        throw new Error(txt || `Save failed (${resp.status})`)
      }
      const saveData = await resp.json()
      const savedCfg = saveData?.config || null
      if (savedCfg) {
        setConfigs((prev) => {
          const others = prev.filter((c) => String(c.PORTFOLIO_ID) !== String(savedCfg.PORTFOLIO_ID))
          return [...others, savedCfg].sort((a, b) => Number(a.PORTFOLIO_ID) - Number(b.PORTFOLIO_ID))
        })
      }
      if (editorMode === 'create') {
        setSelectedPortfolioId(String(portfolioIdNum))
        setDraftPortfolioId('')
        setEditorMode('edit')
      }
      await loadConfigs()
      await loadGuard()
      setStatus({
        loading: false,
        error: '',
        ok: editorMode === 'create'
          ? `Created and saved Live Config #${portfolioIdNum}.`
          : `Saved config for Live Portfolio ID ${portfolioIdNum}.`,
      })
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to save config.', ok: '' }))
    }
  }, [activePortfolioId, buildPayload, editorMode, form.ibkr_account_id, form.sim_portfolio_id, loadConfigs, loadGuard])

  const mapStatus = useMemo(() => {
    const hasLiveId = Boolean(activePortfolioId)
    const hasSim = Boolean(form.sim_portfolio_id)
    const hasIbkr = Boolean(form.ibkr_account_id)
    const hasSavedConfig = Boolean(selectedConfig)
    const guardKnown = Boolean(guard && !guard.error && hasLiveId)
    const guardPass = Boolean(guardKnown && guard.eligible)
    const guardFail = Boolean(guardKnown && !guard.eligible)

    return {
      live: hasLiveId ? 'ok' : 'na',
      sim: hasSim ? 'ok' : 'na',
      ibkr: hasIbkr ? 'ok' : hasLiveId ? 'bad' : 'na',
      persisted: hasSavedConfig ? 'ok' : hasLiveId ? 'bad' : 'na',
      guard: guardPass ? 'ok' : guardFail ? 'bad' : 'na',
      readiness: hasLiveId && hasSim && hasIbkr && hasSavedConfig && guardPass ? 'ok' : 'bad',
      linkLiveSim: hasLiveId && hasSim ? 'ok' : 'na',
      linkLiveIbkr: hasLiveId && hasIbkr ? 'ok' : hasLiveId ? 'bad' : 'na',
      linkLiveGuard: guardPass ? 'ok' : guardFail ? 'bad' : 'na',
      linkGuardReady: guardPass ? 'ok' : guardFail ? 'bad' : 'na',
    }
  }, [activePortfolioId, form.ibkr_account_id, form.sim_portfolio_id, guard, selectedConfig])

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
    if (editorMode === 'create') {
      setStatus((s) => ({ ...s, error: 'Save the new config first before changing activation mode.', ok: '' }))
      return
    }
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
  }, [editorMode, selectedPortfolioId, loadConfigs, loadGuard])

  const disableLive = useCallback(async () => {
    if (editorMode === 'create') {
      setStatus((s) => ({ ...s, error: 'Save the new config first before changing activation mode.', ok: '' }))
      return
    }
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
  }, [editorMode, selectedPortfolioId, loadConfigs, loadGuard])

  const deleteCurrentConfig = useCallback(async () => {
    if (editorMode === 'create') {
      setStatus((s) => ({ ...s, error: 'Cancel or save create mode first.', ok: '' }))
      return
    }
    if (!selectedPortfolioId) {
      setStatus((s) => ({ ...s, error: 'Select a live config first.', ok: '' }))
      return
    }
    const ok = window.confirm(
      `Delete Live Portfolio Link #${selectedPortfolioId}?\n\nThis removes only the config row. ` +
      `If linked live actions/orders exist, deletion will be blocked unless forced.`
    )
    if (!ok) return

    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const resp = await fetch(`${API_BASE}/live/portfolio-config/${selectedPortfolioId}`, { method: 'DELETE' })
      if (!resp.ok) {
        const txt = await resp.text()
        throw new Error(txt || `Delete failed (${resp.status})`)
      }
      await loadConfigs()
      setSelectedPortfolioId('')
      setGuard(null)
      setStatus((s) => ({ ...s, ok: `Deleted Live Portfolio Link #${selectedPortfolioId}.` }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to delete live config.', ok: '' }))
    }
  }, [editorMode, loadConfigs, selectedPortfolioId])

  return (
    <div className="page lpc-page">
      <div className="lpc-header">
        <div>
          <h2>Live Portfolio Link</h2>
          <p className="lpc-subtitle">Bind MIP live container to research + IBKR with explicit readiness gates.</p>
        </div>
      </div>

      <div className="lpc-workflow">
        <div className="lpc-step"><b>1</b><span>Select existing config or create a new one (ID auto-generated)</span></div>
        <div className="lpc-step"><b>2</b><span>Connect SIM portfolio and IBKR account</span></div>
        <div className="lpc-step"><b>3</b><span>Set execution risk + freshness controls</span></div>
        <div className="lpc-step"><b>4</b><span>Save and verify guard turns eligible</span></div>
      </div>

      <div className="lpc-top-row">
        <label className="lpc-top-control">
          Live Config (system-created ID)
          <select
            value={editorMode === 'create' ? '' : selectedPortfolioId}
            onChange={(e) => {
              setSelectedPortfolioId(e.target.value)
              setEditorMode('edit')
              setDraftPortfolioId('')
            }}
          >
            <option value="">{configs.length ? 'Select existing config' : 'No saved configs yet'}</option>
            {configs.map((c) => (
              <option key={c.PORTFOLIO_ID} value={String(c.PORTFOLIO_ID)}>
                #{c.PORTFOLIO_ID} - {c.IBKR_ACCOUNT_ID || 'no-account'}
              </option>
            ))}
          </select>
          <span className="lpc-hint">IDs are assigned by system when you click Create New Live Config.</span>
        </label>
        <div className="lpc-top-actions">
          <button className="lpc-btn lpc-btn-primary" onClick={createNewConfig} disabled={status.loading}>
            {editorMode === 'create' ? 'Create Mode Active' : 'Create New Live Config'}
          </button>
          {editorMode === 'create' ? (
            <button className="lpc-btn" onClick={cancelCreateMode} disabled={status.loading}>
              Cancel Create
            </button>
          ) : null}
          <button className="lpc-btn" onClick={refreshAll} disabled={status.loading}>
            {status.loading ? 'Loading...' : 'Refresh'}
          </button>
        </div>
      </div>

      {status.error ? <div className="lpc-msg lpc-msg-error">{status.error}</div> : null}
      {status.ok ? <div className="lpc-msg lpc-msg-ok">{status.ok}</div> : null}

      <div className="lpc-map-card">
        <div className="lpc-map-head">
          <h3>Connection Schematic</h3>
          <div className="lpc-legend">
            <span><i className="lpc-dot lpc-dot-ok" /> Connected</span>
            <span><i className="lpc-dot lpc-dot-bad" /> Blocking / missing</span>
            <span><i className="lpc-dot lpc-dot-na" /> Not configured</span>
          </div>
        </div>
        <div className="lpc-map-grid">
          <div className={`lpc-node ${statusClass(mapStatus.sim)}`}>
            <h4>SIM Portfolio</h4>
            <p>{form.sim_portfolio_id || 'Not linked'}</p>
          </div>
          <div className={`lpc-link ${statusClass(mapStatus.linkLiveSim)}`} />
          <div className={`lpc-node ${statusClass(mapStatus.live)}`}>
            <h4>MIP Live Portfolio</h4>
            <p>
              {activePortfolioId
                ? (editorMode === 'create' ? `#${activePortfolioId} (draft)` : `#${activePortfolioId}`)
                : 'Not selected'}
            </p>
          </div>
          <div className={`lpc-link ${statusClass(mapStatus.linkLiveIbkr)}`} />
          <div className={`lpc-node ${statusClass(mapStatus.ibkr)}`}>
            <h4>IBKR Account</h4>
            <p>{form.ibkr_account_id || 'Not linked'}</p>
          </div>

          <div className="lpc-spacer" />
          <div className="lpc-spacer" />
          <div className={`lpc-link-vertical ${statusClass(mapStatus.linkLiveGuard)}`} />
          <div className="lpc-spacer" />
          <div className="lpc-spacer" />

          <div className="lpc-spacer" />
          <div className="lpc-spacer" />
          <div className={`lpc-node ${statusClass(mapStatus.guard)}`}>
            <h4>Activation Guard</h4>
            <p>{guard?.eligible ? 'Eligible' : (guard?.error ? 'Guard error' : 'Not eligible')}</p>
          </div>
          <div className="lpc-spacer" />
          <div className="lpc-spacer" />

          <div className="lpc-spacer" />
          <div className="lpc-spacer" />
          <div className={`lpc-link-vertical ${statusClass(mapStatus.linkGuardReady)}`} />
          <div className="lpc-spacer" />
          <div className="lpc-spacer" />

          <div className="lpc-spacer" />
          <div className="lpc-spacer" />
          <div className={`lpc-node ${statusClass(mapStatus.readiness)}`}>
            <h4>Execution Readiness</h4>
            <p>{mapStatus.readiness === 'ok' ? 'Ready to proceed' : 'Not ready'}</p>
          </div>
          <div className="lpc-spacer" />
          <div className="lpc-spacer" />
        </div>
      </div>

      <div className="lpc-panels">
        <div className="lpc-guide">
          <h3>Guard Details</h3>
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

        <div className="lpc-guide">
          <h3>Persistence & Correlation</h3>
          <p>
            <b>Save Config</b> writes to <code>MIP.LIVE.LIVE_PORTFOLIO_CONFIG</code> for this Live Portfolio ID.
          </p>
          <ul>
            <li><b>PORTFOLIO_ID</b>: MIP live execution container.</li>
            <li><b>SIM_PORTFOLIO_ID</b>: research source for proposal import.</li>
            <li><b>IBKR_ACCOUNT_ID</b>: broker truth account mirrored by snapshots.</li>
          </ul>
          <p>
            <b>Link contract:</b> SIM linkage is read-context only (proposal sourcing). Live execution writes stay in
            live-domain tables and must not mutate research truth tables.
          </p>
          <p className="lpc-faint">
            Saved config: <b>{selectedConfig ? 'Yes' : 'No'}</b>
            {' '}| Config version: <b>{selectedConfig?.CONFIG_VERSION ?? '—'}</b>
            {' '}| Updated: <b>{selectedConfig?.UPDATED_AT || '—'}</b>
            {lastRefreshAt ? <> | Last refresh: <b>{lastRefreshAt}</b></> : null}
          </p>
        </div>
      </div>

      <div className="lpc-form-section">
        <h3>Identity & Linkage</h3>
        <div className="lpc-grid">
        <label>Default Source Portfolio (legacy fallback)
          <select value={form.sim_portfolio_id} onChange={(e) => setForm((v) => ({ ...v, sim_portfolio_id: e.target.value }))}>
            <option value="">{simPortfolios.length ? 'Optional: select fallback source portfolio' : 'No portfolios available'}</option>
            {simPortfolios.map((p) => {
              const pid = p.PORTFOLIO_ID ?? p.portfolio_id
              const name = p.NAME ?? p.name ?? `Portfolio ${pid}`
              return (
                <option key={String(pid)} value={String(pid)}>
                  {pid} - {name}
                </option>
              )
            })}
          </select>
          <span className="lpc-hint">Optional legacy fallback only. Preferred flow: choose source portfolio directly on Live Trades import.</span>
        </label>
        <label>IBKR Account ID<input value={form.ibkr_account_id} onChange={(e) => setForm((v) => ({ ...v, ibkr_account_id: e.target.value }))} /><span className="lpc-hint">Paper/live account code (required for create).</span></label>
        <label>Adapter Mode
          <select value={form.adapter_mode} onChange={(e) => setForm((v) => ({ ...v, adapter_mode: e.target.value }))}>
            <option value="PAPER">PAPER</option>
            <option value="LIVE">LIVE</option>
          </select>
          <span className="lpc-hint">Policy mode for this live container; guarded by activation checks.</span>
        </label>
        <label>Base Currency<input value={form.base_currency} onChange={(e) => setForm((v) => ({ ...v, base_currency: e.target.value.toUpperCase() }))} /><span className="lpc-hint">Reference currency for sizing/risk values.</span></label>
        </div>
      </div>

      <div className="lpc-guide">
        <h3>Live Guardrails vs SIM Settings</h3>
        <p>
          Linking a SIM portfolio provides <b>proposal source</b>. The controls below are <b>live execution guardrails</b>
          for broker-side safety (freshness, sizing limits, drawdown brakes). They are separate on purpose.
        </p>
        <label className="lpc-toggle-row">
          <input
            type="checkbox"
            checked={showAdvancedGuards}
            onChange={(e) => setShowAdvancedGuards(e.target.checked)}
          />
          <span>Show advanced live guardrail controls</span>
        </label>
      </div>

      {showAdvancedGuards ? (
        <>
          <div className="lpc-form-section">
            <h3>Sizing & Risk Limits</h3>
            <div className="lpc-grid">
            <label>Max Positions<input value={form.max_positions} onChange={(e) => setForm((v) => ({ ...v, max_positions: e.target.value }))} /><span className="lpc-hint">Hard cap on concurrently open positions.</span></label>
            <label>Max Position %<input value={form.max_position_pct} onChange={(e) => setForm((v) => ({ ...v, max_position_pct: e.target.value }))} /><span className="lpc-hint">Per-position size cap (% of equity).</span></label>
            <label>Cash Buffer %<input value={form.cash_buffer_pct} onChange={(e) => setForm((v) => ({ ...v, cash_buffer_pct: e.target.value }))} /><span className="lpc-hint">Minimum reserve cash before submit.</span></label>
            <label>Max Slippage %<input value={form.max_slippage_pct} onChange={(e) => setForm((v) => ({ ...v, max_slippage_pct: e.target.value }))} /><span className="lpc-hint">Max tolerated slippage on execution checks.</span></label>
            <label>Drawdown Stop %<input value={form.drawdown_stop_pct} onChange={(e) => setForm((v) => ({ ...v, drawdown_stop_pct: e.target.value }))} /><span className="lpc-hint">Portfolio-level drawdown kill-switch threshold.</span></label>
            <label>Bust %<input value={form.bust_pct} onChange={(e) => setForm((v) => ({ ...v, bust_pct: e.target.value }))} /><span className="lpc-hint">Emergency safety threshold for catastrophic loss.</span></label>
            </div>
          </div>

          <div className="lpc-form-section">
            <h3>Freshness & Lifecycle Controls</h3>
            <div className="lpc-grid">
            <label>Validity Window Sec<input value={form.validity_window_sec} onChange={(e) => setForm((v) => ({ ...v, validity_window_sec: e.target.value }))} /><span className="lpc-hint">How long candidate remains valid before expiry.</span></label>
            <label>Quote Freshness Sec<input value={form.quote_freshness_threshold_sec} onChange={(e) => setForm((v) => ({ ...v, quote_freshness_threshold_sec: e.target.value }))} /><span className="lpc-hint">Max age of market data used for checks.</span></label>
            <label>Snapshot Freshness Sec<input value={form.snapshot_freshness_threshold_sec} onChange={(e) => setForm((v) => ({ ...v, snapshot_freshness_threshold_sec: e.target.value }))} /><span className="lpc-hint">Max age of broker snapshot allowed before block.</span></label>
            <label>Cooldown Bars<input value={form.cooldown_bars} onChange={(e) => setForm((v) => ({ ...v, cooldown_bars: e.target.value }))} /><span className="lpc-hint">Bars to wait after exit before re-entry.</span></label>
            <label className="lpc-checkbox">Active
              <input
                type="checkbox"
                checked={Boolean(form.is_active)}
                onChange={(e) => setForm((v) => ({ ...v, is_active: e.target.checked }))}
              />
            </label>
            </div>
          </div>
        </>
      ) : (
        <div className="lpc-guide">
          <p>
            Advanced live guardrail fields are hidden. MIP will keep existing guardrail values
            (or defaults for brand-new configs).
          </p>
        </div>
      )}

      <div className="lpc-actions">
        <button className="lpc-btn lpc-btn-primary" onClick={onSave} disabled={!activePortfolioId || status.loading}>
          Save Config
        </button>
        <button className="lpc-btn" onClick={() => enableLive(false)} disabled={!selectedPortfolioId || editorMode === 'create' || status.loading}>
          Enable LIVE (Guarded)
        </button>
        <button className="lpc-btn" onClick={() => enableLive(true)} disabled={!selectedPortfolioId || editorMode === 'create' || status.loading}>
          Force Enable LIVE
        </button>
        <button className="lpc-btn" onClick={disableLive} disabled={!selectedPortfolioId || editorMode === 'create' || status.loading}>
          Disable LIVE (Rollback)
        </button>
        <button className="lpc-btn" onClick={runPhaseGateSmoke} disabled={status.loading}>
          Run Phase-Gate Smoke
        </button>
        <button className="lpc-btn lpc-btn-danger" onClick={deleteCurrentConfig} disabled={!selectedPortfolioId || editorMode === 'create' || status.loading}>
          Delete Live Link
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
