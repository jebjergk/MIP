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
  return Number.isFinite(n) ? String(n) : ''
}

function statusClass(status) {
  if (status === 'ok') return 'lpc-status-ok'
  if (status === 'bad') return 'lpc-status-bad'
  return 'lpc-status-na'
}

const EMPTY_FORM = {
  ibkr_account_id: '',
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
}

export default function LivePortfolioConfig() {
  const [configs, setConfigs] = useState([])
  const [wiringById, setWiringById] = useState({})
  const [view, setView] = useState('list') // list | create | edit
  const [selectedConfigId, setSelectedConfigId] = useState(null)
  const [form, setForm] = useState(EMPTY_FORM)
  const [guard, setGuard] = useState(null)
  const [showAdvancedGuards, setShowAdvancedGuards] = useState(false)
  const [smokeResult, setSmokeResult] = useState(null)
  const [status, setStatus] = useState({ loading: true, error: '', ok: '' })

  const loadConfigs = useCallback(async () => {
    setStatus((s) => ({ ...s, loading: true, error: '' }))
    try {
      const resp = await fetch(`${API_BASE}/live/portfolio-config`)
      if (!resp.ok) throw new Error(`Failed to load live configs (${resp.status})`)
      const data = await resp.json()
      const rows = Array.isArray(data?.configs) ? data.configs : []
      setConfigs(rows)

      const guardPairs = await Promise.all(rows.map(async (row) => {
        try {
          const gResp = await fetch(`${API_BASE}/live/activation/guard?portfolio_id=${encodeURIComponent(row.PORTFOLIO_ID)}`)
          if (!gResp.ok) return [row.PORTFOLIO_ID, { eligible: false, reasons: [`HTTP_${gResp.status}`] }]
          const g = await gResp.json()
          return [row.PORTFOLIO_ID, g]
        } catch {
          return [row.PORTFOLIO_ID, { eligible: false, reasons: ['GUARD_UNAVAILABLE'] }]
        }
      }))
      setWiringById(Object.fromEntries(guardPairs))
      setStatus((s) => ({ ...s, loading: false, error: '' }))
    } catch (e) {
      setStatus((s) => ({ ...s, loading: false, error: e.message || 'Failed to load configs.' }))
    }
  }, [])

  useEffect(() => {
    loadConfigs()
  }, [loadConfigs])

  const selectedConfig = useMemo(
    () => configs.find((c) => Number(c.PORTFOLIO_ID) === Number(selectedConfigId)) || null,
    [configs, selectedConfigId]
  )

  const loadGuard = useCallback(async (portfolioId) => {
    if (!portfolioId) {
      setGuard(null)
      return
    }
    try {
      const resp = await fetch(`${API_BASE}/live/activation/guard?portfolio_id=${encodeURIComponent(portfolioId)}`)
      if (!resp.ok) throw new Error(`Guard check failed (${resp.status})`)
      const data = await resp.json()
      setGuard(data)
    } catch (e) {
      setGuard({ error: e.message || 'Failed to load guard.' })
    }
  }, [])

  const refreshAll = useCallback(async () => {
    await loadConfigs()
    if (view === 'edit' && selectedConfigId) {
      await loadGuard(selectedConfigId)
    }
  }, [loadConfigs, loadGuard, selectedConfigId, view])

  const buildPayload = useCallback(() => ({
    ibkr_account_id: form.ibkr_account_id || null,
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

  const openCreate = useCallback(() => {
    setView('create')
    setSelectedConfigId(null)
    setForm(EMPTY_FORM)
    setGuard(null)
    setShowAdvancedGuards(false)
    setStatus((s) => ({ ...s, error: '', ok: 'Create mode started. ID will be assigned by server on save.' }))
  }, [])

  const openEdit = useCallback(async (config) => {
    setView('edit')
    setSelectedConfigId(Number(config.PORTFOLIO_ID))
    setShowAdvancedGuards(false)
    setForm({
      ibkr_account_id: config.IBKR_ACCOUNT_ID ?? '',
      base_currency: config.BASE_CURRENCY ?? 'EUR',
      max_positions: config.MAX_POSITIONS ?? '',
      max_position_pct: ratioToPct(config.MAX_POSITION_PCT),
      cash_buffer_pct: ratioToPct(config.CASH_BUFFER_PCT),
      max_slippage_pct: ratioToPct(config.MAX_SLIPPAGE_PCT),
      drawdown_stop_pct: ratioToPct(config.DRAWDOWN_STOP_PCT),
      bust_pct: ratioToPct(config.BUST_PCT),
      validity_window_sec: config.VALIDITY_WINDOW_SEC ?? '',
      quote_freshness_threshold_sec: config.QUOTE_FRESHNESS_THRESHOLD_SEC ?? '',
      snapshot_freshness_threshold_sec: config.SNAPSHOT_FRESHNESS_THRESHOLD_SEC ?? '',
      cooldown_bars: config.COOLDOWN_BARS ?? '',
      is_active: config.IS_ACTIVE ?? true,
    })
    await loadGuard(config.PORTFOLIO_ID)
  }, [loadGuard])

  const backToList = useCallback(() => {
    setView('list')
    setSelectedConfigId(null)
    setGuard(null)
    setSmokeResult(null)
  }, [])

  const onSave = useCallback(async () => {
    if (!form.ibkr_account_id) {
      setStatus((s) => ({ ...s, error: 'IBKR Account ID is required.', ok: '' }))
      return
    }
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const payload = buildPayload()
      const isCreate = view === 'create'
      const url = isCreate
        ? `${API_BASE}/live/portfolio-config`
        : `${API_BASE}/live/portfolio-config/${encodeURIComponent(selectedConfigId)}`
      const method = isCreate ? 'POST' : 'PUT'
      const resp = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!resp.ok) throw new Error((await resp.text()) || `Save failed (${resp.status})`)
      const data = await resp.json()
      const newId = Number(data?.portfolio_id || data?.config?.PORTFOLIO_ID || selectedConfigId)
      await loadConfigs()
      setView('edit')
      setSelectedConfigId(newId)
      await loadGuard(newId)
      setStatus((s) => ({ ...s, ok: isCreate ? `Created Live Config #${newId}.` : `Saved Live Config #${newId}.` }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to save config.' }))
    }
  }, [buildPayload, form.ibkr_account_id, loadConfigs, loadGuard, selectedConfigId, view])

  const deleteConfig = useCallback(async (portfolioId) => {
    const yes = window.confirm(`Delete Live Portfolio Link #${portfolioId}?`)
    if (!yes) return
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const resp = await fetch(`${API_BASE}/live/portfolio-config/${portfolioId}`, { method: 'DELETE' })
      if (!resp.ok) throw new Error((await resp.text()) || `Delete failed (${resp.status})`)
      await loadConfigs()
      if (Number(selectedConfigId) === Number(portfolioId)) backToList()
      setStatus((s) => ({ ...s, ok: `Deleted Live Config #${portfolioId}.` }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to delete config.' }))
    }
  }, [backToList, loadConfigs, selectedConfigId])

  const refreshBrokerSnapshot = useCallback(async () => {
    if (!selectedConfigId) return
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const resp = await fetch(
        `${API_BASE}/live/snapshot/refresh?portfolio_id=${encodeURIComponent(selectedConfigId)}`,
        { method: 'POST' }
      )
      if (!resp.ok) {
        throw new Error((await resp.text()) || `Snapshot refresh failed (${resp.status})`)
      }
      await loadConfigs()
      await loadGuard(selectedConfigId)
      setStatus((s) => ({ ...s, ok: 'Broker snapshot refreshed and guard re-evaluated.' }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to refresh broker snapshot.' }))
    }
  }, [loadConfigs, loadGuard, selectedConfigId])

  const enableLive = useCallback(async (force = false) => {
    if (!selectedConfigId) return
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      try {
        await refreshBrokerSnapshot()
      } catch (snapshotErr) {
        setStatus((s) => ({ ...s, ok: `Snapshot refresh warning: ${snapshotErr.message || 'unable to refresh snapshot'}` }))
      }

      const resp = await fetch(`${API_BASE}/live/activation/enable`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ portfolio_id: Number(selectedConfigId), actor: 'ui_operator', force }),
      })
      if (!resp.ok) {
        const raw = await resp.text()
        if (resp.status === 409 && raw.includes('NAV_SNAPSHOT_STALE')) {
          throw new Error(
            'Enable LIVE blocked: NAV snapshot is stale. Connect IBKR and refresh broker snapshot, then retry. ' +
            'Use Force Enable LIVE only if you intentionally bypass guard checks.'
          )
        }
        throw new Error(raw || `Enable failed (${resp.status})`)
      }
      await loadConfigs()
      await loadGuard(selectedConfigId)
      setStatus((s) => ({ ...s, ok: force ? 'LIVE mode force-enabled.' : 'LIVE mode enabled.' }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to enable LIVE mode.' }))
    }
  }, [loadConfigs, loadGuard, refreshBrokerSnapshot, selectedConfigId])

  const disableLive = useCallback(async () => {
    if (!selectedConfigId) return
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const resp = await fetch(`${API_BASE}/live/activation/disable`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ portfolio_id: Number(selectedConfigId), actor: 'ui_operator', reason: 'manual rollback' }),
      })
      if (!resp.ok) throw new Error((await resp.text()) || `Disable failed (${resp.status})`)
      await loadConfigs()
      await loadGuard(selectedConfigId)
      setStatus((s) => ({ ...s, ok: 'LIVE mode disabled (rollback to PAPER).' }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to disable LIVE mode.' }))
    }
  }, [loadConfigs, loadGuard, selectedConfigId])

  const runPhaseGateSmoke = useCallback(async () => {
    setSmokeResult(null)
    setStatus((s) => ({ ...s, error: '', ok: '' }))
    try {
      const resp = await fetch(`${API_BASE}/live/smoke/phase-gate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phase: 'phase6_7', include_db_checks: true, include_write_checks: false }),
      })
      if (!resp.ok) throw new Error((await resp.text()) || `Phase smoke failed (${resp.status})`)
      const data = await resp.json()
      setSmokeResult(data)
      setStatus((s) => ({ ...s, ok: data.ok ? 'Phase-gate smoke passed.' : 'Phase-gate smoke reported failures.' }))
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to run phase-gate smoke.' }))
    }
  }, [])

  const mapStatus = useMemo(() => {
    const hasLive = view === 'edit' && !!selectedConfigId
    const hasIbkr = !!form.ibkr_account_id
    const guardKnown = !!guard && !guard?.error && hasLive
    const guardPass = !!(guardKnown && guard.eligible)
    const guardFail = !!(guardKnown && !guard.eligible)
    return {
      research: 'ok',
      live: hasLive ? 'ok' : (view === 'create' ? 'na' : 'bad'),
      ibkr: hasIbkr ? 'ok' : (view === 'create' || hasLive ? 'bad' : 'na'),
      guard: guardPass ? 'ok' : guardFail ? 'bad' : 'na',
      readiness: hasLive && hasIbkr && guardPass ? 'ok' : 'bad',
      linkResearchLive: hasLive ? 'ok' : 'na',
      linkLiveIbkr: hasIbkr && hasLive ? 'ok' : (hasLive ? 'bad' : 'na'),
      linkLiveGuard: guardPass ? 'ok' : guardFail ? 'bad' : 'na',
      linkGuardReady: guardPass ? 'ok' : guardFail ? 'bad' : 'na',
    }
  }, [form.ibkr_account_id, guard, selectedConfigId, view])

  return (
    <div className="page lpc-page">
      <div className="lpc-header">
        <div>
          <h2>Live Portfolio Link</h2>
          <p className="lpc-subtitle">Cloud-style live config management: list, wiring health, create/edit lifecycle.</p>
        </div>
        <div className="lpc-header-actions">
          <button className="lpc-btn" onClick={refreshAll} disabled={status.loading}>{status.loading ? 'Refreshing...' : 'Refresh'}</button>
          {view === 'list' ? (
            <button className="lpc-btn lpc-btn-primary" onClick={openCreate}>Create New Live Config</button>
          ) : null}
        </div>
      </div>

      {status.error ? <div className="lpc-msg lpc-msg-error">{status.error}</div> : null}
      {status.ok ? <div className="lpc-msg lpc-msg-ok">{status.ok}</div> : null}

      {view === 'list' ? (
        <div className="lpc-guide">
          <h3>Live Configs</h3>
          {!configs.length ? (
            <p>No live configs yet. Create one to bind a live portfolio to IBKR and guardrails.</p>
          ) : (
            <div className="lpc-table-wrap">
              <table className="lpc-table">
                <thead>
                  <tr>
                    <th>Live ID</th>
                    <th>IBKR Account</th>
                    <th>Mode</th>
                    <th>Wired</th>
                    <th>Updated</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {configs.map((cfg) => {
                    const guardInfo = wiringById[cfg.PORTFOLIO_ID]
                    const wired = !!cfg.IBKR_ACCOUNT_ID
                    const guardKnown = guardInfo && !guardInfo?.error
                    let wiredLabel = 'Needs setup'
                    if (wired) wiredLabel = 'Wired'
                    if (wired && guardKnown && guardInfo?.eligible === false) wiredLabel = 'Wired (guard blocked)'
                    return (
                      <tr key={cfg.PORTFOLIO_ID}>
                        <td>#{cfg.PORTFOLIO_ID}</td>
                        <td>{cfg.IBKR_ACCOUNT_ID || '—'}</td>
                        <td>{cfg.ADAPTER_MODE || 'PAPER'}</td>
                        <td>
                          <span className={`lpc-inline-pill ${wired ? 'wired' : 'not-wired'}`}>
                            {wiredLabel}
                          </span>
                        </td>
                        <td>{cfg.UPDATED_AT || '—'}</td>
                        <td className="lpc-row-actions">
                          <button className="lpc-btn" onClick={() => openEdit(cfg)}>Edit</button>
                          <button className="lpc-btn lpc-btn-danger" onClick={() => deleteConfig(cfg.PORTFOLIO_ID)}>Delete</button>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      ) : (
        <>
          <div className="lpc-detail-nav">
            <button className="lpc-btn" onClick={backToList}>Back to list</button>
          </div>
          <div className="lpc-top-row">
            <div className="lpc-top-control">
              <label>
                {view === 'create' ? 'Live Config ID' : 'Editing Live Config'}
                <input value={view === 'create' ? 'Assigned on save' : `#${selectedConfigId}`} disabled />
                <span className="lpc-hint">Research source portfolio is selected in Live Portfolio Activity at import time.</span>
              </label>
            </div>
            <div className="lpc-top-actions" />
          </div>

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
              <div className={`lpc-node ${statusClass(mapStatus.research)}`}>
                <h4>Research Proposals</h4>
                <p>Chosen in Live Portfolio Activity import</p>
              </div>
              <div className={`lpc-link ${statusClass(mapStatus.linkResearchLive)}`} />
              <div className={`lpc-node ${statusClass(mapStatus.live)}`}>
                <h4>MIP Live Portfolio</h4>
                <p>{view === 'edit' ? `#${selectedConfigId}` : 'Draft (not saved)'}</p>
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
                <p>{guard?.eligible ? 'Eligible' : guard?.error ? 'Guard error' : 'Not evaluated'}</p>
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
                <p>{mapStatus.readiness === 'ok' ? 'Ready' : 'Not ready'}</p>
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
                    Mode: <b>{selectedConfig?.ADAPTER_MODE || 'PAPER'}</b>
                    {' '}|{' '}
                    Eligible: <b>{guard?.eligible ? 'Yes' : 'No'}</b>
                    {' '}| Drift: <b>{guard?.checks?.drift_status ?? '—'}</b>
                    {' '}| Snapshot age sec: <b>{guard?.checks?.snapshot_age_sec ?? '—'}</b>
                  </p>
                  {Array.isArray(guard?.reasons) && guard.reasons.length > 0 ? (
                    <ul>{guard.reasons.map((r) => <li key={r}>{r}</li>)}</ul>
                  ) : (
                    <p>No blocking reasons.</p>
                  )}
                </>
              )}
            </div>
          </div>

          <div className="lpc-form-section">
            <h3>Identity & Execution Mode</h3>
            <div className="lpc-grid">
              <label>IBKR Account ID
                <input value={form.ibkr_account_id} onChange={(e) => setForm((v) => ({ ...v, ibkr_account_id: e.target.value }))} />
              </label>
              <label>Adapter Mode (managed)
                <input value={selectedConfig?.ADAPTER_MODE || 'PAPER'} disabled />
                <span className="lpc-hint">Use `Enable LIVE` / `Disable LIVE` to switch mode. This field is not edited manually.</span>
              </label>
              <label>Base Currency
                <input value={form.base_currency} onChange={(e) => setForm((v) => ({ ...v, base_currency: e.target.value.toUpperCase() }))} />
              </label>
              <label className="lpc-checkbox">Active
                <input type="checkbox" checked={Boolean(form.is_active)} onChange={(e) => setForm((v) => ({ ...v, is_active: e.target.checked }))} />
              </label>
            </div>
          </div>

          <div className="lpc-guide">
            <h3>Risk & Guardrails</h3>
            <label className="lpc-toggle-row">
              <input type="checkbox" checked={showAdvancedGuards} onChange={(e) => setShowAdvancedGuards(e.target.checked)} />
              <span>Show advanced guardrail fields</span>
            </label>
          </div>

          {showAdvancedGuards ? (
            <>
              <div className="lpc-form-section">
                <h3>Sizing & Risk Limits</h3>
                <div className="lpc-grid">
                  <label>Max Positions<input value={form.max_positions} onChange={(e) => setForm((v) => ({ ...v, max_positions: e.target.value }))} /></label>
                  <label>Max Position %<input value={form.max_position_pct} onChange={(e) => setForm((v) => ({ ...v, max_position_pct: e.target.value }))} /></label>
                  <label>Cash Buffer %<input value={form.cash_buffer_pct} onChange={(e) => setForm((v) => ({ ...v, cash_buffer_pct: e.target.value }))} /></label>
                  <label>Max Slippage %<input value={form.max_slippage_pct} onChange={(e) => setForm((v) => ({ ...v, max_slippage_pct: e.target.value }))} /></label>
                  <label>Drawdown Stop %<input value={form.drawdown_stop_pct} onChange={(e) => setForm((v) => ({ ...v, drawdown_stop_pct: e.target.value }))} /></label>
                  <label>Bust %<input value={form.bust_pct} onChange={(e) => setForm((v) => ({ ...v, bust_pct: e.target.value }))} /></label>
                </div>
              </div>
              <div className="lpc-form-section">
                <h3>Freshness & Lifecycle</h3>
                <div className="lpc-grid">
                  <label>Validity Window Sec<input value={form.validity_window_sec} onChange={(e) => setForm((v) => ({ ...v, validity_window_sec: e.target.value }))} /></label>
                  <label>Quote Freshness Sec<input value={form.quote_freshness_threshold_sec} onChange={(e) => setForm((v) => ({ ...v, quote_freshness_threshold_sec: e.target.value }))} /></label>
                  <label>Snapshot Freshness Sec<input value={form.snapshot_freshness_threshold_sec} onChange={(e) => setForm((v) => ({ ...v, snapshot_freshness_threshold_sec: e.target.value }))} /></label>
                  <label>Cooldown Bars<input value={form.cooldown_bars} onChange={(e) => setForm((v) => ({ ...v, cooldown_bars: e.target.value }))} /></label>
                </div>
              </div>
            </>
          ) : null}

          <div className="lpc-actions">
            <button className="lpc-btn lpc-btn-primary" onClick={onSave}>Save Config</button>
            {view === 'edit' && (
              <>
                <button className="lpc-btn" onClick={refreshBrokerSnapshot}>Refresh Broker Snapshot</button>
                <button className="lpc-btn" onClick={() => enableLive(false)}>Enable LIVE (Guarded)</button>
                <button className="lpc-btn" onClick={() => enableLive(true)}>Force Enable LIVE</button>
                <button className="lpc-btn" onClick={disableLive}>Disable LIVE</button>
              </>
            )}
            <button className="lpc-btn" onClick={runPhaseGateSmoke}>Run Phase-Gate Smoke</button>
          </div>

          {smokeResult ? (
            <div className="lpc-guide">
              <h3>Latest Smoke Result</h3>
              <p>Overall: <b>{smokeResult.ok ? 'PASS' : 'FAIL'}</b> | Checks: <b>{(smokeResult.checks || []).length}</b></p>
            </div>
          ) : null}
        </>
      )}
    </div>
  )
}
