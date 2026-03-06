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
      setStatus({ loading: false, error: '', ok: 'Saved live portfolio config.' })
    } catch (e) {
      setStatus((s) => ({ ...s, error: e.message || 'Failed to save config.', ok: '' }))
    }
  }, [form, loadConfigs, selectedPortfolioId])

  return (
    <div className="page lpc-page">
      <div className="lpc-header">
        <h2>Live Portfolio Config</h2>
        <button className="lpc-btn" onClick={loadConfigs} disabled={status.loading}>
          {status.loading ? 'Loading...' : 'Refresh'}
        </button>
      </div>

      <div className="lpc-top-row">
        <label>
          Live Portfolio ID
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

      <div className="lpc-grid">
        <label>SIM Portfolio ID<input value={form.sim_portfolio_id} onChange={(e) => setForm((v) => ({ ...v, sim_portfolio_id: e.target.value }))} /></label>
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
      </div>
    </div>
  )
}
