import React, { useState, useEffect, useCallback } from 'react'

const RANGES = [
  { label: '30d', value: '30' },
  { label: '60d', value: '60' },
  { label: '90d', value: '90' },
  { label: '6mo', value: '180' },
  { label: '1yr', value: '365' },
]

const OVERLAY_DEFS = [
  { key: 'setupMarkers', label: 'Setup markers' },
  { key: 'proposalMarkers', label: 'Proposal markers' },
  { key: 'tradeMarkers', label: 'Trade markers' },
  { key: 'stateStrip', label: 'State strip' },
  { key: 'levels', label: 'S/R levels' },
  { key: 'zones', label: 'S/R zones' },
  { key: 'regimeStrip', label: 'Regime strip' },
  { key: 'entryZones', label: 'Entry zones' },
  { key: 'invalidationLines', label: 'Invalidation lines' },
]

export default function StlSymbolControls({
  symbolList, symbol, setSymbol,
  marketType, setMarketType,
  dateRange, setDateRange,
  chartMode, setChartMode,
  dirFilter, setDirFilter,
  familyFilter, setFamilyFilter,
  familyOptions,
  overlays, toggleOverlay,
  get,
}) {
  const [input, setInput] = useState(symbol)

  useEffect(() => { setInput(symbol) }, [symbol])

  const handleSubmit = useCallback(() => {
    const val = input.trim().toUpperCase()
    if (val && val !== symbol) setSymbol(val)
  }, [input, symbol, setSymbol])

  const handleKeyDown = useCallback((e) => {
    if (e.key === 'Enter') handleSubmit()
  }, [handleSubmit])

  return (
    <>
      <div className="stl-controls">
        <label>Market</label>
        <select value={marketType} onChange={e => setMarketType(e.target.value)}>
          <option value="STOCK">Stock</option>
          <option value="FX">FX</option>
        </select>

        <div className="stl-sep" />

        <label>Symbol</label>
        <input
          type="text"
          value={input}
          onChange={e => setInput(e.target.value.toUpperCase())}
          onKeyDown={handleKeyDown}
          onBlur={handleSubmit}
          placeholder="Type symbol…"
          list="stl-sym-list"
          style={{ width: 100, textTransform: 'uppercase' }}
        />
        <datalist id="stl-sym-list">
          {symbolList.slice(0, 30).map(s => (
            <option key={get(s, 'SYMBOL')} value={get(s, 'SYMBOL')} />
          ))}
        </datalist>

        <div className="stl-sep" />

        {RANGES.map(r => (
          <button
            key={r.value}
            className={`stl-range-btn${dateRange === r.value ? ' stl-range-btn--active' : ''}`}
            onClick={() => setDateRange(r.value)}
          >{r.label}</button>
        ))}

        <div className="stl-sep" />

        <button
          className={`stl-chart-toggle${chartMode === 'line' ? ' stl-chart-toggle--active' : ''}`}
          onClick={() => setChartMode('line')}
        >Line</button>
        <button
          className={`stl-chart-toggle${chartMode === 'candle' ? ' stl-chart-toggle--active' : ''}`}
          onClick={() => setChartMode('candle')}
        >Candle</button>

        <div className="stl-sep" />

        <label>Direction</label>
        <select value={dirFilter} onChange={e => setDirFilter(e.target.value)}>
          <option value="">All</option>
          <option value="LONG">Long</option>
          <option value="SHORT">Short</option>
        </select>

        <label>Family</label>
        <select value={familyFilter} onChange={e => setFamilyFilter(e.target.value)}>
          <option value="">All</option>
          {familyOptions.map(f => <option key={f} value={f}>{f.replace(/_/g, ' ')}</option>)}
        </select>
      </div>

      <div className="stl-overlays">
        {OVERLAY_DEFS.map(o => (
          <span
            key={o.key}
            className={`stl-overlay-chip${overlays[o.key] ? ' stl-overlay-chip--on' : ''}`}
            onClick={() => toggleOverlay(o.key)}
          >
            {overlays[o.key] ? '●' : '○'} {o.label}
          </span>
        ))}
      </div>
    </>
  )
}
