import { useCallback, useMemo } from 'react'
import Plot from 'react-plotly.js'
import {
  buildExpectationForwardSeries,
  buildLivingChartShapesAndTA,
  barTimeMs,
  LIVING_CHART_COLORS,
} from '../../lib/livingChartOverlays'

const UI_REVISION = 'living-chart-v1'

const BASE_LAYOUT = {
  paper_bgcolor: '#0f172a',
  plot_bgcolor: '#0f172a',
  font: { color: '#94a3b8', size: 11 },
  margin: { t: 28, r: 48, b: 48, l: 56 },
  showlegend: true,
  legend: {
    orientation: 'h',
    yanchor: 'bottom',
    y: 1.02,
    x: 0,
    bgcolor: 'rgba(15,23,42,0.85)',
  },
  hovermode: 'x unified',
  dragmode: 'pan',
  xaxis: {
    type: 'date',
    gridcolor: '#1e293b',
    zeroline: false,
    showspikes: true,
    spikemode: 'across',
    spikesnap: 'cursor',
    spikecolor: '#64748b',
    spikethickness: 1,
  },
  yaxis: {
    gridcolor: '#1e293b',
    zeroline: false,
    side: 'right',
    showspikes: true,
    spikemode: 'across',
    spikesnap: 'cursor',
    spikecolor: '#64748b',
    spikethickness: 1,
  },
}

function toNum(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

export default function LivingChartPlot({
  tile,
  bars,
  chartStyle,
  horizonBars,
  showAdvancedTA,
  liveState,
  committee,
  exitRec,
  followLatest,
  viewportLocked,
  onViewportLockedChange,
  className,
}) {
  const { data, shapePack, xExtents } = useMemo(() => {
    const list = Array.isArray(bars) ? bars : []
    const tMs = []
    const open = []
    const high = []
    const low = []
    const close = []
    for (const b of list) {
      const t = barTimeMs(b)
      if (t == null) continue
      tMs.push(t)
      open.push(toNum(b.open))
      high.push(toNum(b.high))
      low.push(toNum(b.low))
      close.push(toNum(b.close))
    }

    if (tMs.length === 0) {
      return { data: [], shapePack: { shapes: [], taTraces: [] }, xExtents: null }
    }

    const traces = []
    if (chartStyle === 'candles' && high.some((h) => h != null)) {
      traces.push({
        type: 'candlestick',
        name: 'Price',
        x: tMs,
        open,
        high,
        low,
        close,
        increasing: { line: { color: '#10b981' } },
        decreasing: { line: { color: '#ef4444' } },
      })
    } else {
      traces.push({
        type: 'scatter',
        mode: 'lines',
        name: 'Price',
        x: tMs,
        y: close.map((c, i) => (c != null ? c : low[i] ?? high[i])),
        line: { color: LIVING_CHART_COLORS.price, width: 2 },
        connectgaps: false,
      })
    }

    const lastClose = [...close].reverse().find((c) => c != null)
    const lastT = tMs[tMs.length - 1]
    if (lastClose != null && lastT != null) {
      traces.push({
        type: 'scatter',
        mode: 'markers',
        name: 'Now',
        x: [lastT],
        y: [lastClose],
        marker: {
          size: 11,
          color: '#f8fafc',
          line: { color: '#0f172a', width: 2 },
        },
        hovertemplate: 'Now: %{y:.4f}<extra></extra>',
      })
    }

    const fwd = buildExpectationForwardSeries(tile, list, horizonBars)
    if (fwd && fwd.tMs.length > 0) {
      const { tMs: tx, center, upper, lower } = fwd
      const validLower = lower.every((v) => v != null && Number.isFinite(v))
      const validUpper = upper.every((v) => v != null && Number.isFinite(v))
      const validCenter = center.every((v) => v != null && Number.isFinite(v))
      if (validLower && validUpper) {
        traces.push({
          type: 'scatter',
          mode: 'lines',
          x: tx,
          y: lower,
          line: { width: 0 },
          showlegend: false,
          hoverinfo: 'skip',
        })
        traces.push({
          type: 'scatter',
          mode: 'lines',
          name: 'Expected band',
          x: tx,
          y: upper,
          fill: 'tonexty',
          fillcolor: LIVING_CHART_COLORS.expectedBand,
          line: { color: 'rgba(251,191,36,0.35)', width: 1, dash: '4px,3px' },
          hoverinfo: 'skip',
        })
      }
      if (validCenter) {
        traces.push({
          type: 'scatter',
          mode: 'lines',
          name: 'Expected path',
          x: tx,
          y: center,
          line: { color: LIVING_CHART_COLORS.expectedCenter, width: 2, dash: '6px,3px' },
          connectgaps: false,
          hovertemplate: 'Expected: %{y:.4f}<extra></extra>',
        })
      }
    }

    const shapePack = buildLivingChartShapesAndTA({
      tile,
      bars: list,
      liveState,
      committee,
      exitRec,
      showAdvancedTA,
      horizonBars,
    })

    for (const t of shapePack.taTraces) {
      traces.push(t)
    }

    const xMin = tMs[0]
    const fwdEnd = fwd?.tMs?.length ? fwd.tMs[fwd.tMs.length - 1] : null
    const xMax = fwdEnd != null ? Math.max(tMs[tMs.length - 1], fwdEnd) : tMs[tMs.length - 1]
    return { data: traces, shapePack, xExtents: { xMin, xMax } }
  }, [tile, bars, chartStyle, horizonBars, showAdvancedTA, liveState, committee, exitRec])

  const layout = useMemo(() => {
    const ly = {
      ...BASE_LAYOUT,
      shapes: shapePack.shapes || [],
      annotations: shapePack.annotations || [],
      uirevision: UI_REVISION,
    }

    if (followLatest && !viewportLocked && xExtents) {
      const { xMin, xMax } = xExtents
      const span = Math.max(xMax - xMin, 120000)
      const windowMs = Math.min(span, Math.max(span * 0.35, 45 * 60 * 1000))
      ly.xaxis = {
        ...BASE_LAYOUT.xaxis,
        range: [new Date(xMax - windowMs), new Date(xMax + span * 0.08)],
        autorange: false,
      }
    } else {
      ly.xaxis = { ...BASE_LAYOUT.xaxis, autorange: true }
    }

    return ly
  }, [shapePack, followLatest, viewportLocked, xExtents])

  const onRelayout = useCallback(
    (e) => {
      if (!e) return
      const keys = Object.keys(e)
      const userChangedRange =
        keys.some((k) => k.startsWith('xaxis.range'))
        || keys.includes('xaxis.range[0]')
        || keys.includes('yaxis.range[0]')
        || keys.includes('yaxis.autorange')
      if (userChangedRange) {
        onViewportLockedChange?.(true)
      }
    },
    [onViewportLockedChange],
  )

  const config = useMemo(
    () => ({
      responsive: true,
      displayModeBar: true,
      displaylogo: false,
      scrollZoom: true,
      modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    }),
    [],
  )

  if (data.length === 0) {
    return <div className={className || 'lc-chart-empty'}>No market bars for this symbol yet.</div>
  }

  return (
    <div className={className || 'lc-plot-wrap'} style={{ width: '100%', height: '100%', minHeight: 0 }}>
      <Plot
        data={data}
        layout={layout}
        config={config}
        style={{ width: '100%', height: '100%' }}
        useResizeHandler
        onRelayout={onRelayout}
      />
    </div>
  )
}
