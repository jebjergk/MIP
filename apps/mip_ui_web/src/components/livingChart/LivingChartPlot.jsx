import { useCallback, useMemo } from 'react'
import Plot from 'react-plotly.js'
import {
  buildExpectationForwardSeries,
  buildLivingChartShapesAndTA,
  barTimeMs,
  LIVING_CHART_COLORS,
} from '../../lib/livingChartOverlays'

const UI_REVISION_BASE = 'living-chart-v2'

const BASE_LAYOUT = {
  paper_bgcolor: '#0f172a',
  plot_bgcolor: '#0f172a',
  font: { color: '#94a3b8', size: 11 },
  margin: { t: 22, r: 52, b: 44, l: 52 },
  showlegend: false,
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
  layoutRevision = 0,
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

    const lastClose = [...close].reverse().find((c) => c != null)
    const lastT = tMs[tMs.length - 1]
    const fwd = buildExpectationForwardSeries(tile, list, horizonBars)

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
        line: { color: '#4a7ec9', width: 2.45 },
        connectgaps: false,
      })
    }

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
          fillcolor: 'rgba(251, 191, 36, 0.07)',
          line: { color: 'rgba(251,191,36,0.2)', width: 0.6, dash: '5px,4px' },
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
          line: { color: 'rgba(234, 179, 8, 0.88)', width: 1.25, dash: '8px,4px' },
          connectgaps: false,
          hovertemplate: 'Expected: %{y:.4f}<extra></extra>',
        })
      }
    }

    if (lastClose != null && lastT != null) {
      let fill = '#f8fafc'
      let line = { color: '#38bdf8', width: 2.75 }
      if (liveState?.derived_features?.inside_cone === false) {
        fill = '#fef9c3'
        line = { color: '#eab308', width: 2.75 }
      } else if (
        fwd?.lower?.[0] != null
        && fwd?.upper?.[0] != null
        && Number.isFinite(fwd.lower[0])
        && Number.isFinite(fwd.upper[0])
      ) {
        if (lastClose < fwd.lower[0] || lastClose > fwd.upper[0]) {
          fill = '#ffedd5'
          line = { color: '#fb923c', width: 2.75 }
        }
      }
      traces.push({
        type: 'scatter',
        mode: 'markers',
        x: [lastT],
        y: [lastClose],
        marker: {
          size: 28,
          color: 'rgba(56, 189, 248, 0.2)',
          line: { width: 0 },
        },
        hoverinfo: 'skip',
        showlegend: false,
      })
      traces.push({
        type: 'scatter',
        mode: 'markers',
        name: 'Now',
        x: [lastT],
        y: [lastClose],
        marker: {
          size: 16,
          color: fill,
          line,
        },
        hovertemplate: 'Now: %{y:.4f}<extra></extra>',
      })
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

  const symbolKey = String(tile?.symbol || '').toUpperCase() || 'none'

  const layout = useMemo(() => {
    const ly = {
      ...BASE_LAYOUT,
      shapes: shapePack.shapes || [],
      annotations: shapePack.annotations || [],
      // Include symbol so pan/zoom from one ticker is not reused after switching symbols.
      uirevision: `${UI_REVISION_BASE}-${layoutRevision}-${symbolKey}`,
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
      ly.yaxis = { ...BASE_LAYOUT.yaxis, autorange: true }
    }

    return ly
  }, [shapePack, followLatest, viewportLocked, xExtents, layoutRevision, symbolKey])

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
