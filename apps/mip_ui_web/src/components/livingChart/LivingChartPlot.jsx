import { useCallback, useMemo } from 'react'
import Plot from 'react-plotly.js'
import {
  buildExpectationForwardSeries,
  buildLivingChartShapesAndTA,
  barTimeMs,
} from '../../lib/livingChartOverlays'
import { dominantActivePlotTag } from '../../lib/livingChartVisualState'

const UI_REVISION_BASE = 'living-chart-v2'

/** One subtle full-plot tint from highest-priority conditional zone (parent passes sorted keys). */
const LIVE_STATE_TINT = {
  stop_danger: { paper: '#12090c', plot: '#10080b' },
  target_near: { paper: '#081210', plot: '#07100f' },
  thesis_weakening: { paper: '#110f0a', plot: '#0f0d09' },
  mean_reversion: { paper: '#061218', plot: '#051015' },
}

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
  conditionalKeys = [],
  dominantActiveKey = null,
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
          fillcolor: 'rgba(251, 191, 36, 0.085)',
          line: { color: 'rgba(251,191,36,0.5)', width: 1.05, dash: '3px,3px' },
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
          line: { color: 'rgba(254, 240, 138, 0.99)', width: 2, dash: '12px,5px' },
          connectgaps: false,
          hovertemplate: 'Expected: %{y:.4f}<extra></extra>',
        })
      }
    }

    if (lastClose != null && lastT != null) {
      let fill = '#f8fafc'
      let line = { color: '#38bdf8', width: 3.25 }
      const inside = liveState?.derived_features?.inside_cone
      if (inside === false) {
        fill = '#fef9c3'
        line = { color: '#eab308', width: 3.25 }
      } else if (
        fwd?.lower?.[0] != null
        && fwd?.upper?.[0] != null
        && Number.isFinite(fwd.lower[0])
        && Number.isFinite(fwd.upper[0])
      ) {
        const lo = fwd.lower[0]
        const hi = fwd.upper[0]
        if (lastClose < lo || lastClose > hi) {
          fill = '#ffedd5'
          line = { color: '#fb923c', width: 3.25 }
        } else {
          const span = hi - lo
          if (span > 0) {
            const t = (lastClose - lo) / span
            if (t < 0.2 || t > 0.8) {
              fill = '#fff7ed'
              line = { color: '#f59e0b', width: 3.35 }
            }
          }
        }
      }
      traces.push({
        type: 'scatter',
        mode: 'markers',
        x: [lastT],
        y: [lastClose],
        marker: {
          size: 44,
          color: 'rgba(56, 189, 248, 0.36)',
          line: { width: 0 },
        },
        hoverinfo: 'skip',
        showlegend: false,
      })
      traces.push({
        type: 'scatter',
        mode: 'markers',
        x: [lastT],
        y: [lastClose],
        marker: {
          size: 22,
          color: fill,
          line: { ...line, width: (line?.width || 3) + 0.15 },
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
          size: 8,
          color: '#0f172a',
          line: { width: 0 },
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
    const topKey = Array.isArray(conditionalKeys) && conditionalKeys.length > 0 ? conditionalKeys[0] : null
    const tint = topKey && LIVE_STATE_TINT[topKey] ? LIVE_STATE_TINT[topKey] : null

    const ann = [...(shapePack.annotations || [])]
    const tag = dominantActivePlotTag(dominantActiveKey)
    const barList = Array.isArray(bars) ? bars : []
    if (tag && barList.length > 0 && tile) {
      const lastBar = barList[barList.length - 1]
      const tLast = barTimeMs(lastBar)
      const spot =
        toNum(liveState?.last_price)
        ?? toNum(tile?.current_price)
        ?? toNum(lastBar?.close)
        ?? toNum(lastBar?.low)
      if (tLast != null && spot != null && Number.isFinite(spot)) {
        ann.push({
          xref: 'x',
          yref: 'y',
          x: tLast,
          y: spot,
          text: tag,
          showarrow: false,
          xanchor: 'right',
          xshift: -4,
          yshift: 28,
          font: { size: 9, color: '#e2e8f0' },
          bgcolor: 'rgba(15,23,42,0.9)',
          bordercolor: 'rgba(51,65,85,0.95)',
          borderwidth: 1,
          borderpad: 3,
        })
      }
    }

    const ly = {
      ...BASE_LAYOUT,
      paper_bgcolor: tint?.paper ?? BASE_LAYOUT.paper_bgcolor,
      plot_bgcolor: tint?.plot ?? BASE_LAYOUT.plot_bgcolor,
      shapes: shapePack.shapes || [],
      annotations: ann,
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
    }

    return ly
  }, [
    shapePack,
    followLatest,
    viewportLocked,
    xExtents,
    layoutRevision,
    symbolKey,
    conditionalKeys,
    dominantActiveKey,
    bars,
    tile,
    liveState,
  ])

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
        revision={layoutRevision}
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
