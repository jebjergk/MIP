import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  ReferenceArea,
  ReferenceDot,
} from 'recharts'

import { API_BASE } from '../App'
import useVisibleInterval from '../hooks/useVisibleInterval'
import { useSymbolMeta } from '../context/SymbolMetaContext'
import {
  buildLiveState,
  confidenceRank,
  evaluateCommittee,
  isMaterialUpdate,
  severityRank,
  computeChartOverlays,
  generateExitRecommendation,
  generateSituationalReport,
  computeMomentumGauge,
} from './symbolTrackerCommittee'
import GlossaryHoverCard from '../components/GlossaryHoverCard'
import './SymbolTracker.css'

const SORT_OPTIONS = [
  { value: 'worst_pnl', label: 'Worst P&L' },
  { value: 'best_pnl', label: 'Best P&L' },
  { value: 'closest_tp', label: 'Closest to TP' },
  { value: 'closest_sl', label: 'Closest to SL' },
  { value: 'newest', label: 'Newest Position' },
]

function fmtNum(value, digits = 2) {
  if (value == null) return '—'
  const n = Number(value)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

function fmtPct(value) {
  if (value == null) return '—'
  const n = Number(value)
  if (!Number.isFinite(n)) return '—'
  return `${(n * 100).toFixed(2)}%`
}

function fmtSigned(value, digits = 2) {
  if (value == null) return '—'
  const n = Number(value)
  if (!Number.isFinite(n)) return '—'
  const text = Math.abs(n).toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })
  if (n > 0) return `+${text}`
  if (n < 0) return `-${text}`
  return text
}

function toDateLabel(ts) {
  if (!ts) return ''
  try {
    const d = new Date(ts)
    if (Number.isNaN(d.getTime())) return String(ts).slice(0, 10)
    return d.toISOString().slice(0, 10)
  } catch {
    return String(ts).slice(0, 10)
  }
}

function fmtEventTs(ts) {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    if (Number.isNaN(d.getTime())) return String(ts).slice(0, 16)
    return `${d.toISOString().slice(0, 10)} ${d.toISOString().slice(11, 16)}`
  } catch {
    return String(ts).slice(0, 16)
  }
}

function fmtBarTs(ts) {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    if (Number.isNaN(d.getTime())) return String(ts).slice(0, 19).replace('T', ' ')
    return `${d.toISOString().slice(0, 10)} ${d.toISOString().slice(11, 19)}`
  } catch {
    return String(ts).slice(0, 19).replace('T', ' ')
  }
}

function eventStyle(eventType) {
  const t = String(eventType || '').toUpperCase()
  if (t === 'NEWS') return { color: '#f59e0b', glyph: 'N', anchor: 'top' }
  if (t === 'ACTION') return { color: '#22d3ee', glyph: 'A', anchor: 'top' }
  if (t === 'FILL') return { color: '#34d399', glyph: 'F', anchor: 'bottom' }
  if (t === 'ENTRY') return { color: '#a78bfa', glyph: 'E', anchor: 'bottom' }
  return { color: '#94a3b8', glyph: '•', anchor: 'top' }
}

function mergeIbLiveRows(prevData, livePayload) {
  if (!prevData || !Array.isArray(prevData.tiles)) return prevData
  const rows = Array.isArray(livePayload?.rows) ? livePayload.rows : []
  if (rows.length === 0) return prevData
  const bySymbol = new Map(rows.map((r) => [String(r.symbol || '').toUpperCase(), r]))
  const intervalMinutes = Number(livePayload?.interval_minutes)
  const tiles = prevData.tiles.map((tile) => {
    const symbol = String(tile?.symbol || '').toUpperCase()
    const live = bySymbol.get(symbol)
    if (!live || !Array.isArray(live.bars) || live.bars.length === 0) return tile
    const bars = live.bars
    const currentPrice = Number(live.current_price)
    const resolvedCurrent = Number.isFinite(currentPrice)
      ? currentPrice
      : Number(bars[bars.length - 1]?.close)
    const entry = Number(tile?.entry_price)
    const qty = Number(tile?.quantity)
    let unrealized = tile?.unrealized_pnl
    if (Number.isFinite(resolvedCurrent) && Number.isFinite(entry) && Number.isFinite(qty)) {
      unrealized = tile?.side === 'SHORT'
        ? (entry - resolvedCurrent) * qty
        : (resolvedCurrent - entry) * qty
    }
    return {
      ...tile,
      current_price: Number.isFinite(resolvedCurrent) ? resolvedCurrent : tile?.current_price,
      unrealized_pnl: unrealized,
      chart: {
        ...(tile?.chart || {}),
        interval_minutes: Number.isFinite(intervalMinutes) ? intervalMinutes : tile?.chart?.interval_minutes,
        bars,
      },
      overlays: {
        ...(tile?.overlays || {}),
        current: Number.isFinite(resolvedCurrent) ? resolvedCurrent : tile?.overlays?.current,
      },
    }
  })
  return {
    ...prevData,
    tiles,
    updated_at: livePayload?.updated_at || new Date().toISOString(),
  }
}

function solveLinearSystem(matrix, vector) {
  const n = matrix.length
  const a = matrix.map((row) => [...row])
  const b = [...vector]
  for (let i = 0; i < n; i += 1) {
    let maxRow = i
    for (let k = i + 1; k < n; k += 1) {
      if (Math.abs(a[k][i]) > Math.abs(a[maxRow][i])) maxRow = k
    }
    if (Math.abs(a[maxRow][i]) < 1e-12) return null
    if (maxRow !== i) {
      const tempRow = a[i]
      a[i] = a[maxRow]
      a[maxRow] = tempRow
      const tempVal = b[i]
      b[i] = b[maxRow]
      b[maxRow] = tempVal
    }
    const pivot = a[i][i]
    for (let j = i; j < n; j += 1) a[i][j] /= pivot
    b[i] /= pivot
    for (let k = 0; k < n; k += 1) {
      if (k === i) continue
      const factor = a[k][i]
      for (let j = i; j < n; j += 1) a[k][j] -= factor * a[i][j]
      b[k] -= factor * b[i]
    }
  }
  return b
}

function fitPolynomialSeries(xValues, yValues, degree = 3) {
  const n = xValues.length
  if (n < 3) return null
  const d = Math.max(1, Math.min(degree, n - 1))
  const size = d + 1
  const normal = Array.from({ length: size }, () => Array(size).fill(0))
  const rhs = Array(size).fill(0)
  for (let i = 0; i < n; i += 1) {
    const x = Number(xValues[i])
    const y = Number(yValues[i])
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue
    const powers = [1]
    for (let p = 1; p <= 2 * d; p += 1) powers[p] = powers[p - 1] * x
    for (let r = 0; r <= d; r += 1) {
      rhs[r] += y * powers[r]
      for (let c = 0; c <= d; c += 1) normal[r][c] += powers[r + c]
    }
  }
  return solveLinearSystem(normal, rhs)
}

function evalPolynomial(coeffs, x) {
  if (!coeffs) return null
  let y = 0
  for (let i = 0; i < coeffs.length; i += 1) y += coeffs[i] * (x ** i)
  return y
}

function ProjectionDetail({ tile, projectionMode }) {
  const path = tile?.expectation?.center_path || []
  const entry = Number(tile?.entry_price || tile?.overlays?.entry || 0)
  if (path.length < 2 || !Number.isFinite(entry) || entry <= 0) return null

  const indexed = path
    .map((p) => ({ step: Number(p.step), value: (Number(p.price) / entry) * 100 }))
    .filter((p) => Number.isFinite(p.step) && Number.isFinite(p.value))
  if (indexed.length < 2) return null

  const width = 230
  const height = 64
  const pad = 6
  const min = Math.min(...indexed.map((p) => p.value))
  const max = Math.max(...indexed.map((p) => p.value))
  const spread = Math.max(max - min, 0.0001)
  const xMax = indexed[indexed.length - 1].step || indexed.length
  const toXY = (step, value) => {
    const x = pad + ((step - 1) / Math.max(xMax - 1, 1)) * (width - pad * 2)
    const y = height - pad - ((value - min) / spread) * (height - pad * 2)
    return [x, y]
  }
  const linePath = indexed
    .map((p, idx) => {
      const [x, y] = toXY(p.step, p.value)
      return `${idx === 0 ? 'M' : 'L'}${x},${y}`
    })
    .join(' ')

  const first = indexed[0].value
  const last = indexed[indexed.length - 1].value

  // Build a linear benchmark to quantify the midpoint difference.
  const midpointIdx = Math.floor(indexed.length / 2)
  const midpoint = indexed[midpointIdx]
  const midpointLinear = first + ((last - first) * midpointIdx) / Math.max(indexed.length - 1, 1)
  const midpointDeltaBps = (midpoint.value - midpointLinear) * 100
  const modeLabel = projectionMode === 'stitched'
    ? 'Horizon-stitched'
    : projectionMode === 'geometric'
      ? 'Geometric'
      : 'Linear'

  return (
    <div className="symbol-tracker-projection-detail">
      <div className="symbol-tracker-projection-head">
        <span>Projection detail (index, entry=100)</span>
        <span>{modeLabel} · mid delta {midpointDeltaBps.toFixed(2)} bps</span>
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} className="symbol-tracker-projection-svg" preserveAspectRatio="none" aria-hidden>
        <path d={linePath} fill="none" stroke="#f59e0b" strokeWidth="2" />
      </svg>
      <div className="symbol-tracker-projection-foot">
        <span>Start {first.toFixed(3)}</span>
        <span>End {last.toFixed(3)}</span>
      </div>
    </div>
  )
}

function TrackerTooltip({ active, payload }) {
  if (!active || !payload || payload.length === 0) return null
  const row = payload[0]?.payload
  if (!row) return null
  return (
    <div className="symbol-tracker-tooltip">
      <div className="symbol-tracker-tooltip-title">{fmtBarTs(row.ts)}</div>
      {row.close != null ? <div>Close: {fmtNum(row.close, 4)}</div> : null}
      {row.open != null ? <div>Open: {fmtNum(row.open, 4)}</div> : null}
      {row.high != null ? <div>High: {fmtNum(row.high, 4)}</div> : null}
      {row.low != null ? <div>Low: {fmtNum(row.low, 4)}</div> : null}
      {row.projected_center != null ? <div>Training center: {fmtNum(row.projected_center, 4)}</div> : null}
      {row.projected_upper != null ? <div>Training upper: {fmtNum(row.projected_upper, 4)}</div> : null}
      {row.projected_lower != null ? <div>Training lower: {fmtNum(row.projected_lower, 4)}</div> : null}
    </div>
  )
}

function TileChart({ tile, mode, chartStyle, density, projectionMode, trendRender, showOverlays = true }) {
  const bars = Array.isArray(tile?.chart?.bars) ? tile.chart.bars : []
  if (bars.length === 0) {
    return <div className="symbol-tracker-chart-empty">No market bars available for this symbol yet.</div>
  }

  const overlays = showOverlays && mode === 'intraday' ? computeChartOverlays(bars) : null

  const chartData = bars.map((bar, idx) => {
    const isUp = Number(bar.close) >= Number(bar.open)
    return {
      ...bar,
      idx,
      label: toDateLabel(bar.ts),
      wick: bar.high != null && bar.low != null ? [bar.low, bar.high] : null,
      bodyUp: isUp ? [bar.open, bar.close] : null,
      bodyDown: !isUp ? [bar.close, bar.open] : null,
      projected_center: null,
      projected_upper: null,
      projected_lower: null,
      vwap: overlays?.vwap?.[idx] ?? null,
      bb_upper: overlays?.bollinger?.upper?.[idx] ?? null,
      bb_middle: overlays?.bollinger?.middle?.[idx] ?? null,
      bb_lower: overlays?.bollinger?.lower?.[idx] ?? null,
    }
  })

  if (mode === 'daily' && tile?.expectation?.is_available) {
    const lastIdx = chartData.length - 1
    const centerPath = tile.expectation.center_path || []
    const upperPath = tile.expectation.upper_path || []
    const lowerPath = tile.expectation.lower_path || []
    if (projectionMode === 'stitched' && centerPath.length > 1) {
      const densify = 6
      const entryBaseline = Number(tile?.overlays?.entry)
      for (let i = 0; i < centerPath.length; i += 1) {
        const step = centerPath[i]?.step ?? i + 1
        const idxStart = lastIdx + i
        const prevCenter = i === 0 ? (Number.isFinite(entryBaseline) ? entryBaseline : Number(centerPath[i]?.price)) : Number(centerPath[i - 1]?.price)
        const prevUpper = i === 0 ? (Number.isFinite(entryBaseline) ? entryBaseline : Number(upperPath[i]?.price)) : Number(upperPath[i - 1]?.price)
        const prevLower = i === 0 ? (Number.isFinite(entryBaseline) ? entryBaseline : Number(lowerPath[i]?.price)) : Number(lowerPath[i - 1]?.price)
        const nextCenter = Number(centerPath[i]?.price)
        const nextUpper = Number(upperPath[i]?.price)
        const nextLower = Number(lowerPath[i]?.price)
        for (let sub = 1; sub <= densify; sub += 1) {
          const frac = sub / densify
          const interp = (a, b) => {
            if (!Number.isFinite(a) || !Number.isFinite(b)) return null
            if (a > 0 && b > 0) return a * ((b / a) ** frac)
            return a + (b - a) * frac
          }
          chartData.push({
            idx: idxStart + frac,
            label: sub === densify ? `+${step}` : '',
            open: null,
            high: null,
            low: null,
            close: null,
            wick: null,
            bodyUp: null,
            bodyDown: null,
            projected_center: interp(prevCenter, nextCenter),
            projected_upper: interp(prevUpper, nextUpper),
            projected_lower: interp(prevLower, nextLower),
          })
        }
      }
    } else {
      for (let i = 0; i < centerPath.length; i += 1) {
        chartData.push({
          idx: lastIdx + i + 1,
          label: `+${centerPath[i]?.step ?? i + 1}`,
          open: null,
          high: null,
          low: null,
          close: null,
          wick: null,
          bodyUp: null,
          bodyDown: null,
          projected_center: centerPath[i]?.price ?? null,
          projected_upper: upperPath[i]?.price ?? null,
          projected_lower: lowerPath[i]?.price ?? null,
        })
      }
    }
  }

  if (mode === 'daily' && trendRender === 'soft') {
    const forwardRows = chartData.filter((row) => row.projected_center != null)
    if (forwardRows.length >= 4) {
      const x = forwardRows.map((row) => Number(row.idx))
      const centerRaw = forwardRows.map((row) => Number(row.projected_center))
      const upperRaw = forwardRows.map((row) => Number(row.projected_upper))
      const lowerRaw = forwardRows.map((row) => Number(row.projected_lower))
      const cCoef = fitPolynomialSeries(x, centerRaw, 3)
      const uCoef = fitPolynomialSeries(x, upperRaw, 3)
      const lCoef = fitPolynomialSeries(x, lowerRaw, 3)
      if (cCoef && uCoef && lCoef) {
        chartData.forEach((row) => {
          if (row.projected_center == null) return
          const px = Number(row.idx)
          const c = evalPolynomial(cCoef, px)
          const u = evalPolynomial(uCoef, px)
          const l = evalPolynomial(lCoef, px)
          row.projected_center = Number.isFinite(c) ? c : row.projected_center
          row.projected_upper = Number.isFinite(u) ? u : row.projected_upper
          row.projected_lower = Number.isFinite(l) ? l : row.projected_lower
        })
      }
    }
  }

  const entry = tile?.overlays?.entry
  const tp = tile?.overlays?.take_profit
  const sl = tile?.overlays?.stop_loss
  const current = tile?.overlays?.current
  const side = tile?.side
  const currentIdx = bars.length - 1
  const labelByIdx = new Map(
    chartData
      .filter((row) => row.label)
      .map((row) => [String(row.idx), row.label]),
  )
  const barDates = bars.map((b) => String(b.ts || '').slice(0, 10))
  const dateToIdx = new Map()
  barDates.forEach((d, idx) => dateToIdx.set(d, idx))
  const highs = bars.map((b) => Number(b.high)).filter((v) => Number.isFinite(v))
  const lows = bars.map((b) => Number(b.low)).filter((v) => Number.isFinite(v))
  const yMax = highs.length > 0 ? Math.max(...highs) : Number(current || entry || 1)
  const yMin = lows.length > 0 ? Math.min(...lows) : Number(current || entry || 0)
  const yRange = Math.max(yMax - yMin, 1)
  const markerEvents = (Array.isArray(tile?.events) ? tile.events : []).slice(0, 6).reverse()
  const markerPoints = markerEvents.map((event, idx) => {
    const style = eventStyle(event.type)
    const eventDate = String(event.ts || '').slice(0, 10)
    const eventIdx = dateToIdx.has(eventDate) ? dateToIdx.get(eventDate) : currentIdx
    const laneOffset = (idx % 3) * (yRange * 0.02)
    const y = style.anchor === 'top'
      ? (yMax - laneOffset)
      : (yMin + laneOffset)
    return {
      ...event,
      markerColor: style.color,
      markerGlyph: style.glyph,
      markerAnchor: style.anchor,
      markerIdx: eventIdx,
      markerY: y,
    }
  })
  const dayBoundaryLines = []
  if (mode === 'intraday') {
    let prevDay = null
    bars.forEach((bar, idx) => {
      const day = String(bar?.ts || '').slice(0, 10)
      if (!day) return
      if (prevDay && day !== prevDay) {
        dayBoundaryLines.push({ idx, day })
      }
      prevDay = day
    })
  }

  return (
    <ResponsiveContainer width="100%" height={density === 'compact' ? 190 : 260}>
      <ComposedChart data={chartData} margin={{ top: 8, right: 16, left: 0, bottom: 8 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#2f3745" vertical={false} />
        <XAxis
          dataKey="idx"
          tickFormatter={(idx) => labelByIdx.get(String(idx)) || ''}
          tick={{ fontSize: 10 }}
        />
        <YAxis tick={{ fontSize: 10 }} domain={['auto', 'auto']} />
        <Tooltip content={<TrackerTooltip />} />

        {chartStyle === 'line' ? (
          <Line type="monotone" dataKey="close" stroke="#60a5fa" strokeWidth={2} dot={false} connectNulls />
        ) : (
          <>
            <Bar dataKey="wick" fill="none" stroke="#9ca3af" strokeWidth={1} barSize={1} />
            <Bar dataKey="bodyUp" fill="#10b981" stroke="#10b981" barSize={6} />
            <Bar dataKey="bodyDown" fill="#ef4444" stroke="#ef4444" barSize={6} />
          </>
        )}

        {mode === 'daily' && tile?.expectation?.is_available ? (
          <>
            <Line type="monotone" dataKey="projected_center" stroke="#f59e0b" strokeWidth={2} dot={false} connectNulls />
            <Line type="monotone" dataKey="projected_upper" stroke="#fbbf24" strokeWidth={1} dot={false} strokeDasharray="4 4" connectNulls />
            <Line type="monotone" dataKey="projected_lower" stroke="#fbbf24" strokeWidth={1} dot={false} strokeDasharray="4 4" connectNulls />
          </>
        ) : null}

        {overlays ? (
          <>
            <Line type="monotone" dataKey="vwap" stroke="#e879f9" strokeWidth={1.5} dot={false} connectNulls strokeDasharray="6 3" name="VWAP" />
            <Line type="monotone" dataKey="bb_upper" stroke="#38bdf8" strokeWidth={1} dot={false} connectNulls strokeDasharray="3 3" name="BB Upper" />
            <Line type="monotone" dataKey="bb_middle" stroke="#38bdf8" strokeWidth={1} dot={false} connectNulls strokeDasharray="2 4" name="BB Mid" />
            <Line type="monotone" dataKey="bb_lower" stroke="#38bdf8" strokeWidth={1} dot={false} connectNulls strokeDasharray="3 3" name="BB Lower" />
          </>
        ) : null}

        {overlays?.sr?.support != null ? <ReferenceLine y={overlays.sr.support} stroke="#22c55e" strokeWidth={1} strokeDasharray="8 4" label={{ value: 'S', position: 'left', fill: '#22c55e', fontSize: 9 }} /> : null}
        {overlays?.sr?.resistance != null ? <ReferenceLine y={overlays.sr.resistance} stroke="#f87171" strokeWidth={1} strokeDasharray="8 4" label={{ value: 'R', position: 'left', fill: '#f87171', fontSize: 9 }} /> : null}

        {overlays?.sr?.support != null && overlays?.sr?.resistance != null ? (
          <ReferenceArea
            y1={overlays.sr.support}
            y2={overlays.sr.resistance}
            fill="#334155"
            fillOpacity={0.06}
          />
        ) : null}

        {entry != null ? <ReferenceLine y={entry} stroke="#a78bfa" strokeDasharray="4 3" label="Entry" /> : null}
        {tp != null ? <ReferenceLine y={tp} stroke="#10b981" strokeDasharray="4 3" label="TP" /> : null}
        {sl != null ? <ReferenceLine y={sl} stroke="#ef4444" strokeDasharray="4 3" label="SL" /> : null}

        {entry != null && tp != null ? (
          <ReferenceArea
            y1={Math.min(entry, tp)}
            y2={Math.max(entry, tp)}
            x1={0}
            x2={chartData.length - 1}
            fill={side === 'LONG' ? '#064e3b' : '#7c2d12'}
            fillOpacity={0.15}
          />
        ) : null}
        {entry != null && sl != null ? (
          <ReferenceArea
            y1={Math.min(entry, sl)}
            y2={Math.max(entry, sl)}
            x1={0}
            x2={chartData.length - 1}
            fill={side === 'LONG' ? '#7c2d12' : '#064e3b'}
            fillOpacity={0.12}
          />
        ) : null}

        {current != null ? (
          <ReferenceDot
            x={currentIdx}
            y={current}
            r={5}
            fill="#f8fafc"
            stroke="#0f172a"
            strokeWidth={1.5}
            label={{ position: 'top', value: 'Now', fill: '#cbd5e1', fontSize: 10 }}
          />
        ) : null}

        {markerPoints.map((event, idx) => (
          <ReferenceDot
            key={`${event.type || 'EVENT'}_${event.ts || idx}_${idx}`}
            x={event.markerIdx}
            y={event.markerY}
            r={5}
            fill={event.markerColor}
            stroke="#0f172a"
            strokeWidth={1.25}
            label={{
              position: event.markerAnchor === 'top' ? 'top' : 'bottom',
              value: event.markerGlyph,
              fill: event.markerColor,
              fontSize: 10,
            }}
          />
        ))}

        {dayBoundaryLines.map((boundary) => (
          <ReferenceLine
            key={`day_boundary_${boundary.day}_${boundary.idx}`}
            x={boundary.idx}
            stroke="#3b4b63"
            strokeDasharray="3 5"
            ifOverflow="extendDomain"
          />
        ))}
      </ComposedChart>
    </ResponsiveContainer>
  )
}

function ExitRecommendationBar({ exitRec }) {
  if (!exitRec) return null
  const urgency = exitRec.urgency || 'HOLD'
  const colorMap = {
    HOLD: { bg: '#0c1e0c', border: '#166534', text: '#86efac', label: 'HOLD' },
    MONITOR: { bg: '#1a1a05', border: '#a16207', text: '#fcd34d', label: 'MONITOR' },
    PREPARE: { bg: '#2a1005', border: '#c2410c', text: '#fdba74', label: 'PREPARE TO EXIT' },
    EXIT_NOW: { bg: '#2a0505', border: '#dc2626', text: '#fca5a5', label: 'EXIT NOW' },
  }
  const style = colorMap[urgency] || colorMap.HOLD
  return (
    <div
      className={`st-exit-bar st-exit-bar--${urgency.toLowerCase()}`}
      style={{ background: style.bg, borderColor: style.border }}
    >
      <span className="st-exit-bar-label" style={{ color: style.text }}>{style.label}</span>
      <span className="st-exit-bar-text">{exitRec.headline}</span>
    </div>
  )
}

function MomentumGauge({ momentum }) {
  if (!momentum) return null
  const { score, label } = momentum
  const pct = ((score + 100) / 200) * 100
  const colorStops = score > 25 ? '#22c55e' : score > -25 ? '#f59e0b' : '#ef4444'
  const labelMap = {
    STRONG_BULLISH: 'Strong Bullish',
    BULLISH: 'Bullish',
    NEUTRAL: 'Neutral',
    BEARISH: 'Bearish',
    STRONG_BEARISH: 'Strong Bearish',
  }
  return (
    <div className="st-momentum-gauge">
      <div className="st-momentum-gauge-header">
        <span>Momentum</span>
        <span style={{ color: colorStops }}>{labelMap[label] || label}</span>
      </div>
      <div className="st-momentum-gauge-track">
        <div
          className="st-momentum-gauge-fill"
          style={{ width: `${pct}%`, background: colorStops }}
        />
        <div className="st-momentum-gauge-center" />
      </div>
      <div className="st-momentum-gauge-labels">
        <span>Bearish</span>
        <span>{score > 0 ? '+' : ''}{score}</span>
        <span>Bullish</span>
      </div>
    </div>
  )
}

function Tile({ tile, mode, chartStyle, density, projectionMode, trendRender, formatSymbolLabel, selected, onSelect }) {
  const pnl = Number(tile?.unrealized_pnl || 0)
  const pnlClass = pnl >= 0 ? 'symbol-tracker-pos' : 'symbol-tracker-neg'
  const thesisClass = String(tile?.thesis?.status || '').toLowerCase().replaceAll('_', '-')
  const exitRec = tile?.exitRecommendation
  const momentum = tile?.momentum
  const exitUrgency = exitRec?.urgency || 'HOLD'
  const tileExtraClass = exitUrgency === 'EXIT_NOW'
    ? 'symbol-tracker-tile--exit-now'
    : exitUrgency === 'PREPARE'
      ? 'symbol-tracker-tile--prepare'
      : ''
  return (
    <article
      className={`symbol-tracker-tile ${density === 'compact' ? 'symbol-tracker-tile--compact' : ''} ${selected ? 'symbol-tracker-tile--selected' : ''} ${tileExtraClass}`}
      role="button"
      tabIndex={0}
      onClick={() => onSelect?.(tile.symbol)}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          onSelect?.(tile.symbol)
        }
      }}
    >
      <ExitRecommendationBar exitRec={exitRec} />

      <header className="symbol-tracker-tile-head">
        <div>
          <h3>{formatSymbolLabel(tile.symbol, tile.market_type)}</h3>
          <div className="symbol-tracker-subline">
            {tile.side} · Qty {fmtNum(tile.quantity, 0)} · {tile.market_type}
          </div>
        </div>
        <div className={`symbol-tracker-thesis symbol-tracker-thesis--${thesisClass}`}>
          {tile?.thesis?.status || 'UNKNOWN'}
        </div>
      </header>

      <div className="symbol-tracker-kpi-row">
        <div><span>Avg Cost (IBKR)</span><b>{fmtNum(tile.entry_price, 4)}</b></div>
        <div><span>Current</span><b>{fmtNum(tile.current_price, 4)}</b></div>
        <div><span>Unrealized P&L</span><b className={pnlClass}>{fmtSigned(tile.unrealized_pnl, 2)}</b></div>
      </div>

      <MomentumGauge momentum={momentum} />

      <div className="symbol-tracker-badges">
        {(tile.position_status_badges || []).map((badge) => (
          <span key={badge} className="symbol-tracker-badge">{badge}</span>
        ))}
      </div>

      <TileChart
        tile={tile}
        mode={mode}
        chartStyle={chartStyle}
        density={density}
        projectionMode={projectionMode}
        trendRender={trendRender}
        showOverlays={mode === 'intraday'}
      />
      {mode === 'daily' && tile?.expectation?.is_available ? (
        <ProjectionDetail tile={tile} projectionMode={projectionMode} />
      ) : null}

      {(tile?.events || []).length > 0 ? (
        <div className="symbol-tracker-events">
          <div className="symbol-tracker-events-title">Recent events</div>
          {(tile.events || []).slice(0, 4).map((event, idx) => (
            <div key={`${event.type || 'event'}_${event.ts || idx}_${idx}`} className="symbol-tracker-event-row">
              <span className={`symbol-tracker-event-pill symbol-tracker-event-pill--${String(event.type || 'event').toLowerCase()}`}>
                {event.type || 'EVENT'}
              </span>
              <span className="symbol-tracker-event-time">{fmtEventTs(event.ts)}</span>
              {event.url ? (
                <a className="symbol-tracker-event-link" href={event.url} target="_blank" rel="noreferrer">
                  {event.label || 'Open'}
                </a>
              ) : (
                <span className="symbol-tracker-event-label">{event.label || '—'}</span>
              )}
            </div>
          ))}
        </div>
      ) : null}

      <div className="symbol-tracker-metrics">
        <div><span>Distance to TP</span><b>{fmtPct(tile?.progress_metrics?.distance_to_tp_pct)}</b></div>
        <div><span>Distance to SL</span><b>{fmtPct(tile?.progress_metrics?.distance_to_sl_pct)}</b></div>
        <div><span>Progress to TP</span><b>{fmtPct(tile?.progress_metrics?.progress_to_tp_pct)}</b></div>
        <div><span>Expected move reached</span><b>{fmtPct(tile?.progress_metrics?.expected_progress_pct)}</b></div>
        <div><span>Open R</span><b>{fmtNum(tile?.progress_metrics?.r_multiple_open, 2)}R</b></div>
        <div><span>Days since entry</span><b>{tile?.holding_context?.days_since_entry ?? '—'}</b></div>
        <div><span>Bars since entry</span><b>{tile?.holding_context?.bars_since_entry ?? '—'}</b></div>
        <div><span>Vol regime</span><b>{tile?.volatility_context?.status || 'UNKNOWN'}</b></div>
      </div>

      {(exitRec?.signals || []).length > 1 ? (
        <div className="st-exit-signals-detail">
          <div className="st-exit-signals-title">Active Signals</div>
          {exitRec.signals.map((sig, idx) => (
            <div key={idx} className="st-exit-signal-row">{sig}</div>
          ))}
        </div>
      ) : null}
    </article>
  )
}

function fmtTime(ts) {
  if (!ts) return '—'
  const d = new Date(ts)
  if (Number.isNaN(d.getTime())) return '—'
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function feedAlertClass(item) {
  const alertTone = String(item?.alert || 'NONE').toUpperCase()
  if (alertTone === 'RED') return 'symbol-tracker-feed-row--alert-red'
  if (alertTone === 'GREEN') return 'symbol-tracker-feed-row--alert-green'
  return ''
}

function feedDisplayLabel(item, formatSymbolLabel) {
  return formatSymbolLabel?.(item?.symbol, item?.market_type) || String(item?.symbol || '—')
}

function fmtReasonTag(value) {
  const raw = String(value || 'NO_CHANGE').toUpperCase()
  return raw.replaceAll('_', ' ')
}

function UrgencyBanner({ tiles }) {
  const critical = tiles.filter((t) => t?.exitRecommendation?.urgency === 'EXIT_NOW')
  const warnings = tiles.filter((t) => t?.exitRecommendation?.urgency === 'PREPARE')
  if (critical.length === 0 && warnings.length === 0) return null
  return (
    <div className={`st-urgency-banner ${critical.length > 0 ? 'st-urgency-banner--critical' : 'st-urgency-banner--warning'}`}>
      {critical.length > 0 ? (
        <div className="st-urgency-banner-line">
          <span className="st-urgency-banner-icon">!!</span>
          <span>EXIT SIGNAL on {critical.map((t) => t.symbol).join(', ')} — immediate review needed</span>
        </div>
      ) : null}
      {warnings.length > 0 ? (
        <div className="st-urgency-banner-line">
          <span className="st-urgency-banner-icon">!</span>
          <span>APPROACHING EXIT on {warnings.map((t) => t.symbol).join(', ')} — stay alert</span>
        </div>
      ) : null}
    </div>
  )
}

function SituationalBriefingPanel({
  selectedSymbol,
  setSelectedSymbol,
  reportsBySymbol,
  committeeBySymbol,
  watchlist,
  feed,
  formatSymbolLabel,
  contextUpdatedAt,
  liveUpdatedAt,
}) {
  const feedRef = useRef(null)
  const activeReport = selectedSymbol ? reportsBySymbol[selectedSymbol] : null
  const activeCommittee = selectedSymbol ? committeeBySymbol[selectedSymbol] : null
  const [panelView, setPanelView] = useState('briefing')

  const filteredFeed = [...feed]
    .sort((a, b) => new Date(b?.ts || 0).getTime() - new Date(a?.ts || 0).getTime())
    .slice(0, 30)

  useEffect(() => {
    const el = feedRef.current
    if (!el) return
    el.scrollTop = 0
  }, [filteredFeed.length])

  const urgencyColor = {
    EXIT_NOW: '#fca5a5',
    PREPARE: '#fdba74',
    MONITOR: '#fcd34d',
    HOLD: '#86efac',
  }

  return (
    <aside className="symbol-tracker-committee">
      <div className="symbol-tracker-committee-head">
        <h3>Situation Room</h3>
        <div className="symbol-tracker-committee-context-ts">
          <div>Context: {fmtTime(contextUpdatedAt)}</div>
          <div>Live: {fmtTime(liveUpdatedAt)}</div>
        </div>
      </div>

      <div className="st-panel-tabs">
        <button type="button" className={`st-panel-tab ${panelView === 'briefing' ? 'st-panel-tab--active' : ''}`} onClick={() => setPanelView('briefing')}>Briefing</button>
        <button type="button" className={`st-panel-tab ${panelView === 'feed' ? 'st-panel-tab--active' : ''}`} onClick={() => setPanelView('feed')}>Live Feed</button>
        <button type="button" className={`st-panel-tab ${panelView === 'agents' ? 'st-panel-tab--active' : ''}`} onClick={() => setPanelView('agents')}>Agent Detail</button>
      </div>

      <section className="symbol-tracker-committee-section symbol-tracker-committee-section--watchlist">
        <div className="symbol-tracker-committee-title">Positions</div>
        <div className="symbol-tracker-watchlist">
          {watchlist.length === 0 ? <div className="symbol-tracker-committee-empty">No positions.</div> : null}
          {watchlist.map((row) => {
            const report = reportsBySymbol[row.symbol]
            const urg = report?.overall_urgency || 'HOLD'
            return (
              <button
                type="button"
                key={row.symbol}
                className={`symbol-tracker-watch-row ${selectedSymbol === row.symbol ? 'symbol-tracker-watch-row--active' : ''}`}
                onClick={() => setSelectedSymbol(row.symbol)}
              >
                <div className="symbol-tracker-watch-top">
                  <span>{formatSymbolLabel(row.symbol, row.market_type)}</span>
                  <span className="st-urgency-pill" style={{ borderColor: urgencyColor[urg], color: urgencyColor[urg] }}>{urg.replace('_', ' ')}</span>
                </div>
                <div className="symbol-tracker-watch-mid">
                  <span style={{ fontSize: '0.68rem', color: '#94a3b8' }}>{report?.summary_line || row.headline_text || ''}</span>
                </div>
              </button>
            )
          })}
        </div>
      </section>

      {panelView === 'briefing' && (
        <section className="symbol-tracker-committee-section symbol-tracker-committee-section--discussion">
          <div className="symbol-tracker-committee-title">Situational Briefing</div>
          {!activeReport ? <div className="symbol-tracker-committee-empty">Select a position to see its briefing.</div> : null}
          {activeReport ? (
            <div className="st-briefing-scroll">
              <div className="st-briefing-summary" style={{ borderColor: urgencyColor[activeReport.overall_urgency] || '#334155' }}>
                {activeReport.summary_line}
              </div>
              {activeReport.sections.map((section, idx) => (
                <div key={idx} className="st-briefing-section">
                  <div className="st-briefing-section-title">{section.title}</div>
                  <div className="st-briefing-section-text">{section.text}</div>
                </div>
              ))}
              <div className="st-briefing-ts">Updated {fmtTime(activeReport.timestamp)}</div>
            </div>
          ) : null}
        </section>
      )}

      {panelView === 'feed' && (
        <section className="symbol-tracker-committee-section symbol-tracker-committee-section--feed">
          <div className="symbol-tracker-committee-title">Live Committee Feed</div>
          <div ref={feedRef} className="symbol-tracker-committee-feed">
            {filteredFeed.length === 0 ? <div className="symbol-tracker-committee-empty">No material changes yet.</div> : null}
            {filteredFeed.map((item) => (
              <div key={item.id} className={`symbol-tracker-feed-row ${feedAlertClass(item)}`}>
                <div className="symbol-tracker-feed-meta">
                  <span>{fmtTime(item.ts)}</span>
                  <span>{feedDisplayLabel(item, formatSymbolLabel)}</span>
                  <span>{item.agent}</span>
                </div>
                <div className="symbol-tracker-feed-text">{item.text}</div>
              </div>
            ))}
          </div>
        </section>
      )}

      {panelView === 'agents' && (
        <section className="symbol-tracker-committee-section symbol-tracker-committee-section--discussion">
          <div className="symbol-tracker-committee-title">Agent Detail</div>
          {!activeCommittee ? <div className="symbol-tracker-committee-empty">Select a position.</div> : null}
          {activeCommittee ? (
            <div className="symbol-tracker-thread">
              <div className="symbol-tracker-thread-summary">
                <div><b>{formatSymbolLabel(activeCommittee.symbol, activeCommittee.market_type)}</b></div>
                <div className="symbol-tracker-pill">{activeCommittee.committee_stance}</div>
                <div>{activeCommittee.committee_confidence}</div>
                <div>{activeCommittee.headline_text}</div>
              </div>
              {[...(activeCommittee.agent_messages || [])]
                .sort((a, b) => (
                  Number(Boolean(b.change_detected)) - Number(Boolean(a.change_detected))
                  || Number(b.materiality_score || 0) - Number(a.materiality_score || 0)
                ))
                .map((msg) => (
                <article key={`${activeCommittee.symbol}_${msg.agent_name}`} className="symbol-tracker-thread-msg">
                  <header>
                    <b>{msg.agent_name.replaceAll('_', ' ')}</b>
                    <span>{msg.confidence}</span>
                    <span>{msg.stance}</span>
                  </header>
                  <p>{msg.short_text}</p>
                </article>
                ))}
              {(activeCommittee.disagreement_points || []).length > 0 ? (
                <div className="symbol-tracker-thread-disagreement">
                  <b>Disagreement points</b>
                  <div>{activeCommittee.disagreement_points.join(' ')}</div>
                </div>
              ) : null}
              <div className="symbol-tracker-thread-actions">
                <b>Actions to consider:</b> {(activeCommittee.actions_to_consider || []).join(', ')}
              </div>
            </div>
          ) : null}
        </section>
      )}
    </aside>
  )
}

export default function SymbolTracker() {
  const { formatSymbolLabel } = useSymbolMeta()
  const [sensitivityMode, setSensitivityMode] = useState('BALANCED')
  const [mode, setMode] = useState('intraday')
  const [chartStyle, setChartStyle] = useState('line')
  const [horizonBars, setHorizonBars] = useState('5')
  const [projectionMode, setProjectionMode] = useState('stitched')
  const [trendRender, setTrendRender] = useState('soft')
  const [sortBy, setSortBy] = useState('worst_pnl')
  const [longsOnly, setLongsOnly] = useState(false)
  const [shortsOnly, setShortsOnly] = useState(false)
  const [activeTpSlOnly, setActiveTpSlOnly] = useState(false)
  const [density, setDensity] = useState('comfortable')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [data, setData] = useState({ tiles: [], updated_at: null })
  const [contextReloadAt, setContextReloadAt] = useState(null)
  const [liveUpdatedAt, setLiveUpdatedAt] = useState(null)
  const [selectedSymbol, setSelectedSymbol] = useState(null)
  const [committeeBySymbol, setCommitteeBySymbol] = useState({})
  const [committeeThreadBySymbol, setCommitteeThreadBySymbol] = useState({})
  const [committeeFeed, setCommitteeFeed] = useState([])
  const [exitRecBySymbol, setExitRecBySymbol] = useState({})
  const [reportsBySymbol, setReportsBySymbol] = useState({})

  const fetchIbLive = useCallback(async (tiles, selectedMode) => {
    const symbols = (Array.isArray(tiles) ? tiles : [])
      .map((t) => ({
        symbol: t?.symbol,
        market_type: t?.market_type,
      }))
      .filter((t) => t.symbol)
    if (symbols.length === 0) return null
    const body = {
      mode: selectedMode,
      intraday_interval_minutes: 60,
      window_bars: 120,
      symbols,
    }
    const resp = await fetch(`${API_BASE}/symbol-tracker/ib-live`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!resp.ok) throw new Error(`IB live refresh failed (${resp.status})`)
    return resp.json()
  }, [])

  const runCommitteeCycle = useCallback((nextData) => {
    const nextTiles = Array.isArray(nextData?.tiles) ? nextData.tiles : []
    if (nextTiles.length === 0) {
      setCommitteeBySymbol({})
      setCommitteeThreadBySymbol({})
      setCommitteeFeed([])
      setExitRecBySymbol({})
      setReportsBySymbol({})
      setLiveUpdatedAt(nextData?.updated_at || new Date().toISOString())
      return
    }

    setCommitteeBySymbol((prevCommitteeMap) => {
      const nextCommitteeMap = {}
      const nextExitRecs = {}
      const nextReports = {}
      const feedRows = []
      const threadRowsBySymbol = {}
      for (const tile of nextTiles) {
        const symbol = String(tile?.symbol || '').toUpperCase()
        const prevCommittee = prevCommitteeMap[symbol]
        const liveState = buildLiveState(tile, prevCommittee?.live_state || null, sensitivityMode)
        const committee = evaluateCommittee(tile, liveState, prevCommittee)
        nextCommitteeMap[symbol] = committee

        const exitRec = generateExitRecommendation(tile, liveState, committee)
        nextExitRecs[symbol] = exitRec

        const report = generateSituationalReport(tile, liveState, committee, exitRec)
        nextReports[symbol] = report

        const hasMaterial = isMaterialUpdate(prevCommittee, committee)
        threadRowsBySymbol[symbol] = {
          id: `${symbol}_${Date.now()}_${Math.random()}`,
          ts: committee.updated_at,
          reason: hasMaterial
            ? (committee?.latest_material_changes?.[0]?.reason || 'STATE_SHIFT')
            : 'NO_CHANGE',
          text: hasMaterial
            ? `${committee.committee_stance} (${committee.committee_confidence}) - ${committee.headline_text}`
            : `No material change. ${committee.headline_text}`,
          material: hasMaterial,
        }

        if (hasMaterial) {
          const alert = committee.committee_stance === 'ESCALATE'
            ? 'RED'
            : committee.committee_stance === 'THESIS_INTACT' && committee.top_reason_tags?.includes('IN_PROFIT')
              ? 'GREEN'
              : 'NONE'
          for (const agentMessage of committee.agent_messages || []) {
            if (!agentMessage.change_detected || (agentMessage.materiality_score ?? 0) < 0.55) continue
            feedRows.push({
              id: `${symbol}_${agentMessage.agent_name}_${Date.now()}_${Math.random()}`,
              ts: committee.updated_at,
              symbol,
              market_type: committee.market_type,
              agent: agentMessage.agent_name.replaceAll('_', ' '),
              text: agentMessage.short_text,
              alert,
            })
          }
        }
      }
      setExitRecBySymbol(nextExitRecs)
      setReportsBySymbol(nextReports)
      if (feedRows.length > 0) {
        setCommitteeFeed((prevFeed) => [...feedRows, ...prevFeed].slice(0, 120))
      } else {
        setCommitteeFeed((prevFeed) => {
          const first = prevFeed[0]
          const firstTs = new Date(first?.ts || 0).getTime()
          const nowTs = new Date(nextData?.updated_at || new Date().toISOString()).getTime()
          const recentHeartbeat = first?.agent === 'COMMITTEE' && Number.isFinite(firstTs) && (nowTs - firstTs) < 120000
          if (recentHeartbeat) return prevFeed
          const next = [{
            id: `HEARTBEAT_${Date.now()}`,
            ts: nextData?.updated_at || new Date().toISOString(),
            symbol: 'ALL',
            market_type: '',
            agent: 'COMMITTEE',
            text: 'Cycle checked: no material committee changes.',
            alert: 'NONE',
          }, ...prevFeed]
          return next.slice(0, 120)
        })
      }
      setCommitteeThreadBySymbol((prev) => {
        const next = { ...prev }
        Object.entries(threadRowsBySymbol).forEach(([symbol, row]) => {
          const prior = Array.isArray(next[symbol]) ? next[symbol] : []
          const last = prior[0]
          if (last?.text === row.text && (new Date(row.ts).getTime() - new Date(last.ts || 0).getTime()) < 120000) {
            return
          }
          next[symbol] = [row, ...prior].slice(0, 16)
        })
        return next
      })
      setLiveUpdatedAt(nextData?.updated_at || new Date().toISOString())
      return nextCommitteeMap
    })
  }, [sensitivityMode])

  const loadContext = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const params = new URLSearchParams({
        mode: 'intraday',
        chart_style: 'line',
        horizon_bars: '20',
        projection_mode: 'stitched',
        intraday_interval_minutes: '60',
      })
      const resp = await fetch(`${API_BASE}/symbol-tracker/tiles?${params.toString()}`)
      if (!resp.ok) throw new Error(`Failed to load symbol tracker (${resp.status})`)
      const payload = await resp.json()
      setData(payload)
      setContextReloadAt(new Date().toISOString())
      setLiveUpdatedAt(payload?.updated_at || new Date().toISOString())
      if (!selectedSymbol && payload?.tiles?.[0]?.symbol) {
        setSelectedSymbol(String(payload.tiles[0].symbol).toUpperCase())
      }
      try {
        const ibPayload = await fetchIbLive(payload?.tiles || [], 'intraday')
        const merged = ibPayload ? mergeIbLiveRows(payload, ibPayload) : payload
        setData(merged)
        runCommitteeCycle(merged)
      } catch {
        runCommitteeCycle(payload)
      }
    } catch (e) {
      setError(e.message || 'Failed to load symbol tracker data.')
    } finally {
      setLoading(false)
    }
  }, [fetchIbLive, runCommitteeCycle, selectedSymbol])

  useEffect(() => {
    loadContext()
  }, [loadContext])

  useEffect(() => {
    if (Array.isArray(data?.tiles) && data.tiles.length > 0) {
      runCommitteeCycle(data)
    }
  }, [sensitivityMode]) // eslint-disable-line react-hooks/exhaustive-deps

  const refreshIbOnly = useCallback(async () => {
    try {
      setError('')
      const ibPayload = await fetchIbLive(data?.tiles || [], mode)
      setLiveUpdatedAt(ibPayload?.updated_at || new Date().toISOString())
      if (ibPayload) {
        setData((prev) => {
          const merged = mergeIbLiveRows(prev, ibPayload)
          runCommitteeCycle(merged)
          return merged
        })
      } else {
        runCommitteeCycle(data)
      }
    } catch (e) {
      setLiveUpdatedAt(new Date().toISOString())
      runCommitteeCycle(data)
      setError(e.message || 'IB live refresh failed.')
    }
  }, [data, data?.tiles, fetchIbLive, mode, runCommitteeCycle])

  useVisibleInterval(refreshIbOnly, 30000)

  const tiles = useMemo(() => {
    let rows = Array.isArray(data?.tiles) ? [...data.tiles] : []
    if (longsOnly) rows = rows.filter((t) => t.side === 'LONG')
    if (shortsOnly) rows = rows.filter((t) => t.side === 'SHORT')
    if (activeTpSlOnly) {
      rows = rows.filter((t) => t?.overlays?.take_profit != null || t?.overlays?.stop_loss != null)
    }
    rows.sort((a, b) => {
      if (sortBy === 'best_pnl') return Number(b.unrealized_pnl || 0) - Number(a.unrealized_pnl || 0)
      if (sortBy === 'worst_pnl') return Number(a.unrealized_pnl || 0) - Number(b.unrealized_pnl || 0)
      if (sortBy === 'closest_tp') {
        return Math.abs(Number(a?.progress_metrics?.distance_to_tp_pct ?? 999)) - Math.abs(Number(b?.progress_metrics?.distance_to_tp_pct ?? 999))
      }
      if (sortBy === 'closest_sl') {
        return Math.abs(Number(a?.progress_metrics?.distance_to_sl_pct ?? 999)) - Math.abs(Number(b?.progress_metrics?.distance_to_sl_pct ?? 999))
      }
      if (sortBy === 'newest') {
        return String(b.opened_at || '').localeCompare(String(a.opened_at || ''))
      }
      return 0
    })

    rows = rows.map((row) => {
      const symbol = String(row?.symbol || '').toUpperCase()
      const committee = committeeBySymbol[symbol]
      const exitRec = exitRecBySymbol[symbol]
      const liveState = committee?.live_state
      const momentum = liveState ? computeMomentumGauge(liveState) : null
      const horizon = Number(horizonBars)
      const baseExpectation = row?.expectation || {}
      const trimmedExpectation = Number.isFinite(horizon) && horizon > 0
        ? {
          ...baseExpectation,
          horizon_bars: horizon,
          center_path: (baseExpectation.center_path || []).slice(0, horizon),
          upper_path: (baseExpectation.upper_path || []).slice(0, horizon),
          lower_path: (baseExpectation.lower_path || []).slice(0, horizon),
        }
        : baseExpectation
      return {
        ...row,
        expectation: trimmedExpectation,
        committee,
        exitRecommendation: exitRec,
        momentum,
      }
    })
    return rows
  }, [data?.tiles, longsOnly, shortsOnly, activeTpSlOnly, sortBy, committeeBySymbol, exitRecBySymbol, horizonBars])

  const watchlist = useMemo(() => {
    return Object.values(committeeBySymbol)
      .sort((a, b) => {
        const stanceCmp = severityRank(b.committee_stance) - severityRank(a.committee_stance)
        if (stanceCmp !== 0) return stanceCmp
        const confidenceCmp = confidenceRank(b.committee_confidence) - confidenceRank(a.committee_confidence)
        if (confidenceCmp !== 0) return confidenceCmp
        return String(a.symbol).localeCompare(String(b.symbol))
      })
  }, [committeeBySymbol])

  return (
    <div className="symbol-tracker-page">
      <div className="symbol-tracker-head">
        <div>
          <h2>Trading Command Center</h2>
          <p>Live positions with 30s updates · VWAP · Bollinger · Mean-Rev · Exit signals</p>
          <p style={{ marginTop: 6, fontSize: '0.8rem', color: '#64748b' }}>
            Terms: drawdown <GlossaryHoverCard scope="risk" entryKey="portfolio_drawdown_pct" />, retrigger <GlossaryHoverCard scope="signals" entryKey="retrigger" />, exposure <GlossaryHoverCard scope="positions" entryKey="position_size_pct" />.
          </p>
        </div>
        <div className="symbol-tracker-head-actions">
          <button type="button" className="symbol-tracker-btn" onClick={refreshIbOnly}>Refresh Live Only</button>
          <button type="button" className="symbol-tracker-btn" onClick={loadContext}>Reload Snowflake Context</button>
        </div>
      </div>

      <div className="symbol-tracker-controls">
        <label>
          Sensitivity
          <select value={sensitivityMode} onChange={(e) => setSensitivityMode(e.target.value)}>
            <option value="CONSERVATIVE">Conservative</option>
            <option value="BALANCED">Balanced</option>
            <option value="AGGRESSIVE">Aggressive</option>
          </select>
        </label>
        <label>
          Mode
          <select value={mode} onChange={(e) => setMode(e.target.value)}>
            <option value="intraday">Live Intraday</option>
            <option value="daily">Daily</option>
          </select>
        </label>
        <label>
          Chart
          <select value={chartStyle} onChange={(e) => setChartStyle(e.target.value)}>
            <option value="line">Line</option>
            <option value="candles">Candles</option>
          </select>
        </label>
        <label>
          Horizon
          <select value={horizonBars} onChange={(e) => setHorizonBars(e.target.value)}>
            <option value="1">H1</option>
            <option value="3">H3</option>
            <option value="5">H5</option>
            <option value="10">H10</option>
            <option value="20">H20</option>
          </select>
        </label>
        <label>
          Curve
          <select value={projectionMode} onChange={(e) => setProjectionMode(e.target.value)}>
            <option value="stitched">Horizon-stitched</option>
            <option value="geometric">Geometric</option>
            <option value="linear">Linear</option>
          </select>
        </label>
        <label>
          Trend style
          <select value={trendRender} onChange={(e) => setTrendRender(e.target.value)}>
            <option value="soft">Soft polynomial</option>
            <option value="raw">Raw points</option>
          </select>
        </label>
        <label>
          Sort
          <select value={sortBy} onChange={(e) => setSortBy(e.target.value)}>
            {SORT_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
        </label>
        <label>
          View
          <select value={density} onChange={(e) => setDensity(e.target.value)}>
            <option value="comfortable">Comfortable</option>
            <option value="compact">Compact</option>
          </select>
        </label>
        <label className="symbol-tracker-check">
          <input type="checkbox" checked={longsOnly} onChange={(e) => setLongsOnly(e.target.checked)} />
          Longs only
        </label>
        <label className="symbol-tracker-check">
          <input type="checkbox" checked={shortsOnly} onChange={(e) => setShortsOnly(e.target.checked)} />
          Shorts only
        </label>
        <label className="symbol-tracker-check">
          <input type="checkbox" checked={activeTpSlOnly} onChange={(e) => setActiveTpSlOnly(e.target.checked)} />
          Active TP/SL only
        </label>
      </div>

      {error ? <div className="symbol-tracker-error">{error}</div> : null}
      {loading ? <div className="symbol-tracker-loading">Loading symbol tracker...</div> : null}
      {!loading && tiles.length === 0 ? <div className="symbol-tracker-empty">No open positions found.</div> : null}

      <UrgencyBanner tiles={tiles} />

      {!loading && tiles.length > 0 ? (
        <div className="st-chart-legend-bar">
          <span className="st-legend-item"><span className="st-legend-swatch" style={{ background: '#e879f9' }} />VWAP</span>
          <span className="st-legend-item"><span className="st-legend-swatch" style={{ background: '#38bdf8' }} />Bollinger Bands</span>
          <span className="st-legend-item"><span className="st-legend-swatch" style={{ background: '#22c55e' }} />Support</span>
          <span className="st-legend-item"><span className="st-legend-swatch" style={{ background: '#f87171' }} />Resistance</span>
          <span className="st-legend-item"><span className="st-legend-swatch" style={{ background: '#a78bfa' }} />Entry</span>
          <span className="st-legend-item"><span className="st-legend-swatch" style={{ background: '#10b981' }} />TP</span>
          <span className="st-legend-item"><span className="st-legend-swatch" style={{ background: '#ef4444' }} />SL</span>
        </div>
      ) : null}

      <div className="symbol-tracker-layout">
        <div className={`symbol-tracker-grid ${density === 'compact' ? 'symbol-tracker-grid--compact' : ''}`}>
          {tiles.map((tile) => (
            <Tile
              key={tile.symbol}
              tile={tile}
              mode={mode}
              chartStyle={chartStyle}
              density={density}
              projectionMode={projectionMode}
              trendRender={trendRender}
              formatSymbolLabel={formatSymbolLabel}
              selected={selectedSymbol === tile.symbol}
              onSelect={setSelectedSymbol}
            />
          ))}
        </div>
        <SituationalBriefingPanel
          selectedSymbol={selectedSymbol}
          setSelectedSymbol={setSelectedSymbol}
          reportsBySymbol={reportsBySymbol}
          committeeBySymbol={committeeBySymbol}
          watchlist={watchlist}
          feed={committeeFeed}
          formatSymbolLabel={formatSymbolLabel}
          contextUpdatedAt={contextReloadAt}
          liveUpdatedAt={liveUpdatedAt}
        />
      </div>
    </div>
  )
}
