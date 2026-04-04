/**
 * Pure overlay model for Living Chart: tiers, caps, no React.
 * Data sources: tile payload + optional liveState / committee / exitRec (client-derived).
 */

import { computeChartOverlays } from '../pages/symbolTrackerCommittee'

const COLORS = {
  price: '#60a5fa',
  entry: '#a78bfa',
  tp: '#10b981',
  sl: '#ef4444',
  expectedCenter: '#f59e0b',
  expectedBand: 'rgba(251, 191, 36, 0.28)',
  stopDanger: 'rgba(239, 68, 68, 0.22)',
  targetNear: 'rgba(16, 185, 129, 0.2)',
  meanRev: 'rgba(56, 189, 248, 0.26)',
  thesisWeak: 'rgba(245, 158, 11, 0.28)',
  vwap: '#e879f9',
  bb: '#38bdf8',
  srSupport: '#22c55e',
  srResist: '#f87171',
}

const MAX_CONDITIONAL_ZONES = 3

function toNum(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

export function barStepSeconds(tile) {
  const bs = toNum(tile?.chart?.bar_seconds)
  if (bs != null && bs > 0) return bs
  const mins = toNum(tile?.chart?.interval_minutes)
  if (mins != null && mins > 0) return mins * 60
  return 3600
}

export function barTimeMs(bar) {
  if (!bar?.ts) return null
  const t = new Date(bar.ts).getTime()
  return Number.isFinite(t) ? t : null
}

/** Merge bar arrays by timestamp (last write wins). */
export function mergeBarsByTimestamp(existing, incoming) {
  const a = Array.isArray(existing) ? existing : []
  const b = Array.isArray(incoming) ? incoming : []
  if (b.length === 0) return a
  const map = new Map()
  for (const bar of a) {
    const k = barTimeMs(bar)
    if (k != null) map.set(k, bar)
  }
  for (const bar of b) {
    const k = barTimeMs(bar)
    if (k != null) map.set(k, bar)
  }
  return [...map.entries()].sort(([x], [y]) => x - y).map(([, v]) => v)
}

function geomInterp(a, b, frac) {
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null
  if (a > 0 && b > 0) return a * (b / a) ** frac
  return a + (b - a) * frac
}

/**
 * Forward expectation points in time after last bar (stitched steps).
 * Returns { tMs[], center[], upper[], lower[] } aligned arrays.
 */
export function buildExpectationForwardSeries(tile, bars, horizonBars = 5) {
  const exp = tile?.expectation
  if (!exp?.is_available) return null
  const centerPath = Array.isArray(exp.center_path) ? exp.center_path : []
  const upperPath = Array.isArray(exp.upper_path) ? exp.upper_path : []
  const lowerPath = Array.isArray(exp.lower_path) ? exp.lower_path : []
  const h = Math.max(1, Math.min(Number(horizonBars) || 5, centerPath.length))
  const sliceC = centerPath.slice(0, h)
  if (sliceC.length < 1) return null

  const lastBar = bars[bars.length - 1]
  const t0 = barTimeMs(lastBar)
  if (t0 == null) return null

  const stepMs = barStepSeconds(tile) * 1000
  const entryBaseline = toNum(tile?.overlays?.entry)

  const tMs = []
  const center = []
  const upper = []
  const lower = []

  const densify = 5
  for (let i = 0; i < sliceC.length; i += 1) {
    const prevC = i === 0
      ? (toNum(entryBaseline) ?? toNum(sliceC[0]?.price))
      : toNum(sliceC[i - 1]?.price)
    const prevU = i === 0
      ? (toNum(entryBaseline) ?? toNum(upperPath[0]?.price))
      : toNum(upperPath[i - 1]?.price)
    const prevL = i === 0
      ? (toNum(entryBaseline) ?? toNum(lowerPath[0]?.price))
      : toNum(lowerPath[i - 1]?.price)
    const nextC = toNum(sliceC[i]?.price)
    const nextU = toNum(upperPath[i]?.price)
    const nextL = toNum(lowerPath[i]?.price)

    for (let sub = 1; sub <= densify; sub += 1) {
      const frac = sub / densify
      const dt = ((i * densify + sub) * stepMs)
      tMs.push(t0 + dt)
      center.push(geomInterp(prevC, nextC, frac))
      upper.push(geomInterp(prevU, nextU, frac))
      lower.push(geomInterp(prevL, nextL, frac))
    }
  }

  return { tMs, center, upper, lower }
}

/**
 * Priority-sorted conditional overlays (max MAX_CONDITIONAL_ZONES drawn as shapes).
 */
export function pickConditionalZones(tile, liveState, committee, exitRec, taOverlays) {
  const feats = liveState?.derived_features || {}
  const side = String(tile?.side || 'LONG').toUpperCase()
  const current = toNum(liveState?.last_price) ?? toNum(tile?.current_price) ?? toNum(tile?.overlays?.current)
  const sl = toNum(tile?.overlays?.stop_loss)
  const tp = toNum(tile?.overlays?.take_profit)
  const distSl = feats.distance_to_sl_pct
  const distTp = feats.distance_to_tp_pct

  const candidates = []

  if (sl != null && current != null && distSl != null && Number.isFinite(distSl)) {
    const absDist = Math.abs(distSl)
    if (absDist < 0.028) {
      const tight = absDist < 0.01
      candidates.push({
        key: 'stop_danger',
        priority: tight ? 100 : 72,
        y0: Math.min(current, sl),
        y1: Math.max(current, sl),
        fillcolor: tight ? 'rgba(239, 68, 68, 0.5)' : 'rgba(239, 68, 68, 0.28)',
        line: { color: '#fecaca', width: tight ? 2.75 : 1.75, dash: 'dot' },
        label: 'Near stop',
        layer: 'above',
      })
    }
  }

  if (tp != null && current != null && distTp != null && Number.isFinite(distTp)) {
    const absDist = Math.abs(distTp)
    if (absDist < 0.022) {
      const tight = absDist < 0.006
      candidates.push({
        key: 'target_near',
        priority: tight ? 90 : 62,
        y0: Math.min(current, tp),
        y1: Math.max(current, tp),
        fillcolor: tight ? 'rgba(16, 185, 129, 0.45)' : 'rgba(16, 185, 129, 0.26)',
        line: { color: '#bbf7d0', width: tight ? 2.75 : 1.75, dash: 'dot' },
        label: 'Near target',
        layer: 'above',
      })
    }
  }

  const meanRev =
    (feats.deviation_from_h5_lower_band != null && feats.deviation_from_h5_lower_band < -0.008)
    || (taOverlays?.bollinger?.lower?.length > 0 && current != null
      && side === 'LONG'
      && current < toNum(taOverlays.bollinger.lower[taOverlays.bollinger.lower.length - 1]))

  if (meanRev) {
    candidates.push({
      key: 'mean_reversion',
      priority: 55,
      kind: 'hline',
      y: taOverlays?.bollinger?.lower?.length > 0
        ? toNum(taOverlays.bollinger.lower[taOverlays.bollinger.lower.length - 1])
        : current * 0.998,
      fillcolor: COLORS.meanRev,
      line: { color: '#67e8f9', width: 3.4, dash: '8px,4px' },
      label: 'Mean reversion',
      layer: 'above',
    })
  }

  const thesis = String(tile?.thesis?.status || '').toUpperCase()
  const stance = String(committee?.committee_stance || '').toUpperCase()
  const devMed = feats.deviation_from_h5_median
  const pathDrift =
    feats.inside_cone === false
    && devMed != null
    && Number.isFinite(devMed)
    && Math.abs(devMed) > 0.01

  const weakThesis =
    thesis.includes('WEAK')
    || thesis.includes('BROKEN')
    || stance === 'ESCALATE'
    || stance === 'RISK_OFF'
    || pathDrift

  if (weakThesis && current != null) {
    candidates.push({
      key: 'thesis_weakening',
      priority: pathDrift ? 68 : 65,
      y0: current * (side === 'SHORT' ? 1.002 : 0.998),
      y1: current * (side === 'SHORT' ? 0.998 : 1.002),
      fillcolor: pathDrift ? 'rgba(245, 158, 11, 0.38)' : 'rgba(245, 158, 11, 0.26)',
      line: { color: 'rgba(251, 191, 36, 0.92)', width: pathDrift ? 2.25 : 1.4, dash: '4px,2px' },
      label: 'Thesis pressure',
      layer: 'above',
      pathDrift,
    })
  }

  candidates.sort((a, b) => b.priority - a.priority)
  return candidates.slice(0, MAX_CONDITIONAL_ZONES)
}

function shapeForHorizontalBand(y0, y1, x0, x1, fillcolor, line, layer = 'below') {
  if (!Number.isFinite(y0) || !Number.isFinite(y1)) return null
  return {
    type: 'rect',
    xref: 'x',
    yref: 'y',
    x0,
    x1,
    y0: Math.min(y0, y1),
    y1: Math.max(y0, y1),
    fillcolor,
    line: line || { width: 0 },
    layer: layer || 'below',
  }
}

function hLineShape(y, x0, x1, color, dash, opts = {}) {
  if (!Number.isFinite(y)) return null
  const { width = 1.5, layer = 'below' } = opts
  return {
    type: 'line',
    xref: 'x',
    yref: 'y',
    x0,
    x1,
    y0: y,
    y1: y,
    line: { color, width, dash: dash || 'dash' },
    layer: layer || 'below',
  }
}

/** Short labels at the right edge of the tape for entry / stop / target. */
export function buildPersistentLevelAnnotations(xRightMs, entry, sl, tp) {
  if (xRightMs == null || !Number.isFinite(Number(xRightMs))) return []
  const xr = Number(xRightMs)
  const base = {
    xref: 'x',
    yref: 'y',
    x: xr,
    showarrow: false,
    xanchor: 'left',
    font: { size: 11 },
  }
  const out = []
  if (entry != null && Number.isFinite(entry)) {
    out.push({
      ...base,
      y: entry,
      xshift: 6,
      yshift: -10,
      text: 'Entry',
      font: { ...base.font, color: '#c4b5fd' },
    })
  }
  if (sl != null && Number.isFinite(sl)) {
    out.push({
      ...base,
      y: sl,
      xshift: 6,
      yshift: 10,
      text: 'Stop',
      font: { ...base.font, color: '#f87171', size: 12 },
    })
  }
  if (tp != null && Number.isFinite(tp)) {
    out.push({
      ...base,
      y: tp,
      xshift: 6,
      yshift: -4,
      text: 'Target',
      font: { ...base.font, color: '#4ade80' },
    })
  }
  return out
}

/** Minimal labels at forward horizon for expected path / band (non-expert). */
export function buildExpectationHintAnnotations(tile, bars, horizonBars) {
  const fwd = buildExpectationForwardSeries(tile, bars, horizonBars)
  if (!fwd?.tMs?.length) return []
  const n = fwd.tMs.length - 1
  const te = fwd.tMs[n]
  const yc = fwd.center[n]
  const yu = fwd.upper[n]
  if (!Number.isFinite(yc) || !Number.isFinite(te)) return []
  return [
    {
      xref: 'x',
      yref: 'y',
      x: te,
      y: yc,
      text: 'Expected',
      showarrow: false,
      xanchor: 'left',
      xshift: 8,
      yshift: 0,
      font: { size: 10, color: 'rgba(251, 191, 36, 0.95)' },
    },
    {
      xref: 'x',
      yref: 'y',
      x: te,
      y: yu,
      text: 'Range',
      showarrow: false,
      xanchor: 'left',
      xshift: 8,
      yshift: -6,
      font: { size: 9, color: 'rgba(148, 163, 184, 0.9)' },
    },
  ]
}

/** Subtle "Now" label at last bar (structural read). */
export function buildNowLabelAnnotation(xLastMs, yPrice) {
  if (xLastMs == null || !Number.isFinite(Number(xLastMs)) || yPrice == null || !Number.isFinite(yPrice)) {
    return []
  }
  return [{
    xref: 'x',
    yref: 'y',
    x: Number(xLastMs),
    y: yPrice,
    text: 'Now',
    showarrow: false,
    xanchor: 'left',
    xshift: 14,
    yshift: -18,
    font: { size: 11, color: '#f8fafc', family: 'system-ui, sans-serif' },
  }]
}

/**
 * Build Plotly shapes + optional extra traces for advanced TA.
 */
export function buildLivingChartShapesAndTA({
  tile,
  bars,
  liveState,
  committee,
  exitRec,
  showAdvancedTA,
  horizonBars,
  flowBurst = null,
}) {
  const tMs = bars.map(barTimeMs).filter((x) => x != null)
  if (tMs.length === 0) return { shapes: [], taTraces: [], annotations: [], conditionalKeys: [] }

  const x0 = tMs[0]
  const x1 = tMs[tMs.length - 1]
  const span = Math.max(x1 - x0, 60000)

  const shapes = []
  const annotations = []
  const taTraces = []

  const entry = toNum(tile?.overlays?.entry)
  const tp = toNum(tile?.overlays?.take_profit)
  const sl = toNum(tile?.overlays?.stop_loss)

  const addHLine = (y, color, dash, lineOpts = {}) => {
    const s = hLineShape(y, x0 - span * 0.02, x1 + span * 0.35, color, dash, { layer: 'below', ...lineOpts })
    if (s) shapes.push(s)
  }

  /* Structural levels: cooler, thinner — distinct from temporal conditional zones. */
  if (entry != null) addHLine(entry, '#8b7eb8', '8px,5px', { width: 1.05 })
  if (tp != null) addHLine(tp, '#15803d', '5px,4px', { width: 1.65 })
  if (sl != null) addHLine(sl, '#b91c1c', '5px,4px', { width: 1.85 })

  let taOverlays = null
  if (showAdvancedTA && bars.length >= 8) {
    taOverlays = computeChartOverlays(bars)
    const idx = bars.map((_, i) => i)
    const vwapY = (taOverlays.vwap || []).map((y, i) => (Number.isFinite(y) ? [tMs[i], y] : null)).filter(Boolean)
    if (vwapY.length > 1) {
      taTraces.push({
        type: 'scatter',
        mode: 'lines',
        name: 'VWAP',
        x: vwapY.map((p) => p[0]),
        y: vwapY.map((p) => p[1]),
        line: { color: COLORS.vwap, width: 1.5, dash: '6px,3px' },
        hoverinfo: 'y+name',
      })
    }
    const bbU = (taOverlays.bollinger?.upper || []).map((y, i) => (Number.isFinite(y) ? [tMs[i], y] : null)).filter(Boolean)
    const bbL = (taOverlays.bollinger?.lower || []).map((y, i) => (Number.isFinite(y) ? [tMs[i], y] : null)).filter(Boolean)
    if (bbU.length > 1) {
      taTraces.push({
        type: 'scatter',
        mode: 'lines',
        name: 'BB upper',
        x: bbU.map((p) => p[0]),
        y: bbU.map((p) => p[1]),
        line: { color: COLORS.bb, width: 1, dash: '3px,3px' },
        hoverinfo: 'y+name',
      })
    }
    if (bbL.length > 1) {
      taTraces.push({
        type: 'scatter',
        mode: 'lines',
        name: 'BB lower',
        x: bbL.map((p) => p[0]),
        y: bbL.map((p) => p[1]),
        line: { color: COLORS.bb, width: 1, dash: '3px,3px' },
        hoverinfo: 'y+name',
      })
    }
    const sup = taOverlays.sr?.support
    const res = taOverlays.sr?.resistance
    if (sup != null) addHLine(sup, COLORS.srSupport, '8px,4px', { width: 1.2 })
    if (res != null) addHLine(res, COLORS.srResist, '8px,4px', { width: 1.2 })
  } else if (!showAdvancedTA) {
    taOverlays = null
  }

  const zones = pickConditionalZones(tile, liveState, committee, exitRec, taOverlays)
  const conditionalKeys = zones.map((z) => z.key)
  const xPad = span * 0.02
  const xAnnot = x1 + span * 0.02
  const levelAnn = buildPersistentLevelAnnotations(xAnnot, entry, sl, tp)
  for (const a of levelAnn) {
    annotations.push(a)
  }

  for (const a of buildExpectationHintAnnotations(tile, bars, horizonBars)) {
    annotations.push(a)
  }

  const lastBar = bars[bars.length - 1]
  const lastClose = toNum(lastBar?.close) ?? toNum(lastBar?.low) ?? toNum(lastBar?.high)
  const spot = toNum(liveState?.last_price) ?? toNum(tile?.current_price) ?? lastClose
  for (const a of buildNowLabelAnnotation(x1, spot)) {
    annotations.push(a)
  }

  /* Temporal zones: clip x-extent to recent tape so they read as “live” bands, not full-history furniture. */
  const stepMs = barStepSeconds(tile) * 1000
  const temporalWindowMs = stepMs * 56
  const xTemporalLeft = Math.max(x0 - xPad, x1 - temporalWindowMs)
  const xTemporalRight = x1 + span * 0.18

  for (const z of zones) {
    const zLayer = z.layer || 'above'
    if (z.kind === 'hline' && z.y != null) {
      const lw = z.line?.width ?? 2
      const s = hLineShape(z.y, xTemporalLeft, xTemporalRight, z.line.color, z.line.dash, {
        width: lw,
        layer: zLayer,
      })
      if (s) shapes.push(s)
      continue
    }
    if (z.y0 != null && z.y1 != null) {
      const sh = shapeForHorizontalBand(z.y0, z.y1, xTemporalLeft, xTemporalRight, z.fillcolor, z.line, zLayer)
      if (sh) shapes.push(sh)
    }
  }


  /* v1: single subtle confirmed-burst highlight (flow intelligence) */
  if (
    flowBurst
    && flowBurst.x0_ms != null
    && flowBurst.x1_ms != null
    && flowBurst.y0 != null
    && flowBurst.y1 != null
  ) {
    const xA = Number(flowBurst.x0_ms)
    const xB = Number(flowBurst.x1_ms)
    const yA = Number(flowBurst.y0)
    const yB = Number(flowBurst.y1)
    if (Number.isFinite(xA) && Number.isFinite(xB) && Number.isFinite(yA) && Number.isFinite(yB)) {
      shapes.push({
        type: 'rect',
        xref: 'x',
        yref: 'y',
        x0: Math.min(xA, xB),
        x1: Math.max(xA, xB),
        y0: Math.min(yA, yB),
        y1: Math.max(yA, yB),
        fillcolor: 'rgba(251, 191, 36, 0.07)',
        line: { width: 0 },
        layer: 'below',
      })
    }
  }

  return { shapes, taTraces, annotations, conditionalKeys }
}

export function buildStatusChips(tile, exitRec, liveState, conditionalKeys) {
  const chips = []
  const urg = exitRec?.urgency
  if (urg && urg !== 'HOLD') {
    chips.push({ key: 'urgency', label: urg.replace(/_/g, ' '), tone: urg === 'EXIT_NOW' ? 'bad' : urg === 'PREPARE' ? 'warn' : 'info' })
  }
  for (const k of conditionalKeys || []) {
    if (k === 'stop_danger') chips.push({ key: k, label: 'Near stop', tone: 'bad' })
    if (k === 'target_near') chips.push({ key: k, label: 'Near target', tone: 'good' })
    if (k === 'mean_reversion') chips.push({ key: k, label: 'Mean reversion', tone: 'info' })
    if (k === 'thesis_weakening') chips.push({ key: k, label: 'Thesis pressure', tone: 'warn' })
  }
  const inside = liveState?.derived_features?.inside_cone
  if (inside === false) {
    chips.push({ key: 'outside_cone', label: 'Outside expected band', tone: 'warn' })
  }
  return chips
}

export { COLORS as LIVING_CHART_COLORS }
