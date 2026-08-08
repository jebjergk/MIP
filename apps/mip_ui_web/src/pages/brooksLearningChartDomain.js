/** G2.1 — Y-domain, fit windows, and axis ticks for the learning chart. */

import { normalizeBarTs } from './brooksLearningBarSelection'

const MS_PER_BAR = 5 * 60 * 1000

/** Whether a price is close enough to the visible candle range to affect scaling/lines. */
export function priceNearCandleRange(candleLo, candleHi, price, extensionPct = 0.12) {
  if (price == null || !Number.isFinite(Number(price))) return false
  const p = Number(price)
  const span = Math.max(candleHi - candleLo, 0.01)
  const pad = span * extensionPct
  return p >= candleLo - pad && p <= candleHi + pad
}

export function barOverlayPrices(bar, candleLo, candleHi, extensionPct = 0.12) {
  const out = []
  if (!bar) return out
  const near = (p) => priceNearCandleRange(candleLo, candleHi, p, extensionPct)
  if (bar.active_stop != null && near(bar.active_stop)) {
    out.push(Number(bar.active_stop))
  }
  for (const lv of bar.confirmation_levels || []) {
    if (lv?.level != null && near(lv.level)) out.push(Number(lv.level))
  }
  for (const lv of bar.invalidation_levels || []) {
    if (lv?.level != null && near(lv.level)) out.push(Number(lv.level))
  }
  return out
}

export function candlePriceExtents(bars) {
  let lo = Infinity
  let hi = -Infinity
  for (const b of bars || []) {
    if (b.low != null) lo = Math.min(lo, Number(b.low))
    if (b.high != null) hi = Math.max(hi, Number(b.high))
    if (b.open != null) {
      lo = Math.min(lo, Number(b.open))
      hi = Math.max(hi, Number(b.open))
    }
    if (b.close != null) {
      lo = Math.min(lo, Number(b.close))
      hi = Math.max(hi, Number(b.close))
    }
    const ohlc = b.ohlc
    if (ohlc && typeof ohlc === 'object') {
      for (const k of ['o', 'open', 'h', 'high', 'l', 'low', 'c', 'close']) {
        if (ohlc[k] != null && Number.isFinite(Number(ohlc[k]))) {
          lo = Math.min(lo, Number(ohlc[k]))
          hi = Math.max(hi, Number(ohlc[k]))
        }
      }
    }
  }
  if (!Number.isFinite(lo) || !Number.isFinite(hi)) {
    return { lo: 0, hi: 1 }
  }
  for (const b of bars || []) {
    for (const p of barOverlayPrices(b, lo, hi)) {
      lo = Math.min(lo, p)
      hi = Math.max(hi, p)
    }
  }
  return { lo, hi }
}

/** Include overlay/stop prices only when near the visible candle range (not distant dossier levels). */
export function overlayPricesNearCandles(candleLo, candleHi, overlays, extensionPct = 0.12) {
  const span = Math.max(candleHi - candleLo, 0.01)
  const pad = span * extensionPct
  const minP = candleLo - pad
  const maxP = candleHi + pad
  const near = (p) => p != null && p >= minP && p <= maxP

  const out = []
  if (near(overlays?.entry?.price)) out.push(Number(overlays.entry.price))
  if (near(overlays?.exit?.price)) out.push(Number(overlays.exit.price))
  if (near(overlays?.initial_stop)) out.push(Number(overlays.initial_stop))
  for (const st of overlays?.stop_steps || []) {
    if (near(st.stop_price)) out.push(Number(st.stop_price))
  }
  return out
}

export function computeYDomain(visibleBars, overlays, paddingPct = 0.06) {
  const { lo: cLo, hi: cHi } = candlePriceExtents(visibleBars)
  let lo = cLo
  let hi = cHi
  for (const p of overlayPricesNearCandles(cLo, cHi, overlays)) {
    lo = Math.min(lo, p)
    hi = Math.max(hi, p)
  }
  const span = Math.max(hi - lo, 0.01)
  const pctPad = span * paddingPct
  const roughTick = span / 6
  const tickMag = 10 ** Math.floor(Math.log10(Math.max(roughTick, 1e-6)))
  const tickStep = roughTick / tickMag >= 5 ? tickMag * 5 : roughTick / tickMag >= 2 ? tickMag * 2 : tickMag
  const pad = Math.max(pctPad, tickStep * 0.55, span * 0.02)
  return { lo: lo - pad, hi: hi + pad }
}

export function dossierLevelsWithVisibility(levels, domainLo, domainHi) {
  if (!levels) return []
  const labels = {
    support: 'Support',
    resistance: 'Resistance',
    reclaim: 'Reclaim',
    do_not_chase: 'Do not chase',
    daily_thesis_invalidation: 'Thesis invalidation',
  }
  return Object.entries(labels)
    .filter(([k]) => levels[k] != null)
    .map(([k, label]) => {
      const price = Number(levels[k])
      const inView = price >= domainLo && price <= domainHi
      return {
        key: k,
        label,
        price,
        inView,
        offScreenHint: inView ? null : `${label} ${price < domainLo ? 'below' : 'above'} visible range`,
      }
    })
}

export function sliceBarsForFitMode(allBars, fitMode, tradeSummary) {
  const bars = allBars || []
  if (!bars.length || fitMode === 'session') {
    return { bars, windowLabel: 'Full session' }
  }
  if (fitMode === 'trade' && tradeSummary?.entry_ts && tradeSummary?.exit_ts) {
    const entryKey = normalizeBarTs(tradeSummary.entry_ts)
    const exitKey = normalizeBarTs(tradeSummary.exit_ts)
    const entryMs = Date.parse(entryKey)
    const exitMs = Date.parse(exitKey)
    const fromMs = entryMs - 30 * 60 * 1000
    const toMs = exitMs + 30 * 60 * 1000
    const sliced = bars.filter((b) => {
      const t = Date.parse(normalizeBarTs(b.ts_utc))
      return t >= fromMs && t <= toMs
    })
    return {
      bars: sliced.length ? sliced : bars,
      windowLabel: 'Entry −30m → exit +30m',
    }
  }
  return { bars, windowLabel: 'Full session' }
}

export function generatePriceTicks(lo, hi, targetCount = 6) {
  const span = hi - lo
  if (span <= 0) return [lo, hi]
  const rough = span / targetCount
  const mag = 10 ** Math.floor(Math.log10(rough))
  const step = rough / mag >= 5 ? mag * 5 : rough / mag >= 2 ? mag * 2 : mag
  const start = Math.ceil(lo / step) * step
  const ticks = []
  for (let p = start; p <= hi + step * 0.001; p += step) {
    ticks.push(Number(p.toFixed(4)))
  }
  if (!ticks.length) ticks.push(lo, hi)
  return ticks
}

export function formatAxisPrice(price, { stopPrecision = false } = {}) {
  if (stopPrecision || Math.abs(price - Math.round(price * 1000) / 1000) > 0.0001) {
    return price.toFixed(3)
  }
  return price.toFixed(2)
}

function nyMinutes(tsNy) {
  const s = String(tsNy || '').replace(' ', 'T')
  const hm = s.slice(11, 16)
  if (!hm.includes(':')) return null
  const [h, m] = hm.split(':').map(Number)
  return h * 60 + m
}

/** X-axis ticks every 30 minutes (NY), deduped. */
export function generateTimeTicksNy(bars, intervalMin = 30) {
  const seen = new Set()
  const ticks = []
  for (const b of bars || []) {
    const mins = nyMinutes(b.ts_ny)
    if (mins == null) continue
    if (mins % intervalMin !== 0 && mins !== 9 * 60 + 30) continue
    const label = String(b.ts_ny || '').replace(' ', 'T').slice(11, 16)
    if (!label || seen.has(label)) continue
    seen.add(label)
    ticks.push({ label, barTs: normalizeBarTs(b.ts_utc), index: ticks.length })
  }
  // re-index with bar index in array
  return (bars || [])
    .map((b, i) => {
      const label = String(b.ts_ny || '').replace(' ', 'T').slice(11, 16)
      const mins = nyMinutes(b.ts_ny)
      if (!label) return null
      const onHalfHour = mins != null && mins % intervalMin === 0
      const isOpen = mins === 9 * 60 + 30
      if (!onHalfHour && !isOpen) return null
      return { label, index: i, barTs: normalizeBarTs(b.ts_utc) }
    })
    .filter(Boolean)
    .filter((t, i, arr) => arr.findIndex((x) => x.label === t.label) === i)
}

export function chartInnerWidth(barCount, containerWidth, minSlot = 10) {
  const plotMin = Math.max(containerWidth, barCount * minSlot)
  return plotMin
}

export function barIndexForTs(bars, ts) {
  const key = normalizeBarTs(ts)
  return (bars || []).findIndex((b) => normalizeBarTs(b.ts_utc) === key)
}

export function msOffsetBars(minutes) {
  return (minutes / 5) * MS_PER_BAR
}
