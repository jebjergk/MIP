/**
 * Flow Intelligence — bar-derived, advisory heuristics (v1).
 * Move efficiency is the anchor; direction + cautions derive from it.
 */

export const FLOW_STRIP_PREFIX_FLOW = 'Flow:'
export const FLOW_STRIP_PREFIX_MOVE_QUALITY = 'Move quality:'

const EPS = 1e-9

function toNum(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function barTimeMs(bar) {
  if (!bar?.ts) return null
  const t = new Date(bar.ts).getTime()
  return Number.isFinite(t) ? t : null
}

function median(arr) {
  const a = arr.filter((x) => Number.isFinite(x)).sort((x, y) => x - y)
  if (a.length === 0) return null
  const m = Math.floor(a.length / 2)
  return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2
}

function mean(arr) {
  const a = arr.filter((x) => Number.isFinite(x))
  if (a.length === 0) return null
  return a.reduce((s, x) => s + x, 0) / a.length
}

function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x))
}

export const FLOW_TUNING = {
  default: {
    w_short: 6,
    w_mid: 16,
    w_long: 48,
    min_bars: 24,
    burst_ratio: 2.0,
    burst_min_consecutive: 2,
    spike_ratio: 2.8,
    activity_elevated: 1.35,
    eff_ratio_efficient: 0.22,
    eff_ratio_inefficient: 0.09,
    effort_floor: 0.0012,
    pressure_theta: 0.18,
    pressure_mixed: 0.08,
    unwind_giveback: 0.32,
    unwind_giveback_high: 0.48,
    liquidity_flip_ratio: 0.55,
    chop_tr_mult: 1.65,
    hysteresis_confirm: 2,
    hysteresis_decay: 2,
    burst_overlay_confirm: 2,
    annotation_confirm_refreshes: 2,
  },
  equities: {},
  FX: {
    eff_ratio_efficient: 0.18,
    eff_ratio_inefficient: 0.07,
    pressure_theta: 0.12,
  },
  ETF: {},
  high_volatility: {
    burst_ratio: 2.35,
    eff_ratio_efficient: 0.26,
    eff_ratio_inefficient: 0.1,
  },
}

function mergeTuning(profile) {
  const base = { ...FLOW_TUNING.default }
  const extra = FLOW_TUNING[profile] && Object.keys(FLOW_TUNING[profile]).length ? FLOW_TUNING[profile] : {}
  return { ...base, ...extra }
}

export function selectFlowTuningProfile(tile) {
  const mt = String(tile?.market_type || '').toUpperCase()
  if (mt === 'FX' || mt === 'CASH' || mt === 'FOREX') return 'FX'
  if (mt === 'ETF') return 'ETF'
  if (mt === 'STOCK') return 'equities'
  return 'default'
}

function trueRange(h, l, prevC) {
  if (!Number.isFinite(h) || !Number.isFinite(l)) return null
  if (!Number.isFinite(prevC)) return h - l
  return Math.max(h - l, Math.abs(h - prevC), Math.abs(l - prevC))
}

export function normalizeSeries(bars) {
  const list = Array.isArray(bars) ? bars : []
  const tMs = []
  const o = []
  const h = []
  const l = []
  const c = []
  const v = []
  let volCount = 0
  for (let i = 0; i < list.length; i += 1) {
    const b = list[i]
    const tm = barTimeMs(b)
    if (tm == null) continue
    tMs.push(tm)
    o.push(toNum(b.open))
    h.push(toNum(b.high))
    l.push(toNum(b.low))
    c.push(toNum(b.close))
    const vv = toNum(b.volume)
    v.push(vv)
    if (vv != null && vv > 0) volCount += 1
  }
  const hasVolume = volCount >= Math.max(8, Math.floor(tMs.length * 0.25))
  const tr = []
  for (let i = 0; i < tMs.length; i += 1) {
    const prevC = i > 0 ? c[i - 1] : o[i]
    tr.push(trueRange(h[i], l[i], prevC))
  }
  return { tMs, o, h, l, c, v, tr, hasVolume, bars: list }
}

function barStepSeconds(tile) {
  const bs = toNum(tile?.chart?.bar_seconds)
  if (bs != null && bs > 0) return bs
  const mins = toNum(tile?.chart?.interval_minutes)
  if (mins != null && mins > 0) return mins * 60
  return 3600
}

function freshnessConfidence(tMs, tile, liveUpdatedAt) {
  if (!tMs.length) return 'UNAVAILABLE'
  const last = tMs[tMs.length - 1]
  const step = barStepSeconds(tile) * 1000 * 2.5
  const now = Date.now()
  if (now - last > step * 4) return 'MEDIUM'
  if (liveUpdatedAt) {
    const lu = new Date(liveUpdatedAt).getTime()
    if (Number.isFinite(lu) && now - lu > 120000) return 'MEDIUM'
  }
  return 'HIGH'
}

function detectBurst(series, T) {
  const { tMs, c, tr, v, hasVolume } = series
  const n = tMs.length
  if (n < T.min_bars) {
    return {
      confirmed: false,
      window_start_idx: null,
      window_end_idx: null,
      direction: 0,
      pending_single_bar: false,
      activity_baseline: EPS,
    }
  }
  const wL = Math.min(T.w_long, n - 1)
  const act = []
  for (let i = 0; i < n; i += 1) {
    if (hasVolume && v[i] != null && v[i] >= 0) {
      act.push(Math.log1p(Math.max(0, v[i])))
    } else if (tr[i] != null && c[i]) {
      act.push(tr[i] / (Math.abs(c[i]) + EPS))
    } else {
      act.push(0)
    }
  }
  const baselineWindow = act.slice(Math.max(0, n - wL), n)
  const base = median(baselineWindow) || EPS

  let bestRun = { len: 0, start: null, end: null }
  let i = n - 1
  while (i >= 0) {
    if ((act[i] / (base + EPS)) >= T.burst_ratio) {
      let j = i
      while (j >= 0 && (act[j] / (base + EPS)) >= T.burst_ratio) j -= 1
      const runLen = i - j
      if (runLen > bestRun.len) bestRun = { len: runLen, start: j + 1, end: i }
      i = j
    } else {
      i -= 1
    }
  }

  const confirmed = bestRun.len >= T.burst_min_consecutive
  let direction = 0
  if (confirmed && bestRun.start != null && bestRun.end != null) {
    const c0 = c[bestRun.start]
    const c1 = c[bestRun.end]
    if (Number.isFinite(c0) && Number.isFinite(c1) && c0 !== 0) {
      direction = Math.sign(c1 - c0)
    }
  }

  const last3 = act.slice(Math.max(0, n - 3), n)
  const maxRat = last3.length ? Math.max(...last3.map((a) => a / (base + EPS))) : 0
  const pending_single_bar = !confirmed && maxRat >= T.spike_ratio

  return {
    confirmed,
    window_start_idx: confirmed ? bestRun.start : null,
    window_end_idx: confirmed ? bestRun.end : null,
    direction,
    pending_single_bar,
    activity_baseline: base,
  }
}

function computeEfficiencyAndPressure(series, T) {
  const { c, tr, h, l, tMs } = series
  const n = tMs.length
  const ws = Math.min(T.w_short, n - 1)
  const wm = Math.min(T.w_mid, n - 1)
  if (ws < 2 || wm < 2) {
    return {
      efficiency_ratio: null,
      move_efficiency: 'UNAVAILABLE',
      pressure_score: 0,
      flow_direction: 'UNAVAILABLE',
      r_net_mid: 0,
    }
  }
  const c0 = c[n - 1 - ws]
  const c1 = c[n - 1]
  const net_short =
    Number.isFinite(c0) && Number.isFinite(c1) && Math.abs(c0) > EPS ? (c1 - c0) / Math.abs(c0) : null
  let sumTr = 0
  for (let j = n - ws; j < n; j += 1) {
    if (tr[j] != null) sumTr += tr[j]
  }
  const effort = Number.isFinite(c1) && c1 !== 0 ? sumTr / Math.abs(c1) : null
  const efficiency_ratio =
    net_short != null && effort != null && effort > EPS ? Math.abs(net_short) / (effort + EPS) : null

  let move_efficiency = 'NORMAL'
  if (efficiency_ratio == null) move_efficiency = 'UNAVAILABLE'
  else if (efficiency_ratio >= T.eff_ratio_efficient && (effort || 0) >= T.effort_floor) {
    move_efficiency = 'EFFICIENT'
  } else if (efficiency_ratio <= T.eff_ratio_inefficient && (effort || 0) >= T.effort_floor) {
    move_efficiency = 'INEFFICIENT'
  }

  const cStart = c[n - 1 - wm]
  const r_net =
    Number.isFinite(cStart) && Number.isFinite(c1) && Math.abs(cStart) > EPS ? (c1 - cStart) / Math.abs(cStart) : 0
  let up = 0
  let clvSum = 0
  let clvN = 0
  for (let j = n - wm; j < n; j += 1) {
    if (j > 0 && c[j] != null && c[j - 1] != null && c[j] > c[j - 1]) up += 1
    const hi = h[j]
    const lo = l[j]
    const cc = c[j]
    if (Number.isFinite(hi) && Number.isFinite(lo) && hi > lo + EPS && Number.isFinite(cc)) {
      clvSum += (cc - lo) / (hi - lo)
      clvN += 1
    }
  }
  const adv_ratio = wm > 0 ? up / wm : 0.5
  const clv = clvN > 0 ? clvSum / clvN : 0.5
  const pressure_score =
    clamp(0.45 * Math.sign(r_net) * Math.min(Math.abs(r_net) / 0.025, 1), -1, 1)
    + clamp(0.35 * (adv_ratio - 0.5) * 2, -0.35, 0.35)
    + clamp(0.2 * (clv - 0.5) * 2, -0.2, 0.2)

  let flow_direction = 'MIXED'
  if (Math.abs(pressure_score) < T.pressure_mixed) flow_direction = 'NEUTRAL'
  else if (pressure_score > T.pressure_theta) flow_direction = 'BUY_DOMINANT'
  else if (pressure_score < -T.pressure_theta) flow_direction = 'SELL_DOMINANT'

  return {
    efficiency_ratio,
    move_efficiency,
    pressure_score,
    flow_direction,
    r_net_mid: r_net,
  }
}

function meanActivityRatio(series, burstBase) {
  const { tr, v, c, hasVolume, tMs } = series
  const n = tMs.length
  const act = []
  for (let i = 0; i < n; i += 1) {
    if (hasVolume && v[i] != null && v[i] >= 0) act.push(Math.log1p(Math.max(0, v[i])))
    else if (tr[i] != null && c[i]) act.push(tr[i] / (Math.abs(c[i]) + EPS))
    else act.push(0)
  }
  const w = Math.min(5, n)
  const slice = act.slice(n - w, n)
  const base = burstBase || EPS
  const ratios = slice.map((a) => a / (base + EPS))
  return mean(ratios) || 0
}

function upperWickBias(series, wm) {
  const { h, l, c, o, tMs } = series
  const n = tMs.length
  let sum = 0
  let cnt = 0
  const from = Math.max(0, n - wm)
  for (let i = from; i < n; i += 1) {
    const bodyTop = Math.max(o[i] ?? c[i], c[i])
    const bodyBot = Math.min(o[i] ?? c[i], c[i])
    if (!Number.isFinite(h[i]) || !Number.isFinite(l[i]) || h[i] <= l[i] + EPS) continue
    const range = h[i] - l[i]
    const upper = h[i] - bodyTop
    const lower = bodyBot - l[i]
    sum += (upper - lower) / range
    cnt += 1
  }
  return cnt > 0 ? sum / cnt : 0
}

export function computeFlowIntelligenceRaw({ bars, tile, liveState, liveUpdatedAt }) {
  const profile = selectFlowTuningProfile(tile)
  const T = mergeTuning(profile)
  const series = normalizeSeries(bars)
  const n = series.tMs.length

  if (n < T.min_bars) {
    return {
      tuning_profile: profile,
      confidence: 'UNAVAILABLE',
      flow_direction: 'UNAVAILABLE',
      move_efficiency: 'UNAVAILABLE',
      absorption_state: 'UNAVAILABLE',
      exhaustion_state: 'UNAVAILABLE',
      unwind_risk: 'UNAVAILABLE',
      liquidity_stress: 'UNAVAILABLE',
      burst: { confirmed: false, pending_single_bar: false, window_start_idx: null, window_end_idx: null },
      scores: {},
      primary_quality_key: 'none',
      primary_quality_priority: 0,
      burst_overlay_eligible_raw: false,
      burst_y0: null,
      burst_y1: null,
      burst_x0_ms: null,
      burst_x1_ms: null,
      _pending_single_bar: false,
    }
  }

  const burst = detectBurst(series, T)
  const eff = computeEfficiencyAndPressure(series, T)
  let confidence = freshnessConfidence(series.tMs, tile, liveUpdatedAt)
  if (!series.hasVolume && confidence === 'HIGH') confidence = 'MEDIUM'

  const actMean = meanActivityRatio(series, burst.activity_baseline)
  const elevated = actMean >= T.activity_elevated

  let absorption_state = 'NONE'
  if (
    elevated
    && (eff.move_efficiency === 'INEFFICIENT'
      || (eff.move_efficiency === 'NORMAL' && (eff.efficiency_ratio || 0) < T.eff_ratio_efficient))
  ) {
    if (eff.pressure_score > T.pressure_mixed) absorption_state = 'BUY_ABSORPTION'
    else if (eff.pressure_score < -T.pressure_mixed) absorption_state = 'SELL_ABSORPTION'
  }
  const wick = upperWickBias(series, T.w_mid)
  if (absorption_state === 'BUY_ABSORPTION' && wick < -0.08) absorption_state = 'NONE'
  if (absorption_state === 'SELL_ABSORPTION' && wick > 0.08) absorption_state = 'NONE'

  let exhaustion_state = 'NONE'
  if (burst.confirmed && burst.window_end_idx != null && n - 1 > burst.window_end_idx) {
    const post = Math.min(5, n - 1 - burst.window_end_idx)
    if (post >= 2) {
      let trPost = 0
      for (let j = n - post; j < n; j += 1) if (series.tr[j] != null) trPost += series.tr[j]
      const trBurst = []
      for (let j = burst.window_start_idx; j <= burst.window_end_idx; j += 1) {
        if (series.tr[j] != null) trBurst.push(series.tr[j])
      }
      const mb = mean(trBurst) || EPS
      if (trPost / post < mb * 0.55 && Math.abs(eff.pressure_score) < T.pressure_theta) {
        exhaustion_state = burst.direction >= 0 ? 'BUY_EXHAUSTION' : 'SELL_EXHAUSTION'
      }
    }
  }

  let unwind_risk = 'LOW'
  if (burst.confirmed && burst.window_start_idx != null && burst.window_end_idx != null) {
    const sliceC = series.c.slice(burst.window_start_idx, burst.window_end_idx + 1).filter(Number.isFinite)
    const mx = Math.max(...sliceC)
    const mn = Math.min(...sliceC)
    const pNow = series.c[n - 1]
    const p0 = series.c[burst.window_start_idx]
    if (Number.isFinite(mx) && Number.isFinite(mn) && Number.isFinite(pNow) && Number.isFinite(p0)) {
      const burstMove = burst.direction >= 0 ? mx - p0 : p0 - mn
      if (burstMove > EPS) {
        const giveback =
          burst.direction >= 0 ? (mx - pNow) / burstMove : (pNow - mn) / burstMove
        if (giveback >= T.unwind_giveback_high && eff.pressure_score * burst.direction < 0) {
          unwind_risk = 'HIGH'
        } else if (giveback >= T.unwind_giveback) {
          unwind_risk = 'ELEVATED'
        }
      }
    }
  }

  let liquidity_stress = 'LOW'
  const wm = Math.min(T.w_mid, n - 1)
  let flips = 0
  for (let j = n - wm; j < n; j += 1) {
    if (j < 2) continue
    const r0 = series.c[j] - series.c[j - 1]
    const r1 = series.c[j - 1] - series.c[j - 2]
    if (r0 * r1 < 0) flips += 1
  }
  const flipRatio = wm > 2 ? flips / (wm - 2) : 0
  const trRecent = series.tr.slice(n - wm, n).filter(Number.isFinite)
  const trMean = mean(trRecent) || 0
  const trLong = mean(series.tr.slice(Math.max(0, n - T.w_long), n).filter(Number.isFinite)) || EPS
  if (
    flipRatio >= T.liquidity_flip_ratio
    && trMean / (trLong + EPS) >= T.chop_tr_mult
    && (eff.move_efficiency === 'INEFFICIENT' || Math.abs(eff.r_net_mid || 0) < 0.004)
  ) {
    liquidity_stress = 'HIGH'
  } else if (flipRatio >= 0.42 && eff.move_efficiency === 'INEFFICIENT') {
    liquidity_stress = 'ELEVATED'
  }

  const insideCone = liveState?.derived_features?.inside_cone
  if (insideCone === false && liquidity_stress === 'LOW' && flipRatio > 0.38) {
    liquidity_stress = 'ELEVATED'
  }

  const PRI = {
    none: 0,
    efficient: 15,
    normal: 12,
    inefficient: 35,
    liquidity_elevated: 50,
    liquidity_high: 62,
    exhaustion: 58,
    absorption: 65,
    unwind_elevated: 78,
    unwind_high: 90,
  }

  let primary_quality_key = 'none'
  let primary_quality_priority = 0
  const setPrimary = (key, pr) => {
    if (pr > primary_quality_priority) {
      primary_quality_key = key
      primary_quality_priority = pr
    }
  }

  if (unwind_risk === 'HIGH') setPrimary('unwind_high', PRI.unwind_high)
  else if (unwind_risk === 'ELEVATED') setPrimary('unwind_elevated', PRI.unwind_elevated)
  if (absorption_state === 'BUY_ABSORPTION') setPrimary('absorption_buy', PRI.absorption)
  if (absorption_state === 'SELL_ABSORPTION') setPrimary('absorption_sell', PRI.absorption)
  if (exhaustion_state !== 'NONE') setPrimary('exhaustion', PRI.exhaustion)
  if (liquidity_stress === 'HIGH') setPrimary('liquidity_high', PRI.liquidity_high)
  else if (liquidity_stress === 'ELEVATED') setPrimary('liquidity_elevated', PRI.liquidity_elevated)
  if (eff.move_efficiency === 'INEFFICIENT') setPrimary('inefficient', PRI.inefficient)
  if (eff.move_efficiency === 'EFFICIENT') setPrimary('efficient', PRI.efficient)
  if (eff.move_efficiency === 'NORMAL' && primary_quality_key === 'none') setPrimary('normal', PRI.normal)

  let burst_y0 = null
  let burst_y1 = null
  let burst_x0_ms = null
  let burst_x1_ms = null
  if (burst.confirmed && burst.window_start_idx != null && burst.window_end_idx != null) {
    const from = burst.window_start_idx
    const to = burst.window_end_idx
    const hs = series.h.slice(from, to + 1).filter(Number.isFinite)
    const ls = series.l.slice(from, to + 1).filter(Number.isFinite)
    if (hs.length && ls.length) {
      burst_y1 = Math.max(...hs)
      burst_y0 = Math.min(...ls)
    }
    burst_x0_ms = series.tMs[from]
    burst_x1_ms = series.tMs[to]
  }

  return {
    tuning_profile: profile,
    confidence,
    flow_direction: eff.flow_direction,
    move_efficiency: eff.move_efficiency,
    absorption_state,
    exhaustion_state,
    unwind_risk,
    liquidity_stress,
    burst: {
      confirmed: burst.confirmed,
      pending_single_bar: burst.pending_single_bar,
      window_start_idx: burst.window_start_idx,
      window_end_idx: burst.window_end_idx,
      direction: burst.direction,
    },
    scores: {
      efficiency_ratio: eff.efficiency_ratio,
      pressure_score: eff.pressure_score,
      activity_mean_ratio: actMean,
      flip_ratio: flipRatio,
    },
    primary_quality_key,
    primary_quality_priority,
    burst_overlay_eligible_raw: burst.confirmed && burst_x0_ms != null,
    burst_x0_ms,
    burst_x1_ms,
    burst_y0,
    burst_y1,
    _pending_single_bar: burst.pending_single_bar,
  }
}

const QUALITY_PHRASE = {
  none: 'No strong read',
  normal: 'Normal',
  efficient: 'Efficient',
  inefficient: 'Move losing efficiency',
  absorption_buy: 'Possible buy absorption',
  absorption_sell: 'Possible sell absorption',
  exhaustion: 'Move tiring',
  unwind_elevated: 'Unwind risk elevated',
  unwind_high: 'Unwind risk elevated',
  liquidity_elevated: 'Liquidity stress rising',
  liquidity_high: 'Thin / jumpy tape',
}

const DIRECTION_PHRASE = {
  BUY_DOMINANT: 'Buy pressure dominant',
  SELL_DOMINANT: 'Sell pressure dominant',
  MIXED: 'Mixed flow',
  NEUTRAL: 'Quiet tape',
  UNAVAILABLE: 'Unavailable',
}

export function applyFlowVisibleState(prev, proposed) {
  const T = mergeTuning(proposed.tuning_profile || 'default')
  const p = prev || {
    quality_key: null,
    quality_pending: null,
    quality_confirm: 0,
    quality_decay: 0,
    burst_overlay: false,
    burst_pending_confirm: 0,
  }

  const cand =
    proposed.confidence === 'UNAVAILABLE' || proposed.confidence === 'LOW'
      ? null
      : proposed.primary_quality_key

  const next = { ...p }

  if (proposed.confidence === 'LOW' || proposed.confidence === 'UNAVAILABLE') {
    next.quality_decay = (p.quality_decay || 0) + 1
    if (next.quality_decay >= T.hysteresis_decay) {
      next.quality_key = null
      next.quality_pending = null
      next.quality_confirm = 0
      next.quality_decay = 0
    }
    next.burst_overlay = false
    next.burst_pending_confirm = 0
    return finalizeVisible(next, proposed, T)
  }

  if (!cand || cand === 'none' || cand === 'normal') {
    if (p.quality_key && p.quality_key !== 'none' && p.quality_key !== 'normal') {
      next.quality_decay = (p.quality_decay || 0) + 1
      if (next.quality_decay >= T.hysteresis_decay) {
        next.quality_key = cand === 'normal' ? 'normal' : null
        next.quality_pending = null
        next.quality_confirm = 0
        next.quality_decay = 0
      }
    } else {
      next.quality_key = cand === 'normal' ? 'normal' : null
      next.quality_pending = null
      next.quality_confirm = 0
      next.quality_decay = 0
    }
  } else if (cand === p.quality_key) {
    next.quality_confirm = 0
    next.quality_pending = null
    next.quality_decay = 0
  } else if (proposed.primary_quality_priority >= 85) {
    next.quality_key = cand
    next.quality_pending = null
    next.quality_confirm = 0
    next.quality_decay = 0
  } else if (cand === p.quality_pending) {
    next.quality_confirm = (p.quality_confirm || 0) + 1
    if (next.quality_confirm >= T.hysteresis_confirm) {
      next.quality_key = cand
      next.quality_pending = null
      next.quality_confirm = 0
      next.quality_decay = 0
    }
  } else {
    next.quality_pending = cand
    next.quality_confirm = 1
    next.quality_decay = 0
  }

  if (proposed.burst_overlay_eligible_raw) {
    next.burst_pending_confirm = (p.burst_pending_confirm || 0) + 1
    if (next.burst_pending_confirm >= T.burst_overlay_confirm) {
      next.burst_overlay = true
    }
  } else {
    next.burst_pending_confirm = 0
    next.burst_overlay = false
  }

  return finalizeVisible(next, proposed, T)
}

function finalizeVisible(hyst, proposed, T) {
  const qKey = hyst.quality_key || 'none'
  const quality_phrase = QUALITY_PHRASE[qKey] || QUALITY_PHRASE.none
  const direction_phrase = DIRECTION_PHRASE[proposed.flow_direction] || DIRECTION_PHRASE.UNAVAILABLE

  let annotation_key = null
  if (proposed.confidence === 'HIGH' || proposed.confidence === 'MEDIUM') {
    if (['unwind_high', 'unwind_elevated', 'absorption_buy', 'absorption_sell', 'exhaustion', 'liquidity_high', 'liquidity_elevated', 'inefficient'].includes(qKey)) {
      annotation_key = qKey
    }
  }

  return {
    hysteresis: hyst,
    confidence: proposed.confidence,
    flow_direction: proposed.flow_direction,
    move_efficiency: proposed.move_efficiency,
    absorption_state: proposed.absorption_state,
    exhaustion_state: proposed.exhaustion_state,
    unwind_risk: proposed.unwind_risk,
    liquidity_stress: proposed.liquidity_stress,
    quality_key_visible: qKey,
    quality_phrase,
    direction_phrase,
    line1: `${FLOW_STRIP_PREFIX_FLOW} ${direction_phrase}`,
    line2: `${FLOW_STRIP_PREFIX_MOVE_QUALITY} ${quality_phrase}`,
    burst_overlay: hyst.burst_overlay && proposed.burst_overlay_eligible_raw,
    burst_x0_ms: proposed.burst_x0_ms,
    burst_x1_ms: proposed.burst_x1_ms,
    burst_y0: proposed.burst_y0,
    burst_y1: proposed.burst_y1,
    annotation_key,
    annotation_confirm_refreshes: T.annotation_confirm_refreshes,
    raw: proposed,
  }
}

export function createInitialFlowHysteresis() {
  return {
    quality_key: null,
    quality_pending: null,
    quality_confirm: 0,
    quality_decay: 0,
    burst_overlay: false,
    burst_pending_confirm: 0,
  }
}

const ANNOTATION_TEXT = {
  unwind_high: 'Unwind risk',
  unwind_elevated: 'Unwind risk',
  absorption_buy: 'Buy absorption',
  absorption_sell: 'Sell absorption',
  exhaustion: 'Move tiring',
  liquidity_high: 'Thin tape',
  liquidity_elevated: 'Thin tape',
  inefficient: 'Losing efficiency',
}

export function getFlowAnnotationText(annotationKey) {
  return annotationKey ? ANNOTATION_TEXT[annotationKey] || null : null
}
