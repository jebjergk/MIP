const CONFIDENCE_RANK = {
  LOW: 1,
  MEDIUM: 2,
  HIGH: 3,
}

const STANCE_SEVERITY = {
  RISK_OFF: 5,
  ESCALATE: 4,
  WATCH_CLOSELY: 3,
  THESIS_INTACT: 2,
  HOLD: 1,
  UNKNOWN: 0,
}

const ACTION_PRIORITY = {
  ADD_PROTECTION: 7,
  TIGHTEN_PROTECTION: 6,
  PARTIAL_DE_RISK: 5,
  HOLD_WITH_MONITORING: 4,
  WATCH: 3,
  HOLD: 2,
  NO_ACTION: 1,
}

const SENSITIVITY_MULTIPLIER = {
  CONSERVATIVE: 1.3,
  BALANCED: 1.0,
  AGGRESSIVE: 0.75,
}

function toNum(value) {
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function confidenceFromMateriality(score) {
  if (score >= 0.8) return 'HIGH'
  if (score >= 0.5) return 'MEDIUM'
  return 'LOW'
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function pctChange(curr, prev) {
  const c = toNum(curr)
  const p = toNum(prev)
  if (!Number.isFinite(c) || !Number.isFinite(p) || p === 0) return null
  return (c / p) - 1
}

function latestBars(tile) {
  return Array.isArray(tile?.chart?.bars) ? tile.chart.bars : []
}

/** Seconds per bar: 30s ticks, 1m bars, or hourly etc. */
function barStepSeconds(tile) {
  const bs = toNum(tile?.chart?.bar_seconds)
  if (bs != null && bs > 0) return bs
  const mins = toNum(tile?.chart?.interval_minutes)
  if (mins != null && mins > 0) return mins * 60
  return 3600
}

function barIndexMinutesAgo(bars, minutesAgo, stepSec) {
  if (!bars.length) return {}
  const n = Math.max(1, Math.round((minutesAgo * 60) / stepSec))
  return bars[Math.max(0, bars.length - 1 - n)] || {}
}

function classifyPattern(features) {
  if ((features.vol_15m ?? 0) > 0.022) return 'VOLATILITY_SPIKE'
  if ((features.ret_15m ?? 0) < -0.012 && (features.momentum_decay ?? 0) > 0.5) return 'RISK_OFF_BREAKDOWN'
  if ((features.ret_15m ?? 0) > 0.009 && (features.vs_benchmark ?? 0) > 0) return 'TREND_CONTINUATION'
  if ((features.ret_5m ?? 0) > 0.004 && (features.ret_15m ?? 0) <= 0) return 'FAILED_BOUNCE'
  if (Math.abs(features.ret_15m ?? 0) < 0.002 && (features.range_expansion ?? 0) < 0.2) return 'CHOP_NOISE'
  return 'WEAK_DRIFT'
}

function fmtPct(value) {
  const n = toNum(value)
  if (n == null) return 'n/a'
  return `${(n * 100).toFixed(2)}%`
}

function fmtNum(value, digits = 2) {
  const n = toNum(value)
  if (n == null) return 'n/a'
  return n.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

function fmtBps(value) {
  const n = toNum(value)
  if (n == null) return 'n/a'
  return `${(n * 10000).toFixed(0)} bps`
}

function pickActionRanked(actions) {
  return [...new Set(actions)]
    .sort((a, b) => (ACTION_PRIORITY[b] || 0) - (ACTION_PRIORITY[a] || 0))
}

function marketTone(feats) {
  if ((feats.ret_15m ?? 0) > 0.007) return 'bid tone improving'
  if ((feats.ret_15m ?? 0) < -0.007) return 'sellers in control'
  return 'tape still mixed'
}

function isHotNews(item) {
  const badge = String(item?.meta?.badge || '').toUpperCase()
  const severity = String(item?.severity || '').toUpperCase()
  return badge === 'HOT' || badge === 'RISK' || severity === 'WARN'
}

function shortHeadline(item) {
  const text = String(item?.label || item?.title || '').trim()
  if (!text) return 'No headline text'
  return text.length > 96 ? `${text.slice(0, 93)}...` : text
}

function computeAdaptiveThresholds({ vol15m, currentPrice, quantity, sensitivity }) {
  const vol = Math.max(Math.abs(toNum(vol15m) ?? 0), 0.004)
  const price = Math.max(Math.abs(toNum(currentPrice) ?? 0), 1)
  const qty = Math.max(Math.abs(toNum(quantity) ?? 0), 1)
  const notional = price * qty
  const key = String(sensitivity || 'BALANCED').toUpperCase()
  const mult = SENSITIVITY_MULTIPLIER[key] || 1.0
  return {
    ret15_surge_threshold: clamp(vol * 2.4 * mult, 0.0045, 0.04),
    entry_recovery_threshold: clamp(vol * 1.8 * mult, 0.0045, 0.03),
    pnl_delta_threshold: clamp(notional * vol * 0.9 * mult, 60, 2600),
    price_drift_threshold: clamp(vol * 0.9 * mult, 0.0025, 0.03),
    sensitivity_mode: key,
  }
}

export function buildLiveState(tile, previousLive = null, sensitivity = 'BALANCED') {
  const bars = latestBars(tile)
  const stepSec = barStepSeconds(tile)
  const last = bars[bars.length - 1] || {}
  const prev5 = barIndexMinutesAgo(bars, 5, stepSec)
  const prev15 = barIndexMinutesAgo(bars, 15, stepSec)
  const close = toNum(last.close) ?? toNum(tile?.current_price)
  const current = toNum(tile?.current_price) ?? close
  const entry = toNum(tile?.entry_price)
  const horizonPath = Array.isArray(tile?.expectation?.center_path) ? tile.expectation.center_path : []
  const coneLowerPath = Array.isArray(tile?.expectation?.lower_path) ? tile.expectation.lower_path : []
  const coneUpperPath = Array.isArray(tile?.expectation?.upper_path) ? tile.expectation.upper_path : []
  const centerNow = toNum(horizonPath[0]?.price)
  const lowerNow = toNum(coneLowerPath[0]?.price)
  const upperNow = toNum(coneUpperPath[0]?.price)

  const ret5m = pctChange(close, prev5.close)
  const ret15m = pctChange(close, prev15.close)
  const volBars = Math.min(
    bars.length,
    Math.max(16, Math.round((16 * 3600) / stepSec)),
  )
  const volWindow = bars.slice(-volBars).map((b) => toNum(b.close)).filter((v) => Number.isFinite(v))
  const rets = []
  for (let i = 1; i < volWindow.length; i += 1) {
    if (volWindow[i - 1] > 0) rets.push((volWindow[i] / volWindow[i - 1]) - 1)
  }
  const mean = rets.length ? rets.reduce((a, b) => a + b, 0) / rets.length : 0
  const variance = rets.length ? rets.reduce((a, b) => a + ((b - mean) ** 2), 0) / rets.length : 0
  const vol15m = Math.sqrt(variance)
  const high = Math.max(...bars.slice(-volBars).map((b) => toNum(b.high) ?? -Infinity))
  const low = Math.min(...bars.slice(-volBars).map((b) => toNum(b.low) ?? Infinity))
  const rangeExpansion = Number.isFinite(high) && Number.isFinite(low) && close
    ? (high - low) / close
    : null

  const momentumDecay = previousLive?.derived_features?.ret_15m != null && ret15m != null
    ? clamp(Math.abs(previousLive.derived_features.ret_15m) - Math.abs(ret15m), -1, 1)
    : 0

  const deviationFromMedian = (current != null && centerNow != null && centerNow !== 0)
    ? (current / centerNow) - 1
    : null
  const deviationFromLower = (current != null && lowerNow != null && lowerNow !== 0)
    ? (current / lowerNow) - 1
    : null
  const insideCone = current != null && lowerNow != null && upperNow != null
    ? current >= Math.min(lowerNow, upperNow) && current <= Math.max(lowerNow, upperNow)
    : null

  const distanceToEntry = entry && current ? ((current / entry) - 1) : null
  const tp = toNum(tile?.overlays?.take_profit)
  const sl = toNum(tile?.overlays?.stop_loss)
  const distanceToTp = tp && current ? ((tp / current) - 1) : null
  const distanceToSl = sl && current ? ((sl / current) - 1) : null

  const features = {
    ret_5m: ret5m,
    ret_15m: ret15m,
    vol_15m: Number.isFinite(vol15m) ? vol15m : null,
    range_expansion: rangeExpansion,
    momentum_decay: momentumDecay,
    distance_to_entry_pct: distanceToEntry,
    distance_to_tp_pct: distanceToTp,
    distance_to_sl_pct: distanceToSl,
    deviation_from_h5_median: deviationFromMedian,
    deviation_from_h5_lower_band: deviationFromLower,
    inside_cone: insideCone,
    regime_label: (ret15m ?? 0) > 0.004 ? 'RISK_ON' : (ret15m ?? 0) < -0.006 ? 'RISK_OFF' : 'WEAK_DRIFT',
    pattern_label: 'WEAK_DRIFT',
    vs_sector: null,
    vs_benchmark: null,
  }
  features.pattern_label = classifyPattern(features)
  const quantity = toNum(tile?.quantity)
  const adaptiveThresholds = computeAdaptiveThresholds({
    vol15m: features.vol_15m,
    currentPrice: current,
    quantity,
    sensitivity,
  })

  return {
    symbol: tile?.symbol,
    timestamp: new Date().toISOString(),
    last_price: current,
    bid: current != null ? current * 0.9995 : null,
    ask: current != null ? current * 1.0005 : null,
    day_change_pct: pctChange(close, bars[0]?.open),
    intraday_bars: bars.slice(-30),
    derived_features: features,
    quantity,
    position_notional: current != null && quantity != null ? Math.abs(current * quantity) : null,
    adaptive_thresholds: adaptiveThresholds,
    live_news: (tile?.events || []).filter((e) => e?.type === 'NEWS').slice(0, 2),
    unrealized_pnl: toNum(tile?.unrealized_pnl),
    comparison: {
      vs_sector: null,
      vs_benchmark: null,
    },
  }
}

function makeAgentOutput(agent_name, symbol, payload, previousAgent = null) {
  const materiality = clamp(payload.materiality_score ?? 0.5, 0, 1)
  const confidence = payload.confidence || confidenceFromMateriality(materiality)
  const prevTags = new Set(previousAgent?.reason_tags || [])
  const newTags = (payload.reason_tags || []).filter((tag) => !prevTags.has(tag))
  const changed = (
    !previousAgent
    || previousAgent.stance !== payload.stance
    || previousAgent.action_bias !== payload.action_bias
    || previousAgent.confidence !== confidence
    || newTags.length > 0
  )
  return {
    agent_name,
    symbol,
    stance: payload.stance,
    confidence,
    reason_tags: payload.reason_tags || [],
    action_bias: payload.action_bias || 'WATCH',
    materiality_score: materiality,
    change_detected: changed,
    short_text: payload.short_text,
  }
}

export function evaluateCommittee(tile, liveState, previousCommittee = null) {
  const symbol = tile?.symbol
  const marketType = tile?.market_type
  const side = String(tile?.side || 'LONG').toUpperCase()
  const isProtected = tile?.overlays?.stop_loss != null || tile?.overlays?.take_profit != null
  const underwater = Number(tile?.unrealized_pnl || 0) < 0
  const feats = liveState?.derived_features || {}
  const pathInside = feats.inside_cone
  const prevLive = previousCommittee?.live_state || {}
  const prevFeats = prevLive?.derived_features || {}
  const prevPnl = toNum(prevLive?.unrealized_pnl)
  const currPnl = toNum(liveState?.unrealized_pnl)
  const prevDistToEntry = toNum(prevFeats?.distance_to_entry_pct)
  const currDistToEntry = toNum(feats?.distance_to_entry_pct)
  const thresholds = liveState?.adaptive_thresholds || {}
  const ret15SurgeThreshold = toNum(thresholds?.ret15_surge_threshold) ?? 0.01
  const entryRecoveryThreshold = toNum(thresholds?.entry_recovery_threshold) ?? 0.01
  const pnlFlipPositive = prevPnl != null && currPnl != null && prevPnl < 0 && currPnl > 0
  const pnlMomentumDelta = prevPnl != null && currPnl != null ? currPnl - prevPnl : null
  const entryRecoveryBurst = (
    prevDistToEntry != null
    && currDistToEntry != null
    && (currDistToEntry - prevDistToEntry) >= entryRecoveryThreshold
  )
  const shortTermSurge = (feats.ret_15m ?? 0) >= ret15SurgeThreshold
  const recoverySurge = Boolean(pnlFlipPositive || entryRecoveryBurst || shortTermSurge)

  const previousByAgent = new Map((previousCommittee?.agent_messages || []).map((m) => [m.agent_name, m]))
  const outputs = []

  outputs.push(
    makeAgentOutput('POSITION_MANAGER_AGENT', symbol, {
      stance: pnlFlipPositive ? 'RECOVERED_TO_PROFIT' : underwater ? 'LAGGING' : 'ON_TRACK',
      reason_tags: [
        pnlFlipPositive ? 'PNL_REGIME_FLIP' : underwater ? 'UNDERWATER' : 'IN_PROFIT',
        isProtected ? 'PROTECTED' : 'UNPROTECTED',
        recoverySurge ? 'RECOVERY_SURGE' : 'NO_SURGE_SIGNAL',
      ],
      action_bias: underwater ? 'WATCH' : 'HOLD_WITH_MONITORING',
      materiality_score: pnlFlipPositive ? 0.86 : underwater ? 0.68 : recoverySurge ? 0.65 : 0.4,
      short_text: pnlFlipPositive
        ? `Position flipped from underwater to in-profit (delta ${fmtNum(pnlMomentumDelta, 2)} P&L); momentum regime improved rapidly.`
        : underwater
        ? `Position still open, but P/L is underwater (${fmtPct(feats.distance_to_entry_pct)} from entry) with limited objective progress.`
        : recoverySurge
          ? `Price recovery accelerated (${fmtPct(feats.ret_15m)} over 15m vs ${fmtPct(ret15SurgeThreshold)} trigger); thesis quality improved and needs active reassessment.`
          : `Progress is acceptable so far (${fmtPct(feats.distance_to_entry_pct)} from entry), thesis remains workable.`,
    }, previousByAgent.get('POSITION_MANAGER_AGENT')),
  )

  const riskScore = clamp(
    (underwater ? 0.35 : 0)
      + (!isProtected ? 0.35 : 0)
      + ((feats.vol_15m ?? 0) > 0.015 ? 0.2 : 0)
      + ((feats.deviation_from_h5_lower_band ?? 0) < 0 ? 0.2 : 0),
    0,
    1,
  )
  outputs.push(
    makeAgentOutput('RISK_AGENT', symbol, {
      stance: riskScore >= 0.8 ? 'RISK_HIGH' : riskScore >= 0.5 ? 'RISK_ELEVATED' : 'RISK_CONTAINED',
      reason_tags: [
        underwater ? 'UNDERWATER' : 'P_L_STABLE',
        isProtected ? 'HAS_PROTECTION' : 'UNPROTECTED',
        (feats.vol_15m ?? 0) > 0.015 ? 'VOL_EXPANSION' : 'VOL_NORMAL',
        pnlFlipPositive ? 'PNL_REGIME_FLIP' : 'NO_PNL_REGIME_FLIP',
      ],
      action_bias: riskScore >= 0.8 ? 'ADD_PROTECTION' : riskScore >= 0.5 ? 'HOLD_WITH_MONITORING' : 'NO_ACTION',
      materiality_score: riskScore,
      short_text: riskScore >= 0.8
        ? `Risk pressure elevated: ${isProtected ? 'protection is light' : 'position is unprotected'} while volatility is ${fmtPct(feats.vol_15m)}.`
        : riskScore >= 0.5
          ? `Risk building incrementally (${fmtBps(feats.deviation_from_h5_lower_band)} vs lower cone), keep protection readiness high.`
          : 'Risk remains contained versus current intraday volatility profile.',
    }, previousByAgent.get('RISK_AGENT')),
  )

  const trainingMateriality = clamp(
    Math.abs(feats.deviation_from_h5_median ?? 0) * 8 + (pathInside === false ? 0.35 : 0.1),
    0,
    1,
  )
  const expectationLabel = pathInside == null
    ? 'UNKNOWN_EXPECTATION'
    : pathInside
      ? ((feats.deviation_from_h5_median ?? 0) < -0.002 ? 'LOWER_HALF_OF_CONE' : 'INSIDE_EXPECTED_RANGE')
      : 'BELOW_LOWER_BAND'
  outputs.push(
    makeAgentOutput('TRAINING_EXPECTATION_AGENT', symbol, {
      stance: expectationLabel,
      reason_tags: [expectationLabel, (feats.momentum_decay ?? 0) > 0.2 ? 'STALLING_VS_EXPECTED_PATH' : 'PATH_ACCEPTABLE'],
      action_bias: pathInside === false ? 'WATCH' : 'HOLD',
      materiality_score: trainingMateriality,
      short_text: pathInside === false
        ? `Path is below expected cone; deviation now ${fmtBps(feats.deviation_from_h5_median)} vs median and behaving abnormally.`
        : `Path remains inside trained range (${fmtBps(feats.deviation_from_h5_median)} vs median), but follow-through quality is uneven.`,
    }, previousByAgent.get('TRAINING_EXPECTATION_AGENT')),
  )

  const marketStance = feats.regime_label === 'RISK_OFF' ? 'REGIME_AGAINST' : feats.regime_label === 'RISK_ON' ? 'REGIME_SUPPORTIVE' : 'REGIME_MIXED'
  outputs.push(
    makeAgentOutput('MARKET_REGIME_AGENT', symbol, {
      stance: marketStance,
      reason_tags: [feats.regime_label || 'WEAK_DRIFT', side === 'LONG' ? 'LONG_BOOK' : 'SHORT_BOOK'],
      action_bias: marketStance === 'REGIME_AGAINST' ? 'WATCH' : 'HOLD',
      materiality_score: marketStance === 'REGIME_AGAINST' ? 0.7 : 0.4,
      short_text: marketStance === 'REGIME_AGAINST'
        ? `Regime is leaning against this direction; ${marketTone(feats)} and support is weak.`
        : `Regime is not blocking the thesis; ${marketTone(feats)} for now.`,
    }, previousByAgent.get('MARKET_REGIME_AGENT')),
  )

  const liveNews = Array.isArray(liveState?.live_news) ? liveState.live_news : []
  const hasLiveNews = liveNews.length > 0
  const latestNews = hasLiveNews ? liveNews[0] : null
  const hotNews = hasLiveNews && liveNews.some((item) => isHotNews(item))
  const latestTs = new Date(latestNews?.ts || 0).getTime()
  const isFresh = Number.isFinite(latestTs) && (Date.now() - latestTs) <= (4 * 60 * 60 * 1000)
  const newsBrief = hasLiveNews ? shortHeadline(latestNews) : null
  outputs.push(
    makeAgentOutput('NEWS_CATALYST_AGENT', symbol, {
      stance: !hasLiveNews
        ? 'NO_RELIABLE_NEWS_INPUT'
        : hotNews
          ? 'HOT_NEWS_CONTEXT'
          : 'LOW_SIGNAL_NEWS',
      reason_tags: !hasLiveNews
        ? ['NO_RELIABLE_NEWS_INPUT']
        : hotNews
          ? ['HOT_NEWS_CONTEXT', isFresh ? 'FRESH_NEWS' : 'STALE_NEWS']
          : ['LOW_SIGNAL_NEWS', isFresh ? 'FRESH_NEWS' : 'STALE_NEWS'],
      action_bias: !hasLiveNews ? 'NO_ACTION' : hotNews ? 'WATCH' : 'HOLD_WITH_MONITORING',
      materiality_score: !hasLiveNews ? 0.2 : hotNews ? 0.78 : 0.45,
      short_text: !hasLiveNews
        ? 'No reliable new catalyst this cycle; defer to tape and risk structure.'
        : hotNews
          ? `Hot news in flow (${isFresh ? 'fresh' : 'older'}): ${newsBrief}`
          : `News flow looks low-signal (${isFresh ? 'fresh' : 'older'}): ${newsBrief}`,
    }, previousByAgent.get('NEWS_CATALYST_AGENT')),
  )

  const pattern = feats.pattern_label || 'WEAK_DRIFT'
  outputs.push(
    makeAgentOutput('INTRADAY_PATTERN_AGENT', symbol, {
      stance: pattern,
      reason_tags: [pattern, (feats.ret_15m ?? 0) < 0 ? 'WEAK_FOLLOW_THROUGH' : 'FOLLOW_THROUGH_OK'],
      action_bias: ['RISK_OFF_BREAKDOWN', 'FAILED_BOUNCE', 'VOLATILITY_SPIKE'].includes(pattern) ? 'PARTIAL_DE_RISK' : 'HOLD_WITH_MONITORING',
      materiality_score: ['RISK_OFF_BREAKDOWN', 'VOLATILITY_SPIKE'].includes(pattern) ? 0.82 : 0.52,
      short_text: `Intraday pattern prints ${pattern.replaceAll('_', ' ').toLowerCase()} (ret15=${fmtPct(feats.ret_15m)}, vol=${fmtPct(feats.vol_15m)}).`,
    }, previousByAgent.get('INTRADAY_PATTERN_AGENT')),
  )

  const dominantRisk = outputs.some((o) => ['RISK_HIGH', 'BELOW_LOWER_BAND', 'RISK_OFF_BREAKDOWN'].includes(o.stance))
  const elevated = outputs.some((o) => ['RISK_ELEVATED', 'REGIME_AGAINST', 'FAILED_BOUNCE', 'VOLATILITY_SPIKE'].includes(o.stance))
  const committee_stance = dominantRisk
    ? 'ESCALATE'
    : (recoverySurge && !underwater)
      ? 'THESIS_INTACT'
      : elevated
        ? 'WATCH_CLOSELY'
        : 'THESIS_INTACT'
  const confidenceRank = Math.max(...outputs.map((o) => CONFIDENCE_RANK[o.confidence] || 1))
  const committee_confidence = confidenceRank >= 3 ? 'HIGH' : confidenceRank >= 2 ? 'MEDIUM' : 'LOW'
  const reasonTagFreq = new Map()
  outputs.forEach((o) => {
    ;(o.reason_tags || []).forEach((tag) => {
      reasonTagFreq.set(tag, (reasonTagFreq.get(tag) || 0) + 1)
    })
  })
  const top_reason_tags = [...reasonTagFreq.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 4)
    .map(([tag]) => tag)
  const actions_to_consider = pickActionRanked(outputs.map((o) => o.action_bias))

  const disagreement_points = []
  const training = outputs.find((o) => o.agent_name === 'TRAINING_EXPECTATION_AGENT')
  const risk = outputs.find((o) => o.agent_name === 'RISK_AGENT')
  if (training?.stance !== 'BELOW_LOWER_BAND' && ['RISK_HIGH', 'RISK_ELEVATED'].includes(risk?.stance)) {
    disagreement_points.push('Training range remains partially valid, but risk pressure is rising faster than path quality.')
  }
  if (pattern === 'TREND_CONTINUATION' && ['RISK_HIGH', 'RISK_ELEVATED'].includes(risk?.stance)) {
    disagreement_points.push('Tape is attempting continuation, but downside asymmetry still dominates the risk vote.')
  }

  const key_points_for_human = [
    underwater ? 'Position is currently underwater.' : 'Position is not currently underwater.',
    isProtected ? 'Protection exists on the position.' : 'Position currently lacks active TP/SL protection.',
    training?.short_text || 'Expectation state available.',
  ]

  const previousTopTags = new Set(previousCommittee?.top_reason_tags || [])
  const newMajorReasonTag = top_reason_tags.find((tag) => !previousTopTags.has(tag))
  const changeDetected = (
    !previousCommittee
    || previousCommittee.committee_stance !== committee_stance
    || previousCommittee.committee_confidence !== committee_confidence
    || previousCommittee.actions_to_consider?.[0] !== actions_to_consider[0]
    || Boolean(newMajorReasonTag)
  )

  return {
    symbol,
    market_type: marketType,
    committee_stance,
    committee_confidence,
    top_reason_tags,
    actions_to_consider,
    disagreement_points,
    key_points_for_human,
    headline_text: dominantRisk
      ? 'Committee tilt: risk pressure is dominating the setup.'
      : (recoverySurge && !underwater)
        ? 'Committee tilt: strong recovery surge improved setup quality.'
      : elevated
        ? 'Committee tilt: setup remains viable, but quality has softened.'
        : 'Committee tilt: behavior is mostly aligned with thesis.',
    latest_material_changes: changeDetected ? [{
      ts: new Date().toISOString(),
      type: 'COMMITTEE_CHANGE',
      reason: newMajorReasonTag || 'STATE_SHIFT',
    }] : [],
    pattern_label: pattern,
    inside_cone: feats.inside_cone,
    risk_pressure_level: risk?.stance || 'RISK_CONTAINED',
    live_state: liveState,
    agent_messages: outputs,
    updated_at: new Date().toISOString(),
    watchlist_priority: STANCE_SEVERITY[committee_stance] || 0,
    changed_recently: changeDetected,
  }
}

export function isMaterialUpdate(previousCommittee, nextCommittee) {
  if (!previousCommittee) return true
  if (previousCommittee.committee_stance !== nextCommittee.committee_stance) return true
  if (previousCommittee.committee_confidence !== nextCommittee.committee_confidence) return true
  if (previousCommittee.pattern_label !== nextCommittee.pattern_label) return true
  if (previousCommittee.inside_cone !== nextCommittee.inside_cone) return true
  if (previousCommittee.risk_pressure_level !== nextCommittee.risk_pressure_level) return true
  const prevPnl = toNum(previousCommittee?.live_state?.unrealized_pnl)
  const nextPnl = toNum(nextCommittee?.live_state?.unrealized_pnl)
  const dynamicPnlDelta = toNum(nextCommittee?.live_state?.adaptive_thresholds?.pnl_delta_threshold) ?? 120
  if (prevPnl != null && nextPnl != null) {
    if ((prevPnl < 0 && nextPnl > 0) || (prevPnl > 0 && nextPnl < 0)) return true
    if (Math.abs(nextPnl - prevPnl) >= dynamicPnlDelta) return true
  }
  const prevDist = toNum(previousCommittee?.live_state?.derived_features?.distance_to_entry_pct)
  const nextDist = toNum(nextCommittee?.live_state?.derived_features?.distance_to_entry_pct)
  const dynamicEntryDelta = toNum(nextCommittee?.live_state?.adaptive_thresholds?.entry_recovery_threshold) ?? 0.008
  if (prevDist != null && nextDist != null && Math.abs(nextDist - prevDist) >= dynamicEntryDelta) return true
  const prevRet15 = toNum(previousCommittee?.live_state?.derived_features?.ret_15m) || 0
  const nextRet15 = toNum(nextCommittee?.live_state?.derived_features?.ret_15m) || 0
  const dynamicRet15 = toNum(nextCommittee?.live_state?.adaptive_thresholds?.ret15_surge_threshold) ?? 0.01
  if ((prevRet15 <= 0 && nextRet15 >= dynamicRet15) || (prevRet15 >= 0 && nextRet15 <= -dynamicRet15)) return true
  const prevTag = previousCommittee.top_reason_tags?.[0]
  const nextTag = nextCommittee.top_reason_tags?.[0]
  if (prevTag !== nextTag) return true
  const prevAction = previousCommittee.actions_to_consider?.[0]
  const nextAction = nextCommittee.actions_to_consider?.[0]
  if (prevAction !== nextAction) return true
  const prevPrice = toNum(previousCommittee?.live_state?.last_price)
  const nextPrice = toNum(nextCommittee?.live_state?.last_price)
  if (prevPrice != null && nextPrice != null && prevPrice !== 0) {
    const drift = Math.abs((nextPrice / prevPrice) - 1)
    const dynamicDrift = toNum(nextCommittee?.live_state?.adaptive_thresholds?.price_drift_threshold) ?? 0.0035
    if (drift >= dynamicDrift) return true
  }
  return false
}

export function severityRank(stance) {
  return STANCE_SEVERITY[stance] || 0
}

export function confidenceRank(value) {
  return CONFIDENCE_RANK[value] || 0
}

/* ─── Chart technical overlay calculations ─── */

export function computeVWAP(bars) {
  if (!Array.isArray(bars) || bars.length === 0) return []
  let cumPV = 0
  let cumV = 0
  return bars.map((bar) => {
    const typical = ((toNum(bar.high) ?? toNum(bar.close) ?? 0) + (toNum(bar.low) ?? toNum(bar.close) ?? 0) + (toNum(bar.close) ?? 0)) / 3
    const vol = Math.max(toNum(bar.volume) ?? 1, 1)
    cumPV += typical * vol
    cumV += vol
    return cumV > 0 ? cumPV / cumV : null
  })
}

export function computeBollingerBands(bars, period = 20, stdDevMult = 2) {
  if (!Array.isArray(bars)) return { upper: [], middle: [], lower: [] }
  const closes = bars.map((b) => toNum(b.close))
  const upper = []
  const middle = []
  const lower = []
  for (let i = 0; i < closes.length; i += 1) {
    if (i < period - 1 || closes[i] == null) {
      upper.push(null)
      middle.push(null)
      lower.push(null)
      continue
    }
    const window = closes.slice(i - period + 1, i + 1).filter((v) => v != null)
    if (window.length < period * 0.6) {
      upper.push(null)
      middle.push(null)
      lower.push(null)
      continue
    }
    const avg = window.reduce((a, b) => a + b, 0) / window.length
    const variance = window.reduce((a, b) => a + ((b - avg) ** 2), 0) / window.length
    const std = Math.sqrt(variance)
    middle.push(avg)
    upper.push(avg + stdDevMult * std)
    lower.push(avg - stdDevMult * std)
  }
  return { upper, middle, lower }
}

export function detectSupportResistance(bars, lookback = 30) {
  if (!Array.isArray(bars) || bars.length < 5) return { support: null, resistance: null }
  const recent = bars.slice(-lookback)
  const lows = recent.map((b) => toNum(b.low)).filter((v) => v != null)
  const highs = recent.map((b) => toNum(b.high)).filter((v) => v != null)
  if (lows.length < 3 || highs.length < 3) return { support: null, resistance: null }
  lows.sort((a, b) => a - b)
  highs.sort((a, b) => b - a)
  const support = lows.slice(0, Math.max(3, Math.floor(lows.length * 0.15)))
    .reduce((a, b) => a + b, 0) / Math.max(3, Math.floor(lows.length * 0.15))
  const resistance = highs.slice(0, Math.max(3, Math.floor(highs.length * 0.15)))
    .reduce((a, b) => a + b, 0) / Math.max(3, Math.floor(highs.length * 0.15))
  return { support, resistance }
}

export function computeRSI(bars, period = 14) {
  if (!Array.isArray(bars) || bars.length < period + 1) return []
  const closes = bars.map((b) => toNum(b.close))
  const rsi = new Array(closes.length).fill(null)
  let gainSum = 0
  let lossSum = 0
  for (let i = 1; i <= period; i += 1) {
    if (closes[i] == null || closes[i - 1] == null) continue
    const diff = closes[i] - closes[i - 1]
    if (diff > 0) gainSum += diff
    else lossSum += Math.abs(diff)
  }
  let avgGain = gainSum / period
  let avgLoss = lossSum / period
  rsi[period] = avgLoss === 0 ? 100 : 100 - (100 / (1 + avgGain / avgLoss))
  for (let i = period + 1; i < closes.length; i += 1) {
    if (closes[i] == null || closes[i - 1] == null) { rsi[i] = rsi[i - 1]; continue }
    const diff = closes[i] - closes[i - 1]
    avgGain = (avgGain * (period - 1) + (diff > 0 ? diff : 0)) / period
    avgLoss = (avgLoss * (period - 1) + (diff < 0 ? Math.abs(diff) : 0)) / period
    rsi[i] = avgLoss === 0 ? 100 : 100 - (100 / (1 + avgGain / avgLoss))
  }
  return rsi
}

function bollingerPeriodForBarCount(n) {
  if (n < 6) return Math.max(2, n - 1)
  return Math.min(20, Math.max(5, Math.floor(n / 3)))
}

export function computeChartOverlays(bars) {
  const vwap = computeVWAP(bars)
  const n = Array.isArray(bars) ? bars.length : 0
  const bbPeriod = bollingerPeriodForBarCount(n)
  const bollinger = computeBollingerBands(bars, bbPeriod, 2)
  const sr = detectSupportResistance(bars)
  const rsi = computeRSI(bars, 14)
  return { vwap, bollinger, sr, rsi }
}

/* ─── Exit recommendation engine ─── */

const EXIT_URGENCY = { HOLD: 0, MONITOR: 1, PREPARE: 2, EXIT_NOW: 3 }

export function generateExitRecommendation(tile, liveState, committee) {
  const feats = liveState?.derived_features || {}
  const bars = Array.isArray(tile?.chart?.bars) ? tile.chart.bars : []
  const current = toNum(liveState?.last_price) ?? toNum(tile?.current_price)
  const entry = toNum(tile?.entry_price)
  const tp = toNum(tile?.overlays?.take_profit)
  const sl = toNum(tile?.overlays?.stop_loss)
  const pnl = toNum(tile?.unrealized_pnl) ?? 0
  const side = String(tile?.side || 'LONG').toUpperCase()
  const stance = committee?.committee_stance

  const signals = []
  let urgency = 'HOLD'

  if (sl != null && current != null) {
    const distSl = side === 'LONG' ? (current - sl) / current : (sl - current) / current
    if (distSl < 0) {
      urgency = 'EXIT_NOW'
      signals.push('Price has breached your stop loss.')
    } else if (distSl < 0.005) {
      urgency = maxUrgency(urgency, 'EXIT_NOW')
      signals.push(`Only ${(distSl * 100).toFixed(2)}% from stop loss — critical zone.`)
    } else if (distSl < 0.015) {
      urgency = maxUrgency(urgency, 'PREPARE')
      signals.push(`${(distSl * 100).toFixed(1)}% from stop loss, approaching danger.`)
    }
  }

  if (tp != null && current != null) {
    const distTp = side === 'LONG' ? (tp - current) / current : (current - tp) / current
    if (distTp <= 0) {
      urgency = maxUrgency(urgency, 'PREPARE')
      signals.push('Take profit target reached — consider locking in gains.')
    } else if (distTp < 0.005) {
      urgency = maxUrgency(urgency, 'PREPARE')
      signals.push(`Within ${(distTp * 100).toFixed(2)}% of take profit target.`)
    }
  }

  if (stance === 'ESCALATE') {
    urgency = maxUrgency(urgency, 'PREPARE')
    signals.push('Committee has escalated — multiple risk agents flagging concerns.')
  }

  const rsi = computeRSI(bars, 14)
  const lastRsi = rsi.length > 0 ? rsi[rsi.length - 1] : null
  if (lastRsi != null) {
    if (side === 'LONG' && lastRsi > 75) {
      urgency = maxUrgency(urgency, 'MONITOR')
      signals.push(`RSI is overbought at ${lastRsi.toFixed(0)} — momentum may exhaust soon.`)
    }
    if (side === 'SHORT' && lastRsi < 25) {
      urgency = maxUrgency(urgency, 'MONITOR')
      signals.push(`RSI is oversold at ${lastRsi.toFixed(0)} — short squeeze risk rising.`)
    }
  }

  if (feats.pattern_label === 'RISK_OFF_BREAKDOWN') {
    urgency = maxUrgency(urgency, 'PREPARE')
    signals.push('Risk-off breakdown pattern detected — sellers in control.')
  }
  if (feats.pattern_label === 'FAILED_BOUNCE') {
    urgency = maxUrgency(urgency, 'MONITOR')
    signals.push('Failed bounce pattern — recovery attempt rejected.')
  }
  if (feats.pattern_label === 'VOLATILITY_SPIKE') {
    urgency = maxUrgency(urgency, 'MONITOR')
    signals.push('Volatility spike detected — heightened risk of sharp moves.')
  }

  if (feats.deviation_from_h5_lower_band != null && feats.deviation_from_h5_lower_band < -0.01) {
    urgency = maxUrgency(urgency, 'MONITOR')
    signals.push(`Price is ${(Math.abs(feats.deviation_from_h5_lower_band) * 100).toFixed(1)}% below expected lower band — mean reversion signal.`)
  }

  const overlays = computeChartOverlays(bars)
  if (overlays.bollinger.lower.length > 0 && current != null) {
    const lastBBLower = overlays.bollinger.lower[overlays.bollinger.lower.length - 1]
    const lastBBUpper = overlays.bollinger.upper[overlays.bollinger.upper.length - 1]
    if (lastBBLower != null && side === 'LONG' && current < lastBBLower) {
      urgency = maxUrgency(urgency, 'MONITOR')
      signals.push('Price is below Bollinger lower band — mean reversion or breakdown in progress.')
    }
    if (lastBBUpper != null && side === 'LONG' && current > lastBBUpper) {
      signals.push('Price above Bollinger upper band — extended, consider partial profit taking.')
    }
  }

  if (pnl > 0 && entry != null && current != null) {
    const returnPct = side === 'LONG' ? (current - entry) / entry : (entry - current) / entry
    if (returnPct > 0.03) {
      signals.push(`Open gain of ${(returnPct * 100).toFixed(1)}% — consider trailing stop or partial exit.`)
    }
  }

  if (signals.length === 0) {
    signals.push('No immediate action signals. Position is within normal parameters.')
  }

  return {
    urgency,
    urgency_rank: EXIT_URGENCY[urgency] ?? 0,
    signals,
    headline: signals[0],
  }
}

function maxUrgency(current, candidate) {
  return (EXIT_URGENCY[candidate] ?? 0) > (EXIT_URGENCY[current] ?? 0) ? candidate : current
}

/* ─── Situational report generator ─── */

export function generateSituationalReport(tile, liveState, committee, exitRec) {
  const symbol = tile?.symbol || '???'
  const side = String(tile?.side || 'LONG').toUpperCase()
  const entry = toNum(tile?.entry_price)
  const current = toNum(liveState?.last_price) ?? toNum(tile?.current_price)
  const pnl = toNum(tile?.unrealized_pnl) ?? 0
  const feats = liveState?.derived_features || {}
  const isProtected = tile?.overlays?.stop_loss != null || tile?.overlays?.take_profit != null

  const returnPct = entry && current
    ? (side === 'LONG' ? (current - entry) / entry : (entry - current) / entry)
    : null

  const sections = []

  /* 1 - Position status */
  const pnlWord = pnl >= 0 ? 'in profit' : 'underwater'
  const returnStr = returnPct != null ? `${(returnPct * 100).toFixed(2)}%` : 'n/a'
  sections.push({
    title: 'Where We Are',
    text: `${symbol} ${side} position is currently ${pnlWord} at $${fmtNum(pnl, 2)} (${returnStr} from entry). ` +
      `Current price: ${fmtNum(current, 4)}. Entry was at ${fmtNum(entry, 4)}. ` +
      (isProtected ? 'Stop loss and/or take profit are set.' : 'No stop loss or take profit protection is active — you are flying without a net.'),
  })

  /* 2 - Market conditions */
  const regime = feats.regime_label || 'MIXED'
  const pattern = (feats.pattern_label || 'UNKNOWN').replace(/_/g, ' ').toLowerCase()
  const vol = feats.vol_15m != null ? `${(feats.vol_15m * 100).toFixed(2)}%` : 'unknown'
  sections.push({
    title: 'Market Conditions',
    text: `The current intraday regime is ${regime.replace(/_/g, ' ').toLowerCase()}. ` +
      `The dominant price pattern is "${pattern}" with 15-minute volatility at ${vol}. ` +
      (feats.ret_15m != null ? `Over the last 15 bars the price moved ${(feats.ret_15m * 100).toFixed(2)}%.` : ''),
  })

  /* 3 - Mean reversion / technical signals */
  const meanRevSignals = []
  if (feats.deviation_from_h5_median != null) {
    const devPct = (feats.deviation_from_h5_median * 100).toFixed(2)
    const devDir = feats.deviation_from_h5_median > 0 ? 'above' : 'below'
    meanRevSignals.push(`Price is ${Math.abs(Number(devPct)).toFixed(2)}% ${devDir} the expected median path.`)
  }
  if (feats.inside_cone === false) {
    meanRevSignals.push('Price has moved outside the expected trading cone — this is unusual and worth attention.')
  }
  if (feats.deviation_from_h5_lower_band != null && feats.deviation_from_h5_lower_band < -0.005) {
    meanRevSignals.push('Price is below the lower band of the expected range — a mean reversion snap-back or further breakdown is possible.')
  }
  if (meanRevSignals.length > 0) {
    sections.push({
      title: 'Mean Reversion Signals',
      text: meanRevSignals.join(' '),
    })
  }

  /* 4 - Do I need to watch closely? */
  const urgency = exitRec?.urgency || 'HOLD'
  let watchText
  if (urgency === 'EXIT_NOW') {
    watchText = 'YES — IMMEDIATE ATTENTION REQUIRED. Exit conditions have been triggered. Review the exit signals below and act now.'
  } else if (urgency === 'PREPARE') {
    watchText = 'YES — stay at your screen. Multiple warning signals are active and the position may need action within minutes. Key reasons: ' +
      (exitRec?.signals || []).slice(0, 2).join(' ')
  } else if (urgency === 'MONITOR') {
    watchText = 'Keep an eye on it. Some signals are developing but nothing requires immediate action yet. ' +
      (exitRec?.signals || []).slice(0, 1).join(' ')
  } else {
    watchText = 'No. The position is behaving within expected parameters. You can check back at the next refresh cycle.'
  }
  sections.push({
    title: 'Do I Need to Watch Closely?',
    text: watchText,
  })

  /* 5 - Committee consensus */
  const stance = committee?.committee_stance || 'UNKNOWN'
  const actions = (committee?.actions_to_consider || []).join(', ').toLowerCase().replace(/_/g, ' ')
  sections.push({
    title: 'Committee Consensus',
    text: `The committee stance is "${stance.replace(/_/g, ' ')}". ` +
      (committee?.headline_text || '') + ' ' +
      (actions ? `Suggested actions: ${actions}.` : ''),
  })

  /* 6 - Exit outlook */
  sections.push({
    title: 'Exit Outlook',
    text: exitRec?.signals?.join(' ') || 'No exit signals at this time.',
  })

  const overallUrgency = urgency
  const summaryLine = urgency === 'EXIT_NOW'
    ? `CRITICAL: ${symbol} needs immediate exit consideration.`
    : urgency === 'PREPARE'
      ? `WARNING: ${symbol} is approaching exit conditions — stay alert.`
      : urgency === 'MONITOR'
        ? `${symbol} has developing signals — worth monitoring but no rush.`
        : `${symbol} is tracking normally. No action needed right now.`

  return {
    symbol,
    timestamp: new Date().toISOString(),
    overall_urgency: overallUrgency,
    summary_line: summaryLine,
    sections,
  }
}

/* ─── Momentum gauge calculation ─── */

export function computeMomentumGauge(liveState) {
  const feats = liveState?.derived_features || {}
  const ret5 = toNum(feats.ret_5m) ?? 0
  const ret15 = toNum(feats.ret_15m) ?? 0
  const vol = toNum(feats.vol_15m) ?? 0.01
  const rawScore = (ret5 * 0.4 + ret15 * 0.6) / Math.max(vol, 0.001)
  const score = clamp(rawScore * 50, -100, 100)
  let label
  if (score > 60) label = 'STRONG_BULLISH'
  else if (score > 25) label = 'BULLISH'
  else if (score > -25) label = 'NEUTRAL'
  else if (score > -60) label = 'BEARISH'
  else label = 'STRONG_BEARISH'
  return { score: Math.round(score), label }
}

/* ─── Position health radar data ─── */

export function computeRadarData(tile, liveState, committee, exitRec, momentum) {
  const feats = liveState?.derived_features || {}
  const side = String(tile?.side || 'LONG').toUpperCase()

  /* 1 — Momentum (directional: bullish=good for longs, bearish=good for shorts) */
  const rawMom = momentum?.score ?? 0
  const directedMom = side === 'SHORT' ? -rawMom : rawMom
  const momentumScore = clamp((directedMom + 100) / 2, 0, 100)

  /* 2 — Trend Alignment (deviation from expected median path) */
  const devMedian = toNum(feats.deviation_from_h5_median)
  const insideCone = feats.inside_cone
  let trendScore = 50
  if (devMedian != null) {
    const absDev = Math.abs(devMedian)
    trendScore = clamp(100 - absDev * 2000, 0, 100)
  }
  if (insideCone === false) trendScore = Math.min(trendScore, 25)

  /* 3 — Volatility Safety (inverse of vol pressure) */
  const vol = toNum(feats.vol_15m) ?? 0.01
  const volScore = clamp(100 - (vol / 0.04) * 100, 0, 100)

  /* 4 — Risk Buffer (distance from SL as proportion of SL-to-TP range) */
  const distSl = Math.abs(toNum(tile?.progress_metrics?.distance_to_sl_pct) ?? 0.5)
  const distTp = Math.abs(toNum(tile?.progress_metrics?.distance_to_tp_pct) ?? 0.5)
  const totalRange = distSl + distTp
  const riskBufferScore = totalRange > 0
    ? clamp((distSl / totalRange) * 100, 0, 100)
    : 50

  /* 5 — Profit Progress (how far toward TP) */
  const progressRaw = toNum(tile?.progress_metrics?.progress_to_tp_pct)
  const profitScore = progressRaw != null
    ? clamp(progressRaw * 100, 0, 100)
    : 50

  /* 6 — Exit Pressure (inverted: HOLD=100, EXIT_NOW=5) */
  const urgencyMap = { HOLD: 100, MONITOR: 65, PREPARE: 30, EXIT_NOW: 5 }
  const exitScore = urgencyMap[exitRec?.urgency] ?? 100

  const axes = [
    { axis: 'Momentum', value: Math.round(momentumScore) },
    { axis: 'Trend', value: Math.round(trendScore) },
    { axis: 'Vol Safety', value: Math.round(volScore) },
    { axis: 'Risk Buffer', value: Math.round(riskBufferScore) },
    { axis: 'Profit', value: Math.round(profitScore) },
    { axis: 'Exit Calm', value: Math.round(exitScore) },
  ]
  const avg = axes.reduce((sum, a) => sum + a.value, 0) / axes.length

  let fillColor = '#22c55e'
  if (avg < 40) fillColor = '#ef4444'
  else if (avg < 60) fillColor = '#f59e0b'

  return { axes, avg: Math.round(avg), fillColor }
}
