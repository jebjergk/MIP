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

function classifyPattern(features) {
  if ((features.vol_15m ?? 0) > 0.022) return 'VOLATILITY_SPIKE'
  if ((features.ret_15m ?? 0) < -0.012 && (features.momentum_decay ?? 0) > 0.5) return 'RISK_OFF_BREAKDOWN'
  if ((features.ret_15m ?? 0) > 0.009 && (features.vs_benchmark ?? 0) > 0) return 'TREND_CONTINUATION'
  if ((features.ret_5m ?? 0) > 0.004 && (features.ret_15m ?? 0) <= 0) return 'FAILED_BOUNCE'
  if (Math.abs(features.ret_15m ?? 0) < 0.002 && (features.range_expansion ?? 0) < 0.2) return 'CHOP_NOISE'
  return 'WEAK_DRIFT'
}

export function buildLiveState(tile, previousLive = null) {
  const bars = latestBars(tile)
  const last = bars[bars.length - 1] || {}
  const prev5 = bars[Math.max(0, bars.length - 6)] || {}
  const prev15 = bars[Math.max(0, bars.length - 16)] || {}
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
  const volWindow = bars.slice(-16).map((b) => toNum(b.close)).filter((v) => Number.isFinite(v))
  const rets = []
  for (let i = 1; i < volWindow.length; i += 1) {
    if (volWindow[i - 1] > 0) rets.push((volWindow[i] / volWindow[i - 1]) - 1)
  }
  const mean = rets.length ? rets.reduce((a, b) => a + b, 0) / rets.length : 0
  const variance = rets.length ? rets.reduce((a, b) => a + ((b - mean) ** 2), 0) / rets.length : 0
  const vol15m = Math.sqrt(variance)
  const high = Math.max(...bars.slice(-16).map((b) => toNum(b.high) ?? -Infinity))
  const low = Math.min(...bars.slice(-16).map((b) => toNum(b.low) ?? Infinity))
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

  return {
    symbol: tile?.symbol,
    timestamp: new Date().toISOString(),
    last_price: current,
    bid: current != null ? current * 0.9995 : null,
    ask: current != null ? current * 1.0005 : null,
    day_change_pct: pctChange(close, bars[0]?.open),
    intraday_bars: bars.slice(-30),
    derived_features: features,
    live_news: (tile?.events || []).filter((e) => e?.type === 'NEWS').slice(0, 2),
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

  const previousByAgent = new Map((previousCommittee?.agent_messages || []).map((m) => [m.agent_name, m]))
  const outputs = []

  outputs.push(
    makeAgentOutput('POSITION_MANAGER_AGENT', symbol, {
      stance: underwater ? 'LAGGING' : 'ON_TRACK',
      reason_tags: [
        underwater ? 'UNDERWATER' : 'IN_PROFIT',
        isProtected ? 'PROTECTED' : 'UNPROTECTED',
      ],
      action_bias: underwater ? 'WATCH' : 'HOLD',
      materiality_score: underwater ? 0.68 : 0.4,
      short_text: underwater
        ? 'Thesis still open, but position is underwater and not progressing toward objective.'
        : 'Position behavior remains acceptable relative to entry objective.',
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
      ],
      action_bias: riskScore >= 0.8 ? 'ADD_PROTECTION' : riskScore >= 0.5 ? 'HOLD_WITH_MONITORING' : 'NO_ACTION',
      materiality_score: riskScore,
      short_text: riskScore >= 0.8
        ? 'Risk pressure is elevated: asymmetric downside with insufficient protection.'
        : riskScore >= 0.5
          ? 'Risk pressure is rising; maintain tighter monitoring and protection readiness.'
          : 'Risk is currently contained versus visible intraday volatility.',
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
        ? 'Live path moved outside the expected cone boundary; this is now abnormal versus trained behavior.'
        : 'Live path remains inside trained range, but follow-through quality should keep being monitored.',
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
        ? 'Current intraday regime is unsupportive for this position direction.'
        : 'Market regime is not actively opposing the current thesis.',
    }, previousByAgent.get('MARKET_REGIME_AGENT')),
  )

  const hasLiveNews = (liveState?.live_news || []).length > 0
  outputs.push(
    makeAgentOutput('NEWS_CATALYST_AGENT', symbol, {
      stance: hasLiveNews ? 'CATALYST_PRESENT' : 'NO_RELIABLE_NEWS_INPUT',
      reason_tags: hasLiveNews ? ['LIVE_NEWS_CONTEXT_AVAILABLE'] : ['NO_RELIABLE_NEWS_INPUT'],
      action_bias: hasLiveNews ? 'WATCH' : 'NO_ACTION',
      materiality_score: hasLiveNews ? 0.55 : 0.2,
      short_text: hasLiveNews
        ? 'Recent symbol news is present and should be weighed against current tape quality.'
        : 'No reliable fresh catalyst input in this cycle; continue price-action-led monitoring.',
    }, previousByAgent.get('NEWS_CATALYST_AGENT')),
  )

  const pattern = feats.pattern_label || 'WEAK_DRIFT'
  outputs.push(
    makeAgentOutput('INTRADAY_PATTERN_AGENT', symbol, {
      stance: pattern,
      reason_tags: [pattern, (feats.ret_15m ?? 0) < 0 ? 'WEAK_FOLLOW_THROUGH' : 'FOLLOW_THROUGH_OK'],
      action_bias: ['RISK_OFF_BREAKDOWN', 'FAILED_BOUNCE', 'VOLATILITY_SPIKE'].includes(pattern) ? 'PARTIAL_DE_RISK' : 'HOLD_WITH_MONITORING',
      materiality_score: ['RISK_OFF_BREAKDOWN', 'VOLATILITY_SPIKE'].includes(pattern) ? 0.82 : 0.52,
      short_text: `Intraday state is ${pattern.replaceAll('_', ' ').toLowerCase()}, with signal derived from returns, volatility, and momentum decay.`,
    }, previousByAgent.get('INTRADAY_PATTERN_AGENT')),
  )

  const dominantRisk = outputs.some((o) => ['RISK_HIGH', 'BELOW_LOWER_BAND', 'RISK_OFF_BREAKDOWN'].includes(o.stance))
  const elevated = outputs.some((o) => ['RISK_ELEVATED', 'REGIME_AGAINST', 'FAILED_BOUNCE', 'VOLATILITY_SPIKE'].includes(o.stance))
  const committee_stance = dominantRisk ? 'ESCALATE' : elevated ? 'WATCH_CLOSELY' : 'THESIS_INTACT'
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
  const actions_to_consider = [...new Set(outputs.map((o) => o.action_bias))]

  const disagreement_points = []
  const training = outputs.find((o) => o.agent_name === 'TRAINING_EXPECTATION_AGENT')
  const risk = outputs.find((o) => o.agent_name === 'RISK_AGENT')
  if (training?.stance !== 'BELOW_LOWER_BAND' && ['RISK_HIGH', 'RISK_ELEVATED'].includes(risk?.stance)) {
    disagreement_points.push('Training range remains partially valid, but risk pressure is rising faster than path quality.')
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
      ? 'Risk pressure is now dominant relative to trained drift.'
      : elevated
        ? 'Training context holds, but live quality requires close observation.'
        : 'Current behavior remains broadly aligned with thesis.',
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
    if (drift >= 0.0035) return true
  }
  return false
}

export function severityRank(stance) {
  return STANCE_SEVERITY[stance] || 0
}

export function confidenceRank(value) {
  return CONFIDENCE_RANK[value] || 0
}
