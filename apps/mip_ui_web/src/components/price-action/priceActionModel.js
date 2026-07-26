const firstDefined = (...values) => values.find((value) => value !== undefined && value !== null)

const asArray = (value) => (Array.isArray(value) ? value : [])
const asObject = (value) => (value && typeof value === 'object' && !Array.isArray(value) ? value : {})

export function field(object, ...names) {
  const source = asObject(object)
  for (const name of names) {
    if (source[name] !== undefined && source[name] !== null) return source[name]
    const upper = String(name).toUpperCase()
    if (source[upper] !== undefined && source[upper] !== null) return source[upper]
  }
  return undefined
}

export function datePart(value) {
  return value ? String(value).slice(0, 10) : ''
}

export function numberOrNull(value) {
  if (value === '' || value === undefined || value === null) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

export function provenanceOf(item, fallback = 'detected_geometry') {
  const source = String(field(item, 'source', 'provenance', 'annotation_source') || fallback).toLowerCase()
  return source.includes('methodologist') ? 'methodologist_interpretation' : 'detected_geometry'
}

function textList(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => (typeof item === 'string' ? item : field(item, 'text', 'label', 'name', 'description', 'concept')))
      .filter(Boolean)
      .map(String)
  }
  return value ? [String(value)] : []
}

function normalizeZone(zone, index) {
  const low = numberOrNull(firstDefined(field(zone, 'low', 'zone_low', 'price_low'), field(zone, 'price')))
  const high = numberOrNull(firstDefined(field(zone, 'high', 'zone_high', 'price_high'), field(zone, 'price')))
  if (low === null || high === null) return null
  const rawType = String(field(zone, 'type', 'zone_type', 'level_type') || 'support').toLowerCase()
  return {
    id: field(zone, 'id', 'zone_id') || `zone-${index}`,
    type: rawType.includes('resist') ? 'resistance' : 'support',
    low: Math.min(low, high),
    high: Math.max(low, high),
    startDate: datePart(field(zone, 'start_date', 'from_date', 'first_touch_date')),
    endDate: datePart(field(zone, 'end_date', 'to_date', 'last_touch_date')),
    label: String(field(zone, 'label', 'name') || (rawType.includes('resist') ? 'Resistance' : 'Support')),
    provenance: provenanceOf(zone),
    detail: field(zone, 'detail', 'reason', 'interpretation', 'notes'),
  }
}

function normalizeRange(range, index) {
  const low = numberOrNull(field(range, 'low', 'range_low', 'bottom'))
  const high = numberOrNull(field(range, 'high', 'range_high', 'top'))
  if (low === null || high === null) return null
  return {
    id: field(range, 'id', 'range_id') || `range-${index}`,
    low: Math.min(low, high),
    high: Math.max(low, high),
    startDate: datePart(field(range, 'start_date', 'from_date', 'date_start')),
    endDate: datePart(field(range, 'end_date', 'to_date', 'date_end')),
    label: String(field(range, 'label', 'name') || 'Trading range'),
    provenance: provenanceOf(range),
    detail: field(range, 'detail', 'reason', 'interpretation', 'notes'),
  }
}

function normalizeSwing(swing, index) {
  const price = numberOrNull(field(swing, 'price', 'level', 'value'))
  const date = datePart(field(swing, 'date', 'bar_date', 'timestamp', 'ts'))
  if (price === null || !date) return null
  return {
    id: field(swing, 'id', 'swing_id') || `swing-${index}`,
    date,
    price,
    label: String(field(swing, 'label', 'swing_type', 'type') || 'Swing'),
    provenance: provenanceOf(swing),
    detail: field(swing, 'detail', 'reason', 'interpretation', 'notes'),
  }
}

function normalizeNote(note, index) {
  if (typeof note === 'string') {
    return { id: `note-${index}`, text: note, provenance: 'methodologist_interpretation' }
  }
  const text = field(note, 'text', 'note', 'label', 'description', 'interpretation')
  if (!text) return null
  return {
    id: field(note, 'id') || `note-${index}`,
    text: String(text),
    date: datePart(field(note, 'date', 'bar_date', 'timestamp')),
    price: numberOrNull(field(note, 'price', 'level')),
    provenance: provenanceOf(note, 'methodologist_interpretation'),
  }
}

function longOnlyVerdict(value) {
  const verdict = String(value || 'Analysis complete')
  if (/\b(short|sell short)\b/i.test(verdict)) return 'NO LONG SETUP'
  return verdict
}

export function normalizeAnalysisResponse(payload) {
  const root = asObject(payload)
  const result = asObject(firstDefined(root.result, root.analysis, root.data, root))
  const geometry = asObject(firstDefined(result.detected_geometry, root.detected_geometry))
  const methodologist = asObject(firstDefined(
    result.methodologist,
    result.methodologist_interpretation,
    result.expert_read,
    root.methodologist,
  ))
  const chart = asObject(firstDefined(result.chart, geometry.chart))
  const rawNotes = firstDefined(
    field(methodologist, 'notes', 'annotations'),
    field(result, 'methodologist_notes'),
  )
  const expertRead = asObject(field(methodologist, 'expert_read'))
  const plainExplanation = asObject(field(methodologist, 'plain_explanation'))
  const verdict = asObject(field(methodologist, 'verdict'))
  const knowledge = asArray(field(result, 'methodologist_knowledge'))
  const ragStatus = String(field(result, 'rag_status') || 'DISABLED')
  const retrievalCount = Number(field(result, 'retrieval_count') || 0)
  const cardCount = Number(field(result, 'card_count') || 0)
  const totalChars = Number(field(result, 'total_chars') || 0)

  const candles = asArray(firstDefined(
    result.candles,
    result.bars,
    result.price_bars,
    chart.candles,
    chart.bars,
    geometry.candles,
    geometry.bars,
  ))

  const zones = asArray(firstDefined(
    geometry.support_resistance_zones,
    geometry.zones,
    result.support_resistance_zones,
    result.zones,
  )).map(normalizeZone).filter(Boolean)

  const supportLevels = asArray(firstDefined(geometry.support_levels, result.support_levels))
    .map((price, index) => normalizeZone({ price, type: 'support' }, `support-${index}`))
    .filter(Boolean)
  const resistanceLevels = asArray(firstDefined(geometry.resistance_levels, result.resistance_levels))
    .map((price, index) => normalizeZone({ price, type: 'resistance' }, `resistance-${index}`))
    .filter(Boolean)

  return {
    raw: root,
    symbol: String(firstDefined(field(result, 'symbol'), field(root, 'symbol')) || ''),
    status: String(
      firstDefined(
        field(methodologist, 'analysis_status'),
        field(result, 'status'),
        field(root, 'status'),
      ) || 'COMPLETE',
    ),
    cost: firstDefined(
      field(result, 'cost', 'cost_usd', 'analysis_cost'),
      field(root, 'cost', 'cost_usd', 'analysis_cost'),
      field(root.usage, 'cost_usd'),
      {
        status: `RAG ${ragStatus}`,
        amount: `${retrievalCount}/3 calls · ${cardCount}/8 cards · ${totalChars}/8000 chars`,
      },
    ),
    verdict: longOnlyVerdict(firstDefined(
      field(verdict, 'decision'),
      field(result, 'verdict', 'long_verdict'),
    )),
    verdictReason: firstDefined(
      field(verdict, 'reason_summary'),
      field(methodologist, 'summary'),
    ),
    expertRead: firstDefined(
      Object.keys(expertRead).length ? expertRead : undefined,
      field(methodologist, 'read', 'analysis', 'assessment'),
      field(result, 'methodologist_read', 'expert_read'),
    ),
    simpleExplanation: firstDefined(
      Object.keys(plainExplanation).length ? plainExplanation : undefined,
      field(result, 'simple_explanation', 'plain_english'),
    ),
    supportingFactors: textList(firstDefined(
      field(expertRead, 'what_supports_the_long'),
      field(methodologist, 'supporting_factors', 'supports'),
      field(result, 'supporting_factors', 'supports'),
    )),
    warningFactors: textList(firstDefined(
      field(expertRead, 'what_weakens_the_long'),
      field(methodologist, 'warning_factors', 'warnings', 'risks'),
      field(result, 'warning_factors', 'warnings', 'risks'),
    )),
    concepts: textList(firstDefined(
      knowledge,
      field(methodologist, 'knowledge_concepts', 'concepts'),
      field(result, 'knowledge_concepts', 'concepts'),
    )),
    ragStatus,
    retrievalCount,
    cardCount,
    totalChars,
    candles,
    swings: asArray(firstDefined(geometry.swings, geometry.swing_points, result.swings))
      .map(normalizeSwing).filter(Boolean),
    zones: [...zones, ...supportLevels, ...resistanceLevels],
    ranges: asArray(firstDefined(geometry.range_boxes, geometry.ranges, result.range_boxes, result.ranges))
      .map(normalizeRange).filter(Boolean),
    notes: (Array.isArray(rawNotes) ? rawNotes : (rawNotes ? [rawNotes] : []))
      .map(normalizeNote).filter(Boolean),
    currentPrice: numberOrNull(firstDefined(
      field(result, 'current_price', 'last_price'),
      field(chart, 'current_price', 'last_price'),
    )),
  }
}

export function buildChartData(candles) {
  let ema = null
  const multiplier = 2 / 21
  return asArray(candles).map((bar) => {
    const open = numberOrNull(field(bar, 'open'))
    const high = numberOrNull(field(bar, 'high'))
    const low = numberOrNull(field(bar, 'low'))
    const close = numberOrNull(field(bar, 'close'))
    const suppliedEma = numberOrNull(field(bar, 'ema20', 'ema_20'))
    if (suppliedEma !== null) ema = suppliedEma
    else if (close !== null) ema = ema === null ? close : (close - ema) * multiplier + ema
    return {
      date: datePart(field(bar, 'date', 'bar_date', 'timestamp', 'ts')),
      open,
      high,
      low,
      close,
      wick: low !== null && high !== null ? [low, high] : null,
      ema20: ema,
    }
  }).filter((bar) => bar.date && bar.wick)
}
