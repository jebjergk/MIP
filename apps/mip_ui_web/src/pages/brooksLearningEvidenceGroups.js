/**
 * G2.1 — group stored evidence for the Evidence disclosure (presentation only).
 */

const KEY_CODES = new Set([
  'STRUCTURAL_SWING_LOW',
  'STRUCTURAL_SWING_HIGH',
  'CONFIRMED_BREAKOUT',
  'FAILED_BREAKOUT',
])

const BAR_CHARACTER_CODES = new Set([
  'DOJI',
  'INSIDE_BAR',
  'OUTSIDE_BAR',
  'SMALL_BAR',
  'BEAR_BAR',
  'BULL_BAR',
  'UPPER_TAIL',
  'LOWER_TAIL',
  'CLOSE_NEAR_LOW',
  'CLOSE_NEAR_HIGH',
  'CONSECUTIVE_BEAR_BARS',
  'CONSECUTIVE_BULL_BARS',
])

const STRUCTURE_CODES = new Set([
  'HIGHER_HIGH',
  'HIGHER_LOW',
  'LOWER_HIGH',
  'LOWER_LOW',
  'LOCAL_PIVOT_HIGH',
  'LOCAL_PIVOT_LOW',
  'STRUCTURAL_SWING_LOW',
  'STRUCTURAL_SWING_HIGH',
])

const SETUP_CODES = new Set([
  'PULLBACK_DEVELOPING',
  'TWO_LEGGED_PULLBACK',
  'POSSIBLE_H1_LONG',
  'POSSIBLE_H2_LONG',
  'POSSIBLE_PULLBACK',
  'LOCAL_DOUBLE_BOTTOM',
  'MICRO_DOUBLE_BOTTOM',
  'BEAR_MICRO_CHANNEL',
  'BULL_MICRO_CHANNEL',
  'POSSIBLE_BREAKOUT_BAR',
  'POSSIBLE_FOLLOW_THROUGH_BAR',
])

const CAUTION_CODES = new Set([
  'CONSECUTIVE_BEAR_BARS',
  'FAILED_BREAKOUT',
  'FAILED_H1_LONG',
  'DO_NOT_CHASE',
])

function codeOf(item) {
  return String(item?.code || item?.term || '').toUpperCase()
}

function plainOf(item) {
  return item?.plain || item?.term || codeOf(item)
}

function collectItems(narrative) {
  const fromEvidence = (narrative?.evidence || []).map((e) => ({
    code: e.code,
    plain: e.plain,
    lifecycle: e.lifecycle,
  }))
  const terms = narrative?.technical?.objective_terms || []
  const fromTerms = terms.map((t) => {
    const code = typeof t === 'string' ? t : t.term
    return { code, plain: t.plain || code, lifecycle: t.lifecycle }
  })
  const seen = new Set()
  const out = []
  for (const item of [...fromEvidence, ...fromTerms]) {
    const c = codeOf(item)
    if (!c || seen.has(c)) continue
    seen.add(c)
    out.push(item)
  }
  for (const p of narrative?.technical?.active_patterns || []) {
    const c = codeOf(p.pattern_family || p)
    if (!c || seen.has(c)) continue
    seen.add(c)
    out.push({
      code: c,
      plain: p.explanation || c.replace(/_/g, ' ').toLowerCase(),
      lifecycle: p.lifecycle,
    })
  }
  return out
}

function bucketItem(item, buckets) {
  const c = codeOf(item)
  const plain = plainOf(item)
  const lc = String(item.lifecycle || '').toUpperCase()
  const isConfirmedStructural = c.includes('STRUCTURAL_SWING') && lc === 'CONFIRMED'
    || plain.toLowerCase().includes('meaningful higher low')
    || plain.toLowerCase().includes('meaningful swing')

  if (isConfirmedStructural || (KEY_CODES.has(c) && lc === 'CONFIRMED')) {
    buckets.keyConclusion.push(plain)
    return
  }
  if (BAR_CHARACTER_CODES.has(c) || /bar|tail|doji|close near/i.test(plain)) {
    buckets.barCharacter.push(plain)
    return
  }
  if (STRUCTURE_CODES.has(c) || /higher high|higher low|pivot|swing/i.test(plain)) {
    buckets.structure.push(plain)
    return
  }
  if (CAUTION_CODES.has(c) || /consecutive bear|failed|not yet confirmed|caution/i.test(plain)) {
    buckets.caution.push(plain)
    return
  }
  if (SETUP_CODES.has(c) || /pullback|h1|h2|double bottom|micro channel|breakout/i.test(plain)) {
    buckets.setupContext.push(plain)
    return
  }
  buckets.setupContext.push(plain)
}

export function buildEvidenceGroups(narrative) {
  const buckets = {
    keyConclusion: [],
    barCharacter: [],
    structure: [],
    setupContext: [],
    caution: [],
  }
  for (const item of collectItems(narrative)) {
    bucketItem(item, buckets)
  }

  if (!buckets.keyConclusion.length && narrative?.headline) {
    const h = narrative.headline
    if (/profit protection|higher low|stop|entry conditions|protective stop/i.test(h)) {
      buckets.keyConclusion.push(h)
    }
  }

  const interpretation = inferInterpretation(narrative, buckets)

  return { ...buckets, interpretation, whyThisMattered: buildWhyThisMattered(narrative) }
}

export function buildWhyThisMattered(narrative) {
  if (!narrative) return null
  const headline = narrative.headline || ''
  if (/entry conditions became valid/i.test(headline)) {
    return (
      'The bar was small, but it appeared after a controlled pullback while the setup was already armed. '
      + 'That combination allowed a defined-risk entry.'
    )
  }
  if (/profit protection increased/i.test(headline)) {
    return (
      'The bar itself was not strongly bullish. Its importance was that it confirmed the prior pullback low '
      + 'as a meaningful higher low, allowing the stop to be raised.'
    )
  }
  if (/protective stop|trade closed/i.test(headline)) {
    return narrative.story?.why || null
  }
  return narrative.story?.why || null
}

function inferInterpretation(narrative, buckets) {
  const headline = narrative?.headline || ''
  if (/profit protection increased/i.test(headline)) {
    return (
      'The bar itself may not look strongly bullish, but stored structure shows the pullback '
      + 'held at a higher low. That confirmation is what justifies raising the protective stop.'
    )
  }
  if (buckets.keyConclusion.some((k) => /higher low|swing low/i.test(k))) {
    return (
      'The bar itself was not necessarily strongly bullish, but it confirmed that the pullback '
      + 'held at a higher structural low. That confirmation justified raising the protective stop.'
    )
  }
  const noticed = (narrative?.story?.system_noticed || []).join(' ')
  if (noticed.length > 40) return null
  return null
}

export function formatEvidenceGroupsForDisplay(groups) {
  const sections = []
  const mapItems = (items) => (items || []).map((plain) => (
    typeof plain === 'string' ? { plain, code: null } : plain
  ))
  if (groups.keyConclusion?.length) {
    sections.push({ title: 'Key conclusion', items: mapItems(groups.keyConclusion) })
  }
  if (groups.barCharacter?.length) {
    sections.push({ title: 'Bar character', items: mapItems(groups.barCharacter) })
  }
  if (groups.structure?.length) {
    sections.push({ title: 'Structure', items: mapItems(groups.structure) })
  }
  if (groups.setupContext?.length) {
    sections.push({ title: 'Setup context', items: mapItems(groups.setupContext) })
  }
  if (groups.caution?.length) {
    sections.push({ title: 'Caution', items: mapItems(groups.caution) })
  }
  return sections
}
