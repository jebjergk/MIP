/** Plain-language one-line hints for evidence terms (presentation only). */

const GLOSSARY = {
  UPPER_TAIL: 'Price traded higher during the bar but fell back before the close.',
  LOWER_TAIL: 'Price traded lower during the bar but recovered before the close.',
  INSIDE_BAR: 'The bar stayed within the high and low of the previous bar.',
  OUTSIDE_BAR: 'The bar traded above and below the previous bar’s range.',
  DOJI: 'Open and close were very close, showing indecision.',
  SMALL_BAR: 'The bar’s range was small compared with recent bars.',
  BEAR_BAR: 'The bar closed lower than it opened.',
  BULL_BAR: 'The bar closed higher than it opened.',
  CLOSE_NEAR_LOW: 'The close was near the bar’s low.',
  CLOSE_NEAR_HIGH: 'The close was near the bar’s high.',
  HIGHER_HIGH: 'The high was above the prior comparable high.',
  HIGHER_LOW: 'The current low remained above the previous comparable low.',
  LOWER_HIGH: 'The high remained below the prior comparable high.',
  LOWER_LOW: 'The low was below the prior comparable low.',
  LOCAL_PIVOT_LOW: 'A local turning point low formed on this bar or was confirmed here.',
  LOCAL_PIVOT_HIGH: 'A local turning point high formed on this bar or was confirmed here.',
  STRUCTURAL_SWING_LOW: 'A meaningful higher low was confirmed in the broader structure.',
  STRUCTURAL_SWING_HIGH: 'A meaningful swing high was confirmed in the broader structure.',
  BEAR_MICRO_CHANNEL: 'A short sequence where bars continue to make lower highs or lows.',
  BULL_MICRO_CHANNEL: 'A short sequence where bars continue to make higher highs or lows.',
  POSSIBLE_H1_LONG: 'A possible first attempt to resume the upward move after a pullback.',
  POSSIBLE_H2_LONG: 'A possible second attempt after the first long try.',
  PULLBACK_DEVELOPING: 'Price is pulling back after a move without breaking the setup.',
  TWO_LEGGED_PULLBACK: 'The pullback developed in two distinct pushes.',
  LOCAL_DOUBLE_BOTTOM: 'Two nearby lows may be forming a local double bottom.',
  MICRO_DOUBLE_BOTTOM: 'A very small double bottom may be forming.',
  POSSIBLE_BREAKOUT_BAR: 'The bar might be starting a breakout, but confirmation is still needed.',
  POSSIBLE_FOLLOW_THROUGH_BAR: 'The bar might be confirming the prior move.',
  CONSECUTIVE_BEAR_BARS: 'Several bear bars in a row show persistent selling pressure.',
  CONSECUTIVE_BULL_BARS: 'Several bull bars in a row show persistent buying pressure.',
  FAILED_BREAKOUT: 'A breakout attempt failed to follow through.',
  FAILED_H1_LONG: 'The first long attempt after the pullback did not hold.',
}

export function explainEvidenceTerm(code, plainLabel) {
  const key = String(code || '').toUpperCase()
  if (GLOSSARY[key]) return GLOSSARY[key]
  const plain = String(plainLabel || '').toLowerCase()
  for (const [k, v] of Object.entries(GLOSSARY)) {
    if (plain.includes(k.replace(/_/g, ' ').toLowerCase())) return v
  }
  if (/upper tail/i.test(plain)) return GLOSSARY.UPPER_TAIL
  if (/lower tail/i.test(plain)) return GLOSSARY.LOWER_TAIL
  if (/inside bar/i.test(plain)) return GLOSSARY.INSIDE_BAR
  if (/higher low/i.test(plain)) return GLOSSARY.HIGHER_LOW
  if (/h1/i.test(plain)) return GLOSSARY.POSSIBLE_H1_LONG
  if (/micro channel/i.test(plain) && /bear/i.test(plain)) return GLOSSARY.BEAR_MICRO_CHANNEL
  return null
}

export function enrichEvidenceItems(items) {
  return (items || []).map((item) => {
    const code = item.code || item
    const plain = item.plain || item
    return {
      code,
      plain: typeof plain === 'string' ? plain : String(plain),
      hint: explainEvidenceTerm(code, plain),
    }
  })
}
