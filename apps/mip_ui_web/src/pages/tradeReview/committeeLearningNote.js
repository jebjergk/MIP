import {
  alignmentBucket,
  alignmentPlainLanguage,
  committeeSentence,
  labelPwRegretDriver,
  isReconAttention,
} from './formatters.js'

/** Stable keys for Phase 3 aggregation (GROUP BY / counts). */
export const AGGREGATION_KEYS = Object.freeze({
  CAUTION_RECON: 'caution_recon_integrity',
  CAUTION_NO_EIS: 'caution_no_eis',
  CAUTION_NO_PW: 'caution_no_pw_context',
  CAUTION_COMMITTEE_UNKNOWN: 'caution_committee_unknown',
  CAUTION_THIN_CONTEXT: 'caution_thin_outcome_context',
  PATTERN_REDUCE_FAVORABLE: 'pattern_reduce_favorable',
  PATTERN_REDUCE_UNFAVORABLE: 'pattern_reduce_unfavorable',
  PATTERN_BLOCK_FAVORABLE: 'pattern_block_favorable_outcome',
  PATTERN_BLOCK_UNFAVORABLE: 'pattern_block_unfavorable_outcome',
  PATTERN_PW_FOCUS: 'pattern_pw_regret_focus',
  PATTERN_ALIGNED: 'pattern_aligned_expectation',
  PATTERN_NEUTRAL: 'pattern_neutral_memory',
})

function boolish(v) {
  return v === true || v === 1 || v === 'true' || v === '1'
}

function pwMeaningful(row) {
  const d = row.best_pw_vs_actual_delta
  const r = row.pw_regret_amount
  if (d != null && Number.isFinite(Number(d)) && Number(d) > 0) return true
  if (r != null && Number.isFinite(Number(r)) && Number(r) > 0) return true
  return false
}

/**
 * Deterministic committee learning artifact + Phase 3 rollup fields.
 * @param {Record<string, unknown>} row — TIR trade row
 * @param {Record<string, unknown>|null|undefined} _meta — API meta (reserved)
 */
export function buildCommitteeLearningNote(row, _meta) {
  const committee = String(row.committee_action_normalized || '').toUpperCase() || 'UNKNOWN'
  const hasEis = boolish(row.has_eis)
  const ab = alignmentBucket(row.alignment_class, row.outcome_class)
  const reconRisk = isReconAttention(row.reconciliation_class)
  const pwOk = pwMeaningful(row)

  const tags = []
  if (committee === 'REDUCE') tags.push('committee_reduce')
  else if (committee === 'BLOCK') tags.push('committee_block')
  else tags.push('committee_unknown')
  tags.push(`alignment_${ab}`)
  tags.push(hasEis ? 'eis_present' : 'eis_missing')
  tags.push(pwOk ? 'pw_present' : 'pw_missing')
  if (reconRisk) tags.push('recon_caution')
  if (row.pw_regret_driver) {
    tags.push(`pw_driver_${String(row.pw_regret_driver).replace(/\s+/g, '_')}`)
  }

  let bodyText = `${alignmentPlainLanguage(ab)} ${committeeSentence(row.committee_action_normalized, row.committee_action_raw)}`
  if (!hasEis) bodyText += ' No entry-intelligence expectation (EIS) was linked for this closeout.'
  if (pwOk) {
    const lbl = labelPwRegretDriver(row.pw_regret_driver) || 'the dominant policy dimension'
    bodyText += ` Portfolio-day Parallel Worlds context on the exit date suggests the largest hypothetical improvement sat with ${lbl} (not a trade-path replay).`
  } else {
    bodyText += ' No portfolio-day Parallel Worlds regret signal was available for this exit date.'
  }

  let takeawayType = 'pattern'
  let takeawayText = ''
  let aggregationKey = AGGREGATION_KEYS.PATTERN_NEUTRAL

  if (reconRisk) {
    takeawayType = 'caution'
    aggregationKey = AGGREGATION_KEYS.CAUTION_RECON
    takeawayText =
      'When this situation appears again: resolve broker vs MIP reconciliation for the symbol before treating this trade file as fully trustworthy for committee lessons.'
  } else if (!hasEis) {
    takeawayType = 'caution'
    aggregationKey = AGGREGATION_KEYS.CAUTION_NO_EIS
    takeawayText =
      'When this situation appears again: attach or verify an EIS snapshot so reviewers can compare realized outcome to a stated expectation.'
  } else if (committee === 'UNKNOWN') {
    takeawayType = 'caution'
    aggregationKey = AGGREGATION_KEYS.CAUTION_COMMITTEE_UNKNOWN
    takeawayText =
      'When this situation appears again: capture an explicit committee recommendation (reduce, block, or full proceed) so post-trade review can link stance to outcome.'
  } else if (ab === 'unknown') {
    takeawayType = 'caution'
    aggregationKey = AGGREGATION_KEYS.CAUTION_THIN_CONTEXT
    takeawayText =
      'When this situation appears again: ensure alignment and outcome labels are populated for closed trades so learning is not guesswork.'
  } else if (ab === 'aligned' && hasEis) {
    aggregationKey = AGGREGATION_KEYS.PATTERN_ALIGNED
    takeawayText =
      'Remember for similar cases: explicit expectation plus aligned realization is a clean control—reuse the same gate discipline when the setup matches.'
  } else if (committee === 'REDUCE' && ab === 'favorable') {
    aggregationKey = AGGREGATION_KEYS.PATTERN_REDUCE_FAVORABLE
    takeawayText =
      'Remember for similar cases: committee sized below alpha but the trade still finished favorably—note when conservative sizing coincides with upside.'
  } else if (committee === 'REDUCE' && ab === 'unfavorable') {
    aggregationKey = AGGREGATION_KEYS.PATTERN_REDUCE_UNFAVORABLE
    takeawayText =
      'Remember for similar cases: committee sized below alpha and the trade underperformed—ask whether reduction was protective or starved a valid edge.'
  } else if (committee === 'BLOCK' && ab === 'favorable') {
    aggregationKey = AGGREGATION_KEYS.PATTERN_BLOCK_FAVORABLE
    takeawayText =
      'Remember for similar cases: a favorable realized outcome still warrants checking whether a block or reduce would have been safer under the same macro context.'
  } else if (committee === 'BLOCK' && ab === 'unfavorable') {
    aggregationKey = AGGREGATION_KEYS.PATTERN_BLOCK_UNFAVORABLE
    takeawayText =
      'Remember for similar cases: committee blocked relative to alpha while the book underperformed—use this pairing to calibrate how aggressively to intervene next time.'
  } else if (pwOk && row.pw_regret_driver) {
    aggregationKey = AGGREGATION_KEYS.PATTERN_PW_FOCUS
    const lbl = labelPwRegretDriver(row.pw_regret_driver) || String(row.pw_regret_driver)
    takeawayText = `Remember for similar cases: exit-date PW deltas emphasized ${lbl}—start policy discussion there for comparable exits.`
  } else if (!pwOk) {
    takeawayType = 'caution'
    aggregationKey = AGGREGATION_KEYS.CAUTION_NO_PW
    takeawayText =
      'When this situation appears again: confirm portfolio-day PW diffs exist for the exit date before using counterfactual regret in committee discussion.'
  } else {
    aggregationKey = AGGREGATION_KEYS.PATTERN_NEUTRAL
    takeawayText =
      'Remember for similar cases: brief committee stance, realized outcome, and PW context together when the next parallel symbol setup appears.'
  }

  const displayParagraph = `${bodyText} ${takeawayText}`.replace(/\s+/g, ' ').trim()

  return {
    takeaway_type: takeawayType,
    takeaway_text: takeawayText,
    body_text: bodyText,
    tags,
    aggregation_key: aggregationKey,
    display_paragraph: displayParagraph,
  }
}
