/**
 * Presentation-only tile copy + anti-repetition for Live Intelligence Cockpit.
 * Derives shorter thesis, filtered drivers, and chip visibility from existing intel + tile fields.
 */

import { resolveDecisionPresentation } from './licDecisionPresentation'

function str(v, fb = '') {
  if (v == null || v === '') return fb
  if (typeof v === 'string' || typeof v === 'number') return String(v)
  return fb
}

function asArray(x) {
  return Array.isArray(x) ? x : []
}

export function normChip(s) {
  return String(s || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
}

function symbolSalt(sym) {
  const s = String(sym || '').toUpperCase()
  let h = 0
  for (let i = 0; i < s.length; i += 1) h = (h * 31 + s.charCodeAt(i)) >>> 0
  return h
}

/** API / template drivers that add little when repeated across tiles */
const GENERIC_DRIVER_PATTERNS = [
  /quiet tape/i,
  /no strong directional conviction/i,
  /small moves/i,
  /historical parallels are thin/i,
  /lean less on backward/i,
  /backward-looking stats/i,
  /mostly symbol-specific/i,
  /^symbol[- ]specific/i,
  /^thesis is intact versus the expected path\.?$/i,
  /thesis is intact versus the expected path/i,
  /conditions feel familiar versus recent training/i,
  /novelty is normal vs recent training/i,
]

export function isGenericDriverText(text) {
  const t = String(text || '').trim()
  if (!t) return true
  return GENERIC_DRIVER_PATTERNS.some((re) => re.test(t))
}

function medianPrice(tile) {
  const exp = tile?.expectation?.center_path
  if (!Array.isArray(exp) || !exp[0]) return null
  const p = Number(exp[0].price)
  return Number.isFinite(p) ? p : null
}

/** Single strongest stop-context line (consolidates API + contextual duplicates). */
function stopDriverLine(tile, intel) {
  const pm = tile?.progress_metrics || {}
  const dsl = Number(pm.distance_to_sl_pct)
  if (Number.isFinite(dsl) && dsl >= 0 && dsl < 0.025) {
    return `Stop is close; small adverse moves can matter quickly.`
  }
  if (Number.isFinite(dsl) && dsl >= 0 && dsl < 0.035) {
    return `Stop buffer tight (${(dsl * 100).toFixed(1)}% air) — adverse ticks matter.`
  }
  if (Number.isFinite(dsl) && dsl >= 0 && dsl < 0.08) {
    return `Meaningful room to stop still, but cushion is finite.`
  }
  const raw = asArray(intel?.decision_drivers).join(' ')
  if (/stop is close|distance to stop|near stop|stop buffer/i.test(raw)) {
    const kept = asArray(intel?.decision_drivers).find(
      (d) => /stop|buffer|adverse/i.test(String(d)) && !isGenericDriverText(d),
    )
    if (kept) return str(kept).trim()
  }
  return null
}

function targetDriverLine(tile) {
  const pm = tile?.progress_metrics || {}
  const dtp = Number(pm.distance_to_tp_pct)
  if (Number.isFinite(dtp) && Math.abs(dtp) < 0.045) {
    return `Target zone nearby — upside may be partly priced.`
  }
  if (Number.isFinite(dtp) && dtp > 0.06) {
    return `Target still has runway if path holds.`
  }
  return null
}

function pathVsExpectedLine(tile) {
  const med = medianPrice(tile)
  const cur = Number(tile?.current_price)
  if (Number.isFinite(cur) && Number.isFinite(med)) {
    if (cur < med * 0.998) {
      return `Live path below expectation median — needs to prove the modeled path.`
    }
    if (cur > med * 1.002) {
      return `Live path above expectation median — reward partly realized vs plan.`
    }
  }
  return null
}

function pnlDriverLine(tile) {
  const pnl = Number(tile?.unrealized_pnl)
  if (!Number.isFinite(pnl)) return null
  if (pnl < 0) return `Position underwater — risk discipline outweighs patience.`
  if (pnl > 0) return `P&L green — room to let thesis play if stops hold.`
  return null
}

function analogDriverLine(intel) {
  const tier = str(intel?.analog_tier_key, '')
  if (tier === 'strong') return `Strong analog match — treat history as meaningful context.`
  if (tier === 'weak') return `Weak analog match — lean on tape and limits over averages.`
  return null
}

function factorDriverLine(intel) {
  const loc = str(intel?.portfolio_factor_local?.localized, '')
  if (loc === 'portfolio_wide') {
    return `Shared book stress — factor risk stacks with this name.`
  }
  if (loc === 'mixed') {
    return `Portfolio stress flagged — watch spillover into this tape.`
  }
  return null
}

function thesisDriverLine(intel) {
  const th = String(intel?.thesis_fracture || '').toUpperCase()
  if (th === 'THESIS_STRETCHED') return `Thesis stretched versus path — margin for error is thinner.`
  if (th === 'THESIS_DAMAGED') return `Thesis damaged — multiple signals disagree with the original plan.`
  if (th === 'THESIS_BROKEN') return `Thesis broken for risk purposes — narrative no longer carries full size.`
  return null
}

/**
 * When path driver already states live below expectation median, avoid repeating the same failure
 * in thesis (path vs risk vs reward stay dimensionally distinct).
 */
function thesisDriverLineDeduped(tile, intel) {
  const pathLine = pathVsExpectedLine(tile)
  const pathAlreadyBelow =
    pathLine && /below expectation median|below.*modeled path/i.test(String(pathLine))
  if (!pathAlreadyBelow) return thesisDriverLine(intel)

  const th = String(intel?.thesis_fracture || '').toUpperCase()
  if (th === 'THESIS_STRETCHED') {
    return `Opening thesis still allows noise — focus on whether the plan stays internally consistent.`
  }
  if (th === 'THESIS_DAMAGED') {
    return `The plan's core assumptions no longer line up cleanly — risk framing matters more than the headline story.`
  }
  if (th === 'THESIS_BROKEN') {
    return `Treat the original thesis as closed for sizing — from here, discipline is about limits and liquidity.`
  }
  return thesisDriverLine(intel)
}

/** Buckets for at-most-one driver per dimension. */
const BUCKET_ORDER = ['stop', 'thesis', 'pathVsExpected', 'target', 'pnl', 'analog', 'factor', 'other']

export function classifyDriverBucket(text) {
  const t = String(text || '').toLowerCase()
  if (/(^|[^a-z])(stop|stops)\b|buffer to stop|distance to stop|adverse tick|near.stop|small adverse move/i.test(t)) {
    return 'stop'
  }
  if (/\bthesis\b|original idea|invalidat/i.test(t)) return 'thesis'
  if (/expectation median|median path|live path|tape pattern|volatility|quiet tape|path needs/i.test(t)) {
    return 'pathVsExpected'
  }
  if (/target|take.profit|upside room|priced in|tp zone/i.test(t)) return 'target'
  if (/analog|historical|parallel|backward-looking/i.test(t)) return 'analog'
  if (/p&l|\bpnl\b|underwater|position.*green/i.test(t)) return 'pnl'
  if (/portfolio|shared.factor|book.?wide|spillover|stressed together/i.test(t)) return 'factor'
  return 'other'
}

function scoreCandidateForBucket(text, bucket) {
  const t = String(text || '').toLowerCase()
  if (bucket === 'stop') {
    if (/small adverse|stop is close|very close|tight/i.test(t)) return 3
    if (/buffer tight|adverse/i.test(t)) return 2
    return 1
  }
  return t.length > 0 ? 1 : 0
}

/**
 * Up to 3 drivers: one per semantic bucket, distinct dimensions only.
 */
export function buildTileDrivers(tile, intel) {
  if (!intel || typeof intel !== 'object') return []

  const bucketBest = new Map()

  const trySet = (bucket, line) => {
    if (!line || isGenericDriverText(line)) return
    const b = bucket === 'other' ? classifyDriverBucket(line) : bucket
    const prev = bucketBest.get(b)
    if (!prev || scoreCandidateForBucket(line, b) > scoreCandidateForBucket(prev, b)) {
      bucketBest.set(b, line)
    }
  }

  const structured = [
    ['stop', stopDriverLine(tile, intel)],
    ['thesis', thesisDriverLineDeduped(tile, intel)],
    ['pathVsExpected', pathVsExpectedLine(tile)],
    ['target', targetDriverLine(tile)],
    ['pnl', pnlDriverLine(tile)],
    ['analog', analogDriverLine(intel)],
    ['factor', factorDriverLine(intel)],
  ]
  for (const [b, line] of structured) {
    trySet(b, line)
  }

  const raw = asArray(intel?.decision_drivers).map((d) => str(d).trim()).filter(Boolean)
  for (const t of raw) {
    if (isGenericDriverText(t)) continue
    const b = classifyDriverBucket(t)
    trySet(b, t)
  }

  const out = []
  for (const b of BUCKET_ORDER) {
    const line = bucketBest.get(b)
    if (line && !out.some((x) => normChip(x) === normChip(line))) {
      out.push(line)
    }
    if (out.length >= 3) break
  }
  return out.slice(0, 3)
}

function oneSentence(s) {
  let t = String(s || '').trim()
  if (!t) return t
  const semi = t.search(/;\s+/)
  const firstDot = t.indexOf('. ')
  if (semi > 0 && semi < 220 && (firstDot < 0 || semi < firstDot)) {
    t = t.slice(0, semi).trim()
    if (t && !/[.!?]$/.test(t)) t += '.'
  }
  const idx = t.search(/\.\s+[A-Z]/)
  if (idx > 0 && idx < 220) {
    t = t.slice(0, idx + 1)
  } else {
    const first = t.indexOf('. ')
    if (first > 0 && first < 220) t = t.slice(0, first + 1)
  }
  if (t.length > 220) t = `${t.slice(0, 217)}…`
  return t
}

function stripPrefixOverlap(thesis, strip) {
  const a = normChip(thesis)
  const b = normChip(strip)
  if (!a || !b) return false
  if (a.includes(b.slice(0, Math.min(36, b.length))) || b.includes(a.slice(0, Math.min(36, a.length)))) {
    return true
  }
  const aw = a.split(' ').filter(Boolean)
  const bw = b.split(' ').filter(Boolean)
  let common = 0
  for (const w of aw.slice(0, 8)) {
    if (bw.includes(w) && w.length > 3) common += 1
  }
  return common >= 4
}

/**
 * One thesis sentence: does not repeat the recommendation headline; explains why stance is still valid.
 */
export function buildTileThesisLine(symbol, tile, intel, _primaryAction, variantOffset = 0, fallbackStrip = '') {
  if (!intel || typeof intel !== 'object') {
    return 'Intelligence still loading for this symbol.'
  }
  const th = String(intel?.thesis_fracture || 'THESIS_INTACT').toUpperCase()
  const band = String(intel?.final_recommendation || 'STAY_COURSE').toUpperCase()
  const pm = tile?.progress_metrics || {}
  const dsl = Number(pm.distance_to_sl_pct)
  const dtp = Number(pm.distance_to_tp_pct)
  const pnl = Number(tile?.unrealized_pnl)
  const salt = (symbolSalt(symbol) + Number(variantOffset) * 13) % 4

  const nearSl = Number.isFinite(dsl) && dsl >= 0 && dsl < 0.04
  const nearTp = Number.isFinite(dtp) && Math.abs(dtp) < 0.05
  const underwater = Number.isFinite(pnl) && pnl < 0
  const green = Number.isFinite(pnl) && pnl > 0

  const pools = {
    intact_hold: [
      'Primary stance stays justified while live path still tracks the modeled center and stop is not forcing de-risk.',
      'The official posture still holds because structure has not broken and buffer to risk is acceptable for now.',
      'Conviction rests on price staying clear of stop pressure while the thesis remains coherent with tape.',
      'Reward-to-risk only narrows if upside stalls here — until then the path is still credible.',
    ],
    intact_watch: [
      'Watch posture fits because upside still exists but room to target is shrinking versus recent softness.',
      'Tape is noisy yet thesis is not broken — the next sequences decide whether buffer is enough.',
      'One adverse stretch could flip posture quickly; for now the ladder has not forced a harder exit.',
    ],
    stretched: [
      'Thesis is stretched: stance is conditional on risk lines staying honest and price stabilizing.',
      'Setup is alive but fragile — cleaner follow-through is needed to keep the current posture honest.',
      'Drift from the core story means invalidation risk rises if structure does not improve soon.',
    ],
    damaged: [
      'Thesis is damaged — remaining exposure is risk-managed, not a full bet on the original story.',
      'Conflicting signals versus the plan mean size and stops matter more than narrative from here.',
    ],
    broken: [
      'Thesis is broken for practical risk — capital preservation outweighs waiting for narrative repair.',
      'Invalidation dominates — treat any bounce as a new context, not confirmation of the old trade.',
    ],
  }

  let pool = pools.intact_hold
  if (th === 'THESIS_STRETCHED') pool = pools.stretched
  else if (th === 'THESIS_DAMAGED') pool = pools.damaged
  else if (th === 'THESIS_BROKEN') pool = pools.broken
  else if (band === 'WATCH_CLOSELY' || band === 'PREPARE_EXIT') pool = pools.intact_watch

  const pick = (salt + Number(variantOffset) * 3) % pool.length
  let line = pool[pick]

  if (band === 'EXIT_NOW') {
    line =
      'Exit posture is primary because stop proximity and/or thesis damage no longer justify full exposure at this risk.'
  } else if (band === 'PREPARE_EXIT') {
    line =
      'Prepare-exit posture fits because reward-to-risk has narrowed enough that a clean plan beats hoping for more tape.'
  } else if (nearSl) {
    line =
      'Stop pressure is elevated; primary stance still assumes the buffer holds through the next tape sequences.'
  } else if (nearTp && green) {
    line =
      'Target is in play; stance assumes you can still harvest upside without giving back disproportionate reward.'
  } else if (underwater && nearSl) {
    line = 'Underwater with stop nearby — posture is fragile and invalidation is one bad sequence away.'
  } else if (underwater) {
    line =
      'Underwater but not at rails — whether to stay depends on path repair versus further drift toward stop.'
  }

  line = oneSentence(line)

  if (fallbackStrip && stripPrefixOverlap(line, fallbackStrip)) {
    for (let k = 1; k < 10; k += 1) {
      const alt = buildTileThesisLine(symbol, tile, intel, _primaryAction, k, '')
      if (!stripPrefixOverlap(alt, fallbackStrip)) {
        return oneSentence(alt)
      }
    }
  }

  return line
}

function countBy(values) {
  const m = new Map()
  for (const v of values) {
    const k = normChip(v)
    if (!k) continue
    m.set(k, (m.get(k) || 0) + 1)
  }
  return m
}

/**
 * When a chip value is shared by most visible tiles, hide that chip row grid-wide.
 */
export function computeChipSuppressGrid(ranked, intelligenceBySymbol) {
  const n = ranked.length
  const threshold = n <= 1 ? 999 : Math.max(2, Math.ceil(n * 0.5))
  const types = ['attention', 'analog', 'factor', 'regret']
  const columns = { attention: [], analog: [], factor: [], regret: [] }

  for (const t of ranked) {
    const s = String(t.symbol || '').toUpperCase()
    const intel = intelligenceBySymbol[s]
    const au = intel?.analog_ui || {}
    columns.attention.push(str(intel?.attention_band, ''))
    columns.analog.push(str(au.chip_verdict || au.confidence_plain, ''))
    columns.factor.push(str(intel?.portfolio_factor_chip, ''))
    columns.regret.push(str(intel?.regret_tilt_label, ''))
  }

  const suppress = {}
  for (const ty of types) {
    const counts = countBy(columns[ty])
    const maxFreq = counts.size ? Math.max(...counts.values()) : 0
    suppress[ty] = maxFreq >= threshold
  }
  return suppress
}

/**
 * Anti-repetition: avoid identical thesis / first driver across visible symbols.
 */
function dedupeAcrossTiles(rows) {
  const usedThesis = new Set()
  const usedFirstDriver = new Set()

  return rows.map((row) => {
    let { thesis, drivers } = row
    const tKey = normChip(thesis)
    if (tKey && usedThesis.has(tKey)) {
      for (let k = 1; k < 10; k += 1) {
        const alt = buildTileThesisLine(
          row.symbol,
          row.tile,
          row.intel,
          row.primaryAction,
          k,
          row.fallbackStrip || '',
        )
        const ak = normChip(alt)
        if (ak && !usedThesis.has(ak)) {
          thesis = alt
          break
        }
      }
    }
    if (normChip(thesis)) usedThesis.add(normChip(thesis))

    if (drivers.length && drivers[0]) {
      const d0 = normChip(drivers[0])
      if (d0 && usedFirstDriver.has(d0)) {
        const altDrivers = buildTileDrivers(row.tile, row.intel)
        const extra = altDrivers.filter((x) => normChip(x) !== d0)
        for (const e of extra) {
          const ek = normChip(e)
          if (ek && ek !== d0 && !drivers.some((d) => normChip(d) === ek)) {
            drivers = [e, ...drivers.slice(1, 3)].slice(0, 3)
            break
          }
        }
      }
      if (normChip(drivers[0])) usedFirstDriver.add(normChip(drivers[0]))
    }

    return { ...row, thesis, drivers }
  })
}

/**
 * @param {Array} ranked - tracker tiles
 * @param {Object} intelligenceBySymbol
 * @param {function} formatPrimary - (intel, tile) => primary action label
 */
export function buildVisibleTilePresentation(ranked, intelligenceBySymbol, formatPrimary) {
  const rows = ranked.map((tile) => {
    const symbol = String(tile.symbol || '').toUpperCase()
    const intel = intelligenceBySymbol[symbol]
    const primaryAction = intel ? formatPrimary(intel, tile) : '—'
    const pres = intel ? resolveDecisionPresentation(intel, tile) : null
    const fallbackStrip = str(pres?.fallback_strip_text, '').trim()
    let thesis = buildTileThesisLine(symbol, tile, intel, primaryAction, 0, fallbackStrip)
    const drivers = buildTileDrivers(tile, intel)
    const thesisTitle = str(intel?.thesis_plain, '')
    return { symbol, tile, intel, primaryAction, thesis, drivers, thesisTitle, fallbackStrip }
  })

  const deduped = dedupeAcrossTiles(rows)
  const chipSuppress = computeChipSuppressGrid(ranked, intelligenceBySymbol)

  const bySymbol = new Map()
  for (const r of deduped) {
    bySymbol.set(r.symbol, {
      thesis: r.thesis,
      thesisTitle: r.thesisTitle,
      drivers: r.drivers,
      chipSuppress,
    })
  }
  return bySymbol
}
