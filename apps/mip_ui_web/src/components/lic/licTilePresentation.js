/**
 * Presentation-only tile copy + anti-repetition for Live Intelligence Cockpit.
 * Derives shorter thesis, filtered drivers, and chip visibility from existing intel + tile fields.
 */

function str(v, fb = '') {
  if (v == null || v === '') return fb
  if (typeof v === 'string' || typeof v === 'number') return String(v)
  return fb
}

function asArray(x) {
  return Array.isArray(x) ? x : []
}

function normChip(s) {
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

function contextualDriverLines(tile, intel) {
  const pm = tile?.progress_metrics || {}
  const dsl = Number(pm.distance_to_sl_pct)
  const dtp = Number(pm.distance_to_tp_pct)
  const pnl = Number(tile?.unrealized_pnl)
  const lines = []
  if (Number.isFinite(dsl) && dsl >= 0 && dsl < 0.035) {
    lines.push(`Stop buffer tight (${(dsl * 100).toFixed(1)}% air) — adverse ticks matter.`)
  } else if (Number.isFinite(dsl) && dsl >= 0.035 && dsl < 0.08) {
    lines.push(`Meaningful room to stop still, but cushion is finite.`)
  }
  if (Number.isFinite(dtp) && Math.abs(dtp) < 0.045) {
    lines.push(`Target zone nearby — upside may be partly priced.`)
  } else if (Number.isFinite(dtp) && dtp > 0.06) {
    lines.push(`Target still has runway if path holds.`)
  }
  if (Number.isFinite(pnl)) {
    if (pnl < 0) {
      lines.push(`Position underwater — risk discipline matters more than patience.`)
    } else if (pnl > 0) {
      lines.push(`P&L green — room to let thesis play if stops hold.`)
    }
  }
  const tier = str(intel?.analog_tier_key, '')
  if (tier === 'strong') {
    lines.push(`Strong analog match — history supports sizing conviction carefully.`)
  } else if (tier === 'weak') {
    lines.push(`Weak analog match — lean on live tape and risk limits over history averages.`)
  }
  const med = medianPrice(tile)
  const cur = Number(tile?.current_price)
  if (Number.isFinite(cur) && Number.isFinite(med)) {
    if (cur < med * 0.998) {
      lines.push(`Trading under expectation median — path needs to prove itself.`)
    } else if (cur > med * 1.002) {
      lines.push(`Ahead of expectation median — reward partly realized.`)
    }
  }
  return lines
}

/**
 * Short, symbol-distinct thesis line (one sentence).
 * @param {number} [variantOffset=0] — shifts template pick for anti-repetition passes.
 */
export function buildTileThesisLine(symbol, tile, intel, primaryAction, variantOffset = 0) {
  if (!intel || typeof intel !== 'object') {
    return `${primaryAction}: intelligence still loading for this symbol.`
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
      `${primaryAction}: path still credible; edge is avoiding a slow fade vs median.`,
      `${primaryAction} while structure holds — follow-through quality is the swing factor.`,
      `Still tracking the plan; conviction rests on price staying clear of stop pressure.`,
      `${primaryAction} — reward-to-risk narrows if upside stalls here.`,
    ],
    intact_watch: [
      `${primaryAction}: tape noisy but thesis not broken — watch stop distance.`,
      `Elevated watch — path intact but room for error is shrinking.`,
      `${primaryAction} — one adverse sequence could flip the posture quickly.`,
    ],
    stretched: [
      `Thesis stretched — ${primaryAction.toLowerCase()} only if risk lines stay honest.`,
      `Setup alive but fragile; ${primaryAction.toLowerCase()} needs cleaner price action.`,
      `Drift from core story — ${primaryAction.toLowerCase()} is conditional on stabilization.`,
    ],
    damaged: [
      `Thesis damaged — ${primaryAction.toLowerCase()} is a risk-managed hold, not a bet.`,
      `Conflicting signals vs plan — size and stops matter more than narrative.`,
    ],
    broken: [
      `Thesis broken for practice — prioritize capital over narrative.`,
      `Invalidation risk dominates — treat recovery as a new trade, not the old one.`,
    ],
  }

  let pool = pools.intact_hold
  if (th === 'THESIS_STRETCHED') pool = pools.stretched
  else if (th === 'THESIS_DAMAGED') pool = pools.damaged
  else if (th === 'THESIS_BROKEN') pool = pools.broken
  else if (band === 'WATCH_CLOSELY' || band === 'PREPARE_EXIT') pool = pools.intact_watch

  const pick = (salt + Number(variantOffset) * 3) % pool.length
  let line = pool[pick]

  if (nearSl) {
    line = `${primaryAction}: stop danger elevated — posture depends on holding buffer.`
  } else if (nearTp && green) {
    line = `${primaryAction} with target in play — decide how much upside to bank vs give back.`
  } else if (underwater && nearSl) {
    line = `Underwater near risk — ${primaryAction} is fragile; invalidation is one bad tick away.`
  } else if (underwater) {
    line = `Underwater but not at rails — ${primaryAction.toLowerCase()} hinges on path repair vs stop.`
  }

  return line
}

/**
 * Up to 3 drivers: drop generic API lines, add context from tile when thin.
 */
export function buildTileDrivers(tile, intel) {
  if (!intel || typeof intel !== 'object') return []
  const raw = asArray(intel?.decision_drivers).map((d) => str(d).trim()).filter(Boolean)
  const kept = raw.filter((t) => !isGenericDriverText(t))
  const ctx = contextualDriverLines(tile, intel)
  const out = []
  const seen = new Set()
  for (const t of kept) {
    const k = normChip(t)
    if (seen.has(k)) continue
    seen.add(k)
    out.push(t)
    if (out.length >= 3) break
  }
  for (const c of ctx) {
    if (out.length >= 3) break
    const k = normChip(c)
    if (seen.has(k)) continue
    seen.add(k)
    out.push(c)
  }
  return out.slice(0, 3)
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
        const alt = buildTileThesisLine(row.symbol, row.tile, row.intel, row.primaryAction, k)
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
        const extra = contextualDriverLines(row.tile, row.intel).filter((x) => normChip(x) !== d0)
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
 * @param {function} formatPrimary - (intel) => primary action label
 */
export function buildVisibleTilePresentation(ranked, intelligenceBySymbol, formatPrimary) {
  const rows = ranked.map((tile) => {
    const symbol = String(tile.symbol || '').toUpperCase()
    const intel = intelligenceBySymbol[symbol]
    const primaryAction = intel ? formatPrimary(intel) : '—'
    const thesis = buildTileThesisLine(symbol, tile, intel, primaryAction)
    const drivers = buildTileDrivers(tile, intel)
    const thesisTitle = str(intel?.thesis_plain, '')
    return { symbol, tile, intel, primaryAction, thesis, drivers, thesisTitle }
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
