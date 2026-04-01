/**
 * Presentation-only helpers for the Simulator tab (no API / simulation changes).
 */
import {
  buildThreeWorldScenariosFromAnalog,
  worldsConfidenceTier,
} from './licWorldsFromAnalog'

export function formatSimPct(v) {
  const x = Number(v)
  if (!Number.isFinite(x)) return '\u2014'
  return `${(x * 100).toFixed(1)}%`
}

export function findSimRow(rows, actionKey) {
  return (rows || []).find((r) => r?.action === actionKey) || null
}

/** Short tier label + sample size for strip and primary block */
export function worldsConfidenceShort(worldsResult) {
  if (!worldsResult?.ok || worldsResult.totalN == null) {
    return { line: 'Confidence: \u2014 (Worlds need ≥3 matches)', tierKey: null }
  }
  const t = worldsConfidenceTier(worldsResult.totalN)
  const name =
    t.key === 'very_low'
      ? 'VERY LOW'
      : t.key === 'low'
        ? 'LOW'
        : t.key === 'moderate'
          ? 'MODERATE'
          : 'STRONG'
  return { line: `Confidence: ${name} (n=${worldsResult.totalN})`, tierKey: t.key }
}

/**
 * Compact Worlds-linked strip for the simulator context.
 */
export function buildSimulatorWorldsStrip(analogSummary) {
  const wr = buildThreeWorldScenariosFromAnalog(analogSummary)
  const conf = worldsConfidenceShort(wr)

  if (!wr.ok || !wr.scenarios) {
    return {
      ok: false,
      tag: 'Mixed setup',
      upsidePct: null,
      downsidePct: null,
      confidenceLine: conf.line,
      tierKey: null,
      interpret: 'Worlds bucket rates need more analog matches; treat simulator as a structured what-if, not a forecast.',
    }
  }

  const byKey = Object.fromEntries(wr.scenarios.map((s) => [s.key, s]))
  const pu = Math.round((byKey.upside?.probability ?? 0) * 100)
  const pd = Math.round((byKey.downside?.probability ?? 0) * 100)

  const best = wr.scenarios.reduce((a, s) => (s.probability > a.probability ? s : a), wr.scenarios[0])
  let tag = 'Mixed setup'
  if (best.sampleSize > 0 && best.probability >= 0.5) {
    if (best.key === 'upside') tag = 'Upside dominant'
    else if (best.key === 'downside') tag = 'Downside risk elevated'
    else tag = 'Base-heavy setup'
  }

  let interpret = 'Outcomes are uncertain; risk management is important.'
  if (tag === 'Upside dominant') {
    interpret = 'Historical skew favors continuation; size actions to match confidence.'
  } else if (tag === 'Downside risk elevated') {
    interpret = 'Matches show downside often; favor protection over full risk-on hold.'
  } else if (tag === 'Base-heavy setup') {
    interpret = 'Many analogs land near flat; transitions matter more than one-way bets.'
  }

  return {
    ok: true,
    tag,
    upsidePct: pu,
    downsidePct: pd,
    confidenceLine: conf.line,
    tierKey: conf.tierKey,
    interpret,
  }
}

export function partialActionGuidance(tierKey) {
  if (tierKey === 'very_low' || tierKey === 'low') {
    return 'Thin Worlds evidence: prefer partial trims over aggressive full exits unless the tape confirms.'
  }
  return null
}

const EXPOSURE_TARGET = {
  hold: 100,
  tighten_stop: 100,
  trim_25: 75,
  trim_50: 50,
  exit_now: 0,
}

export function positionChangeCopy(actionKey) {
  const k = String(actionKey || '').toLowerCase()
  const target = EXPOSURE_TARGET[k] ?? 100
  const lines = {
    hold: 'No size change — full exposure.',
    tighten_stop: 'Notional ~100% — size unchanged; stop discipline tightens.',
    trim_25: 'Reduce exposure by one quarter.',
    trim_50: 'Reduce exposure by half.',
    exit_now: 'Close the position (0% exposure).',
  }
  return {
    currentPct: 100,
    targetPct: target,
    detail: lines[k] || lines.hold,
  }
}

/** Decisive label e.g. TRIM 50% */
export function recommendedActionHardLabel(label) {
  return String(label || 'HOLD').toUpperCase()
}

function ppImprovement(holdVal, recVal, lowerIsBetter) {
  const h = Number(holdVal)
  const r = Number(recVal)
  if (!Number.isFinite(h) || !Number.isFinite(r)) return null
  const raw = (h - r) * 100
  if (lowerIsBetter) return raw
  return (r - h) * 100
}

/**
 * Trade-off lines vs HOLD for the recommended row.
 */
export function tradeoffVsHoldLines(holdRow, recRow) {
  if (!holdRow || !recRow || recRow.action === 'hold') return []

  const lines = []
  const dImp = ppImprovement(holdRow.expected_downside, recRow.expected_downside, true)
  if (dImp != null && Math.abs(dImp) >= 0.05) {
    const arrow = dImp > 0 ? '\u2193' : '\u2191'
    lines.push({
      key: 'down',
      arrow,
      text: `${arrow} Downside risk: ${formatSimPct(holdRow.expected_downside)} \u2192 ${formatSimPct(recRow.expected_downside)}`,
      good: dImp > 0,
    })
  }

  const gImp = ppImprovement(holdRow.giveback_risk, recRow.giveback_risk, true)
  if (gImp != null && Math.abs(gImp) >= 0.05) {
    const arrow = gImp > 0 ? '\u2193' : '\u2191'
    lines.push({
      key: 'give',
      arrow,
      text: `${arrow} Giveback: ${formatSimPct(holdRow.giveback_risk)} \u2192 ${formatSimPct(recRow.giveback_risk)}`,
      good: gImp > 0,
    })
  }

  const uImp = ppImprovement(holdRow.expected_upside, recRow.expected_upside, false)
  const recUp = Number(recRow.expected_upside)
  if (uImp != null) {
    if (uImp < -0.05) {
      lines.push({
        key: 'up',
        arrow: '\u2193',
        text: `\u2193 Upside vs HOLD: ${formatSimPct(holdRow.expected_upside)} \u2192 ${formatSimPct(recRow.expected_upside)}`,
        good: false,
      })
    } else if (uImp > 0.05) {
      lines.push({
        key: 'up',
        arrow: '\u2191',
        text: `\u2191 Upside vs HOLD: ${formatSimPct(holdRow.expected_upside)} \u2192 ${formatSimPct(recRow.expected_upside)}`,
        good: true,
      })
    } else if (Number.isFinite(recUp) && recUp >= 0.12) {
      lines.push({
        key: 'up',
        arrow: '\u2191',
        text: `\u2191 Upside retained: still meaningful (${formatSimPct(recRow.expected_upside)})`,
        good: true,
      })
    } else if (Number.isFinite(recUp)) {
      lines.push({
        key: 'up',
        arrow: '\u2192',
        text: `\u2192 Upside vs HOLD: ${formatSimPct(recRow.expected_upside)} (trimmed but non-zero)`,
        good: recUp >= 0.06,
      })
    }
  }

  return lines
}

export function primaryRationaleBullets(holdRow, recRow, tierKey) {
  if (!holdRow || !recRow || recRow.action === 'hold') {
    return ['HOLD keeps full upside if the thesis holds; table shows how trims change the risk profile.']
  }
  const bullets = []
  const dImp = ppImprovement(holdRow.expected_downside, recRow.expected_downside, true)
  if (dImp != null && dImp > 1) {
    bullets.push('Downside risk reduced meaningfully vs full hold.')
  }
  const uImp = ppImprovement(holdRow.expected_upside, recRow.expected_upside, false)
  if (uImp != null && uImp >= -0.02) {
    bullets.push('Upside partially preserved relative to cutting deeper.')
  } else if (Number(recRow?.expected_upside) >= 0.1) {
    bullets.push('Meaningful upside still on the table after sizing down.')
  }
  const gImp = ppImprovement(holdRow.giveback_risk, recRow.giveback_risk, true)
  if (gImp != null && gImp > 1) {
    bullets.push('Giveback pressure eased vs HOLD.')
  }
  if (bullets.length === 0) {
    bullets.push('Simulator ranks this action highest on net score for the current tile context.')
  }
  if (tierKey === 'very_low' || tierKey === 'low') {
    bullets.push('Scale size cautiously — Worlds confidence is thin.')
  }
  return bullets.slice(0, 4)
}

export function netScoreSubtitle(recRow, holdRow) {
  if (!recRow) return { title: '', tooltip: '' }
  const tooltip =
    'Net score = expected upside \u2212 expected downside (same 0\u20131 scale as the table). Higher favors the action in this lens. Giveback is shown separately, not inside net score.'

  if (!holdRow || recRow.action === 'hold') {
    return {
      title: `Best risk-adjusted action (net score ${Number(recRow.net_score).toFixed(3)})`,
      tooltip,
    }
  }
  const dImp = ppImprovement(holdRow.expected_downside, recRow.expected_downside, true)
  if (dImp != null && dImp > 0.5) {
    return {
      title: `Best risk-adjusted action (+\u2248${dImp.toFixed(0)}pp downside improvement vs HOLD)`,
      tooltip,
    }
  }
  return {
    title: `Best risk-adjusted action (net score ${Number(recRow.net_score).toFixed(3)} vs HOLD ${Number(holdRow.net_score).toFixed(3)})`,
    tooltip,
  }
}

export function regretHumanNarrative(row, actionKey) {
  if (!row) return ''
  const k = String(actionKey || '').toLowerCase()
  const rp = Number(row.regret_probability)
  const rpOk = Number.isFinite(rp)

  if (k === 'hold') {
    return 'If the tape rolls over, staying full size maximizes drawdown versus trims; you keep full upside if the thesis is still right and liquidity is normal.'
  }

  let regretLead = 'If the tape moves against you after you act, you may second-guess the timing.'
  if (k === 'trim_25' || k === 'trim_50') {
    regretLead = 'If price continues higher, you may regret trimming,'
  } else if (k === 'tighten_stop') {
    regretLead = 'If price wicks your tighter stop before resuming the trend, you may regret the exit,'
  } else if (k === 'exit_now') {
    regretLead = 'If price rebounds sharply after you are flat, you may regret exiting,'
  }

  const why =
    k === 'exit_now'
      ? ' but you remove tail risk and ambiguity while the thesis or execution picture is stressed.'
      : ' but you materially cut exposure to modeled downside and clarify the risk picture.'

  const tier = !rpOk
    ? ''
    : rp >= 0.38
      ? ' Modeled regret probability is elevated for this action.'
      : rp >= 0.28
        ? ' Modeled regret probability is moderate.'
        : ' Modeled regret probability is relatively contained.'

  return `${regretLead}${why}${tier}`
}

export function sortRowsByNetBenefitVsHold(rows, holdRow) {
  const list = Array.isArray(rows) ? [...rows] : []
  const holdNet = Number(holdRow?.net_score)
  if (!Number.isFinite(holdNet)) return list
  return list.sort((a, b) => {
    const da = Number(a.net_score) - holdNet
    const db = Number(b.net_score) - holdNet
    return db - da
  })
}

export function simNumTone(expectedUpside, expectedDownside) {
  const up = Number(expectedUpside)
  const down = Number(expectedDownside)
  return {
    upClass: Number.isFinite(up) && up >= 0.12 ? 'lic-sim-num--up-high' : '',
    downClass: Number.isFinite(down) && down >= 0.3 ? 'lic-sim-num--down-high' : '',
  }
}
