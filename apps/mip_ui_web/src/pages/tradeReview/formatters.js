/** Trade Review — display helpers (TIR row fields are snake_case from API). */

export function formatMoney(val) {
  if (val == null || val === '') return '\u2014'
  const n = Number(val)
  if (!Number.isFinite(n)) return '\u2014'
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(n)
}

export function formatReturnFraction(val) {
  if (val == null || val === '') return '\u2014'
  const n = Number(val)
  if (!Number.isFinite(n)) return '\u2014'
  return `${(n * 100).toFixed(2)}%`
}

export function formatDateTime(ts) {
  if (ts == null || ts === '') return '\u2014'
  try {
    const d = new Date(ts)
    if (Number.isNaN(d.getTime())) return String(ts)
    return d.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  } catch {
    return String(ts)
  }
}

const PW_DRIVER_LABELS = {
  FILTER: 'Signal / filter policy',
  SIZE: 'Position size',
  HORIZON: 'Timing / horizon',
  BASELINE: 'Baseline (e.g. cash)',
  EXIT: 'Early exit policy',
}

export function labelPwRegretDriver(driver) {
  if (driver == null || driver === '') return null
  const d = String(driver).trim()
  return PW_DRIVER_LABELS[d] || `Policy type: ${d}`
}

export function formatCommitteeNormalized(code) {
  const c = String(code || '').toUpperCase()
  if (c === 'REDUCE') return 'Reduced vs alpha baseline'
  if (c === 'BLOCK') return 'Blocked vs alpha baseline'
  if (c === 'UNKNOWN' || !c) return 'Committee stance unclear'
  return code
}

export function committeeSentence(normalized, raw) {
  const n = String(normalized || '').toUpperCase()
  if (n === 'REDUCE') return 'The committee approved a smaller size than the alpha baseline.'
  if (n === 'BLOCK') return 'The committee blocked entry that the alpha baseline would have taken.'
  if (n === 'UNKNOWN' || !n) return 'No clear committee recommendation is on record for this trade.'
  return `Committee recommendation: ${raw || normalized || 'unknown'}.`
}

export function alignmentBucket(alignmentClass, outcomeClass) {
  const a = String(alignmentClass || '').toUpperCase()
  const o = String(outcomeClass || '').toUpperCase()
  const s = `${a} ${o}`
  if (!s.trim()) return 'unknown'
  if (/\bALIGNED\b|\bMET\b|\bMATCH/.test(s)) return 'aligned'
  if (/\bFAVOR|\bOUTPERF|\bPOSITIVE|\bBEAT/.test(s)) return 'favorable'
  if (/\bUNFAVOR|\bUNDER|\bMISALIGN|\bNEGATIVE|\bWORSE/.test(s)) return 'unfavorable'
  return 'unknown'
}

export function alignmentPlainLanguage(bucket) {
  if (bucket === 'aligned') return 'Realized outcome aligned with the entry expectation.'
  if (bucket === 'favorable') return 'Realized outcome was favorable relative to expectation.'
  if (bucket === 'unfavorable') return 'Realized outcome was weaker than expectation.'
  return 'Outcome vs expectation is not classified for this trade.'
}

const CLEAN_RECON = new Set(['LINKED', 'RECONCILED'])

export function isReconAttention(reconciliationClass) {
  const c = String(reconciliationClass || '').trim().toUpperCase()
  if (!c) return false
  return !CLEAN_RECON.has(c)
}

export function reconPlainSummary(row) {
  const cls = row?.reconciliation_class
  if (!cls) return 'No reconciliation row for this symbol — not an error.'
  if (!isReconAttention(cls)) return 'Broker and MIP lifecycle look consistent for this symbol.'
  return `Reconciliation flag: ${cls.replace(/_/g, ' ').toLowerCase()}.`
}
