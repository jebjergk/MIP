/** Forensic legacy review — not shown in normal Adviser foundation Lab UX. */

export function isDiagnosticLegacyFromSearchParams(params) {
  if (!params) return false
  const v = params.get('diagnostic_legacy')
  return v === '1' || v === 'true' || v === 'yes'
}

export function reviewCatalogUrl(apiBase, { diagnosticLegacy } = {}) {
  const base = `${apiBase}/review-catalog`
  if (diagnosticLegacy) return `${base}?diagnostic_legacy=true`
  return base
}

export const ADVISER_EMPTY_STATE_DEFAULT =
  'Brooks Adviser has not yet been run for this session. Prepare a run and replay bars; Adviser thesis and watch conditions will appear here when available.'
