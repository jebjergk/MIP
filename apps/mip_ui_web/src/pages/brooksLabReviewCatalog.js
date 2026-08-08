/** G2.4 — parse and validate review-catalog API responses. */

import { CANONICAL_G1 } from './brooksTradeLearningG1'

export function isAdviserFoundationCatalog(catalog) {
  return catalog?.catalog_mode === 'adviser_foundation' && !catalog?.diagnostic_legacy
}

export function normalizeReviewCatalogPayload(data) {
  if (data == null || typeof data !== 'object') return null
  const runs = Array.isArray(data.runs) ? data.runs : null
  if (!runs) return null
  return {
    runs,
    defaults: data.defaults && typeof data.defaults === 'object' ? data.defaults : {},
    canonical_trade_id: data.canonical_trade_id ?? null,
    note: data.note ?? null,
    catalog_mode: data.catalog_mode ?? null,
    diagnostic_legacy: Boolean(data.diagnostic_legacy),
    adviser_empty_state: data.adviser_empty_state ?? null,
    v1_validation_sessions: Array.isArray(data.v1_validation_sessions) ? data.v1_validation_sessions : [],
    amzn_v1_status: data.amzn_v1_status ?? null,
  }
}

export function catalogHasRuns(catalog) {
  if (isAdviserFoundationCatalog(catalog)) {
    return true
  }
  return Boolean(catalog?.runs?.length)
}

export function catalogIncludesRun(catalog, runId) {
  return (catalog?.runs || []).some((r) => r.run_id === runId)
}

export function catalogErrorFromHttp(status, body) {
  if (status === 404) return 'Review catalog endpoint not found (404). Restart the UI API to load the new route.'
  if (status >= 500) return `Review catalog server error (${status}).`
  const detail = body?.detail
  if (typeof detail === 'string' && detail.trim()) return detail
  if (Array.isArray(detail) && detail.length) return String(detail[0]?.msg || detail[0])
  return `Could not load review catalog (HTTP ${status}).`
}

export function emptyCatalogMessage(catalog) {
  if (!catalog) return 'Review catalog response was missing or malformed.'
  if (isAdviserFoundationCatalog(catalog)) {
    return null
  }
  if (!catalog?.runs?.length) {
    return 'Review catalog returned no runs. Prepare a Brooks Intraday run or enable diagnostic legacy review.'
  }
  return null
}

export function selectionFromCatalogOrNull(catalog, parseFn, params) {
  if (!catalogHasRuns(catalog)) {
    return { selection: null, warnings: [emptyCatalogMessage(catalog)], fallbackApplied: true }
  }
  return parseFn(params, catalog)
}

export const CANONICAL_RUN_ID = CANONICAL_G1.runId

export function findCanonicalRunEntry(catalog) {
  return (catalog?.runs || []).find((r) => r.run_id === CANONICAL_RUN_ID) || null
}

export function findCanonicalPmChain(runEntry) {
  return (runEntry?.available_chains || []).find((c) => c.canonical_pm_certification) || null
}

export function findAmznTradeInChain(chain) {
  return (chain?.available_trades || []).find((t) => t.symbol === 'AMZN') || null
}
