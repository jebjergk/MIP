/** Canonical bar timestamp key for Learning View selection (UTC, second precision). */

export function normalizeBarTs(ts) {
  if (!ts) return null
  return String(ts).replace(' ', 'T').slice(0, 19)
}

export function barTsFromSearchParams(searchParams) {
  const raw = searchParams?.get?.('bar_ts') || searchParams?.get?.('bar')
  return raw ? normalizeBarTs(raw) : null
}

export function syncBarTsToUrl(barTs, { replace = true } = {}) {
  if (typeof window === 'undefined') return
  const url = new URL(window.location.href)
  const key = normalizeBarTs(barTs)
  if (key) url.searchParams.set('bar_ts', key)
  else url.searchParams.delete('bar_ts')
  if (replace) window.history.replaceState({}, '', url)
  else window.history.pushState({}, '', url)
}

export function scrollRowIntoView(rowEl, containerEl) {
  if (!rowEl) return
  if (containerEl) {
    const top = rowEl.offsetTop - containerEl.clientHeight / 2 + rowEl.clientHeight / 2
    containerEl.scrollTo({ top: Math.max(0, top), behavior: 'smooth' })
    return
  }
  rowEl.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
}

export function scrollChartBarIntoView(barEl, scrollContainerEl) {
  if (!barEl || !scrollContainerEl) return
  const containerRect = scrollContainerEl.getBoundingClientRect()
  const barRect = barEl.getBoundingClientRect()
  const delta = barRect.left + barRect.width / 2 - (containerRect.left + containerRect.width / 2)
  scrollContainerEl.scrollLeft += delta
}

export function findNarrativeForBar(payload, barTs) {
  const key = normalizeBarTs(barTs)
  if (!key || !payload) return null
  const byTs = payload.bar_narratives_by_ts || {}
  if (byTs[key]) return byTs[key]
  const row = (payload.educational_grid || []).find(
    (r) => normalizeBarTs(r.bar_ts) === key,
  )
  return row?.narrative || null
}

/** Reject simulation facts that belong to another symbol. */
export function narrativeSymbolMatches(payload, narrative) {
  const sym = payload?.symbol?.toUpperCase()
  const techSym = narrative?.technical?.symbol?.toUpperCase()
  if (!sym || !techSym) return true
  return sym === techSym
}
