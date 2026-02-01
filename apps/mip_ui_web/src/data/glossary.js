/**
 * Glossary data for InfoTooltip. Source of truth: MIP/docs/ux/UX_METRIC_GLOSSARY.yml
 * Generated JSON: MIP/docs/ux/UX_METRIC_GLOSSARY.json and this copy in src/data.
 * Key convention: use key="audit.has_new_bars" or scope="audit" key="has_new_bars" (backward compatible).
 */
import glossaryData from './UX_METRIC_GLOSSARY.json'

/**
 * Get glossary entry by scope and key (backward compatible).
 * @param {string} scope - e.g. 'audit', 'portfolio', 'risk_gate', 'signals', 'proposals', 'positions', 'trades', 'ui'
 * @param {string} key - e.g. 'run_status', 'total_return'
 * @returns {{ short: string, long: string } | null}
 */
export function getGlossaryEntry(scope, key) {
  if (!scope || !key) return null
  const scopeData = glossaryData[scope]
  if (!scopeData) return null
  const entry = scopeData[key]
  if (!entry || typeof entry.short !== 'string') return null
  return {
    short: entry.short,
    long: entry.long ?? entry.short,
  }
}

/**
 * Get glossary entry by dot-key (e.g. "audit.has_new_bars", "portfolio.max_drawdown").
 * @param {string} dotKey - e.g. 'audit.has_new_bars', 'portfolio.total_return', 'ui.status_badge'
 * @returns {{ short: string, long: string } | null}
 */
export function getGlossaryEntryByDotKey(dotKey) {
  if (!dotKey || typeof dotKey !== 'string') return null
  const idx = dotKey.indexOf('.')
  if (idx <= 0 || idx === dotKey.length - 1) return null
  const scope = dotKey.slice(0, idx)
  const key = dotKey.slice(idx + 1)
  return getGlossaryEntry(scope, key)
}

export default glossaryData
