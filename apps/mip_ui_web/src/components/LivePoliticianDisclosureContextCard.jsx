/**
 * Phase 2 — optional silent live enrichment for politician trade disclosures.
 * Render only when API includes exhibit_live_politician_disclosure_context (success path).
 */

function fmtShortDate(s) {
  if (s == null) return '—'
  const t = String(s).slice(0, 10)
  return t || '—'
}

export default function LivePoliticianDisclosureContextCard({ exhibit, variant = 'hearing' }) {
  if (exhibit == null || typeof exhibit !== 'object') return null

  const root = variant === 'lpa' ? 'lpa-c2-pdc lpa-c2-pdc--live' : 'sch-pdc sch-pdc--live'
  const metaClass = variant === 'lpa' ? 'lpa-c2-pdc-meta' : 'sch-pdc-meta'
  const asofClass = variant === 'lpa' ? 'lpa-c2-pdc-asof' : 'sch-pdc-asof'
  const linesClass = variant === 'lpa' ? 'lpa-c2-pdc-lines' : 'sch-pdc-lines'
  const emptyClass = variant === 'lpa' ? 'lpa-c2-pdc-empty' : 'sch-pdc-empty'
  const linkClass = variant === 'lpa' ? 'lpa-c2-pdc-tx-link' : 'sch-pdc-tx-link'
  const discClass = variant === 'lpa' ? 'lpa-c2-pdc-disclaimer' : 'sch-pdc-disclaimer'

  const lines = Array.isArray(exhibit.summary_lines) ? exhibit.summary_lines : []
  const src = exhibit.source_label != null ? String(exhibit.source_label) : 'Live disclosure lookup'
  const fetched =
    exhibit.fetched_at_utc != null ? String(exhibit.fetched_at_utc).slice(0, 19).replace('T', ' ') + ' UTC' : '—'
  const link = exhibit.link_url != null ? String(exhibit.link_url).trim() : ''
  const txList = Array.isArray(exhibit.scraped_trades) ? exhibit.scraped_trades : []
  const txPrefix = variant === 'lpa' ? 'lpa-c2-pdc-tx' : 'sch-pdc-tx'

  return (
    <div className={root}>
      <div className={variant === 'lpa' ? 'lpa-c2-card-head' : 'sch-pdc-head'}>
        <span className={variant === 'lpa' ? 'lpa-c2-card-icon' : 'sch-pdc-icon'} aria-hidden>
          ↻
        </span>
        Live disclosure context
      </div>

      <div className={metaClass}>
        <span className={asofClass} title="Source label">
          {src}
        </span>
        <span className={asofClass} title="Fetched at">
          Fetched {fetched}
        </span>
      </div>

      {lines.length > 0 ? (
        <ul className={linesClass}>
          {lines.slice(0, 3).map((line, i) => (
            <li key={i}>{line}</li>
          ))}
        </ul>
      ) : null}

      {txList.length > 0 ? (
        <ul className={txPrefix}>
          {txList.map((r, i) => {
            const name = r.filer_display_name != null ? String(r.filer_display_name) : '—'
            const side = r.side != null ? String(r.side) : '—'
            const tick = r.issuer_ticker != null ? String(r.issuer_ticker) : ''
            const td = fmtShortDate(r.transaction_date)
            const who = tick ? `${tick} · ${name}` : name
            return (
              <li key={i}>
                <span className={`${txPrefix}-date`}>{td}</span>
                <span className={`${txPrefix}-side`}>{side}</span>
                <span className={`${txPrefix}-name`}>{who}</span>
              </li>
            )
          })}
        </ul>
      ) : null}

      {link ? (
        <p className={emptyClass}>
          <a href={link} target="_blank" rel="noopener noreferrer" className={linkClass}>
            Source link
          </a>
        </p>
      ) : null}

      <p className={discClass}>
        {exhibit.disclaimer || 'Politician trade disclosures only; not a trade signal.'}
      </p>
    </div>
  )
}
