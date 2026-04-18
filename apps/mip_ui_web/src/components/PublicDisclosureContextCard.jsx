/**
 * Phase 2 — compact factual card for politician trade disclosures (symbol-mapped).
 * Not a trading signal; title stays neutral ("Public disclosure context").
 */

function fmtShortDate(s) {
  if (s == null) return '—'
  const t = String(s).slice(0, 10)
  return t || '—'
}

function badgeClass(variant, kind) {
  const base = variant === 'lpa' ? 'lpa-c2-pdc-badge' : 'sch-pdc-badge'
  return `${base} ${base}--${String(kind || 'na').toLowerCase()}`
}

export default function PublicDisclosureContextCard({ exhibit, variant = 'hearing' }) {
  if (exhibit == null || typeof exhibit !== 'object') return null

  const root = variant === 'lpa' ? 'lpa-c2-pdc' : 'sch-pdc'
  const lines = Array.isArray(exhibit.summary_lines) ? exhibit.summary_lines : []
  const rows = Array.isArray(exhibit.recent_transactions) ? exhibit.recent_transactions : []
  const mq = exhibit.mapping_quality || '—'
  const tone = exhibit.tone_vs_trade || '—'
  const asOf = exhibit.as_of_utc != null ? String(exhibit.as_of_utc).slice(0, 19).replace('T', ' ') + ' UTC' : '—'

  return (
    <div className={root}>
      <div className={variant === 'lpa' ? 'lpa-c2-card-head' : 'sch-pdc-head'}>
        <span className={variant === 'lpa' ? 'lpa-c2-card-icon' : 'sch-pdc-icon'} aria-hidden>
          ◈
        </span>
        Public disclosure context
      </div>

      <div className={`${root}-meta`}>
        <span className={badgeClass(variant, mq)} title="Mapping quality">
          {String(mq).replace(/_/g, ' ')}
        </span>
        <span className={badgeClass(variant, tone)} title="Disposition vs proposal direction (rule-based)">
          {String(tone).replace(/_/g, ' ')}
        </span>
        <span className={`${root}-asof`} title="As of ingest timestamp">
          As of {asOf}
        </span>
      </div>

      {lines.length > 0 ? (
        <ul className={`${root}-lines`}>
          {lines.slice(0, 3).map((line, i) => (
            <li key={i}>{line}</li>
          ))}
        </ul>
      ) : null}

      {rows.length > 0 ? (
        <ul className={`${root}-tx`}>
          {rows.map((r, i) => {
            const url = r.source_url != null ? String(r.source_url).trim() : ''
            const name = r.filer_display_name != null ? String(r.filer_display_name) : '—'
            const side = r.side != null ? String(r.side) : '—'
            const fd = fmtShortDate(r.filed_date || r.transaction_date)
            const inner = (
              <>
                <span className={`${root}-tx-date`}>{fd}</span>
                <span className={`${root}-tx-side`}>{side}</span>
                <span className={`${root}-tx-name`}>{name}</span>
              </>
            )
            return (
              <li key={i}>
                {url ? (
                  <a href={url} target="_blank" rel="noopener noreferrer" className={`${root}-tx-link`}>
                    {inner}
                  </a>
                ) : (
                  <span className={`${root}-tx-row`}>{inner}</span>
                )}
              </li>
            )
          })}
        </ul>
      ) : (
        <p className={`${root}-empty`}>No recent mapped rows to list.</p>
      )}

      <p className={`${root}-disclaimer`}>
        {exhibit.disclaimer || 'Politician trade disclosures only; not a trade signal.'}
      </p>
    </div>
  )
}
