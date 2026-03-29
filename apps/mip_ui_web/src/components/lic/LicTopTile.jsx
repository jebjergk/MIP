/**
 * Top-grid position tile — strict scan contract only:
 * header → recommendation → confidence → microchart → strip → thesis → drivers → chips.
 * No extra narrative blocks; header is scan facts only (no external links).
 */
import LicTileMiniChart from './LicTileMiniChart'

function truncateTileText(s, maxLen) {
  const t = String(s || '').trim()
  if (!t) return { short: '', full: '' }
  if (t.length <= maxLen) return { short: t, full: t }
  return { short: `${t.slice(0, Math.max(0, maxLen - 1))}…`, full: t }
}

function safeText(v, fallback = '\u2014') {
  if (v == null || v === '') return fallback
  if (typeof v === 'string' || typeof v === 'number') return String(v)
  if (typeof v === 'boolean') return v ? 'Yes' : 'No'
  return String(v)
}

export default function LicTopTile({
  symbolKey,
  selected,
  onSelect,
  symbolLabel,
  side,
  priceStr,
  pnlStr,
  pnlNeg,
  ageStr,
  primaryAction,
  showConfidence,
  confPct,
  confTitle,
  recommendationBand,
  tile,
  fallbackStrip,
  thesisLine,
  thesisTooltip,
  driverLines,
  chipSuppress,
  intel,
}) {
  const au = intel?.analog_ui || {}
  const suppress = chipSuppress || {}

  return (
    <button
      type="button"
      className={`lic-tile ${selected ? 'lic-tile--selected' : ''}`}
      onClick={onSelect}
    >
      <div className="lic-tile-head">
        <div className="lic-tile-head-main">
          <span className="lic-tile-sym">{symbolLabel}</span>
          <span className="lic-tile-side">{safeText(side, '')}</span>
        </div>
        <div className="lic-tile-head-metrics">
          <span>{priceStr}</span>
          <span className={pnlNeg ? 'lic-pnl-neg' : 'lic-pnl-pos'}>{pnlStr}</span>
          <span className="lic-tile-age">{ageStr}</span>
        </div>
      </div>
      <div className="lic-tile-rec">{primaryAction}</div>
      {showConfidence ? (
        <div className="lic-tile-conf" title={confTitle}>
          <span className="lic-tile-conf-pct">Confidence {confPct}%</span>
        </div>
      ) : null}
      <LicTileMiniChart tile={tile} recommendationBand={recommendationBand} />
      {fallbackStrip ? (
        <div
          className="lic-tile-fallback-strip"
          title="Safer alternative vs official stance — primary action above is decisive."
        >
          {fallbackStrip}
        </div>
      ) : null}
      <div className="lic-tile-thesis" title={thesisTooltip}>
        {thesisLine}
      </div>
      <ul className="lic-tile-drivers">
        {(driverLines || []).map((d, di) => {
          const txt = safeText(d)
          const t = truncateTileText(txt, 72)
          return (
            <li
              key={`${symbolKey}-d-${di}`}
              className="lic-tile-driver-li"
              title={t.full !== t.short ? t.full : undefined}
            >
              {t.short || txt}
            </li>
          )
        })}
      </ul>
      <div className="lic-tile-chips">
        {!suppress.attention ? (
          <span className="lic-chip" title="Attention">
            <span className="lic-chip-k">Attention</span>
            <span className="lic-chip-v">{safeText(intel?.attention_band)}</span>
          </span>
        ) : null}
        {!suppress.analog ? (
          <span className="lic-chip" title="Historical analog">
            <span className="lic-chip-k">Analog</span>
            <span className="lic-chip-v">{safeText(au.chip_verdict || au.confidence_plain, '—')}</span>
          </span>
        ) : null}
        {!suppress.factor ? (
          <span className="lic-chip" title="Portfolio factor">
            <span className="lic-chip-k">Factor</span>
            <span className="lic-chip-v">{safeText(intel?.portfolio_factor_chip)}</span>
          </span>
        ) : null}
        {!suppress.regret ? (
          <span className="lic-chip" title="Regret tilt">
            <span className="lic-chip-k">Regret</span>
            <span className="lic-chip-v">{safeText(intel?.regret_tilt_label)}</span>
          </span>
        ) : null}
      </div>
    </button>
  )
}
