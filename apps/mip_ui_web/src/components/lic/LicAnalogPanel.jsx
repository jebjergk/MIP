import {
  analogBiasBarFractions,
  analogBiasDominanceLabel,
  analogIsWeak,
  analogParsed,
  analogShowExploratoryCaveat,
  buildAnalogBullets,
  buildAnalogHeadline,
  buildAnalogImplication,
  buildAnalogRates,
  buildAnalogTiming,
} from './licAnalogPanelModel'

function safe(v) {
  if (v == null) return ''
  return String(v).trim()
}

export default function LicAnalogPanel({ analogUi, analogSummary }) {
  const u = analogUi && typeof analogUi === 'object' ? analogUi : {}
  const parsed = analogParsed(u)
  const weak = analogIsWeak(parsed)
  const exploratory = analogShowExploratoryCaveat(parsed)
  const headline = buildAnalogHeadline(parsed)
  const implication = buildAnalogImplication(parsed)
  const rates = buildAnalogRates(parsed)
  const bullets = buildAnalogBullets(parsed, analogSummary)
  const timing = buildAnalogTiming(parsed, analogSummary)
  const bar = analogBiasBarFractions(parsed)
  const dominanceLabel = analogBiasDominanceLabel(parsed)

  return (
    <div className="lic-analog-panel lic-analog-panel--v2">
      <section className="lic-analog-hero">
        <h5 className="lic-analog-headline">{safe(headline)}</h5>
        <p className="lic-analog-implication">{safe(implication)}</p>
      </section>

      {(weak || exploratory) && (
        <div className="lic-analog-confidence-strip" role="status">
          {weak ? (
            <>
              <div className="lic-analog-confidence-strip-title">Weak historical match · Analog guidance weak</div>
              <p className="lic-analog-confidence-strip-body">
                {safe(
                  u.low_similarity_note ||
                    'Current path has low similarity to trained historical episodes — lean on tape, thesis, and risk limits.',
                )}
              </p>
            </>
          ) : null}
          {exploratory ? (
            <p className="lic-analog-exploratory">Low confidence — treat as exploratory</p>
          ) : null}
        </div>
      )}

      <section className="lic-drill-section lic-analog-rates-section">
        <h5 className="lic-drill-h">Outcome rates</h5>
        {rates.emptyRates ? (
          <p className="lic-analog-rate-line">{rates.emptyRates}</p>
        ) : (
          <>
            <p className="lic-analog-rate-line">{rates.winLine}</p>
            <p className="lic-analog-rate-line">{rates.lossLine}</p>
            {rates.smallSampleNote ? (
              <p className="lic-analog-rate-note">{rates.smallSampleNote}</p>
            ) : null}
          </>
        )}
        {bar.show ? (
          <div className="lic-analog-biasbar-wrap" aria-hidden>
            <div className="lic-analog-biasbar">
              <div className="lic-analog-biasbar-win" style={{ width: `${bar.winPct}%` }} />
              <div className="lic-analog-biasbar-loss" style={{ width: `${bar.lossPct}%` }} />
            </div>
            {dominanceLabel ? <div className="lic-analog-biasbar-label">{dominanceLabel}</div> : null}
          </div>
        ) : null}
      </section>

      <section className="lic-drill-section">
        <h5 className="lic-drill-h">Insights</h5>
        <ul className="lic-analog-bullets">
          {bullets.map((line, i) => (
            <li key={`ab-${i}`}>{safe(line)}</li>
          ))}
        </ul>
      </section>

      <section className="lic-drill-section">
        <h5 className="lic-drill-h">Timing</h5>
        <p className="lic-analog-timing">{safe(timing)}</p>
      </section>
    </div>
  )
}
