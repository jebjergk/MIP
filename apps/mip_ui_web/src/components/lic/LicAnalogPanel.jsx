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
  buildClosestAnalogLine,
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
  const closestLine = buildClosestAnalogLine(analogSummary)

  const fullWeakNote = safe(
    u.low_similarity_note ||
      'Current path has low similarity to trained historical episodes — lean on tape, thesis, and risk limits.',
  )
  let compactWarn = ''
  if (weak && exploratory) {
    compactWarn = 'Weak match and small sample — treat as exploratory only'
  } else if (weak) {
    compactWarn =
      fullWeakNote.length > 96 ? `${fullWeakNote.slice(0, 93)}…` : fullWeakNote || 'Weak historical match — exploratory only'
  } else if (exploratory) {
    compactWarn = 'Small sample — treat as exploratory'
  }

  return (
    <div className="lic-analog-panel lic-analog-panel--v2">
      <section className="lic-analog-hero">
        <h5 className="lic-analog-headline">{safe(headline)}</h5>
        <p className="lic-analog-implication">{safe(implication)}</p>
      </section>

      {compactWarn ? (
        <p className="lic-analog-warn-compact" role="status" title={weak && fullWeakNote ? fullWeakNote : undefined}>
          {compactWarn}
        </p>
      ) : null}

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

      {closestLine ? <p className="lic-analog-closest">{safe(closestLine)}</p> : null}

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
