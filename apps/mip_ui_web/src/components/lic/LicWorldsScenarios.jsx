import {
  buildThreeWorldScenariosFromAnalog,
  computeReturnDispersion,
  formatScenarioReturnPct,
  scenarioCardInterpretation,
  worldsCardSampleWarning,
  worldsRecommendationMismatch,
  worldsSparseCaution,
} from './licWorldsFromAnalog'

/** Map actual average returns to a visible slope; preserves ordering; no invented returns. */
function ScenarioPath({ avgReturn, rMin, rMax, scenarioKey }) {
  const x0 = 6
  const x1 = 94
  const yMid = 20
  const amp = 17
  const spread = rMax - rMin
  let t
  if (!Number.isFinite(spread) || spread < 1e-12) {
    t = scenarioKey === 'upside' ? 0.95 : scenarioKey === 'downside' ? -0.95 : 0.1
  } else {
    const u = (avgReturn - rMin) / spread
    t = u * 2 - 1
  }
  if (scenarioKey === 'base') {
    t *= 0.4
  } else {
    const sign = t >= 0 ? 1 : -1
    const mag = Math.max(0.42, Math.abs(t))
    t = sign * Math.min(1, mag)
  }
  const y1 = yMid - t * amp
  const d = `M ${x0} ${yMid} Q ${(x0 + x1) / 2} ${(yMid + y1) / 2} ${x1} ${y1}`
  const strokeW = scenarioKey === 'base' ? 1.25 : 1.55
  return (
    <svg
      className={`lic-worlds-path lic-worlds-path--${scenarioKey}`}
      viewBox="0 0 100 40"
      preserveAspectRatio="none"
      aria-hidden
    >
      <line
        x1={x0}
        y1={yMid}
        x2={x1}
        y2={yMid}
        stroke="currentColor"
        strokeWidth={0.45}
        strokeDasharray="3 2"
        opacity={0.35}
      />
      <path d={d} fill="none" stroke="currentColor" strokeWidth={strokeW} strokeLinecap="round" />
      <circle cx={x0} cy={yMid} r={2} fill="currentColor" opacity={0.85} />
      <circle cx={x1} cy={y1} r={2.35} fill="currentColor" />
    </svg>
  )
}

export default function LicWorldsScenarios({ analogSummary, finalRecommendation }) {
  const worlds = buildThreeWorldScenariosFromAnalog(analogSummary || {})
  const mismatch = worldsRecommendationMismatch(worlds, finalRecommendation)
  const caution = worldsSparseCaution(worlds)

  if (!worlds.ok) {
    return (
      <div className="lic-worlds-scenarios lic-worlds-scenarios--empty">
        <p className="lic-worlds-empty-msg">Not enough comparable historical outcomes to form reliable worlds</p>
        <p className="lic-worlds-empty-sub">
          Fewer than three matched episodes include usable forward returns. See the Analog tab for match quality.
        </p>
      </div>
    )
  }

  const { scenarios, maxAbsReturn, totalN } = worlds
  const rMin = Math.min(...scenarios.map((s) => s.avgReturn))
  const rMax = Math.max(...scenarios.map((s) => s.avgReturn))
  const dispersion = computeReturnDispersion(scenarios)
  const interpCtx = { dispersion, maxAbsReturn }

  return (
    <div className="lic-worlds-scenarios">
      <p className="lic-worlds-lead">
        Grouped historical outcomes from setups similar to this position (same{' '}
        <code className="lic-worlds-lead-code">realized_return</code> as analog matching). Thirds are rank buckets
        over matched episodes — not a prediction or forecast.
      </p>
      {caution ? (
        <div className="lic-worlds-banner lic-worlds-banner--caution" role="status">
          Weak historical match quality — treat frequencies and averages as exploratory, not stable facts.
        </div>
      ) : null}
      {mismatch ? (
        <div className="lic-worlds-banner lic-worlds-banner--warn" role="status">
          Historical outcomes do not align with current recommendation.
        </div>
      ) : null}
      <div className="lic-worlds-cards">
        {scenarios.map((s) => {
          const sampleWarn = worldsCardSampleWarning(s, totalN)
          const retCls =
            s.avgReturn > 1e-12
              ? 'lic-worlds-card-return-val--pos'
              : s.avgReturn < -1e-12
                ? 'lic-worlds-card-return-val--neg'
                : ''
          return (
            <div key={s.key} className={`lic-worlds-card lic-worlds-card--${s.key}`}>
              <div className="lic-worlds-card-title">{s.label}</div>
              <div className="lic-worlds-card-prob">{(s.probability * 100).toFixed(0)}%</div>
              <div className="lic-worlds-card-sub">of matched sample</div>
              <div className="lic-worlds-card-return">
                Avg outcome{' '}
                <span className={`lic-worlds-card-return-val ${retCls}`}>
                  {formatScenarioReturnPct(s.avgReturn)}
                </span>
              </div>
              <ScenarioPath avgReturn={s.avgReturn} rMin={rMin} rMax={rMax} scenarioKey={s.key} />
              <p className="lic-worlds-card-interpret">{scenarioCardInterpretation(s, interpCtx)}</p>
              {sampleWarn ? (
                <div
                  className={`lic-worlds-card-samplewarn lic-worlds-card-samplewarn--${sampleWarn.level}`}
                  role="status"
                >
                  {sampleWarn.text}
                </div>
              ) : null}
              <div className="lic-worlds-card-n">n = {s.sampleSize} episodes in this bucket</div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
