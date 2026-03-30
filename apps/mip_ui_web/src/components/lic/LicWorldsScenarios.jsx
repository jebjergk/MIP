import {
  buildThreeWorldScenariosFromAnalog,
  worldsRecommendationMismatch,
  worldsSparseCaution,
} from './licWorldsFromAnalog'

function ScenarioPath({ avgReturn, maxAbs }) {
  const scale = maxAbs > 0 ? maxAbs : 0.01
  const t = Math.max(-1, Math.min(1, avgReturn / scale))
  const x0 = 6
  const x1 = 94
  const yMid = 22
  const amp = 16
  const y1 = yMid - t * amp
  const d = `M ${x0} ${yMid} Q ${(x0 + x1) / 2} ${(yMid + y1) / 2} ${x1} ${y1}`
  return (
    <svg className="lic-worlds-path" viewBox="0 0 100 36" preserveAspectRatio="none" aria-hidden>
      <line x1={x0} y1={yMid} x2={x1} y2={yMid} stroke="#334155" strokeWidth={0.6} strokeDasharray="3 2" />
      <path d={d} fill="none" stroke="currentColor" strokeWidth={1.35} strokeLinecap="round" />
      <circle cx={x0} cy={yMid} r={2} fill="currentColor" opacity={0.85} />
      <circle cx={x1} cy={y1} r={2.25} fill="currentColor" />
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
        <p className="lic-worlds-empty-msg">Not enough comparable historical outcomes</p>
        <p className="lic-worlds-empty-sub">
          Fewer than three matched episodes include usable forward returns. See the Analog tab for match quality.
        </p>
      </div>
    )
  }

  const { scenarios, maxAbsReturn } = worlds

  return (
    <div className="lic-worlds-scenarios">
      <p className="lic-worlds-lead">
        Outcome split among similar historical setups (same realized_return definition as analog matching). Not a
        prediction — a sample grouped into thirds by return.
      </p>
      {caution ? (
        <div className="lic-worlds-banner lic-worlds-banner--caution" role="status">
          Weak historical match — treat scenario frequencies as exploratory.
        </div>
      ) : null}
      {mismatch ? (
        <div className="lic-worlds-banner lic-worlds-banner--warn" role="status">
          Historical outcomes do not align with current recommendation.
        </div>
      ) : null}
      <div className="lic-worlds-cards">
        {scenarios.map((s) => (
          <div key={s.key} className={`lic-worlds-card lic-worlds-card--${s.key}`}>
            <div className="lic-worlds-card-title">{s.label}</div>
            <div className="lic-worlds-card-prob">{(s.probability * 100).toFixed(0)}%</div>
            <div className="lic-worlds-card-sub">of matched sample</div>
            <div className="lic-worlds-card-return">
              Avg outcome <span>{(s.avgReturn * 100).toFixed(2)}%</span>
            </div>
            <ScenarioPath avgReturn={s.avgReturn} maxAbs={maxAbsReturn} />
            <div className="lic-worlds-card-n">n = {s.sampleSize} episodes</div>
          </div>
        ))}
      </div>
    </div>
  )
}
