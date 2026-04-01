import {
  buildThreeWorldScenariosFromAnalog,
  formatScenarioReturnDisplay,
  worldsBucketProbabilityPresentation,
  worldsConfidenceTier,
  worldsDominantHeaderLine,
  worldsMostLikelyKey,
  worldsRecommendationMismatch,
  worldsScenarioGuidance,
  worldsScenarioInterpretation,
  worldsSparseCaution,
} from "./licWorldsFromAnalog"

const PATH_EXAG = 1.45

function ScenarioPath({ scenarioKey, minReturn, maxReturn, avgReturn, sampleSize }) {
  const x0 = 8
  const x1 = 92
  const yMid = 22
  const amp = 18 * PATH_EXAG

  if (!sampleSize) {
    return null
  }

  let y1 = yMid
  let yCtrl = yMid
  if (scenarioKey === "upside") {
    y1 = yMid - amp
    yCtrl = yMid - amp * 0.55
  } else if (scenarioKey === "downside") {
    y1 = yMid + amp
    yCtrl = yMid + amp * 0.55
  } else {
    yCtrl = yMid + 4.5
    y1 = yMid - 2.2
  }

  const mx = (x0 + x1) / 2
  const d = `M ${x0} ${yMid} Q ${mx} ${yCtrl} ${x1} ${y1}`

  let bandD = null
  if (
    minReturn != null &&
    maxReturn != null &&
    Number.isFinite(minReturn) &&
    Number.isFinite(maxReturn) &&
    avgReturn != null &&
    Math.abs(maxReturn - minReturn) > 1e-8
  ) {
    const span = Math.max(Math.abs(maxReturn - avgReturn), Math.abs(avgReturn - minReturn), 1e-6)
    const tMax = (maxReturn - avgReturn) / span
    const tMin = (minReturn - avgReturn) / span
    const yHi = yMid - Math.max(-1, Math.min(1, tMax)) * amp * 0.85
    const yLo = yMid - Math.max(-1, Math.min(1, tMin)) * amp * 0.85
    const top = Math.min(yHi, yLo)
    const bot = Math.max(yHi, yLo)
    bandD = `M ${x0} ${yMid} L ${x1} ${top} L ${x1} ${bot} Z`
  }

  const strokeW = scenarioKey === "base" ? 1.35 : 1.65

  return (
    <svg
      className={`lic-worlds-path lic-worlds-path--${scenarioKey}`}
      viewBox="0 0 100 44"
      preserveAspectRatio="none"
      aria-hidden
    >
      <line
        x1={x0}
        y1={yMid}
        x2={x1}
        y2={yMid}
        stroke="currentColor"
        strokeWidth={0.4}
        strokeDasharray="3 2"
        opacity={0.3}
      />
      {bandD ? <path d={bandD} fill="currentColor" opacity={0.08} stroke="none" /> : null}
      <path d={d} fill="none" stroke="currentColor" strokeWidth={strokeW} strokeLinecap="round" />
      <circle cx={x0} cy={yMid} r={2.1} fill="currentColor" opacity={0.9} />
      <circle cx={x1} cy={y1} r={2.4} fill="currentColor" />
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

  const { scenarios, totalN } = worlds
  const conf = worldsConfidenceTier(totalN)
  const dominantLine = worldsDominantHeaderLine(scenarios, totalN)
  const likelyKey = worldsMostLikelyKey(scenarios)

  return (
    <div className="lic-worlds-scenarios">
      <p className="lic-worlds-dominant" role="status">
        {dominantLine}
      </p>
      <p className="lic-worlds-lead">
        Historical outcomes from similar setups (same{" "}
        <code className="lic-worlds-lead-code">realized_return</code> as analog matching), grouped into upside (over 1%),
        base (within ±1%), and downside (under -1%). Not a prediction.
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
          const hasSamples = s.sampleSize > 0
          const probPres = worldsBucketProbabilityPresentation(s, totalN)
          const ret = formatScenarioReturnDisplay(s.avgReturn, s.key)
          const retCls =
            ret.tone === "pos"
              ? "lic-worlds-card-return-val--pos"
              : ret.tone === "neg"
                ? "lic-worlds-card-return-val--neg"
                : "lic-worlds-card-return-val--neutral"
          const isLikely = hasSamples && likelyKey === s.key && s.probability > 0
          const lowConf = conf.key === "very_low" || conf.key === "low"
          const emptyWorld = s.sampleSize === 0
          const cardOpacity = emptyWorld ? Math.min(conf.cardOpacity, 0.55) : conf.cardOpacity

          return (
            <div
              key={s.key}
              className={`lic-worlds-card lic-worlds-card--${s.key}${isLikely ? " lic-worlds-card--most-likely" : ""}${lowConf ? " lic-worlds-card--low-conf" : ""}${emptyWorld ? " lic-worlds-card--empty-world" : ""}`}
              style={{ opacity: cardOpacity }}
            >
              <div className="lic-worlds-card-confidence" aria-label={`Confidence ${conf.label}`}>
                <span className="lic-worlds-card-confidence-emoji" aria-hidden>
                  {conf.emoji}
                </span>{" "}
                <span className="lic-worlds-card-confidence-label">{conf.label}</span>
                <span className="lic-worlds-card-confidence-n"> (n = {totalN})</span>
              </div>
              <div className="lic-worlds-card-title">
                {s.label}
                {isLikely ? <span className="lic-worlds-card-most-likely-badge"> Most likely</span> : null}
              </div>
              {emptyWorld ? (
                <>
                  <p className="lic-worlds-card-empty-msg">No historical examples for this scenario</p>
                  <div className="lic-worlds-card-n lic-worlds-card-n--empty-footer">n = 0 in this bucket</div>
                </>
              ) : (
                <>
                  {probPres.showProbBlock ? (
                    <>
                      <div
                        className={`lic-worlds-card-prob${/^\d{1,3}%$/.test(String(probPres.mainLine).trim()) ? "" : " lic-worlds-card-prob--soft"}`}
                      >
                        {probPres.mainLine}
                      </div>
                      {probPres.strengthLabel ? (
                        <div className="lic-worlds-card-prob-label">{probPres.strengthLabel}</div>
                      ) : null}
                      {probPres.subLine ? <div className="lic-worlds-card-sub">{probPres.subLine}</div> : null}
                    </>
                  ) : null}
                  <div className="lic-worlds-card-return">
                    Avg outcome <span className={`lic-worlds-card-return-val ${retCls}`}>{ret.text}</span>
                  </div>
                  <ScenarioPath
                    scenarioKey={s.key}
                    minReturn={s.minReturn}
                    maxReturn={s.maxReturn}
                    avgReturn={s.avgReturn}
                    sampleSize={s.sampleSize}
                  />
                  <p className="lic-worlds-card-interpret">{worldsScenarioInterpretation(s.key)}</p>
                  <div className="lic-worlds-card-n">n = {s.sampleSize} in this bucket</div>
                  <p className="lic-worlds-card-guidance">{worldsScenarioGuidance(s.key)}</p>
                </>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
