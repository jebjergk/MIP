import {
  buildSimulatorWorldsStrip,
  findSimRow,
  formatSimPct,
  netScoreSubtitle,
  partialActionGuidance,
  positionChangeCopy,
  primaryRationaleBullets,
  recommendedActionHardLabel,
  regretHumanNarrative,
  simNumTone,
  sortRowsByNetBenefitVsHold,
  tradeoffVsHoldLines,
} from './licSimulatorPanelModel'

function safeText(v, fallback = '\u2014') {
  if (v == null || v === '') return fallback
  if (typeof v === 'string' || typeof v === 'number') return String(v)
  if (typeof v === 'boolean') return v ? 'Yes' : 'No'
  if (typeof v === 'object') {
    try {
      return JSON.stringify(v).slice(0, 400)
    } catch {
      return fallback
    }
  }
  return String(v)
}

function asArray(x) {
  if (Array.isArray(x)) return x
  if (x == null) return []
  return [x]
}

function resolveRecommendedRow(rows, bestKey) {
  const hit = findSimRow(rows, bestKey)
  if (hit) return hit
  if (!rows.length) return null
  return [...rows].sort((a, b) => Number(b.net_score) - Number(a.net_score))[0]
}

/**
 * Simulator drill panel: decision communication + Worlds context (interpretation only).
 */
export default function LicSimulatorPanel({ activeIntel, workspacePres }) {
  const sim = activeIntel?.action_simulation || {}
  const best = sim.best_action || {}
  const rawRows = asArray(sim.alternatives)
  const holdRow = findSimRow(rawRows, 'hold')
  const bestKey = best?.action
  const recRow = resolveRecommendedRow(rawRows, bestKey)
  const pres = workspacePres
  const misaligned = sim.aligns_with_tile_recommendation === false

  const worldsStrip = buildSimulatorWorldsStrip(activeIntel?.analog_summary)
  const tierKey = worldsStrip.tierKey
  const tradeoffLines = tradeoffVsHoldLines(holdRow, recRow)
  const rationaleBullets = primaryRationaleBullets(holdRow, recRow, tierKey)
  const netSub = netScoreSubtitle(recRow, holdRow)
  const pos = positionChangeCopy(recRow?.action)
  const regretStory = regretHumanNarrative(recRow, recRow?.action)
  const softGuidance = partialActionGuidance(tierKey)
  const displayRows = sortRowsByNetBenefitVsHold(rawRows, holdRow)

  if (!rawRows.length) {
    return <p className="lic-drill-muted">No simulator rows loaded for this symbol.</p>
  }

  return (
    <>
      <div className="lic-sim-worlds-strip" aria-label="Worlds context for simulator">
        <div className="lic-sim-worlds-strip-tag">{worldsStrip.tag}</div>
        <div className="lic-sim-worlds-strip-metrics">
          {worldsStrip.upsidePct != null && worldsStrip.downsidePct != null ? (
            <>
              Upside: {worldsStrip.upsidePct}% <span className="lic-sim-worlds-sep">·</span> Downside:{' '}
              {worldsStrip.downsidePct}%
            </>
          ) : (
            <span className="lic-sim-worlds-muted">Bucket rates unavailable on thin analog sample</span>
          )}
        </div>
        <div className="lic-sim-worlds-strip-conf">{worldsStrip.confidenceLine}</div>
        <div className="lic-sim-worlds-strip-arrow">
          → {worldsStrip.interpret}
        </div>
      </div>

      <p className="lic-sim-tile-ref">
        Official tile: <strong>{safeText(pres?.primary_action)}</strong>
        <span
          className="lic-sim-tile-ref-hint"
          title="Risk ladder posture vs exploratory net-score ranking in the table below."
        >
          {' '}
          · simulator compares five actions on modeled upside, downside, and giveback
        </span>
      </p>

      {misaligned && pres?.fallback_action ? (
        <div className="lic-sim-misalign" role="status">
          <div className="lic-sim-misalign-title">Tile vs simulator</div>
          <p className="lic-sim-misalign-copy">
            Net-score lens ranks <strong>{safeText(recRow?.label || best.label)}</strong> ahead of the tile-preferred{' '}
            <strong>{safeText(pres.fallback_action)}</strong>. Primary stance stays{' '}
            <strong>{safeText(pres.primary_action)}</strong> unless you explicitly prioritize this defensive ranking.
            {sim.misalignment_note ? <> {safeText(sim.misalignment_note)}</> : null}
          </p>
          {pres?.primary_reason ? (
            <p className="lic-sim-misalign-rationale">
              Tile rationale:{' '}
              {(() => {
                const pr = safeText(pres.primary_reason)
                return pr.length > 180 ? `${pr.slice(0, 180)}…` : pr
              })()}
            </p>
          ) : null}
        </div>
      ) : misaligned ? (
        <div className="lic-sim-misalign" role="status">
          <div className="lic-sim-misalign-title">Tile vs simulator</div>
          <p className="lic-sim-misalign-copy">
            Simulator net-score winner differs from the official tile stance. {safeText(sim.misalignment_note)}
          </p>
        </div>
      ) : (
        <p className="lic-sim-frame lic-sim-frame--sub">
          Net-score leader matches the primary stance — use the table to compare trims and tightening versus full exit.
        </p>
      )}

      <div className={`lic-sim-primary-block ${misaligned ? 'lic-sim-primary-block--misalign' : ''}`}>
        <div className="lic-sim-primary-kicker">Primary decision (simulator)</div>
        <div className="lic-sim-primary-action-row">
          <span className="lic-sim-primary-label">Recommended action:</span>{' '}
          <span className="lic-sim-primary-action">
            {recommendedActionHardLabel(recRow?.label || best.label)}
          </span>
        </div>
        <div className="lic-sim-primary-confidence">{worldsStrip.confidenceLine}</div>
        {softGuidance ? <p className="lic-sim-primary-soft">{softGuidance}</p> : null}
        <div className="lic-sim-primary-rationale">
          <span className="lic-sim-primary-rationale-k">Rationale</span>
          <ul>
            {rationaleBullets.map((b, i) => (
              <li key={`rat-${i}`}>{b}</li>
            ))}
          </ul>
        </div>
        <p className="lic-sim-net-line" title={netSub.tooltip}>
          {netSub.title}
        </p>
      </div>

      {recRow && recRow.action !== 'hold' ? (
        <div className="lic-sim-tradeoff-block">
          <div className="lic-sim-tradeoff-h">Impact vs HOLD</div>
          <ul className="lic-sim-tradeoff-list">
            {tradeoffLines.map((line) => (
              <li
                key={line.key}
                className={line.good ? 'lic-sim-tradeoff-line--good' : 'lic-sim-tradeoff-line--warn'}
              >
                {line.text}
              </li>
            ))}
          </ul>
          {tradeoffLines.length === 0 ? (
            <p className="lic-sim-tradeoff-empty">No material deltas vs HOLD on these metrics.</p>
          ) : null}
        </div>
      ) : null}

      <div className="lic-sim-position-block">
        <div className="lic-sim-position-h">Position change (notional)</div>
        <p className="lic-sim-position-lines">
          Current: {pos.currentPct}% → Target: {pos.targetPct}%<br />
          <span className="lic-sim-position-detail">{pos.detail}</span>
        </p>
      </div>

      {recRow ? (
        <div className="lic-sim-regret-block">
          <div className="lic-sim-regret-h">Regret (plain language)</div>
          <p className="lic-sim-regret-copy">{regretStory}</p>
        </div>
      ) : null}

      <section className="lic-drill-section">
        <h5 className="lic-drill-h">All actions</h5>
        <p className="lic-drill-muted">
          Sorted by net benefit vs HOLD. The recommended row is highlighted; official tile stance is in the strip above
          when it differs.
        </p>
        <div className="lic-sim-table-wrap">
          <table className="lic-sim-table lic-sim-table--readable">
            <thead>
              <tr>
                <th>Action</th>
                <th className="lic-sim-th-num">Upside</th>
                <th className="lic-sim-th-num">Downside</th>
                <th className="lic-sim-th-num">Giveback</th>
                <th>Regret</th>
              </tr>
            </thead>
            <tbody>
              {displayRows.map((r, ri) => {
                const isBest = r?.action === bestKey
                const tones = simNumTone(r?.expected_upside, r?.expected_downside)
                return (
                  <tr
                    key={r?.action ?? r?.label ?? `sim-${ri}`}
                    className={isBest ? 'lic-sim-row--best' : undefined}
                  >
                    <td className="lic-sim-td-action">
                      {isBest ? <span className="lic-sim-rec-badge">Recommended</span> : null}
                      {safeText(r?.label)}
                    </td>
                    <td className={`lic-sim-td-num ${tones.upClass}`}>{formatSimPct(r?.expected_upside)}</td>
                    <td className={`lic-sim-td-num ${tones.downClass}`}>{formatSimPct(r?.expected_downside)}</td>
                    <td className="lic-sim-td-num">{formatSimPct(r?.giveback_risk)}</td>
                    <td className="lic-sim-td-regret">{safeText(r?.regret_tilt)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </section>

      <section className="lic-drill-section">
        <h5 className="lic-drill-h">Rationale by action</h5>
        {displayRows.map((r, ri) => (
          <p key={`${r?.action ?? ri}-rat`} className="lic-sim-rat">
            <b>{r?.label}:</b> {safeText(r?.rationale)}
          </p>
        ))}
      </section>
    </>
  )
}
