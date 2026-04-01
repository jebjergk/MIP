import {
  buildSimulatorWorldsStrip,
  findSimRow,
  formatSimPct,
  impactStripVsHold,
  netScoreSubtitle,
  positionChangeCopy,
  primaryRationaleBullets,
  recommendedActionHardLabel,
  regretShortPhrase,
  secondBestActionKey,
  simNumTone,
  sortRowsByNetBenefitVsHold,
  tradeoffOneLineVsHold,
  worldsConfidenceInline,
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
 * Simulator drill panel — compressed decision hierarchy (presentation only).
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
  const rationaleBullets = primaryRationaleBullets(holdRow, recRow, tierKey)
  const netSub = netScoreSubtitle(recRow, holdRow)
  const pos = positionChangeCopy(recRow?.action)
  const tradeLine = tradeoffOneLineVsHold(holdRow, recRow)
  const impactSlots = impactStripVsHold(holdRow, recRow)
  const runnerKey = secondBestActionKey(rawRows)
  const displayRows = sortRowsByNetBenefitVsHold(rawRows, holdRow)
  const confInline = worldsConfidenceInline(worldsStrip)

  const worldsSub =
    worldsStrip.upsidePct != null && worldsStrip.downsidePct != null
      ? `${worldsStrip.tag} · ↑${worldsStrip.upsidePct}% / ↓${worldsStrip.downsidePct}%`
      : `${worldsStrip.tag} · bucket rates need more analogs`

  if (!rawRows.length) {
    return <p className="lic-drill-muted">No simulator rows loaded for this symbol.</p>
  }

  return (
    <div className="lic-sim-stack">
      <div
        className={`lic-sim-decision-hero ${misaligned ? 'lic-sim-decision-hero--warn' : ''}`}
        aria-label="Simulator recommendation"
      >
        <div className="lic-sim-hero-title">
          <span className="lic-sim-hero-action">{recommendedActionHardLabel(recRow?.label || best.label)}</span>
          <span className="lic-sim-hero-conf" title={worldsStrip.confidenceLine}>
            · {confInline}
          </span>
        </div>
        <div className="lic-sim-hero-sub">
          {worldsSub}
          {' · '}
          Tile {safeText(pres?.primary_action)}
          {tierKey === 'very_low' || tierKey === 'low' ? ' · thin Worlds — size conservatively' : ''}
        </div>
        {misaligned ? (
          <div className="lic-sim-hero-clash" role="status">
            Simulator pick differs from official tile ({safeText(pres?.primary_action)}).
          </div>
        ) : null}
        <ul className="lic-sim-hero-bullets">
          {rationaleBullets.map((b, i) => (
            <li key={`b-${i}`}>{b}</li>
          ))}
        </ul>
        <p className="lic-sim-hero-trade" title={netSub.tooltip}>
          {tradeLine}
        </p>
      </div>

      <div className="lic-sim-impact-strip" aria-label="Impact versus HOLD">
        {impactSlots.map((slot) => (
          <div
            key={slot.key}
            className={`lic-sim-impact-cell lic-sim-impact-cell--${slot.tone}`}
          >
            <span className="lic-sim-impact-arrow">{slot.arrow}</span>
            <span className="lic-sim-impact-name">{slot.label}</span>
            <span className="lic-sim-impact-val">{slot.value}</span>
          </div>
        ))}
      </div>

      <div className="lic-sim-pos-compact" title={pos.detail}>
        <div className="lic-sim-pos-bar-label">Exposure</div>
        <div className="lic-sim-pos-bar-row">
          <span className="lic-sim-pos-bar-ext">0</span>
          <div className="lic-sim-pos-bar-track">
            <div className="lic-sim-pos-bar-fill" style={{ width: `${pos.targetPct}%` }} />
          </div>
          <span className="lic-sim-pos-bar-ext">100%</span>
        </div>
        <div className="lic-sim-pos-bar-caption">
          {pos.currentPct}% now → <strong>{pos.targetPct}%</strong> target
        </div>
      </div>

      <p className="lic-sim-regret-one">
        Regret: {regretShortPhrase(recRow)}
      </p>

      <section className="lic-sim-table-section">
        <h5 className="lic-sim-table-h">Actions</h5>
        <div className="lic-sim-table-wrap lic-sim-table-wrap--tight">
          <table className="lic-sim-table lic-sim-table--readable lic-sim-table--scan">
            <thead>
              <tr>
                <th>Action</th>
                <th className="lic-sim-th-num">Upside</th>
                <th className="lic-sim-th-num">Down</th>
                <th className="lic-sim-th-num">Giveback</th>
                <th>Regret</th>
              </tr>
            </thead>
            <tbody>
              {displayRows.map((r, ri) => {
                const isBest = r?.action === bestKey
                const isRunner = r?.action === runnerKey && !isBest
                const tones = simNumTone(r?.expected_upside, r?.expected_downside)
                const rowClass = isBest ? 'lic-sim-row--best' : isRunner ? 'lic-sim-row--runner' : 'lic-sim-row--dim'
                return (
                  <tr key={r?.action ?? r?.label ?? `sim-${ri}`} className={rowClass}>
                    <td className="lic-sim-td-action">
                      {isBest ? <span className="lic-sim-rec-badge">Recommended</span> : null}
                      {isRunner ? <span className="lic-sim-run-badge">2nd</span> : null}
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
    </div>
  )
}
