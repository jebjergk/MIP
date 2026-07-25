/**
 * Read-only display of shadow Stage 0.5 intraday_session_picture slice.
 * Compact interpretation for operators — no raw OHLC bars.
 * Does not affect submit gating.
 */

function verdictClass(bucket) {
  const v = String(bucket || '').toUpperCase()
  if (v === 'SUPPORTS') return 'isp-verdict--supports'
  if (v === 'CHALLENGES') return 'isp-verdict--challenges'
  return 'isp-verdict--mixed'
}

function fmtLevel(v) {
  if (v == null) return null
  const n = Number(v)
  if (!Number.isFinite(n)) return null
  return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function fmtTs(v) {
  if (!v) return null
  const s = String(v)
  const m = s.match(/(\d{2}:\d{2})/)
  return m ? m[1] : s.slice(0, 16)
}

function reclaimStatusLabel(status) {
  const s = String(status || '').toUpperCase()
  if (s === 'INSUFFICIENT_RTH_DATA') return 'Insufficient RTH data (early session)'
  if (s === 'HELD') return 'Held'
  if (s === 'FAILED') return 'Failed'
  if (s === 'PENDING') return 'Pending confirmation'
  return status ? String(status).replace(/_/g, ' ') : null
}

export default function IntradaySessionPictureCard({ picture }) {
  if (!picture || typeof picture !== 'object') return null

  const available = Boolean(picture.session_available)
  const verdict = String(picture.verdict_bucket || '').toUpperCase()
  const vs = picture.vs_overnight_dossier || {}
  const reclaimLevel = fmtLevel(vs.reclaim_level)
  const reclaimStatus = vs.reclaim_status ? reclaimStatusLabel(vs.reclaim_status) : null
  const reclaimSource = vs.reclaim_level_source ? String(vs.reclaim_level_source).replace(/_/g, ' ') : null
  const execGeo = picture.executable_geometry || {}
  const execZoneLow = fmtLevel(execGeo.entry_zone_low)
  const execZoneHigh = fmtLevel(execGeo.entry_zone_high)
  const overnightBinding = vs.overnight_flags_still_binding
  const ibFetch = picture.ib_fetch || {}
  const ibPort = ibFetch.port ?? ibFetch.ib_connect?.port

  return (
    <section className="isp-card" aria-label="RTH intraday substantiation">
      <header className="isp-header">
        <span className="isp-chip">RTH SUBSTANTIATION</span>
        <span className="isp-subtitle">Since session open · no pre-market</span>
      </header>

      {!available ? (
        <div className="isp-unavailable">
          <span className="isp-reason-chip">
            {String(picture.reason || 'UNAVAILABLE').replace(/_/g, ' ')}
          </span>
          <p className="isp-operator">{picture.operator_line || 'Intraday picture not available for this run.'}</p>
          {ibPort != null ? (
            <p className="isp-meta">IB fetch port {ibPort}{ibFetch.status ? ` · ${ibFetch.status}` : ''}</p>
          ) : null}
        </div>
      ) : (
        <>
          <div className="isp-verdict-row">
            <span className={`isp-verdict ${verdictClass(verdict)}`}>
              {verdict || 'MIXED'}
            </span>
            {picture.confidence_band ? (
              <span className="isp-conf-band">{picture.confidence_band} confidence</span>
            ) : null}
            {picture.bar_count != null ? (
              <span className="isp-meta">{picture.bar_count}×15m bars</span>
            ) : null}
          </div>

          {picture.headline ? <p className="isp-headline">{picture.headline}</p> : null}
          {picture.operator_line ? <p className="isp-operator">{picture.operator_line}</p> : null}

          <dl className="isp-metrics">
            {picture.session_character ? (
              <>
                <dt>Session</dt>
                <dd>{String(picture.session_character).replace(/_/g, ' ').toLowerCase()}</dd>
              </>
            ) : null}
            {reclaimStatus && reclaimLevel ? (
              <>
                <dt>Support {reclaimLevel}{reclaimSource ? ` (${reclaimSource})` : ''}</dt>
                <dd>{reclaimStatus}</dd>
              </>
            ) : null}
            {execZoneLow && execZoneHigh ? (
              <>
                <dt>Executable zone</dt>
                <dd>
                  {execZoneLow}–{execZoneHigh}
                  {execGeo.chase_risk ? ' · above zone (do not chase)' : ''}
                </dd>
              </>
            ) : null}
            {overnightBinding != null ? (
              <>
                <dt>Overnight flags</dt>
                <dd className={overnightBinding ? 'isp-binding--yes' : 'isp-binding--no'}>
                  {overnightBinding ? 'Still binding' : 'Overridden by today\'s tape'}
                </dd>
              </>
            ) : null}
            {picture.wick_noise ? (
              <>
                <dt>Wick noise</dt>
                <dd>{picture.wick_noise}</dd>
              </>
            ) : null}
            {vs.dominant_tension ? (
              <>
                <dt>Tension</dt>
                <dd>{vs.dominant_tension}</dd>
              </>
            ) : null}
          </dl>

          {(picture.as_of_ts || picture.rth_open_ts) ? (
            <p className="isp-meta isp-times">
              {picture.rth_open_ts ? `Open ${fmtTs(picture.rth_open_ts)}` : null}
              {picture.as_of_ts ? ` · as of ${fmtTs(picture.as_of_ts)}` : null}
              {picture.low_sample_warning ? ' · early session (low sample)' : null}
            </p>
          ) : null}
        </>
      )}
    </section>
  )
}
