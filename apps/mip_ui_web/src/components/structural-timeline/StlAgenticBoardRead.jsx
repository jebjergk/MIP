/**
 * Agentic Phase 4 read for Structural Market Timeline — same payload as LPA.
 */
import { useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../../config/apiBase'
import Phase4ChairSection from '../board/Phase4ChairSection'

export default function StlAgenticBoardRead({ proposals, get }) {
  const activeRows = useMemo(() => {
    const rows = Array.isArray(proposals) ? proposals : []
    const proposed = rows.filter((p) => {
      const st = String(get(p, 'PROPOSAL_STATUS') || get(p, 'STATUS') || '').toUpperCase()
      return st === 'PROPOSED'
    })
    proposed.sort((a, b) => {
      const ta = new Date(get(a, 'PROPOSAL_CREATED_AT') || get(a, 'CREATED_AT') || 0).getTime()
      const tb = new Date(get(b, 'PROPOSAL_CREATED_AT') || get(b, 'CREATED_AT') || 0).getTime()
      return tb - ta
    })
    return proposed
  }, [proposals, get])

  const [selectedPid, setSelectedPid] = useState(null)

  useEffect(() => {
    if (activeRows.length === 0) {
      setSelectedPid(null)
      return
    }
    if (activeRows.length === 1) {
      const pid = Number(get(activeRows[0], 'PROPOSAL_ID'))
      setSelectedPid(Number.isFinite(pid) ? pid : null)
      return
    }
    const first = Number(get(activeRows[0], 'PROPOSAL_ID'))
    setSelectedPid((prev) => {
      if (prev != null && activeRows.some((r) => Number(get(r, 'PROPOSAL_ID')) === prev)) return prev
      return Number.isFinite(first) ? first : null
    })
  }, [activeRows, get])

  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)

  useEffect(() => {
    if (selectedPid == null) {
      setData(null)
      return undefined
    }
    let cancelled = false
    setLoading(true)
    setErr(null)
    fetch(`${API_BASE}/committee/proposal/${encodeURIComponent(selectedPid)}/board-explanation`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`${r.status}`))))
      .then((j) => {
        if (!cancelled) setData(j)
      })
      .catch((e) => {
        if (!cancelled) setErr(String(e?.message || e))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [selectedPid])

  if (activeRows.length === 0) return null

  const phase4Chair = data?.phase4_chair || null
  const phase4LatestHealth = data?.phase4_latest_health || null
  const hasPhase4 = Boolean(phase4Chair || phase4LatestHealth)

  return (
    <section className="stl-agentic-read">
      <div className="stl-agentic-read-head">
        <h3 className="stl-agentic-read-title">Agentic board read (Phase 4)</h3>
        {activeRows.length > 1 ? (
          <label className="stl-agentic-read-select-wrap">
            <span className="stl-agentic-read-select-label">Proposal</span>
            <select
              className="stl-agentic-read-select"
              value={selectedPid ?? ''}
              onChange={(e) => setSelectedPid(Number(e.target.value))}
            >
              {activeRows.map((r) => {
                const pid = get(r, 'PROPOSAL_ID')
                const fam = get(r, 'SETUP_FAMILY') || ''
                const dt = String(get(r, 'PROPOSAL_CREATED_AT') || get(r, 'CREATED_AT') || '').slice(0, 10)
                return (
                  <option key={pid} value={pid}>
                    #{pid} {fam ? `${fam} · ` : ''}{dt}
                  </option>
                )
              })}
            </select>
          </label>
        ) : (
          <span className="stl-agentic-read-single">Proposal #{selectedPid}</span>
        )}
      </div>
      {loading ? (
        <p className="stl-muted">Loading agentic read…</p>
      ) : err ? (
        <p className="stl-muted">Could not load agentic read: {err}</p>
      ) : data?.available === false ? (
        <p className="stl-muted">{data?.note || 'No board lineage for this proposal.'}</p>
      ) : hasPhase4 ? (
        <>
          <p className="stl-agentic-read-hint">
            Symbol Tracker chart: <strong>orange</strong> dashed = proposal publication date;
            {' '}<strong>teal</strong> solid = latest Phase 4 watch / wait / failure verdict.
            Structural chart: orange <strong>P</strong> = published proposal; <strong>amber M</strong> = board watch / monitor / wait verdict (from board markers feed).
          </p>
          <Phase4ChairSection phase4Chair={phase4Chair} phase4LatestHealth={phase4LatestHealth} />
        </>
      ) : (
        <p className="stl-muted">No Phase 4 chair payload returned.</p>
      )}
    </section>
  )
}
