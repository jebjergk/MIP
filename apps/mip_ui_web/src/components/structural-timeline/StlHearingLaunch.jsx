import { useNavigate } from 'react-router-dom'
import { API_BASE } from '../../config/apiBase'

/**
 * Proposal rows with link to Committee 2.0 hearing room (opens via API then routes by hearing id).
 */
export default function StlHearingLaunch({ proposals, get }) {
  const navigate = useNavigate()
  if (!proposals || proposals.length === 0) return null

  const openHearing = async (proposalId) => {
    const pid = Number(proposalId)
    if (!Number.isFinite(pid) || pid < 1) {
      window.alert('Invalid proposal id — cannot open hearing.')
      return
    }
    try {
      const r = await fetch(`${API_BASE}/committee/hearing/open`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ proposal_id: pid, force_rebuild: false }),
      })
      let j = {}
      try {
        j = await r.json()
      } catch {
        window.alert(`Hearing Replay open failed (${r.status}): response was not JSON — check API / proxy.`)
        return
      }
      if (!r.ok) {
        const det = j.detail
        window.alert(typeof det === 'string' ? det : JSON.stringify(det ?? j))
        return
      }
      const hid = j.hearing_id
      if (!hid) {
        window.alert('API returned no hearing_id — check mip_ui_api /committee/hearing/open response.')
        return
      }
      navigate(`/structural-committee/${encodeURIComponent(hid)}`)
    } catch (e) {
      window.alert(e?.message || String(e))
    }
  }

  return (
    <div className="stl-hearing-launch">
      <h3 className="stl-hearing-launch-title">Hearing Replay launch</h3>
      <p className="stl-hearing-launch-hint">Open a structural hearing for a proposal (requires snapshot + bars).</p>
      <ul className="stl-hearing-launch-list">
        {proposals.slice(0, 12).map((p) => {
          const pid = get(p, 'PROPOSAL_ID') ?? p.proposal_id
          const fam = get(p, 'SETUP_FAMILY') ?? p.setup_family
          const created = get(p, 'PROPOSAL_CREATED_AT') ?? p.proposal_created_at
          return (
            <li key={pid}>
              <span className="stl-hearing-meta">{fam} · {String(created || '').slice(0, 10)}</span>
              <button type="button" className="stl-hearing-btn" onClick={() => openHearing(pid)}>
                Hearing #{pid}
              </button>
            </li>
          )
        })}
      </ul>
    </div>
  )
}
