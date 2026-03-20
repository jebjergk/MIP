import { useEffect, useState } from 'react'
import { API_BASE } from '../App'
import AskCoverageDashboardPanel from '../components/AskCoverageDashboardPanel'

export default function GlossaryAdminPage() {
  const [terms, setTerms] = useState([])
  const [queue, setQueue] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let mounted = true
    const load = async () => {
      setLoading(true)
      try {
        const [termsRes, queueRes] = await Promise.all([
          fetch(`${API_BASE}/ask/glossary?limit=200`),
          fetch(`${API_BASE}/ask/glossary/review-queue?limit=200`),
        ])
        const termsData = await termsRes.json()
        const queueData = await queueRes.json()
        if (!mounted) return
        setTerms(termsData.items || [])
        setQueue(queueData.items || [])
      } finally {
        if (mounted) setLoading(false)
      }
    }
    load()
    return () => {
      mounted = false
    }
  }, [])

  return (
    <div className="page-shell">
      <h2>Ask MIP Glossary Admin</h2>
      <p>Manage approved terms and pending review candidates.</p>

      <section>
        <h3>Approved / Existing Terms</h3>
        {loading ? <p>Loading...</p> : (
          <table className="table">
            <thead>
              <tr>
                <th>Term</th>
                <th>Category</th>
                <th>Approved</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {terms.slice(0, 200).map((row) => (
                <tr key={row.TERM_KEY}>
                  <td>{row.DISPLAY_TERM || row.TERM_KEY}</td>
                  <td>{row.CATEGORY}</td>
                  <td>{String(row.IS_APPROVED)}</td>
                  <td>{row.REVIEW_STATUS}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section style={{ marginTop: 24 }}>
        <h3>Pending Review</h3>
        {loading ? <p>Loading...</p> : (
          <table className="table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Term</th>
                <th>Category</th>
                <th>Source</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {queue.slice(0, 200).map((row) => (
                <tr key={row.CANDIDATE_ID}>
                  <td>{row.CANDIDATE_ID}</td>
                  <td>{row.TERM_TEXT}</td>
                  <td>{row.CATEGORY}</td>
                  <td>{row.SOURCE_TYPE}</td>
                  <td>{row.REVIEW_STATUS}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
      <AskCoverageDashboardPanel />
    </div>
  )
}
