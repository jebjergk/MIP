import { useState, useEffect } from 'react'
import { API_BASE } from '../App'
import LoadingState from '../components/LoadingState'
import { useExplainCenter } from '../context/ExplainCenterContext'
import { DEBUG_EXPLAIN_CONTEXT } from '../data/explainContexts'
import './Debug.css'

function previewFromData(data, isError = false) {
  if (isError) return typeof data === 'string' ? data : String(data)
  if (data == null) return '(null)'
  if (typeof data === 'object' && !Array.isArray(data)) {
    const keys = Object.keys(data)
    const subset = {}
    keys.slice(0, 3).forEach((k) => {
      subset[k] = data[k]
    })
    return JSON.stringify(subset)
  }
  if (Array.isArray(data)) {
    const len = data.length
    const first = data[0]
    if (first == null) return `[] (${len} items)`
    const keys = typeof first === 'object' && first !== null ? Object.keys(first) : []
    const subset = {}
    keys.slice(0, 3).forEach((k) => {
      subset[k] = first[k]
    })
    return `[${len} items] first: ${JSON.stringify(subset)}`
  }
  return String(data).slice(0, 80)
}

export default function Debug() {
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(true)
  const { setContext } = useExplainCenter()

  useEffect(() => {
    setContext(DEBUG_EXPLAIN_CONTEXT)
  }, [setContext])

  useEffect(() => {
    let cancelled = false

    async function runSmoke() {
      const out = []

      const parseResponse = async (r) => {
        const text = await r.text()
        if (!r.ok) return text
        try {
          return JSON.parse(text)
        } catch {
          return text
        }
      }

      // 1. /api/status
      try {
        const r = await fetch(`${API_BASE}/status`)
        const data = await parseResponse(r)
        out.push({
          url: `${API_BASE}/status`,
          status: r.status,
          ok: r.ok,
          preview: previewFromData(data, !r.ok),
          raw: data,
        })
      } catch (e) {
        out.push({
          url: `${API_BASE}/status`,
          status: 0,
          ok: false,
          preview: String(e?.message ?? e),
          raw: String(e?.message ?? e),
        })
      }

      // 2. /api/runs
      try {
        const r = await fetch(`${API_BASE}/runs`)
        const data = await parseResponse(r)
        out.push({
          url: `${API_BASE}/runs`,
          status: r.status,
          ok: r.ok,
          preview: previewFromData(data, !r.ok),
          raw: data,
        })
      } catch (e) {
        out.push({
          url: `${API_BASE}/runs`,
          status: 0,
          ok: false,
          preview: String(e?.message ?? e),
          raw: String(e?.message ?? e),
        })
      }

      // 3. /api/portfolios
      let portfolioId = null
      try {
        const r = await fetch(`${API_BASE}/portfolios`)
        const data = await parseResponse(r)
        if (r.ok && Array.isArray(data) && data.length > 0) {
          const first = data[0]
          portfolioId = first.portfolio_id ?? first.PORTFOLIO_ID ?? first.id
        }
        out.push({
          url: `${API_BASE}/portfolios`,
          status: r.status,
          ok: r.ok,
          preview: previewFromData(data, !r.ok),
          raw: data,
        })
      } catch (e) {
        out.push({
          url: `${API_BASE}/portfolios`,
          status: 0,
          ok: false,
          preview: String(e?.message ?? e),
          raw: String(e?.message ?? e),
        })
      }

      // 4. /api/briefs/latest?portfolio_id=...
      if (portfolioId != null) {
        try {
          const url = `${API_BASE}/briefs/latest?portfolio_id=${portfolioId}`
          const r = await fetch(url)
          const data = await parseResponse(r)
          out.push({
            url,
            status: r.status,
            ok: r.ok,
            preview: previewFromData(data, !r.ok),
            raw: data,
          })
        } catch (e) {
          out.push({
            url: `${API_BASE}/briefs/latest?portfolio_id=${portfolioId}`,
            status: 0,
            ok: false,
            preview: String(e?.message ?? e),
            raw: String(e?.message ?? e),
          })
        }
      } else {
        out.push({
          url: `${API_BASE}/briefs/latest?portfolio_id=...`,
          status: null,
          ok: null,
          preview: 'skipped: no portfolio id',
          raw: null,
        })
      }

      // 5. /api/training/status
      try {
        const r = await fetch(`${API_BASE}/training/status`)
        const data = await parseResponse(r)
        out.push({
          url: `${API_BASE}/training/status`,
          status: r.status,
          ok: r.ok,
          preview: previewFromData(data, !r.ok),
          raw: data,
        })
      } catch (e) {
        out.push({
          url: `${API_BASE}/training/status`,
          status: 0,
          ok: false,
          preview: String(e?.message ?? e),
          raw: String(e?.message ?? e),
        })
      }

      if (!cancelled) {
        setResults(out)
        setLoading(false)
      }
    }

    runSmoke()
    return () => { cancelled = true }
  }, [])

  const copyDiagnostics = () => {
    const blob = results.map((r) => ({
      url: r.url,
      status: r.status,
      ok: r.ok,
      preview: r.preview,
      raw: r.raw,
    }))
    navigator.clipboard.writeText(JSON.stringify(blob, null, 2))
  }

  return (
    <>
      <h1>Route smoke</h1>
      <p>Quick debug: which endpoints fail and why (404 vs 500 vs CORS vs proxy).</p>

      <button
        type="button"
        className="debug-copy-btn"
        onClick={copyDiagnostics}
        disabled={loading}
        aria-label="Copy diagnostics to clipboard"
      >
        Copy diagnostics
      </button>

      {loading && <LoadingState message="Running smoke checks…" />}

      <table className="debug-table">
        <thead>
          <tr>
            <th>URL</th>
            <th>Status</th>
            <th>Preview</th>
          </tr>
        </thead>
        <tbody>
          {results.map((r, i) => (
            <tr key={i} className={r.ok === true ? 'debug-ok' : r.ok === false ? 'debug-fail' : 'debug-skip'}>
              <td className="debug-url" title={r.url}>
                {r.url}
              </td>
              <td className="debug-status">
                {r.status != null ? r.status : '—'}
                {r.status === 0 && ' (network)'}
              </td>
              <td className="debug-preview" title={typeof r.preview === 'string' ? r.preview : JSON.stringify(r.preview)}>
                {r.preview}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}
