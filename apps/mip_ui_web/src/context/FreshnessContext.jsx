import { createContext, useContext, useState, useEffect, useCallback } from 'react'
import { API_BASE } from '../App'

/**
 * FreshnessContext provides system-wide freshness info for staleness checks.
 * 
 * Properties:
 * - latestRunId: The latest successful pipeline run ID
 * - latestRunTs: Timestamp of the latest successful run
 * - loading: Whether freshness data is loading
 * - error: Any error fetching freshness data
 * - refresh: Function to manually refresh freshness data
 * - isStale(runId): Check if a given run ID is stale compared to latest
 */
const FreshnessContext = createContext({
  latestRunId: null,
  latestRunTs: null,
  loading: true,
  error: null,
  refresh: () => {},
  isStale: () => false,
})

export function FreshnessProvider({ children }) {
  const [latestRunId, setLatestRunId] = useState(null)
  const [latestRunTs, setLatestRunTs] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  const fetchFreshness = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch(`${API_BASE}/status`)
      if (!res.ok) throw new Error(res.statusText)
      const data = await res.json()
      setLatestRunId(data.latest_success_run_id || null)
      setLatestRunTs(data.latest_success_ts || null)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchFreshness()
    // Poll every 5 minutes for freshness updates
    const interval = setInterval(fetchFreshness, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [fetchFreshness])

  const isStale = useCallback((runId) => {
    if (!latestRunId || !runId) return false
    return runId !== latestRunId
  }, [latestRunId])

  const value = {
    latestRunId,
    latestRunTs,
    loading,
    error,
    refresh: fetchFreshness,
    isStale,
  }

  return (
    <FreshnessContext.Provider value={value}>
      {children}
    </FreshnessContext.Provider>
  )
}

export function useFreshness() {
  return useContext(FreshnessContext)
}

/**
 * Hook to check if a specific run is stale.
 * Returns { isStale, latestRunId, latestRunTs }
 */
export function useStalenessCheck(runId) {
  const { latestRunId, latestRunTs, isStale } = useFreshness()
  return {
    isStale: isStale(runId),
    latestRunId,
    latestRunTs,
  }
}

/**
 * Format relative time (e.g., "2 min ago", "1 hour ago").
 */
export function relativeTime(isoOrDate) {
  if (isoOrDate == null) return '—'
  const date = typeof isoOrDate === 'string' ? new Date(isoOrDate) : isoOrDate
  if (Number.isNaN(date.getTime())) return '—'
  const now = Date.now()
  const sec = Math.floor((now - date.getTime()) / 1000)
  if (sec < 0) return 'just now'
  if (sec < 60) return `${sec}s ago`
  const min = Math.floor(sec / 60)
  if (min < 60) return `${min}m ago`
  const hr = Math.floor(min / 60)
  if (hr < 24) return `${hr}h ago`
  const day = Math.floor(hr / 24)
  return `${day}d ago`
}

export default FreshnessContext
