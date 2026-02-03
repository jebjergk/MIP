import { createContext, useContext, useState, useEffect, useMemo } from 'react'
import { API_BASE } from '../App'

const PortfolioContext = createContext({
  portfolios: [],
  defaultPortfolioId: null,
  loading: true,
  error: null,
})

/**
 * Fetches portfolio list and exposes default portfolio for multi-portfolio UX.
 * defaultPortfolioId = first ACTIVE portfolio by PORTFOLIO_ID, or first row if none active, or null if empty.
 * Fallback to 1 only when rendering needs a number (e.g. API calls) for backward compatibility.
 */
export function PortfolioProvider({ children }) {
  const [portfolios, setPortfolios] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch(`${API_BASE}/portfolios`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((data) => {
        if (!cancelled && Array.isArray(data)) setPortfolios(data)
      })
      .catch((e) => {
        if (!cancelled) setError(e.message)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [])

  const value = useMemo(() => {
    const active = portfolios.filter((p) => (p.STATUS || p.status || '').toUpperCase() === 'ACTIVE')
    const firstActive = active.length ? active[0] : portfolios[0]
    const defaultPortfolioId = firstActive != null
      ? (firstActive.PORTFOLIO_ID ?? firstActive.portfolio_id ?? firstActive.id)
      : null
    return {
      portfolios,
      defaultPortfolioId: defaultPortfolioId != null ? Number(defaultPortfolioId) : null,
      loading,
      error,
    }
  }, [portfolios, loading, error])

  return (
    <PortfolioContext.Provider value={value}>
      {children}
    </PortfolioContext.Provider>
  )
}

export function usePortfolios() {
  const ctx = useContext(PortfolioContext)
  if (!ctx) throw new Error('usePortfolios must be used within PortfolioProvider')
  return ctx
}

/** Default portfolio ID for API calls: context default or fallback 1 for backward compat. */
export function useDefaultPortfolioId() {
  const { defaultPortfolioId } = usePortfolios()
  return defaultPortfolioId ?? 1
}
