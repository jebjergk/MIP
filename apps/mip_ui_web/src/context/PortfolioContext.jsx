import { createContext, useContext, useState, useEffect, useMemo, useCallback } from 'react'
import { API_BASE } from '../config/apiBase'

// ---------------------------------------------------------------------------
// Normalise a single portfolio config row, handling both Snowflake uppercase
// and API-style lowercase field names.
// ---------------------------------------------------------------------------
function normaliseConfig(raw) {
  if (!raw) return raw
  return {
    portfolio_id:               raw.PORTFOLIO_ID            ?? raw.portfolio_id            ?? null,
    name:                       raw.NAME                    ?? raw.name                    ?? null,
    ibkr_account_id:            raw.IBKR_ACCOUNT_ID         ?? raw.ibkr_account_id         ?? null,
    ibkr_account_mode:          raw.IBKR_ACCOUNT_MODE       ?? raw.ibkr_account_mode       ?? 'UNKNOWN',
    broker_name:                raw.BROKER_NAME             ?? raw.broker_name             ?? null,
    broker_universe_type:       raw.BROKER_UNIVERSE_TYPE    ?? raw.broker_universe_type    ?? null,
    is_active:                  raw.IS_ACTIVE               ?? raw.is_active               ?? null,
    is_execution_enabled:       raw.IS_EXECUTION_ENABLED    ?? raw.is_execution_enabled    ?? false,
    real_money_enabled:         raw.REAL_MONEY_ENABLED      ?? raw.real_money_enabled      ?? false,
    adapter_mode:               raw.ADAPTER_MODE            ?? raw.adapter_mode            ?? null,
    drift_status:               raw.DRIFT_STATUS            ?? raw.drift_status            ?? null,
    snapshot_freshness_threshold_sec:
                                raw.SNAPSHOT_FRESHNESS_THRESHOLD_SEC
                                                            ?? raw.snapshot_freshness_threshold_sec
                                                            ?? null,
    max_positions:              raw.MAX_POSITIONS           ?? raw.max_positions           ?? null,
    max_position_pct:           raw.MAX_POSITION_PCT        ?? raw.max_position_pct        ?? null,
    bust_pct:                   raw.BUST_PCT                ?? raw.bust_pct                ?? null,
    starting_cash:              raw.STARTING_CASH           ?? raw.starting_cash           ?? null,
    // Keep the original for any consumers that still use upper/lower keys directly.
    _raw: raw,
  }
}

const SESSION_KEY = 'mip_selected_portfolio_id'

const PortfolioContext = createContext(null)

export function PortfolioProvider({ children }) {
  const [portfolios, setPortfolios] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedPortfolioId, _setSelectedPortfolioId] = useState(null)
  const [_initialised, setInitialised] = useState(false)

  const setSelectedPortfolioId = useCallback((id) => {
    const numId = id != null ? Number(id) : null
    _setSelectedPortfolioId(numId)
    if (numId != null) {
      try { sessionStorage.setItem(SESSION_KEY, String(numId)) } catch (_) {}
    } else {
      try { sessionStorage.removeItem(SESSION_KEY) } catch (_) {}
    }
  }, [])

  const refreshPortfolios = useCallback(() => {
    setLoading(true)
    setError(null)
    fetch(`${API_BASE}/live/portfolio-config`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((data) => {
        // API returns { configs: [...] } — handle both that shape and a plain array.
        const raw = Array.isArray(data)
          ? data
          : Array.isArray(data?.configs)
            ? data.configs
            : []
        if (raw.length === 0) {
          console.warn('[PortfolioContext] No portfolio configs returned from API.')
        }
        setPortfolios(raw.map(normaliseConfig))
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  // Initial fetch
  useEffect(() => {
    let cancelled = false
    fetch(`${API_BASE}/live/portfolio-config`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((data) => {
        if (cancelled) return
        const raw = Array.isArray(data)
          ? data
          : Array.isArray(data?.configs)
            ? data.configs
            : []
        if (raw.length === 0) {
          console.warn('[PortfolioContext] No portfolio configs returned from API.')
        }
        const configs = raw.map(normaliseConfig)
        setPortfolios(configs)

        // Resolve initial selection: sessionStorage → first IS_ACTIVE → first row → null
        let resolvedId = null
        try {
          const stored = sessionStorage.getItem(SESSION_KEY)
          if (stored) {
            const storedNum = Number(stored)
            if (configs.some((c) => c.portfolio_id === storedNum)) {
              resolvedId = storedNum
            }
          }
        } catch (_) {}

        if (resolvedId == null) {
          const firstActive = configs.find((c) => c.is_active !== false) ?? configs[0] ?? null
          resolvedId = firstActive ? firstActive.portfolio_id : null
        }

        _setSelectedPortfolioId(resolvedId != null ? Number(resolvedId) : null)
        setInitialised(true)
      })
      .catch((e) => {
        if (!cancelled) {
          setError(e.message)
          setInitialised(true)
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const value = useMemo(() => {
    const selectedPortfolio = portfolios.find((p) => p.portfolio_id === selectedPortfolioId) ?? null

    // Legacy alias: defaultPortfolioId kept for backward compat with existing consumers.
    const defaultPortfolioId = selectedPortfolioId

    return {
      portfolios,
      defaultPortfolioId,
      selectedPortfolioId,
      setSelectedPortfolioId,
      selectedPortfolio,
      refreshPortfolios,
      loading,
      error,
    }
  }, [portfolios, selectedPortfolioId, setSelectedPortfolioId, refreshPortfolios, loading, error])

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

/** Primary hook for LPA and other portfolio-aware pages. */
export function usePortfolio() {
  const ctx = useContext(PortfolioContext)
  if (!ctx) throw new Error('usePortfolio must be used within PortfolioProvider')
  return ctx
}

/** Default live portfolio ID for API calls, or null when unavailable. */
export function useDefaultPortfolioId() {
  const { defaultPortfolioId } = usePortfolios()
  return defaultPortfolioId
}
