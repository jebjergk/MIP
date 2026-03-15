import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import { API_BASE } from '../App'

const SymbolMetaContext = createContext({
  symbols: [],
  symbolMap: new Map(),
  loading: true,
  error: null,
  getSymbolDisplayName: () => null,
  formatSymbolLabel: (symbol) => symbol ?? '—',
})

function keyFor(symbol, marketType) {
  const s = String(symbol ?? '').toUpperCase().trim()
  const m = String(marketType ?? '').toUpperCase().trim()
  if (!s) return null
  return `${m}|${s}`
}

export function SymbolMetaProvider({ children }) {
  const [symbols, setSymbols] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch(`${API_BASE}/reference/symbols`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText || 'Failed to load symbol reference'))))
      .then((payload) => {
        if (!cancelled) setSymbols(Array.isArray(payload?.symbols) ? payload.symbols : [])
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
    const map = new Map()
    symbols.forEach((row) => {
      const symbol = row?.symbol
      const marketType = row?.market_type
      const k = keyFor(symbol, marketType)
      if (!k) return
      map.set(k, row)
    })

    const getSymbolDisplayName = (symbol, marketType) => {
      const k = keyFor(symbol, marketType)
      if (!k) return null
      const row = map.get(k)
      return row?.display_name || null
    }

    const formatSymbolLabel = (symbol, marketType) => {
      const ticker = symbol ?? '—'
      const name = getSymbolDisplayName(symbol, marketType)
      if (!name || String(name).toUpperCase() === String(ticker).toUpperCase()) {
        return ticker
      }
      return `${ticker} — ${name}`
    }

    return {
      symbols,
      symbolMap: map,
      loading,
      error,
      getSymbolDisplayName,
      formatSymbolLabel,
    }
  }, [symbols, loading, error])

  return (
    <SymbolMetaContext.Provider value={value}>
      {children}
    </SymbolMetaContext.Provider>
  )
}

export function useSymbolMeta() {
  const ctx = useContext(SymbolMetaContext)
  if (!ctx) throw new Error('useSymbolMeta must be used within SymbolMetaProvider')
  return ctx
}
