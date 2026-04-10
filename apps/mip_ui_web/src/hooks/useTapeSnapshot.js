import { useEffect, useState } from 'react'
import { API_BASE } from '../config/apiBase'

const TAPE_ENABLED = import.meta.env.VITE_TAPE_OBSERVER_ENABLED === '1'

/**
 * Polls Tape observer snapshot (via mip_ui_api proxy). Server owns hysteresis.
 * Returns null when disabled or on error (non-throwing).
 */
export function useTapeSnapshot(symbol, pollMs = 3000) {
  const [snapshot, setSnapshot] = useState(null)

  useEffect(() => {
    if (!TAPE_ENABLED) {
      setSnapshot(null)
      return undefined
    }
    const sym = String(symbol || '').trim().toUpperCase()
    if (!sym) {
      setSnapshot(null)
      return undefined
    }

    let cancelled = false
    let timer = null

    const fetchOnce = async () => {
      try {
        const r = await fetch(
          `${API_BASE}/observation/tape/v1/snapshot?symbol=${encodeURIComponent(sym)}`,
        )
        if (!r.ok) {
          if (!cancelled) setSnapshot(null)
          return
        }
        const j = await r.json()
        if (!cancelled) setSnapshot(j)
      } catch {
        if (!cancelled) setSnapshot(null)
      }
    }

    fetchOnce()
    timer = setInterval(fetchOnce, pollMs)

    return () => {
      cancelled = true
      if (timer) clearInterval(timer)
    }
  }, [symbol, pollMs])

  return TAPE_ENABLED ? snapshot : null
}

export function isTapeObserverEnabled() {
  return TAPE_ENABLED
}
