import { useEffect, useState } from 'react'
import { API_BASE } from '../config/apiBase'

const TAPE_ENABLED = import.meta.env.VITE_TAPE_OBSERVER_ENABLED === '1'

let _tapeProxyWarned = false

function warnTapeProxyOnce(status, detail) {
  if (_tapeProxyWarned || typeof console === 'undefined' || !console.warn) return
  _tapeProxyWarned = true
  if (status === 503) {
    console.warn(
      '[tape] mip_ui_api returned 503 — set TAPE_OBSERVER_BASE_URL (e.g. http://127.0.0.1:8095) on the API and restart it. See MIP/apps/mip_ui_api/.env.example',
    )
    return
  }
  if (status === 502) {
    console.warn(
      '[tape] mip_ui_api could not reach the observer — start tape-observer (port 8095) and check TAPE_OBSERVER_BASE_URL.',
    )
    return
  }
  console.warn('[tape] snapshot request failed:', status, detail || '')
}

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
          let detail = ''
          try {
            const errBody = await r.json()
            detail = errBody?.detail || ''
          } catch {
            /* ignore */
          }
          warnTapeProxyOnce(r.status, detail)
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
