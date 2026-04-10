import { useEffect, useState } from 'react'
import { API_BASE } from '../config/apiBase'

/** When set, the client may poll the tape snapshot API. Does not imply tape UI is shown — use snapshot.tape_active_for_ui. */
const TAPE_API_ENABLED = import.meta.env.VITE_TAPE_OBSERVER_ENABLED === '1'

let _tapeProxyWarned = false

function warnTapeProxyOnce(status, detail) {
  if (!import.meta.env.DEV) return
  if (_tapeProxyWarned || typeof console === 'undefined' || !console.warn) return
  _tapeProxyWarned = true
  if (status === 503) {
    console.warn(
      '[tape] mip_ui_api returned 503 — set TAPE_OBSERVER_BASE_URL on the API. Tape UI still requires tape_active_for_ui from the observer.',
    )
    return
  }
  if (status === 502) {
    console.warn('[tape] Could not reach the observer — check TAPE_OBSERVER_BASE_URL and port 8095.')
    return
  }
  console.warn('[tape] snapshot request failed:', status, detail || '')
}

/**
 * Polls tape snapshot (via mip_ui_api). Living Chart tape chrome uses only `tape_active_for_ui` from the payload.
 */
export function useTapeSnapshot(symbol, pollMs = 3000) {
  const [snapshot, setSnapshot] = useState(null)

  useEffect(() => {
    if (!TAPE_API_ENABLED) {
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

  return TAPE_API_ENABLED ? snapshot : null
}

/** True when VITE allows calling the tape API (not “show tape UI”). */
export function isTapeApiEnabled() {
  return TAPE_API_ENABLED
}

/** @deprecated use isTapeApiEnabled */
export function isTapeObserverEnabled() {
  return TAPE_API_ENABLED
}
