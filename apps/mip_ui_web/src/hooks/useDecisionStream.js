import { useState, useEffect, useRef, useCallback } from 'react'
import { API_BASE } from '../App'

/**
 * SSE hook for the Decision Console live feed.
 * Connects to /api/decisions/stream, receives events, heartbeats.
 * Falls back to REST polling if SSE fails.
 *
 * @param {Object} opts
 * @param {number|null} opts.portfolioId  Filter by portfolio (null = all)
 * @param {boolean}     opts.enabled      Whether streaming is active
 * @returns {{ events, heartbeat, connected, error }}
 */
export default function useDecisionStream({ portfolioId = null, enabled = true } = {}) {
  const [events, setEvents] = useState([])
  const [heartbeat, setHeartbeat] = useState(null)
  const [connected, setConnected] = useState(false)
  const [error, setError] = useState(null)

  const esRef = useRef(null)
  const fallbackRef = useRef(null)
  const lastIdRef = useRef(0)

  const addEvents = useCallback((newEvents) => {
    setEvents(prev => {
      const merged = [...newEvents, ...prev]
      const seen = new Set()
      return merged.filter(e => {
        if (seen.has(e.event_id)) return false
        seen.add(e.event_id)
        return true
      }).slice(0, 500)
    })
  }, [])

  // SSE connection
  useEffect(() => {
    if (!enabled) return

    let cancelled = false
    const params = new URLSearchParams()
    if (portfolioId != null) params.set('portfolio_id', portfolioId)

    const url = `${API_BASE}/decisions/stream?${params}`

    function connect() {
      if (cancelled) return

      const es = new EventSource(url)
      esRef.current = es

      es.addEventListener('connected', () => {
        if (!cancelled) {
          setConnected(true)
          setError(null)
        }
      })

      es.addEventListener('events', (e) => {
        if (cancelled) return
        try {
          const data = JSON.parse(e.data)
          if (data.events?.length) {
            addEvents(data.events)
            lastIdRef.current = data.last_id || lastIdRef.current
          }
        } catch { /* ignore parse errors */ }
      })

      es.addEventListener('heartbeat', (e) => {
        if (cancelled) return
        try {
          setHeartbeat(JSON.parse(e.data))
        } catch { /* ignore */ }
      })

      es.addEventListener('error', (e) => {
        if (cancelled) return
        try {
          const data = JSON.parse(e.data)
          setError(data.message)
        } catch { /* ignore */ }
      })

      es.onerror = () => {
        if (cancelled) return
        setConnected(false)
        es.close()
        // Reconnect after 5s
        setTimeout(() => {
          if (!cancelled) connect()
        }, 5000)
      }
    }

    try {
      connect()
    } catch {
      // SSE not supported, fall back to polling
      startPolling()
    }

    function startPolling() {
      fallbackRef.current = setInterval(async () => {
        if (cancelled || document.hidden) return
        try {
          const params = new URLSearchParams({ limit: '50' })
          if (lastIdRef.current) params.set('after_id', lastIdRef.current)
          if (portfolioId != null) params.set('portfolio_id', portfolioId)
          const resp = await fetch(`${API_BASE}/decisions/events?${params}`)
          if (resp.ok) {
            const data = await resp.json()
            if (data.events?.length) {
              addEvents(data.events)
              const maxId = Math.max(...data.events.map(e => e.event_id || 0))
              if (maxId > lastIdRef.current) lastIdRef.current = maxId
            }
            setConnected(true)
            setError(null)
          }
        } catch (e) {
          setError(e.message)
        }
      }, 1800000)
    }

    return () => {
      cancelled = true
      if (esRef.current) {
        esRef.current.close()
        esRef.current = null
      }
      if (fallbackRef.current) {
        clearInterval(fallbackRef.current)
        fallbackRef.current = null
      }
      setConnected(false)
    }
  }, [portfolioId, enabled, addEvents])

  return { events, heartbeat, connected, error }
}
