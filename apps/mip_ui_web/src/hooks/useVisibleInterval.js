import { useEffect, useRef } from 'react'

/**
 * Like setInterval, but automatically pauses when the browser tab is hidden
 * and resumes (with an immediate call) when the tab becomes visible again.
 *
 * This prevents background tabs from keeping Snowflake warehouses alive
 * with periodic polling queries.
 *
 * @param {Function} callback  Function to call on each tick
 * @param {number}   delayMs   Interval in milliseconds (null/undefined = paused)
 */
export default function useVisibleInterval(callback, delayMs) {
  const savedCallback = useRef(callback)

  useEffect(() => {
    savedCallback.current = callback
  }, [callback])

  useEffect(() => {
    if (delayMs == null) return

    let id = null

    function start() {
      stop()
      id = setInterval(() => savedCallback.current(), delayMs)
    }

    function stop() {
      if (id !== null) {
        clearInterval(id)
        id = null
      }
    }

    function handleVisibility() {
      if (document.hidden) {
        stop()
      } else {
        savedCallback.current()
        start()
      }
    }

    // Initial: fire immediately, then start the interval
    savedCallback.current()
    start()

    document.addEventListener('visibilitychange', handleVisibility)

    return () => {
      stop()
      document.removeEventListener('visibilitychange', handleVisibility)
    }
  }, [delayMs])
}
