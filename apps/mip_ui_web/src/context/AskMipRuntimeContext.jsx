import { createContext, useCallback, useContext, useMemo, useState } from 'react'

const AskMipRuntimeContext = createContext({
  runtime: {},
  setAskMipRuntime: () => {},
  mergeAskMipRuntime: () => {},
})

/**
 * Holds optional Ask MIP 2.0 runtime payload fields; pages merge context-specific slices.
 */
export function AskMipRuntimeProvider({ children }) {
  const [runtime, setRuntime] = useState({})

  const setAskMipRuntime = useCallback((next) => {
    setRuntime(next && typeof next === 'object' ? { ...next } : {})
  }, [])

  const mergeAskMipRuntime = useCallback((patch) => {
    if (!patch || typeof patch !== 'object') return
    setRuntime((prev) => ({ ...prev, ...patch }))
  }, [])

  const value = useMemo(
    () => ({ runtime, setAskMipRuntime, mergeAskMipRuntime }),
    [runtime, setAskMipRuntime, mergeAskMipRuntime],
  )

  return <AskMipRuntimeContext.Provider value={value}>{children}</AskMipRuntimeContext.Provider>
}

export function useAskMipRuntime() {
  return useContext(AskMipRuntimeContext)
}
