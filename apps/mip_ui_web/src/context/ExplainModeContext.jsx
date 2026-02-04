import { createContext, useContext, useState, useCallback } from 'react'

const ExplainModeContext = createContext({
  explainMode: true,
  setExplainMode: () => {},
})

export function useExplainMode() {
  const ctx = useContext(ExplainModeContext)
  if (!ctx) throw new Error('useExplainMode must be used within ExplainModeProvider')
  return ctx
}

export function ExplainModeProvider({ children, defaultOn = true }) {
  const [explainMode, setExplainModeState] = useState(defaultOn)
  const setExplainMode = useCallback((value) => {
    setExplainModeState((prev) => (typeof value === 'function' ? value(prev) : value))
  }, [])
  return (
    <ExplainModeContext.Provider value={{ explainMode: !!explainMode, setExplainMode }}>
      {children}
    </ExplainModeContext.Provider>
  )
}
