import { createContext, useContext, useState, useCallback } from 'react'

/**
 * Standard shape for Explain Center context (page or section).
 * sources.object must only reference canonical Snowflake objects (see docs/ux/74_CANONICAL_OBJECTS.md).
 * fields[].glossaryKey optional: scope.key to resolve label/meaning/calc from glossary.
 */
export const DEFAULT_EXPLAIN_CONTEXT = {
  id: 'default',
  title: 'Explain',
  what: '',
  why: '',
  how: '',
  sources: [],
  fields: [],
  links: [],
  lastUpdated: undefined,
}

/**
 * @typedef {Object} ExplainSource
 * @property {string} object - Canonical Snowflake object (e.g. MIP.APP.RECOMMENDATION_LOG)
 * @property {string} purpose - Plain-language purpose
 *
 * @typedef {Object} ExplainField
 * @property {string} key - Field key
 * @property {string} label - Display label
 * @property {string} meaning - Plain-language meaning
 * @property {string} [calc] - How computed (optional)
 * @property {string} [glossaryKey] - Optional scope.key to resolve from glossary
 *
 * @typedef {Object} ExplainLink
 * @property {string} label
 * @property {string} path
 *
 * @typedef {Object} ExplainContext
 * @property {string} id
 * @property {string} title
 * @property {string} what
 * @property {string} why
 * @property {string} how
 * @property {ExplainSource[]} sources
 * @property {ExplainField[]} fields
 * @property {ExplainLink[]} [links]
 * @property {string} [lastUpdated]
 */

const ExplainCenterContext = createContext({
  isOpen: false,
  open: () => {},
  close: () => {},
  context: DEFAULT_EXPLAIN_CONTEXT,
  setContext: () => {},
})

export function useExplainCenter() {
  const ctx = useContext(ExplainCenterContext)
  if (!ctx) throw new Error('useExplainCenter must be used within ExplainCenterProvider')
  return ctx
}

/**
 * Call with a section-level context (card, table, Evidence Drawer).
 * Returns a function that sets that context and opens the Explain drawer.
 * @param {ExplainContext | null} sectionContext - Section context (or null to not open)
 * @returns {() => void} Call to set this section context and open the drawer
 */
export function useExplainSection(sectionContext) {
  const { setContext, open } = useExplainCenter()
  return useCallback(() => {
    if (sectionContext) {
      setContext(sectionContext)
      open()
    }
  }, [sectionContext, setContext, open])
}

export function ExplainCenterProvider({ children }) {
  const [isOpen, setIsOpen] = useState(false)
  const [context, setContextState] = useState(DEFAULT_EXPLAIN_CONTEXT)

  const open = useCallback(() => setIsOpen(true), [])
  const close = useCallback(() => setIsOpen(false), [])

  const setContext = useCallback((next) => {
    setContextState((prev) => {
      const merged = typeof next === 'function' ? next(prev) : next
      return { ...DEFAULT_EXPLAIN_CONTEXT, ...prev, ...merged }
    })
  }, [])

  return (
    <ExplainCenterContext.Provider
      value={{
        isOpen,
        open,
        close,
        context,
        setContext,
      }}
    >
      {children}
    </ExplainCenterContext.Provider>
  )
}
