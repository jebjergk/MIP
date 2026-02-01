import { useState, useRef, useEffect } from 'react'
import { useExplainMode } from '../context/ExplainModeContext'
import { getGlossaryEntry, getGlossaryEntryByDotKey } from '../data/glossary'
import './InfoTooltip.css'

/**
 * Renders a small "?" icon that shows glossary text on hover.
 * When Explain mode is OFF, renders nothing.
 * Key convention: use key="audit.has_new_bars" (scope optional), or scope="audit" key="has_new_bars" (backward compatible).
 * @param {string} [scope] - Glossary scope (audit, portfolio, risk_gate, signals, proposals, positions, trades, ui)
 * @param {string} key - Glossary key: either dot-key "audit.has_new_bars" or plain "has_new_bars" when scope is set
 * @param {'short' | 'long'} variant - Which text to show (short = brief, long = full explanation)
 */
export default function InfoTooltip({ scope, key: glossaryKey, variant = 'short' }) {
  const { explainMode } = useExplainMode()
  const [showLong, setShowLong] = useState(false)
  const anchorRef = useRef(null)
  const popoverRef = useRef(null)

  const entry =
    glossaryKey?.includes('.')
      ? getGlossaryEntryByDotKey(glossaryKey)
      : getGlossaryEntry(scope, glossaryKey)

  useEffect(() => {
    if (!showLong || !anchorRef.current || !popoverRef.current) return
    const anchor = anchorRef.current
    const popover = popoverRef.current
    const rect = anchor.getBoundingClientRect()
    popover.style.left = `${rect.left}px`
    popover.style.top = `${rect.bottom + 4}px`
  }, [showLong])

  if (!explainMode || !entry) return null

  const displayText = variant === 'long' ? entry.long : entry.short
  const usePopover = variant === 'long' && entry.long.length > 120

  return (
    <span className="info-tooltip-wrap" ref={anchorRef}>
      <span
        className="info-tooltip-icon"
        title={usePopover ? entry.short : displayText}
        onMouseEnter={() => usePopover && setShowLong(true)}
        onMouseLeave={() => usePopover && setShowLong(false)}
        onFocus={() => usePopover && setShowLong(true)}
        onBlur={() => usePopover && setShowLong(false)}
        tabIndex={0}
        role="img"
        aria-label="More info"
      >
        ?
      </span>
      {usePopover && showLong && (
        <span
          ref={popoverRef}
          className="info-tooltip-popover"
          role="tooltip"
          onMouseEnter={() => setShowLong(true)}
          onMouseLeave={() => setShowLong(false)}
        >
          {entry.long}
        </span>
      )}
    </span>
  )
}
