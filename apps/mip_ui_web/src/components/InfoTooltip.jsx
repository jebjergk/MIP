import { useState, useRef, useEffect } from 'react'
import { getGlossaryEntry, getGlossaryEntryByDotKey } from '../data/glossary'
import './InfoTooltip.css'

/**
 * Renders a small "?" icon that shows glossary text on hover.
 * Always visible — no Explain Mode dependency.
 * @param {string} [scope] - Glossary scope
 * @param {string} [key] - Glossary key (React reserves this; use entryKey if not passed)
 * @param {string} [entryKey] - Glossary key: either dot-key "audit.has_new_bars" or plain "has_new_bars" when scope is set
 * @param {'short' | 'long'} variant - Which text to show (short = brief, long = full explanation)
 */
export default function InfoTooltip({ scope, key: glossaryKey, entryKey, variant = 'short' }) {
  const keyToUse = glossaryKey ?? entryKey
  const [showLong, setShowLong] = useState(false)
  const anchorRef = useRef(null)
  const popoverRef = useRef(null)

  const entry =
    keyToUse?.includes('.')
      ? getGlossaryEntryByDotKey(keyToUse)
      : getGlossaryEntry(scope, keyToUse)

  useEffect(() => {
    if (!showLong || !anchorRef.current || !popoverRef.current) return
    const anchor = anchorRef.current
    const popover = popoverRef.current
    const rect = anchor.getBoundingClientRect()
    popover.style.left = `${rect.left}px`
    popover.style.top = `${rect.bottom + 4}px`
  }, [showLong])

  if (!entry) return null

  const hasStructured = entry.what || entry.why || entry.how
  const displayText = variant === 'long' ? entry.long : entry.short
  const usePopover = variant === 'long' && (entry.long.length > 120 || hasStructured)

  const popoverContent = usePopover && showLong && (hasStructured ? (
    <span ref={popoverRef} className="info-tooltip-popover info-tooltip-popover--structured" role="tooltip" onMouseEnter={() => setShowLong(true)} onMouseLeave={() => setShowLong(false)}>
      {entry.what && <><strong>What:</strong> {entry.what}<br /></>}
      {entry.why && <><strong>Why:</strong> {entry.why}<br /></>}
      {entry.how && <><strong>How:</strong> {entry.how}<br /></>}
      {entry.next && <><strong>Next:</strong> {entry.next}</>}
    </span>
  ) : (
    <span ref={popoverRef} className="info-tooltip-popover" role="tooltip" onMouseEnter={() => setShowLong(true)} onMouseLeave={() => setShowLong(false)}>
      {entry.long}
    </span>
  ))

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
      {usePopover && showLong && popoverContent}
    </span>
  )
}
