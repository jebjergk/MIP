import { useState, useMemo, useCallback } from 'react'
import { useExplainMode } from '../context/ExplainModeContext'
import { useExplainCenter } from '../context/ExplainCenterContext'
import { getGlossaryEntryByDotKey } from '../data/glossary'
import './ExplainDrawer.css'

/**
 * Simple markdown-like renderer for explain content.
 * Supports: **bold**, bullet lists (-), numbered lists (1.), line breaks
 */
function FormattedContent({ text }) {
  if (!text) return null
  
  // Split into paragraphs/blocks
  const blocks = text.split('\n\n').filter(Boolean)
  
  return (
    <div className="explain-formatted-content">
      {blocks.map((block, blockIdx) => {
        const lines = block.split('\n')
        
        // Check if this is a list block
        const isBulletList = lines.every(line => line.trim().startsWith('-') || line.trim() === '')
        const isNumberedList = lines.every(line => /^\d+\./.test(line.trim()) || line.trim() === '')
        
        if (isBulletList && lines.some(line => line.trim().startsWith('-'))) {
          return (
            <ul key={blockIdx} className="explain-list">
              {lines.filter(line => line.trim().startsWith('-')).map((line, i) => (
                <li key={i}>{formatInlineText(line.trim().slice(1).trim())}</li>
              ))}
            </ul>
          )
        }
        
        if (isNumberedList && lines.some(line => /^\d+\./.test(line.trim()))) {
          return (
            <ol key={blockIdx} className="explain-list">
              {lines.filter(line => /^\d+\./.test(line.trim())).map((line, i) => (
                <li key={i}>{formatInlineText(line.trim().replace(/^\d+\.\s*/, ''))}</li>
              ))}
            </ol>
          )
        }
        
        // Regular paragraph - may contain inline formatting
        return (
          <p key={blockIdx}>{formatInlineText(block.replace(/\n/g, ' '))}</p>
        )
      })}
    </div>
  )
}

/**
 * Format inline text with **bold** support
 */
function formatInlineText(text) {
  if (!text) return text
  
  // Split by **bold** markers
  const parts = text.split(/(\*\*[^*]+\*\*)/g)
  
  return parts.map((part, i) => {
    if (part.startsWith('**') && part.endsWith('**')) {
      return <strong key={i}>{part.slice(2, -2)}</strong>
    }
    return part
  })
}

function resolveFieldDisplay(field, glossary) {
  if (field.glossaryKey && glossary) {
    return {
      label: field.label,
      meaning: glossary.long ?? glossary.short ?? field.meaning,
      calc: field.calc ?? (glossary.calc || null),
    }
  }
  return {
    label: field.label,
    meaning: field.meaning,
    calc: field.calc ?? null,
  }
}

function contextToMarkdown(ctx, filteredFields) {
  const lines = []
  lines.push(`# ${ctx.title}\n`)
  if (ctx.what) lines.push('## What\n\n' + ctx.what + '\n')
  if (ctx.why) lines.push('## Why\n\n' + ctx.why + '\n')
  if (ctx.how) lines.push('## How\n\n' + ctx.how + '\n')
  if (ctx.sources?.length) {
    lines.push('## Data sources\n\n')
    ctx.sources.forEach((s) => lines.push(`- **${s.object}**: ${s.purpose}\n`))
    lines.push('\n')
  }
  if (filteredFields?.length) {
    lines.push('## Fields & calculations\n\n')
    filteredFields.forEach((f) => {
      lines.push(`- **${f.label}**: ${f.meaning}`)
      if (f.calc) lines.push(`  - How computed: ${f.calc}`)
      lines.push('')
    })
  }
  if (ctx.lastUpdated) lines.push('\n*Last updated: ' + ctx.lastUpdated + '*\n')
  return lines.join('\n')
}

export default function ExplainDrawer() {
  const { explainMode } = useExplainMode()
  const { isOpen, close, context } = useExplainCenter()
  const [fieldSearch, setFieldSearch] = useState('')

  const filteredFields = useMemo(() => {
    const fields = context.fields ?? []
    if (!fieldSearch.trim()) return fields
    const q = fieldSearch.trim().toLowerCase()
    return fields.filter(
      (f) =>
        (f.label || '').toLowerCase().includes(q) ||
        (f.key || '').toLowerCase().includes(q)
    )
  }, [context.fields, fieldSearch])

  const fieldsWithGlossary = useMemo(() => {
    return filteredFields.map((f) => {
      const glossary = f.glossaryKey ? getGlossaryEntryByDotKey(f.glossaryKey) : null
      const resolved = resolveFieldDisplay(f, glossary)
      return { ...f, ...resolved }
    })
  }, [filteredFields])

  const copyAsMarkdown = useCallback(() => {
    const md = contextToMarkdown(context, fieldsWithGlossary)
    navigator.clipboard?.writeText(md).catch(() => {})
  }, [context, fieldsWithGlossary])

  const copySourcesLine = useCallback(() => {
    const sources = (context.sources ?? []).map((s) => s.object).join(' → ')
    const line = sources ? `Data sources: ${sources}` : ''
    if (line) navigator.clipboard?.writeText(line).catch(() => {})
  }, [context.sources])

  if (!explainMode) return null

  return (
    <>
      <div
        className={`explain-drawer-backdrop ${isOpen ? 'explain-drawer-backdrop--open' : ''}`}
        onClick={close}
        onKeyDown={(e) => e.key === 'Escape' && close()}
        role="button"
        tabIndex={-1}
        aria-label="Close Explain drawer"
      />
      <aside
        className={`explain-drawer ${isOpen ? 'explain-drawer--open' : ''}`}
        role="dialog"
        aria-label="Explain Center"
        aria-modal="true"
      >
        <div className="explain-drawer-inner">
          <header className="explain-drawer-header">
            <h2 className="explain-drawer-title">{context.title || 'Explain'}</h2>
            <button
              type="button"
              className="explain-drawer-close"
              onClick={close}
              aria-label="Close"
            >
              ×
            </button>
          </header>

          <div className="explain-drawer-body">
            {context.what && (
              <section className="explain-drawer-section">
                <h3>What</h3>
                <FormattedContent text={context.what} />
              </section>
            )}
            {context.why && (
              <section className="explain-drawer-section">
                <h3>Why</h3>
                <FormattedContent text={context.why} />
              </section>
            )}
            {context.how && (
              <section className="explain-drawer-section">
                <h3>How</h3>
                <FormattedContent text={context.how} />
              </section>
            )}

            {(context.sources?.length ?? 0) > 0 && (
              <section className="explain-drawer-section">
                <h3>Data sources</h3>
                <p className="explain-drawer-sources-copy" title="Click to copy">
                  <button
                    type="button"
                    className="explain-drawer-sources-badge"
                    onClick={copySourcesLine}
                    title="Copy data lineage"
                  >
                    {(context.sources ?? []).map((s) => s.object).join(' → ')}
                  </button>
                </p>
                <ul className="explain-drawer-sources-list">
                  {(context.sources ?? []).map((s, i) => (
                    <li key={i}>
                      <strong>{s.object}</strong>: {s.purpose}
                    </li>
                  ))}
                </ul>
              </section>
            )}

            <section className="explain-drawer-section">
              <h3>Fields & calculations</h3>
              {(context.fields?.length ?? 0) > 0 ? (
                <>
                  <input
                    type="search"
                    className="explain-drawer-search"
                    placeholder="Filter fields…"
                    value={fieldSearch}
                    onChange={(e) => setFieldSearch(e.target.value)}
                    aria-label="Filter fields by name"
                  />
                  <ul className="explain-drawer-fields">
                    {fieldsWithGlossary.map((f, i) => (
                      <li key={f.key || i} className="explain-drawer-field">
                        <strong>{f.label}</strong>: {f.meaning}
                        {f.calc && (
                          <span className="explain-drawer-field-calc"> How computed: {f.calc}</span>
                        )}
                      </li>
                    ))}
                  </ul>
                </>
              ) : (
                <p className="explain-drawer-empty">No fields defined for this context.</p>
              )}
            </section>

            {(context.what || context.sources?.length || context.fields?.length) && (
              <div className="explain-drawer-actions">
                <button
                  type="button"
                  className="explain-drawer-copy"
                  onClick={copyAsMarkdown}
                  title="Copy full explanation as Markdown"
                >
                  Copy as Markdown
                </button>
              </div>
            )}

            {context.lastUpdated && (
              <p className="explain-drawer-updated">Last updated: {context.lastUpdated}</p>
            )}
          </div>
        </div>
      </aside>
    </>
  )
}
