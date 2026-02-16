import { useState, useMemo } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import sections from '../guide/index'
import './UserGuide.css'

const PART_LABELS = {
  1: 'Part 1 — How MIP Works',
  2: 'Part 2 — Page-by-Page Guide',
  3: 'Quick Reference',
}

export default function UserGuide() {
  const [activeId, setActiveId] = useState(null)

  const part1 = useMemo(() => sections.filter(s => s.part === 1), [])
  const part2 = useMemo(() => sections.filter(s => s.part === 2), [])
  const part3 = useMemo(() => sections.filter(s => s.part === 3), [])

  const activeSection = activeId ? sections.find(s => s.id === activeId) : null

  return (
    <div className="user-guide">
      <h1>MIP User Guide</h1>
      <p className="guide-subtitle">
        Everything you need to know about the Market Intelligence Platform — explained
        for humans, with examples and illustrations.
      </p>

      {/* ── Table of Contents ── */}
      <div className="guide-toc">
        <h3>Contents</h3>
        <div className="guide-toc-columns">
          <div className="guide-toc-col">
            <h4>{PART_LABELS[1]}</h4>
            <ol>
              {part1.map(s => (
                <li key={s.id}>
                  <a href={`#${s.id}`} onClick={() => setActiveId(null)}>
                    {s.title}
                  </a>
                </li>
              ))}
            </ol>
          </div>
          <div className="guide-toc-col">
            <h4>{PART_LABELS[2]}</h4>
            <ol start={11}>
              {part2.map(s => (
                <li key={s.id}>
                  <a href={`#${s.id}`} onClick={() => setActiveId(null)}>
                    {s.title}
                  </a>
                </li>
              ))}
            </ol>
          </div>
        </div>
      </div>

      {/* ── Sections ── */}
      {[
        { label: PART_LABELS[1], items: part1 },
        { label: PART_LABELS[2], items: part2 },
        { label: PART_LABELS[3], items: part3 },
      ].map(group => (
        <div key={group.label}>
          <div className="guide-part-header">{group.label}</div>
          {group.items.map(s => (
            <section key={s.id} className="guide-section" id={s.id}>
              <div className="guide-markdown-content">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                  {s.markdown}
                </ReactMarkdown>
              </div>
            </section>
          ))}
        </div>
      ))}
    </div>
  )
}
