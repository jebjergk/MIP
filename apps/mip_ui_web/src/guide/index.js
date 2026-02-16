/**
 * Guide section index — maps section IDs to metadata and raw markdown.
 *
 * Each section has:
 *   id        – unique slug (used in URLs and page-route mapping)
 *   number    – display number
 *   title     – human-readable title
 *   part      – 1 = "How MIP Works", 2 = "Page-by-Page Guide", 3 = "Quick Reference"
 *   markdown  – raw markdown string (imported at build time)
 *   route     – the React Router path this section documents (null for conceptual sections)
 */

import bigPicture from './01-big-picture.md?raw'
import dailyPipeline from './02-daily-pipeline.md?raw'
import signals from './03-signals.md?raw'
import outcomes from './04-outcomes.md?raw'
import trainingStages from './05-training-stages.md?raw'
import trust from './06-trust.md?raw'
import hitRate from './07-hit-rate.md?raw'
import avgReturn from './08-avg-return.md?raw'
import trading from './09-trading.md?raw'
import patterns from './10-patterns.md?raw'
import home from './11-home.md?raw'
import cockpit from './12-cockpit.md?raw'
import portfolio from './13-portfolio.md?raw'
import portfolioManagement from './14-portfolio-management.md?raw'
import trainingStatus from './15-training-status.md?raw'
import suggestions from './16-suggestions.md?raw'
import signalsExplorer from './17-signals.md?raw'
import marketTimeline from './18-market-timeline.md?raw'
import runs from './19-runs.md?raw'
import debug from './20-debug.md?raw'
import parallelWorlds from './21-parallel-worlds.md?raw'
import glossary from './22-glossary.md?raw'

const sections = [
  { id: 'big-picture',      number: 1,  title: 'The Big Picture',              part: 1, markdown: bigPicture,          route: null },
  { id: 'daily-pipeline',   number: 2,  title: 'The Daily Pipeline',           part: 1, markdown: dailyPipeline,       route: null },
  { id: 'signals-concept',  number: 3,  title: 'How Signals Are Generated',    part: 1, markdown: signals,             route: null },
  { id: 'outcomes',         number: 4,  title: 'Outcome Evaluation',           part: 1, markdown: outcomes,            route: null },
  { id: 'training-stages',  number: 5,  title: 'Training Stages',              part: 1, markdown: trainingStages,      route: null },
  { id: 'trust',            number: 6,  title: 'Trust & Eligibility',          part: 1, markdown: trust,               route: null },
  { id: 'hit-rate',         number: 7,  title: 'What Is Hit Rate?',            part: 1, markdown: hitRate,             route: null },
  { id: 'avg-return',       number: 8,  title: 'What Is Avg Return?',          part: 1, markdown: avgReturn,           route: null },
  { id: 'trading',          number: 9,  title: 'From Trust to Trading',        part: 1, markdown: trading,             route: null },
  { id: 'patterns',         number: 10, title: 'What Are Patterns?',           part: 1, markdown: patterns,            route: null },
  { id: 'page-home',        number: 11, title: 'Home',                         part: 2, markdown: home,                route: '/' },
  { id: 'page-cockpit',     number: 12, title: 'Cockpit (Daily Dashboard)',    part: 2, markdown: cockpit,             route: '/cockpit' },
  { id: 'page-portfolio',   number: 13, title: 'Portfolio',                    part: 2, markdown: portfolio,            route: '/portfolios' },
  { id: 'page-manage',      number: 14, title: 'Portfolio Management',         part: 2, markdown: portfolioManagement, route: '/manage' },
  { id: 'page-training',    number: 15, title: 'Training Status',              part: 2, markdown: trainingStatus,      route: '/training' },
  { id: 'page-suggestions', number: 16, title: 'Suggestions',                  part: 2, markdown: suggestions,         route: '/suggestions' },
  { id: 'page-signals',     number: 17, title: 'Signals Explorer',             part: 2, markdown: signalsExplorer,     route: '/signals' },
  { id: 'page-timeline',    number: 18, title: 'Market Timeline',              part: 2, markdown: marketTimeline,      route: '/market-timeline' },
  { id: 'page-runs',        number: 19, title: 'Runs (Audit Viewer)',          part: 2, markdown: runs,                route: '/runs' },
  { id: 'page-debug',       number: 20, title: 'Debug',                        part: 2, markdown: debug,               route: '/debug' },
  { id: 'page-parallel',    number: 21, title: 'Parallel Worlds',              part: 2, markdown: parallelWorlds,      route: '/parallel-worlds' },
  { id: 'glossary',         number: 22, title: 'Key Terms Glossary',           part: 3, markdown: glossary,            route: null },
]

export default sections

/**
 * Find the best matching guide section for a given route path.
 * Returns the section object or null.
 */
export function sectionForRoute(pathname) {
  if (!pathname) return null
  // Exact match first
  const exact = sections.find(s => s.route === pathname)
  if (exact) return exact
  // Prefix match (e.g., /portfolios/3 → /portfolios)
  const prefix = sections
    .filter(s => s.route && pathname.startsWith(s.route))
    .sort((a, b) => b.route.length - a.route.length)
  return prefix[0] || null
}

/**
 * Get all guide markdown concatenated (for system prompt).
 */
export function allGuideMarkdown() {
  return sections.map(s => s.markdown).join('\n\n---\n\n')
}
