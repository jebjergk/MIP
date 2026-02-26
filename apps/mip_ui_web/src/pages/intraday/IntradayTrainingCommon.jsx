import { Link } from 'react-router-dom'

export function HelpTip({ text }) {
  return (
    <span className="it-help-tip" title={text} aria-label={text}>
      ?
    </span>
  )
}

export function EvidenceBadge({ fallbackLevel, evidenceN = 0, minEvidence = 20 }) {
  const insufficient = fallbackLevel === 'GLOBAL' || Number(evidenceN) < minEvidence
  if (!insufficient) return <span className="it-badge it-badge--good">Evidence OK</span>
  return <span className="it-badge it-badge--warn">INSUFFICIENT EVIDENCE</span>
}

export function IntradayHeader({ title, subtitle }) {
  return (
    <div className="it-header">
      <h1>{title}</h1>
      <p>{subtitle}</p>
      <div className="it-nav">
        <Link to="/intraday/dashboard">Dashboard</Link>
        <Link to="/intraday/pattern/501">Pattern Detail</Link>
        <Link to="/intraday/terrain">Terrain Explorer</Link>
        <Link to="/intraday/health">Pipeline Health</Link>
      </div>
    </div>
  )
}

export function fmtNum(value, digits = 0) {
  if (value == null || Number.isNaN(Number(value))) return '—'
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

export function fmtPct(value, digits = 1) {
  if (value == null || Number.isNaN(Number(value))) return '—'
  return `${(Number(value) * 100).toFixed(digits)}%`
}
