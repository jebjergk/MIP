export default function AskProvenanceBadges({ sources = [] }) {
  const types = Array.from(new Set((sources || []).map((s) => s?.source_type).filter(Boolean)))
  if (!types.length) return null

  const map = {
    DOC: 'Based on MIP docs',
    GLOSSARY: 'Based on MIP glossary',
    WEB: 'Includes external sources',
    INFERENCE: 'Includes inference',
  }

  return (
    <div className="ask-mip-badges">
      {types.map((type) => (
        <span key={type} className={`ask-mip-badge ask-mip-badge--${type.toLowerCase()}`}>
          {map[type] || type}
        </span>
      ))}
    </div>
  )
}
