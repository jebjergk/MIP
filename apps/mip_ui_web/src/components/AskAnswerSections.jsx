import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import AskProvenanceBadges from './AskProvenanceBadges'

export default function AskAnswerSections({ sections = [], sources = [], confidence = null }) {
  if (!sections?.length) {
    return (
      <div className="ask-mip-sections">
        <AskProvenanceBadges sources={sources} />
      </div>
    )
  }

  return (
    <div className="ask-mip-sections">
      <AskProvenanceBadges sources={sources} />
      {confidence && (
        <div className="ask-mip-confidence">
          Confidence: {Math.round((confidence.overall || 0) * 100)}%
        </div>
      )}
      {sections.map((section, idx) => (
        <div key={`${section.section_type}-${idx}`} className="ask-mip-section">
          <div className="ask-mip-section-title">{section.title}</div>
          <div className="ask-mip-section-body">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{section.text || ''}</ReactMarkdown>
          </div>
        </div>
      ))}
    </div>
  )
}
