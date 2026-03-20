import AskProvenanceBadges from './AskProvenanceBadges'

export default function AskAnswerSections({ sources = [], confidence = null }) {
  return (
    <div className="ask-mip-sections">
      <AskProvenanceBadges sources={sources} />
      {confidence && confidence.overall > 0 && (
        <div className="ask-mip-confidence">
          Confidence: {Math.round((confidence.overall || 0) * 100)}%
        </div>
      )}
    </div>
  )
}
