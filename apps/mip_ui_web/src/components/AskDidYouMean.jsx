export default function AskDidYouMean({ terms = [], onPick = null }) {
  if (!terms?.length) return null
  return (
    <div className="ask-mip-suggestions">
      <div className="ask-mip-suggestions-title">Did you mean:</div>
      <div className="ask-mip-suggestions-list">
        {terms.slice(0, 5).map((term) => (
          <button
            type="button"
            key={term}
            className="ask-mip-suggestion-chip"
            onClick={() => onPick?.(term)}
          >
            {term}
          </button>
        ))}
      </div>
    </div>
  )
}
