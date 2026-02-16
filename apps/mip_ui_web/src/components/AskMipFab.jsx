import './AskMipFab.css'

/**
 * Floating action button (bottom-right) that toggles the Ask MIP panel.
 */
export default function AskMipFab({ open, onClick }) {
  return (
    <button
      type="button"
      className={`ask-mip-fab ${open ? 'ask-mip-fab--open' : ''}`}
      onClick={onClick}
      aria-label={open ? 'Close Ask MIP' : 'Open Ask MIP'}
      title="Ask MIP"
    >
      <span className="ask-mip-fab-icon" aria-hidden="true">
        {open ? '\u2715' : '\u2753'}
      </span>
    </button>
  )
}
