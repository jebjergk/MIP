import { useExplainMode } from '../context/ExplainModeContext'
import './ExplainModeToggle.css'

export default function ExplainModeToggle() {
  const { explainMode, setExplainMode } = useExplainMode()
  return (
    <label className="explain-mode-toggle">
      <input
        type="checkbox"
        checked={explainMode}
        onChange={() => setExplainMode((prev) => !prev)}
        aria-label="Explain mode: show tooltips and helper callouts"
      />
      <span className="explain-mode-label">Explain</span>
    </label>
  )
}
