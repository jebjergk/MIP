import { Link } from 'react-router-dom'

export default function Home() {
  return (
    <>
      <h1>MIP UI</h1>
      <p>Read-only view of pipeline runs, portfolios, morning briefs, and training status.</p>
      <ul>
        <li><Link to="/audit">Audit Viewer</Link> — recent runs and run timeline + interpreted summary</li>
        <li><Link to="/portfolios">Portfolios</Link> — list and detail (header, snapshot)</li>
        <li><Link to="/brief">Morning Brief</Link> — latest brief per portfolio</li>
        <li><Link to="/training">Training Status</Link> — training status (first draft)</li>
        <li><Link to="/suggestions">Suggestions</Link> — ranked symbol/pattern recommendations</li>
        <li><Link to="/debug">Debug</Link> — route smoke checks (API health)</li>
      </ul>
    </>
  )
}
