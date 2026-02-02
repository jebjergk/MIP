import { Link } from 'react-router-dom'
import EmptyState from '../components/EmptyState'

export default function Home() {
  return (
    <>
      <h1>MIP UI</h1>
      <p>Read-only view of pipeline runs, portfolios, morning briefs, and training status.</p>
      <ul>
        <li><Link to="/runs">Runs</Link> — recent pipeline runs and run detail (timeline + narrative)</li>
        <li><Link to="/portfolios">Portfolios</Link> — list and detail (header, snapshot)</li>
        <li><Link to="/brief">Morning Brief</Link> — latest brief per portfolio</li>
        <li><Link to="/training">Training Status</Link> — training status (first draft)</li>
        <li><Link to="/suggestions">Suggestions</Link> — ranked symbol/pattern recommendations</li>
        <li><Link to="/debug">Debug</Link> — route smoke checks (API health)</li>
      </ul>
      <EmptyState
        title="Seeing empty pages?"
        action={<>Run the daily pipeline in Snowflake, then check <Link to="/runs">Runs</Link>.</>}
        explanation="MIP data is populated by the daily pipeline. If pages look empty, run SP_RUN_DAILY_PIPELINE in Snowflake."
        reasons={['Pipeline has not run yet.', 'Snowflake credentials may be missing or invalid.']}
      />
    </>
  )
}
