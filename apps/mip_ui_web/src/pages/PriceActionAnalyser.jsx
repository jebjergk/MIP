import { useState } from 'react'
import PriceActionChart from '../components/price-action/PriceActionChart'
import PriceActionRead from '../components/price-action/PriceActionRead'
import { normalizeAnalysisResponse } from '../components/price-action/priceActionModel'
import './PriceActionAnalyser.css'

const LOOKBACKS = [90, 120, 180, 250]

export default function PriceActionAnalyser() {
  const [symbol, setSymbol] = useState('')
  const [lookback, setLookback] = useState(120)
  const [analysis, setAnalysis] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const analyse = async (event) => {
    event.preventDefault()
    const cleanSymbol = symbol.trim().toUpperCase()
    if (!cleanSymbol) {
      setError('Enter a symbol to analyse.')
      return
    }

    setLoading(true)
    setError('')
    try {
      const response = await fetch('/api/price-action-analyser/analyse', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ symbol: cleanSymbol, lookback_bars: lookback, side: 'LONG' }),
      })
      const payload = await response.json().catch(() => ({}))
      if (!response.ok) {
        const message = payload?.detail || payload?.message || `Analysis failed (${response.status})`
        throw new Error(typeof message === 'string' ? message : JSON.stringify(message))
      }
      setAnalysis(normalizeAnalysisResponse(payload))
      setSymbol(cleanSymbol)
    } catch (requestError) {
      setAnalysis(null)
      setError(requestError?.message || 'Unable to run the analysis.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="paa-page">
      <header className="paa-page-header">
        <div>
          <span className="paa-kicker">Standalone research tool</span>
          <h1>Price Action Analyser</h1>
          <p>
            Request an expert, long-only read of one symbol. Analysis runs only when you press
            <strong> Analyse</strong>; this page does not scan or refresh in the background.
          </p>
        </div>
        <span className="paa-research-label">Research only · no execution</span>
      </header>

      <form className="paa-controls" onSubmit={analyse}>
        <label className="paa-symbol-field">
          <span>Symbol</span>
          <input
            value={symbol}
            onChange={(event) => setSymbol(event.target.value.toUpperCase())}
            placeholder="e.g. AAPL"
            maxLength={16}
            autoComplete="off"
            spellCheck="false"
            disabled={loading}
          />
        </label>
        <fieldset>
          <legend>Lookback</legend>
          <div className="paa-lookbacks">
            {LOOKBACKS.map((days) => (
              <button
                type="button"
                key={days}
                className={lookback === days ? 'paa-lookback--active' : ''}
                onClick={() => setLookback(days)}
                disabled={loading}
              >
                {days}d
              </button>
            ))}
          </div>
        </fieldset>
        <div className="paa-fixed-side">
          <span>Side</span>
          <strong>LONG</strong>
        </div>
        <button className="paa-analyse-button" type="submit" disabled={loading}>
          {loading ? <span className="paa-spinner" aria-hidden="true" /> : null}
          {loading ? 'Analysing…' : 'Analyse'}
        </button>
      </form>

      {error && <div className="paa-error" role="alert">{error}</div>}

      {!analysis && !loading && !error && (
        <section className="paa-awaiting">
          <div className="paa-awaiting-icon" aria-hidden="true">⌁</div>
          <h2>Ready for a manual analysis</h2>
          <p>Choose a lookback, enter a symbol, and press Analyse. No request has been made yet.</p>
        </section>
      )}

      {loading && (
        <section className="paa-awaiting" aria-live="polite">
          <div className="paa-loading-mark" aria-hidden="true" />
          <h2>Reading {symbol.trim().toUpperCase()}</h2>
          <p>Detecting geometry and preparing the Price Action Methodologist interpretation.</p>
        </section>
      )}

      {analysis && !loading && (
        <div className="paa-results">
          <PriceActionRead analysis={analysis} />
          <PriceActionChart analysis={analysis} />
        </div>
      )}
    </div>
  )
}
