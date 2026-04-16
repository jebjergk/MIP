import React from 'react'
import InfoTooltip from '../InfoTooltip'

const SCOPE = 'structural_training'

const FAMILY_DESCRIPTIONS = {
  BREAKOUT_RETEST_LONG: 'Price broke above resistance, pulled back, and is attempting to hold prior resistance as support. A classic continuation entry.',
  SUPPORT_WICK_LONG: 'Price tested support and was rejected with a strong lower wick, suggesting buyers are defending the level.',
  THREE_BAR_REVERSAL_LONG: 'A three-bar directional change near support — the market pivoted from selling to buying with structural confirmation.',
  TREND_PULLBACK_LONG: 'Within an established uptrend, price pulled back toward the moving average or a key level, creating a lower-risk trend continuation entry.',
  BREAKDOWN_RETEST_SHORT: 'Price broke below support, bounced to retest it from below, and is now failing to reclaim it. A classic breakdown short.',
  RESISTANCE_WICK_SHORT: 'Price tested resistance and was rejected with a strong upper wick, suggesting sellers are defending the level.',
  THREE_BAR_REVERSAL_SHORT: 'A three-bar directional change near resistance — the market pivoted from buying to selling with structural confirmation.',
  FAILED_BREAKOUT_SHORT: 'Price attempted to break above resistance but failed and closed back below. Trapped longs above resistance create selling pressure.',
}

export default function StiIdentityCard({ row, get }) {
  const family = get(row, 'SETUP_FAMILY') || ''
  const desc = FAMILY_DESCRIPTIONS[family] || 'Structural setup family.'

  return (
    <div className="sti-module">
      <div className="sti-module-title">
        Setup Identity
        <InfoTooltip scope={SCOPE} entryKey="setup_family" variant="short" />
      </div>
      <div className="sti-identity">
        <div className="sti-identity-main">
          <div style={{ fontSize: '1.1rem', fontWeight: 700, marginBottom: '0.25rem' }}>
            {formatFamily(family)}
          </div>
          <p className="sti-identity-desc">{desc}</p>
        </div>
        <div className="sti-identity-chips">
          <span className={`sti-badge sti-dir-${(get(row, 'DIRECTION') || '').toLowerCase()}`}>{get(row, 'DIRECTION')}</span>
          <span className={`sti-badge sti-trust-pill ${trustClass(get(row, 'TRUST_LABEL'))}`}>{get(row, 'TRUST_LABEL')}</span>
          <span className="sti-badge" style={{ background: '#e9ecef', color: '#495057' }}>{get(row, 'MARKET_TYPE')}</span>
          <span className="sti-badge" style={{ background: '#e9ecef', color: '#495057' }}>Window: {get(row, 'BEST_WINDOW')}</span>
          {get(row, 'DOMINANT_RISK_CLASS') && <span className={`sti-badge sti-risk-${(get(row, 'DOMINANT_RISK_CLASS') || '').toLowerCase()}`}>Risk: {get(row, 'DOMINANT_RISK_CLASS')}</span>}
          {get(row, 'TRAIL_STRATEGY_RECOMMENDATION') && <span className="sti-badge" style={{ background: '#e3f2fd', color: '#0a5276' }}>Trail: {get(row, 'TRAIL_STRATEGY_RECOMMENDATION')}</span>}
          {get(row, 'EXIT_STYLE_RECOMMENDATION') && <span className="sti-badge" style={{ background: '#e3f2fd', color: '#0a5276' }}>Exit: {get(row, 'EXIT_STYLE_RECOMMENDATION')}</span>}
          {get(row, 'DOMINANT_STATE') && <span className="sti-badge" style={{ background: '#f5e6c8', color: '#7d5a00' }}>State: {get(row, 'DOMINANT_STATE')}</span>}
        </div>
      </div>
    </div>
  )
}

function formatFamily(f) {
  if (!f) return '—'
  return f.replace(/_/g, ' ')
}

function trustClass(t) {
  if (!t) return 'sti-trust-unknown'
  return `sti-trust-${t.toLowerCase()}`
}
