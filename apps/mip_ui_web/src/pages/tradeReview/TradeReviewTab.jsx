import { useCallback, useEffect, useState } from 'react'
import { API_BASE } from '../../config/apiBase'
import EmptyState from '../../components/EmptyState'
import LoadingState from '../../components/LoadingState'
import TradeReviewDetail from './TradeReviewDetail'
import TradeReviewTable from './TradeReviewTable'
import './tradeReview.css'

export default function TradeReviewTab({ portfolioId, active }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [trades, setTrades] = useState([])
  const [meta, setMeta] = useState(null)
  const [selectedRow, setSelectedRow] = useState(null)

  const load = useCallback(async () => {
    if (!active) return
    if (!portfolioId) {
      setTrades([])
      setMeta(null)
      setError(null)
      setLoading(false)
      return
    }
    setLoading(true)
    setError(null)
    try {
      const url = `${API_BASE}/parallel-worlds/trade-intelligence?portfolio_id=${portfolioId}&limit=500`
      const r = await fetch(url)
      if (!r.ok) {
        throw new Error(`HTTP ${r.status}`)
      }
      const data = await r.json()
      if (data && data.ok === false) {
        throw new Error(data.detail || data.message || 'Failed to load trade intelligence')
      }
      const list = Array.isArray(data.trades) ? data.trades : []
      setTrades(list)
      setMeta(data.meta || null)
      setSelectedRow((prev) => {
        if (!prev) return null
        const id = prev.closeout_id ?? prev.CLOSEOUT_ID
        const next = list.find((t) => (t.closeout_id ?? t.CLOSEOUT_ID) === id)
        return next || null
      })
    } catch (e) {
      setError(e.message || String(e))
      setTrades([])
      setMeta(null)
      setSelectedRow(null)
    } finally {
      setLoading(false)
    }
  }, [portfolioId, active])

  useEffect(() => {
    load()
  }, [load])

  useEffect(() => {
    setSelectedRow(null)
  }, [portfolioId])

  if (!active) return null

  return (
    <div className="tr-layout">
      {loading && <LoadingState message="Loading trade intelligence..." />}
      {error && <div className="pw-error">Trade Review: {error}</div>}
      {!loading && !error && !portfolioId && (
        <EmptyState
          title="Select a portfolio"
          explanation="Choose a live portfolio above to load closed trades."
          reasons={[]}
        />
      )}
      {!loading && !error && portfolioId && trades.length === 0 && (
        <EmptyState
          title="No closed trades yet"
          explanation="Trade Review lists one row per live closeout. This portfolio has none in the database yet."
          reasons={[
            'Closeouts appear when the live pipeline records TRADE_CLOSEOUT rows.',
            'Counterfactual What-If data is separate — open Overview after trades exist.',
          ]}
        />
      )}
      {!loading && !error && trades.length > 0 && (
        <div className="tr-layout tr-layout--split">
          <div>
            <TradeReviewTable
              trades={trades}
              selectedRow={selectedRow}
              onSelectRow={setSelectedRow}
            />
          </div>
          <TradeReviewDetail row={selectedRow} meta={meta} />
        </div>
      )}
    </div>
  )
}
