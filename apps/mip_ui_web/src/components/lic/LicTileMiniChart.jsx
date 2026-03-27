import { useMemo } from 'react'

function n(v) {
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

export default function LicTileMiniChart({ tile }) {
  const model = useMemo(() => {
    const bars = (tile?.chart?.bars || []).slice(-48)
    const closes = bars.map((b) => n(b.close)).filter((x) => x != null)
    if (closes.length < 2) return null
    const entry = n(tile?.entry_price)
    const sl = n(tile?.overlays?.stop_loss)
    const tp = n(tile?.overlays?.take_profit)
    const exp = tile?.expectation || {}
    const med = n(exp?.center_path?.[0]?.price)
    const lo = n(exp?.lower_path?.[0]?.price)
    const hi = n(exp?.upper_path?.[0]?.price)
    const cur = n(tile?.current_price) ?? closes[closes.length - 1]
    const all = [cur, ...closes, entry, sl, tp, med, lo, hi].filter((x) => x != null)
    let ymin = Math.min(...all)
    let ymax = Math.max(...all)
    const span = ymax - ymin || 1
    const pad = span * 0.06
    ymin -= pad
    ymax += pad
    const W = 100
    const H = 36
    const normY = (y) => H - ((y - ymin) / (ymax - ymin)) * H
    const normX = (i) => (i / Math.max(closes.length - 1, 1)) * W
    const pairs = closes.map((y, i) => `${normX(i).toFixed(2)},${normY(y).toFixed(2)}`)
    const linePath = pairs.length ? `M ${pairs.join(' L ')}` : ''
    const ref = (y, dash) => {
      if (y == null) return null
      const yy = normY(y)
      return (
        <line
          key={`${y}-${dash}`}
          x1={0}
          x2={W}
          y1={yy}
          y2={yy}
          strokeDasharray={dash ? '3 2' : undefined}
          className="lic-mini-ref"
        />
      )
    }
    return { W, H, linePath, cur, normY, normX, closes, entry, sl, tp, med, lo, hi, ref, curIdx: closes.length - 1 }
  }, [tile])

  if (!model) {
    return <div className="lic-mini-chart lic-mini-chart--empty">No price path</div>
  }

  const { W, H, linePath, normY, normX, ref, curIdx } = model
  const cx = normX(curIdx)
  const cy = normY(cur)

  return (
    <div className="lic-mini-chart" aria-hidden>
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="lic-mini-svg">
        {ref(lo, true)}
        {ref(hi, true)}
        {ref(med, false)}
        {ref(entry, true)}
        {ref(sl, true)}
        {ref(tp, true)}
        <path d={linePath} fill="none" className="lic-mini-line" vectorEffect="non-scaling-stroke" />
        <circle cx={cx} cy={cy} r={2.2} className="lic-mini-dot" />
      </svg>
      <div className="lic-mini-legend">
        <span>Path</span>
        <span className="lic-mini-lg">Med</span>
        <span className="lic-mini-lg">Band</span>
        <span>Entry/SL/TP</span>
      </div>
    </div>
  )
}
