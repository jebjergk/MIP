import { useMemo } from 'react'

function n(v) {
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

/** Y-scale from closes + decision levels; lo/hi band excluded from scale when they would flatten the path. */
export default function LicTileMiniChart({ tile, recommendationBand }) {
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

    const forScale = [cur, ...closes, entry, sl, tp, med].filter((x) => x != null)
    let ymin = Math.min(...forScale)
    let ymax = Math.max(...forScale)
    const mid = (ymax + ymin) / 2
    const rawSpan = ymax - ymin || 1
    const lastN = closes.slice(-8)
    let atrLike = 0
    for (let i = 1; i < lastN.length; i += 1) {
      atrLike += Math.abs(lastN[i] - lastN[i - 1])
    }
    atrLike = lastN.length > 1 ? atrLike / (lastN.length - 1) : rawSpan * 0.01
    const minSpan = Math.max(rawSpan, Math.abs(mid) * 0.006, atrLike * 2.5, 1e-6)
    if (rawSpan < minSpan) {
      const c = mid
      ymin = c - minSpan / 2
      ymax = c + minSpan / 2
    }
    const span = ymax - ymin || 1
    const pad = span * 0.05
    ymin -= pad
    ymax += pad

    const W = 100
    const H = 64
    const normY = (y) => H - ((y - ymin) / (ymax - ymin)) * H
    const normX = (i) => (i / Math.max(closes.length - 1, 1)) * W
    const pairs = closes.map((y, i) => `${normX(i).toFixed(2)},${normY(y).toFixed(2)}`)
    const linePath = pairs.length ? `M ${pairs.join(' L ')}` : ''

    const bandTop = hi != null && lo != null ? normY(Math.max(hi, lo)) : null
    const bandBot = hi != null && lo != null ? normY(Math.min(hi, lo)) : null
    const bandH =
      bandTop != null && bandBot != null && Math.abs(bandBot - bandTop) > 0.8
        ? Math.abs(bandBot - bandTop)
        : null
    const bandY = bandH != null ? Math.min(bandTop, bandBot) : null

    const refMuted = (y, dash) => {
      if (y == null) return null
      const yy = normY(y)
      if (!Number.isFinite(yy)) return null
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

    return {
      W,
      H,
      linePath,
      cur,
      normY,
      normX,
      closes,
      entry,
      sl,
      tp,
      med,
      lo,
      hi,
      refMuted,
      curIdx: closes.length - 1,
      bandY,
      bandH,
      bandW: W,
      exitStress: String(recommendationBand || '').toUpperCase() === 'EXIT_NOW',
    }
  }, [tile, recommendationBand])

  if (!model) {
    return <div className="lic-mini-chart lic-mini-chart--empty">No price path</div>
  }

  const { W, H, linePath, normY, normX, refMuted, curIdx, cur, lo, hi, med, entry, sl, tp, bandY, bandH, bandW, exitStress } =
    model
  const cx = normX(curIdx)
  const cy = normY(cur)

  const lineClass = exitStress ? 'lic-mini-line lic-mini-line--stress' : 'lic-mini-line'

  return (
    <div className="lic-mini-chart" aria-hidden>
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="lic-mini-svg">
        {refMuted(med, false)}
        {refMuted(entry, true)}
        {refMuted(sl, true)}
        {refMuted(tp, true)}
        {bandY != null && bandH != null ? (
          <rect x={0} y={bandY} width={bandW} height={bandH} className="lic-mini-band" />
        ) : null}
        {!bandH ? (
          <>
            {refMuted(lo, true)}
            {refMuted(hi, true)}
          </>
        ) : null}
        <path d={linePath} fill="none" className={lineClass} vectorEffect="non-scaling-stroke" />
        <circle cx={cx} cy={cy} r={2.4} className="lic-mini-dot" />
      </svg>
    </div>
  )
}
