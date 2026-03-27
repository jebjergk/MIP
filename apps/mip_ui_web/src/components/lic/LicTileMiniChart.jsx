import { useMemo } from 'react'

function num(v) {
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

function barClose(b) {
  if (!b || typeof b !== 'object') return null
  return num(b.close ?? b.CLOSE ?? b.c ?? b.last ?? b.Close ?? b.adj_close)
}

/**
 * Tile microchart: live path (dominant) vs expected median, stop/target, current marker.
 * Lines only — no bands, gradients, or in-chart legends.
 */
export default function LicTileMiniChart({ tile, recommendationBand }) {
  const model = useMemo(() => {
    const rawBars = (tile?.chart?.bars || []).slice(-56)
    let closes = rawBars.map(barClose).filter((x) => x != null)
    const barLoHi = []
    for (const b of rawBars) {
      if (!b || typeof b !== 'object') continue
      const lo = num(b.low ?? b.LOW)
      const hi = num(b.high ?? b.HIGH)
      if (lo != null) barLoHi.push(lo)
      if (hi != null) barLoHi.push(hi)
    }
    const curLive = num(tile?.current_price) ?? num(tile?.overlays?.current)
    const lastBar = closes.length ? closes[closes.length - 1] : null
    const cur = curLive ?? lastBar

    if (closes.length === 0 && cur != null) {
      closes = [cur, cur]
    } else if (closes.length === 1) {
      const one = closes[0]
      const other = cur != null && cur !== one ? cur : one * (1 + 1e-5)
      closes = [one, other]
    }

    if (closes.length < 2 || cur == null) return null

    const entry = num(tile?.entry_price)
    const sl = num(tile?.overlays?.stop_loss)
    const tp = num(tile?.overlays?.take_profit)
    const exp = tile?.expectation || {}
    const med = num(exp?.center_path?.[0]?.price)

    // Scale Y from price action (closes, mark, median, entry). Stop/target are often far away
    // and previously crushed the path into a flat line at one edge of the chart.
    const forPathScale = [cur, ...closes, med, entry, ...barLoHi].filter((x) => x != null)
    if (forPathScale.length === 0) return null

    let ymin = Math.min(...forPathScale)
    let ymax = Math.max(...forPathScale)
    const mid = (ymax + ymin) / 2
    const rawSpan = ymax - ymin || 1
    const lastN = closes.slice(-10)
    let atrLike = 0
    for (let i = 1; i < lastN.length; i += 1) {
      atrLike += Math.abs(lastN[i] - lastN[i - 1])
    }
    atrLike = lastN.length > 1 ? atrLike / (lastN.length - 1) : rawSpan * 0.012
    const minSpan = Math.max(rawSpan, Math.abs(mid) * 0.008, atrLike * 3, 1e-6)
    if (rawSpan < minSpan) {
      const c = mid
      ymin = c - minSpan / 2
      ymax = c + minSpan / 2
    }
    const span = ymax - ymin || 1
    const pad = span * 0.06
    ymin -= pad
    ymax += pad

    const W = 120
    const H = 76
    const normYRaw = (y) => H - ((y - ymin) / (ymax - ymin)) * H
    const clampY = (y) => Math.min(H - 0.5, Math.max(0.5, y))
    const normY = (y) => clampY(normYRaw(y))
    const normX = (i) => (i / Math.max(closes.length - 1, 1)) * W

    const pairs = closes.map((y, i) => `${normX(i).toFixed(2)},${normY(y).toFixed(2)}`)
    const linePath = pairs.length ? `M ${pairs.join(' L ')}` : ''

    let medianPath = ''
    if (med != null) {
      const yM = normY(med)
      if (Number.isFinite(yM)) {
        medianPath = `M 0,${yM.toFixed(2)} L ${W},${yM.toFixed(2)}`
      }
    }

    const ySl = sl != null ? normY(sl) : null
    const yTp = tp != null ? normY(tp) : null
    const yEntry = entry != null ? normY(entry) : null
    const showEntry =
      yEntry != null &&
      med != null &&
      Number.isFinite(yEntry) &&
      Math.abs(entry - med) / (Math.abs(med) || 1) > 0.002

    const curIdx = closes.length - 1
    const cx = normX(curIdx)
    const cy = normY(cur)

    const bandU = String(recommendationBand || '').toUpperCase()
    const stress = bandU === 'EXIT_NOW'

    return {
      W,
      H,
      linePath,
      medianPath,
      ySl,
      yTp,
      yEntry,
      showEntry,
      cx,
      cy,
      lineClass: stress ? 'lic-mini-line lic-mini-line--stress' : 'lic-mini-line',
      dotR: stress ? 3.6 : 3,
      dotClass: stress ? 'lic-mini-dot lic-mini-dot--stress' : 'lic-mini-dot',
    }
  }, [tile, recommendationBand])

  if (!model) {
    return <div className="lic-mini-chart lic-mini-chart--empty">No price path yet — refresh IB or open workspace chart.</div>
  }

  const {
    W,
    H,
    linePath,
    medianPath,
    ySl,
    yTp,
    yEntry,
    showEntry,
    cx,
    cy,
    lineClass,
    dotR,
    dotClass,
  } = model

  return (
    <div className="lic-mini-chart">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="lic-mini-svg" aria-hidden>
        {medianPath ? <path d={medianPath} fill="none" className="lic-mini-median" vectorEffect="non-scaling-stroke" /> : null}
        {ySl != null && Number.isFinite(ySl) ? (
          <line x1={0} x2={W} y1={ySl} y2={ySl} className="lic-mini-ref lic-mini-ref--sl" vectorEffect="non-scaling-stroke" />
        ) : null}
        {yTp != null && Number.isFinite(yTp) ? (
          <line x1={0} x2={W} y1={yTp} y2={yTp} className="lic-mini-ref lic-mini-ref--tp" vectorEffect="non-scaling-stroke" />
        ) : null}
        {showEntry && yEntry != null ? (
          <line
            x1={0}
            x2={W}
            y1={yEntry}
            y2={yEntry}
            className="lic-mini-ref lic-mini-ref--entry"
            strokeDasharray="5 4"
            vectorEffect="non-scaling-stroke"
          />
        ) : null}
        <path d={linePath} fill="none" className={lineClass} vectorEffect="non-scaling-stroke" />
        <circle cx={cx} cy={cy} r={dotR} className={dotClass} vectorEffect="non-scaling-stroke" />
      </svg>
    </div>
  )
}
