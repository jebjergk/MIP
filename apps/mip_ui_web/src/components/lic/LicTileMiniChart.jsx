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
 * Tile microchart contract: live path, expected path, stop, target, current marker only.
 * No entry line, legends, or in-chart labels.
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

    const sl = num(tile?.overlays?.stop_loss)
    const tp = num(tile?.overlays?.take_profit)
    const exp = tile?.expectation || {}
    const centerPath = Array.isArray(exp?.center_path) ? exp.center_path : []
    const expPrices = centerPath.map((p) => num(p?.price)).filter((x) => x != null)
    const med = expPrices.length ? expPrices[0] : null

    const forPathScale = [cur, ...closes, med, ...expPrices, ...barLoHi].filter((x) => x != null)
    if (forPathScale.length === 0) return null

    let ymin = Math.min(...forPathScale)
    let ymax = Math.max(...forPathScale)
    const prePadSpan = ymax - ymin || 1
    const margin = prePadSpan * 1.5
    if (sl != null && sl >= ymin - margin && sl <= ymax + margin) {
      ymin = Math.min(ymin, sl)
      ymax = Math.max(ymax, sl)
    }
    if (tp != null && tp >= ymin - margin && tp <= ymax + margin) {
      ymin = Math.min(ymin, tp)
      ymax = Math.max(ymax, tp)
    }

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
    const H = 92
    const normYRaw = (y) => H - ((y - ymin) / (ymax - ymin)) * H
    const clampY = (y) => Math.min(H - 0.5, Math.max(0.5, y))
    const normY = (y) => clampY(normYRaw(y))
    const normX = (i) => (i / Math.max(closes.length - 1, 1)) * W

    const pairs = closes.map((y, i) => `${normX(i).toFixed(2)},${normY(y).toFixed(2)}`)
    const linePath = pairs.length ? `M ${pairs.join(' L ')}` : ''

    let expectedPathD = ''
    if (expPrices.length >= 2) {
      const pts = expPrices.map((y, i) => {
        const x = (i / (expPrices.length - 1)) * W
        return `${x.toFixed(2)},${normY(y).toFixed(2)}`
      })
      expectedPathD = `M ${pts.join(' L ')}`
    } else if (med != null) {
      const yM = normY(med)
      if (Number.isFinite(yM)) {
        expectedPathD = `M 0,${yM.toFixed(2)} L ${W},${yM.toFixed(2)}`
      }
    }

    const ySl = sl != null ? normY(sl) : null
    const yTp = tp != null ? normY(tp) : null

    const curIdx = closes.length - 1
    const cx = normX(curIdx)
    const cy = normY(cur)

    const bandU = String(recommendationBand || '').toUpperCase()
    const stress = bandU === 'EXIT_NOW'

    return {
      W,
      H,
      linePath,
      expectedPathD,
      ySl,
      yTp,
      cx,
      cy,
      lineClass: stress ? 'lic-mini-line lic-mini-line--stress' : 'lic-mini-line',
      dotR: stress ? 3.6 : 3,
      dotClass: stress ? 'lic-mini-dot lic-mini-dot--stress' : 'lic-mini-dot',
    }
  }, [tile, recommendationBand])

  if (!model) {
    return <div className="lic-mini-chart lic-mini-chart--empty">No bars yet — refresh live data.</div>
  }

  const { W, H, linePath, expectedPathD, ySl, yTp, cx, cy, lineClass, dotR, dotClass } = model

  return (
    <div className="lic-mini-chart">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="lic-mini-svg" aria-hidden>
        {expectedPathD ? (
          <path d={expectedPathD} fill="none" className="lic-mini-median" vectorEffect="non-scaling-stroke" />
        ) : null}
        {ySl != null && Number.isFinite(ySl) ? (
          <line x1={0} x2={W} y1={ySl} y2={ySl} className="lic-mini-ref lic-mini-ref--sl" vectorEffect="non-scaling-stroke" />
        ) : null}
        {yTp != null && Number.isFinite(yTp) ? (
          <line x1={0} x2={W} y1={yTp} y2={yTp} className="lic-mini-ref lic-mini-ref--tp" vectorEffect="non-scaling-stroke" />
        ) : null}
        <path d={linePath} fill="none" className={lineClass} vectorEffect="non-scaling-stroke" />
        <circle cx={cx} cy={cy} r={dotR} className={dotClass} vectorEffect="non-scaling-stroke" />
      </svg>
    </div>
  )
}
