import { useId, useMemo } from 'react'

function num(v) {
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}

function barClose(b) {
  if (!b || typeof b !== 'object') return null
  return num(b.close ?? b.c ?? b.last ?? b.Close ?? b.adj_close)
}

/**
 * Decision micro-chart: live path (dominant) vs flat expected median, stop/target zones, current marker.
 * Tolerates sparse IB bars (single bar, alternate field names) so the chart rarely stays empty.
 */
export default function LicTileMiniChart({ tile, recommendationBand, intel }) {
  const gradId = useId().replace(/[^a-zA-Z0-9_-]/g, '')
  const model = useMemo(() => {
    const rawBars = (tile?.chart?.bars || []).slice(-56)
    let closes = rawBars.map(barClose).filter((x) => x != null)
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

    const side = String(tile?.side || 'LONG').toUpperCase()
    const isLong = side !== 'SHORT'

    const entry = num(tile?.entry_price)
    const sl = num(tile?.overlays?.stop_loss)
    const tp = num(tile?.overlays?.take_profit)
    const exp = tile?.expectation || {}
    const med = num(exp?.center_path?.[0]?.price)
    const lo = num(exp?.lower_path?.[0]?.price)
    const hi = num(exp?.upper_path?.[0]?.price)

    const pm = tile?.progress_metrics || {}
    const distSlPct = num(pm.distance_to_sl_pct)
    const distTpPct = num(pm.distance_to_tp_pct)
    const slNear = Boolean(intel?.sl_near) || (distSlPct != null && distSlPct < 0.04)

    const forScale = [cur, ...closes, entry, sl, tp, med].filter((x) => x != null)
    let ymin = Math.min(...forScale)
    let ymax = Math.max(...forScale)
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
    const H = 72
    const normY = (y) => H - ((y - ymin) / (ymax - ymin)) * H
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

    const bandTop = hi != null && lo != null ? normY(Math.max(hi, lo)) : null
    const bandBot = hi != null && lo != null ? normY(Math.min(hi, lo)) : null
    const bandH =
      bandTop != null && bandBot != null && Math.abs(bandBot - bandTop) > 1.2
        ? Math.abs(bandBot - bandTop)
        : null
    const bandY = bandH != null ? Math.min(bandTop, bandBot) : null

    const yCur = normY(cur)
    const ySl = sl != null ? normY(sl) : null
    const yTp = tp != null ? normY(tp) : null

    let stopZone = null
    if (ySl != null && Number.isFinite(yCur)) {
      const y1 = Math.min(yCur, ySl)
      const y2 = Math.max(yCur, ySl)
      if (y2 - y1 > 0.5) {
        stopZone = { y: y1, h: y2 - y1 }
      }
    }

    let targetZone = null
    if (yTp != null && Number.isFinite(yCur)) {
      if (isLong && tp > cur) {
        const y1 = Math.min(yCur, yTp)
        const y2 = Math.max(yCur, yTp)
        if (y2 - y1 > 0.5) targetZone = { y: y1, h: y2 - y1 }
      } else if (!isLong && tp < cur) {
        const y1 = Math.min(yCur, yTp)
        const y2 = Math.max(yCur, yTp)
        if (y2 - y1 > 0.5) targetZone = { y: y1, h: y2 - y1 }
      }
    }

    const bandU = String(recommendationBand || '').toUpperCase()
    const stress = bandU === 'EXIT_NOW'
    const defensive = bandU === 'PREPARE_EXIT' || bandU === 'WATCH_CLOSELY'
    const stopAlpha = stress ? 0.38 : slNear ? 0.28 : defensive ? 0.18 : 0.12
    const targetAlpha = bandU === 'STAY_COURSE' || bandU === '' ? 0.14 : 0.08

    const refEntry = (y) => {
      if (y == null || !Number.isFinite(y)) return null
      return (
        <line
          key={`ent-${y}`}
          x1={0}
          x2={W}
          y1={y}
          y2={y}
          strokeDasharray="4 3"
          className="lic-mini-ref lic-mini-ref--entry"
        />
      )
    }

    const refStopTp = (y, kind) => {
      if (y == null || !Number.isFinite(y)) return null
      return (
        <line
          key={`${kind}-${y}`}
          x1={0}
          x2={W}
          y1={y}
          y2={y}
          className={kind === 'sl' ? 'lic-mini-ref lic-mini-ref--sl' : 'lic-mini-ref lic-mini-ref--tp'}
        />
      )
    }

    const curIdx = closes.length - 1
    const cx = normX(curIdx)
    const cy = normY(cur)

    const belowMed = med != null && (isLong ? cur < med : cur > med)
    const legendHint = belowMed ? 'Below expected path' : 'Near/on expected path'

    return {
      W,
      H,
      linePath,
      medianPath,
      bandY,
      bandH,
      bandW: W,
      stopZone,
      targetZone,
      stopAlpha,
      targetAlpha,
      refEntry: entry != null ? refEntry(normY(entry)) : null,
      refSl: refStopTp(ySl, 'sl'),
      refTp: refStopTp(yTp, 'tp'),
      showLoHi: !bandH,
      lo: lo != null ? normY(lo) : null,
      hi: hi != null ? normY(hi) : null,
      cx,
      cy,
      lineClass: stress ? 'lic-mini-line lic-mini-line--stress' : 'lic-mini-line',
      legendHint,
      hasMed: medianPath.length > 0,
      hasSl: ySl != null,
      hasTp: yTp != null,
      gradId,
    }
  }, [tile, recommendationBand, intel, gradId])

  if (!model) {
    return <div className="lic-mini-chart lic-mini-chart--empty">No price path yet — refresh IB or open drill-down chart.</div>
  }

  const {
    W,
    H,
    linePath,
    medianPath,
    bandY,
    bandH,
    bandW,
    stopZone,
    targetZone,
    stopAlpha,
    targetAlpha,
    refEntry,
    refSl,
    refTp,
    showLoHi,
    lo,
    hi,
    cx,
    cy,
    lineClass,
    legendHint,
    hasMed,
    hasSl,
    hasTp,
    gradId: gid,
  } = model

  return (
    <div className="lic-mini-chart">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="lic-mini-svg" aria-hidden>
        <defs>
          <linearGradient id={`licMiniTpGrad-${gid}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#4ade80" stopOpacity="0.2" />
            <stop offset="100%" stopColor="#4ade80" stopOpacity="0.04" />
          </linearGradient>
          <linearGradient id={`licMiniStopGrad-${gid}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#f87171" stopOpacity="0.45" />
            <stop offset="100%" stopColor="#f87171" stopOpacity="0.06" />
          </linearGradient>
        </defs>
        {targetZone ? (
          <rect
            x={0}
            y={targetZone.y}
            width={W}
            height={targetZone.h}
            fill={`url(#licMiniTpGrad-${gid})`}
            style={{ opacity: targetAlpha * 4 }}
            className="lic-mini-zone lic-mini-zone--tp"
          />
        ) : null}
        {stopZone ? (
          <rect
            x={0}
            y={stopZone.y}
            width={W}
            height={stopZone.h}
            fill={`url(#licMiniStopGrad-${gid})`}
            style={{ opacity: stopAlpha * 2.2 }}
            className="lic-mini-zone lic-mini-zone--sl"
          />
        ) : null}
        {bandY != null && bandH != null ? (
          <rect x={0} y={bandY} width={bandW} height={bandH} className="lic-mini-band" />
        ) : null}
        {showLoHi && lo != null && hi != null ? (
          <>
            <line x1={0} x2={W} y1={lo} y2={lo} className="lic-mini-ref" strokeDasharray="3 3" />
            <line x1={0} x2={W} y1={hi} y2={hi} className="lic-mini-ref" strokeDasharray="3 3" />
          </>
        ) : null}
        {refEntry}
        {medianPath ? <path d={medianPath} fill="none" className="lic-mini-median" vectorEffect="non-scaling-stroke" /> : null}
        {refSl}
        {refTp}
        <path d={linePath} fill="none" className={lineClass} vectorEffect="non-scaling-stroke" />
        <circle cx={cx} cy={cy} r={3} className="lic-mini-dot" />
      </svg>
      <div className="lic-mini-microcopy" title={`${legendHint}. Cyan = live path. Purple dashed = expected median. Red zone = toward stop. Green tint = room toward target.`}>
        <span className="lic-mini-legend-hint">{legendHint}</span>
        <span className="lic-mini-legend-keys">
          <span className="lic-mini-key lic-mini-key--price">Live</span>
          {hasMed ? <span className="lic-mini-key lic-mini-key--med">Expected</span> : null}
          {hasSl ? <span className="lic-mini-key lic-mini-key--sl">Stop</span> : null}
          {hasTp ? <span className="lic-mini-key lic-mini-key--tp">Target</span> : null}
        </span>
      </div>
    </div>
  )
}
