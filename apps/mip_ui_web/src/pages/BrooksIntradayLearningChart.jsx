import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { normalizeBarTs } from './brooksLearningBarSelection'
import {
  ADVISER_V1_CHART_LEGEND,
  CHART_LEGEND,
  dossierLevelLabels,
  initialStopLabel,
  stopRaisedLabel,
} from './brooksLearningChartLabels'
import {
  computeYDomain,
  dossierLevelsWithVisibility,
  formatAxisPrice,
  generatePriceTicks,
  generateTimeTicksNy,
} from './brooksLearningChartDomain'
import { selectionBandGeometry } from './brooksLearningSessionNav'

const MARGIN = { top: 18, right: 62, bottom: 40, left: 14 }
const MIN_BODY = 3

function yForPrice(price, lo, hi, plotH) {
  return MARGIN.top + plotH - ((price - lo) / (hi - lo)) * plotH
}

function xForIndex(i, slot) {
  return MARGIN.left + i * slot + slot / 2
}

function priceFromY(y, lo, hi, plotH) {
  return lo + (1 - (y - MARGIN.top) / plotH) * (hi - lo)
}

export default function BrooksIntradayLearningChart({
  bars,
  overlays,
  selectedTs,
  onSelectBar,
  registerCandleRef,
  chartHeight = 580,
  tradeSummary,
  adviserChart = false,
  highlightRange = null,
}) {
  const wrapRef = useRef(null)
  const [containerWidth, setContainerWidth] = useState(960)
  const [hoverTs, setHoverTs] = useState(null)
  const [cursorPrice, setCursorPrice] = useState(null)

  useEffect(() => {
    const el = wrapRef.current
    if (!el) return undefined
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect?.width
      if (w && w > 0) setContainerWidth(w)
    })
    ro.observe(el)
    setContainerWidth(el.clientWidth || 960)
    return () => ro.disconnect()
  }, [])

  const n = Math.max((bars || []).length, 1)
  const minSlot = 11
  const innerW = Math.max(containerWidth - MARGIN.left - MARGIN.right, n * minSlot)
  const totalW = innerW + MARGIN.left + MARGIN.right
  const plotH = chartHeight - MARGIN.top - MARGIN.bottom
  const slot = innerW / n
  const bodyW = Math.max(MIN_BODY, Math.min(14, slot * 0.72))

  const referenceBarTs = selectedTs || bars[bars.length - 1]?.ts_utc
  const entryTs = tradeSummary?.entry_ts || overlays?.entry?.ts
  const entryPrice = tradeSummary?.entry_price ?? overlays?.entry?.price

  const { lo, hi } = useMemo(() => computeYDomain(bars, overlays), [bars, overlays])
  const priceTicks = useMemo(() => generatePriceTicks(lo, hi), [lo, hi])
  const timeTicks = useMemo(() => generateTimeTicksNy(bars, 30), [bars])
  const levelMeta = useMemo(
    () => dossierLevelsWithVisibility(overlays?.levels, lo, hi),
    [overlays, lo, hi],
  )
  const levelLabels = useMemo(
    () => dossierLevelLabels(overlays?.levels, { referenceBarTs, entryTs, entryPrice }),
    [overlays, referenceBarTs, entryTs, entryPrice],
  )

  const selectedIdx = useMemo(
    () => (bars || []).findIndex((b) => normalizeBarTs(b.ts_utc) === normalizeBarTs(selectedTs)),
    [bars, selectedTs],
  )
  const selectedBar = selectedIdx >= 0 ? bars[selectedIdx] : null
  const hoverBar = useMemo(
    () => (bars || []).find((b) => normalizeBarTs(b.ts_utc) === normalizeBarTs(hoverTs)),
    [bars, hoverTs],
  )

  const activeStopPrice = selectedBar?.active_stop ?? hoverBar?.active_stop

  const stopSegments = useMemo(() => {
    const steps = overlays?.stop_steps || []
    return steps
      .map((st, si) => {
        const fromIdx = bars.findIndex(
          (b) => normalizeBarTs(b.ts_utc) >= normalizeBarTs(st.from_ts),
        )
        let toIdx = bars.length - 1
        if (st.until_ts) {
          const u = bars.findIndex(
            (b) => normalizeBarTs(b.ts_utc) >= normalizeBarTs(st.until_ts),
          )
          if (u >= 0) toIdx = Math.max(fromIdx, u - 1)
        }
        if (fromIdx < 0) return null
        const y = yForPrice(st.stop_price, lo, hi, plotH)
        const isLast = si === steps.length - 1
        const isActive = isLast || Math.abs(Number(st.stop_price) - Number(activeStopPrice)) < 0.001
        return {
          key: `stop-${si}`,
          x1: xForIndex(fromIdx, slot),
          x2: xForIndex(toIdx, slot),
          y,
          price: st.stop_price,
          label: stopRaisedLabel(st.stop_price, { isActive }),
        }
      })
      .filter(Boolean)
  }, [bars, overlays, slot, lo, hi, plotH, activeStopPrice])

  const onSvgLeave = useCallback(() => {
    setHoverTs(null)
    setCursorPrice(null)
  }, [])

  const onSvgMove = useCallback(
    (e) => {
      const svg = e.currentTarget
      const pt = svg.createSVGPoint()
      pt.x = e.clientX
      pt.y = e.clientY
      const ctm = svg.getScreenCTM()
      if (!ctm) return
      const p = pt.matrixTransform(ctm.inverse())
      if (p.y >= MARGIN.top && p.y <= MARGIN.top + plotH) {
        setCursorPrice(priceFromY(p.y, lo, hi, plotH))
      }
    },
    [lo, hi, plotH],
  )

  const tooltipBar = hoverBar || selectedBar
  const crosshairY = hoverTs != null && cursorPrice != null
    ? yForPrice(cursorPrice, lo, hi, plotH)
    : null

  const selectedBand = useMemo(() => {
    if (selectedIdx < 0) return null
    const cx = xForIndex(selectedIdx, slot)
    return selectionBandGeometry(cx, slot, MARGIN.left, innerW, MARGIN.top, plotH)
  }, [selectedIdx, slot, innerW, plotH])

  const tradeHighlightBand = useMemo(() => {
    if (!highlightRange?.fromTs || !highlightRange?.toTs || !bars?.length) return null
    const fromKey = normalizeBarTs(highlightRange.fromTs)
    const toKey = normalizeBarTs(highlightRange.toTs)
    const fromIdx = bars.findIndex((b) => normalizeBarTs(b.ts_utc) >= fromKey)
    const toIdx = bars.findIndex((b) => normalizeBarTs(b.ts_utc) >= toKey)
    if (fromIdx < 0 || toIdx < 0) return null
    const x1 = xForIndex(fromIdx, slot) - slot / 2
    const x2 = xForIndex(toIdx, slot) + slot / 2
    return {
      x: Math.max(MARGIN.left, x1),
      y: MARGIN.top,
      width: Math.min(innerW, x2 - x1),
      height: plotH,
    }
  }, [highlightRange, bars, slot, innerW, plotH])

  const legendBlocks = adviserChart ? ADVISER_V1_CHART_LEGEND : CHART_LEGEND

  return (
    <div ref={wrapRef} className={`bil-learning-chart-wrap bil-learning-chart-wrap--g21${adviserChart ? ' bil-learning-chart-wrap--adviser-v1' : ''}`}>
      <ul className="bil-learning-chart-legend" aria-label="Chart legend">
        {legendBlocks.map((block) => (
          <li key={block.group}>
            <strong>{block.group}:</strong>
            {' '}
            {block.items.join(' · ')}
          </li>
        ))}
      </ul>

      <div className="bil-learning-chart-tooltip" role="status">
        {tooltipBar ? (
          <>
            <strong>{String(tooltipBar.ts_ny || '').replace(' ', 'T').slice(11, 16)} NY</strong>
            {' · O '}
            {Number(tooltipBar.open).toFixed(2)}
            {' H '}
            {Number(tooltipBar.high).toFixed(2)}
            {' L '}
            {Number(tooltipBar.low).toFixed(2)}
            {' C '}
            {Number(tooltipBar.close).toFixed(2)}
          </>
        ) : (
          <span>Select a bar</span>
        )}
        {hoverTs != null && cursorPrice != null ? (
          <span className="bil-learning-cursor-price">
            {' · Cursor price: $'}
            {cursorPrice.toFixed(2)}
          </span>
        ) : null}
      </div>

      <svg
        width={totalW}
        height={chartHeight}
        className="bil-learning-chart-svg"
        role="img"
        aria-label="5-minute session candlesticks"
        onMouseLeave={onSvgLeave}
        onMouseMove={onSvgMove}
      >
        <defs>
          <clipPath id="bil-learning-plot-clip">
            <rect x={MARGIN.left} y={MARGIN.top} width={innerW} height={plotH} />
          </clipPath>
        </defs>

        {priceTicks.map((p) => {
          const y = yForPrice(p, lo, hi, plotH)
          return (
            <g key={`yt-${p}`}>
              <line
                x1={MARGIN.left}
                x2={totalW - MARGIN.right}
                y1={y}
                y2={y}
                className="bil-learning-grid-line"
              />
              <text
                x={totalW - MARGIN.right + 6}
                y={y + 4}
                className="bil-learning-y-label"
              >
                {formatAxisPrice(p, { stopPrecision: p < 247 && p > 246 })}
              </text>
            </g>
          )
        })}

        {crosshairY != null ? (
          <line
            x1={MARGIN.left}
            x2={totalW - MARGIN.right}
            y1={crosshairY}
            y2={crosshairY}
            className="bil-learning-crosshair"
          />
        ) : null}

        {levelMeta
          .filter((ln) => ln.inView)
          .map((ln) => {
            const y = yForPrice(ln.price, lo, hi, plotH)
            const labelEntry = levelLabels.find((l) => l.key === ln.key)
            return (
              <g key={ln.key}>
                <line
                  x1={MARGIN.left}
                  x2={totalW - MARGIN.right}
                  y1={y}
                  y2={y}
                  className={`bil-learning-level bil-learning-level--${ln.key}`}
                />
                <text x={MARGIN.left + 4} y={y - 4} className="bil-learning-level-label">
                  {labelEntry?.text || ln.label}
                </text>
              </g>
            )
          })}

        {stopSegments.map((seg) => (
          <g key={seg.key}>
            <line
              x1={seg.x1}
              x2={seg.x2}
              y1={seg.y}
              y2={seg.y}
              className="bil-learning-stop-step"
            />
            <text x={seg.x2 + 4} y={seg.y + 4} className="bil-learning-stop-label">
              {seg.label}
            </text>
          </g>
        ))}

        {overlays?.initial_stop != null
        && overlays.initial_stop >= lo
        && overlays.initial_stop <= hi ? (
          <g>
            <line
              x1={MARGIN.left}
              x2={totalW - MARGIN.right}
              y1={yForPrice(overlays.initial_stop, lo, hi, plotH)}
              y2={yForPrice(overlays.initial_stop, lo, hi, plotH)}
              className="bil-learning-initial-stop"
            />
            <text
              x={MARGIN.left + 4}
              y={yForPrice(overlays.initial_stop, lo, hi, plotH) - 6}
              className="bil-learning-initial-stop-label"
            >
              {initialStopLabel(overlays.initial_stop)}
            </text>
          </g>
        ) : null}

        <g clipPath="url(#bil-learning-plot-clip)">
          {tradeHighlightBand ? (
            <rect {...tradeHighlightBand} className="bil-learning-trade-highlight" />
          ) : null}
          {selectedBand ? (
            <rect
              {...selectedBand}
              className="bil-learning-select-band"
              data-testid="learning-selected-band"
            />
          ) : null}
        </g>

        {(bars || []).map((b, i) => {
          const cx = xForIndex(i, slot)
          const o = Number(b.open)
          const h = Number(b.high)
          const l = Number(b.low)
          const c = Number(b.close)
          const up = c >= o
          const yO = yForPrice(o, lo, hi, plotH)
          const yC = yForPrice(c, lo, hi, plotH)
          const yH = yForPrice(h, lo, hi, plotH)
          const yL = yForPrice(l, lo, hi, plotH)
          const top = Math.min(yO, yC)
          const bot = Math.max(yO, yC)
          const bodyH = Math.max(MIN_BODY, bot - top)
          const selected = i === selectedIdx
          const hovered = normalizeBarTs(b.ts_utc) === normalizeBarTs(hoverTs)
          return (
            <g
              key={b.ts_utc || i}
              ref={(el) => registerCandleRef?.(b.ts_utc, el)}
              className={
                selected
                  ? 'bil-learning-candle bil-learning-candle--selected'
                  : hovered
                    ? 'bil-learning-candle bil-learning-candle--hover'
                    : 'bil-learning-candle'
              }
              onMouseEnter={() => setHoverTs(b.ts_utc)}
              onClick={() => onSelectBar?.(b.ts_utc)}
              style={{ cursor: onSelectBar ? 'pointer' : undefined }}
            >
              <line
                x1={cx}
                x2={cx}
                y1={yH}
                y2={yL}
                className={up ? 'bil-learning-wick-up' : 'bil-learning-wick-down'}
              />
              <rect
                x={cx - bodyW / 2}
                y={top}
                width={bodyW}
                height={bodyH}
                className={up ? 'bil-learning-body-up' : 'bil-learning-body-down'}
              />
              {adviserChart && b.active_stop != null ? (
                <line
                  x1={cx - slot * 0.42}
                  x2={cx + slot * 0.42}
                  y1={yForPrice(Number(b.active_stop), lo, hi, plotH)}
                  y2={yForPrice(Number(b.active_stop), lo, hi, plotH)}
                  className="bil-adviser-bar-stop"
                />
              ) : null}
              {adviserChart && (b.markers || []).map((m, mi) => {
                const isSim = m.kind === 'sim_entry' || m.kind === 'sim_exit'
                const yMark = isSim ? yH - 6 - mi * 4 : MARGIN.top + plotH + 12 + mi * 11
                const xMark = isSim ? cx : cx
                if (isSim) {
                  return (
                    <g
                      key={`${b.ts_utc}-m-${mi}`}
                      className={`bil-learning-marker-dot bil-learning-marker--${m.kind}`}
                      onClick={(e) => {
                        e.stopPropagation()
                        onSelectBar?.(b.ts_utc)
                      }}
                    >
                      <circle cx={xMark} cy={yMark} r={4} />
                      <title>{m.label}</title>
                    </g>
                  )
                }
                return (
                  <text
                    key={`${b.ts_utc}-m-${mi}`}
                    x={xMark}
                    y={yMark}
                    textAnchor="middle"
                    className={`bil-learning-marker-label bil-learning-marker--${m.kind}`}
                    onClick={(e) => {
                      e.stopPropagation()
                      onSelectBar?.(b.ts_utc)
                    }}
                  >
                    {m.label}
                  </text>
                )
              })}
              {adviserChart && (b.confirmation_levels || []).map((lv, li) => {
                const y = yForPrice(lv.level, lo, hi, plotH)
                if (y < MARGIN.top || y > MARGIN.top + plotH) return null
                return (
                  <line
                    key={`conf-${i}-${li}`}
                    x1={cx - slot * 0.45}
                    x2={cx + slot * 0.45}
                    y1={y}
                    y2={y}
                    className="bil-adviser-level-conf"
                  />
                )
              })}
              {adviserChart && (b.invalidation_levels || []).map((lv, li) => {
                const y = yForPrice(lv.level, lo, hi, plotH)
                if (y < MARGIN.top || y > MARGIN.top + plotH) return null
                return (
                  <line
                    key={`inv-${i}-${li}`}
                    x1={cx - slot * 0.45}
                    x2={cx + slot * 0.45}
                    y1={y}
                    y2={y}
                    className="bil-adviser-level-inv"
                  />
                )
              })}
            </g>
          )
        })}

        {timeTicks.map((t) => {
          const x = xForIndex(t.index, slot)
          return (
            <g key={`xt-${t.label}`}>
              <line
                x1={x}
                x2={x}
                y1={MARGIN.top + plotH}
                y2={MARGIN.top + plotH + 6}
                className="bil-learning-x-tick"
              />
              <text
                x={x}
                y={chartHeight - 8}
                textAnchor="middle"
                className="bil-learning-x-label"
              >
                {t.label}
              </text>
            </g>
          )
        })}
      </svg>

      {levelMeta.some((l) => !l.inView) ? (
        <ul className="bil-learning-level-legend">
          {levelMeta
            .filter((l) => !l.inView)
            .map((l) => (
              <li key={l.key}>{l.offScreenHint}</li>
            ))}
        </ul>
      ) : null}
    </div>
  )
}
