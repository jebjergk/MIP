import React, { useMemo, useCallback } from 'react'
import {
  ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Scatter, ReferenceLine,
  ReferenceArea, ReferenceDot,
} from 'recharts'

const STATE_COLORS = {
  BREAKOUT_EXPANSION: '#26a69a',
  TREND_UP: '#66bb6a',
  PULLBACK_IN_UPTREND: '#a5d6a7',
  PULLBACK_IN_TREND: '#a5d6a7',
  RANGE_BOUND: '#9e9e9e',
  RANGE_CONTRACTION: '#bdbdbd',
  PULLBACK_IN_DOWNTREND: '#ef9a9a',
  TREND_DOWN: '#ef5350',
  BREAKDOWN: '#c62828',
  RECOVERY: '#42a5f5',
  REVERSAL_FORMING: '#7e57c2',
  FAILED_MOVE: '#ff7043',
}

const LEVEL_COLORS = {
  SUPPORT: '#26a69a',
  RESISTANCE: '#ef5350',
  SUPPORT_ZONE: 'rgba(38,166,154,0.10)',
  RESISTANCE_ZONE: 'rgba(239,83,80,0.10)',
}

/** HH / HL / LH / LL only — SH/SL omitted to reduce noise (hover still lists from MSM when needed). */
const MAJOR_SWING_LABEL_TYPES = new Set(['HH', 'HL', 'LH', 'LL'])
const SWING_HIGH_CHAIN_TYPES = new Set(['HH', 'LH'])
const SWING_LOW_CHAIN_TYPES = new Set(['HL', 'LL'])

/** Prefer interpretability.major_swings_full (all pivots in MSM window), else major_swings tail. */
function msmSwingArray(msm) {
  if (!msm || typeof msm !== 'object') return []
  const interp = msm.interpretability
  const full = interp?.major_swings_full
  if (Array.isArray(full) && full.length > 0) return full
  const tail = msm.major_swings
  return Array.isArray(tail) ? tail : []
}

function truthyFlag(v) {
  if (v === true || v === 1) return true
  if (typeof v === 'string') return v.toLowerCase() === 'true'
  return false
}

function fmtDate(d) {
  if (!d) return ''
  return String(d).slice(0, 10)
}

function fmtPrice(v) {
  if (v == null || isNaN(v)) return ''
  if (v >= 100) return v.toFixed(0)
  if (v >= 10) return v.toFixed(1)
  return v.toFixed(2)
}

/** Reject mega-ranges that read like “whole chart” rather than a horizontal bracket. */
const MAX_CONSOLIDATION_BAND_PCT = 38

/**
 * Chart consolidation bracket from MSM swings: overlap between the last two HH/LH highs
 * (min of those prices = tighter ceiling) and last two HL/LL lows (max = tighter floor).
 * Only drawn when that overlap exists (top > bottom) and the band is narrow enough.
 */
function computeSwingConsolidationBracket(msm, snapStart, firstBarDate, lastBarDate) {
  if (!msm || !firstBarDate || !lastBarDate) return null
  const swings = msmSwingArray(msm)
  const normalized = swings.map((sw) => {
    const swingType = (sw.swing_type != null ? String(sw.swing_type) : '').toUpperCase()
    if (!MAJOR_SWING_LABEL_TYPES.has(swingType)) return null
    const date = snapStart(fmtDate(sw.date))
    const price = Number(sw.price)
    if (Number.isNaN(price) || !date || date < firstBarDate || date > lastBarDate) return null
    return { date, price, type: swingType }
  }).filter(Boolean)

  const byDate = (a, b) => a.date.localeCompare(b.date)
  const highs = normalized.filter(s => SWING_HIGH_CHAIN_TYPES.has(s.type)).sort(byDate)
  const lows = normalized.filter(s => SWING_LOW_CHAIN_TYPES.has(s.type)).sort(byDate)
  if (!highs.length || !lows.length) return null

  let top
  let bot
  let x1

  if (highs.length >= 2 && lows.length >= 2) {
    const hA = highs[highs.length - 2]
    const hB = highs[highs.length - 1]
    const lA = lows[lows.length - 2]
    const lB = lows[lows.length - 1]
    top = Math.min(hA.price, hB.price)
    bot = Math.max(lA.price, lB.price)
    if (top <= bot) return null
    const pivotDates = [hA.date, hB.date, lA.date, lB.date]
    x1 = pivotDates.reduce((acc, d) => (d < acc ? d : acc))
  } else {
    const h = highs[highs.length - 1]
    const l = lows[lows.length - 1]
    if (h.price <= l.price) return null
    top = h.price
    bot = l.price
    x1 = h.date < l.date ? h.date : l.date
  }

  const mid = (top + bot) / 2
  const bandPct = mid > 0 ? ((top - bot) / mid) * 100 : 100
  if (bandPct > MAX_CONSOLIDATION_BAND_PCT) return null

  return { x1, x2: lastBarDate, y1: bot, y2: top }
}

function structureRuleCopy(latestStructureEvent) {
  const ev = latestStructureEvent != null ? String(latestStructureEvent) : ''
  if (ev.startsWith('WICK_PROBE')) return 'Wick probe only — no confirmed break'
  if (ev.startsWith('BODY_CLOSE_BOS')) return 'Body-close structure break'
  return null
}

function CandlestickShape(props) {
  const { x, y, width, height, payload } = props
  if (x == null || y == null || width == null || height == null || !payload) return null
  const { open, high, low, close } = payload
  if ([high, low, open, close].some(v => v == null)) return null

  const cx = x + width / 2
  const wickTop = y
  const wickBottom = y + Math.max(height, 1)
  const valueRange = high - low
  const isUp = close >= open
  const color = isUp ? '#26a69a' : '#ef5350'

  let openY = wickBottom; let closeY = wickBottom
  if (Math.abs(valueRange) > 1e-12) {
    openY = wickTop + ((high - open) / valueRange) * (wickBottom - wickTop)
    closeY = wickTop + ((high - close) / valueRange) * (wickBottom - wickTop)
  }
  const bodyTop = Math.min(openY, closeY)
  const bodyHeight = Math.max(Math.abs(openY - closeY), 1)
  const bodyWidth = Math.max(Math.min(width * 0.78, 8), 4)
  const bodyLeft = cx - bodyWidth / 2

  return (
    <g>
      <line x1={cx} x2={cx} y1={wickTop} y2={wickBottom} stroke="#888" strokeWidth={1} shapeRendering="crispEdges" />
      <rect x={bodyLeft} y={bodyTop} width={bodyWidth} height={bodyHeight} fill={color} stroke={color} strokeWidth={1} shapeRendering="crispEdges" />
    </g>
  )
}

function SetupMarkerShape(props) {
  const { cx, cy, payload } = props
  if (cx == null || cy == null || !payload) return null
  const dir = (payload.setupDir || '').toUpperCase()
  const hasProposal = payload.setupHasProposal
  const isLong = dir === 'LONG'
  const fill = hasProposal ? '#f97316' : (isLong ? '#1a73e8' : '#d93025')
  const size = 5
  const points = isLong
    ? `${cx},${cy - size} ${cx - size},${cy + size} ${cx + size},${cy + size}`
    : `${cx},${cy + size} ${cx - size},${cy - size} ${cx + size},${cy - size}`
  return <polygon points={points} fill={fill} stroke="#fff" strokeWidth={1} />
}

function ProposalMarkerShape(props) {
  const { cx, cy } = props
  if (cx == null || cy == null) return null
  return (
    <g>
      <circle cx={cx} cy={cy} r={6} fill="#f97316" stroke="#fff" strokeWidth={1.5} />
      <text x={cx} y={cy + 1} textAnchor="middle" dominantBaseline="middle" fontSize={8} fill="#fff" fontWeight="bold">P</text>
    </g>
  )
}

function MonitorMarkerShape(props) {
  const { cx, cy } = props
  if (cx == null || cy == null) return null
  return (
    <g>
      <rect x={cx - 7} y={cy - 7} width={14} height={14} rx={3} fill="#f59e0b" stroke="#fff" strokeWidth={1.5} />
      <text x={cx} y={cy + 1} textAnchor="middle" dominantBaseline="middle" fontSize={7} fill="#fff" fontWeight="bold">M</text>
    </g>
  )
}

function BosMarkerShape(props) {
  const { cx, cy, payload } = props
  if (cx == null || cy == null || !payload) return null
  const dir = (payload.bosDir || '').toUpperCase()
  const up = dir === 'UP'
  const color = up ? '#16a34a' : '#dc2626'
  const isWick = !!payload.bosWickStyle
  const size = 7
  const pts = up
    ? `${cx},${cy - size} ${cx - size},${cy + size * 0.6} ${cx + size},${cy + size * 0.6}`
    : `${cx},${cy + size} ${cx - size},${cy - size * 0.6} ${cx + size},${cy - size * 0.6}`
  return (
    <g>
      <polygon
        points={pts}
        fill={isWick ? 'transparent' : color}
        stroke={color}
        strokeWidth={isWick ? 2 : 1}
        strokeDasharray={isWick ? '3 2' : ''}
      />
      <text x={cx} y={(up ? cy + size + 10 : cy - size - 4)} textAnchor="middle" fontSize={9} fill={color} fontWeight={600}>
        {up ? 'BOS↑' : 'BOS↓'}
      </text>
    </g>
  )
}

function ChochMarkerShape(props) {
  const { cx, cy, payload } = props
  if (cx == null || cy == null || !payload) return null
  const dir = (payload.chochDir || '').toUpperCase()
  const up = dir === 'UP'
  const color = '#9333ea'
  const size = 6
  return (
    <g>
      <polygon
        points={`${cx},${cy - size} ${cx + size},${cy} ${cx},${cy + size} ${cx - size},${cy}`}
        fill="#faf5ff"
        stroke={color}
        strokeWidth={2}
      />
      <text x={cx} y={cy + size + 12} textAnchor="middle" fontSize={9} fill={color} fontWeight={600}>
        {up ? 'CHOCH↑' : 'CHOCH↓'}
      </text>
    </g>
  )
}

function SwingMarkerShape(props) {
  const { cx, cy, payload } = props
  if (cx == null || cy == null || !payload) return null
  const label = payload.swingLabel || ''
  const showPsych = !!payload.showPsychChip
  return (
    <g>
      <circle cx={cx} cy={cy} r={4} fill="#475569" stroke="#fff" strokeWidth={1} />
      {label ? (
        <text x={cx + 6} y={cy - 4} fontSize={9} fill="#334155" fontWeight={600}>{label}</text>
      ) : null}
      {showPsych ? (
        <text x={cx + 6} y={cy + 8} fontSize={7} fill="#64748b">wick</text>
      ) : null}
    </g>
  )
}

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload || !payload.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  return (
    <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 4, padding: '6px 10px', fontSize: '0.8rem', maxWidth: 280 }}>
      <div style={{ fontWeight: 600 }}>{label}</div>
      {d.open != null && <div>O: {fmtPrice(d.open)} H: {fmtPrice(d.high)} L: {fmtPrice(d.low)} C: {fmtPrice(d.close)}</div>}
      {d.STRUCTURAL_STATE && <div style={{ color: '#6c757d' }}>State: {d.STRUCTURAL_STATE.replace(/_/g, ' ')}</div>}
      {d.msmLatestStructureEvent && (
        <div style={{ fontSize: '0.72rem', color: '#64748b', marginTop: 4 }}>
          Structure event: {String(d.msmLatestStructureEvent).replace(/_/g, ' ')}
        </div>
      )}
      {d.setupEvents && d.setupEvents.length > 0 && (
        <div style={{ marginTop: 4, borderTop: '1px solid #eee', paddingTop: 4 }}>
          {d.setupEvents.map((s, i) => (
            <div key={i} style={{ fontSize: '0.75rem', color: '#333' }}>
              {(s.SETUP_FAMILY || s.setup_family || '').replace(/_/g, ' ')} ({s.DIRECTION || s.direction})
              {(s.BECAME_PROPOSAL || s.became_proposal) ? ' → Proposal' : ''}
            </div>
          ))}
        </div>
      )}
      {d.proposalEvents && d.proposalEvents.length > 0 && (
        <div style={{ marginTop: 4, color: '#f97316', fontWeight: 600, fontSize: '0.75rem' }}>
          Proposal: {(d.proposalEvents[0].SETUP_FAMILY || d.proposalEvents[0].setup_family || '').replace(/_/g, ' ')}
        </div>
      )}
      {d.boardMarkerEvents && d.boardMarkerEvents.length > 0 && (
        <div style={{ marginTop: 4, color: '#d97706', fontWeight: 600, fontSize: '0.72rem' }}>
          Monitor: {d.boardMarkerEvents.map((m, i) => (
            <span key={i}>{i ? '; ' : ''}{(m.FINAL_ACTION || m.final_action || '').replace(/_/g, ' ')}</span>
          ))}
        </div>
      )}
      {d.swingTooltipLines && d.swingTooltipLines.length > 0 && (
        <div style={{ marginTop: 4, borderTop: '1px solid #eee', paddingTop: 4, fontSize: '0.72rem', color: '#475569' }}>
          {d.swingTooltipLines.map((t, i) => <div key={i}>{t}</div>)}
        </div>
      )}
    </div>
  )
}

export default function StlChart({
  bars, levels, setups, proposals, boardMarkers = [], marketStructureMap,
  overlays,
  chartMode, selectedSetupId, onSelectSetup, get,
}) {
  const msm = marketStructureMap && typeof marketStructureMap === 'object' ? marketStructureMap : null
  const latestStructureEvent = msm?.latest_structure_event != null ? String(msm.latest_structure_event) : ''
  const structureNote = overlays.bosChoch ? structureRuleCopy(latestStructureEvent) : null

  const chartData = useMemo(() => {
    if (!bars.length) return []

    const setupsByDate = {}
    setups.forEach(s => {
      const d = fmtDate(get(s, 'SETUP_DATE'))
      if (!setupsByDate[d]) setupsByDate[d] = []
      setupsByDate[d].push(s)
    })

    const proposalsByDate = {}
    proposals.forEach(p => {
      const d = fmtDate(get(p, 'PROPOSAL_CREATED_AT') || get(p, 'SETUP_DATE'))
      if (!proposalsByDate[d]) proposalsByDate[d] = []
      proposalsByDate[d].push(p)
    })

    const boardByDate = {}
    ;(boardMarkers || []).forEach(m => {
      const d = fmtDate(get(m, 'MARKER_DATE'))
      if (!boardByDate[d]) boardByDate[d] = []
      boardByDate[d].push(m)
    })

    const lastBarDate = fmtDate(get(bars[bars.length - 1], 'BAR_DATE'))

    proposals.forEach(p => {
      const d = fmtDate(get(p, 'PROPOSAL_CREATED_AT') || get(p, 'SETUP_DATE'))
      if (d > lastBarDate) {
        if (!proposalsByDate[lastBarDate]) proposalsByDate[lastBarDate] = []
        if (!proposalsByDate[lastBarDate].includes(p)) proposalsByDate[lastBarDate].push(p)
      }
    })

    ;(boardMarkers || []).forEach(m => {
      const d = fmtDate(get(m, 'MARKER_DATE'))
      if (d > lastBarDate) {
        if (!boardByDate[lastBarDate]) boardByDate[lastBarDate] = []
        const arr = boardByDate[lastBarDate]
        const fa = get(m, 'FINAL_ACTION')
        if (!arr.some(x => get(x, 'FINAL_ACTION') === fa)) arr.push(m)
      }
    })

    const msmSwingsList = msm ? msmSwingArray(msm) : []
    const swingsOnDay = (targetDate) => msmSwingsList.filter(sw => fmtDate(sw?.date) === targetDate)

    return bars.map(b => {
      const date = fmtDate(get(b, 'BAR_DATE'))
      const open = Number(get(b, 'OPEN'))
      const high = Number(get(b, 'HIGH'))
      const low = Number(get(b, 'LOW'))
      const close = Number(get(b, 'CLOSE'))
      const barSetups = setupsByDate[date] || []
      const barProposals = proposalsByDate[date] || []
      const barBoard = boardByDate[date] || []

      const firstSetup = barSetups[0]
      const setupDir = firstSetup ? (get(firstSetup, 'DIRECTION') || '') : ''
      const setupHasProposal = barSetups.some(s => get(s, 'BECAME_PROPOSAL'))

      const spread = Math.max(high - low, Math.abs(high) * 0.001 || 0.01)

      const daySwings = swingsOnDay(date)
      const swingTooltipLines = daySwings.map(sw => {
        const st = (sw.swing_type != null ? String(sw.swing_type) : '').toUpperCase()
        const px = sw.price != null ? fmtPrice(Number(sw.price)) : ''
        const wick = sw.wick_probe ? ' · wick-heavy pivot' : ''
        const body = sw.body_confirmed === false ? ' · weak body close' : ''
        return `${st} @ ${px}${wick}${body}`
      })

      return {
        date,
        open, high, low, close,
        wick: [low, high],
        STRUCTURAL_STATE: get(b, 'STRUCTURAL_STATE'),
        VOL_REGIME: get(b, 'VOL_REGIME'),
        TREND_REGIME: get(b, 'TREND_REGIME'),
        stateColor: STATE_COLORS[get(b, 'STRUCTURAL_STATE')] || '#e0e0e0',
        stateVal: 1,
        setupMarker: barSetups.length > 0 ? (setupDir === 'LONG' ? low - spread * 0.12 : high + spread * 0.12) : null,
        setupDir,
        setupHasProposal,
        setupEvents: barSetups,
        proposalMarker: barProposals.length > 0 ? high + spread * 0.18 : null,
        proposalEvents: barProposals,
        monitorMarker: overlays.monitorMarkers && barBoard.length > 0 ? low - spread * 0.28 : null,
        boardMarkerEvents: overlays.monitorMarkers ? barBoard : [],
        msmLatestStructureEvent: latestStructureEvent || null,
        swingTooltipLines: swingTooltipLines.length ? swingTooltipLines : null,
      }
    })
  }, [bars, setups, proposals, boardMarkers, overlays.monitorMarkers, msm, latestStructureEvent, get])

  const selectedSetup = useMemo(() => {
    if (!selectedSetupId) return null
    return setups.find(s => get(s, 'SETUP_EVENT_ID') === selectedSetupId)
  }, [setups, selectedSetupId, get])

  const chartDates = useMemo(() => chartData.map(d => d.date), [chartData])
  const firstBarDate = chartDates[0]
  const lastBarDate = chartDates[chartDates.length - 1]
  const chartDateSet = useMemo(() => new Set(chartDates), [chartDates])

  const snapStart = useCallback((d) => {
    if (!d) return firstBarDate
    if (chartDateSet.has(d)) return d
    for (let i = 0; i < chartDates.length; i++) {
      if (chartDates[i] >= d) return chartDates[i]
    }
    return lastBarDate
  }, [chartDates, chartDateSet, firstBarDate, lastBarDate])

  const snapEnd = useCallback((d) => {
    if (!d) return lastBarDate
    if (chartDateSet.has(d)) return d
    for (let i = chartDates.length - 1; i >= 0; i--) {
      if (chartDates[i] <= d) return chartDates[i]
    }
    return firstBarDate
  }, [chartDates, chartDateSet, firstBarDate, lastBarDate])

  const visibleLevels = useMemo(() => {
    if (!overlays.levels && !overlays.zones && !selectedSetup) return []
    if (!levels.length || !firstBarDate || !lastBarDate) return []

    const seen = new Map()
    levels.forEach(l => {
      const first = fmtDate(get(l, 'FIRST_TOUCH_DATE'))
      const last = fmtDate(get(l, 'LAST_TOUCH_DATE'))
      if (last && last < firstBarDate) return
      if (first && first > lastBarDate) return

      const price = Number(get(l, 'LEVEL_PRICE') || 0)
      const rounded = Math.round(price * 10) / 10
      const key = `${get(l, 'LEVEL_TYPE')}|${rounded}`
      const existing = seen.get(key)
      const sig = Number(get(l, 'LEVEL_SIGNIFICANCE') || 0)
      if (!existing || sig > Number(get(existing, 'LEVEL_SIGNIFICANCE') || 0)) {
        seen.set(key, l)
      }
    })

    return [...seen.values()]
      .sort((a, b) => Number(get(b, 'LEVEL_SIGNIFICANCE') || 0) - Number(get(a, 'LEVEL_SIGNIFICANCE') || 0))
      .slice(0, 12)
  }, [levels, overlays.levels, overlays.zones, selectedSetup, get, firstBarDate, lastBarDate])

  const entryZoneSetups = useMemo(() => {
    const result = []
    if (overlays.entryZones) {
      setups.forEach(s => {
        const status = (get(s, 'SETUP_STATUS') || '').toUpperCase()
        if (status === 'DETECTED' || status === 'ELIGIBLE') {
          const lo = Number(get(s, 'ENTRY_ZONE_LOW'))
          const hi = Number(get(s, 'ENTRY_ZONE_HIGH'))
          if (lo && hi && lo !== hi) result.push(s)
        }
      })
    }
    if (selectedSetup && !result.find(s => get(s, 'SETUP_EVENT_ID') === selectedSetupId)) {
      result.push(selectedSetup)
    }
    return result
  }, [setups, overlays.entryZones, selectedSetup, selectedSetupId, get])

  const invalidationSetups = useMemo(() => {
    const result = []
    if (overlays.invalidationLines) {
      setups.forEach(s => {
        const status = (get(s, 'SETUP_STATUS') || '').toUpperCase()
        if (status === 'DETECTED' || status === 'ELIGIBLE') {
          const inv = Number(get(s, 'PRICE_INVALIDATION_LEVEL'))
          if (inv) result.push(s)
        }
      })
    }
    if (selectedSetup && !result.find(s => get(s, 'SETUP_EVENT_ID') === selectedSetupId)) {
      const inv = Number(get(selectedSetup, 'PRICE_INVALIDATION_LEVEL'))
      if (inv) result.push(selectedSetup)
    }
    return result
  }, [setups, overlays.invalidationLines, selectedSetup, selectedSetupId, get])

  const swingScatterPts = useMemo(() => {
    if (!overlays.swingStructure || !msm || !chartDates.length) return []
    const swings = msmSwingArray(msm)
    const perDateIdx = {}
    const pts = []
    swings.forEach(sw => {
      const swingType = (sw.swing_type != null ? String(sw.swing_type) : '').toUpperCase()
      if (!MAJOR_SWING_LABEL_TYPES.has(swingType)) return
      const rawD = fmtDate(sw.date)
      const date = snapStart(rawD)
      const bar = chartData.find(c => c.date === date)
      const price = Number(sw.price)
      if (!bar || price == null || isNaN(price)) return
      const spread = Math.max(bar.high - bar.low, Math.abs(bar.high) * 0.001 || 0.01)
      perDateIdx[date] = (perDateIdx[date] || 0) + 1
      const idx = perDateIdx[date] - 1
      const stagger = (idx % 4) * spread * 0.07
      const swingY = price + stagger
      const showPsychChip = !!overlays.candlePsychology && !!sw.wick_probe
      pts.push({
        date,
        swingY,
        swingLabel: swingType,
        showPsychChip,
      })
    })
    return pts
  }, [overlays.swingStructure, overlays.candlePsychology, msm, chartData, chartDates.length, snapStart])

  const swingConnectorSegments = useMemo(() => {
    if (!overlays.swingStructure || !msm || !chartDates.length) return { highs: [], lows: [] }
    const swings = msmSwingArray(msm)
    const byDateSort = (a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0)
    const normalized = swings.map((sw) => {
      const swingType = (sw.swing_type != null ? String(sw.swing_type) : '').toUpperCase()
      if (!MAJOR_SWING_LABEL_TYPES.has(swingType)) return null
      const date = snapStart(fmtDate(sw.date))
      const price = Number(sw.price)
      if (Number.isNaN(price)) return null
      if (date < firstBarDate || date > lastBarDate) return null
      return { date, price, type: swingType }
    }).filter(Boolean)

    const highs = normalized.filter(s => SWING_HIGH_CHAIN_TYPES.has(s.type)).sort(byDateSort)
    const lows = normalized.filter(s => SWING_LOW_CHAIN_TYPES.has(s.type)).sort(byDateSort)
    const highSegs = []
    for (let i = 0; i < highs.length - 1; i++) highSegs.push([highs[i], highs[i + 1]])
    const lowSegs = []
    for (let i = 0; i < lows.length - 1; i++) lowSegs.push([lows[i], lows[i + 1]])
    return { highs: highSegs, lows: lowSegs }
  }, [overlays.swingStructure, msm, chartDates.length, snapStart, firstBarDate, lastBarDate])

  const bosScatterPts = useMemo(() => {
    if (!overlays.bosChoch || !msm?.bos || !chartDates.length) return []
    const bos = msm.bos
    const dir = (bos.last_bos_direction != null ? String(bos.last_bos_direction) : 'NONE').toUpperCase()
    if (dir === 'NONE' || bos.last_bos_date == null || bos.last_bos_level == null) return []
    const date = snapStart(fmtDate(bos.last_bos_date))
    const lv = Number(bos.last_bos_level)
    if (isNaN(lv)) return []
    const bosWickStyle = latestStructureEvent.startsWith('WICK_PROBE')
    return [{ date, bosY: lv, bosDir: dir, bosWickStyle }]
  }, [overlays.bosChoch, msm, chartDates.length, snapStart, latestStructureEvent])

  const chochScatterPts = useMemo(() => {
    if (!overlays.bosChoch || !msm?.choch || !chartDates.length) return []
    const ch = msm.choch
    if (!truthyFlag(ch.detected)) return []
    const dir = (ch.direction != null ? String(ch.direction) : 'NONE').toUpperCase()
    if (dir === 'NONE' || ch.date == null || ch.level == null) return []
    const date = snapStart(fmtDate(ch.date))
    const lv = Number(ch.level)
    if (isNaN(lv)) return []
    const bar = chartData.find(c => c.date === date)
    const spread = bar ? Math.max(bar.high - bar.low, Math.abs(bar.high) * 0.001 || 0.01) : lv * 0.002
    return [{ date, chochY: lv + spread * 0.03, chochDir: dir }]
  }, [overlays.bosChoch, msm, chartData, chartDates.length, snapStart])

  const consolidationGeom = useMemo(() => {
    if (!overlays.consolidation || !msm || !chartDates.length) return null

    const swingBracket = computeSwingConsolidationBracket(msm, snapStart, firstBarDate, lastBarDate)
    if (swingBracket) return swingBracket

    const c = msm.consolidation || {}
    const active = !!(c.active ?? c.ACTIVE)
    const lo = Number(c.range_low)
    const hi = Number(c.range_high)
    if (!active || Number.isNaN(lo) || Number.isNaN(hi)) return null
    const top = Math.max(lo, hi)
    const bot = Math.min(lo, hi)
    const mid = (top + bot) / 2
    const widthPct = mid > 0 ? ((top - bot) / mid) * 100 : 100
    if (widthPct > 30) return null
    let x1 = firstBarDate
    let x2 = lastBarDate
    const sd = msm.start_date != null ? fmtDate(msm.start_date) : null
    const ad = msm.anchor_date != null ? fmtDate(msm.anchor_date) : null
    if (sd || ad) {
      const sx = sd ? snapStart(sd) : firstBarDate
      const ex = ad ? snapEnd(ad) : lastBarDate
      if (sx && ex && sx <= ex) {
        x1 = sx
        x2 = ex
      }
    }
    return { x1, x2, y1: bot, y2: top }
  }, [overlays.consolidation, msm, chartDates.length, firstBarDate, lastBarDate, snapStart, snapEnd])

  const impulseBand = useMemo(() => {
    if (!overlays.impulseCorrectionShade || !msm?.impulse_correction || !chartDates.length) return null
    const ic = msm.impulse_correction
    const s = ic.last_impulse_start != null ? fmtDate(ic.last_impulse_start) : null
    const e = ic.last_impulse_end != null ? fmtDate(ic.last_impulse_end) : null
    if (!s && !e) return null
    const x1 = s ? snapStart(s) : firstBarDate
    const x2 = e ? snapEnd(e) : lastBarDate
    if (!x1 || !x2 || x1 > x2) return null
    return { x1, x2 }
  }, [overlays.impulseCorrectionShade, msm, chartDates.length, firstBarDate, lastBarDate, snapStart, snapEnd])

  if (!chartData.length) return null

  const yValues = chartData.flatMap(d => [d.high, d.low].filter(v => v != null && !isNaN(v)))
  let yMin = Math.min(...yValues) * 0.995
  let yMax = Math.max(...yValues) * 1.005
  const expandForMsm = (price) => {
    if (price == null || isNaN(price)) return
    yMin = Math.min(yMin, price * 0.998)
    yMax = Math.max(yMax, price * 1.002)
  }
  bosScatterPts.forEach(p => expandForMsm(p.bosY))
  chochScatterPts.forEach(p => expandForMsm(p.chochY))
  swingScatterPts.forEach(p => expandForMsm(p.swingY))
  swingConnectorSegments.highs.forEach(([a, b]) => {
    expandForMsm(a.price)
    expandForMsm(b.price)
  })
  swingConnectorSegments.lows.forEach(([a, b]) => {
    expandForMsm(a.price)
    expandForMsm(b.price)
  })
  if (consolidationGeom) {
    expandForMsm(consolidationGeom.y1)
    expandForMsm(consolidationGeom.y2)
  }

  const badgePrimary = msm?.primary_structure != null ? String(msm.primary_structure) : ''
  const badgeHealth = msm?.structure_health != null ? String(msm.structure_health) : ''
  const badgePhase = msm?.current_phase != null ? String(msm.current_phase) : ''
  const badgeLine = [badgePrimary, badgeHealth, badgePhase].filter(Boolean).join(' · ')
  const postureHint = msm?.structure_posture_hint != null ? String(msm.structure_posture_hint) : ''

  return (
    <div className="stl-chart-wrap">
      {badgeLine ? (
        <div className="stl-structure-badge-row">
          <div className="stl-structure-badge">{badgeLine}</div>
          {postureHint ? (
            <div className="stl-structure-posture-hint" title="Evidence-only posture hint (not operational_state)">{postureHint}</div>
          ) : null}
        </div>
      ) : null}

      {structureNote ? (
        <div className="stl-structure-rule-note">{structureNote}</div>
      ) : null}

      <div className="stl-chart-legend">
        <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#1a73e8' }} /> Long setup</span>
        <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#d93025' }} /> Short setup</span>
        <span className="stl-legend-item" title="Published structural proposal">
          <span className="stl-legend-swatch" style={{ background: '#f97316', borderRadius: '50%' }} /> Proposal (P)
        </span>
        {overlays.monitorMarkers && (
          <span className="stl-legend-item" title="Phase 4 watch / monitor verdict">
            <span className="stl-legend-swatch stl-legend-swatch--monitor" /> Monitor (M)
          </span>
        )}
        {overlays.swingStructure && (
          <>
            <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#475569', borderRadius: '50%' }} /> Swing pivot</span>
            <span className="stl-legend-item" title="Consecutive swing highs (HH → LH → …)">
              <span className="stl-legend-swatch stl-legend-swatch--dash-pink" /> HH / LH chain
            </span>
            <span className="stl-legend-item" title="Consecutive swing lows (HL → LL → …)">
              <span className="stl-legend-swatch stl-legend-swatch--dash-teal" /> HL / LL chain
            </span>
          </>
        )}
        {overlays.bosChoch && (
          <>
            <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#16a34a' }} /> BOS↑</span>
            <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#dc2626' }} /> BOS↓</span>
            <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ border: '2px solid #9333ea', background: '#faf5ff' }} /> CHOCH</span>
          </>
        )}
        {overlays.consolidation && consolidationGeom && (
          <span
            className="stl-legend-item"
            title="Shaded band between recent HH/LH highs and HL/LL lows (overlap pocket), not the full MSM window high/low"
          >
            <span className="stl-legend-swatch" style={{ background: 'rgba(100,116,139,0.35)' }} /> Consolidation
          </span>
        )}
        {overlays.levels && <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#26a69a' }} /> Support</span>}
        {overlays.levels && <span className="stl-legend-item"><span className="stl-legend-swatch" style={{ background: '#ef5350' }} /> Resistance</span>}
      </div>

      {(overlays.swingStructure || overlays.consolidation) && msm ? (
        <p className="stl-msm-swing-footnote">
          {overlays.swingStructure ? (
            <>
              MSM swing pivots are computed on Snowflake over roughly the <strong>last 90 calendar days</strong> of daily bars.
              Longer chart ranges only show markers inside that window (not the full 6-month history).
              <span className="stl-msm-swing-footnote__sep"> </span>
              Pink dashed segments link consecutive <strong>HH / LH</strong> highs; teal dashed segments link consecutive <strong>HL / LL</strong> lows.
            </>
          ) : null}
          {overlays.consolidation ? (
            <>
              {overlays.swingStructure ? <span className="stl-msm-swing-footnote__sep"> </span> : null}
              <strong>Consolidation</strong> shading is the overlap between the last two swing highs (HH/LH) and last two swing lows (HL/LL)—a horizontal bracket—not the min/max of the whole MSM window.
            </>
          ) : null}
        </p>
      ) : null}

      <ResponsiveContainer width="100%" height={420}>
        <ComposedChart data={chartData} margin={{ top: 10, right: 10, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="date" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
          <YAxis
            domain={[yMin, yMax]}
            tick={{ fontSize: 10 }}
            width={60}
            tickFormatter={fmtPrice}
          />
          <Tooltip content={<ChartTooltip />} />

          {impulseBand && (
            <ReferenceArea
              x1={impulseBand.x1}
              x2={impulseBand.x2}
              y1={yMin}
              y2={yMax}
              fill="rgba(147,51,234,0.06)"
              strokeOpacity={0}
            />
          )}

          {consolidationGeom && (
            <ReferenceArea
              x1={consolidationGeom.x1}
              x2={consolidationGeom.x2}
              y1={consolidationGeom.y1}
              y2={consolidationGeom.y2}
              fill="rgba(100,116,139,0.28)"
              stroke="#64748b"
              strokeWidth={1}
              strokeDasharray="4 3"
              strokeOpacity={0.5}
            />
          )}

          {(overlays.levels || selectedSetup) && visibleLevels.map(l => {
            const price = Number(get(l, 'LEVEL_PRICE'))
            const lType = (get(l, 'LEVEL_TYPE') || '').toUpperCase()
            const color = lType.includes('RESIST') ? '#ef5350' : '#26a69a'
            const first = snapStart(fmtDate(get(l, 'FIRST_TOUCH_DATE')))
            const last = snapEnd(fmtDate(get(l, 'LAST_TOUCH_DATE')))
            const isLive = last === lastBarDate
            return (
              <ReferenceLine
                key={get(l, 'LEVEL_ID') || `${lType}-${price}`}
                stroke={color}
                strokeDasharray="4 3"
                strokeOpacity={isLive ? 0.7 : 0.35}
                strokeWidth={isLive ? 1.2 : 0.8}
                segment={[{ x: first, y: price }, { x: last, y: price }]}
                ifOverflow="extendDomain"
              />
            )
          })}

          {overlays.zones && visibleLevels.filter(l => get(l, 'LEVEL_LOW') && get(l, 'LEVEL_HIGH')).map(l => {
            const lType = (get(l, 'LEVEL_TYPE') || '').toUpperCase()
            const fill = lType.includes('RESIST') ? LEVEL_COLORS.RESISTANCE_ZONE : LEVEL_COLORS.SUPPORT_ZONE
            const first = snapStart(fmtDate(get(l, 'FIRST_TOUCH_DATE')))
            const last = snapEnd(fmtDate(get(l, 'LAST_TOUCH_DATE')))
            return (
              <ReferenceArea
                key={`zone-${get(l, 'LEVEL_ID')}`}
                x1={first}
                x2={last}
                y1={Number(get(l, 'LEVEL_LOW'))}
                y2={Number(get(l, 'LEVEL_HIGH'))}
                fill={fill}
                strokeOpacity={0}
              />
            )
          })}

          {entryZoneSetups.map(s => {
            const lo = Number(get(s, 'ENTRY_ZONE_LOW'))
            const hi = Number(get(s, 'ENTRY_ZONE_HIGH'))
            const isSelected = get(s, 'SETUP_EVENT_ID') === selectedSetupId
            if (!lo || !hi || lo === hi) return null
            const start = snapStart(fmtDate(get(s, 'SETUP_DATE')))
            const end = snapEnd(fmtDate(get(s, 'EXPIRY_DATE')))
            return (
              <ReferenceArea
                key={`ez-${get(s, 'SETUP_EVENT_ID')}`}
                x1={start}
                x2={end}
                y1={Math.min(lo, hi)}
                y2={Math.max(lo, hi)}
                fill={isSelected ? 'rgba(26,115,232,0.22)' : 'rgba(26,115,232,0.10)'}
                stroke={isSelected ? '#1a73e8' : 'none'}
                strokeDasharray={isSelected ? '4 2' : ''}
                strokeOpacity={0.5}
              />
            )
          })}

          {invalidationSetups.map(s => {
            const inv = Number(get(s, 'PRICE_INVALIDATION_LEVEL'))
            const isSelected = get(s, 'SETUP_EVENT_ID') === selectedSetupId
            if (!inv) return null
            const start = snapStart(fmtDate(get(s, 'SETUP_DATE')))
            const end = snapEnd(fmtDate(get(s, 'EXPIRY_DATE')))
            return (
              <ReferenceLine
                key={`inv-${get(s, 'SETUP_EVENT_ID')}`}
                stroke="#d93025"
                strokeDasharray="6 3"
                strokeWidth={isSelected ? 1.5 : 0.8}
                strokeOpacity={isSelected ? 1 : 0.4}
                segment={[{ x: start, y: inv }, { x: end, y: inv }]}
                ifOverflow="extendDomain"
                label={isSelected ? { value: 'Invalidation', position: 'right', fontSize: 10, fill: '#d93025' } : undefined}
              />
            )
          })}

          {overlays.swingStructure && swingConnectorSegments.lows.map((seg, i) => (
            <ReferenceLine
              key={`sw-low-${i}`}
              segment={[{ x: seg[0].date, y: seg[0].price }, { x: seg[1].date, y: seg[1].price }]}
              stroke="#0d9488"
              strokeWidth={1.25}
              strokeDasharray="5 4"
              strokeOpacity={0.65}
              ifOverflow="visible"
            />
          ))}
          {overlays.swingStructure && swingConnectorSegments.highs.map((seg, i) => (
            <ReferenceLine
              key={`sw-high-${i}`}
              segment={[{ x: seg[0].date, y: seg[0].price }, { x: seg[1].date, y: seg[1].price }]}
              stroke="#be185d"
              strokeWidth={1.25}
              strokeDasharray="5 4"
              strokeOpacity={0.65}
              ifOverflow="visible"
            />
          ))}

          {chartMode === 'candle' ? (
            <Bar dataKey="wick" barSize={6} shape={<CandlestickShape />} isAnimationActive={false} />
          ) : (
            <>
              <Line type="monotone" dataKey="close" stroke="#3366cc" strokeWidth={1.5} dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="high" stroke="#ccc" strokeWidth={0.5} dot={false} strokeDasharray="2 2" isAnimationActive={false} />
              <Line type="monotone" dataKey="low" stroke="#ccc" strokeWidth={0.5} dot={false} strokeDasharray="2 2" isAnimationActive={false} />
            </>
          )}

          {overlays.swingStructure && swingScatterPts.map((pt, i) => (
            <ReferenceDot
              key={`swing-${pt.date}-${i}`}
              x={pt.date}
              y={pt.swingY}
              r={0}
              fill="none"
              stroke="none"
              ifOverflow="visible"
              zIndex={620}
              shape={dotProps => <SwingMarkerShape {...dotProps} payload={pt} />}
            />
          ))}
          {overlays.bosChoch && bosScatterPts.map(pt => (
            <ReferenceDot
              key={`bos-${pt.date}-${pt.bosDir}`}
              x={pt.date}
              y={pt.bosY}
              r={0}
              fill="none"
              stroke="none"
              ifOverflow="visible"
              zIndex={625}
              shape={dotProps => <BosMarkerShape {...dotProps} payload={pt} />}
            />
          ))}
          {overlays.bosChoch && chochScatterPts.map(pt => (
            <ReferenceDot
              key={`choch-${pt.date}-${pt.chochDir}`}
              x={pt.date}
              y={pt.chochY}
              r={0}
              fill="none"
              stroke="none"
              ifOverflow="visible"
              zIndex={625}
              shape={dotProps => <ChochMarkerShape {...dotProps} payload={pt} />}
            />
          ))}

          {overlays.setupMarkers && (
            <Scatter
              dataKey="setupMarker"
              shape={<SetupMarkerShape />}
              isAnimationActive={false}
              onClick={(data) => {
                const evts = data?.setupEvents || data?.payload?.setupEvents
                if (evts && evts.length > 0) {
                  const id = get(evts[0], 'SETUP_EVENT_ID')
                  if (id) onSelectSetup(id)
                }
              }}
            />
          )}

          {overlays.proposalMarkers && (
            <Scatter dataKey="proposalMarker" shape={<ProposalMarkerShape />} isAnimationActive={false} />
          )}
          {overlays.monitorMarkers && (
            <Scatter dataKey="monitorMarker" shape={<MonitorMarkerShape />} isAnimationActive={false} />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {overlays.stateStrip && (
        <div className="stl-state-strip" style={{ paddingLeft: 60, paddingRight: 10 }}>
          <div
            className="stl-state-ribbon"
            style={{
              display: 'flex',
              width: '100%',
              height: 12,
              borderRadius: 2,
              overflow: 'hidden',
            }}
          >
            {chartData.map((d, i) => (
              <div
                key={i}
                title={`${d.date}: ${(d.STRUCTURAL_STATE || '—').replace(/_/g, ' ')}`}
                style={{
                  flex: '1 1 0',
                  minWidth: 0,
                  background: d.stateColor,
                }}
              />
            ))}
          </div>
        </div>
      )}

      {overlays.regimeStrip && (
        <div style={{ fontSize: '0.72rem', color: '#6c757d', padding: '2px 8px', display: 'flex', gap: 12 }}>
          <span>Latest regime: {chartData.length > 0 ? (chartData[chartData.length - 1].VOL_REGIME || '—').replace(/_/g, ' ') : '—'}</span>
          <span>Trend: {chartData.length > 0 ? (chartData[chartData.length - 1].TREND_REGIME || '—').replace(/_/g, ' ') : '—'}</span>
        </div>
      )}
    </div>
  )
}
