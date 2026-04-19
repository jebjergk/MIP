/**
 * Committee 2.0 — evidence-only intraday chart (Expected vs observed today).
 */
import { useId } from 'react'

function fmtNum(v, digits = 4) {
  if (v == null) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

export default function IntradaySubstantiationMapCard({ exhibit, variant = 'lpa' }) {
  const gradId = useId().replace(/:/g, '')
  if (exhibit == null || typeof exhibit !== 'object') return null
  const bars = Array.isArray(exhibit.bars) ? exhibit.bars : []
  if (bars.length < 2) return null

  const root = variant === 'lpa' ? 'lpa-c2-ism' : 'sch-ism'
  const caps = exhibit.captions || {}
  const ovs = exhibit.overlays || {}
  const zone = ovs.entry_zone || {}
  const inv = ovs.invalidation || {}
  const wsr = ovs.window_start_range || {}
  const verdict = String(caps.verdict_bucket || 'MIXED').toUpperCase()

  const zl = Number(zone.low)
  const zh = Number(zone.high)
  const invLv = inv.level != null ? Number(inv.level) : NaN
  const lastPx = Number(ovs.last_price)
  const wsl = Number(wsr.low)
  const wsh = Number(wsr.high)

  const closes = bars.map((b) => Number(b.c)).filter((x) => Number.isFinite(x))
  const highs = bars.map((b) => Number(b.h != null ? b.h : b.c)).filter((x) => Number.isFinite(x))
  const lows = bars.map((b) => Number(b.l != null ? b.l : b.c)).filter((x) => Number.isFinite(x))
  const vols = bars.map((b) => (b.v != null ? Number(b.v) : 0))

  let yLo = Math.min(...lows, ...closes)
  let yHi = Math.max(...highs, ...closes)
  if (Number.isFinite(zl)) yLo = Math.min(yLo, zl)
  if (Number.isFinite(zh)) yHi = Math.max(yHi, zh)
  if (Number.isFinite(invLv)) {
    yLo = Math.min(yLo, invLv)
    yHi = Math.max(yHi, invLv)
  }
  if (Number.isFinite(wsl) && Number.isFinite(wsh)) {
    yLo = Math.min(yLo, wsl, wsh)
    yHi = Math.max(yHi, wsl, wsh)
  }
  const padY = (yHi - yLo) * 0.08 || Math.abs(lastPx) * 0.002 || 0.01
  yLo -= padY
  yHi += padY
  const ySpan = yHi - yLo || 1

  const W = variant === 'lpa' ? 300 : 360
  const H = variant === 'lpa' ? 158 : 172
  const volH = 34
  const padL = 6
  const padR = 6
  const padTop = 2
  const priceH = H - volH - padTop - 10
  const innerW = W - padL - padR

  const normX = (i) => padL + (bars.length <= 1 ? 0 : (innerW * i) / (bars.length - 1))
  const normY = (p) => padTop + priceH - ((Number(p) - yLo) / ySpan) * priceH

  const maxVol = Math.max(...vols, 1)
  const volY0 = padTop + priceH + 6

  let dLine = ''
  bars.forEach((b, i) => {
    const c = Number(b.c)
    if (!Number.isFinite(c)) return
    const x = normX(i)
    const y = normY(c)
    dLine += `${i === 0 ? 'M' : 'L'}${x},${y} `
  })

  const pm = exhibit.proposal_marker
  const propX = pm != null && pm.bar_index != null ? normX(Math.min(Math.max(0, Number(pm.bar_index)), bars.length - 1)) : null

  const markerEls = (exhibit.markers || []).map((m, idx) => {
    const bi = Number(m.bar_index)
    if (!Number.isFinite(bi) || bi < 0 || bi >= bars.length) return null
    const c = Number(bars[bi].c)
    if (!Number.isFinite(c)) return null
    return (
      <circle
        key={`${m.kind}-${idx}`}
        className={`${root}__dot`}
        cx={normX(bi)}
        cy={normY(c)}
        r={4}
        title={m.label || m.kind}
      />
    )
  })

  const zoneY1 = Number.isFinite(zl) ? normY(zl) : null
  const zoneY2 = Number.isFinite(zh) ? normY(zh) : null
  const invY = Number.isFinite(invLv) ? normY(invLv) : null
  const lastY = Number.isFinite(lastPx) ? normY(lastPx) : null

  return (
    <div className={`${root} ${root}--verdict-${verdict.toLowerCase()}`} data-verdict={verdict}>
      <div className={`${root}__title`}>{exhibit.headline || 'Expected vs observed today'}</div>
      <p className={`${root}__expect`}>{caps.proposal_expectation || ''}</p>
      <div className={`${root}__badge-row`}>
        <span className={`${root}__badge`}>{caps.session_behavior_badge || '—'}</span>
        <span className={`${root}__meta`}>
          {exhibit.interval_minutes || 15}m · {exhibit.meta?.bar_count != null ? `${exhibit.meta.bar_count} bars` : ''}
        </span>
      </div>
      <p className={`${root}__interpret`}>{caps.interpretation_line || ''}</p>
      <p className={`${root}__verdict`}>{caps.trader_verdict_line || ''}</p>

      <svg className={`${root}__svg`} viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Intraday price and volume vs proposal">
        <defs>
          <linearGradient id={`${root}-zone`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#5c6bc0" stopOpacity="0.12" />
            <stop offset="100%" stopColor="#5c6bc0" stopOpacity="0.04" />
          </linearGradient>
        </defs>

        {Number.isFinite(wsl) && Number.isFinite(wsh) ? (
          <rect
            x={padL}
            y={Math.min(normY(wsh), normY(wsl))}
            width={innerW}
            height={Math.abs(normY(wsl) - normY(wsh)) || 1}
            className={`${root}__wsr`}
            title={ovs.window_start_label || 'Window start (first bar)'}
          />
        ) : null}

        {zoneY1 != null && zoneY2 != null ? (
          <rect
            x={padL}
            y={Math.min(zoneY1, zoneY2)}
            width={innerW}
            height={Math.max(2, Math.abs(zoneY2 - zoneY1))}
            fill={`url(#${root}-zone-${gradId})`}
            className={`${root}__zone`}
          />
        ) : null}

        {invY != null ? (
          <line x1={padL} y1={invY} x2={W - padR} y2={invY} className={`${root}__inv ${inv.breached ? 'is-breach' : ''}`} />
        ) : null}

        {bars.map((b, i) => {
          const v = vols[i] || 0
          const bw = Math.max(1.2, innerW / bars.length - 1)
          const x = normX(i) - bw / 2
          const vh = (v / maxVol) * (volH - 4)
          return <rect key={i} x={x} y={volY0 + (volH - 4 - vh)} width={bw} height={vh} className={`${root}__vol`} />
        })}

        {dLine ? <path d={dLine} className={`${root}__line`} fill="none" /> : null}

        {lastY != null ? (
          <line x1={padL} y1={lastY} x2={W - padR} y2={lastY} className={`${root}__last`} strokeDasharray="4 3" />
        ) : null}

        {propX != null ? <line x1={propX} y1={padTop} x2={propX} y2={volY0 + volH} className={`${root}__prop`} /> : null}

        {markerEls}
      </svg>

      <div className={`${root}__legend`}>
        <span>
          <i className={`${root}__swatch ${root}__swatch--zone`} /> Zone
        </span>
        <span>
          <i className={`${root}__swatch ${root}__swatch--inv`} /> Inv {fmtNum(invLv, 4)}
        </span>
        <span>
          <i className={`${root}__swatch ${root}__swatch--last`} /> Last {fmtNum(lastPx, 4)}
        </span>
      </div>
    </div>
  )
}
