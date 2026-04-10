/**
 * Subtle Plotly shapes from Tape observer overlay_hints (Phase 2).
 */

function toDate(ms) {
  if (ms == null || !Number.isFinite(Number(ms))) return null
  return new Date(Number(ms))
}

/**
 * @param {Array<Record<string, unknown>> | null | undefined} hints
 * @returns {object[]}
 */
export function tapeOverlayShapes(hints) {
  if (!Array.isArray(hints) || hints.length === 0) return []
  const shapes = []

  for (const h of hints) {
    const kind = String(h?.kind || '')
    const x0 = toDate(h.t_start_ms)
    const x1 = toDate(h.t_end_ms)
    if (!x0 || !x1) continue

    if (kind === 'burst_zone') {
      shapes.push({
        type: 'rect',
        xref: 'x',
        yref: 'paper',
        x0,
        x1,
        y0: 0.3,
        y1: 1,
        fillcolor: 'rgba(251, 191, 36, 0.06)',
        line: { width: 0 },
        layer: 'below',
      })
    } else if (kind === 'vacuum') {
      shapes.push({
        type: 'rect',
        xref: 'x',
        yref: 'paper',
        x0,
        x1,
        y0: 0.3,
        y1: 1,
        fillcolor: 'rgba(56, 189, 248, 0.07)',
        line: { width: 0 },
        layer: 'below',
      })
    } else if (kind === 'absorption_band') {
      const lo = Number(h.price_low)
      const hi = Number(h.price_high)
      if (!Number.isFinite(lo) || !Number.isFinite(hi)) continue
      shapes.push({
        type: 'rect',
        xref: 'x',
        yref: 'y',
        x0,
        x1,
        y0: lo,
        y1: hi,
        fillcolor: 'rgba(167, 139, 250, 0.12)',
        line: { color: 'rgba(167, 139, 250, 0.35)', width: 1 },
        layer: 'below',
      })
    } else if (kind === 'exhaustion') {
      shapes.push({
        type: 'rect',
        xref: 'x',
        yref: 'paper',
        x0,
        x1,
        y0: 0.3,
        y1: 1,
        fillcolor: 'rgba(148, 163, 184, 0.06)',
        line: { width: 0 },
        layer: 'below',
      })
    }
  }

  return shapes
}
