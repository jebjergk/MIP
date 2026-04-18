import { useEffect } from 'react'
import { useLocation } from 'react-router-dom'
import { useAskMipRuntime } from '../context/AskMipRuntimeContext'

const CLEAR = {
  page_id: null,
  visible_widget_ids: [],
  selected_widget_id: null,
  current_kpi_snapshot: null,
  current_badges_or_statuses: [],
  active_filters: {},
  selected_row_context: null,
  selected_card_context: null,
}

/**
 * Register Ask MIP runtime context for the current page (page_id + widgets + optional extras).
 * Clears on unmount or when pageId changes.
 *
 * @param {string} pageId - Matches MIP knowledge page_contract page_id
 * @param {string[]} visibleWidgetIds - Stable widget_ids; use same strings in knowledge widget_contracts
 * @param {Record<string, unknown>|null} extra - Optional portfolio_id, session_mode, filters, etc.
 */
export function useAskMipPageRuntime(pageId, visibleWidgetIds = [], extra = null) {
  const { mergeAskMipRuntime } = useAskMipRuntime()
  const { pathname } = useLocation()
  const widKey = visibleWidgetIds.join('|')
  const extraKey = extra && typeof extra === 'object' ? JSON.stringify(extra) : ''

  useEffect(() => {
    mergeAskMipRuntime({
      page_id: pageId,
      visible_widget_ids: visibleWidgetIds,
      ...(extra && typeof extra === 'object' ? extra : {}),
    })
    return () => mergeAskMipRuntime(CLEAR)
  }, [pageId, pathname, mergeAskMipRuntime, widKey, extraKey])
}
