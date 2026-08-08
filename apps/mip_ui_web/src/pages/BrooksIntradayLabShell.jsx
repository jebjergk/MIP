import { useCallback, useEffect, useMemo, useState } from 'react'

import { API_BASE } from '../config/apiBase'

import { BrooksIntradayTechnicalView } from './BrooksIntradayLab'

import BrooksIntradayLearningView from './BrooksIntradayLearningView'

import BrooksIntradayReviewSelector from './BrooksIntradayReviewSelector'

import BrooksIntradayLabToolbar from './BrooksIntradayLabToolbar'

import BrooksIntradayValidationLauncher, {

  selectionAfterValidation,

} from './BrooksIntradayValidationLauncher'

import {

  catalogErrorFromHttp,

  catalogHasRuns,

  emptyCatalogMessage,

  normalizeReviewCatalogPayload,

} from './brooksLabReviewCatalog'

import {

  isDiagnosticLegacyFromSearchParams,

  reviewCatalogUrl,

} from './brooksLabDiagnosticMode'

import {

  parseReviewSelectionFromSearchParams,

  selectionToReviewProps,

  syncReviewSelectionToUrl,

} from './brooksLabReviewSelection'

import './BrooksIntradayLab.css'



const API = `${API_BASE}/research/brooks-intraday`



function viewFromLocation() {

  if (typeof window === 'undefined') return 'learning'

  const v = new URLSearchParams(window.location.search).get('view')

  return v === 'technical' ? 'technical' : 'learning'

}



export default function BrooksIntradayLabShell() {

  const [view, setView] = useState(viewFromLocation)

  const [catalog, setCatalog] = useState(null)

  const [catalogLoading, setCatalogLoading] = useState(true)

  const [catalogError, setCatalogError] = useState('')

  const [selection, setSelection] = useState(null)

  const [urlWarnings, setUrlWarnings] = useState([])

  const [validationOpen, setValidationOpen] = useState(false)



  const loadCatalog = useCallback(async () => {

    setCatalogLoading(true)

    setCatalogError('')

    try {

      const params = new URLSearchParams(window.location.search)

      const diagnosticLegacy = isDiagnosticLegacyFromSearchParams(params)

      const resp = await fetch(reviewCatalogUrl(API, { diagnosticLegacy }))

      let data = {}

      try {

        data = await resp.json()

      } catch {

        data = {}

      }

      if (!resp.ok) {

        throw new Error(catalogErrorFromHttp(resp.status, data))

      }

      const normalized = normalizeReviewCatalogPayload(data)

      if (!normalized) {

        throw new Error('Review catalog response was missing or malformed (expected runs array).')

      }

      const emptyMsg = emptyCatalogMessage(normalized)

      if (emptyMsg) {

        setCatalog(normalized)

        throw new Error(emptyMsg)

      }

      setCatalog(normalized)

      const { selection: sel, warnings } = parseReviewSelectionFromSearchParams(params, normalized)

      if (!sel) {

        throw new Error('Could not derive review selection from catalog.')

      }

      setSelection(sel)

      setUrlWarnings(warnings)

      syncReviewSelectionToUrl(sel, { view: viewFromLocation() })

      return normalized

    } catch (e) {

      setCatalogError(e.message || 'Could not load review catalog')

      return null

    } finally {

      setCatalogLoading(false)

    }

  }, [])



  useEffect(() => {

    loadCatalog()

  }, [loadCatalog])



  const review = useMemo(

    () => (selection ? selectionToReviewProps(selection) : null),

    [selection],

  )



  const catalogReady = catalogHasRuns(catalog) && !catalogError && selection

  const adviserFoundationOnly = Boolean(selection?.adviserFoundationOnly || (

    catalog?.catalog_mode === 'adviser_foundation' && !catalog?.diagnostic_legacy

  ))



  const switchView = useCallback((next) => {

    setView(next)

    if (selection) syncReviewSelectionToUrl(selection, { view: next })

  }, [selection])



  const onSelectionChange = useCallback((next) => {

    setSelection(next)

    syncReviewSelectionToUrl(next, { view })

  }, [view])



  const onBarTsChange = useCallback((barTs) => {

    setSelection((prev) => {

      if (!prev) return prev

      const next = { ...prev, barTs: barTs || null }

      syncReviewSelectionToUrl(next, { view })

      return next

    })

  }, [view])



  const onValidationCompleted = useCallback(async (result) => {
    try {
      const params = new URLSearchParams(window.location.search)
      const diagnosticLegacy = isDiagnosticLegacyFromSearchParams(params)
      const resp = await fetch(reviewCatalogUrl(API, { diagnosticLegacy }))
      const data = await resp.json()
      const normalized = normalizeReviewCatalogPayload(data)
      if (normalized) setCatalog(normalized)
      const sel = normalized ? selectionAfterValidation(normalized, result) : null
      if (sel) {
        setSelection(sel)
        syncReviewSelectionToUrl(sel, { view: 'learning' })
        setView('learning')
      }
    } catch {
      await loadCatalog()
    }
    setValidationOpen(false)
  }, [loadCatalog])



  return (

    <div className="bil-shell bil-shell--toolbar">

      {catalogReady ? (

        <BrooksIntradayLabToolbar

          catalog={catalog}

          selection={selection}

          view={view}

          onViewChange={switchView}

          onSelectionChange={onSelectionChange}

          onNewValidation={() => setValidationOpen(true)}

          adviserFoundationOnly={adviserFoundationOnly}

        />

      ) : null}



      {catalogLoading ? (

        <p className="bil-note bil-review-catalog-status" role="status">Loading review catalog…</p>

      ) : null}



      {!catalogLoading && catalogError ? (

        <div className="bil-error bil-review-catalog-status" role="alert">

          <p>{`Could not load review catalog: ${catalogError}`}</p>

          <button type="button" className="primary" onClick={loadCatalog}>Retry catalog</button>

        </div>

      ) : null}



      {catalogReady && !adviserFoundationOnly ? (

        <BrooksIntradayReviewSelector

          catalog={catalog}

          selection={selection}

          onSelectionChange={onSelectionChange}

          hideLegacyChains={false}

        />

      ) : null}



      {catalogReady && adviserFoundationOnly && !selection?.contextAttemptId ? (

        <p className="bil-note bil-adviser-empty" role="status">

          {catalog?.adviser_empty_state || 'Select or run a V1.0 validation session.'}

        </p>

      ) : null}



      {catalogReady && !adviserFoundationOnly && catalog?.amzn_v1_status ? (

        <p className="bil-note bil-adviser-v1-amzn-note" role="note">{catalog.amzn_v1_status}</p>

      ) : null}



      {urlWarnings.length ? (

        <div className="bil-note bil-review-url-warn" role="status">

          {urlWarnings.map((w) => (

            <p key={w}>{w}</p>

          ))}

        </div>

      ) : null}



      {view === 'learning' && review?.contextAttemptId ? (

        <BrooksIntradayLearningView

          review={review}

          selectedBarTs={selection?.barTs}

          onSelectedBarTsChange={onBarTsChange}

          onOpenTechnicalView={() => switchView('technical')}

          compactV1={adviserFoundationOnly}

        />

      ) : null}

      {view === 'technical' && review?.contextAttemptId ? (

        <BrooksIntradayTechnicalView

          sharedReview={review}

          sharedBarTs={selection?.barTs}

          onSharedReviewChange={onSelectionChange}

          onSharedBarTsChange={onBarTsChange}

          shellControlled

        />

      ) : null}



      <BrooksIntradayValidationLauncher

        open={validationOpen}

        onClose={() => setValidationOpen(false)}

        onCompleted={onValidationCompleted}

        defaultRunId={selection?.runId}

      />

    </div>

  )

}


