function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

function isTransientNetworkError(e) {
  const msg = String(e?.message || e || '')
  return /fail|fetch|network|reset|aborted|timeout|ECONNRESET|ECONNREFUSED/i.test(msg)
}

/**
 * fetch() with retries for dev/proxy flakes (API reload, ECONNRESET through Vite proxy).
 * Retries on thrown network errors and on optional HTTP statuses (default 502/503/504).
 */
export async function fetchWithRetry(url, init = {}, options = {}) {
  const retries = options.retries ?? 4
  const backoffMs = options.backoffMs ?? 500
  const retryStatuses = options.retryStatuses ?? [502, 503, 504]
  let lastErr
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const resp = await fetch(url, init)
      if (!resp.ok && attempt < retries && retryStatuses.includes(resp.status)) {
        await sleep(backoffMs * (attempt + 1))
        continue
      }
      return resp
    } catch (e) {
      lastErr = e
      if (attempt < retries && isTransientNetworkError(e)) {
        await sleep(backoffMs * (attempt + 1))
        continue
      }
      throw e
    }
  }
  throw lastErr
}
