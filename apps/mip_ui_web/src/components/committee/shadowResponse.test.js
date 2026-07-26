import { describe, expect, it } from 'vitest'
import { normalizeShadowBoardResponse } from './shadowResponse'

describe('shadowResponse literature support', () => {
  it('normalizes compact support and methodologist effect', () => {
    const normalized = normalizeShadowBoardResponse({
      status: 'COMPLETE',
      shadow_stance: 'WAIT_RECLAIM',
      literature_support: {
        enabled: true,
        status: 'USED',
        cards: [{ card_id: 'approved-1', snippet: 'Wait for confirmation.' }],
      },
      chair: {
        methodologist_effect: {
          used: true,
          effect: 'SUPPORTED_WAIT',
          summary: 'The Price Action & Trading Methodologist supports waiting for reclaim.',
        },
      },
    })
    expect(normalized.literatureSupport.status).toBe('USED')
    expect(normalized.literatureSupport.cards).toHaveLength(1)
    expect(normalized.methodologistEffect.effect).toBe('SUPPORTED_WAIT')
  })

  it('defaults to disabled and no material effect', () => {
    const normalized = normalizeShadowBoardResponse({ status: 'COMPLETE' })
    expect(normalized.literatureSupport.status).toBe('DISABLED')
    expect(normalized.methodologistEffect).toEqual({
      used: false,
      effect: 'NO_MATERIAL_EFFECT',
      summary: '',
    })
  })
})
