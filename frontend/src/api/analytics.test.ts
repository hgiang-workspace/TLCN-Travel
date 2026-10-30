import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  AnalyticsApiError,
  compareProvinces,
  getDataCoverage,
  getMarketOverview,
} from './analytics'

afterEach(() => vi.unstubAllGlobals())

const jsonResponse = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })

describe('analytics API', () => {
  it('parses overview and coverage responses', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ collected_entity_count: 4000 }))
      .mockResolvedValueOnce(jsonResponse([{ mapping_coverage_ratio: 0.5 }]))
    vi.stubGlobal('fetch', fetchMock)

    await expect(getMarketOverview()).resolves.toMatchObject({
      collected_entity_count: 4000,
    })
    await expect(getDataCoverage(['vn-province:test'])).resolves.toEqual([
      { mapping_coverage_ratio: 0.5 },
    ])
    expect(String(fetchMock.mock.calls[1][0])).toContain(
      'province_id=vn-province%3Atest',
    )
  })

  it('preserves province order and sends supported comparison filters', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse([]))
    vi.stubGlobal('fetch', fetchMock)
    await compareProvinces({
      provinceIds: ['province-b', 'province-a'],
      metricId: 'review_evidence_count',
      domainId: 'destination',
    })
    const url = new URL(String(fetchMock.mock.calls[0][0]))
    expect(url.pathname).toBe('/api/v1/market/provinces/compare')
    expect(url.searchParams.getAll('province_id')).toEqual([
      'province-b',
      'province-a',
    ])
    expect(url.searchParams.get('metric_id')).toBe('review_evidence_count')
    expect(url.searchParams.get('domain_id')).toBe('destination')
  })

  it('returns safe client errors for unavailable APIs', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({}, 503)))
    await expect(getMarketOverview()).rejects.toEqual(
      expect.objectContaining<Partial<AnalyticsApiError>>({
        message: 'Dịch vụ phân tích tạm thời chưa sẵn sàng.',
        status: 503,
      }),
    )
  })
})
