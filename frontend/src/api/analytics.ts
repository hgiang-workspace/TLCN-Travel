import type {
  CoverageItem,
  MarketDomain,
  MarketOverview,
  ProvinceComparison,
  ProvinceComparisonRequest,
} from '../types/analytics'

const API_BASE_URL = (
  import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000'
).replace(/\/$/, '')

export class AnalyticsApiError extends Error {
  readonly status?: number

  constructor(
    message: string,
    status?: number,
  ) {
    super(message)
    this.status = status
  }
}

async function getJson<T>(path: string, params?: URLSearchParams): Promise<T> {
  const query = params?.size ? `?${params.toString()}` : ''
  let response: Response
  try {
    response = await fetch(`${API_BASE_URL}${path}${query}`, {
      headers: { Accept: 'application/json' },
    })
  } catch {
    throw new AnalyticsApiError('Không thể kết nối tới API phân tích.')
  }
  if (!response.ok) {
    throw new AnalyticsApiError(
      response.status === 503
        ? 'Dịch vụ phân tích tạm thời chưa sẵn sàng.'
        : 'Yêu cầu dữ liệu không hợp lệ.',
      response.status,
    )
  }
  return (await response.json()) as T
}

export function getMarketOverview(): Promise<MarketOverview> {
  return getJson('/api/v1/market/overview')
}

export function getMarketDomains(): Promise<MarketDomain[]> {
  return getJson('/api/v1/market/domains')
}

export function compareProvinces(
  request: ProvinceComparisonRequest,
): Promise<ProvinceComparison[]> {
  const params = new URLSearchParams()
  request.provinceIds.forEach((id) => params.append('province_id', id))
  params.set('metric_id', request.metricId)
  if (request.domainId) params.set('domain_id', request.domainId)
  return getJson('/api/v1/market/provinces/compare', params)
}

export function getDataCoverage(provinceIds?: string[]): Promise<CoverageItem[]> {
  const params = new URLSearchParams()
  provinceIds?.forEach((id) => params.append('province_id', id))
  return getJson('/api/v1/data/coverage', params)
}
