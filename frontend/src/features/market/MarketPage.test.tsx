import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  compareProvinces,
  getMarketDomains,
  getMarketOverview,
} from '../../api/analytics'
import { AppProvider } from '../../shared/context/AppContext'
import { MarketPage } from './MarketPage'

vi.mock('recharts', () => ({
  ResponsiveContainer: ({ children }: { children: unknown }) => children,
  BarChart: ({ children }: { children: unknown }) => children,
  CartesianGrid: () => null,
  XAxis: () => null,
  YAxis: () => null,
  Tooltip: () => null,
  Bar: () => null,
}))

vi.mock('../../api/analytics', () => ({
  compareProvinces: vi.fn(),
  getMarketDomains: vi.fn(),
  getMarketOverview: vi.fn(),
}))

const renderPage = () => {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <AppProvider><MarketPage /></AppProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

describe('MarketPage', () => {
  beforeEach(() => {
    vi.mocked(getMarketOverview).mockResolvedValue({
      collected_entity_count: 4000,
      mapped_entity_count: 1973,
      excluded_entity_count: 47,
      unreviewed_entity_count: 1980,
      entity_count_with_sentiment_evidence: 2531,
      entity_count_with_aspect_evidence: 2466,
      mapping_coverage_ratio: 0.49325,
      sentiment_entity_coverage_ratio: 0.63275,
      aspect_entity_coverage_ratio: 0.6165,
      review_evidence_count: 8749,
      aspect_mention_count: 32137,
      support_status: 'insufficient_data',
    })
    vi.mocked(getMarketDomains).mockResolvedValue([
      domain('destination', 1722, 7842, 28640),
      domain('accommodation', null, null, null, 'no_data'),
      domain('food_dining', 108, 520, 2215),
      domain('transportation', 143, 387, 1282),
    ])
    vi.mocked(compareProvinces).mockResolvedValue([{
      province_id: 'vn-province:4f858848735ff411ba23b4bf',
      domain_id: 'destination',
      metric_id: 'entity_count',
      unit: 'entities',
      count: 294,
      support_status: 'insufficient_data',
    }])
  })

  it('renders domain and comparison counts without fake trends', async () => {
    renderPage()
    expect(await screen.findAllByText('1.722')).not.toHaveLength(0)
    expect(screen.getByText('7.842')).toBeTruthy()
    expect(screen.getByText('28.640')).toBeTruthy()
    expect(screen.getByText('294')).toBeTruthy()
    expect(screen.queryByText('Xu hướng cảm xúc')).toBeNull()
  })

  it('renders accommodation as no data instead of zero', async () => {
    renderPage()
    const select = await screen.findByLabelText('Lĩnh vực')
    fireEvent.change(select, { target: { value: 'accommodation' } })
    expect(await screen.findAllByText('Không có dữ liệu')).not.toHaveLength(0)
  })
})

function domain(
  domain_id: 'destination' | 'accommodation' | 'food_dining' | 'transportation',
  entity_count: number | null,
  review_evidence_count: number | null,
  aspect_mention_count: number | null,
  support_status: 'insufficient_data' | 'no_data' = 'insufficient_data',
) {
  return {
    domain_id,
    entity_count,
    entity_count_with_sentiment_evidence: entity_count,
    entity_count_with_aspect_evidence: entity_count,
    review_evidence_count,
    positive_review_count: entity_count,
    neutral_review_count: entity_count,
    negative_review_count: entity_count,
    aspect_mention_count,
    support_status,
  }
}
