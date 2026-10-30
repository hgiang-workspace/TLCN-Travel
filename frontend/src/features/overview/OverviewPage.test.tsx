import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { getDataCoverage, getMarketOverview } from '../../api/analytics'
import { AppProvider } from '../../shared/context/AppContext'
import { OverviewPage } from './OverviewPage'

vi.mock('../../api/analytics', () => ({
  getMarketOverview: vi.fn(),
  getDataCoverage: vi.fn(),
}))

const renderPage = () => {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <AppProvider><OverviewPage /></AppProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

describe('OverviewPage', () => {
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
    vi.mocked(getDataCoverage).mockResolvedValue([{
      province_id: 'vn-province:4f858848735ff411ba23b4bf',
      collected_entity_count: 547,
      mapped_entity_count: 339,
      excluded_entity_count: 7,
      unreviewed_entity_count: 201,
      entity_count_with_sentiment_evidence: 426,
      entity_count_with_aspect_evidence: 415,
      mapping_coverage_ratio: 0.619744,
      sentiment_entity_coverage_ratio: 0.778793,
      aspect_entity_coverage_ratio: 0.758684,
      support_status: 'insufficient_data',
    }])
  })

  it('renders real overview and coverage values with limitations', async () => {
    renderPage()
    expect(await screen.findByText('4.000')).toBeTruthy()
    expect(screen.getByText('8.749')).toBeTruthy()
    expect(screen.getByText('32.137')).toBeTruthy()
    expect(screen.getByText('62%')).toBeTruthy()
    expect(screen.getByText(/chưa đủ cơ sở để đại diện/i)).toBeTruthy()
  })

  it('shows the existing loading state while API requests are pending', () => {
    vi.mocked(getMarketOverview).mockReturnValue(new Promise(() => undefined))
    vi.mocked(getDataCoverage).mockReturnValue(new Promise(() => undefined))
    renderPage()
    expect(screen.getByText('Đang tải dữ liệu…')).toBeTruthy()
  })
})
