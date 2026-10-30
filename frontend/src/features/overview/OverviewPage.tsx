import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router'
import { getDataCoverage, getMarketOverview } from '../../api/analytics'
import { provinceName } from '../../data/provinces'
import { KpiCard, PageIntro, PanelTitle, StateView } from '../../shared/components/UI'
import { useAppContext } from '../../shared/context/AppContext'
import { formatCount, formatPercent } from '../../shared/format'

const domains = [
  ['Điểm đến', 'Địa điểm tham quan', 'destination'],
  ['Lưu trú', 'Chưa có dữ liệu domain', 'stay'],
  ['Ẩm thực', 'Nhà hàng, quán ăn', 'food'],
  ['Di chuyển', 'Nhà xe, tuyến vận tải', 'transport'],
]

export function OverviewPage() {
  const { geo } = useAppContext()
  const overview = useQuery({
    queryKey: ['market-overview'],
    queryFn: getMarketOverview,
  })
  const coverage = useQuery({
    queryKey: ['data-coverage', geo.provinceId],
    queryFn: () => getDataCoverage([geo.provinceId]),
  })
  const loading = overview.isPending || coverage.isPending
  const error = overview.isError || coverage.isError
  const retry = () => {
    void overview.refetch()
    void coverage.refetch()
  }
  if (loading || error) {
    return (
      <div>
        <PageIntro title="Tổng quan" subtitle="Dữ liệu Gold có giới hạn và có thể kiểm chứng" page="01" />
        <div className="mt-10">
          <StateView state={loading ? 'loading' : 'error'} onRetry={retry} />
        </div>
      </div>
    )
  }
  if (!overview.data) return <StateView state="empty" />
  const data = overview.data
  const provinceCoverage = coverage.data?.[0]
  const kpis = [
    { label: 'Thực thể đã thu thập', value: formatCount(data.collected_entity_count), note: 'TripAdvisor · dữ liệu quan sát' },
    { label: 'Đã ánh xạ lĩnh vực', value: formatCount(data.mapped_entity_count), note: formatPercent(data.mapping_coverage_ratio) },
    { label: 'Review có bằng chứng', value: formatCount(data.review_evidence_count), note: 'Không phải hoạt động thị trường hiện tại' },
    { label: 'Lượt đề cập khía cạnh', value: formatCount(data.aspect_mention_count), note: 'Không phải số review riêng biệt' },
  ]
  const coverageRows = [
    ['Đã thu thập', provinceCoverage?.collected_entity_count ?? null],
    ['Đã ánh xạ', provinceCoverage?.mapped_entity_count ?? null],
    ['Bị loại trừ', provinceCoverage?.excluded_entity_count ?? null],
    ['Chưa đánh giá', provinceCoverage?.unreviewed_entity_count ?? null],
    ['Có sentiment', provinceCoverage?.entity_count_with_sentiment_evidence ?? null],
    ['Có khía cạnh', provinceCoverage?.entity_count_with_aspect_evidence ?? null],
  ] as const
  return (
    <div>
      <PageIntro title="Tổng quan" subtitle="Dữ liệu Gold có giới hạn và có thể kiểm chứng" page="01" />
      <div className="mt-10">
        <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {kpis.map((item) => <KpiCard item={item} key={item.label} />)}
        </section>
        <section className="mt-4 grid gap-4 lg:grid-cols-[1.8fr_1fr]">
          <div className="panel p-[18px]">
            <PanelTitle title="Các lĩnh vực du lịch" subtitle="Chuyển sang khám phá hoặc phân tích theo domain" />
            <div className="mt-8 grid gap-3 sm:grid-cols-2">
              {domains.map(([title, description, id], index) => (
                <article className={`rounded-xl p-4 ${index === 0 ? 'bg-brand-soft' : 'bg-canvas'}`} key={id}>
                  <h3 className="text-[17px] font-semibold">{title}</h3>
                  <p className="mt-2 text-xs text-muted">{description}</p>
                  <Link className="mt-2 block text-right text-[11px] font-medium text-brand" to={`/explore?domain=${id}`}>Khám phá ↗</Link>
                </article>
              ))}
            </div>
          </div>
          <div className="panel p-[18px]">
            <PanelTitle title={`Độ phủ · ${provinceName(geo.provinceId)}`} subtitle="Độ phủ dữ liệu ≠ đại diện thị trường" />
            {provinceCoverage?.support_status === 'no_data' ? <div className="mt-5"><StateView state="empty" /></div> : (
              <div className="mt-6 space-y-3 text-xs">
                {coverageRows.map(([label, value]) => <div className="flex justify-between" key={label}><span className="text-muted">{label}</span><strong>{formatCount(value)}</strong></div>)}
                <div className="border-t border-line pt-3"><span className="text-muted">Tỷ lệ ánh xạ</span><strong className="float-right text-brand">{formatPercent(provinceCoverage?.mapping_coverage_ratio ?? null)}</strong></div>
              </div>
            )}
          </div>
        </section>
        <section className="panel mt-4 p-5">
          <PanelTitle title="Minh bạch dữ liệu" subtitle="Dữ liệu quan sát từ TripAdvisor; chưa đủ cơ sở để đại diện cho toàn thị trường." />
          <div className="mt-4 grid gap-2 text-xs text-muted sm:grid-cols-2">
            <span>Chưa ánh xạ: {formatCount(data.unreviewed_entity_count)}</span>
            <span>Loại trừ: {formatCount(data.excluded_entity_count)}</span>
            <span>Thực thể có sentiment: {formatCount(data.entity_count_with_sentiment_evidence)}</span>
            <span>Thực thể có khía cạnh: {formatCount(data.entity_count_with_aspect_evidence)}</span>
          </div>
        </section>
      </div>
      <p className="mt-7 text-[10px] text-muted">Nguồn Gold · Trạng thái hỗ trợ: chưa đủ dữ liệu</p>
    </div>
  )
}
