import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { compareProvinces, getMarketDomains, getMarketOverview } from '../../api/analytics'
import { provinceName, provinceOptions } from '../../data/provinces'
import { KpiCard, PageIntro, PanelTitle, StateView } from '../../shared/components/UI'
import { useAppContext } from '../../shared/context/AppContext'
import { formatCount } from '../../shared/format'
import type { ApiDomainId, MarketMetricId } from '../../types/analytics'

const domainLabels: Record<ApiDomainId, string> = {
  destination: 'Điểm đến',
  accommodation: 'Lưu trú',
  food_dining: 'Ẩm thực',
  transportation: 'Di chuyển',
}

const metricOptions: { id: MarketMetricId; label: string }[] = [
  { id: 'entity_count', label: 'Số thực thể' },
  { id: 'entity_count_with_sentiment_evidence', label: 'Thực thể có sentiment' },
  { id: 'entity_count_with_aspect_evidence', label: 'Thực thể có khía cạnh' },
  { id: 'review_evidence_count', label: 'Review có bằng chứng' },
  { id: 'positive_review_count', label: 'Review tích cực' },
  { id: 'neutral_review_count', label: 'Review trung lập' },
  { id: 'negative_review_count', label: 'Review tiêu cực' },
  { id: 'aspect_mention_count', label: 'Lượt đề cập khía cạnh' },
]

export function MarketPage() {
  const { geo } = useAppContext()
  const [domainId, setDomainId] = useState<ApiDomainId>('destination')
  const [metricId, setMetricId] = useState<MarketMetricId>('entity_count')
  const [selected, setSelected] = useState([geo.provinceId])
  const domains = useQuery({ queryKey: ['market-domains'], queryFn: getMarketDomains })
  const overview = useQuery({ queryKey: ['market-overview'], queryFn: getMarketOverview })
  const comparison = useQuery({
    queryKey: ['province-comparison', selected, domainId, metricId],
    queryFn: () => compareProvinces({ provinceIds: selected, domainId, metricId }),
    enabled: selected.length > 0,
  })
  const loading = domains.isPending || overview.isPending || (selected.length > 0 && comparison.isPending)
  const error = domains.isError || overview.isError || comparison.isError
  const retry = () => {
    void domains.refetch()
    void overview.refetch()
    if (selected.length) void comparison.refetch()
  }
  if (loading || error) {
    return (
      <div>
        <PageIntro title="Phân tích thị trường" subtitle="So sánh dữ liệu quan sát theo tỉnh và lĩnh vực" page="02" />
        <div className="mt-8"><StateView state={loading ? 'loading' : 'error'} onRetry={retry} /></div>
      </div>
    )
  }
  if (!domains.data || !overview.data) return <StateView state="empty" />
  const activeDomain = domains.data.find((domain) => domain.domain_id === domainId)
  const noDomainData = !activeDomain || activeDomain.support_status === 'no_data'
  const kpis = [
    { label: `${domainLabels[domainId]} · thực thể`, value: formatCount(activeDomain?.entity_count ?? null) },
    { label: 'Review có bằng chứng', value: formatCount(activeDomain?.review_evidence_count ?? null) },
    { label: 'Lượt đề cập khía cạnh', value: formatCount(activeDomain?.aspect_mention_count ?? null) },
    { label: 'Tổng thực thể đã ánh xạ', value: formatCount(overview.data.mapped_entity_count), note: 'Tất cả lĩnh vực' },
  ]
  const chartData = domains.data.map((domain) => ({
    domain: domainLabels[domain.domain_id],
    entities: domain.entity_count,
  }))
  return (
    <div>
      <PageIntro title="Phân tích thị trường" subtitle="So sánh dữ liệu quan sát theo tỉnh và lĩnh vực" page="02" />
      <div className="mt-8 flex flex-wrap gap-3">
        <select aria-label="Lĩnh vực" className="chip chip-active" value={domainId} onChange={(event) => setDomainId(event.target.value as ApiDomainId)}>
          {(Object.keys(domainLabels) as ApiDomainId[]).map((id) => <option value={id} key={id}>{domainLabels[id]}</option>)}
        </select>
        <select aria-label="Chỉ số so sánh" className="chip" value={metricId} onChange={(event) => setMetricId(event.target.value as MarketMetricId)}>
          {metricOptions.map((metric) => <option value={metric.id} key={metric.id}>{metric.label}</option>)}
        </select>
        <select aria-label="Nguồn dữ liệu" className="chip" disabled><option>TripAdvisor</option></select>
      </div>
      <div className="mt-[18px]">
        <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {kpis.map((item) => <KpiCard key={item.label} item={item} />)}
        </section>
        <section className="mt-5 grid gap-4 lg:grid-cols-[1.8fr_1fr]">
          <div className="panel h-[304px] p-5">
            <PanelTitle title="Thực thể theo lĩnh vực" subtitle="Số thực thể đã được ánh xạ; không phải thị phần quốc gia" />
            <div className="mt-4 h-[205px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData}><CartesianGrid stroke="#dfe8e8" vertical={false} /><XAxis dataKey="domain" axisLine={false} tickLine={false} fontSize={10} /><YAxis axisLine={false} tickLine={false} fontSize={10} /><Tooltip /><Bar dataKey="entities" fill="#087e72" radius={[5, 5, 0, 0]} /></BarChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div className="panel h-[304px] p-5">
            <PanelTitle title={`Sentiment · ${domainLabels[domainId]}`} subtitle="Số review theo nhãn, không phải điểm hài lòng" />
            {noDomainData ? <div className="mt-5"><StateView state="empty" /></div> : (
              <div className="mt-7 space-y-5 text-xs">
                {[['Tích cực', activeDomain.positive_review_count], ['Trung lập', activeDomain.neutral_review_count], ['Tiêu cực', activeDomain.negative_review_count]].map(([label, value]) => <div className="flex justify-between border-b border-line pb-3" key={String(label)}><span className="text-muted">{label}</span><strong>{formatCount(value as number | null)}</strong></div>)}
                <p className="text-[10px] text-muted">Chưa đủ cơ sở để đại diện cho toàn thị trường.</p>
              </div>
            )}
          </div>
        </section>
        <section className="panel mt-5 p-5">
          <PanelTitle title="So sánh tỉnh" subtitle="Một chỉ số, cùng nguồn và cùng kỳ xử lý dữ liệu" />
          <div className="mt-3 flex flex-wrap gap-2">
            {provinceOptions.map((province) => <button key={province.id} onClick={() => setSelected((current) => current.includes(province.id) ? current.filter((id) => id !== province.id) : current.length < 20 ? [...current, province.id] : current)} className={`chip ${selected.includes(province.id) ? 'chip-active' : ''}`}>{province.name}</button>)}
          </div>
          {!selected.length ? <div className="mt-4"><StateView state="empty" /></div> : (
            <div className="mt-5 grid gap-3 sm:grid-cols-3">
              {comparison.data?.map((item) => <article className="rounded-xl bg-canvas p-4" key={item.province_id}><p className="text-xs text-muted">{provinceName(item.province_id)}</p><strong className="mt-2 block text-xl">{formatCount(item.count)}</strong><p className="mt-1 text-[10px] text-muted">{item.unit} · {item.support_status === 'no_data' ? 'Không có dữ liệu' : 'Chưa đủ mẫu đại diện'}</p></article>)}
            </div>
          )}
        </section>
      </div>
      <p className="mt-4 text-[10px] text-muted">Dữ liệu quan sát từ TripAdvisor · Không phải xu hướng lịch sử hoặc thị phần</p>
    </div>
  )
}
