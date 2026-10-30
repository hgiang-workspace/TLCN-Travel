export type SupportStatus =
  | 'insufficient_data'
  | 'no_data'
  | 'sufficient'
  | 'stale_data'
  | 'unsupported'

export type ApiDomainId =
  | 'destination'
  | 'accommodation'
  | 'food_dining'
  | 'transportation'

export type MarketMetricId =
  | 'entity_count'
  | 'entity_count_with_sentiment_evidence'
  | 'entity_count_with_aspect_evidence'
  | 'review_evidence_count'
  | 'positive_review_count'
  | 'neutral_review_count'
  | 'negative_review_count'
  | 'aspect_mention_count'

export interface MarketOverview {
  collected_entity_count: number | null
  mapped_entity_count: number | null
  excluded_entity_count: number | null
  unreviewed_entity_count: number | null
  entity_count_with_sentiment_evidence: number | null
  entity_count_with_aspect_evidence: number | null
  mapping_coverage_ratio: number | null
  sentiment_entity_coverage_ratio: number | null
  aspect_entity_coverage_ratio: number | null
  review_evidence_count: number | null
  aspect_mention_count: number | null
  support_status: SupportStatus
}

export interface MarketDomain {
  domain_id: ApiDomainId
  entity_count: number | null
  entity_count_with_sentiment_evidence: number | null
  entity_count_with_aspect_evidence: number | null
  review_evidence_count: number | null
  positive_review_count: number | null
  neutral_review_count: number | null
  negative_review_count: number | null
  aspect_mention_count: number | null
  support_status: SupportStatus
}

export interface ProvinceComparison {
  province_id: string
  domain_id: ApiDomainId | null
  metric_id: MarketMetricId
  unit: string
  count: number | null
  support_status: SupportStatus
}

export interface CoverageItem {
  province_id: string
  collected_entity_count: number | null
  mapped_entity_count: number | null
  excluded_entity_count: number | null
  unreviewed_entity_count: number | null
  entity_count_with_sentiment_evidence: number | null
  entity_count_with_aspect_evidence: number | null
  mapping_coverage_ratio: number | null
  sentiment_entity_coverage_ratio: number | null
  aspect_entity_coverage_ratio: number | null
  support_status: SupportStatus
}

export interface ProvinceComparisonRequest {
  provinceIds: string[]
  metricId: MarketMetricId
  domainId?: ApiDomainId
}
