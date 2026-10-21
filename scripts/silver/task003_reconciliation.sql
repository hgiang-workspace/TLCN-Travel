-- TASK-003F read-only reconciliation assertions.
-- Set <BATCH_ID> from the successful Bronze v2 manifest being checked.
-- This script emits counts/violations only; it does not expose payload text.

USE travel.silver;

-- Batch and release ledger status.
SELECT source_id, bronze_batch_id, normalization_version, attempt, status,
       input_row_count, accepted_row_count, rejected_row_count,
       nested_input_count, target_counts, table_snapshot_refs
FROM normalization_run
WHERE bronze_batch_id = '<BATCH_ID>'
ORDER BY attempt;

-- Committed-only release visibility. Physical rows written before a failure
-- are intentionally excluded until the batch ledger reaches committed.
WITH committed_batches AS (
    SELECT DISTINCT source_id, bronze_batch_id
    FROM normalization_run
    WHERE status = 'committed'
      AND table_snapshot_refs IS NOT NULL
      AND element_at(table_snapshot_refs, 'source_record') IS NOT NULL
      AND element_at(table_snapshot_refs, 'normalization_run') IS NOT NULL
      AND CASE source_id
          WHEN 'tripadvisor' THEN
              element_at(table_snapshot_refs, 'place') IS NOT NULL
              AND element_at(table_snapshot_refs, 'place_rating') IS NOT NULL
              AND element_at(table_snapshot_refs, 'review') IS NOT NULL
              AND element_at(table_snapshot_refs, 'source_entity_mapping') IS NOT NULL
          WHEN 'foody' THEN
              element_at(table_snapshot_refs, 'place') IS NOT NULL
              AND element_at(table_snapshot_refs, 'place_rating') IS NOT NULL
              AND element_at(table_snapshot_refs, 'source_entity_mapping') IS NOT NULL
          WHEN 'tiktok' THEN
              element_at(table_snapshot_refs, 'social_content') IS NOT NULL
              AND element_at(table_snapshot_refs, 'social_engagement') IS NOT NULL
              AND element_at(table_snapshot_refs, 'social_comment') IS NOT NULL
          WHEN 'vexere' THEN
              element_at(table_snapshot_refs, 'transport_route') IS NOT NULL
          ELSE FALSE
      END
)
SELECT 'source_record' AS table_name, COUNT(*) AS visible_count
FROM source_record sr
JOIN committed_batches cb
  ON cb.source_id = sr.source_id AND cb.bronze_batch_id = sr.batch_id
WHERE sr.batch_id = '<BATCH_ID>'
UNION ALL
SELECT 'social_content', COUNT(*)
FROM social_content sc
JOIN source_record sr ON sr.source_record_sk = sc.source_record_sk
JOIN committed_batches cb
  ON cb.source_id = sr.source_id AND cb.bronze_batch_id = sr.batch_id
WHERE sr.batch_id = '<BATCH_ID>'
UNION ALL
SELECT 'social_engagement', COUNT(*)
FROM social_engagement se
JOIN source_record sr ON sr.source_record_sk = se.source_record_sk
JOIN committed_batches cb
  ON cb.source_id = sr.source_id AND cb.bronze_batch_id = sr.batch_id
WHERE sr.batch_id = '<BATCH_ID>'
UNION ALL
SELECT 'social_comment', COUNT(*)
FROM social_comment sc
JOIN source_record sr ON sr.source_record_sk = sc.source_record_sk
JOIN committed_batches cb
  ON cb.source_id = sr.source_id AND cb.bronze_batch_id = sr.batch_id
WHERE sr.batch_id = '<BATCH_ID>';

-- Top-level Bronze occurrence reconciliation.
SELECT source_id, COUNT(*) AS source_record_count,
       COUNT(DISTINCT source_record_sk) AS distinct_source_record_count
FROM source_record
WHERE batch_id = '<BATCH_ID>'
GROUP BY source_id;

-- TripAdvisor nested review reconciliation.
SELECT sr.source_id, COUNT(DISTINCT sr.source_record_sk) AS parent_count,
       COUNT(r.review_id) AS review_occurrence_count
FROM source_record sr
LEFT JOIN review r ON r.source_record_sk = sr.source_record_sk
WHERE sr.batch_id = '<BATCH_ID>' AND sr.source_id = 'tripadvisor'
GROUP BY sr.source_id;

-- TikTok nested comment reconciliation.
WITH batch_parents AS (
    SELECT source_record_sk
    FROM source_record
    WHERE batch_id = '<BATCH_ID>' AND source_id = 'tiktok'
)
SELECT COUNT(DISTINCT bp.source_record_sk) AS parent_count,
       COUNT(sc.comment_id) AS comment_occurrence_count
FROM batch_parents bp
LEFT JOIN social_comment sc ON sc.source_record_sk = bp.source_record_sk;

-- Vexere is one route observation per accepted top-level occurrence.
SELECT COUNT(DISTINCT sr.source_record_sk) AS source_record_count,
       COUNT(DISTINCT tr.transport_id) AS route_count
FROM source_record sr
LEFT JOIN transport_route tr ON tr.source_record_sk = sr.source_record_sk
WHERE sr.batch_id = '<BATCH_ID>' AND sr.source_id = 'vexere';

-- Referential-integrity violation counts; every result should be zero.
SELECT 'mapping_without_source_record' AS check_name, COUNT(*) AS violations
FROM source_entity_mapping m
LEFT JOIN source_record sr ON sr.source_record_sk = m.source_record_sk
WHERE sr.source_record_sk IS NULL
UNION ALL
SELECT 'review_without_source_record', COUNT(*)
FROM review r LEFT JOIN source_record sr ON sr.source_record_sk = r.source_record_sk
WHERE sr.source_record_sk IS NULL
UNION ALL
SELECT 'comment_without_source_record', COUNT(*)
FROM social_comment c LEFT JOIN source_record sr ON sr.source_record_sk = c.source_record_sk
WHERE sr.source_record_sk IS NULL
UNION ALL
SELECT 'route_without_source_record', COUNT(*)
FROM transport_route t LEFT JOIN source_record sr ON sr.source_record_sk = t.source_record_sk
WHERE sr.source_record_sk IS NULL;

-- Snapshot history is inspected without assuming cross-table atomicity.
SELECT table_name, snapshot_id, committed_at
FROM (
    SELECT 'source_record' AS table_name, snapshot_id, committed_at
    FROM travel.silver."source_record$snapshots"
    UNION ALL SELECT 'normalization_run', snapshot_id, committed_at
    FROM travel.silver."normalization_run$snapshots"
) snapshots
ORDER BY committed_at DESC;
