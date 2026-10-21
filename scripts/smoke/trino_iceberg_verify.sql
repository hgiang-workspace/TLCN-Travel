SELECT id, entity_name, score, batch_id
FROM travel.integration_test.iceberg_smoke
ORDER BY id;

SELECT count(*) AS row_count, sum(score) AS score_sum
FROM travel.integration_test.iceberg_smoke;
