import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dagster import Definitions

from assets.discovery import (
    province_seeds,
    category_seeds,
    entity_candidates_vn,
    entity_candidates_en,
    entity_mapped,
    entity_registry,
    discovery_export_csv,
)
from assets.ingestion import bronze_tripadvisor
from assets.silver import silver_tripadvisor, silver_entity
from assets.nlp import sentiment_dataset, ner_dataset, absa_result
from assets.gold import (
    gold_aspect_sentiment,
    gold_destination_sentiment,
    gold_platform_sentiment,
    gold_sentiment_daily,
)

from resources.minio import minio_resource
from resources.spark import spark_resource

defs = Definitions(
    assets=[
        province_seeds,
        category_seeds,
        entity_candidates_vn,
        entity_candidates_en,
        entity_mapped,
        entity_registry,
        discovery_export_csv,
        bronze_tripadvisor,
        silver_tripadvisor,
        silver_entity,
        sentiment_dataset,
        ner_dataset,
        absa_result,
        gold_aspect_sentiment,
        gold_destination_sentiment,
        gold_platform_sentiment,
        gold_sentiment_daily,
    ],
    resources={
        "minio": minio_resource,
        "spark": spark_resource,
    },
)
