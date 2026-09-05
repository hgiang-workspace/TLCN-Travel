from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, when, lit, collect_list, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("EntityProcessing") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("fs.s3a.endpoint", "minio:9000") \
        .config("fs.s3a.access.key", "minioadmin") \
        .config("fs.s3a.secret.key", "minioadmin") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # Read silver tables
    silver_df = spark.read.table("silver.social_post")

    # Entity extraction: extract entities from text
    # Simple keyword-based extraction for demo
    entity_df = silver_df.withColumn(
        "extracted_entities",
        regexp_replace(col("raw_text"), r"([A-Z][a-z]+ [A-Z][a-z]+)", "<ENTITY>")
    )

    # Entity typing (simple mapping)
    typed_df = entity_df.withColumn(
        "entity_type",
        when(col("raw_text").like("%province%") | col("raw_text").like("%% จังหวัด%"), "PROVINCE")
        .when(col("raw_text").like("%district%") | col("raw_text").like("%% เขต%"), "DISTRICT")
        .when(col("raw_text").like("%destination%") | col("raw_text").like("%% ที่ไป%"), "DESTINATION")
        .when(col("raw_text").like("%hotel%") | col("raw_text").like("%% โรงแรม%"), "HOTEL")
        .when(col("raw_text").like("%restaurant%") | col("raw_text").like("%% ร้านอาหาร%"), "RESTAURANT")
        .when(col("raw_text").like("%beach%") | col("raw_text").like("%% ชายหาด%"), "BEACH")
        .when(col("raw_text").like("%museum%") | col("raw_text").like("%% พิพิธภัณฑ์%"), "MUSEUM")
        .otherwise("OTHER")
    )

    # Normalize entity names
    normalized_df = typed_df.withColumn(
        "normalized_name",
        regexp_replace(trim(col("raw_text")), r"\s+", " ")
    )

    # Entity resolution: deduplicate similar entities
    dim_entity = normalized_df.select(
        col("record_id"),
        col("normalized_name").alias("entity_name"),
        col("entity_type"),
        col("province"),
        col("specific_place"),
        lit("tourism").alias("source"),
        lit(1.0).alias("confidence"),
        col("collected_at"),
        current_timestamp().alias("updated_at")
    ).dropDuplicates(["entity_name", "entity_type"])

    # Write entity table
    dim_entity.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("silver.entity")

    print(f"Entity processing completed: {dim_entity.count()} entities extracted")

    spark.stop()

if __name__ == "__main__":
    main()