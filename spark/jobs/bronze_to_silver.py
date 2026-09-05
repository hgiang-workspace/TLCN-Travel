from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, to_timestamp, year, month, dayofmonth, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("BronzeToSilverETL") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("fs.s3a.endpoint", "minio:9000") \
        .config("fs.s3a.access.key", "minioadmin") \
        .config("fs.s3a.secret.key", "minioadmin") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # Read JSON from MinIO Bronze
    schema = StructType([
        StructField("record_id", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("data_type", StringType(), True),
        StructField("content_raw", StringType(), True),
        StructField("location_province", StringType(), True),
        StructField("location_place", StringType(), True),
        StructField("category", StringType(), True),
        StructField("engagement_like", IntegerType(), True),
        StructField("engagement_comment", IntegerType(), True),
        StructField("engagement_share", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("scraped_at", StringType(), True)
    ])

    raw_df = spark.read.schema(schema) \
        .option("multiLine", "true") \
        .json("s3a://tourism/bronze/social/tripadvisor/")

    print(f"Bronze read: {raw_df.count()} records")

    # Processing pipeline
    # 1. Parse and clean text
    processed_df = raw_df.withColumn("clean_text", regexp_replace(col("content_raw"), r"<.*?>", " ")) \
        .withColumn("clean_text", trim(col("clean_text"))) \
        .withColumn("clean_text", regexp_replace(col("clean_text"), r"\s+", " "))

    # 2. Validate and clean rating
    processed_df = processed_df.withColumn(
        "rating",
        regexp_replace(col("rating"), r"[^\d.]", "")
    ).withColumn(
        "rating",
        when(col("rating").cast("double").isNull(), None).otherwise(
            col("rating").cast("double")
        )
    )

    # 3. Normalize location
    processed_df = processed_df.withColumn(
        "province", 
        when(col("province").isNull(), lit("Unknown")).otherwise(trim(col("province")))
    ).withColumn(
        "specific_place",
        when(col("specific_place").isNull(), lit("Unknown")).otherwise(trim(col("specific_place")))
    )

    # 4. Deduplicate by record_id
    deduplicated = processed_df.dropDuplicates(["record_id"])

    # 5. Add partition columns for Iceberg
    partitioned = deduplicated \
        .withColumn("year", year(col("collected_at"))) \
        .withColumn("month", month(col("collected_at"))) \
        .withColumn("day", dayofmonth(col("collected_at")))

    # 6. Write as Iceberg table to Silver
    partitioned.write.format("iceberg") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .option("path", "s3a://tourism/silver/social/tripadvisor/") \
        .saveAsTable("silver.social_post")

    print(f"Silver ETL completed: {deduplicated.count()} records written to silver.social_post")

    # Also write review table
    raw_df.select(
        col("record_id"),
        col("platform"),
        col("data_type"),
        col("content_raw").alias("raw_text"),
        col("location_province"),
        col("location_place"),
        col("category"),
        col("engagement_like").alias("like_count"),
        col("engagement_comment").alias("comment_count"),
        col("engagement_share").alias("share_count"),
        col("rating"),
        col("scraped_at"),
        current_timestamp().alias("collected_at")
    ).write.format("iceberg") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .option("path", "s3a://tourism/silver/social/tripadvisor/") \
        .saveAsTable("silver.review")

    print(f"Silver review table completed: {raw_df.count()} records")

    spark.stop()

if __name__ == "__main__":
    main()