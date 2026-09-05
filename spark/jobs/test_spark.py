from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, year, month, dayofmonth, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("TestSparkMinIO") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("fs.s3a.endpoint", "minio:9000") \
        .config("fs.s3a.access.key", "minioadmin") \
        .config("fs.s3a.secret.key", "minioadmin") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # Read JSON from MinIO Bronze bucket
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

    print(f"Read {raw_df.count()} records from MinIO Bronze")

    # Transform to DataFrame with clean schema
    processed_df = raw_df.select(
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
    )

    # Write as Parquet to MinIO Silver bucket
    processed_df.write \
        .mode("overwrite") \
        .option("parquet.compression", "snappy") \
        .parquet("s3a://tourism/silver/social/tripadvisor/")

    print("Written Parquet to MinIO Silver")

    # Show sample
    processed_df.show(5)

    spark.stop()

if __name__ == "__main__":
    main()