from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, floor, date_format, lit
from pyspark.sql.types import StringType, DoubleType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("GoldAggregation") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("fs.s3a.endpoint", "minio:9000") \
        .config("fs.s3a.access.key", "minioadmin") \
        .config("fs.s3a.secret.key", "minioadmin") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # Read ML datasets
    sentiment_ds = spark.read.table("ml.sentiment_dataset")
    entity = spark.read.table("silver.entity")

    # Gold: aspect sentiment aggregation
    aspect_sentiment = sentiment_ds.groupBy(
        col("tokens").alias("aspect")
    ).agg(
        count("record_id").alias("review_count"),
        avg("score").alias("avg_sentiment_score")
    ).withColumn("aspect", col("aspect"))

    # Gold: destination sentiment daily
    destination_sentiment = sentiment_ds.withColumn(
        "date", date_format(col("collected_at"), "yyyy-MM-dd")
    ).groupBy(
        col("date"),
        col("aspect").alias("aspect")
    ).agg(
        count("record_id").alias("review_count"),
        avg("score").alias("avg_sentiment_score")
    )

    # Gold: platform sentiment
    platform_sentiment = sentiment_ds.groupBy(
        lit("tripadvisor").alias("platform")
    ).agg(
        count("record_id").alias("review_count"),
        avg("score").alias("avg_sentiment_score")
    )

    # Gold: sentiment daily
    sentiment_daily = sentiment_ds.withColumn(
        "date", date_format(col("collected_at"), "yyyy-MM-dd")
    ).groupBy(
        col("date")
    ).agg(
        count("record_id").alias("review_count"),
        avg("score").alias("avg_sentiment_score")
    )

    # Write gold tables
    aspect_sentiment.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("gold.aspect_sentiment")

    destination_sentiment.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("gold.destination_sentiment")

    platform_sentiment.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("gold.platform_sentiment")

    sentiment_daily.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("gold.sentiment_daily")

    print(f"Gold tables created: aspect_sentiment, destination_sentiment, platform_sentiment, sentiment_daily")

    spark.stop()

if __name__ == "__main__":
    main()