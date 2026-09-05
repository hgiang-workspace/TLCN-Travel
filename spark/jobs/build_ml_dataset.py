from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, rand, floor

def main():
    spark = SparkSession.builder \
        .appName("MLDatasetBuilder") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("fs.s3a.endpoint", "minio:9000") \
        .config("fs.s3a.access.key", "minioadmin") \
        .config("fs.s3a.secret.key", "minioadmin") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # Read silver tables
    social_post = spark.read.table("silver.social_post")
    review = spark.read.table("silver.review")
    entity = spark.read.table("silver.entity")

    # Build sentiment dataset from social_post
    sentiment_df = social_post.select(
        col("record_id"),
        col("raw_text").alias("text"),
        col("rating").alias("label"),
        col("collected_at")
    )

    # Build NER dataset from entity table
    ner_dataset = entity.select(
        col("entity_name").alias("tokens"),
        col("entity_type").alias("ner_tags"),
        col("source"),
        col("confidence")
    )

    # Add split column
    sentiment_with_split = sentiment_df.withColumn(
        "split",
        when(col("rating") >= 4, "positive")
        .when(col("rating") <= 2, "negative")
        .otherwise("neutral")
    ).withColumn(
        "split_label",
        when(rand() < 0.8, "train")
        .when(rand() < 0.9, "validation")
        .otherwise("test")
    )

    # Write ML datasets
    sentiment_with_split.write.format("iceberg") \
        .mode("overwrite") \
        .partitionBy("split_label") \
        .saveAsTable("ml.sentiment_dataset")

    ner_dataset.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("ml.ner_dataset")

    print(f"ML dataset built: {sentiment_with_split.count()} sentiment records, {ner_dataset.count()} NER records")

    spark.stop()

if __name__ == "__main__":
    main()