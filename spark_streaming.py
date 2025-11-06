#!/usr/bin/env python3
"""
Media Publishing - Real-Time User Analytics Streaming Pipeline

This Spark Streaming application processes user events from Kafka and:
1. Aggregates session-level metrics
2. Calculates engagement analytics
3. Writes to MinIO (Delta Lake) for data lake storage
4. Writes to ClickHouse for real-time analytics

Author: Data Engineering Team
Company: Media Publishing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg as spark_avg
from pyspark.sql.functions import (col, collect_list, count, expr, first,
                                   from_json, from_unixtime, lit)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import struct
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_json, when
from pyspark.sql.types import (ArrayType, IntegerType, LongType, MapType,
                               StringType, StructField, StructType,
                               TimestampType)

# Create SparkSession with Kafka connector, S3/MinIO, and Delta Lake support
# Note: ClickHouse writes use clickhouse-driver (Python library), not JDBC
# Note: The first run will download JARs from Maven (requires internet)
spark = (
    SparkSession.builder.appName("MediaPublishingRealTimeAnalytics")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "io.delta:delta-core_2.12:2.4.0",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# Define comprehensive schema for Kafka messages matching UserEvent structure
schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("brand", StringType(), True),
        StructField("category", StringType(), True),
        StructField("article_id", StringType(), True),
        StructField("article_title", StringType(), True),
        StructField("article_type", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("device_os", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("user_segment", StringType(), True),
        StructField(
            "engagement_metrics", MapType(StringType(), StringType()), True
        ),  # JSON-like structure
        StructField(
            "metadata", MapType(StringType(), StringType()), True
        ),  # JSON-like structure
    ]
)

# Read events from Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "web_clicks")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON
df_parsed = (
    df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .filter(col("event_type") != "session_end")
)  # Filter out session_end for session tracking

# Calculate session-level metrics grouped by session_id (more accurate than user_id)
# This includes all the rich metadata from the new event structure
session_metrics = (
    df_parsed.groupBy("session_id", "user_id")
    .agg(
        # Session-level metrics
        first("brand").alias("brand"),
        first("country").alias("country"),
        first("city").alias("city"),
        first("device_type").alias("device_type"),
        first("device_os").alias("device_os"),
        first("browser").alias("browser"),
        first("subscription_tier").alias("subscription_tier"),
        first("user_segment").alias("user_segment"),
        first("referrer").alias("referrer"),
        first("timezone").alias("timezone"),
        # Temporal metrics
        spark_min("timestamp").alias("session_start"),
        spark_max("timestamp").alias("session_end"),
        # Event counts
        count("*").alias("total_events"),
        count(when(col("event_type") == "article_view", 1)).alias("article_views"),
        count(when(col("event_type") == "article_click", 1)).alias("article_clicks"),
        count(when(col("event_type") == "video_play", 1)).alias("video_plays"),
        count(when(col("event_type") == "newsletter_signup", 1)).alias(
            "newsletter_signups"
        ),
        count(when(col("event_type") == "ad_click", 1)).alias("ad_clicks"),
        count(when(col("event_type") == "search", 1)).alias("searches"),
        # Page tracking
        collect_list(col("category")).alias("categories_visited"),
        collect_list(col("article_id")).alias("article_ids_visited"),
        collect_list(col("article_title")).alias("article_titles_visited"),
        # Collect all page events with full metadata for detailed analysis
        collect_list(
            struct(
                col("event_type"),
                col("category"),
                col("article_id"),
                col("article_title"),
                col("page_url"),
                col("timestamp"),
                col("engagement_metrics"),
            )
        ).alias("page_events"),
    )
    .withColumn("session_duration_sec", expr("session_end - session_start"))
    .withColumn("pages_visited_count", expr("size(article_ids_visited)"))
    .withColumn("unique_pages_count", expr("size(array_distinct(article_ids_visited))"))
    .withColumn(
        "unique_categories_count", expr("size(array_distinct(categories_visited))")
    )
)

# Transform data for ML Analytics and ClickHouse
# Convert timestamps to DateTime, flatten complex structures to JSON strings
session_metrics_transformed = (
    session_metrics.withColumn("session_start_dt", from_unixtime(col("session_start")))
    .withColumn("session_end_dt", from_unixtime(col("session_end")))
    .withColumn("page_events_json", to_json(col("page_events")))
    .withColumn("categories_visited_json", to_json(col("categories_visited")))
    .withColumn("article_ids_visited_json", to_json(col("article_ids_visited")))
    .withColumn("article_titles_visited_json", to_json(col("article_titles_visited")))
    .select(
        col("session_id"),
        col("user_id"),
        col("brand"),
        col("country"),
        col("city"),
        col("device_type"),
        col("device_os"),
        col("browser"),
        col("subscription_tier"),
        col("user_segment"),
        col("referrer"),
        col("timezone"),
        col("session_start_dt").alias("session_start"),
        col("session_end_dt").alias("session_end"),
        col("session_duration_sec"),
        col("total_events"),
        col("article_views"),
        col("article_clicks"),
        col("video_plays"),
        col("newsletter_signups"),
        col("ad_clicks"),
        col("searches"),
        col("pages_visited_count"),
        col("unique_pages_count"),
        col("unique_categories_count"),
        col("categories_visited_json").alias("categories_visited"),
        col("article_ids_visited_json").alias("article_ids_visited"),
        col("article_titles_visited_json").alias("article_titles_visited"),
        col("page_events_json").alias("page_events"),
    )
)

# Output configuration
MINIO_BUCKET = "s3a://session-metrics"
# Use local checkpoint location to avoid S3A path issues
CHECKPOINT_LOCATION = "/tmp/spark-checkpoints"
# ClickHouse connection settings (using native protocol with clickhouse-driver)
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9002  # Native protocol port (mapped from container's 9000)
CLICKHOUSE_DATABASE = "analytics"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse"


def write_to_storage_and_analytics(batch_df, batch_id):
    """Write batch to MinIO (Data Lake), ClickHouse (ML Analytics), and console"""
    try:
        # Add batch_id to DataFrame for tracking
        batch_df_with_id = batch_df.withColumn("batch_id", lit(batch_id))
        record_count = batch_df_with_id.count()

        if record_count == 0:
            print(f"⚠ Batch {batch_id}: No records to process")
            return

        # 1. Write to MinIO (Data Lake) - Delta Lake format (ACID transactions, time travel)
        try:
            output_path = f"{MINIO_BUCKET}/sessions/batch_{batch_id}"
            batch_df_with_id.write.mode("overwrite").format("delta").save(output_path)
            print(
                f"✓ Saved to MinIO (Delta Lake): {output_path} ({record_count} sessions)"
            )
        except Exception as e:
            print(f"✗ Error writing to MinIO: {e}")
            if "NoSuchBucket" in str(e):
                print(f"  Note: Create bucket 'session-metrics' first:")
                print(f"    1. Go to MinIO Console: http://localhost:9001")
                print(f"    2. Login: minioadmin / minioadmin")
                print(f"    3. Create bucket: 'session-metrics'")
                print(f"  Or run: python create_minio_bucket.py")

        # 2. Write to ClickHouse (ML Analytics) using clickhouse-driver
        try:
            from datetime import datetime

            from clickhouse_driver import Client

            # Connect to ClickHouse using native protocol
            ch_client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                database=CLICKHOUSE_DATABASE,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
            )

            try:
                # Collect data and convert to list of tuples
                rows = batch_df_with_id.collect()
                data_to_insert = []

                for row in rows:
                    # Convert string timestamps to datetime objects for ClickHouse
                    session_start_dt = datetime.strptime(
                        str(row.session_start), "%Y-%m-%d %H:%M:%S"
                    )
                    session_end_dt = datetime.strptime(
                        str(row.session_end), "%Y-%m-%d %H:%M:%S"
                    )

                    data_to_insert.append(
                        (
                            str(row.session_id),
                            int(row.user_id),
                            str(row.brand),
                            str(row.country),
                            str(row.city),
                            str(row.device_type),
                            str(row.device_os),
                            str(row.browser),
                            str(row.subscription_tier),
                            str(row.user_segment),
                            str(row.referrer),
                            str(row.timezone),
                            session_start_dt,
                            session_end_dt,
                            int(row.session_duration_sec),
                            int(row.total_events),
                            int(row.article_views),
                            int(row.article_clicks),
                            int(row.video_plays),
                            int(row.newsletter_signups),
                            int(row.ad_clicks),
                            int(row.searches),
                            int(row.pages_visited_count),
                            int(row.unique_pages_count),
                            int(row.unique_categories_count),
                            str(row.categories_visited),
                            str(row.article_ids_visited),
                            str(row.article_titles_visited),
                            str(row.page_events),
                            int(row.batch_id),
                        )
                    )

                if data_to_insert:
                    # Execute INSERT statement (using session_id for deduplication)
                    insert_query = """
                    INSERT INTO session_metrics 
                    (session_id, user_id, brand, country, city, device_type, device_os, browser,
                     subscription_tier, user_segment, referrer, timezone,
                     session_start, session_end, session_duration_sec, 
                     total_events, article_views, article_clicks, video_plays, newsletter_signups,
                     ad_clicks, searches, pages_visited_count, unique_pages_count, unique_categories_count,
                     categories_visited, article_ids_visited, article_titles_visited, page_events, batch_id)
                    VALUES
                    """
                    ch_client.execute(insert_query, data_to_insert)
                    print(f"✓ Saved to ClickHouse: {len(data_to_insert)} sessions")

            finally:
                ch_client.disconnect()

        except Exception as e:
            print(f"✗ Error writing to ClickHouse: {e}")
            import traceback

            traceback.print_exc()

        # 3. Print to console for monitoring
        print(f"\n{'='*80}")
        print(f"Batch {batch_id} - Processed {record_count} sessions")
        print(f"{'='*80}")
        # Show key metrics only (truncate long fields)
        batch_df_with_id.select(
            "session_id",
            "user_id",
            "brand",
            "country",
            "device_type",
            "subscription_tier",
            "session_start",
            "session_end",
            "session_duration_sec",
            "total_events",
            "article_views",
            "pages_visited_count",
            "batch_id",
        ).show(20, truncate=False)
        print(f"{'='*80}\n")

    except Exception as e:
        print(f"✗ Error in batch {batch_id}: {e}")
        import traceback

        traceback.print_exc()


# Output to MinIO (Data Lake) and ClickHouse (ML Analytics) with checkpointing
query = (
    session_metrics_transformed.writeStream.outputMode("complete")
    .foreachBatch(write_to_storage_and_analytics)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="10 seconds")
    .start()
)

print("=" * 80)
print("Media Publishing - Real-Time Analytics Pipeline")
print("=" * 80)
print("Spark Streaming started!")
print(f"Data Lake (MinIO/Delta Lake): s3a://session-metrics")
print(f"MinIO Console: http://localhost:9001 (minioadmin/minioadmin)")
print(
    f"ClickHouse Analytics: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}"
)
print("=" * 80)
print("Pipeline: Kafka → Spark Streaming → MinIO (Delta Lake) + ClickHouse (Analytics)")
print("=" * 80)
print("\nTracking:")
print("  - Session-level metrics (duration, events, pages)")
print("  - Brand-level analytics (Bild, Welt, Business Insider, Politico, Sport Bild)")
print("  - Geographic analytics (country, city, timezone)")
print("  - Device analytics (type, OS, browser)")
print("  - Engagement metrics (views, clicks, videos, signups)")
print("  - Subscription tier analysis")
print("=" * 80)

query.awaitTermination()
