#!/usr/bin/env python3
"""
Data Loading Utilities

Helper functions for loading data from ClickHouse and MinIO
"""

from clickhouse_driver import Client
import pandas as pd
from pyspark.sql import SparkSession

# ClickHouse connection settings
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9002
CLICKHOUSE_DATABASE = "analytics"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse"

def get_clickhouse_client():
    """Get ClickHouse client connection"""
    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DATABASE,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        return client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        return None

def load_session_data_from_clickhouse(limit=None):
    """Load session data from ClickHouse"""
    client = get_clickhouse_client()
    if not client:
        return None
    
    try:
        query = """
        SELECT *
        FROM session_metrics
        ORDER BY session_start DESC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        results = client.execute(query)
        columns = [
            'session_id', 'user_id', 'brand', 'country', 'city',
            'device_type', 'device_os', 'browser', 'subscription_tier',
            'user_segment', 'referrer', 'timezone', 'session_start',
            'session_end', 'session_duration_sec', 'total_events',
            'article_views', 'article_clicks', 'video_plays',
            'newsletter_signups', 'ad_clicks', 'searches',
            'pages_visited_count', 'unique_pages_count',
            'unique_categories_count', 'categories_visited',
            'article_ids_visited', 'article_titles_visited',
            'page_events', 'created_at', 'batch_id'
        ]
        
        df = pd.DataFrame(results, columns=columns)
        return df
    finally:
        client.disconnect()

def load_user_features_from_clickhouse(limit=None):
    """Load user-level features from ClickHouse"""
    client = get_clickhouse_client()
    if not client:
        return None
    
    try:
        query = """
        SELECT 
            user_id,
            count() as total_sessions,
            avg(session_duration_sec) as avg_session_duration,
            avg(total_events) as avg_events_per_session,
            avg(article_views) as avg_article_views,
            avg(article_clicks) as avg_article_clicks,
            sum(newsletter_signups) as total_newsletter_signups,
            argMax(subscription_tier, session_start) as subscription_tier,
            argMax(country, session_start) as country,
            argMax(device_type, session_start) as device_type,
            max(session_start) as last_session
        FROM session_metrics
        GROUP BY user_id
        ORDER BY total_sessions DESC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        results = client.execute(query)
        columns = [
            'user_id', 'total_sessions', 'avg_session_duration',
            'avg_events_per_session', 'avg_article_views',
            'avg_article_clicks', 'total_newsletter_signups',
            'subscription_tier', 'country', 'device_type', 'last_session'
        ]
        
        df = pd.DataFrame(results, columns=columns)
        return df
    finally:
        client.disconnect()

def load_from_minio_spark(bucket="session-metrics", path="sessions/", limit=10000):
    """Load data from MinIO using Spark"""
    spark = SparkSession.builder \
        .appName("MLDataLoader") \
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    try:
        # Read all Delta Lake files
        df = spark.read.format("delta").load(f"s3a://{bucket}/{path}")
        
        if limit:
            df = df.limit(limit)
        
        # Convert to Pandas
        pandas_df = df.toPandas()
        return pandas_df
    finally:
        spark.stop()

def load_ml_features(file_path='user_features_ml_ready.csv'):
    """Load pre-processed ML features"""
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        print(f"Features file not found: {file_path}")
        print("Run 02_feature_engineering.py first")
        return None

