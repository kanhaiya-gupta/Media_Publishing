#!/usr/bin/env python3
"""
Data Loading Utilities

Helper functions for loading data from ClickHouse and MinIO.
Adapted for ownlens framework with configuration management.
"""

from clickhouse_driver import Client
import pandas as pd
from pyspark.sql import SparkSession
import logging

from .config import get_ml_config

logger = logging.getLogger(__name__)


def get_clickhouse_client():
    """
    Get ClickHouse client connection using ownlens framework configuration.
    
    Returns:
        ClickHouse Client instance or None if connection fails
    """
    config = get_ml_config()
    try:
        client = Client(**config.get_clickhouse_connection_params())
        logger.info(f"Connected to ClickHouse at {config.clickhouse_host}:{config.clickhouse_port}/{config.clickhouse_database}")
        return client
    except Exception as e:
        logger.error(f"Error connecting to ClickHouse: {e}")
        print(f"Error connecting to ClickHouse: {e}")
        return None

def load_session_data_from_clickhouse(limit=None):
    """
    Load session data from ClickHouse using customer_sessions table.
    
    Args:
        limit: Maximum number of rows to return
    
    Returns:
        DataFrame with session data
    """
    client = get_clickhouse_client()
    if not client:
        return None
    
    try:
        query = """
        SELECT 
            session_id, user_id, company_id, brand_id, account_id,
            country_code, city_id, timezone,
            device_type_id, os_id, browser_id,
            referrer, user_segment, subscription_tier,
            session_start, session_end, session_duration_sec,
            total_events, article_views, article_clicks, video_plays,
            newsletter_signups, ad_clicks, searches,
            pages_visited_count, unique_pages_count, unique_categories_count,
            categories_visited, article_ids_visited, article_titles_visited,
            page_events, engagement_score, scroll_depth_avg, time_on_page_avg,
            metadata, created_at, updated_at, batch_id
        FROM customer_sessions
        ORDER BY session_start DESC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        results = client.execute(query)
        columns = [
            'session_id', 'user_id', 'company_id', 'brand_id', 'account_id',
            'country_code', 'city_id', 'timezone',
            'device_type_id', 'os_id', 'browser_id',
            'referrer', 'user_segment', 'subscription_tier',
            'session_start', 'session_end', 'session_duration_sec',
            'total_events', 'article_views', 'article_clicks', 'video_plays',
            'newsletter_signups', 'ad_clicks', 'searches',
            'pages_visited_count', 'unique_pages_count', 'unique_categories_count',
            'categories_visited', 'article_ids_visited', 'article_titles_visited',
            'page_events', 'engagement_score', 'scroll_depth_avg', 'time_on_page_avg',
            'metadata', 'created_at', 'updated_at', 'batch_id'
        ]
        
        df = pd.DataFrame(results, columns=columns)
        logger.info(f"Loaded {len(df)} sessions from ClickHouse")
        return df
    finally:
        client.disconnect()

def load_user_features_from_clickhouse(limit=None):
    """
    Load user-level features from ClickHouse using customer_sessions table.
    
    Args:
        limit: Maximum number of users to return
    
    Returns:
        DataFrame with user-level features
    """
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
            argMax(country_code, session_start) as country_code,
            argMax(device_type_id, session_start) as device_type_id,
            max(session_start) as last_session
        FROM customer_sessions
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
            'subscription_tier', 'country_code', 'device_type_id', 'last_session'
        ]
        
        df = pd.DataFrame(results, columns=columns)
        logger.info(f"Loaded features for {len(df)} users from ClickHouse")
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

def load_ml_features(file_path=None):
    """
    Load pre-processed ML features.
    
    Args:
        file_path: Path to features file. If None, uses default from config.
    
    Returns:
        DataFrame with ML features or None if file not found
    """
    config = get_ml_config()
    if file_path is None:
        file_path = f"{config.ml_data_dir}/user_features_ml_ready.csv"
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded ML features from {file_path}: {len(df)} rows")
        return df
    except FileNotFoundError:
        logger.error(f"Features file not found: {file_path}")
        print(f"Features file not found: {file_path}")
        print("Run 02_feature_engineering.py first")
        return None

