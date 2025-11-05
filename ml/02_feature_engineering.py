#!/usr/bin/env python3
"""
Phase 1: Feature Engineering

This script creates ML-ready features from raw session data:
- User-level features
- Session-level features
- Engagement features
- Behavioral features
- Temporal features

Usage:
    python 02_feature_engineering.py
"""

import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime, timedelta
import json
import os
from pathlib import Path

# Get script directory for relative paths
SCRIPT_DIR = Path(__file__).parent.absolute()

# ClickHouse connection
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9002
CLICKHOUSE_DATABASE = "analytics"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse"

def connect_to_clickhouse():
    """Connect to ClickHouse"""
    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DATABASE,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            connect_timeout=10,
            send_receive_timeout=300  # 5 minutes max for queries
        )
        print("✓ Connected to ClickHouse")
        return client
    except Exception as e:
        print(f"✗ Error connecting to ClickHouse: {e}")
        return None

def extract_user_features(client, limit=10000):
    """Extract user-level features for ML"""
    print("\n" + "="*80)
    print(f"EXTRACTING USER-LEVEL FEATURES (Limit: {limit:,})")
    print("="*80)
    
    # Define columns first
    columns = [
        'user_id', 'total_sessions', 'avg_session_duration', 'min_session_duration',
        'max_session_duration', 'std_session_duration', 'avg_events_per_session',
        'avg_pages_per_session', 'avg_categories_diversity',
        'avg_article_views', 'avg_article_clicks', 'avg_video_plays', 'avg_searches',
        'total_newsletter_signups', 'total_ad_clicks',
        'avg_engagement_score',
        'has_newsletter', 'newsletter_signup_rate',
        'preferred_brand', 'bild_sessions', 'welt_sessions', 'bi_sessions',
        'politico_sessions', 'sport_bild_sessions',
        'primary_country', 'primary_city',
        'primary_device', 'primary_os', 'primary_browser',
        'current_subscription_tier', 'current_user_segment',
        'first_session', 'last_session', 'days_active',
        'days_since_last_session', 'active_today', 'active_last_7_days', 'active_last_30_days'
    ]
    
    # First check if table has data
    print("  Checking if table has data...")
    count_query = "SELECT count() as total_sessions FROM session_metrics"
    try:
        count_result = client.execute(count_query)
        total_sessions = count_result[0][0] if count_result else 0
        print(f"  Found {total_sessions:,} total sessions in session_metrics")
        
        if total_sessions == 0:
            print("⚠️  No data in session_metrics table")
            return pd.DataFrame(columns=columns)
        
        # Check unique users count
        user_count_query = "SELECT uniqExact(user_id) as unique_users FROM session_metrics"
        user_count_result = client.execute(user_count_query)
        unique_users = user_count_result[0][0] if user_count_result else 0
        print(f"  Found {unique_users:,} unique users")
        
        # Adjust limit if needed
        if unique_users < limit:
            limit = unique_users
            print(f"  Adjusted limit to {limit:,} (all available users)")
        
    except Exception as e:
        print(f"⚠️  Could not check table statistics: {e}")
    
    # Optimized query - use format string instead of parameterized query
    # ClickHouse driver has issues with ? in CTE LIMIT clauses
    query = f"""
    WITH user_stats AS (
        SELECT 
            user_id,
            count() as total_sessions
        FROM session_metrics
        GROUP BY user_id
        ORDER BY total_sessions DESC
        LIMIT {limit}
    )
    SELECT 
        sm.user_id,
        -- Behavioral Features
        count() as total_sessions,
        avg(sm.session_duration_sec) as avg_session_duration,
        min(sm.session_duration_sec) as min_session_duration,
        max(sm.session_duration_sec) as max_session_duration,
        stddevPop(sm.session_duration_sec) as std_session_duration,
        avg(sm.total_events) as avg_events_per_session,
        avg(sm.pages_visited_count) as avg_pages_per_session,
        avg(sm.unique_categories_count) as avg_categories_diversity,
        
        -- Engagement Features
        avg(sm.article_views) as avg_article_views,
        avg(sm.article_clicks) as avg_article_clicks,
        avg(sm.video_plays) as avg_video_plays,
        avg(sm.searches) as avg_searches,
        sum(sm.newsletter_signups) as total_newsletter_signups,
        sum(sm.ad_clicks) as total_ad_clicks,
        
        -- Engagement Score
        avg((sm.article_views * 2 + 
             sm.article_clicks * 3 + 
             sm.video_plays * 5 + 
             sm.newsletter_signups * 20 + 
             sm.pages_visited_count * 1 + 
             sm.unique_categories_count * 2)) as avg_engagement_score,
        
        -- Conversion Features
        sum(sm.newsletter_signups) > 0 as has_newsletter,
        sum(sm.newsletter_signups) * 1.0 / count() as newsletter_signup_rate,
        
        -- Content Features
        argMax(sm.brand, sm.session_start) as preferred_brand,
        countIf(sm.brand = 'bild') as bild_sessions,
        countIf(sm.brand = 'welt') as welt_sessions,
        countIf(sm.brand = 'business_insider') as bi_sessions,
        countIf(sm.brand = 'politico') as politico_sessions,
        countIf(sm.brand = 'sport_bild') as sport_bild_sessions,
        
        -- Geographic Features
        argMax(sm.country, sm.session_start) as primary_country,
        argMax(sm.city, sm.session_start) as primary_city,
        
        -- Device Features
        argMax(sm.device_type, sm.session_start) as primary_device,
        argMax(sm.device_os, sm.session_start) as primary_os,
        argMax(sm.browser, sm.session_start) as primary_browser,
        
        -- Subscription Features
        argMax(sm.subscription_tier, sm.session_start) as current_subscription_tier,
        argMax(sm.user_segment, sm.session_start) as current_user_segment,
        
        -- Temporal Features
        min(sm.session_start) as first_session,
        max(sm.session_start) as last_session,
        toDate(max(sm.session_start)) - toDate(min(sm.session_start)) as days_active,
        
        -- Recency Features
        now() - max(sm.session_start) as days_since_last_session,
        toDate(max(sm.session_start)) = today() as active_today,
        toDate(max(sm.session_start)) >= today() - 7 as active_last_7_days,
        toDate(max(sm.session_start)) >= today() - 30 as active_last_30_days
        
    FROM session_metrics sm
    INNER JOIN user_stats us ON sm.user_id = us.user_id
    GROUP BY sm.user_id
    ORDER BY total_sessions DESC
    SETTINGS max_threads = 4, max_execution_time = 300
    """
    
    print(f"\nExecuting optimized query (limit: {limit:,} users)...")
    print("  Using two-step approach:")
    print("  1. First: Find top users by session count")
    print("  2. Then: Calculate features for those users")
    print("  Query timeout: 5 minutes")
    
    import time
    start_time = time.time()
    
    try:
        # Execute query (no parameters - limit is in format string)
        results = client.execute(query)
        elapsed = time.time() - start_time
        print(f"✓ Query completed in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"✗ Query failed after {elapsed:.1f} seconds ({elapsed/60:.1f} minutes): {e}")
        print(f"  Error type: {type(e).__name__}")
        raise
    
    # Create DataFrame
    if not results:
        print("⚠️  Query returned no results")
        return pd.DataFrame(columns=columns)
    
    df = pd.DataFrame(results, columns=columns)
    
    print(f"✓ Extracted {len(df):,} user features")
    print(f"  DataFrame Shape: {df.shape}")
    
    return df

def create_churn_labels(df):
    """Create churn labels for churn prediction"""
    print("\n" + "="*80)
    print("CREATING CHURN LABELS")
    print("="*80)
    
    # Churn definition: User inactive for 30+ days
    df['churned'] = (df['days_since_last_session'] >= 30).astype(int)
    
    # Churn risk categories
    df['churn_risk'] = pd.cut(
        df['days_since_last_session'],
        bins=[0, 7, 14, 30, float('inf')],
        labels=['low', 'medium', 'high', 'churned']
    )
    
    churn_counts = df['churned'].value_counts()
    churn_rate = df['churned'].mean() * 100
    
    print(f"\nChurn Distribution:")
    print(f"  Non-Churned (0): {churn_counts.get(0, 0):,} ({100-churn_rate:.2f}%)")
    print(f"  Churned (1): {churn_counts.get(1, 0):,} ({churn_rate:.2f}%)")
    
    print(f"\nChurn Risk Distribution:")
    print(df['churn_risk'].value_counts())
    
    return df

def create_conversion_labels(df):
    """Create conversion labels for subscription conversion prediction"""
    print("\n" + "="*80)
    print("CREATING CONVERSION LABELS")
    print("="*80)
    
    # Conversion: Has newsletter signup
    df['converted'] = df['has_newsletter'].astype(int)
    
    # High-value conversion: Multiple signups or high engagement
    df['high_value_conversion'] = (
        (df['total_newsletter_signups'] > 1) | 
        (df['avg_engagement_score'] > df['avg_engagement_score'].quantile(0.75))
    ).astype(int)
    
    conversion_counts = df['converted'].value_counts()
    conversion_rate = df['converted'].mean() * 100
    
    print(f"\nConversion Distribution:")
    print(f"  Non-Converted (0): {conversion_counts.get(0, 0):,} ({100-conversion_rate:.2f}%)")
    print(f"  Converted (1): {conversion_counts.get(1, 0):,} ({conversion_rate:.2f}%)")
    
    high_value_counts = df['high_value_conversion'].value_counts()
    high_value_rate = df['high_value_conversion'].mean() * 100
    
    print(f"\nHigh-Value Conversion Distribution:")
    print(f"  Non-High-Value (0): {high_value_counts.get(0, 0):,} ({100-high_value_rate:.2f}%)")
    print(f"  High-Value (1): {high_value_counts.get(1, 0):,} ({high_value_rate:.2f}%)")
    
    return df

def encode_categorical_features(df):
    """Encode categorical features for ML"""
    print("\n" + "="*80)
    print("ENCODING CATEGORICAL FEATURES")
    print("="*80)
    
    # Create brand preference ratios
    df['brand_bild_ratio'] = df['bild_sessions'] / df['total_sessions']
    df['brand_welt_ratio'] = df['welt_sessions'] / df['total_sessions']
    df['brand_bi_ratio'] = df['bi_sessions'] / df['total_sessions']
    df['brand_politico_ratio'] = df['politico_sessions'] / df['total_sessions']
    df['brand_sport_bild_ratio'] = df['sport_bild_sessions'] / df['total_sessions']
    
    # One-hot encode subscription tier
    subscription_dummies = pd.get_dummies(df['current_subscription_tier'], prefix='subscription')
    df = pd.concat([df, subscription_dummies], axis=1)
    
    # One-hot encode user segment
    segment_dummies = pd.get_dummies(df['current_user_segment'], prefix='segment')
    df = pd.concat([df, segment_dummies], axis=1)
    
    # One-hot encode device type
    device_dummies = pd.get_dummies(df['primary_device'], prefix='device')
    df = pd.concat([df, device_dummies], axis=1)
    
    # One-hot encode country (top 10)
    top_countries = df['primary_country'].value_counts().head(10).index
    df['primary_country_encoded'] = df['primary_country'].apply(
        lambda x: x if x in top_countries else 'other'
    )
    country_dummies = pd.get_dummies(df['primary_country_encoded'], prefix='country')
    df = pd.concat([df, country_dummies], axis=1)
    
    print(f"✓ Encoded categorical features")
    print(f"  New DataFrame Shape: {df.shape}")
    
    return df

def create_derived_features(df):
    """Create derived/engineered features"""
    print("\n" + "="*80)
    print("CREATING DERIVED FEATURES")
    print("="*80)
    
    # Session frequency
    df['session_frequency'] = df['total_sessions'] / (df['days_active'] + 1)  # Avoid division by zero
    
    # Engagement trends
    df['engagement_per_session'] = df['avg_engagement_score']
    df['engagement_per_minute'] = df['avg_engagement_score'] / (df['avg_session_duration'] / 60 + 1)
    
    # Content preference scores
    df['content_diversity_score'] = df['avg_categories_diversity']
    df['brand_loyalty_score'] = df[['brand_bild_ratio', 'brand_welt_ratio', 
                                     'brand_bi_ratio', 'brand_politico_ratio', 
                                     'brand_sport_bild_ratio']].max(axis=1)
    
    # Click-through rate
    df['click_through_rate'] = df['avg_article_clicks'] / (df['avg_article_views'] + 1)
    
    # Video engagement rate
    df['video_engagement_rate'] = df['avg_video_plays'] / (df['total_sessions'] + 1)
    
    # Recency score (inverse of days since last session)
    df['recency_score'] = 1 / (df['days_since_last_session'] + 1)
    
    # Activity score (combination of recency and frequency)
    df['activity_score'] = (df['recency_score'] * df['session_frequency']) / 2
    
    print(f"✓ Created derived features")
    print(f"  Final DataFrame Shape: {df.shape}")
    
    return df

def prepare_ml_features(df):
    """Prepare final ML-ready feature set"""
    print("\n" + "="*80)
    print("PREPARING ML-READY FEATURES")
    print("="*80)
    
    # Select numerical features
    numerical_features = [
        # Behavioral
        'total_sessions', 'avg_session_duration', 'min_session_duration',
        'max_session_duration', 'std_session_duration', 'avg_events_per_session',
        'avg_pages_per_session', 'avg_categories_diversity',
        
        # Engagement
        'avg_article_views', 'avg_article_clicks', 'avg_video_plays', 'avg_searches',
        'total_newsletter_signups', 'total_ad_clicks', 'avg_engagement_score',
        
        # Conversion
        'has_newsletter', 'newsletter_signup_rate',
        
        # Brand preferences
        'brand_bild_ratio', 'brand_welt_ratio', 'brand_bi_ratio',
        'brand_politico_ratio', 'brand_sport_bild_ratio',
        
        # Derived features
        'session_frequency', 'engagement_per_session', 'engagement_per_minute',
        'content_diversity_score', 'brand_loyalty_score', 'click_through_rate',
        'video_engagement_rate', 'recency_score', 'activity_score',
        
        # Temporal
        'days_active', 'days_since_last_session',
        'active_today', 'active_last_7_days', 'active_last_30_days'
    ]
    
    # Add one-hot encoded features
    subscription_cols = [col for col in df.columns if col.startswith('subscription_')]
    segment_cols = [col for col in df.columns if col.startswith('segment_')]
    device_cols = [col for col in df.columns if col.startswith('device_')]
    country_cols = [col for col in df.columns if col.startswith('country_')]
    
    all_features = numerical_features + subscription_cols + segment_cols + device_cols + country_cols
    
    # Check which features exist
    available_features = [f for f in all_features if f in df.columns]
    
    print(f"\nSelected Features: {len(available_features)}")
    print(f"  Numerical: {len(numerical_features)}")
    print(f"  Categorical (one-hot): {len(subscription_cols + segment_cols + device_cols + country_cols)}")
    
    # Create feature DataFrame
    feature_df = df[['user_id'] + available_features + ['churned', 'converted', 'high_value_conversion']].copy()
    
    # Fill any remaining NaN values
    feature_df = feature_df.fillna(0)
    
    print(f"\n✓ Prepared ML-ready features")
    print(f"  Feature DataFrame Shape: {feature_df.shape}")
    print(f"  Features: {len(available_features)}")
    print(f"  Targets: churned, converted, high_value_conversion")
    
    return feature_df, available_features

def main():
    """Main feature engineering function"""
    print("="*80)
    print("MEDIA PUBLISHING - FEATURE ENGINEERING")
    print("="*80)
    
    # Connect to ClickHouse
    client = connect_to_clickhouse()
    if not client:
        print("\n✗ Cannot proceed without ClickHouse connection")
        import sys
        sys.exit(1)
    
    try:
        # Extract user features (reduced limit for faster processing)
        df = extract_user_features(client, limit=10000)
        
        # Validate that we have data
        if df is None or len(df) == 0:
            print("\n✗ No data extracted from ClickHouse")
            print("  Please ensure:")
            print("  1. Kafka producer is running and generating events")
            print("  2. Spark streaming is running and processing events")
            print("  3. Data exists in ClickHouse session_metrics table")
            import sys
            sys.exit(1)
        
        # Create labels
        df = create_churn_labels(df)
        df = create_conversion_labels(df)
        
        # Encode categorical features
        df = encode_categorical_features(df)
        
        # Create derived features
        df = create_derived_features(df)
        
        # Prepare ML-ready features
        feature_df, feature_list = prepare_ml_features(df)
        
        # Validate feature DataFrame
        if feature_df is None or len(feature_df) == 0:
            print("\n✗ Failed to prepare ML-ready features")
            import sys
            sys.exit(1)
        
        # Save to CSV (relative to script directory)
        output_file = SCRIPT_DIR / 'user_features_ml_ready.csv'
        feature_df.to_csv(output_file, index=False)
        print(f"\n✓ Saved ML-ready features to {output_file}")
        
        # Verify file was created
        if not output_file.exists():
            print(f"\n✗ File was not created: {output_file}")
            import sys
            sys.exit(1)
        
        # Save feature list (relative to script directory)
        feature_list_file = SCRIPT_DIR / 'feature_list.json'
        with open(feature_list_file, 'w') as f:
            json.dump(feature_list, f, indent=2)
        print(f"✓ Saved feature list to {feature_list_file}")
        
        # Verify file was created
        if not feature_list_file.exists():
            print(f"\n✗ File was not created: {feature_list_file}")
            import sys
            sys.exit(1)
        
        # Summary
        print("\n" + "="*80)
        print("FEATURE ENGINEERING COMPLETE")
        print("="*80)
        print(f"\nSummary:")
        print(f"  Total Users: {len(feature_df):,}")
        print(f"  Features: {len(feature_list)}")
        print(f"  Churn Rate: {feature_df['churned'].mean()*100:.2f}%")
        print(f"  Conversion Rate: {feature_df['converted'].mean()*100:.2f}%")
        print(f"\nNext Steps:")
        print("  1. Review user_features_ml_ready.csv")
        print("  2. Proceed to churn prediction (04_churn_prediction.py)")
        print("  3. Or start with baseline models (03_baseline_models.py)")
        
    except Exception as e:
        print(f"\n✗ Error during feature engineering: {e}")
        import traceback
        traceback.print_exc()
        import sys
        sys.exit(1)
    finally:
        if client:
            client.disconnect()

if __name__ == "__main__":
    main()

