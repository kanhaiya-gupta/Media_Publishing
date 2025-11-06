#!/usr/bin/env python3
"""
Phase 1: Data Exploration

This script explores the ClickHouse data to understand:
- Data quality and distributions
- Feature availability
- Missing values
- Feature correlations
- Data patterns

Usage:
    python 01_data_exploration.py
"""

from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from clickhouse_driver import Client

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
        )
        print("✓ Connected to ClickHouse")
        return client
    except Exception as e:
        print(f"✗ Error connecting to ClickHouse: {e}")
        return None


def get_data_summary(client):
    """Get basic data summary"""
    print("\n" + "=" * 80)
    print("DATA SUMMARY")
    print("=" * 80)

    # Total sessions
    total_sessions = client.execute("SELECT count() FROM session_metrics")[0][0]
    print(f"\nTotal Sessions: {total_sessions:,}")

    # Date range
    date_range = client.execute(
        """
        SELECT 
            min(session_start) as earliest,
            max(session_start) as latest
        FROM session_metrics
    """
    )[0]
    print(f"Date Range: {date_range[0]} to {date_range[1]}")

    # Unique users
    unique_users = client.execute(
        "SELECT count(DISTINCT user_id) FROM session_metrics"
    )[0][0]
    print(f"Unique Users: {unique_users:,}")

    # Brands distribution
    print("\nBrands Distribution:")
    brands = client.execute(
        """
        SELECT brand, count() as sessions
        FROM session_metrics
        GROUP BY brand
        ORDER BY sessions DESC
    """
    )
    for brand, count in brands:
        print(f"  {brand}: {count:,} sessions ({count/total_sessions*100:.1f}%)")

    # Countries distribution
    print("\nTop 10 Countries:")
    countries = client.execute(
        """
        SELECT country, count() as sessions
        FROM session_metrics
        GROUP BY country
        ORDER BY sessions DESC
        LIMIT 10
    """
    )
    for country, count in countries:
        print(f"  {country}: {count:,} sessions")

    # Device types
    print("\nDevice Types Distribution:")
    devices = client.execute(
        """
        SELECT device_type, count() as sessions
        FROM session_metrics
        GROUP BY device_type
        ORDER BY sessions DESC
    """
    )
    for device, count in devices:
        print(f"  {device}: {count:,} sessions ({count/total_sessions*100:.1f}%)")

    # Subscription tiers
    print("\nSubscription Tiers Distribution:")
    tiers = client.execute(
        """
        SELECT subscription_tier, count() as sessions
        FROM session_metrics
        GROUP BY subscription_tier
        ORDER BY sessions DESC
    """
    )
    for tier, count in tiers:
        print(f"  {tier}: {count:,} sessions ({count/total_sessions*100:.1f}%)")


def get_numerical_statistics(client):
    """Get numerical feature statistics"""
    print("\n" + "=" * 80)
    print("NUMERICAL FEATURES STATISTICS")
    print("=" * 80)

    stats_query = """
    SELECT 
        avg(session_duration_sec) as avg_duration,
        min(session_duration_sec) as min_duration,
        max(session_duration_sec) as max_duration,
        stddevPop(session_duration_sec) as std_duration,
        
        avg(total_events) as avg_events,
        min(total_events) as min_events,
        max(total_events) as max_events,
        stddevPop(total_events) as std_events,
        
        avg(article_views) as avg_article_views,
        avg(article_clicks) as avg_article_clicks,
        avg(video_plays) as avg_video_plays,
        avg(newsletter_signups) as avg_newsletter_signups,
        avg(ad_clicks) as avg_ad_clicks,
        avg(searches) as avg_searches,
        
        avg(pages_visited_count) as avg_pages,
        avg(unique_pages_count) as avg_unique_pages,
        avg(unique_categories_count) as avg_categories
    FROM session_metrics
    """

    stats = client.execute(stats_query)[0]

    print(f"\nSession Duration:")
    print(f"  Average: {stats[0]:.2f} seconds ({stats[0]/60:.2f} minutes)")
    print(f"  Min: {stats[1]} seconds")
    print(f"  Max: {stats[2]} seconds ({stats[2]/60:.2f} minutes)")
    print(f"  Std Dev: {stats[3]:.2f} seconds")

    print(f"\nTotal Events:")
    print(f"  Average: {stats[4]:.2f} events per session")
    print(f"  Min: {stats[5]} events")
    print(f"  Max: {stats[6]} events")
    print(f"  Std Dev: {stats[7]:.2f} events")

    print(f"\nEngagement Metrics (Average per session):")
    print(f"  Article Views: {stats[8]:.2f}")
    print(f"  Article Clicks: {stats[9]:.2f}")
    print(f"  Video Plays: {stats[10]:.2f}")
    print(f"  Newsletter Signups: {stats[11]:.2f}")
    print(f"  Ad Clicks: {stats[12]:.2f}")
    print(f"  Searches: {stats[13]:.2f}")

    print(f"\nPage Metrics (Average per session):")
    print(f"  Pages Visited: {stats[14]:.2f}")
    print(f"  Unique Pages: {stats[15]:.2f}")
    print(f"  Unique Categories: {stats[16]:.2f}")


def get_user_level_features(client, sample_size=10000):
    """Extract user-level features for ML"""
    print("\n" + "=" * 80)
    print(f"EXTRACTING USER-LEVEL FEATURES (Sample: {sample_size:,})")
    print("=" * 80)

    # Use format string instead of parameterized query (ClickHouse driver limitation)
    query = f"""
    SELECT 
        user_id,
        count() as total_sessions,
        avg(session_duration_sec) as avg_session_duration,
        avg(total_events) as avg_events_per_session,
        avg(article_views) as avg_article_views,
        avg(article_clicks) as avg_article_clicks,
        avg(video_plays) as avg_video_plays,
        avg(newsletter_signups) as avg_newsletter_signups,
        avg(pages_visited_count) as avg_pages_per_session,
        avg(unique_categories_count) as avg_categories_diversity,
        sum(newsletter_signups) > 0 as has_newsletter,
        argMax(subscription_tier, session_start) as current_subscription_tier,
        argMax(country, session_start) as primary_country,
        argMax(device_type, session_start) as primary_device,
        argMax(brand, session_start) as preferred_brand,
        min(session_start) as first_session,
        max(session_start) as last_session,
        toDate(max(session_start)) - toDate(min(session_start)) as days_active
    FROM session_metrics
    GROUP BY user_id
    ORDER BY total_sessions DESC
    LIMIT {sample_size}
    """

    columns = [
        "user_id",
        "total_sessions",
        "avg_session_duration",
        "avg_events_per_session",
        "avg_article_views",
        "avg_article_clicks",
        "avg_video_plays",
        "avg_newsletter_signups",
        "avg_pages_per_session",
        "avg_categories_diversity",
        "has_newsletter",
        "current_subscription_tier",
        "primary_country",
        "primary_device",
        "preferred_brand",
        "first_session",
        "last_session",
        "days_active",
    ]

    results = client.execute(query)
    df = pd.DataFrame(results, columns=columns)

    print(f"\n✓ Extracted {len(df):,} user features")
    print(f"\nDataFrame Shape: {df.shape}")
    print(f"\nColumn Names:")
    for col in df.columns:
        print(f"  - {col}")

    print(f"\nSample Data:")
    print(df.head(10))

    print(f"\nData Types:")
    print(df.dtypes)

    print(f"\nMissing Values:")
    missing = df.isnull().sum()
    if missing.sum() > 0:
        print(missing[missing > 0])
    else:
        print("  No missing values!")

    print(f"\nBasic Statistics:")
    print(df.describe())

    return df


def analyze_feature_correlations(df):
    """Analyze feature correlations"""
    print("\n" + "=" * 80)
    print("FEATURE CORRELATIONS")
    print("=" * 80)

    # Select numerical columns
    numerical_cols = df.select_dtypes(include=[np.number]).columns
    corr_matrix = df[numerical_cols].corr()

    print("\nTop Correlations (>0.5):")
    for i in range(len(corr_matrix.columns)):
        for j in range(i + 1, len(corr_matrix.columns)):
            corr = corr_matrix.iloc[i, j]
            if abs(corr) > 0.5:
                print(
                    f"  {corr_matrix.columns[i]} <-> {corr_matrix.columns[j]}: {corr:.3f}"
                )

    # Save correlation matrix (relative to script directory)
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap="coolwarm", center=0)
    plt.title("Feature Correlation Matrix")
    plt.tight_layout()
    corr_file = SCRIPT_DIR / "correlation_matrix.png"
    plt.savefig(corr_file, dpi=300, bbox_inches="tight")
    print(f"\n✓ Saved correlation matrix to {corr_file}")


def identify_target_variables(client):
    """Identify potential target variables for ML"""
    print("\n" + "=" * 80)
    print("TARGET VARIABLES IDENTIFICATION")
    print("=" * 80)

    # Churn indicator (users not active in last 30 days)
    churn_query = """
    SELECT 
        count(DISTINCT user_id) as total_users,
        count(DISTINCT CASE 
            WHEN last_session < now() - INTERVAL 30 DAY 
            THEN user_id 
        END) as churned_users
    FROM (
        SELECT 
            user_id,
            max(session_start) as last_session
        FROM session_metrics
        GROUP BY user_id
    )
    """

    churn_stats = client.execute(churn_query)[0]
    churn_rate = churn_stats[1] / churn_stats[0] * 100 if churn_stats[0] > 0 else 0

    print(f"\nChurn Analysis:")
    print(f"  Total Users: {churn_stats[0]:,}")
    print(f"  Churned Users (inactive 30+ days): {churn_stats[1]:,}")
    print(f"  Churn Rate: {churn_rate:.2f}%")

    # Conversion indicator (newsletter signups)
    conversion_query = """
    SELECT 
        count(DISTINCT user_id) as total_users,
        count(DISTINCT CASE 
            WHEN sum(newsletter_signups) > 0 
            THEN user_id 
        END) as converted_users
    FROM session_metrics
    GROUP BY user_id
    """

    conversion_stats = client.execute(
        """
        SELECT 
            count(DISTINCT user_id) as total_users,
            count(DISTINCT CASE 
                WHEN total_newsletter_signups > 0 
                THEN user_id 
            END) as converted_users
        FROM (
            SELECT 
                user_id,
                sum(newsletter_signups) as total_newsletter_signups
            FROM session_metrics
            GROUP BY user_id
        )
    """
    )[0]

    conversion_rate = (
        conversion_stats[1] / conversion_stats[0] * 100
        if conversion_stats[0] > 0
        else 0
    )

    print(f"\nConversion Analysis:")
    print(f"  Total Users: {conversion_stats[0]:,}")
    print(f"  Converted Users (newsletter signups): {conversion_stats[1]:,}")
    print(f"  Conversion Rate: {conversion_rate:.2f}%")

    # Engagement distribution
    engagement_query = """
    SELECT 
        quantile(0.25)(engagement_score) as q25,
        quantile(0.50)(engagement_score) as median,
        quantile(0.75)(engagement_score) as q75,
        quantile(0.90)(engagement_score) as q90,
        quantile(0.95)(engagement_score) as q95
    FROM (
        SELECT 
            (article_views * 2 + 
             article_clicks * 3 + 
             video_plays * 5 + 
             newsletter_signups * 20) as engagement_score
        FROM session_metrics
    )
    """

    engagement_stats = client.execute(engagement_query)[0]

    print(f"\nEngagement Score Distribution:")
    print(f"  25th Percentile: {engagement_stats[0]:.2f}")
    print(f"  Median: {engagement_stats[1]:.2f}")
    print(f"  75th Percentile: {engagement_stats[2]:.2f}")
    print(f"  90th Percentile: {engagement_stats[3]:.2f}")
    print(f"  95th Percentile: {engagement_stats[4]:.2f}")


def main():
    """Main exploration function"""
    print("=" * 80)
    print("MEDIA PUBLISHING - DATA EXPLORATION")
    print("=" * 80)

    # Connect to ClickHouse
    client = connect_to_clickhouse()
    if not client:
        return

    try:
        # Basic summary
        get_data_summary(client)

        # Numerical statistics
        get_numerical_statistics(client)

        # Extract user-level features
        df = get_user_level_features(client, sample_size=10000)

        # Save to CSV for further analysis (relative to script directory)
        sample_file = SCRIPT_DIR / "user_features_sample.csv"
        df.to_csv(sample_file, index=False)
        print(f"\n✓ Saved user features to {sample_file}")

        # Feature correlations
        analyze_feature_correlations(df)

        # Target variables
        identify_target_variables(client)

        print("\n" + "=" * 80)
        print("DATA EXPLORATION COMPLETE")
        print("=" * 80)
        print("\nNext Steps:")
        print("  1. Review the correlation matrix")
        print("  2. Analyze user_features_sample.csv")
        print("  3. Proceed to feature engineering (02_feature_engineering.py)")

    except Exception as e:
        print(f"\n✗ Error during exploration: {e}")
        import traceback

        traceback.print_exc()
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
