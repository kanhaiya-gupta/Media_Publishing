#!/usr/bin/env python3
"""
Phase 4: Ad Revenue Optimization

This script implements ad revenue optimization using Multi-Armed Bandit:
- Thompson Sampling algorithm
- Ad placement optimization
- Revenue prediction
- A/B testing framework

Usage:
    python 10_ad_optimization.py
"""

import json
import pickle
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

# OwnLens framework imports
from ownlens.ml.utils import get_clickhouse_client, get_ml_config

# Get script directory for relative paths
SCRIPT_DIR = Path(__file__).parent.absolute()


def connect_to_clickhouse():
    """
    Connect to ClickHouse using ownlens framework configuration.
    
    Returns:
        ClickHouse Client instance or None if connection fails
    """
    client = get_clickhouse_client()
    if client:
        print("✓ Connected to ClickHouse")
    else:
        print("✗ Error connecting to ClickHouse")
    return client


def load_ad_data(client, limit=100000):
    """Load ad interaction data"""
    print("\n" + "=" * 80)
    print("LOADING AD INTERACTION DATA")
    print("=" * 80)

    # Use brand/category as article proxy (same as other scripts)
    query = f"""
    SELECT 
        user_id,
        session_id,
        brand,
        '' as category,
        concat(brand, '_cat_', toString(unique_categories_count)) as article_id,
        device_type,
        device_os,
        browser,
        country,
        subscription_tier,
        user_segment,
        ad_clicks,
        article_views,
        session_duration_sec,
        total_events,
        session_start,
        CASE WHEN ad_clicks > 0 THEN 1 ELSE 0 END as clicked_ad,
        CASE WHEN ad_clicks > 0 THEN 1.0 ELSE 0.0 END as revenue
    FROM customer_sessions
    WHERE session_start IS NOT NULL
    AND brand_id != '' AND brand_id IS NOT NULL
    ORDER BY session_start DESC
    LIMIT {limit}
    """

    results = client.execute(query)

    columns = [
        "user_id",
        "session_id",
        "brand_id",
        "category",
        "article_id",
        "device_type_id",
        "os_id",
        "browser_id",
        "country_code",
        "subscription_tier",
        "user_segment",
        "ad_clicks",
        "article_views",
        "session_duration_sec",
        "total_events",
        "session_start",
        "clicked_ad",
        "revenue",
    ]

    df = pd.DataFrame(results, columns=columns)

    # Simulate ad revenue (in real scenario, this would come from ad server)
    df["revenue"] = df["ad_clicks"] * np.random.uniform(0.1, 2.0, len(df))  # Simulated revenue per click

    print(f"✓ Loaded {len(df):,} ad interactions")
    print(f"  Ad Click Rate: {df['clicked_ad'].mean()*100:.2f}%")
    print(f"  Total Revenue: ${df['revenue'].sum():.2f}")
    print(f"  Avg Revenue per Session: ${df['revenue'].mean():.2f}")

    return df


def create_ad_features(df):
    """Create features for ad optimization"""
    print("\n" + "=" * 80)
    print("CREATING AD FEATURES")
    print("=" * 80)

    # Time-based features
    df["session_start"] = pd.to_datetime(df["session_start"])
    df["hour"] = df["session_start"].dt.hour
    df["day_of_week"] = df["session_start"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    # User features
    user_stats = (
        df.groupby("user_id")
        .agg(
            {
                "ad_clicks": ["mean", "sum", "count"],
                "revenue": ["mean", "sum"],
                "article_views": "mean",
                "session_duration_sec": "mean",
            }
        )
        .reset_index()
    )

    user_stats.columns = ["user_id"] + [f"user_{col[0]}_{col[1]}" for col in user_stats.columns[1:]]
    df = df.merge(user_stats, on="user_id", how="left")

    # Content features
    article_stats = (
        df.groupby("article_id").agg({"ad_clicks": "mean", "revenue": "mean", "article_views": "mean"}).reset_index()
    )

    article_stats.columns = [
        "article_id",
        "article_avg_ad_clicks",
        "article_avg_revenue",
        "article_avg_views",
    ]
    df = df.merge(article_stats, on="article_id", how="left")

    # Brand features
    brand_stats = df.groupby("brand").agg({"revenue": "mean", "ad_clicks": "mean"}).reset_index()
    brand_stats.columns = ["brand", "brand_avg_revenue", "brand_avg_ad_clicks"]
    df = df.merge(brand_stats, on="brand", how="left")

    print(f"✓ Created {len(df.columns)} features")

    return df


def train_revenue_prediction_model(X_train, y_train, X_test, y_test):
    """Train revenue prediction model"""
    print("\n" + "=" * 80)
    print("TRAINING REVENUE PREDICTION MODEL")
    print("=" * 80)

    # Random Forest model
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )

    print("\nTraining model...")
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"\nModel Performance:")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  MAE: {mae:.4f}")
    print(f"  R² Score: {r2:.4f}")

    return model


class ThompsonSampling:
    """Thompson Sampling for Multi-Armed Bandit"""

    def __init__(self, n_arms):
        self.n_arms = n_arms
        self.alpha = np.ones(n_arms)  # Success counts
        self.beta = np.ones(n_arms)  # Failure counts
        self.rewards = [[] for _ in range(n_arms)]

    def select_arm(self):
        """Select arm using Thompson Sampling"""
        samples = [np.random.beta(self.alpha[i], self.beta[i]) for i in range(self.n_arms)]
        return np.argmax(samples)

    def update(self, arm, reward):
        """Update arm statistics"""
        if reward > 0:
            self.alpha[arm] += 1
        else:
            self.beta[arm] += 1
        self.rewards[arm].append(reward)

    def get_arm_stats(self):
        """Get statistics for each arm"""
        stats = []
        for i in range(self.n_arms):
            if len(self.rewards[i]) > 0:
                stats.append(
                    {
                        "arm": i,
                        "mean_reward": np.mean(self.rewards[i]),
                        "total_reward": np.sum(self.rewards[i]),
                        "count": len(self.rewards[i]),
                        "success_rate": self.alpha[i] / (self.alpha[i] + self.beta[i]),
                    }
                )
            else:
                stats.append(
                    {
                        "arm": i,
                        "mean_reward": 0,
                        "total_reward": 0,
                        "count": 0,
                        "success_rate": 0.5,
                    }
                )
        return stats


def simulate_ad_placement_optimization(df, n_arms=5):
    """Simulate ad placement optimization using Thompson Sampling"""
    print("\n" + "=" * 80)
    print("SIMULATING AD PLACEMENT OPTIMIZATION")
    print("=" * 80)

    # Initialize Thompson Sampling
    ts = ThompsonSampling(n_arms)

    # Simulate ad placements
    total_revenue = 0
    arm_selections = np.zeros(n_arms)
    arm_revenues = np.zeros(n_arms)

    print(f"\nRunning Thompson Sampling simulation with {n_arms} ad placements...")

    for idx, row in df.iterrows():
        # Select arm (ad placement)
        arm = ts.select_arm()
        arm_selections[arm] += 1

        # Simulate reward (revenue based on user features)
        # In real scenario, this would be actual ad revenue
        base_revenue = row["revenue"]
        placement_multiplier = [0.8, 1.0, 1.2, 0.9, 1.1][arm]  # Different placement effectiveness
        reward = base_revenue * placement_multiplier

        # Update Thompson Sampling
        ts.update(arm, reward)

        total_revenue += reward
        arm_revenues[arm] += reward

    print(f"\nSimulation Results:")
    print(f"  Total Revenue: ${total_revenue:.2f}")
    print(f"  Avg Revenue per Session: ${total_revenue/len(df):.2f}")

    # Arm statistics
    stats = ts.get_arm_stats()
    print(f"\nArm Statistics:")
    for stat in stats:
        print(f"  Arm {stat['arm']}:")
        print(f"    Selections: {arm_selections[stat['arm']]:.0f}")
        print(f"    Total Revenue: ${arm_revenues[stat['arm']]:.2f}")
        print(f"    Avg Revenue: ${stat['mean_reward']:.2f}")
        print(f"    Success Rate: {stat['success_rate']:.2%}")

    # Find best arm
    best_arm = np.argmax([stat["mean_reward"] for stat in stats])
    print(f"\n✓ Best Ad Placement: Arm {best_arm}")
    print(f"  Avg Revenue: ${stats[best_arm]['mean_reward']:.2f}")
    print(f"  Success Rate: {stats[best_arm]['success_rate']:.2%}")

    return ts, stats


def visualize_ad_optimization(ts, stats):
    """Visualize ad optimization results"""
    print("\n" + "=" * 80)
    print("VISUALIZING AD OPTIMIZATION RESULTS")
    print("=" * 80)

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    # Arm selection counts
    arms = [stat["arm"] for stat in stats]
    selections = [stat["count"] for stat in stats]
    axes[0, 0].bar(arms, selections, color="steelblue")
    axes[0, 0].set_xlabel("Ad Placement (Arm)")
    axes[0, 0].set_ylabel("Number of Selections")
    axes[0, 0].set_title("Ad Placement Selection Counts")
    axes[0, 0].grid(True, axis="y")

    # Arm revenue
    revenues = [stat["total_reward"] for stat in stats]
    axes[0, 1].bar(arms, revenues, color="green")
    axes[0, 1].set_xlabel("Ad Placement (Arm)")
    axes[0, 1].set_ylabel("Total Revenue ($)")
    axes[0, 1].set_title("Total Revenue by Ad Placement")
    axes[0, 1].grid(True, axis="y")

    # Average revenue per arm
    avg_revenues = [stat["mean_reward"] for stat in stats]
    axes[1, 0].bar(arms, avg_revenues, color="orange")
    axes[1, 0].set_xlabel("Ad Placement (Arm)")
    axes[1, 0].set_ylabel("Average Revenue per Session ($)")
    axes[1, 0].set_title("Average Revenue by Ad Placement")
    axes[1, 0].grid(True, axis="y")

    # Success rates
    success_rates = [stat["success_rate"] for stat in stats]
    axes[1, 1].bar(arms, success_rates, color="purple")
    axes[1, 1].set_xlabel("Ad Placement (Arm)")
    axes[1, 1].set_ylabel("Success Rate")
    axes[1, 1].set_title("Success Rate by Ad Placement")
    axes[1, 1].grid(True, axis="y")

    plt.tight_layout()
    viz_file = SCRIPT_DIR / "ad_optimization_results.png"
    plt.savefig(viz_file, dpi=300, bbox_inches="tight")
    print(f"✓ Saved visualization to {viz_file}")


def save_ad_optimization_model(ts, stats, revenue_model):
    """Save ad optimization model"""
    print("\n" + "=" * 80)
    print("SAVING AD OPTIMIZATION MODEL")
    print("=" * 80)

    import os

    models_dir = SCRIPT_DIR / "models"
    os.makedirs(models_dir, exist_ok=True)

    # Save Thompson Sampling
    model_file = models_dir / "ad_optimization_thompson_sampling.pkl"
    with open(model_file, "wb") as f:
        pickle.dump(ts, f)
    print(f"✓ Saved Thompson Sampling to {model_file}")

    # Save revenue prediction model
    revenue_file = models_dir / "ad_revenue_prediction_model.pkl"
    with open(revenue_file, "wb") as f:
        pickle.dump(revenue_model, f)
    print(f"✓ Saved revenue prediction model to {revenue_file}")

    # Save statistics
    stats_file = models_dir / "ad_optimization_stats.json"
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    print(f"✓ Saved statistics to {stats_file}")


def main():
    """Main ad optimization function"""
    print("=" * 80)
    print("MEDIA PUBLISHING - AD REVENUE OPTIMIZATION")
    print("=" * 80)

    # Connect to ClickHouse
    client = connect_to_clickhouse()
    if not client:
        return

    try:
        # Load ad data
        df = load_ad_data(client, limit=100000)

        # Create features
        df = create_ad_features(df)

        # Prepare features for revenue prediction
        feature_columns = [
            "hour",
            "day_of_week",
            "is_weekend",
            "article_views",
            "session_duration_sec",
            "total_events",
            "user_ad_clicks_mean",
            "user_revenue_mean",
            "article_avg_revenue",
            "brand_avg_revenue",
        ]

        available_features = [f for f in feature_columns if f in df.columns]

        # One-hot encode categorical
        categorical_features = [
            "brand",
            "category",
            "device_type",
            "country",
            "subscription_tier",
        ]
        available_categorical = [f for f in categorical_features if f in df.columns]
        df_encoded = pd.get_dummies(df, columns=available_categorical, prefix=available_categorical)

        all_features = available_features + [col for col in df_encoded.columns if col not in df.columns]
        all_features = [
            f
            for f in all_features
            if f
            not in [
                "user_id",
                "session_id",
                "article_id",
                "session_start",
                "revenue",
                "clicked_ad",
                "ad_clicks",
            ]
        ]

        X = df_encoded[all_features].fillna(0).replace([np.inf, -np.inf], 0)
        y = df_encoded["revenue"]

        # Train revenue prediction model
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        revenue_model = train_revenue_prediction_model(X_train, y_train, X_test, y_test)

        # Simulate ad placement optimization
        ts, stats = simulate_ad_placement_optimization(df, n_arms=5)

        # Visualize results
        visualize_ad_optimization(ts, stats)

        # Save models
        save_ad_optimization_model(ts, stats, revenue_model)

        print("\n" + "=" * 80)
        print("AD REVENUE OPTIMIZATION COMPLETE")
        print("=" * 80)
        print(f"\nOptimization Results:")
        best_arm = np.argmax([stat["mean_reward"] for stat in stats])
        print(f"  Best Ad Placement: Arm {best_arm}")
        print(f"  Avg Revenue: ${stats[best_arm]['mean_reward']:.2f}")
        print(f"\nNext Steps:")
        print("  1. Deploy Thompson Sampling for real-time ad placement")
        print("  2. Integrate with ad server")
        print("  3. A/B test different ad placements")
        print("  4. Monitor revenue optimization")

    except Exception as e:
        print(f"\n✗ Error during ad optimization: {e}")
        import traceback

        traceback.print_exc()
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
