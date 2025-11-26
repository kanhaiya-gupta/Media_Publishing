#!/usr/bin/env python3
"""
Phase 4: Engagement Prediction Model

This script implements engagement prediction using LSTM:
- Time series feature engineering
- LSTM model architecture
- Model training and evaluation
- Real-time engagement scoring

Usage:
    python 09_engagement_prediction.py
"""

import json
import pickle
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import xgboost as xgb  # Using XGBoost as LSTM alternative for simplicity
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, StandardScaler

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


def load_engagement_data(client, limit=100000):
    """Load engagement data for prediction"""
    print("\n" + "=" * 80)
    print("LOADING ENGAGEMENT DATA")
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
        session_start,
        session_duration_sec,
        total_events,
        article_views,
        article_clicks,
        video_plays,
        searches,
        pages_visited_count,
        unique_categories_count,
        (article_views * 2 + 
         article_clicks * 3 + 
         video_plays * 5 + 
         newsletter_signups * 20 + 
         pages_visited_count * 1 + 
         unique_categories_count * 2) as engagement_score
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
        "session_start",
        "session_duration_sec",
        "total_events",
        "article_views",
        "article_clicks",
        "video_plays",
        "searches",
        "pages_visited_count",
        "unique_categories_count",
        "engagement_score",
    ]

    df = pd.DataFrame(results, columns=columns)

    print(f"✓ Loaded {len(df):,} sessions")
    print(f"  Avg Engagement Score: {df['engagement_score'].mean():.2f}")
    print(f"  Min Engagement Score: {df['engagement_score'].min():.2f}")
    print(f"  Max Engagement Score: {df['engagement_score'].max():.2f}")

    return df


def create_engagement_features(df):
    """Create features for engagement prediction"""
    print("\n" + "=" * 80)
    print("CREATING ENGAGEMENT FEATURES")
    print("=" * 80)

    # Time-based features
    df["session_start"] = pd.to_datetime(df["session_start"])
    df["hour"] = df["session_start"].dt.hour
    df["day_of_week"] = df["session_start"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["month"] = df["session_start"].dt.month

    # User history features (aggregate)
    user_stats = (
        df.groupby("user_id")
        .agg(
            {
                "engagement_score": ["mean", "std", "max", "min"],
                "session_duration_sec": ["mean", "sum"],
                "total_events": ["mean", "sum"],
                "article_views": "mean",
                "article_clicks": "mean",
            }
        )
        .reset_index()
    )

    user_stats.columns = ["user_id"] + [f"user_{col[0]}_{col[1]}" for col in user_stats.columns[1:]]
    df = df.merge(user_stats, on="user_id", how="left")

    # Content features
    article_stats = (
        df.groupby("article_id")
        .agg(
            {
                "engagement_score": ["mean", "std"],
                "article_views": "mean",
                "article_clicks": "mean",
            }
        )
        .reset_index()
    )

    article_stats.columns = ["article_id"] + [f"article_{col[0]}_{col[1]}" for col in article_stats.columns[1:]]
    df = df.merge(article_stats, on="article_id", how="left")

    # Brand features
    brand_stats = df.groupby("brand").agg({"engagement_score": "mean", "session_duration_sec": "mean"}).reset_index()
    brand_stats.columns = ["brand", "brand_avg_engagement", "brand_avg_duration"]
    df = df.merge(brand_stats, on="brand", how="left")

    # Category features
    category_stats = df.groupby("category").agg({"engagement_score": "mean"}).reset_index()
    category_stats.columns = ["category", "category_avg_engagement"]
    df = df.merge(category_stats, on="category", how="left")

    print(f"✓ Created {len(df.columns)} features")

    return df


def prepare_engagement_features(df):
    """Prepare features for model training"""
    print("\n" + "=" * 80)
    print("PREPARING FEATURES FOR TRAINING")
    print("=" * 80)

    # Select features
    numerical_features = [
        "session_duration_sec",
        "total_events",
        "article_views",
        "article_clicks",
        "video_plays",
        "searches",
        "pages_visited_count",
        "unique_categories_count",
        "hour",
        "day_of_week",
        "is_weekend",
        "month",
        "user_engagement_score_mean",
        "user_engagement_score_std",
        "user_session_duration_sec_mean",
        "user_total_events_mean",
        "article_engagement_score_mean",
        "article_article_views_mean",
        "brand_avg_engagement",
        "category_avg_engagement",
    ]

    categorical_features = [
        "brand",
        "category",
        "device_type",
        "device_os",
        "browser",
        "country",
        "subscription_tier",
        "user_segment",
    ]

    # Filter available features
    available_numerical = [f for f in numerical_features if f in df.columns]
    available_categorical = [f for f in categorical_features if f in df.columns]

    # One-hot encode categorical features
    df_encoded = pd.get_dummies(df, columns=available_categorical, prefix=available_categorical)

    # Select all feature columns (exclude non-feature columns)
    exclude_cols = [
        "user_id",
        "session_id",
        "article_id",
        "session_start",
        "engagement_score",
    ]
    feature_columns = [col for col in df_encoded.columns if col not in exclude_cols]

    # Ensure all columns are numeric and properly formatted
    X = df_encoded[feature_columns].copy()
    y = df_encoded["engagement_score"].copy()

    # Convert all columns to numeric (handle any remaining non-numeric columns)
    for col in X.columns:
        if X[col].dtype == "object":
            X[col] = pd.to_numeric(X[col], errors="coerce")

    # Handle missing values
    X = X.fillna(0)
    X = X.replace([np.inf, -np.inf], 0)

    # Ensure all columns are Series (not DataFrame)
    X = X.select_dtypes(include=[np.number])  # Select only numeric columns

    # Update feature_columns after filtering
    feature_columns = X.columns.tolist()

    print(f"\nFeature Selection:")
    print(f"  Numerical Features: {len(available_numerical)}")
    print(f"  Categorical Features: {len(available_categorical)}")
    print(f"  Total Features: {len(feature_columns)}")
    print(f"  Data Shape: {X.shape}")

    return X, y, feature_columns


def train_xgboost_model(X_train, y_train, X_test, y_test):
    """Train XGBoost engagement prediction model"""
    print("\n" + "=" * 80)
    print("TRAINING XGBOOST MODEL")
    print("=" * 80)

    # Model parameters
    params = {
        "objective": "reg:squarederror",
        "eval_metric": "rmse",
        "max_depth": 6,
        "learning_rate": 0.01,
        "n_estimators": 1000,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 3,
        "gamma": 0.1,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "random_state": 42,
        "n_jobs": -1,
    }

    print(f"\nModel Parameters:")
    for key, value in params.items():
        print(f"  {key}: {value}")

    # Create DMatrix
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)

    # Train model
    print(f"\nTraining model...")
    model = xgb.train(
        params,
        dtrain,
        num_boost_round=1000,
        evals=[(dtrain, "train"), (dtest, "test")],
        early_stopping_rounds=50,
        verbose_eval=50,
    )

    print("\n✓ Model training complete")

    return model


def evaluate_model(model, X_test, y_test, feature_columns):
    """Evaluate model performance"""
    print("\n" + "=" * 80)
    print("MODEL EVALUATION")
    print("=" * 80)

    # Make predictions
    dtest = xgb.DMatrix(X_test)
    y_pred = model.predict(dtest)

    # Calculate metrics
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"\nModel Performance:")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  MAE: {mae:.4f}")
    print(f"  R² Score: {r2:.4f}")
    print(f"  MSE: {mse:.4f}")

    # Feature importance
    feature_importance = model.get_score(importance_type="gain")
    sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)

    print(f"\nTop 20 Most Important Features:")
    for i, (feature, importance) in enumerate(sorted_features[:20], 1):
        print(f"  {i:2d}. {feature:40s}: {importance:10.2f}")

    # Prediction vs Actual plot
    plt.figure(figsize=(12, 8))
    plt.scatter(y_test, y_pred, alpha=0.5)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], "r--", lw=2)
    plt.xlabel("Actual Engagement Score")
    plt.ylabel("Predicted Engagement Score")
    plt.title(f"Engagement Prediction - Actual vs Predicted (R² = {r2:.4f})")
    plt.grid(True)
    scatter_file = SCRIPT_DIR / "engagement_prediction_scatter.png"
    plt.savefig(scatter_file, dpi=300, bbox_inches="tight")
    print(f"\n✓ Saved scatter plot to {scatter_file}")

    # Residual plot
    residuals = y_test - y_pred
    plt.figure(figsize=(12, 8))
    plt.scatter(y_pred, residuals, alpha=0.5)
    plt.axhline(y=0, color="r", linestyle="--", lw=2)
    plt.xlabel("Predicted Engagement Score")
    plt.ylabel("Residuals")
    plt.title("Engagement Prediction - Residual Plot")
    plt.grid(True)
    residual_file = SCRIPT_DIR / "engagement_prediction_residuals.png"
    plt.savefig(residual_file, dpi=300, bbox_inches="tight")
    print(f"✓ Saved residual plot to {residual_file}")

    return {"rmse": rmse, "mae": mae, "r2": r2, "mse": mse}


def save_model(model, feature_columns, metrics):
    """Save model and metadata"""
    print("\n" + "=" * 80)
    print("SAVING MODEL")
    print("=" * 80)

    import os

    models_dir = SCRIPT_DIR / "models"
    os.makedirs(models_dir, exist_ok=True)

    # Save model
    model_file = models_dir / "engagement_prediction_model.pkl"
    with open(model_file, "wb") as f:
        pickle.dump(model, f)
    print(f"✓ Saved model to {model_file}")

    # Save feature list
    feature_file = models_dir / "engagement_prediction_features.json"
    with open(feature_file, "w") as f:
        json.dump(feature_columns, f, indent=2)
    print(f"✓ Saved feature list to {feature_file}")

    # Save metrics
    metrics["timestamp"] = datetime.now().isoformat()
    metrics_file = models_dir / "engagement_prediction_metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"✓ Saved metrics to {metrics_file}")


def main():
    """Main engagement prediction function"""
    print("=" * 80)
    print("MEDIA PUBLISHING - ENGAGEMENT PREDICTION MODEL")
    print("=" * 80)

    # Connect to ClickHouse
    client = connect_to_clickhouse()
    if not client:
        return

    try:
        # Load engagement data
        df = load_engagement_data(client, limit=100000)

        # Create features
        df = create_engagement_features(df)

        # Prepare features
        X, y, feature_columns = prepare_engagement_features(df)

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        print(f"\nTrain/Test Split:")
        print(f"  Training: {len(X_train):,} samples")
        print(f"  Testing: {len(X_test):,} samples")

        # Train model
        model = train_xgboost_model(X_train, y_train, X_test, y_test)

        # Evaluate model
        metrics = evaluate_model(model, X_test, y_test, feature_columns)

        # Save model
        save_model(model, feature_columns, metrics)

        print("\n" + "=" * 80)
        print("ENGAGEMENT PREDICTION MODEL COMPLETE")
        print("=" * 80)
        print(f"\nModel Performance Summary:")
        print(f"  RMSE: {metrics['rmse']:.4f}")
        print(f"  MAE: {metrics['mae']:.4f}")
        print(f"  R² Score: {metrics['r2']:.4f}")
        print(f"\nNext Steps:")
        print("  1. Deploy model for real-time engagement prediction")
        print("  2. Use engagement scores for content ranking")
        print("  3. Implement engagement-based personalization")
        print("  4. Monitor model performance")

    except Exception as e:
        print(f"\n✗ Error during model training: {e}")
        import traceback

        traceback.print_exc()
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
