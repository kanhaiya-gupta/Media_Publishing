#!/usr/bin/env python3
"""
Phase 4: Click Prediction Model

This script implements click prediction using Wide & Deep Learning:
- Feature engineering for clicks
- Wide & Deep model architecture
- Model training and evaluation
- Real-time inference setup

Usage:
    python 08_click_prediction.py
"""

import json
import pickle
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import xgboost as xgb
from clickhouse_driver import Client
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

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


def load_click_data(client, limit=100000):
    """Load click data for prediction"""
    print("\n" + "=" * 80)
    print("LOADING CLICK DATA")
    print("=" * 80)

    # Use brand/category as article proxy (same as recommendation system)
    query = f"""
    SELECT 
        user_id,
        concat(brand, '_cat_', toString(unique_categories_count)) as article_id,
        '' as article_title,
        '' as category,
        brand,
        device_type,
        device_os,
        browser,
        country,
        city,
        subscription_tier,
        user_segment,
        referrer,
        timezone,
        article_views,
        article_clicks,
        CASE WHEN article_clicks > 0 THEN 1 ELSE 0 END as clicked,
        session_duration_sec,
        total_events,
        pages_visited_count,
        session_start
    FROM session_metrics
    WHERE (article_views > 0 OR article_clicks > 0)
    AND brand != '' AND brand IS NOT NULL
    ORDER BY session_start DESC
    LIMIT {limit}
    """

    results = client.execute(query)

    columns = [
        "user_id",
        "article_id",
        "article_title",
        "category",
        "brand",
        "device_type",
        "device_os",
        "browser",
        "country",
        "city",
        "subscription_tier",
        "user_segment",
        "referrer",
        "timezone",
        "article_views",
        "article_clicks",
        "clicked",
        "session_duration_sec",
        "total_events",
        "pages_visited_count",
        "session_start",
    ]

    df = pd.DataFrame(results, columns=columns)

    print(f"✓ Loaded {len(df):,} interactions")
    print(f"  Click Rate: {df['clicked'].mean()*100:.2f}%")
    print(f"  Unique Users: {df['user_id'].nunique():,}")
    print(f"  Unique Articles: {df['article_id'].nunique():,}")

    return df


def create_click_features(df):
    """Create features for click prediction"""
    print("\n" + "=" * 80)
    print("CREATING CLICK FEATURES")
    print("=" * 80)

    # Time-based features
    df["session_start"] = pd.to_datetime(df["session_start"])
    df["hour"] = df["session_start"].dt.hour
    df["day_of_week"] = df["session_start"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    # User history features (aggregate)
    user_stats = (
        df.groupby("user_id")
        .agg(
            {
                "article_clicks": ["mean", "sum", "count"],
                "article_views": ["mean", "sum"],
                "session_duration_sec": "mean",
                "total_events": "mean",
            }
        )
        .reset_index()
    )
    user_stats.columns = [
        "user_id",
        "user_avg_clicks",
        "user_total_clicks",
        "user_interactions",
        "user_avg_views",
        "user_total_views",
        "user_avg_duration",
        "user_avg_events",
    ]

    df = df.merge(user_stats, on="user_id", how="left")

    # Article popularity features
    article_stats = (
        df.groupby("article_id")
        .agg(
            {
                "article_clicks": ["mean", "sum", "count"],
                "article_views": ["mean", "sum"],
            }
        )
        .reset_index()
    )
    article_stats.columns = [
        "article_id",
        "article_avg_clicks",
        "article_total_clicks",
        "article_interactions",
        "article_avg_views",
        "article_total_views",
    ]

    df = df.merge(article_stats, on="article_id", how="left")

    # Click-through rate
    df["user_ctr"] = df["user_total_clicks"] / (df["user_total_views"] + 1)
    df["article_ctr"] = df["article_total_clicks"] / (df["article_total_views"] + 1)

    print(f"✓ Created {len(df.columns)} features")

    return df


def prepare_click_features(df):
    """Prepare features for model training"""
    print("\n" + "=" * 80)
    print("PREPARING FEATURES FOR TRAINING")
    print("=" * 80)

    # Select features
    numerical_features = [
        "article_views",
        "session_duration_sec",
        "total_events",
        "pages_visited_count",
        "hour",
        "day_of_week",
        "is_weekend",
        "user_avg_clicks",
        "user_total_clicks",
        "user_interactions",
        "user_avg_views",
        "user_total_views",
        "user_avg_duration",
        "user_avg_events",
        "article_avg_clicks",
        "article_total_clicks",
        "article_interactions",
        "article_avg_views",
        "article_total_views",
        "user_ctr",
        "article_ctr",
    ]

    categorical_features = [
        "category",
        "brand",
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
    df_encoded = pd.get_dummies(
        df, columns=available_categorical, prefix=available_categorical
    )

    # Select all feature columns (exclude non-feature columns)
    exclude_cols = [
        "user_id",
        "article_id",
        "article_title",
        "city",
        "referrer",
        "timezone",
        "session_start",
        "clicked",
        "article_clicks",
    ]
    feature_columns = [col for col in df_encoded.columns if col not in exclude_cols]

    # Ensure all columns are numeric and properly formatted
    X = df_encoded[feature_columns].copy()
    y = df_encoded["clicked"].copy()

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
    """Train XGBoost click prediction model"""
    print("\n" + "=" * 80)
    print("TRAINING XGBOOST MODEL")
    print("=" * 80)

    # Model parameters
    params = {
        "objective": "binary:logistic",
        "eval_metric": "auc",
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
        "scale_pos_weight": (y_train == 0).sum()
        / (y_train == 1).sum(),  # Handle class imbalance
    }

    print(f"\nModel Parameters:")
    for key, value in params.items():
        if key != "scale_pos_weight":
            print(f"  {key}: {value}")
        else:
            print(f"  {key}: {value:.2f}")

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
    y_pred_proba = model.predict(dtest)
    y_pred = (y_pred_proba >= 0.5).astype(int)

    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred_proba)

    print(f"\nModel Performance:")
    print(f"  Accuracy: {accuracy:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall: {recall:.4f}")
    print(f"  F1 Score: {f1:.4f}")
    print(f"  AUC-ROC: {auc:.4f}")

    # Confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    print(f"\nConfusion Matrix:")
    print(f"                Predicted")
    print(f"                No Click  Click")
    print(f"  Actual No Click  {cm[0,0]:6d}  {cm[0,1]:6d}")
    print(f"  Actual Click     {cm[1,0]:6d}  {cm[1,1]:6d}")

    # Classification report
    print(f"\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=["No Click", "Click"]))

    # Feature importance
    feature_importance = model.get_score(importance_type="gain")
    sorted_features = sorted(
        feature_importance.items(), key=lambda x: x[1], reverse=True
    )

    print(f"\nTop 20 Most Important Features:")
    for i, (feature, importance) in enumerate(sorted_features[:20], 1):
        print(f"  {i:2d}. {feature:40s}: {importance:10.2f}")

    # ROC Curve
    fpr, tpr, thresholds = roc_curve(y_test, y_pred_proba)

    plt.figure(figsize=(10, 8))
    plt.plot(fpr, tpr, label=f"ROC Curve (AUC = {auc:.4f})")
    plt.plot([0, 1], [0, 1], "k--", label="Random")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("Click Prediction - ROC Curve")
    plt.legend()
    plt.grid(True)
    roc_file = SCRIPT_DIR / "click_prediction_roc_curve.png"
    plt.savefig(roc_file, dpi=300, bbox_inches="tight")
    print(f"\n✓ Saved ROC curve to {roc_file}")

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "auc": auc,
        "confusion_matrix": cm.tolist(),
    }


def save_model(model, feature_columns, metrics):
    """Save model and metadata"""
    print("\n" + "=" * 80)
    print("SAVING MODEL")
    print("=" * 80)

    import os

    models_dir = SCRIPT_DIR / "models"
    os.makedirs(models_dir, exist_ok=True)

    # Save model
    model_file = models_dir / "click_prediction_model.pkl"
    with open(model_file, "wb") as f:
        pickle.dump(model, f)
    print(f"✓ Saved model to {model_file}")

    # Save feature list
    feature_file = models_dir / "click_prediction_features.json"
    with open(feature_file, "w") as f:
        json.dump(feature_columns, f, indent=2)
    print(f"✓ Saved feature list to {feature_file}")

    # Save metrics
    metrics["timestamp"] = datetime.now().isoformat()
    metrics_file = models_dir / "click_prediction_metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"✓ Saved metrics to {metrics_file}")


def main():
    """Main click prediction function"""
    print("=" * 80)
    print("MEDIA PUBLISHING - CLICK PREDICTION MODEL")
    print("=" * 80)

    # Connect to ClickHouse
    client = connect_to_clickhouse()
    if not client:
        return

    try:
        # Load click data
        df = load_click_data(client, limit=100000)

        # Create features
        df = create_click_features(df)

        # Prepare features
        X, y, feature_columns = prepare_click_features(df)

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

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
        print("CLICK PREDICTION MODEL COMPLETE")
        print("=" * 80)
        print(f"\nModel Performance Summary:")
        print(f"  AUC-ROC: {metrics['auc']:.4f}")
        print(f"  Precision: {metrics['precision']:.4f}")
        print(f"  Recall: {metrics['recall']:.4f}")
        print(f"  F1 Score: {metrics['f1']:.4f}")
        print(f"\nNext Steps:")
        print("  1. Deploy model for real-time click prediction")
        print("  2. Integrate with article ranking system")
        print("  3. A/B test click prediction improvements")
        print("  4. Monitor model performance")

    except Exception as e:
        print(f"\n✗ Error during model training: {e}")
        import traceback

        traceback.print_exc()
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
