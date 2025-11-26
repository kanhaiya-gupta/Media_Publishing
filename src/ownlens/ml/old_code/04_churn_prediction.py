#!/usr/bin/env python3
"""
Phase 2: Churn Prediction Model

This script implements churn prediction using XGBoost:
- Loads ML-ready features
- Trains XGBoost model
- Evaluates model performance
- Saves model for deployment

Usage:
    python 04_churn_prediction.py
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
from sklearn.model_selection import cross_val_score, train_test_split

# Get script directory for relative paths
SCRIPT_DIR = Path(__file__).parent.absolute()


def load_features():
    """Load ML-ready features"""
    print("\n" + "=" * 80)
    print("LOADING FEATURES")
    print("=" * 80)

    try:
        features_file = SCRIPT_DIR / "user_features_ml_ready.csv"
        df = pd.read_csv(features_file)
        print(f"✓ Loaded {len(df):,} user features")

        # Load feature list
        feature_list_file = SCRIPT_DIR / "feature_list.json"
        with open(feature_list_file, "r") as f:
            feature_list = json.load(f)

        print(f"✓ Loaded {len(feature_list)} features")

        return df, feature_list
    except FileNotFoundError:
        print("✗ Features file not found. Run 02_feature_engineering.py first")
        return None, None


def prepare_data(df, feature_list):
    """Prepare data for training"""
    print("\n" + "=" * 80)
    print("PREPARING DATA")
    print("=" * 80)

    # Select features
    X = df[feature_list].copy()
    y = df["churned"].copy()

    # Handle missing values
    X = X.fillna(0)

    # Check for infinite values
    X = X.replace([np.inf, -np.inf], 0)

    print(f"\nData Shape:")
    print(f"  Features (X): {X.shape}")
    print(f"  Target (y): {y.shape}")

    print(f"\nTarget Distribution:")
    print(f"  Non-Churned (0): {(y == 0).sum():,} ({(y == 0).mean()*100:.2f}%)")
    print(f"  Churned (1): {(y == 1).sum():,} ({(y == 1).mean()*100:.2f}%)")

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    print(f"\nTrain/Test Split:")
    print(f"  Training: {len(X_train):,} samples")
    print(f"  Testing: {len(X_test):,} samples")

    return X_train, X_test, y_train, y_test


def train_xgboost_model(X_train, y_train, X_test, y_test):
    """Train XGBoost churn prediction model"""
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
    }

    print(f"\nModel Parameters:")
    for key, value in params.items():
        print(f"  {key}: {value}")

    # Create DMatrix for XGBoost
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


def evaluate_model(model, X_test, y_test, feature_list):
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
    print(f"                No Churn  Churn")
    print(f"  Actual No Churn  {cm[0,0]:6d}  {cm[0,1]:6d}")
    print(f"  Actual Churn     {cm[1,0]:6d}  {cm[1,1]:6d}")

    # Classification report
    print(f"\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=["No Churn", "Churn"]))

    # Feature importance
    print(f"\nTop 20 Most Important Features:")
    feature_importance = model.get_score(importance_type="gain")
    sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
    for i, (feature, importance) in enumerate(sorted_features[:20], 1):
        print(f"  {i:2d}. {feature:40s}: {importance:10.2f}")

    # ROC Curve
    fpr, tpr, thresholds = roc_curve(y_test, y_pred_proba)

    plt.figure(figsize=(10, 8))
    plt.plot(fpr, tpr, label=f"ROC Curve (AUC = {auc:.4f})")
    plt.plot([0, 1], [0, 1], "k--", label="Random")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("Churn Prediction - ROC Curve")
    plt.legend()
    plt.grid(True)
    roc_file = SCRIPT_DIR / "churn_roc_curve.png"
    plt.savefig(roc_file, dpi=300, bbox_inches="tight")
    print(f"\n✓ Saved ROC curve to {roc_file}")

    # Feature importance plot
    plt.figure(figsize=(12, 10))
    top_features = sorted_features[:20]
    features = [f[0] for f in top_features]
    importances = [f[1] for f in top_features]

    plt.barh(range(len(features)), importances)
    plt.yticks(range(len(features)), features)
    plt.xlabel("Feature Importance (Gain)")
    plt.title("Top 20 Feature Importance - Churn Prediction")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    importance_file = SCRIPT_DIR / "churn_feature_importance.png"
    plt.savefig(importance_file, dpi=300, bbox_inches="tight")
    print(f"✓ Saved feature importance plot to {importance_file}")

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "auc": auc,
        "confusion_matrix": cm.tolist(),
    }


def save_model(model, feature_list, metrics):
    """Save model and metadata"""
    print("\n" + "=" * 80)
    print("SAVING MODEL")
    print("=" * 80)

    import os

    models_dir = SCRIPT_DIR / "models"
    os.makedirs(models_dir, exist_ok=True)

    # Save model
    model_file = models_dir / "churn_model_xgboost.pkl"
    with open(model_file, "wb") as f:
        pickle.dump(model, f)
    print(f"✓ Saved model to {model_file}")

    # Save feature list
    feature_file = models_dir / "churn_feature_list.json"
    with open(feature_file, "w") as f:
        json.dump(feature_list, f, indent=2)
    print(f"✓ Saved feature list to {feature_file}")

    # Save metrics
    metrics["timestamp"] = datetime.now().isoformat()
    metrics_file = models_dir / "churn_model_metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"✓ Saved metrics to {metrics_file}")

    # Save model metadata
    metadata = {
        "model_type": "XGBoost",
        "task": "Binary Classification",
        "target": "churned",
        "features": len(feature_list),
        "training_date": datetime.now().isoformat(),
        "metrics": metrics,
    }

    metadata_file = models_dir / "churn_model_metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"✓ Saved metadata to {metadata_file}")

    return model_file


def predict_churn_risk(model, feature_list, sample_users=None):
    """Predict churn risk for sample users"""
    print("\n" + "=" * 80)
    print("CHURN RISK PREDICTION")
    print("=" * 80)

    if sample_users is None:
        # Load full dataset for sample predictions
        features_file = SCRIPT_DIR / "user_features_ml_ready.csv"
        df = pd.read_csv(features_file)
        sample_users = df.sample(min(10, len(df)))

    X_sample = sample_users[feature_list].fillna(0).replace([np.inf, -np.inf], 0)
    dtest = xgb.DMatrix(X_sample)
    churn_proba = model.predict(dtest)

    print(f"\nSample Churn Predictions:")
    print(f"{'User ID':<10} {'Churn Probability':<20} {'Risk Level':<15}")
    print("-" * 50)

    for i, (user_id, proba) in enumerate(zip(sample_users["user_id"], churn_proba)):
        if proba < 0.3:
            risk = "Low"
        elif proba < 0.7:
            risk = "Medium"
        else:
            risk = "High"

        print(f"{user_id:<10} {proba:<20.4f} {risk:<15}")

    return churn_proba


def main():
    """Main churn prediction function"""
    print("=" * 80)
    print("MEDIA PUBLISHING - CHURN PREDICTION MODEL")
    print("=" * 80)

    # Load features
    df, feature_list = load_features()
    if df is None:
        return

    try:
        # Prepare data
        X_train, X_test, y_train, y_test = prepare_data(df, feature_list)

        # Train model
        model = train_xgboost_model(X_train, y_train, X_test, y_test)

        # Evaluate model
        metrics = evaluate_model(model, X_test, y_test, feature_list)

        # Save model
        import os

        models_dir = SCRIPT_DIR / "models"
        os.makedirs(models_dir, exist_ok=True)
        model_file = save_model(model, feature_list, metrics)

        # Sample predictions
        sample_df = df.sample(min(10, len(df)))
        predict_churn_risk(model, feature_list, sample_df)

        print("\n" + "=" * 80)
        print("CHURN PREDICTION MODEL COMPLETE")
        print("=" * 80)
        print(f"\nModel Performance Summary:")
        print(f"  AUC-ROC: {metrics['auc']:.4f}")
        print(f"  Accuracy: {metrics['accuracy']:.4f}")
        print(f"  F1 Score: {metrics['f1']:.4f}")
        print(f"\nModel saved to: {model_file}")
        print(f"\nNext Steps:")
        print("  1. Review model performance metrics")
        print("  2. Deploy model for real-time predictions")
        print("  3. Set up model monitoring")
        print("  4. Implement retention campaigns for high-risk users")

    except Exception as e:
        print(f"\n✗ Error during model training: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
