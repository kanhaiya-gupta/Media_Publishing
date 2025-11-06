#!/usr/bin/env python3
"""
Phase 1: Baseline Models

This script implements baseline models for comparison:
- Logistic Regression
- Random Forest
- Simple Neural Network

These serve as benchmarks before implementing more complex models.

Usage:
    python 03_baseline_models.py
"""

import json
import pickle
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
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
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import StandardScaler

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

        feature_list_file = SCRIPT_DIR / "feature_list.json"
        with open(feature_list_file, "r") as f:
            feature_list = json.load(f)

        print(f"✓ Loaded {len(feature_list)} features")
        return df, feature_list
    except FileNotFoundError:
        print("✗ Features file not found. Run 02_feature_engineering.py first")
        return None, None


def prepare_data(df, feature_list, target="churned"):
    """Prepare data for training"""
    print("\n" + "=" * 80)
    print(f"PREPARING DATA FOR {target.upper()}")
    print("=" * 80)

    X = df[feature_list].copy()
    y = df[target].copy()

    # Handle missing and infinite values
    X = X.fillna(0).replace([np.inf, -np.inf], 0)

    print(f"\nData Shape:")
    print(f"  Features (X): {X.shape}")
    print(f"  Target (y): {y.shape}")

    print(f"\nTarget Distribution:")
    print(f"  Class 0: {(y == 0).sum():,} ({(y == 0).mean()*100:.2f}%)")
    print(f"  Class 1: {(y == 1).sum():,} ({(y == 1).mean()*100:.2f}%)")

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"\nTrain/Test Split:")
    print(f"  Training: {len(X_train):,} samples")
    print(f"  Testing: {len(X_test):,} samples")

    return X_train, X_test, y_train, y_test


def train_logistic_regression(X_train, y_train, X_test, y_test):
    """Train Logistic Regression baseline"""
    print("\n" + "=" * 80)
    print("TRAINING LOGISTIC REGRESSION")
    print("=" * 80)

    # Scale features for logistic regression
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train model
    model = LogisticRegression(
        max_iter=1000, random_state=42, class_weight="balanced", solver="lbfgs"
    )

    print("\nTraining model...")
    model.fit(X_train_scaled, y_train)

    # Evaluate
    y_pred = model.predict(X_test_scaled)
    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]

    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
        "auc": roc_auc_score(y_test, y_pred_proba),
    }

    print(f"\nModel Performance:")
    print(f"  Accuracy: {metrics['accuracy']:.4f}")
    print(f"  Precision: {metrics['precision']:.4f}")
    print(f"  Recall: {metrics['recall']:.4f}")
    print(f"  F1 Score: {metrics['f1']:.4f}")
    print(f"  AUC-ROC: {metrics['auc']:.4f}")

    return model, scaler, metrics


def train_random_forest(X_train, y_train, X_test, y_test):
    """Train Random Forest baseline"""
    print("\n" + "=" * 80)
    print("TRAINING RANDOM FOREST")
    print("=" * 80)

    # Train model
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        class_weight="balanced",
        n_jobs=-1,
    )

    print("\nTraining model...")
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
        "auc": roc_auc_score(y_test, y_pred_proba),
    }

    print(f"\nModel Performance:")
    print(f"  Accuracy: {metrics['accuracy']:.4f}")
    print(f"  Precision: {metrics['precision']:.4f}")
    print(f"  Recall: {metrics['recall']:.4f}")
    print(f"  F1 Score: {metrics['f1']:.4f}")
    print(f"  AUC-ROC: {metrics['auc']:.4f}")

    # Feature importance
    feature_importance = pd.DataFrame(
        {"feature": X_train.columns, "importance": model.feature_importances_}
    ).sort_values("importance", ascending=False)

    print(f"\nTop 10 Feature Importance:")
    for i, row in feature_importance.head(10).iterrows():
        print(f"  {row['feature']:40s}: {row['importance']:.4f}")

    return model, metrics, feature_importance


def train_neural_network(X_train, y_train, X_test, y_test):
    """Train Simple Neural Network baseline"""
    print("\n" + "=" * 80)
    print("TRAINING NEURAL NETWORK")
    print("=" * 80)

    # Scale features for neural network
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train model
    model = MLPClassifier(
        hidden_layer_sizes=(64, 32),
        activation="relu",
        solver="adam",
        alpha=0.001,
        batch_size="auto",
        learning_rate="constant",
        learning_rate_init=0.001,
        max_iter=500,
        random_state=42,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=10,
    )

    print("\nTraining model...")
    model.fit(X_train_scaled, y_train)

    # Evaluate
    y_pred = model.predict(X_test_scaled)
    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]

    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
        "auc": roc_auc_score(y_test, y_pred_proba),
    }

    print(f"\nModel Performance:")
    print(f"  Accuracy: {metrics['accuracy']:.4f}")
    print(f"  Precision: {metrics['precision']:.4f}")
    print(f"  Recall: {metrics['recall']:.4f}")
    print(f"  F1 Score: {metrics['f1']:.4f}")
    print(f"  AUC-ROC: {metrics['auc']:.4f}")

    return model, scaler, metrics


def compare_models(models_metrics):
    """Compare baseline models"""
    print("\n" + "=" * 80)
    print("BASELINE MODELS COMPARISON")
    print("=" * 80)

    comparison_df = pd.DataFrame(models_metrics).T
    comparison_df = comparison_df.sort_values("auc", ascending=False)

    print("\nModel Comparison:")
    print(comparison_df.to_string())

    # Visualization
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))

    # Metrics comparison
    metrics = ["accuracy", "precision", "recall", "f1", "auc"]
    comparison_df[metrics].plot(kind="bar", ax=axes[0, 0])
    axes[0, 0].set_title("Model Metrics Comparison")
    axes[0, 0].set_ylabel("Score")
    axes[0, 0].legend(loc="best")
    axes[0, 0].tick_params(axis="x", rotation=45)

    # AUC comparison
    comparison_df["auc"].plot(kind="bar", ax=axes[0, 1], color="green")
    axes[0, 1].set_title("AUC-ROC Comparison")
    axes[0, 1].set_ylabel("AUC-ROC")
    axes[0, 1].tick_params(axis="x", rotation=45)

    # F1 Score comparison
    comparison_df["f1"].plot(kind="bar", ax=axes[1, 0], color="orange")
    axes[1, 0].set_title("F1 Score Comparison")
    axes[1, 0].set_ylabel("F1 Score")
    axes[1, 0].tick_params(axis="x", rotation=45)

    # Accuracy comparison
    comparison_df["accuracy"].plot(kind="bar", ax=axes[1, 1], color="blue")
    axes[1, 1].set_title("Accuracy Comparison")
    axes[1, 1].set_ylabel("Accuracy")
    axes[1, 1].tick_params(axis="x", rotation=45)

    plt.tight_layout()
    comparison_file = SCRIPT_DIR / "baseline_models_comparison.png"
    plt.savefig(comparison_file, dpi=300, bbox_inches="tight")
    print(f"\n✓ Saved comparison plot to {comparison_file}")

    return comparison_df


def save_baseline_models(models_dict, metrics_dict):
    """Save baseline models"""
    print("\n" + "=" * 80)
    print("SAVING BASELINE MODELS")
    print("=" * 80)

    import os

    models_dir = SCRIPT_DIR / "models" / "baseline"
    os.makedirs(models_dir, exist_ok=True)

    for model_name, model_data in models_dict.items():
        model_file = models_dir / f"{model_name}_baseline.pkl"

        if model_name == "logistic_regression":
            model, scaler = model_data
            with open(model_file, "wb") as f:
                pickle.dump({"model": model, "scaler": scaler}, f)
        elif model_name == "neural_network":
            model, scaler = model_data
            with open(model_file, "wb") as f:
                pickle.dump({"model": model, "scaler": scaler}, f)
        else:
            model = model_data
            with open(model_file, "wb") as f:
                pickle.dump(model, f)

        print(f"✓ Saved {model_name} to {model_file}")

    # Save metrics
    metrics_file = SCRIPT_DIR / "models" / "baseline" / "baseline_models_metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics_dict, f, indent=2, default=str)
    print(f"✓ Saved metrics to {metrics_file}")


def main():
    """Main baseline models function"""
    print("=" * 80)
    print("MEDIA PUBLISHING - BASELINE MODELS")
    print("=" * 80)

    # Load features
    df, feature_list = load_features()
    if df is None:
        return

    try:
        # Prepare data
        X_train, X_test, y_train, y_test = prepare_data(
            df, feature_list, target="churned"
        )

        models_dict = {}
        metrics_dict = {}

        # Train Logistic Regression
        lr_model, lr_scaler, lr_metrics = train_logistic_regression(
            X_train, y_train, X_test, y_test
        )
        models_dict["logistic_regression"] = (lr_model, lr_scaler)
        metrics_dict["logistic_regression"] = lr_metrics

        # Train Random Forest
        rf_model, rf_metrics, rf_importance = train_random_forest(
            X_train, y_train, X_test, y_test
        )
        models_dict["random_forest"] = rf_model
        metrics_dict["random_forest"] = rf_metrics

        # Train Neural Network
        nn_model, nn_scaler, nn_metrics = train_neural_network(
            X_train, y_train, X_test, y_test
        )
        models_dict["neural_network"] = (nn_model, nn_scaler)
        metrics_dict["neural_network"] = nn_metrics

        # Compare models
        comparison_df = compare_models(metrics_dict)

        # Save models
        save_baseline_models(models_dict, metrics_dict)

        print("\n" + "=" * 80)
        print("BASELINE MODELS COMPLETE")
        print("=" * 80)
        print(f"\nBest Model (by AUC): {comparison_df.index[0]}")
        print(f"  AUC-ROC: {comparison_df.loc[comparison_df.index[0], 'auc']:.4f}")
        print(f"\nNext Steps:")
        print("  1. Compare with XGBoost model (04_churn_prediction.py)")
        print("  2. Use best baseline as benchmark")
        print("  3. Proceed to advanced models")

    except Exception as e:
        print(f"\n✗ Error during baseline model training: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
