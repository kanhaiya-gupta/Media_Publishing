#!/usr/bin/env python3
"""
OwnLens - ML Module: Train Churn Model

Example script to train churn prediction model using the new architecture.

Usage:
    python -m ownlens.ml.scripts.train_churn
"""

import sys
from pathlib import Path
from datetime import date

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from ownlens.ml.pipelines.training import ChurnTrainingPipeline
from ownlens.ml.utils.logging import setup_logging

# Setup logging
setup_logging(level=10)  # INFO level

if __name__ == "__main__":
    print("=" * 80)
    print("CHURN PREDICTION MODEL TRAINING")
    print("=" * 80)
    
    # Initialize pipeline
    pipeline = ChurnTrainingPipeline()
    
    # Run training
    metrics = pipeline.run(
        feature_date=date.today(),
        limit=10000,
        test_size=0.2,
        save_model=True
    )
    
    # Print results
    print("\n" + "=" * 80)
    print("TRAINING RESULTS")
    print("=" * 80)
    print(f"Model ID: {metrics.get('model_id', 'N/A')}")
    print(f"Accuracy: {metrics.get('accuracy', 0):.4f}")
    print(f"Precision: {metrics.get('precision', 0):.4f}")
    print(f"Recall: {metrics.get('recall', 0):.4f}")
    print(f"F1 Score: {metrics.get('f1', 0):.4f}")
    print(f"ROC AUC: {metrics.get('roc_auc', 0):.4f}")
    print(f"Model Path: {metrics.get('model_path', 'N/A')}")
    print(f"Metrics Path: {metrics.get('metrics_path', 'N/A')}")
    print("=" * 80)





