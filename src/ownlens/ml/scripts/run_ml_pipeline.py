#!/usr/bin/env python3
"""
OwnLens - ML Module: Complete ML Pipeline

Main script to run the complete ML pipeline:
1. Train models (if needed)
2. Run batch predictions
3. Validate predictions
4. Monitor models

Usage:
    python -m ownlens.ml.scripts.run_ml_pipeline
    python -m ownlens.ml.scripts.run_ml_pipeline --skip-training
    python -m ownlens.ml.scripts.run_ml_pipeline --domain customer
"""

import sys
import argparse
from pathlib import Path
from datetime import date, timedelta
import logging

# Load environment variables from .env file
from dotenv import load_dotenv
project_root = Path(__file__).parent.parent.parent.parent.parent
env_file = project_root / "development.env"
if env_file.exists():
    load_dotenv(env_file)
    print(f"✓ Loaded environment variables from {env_file}")
else:
    print(f"⚠ Warning: development.env not found at {env_file}. Using system environment variables.")

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from ownlens.ml.utils.logging import setup_logging
from ownlens.ml.registry import ModelRegistry

# Setup logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_ml_pipeline(
    domain: str = 'all',
    skip_training: bool = False,
    skip_predictions: bool = False,
    skip_validation: bool = False,
    skip_monitoring: bool = False,
    brand_id: str = None,
    limit: int = None
):
    """Run complete ML pipeline."""
    logger.info("=" * 80)
    logger.info("OWNLENS ML FRAMEWORK - COMPLETE PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Domain: {domain}")
    logger.info(f"Skip training: {skip_training}")
    logger.info(f"Skip predictions: {skip_predictions}")
    logger.info(f"Skip validation: {skip_validation}")
    logger.info(f"Skip monitoring: {skip_monitoring}")
    logger.info("=" * 80)
    
    # Step 1: Train models
    if not skip_training:
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: TRAINING MODELS")
        logger.info("=" * 80)
        
        from ownlens.ml.scripts.train_all_models import train_customer_models, train_editorial_models
        
        if domain in ['customer', 'all']:
            train_customer_models(brand_id=brand_id, limit=limit)
        
        if domain in ['editorial', 'all']:
            train_editorial_models(brand_id=brand_id, limit=limit)
    else:
        logger.info("Skipping training...")
    
    # Step 2: Run batch predictions
    if not skip_predictions:
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: BATCH PREDICTIONS")
        logger.info("=" * 80)
        
        from ownlens.ml.scripts.predict_batch import (
            predict_churn_batch,
            predict_conversion_batch,
            predict_segmentation_batch
        )
        
        if domain in ['customer', 'all']:
            logger.info("Running churn predictions...")
            predict_churn_batch(brand_id=brand_id, batch_size=limit or 1000)
            
            logger.info("Running conversion predictions...")
            predict_conversion_batch(brand_id=brand_id, batch_size=limit or 1000)
            
            logger.info("Running user segmentation...")
            predict_segmentation_batch(brand_id=brand_id, batch_size=limit or 1000)
    else:
        logger.info("Skipping predictions...")
    
    # Step 3: Validate predictions
    if not skip_validation:
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: VALIDATE PREDICTIONS")
        logger.info("=" * 80)
        
        from ownlens.ml.scripts.validate_predictions import (
            validate_churn_predictions,
            validate_conversion_predictions
        )
        
        if domain in ['customer', 'all']:
            logger.info("Validating churn predictions...")
            validate_churn_predictions(days=30, brand_id=brand_id)
            
            logger.info("Validating conversion predictions...")
            validate_conversion_predictions(days=30, brand_id=brand_id)
    else:
        logger.info("Skipping validation...")
    
    # Step 4: Monitor models
    if not skip_monitoring:
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: MODEL MONITORING")
        logger.info("=" * 80)
        
        from ownlens.ml.scripts.monitor_models import monitor_all_models
        
        monitor_all_models(monitoring_date=date.today())
    else:
        logger.info("Skipping monitoring...")
    
    logger.info("\n" + "=" * 80)
    logger.info("ML PIPELINE COMPLETE")
    logger.info("=" * 80)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Run complete ML pipeline')
    parser.add_argument('--domain', choices=['customer', 'editorial', 'all'], default='all',
                        help='Domain to process (default: all)')
    parser.add_argument('--skip-training', action='store_true', help='Skip training step')
    parser.add_argument('--skip-predictions', action='store_true', help='Skip predictions step')
    parser.add_argument('--skip-validation', action='store_true', help='Skip validation step')
    parser.add_argument('--skip-monitoring', action='store_true', help='Skip monitoring step')
    parser.add_argument('--brand-id', type=str, help='Brand ID filter')
    parser.add_argument('--limit', type=int, help='Limit number of samples')
    
    args = parser.parse_args()
    
    run_ml_pipeline(
        domain=args.domain,
        skip_training=args.skip_training,
        skip_predictions=args.skip_predictions,
        skip_validation=args.skip_validation,
        skip_monitoring=args.skip_monitoring,
        brand_id=args.brand_id,
        limit=args.limit
    )


if __name__ == "__main__":
    main()

