#!/usr/bin/env python3
"""
OwnLens - ML Module: Batch Prediction

Script to run batch predictions for all models and save to ClickHouse.

Usage:
    python -m ownlens.ml.scripts.predict_batch --model churn_prediction
    python -m ownlens.ml.scripts.predict_batch --model churn_prediction --user-id user-123
    python -m ownlens.ml.scripts.predict_batch --model churn_prediction --batch-size 1000
"""

import sys
import argparse
from pathlib import Path
from datetime import date, datetime
import time
import logging
import uuid

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
from ownlens.ml.storage import PredictionStorage
from ownlens.ml.monitoring import PerformanceMonitor

# Customer domain models
from ownlens.ml.models.customer.churn import ChurnPredictor
from ownlens.ml.models.customer.segmentation import UserSegmentationPredictor
from ownlens.ml.models.customer.conversion import ConversionPredictor
from ownlens.ml.models.customer.recommendation import UserRecommendationPredictor

# Editorial domain models
from ownlens.ml.models.editorial.performance import ArticlePerformancePredictor
from ownlens.ml.models.editorial.trending import TrendingTopicsPredictor
from ownlens.ml.models.editorial.author import AuthorPerformancePredictor
from ownlens.ml.models.editorial.headline import HeadlineOptimizationPredictor
from ownlens.ml.models.editorial.recommendation import ContentRecommendationPredictor

# Setup logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def predict_churn_batch(
    model_id: str = None,
    user_ids: list = None,
    batch_size: int = 1000,
    brand_id: str = None,
    company_id: str = None
):
    """Run batch churn predictions."""
    logger.info("=" * 80)
    logger.info("BATCH CHURN PREDICTIONS")
    logger.info("=" * 80)
    
    registry = ModelRegistry()
    storage = PredictionStorage()
    monitor = PerformanceMonitor()
    
    # Load model
    if model_id:
        # Load specific model
        model, metadata = registry.get_model(model_id=model_id)
    else:
        # Load latest production model
        model, metadata = registry.get_model('churn_prediction', status='PRODUCTION')
    
    model_id = metadata['model_id']
    logger.info(f"Using model: {model_id} ({metadata['model_code']} v{metadata['model_version']})")
    
    # Create predictor
    predictor = ChurnPredictor(model)
    
    # Get user IDs if not provided
    if user_ids is None:
        from ownlens.ml.data.loaders.user_features_loader import UserFeaturesLoader
        loader = UserFeaturesLoader()
        user_features = loader.load(
            feature_date=date.today(),
            brand_id=brand_id,
            company_id=company_id,
            limit=batch_size
        )
        user_ids = user_features['user_id'].unique().tolist() if 'user_id' in user_features.columns else []
    
    logger.info(f"Predicting churn for {len(user_ids)} users...")
    
    # Batch predictions
    predictions_saved = 0
    batch_id = str(uuid.uuid4())
    
    for i, user_id in enumerate(user_ids, 1):
        try:
            start_time = time.time()
            
            # Make prediction
            predictions = predictor.predict(user_id, return_proba=True)
            latency_ms = (time.time() - start_time) * 1000
            
            # Save prediction
            prediction_id = storage.save_churn_prediction(
                user_id=user_id,
                prediction=predictions,
                model_id=model_id,
                company_id=company_id,
                brand_id=brand_id,
                batch_id=batch_id,
                model_version=metadata['model_version'],
                model_confidence=predictions['churn_probability'].iloc[0]
            )
            
            # Track for monitoring
            monitor.track_prediction(
                model_id=model_id,
                prediction=predictions['churn_probability'].iloc[0],
                latency_ms=latency_ms
            )
            
            predictions_saved += 1
            
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{len(user_ids)} predictions saved")
        
        except Exception as e:
            logger.error(f"Failed to predict for user {user_id}: {e}")
            continue
    
    logger.info(f"✅ Saved {predictions_saved} churn predictions (batch: {batch_id})")
    return predictions_saved


def predict_conversion_batch(
    model_id: str = None,
    user_ids: list = None,
    batch_size: int = 1000,
    brand_id: str = None,
    company_id: str = None
):
    """Run batch conversion predictions."""
    logger.info("=" * 80)
    logger.info("BATCH CONVERSION PREDICTIONS")
    logger.info("=" * 80)
    
    registry = ModelRegistry()
    storage = PredictionStorage()
    monitor = PerformanceMonitor()
    
    # Load model
    if model_id:
        model, metadata = registry.get_model(model_id=model_id)
    else:
        model, metadata = registry.get_model('conversion_prediction', status='PRODUCTION')
    
    model_id = metadata['model_id']
    logger.info(f"Using model: {model_id} ({metadata['model_code']} v{metadata['model_version']})")
    
    # Create predictor
    predictor = ConversionPredictor(model)
    
    # Get user IDs if not provided
    if user_ids is None:
        from ownlens.ml.data.loaders.user_features_loader import UserFeaturesLoader
        loader = UserFeaturesLoader()
        user_features = loader.load(
            feature_date=date.today(),
            brand_id=brand_id,
            company_id=company_id,
            limit=batch_size
        )
        user_ids = user_features['user_id'].unique().tolist() if 'user_id' in user_features.columns else []
    
    logger.info(f"Predicting conversion for {len(user_ids)} users...")
    
    # Batch predictions
    predictions_saved = 0
    batch_id = str(uuid.uuid4())
    
    for i, user_id in enumerate(user_ids, 1):
        try:
            start_time = time.time()
            
            # Make prediction
            predictions = predictor.predict(user_id, return_proba=True)
            latency_ms = (time.time() - start_time) * 1000
            
            # Save prediction
            prediction_id = storage.save_conversion_prediction(
                user_id=user_id,
                prediction=predictions,
                model_id=model_id,
                company_id=company_id,
                brand_id=brand_id,
                batch_id=batch_id,
                model_version=metadata['model_version'],
                model_confidence=predictions['conversion_probability'].iloc[0]
            )
            
            # Track for monitoring
            monitor.track_prediction(
                model_id=model_id,
                prediction=predictions['conversion_probability'].iloc[0],
                latency_ms=latency_ms
            )
            
            predictions_saved += 1
            
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{len(user_ids)} predictions saved")
        
        except Exception as e:
            logger.error(f"Failed to predict for user {user_id}: {e}")
            continue
    
    logger.info(f"✅ Saved {predictions_saved} conversion predictions (batch: {batch_id})")
    return predictions_saved


def predict_segmentation_batch(
    model_id: str = None,
    user_ids: list = None,
    batch_size: int = 1000,
    brand_id: str = None,
    company_id: str = None
):
    """Run batch segmentation predictions."""
    logger.info("=" * 80)
    logger.info("BATCH USER SEGMENTATION")
    logger.info("=" * 80)
    
    registry = ModelRegistry()
    storage = PredictionStorage()
    
    # Load model
    if model_id:
        model, metadata = registry.get_model(model_id=model_id)
    else:
        model, metadata = registry.get_model('user_segmentation', status='PRODUCTION')
    
    model_id = metadata['model_id']
    logger.info(f"Using model: {model_id} ({metadata['model_code']} v{metadata['model_version']})")
    
    # Create predictor
    predictor = UserSegmentationPredictor(model)
    
    # Get user IDs if not provided
    if user_ids is None:
        from ownlens.ml.data.loaders.user_features_loader import UserFeaturesLoader
        loader = UserFeaturesLoader()
        user_features = loader.load(
            feature_date=date.today(),
            brand_id=brand_id,
            company_id=company_id,
            limit=batch_size
        )
        user_ids = user_features['user_id'].unique().tolist() if 'user_id' in user_features.columns else []
    
    logger.info(f"Segmenting {len(user_ids)} users...")
    
    # Batch predictions
    assignments_saved = 0
    
    for i, user_id in enumerate(user_ids, 1):
        try:
            # Make prediction
            segments = predictor.predict(user_id)
            
            # Get segment info (would need segment_id from segments table)
            segment_number = segments['segment_number'].iloc[0]
            confidence = segments['confidence_score'].iloc[0] if 'confidence_score' in segments.columns else 0.5
            
            # Save segment assignment
            # Note: segment_id would come from customer_user_segments table
            # For now, we'll use segment_number as segment_id
            segment_id = f"segment_{segment_number}"
            
            assignment_id = storage.save_segment_assignment(
                user_id=user_id,
                segment_id=segment_id,
                confidence=confidence,
                model_id=model_id,
                company_id=company_id,
                brand_id=brand_id,
                model_version=metadata['model_version']
            )
            
            assignments_saved += 1
            
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{len(user_ids)} assignments saved")
        
        except Exception as e:
            logger.error(f"Failed to segment user {user_id}: {e}")
            continue
    
    logger.info(f"✅ Saved {assignments_saved} segment assignments")
    return assignments_saved


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Run batch predictions')
    parser.add_argument('--model', type=str, required=True,
                        choices=['churn_prediction', 'conversion_prediction', 'user_segmentation',
                                'user_recommendation', 'article_performance', 'trending_topics',
                                'author_performance', 'headline_optimization', 'content_recommendation'],
                        help='Model to use for prediction')
    parser.add_argument('--model-id', type=str, help='Specific model ID (overrides model code)')
    parser.add_argument('--user-id', type=str, help='Single user ID to predict')
    parser.add_argument('--user-ids', type=str, nargs='+', help='List of user IDs to predict')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size (default: 1000)')
    parser.add_argument('--brand-id', type=str, help='Brand ID filter')
    parser.add_argument('--company-id', type=str, help='Company ID filter')
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("OWNLENS ML FRAMEWORK - BATCH PREDICTIONS")
    logger.info("=" * 80)
    logger.info(f"Model: {args.model}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info("=" * 80)
    
    user_ids = None
    if args.user_id:
        user_ids = [args.user_id]
    elif args.user_ids:
        user_ids = args.user_ids
    
    # Route to appropriate prediction function
    if args.model == 'churn_prediction':
        result = predict_churn_batch(
            model_id=args.model_id,
            user_ids=user_ids,
            batch_size=args.batch_size,
            brand_id=args.brand_id,
            company_id=args.company_id
        )
    elif args.model == 'conversion_prediction':
        result = predict_conversion_batch(
            model_id=args.model_id,
            user_ids=user_ids,
            batch_size=args.batch_size,
            brand_id=args.brand_id,
            company_id=args.company_id
        )
    elif args.model == 'user_segmentation':
        result = predict_segmentation_batch(
            model_id=args.model_id,
            user_ids=user_ids,
            batch_size=args.batch_size,
            brand_id=args.brand_id,
            company_id=args.company_id
        )
    else:
        logger.error(f"Batch prediction not yet implemented for model: {args.model}")
        return
    
    logger.info("\n" + "=" * 80)
    logger.info(f"BATCH PREDICTION COMPLETE: {result} predictions saved")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

