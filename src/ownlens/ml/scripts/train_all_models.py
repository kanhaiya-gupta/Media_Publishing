#!/usr/bin/env python3
"""
OwnLens - ML Module: Train All Models

Script to train all ML models and register them in the model registry.

Usage:
    python -m ownlens.ml.scripts.train_all_models
    python -m ownlens.ml.scripts.train_all_models --domain customer
    python -m ownlens.ml.scripts.train_all_models --domain editorial
"""

import sys
import argparse
from pathlib import Path
from datetime import date, datetime, timedelta
import time
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
from ownlens.ml.registry import ModelRegistry, MetadataManager

# Customer domain models
from ownlens.ml.models.customer.churn import ChurnTrainer
from ownlens.ml.models.customer.segmentation import UserSegmentationTrainer
from ownlens.ml.models.customer.conversion import ConversionTrainer
from ownlens.ml.models.customer.recommendation import UserRecommendationTrainer

# Editorial domain models
from ownlens.ml.models.editorial.performance import ArticlePerformanceTrainer
from ownlens.ml.models.editorial.trending import TrendingTopicsTrainer
from ownlens.ml.models.editorial.author import AuthorPerformanceTrainer
from ownlens.ml.models.editorial.headline import HeadlineOptimizationTrainer
from ownlens.ml.models.editorial.recommendation import ContentRecommendationTrainer

# Setup logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def train_customer_models(brand_id: str = None, limit: int = None):
    """Train all customer domain models."""
    logger.info("=" * 80)
    logger.info("TRAINING CUSTOMER DOMAIN MODELS")
    logger.info("=" * 80)
    
    registry = ModelRegistry()
    metadata_manager = MetadataManager()
    feature_date = date.today()
    
    models_trained = []
    
    # 1. Churn Prediction
    logger.info("\n[1/4] Training Churn Prediction Model...")
    try:
        trainer = ChurnTrainer()
        start_time = time.time()
        metrics = trainer.train(
            feature_date=feature_date,
            brand_id=brand_id,
            limit=limit or 10000,
            test_size=0.2
        )
        training_duration = int(time.time() - start_time)
        
        model_id = registry.register_model(
            model=trainer.model,
            metadata={
                'model_code': 'churn_prediction',
                'model_type': 'customer',
                'domain': 'customer',
                'algorithm': 'XGBoost',
                'model_framework': 'xgboost',
                'model_format': 'pickle',
                'model_name': 'Churn Prediction Model',
                'model_version': '1.0.0',
                'description': 'Predicts user churn probability',
                'brand_id': brand_id,
                'model_status': 'VALIDATED'
            },
            metrics={
                'n_samples': metrics.get('n_samples', 0),
                'n_features': metrics.get('n_features', 0),
                'training_duration_sec': training_duration,
                'performance_metrics': {
                    'accuracy': metrics.get('accuracy', 0.0),
                    'roc_auc': metrics.get('roc_auc', 0.0),
                    'precision': metrics.get('precision', 0.0),
                    'recall': metrics.get('recall', 0.0),
                    'f1': metrics.get('f1', 0.0)
                },
                'feature_importance': metrics.get('feature_importance', {})
            }
        )
        
        # Save training run
        metadata_manager.save_training_run(
            model_id=model_id,
            run_metadata={
                'run_name': 'Churn Model Training',
                'run_type': 'TRAINING',
                'run_status': 'COMPLETED',
                'started_at': datetime.fromtimestamp(start_time),
                'completed_at': datetime.now(),
                'training_dataset_size': metrics.get('n_samples', 0),
                'hyperparameters': trainer.model.model_config
            },
            metrics=metrics
        )
        
        models_trained.append(('churn_prediction', model_id))
        logger.info(f"✅ Churn model trained and registered: {model_id}")
    except Exception as e:
        logger.error(f"❌ Failed to train churn model: {e}", exc_info=True)
    
    # 2. User Segmentation
    logger.info("\n[2/4] Training User Segmentation Model...")
    try:
        trainer = UserSegmentationTrainer(n_clusters=5)
        start_time = time.time()
        metrics = trainer.train(
            feature_date=feature_date,
            brand_id=brand_id,
            limit=limit or 10000
        )
        training_duration = int(time.time() - start_time)
        
        model_id = registry.register_model(
            model=trainer.model,
            metadata={
                'model_code': 'user_segmentation',
                'model_type': 'customer',
                'domain': 'customer',
                'algorithm': 'K-Means',
                'model_framework': 'scikit-learn',
                'model_format': 'pickle',
                'model_name': 'User Segmentation Model',
                'model_version': '1.0.0',
                'description': 'Segments users into groups based on behavior',
                'brand_id': brand_id,
                'model_status': 'VALIDATED'
            },
            metrics={
                'n_samples': metrics.get('n_samples', 0),
                'n_features': metrics.get('n_features', 0),
                'n_clusters': metrics.get('n_clusters', 5),
                'training_duration_sec': training_duration,
                'performance_metrics': {
                    'inertia': metrics.get('inertia', 0.0),
                    'cluster_stats': metrics.get('cluster_stats', {})
                },
                'feature_importance': metrics.get('feature_importance', {})
            }
        )
        
        models_trained.append(('user_segmentation', model_id))
        logger.info(f"✅ Segmentation model trained and registered: {model_id}")
    except Exception as e:
        logger.error(f"❌ Failed to train segmentation model: {e}", exc_info=True)
    
    # 3. Conversion Prediction
    logger.info("\n[3/4] Training Conversion Prediction Model...")
    try:
        trainer = ConversionTrainer()
        start_time = time.time()
        metrics = trainer.train(
            feature_date=feature_date,
            brand_id=brand_id,
            limit=limit or 10000,
            test_size=0.2
        )
        training_duration = int(time.time() - start_time)
        
        model_id = registry.register_model(
            model=trainer.model,
            metadata={
                'model_code': 'conversion_prediction',
                'model_type': 'customer',
                'domain': 'customer',
                'algorithm': 'XGBoost',
                'model_framework': 'xgboost',
                'model_format': 'pickle',
                'model_name': 'Conversion Prediction Model',
                'model_version': '1.0.0',
                'description': 'Predicts subscription conversion probability',
                'brand_id': brand_id,
                'model_status': 'VALIDATED'
            },
            metrics={
                'n_samples': metrics.get('n_samples', 0),
                'n_features': metrics.get('n_features', 0),
                'training_duration_sec': training_duration,
                'performance_metrics': metrics.get('performance_metrics', {}),
                'feature_importance': metrics.get('feature_importance', {})
            }
        )
        
        models_trained.append(('conversion_prediction', model_id))
        logger.info(f"✅ Conversion model trained and registered: {model_id}")
    except Exception as e:
        logger.error(f"❌ Failed to train conversion model: {e}", exc_info=True)
    
    # 4. User Recommendation
    logger.info("\n[4/4] Training User Recommendation Model...")
    try:
        trainer = UserRecommendationTrainer(n_components=50)
        start_time = time.time()
        metrics = trainer.train(
            start_date=date.today() - timedelta(days=30),
            end_date=date.today(),
            brand_id=brand_id,
            limit=limit or 100000
        )
        training_duration = int(time.time() - start_time)
        
        model_id = registry.register_model(
            model=trainer.model,
            metadata={
                'model_code': 'user_recommendation',
                'model_type': 'customer',
                'domain': 'customer',
                'algorithm': 'NMF',
                'model_framework': 'scikit-learn',
                'model_format': 'pickle',
                'model_name': 'User Content Recommendation Model',
                'model_version': '1.0.0',
                'description': 'Recommends articles to users based on interaction history',
                'brand_id': brand_id,
                'model_status': 'VALIDATED'
            },
            metrics={
                'n_users': metrics.get('n_users', 0),
                'n_articles': metrics.get('n_articles', 0),
                'n_interactions': metrics.get('n_interactions', 0),
                'training_duration_sec': training_duration,
                'performance_metrics': {
                    'reconstruction_error': metrics.get('reconstruction_error', 0.0)
                }
            }
        )
        
        models_trained.append(('user_recommendation', model_id))
        logger.info(f"✅ Recommendation model trained and registered: {model_id}")
    except Exception as e:
        logger.error(f"❌ Failed to train recommendation model: {e}", exc_info=True)
    
    logger.info("\n" + "=" * 80)
    logger.info(f"CUSTOMER DOMAIN TRAINING COMPLETE: {len(models_trained)} models trained")
    logger.info("=" * 80)
    
    return models_trained


def train_editorial_models(brand_id: str = None, limit: int = None):
    """Train all editorial domain models."""
    logger.info("=" * 80)
    logger.info("TRAINING EDITORIAL DOMAIN MODELS")
    logger.info("=" * 80)
    
    registry = ModelRegistry()
    metadata_manager = MetadataManager()
    performance_date = date.today()
    
    models_trained = []
    
    # 1. Article Performance
    logger.info("\n[1/5] Training Article Performance Model...")
    try:
        trainer = ArticlePerformanceTrainer()
        start_time = time.time()
        metrics = trainer.train(
            performance_date=performance_date,
            brand_id=brand_id,
            limit=limit or 1000,
            test_size=0.2,
            target_col='engagement_score'
        )
        training_duration = int(time.time() - start_time)
        
        model_id = registry.register_model(
            model=trainer.model,
            metadata={
                'model_code': 'article_performance',
                'model_type': 'editorial',
                'domain': 'editorial',
                'algorithm': 'XGBoost',
                'model_framework': 'xgboost',
                'model_format': 'pickle',
                'model_name': 'Article Performance Prediction Model',
                'model_version': '1.0.0',
                'description': 'Predicts article engagement score',
                'brand_id': brand_id,
                'model_status': 'VALIDATED'
            },
            metrics={
                'n_samples': metrics.get('n_samples', 0),
                'n_features': metrics.get('n_features', 0),
                'training_duration_sec': training_duration,
                'performance_metrics': metrics.get('performance_metrics', {}),
                'feature_importance': metrics.get('feature_importance', {})
            }
        )
        
        models_trained.append(('article_performance', model_id))
        logger.info(f"✅ Article performance model trained and registered: {model_id}")
    except Exception as e:
        logger.error(f"❌ Failed to train article performance model: {e}", exc_info=True)
    
    # 2. Trending Topics
    logger.info("\n[2/5] Training Trending Topics Model...")
    try:
        trainer = TrendingTopicsTrainer()
        start_time = time.time()
        metrics = trainer.train(
            performance_date=performance_date,
            brand_id=brand_id,
            limit=limit or 100,
            test_size=0.2
        )
        training_duration = int(time.time() - start_time)
        
        model_id = registry.register_model(
            model=trainer.model,
            metadata={
                'model_code': 'trending_topics',
                'model_type': 'editorial',
                'domain': 'editorial',
                'algorithm': 'XGBoost',
                'model_framework': 'xgboost',
                'model_format': 'pickle',
                'model_name': 'Trending Topics Detection Model',
                'model_version': '1.0.0',
                'description': 'Detects trending topics for categories',
                'brand_id': brand_id,
                'model_status': 'VALIDATED'
            },
            metrics={
                'n_samples': metrics.get('n_samples', 0),
                'n_features': metrics.get('n_features', 0),
                'training_duration_sec': training_duration,
                'feature_importance': metrics.get('feature_importance', {})
            }
        )
        
        models_trained.append(('trending_topics', model_id))
        logger.info(f"✅ Trending topics model trained and registered: {model_id}")
    except Exception as e:
        logger.error(f"❌ Failed to train trending topics model: {e}", exc_info=True)
    
    # 3. Author Performance
    logger.info("\n[3/5] Training Author Performance Model...")
    try:
        trainer = AuthorPerformanceTrainer()
        start_time = time.time()
        metrics = trainer.train(
            performance_date=performance_date,
            brand_id=brand_id,
            limit=limit or 100,
            test_size=0.2
        )
        training_duration = int(time.time() - start_time)
        
        model_id = registry.register_model(
            model=trainer.model,
            metadata={
                'model_code': 'author_performance',
                'model_type': 'editorial',
                'domain': 'editorial',
                'algorithm': 'XGBoost',
                'model_framework': 'xgboost',
                'model_format': 'pickle',
                'model_name': 'Author Performance Prediction Model',
                'model_version': '1.0.0',
                'description': 'Predicts author engagement score',
                'brand_id': brand_id,
                'model_status': 'VALIDATED'
            },
            metrics={
                'n_samples': metrics.get('n_samples', 0),
                'n_features': metrics.get('n_features', 0),
                'training_duration_sec': training_duration,
                'feature_importance': metrics.get('feature_importance', {})
            }
        )
        
        models_trained.append(('author_performance', model_id))
        logger.info(f"✅ Author performance model trained and registered: {model_id}")
    except Exception as e:
        logger.error(f"❌ Failed to train author performance model: {e}", exc_info=True)
    
    # 4. Headline Optimization
    logger.info("\n[4/5] Training Headline Optimization Model...")
    try:
        trainer = HeadlineOptimizationTrainer()
        start_time = time.time()
        metrics = trainer.train(
            brand_id=brand_id,
            limit=limit or 1000,
            test_size=0.2
        )
        training_duration = int(time.time() - start_time)
        
        model_id = registry.register_model(
            model=trainer.model,
            metadata={
                'model_code': 'headline_optimization',
                'model_type': 'editorial',
                'domain': 'editorial',
                'algorithm': 'XGBoost',
                'model_framework': 'xgboost',
                'model_format': 'pickle',
                'model_name': 'Headline Optimization Model',
                'model_version': '1.0.0',
                'description': 'Predicts CTR for headline variants',
                'brand_id': brand_id,
                'model_status': 'VALIDATED'
            },
            metrics={
                'n_samples': metrics.get('n_samples', 0),
                'n_features': metrics.get('n_features', 0),
                'training_duration_sec': training_duration,
                'feature_importance': metrics.get('feature_importance', {})
            }
        )
        
        models_trained.append(('headline_optimization', model_id))
        logger.info(f"✅ Headline optimization model trained and registered: {model_id}")
    except Exception as e:
        logger.error(f"❌ Failed to train headline optimization model: {e}", exc_info=True)
    
    # 5. Content Recommendation
    logger.info("\n[5/5] Training Content Recommendation Model...")
    try:
        trainer = ContentRecommendationTrainer()
        start_time = time.time()
        metrics = trainer.train(
            performance_date=performance_date,
            brand_id=brand_id,
            limit=limit or 1000,
            min_engagement_score=10.0
        )
        training_duration = int(time.time() - start_time)
        
        model_id = registry.register_model(
            model=trainer.model,
            metadata={
                'model_code': 'content_recommendation',
                'model_type': 'editorial',
                'domain': 'editorial',
                'algorithm': 'Collaborative Filtering',
                'model_framework': 'scikit-learn',
                'model_format': 'pickle',
                'model_name': 'Content Recommendation Model',
                'model_version': '1.0.0',
                'description': 'Recommends similar articles based on content',
                'brand_id': brand_id,
                'model_status': 'VALIDATED'
            },
            metrics={
                'n_articles': metrics.get('n_articles', 0),
                'n_features': metrics.get('n_features', 0),
                'training_duration_sec': training_duration,
                'performance_metrics': {
                    'similarity_matrix_shape': metrics.get('similarity_matrix_shape', (0, 0))
                }
            }
        )
        
        models_trained.append(('content_recommendation', model_id))
        logger.info(f"✅ Content recommendation model trained and registered: {model_id}")
    except Exception as e:
        logger.error(f"❌ Failed to train content recommendation model: {e}", exc_info=True)
    
    logger.info("\n" + "=" * 80)
    logger.info(f"EDITORIAL DOMAIN TRAINING COMPLETE: {len(models_trained)} models trained")
    logger.info("=" * 80)
    
    return models_trained


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Train all ML models')
    parser.add_argument('--domain', choices=['customer', 'editorial', 'all'], default='all',
                        help='Domain to train (default: all)')
    parser.add_argument('--brand-id', type=str, help='Brand ID filter')
    parser.add_argument('--limit', type=int, help='Limit number of samples')
    parser.add_argument('--skip-registry', action='store_true',
                        help='Skip model registry (for testing)')
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("OWNLENS ML FRAMEWORK - TRAIN ALL MODELS")
    logger.info("=" * 80)
    logger.info(f"Domain: {args.domain}")
    logger.info(f"Brand ID: {args.brand_id or 'All'}")
    logger.info(f"Limit: {args.limit or 'None'}")
    logger.info("=" * 80)
    
    all_models = []
    
    if args.domain in ['customer', 'all']:
        customer_models = train_customer_models(args.brand_id, args.limit)
        all_models.extend(customer_models)
    
    if args.domain in ['editorial', 'all']:
        editorial_models = train_editorial_models(args.brand_id, args.limit)
        all_models.extend(editorial_models)
    
    logger.info("\n" + "=" * 80)
    logger.info("TRAINING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total models trained: {len(all_models)}")
    for model_code, model_id in all_models:
        logger.info(f"  ✅ {model_code}: {model_id}")
    logger.info("=" * 80)
    
    return all_models


if __name__ == "__main__":
    main()

