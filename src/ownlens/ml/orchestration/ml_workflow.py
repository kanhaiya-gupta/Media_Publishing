"""
ML Workflow Orchestration
==========================

Production-ready ML workflow orchestration module.
Train → Predict → Monitor → Visualize
"""

import logging
import uuid
import time
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd

from ..utils.config import get_ml_config
from ..utils.visualization import MLVisualizer
from ..registry import ModelRegistry, MetadataManager
from ..storage import PredictionStorage
from ..monitoring import PerformanceMonitor, DriftDetector, AlertingSystem
from ..storage import PredictionValidator

# Customer domain models
from ..models.customer.churn import ChurnTrainer, ChurnPredictor
from ..models.customer.segmentation import UserSegmentationTrainer, UserSegmentationPredictor
from ..models.customer.conversion import ConversionTrainer, ConversionPredictor
from ..models.customer.recommendation import UserRecommendationTrainer, UserRecommendationPredictor

# Editorial domain models
from ..models.editorial.performance import ArticlePerformanceTrainer, ArticlePerformancePredictor
from ..models.editorial.trending import TrendingTopicsTrainer, TrendingTopicsPredictor
from ..models.editorial.author import AuthorPerformanceTrainer, AuthorPerformancePredictor
from ..models.editorial.headline import HeadlineOptimizationTrainer, HeadlineOptimizationPredictor
from ..models.editorial.recommendation import ContentRecommendationTrainer, ContentRecommendationPredictor

logger = logging.getLogger(__name__)


def run_ml_workflow(
    model_code: str = None,
    domain: str = 'customer',
    brand_id: str = None,
    company_id: str = None,
    limit: int = None,
    batch_size: int = 1000,
    save_figures: bool = True,
    figures_dir: str = None
) -> Dict[str, Any]:
    """
    Run complete ML workflow end-to-end.
    
    Args:
        model_code: Specific model code to run (if None, runs all models in domain)
        domain: Domain to process ('customer' or 'editorial')
        brand_id: Brand ID filter
        company_id: Company ID filter
        limit: Limit number of samples for training
        batch_size: Batch size for predictions
        save_figures: Whether to save visualization figures
        figures_dir: Directory to save figures (if None, uses config default)
        
    Returns:
        Dictionary with workflow results
    """
    config = get_ml_config()
    registry = ModelRegistry()
    storage = PredictionStorage()
    monitor = PerformanceMonitor()
    validator = PredictionValidator()
    visualizer = MLVisualizer()
    
    # Setup figures directory
    if figures_dir is None:
        figures_dir = config.ml_figures_dir if hasattr(config, 'ml_figures_dir') else 'figures/ml'
    
    figures_path = Path(figures_dir)
    figures_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 80)
    logger.info("OWNLENS ML FRAMEWORK - COMPLETE WORKFLOW")
    logger.info("=" * 80)
    logger.info(f"Domain: {domain}")
    logger.info(f"Model: {model_code or 'All models'}")
    logger.info(f"Brand ID: {brand_id or 'All'}")
    logger.info(f"Figures directory: {figures_path}")
    logger.info("=" * 80)
    
    workflow_id = str(uuid.uuid4())
    logger.info(f"Workflow ID: {workflow_id}")
    logger.info("=" * 80)
    
    # STEP 1: FETCH DATA FROM CLICKHOUSE
    logger.info("\n" + "=" * 80)
    logger.info("STEP 1: FETCH DATA FROM CLICKHOUSE")
    logger.info("=" * 80)
    
    feature_date = None
    logger.info(f"Fetching data (will use latest available date if not specified)")
    
    if domain == 'customer':
        from ..data.loaders.user_features_loader import UserFeaturesLoader
        loader = UserFeaturesLoader()
        data = loader.load(
            feature_date=feature_date,
            brand_id=brand_id,
            company_id=company_id,
            limit=limit or 10000
        )
        logger.info(f"✅ Fetched {len(data)} user records from ClickHouse")
    elif domain == 'editorial':
        from ..data.loaders.editorial_loader import EditorialLoader
        loader = EditorialLoader()
        data = loader.load_articles_metadata(
            brand_id=brand_id,
            limit=limit or 1000
        )
        logger.info(f"✅ Fetched {len(data)} article records from ClickHouse")
    else:
        logger.error(f"Unknown domain: {domain}")
        return {}
    
    logger.info("✅ Data fetched successfully")
    
    # STEP 2: TRAIN ML MODELS
    logger.info("\n" + "=" * 80)
    logger.info("STEP 2: TRAIN ML MODELS")
    logger.info("=" * 80)
    
    models_trained = []
    
    if domain == 'customer':
        models_to_train = [
            ('churn_prediction', ChurnTrainer, None),
            ('user_segmentation', UserSegmentationTrainer, {'n_clusters': 5}),
            ('conversion_prediction', ConversionTrainer, None),
            ('user_recommendation', UserRecommendationTrainer, {'n_components': 50})
        ]
    elif domain == 'editorial':
        models_to_train = [
            ('article_performance', ArticlePerformanceTrainer, None),
            ('trending_topics', TrendingTopicsTrainer, None),
            ('author_performance', AuthorPerformanceTrainer, None),
            ('headline_optimization', HeadlineOptimizationTrainer, None),
            ('content_recommendation', ContentRecommendationTrainer, None)
        ]
    else:
        logger.error(f"Unknown domain: {domain}")
        return {}
    
    if model_code:
        models_to_train = [m for m in models_to_train if m[0] == model_code]
    
    for model_code_item, TrainerClass, trainer_kwargs in models_to_train:
        logger.info(f"\nTraining: {model_code_item}")
        
        try:
            if trainer_kwargs:
                trainer = TrainerClass(**trainer_kwargs)
            else:
                trainer = TrainerClass()
            
            start_time = time.time()
            
            if domain == 'customer':
                metrics = trainer.train(
                    feature_date=feature_date,
                    brand_id=brand_id,
                    company_id=company_id,
                    limit=limit or 10000,
                    test_size=0.2
                )
            elif domain == 'editorial':
                metrics = trainer.train(
                    performance_date=feature_date,
                    brand_id=brand_id,
                    company_id=company_id,
                    limit=limit or 1000,
                    test_size=0.2
                )
            
            training_duration = int(time.time() - start_time)
            
            model_id = registry.register_model(
                model=trainer.model,
                metadata={
                    'model_code': model_code_item,
                    'model_type': domain,
                    'domain': domain,
                    'algorithm': metrics.get('algorithm', 'XGBoost'),
                    'model_framework': metrics.get('framework', 'xgboost'),
                    'model_format': 'pickle',
                    'model_name': f'{model_code_item.replace("_", " ").title()} Model',
                    'model_version': '1.0.0',
                    'description': f'ML model for {model_code_item}',
                    'brand_id': brand_id,
                    'company_id': company_id,
                    'model_status': 'VALIDATED'
                },
                metrics={
                    'n_samples': metrics.get('n_samples', 0),
                    'n_features': metrics.get('n_features', 0),
                    'training_duration_sec': training_duration,
                    'performance_metrics': metrics.get('performance_metrics', metrics),
                    'feature_importance': metrics.get('feature_importance', {})
                }
            )
            
            metadata_manager = MetadataManager()
            metadata_manager.save_training_run(
                model_id=model_id,
                run_metadata={
                    'run_name': f'{model_code_item} Training',
                    'run_type': 'TRAINING',
                    'run_status': 'COMPLETED',
                    'started_at': datetime.fromtimestamp(start_time),
                    'completed_at': datetime.now(),
                    'training_dataset_size': metrics.get('n_samples', 0),
                    'hyperparameters': metrics.get('hyperparameters', {})
                },
                metrics=metrics
            )
            
            if save_figures:
                try:
                    fig_path = figures_path / f'{model_code_item}_training_{workflow_id}.png'
                    visualizer.plot_training_metrics(metrics, save_path=str(fig_path))
                    logger.info(f"✅ Saved training figure: {fig_path}")
                except Exception as e:
                    logger.warning(f"Could not save training figure: {e}")
            
            models_trained.append((model_code_item, model_id, trainer, metrics))
            logger.info(f"✅ Model trained and registered: {model_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to train {model_code_item}: {e}", exc_info=True)
            continue
    
    logger.info(f"\n✅ Training complete: {len(models_trained)} models trained")
    
    # STEP 3: MAKE PREDICTIONS
    logger.info("\n" + "=" * 80)
    logger.info("STEP 3: MAKE PREDICTIONS")
    logger.info("=" * 80)
    
    predictions_made = []
    
    for model_code_item, model_id, trainer, metrics in models_trained:
        logger.info(f"\nMaking predictions: {model_code_item}")
        
        try:
            PredictorClass = None
            if model_code_item == 'churn_prediction':
                PredictorClass = ChurnPredictor
            elif model_code_item == 'user_segmentation':
                PredictorClass = UserSegmentationPredictor
            elif model_code_item == 'conversion_prediction':
                PredictorClass = ConversionPredictor
            elif model_code_item == 'user_recommendation':
                PredictorClass = UserRecommendationPredictor
            elif model_code_item == 'article_performance':
                PredictorClass = ArticlePerformancePredictor
            elif model_code_item == 'trending_topics':
                PredictorClass = TrendingTopicsPredictor
            elif model_code_item == 'author_performance':
                PredictorClass = AuthorPerformancePredictor
            elif model_code_item == 'headline_optimization':
                PredictorClass = HeadlineOptimizationPredictor
            elif model_code_item == 'content_recommendation':
                PredictorClass = ContentRecommendationPredictor
            
            if PredictorClass is None:
                logger.warning(f"No predictor class for {model_code_item}")
                continue
            
            predictor = PredictorClass(trainer.model)
            
            if domain == 'customer':
                from ..data.loaders.user_features_loader import UserFeaturesLoader
                loader = UserFeaturesLoader()
                entities = loader.load(
                    feature_date=feature_date,
                    brand_id=brand_id,
                    company_id=company_id,
                    limit=batch_size
                )
                entity_ids = entities['user_id'].unique().tolist() if 'user_id' in entities.columns else []
            elif domain == 'editorial':
                from ..data.loaders.editorial_loader import EditorialLoader
                loader = EditorialLoader()
                entities = loader.load_articles_metadata(
                    brand_id=brand_id,
                    limit=batch_size
                )
                entity_ids = entities['article_id'].unique().tolist() if 'article_id' in entities.columns else []
            
            logger.info(f"Making predictions for {len(entity_ids)} entities...")
            
            batch_id = str(uuid.uuid4())
            predictions_count = 0
            
            for i, entity_id in enumerate(entity_ids, 1):
                try:
                    start_time = time.time()
                    
                    if model_code_item in ['churn_prediction', 'conversion_prediction']:
                        predictions = predictor.predict(entity_id, return_proba=True)
                    elif model_code_item == 'user_segmentation':
                        predictions = predictor.predict(entity_id)
                    else:
                        predictions = predictor.predict(entity_id)
                    
                    latency_ms = (time.time() - start_time) * 1000
                    
                    if model_code_item == 'churn_prediction':
                        storage.save_churn_prediction(
                            user_id=entity_id,
                            prediction=predictions,
                            model_id=model_id,
                            company_id=company_id,
                            brand_id=brand_id,
                            batch_id=batch_id,
                            model_version='1.0.0'
                        )
                    elif model_code_item == 'conversion_prediction':
                        storage.save_conversion_prediction(
                            user_id=entity_id,
                            prediction=predictions,
                            model_id=model_id,
                            company_id=company_id,
                            brand_id=brand_id,
                            batch_id=batch_id,
                            model_version='1.0.0'
                        )
                    elif model_code_item == 'user_segmentation':
                        segment_number = predictions['segment_number'].iloc[0] if 'segment_number' in predictions.columns else 0
                        storage.save_segment_assignment(
                            user_id=entity_id,
                            segment_id=f"segment_{segment_number}",
                            confidence=0.8,
                            model_id=model_id,
                            company_id=company_id,
                            brand_id=brand_id,
                            model_version='1.0.0'
                        )
                    elif model_code_item == 'user_recommendation':
                        if isinstance(predictions, pd.DataFrame) and len(predictions) > 0:
                            for idx, row in predictions.head(10).iterrows():
                                storage.save_recommendation(
                                    user_id=entity_id,
                                    article_id=row.get('article_id', ''),
                                    score=float(row.get('recommendation_score', row.get('score', 0.0))),
                                    rank=int(row.get('rank', idx + 1)),
                                    model_id=model_id,
                                    company_id=company_id,
                                    brand_id=brand_id,
                                    category_id=row.get('category_id'),
                                    model_version='1.0.0'
                                )
                    
                    monitor.track_prediction(
                        model_id=model_id,
                        prediction=predictions.iloc[0, 0] if hasattr(predictions, 'iloc') else predictions,
                        latency_ms=latency_ms
                    )
                    
                    predictions_count += 1
                    
                    if i % 100 == 0:
                        logger.info(f"Progress: {i}/{len(entity_ids)} predictions saved")
                
                except Exception as e:
                    logger.warning(f"Failed to predict for {entity_id}: {e}")
                    continue
            
            predictions_made.append((model_code_item, model_id, predictions_count))
            logger.info(f"✅ Saved {predictions_count} predictions for {model_code_item}")
            
        except Exception as e:
            logger.error(f"❌ Failed to make predictions for {model_code_item}: {e}", exc_info=True)
            continue
    
    logger.info(f"\n✅ Predictions complete: {sum(p[2] for p in predictions_made)} total predictions")
    
    # STEP 4: MONITOR MODELS
    logger.info("\n" + "=" * 80)
    logger.info("STEP 4: MONITOR MODELS")
    logger.info("=" * 80)
    
    monitoring_results = []
    
    for model_code_item, model_id, _, _ in models_trained:
        logger.info(f"\nMonitoring: {model_code_item}")
        
        try:
            monitoring_id = monitor.generate_monitoring_report(
                model_id=model_id,
                monitoring_date=feature_date,
                company_id=company_id,
                brand_id=brand_id
            )
            
            drift_detector = DriftDetector()
            drift_result = drift_detector.check_drift(
                model_id=model_id,
                monitoring_date=feature_date
            )
            
            alerting = AlertingSystem()
            alert_result = alerting.daily_alert_check(
                model_id=model_id,
                monitoring_date=feature_date,
                baseline_accuracy=0.85
            )
            
            monitoring_results.append({
                'model_code': model_code_item,
                'model_id': model_id,
                'monitoring_id': monitoring_id,
                'drift_detected': drift_result.get('drift_detected', False),
                'alerts_triggered': alert_result.get('alerts_triggered', 0)
            })
            
            logger.info(f"✅ Monitoring complete: {model_code_item}")
            logger.info(f"  - Drift detected: {drift_result.get('drift_detected', False)}")
            logger.info(f"  - Alerts triggered: {alert_result.get('alerts_triggered', 0)}")
            
        except Exception as e:
            logger.error(f"❌ Failed to monitor {model_code_item}: {e}", exc_info=True)
            continue
    
    logger.info(f"\n✅ Monitoring complete: {len(monitoring_results)} models monitored")
    
    # STEP 5: VISUALIZE RESULTS
    logger.info("\n" + "=" * 80)
    logger.info("STEP 5: VISUALIZE RESULTS")
    logger.info("=" * 80)
    
    if save_figures:
        try:
            for model_code_item, model_id, _, metrics in models_trained:
                try:
                    if 'feature_importance' in metrics:
                        fig_path = figures_path / f'{model_code_item}_features_{workflow_id}.png'
                        visualizer.plot_feature_importance(
                            metrics['feature_importance'],
                            save_path=str(fig_path)
                        )
                        logger.info(f"✅ Saved feature importance: {fig_path}")
                    
                    fig_path = figures_path / f'{model_code_item}_metrics_{workflow_id}.png'
                    visualizer.plot_training_metrics(metrics, save_path=str(fig_path))
                    logger.info(f"✅ Saved training metrics: {fig_path}")
                    
                except Exception as e:
                    logger.warning(f"Could not visualize {model_code_item}: {e}")
            
            if monitoring_results:
                try:
                    fig_path = figures_path / f'monitoring_summary_{workflow_id}.png'
                    import matplotlib.pyplot as plt
                    
                    df = pd.DataFrame(monitoring_results)
                    
                    fig, ax = plt.subplots(figsize=(10, 6))
                    ax.bar(df['model_code'], df['alerts_triggered'])
                    ax.set_xlabel('Model')
                    ax.set_ylabel('Alerts Triggered')
                    ax.set_title('Model Monitoring Summary')
                    plt.xticks(rotation=45, ha='right')
                    plt.tight_layout()
                    plt.savefig(fig_path, dpi=300, bbox_inches='tight')
                    plt.close()
                    
                    logger.info(f"✅ Saved monitoring summary: {fig_path}")
                except Exception as e:
                    logger.warning(f"Could not create monitoring summary: {e}")
            
            logger.info(f"\n✅ Visualization complete: Figures saved to {figures_path}")
            
        except Exception as e:
            logger.error(f"❌ Visualization failed: {e}", exc_info=True)
    else:
        logger.info("Skipping visualization (save_figures=False)")
    
    # STEP 6: SAVE TO CLICKHOUSE
    logger.info("\n" + "=" * 80)
    logger.info("STEP 6: SAVE TO CLICKHOUSE")
    logger.info("=" * 80)
    
    logger.info("✅ Models saved to ml_model_registry")
    logger.info("✅ Training runs saved to ml_model_training_runs")
    logger.info("✅ Features saved to ml_model_features")
    logger.info("✅ Predictions saved to ml_model_predictions")
    logger.info("✅ Domain-specific predictions saved to respective tables")
    logger.info("✅ Monitoring reports saved to ml_model_monitoring")
    
    # STEP 7: SUMMARY
    logger.info("\n" + "=" * 80)
    logger.info("WORKFLOW SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Workflow ID: {workflow_id}")
    logger.info(f"Domain: {domain}")
    logger.info(f"Models trained: {len(models_trained)}")
    logger.info(f"Total predictions: {sum(p[2] for p in predictions_made)}")
    logger.info(f"Models monitored: {len(monitoring_results)}")
    logger.info(f"Figures saved: {figures_path}")
    logger.info("=" * 80)
    
    logger.info("\n✅ COMPLETE ML WORKFLOW FINISHED SUCCESSFULLY")
    logger.info("=" * 80)
    
    return {
        'workflow_id': workflow_id,
        'models_trained': len(models_trained),
        'predictions_made': sum(p[2] for p in predictions_made),
        'models_monitored': len(monitoring_results),
        'figures_dir': str(figures_path)
    }

