#!/usr/bin/env python3
"""
OwnLens - ML Module: Validate Predictions

Script to validate predictions against actual outcomes.

Usage:
    python -m ownlens.ml.scripts.validate_predictions --model churn_prediction
    python -m ownlens.ml.scripts.validate_predictions --model churn_prediction --days 30
"""

import sys
import argparse
from pathlib import Path
from datetime import date, datetime, timedelta
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
from ownlens.ml.storage import PredictionValidator
from clickhouse_driver import Client
from ownlens.ml.utils.config import get_ml_config

# Setup logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_churn_predictions(
    model_id: str = None,
    days: int = 30,
    brand_id: str = None
):
    """Validate churn predictions."""
    logger.info("=" * 80)
    logger.info("VALIDATING CHURN PREDICTIONS")
    logger.info("=" * 80)
    
    registry = ModelRegistry()
    validator = PredictionValidator()
    config = get_ml_config()
    client = Client(**config.get_clickhouse_connection_params())
    
    try:
        # Get model
        if model_id:
            model_list = registry.list_models(limit=1)
            if len(model_list) == 0:
                logger.error(f"Model not found: {model_id}")
                return
        else:
            model, metadata = registry.get_model('churn_prediction', status='PRODUCTION')
            model_id = metadata['model_id']
        
        # Get predictions to validate
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        query = """
        SELECT prediction_id, user_id, prediction_date, churn_probability
        FROM customer_churn_predictions
        WHERE model_id = ? AND prediction_date >= ? AND prediction_date <= ?
        """
        
        params = [model_id, start_date, end_date]
        if brand_id:
            query += " AND brand_id = ?"
            params.append(brand_id)
        
        query += " ORDER BY prediction_date DESC LIMIT 1000"
        
        results = client.execute(query, params)
        
        if not results:
            logger.warning("No predictions found to validate")
            return
        
        logger.info(f"Validating {len(results)} churn predictions...")
        
        validated = 0
        for row in results:
            prediction_id = row[0]
            user_id = row[1]
            
            try:
                result = validator.validate_churn_prediction(
                    prediction_id=prediction_id,
                    user_id=user_id
                )
                if result.get('validated', False):
                    validated += 1
            except Exception as e:
                logger.warning(f"Failed to validate prediction {prediction_id}: {e}")
                continue
        
        logger.info(f"✅ Validated {validated}/{len(results)} predictions")
        
        # Calculate accuracy
        accuracy = validator.calculate_accuracy(
            model_id=model_id,
            prediction_type='churn',
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info(f"Model accuracy: {accuracy.get('accuracy', 0.0):.4f}")
        logger.info(f"Total predictions: {accuracy.get('total_predictions', 0)}")
        logger.info(f"Correct predictions: {accuracy.get('correct_predictions', 0)}")
        
    finally:
        client.disconnect()


def validate_conversion_predictions(
    model_id: str = None,
    days: int = 30,
    brand_id: str = None
):
    """Validate conversion predictions."""
    logger.info("=" * 80)
    logger.info("VALIDATING CONVERSION PREDICTIONS")
    logger.info("=" * 80)
    
    registry = ModelRegistry()
    validator = PredictionValidator()
    config = get_ml_config()
    client = Client(**config.get_clickhouse_connection_params())
    
    try:
        # Get model
        if model_id:
            model_list = registry.list_models(limit=1)
            if len(model_list) == 0:
                logger.error(f"Model not found: {model_id}")
                return
        else:
            model, metadata = registry.get_model('conversion_prediction', status='PRODUCTION')
            model_id = metadata['model_id']
        
        # Get predictions to validate
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        query = """
        SELECT prediction_id, user_id, prediction_date, conversion_probability
        FROM customer_conversion_predictions
        WHERE model_id = ? AND prediction_date >= ? AND prediction_date <= ?
        """
        
        params = [model_id, start_date, end_date]
        if brand_id:
            query += " AND brand_id = ?"
            params.append(brand_id)
        
        query += " ORDER BY prediction_date DESC LIMIT 1000"
        
        results = client.execute(query, params)
        
        if not results:
            logger.warning("No predictions found to validate")
            return
        
        logger.info(f"Validating {len(results)} conversion predictions...")
        
        validated = 0
        for row in results:
            prediction_id = row[0]
            user_id = row[1]
            
            try:
                result = validator.validate_conversion_prediction(
                    prediction_id=prediction_id,
                    user_id=user_id
                )
                if result.get('validated', False):
                    validated += 1
            except Exception as e:
                logger.warning(f"Failed to validate prediction {prediction_id}: {e}")
                continue
        
        logger.info(f"✅ Validated {validated}/{len(results)} predictions")
        
        # Calculate accuracy
        accuracy = validator.calculate_accuracy(
            model_id=model_id,
            prediction_type='conversion',
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info(f"Model accuracy: {accuracy.get('accuracy', 0.0):.4f}")
        logger.info(f"Total predictions: {accuracy.get('total_predictions', 0)}")
        logger.info(f"Correct predictions: {accuracy.get('correct_predictions', 0)}")
        
    finally:
        client.disconnect()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Validate predictions')
    parser.add_argument('--model', type=str, required=True,
                        choices=['churn_prediction', 'conversion_prediction'],
                        help='Model to validate')
    parser.add_argument('--model-id', type=str, help='Specific model ID')
    parser.add_argument('--days', type=int, default=30, help='Number of days to validate (default: 30)')
    parser.add_argument('--brand-id', type=str, help='Brand ID filter')
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("OWNLENS ML FRAMEWORK - VALIDATE PREDICTIONS")
    logger.info("=" * 80)
    logger.info(f"Model: {args.model}")
    logger.info(f"Days: {args.days}")
    logger.info("=" * 80)
    
    if args.model == 'churn_prediction':
        validate_churn_predictions(
            model_id=args.model_id,
            days=args.days,
            brand_id=args.brand_id
        )
    elif args.model == 'conversion_prediction':
        validate_conversion_predictions(
            model_id=args.model_id,
            days=args.days,
            brand_id=args.brand_id
        )
    else:
        logger.error(f"Validation not yet implemented for model: {args.model}")


if __name__ == "__main__":
    main()

