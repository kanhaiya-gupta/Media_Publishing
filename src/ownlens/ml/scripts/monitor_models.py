#!/usr/bin/env python3
"""
OwnLens - ML Module: Model Monitoring

Script to run daily model monitoring and generate reports.

Usage:
    python -m ownlens.ml.scripts.monitor_models
    python -m ownlens.ml.scripts.monitor_models --model-id model-123
    python -m ownlens.ml.scripts.monitor_models --date 2024-01-01
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
from ownlens.ml.monitoring import PerformanceMonitor, DriftDetector, AlertingSystem
from ownlens.ml.storage import PredictionValidator

# Setup logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def monitor_model(
    model_id: str,
    monitoring_date: date,
    baseline_accuracy: float = None
):
    """Monitor a single model."""
    logger.info(f"Monitoring model: {model_id} on {monitoring_date}")
    
    monitor = PerformanceMonitor()
    drift_detector = DriftDetector()
    alerting = AlertingSystem()
    validator = PredictionValidator()
    
    # Generate monitoring report
    logger.info("Generating monitoring report...")
    monitoring_id = monitor.generate_monitoring_report(
        model_id=model_id,
        monitoring_date=monitoring_date
    )
    logger.info(f"✅ Monitoring report generated: {monitoring_id}")
    
    # Check for drift
    logger.info("Checking for drift...")
    drift_result = drift_detector.check_drift(
        model_id=model_id,
        monitoring_date=monitoring_date
    )
    logger.info(f"✅ Drift check completed: detected={drift_result.get('drift_detected', False)}")
    
    # Calculate accuracy if we have actuals
    logger.info("Calculating accuracy...")
    try:
        # Get model metadata to determine prediction type
        registry = ModelRegistry()
        model_list = registry.list_models(limit=1)
        if len(model_list) > 0:
            model_code = model_list[model_list['model_id'] == model_id]['model_code'].iloc[0]
            prediction_type = model_code.replace('_prediction', '').replace('_', '')
            
            accuracy = validator.calculate_accuracy(
                model_id=model_id,
                prediction_type=prediction_type,
                start_date=monitoring_date,
                end_date=monitoring_date
            )
            logger.info(f"✅ Accuracy calculated: {accuracy.get('accuracy', 0.0):.4f}")
    except Exception as e:
        logger.warning(f"Could not calculate accuracy: {e}")
    
    # Check alerts
    logger.info("Checking alerts...")
    alert_result = alerting.daily_alert_check(
        model_id=model_id,
        monitoring_date=monitoring_date,
        baseline_accuracy=baseline_accuracy
    )
    
    alerts_triggered = alert_result.get('alerts_triggered', 0)
    if alerts_triggered > 0:
        logger.warning(f"⚠️  {alerts_triggered} alerts triggered:")
        for alert in alert_result.get('alerts', []):
            logger.warning(f"  - [{alert['severity'].upper()}] {alert['alert_type']}: {alert['message']}")
    else:
        logger.info("✅ No alerts triggered")
    
    return {
        'monitoring_id': monitoring_id,
        'drift_detected': drift_result.get('drift_detected', False),
        'alerts_triggered': alerts_triggered
    }


def monitor_all_models(monitoring_date: date = None):
    """Monitor all production models."""
    if monitoring_date is None:
        monitoring_date = date.today()
    
    logger.info("=" * 80)
    logger.info("MODEL MONITORING - ALL PRODUCTION MODELS")
    logger.info("=" * 80)
    logger.info(f"Monitoring date: {monitoring_date}")
    logger.info("=" * 80)
    
    registry = ModelRegistry()
    
    # Get all production models
    models = registry.list_models(status='PRODUCTION')
    
    if len(models) == 0:
        logger.warning("No production models found")
        return
    
    logger.info(f"Found {len(models)} production models")
    
    results = []
    
    for _, model_row in models.iterrows():
        model_id = model_row['model_id']
        model_code = model_row['model_code']
        model_version = model_row['model_version']
        
        logger.info(f"\nMonitoring: {model_code} v{model_version} ({model_id})")
        
        try:
            result = monitor_model(model_id, monitoring_date)
            results.append({
                'model_id': model_id,
                'model_code': model_code,
                'model_version': model_version,
                **result
            })
        except Exception as e:
            logger.error(f"Failed to monitor model {model_id}: {e}", exc_info=True)
            continue
    
    logger.info("\n" + "=" * 80)
    logger.info("MONITORING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Models monitored: {len(results)}")
    logger.info(f"Models with drift: {sum(1 for r in results if r.get('drift_detected', False))}")
    logger.info(f"Total alerts: {sum(r.get('alerts_triggered', 0) for r in results)}")
    logger.info("=" * 80)
    
    return results


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Monitor ML models')
    parser.add_argument('--model-id', type=str, help='Specific model ID to monitor')
    parser.add_argument('--model-code', type=str, help='Model code to monitor (e.g., churn_prediction)')
    parser.add_argument('--date', type=str, help='Monitoring date (YYYY-MM-DD, default: today)')
    parser.add_argument('--baseline-accuracy', type=float, help='Baseline accuracy for comparison')
    
    args = parser.parse_args()
    
    # Parse date
    monitoring_date = date.today()
    if args.date:
        monitoring_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    
    logger.info("=" * 80)
    logger.info("OWNLENS ML FRAMEWORK - MODEL MONITORING")
    logger.info("=" * 80)
    logger.info(f"Monitoring date: {monitoring_date}")
    logger.info("=" * 80)
    
    if args.model_id:
        # Monitor specific model
        monitor_model(args.model_id, monitoring_date, args.baseline_accuracy)
    elif args.model_code:
        # Monitor model by code
        registry = ModelRegistry()
        model, metadata = registry.get_model(args.model_code, status='PRODUCTION')
        monitor_model(metadata['model_id'], monitoring_date, args.baseline_accuracy)
    else:
        # Monitor all production models
        monitor_all_models(monitoring_date)


if __name__ == "__main__":
    main()

