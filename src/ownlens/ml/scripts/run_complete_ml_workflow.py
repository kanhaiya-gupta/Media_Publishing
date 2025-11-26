#!/usr/bin/env python3
"""
OwnLens - ML Module: Complete ML Workflow

Thin wrapper script that calls the ML orchestration module.

Complete end-to-end ML workflow:
1. Fetch data from ClickHouse
2. Train ML models
3. Make predictions
4. Monitor models
5. Visualize results
6. Save figures to designated place
7. Save everything to ClickHouse

Usage:
    python -m ownlens.ml.scripts.run_complete_ml_workflow
    python -m ownlens.ml.scripts.run_complete_ml_workflow --domain customer
    python -m ownlens.ml.scripts.run_complete_ml_workflow --model churn_prediction
"""

import sys
import argparse
import logging
from pathlib import Path

# Add project root to path (for running from project root)
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables from .env file
from dotenv import load_dotenv
env_file = project_root / "development.env"
if env_file.exists():
    load_dotenv(env_file)
    print(f"✓ Loaded environment variables from {env_file}")
else:
    print(f"⚠ Warning: development.env not found at {env_file}. Using system environment variables.")

# Import using src.ownlens for consistency with ETL script
from src.ownlens.ml.utils.logging import setup_logging

# Setup logging
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import the orchestration function - no duplication!
from src.ownlens.ml.orchestration import run_ml_workflow


# Backward compatibility alias
def run_complete_workflow(*args, **kwargs):
    """
    Backward compatibility wrapper for run_ml_workflow.
    
    This function is kept for backward compatibility.
    New code should use run_ml_workflow directly from ownlens.ml.orchestration.
    """
    return run_ml_workflow(*args, **kwargs)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Run complete ML workflow')
    parser.add_argument('--model', type=str, help='Specific model code to run')
    parser.add_argument('--domain', choices=['customer', 'editorial'], default='customer',
                        help='Domain to process (default: customer)')
    parser.add_argument('--brand-id', type=str, help='Brand ID filter')
    parser.add_argument('--company-id', type=str, help='Company ID filter')
    parser.add_argument('--limit', type=int, help='Limit number of samples for training')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for predictions (default: 1000)')
    parser.add_argument('--no-figures', action='store_true', help='Skip saving figures')
    parser.add_argument('--figures-dir', type=str, help='Directory to save figures')
    
    args = parser.parse_args()
    
    # Call the orchestration function - no duplication!
    result = run_ml_workflow(
        model_code=args.model,
        domain=args.domain,
        brand_id=args.brand_id,
        company_id=args.company_id,
        limit=args.limit,
        batch_size=args.batch_size,
        save_figures=not args.no_figures,
        figures_dir=args.figures_dir
    )
    
    logger.info(f"\nWorkflow completed: {result}")
    
    return result


if __name__ == "__main__":
    main()
