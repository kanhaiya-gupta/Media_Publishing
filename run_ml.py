#!/usr/bin/env python3
"""
ML Pipeline Master Script

This script runs all ML models in the correct order:
1. Data Exploration
2. Feature Engineering
3. Baseline Models
4. Churn Prediction
5. User Segmentation
6. Content Recommendation
7. Conversion Prediction
8. Click Prediction
9. Engagement Prediction
10. Ad Optimization

Usage:
    python run_ml.py [--skip-exploration] [--skip-features] [--models-only]
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ML directory path
ML_DIR = os.path.join(os.path.dirname(__file__), "ml")
ML_SCRIPTS_DIR = ML_DIR

# Script execution order
ML_SCRIPTS = [
    {
        "name": "Data Exploration",
        "file": "01_data_exploration.py",
        "description": "Explores data quality, distributions, and patterns",
        "required": True,
        "outputs": ["user_features_sample.csv", "correlation_matrix.png"],
    },
    {
        "name": "Feature Engineering",
        "file": "02_feature_engineering.py",
        "description": "Creates ML-ready features from raw data",
        "required": True,
        "outputs": ["user_features_ml_ready.csv", "feature_list.json"],
    },
    {
        "name": "Baseline Models",
        "file": "03_baseline_models.py",
        "description": "Trains baseline models for comparison",
        "required": False,
        "outputs": ["models/baseline/baseline_models_metrics.json"],
    },
    {
        "name": "Churn Prediction",
        "file": "04_churn_prediction.py",
        "description": "Predicts user churn using XGBoost",
        "required": False,
        "outputs": ["models/churn_model_xgboost.pkl"],
    },
    {
        "name": "User Segmentation",
        "file": "05_user_segmentation.py",
        "description": "Segments users using K-Means clustering",
        "required": False,
        "outputs": ["user_segments.csv", "segment_profiles.csv"],
    },
    {
        "name": "Content Recommendation",
        "file": "06_recommendation_system.py",
        "description": "Builds hybrid recommendation system",
        "required": False,
        "outputs": ["models/recommendation_model.pkl"],
    },
    {
        "name": "Conversion Prediction",
        "file": "07_conversion_prediction.py",
        "description": "Predicts subscription conversion using LightGBM",
        "required": False,
        "outputs": ["models/conversion_model_lightgbm.pkl"],
    },
    {
        "name": "Click Prediction",
        "file": "08_click_prediction.py",
        "description": "Predicts article click-through rates",
        "required": False,
        "outputs": ["models/click_prediction_model.pkl"],
    },
    {
        "name": "Engagement Prediction",
        "file": "09_engagement_prediction.py",
        "description": "Predicts user engagement scores",
        "required": False,
        "outputs": ["models/engagement_prediction_model.pkl"],
    },
    {
        "name": "Ad Optimization",
        "file": "10_ad_optimization.py",
        "description": "Optimizes ad placement using Thompson Sampling",
        "required": False,
        "outputs": ["models/ad_optimization_thompson_sampling.pkl"],
    },
]


def print_header(text):
    """Print formatted header"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")


def print_step(step_num, total_steps, name):
    """Print step information"""
    print(f"\n{'='*80}")
    print(f"STEP {step_num}/{total_steps}: {name}")
    print(f"{'='*80}\n")


def check_prerequisites():
    """Check if required files and dependencies exist"""
    print_header("CHECKING PREREQUISITES")

    # Check if ml directory exists
    if not os.path.exists(ML_DIR):
        print(f"✗ Error: ML directory not found: {ML_DIR}")
        print("  Please ensure you're running from the kafka_check directory")
        return False

    # Check if we have the required scripts
    if not os.path.exists(os.path.join(ML_DIR, "01_data_exploration.py")):
        print(f"✗ Error: ML scripts not found in {ML_DIR}")
        return False

    print(f"✓ ML directory found: {ML_DIR}")

    # Check if ClickHouse is accessible
    try:
        from clickhouse_driver import Client

        client = Client(
            host="localhost",
            port=9002,
            database="analytics",
            user="default",
            password="clickhouse",
        )
        client.execute("SELECT 1")
        client.disconnect()
        print("✓ ClickHouse connection verified")
    except Exception as e:
        print(f"✗ Warning: ClickHouse connection failed: {e}")
        print("  Make sure ClickHouse is running: docker-compose up -d")
        return False

    # Check required Python packages
    # Map package names to their import names
    package_imports = {
        "pandas": "pandas",
        "numpy": "numpy",
        "scikit-learn": "sklearn",  # Package name vs import name
        "xgboost": "xgboost",
        "lightgbm": "lightgbm",
        "clickhouse_driver": "clickhouse_driver",
        "matplotlib": "matplotlib",
        "seaborn": "seaborn",
    }

    missing_packages = []
    for package_name, import_name in package_imports.items():
        try:
            __import__(import_name)
            print(f"✓ {package_name} installed")
        except ImportError:
            missing_packages.append(package_name)
            print(f"✗ {package_name} not installed")

    if missing_packages:
        print(f"\n✗ Missing packages: {', '.join(missing_packages)}")
        print(f"  Install with: pip install {' '.join(missing_packages)}")
        return False

    print("\n✓ All prerequisites met")
    return True


def run_script(script_info, skip_exploration=False, skip_features=False):
    """Run a single ML script"""
    script_name = script_info["name"]
    script_file = script_info["file"]
    description = script_info["description"]
    required = script_info["required"]

    # Skip if required but skipped
    if skip_exploration and script_file == "01_data_exploration.py":
        print(f"⏭️  Skipping {script_name} (--skip-exploration)")
        return True, None

    if skip_features and script_file == "02_feature_engineering.py":
        print(f"⏭️  Skipping {script_name} (--skip-features)")
        return True, None

    print(f"📋 {description}")
    print(f"📄 Running: {script_file}")

    # Full path to script
    script_path = os.path.join(ML_DIR, script_file)

    if not os.path.exists(script_path):
        print(f"✗ Script not found: {script_path}")
        if required:
            return False, None
        else:
            print(f"  ⚠️  Continuing with next step...")
            return True, None

    start_time = time.time()

    try:
        # Run the script from the ml directory
        result = subprocess.run(
            [sys.executable, script_file],
            capture_output=True,
            text=True,
            cwd=ML_DIR,  # Change working directory to ml folder
        )

        elapsed_time = time.time() - start_time

        if result.returncode == 0:
            print(f"✓ {script_name} completed successfully ({elapsed_time:.1f}s)")

            # Show stdout/stderr if there are warnings or if outputs are missing
            # Check for expected outputs (in ml directory)
            missing_outputs = []
            for output in script_info.get("outputs", []):
                output_path = os.path.join(ML_DIR, output)
                if not os.path.exists(output_path):
                    missing_outputs.append(output)

            if missing_outputs:
                print(f"  ⚠️  Warning: Some expected outputs not found: {', '.join(missing_outputs)}")
                # Show output to help debug
                if result.stdout:
                    print(f"  Script output:\n{result.stdout}")
                if result.stderr:
                    print(f"  Script errors:\n{result.stderr}")

            return True, elapsed_time
        else:
            print(f"✗ {script_name} failed")
            if result.stderr:
                print(f"  Error output:\n{result.stderr}")
            if result.stdout:
                print(f"  Output:\n{result.stdout}")

            if required:
                print(f"  ⚠️  This is a required step. Stopping pipeline.")
                return False, None
            else:
                print(f"  ⚠️  Continuing with next step...")
                return True, None  # Continue even if optional step fails

    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"✗ {script_name} failed with exception")
        print(f"  Exception: {e}")

        if required:
            return False, None
        else:
            print(f"  ⚠️  Continuing with next step...")
            return True, None


def run_all_models(skip_exploration=False, skip_features=False, models_only=False):
    """Run all ML models in correct order"""
    print_header("MEDIA PUBLISHING - ML PIPELINE MASTER SCRIPT")

    start_time = time.time()
    total_steps = len(ML_SCRIPTS)
    successful_steps = 0
    failed_steps = []
    skipped_steps = []
    step_times = []

    # Determine which steps to run
    if models_only:
        scripts_to_run = ML_SCRIPTS[2:]  # Skip exploration and features
        print("📌 Running models only (skipping exploration and feature engineering)")
    else:
        scripts_to_run = ML_SCRIPTS

    print(f"\n📊 Pipeline Summary:")
    print(f"   Total Steps: {len(scripts_to_run)}")
    print(f"   Required Steps: {sum(1 for s in scripts_to_run if s['required'])}")
    print(f"   Optional Steps: {sum(1 for s in scripts_to_run if not s['required'])}")
    print(f"   ML Directory: {ML_DIR}")
    print(f"\n🚀 Starting ML pipeline...\n")

    # Run each script
    for step_num, script_info in enumerate(scripts_to_run, 1):
        script_name = script_info["name"]
        script_file = script_info["file"]

        print_step(step_num, len(scripts_to_run), script_name)

        # Check if we should skip
        if skip_exploration and script_file == "01_data_exploration.py":
            skipped_steps.append(script_name)
            print(f"⏭️  Skipping {script_name}")
            continue

        if skip_features and script_file == "02_feature_engineering.py":
            skipped_steps.append(script_name)
            print(f"⏭️  Skipping {script_name}")
            continue

        # Check if required files exist for this step (only for models that need features)
        # Feature engineering and later models need features file
        if script_file != "01_data_exploration.py":
            features_file = os.path.join(ML_DIR, "user_features_ml_ready.csv")
            if not os.path.exists(features_file):
                # For feature engineering itself, this is OK - it will create the file
                if script_file == "02_feature_engineering.py":
                    pass  # Continue - feature engineering will create this file
                else:
                    # For other models, we need the features file
                    print(f"✗ Required file not found: {features_file}")
                    print(f"  Please run feature engineering first: python run_ml.py")
                    if script_info["required"]:
                        print(f"  ⚠️  This is a required step. Stopping pipeline.")
                        failed_steps.append(script_name)
                        break
                    else:
                        print(f"  ⚠️  Skipping {script_name}...")
                        skipped_steps.append(script_name)
                        continue

        # Run the script
        success, elapsed_time = run_script(script_info, skip_exploration, skip_features)

        if elapsed_time:
            step_times.append((script_name, elapsed_time))

        if success:
            successful_steps += 1
        else:
            failed_steps.append(script_name)
            if script_info["required"]:
                break

    # Print summary
    total_time = time.time() - start_time

    print_header("ML PIPELINE SUMMARY")

    print(f"📊 Execution Summary:")
    print(f"   Total Steps: {len(scripts_to_run)}")
    print(f"   Successful: {successful_steps}")
    print(f"   Failed: {len(failed_steps)}")
    print(f"   Skipped: {len(skipped_steps)}")
    print(f"   Total Time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")

    if step_times:
        print(f"\n⏱️  Step Execution Times:")
        for step_name, elapsed_time in step_times:
            print(f"   {step_name:30s}: {elapsed_time:6.1f}s")

    if successful_steps > 0:
        print(f"\n✅ Successfully completed steps:")
        for i, script_info in enumerate(scripts_to_run):
            if script_info["name"] not in failed_steps and script_info["name"] not in skipped_steps:
                print(f"   ✓ {script_info['name']}")

    if skipped_steps:
        print(f"\n⏭️  Skipped steps:")
        for step in skipped_steps:
            print(f"   ⏭️  {step}")

    if failed_steps:
        print(f"\n✗ Failed steps:")
        for step in failed_steps:
            print(f"   ✗ {step}")
        print(f"\n⚠️  Pipeline completed with errors. Please check the logs above.")
        return False
    else:
        print(f"\n🎉 ML Pipeline completed successfully!")
        print(f"\n📁 Generated Files:")
        print(f"   Models: {os.path.join(ML_DIR, 'models')}/")
        print(f"   Features: {os.path.join(ML_DIR, 'user_features_ml_ready.csv')}")
        print(f"   Visualizations: {os.path.join(ML_DIR, '*.png')}")
        return True


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Run all ML models in the correct order",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pipeline
  python run_ml.py
  
  # Skip data exploration (if already done)
  python run_ml.py --skip-exploration
  
  # Skip feature engineering (if already done)
  python run_ml.py --skip-features
  
  # Run only models (skip exploration and features)
  python run_ml.py --models-only
  
  # Skip prerequisite checks
  python run_ml.py --skip-prerequisites
        """,
    )

    parser.add_argument("--skip-exploration", action="store_true", help="Skip data exploration step")

    parser.add_argument("--skip-features", action="store_true", help="Skip feature engineering step")

    parser.add_argument(
        "--models-only",
        action="store_true",
        help="Run only model training (skip exploration and features)",
    )

    parser.add_argument("--skip-prerequisites", action="store_true", help="Skip prerequisite checks")

    args = parser.parse_args()

    # Check prerequisites
    if not args.skip_prerequisites:
        if not check_prerequisites():
            print("\n✗ Prerequisites check failed. Use --skip-prerequisites to skip.")
            sys.exit(1)

    # Run all models
    success = run_all_models(
        skip_exploration=args.skip_exploration,
        skip_features=args.skip_features,
        models_only=args.models_only,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
