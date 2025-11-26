# ML Module - Run Scripts

Scripts to run the ML framework end-to-end.

## Available Scripts

### 1. Train All Models (`train_all_models.py`)

Train all ML models and register them in the model registry.

**Usage**:
```bash
# Train all models
python -m ownlens.ml.scripts.train_all_models

# Train customer domain only
python -m ownlens.ml.scripts.train_all_models --domain customer

# Train editorial domain only
python -m ownlens.ml.scripts.train_all_models --domain editorial

# Train with filters
python -m ownlens.ml.scripts.train_all_models --brand-id brand-123 --limit 5000
```

**What it does**:
- Trains all models (customer + editorial)
- Registers models in `ml_model_registry`
- Saves training runs to `ml_model_training_runs`
- Saves features to `ml_model_features`

### 2. Batch Predictions (`predict_batch.py`)

Run batch predictions and save to ClickHouse.

**Usage**:
```bash
# Predict churn for all users
python -m ownlens.ml.scripts.predict_batch --model churn_prediction

# Predict for specific user
python -m ownlens.ml.scripts.predict_batch --model churn_prediction --user-id user-123

# Predict for multiple users
python -m ownlens.ml.scripts.predict_batch --model churn_prediction --user-ids user-1 user-2 user-3

# Predict with batch size
python -m ownlens.ml.scripts.predict_batch --model churn_prediction --batch-size 5000
```

**Available models**:
- `churn_prediction` - Churn predictions
- `conversion_prediction` - Conversion predictions
- `user_segmentation` - User segmentation
- `user_recommendation` - Content recommendations
- `article_performance` - Article performance
- `trending_topics` - Trending topics
- `author_performance` - Author performance
- `headline_optimization` - Headline CTR
- `content_recommendation` - Content recommendations

**What it does**:
- Loads model from registry
- Makes predictions
- Saves predictions to ClickHouse tables
- Tracks predictions for monitoring

### 3. Model Monitoring (`monitor_models.py`)

Run daily model monitoring and generate reports.

**Usage**:
```bash
# Monitor all production models
python -m ownlens.ml.scripts.monitor_models

# Monitor specific model
python -m ownlens.ml.scripts.monitor_models --model-code churn_prediction

# Monitor for specific date
python -m ownlens.ml.scripts.monitor_models --date 2024-01-01

# Monitor with baseline accuracy
python -m ownlens.ml.scripts.monitor_models --model-code churn_prediction --baseline-accuracy 0.85
```

**What it does**:
- Generates daily monitoring reports
- Checks for data drift
- Checks for performance drift
- Triggers alerts if needed
- Saves to `ml_model_monitoring` table

### 4. Validate Predictions (`validate_predictions.py`)

Validate predictions against actual outcomes.

**Usage**:
```bash
# Validate churn predictions
python -m ownlens.ml.scripts.validate_predictions --model churn_prediction

# Validate conversion predictions
python -m ownlens.ml.scripts.validate_predictions --model conversion_prediction

# Validate last 60 days
python -m ownlens.ml.scripts.validate_predictions --model churn_prediction --days 60
```

**What it does**:
- Gets predictions from ClickHouse
- Validates against actual outcomes
- Updates predictions with actuals
- Calculates accuracy metrics

### 5. Complete ML Pipeline (`run_ml_pipeline.py`)

Run the complete ML pipeline end-to-end.

**Usage**:
```bash
# Run complete pipeline
python -m ownlens.ml.scripts.run_ml_pipeline

# Run customer domain only
python -m ownlens.ml.scripts.run_ml_pipeline --domain customer

# Skip training (use existing models)
python -m ownlens.ml.scripts.run_ml_pipeline --skip-training

# Run only monitoring
python -m ownlens.ml.scripts.run_ml_pipeline --skip-training --skip-predictions --skip-validation
```

**What it does**:
1. Train models (if not skipped)
2. Run batch predictions (if not skipped)
3. Validate predictions (if not skipped)
4. Monitor models (if not skipped)

## Complete Workflow Example

### Daily ML Pipeline

```bash
# 1. Train models (weekly or when needed)
python -m ownlens.ml.scripts.train_all_models --domain customer

# 2. Run batch predictions (daily)
python -m ownlens.ml.scripts.predict_batch --model churn_prediction --batch-size 10000

# 3. Validate predictions (daily)
python -m ownlens.ml.scripts.validate_predictions --model churn_prediction --days 7

# 4. Monitor models (daily)
python -m ownlens.ml.scripts.monitor_models

# Or run everything at once:
python -m ownlens.ml.scripts.run_ml_pipeline
```

## Scheduled Jobs

### Daily Jobs (Cron/Scheduler)

```bash
# Daily predictions (6 AM)
0 6 * * * python -m ownlens.ml.scripts.predict_batch --model churn_prediction --batch-size 10000

# Daily validation (7 AM)
0 7 * * * python -m ownlens.ml.scripts.validate_predictions --model churn_prediction --days 1

# Daily monitoring (8 AM)
0 8 * * * python -m ownlens.ml.scripts.monitor_models

# Weekly training (Sunday 2 AM)
0 2 * * 0 python -m ownlens.ml.scripts.train_all_models --domain customer
```

## Quick Start

```bash
# 1. Train all models
python -m ownlens.ml.scripts.train_all_models

# 2. Run predictions
python -m ownlens.ml.scripts.predict_batch --model churn_prediction

# 3. Monitor models
python -m ownlens.ml.scripts.monitor_models

# Or run everything:
python -m ownlens.ml.scripts.run_ml_pipeline
```

## Output

All scripts:
- ✅ Save results to ClickHouse
- ✅ Log progress and results
- ✅ Handle errors gracefully
- ✅ Provide summary statistics

## Next Steps

1. Set up scheduled jobs (cron, Airflow, etc.)
2. Configure alerting channels (email, Slack, etc.)
3. Set up dashboards (Grafana, etc.) to visualize ClickHouse data
4. Add more models as needed





