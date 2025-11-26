# ML Module - Run Scripts Guide

## 🚀 How to Run the ML Framework

The ML framework includes run scripts to execute all ML operations end-to-end.

## 📋 Available Scripts

### 1. `train_all_models.py` - Train All Models

**Purpose**: Train all ML models and register them in ClickHouse.

**Usage**:
```bash
# Train all models (customer + editorial)
python -m ownlens.ml.scripts.train_all_models

# Train customer domain only
python -m ownlens.ml.scripts.train_all_models --domain customer

# Train editorial domain only
python -m ownlens.ml.scripts.train_all_models --domain editorial

# Train with filters
python -m ownlens.ml.scripts.train_all_models --brand-id brand-123 --limit 5000
```

**What it does**:
- ✅ Trains all models (9 models total)
- ✅ Registers models in `ml_model_registry`
- ✅ Saves training runs to `ml_model_training_runs`
- ✅ Saves features to `ml_model_features`
- ✅ Updates model status to `VALIDATED`

**Output**: Models registered in ClickHouse, ready for production.

---

### 2. `predict_batch.py` - Batch Predictions

**Purpose**: Run batch predictions and save to ClickHouse.

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
- ✅ Loads model from registry
- ✅ Makes predictions
- ✅ Saves to `ml_model_predictions` (unified)
- ✅ Saves to domain-specific tables
- ✅ Tracks predictions for monitoring

**Output**: Predictions saved to ClickHouse tables.

---

### 3. `monitor_models.py` - Model Monitoring

**Purpose**: Run daily model monitoring and generate reports.

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
- ✅ Generates daily monitoring reports
- ✅ Calculates accuracy metrics
- ✅ Checks for data drift
- ✅ Checks for performance drift
- ✅ Triggers alerts if needed
- ✅ Saves to `ml_model_monitoring` table

**Output**: Monitoring reports in ClickHouse, alerts if issues detected.

---

### 4. `validate_predictions.py` - Validate Predictions

**Purpose**: Validate predictions against actual outcomes.

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
- ✅ Gets predictions from ClickHouse
- ✅ Validates against actual outcomes
- ✅ Updates predictions with actuals
- ✅ Calculates accuracy metrics

**Output**: Validated predictions with accuracy metrics.

---

### 5. `run_ml_pipeline.py` - Complete ML Pipeline

**Purpose**: Run the complete ML pipeline end-to-end.

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
1. ✅ Train models (if not skipped)
2. ✅ Run batch predictions (if not skipped)
3. ✅ Validate predictions (if not skipped)
4. ✅ Monitor models (if not skipped)

**Output**: Complete ML pipeline execution.

---

## 🎯 Typical Workflow

### Initial Setup (One-time)

```bash
# 1. Train all models
python -m ownlens.ml.scripts.train_all_models

# 2. Deploy models to production
# (Update model status to PRODUCTION via registry)
```

### Daily Operations

```bash
# Option 1: Run complete pipeline
python -m ownlens.ml.scripts.run_ml_pipeline

# Option 2: Run steps individually
python -m ownlens.ml.scripts.predict_batch --model churn_prediction --batch-size 10000
python -m ownlens.ml.scripts.validate_predictions --model churn_prediction --days 1
python -m ownlens.ml.scripts.monitor_models
```

### Weekly Operations

```bash
# Retrain models weekly
python -m ownlens.ml.scripts.train_all_models --domain customer
```

---

## 📅 Scheduled Jobs (Cron/Airflow)

### Daily Schedule

```bash
# 6 AM - Run batch predictions
0 6 * * * python -m ownlens.ml.scripts.predict_batch --model churn_prediction --batch-size 10000

# 7 AM - Validate predictions
0 7 * * * python -m ownlens.ml.scripts.validate_predictions --model churn_prediction --days 1

# 8 AM - Monitor models
0 8 * * * python -m ownlens.ml.scripts.monitor_models
```

### Weekly Schedule

```bash
# Sunday 2 AM - Retrain models
0 2 * * 0 python -m ownlens.ml.scripts.train_all_models --domain customer
```

---

## 🔧 Configuration

All scripts use environment variables from `MLConfig`:
- `CLICKHOUSE_HOST` - ClickHouse host
- `CLICKHOUSE_PORT` - ClickHouse port
- `CLICKHOUSE_DB` - ClickHouse database
- `CLICKHOUSE_USER` - ClickHouse user
- `CLICKHOUSE_PASSWORD` - ClickHouse password

Set these in your environment or `.env` file.

---

## 📊 Output Examples

### Training Output

```
================================================================================
TRAINING CUSTOMER DOMAIN MODELS
================================================================================

[1/4] Training Churn Prediction Model...
✅ Churn model trained and registered: abc-123-def-456

[2/4] Training User Segmentation Model...
✅ Segmentation model trained and registered: xyz-789-uvw-012

================================================================================
CUSTOMER DOMAIN TRAINING COMPLETE: 4 models trained
================================================================================
```

### Prediction Output

```
================================================================================
BATCH CHURN PREDICTIONS
================================================================================
Using model: abc-123-def-456 (churn_prediction v1.0.0)
Predicting churn for 1000 users...
Progress: 100/1000 predictions saved
Progress: 200/1000 predictions saved
...
✅ Saved 1000 churn predictions (batch: batch-123-456)
```

### Monitoring Output

```
================================================================================
MODEL MONITORING - ALL PRODUCTION MODELS
================================================================================
Monitoring date: 2024-01-15

Monitoring: churn_prediction v1.0.0 (abc-123-def-456)
Generating monitoring report...
✅ Monitoring report generated: monitoring-123
Checking for drift...
✅ Drift check completed: detected=False
Checking alerts...
✅ No alerts triggered

================================================================================
MONITORING SUMMARY
================================================================================
Models monitored: 9
Models with drift: 0
Total alerts: 0
================================================================================
```

---

## 🚀 Quick Start

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

---

## ✅ All Scripts Ready!

All run scripts are implemented and ready to use:
- ✅ `train_all_models.py` - Train and register models
- ✅ `predict_batch.py` - Batch predictions
- ✅ `monitor_models.py` - Model monitoring
- ✅ `validate_predictions.py` - Validate predictions
- ✅ `run_ml_pipeline.py` - Complete pipeline

The ML framework is now **fully operational**! 🎉





