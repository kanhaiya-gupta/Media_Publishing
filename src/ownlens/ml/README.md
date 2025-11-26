# OwnLens ML Module

Machine learning models and pipelines for the ownlens framework.

## Architecture

The ML module follows a domain-driven design aligned with the ownlens framework:

```
src/ownlens/ml/
├── base/                    # Base classes (Model, Trainer, Predictor, Evaluator)
├── data/                    # Data loading and processing
│   ├── loaders/            # Load from ClickHouse tables
│   ├── processors/         # Data processing
│   └── joiners/            # Table joins
├── features/                # Feature engineering by domain
│   ├── customer/           # Customer domain features
│   ├── editorial/          # Editorial domain features
│   └── shared/             # Shared features
├── models/                  # ML models by domain
│   ├── customer/           # Churn, segmentation, conversion, recommendation
│   ├── editorial/          # Performance, trending
│   └── shared/             # Baseline models
├── pipelines/              # End-to-end workflows
│   ├── training/          # Training pipelines
│   ├── inference/         # Inference pipelines
│   └── evaluation/        # Evaluation pipelines
├── registry/               # Model registry integration
├── monitoring/             # Model monitoring
├── evaluation/             # Model evaluation
└── utils/                   # Shared utilities
```

## Quick Start

### Training a Churn Model

```python
from ownlens.ml.pipelines.training import ChurnTrainingPipeline
from datetime import date

# Initialize pipeline
pipeline = ChurnTrainingPipeline()

# Run training
metrics = pipeline.run(
    feature_date=date.today(),
    brand_id="your-brand-id",
    limit=10000,
    test_size=0.2
)

print(f"Model accuracy: {metrics['accuracy']}")
print(f"Model AUC: {metrics['roc_auc']}")
```

### Making Predictions

```python
from ownlens.ml.models.customer.churn import ChurnPredictor, ChurnModel

# Load trained model
model = ChurnModel()
model.load("path/to/model.pkl")

# Create predictor
predictor = ChurnPredictor(model)

# Predict for a user
predictions = predictor.predict("user-id", return_proba=True)
print(f"Churn probability: {predictions['churn_probability'].iloc[0]}")
```

### Loading Data

```python
from ownlens.ml.data.loaders import UserFeaturesLoader
from datetime import date

# Load pre-computed user features
loader = UserFeaturesLoader()
features = loader.load(
    feature_date=date.today(),
    brand_id="your-brand-id",
    include_references=True  # Get human-readable names
)

print(f"Loaded {len(features)} user features")
```

## Key Features

### ✅ Pre-Computed Features

Uses `customer_user_features` table for **10-100x faster** training:
- Pre-computed ML-ready features
- No on-the-fly computation
- Consistent features across models

### ✅ Reference Table Joins

Get human-readable names:
- Brand names (not just UUIDs)
- Country names
- Device type names
- Browser names

### ✅ Domain-Driven Design

Organized by domain:
- **Customer domain**: Churn, segmentation, conversion, recommendation
- **Editorial domain**: Performance, trending
- **Shared**: Baseline models, common utilities

### ✅ Base Classes

Reusable base classes:
- `BaseMLModel`: All models inherit from this
- `BaseTrainer`: Standardized training workflow
- `BasePredictor`: Consistent prediction interface
- `BaseEvaluator`: Common evaluation metrics

### ✅ Production Ready

- Model versioning
- Model registry integration (planned)
- Model monitoring (planned)
- Comprehensive logging

## Data Sources

### Primary: `customer_user_features`

**Use this for ML training** - pre-computed features:
- Behavioral features
- Engagement features
- Temporal features
- Churn features

### Secondary: `customer_sessions`

Use as fallback or for session-level analysis:
- Session metrics
- Event counts
- Engagement scores

### Additional: Editorial Tables

For content-based features:
- `editorial_article_performance`: Article engagement
- `editorial_content_events`: Content interactions
- `editorial_articles`: Article metadata

## Examples

See the `old_code/` folder for legacy scripts that have been migrated to the new architecture.

## Migration

The old numbered scripts (`01_data_exploration.py`, etc.) have been moved to `old_code/` folder.

New architecture provides:
- Better organization
- Reusable components
- Production-ready infrastructure
- Framework alignment

## Next Steps

1. **Complete remaining models**: Segmentation, conversion, recommendation
2. **Add model registry**: Integrate with `ml_model_registry` table
3. **Add monitoring**: Integrate with `ml_model_monitoring` table
4. **Add inference pipelines**: Real-time and batch inference
5. **Add evaluation pipelines**: Automated model evaluation

## Documentation

- `ARCHITECTURE_PROPOSAL.md`: Architecture design
- `ML_SCHEMA_DESIGN_PLAN.md`: Schema usage plan





