# ML Module Architecture Proposal

## Current Architecture Issues

### ❌ Problems with Current Structure

```
src/ownlens/ml/
├── 01_data_exploration.py          # Flat structure
├── 02_feature_engineering.py        # Numbered scripts
├── 03_baseline_models.py            # No organization
├── 04_churn_prediction.py           # No reusability
├── 05_user_segmentation.py          # No domain separation
├── 06_recommendation_system.py      # Hard to maintain
├── 07_conversion_prediction.py      # No shared code
├── 08_click_prediction.py           # No base classes
├── 09_engagement_prediction.py      # No versioning
├── 10_ad_optimization.py            # No monitoring
├── utils/
│   ├── data_loader.py               # Limited utilities
│   └── config.py                    # Basic config
└── models/                          # Just pickle files
    └── *.pkl
```

**Issues:**
1. **Flat structure** - Everything in root directory
2. **No domain separation** - All models mixed together
3. **No reusability** - Each script duplicates code
4. **No base classes** - No shared model infrastructure
5. **No versioning** - Models saved as pickle files
6. **No monitoring** - No integration with ML model tables
7. **Hard to test** - Scripts are not modular
8. **Hard to maintain** - Changes require updating multiple files

## Proposed Architecture

### ✅ Better Structure (Aligned with OwnLens Framework)

```
src/ownlens/ml/
├── __init__.py
├── README.md
│
├── base/                              # Base classes and interfaces
│   ├── __init__.py
│   ├── model.py                      # Base model class
│   ├── trainer.py                    # Base trainer class
│   ├── predictor.py                  # Base predictor class
│   ├── evaluator.py                  # Base evaluator class
│   └── feature_engineer.py            # Base feature engineer class
│
├── data/                              # Data loading and processing
│   ├── __init__.py
│   ├── loaders/                      # Data loaders
│   │   ├── __init__.py
│   │   ├── user_features_loader.py   # Load from customer_user_features
│   │   ├── session_loader.py         # Load from customer_sessions
│   │   ├── event_loader.py           # Load from customer_events
│   │   ├── editorial_loader.py       # Load from editorial tables
│   │   └── reference_loader.py       # Load from reference tables
│   ├── processors/                   # Data processors
│   │   ├── __init__.py
│   │   ├── feature_processor.py      # Feature processing
│   │   ├── data_quality.py           # Data quality checks
│   │   └── transformers.py           # Data transformations
│   └── joiners/                      # Table joiners
│       ├── __init__.py
│       ├── reference_joiner.py       # Join with reference tables
│       └── editorial_joiner.py       # Join with editorial tables
│
├── features/                          # Feature engineering
│   ├── __init__.py
│   ├── base.py                       # Base feature engineer
│   ├── customer/                     # Customer domain features
│   │   ├── __init__.py
│   │   ├── behavioral_features.py    # Behavioral features
│   │   ├── engagement_features.py    # Engagement features
│   │   ├── temporal_features.py      # Temporal features
│   │   └── churn_features.py         # Churn-specific features
│   ├── editorial/                    # Editorial domain features
│   │   ├── __init__.py
│   │   ├── content_features.py       # Content features
│   │   ├── author_features.py        # Author features
│   │   └── performance_features.py   # Performance features
│   └── shared/                       # Shared features
│       ├── __init__.py
│       └── geographic_features.py    # Geographic features
│
├── models/                            # ML models by domain
│   ├── __init__.py
│   ├── customer/                     # Customer domain models
│   │   ├── __init__.py
│   │   ├── churn/                    # Churn prediction
│   │   │   ├── __init__.py
│   │   │   ├── model.py              # Churn model definition
│   │   │   ├── trainer.py            # Churn trainer
│   │   │   ├── predictor.py          # Churn predictor
│   │   │   └── evaluator.py          # Churn evaluator
│   │   ├── segmentation/             # User segmentation
│   │   │   ├── __init__.py
│   │   │   ├── model.py
│   │   │   ├── trainer.py
│   │   │   └── predictor.py
│   │   ├── conversion/               # Conversion prediction
│   │   │   ├── __init__.py
│   │   │   ├── model.py
│   │   │   ├── trainer.py
│   │   │   └── predictor.py
│   │   └── recommendation/           # Content recommendation
│   │       ├── __init__.py
│   │       ├── model.py
│   │       ├── trainer.py
│   │       └── predictor.py
│   ├── editorial/                    # Editorial domain models
│   │   ├── __init__.py
│   │   ├── performance/              # Article performance
│   │   │   ├── __init__.py
│   │   │   ├── model.py
│   │   │   ├── trainer.py
│   │   │   └── predictor.py
│   │   └── trending/                # Trending topics
│   │       ├── __init__.py
│   │       ├── model.py
│   │       └── trainer.py
│   └── shared/                       # Shared models
│       ├── __init__.py
│       └── baseline/                 # Baseline models
│           ├── __init__.py
│           ├── logistic_regression.py
│           ├── random_forest.py
│           └── neural_network.py
│
├── pipelines/                         # End-to-end pipelines
│   ├── __init__.py
│   ├── training/                    # Training pipelines
│   │   ├── __init__.py
│   │   ├── churn_training.py        # Churn training pipeline
│   │   ├── segmentation_training.py # Segmentation training
│   │   └── recommendation_training.py
│   ├── inference/                   # Inference pipelines
│   │   ├── __init__.py
│   │   ├── batch_inference.py       # Batch inference
│   │   └── realtime_inference.py    # Real-time inference
│   └── evaluation/                  # Evaluation pipelines
│       ├── __init__.py
│       └── model_evaluation.py      # Model evaluation
│
├── registry/                          # Model registry integration
│   ├── __init__.py
│   ├── model_registry.py             # Model registry client
│   ├── version_manager.py            # Version management
│   └── metadata_manager.py           # Metadata management
│
├── monitoring/                        # Model monitoring
│   ├── __init__.py
│   ├── performance_monitor.py        # Performance monitoring
│   ├── drift_detector.py             # Data drift detection
│   └── alerting.py                   # Alerting system
│
├── evaluation/                        # Model evaluation
│   ├── __init__.py
│   ├── metrics.py                    # Evaluation metrics
│   ├── validators.py                 # Model validators
│   └── reports.py                    # Evaluation reports
│
├── utils/                             # Shared utilities
│   ├── __init__.py
│   ├── config.py                     # Configuration
│   ├── logging.py                    # Logging utilities
│   ├── serialization.py              # Model serialization
│   └── visualization.py              # Visualization utilities
│
└── scripts/                           # Executable scripts (legacy/CLI)
    ├── __init__.py
    ├── explore_data.py               # Data exploration
    ├── train_churn.py                # Train churn model
    ├── train_segmentation.py         # Train segmentation
    ├── predict_churn.py              # Predict churn
    └── evaluate_model.py             # Evaluate model
```

## Architecture Principles

### 1. Domain-Driven Design
- **Customer domain**: Churn, segmentation, conversion, recommendation
- **Editorial domain**: Performance, trending
- **Shared**: Baseline models, common utilities

### 2. Separation of Concerns
- **Data layer**: Loading, processing, joining
- **Feature layer**: Feature engineering by domain
- **Model layer**: Models by domain and type
- **Pipeline layer**: End-to-end workflows
- **Registry layer**: Model versioning and metadata
- **Monitoring layer**: Model monitoring and alerting

### 3. Reusability
- **Base classes**: Shared model, trainer, predictor interfaces
- **Shared utilities**: Common functions across domains
- **Feature modules**: Reusable feature engineering

### 4. Production Readiness
- **Model registry**: Integration with `ml_model_registry`
- **Monitoring**: Integration with `ml_model_monitoring`
- **Versioning**: Proper model version management
- **Testing**: Modular, testable components

### 5. Framework Alignment
- **Matches ownlens structure**: Similar to `repositories/`, `services/`, `models/`
- **Uses framework patterns**: Configuration, logging, error handling
- **Integrates with framework**: Uses repositories, services where appropriate

## Component Details

### Base Classes

```python
# base/model.py
class BaseMLModel(ABC):
    """Base class for all ML models."""
    
    def __init__(self, model_id: str, model_config: dict):
        self.model_id = model_id
        self.model_config = model_config
        self.model = None
    
    @abstractmethod
    def build_model(self):
        """Build the model architecture."""
        pass
    
    @abstractmethod
    def train(self, X, y):
        """Train the model."""
        pass
    
    @abstractmethod
    def predict(self, X):
        """Make predictions."""
        pass
    
    def save(self, path: str):
        """Save model to registry."""
        pass
    
    def load(self, path: str):
        """Load model from registry."""
        pass
```

### Data Loaders

```python
# data/loaders/user_features_loader.py
class UserFeaturesLoader:
    """Load pre-computed user features from customer_user_features."""
    
    def load(self, feature_date: date = None, brand_id: str = None):
        """Load user features with optional filters."""
        pass
    
    def load_with_references(self, feature_date: date = None):
        """Load user features joined with reference tables."""
        pass
    
    def load_with_content(self, user_id: str, feature_date: date = None):
        """Load user features with content-based features."""
        pass
```

### Feature Engineers

```python
# features/customer/churn_features.py
class ChurnFeatureEngineer(BaseFeatureEngineer):
    """Churn-specific feature engineering."""
    
    def engineer_features(self, user_features: pd.DataFrame):
        """Engineer churn-specific features."""
        # Add churn-specific features
        pass
```

### Models

```python
# models/customer/churn/model.py
class ChurnModel(BaseMLModel):
    """Churn prediction model."""
    
    def build_model(self):
        """Build XGBoost churn model."""
        self.model = xgb.XGBClassifier(**self.model_config)
    
    def train(self, X, y):
        """Train churn model."""
        self.model.fit(X, y)
        # Store in ml_model_registry
        # Track in ml_model_training_runs
```

### Pipelines

```python
# pipelines/training/churn_training.py
class ChurnTrainingPipeline:
    """End-to-end churn training pipeline."""
    
    def run(self, feature_date: date = None):
        """Run complete training pipeline."""
        # 1. Load data
        loader = UserFeaturesLoader()
        data = loader.load_with_references(feature_date)
        
        # 2. Engineer features
        engineer = ChurnFeatureEngineer()
        features = engineer.engineer_features(data)
        
        # 3. Train model
        model = ChurnModel(model_id="churn_v1.0.0")
        model.train(features.X, features.y)
        
        # 4. Evaluate
        evaluator = ChurnEvaluator()
        metrics = evaluator.evaluate(model, features.X_test, features.y_test)
        
        # 5. Save to registry
        registry = ModelRegistry()
        registry.register_model(model, metrics)
        
        # 6. Monitor
        monitor = PerformanceMonitor()
        monitor.track_training(model, metrics)
```

## Migration Path

### Phase 1: Create Base Structure
1. Create base classes
2. Create data loaders
3. Create feature engineers
4. Create model base classes

### Phase 2: Migrate Existing Models
1. Migrate churn prediction
2. Migrate segmentation
3. Migrate conversion prediction
4. Migrate recommendation

### Phase 3: Add New Capabilities
1. Add model registry integration
2. Add monitoring
3. Add evaluation pipelines
4. Add inference pipelines

### Phase 4: Cleanup
1. Move old scripts to `scripts/` folder
2. Update documentation
3. Add tests
4. Remove deprecated code

## Benefits

### ✅ Organization
- **Clear structure** - Easy to find code
- **Domain separation** - Customer vs Editorial
- **Logical grouping** - Related code together

### ✅ Reusability
- **Base classes** - Shared functionality
- **Shared utilities** - Common functions
- **Feature modules** - Reusable features

### ✅ Maintainability
- **Modular** - Easy to update individual components
- **Testable** - Each component can be tested
- **Documented** - Clear structure = clear documentation

### ✅ Production Ready
- **Model registry** - Proper versioning
- **Monitoring** - Track model performance
- **Pipelines** - End-to-end workflows

### ✅ Framework Alignment
- **Matches ownlens** - Similar structure to other modules
- **Uses patterns** - Configuration, logging, error handling
- **Integrates** - Uses repositories, services

## Comparison

### Current (Bad)
```python
# 04_churn_prediction.py (500+ lines)
# - Data loading
# - Feature engineering
# - Model training
# - Evaluation
# - Saving
# All in one file!
```

### Proposed (Good)
```python
# models/customer/churn/trainer.py (100 lines)
class ChurnTrainer(BaseTrainer):
    def train(self):
        data = UserFeaturesLoader().load()
        features = ChurnFeatureEngineer().engineer(data)
        model = ChurnModel().train(features)
        metrics = ChurnEvaluator().evaluate(model)
        ModelRegistry().register(model, metrics)
```

## Next Steps

1. **Review this proposal** - Get feedback
2. **Create base structure** - Set up directories
3. **Implement base classes** - Create interfaces
4. **Migrate one model** - Churn as example
5. **Iterate** - Refine based on experience
6. **Migrate remaining** - Move all models
7. **Cleanup** - Remove old code

## Conclusion

The proposed architecture:
- ✅ **Better organized** - Clear structure
- ✅ **More reusable** - Base classes and shared code
- ✅ **Production ready** - Registry and monitoring
- ✅ **Framework aligned** - Matches ownlens patterns
- ✅ **Maintainable** - Modular and testable

This architecture will make the ML module:
- **Easier to maintain** - Clear structure
- **Easier to extend** - Base classes
- **Easier to test** - Modular components
- **Production ready** - Registry and monitoring





