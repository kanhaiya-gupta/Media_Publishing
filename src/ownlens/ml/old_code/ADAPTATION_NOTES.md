# ML Module Adaptation for OwnLens Framework

## Overview

The ML scripts have been adapted from a past project to integrate with the ownlens framework. This document describes the changes made and how to use the adapted scripts.

## Changes Made

### 1. Configuration Management

**Created:** `src/ownlens/ml/utils/config.py`

- Added `MLConfig` class following ownlens framework patterns
- Uses environment variables with sensible defaults
- Centralizes ClickHouse connection configuration
- Supports ML-specific settings (model directories, data directories, etc.)

**Environment Variables:**
- `CLICKHOUSE_HOST` (default: "localhost")
- `CLICKHOUSE_PORT` (default: 9002)
- `CLICKHOUSE_DB` (default: "ownlens_analytics")
- `CLICKHOUSE_USER` (default: "default")
- `CLICKHOUSE_PASSWORD` (default: "")
- `ML_MODELS_DIR` (default: "models")
- `ML_DATA_DIR` (default: ".")
- `ML_RESULTS_DIR` (default: ".")

### 2. Data Loading Utilities

**Updated:** `src/ownlens/ml/utils/data_loader.py`

- Updated `get_clickhouse_client()` to use framework configuration
- Added logging support
- Updated `load_ml_features()` to use configuration for file paths
- All functions now use the centralized configuration

### 3. Script Updates

**Updated Scripts:**
- `01_data_exploration.py`
- `02_feature_engineering.py`
- `06_recommendation_system.py`
- `08_click_prediction.py`
- `09_engagement_prediction.py`
- `10_ad_optimization.py`

**Changes:**
- Removed hardcoded ClickHouse connection parameters
- Updated `connect_to_clickhouse()` functions to use `get_clickhouse_client()` from utils
- Added framework imports: `from ownlens.ml.utils import get_clickhouse_client, get_ml_config`
- Removed direct `clickhouse_driver` imports where replaced by framework utilities

### 4. Package Structure

**Created:**
- `src/ownlens/ml/__init__.py` - Package initialization
- `src/ownlens/ml/utils/__init__.py` - Updated with proper exports

## Usage

### Setting Environment Variables

Before running the scripts, set the appropriate environment variables:

```bash
# ClickHouse Configuration
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=9002
export CLICKHOUSE_DB=ownlens_analytics
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=your_password

# ML Configuration (optional)
export ML_MODELS_DIR=models
export ML_DATA_DIR=.
export ML_RESULTS_DIR=.
```

### Running Scripts

The scripts can be run directly from the `src/ownlens/ml/` directory:

```bash
# Make sure PYTHONPATH includes the src directory
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Or run from project root with module syntax
python -m ownlens.ml.01_data_exploration
python -m ownlens.ml.02_feature_engineering
python -m ownlens.ml.04_churn_prediction
# ... etc
```

### Using Configuration Programmatically

```python
from ownlens.ml.utils import get_ml_config, get_clickhouse_client

# Get configuration
config = get_ml_config()
print(f"ClickHouse: {config.clickhouse_host}:{config.clickhouse_port}")

# Get ClickHouse client
client = get_clickhouse_client()
if client:
    # Use client...
    client.disconnect()
```

## Framework Integration

### Connection Management

The ML scripts now use the same configuration pattern as the ETL module:
- Configuration from environment variables
- Centralized connection management
- Consistent error handling

### Future Integration Points

The ML module is now ready for further integration with:
- **Repositories**: Can use `ownlens.repositories.ml_models.*` for data access
- **Services**: Can use `ownlens.services.ml_models.*` for business logic
- **Models**: Can use `ownlens.models.ml_models.*` for validation

## Migration Notes

### Backward Compatibility

The scripts maintain backward compatibility:
- Default values match the original hardcoded values
- Scripts can still be run standalone
- No breaking changes to script functionality

### Import Paths

The scripts use absolute imports (`from ownlens.ml.utils import ...`). To use them:
1. Install the package: `pip install -e .` (if setup.py exists)
2. Or add `src/` to PYTHONPATH: `export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"`

## Next Steps

1. **Further Integration**: Consider integrating with repositories/services for data access
2. **Model Registry**: Use `ownlens.models.ml_models.model_registry` for model versioning
3. **Monitoring**: Integrate with `ownlens.services.ml_models.model_monitoring_service`
4. **Deployment**: Use framework services for model deployment and serving

## Files Modified

- `src/ownlens/ml/utils/config.py` (new)
- `src/ownlens/ml/utils/data_loader.py` (updated)
- `src/ownlens/ml/utils/__init__.py` (updated)
- `src/ownlens/ml/__init__.py` (new)
- `src/ownlens/ml/01_data_exploration.py` (updated)
- `src/ownlens/ml/02_feature_engineering.py` (updated)
- `src/ownlens/ml/06_recommendation_system.py` (updated)
- `src/ownlens/ml/08_click_prediction.py` (updated)
- `src/ownlens/ml/09_engagement_prediction.py` (updated)
- `src/ownlens/ml/10_ad_optimization.py` (updated)

## Testing

To verify the adaptations work:

```bash
# Test configuration
python -c "from ownlens.ml.utils import get_ml_config; print(get_ml_config())"

# Test ClickHouse connection
python -c "from ownlens.ml.utils import get_clickhouse_client; client = get_clickhouse_client(); print('Connected!' if client else 'Failed')"
```

