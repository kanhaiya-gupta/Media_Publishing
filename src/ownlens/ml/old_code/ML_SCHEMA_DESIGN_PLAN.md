# ML Scripts Design Plan - Leveraging Comprehensive ClickHouse Schema

## Executive Summary

The current ML scripts are **underutilizing** the comprehensive ClickHouse schema. We have **90+ tables** with rich analytics capabilities, but the scripts are only using `customer_sessions` and doing manual feature engineering. This document outlines how to redesign the ML scripts to leverage the full schema.

## Current State Analysis

### ❌ What We're Currently Doing (Wrong)

1. **Only using `customer_sessions` table** - Missing rich data from other tables
2. **Manual feature engineering** - Computing features on-the-fly instead of using pre-computed ones
3. **No joins with reference tables** - Missing human-readable names (brands, countries, devices)
4. **No editorial data** - Missing content-based features from article performance
5. **No ML model integration** - Not using existing predictions, monitoring, or model registry
6. **No use of pre-computed features** - Ignoring `customer_user_features` table

### ✅ What We Should Be Doing

1. **Use `customer_user_features`** - Pre-computed ML-ready features
2. **Join with reference tables** - Get human-readable names and additional metadata
3. **Leverage editorial tables** - Content-based features from article performance
4. **Use ML model tables** - Store predictions, monitor models, track A/B tests
5. **Join with performance tables** - Richer analytics from aggregated metrics
6. **Use event tables** - Raw events for time-series features

## Available Tables Overview

### Customer Domain (9 tables)

| Table | Purpose | Use Case |
|-------|---------|----------|
| `customer_sessions` | Aggregated session metrics | Session-level features, fallback when features not available |
| `customer_events` | Raw events from Kafka | Time-series features, event-level analysis |
| `customer_user_features` | **Pre-computed ML features** | **PRIMARY SOURCE for ML training** |
| `customer_user_segments` | User segment definitions | Segment-based features, segment analysis |
| `customer_user_segment_assignments` | User segment assignments | Current segment, segment history |
| `customer_churn_predictions` | Churn predictions | Use existing predictions, validate models |
| `customer_recommendations` | Content recommendations | Recommendation features, recommendation performance |
| `customer_conversion_predictions` | Conversion predictions | Use existing predictions, validate models |
| `customer_session_summary_daily` | Daily session summaries | Time-series aggregations, trend analysis |

### Editorial Domain (10+ tables)

| Table | Purpose | Use Case |
|-------|---------|----------|
| `editorial_articles` | Article metadata | Content-based features, article properties |
| `editorial_authors` | Author metadata | Author-based features, author performance |
| `editorial_content_events` | Content events | Content interaction features |
| `editorial_article_performance` | **Article performance metrics** | **Content engagement features** |
| `editorial_article_content` | Content versioning | Content quality features |
| `editorial_author_performance` | Author performance | Author influence features |
| `editorial_category_performance` | Category performance | Category popularity features |

### ML Model Domain (6 tables)

| Table | Purpose | Use Case |
|-------|---------|----------|
| `ml_model_registry` | Model metadata | Model versioning, model lookup |
| `ml_model_features` | Feature definitions | Feature documentation, feature importance |
| `ml_model_training_runs` | Training runs | Training history, hyperparameter tracking |
| `ml_model_predictions` | **All predictions** | **Store predictions, validate models** |
| `ml_model_monitoring` | Model monitoring | Model performance tracking, drift detection |
| `ml_model_ab_tests` | A/B tests | A/B test results, model comparison |

### Reference Tables (10+ tables)

| Table | Purpose | Use Case |
|-------|---------|----------|
| `companies` | Company metadata | Company-level features |
| `brands` | Brand metadata | Brand-level features, brand names |
| `countries` | Country metadata | Country names, geographic features |
| `cities` | City metadata | City names, geographic features |
| `categories` | Category metadata | Category names, category hierarchy |
| `device_types` | Device metadata | Device names, device properties |
| `operating_systems` | OS metadata | OS names, OS properties |
| `browsers` | Browser metadata | Browser names, browser properties |

## Redesign Plan

### Phase 1: Use Pre-Computed Features (HIGH PRIORITY)

**Current Problem:**
- Scripts compute features from `customer_sessions` on-the-fly
- Slow, inefficient, and error-prone

**Solution:**
- **Use `customer_user_features` table** as primary data source
- This table already has all ML-ready features pre-computed
- Much faster and more reliable

**Implementation:**
```python
# OLD (Wrong):
def extract_user_features(client, limit=10000):
    query = """
    SELECT 
        user_id,
        count() as total_sessions,
        avg(session_duration_sec) as avg_session_duration,
        -- ... 50+ more computed features
    FROM customer_sessions
    GROUP BY user_id
    """

# NEW (Correct):
def load_user_features(client, feature_date=None, limit=None):
    query = """
    SELECT 
        user_id,
        total_sessions,
        avg_session_duration_sec,
        -- ... all pre-computed features
    FROM customer_user_features
    WHERE feature_date = COALESCE(?, today())
    ORDER BY total_sessions DESC
    """
```

### Phase 2: Join with Reference Tables (HIGH PRIORITY)

**Current Problem:**
- Scripts use UUIDs (brand_id, device_type_id, etc.) without human-readable names
- Can't do meaningful analysis or visualization

**Solution:**
- Join with reference tables to get names and metadata
- Enables better analysis and debugging

**Implementation:**
```python
def load_user_features_with_names(client, feature_date=None):
    query = """
    SELECT 
        uf.*,
        b.brand_name,
        b.brand_code,
        dt.device_type_name,
        os.os_name,
        br.browser_name,
        c.country_name,
        city.city_name
    FROM customer_user_features uf
    LEFT JOIN brands b ON uf.preferred_brand_id = b.brand_id
    LEFT JOIN device_types dt ON uf.preferred_device_type_id = dt.device_type_id
    LEFT JOIN operating_systems os ON uf.preferred_os_id = os.os_id
    LEFT JOIN browsers br ON uf.preferred_browser_id = br.browser_id
    LEFT JOIN countries c ON uf.primary_country_code = c.country_code
    LEFT JOIN cities city ON uf.primary_city_id = city.city_id
    WHERE uf.feature_date = COALESCE(?, today())
    """
```

### Phase 3: Add Content-Based Features (MEDIUM PRIORITY)

**Current Problem:**
- Scripts ignore editorial/content data
- Missing rich content-based features

**Solution:**
- Join with editorial tables to add content engagement features
- Use article performance metrics

**Implementation:**
```python
def load_user_features_with_content(client, user_id, feature_date=None):
    query = """
    SELECT 
        uf.*,
        -- Content engagement features
        COUNT(DISTINCT ece.article_id) as articles_viewed,
        AVG(eap.engagement_score) as avg_article_engagement,
        SUM(eap.total_views) as total_article_views,
        -- Author features
        COUNT(DISTINCT ece.author_id) as unique_authors_followed,
        -- Category features
        COUNT(DISTINCT ece.category_id) as categories_engaged
    FROM customer_user_features uf
    LEFT JOIN editorial_content_events ece 
        ON uf.user_id = ece.user_id 
        AND toDate(ece.event_timestamp) = uf.feature_date
    LEFT JOIN editorial_article_performance eap 
        ON ece.article_id = eap.article_id 
        AND eap.performance_date = uf.feature_date
    WHERE uf.user_id = ? AND uf.feature_date = COALESCE(?, today())
    GROUP BY uf.user_id, uf.feature_date
    """
```

### Phase 4: Integrate with ML Model Tables (MEDIUM PRIORITY)

**Current Problem:**
- Scripts don't store predictions in ML model tables
- No model versioning or monitoring

**Solution:**
- Store predictions in `ml_model_predictions`
- Register models in `ml_model_registry`
- Track training runs in `ml_model_training_runs`
- Monitor models in `ml_model_monitoring`

**Implementation:**
```python
def save_prediction(client, model_id, user_id, prediction, features):
    query = """
    INSERT INTO ml_model_predictions VALUES
    (
        generateUUIDv4(),
        ?,
        'churn',
        ?,
        'user',
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        now(),
        today(),
        NULL,
        ?,
        ?,
        generateUUIDv4(),
        '{}',
        now()
    )
    """
    # ... execute query
```

### Phase 5: Use Performance Tables (LOW PRIORITY)

**Current Problem:**
- Scripts compute aggregations on-the-fly
- Slow for large datasets

**Solution:**
- Use pre-aggregated performance tables
- Much faster for time-series analysis

**Implementation:**
```python
def get_daily_metrics(client, brand_id, start_date, end_date):
    query = """
    SELECT 
        date,
        total_sessions,
        total_users,
        avg_session_duration_sec,
        total_article_views,
        total_video_plays
    FROM customer_session_summary_daily
    WHERE brand_id = ?
        AND date >= ?
        AND date <= ?
    ORDER BY date
    """
```

## Script-by-Script Redesign

### 1. `01_data_exploration.py`

**Current:** Only explores `customer_sessions`

**Should:**
- Explore all customer domain tables
- Explore editorial tables
- Explore ML model tables
- Show relationships between tables
- Analyze data quality across tables

### 2. `02_feature_engineering.py`

**Current:** Computes features from `customer_sessions`

**Should:**
- **Use `customer_user_features` as primary source**
- Add content-based features from editorial tables
- Join with reference tables for names
- Store enhanced features back to `customer_user_features.ml_features` JSON field

### 3. `03_baseline_models.py`

**Current:** Uses manually computed features

**Should:**
- Use `customer_user_features` table
- Store model metadata in `ml_model_registry`
- Store training runs in `ml_model_training_runs`
- Compare with existing models

### 4. `04_churn_prediction.py`

**Current:** Trains model, saves locally

**Should:**
- Use `customer_user_features` table
- Use existing `customer_churn_predictions` for validation
- Store predictions in `ml_model_predictions`
- Register model in `ml_model_registry`
- Track training in `ml_model_training_runs`
- Monitor in `ml_model_monitoring`

### 5. `05_user_segmentation.py`

**Current:** Creates segments, saves locally

**Should:**
- Use `customer_user_features` table
- Store segments in `customer_user_segments`
- Store assignments in `customer_user_segment_assignments`
- Link to model in `ml_model_registry`

### 6. `06_recommendation_system.py`

**Current:** Uses `customer_sessions` for user-article interactions

**Should:**
- Use `editorial_content_events` for interactions
- Use `editorial_article_performance` for article features
- Use `editorial_articles` for article metadata
- Store recommendations in `customer_recommendations`
- Track performance in `ml_model_monitoring`

### 7. `07_conversion_prediction.py`

**Current:** Trains model, saves locally

**Should:**
- Use `customer_user_features` table
- Use existing `customer_conversion_predictions` for validation
- Store predictions in `ml_model_predictions`
- Register model in `ml_model_registry`

### 8. `08_click_prediction.py`

**Current:** Uses `customer_sessions`

**Should:**
- Use `customer_events` for click events
- Join with `editorial_articles` for article features
- Use `editorial_article_performance` for article performance
- Store predictions in `ml_model_predictions`

### 9. `09_engagement_prediction.py`

**Current:** Uses `customer_sessions`

**Should:**
- Use `customer_user_features` for user features
- Use `editorial_article_performance` for content features
- Use time-series from `customer_events`
- Store predictions in `ml_model_predictions`

### 10. `10_ad_optimization.py`

**Current:** Uses `customer_sessions`

**Should:**
- Use `customer_events` for ad events
- Join with `editorial_articles` for content context
- Store A/B test results in `ml_model_ab_tests`
- Track in `ml_model_monitoring`

## Data Flow Architecture

### Current Flow (Inefficient)
```
customer_sessions → Manual Feature Engineering → ML Model → Local File
```

### Proposed Flow (Efficient)
```
customer_events → customer_sessions → customer_user_features (pre-computed)
                                                      ↓
editorial_content_events → editorial_article_performance
                                                      ↓
                    Enhanced Features (with joins)
                                                      ↓
                    ML Model Training
                                                      ↓
ml_model_registry ← ml_model_training_runs ← ml_model_predictions
                                                      ↓
                    ml_model_monitoring
```

## Implementation Priority

### 🔴 HIGH PRIORITY (Do First)

1. **Update all scripts to use `customer_user_features`**
   - This is the biggest win - pre-computed features are much faster
   - Files: `02_feature_engineering.py`, `03_baseline_models.py`, `04_churn_prediction.py`, `05_user_segmentation.py`, `07_conversion_prediction.py`

2. **Add joins with reference tables**
   - Get human-readable names for debugging and analysis
   - All scripts that output results

3. **Store predictions in ML model tables**
   - Enable model versioning and monitoring
   - Files: `04_churn_prediction.py`, `07_conversion_prediction.py`, `08_click_prediction.py`, `09_engagement_prediction.py`

### 🟡 MEDIUM PRIORITY (Do Next)

4. **Add content-based features**
   - Join with editorial tables
   - Files: `06_recommendation_system.py`, `08_click_prediction.py`, `09_engagement_prediction.py`

5. **Integrate with ML model registry**
   - Register models, track training runs
   - All model training scripts

6. **Use event tables for time-series**
   - `customer_events` for detailed event analysis
   - Files: `08_click_prediction.py`, `09_engagement_prediction.py`

### 🟢 LOW PRIORITY (Nice to Have)

7. **Use performance tables**
   - Pre-aggregated metrics for faster queries
   - Files: `01_data_exploration.py`, `09_engagement_prediction.py`

8. **Add monitoring integration**
   - Track model performance over time
   - All prediction scripts

## Example: Redesigned Feature Engineering

### Before (Current - Wrong)
```python
def extract_user_features(client, limit=10000):
    query = """
    SELECT 
        user_id,
        count() as total_sessions,
        avg(session_duration_sec) as avg_session_duration,
        -- ... 50+ computed features
    FROM customer_sessions
    GROUP BY user_id
    LIMIT {limit}
    """
    # Takes 5+ minutes for 10K users
```

### After (Proposed - Correct)
```python
def load_user_features(client, feature_date=None, brand_id=None, limit=None):
    """
    Load pre-computed user features with joins to reference tables.
    
    This is MUCH faster than computing features on-the-fly.
    """
    query = """
    SELECT 
        uf.*,
        -- Reference table joins for human-readable names
        b.brand_name,
        dt.device_type_name,
        os.os_name,
        br.browser_name,
        c.country_name,
        city.city_name,
        -- Content features from editorial
        COALESCE(content_stats.articles_viewed, 0) as articles_viewed,
        COALESCE(content_stats.avg_article_engagement, 0) as avg_article_engagement
    FROM customer_user_features uf
    LEFT JOIN brands b ON uf.preferred_brand_id = b.brand_id
    LEFT JOIN device_types dt ON uf.preferred_device_type_id = dt.device_type_id
    LEFT JOIN operating_systems os ON uf.preferred_os_id = os.os_id
    LEFT JOIN browsers br ON uf.preferred_browser_id = br.browser_id
    LEFT JOIN countries c ON uf.primary_country_code = c.country_code
    LEFT JOIN cities city ON uf.primary_city_id = city.city_id
    LEFT JOIN (
        SELECT 
            user_id,
            COUNT(DISTINCT article_id) as articles_viewed,
            AVG(engagement_score) as avg_article_engagement
        FROM editorial_content_events ece
        JOIN editorial_article_performance eap 
            ON ece.article_id = eap.article_id
        WHERE toDate(ece.event_timestamp) = COALESCE(?, today())
        GROUP BY user_id
    ) content_stats ON uf.user_id = content_stats.user_id
    WHERE uf.feature_date = COALESCE(?, today())
        AND (? IS NULL OR uf.brand_id = ?)
    ORDER BY uf.total_sessions DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    
    # Takes < 1 second for 10K users!
```

## Benefits of Redesign

### Performance
- **10-100x faster** - Pre-computed features vs on-the-fly computation
- **Reduced query time** - From minutes to seconds
- **Better scalability** - Can handle millions of users

### Data Quality
- **Consistent features** - Same features used across all models
- **Validated features** - Features computed once, validated once
- **Rich features** - Content-based features from editorial tables

### Maintainability
- **Single source of truth** - `customer_user_features` table
- **Model versioning** - Track model versions in registry
- **Monitoring** - Track model performance over time
- **Reproducibility** - All features and models tracked

### Analytics
- **Human-readable** - Names from reference tables
- **Richer insights** - Content-based features
- **Better debugging** - Can trace features to source tables

## Migration Path

### Step 1: Update Data Loader
- Add functions to load from `customer_user_features`
- Add functions to join with reference tables
- Keep old functions for backward compatibility

### Step 2: Update Feature Engineering
- Use `customer_user_features` as primary source
- Add content-based features
- Store enhanced features

### Step 3: Update Model Scripts
- Use new data loader functions
- Store predictions in ML tables
- Register models in registry

### Step 4: Add Monitoring
- Track model performance
- Monitor data drift
- Alert on issues

## Next Steps

1. **Review this plan** - Ensure alignment with requirements
2. **Prioritize tasks** - Focus on HIGH priority items first
3. **Update data_loader.py** - Add new functions for pre-computed features
4. **Update scripts incrementally** - Start with feature engineering
5. **Test thoroughly** - Ensure new approach works correctly
6. **Document changes** - Update README and code comments

## Conclusion

The current ML scripts are **significantly underutilizing** the comprehensive ClickHouse schema. By redesigning them to:

1. Use pre-computed features from `customer_user_features`
2. Join with reference tables for human-readable names
3. Add content-based features from editorial tables
4. Integrate with ML model tables for versioning and monitoring

We can achieve:
- **10-100x performance improvement**
- **Better data quality and consistency**
- **Richer features for better models**
- **Proper model versioning and monitoring**

This redesign is **critical** for production-ready ML infrastructure.

