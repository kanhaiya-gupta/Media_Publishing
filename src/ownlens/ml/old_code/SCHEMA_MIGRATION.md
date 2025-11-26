# Schema Migration Guide: session_metrics → customer_sessions

## Overview

The ML scripts have been updated to use the new comprehensive ClickHouse schema. This document outlines the changes needed.

## Table Name Changes

| Old Table | New Table |
|-----------|-----------|
| `session_metrics` | `customer_sessions` |

## Field Name Changes

| Old Field | New Field | Notes |
|-----------|-----------|-------|
| `brand` | `brand_id` | Now UUID instead of string |
| `country` | `country_code` | Now code instead of full name |
| `city` | `city_id` | Now UUID instead of string |
| `device_type` | `device_type_id` | Now UUID instead of string |
| `device_os` | `os_id` | Now UUID instead of string |
| `browser` | `browser_id` | Now UUID instead of string |

## Fields That Remain the Same

- `session_id`
- `user_id`
- `session_start`
- `session_end`
- `session_duration_sec`
- `total_events`
- `article_views`
- `article_clicks`
- `video_plays`
- `newsletter_signups`
- `ad_clicks`
- `searches`
- `pages_visited_count`
- `unique_pages_count`
- `unique_categories_count`
- `subscription_tier`
- `user_segment`
- `referrer`
- `timezone`

## New Fields Available

- `company_id` - Company UUID
- `account_id` - Account UUID
- `engagement_score` - Pre-calculated engagement score
- `scroll_depth_avg` - Average scroll depth
- `time_on_page_avg` - Average time on page
- `metadata` - JSON metadata field

## Migration Status

### ✅ Updated Files

1. **data_loader.py** - Updated with `use_new_schema` parameter
2. **01_data_exploration.py** - Partially updated (needs complete review)
3. **schema_mapper.py** - New utility for field mapping

### ⏳ Pending Updates

1. **02_feature_engineering.py** - Needs field name updates
2. **06_recommendation_system.py** - Needs table and field updates
3. **08_click_prediction.py** - Needs table and field updates
4. **09_engagement_prediction.py** - Needs table and field updates
5. **10_ad_optimization.py** - Needs table and field updates

## Usage

### Using New Schema (Default)

```python
from ownlens.ml.utils import load_session_data_from_clickhouse

# Uses customer_sessions table by default
df = load_session_data_from_clickhouse(limit=1000)
```

### Using Legacy Schema

```python
from ownlens.ml.utils import load_session_data_from_clickhouse

# Uses session_metrics table (legacy)
df = load_session_data_from_clickhouse(limit=1000, use_new_schema=False)
```

## Important Notes

1. **ID Fields**: The new schema uses UUIDs for IDs (brand_id, device_type_id, etc.). If you need the actual names, you'll need to join with lookup tables.

2. **Backward Compatibility**: The data_loader functions support both schemas via the `use_new_schema` parameter.

3. **Field Mapping**: Use `schema_mapper.py` utilities to map between old and new field names.

4. **Query Updates**: All SQL queries in ML scripts need to be updated to:
   - Use `customer_sessions` instead of `session_metrics`
   - Use new field names (country_code, device_type_id, etc.)
   - Handle UUID fields appropriately

## Example Query Updates

### Old Query
```sql
SELECT 
    user_id,
    brand,
    country,
    device_type,
    count() as sessions
FROM session_metrics
GROUP BY user_id, brand, country, device_type
```

### New Query
```sql
SELECT 
    user_id,
    brand_id,
    country_code,
    device_type_id,
    count() as sessions
FROM customer_sessions
GROUP BY user_id, brand_id, country_code, device_type_id
```

## Next Steps

1. Complete updating all ML scripts to use new schema
2. Add joins to lookup tables if human-readable names are needed
3. Update feature engineering to use new fields (engagement_score, etc.)
4. Test all scripts with new schema
5. Consider using `customer_user_features` table for pre-computed features

