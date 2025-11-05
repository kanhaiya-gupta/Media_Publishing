# Data Model for Machine Learning

## Overview

This document describes the data model optimized for machine learning use cases, including feature engineering, model training, and real-time inference.

## ML-Ready Data Model

```mermaid
erDiagram
    session_metrics ||--o{ ml_features : "generates"
    session_metrics ||--o{ user_features : "aggregates"
    session_metrics ||--o{ content_features : "extracts"
    session_metrics ||--o{ engagement_features : "calculates"
    
    session_metrics {
        string session_id PK
        uint32 user_id
        string brand
        string country
        string city
        string device_type
        string device_os
        string browser
        string subscription_tier
        string user_segment
        datetime session_start
        datetime session_end
        uint32 session_duration_sec
        uint16 total_events
        uint16 article_views
        uint16 article_clicks
        uint16 video_plays
        uint16 newsletter_signups
        uint16 ad_clicks
        uint16 searches
        uint16 pages_visited_count
        uint16 unique_pages_count
        uint16 unique_categories_count
        string categories_visited
        string article_ids_visited
        string article_titles_visited
        string page_events
    }
    
    ml_features {
        float session_duration_normalized
        float engagement_score
        float content_diversity_score
        float device_engagement_score
        float geographic_engagement_score
        float subscription_value_score
    }
    
    user_features {
        uint32 user_id PK
        uint32 total_sessions
        float avg_session_duration
        float avg_events_per_session
        string preferred_brand
        string preferred_device
        string preferred_country
        float lifetime_value
    }
    
    content_features {
        string article_id PK
        uint32 total_views
        uint32 total_clicks
        float avg_time_on_page
        float completion_rate
        string category
        string brand
    }
    
    engagement_features {
        float scroll_depth_avg
        float read_progress_avg
        float time_on_page_avg
        float article_completion_rate
        float video_completion_rate
        float click_through_rate
    }
```

## Feature Engineering Pipeline

```mermaid
flowchart TD
    A[Raw Session Data] --> B[Feature Extraction]
    
    B --> C[User Features]
    B --> D[Session Features]
    B --> E[Content Features]
    B --> F[Engagement Features]
    B --> G[Behavioral Features]
    
    C --> C1[User ID]
    C --> C2[Total Sessions]
    C --> C3[Average Duration]
    C --> C4[Preferred Brand]
    C --> C5[Subscription Tier]
    
    D --> D1[Session Duration]
    D --> D2[Event Count]
    D --> D3[Pages Visited]
    D --> D4[Unique Categories]
    D --> D5[Time of Day]
    D --> D6[Day of Week]
    
    E --> E1[Article Views]
    E --> E2[Article Clicks]
    E --> E3[Video Plays]
    E --> E4[Categories Visited]
    E --> E5[Content Diversity]
    
    F --> F1[Scroll Depth]
    F --> F2[Read Progress]
    F --> F3[Time on Page]
    F --> F4[Completion Rate]
    
    G --> G1[Navigation Patterns]
    G --> G2[Search Behavior]
    G --> G3[Ad Interaction]
    G --> G4[Newsletter Signups]
    
    C1 --> H[ML Feature Vector]
    C2 --> H
    D1 --> H
    E1 --> H
    F1 --> H
    G1 --> H
    
    H --> I[Model Training]
    H --> J[Real-time Inference]
    
    style A fill:#e1f5ff
    style H fill:#e8f5e9
    style I fill:#f3e5f5
    style J fill:#fff4e1
```

## Feature Categories

### 1. User-Level Features

```mermaid
graph TB
    A[User Features] --> B[Demographics]
    A --> C[Behavioral]
    A --> D[Engagement]
    A --> E[Value]
    
    B --> B1[Country]
    B --> B2[City]
    B --> B3[Timezone]
    B --> B4[Device Type]
    B --> B5[Device OS]
    B --> B6[Browser]
    
    C --> C1[Total Sessions]
    C --> C2[Avg Session Duration]
    C --> C3[Preferred Brand]
    C --> C4[Preferred Category]
    C --> C5[Preferred Time]
    C --> C6[Session Frequency]
    
    D --> D1[Avg Events per Session]
    D --> D2[Avg Pages per Session]
    D --> D3[Engagement Score]
    D --> D4[Content Diversity]
    D --> D5[Video Engagement]
    
    E --> E1[Subscription Tier]
    E --> E2[User Segment]
    E --> E3[Newsletter Status]
    E --> E4[Ad Click Rate]
    E --> E5[Conversion Rate]
    
    style A fill:#e1f5ff
```

### 2. Session-Level Features

```mermaid
graph LR
    A[Session Features] --> B[Temporal]
    A --> C[Volume]
    A --> D[Diversity]
    A --> E[Engagement]
    
    B --> B1[Session Duration]
    B --> B2[Time of Day]
    B --> B3[Day of Week]
    B --> B4[Session Start Time]
    
    C --> C1[Total Events]
    C --> C2[Pages Visited]
    C --> C3[Article Views]
    C --> C4[Video Plays]
    
    D --> D1[Unique Pages]
    D --> D2[Unique Categories]
    D --> D3[Brand Diversity]
    D --> D4[Content Types]
    
    E --> E1[Scroll Depth]
    E --> E2[Read Progress]
    E --> E3[Time on Page]
    E --> E4[Completion Rate]
    
    style A fill:#e8f5e9
```

### 3. Content-Level Features

```mermaid
graph TB
    A[Content Features] --> B[Article Metrics]
    A --> C[Category Metrics]
    A --> D[Brand Metrics]
    
    B --> B1[Article Views]
    B --> B2[Article Clicks]
    B --> B3[Time on Article]
    B --> B4[Completion Rate]
    B --> B5[Scroll Depth]
    
    C --> C1[Category Views]
    C --> C2[Category Engagement]
    C --> C3[Category Diversity]
    C --> C4[Category Preference]
    
    D --> D1[Brand Views]
    D --> D2[Brand Engagement]
    D --> D3[Brand Preference]
    D --> D4[Brand Switching]
    
    style A fill:#f3e5f5
```

### 4. Engagement Features

```mermaid
graph LR
    A[Engagement Features] --> B[Interaction]
    A --> C[Time-Based]
    A --> D[Conversion]
    
    B --> B1[Click Rate]
    B --> B2[Scroll Rate]
    B --> B3[Video Play Rate]
    B --> B4[Search Rate]
    
    C --> C1[Avg Time on Page]
    C --> C2[Session Duration]
    C --> C3[Time Between Events]
    C --> C4[Peak Engagement Time]
    
    D --> D1[Newsletter Signup Rate]
    D --> D2[Subscription Prompt Rate]
    D --> D3[Ad Click Rate]
    D --> D4[Conversion Rate]
    
    style A fill:#fff4e1
```

## Quick ML Feature Extraction

### Feature Extraction Queries

```mermaid
sequenceDiagram
    participant ML as ML Engineer
    participant CH as ClickHouse
    participant M as MinIO
    participant S as Spark
    
    ML->>CH: Query User Features
    CH->>ML: Return User Aggregates
    
    ML->>CH: Query Session Features
    CH->>ML: Return Session Metrics
    
    ML->>M: Query Raw Events
    M->>S: Read Delta Lake
    S->>ML: Return Event Stream
    
    ML->>ML: Feature Engineering
    ML->>ML: Model Training
```

### ClickHouse Queries for ML

#### User-Level Features

```sql
-- User feature vector for ML
SELECT 
    user_id,
    -- Behavioral Features
    count() as total_sessions,
    avg(session_duration_sec) as avg_session_duration,
    avg(total_events) as avg_events_per_session,
    avg(pages_visited_count) as avg_pages_per_session,
    
    -- Engagement Features
    avg(article_views) as avg_article_views,
    avg(video_plays) as avg_video_plays,
    avg(newsletter_signups) as avg_newsletter_signups,
    sum(newsletter_signups) > 0 as has_newsletter,
    
    -- Diversity Features
    avg(unique_categories_count) as avg_categories_diversity,
    avg(unique_pages_count) as avg_pages_diversity,
    
    -- Value Features
    subscription_tier,
    user_segment,
    
    -- Geographic Features
    country,
    city,
    
    -- Device Features
    device_type,
    device_os,
    
    -- Brand Preference
    argMax(brand, session_start) as preferred_brand,
    countIf(brand = 'bild') as bild_sessions,
    countIf(brand = 'welt') as welt_sessions,
    countIf(brand = 'business_insider') as bi_sessions,
    countIf(brand = 'politico') as politico_sessions,
    countIf(brand = 'sport_bild') as sport_bild_sessions
    
FROM session_metrics
GROUP BY user_id, subscription_tier, user_segment, country, city, device_type, device_os
ORDER BY total_sessions DESC;
```

#### Session-Level Features

```sql
-- Session feature vector for ML
SELECT 
    session_id,
    user_id,
    
    -- Temporal Features
    session_duration_sec,
    toHour(session_start) as hour_of_day,
    toDayOfWeek(session_start) as day_of_week,
    toDate(session_start) as date,
    
    -- Volume Features
    total_events,
    pages_visited_count,
    unique_pages_count,
    unique_categories_count,
    
    -- Engagement Features
    article_views,
    article_clicks,
    video_plays,
    searches,
    ad_clicks,
    
    -- Calculated Features
    article_views * 1.0 / total_events as article_view_ratio,
    article_clicks * 1.0 / total_events as click_ratio,
    video_plays * 1.0 / total_events as video_ratio,
    unique_categories_count * 1.0 / pages_visited_count as content_diversity,
    
    -- Conversion Features
    newsletter_signups,
    CASE WHEN newsletter_signups > 0 THEN 1 ELSE 0 END as converted,
    
    -- Brand Features
    brand,
    country,
    device_type,
    subscription_tier,
    user_segment
    
FROM session_metrics
ORDER BY session_start DESC;
```

#### Engagement Score Calculation

```sql
-- Engagement score feature
SELECT 
    session_id,
    user_id,
    
    -- Base Engagement Score (0-100)
    (
        (article_views * 2) +
        (article_clicks * 3) +
        (video_plays * 5) +
        (newsletter_signups * 20) +
        (searches * 2) +
        (pages_visited_count * 1) +
        (unique_categories_count * 2)
    ) as engagement_score,
    
    -- Normalized Engagement Score
    (
        (article_views * 2.0 / total_events) +
        (article_clicks * 3.0 / total_events) +
        (video_plays * 5.0 / total_events) +
        (newsletter_signups * 20.0 / total_events)
    ) * 100 as normalized_engagement_score,
    
    -- Time-Based Engagement
    session_duration_sec / 60.0 as session_duration_minutes,
    total_events / (session_duration_sec / 60.0) as events_per_minute,
    
    -- Content Engagement
    article_views * 1.0 / pages_visited_count as article_view_rate,
    article_clicks * 1.0 / article_views as click_through_rate
    
FROM session_metrics
ORDER BY engagement_score DESC;
```

### Spark Feature Engineering

```python
# Feature engineering in Spark
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Read session data
df = spark.read.format("delta").load("s3a://session-metrics/sessions/")

# Create ML features
ml_features = df.withColumn(
    "engagement_score",
    (F.col("article_views") * 2) +
    (F.col("article_clicks") * 3) +
    (F.col("video_plays") * 5) +
    (F.col("newsletter_signups") * 20) +
    (F.col("pages_visited_count") * 1) +
    (F.col("unique_categories_count") * 2)
).withColumn(
    "normalized_engagement",
    F.col("engagement_score") / F.col("total_events")
).withColumn(
    "content_diversity",
    F.col("unique_categories_count") / F.col("pages_visited_count")
).withColumn(
    "click_through_rate",
    F.col("article_clicks") / F.col("article_views")
).withColumn(
    "hour_of_day",
    F.hour("session_start")
).withColumn(
    "day_of_week",
    F.dayofweek("session_start")
).withColumn(
    "is_weekend",
    F.when(F.dayofweek("session_start").isin([1, 7]), 1).otherwise(0)
).withColumn(
    "session_duration_minutes",
    F.col("session_duration_sec") / 60.0
).withColumn(
    "events_per_minute",
    F.col("total_events") / (F.col("session_duration_sec") / 60.0)
)

# Select ML-ready features
ml_ready = ml_features.select(
    "session_id",
    "user_id",
    "brand",
    "country",
    "device_type",
    "subscription_tier",
    "user_segment",
    "engagement_score",
    "normalized_engagement",
    "content_diversity",
    "click_through_rate",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "session_duration_minutes",
    "events_per_minute",
    "article_views",
    "article_clicks",
    "video_plays",
    "newsletter_signups",
    "pages_visited_count",
    "unique_categories_count"
)
```

## ML Use Cases

### 1. Churn Prediction

```mermaid
flowchart TD
    A[User Sessions] --> B[Feature Extraction]
    B --> C[User-Level Aggregates]
    C --> D[Churn Features]
    D --> E[Model Training]
    E --> F[Churn Prediction]
    
    D --> D1[Session Frequency]
    D --> D2[Engagement Decline]
    D --> D3[Time Since Last Session]
    D --> D4[Subscription Status]
    
    style A fill:#e1f5ff
    style E fill:#e8f5e9
    style F fill:#f3e5f5
```

### 2. Content Recommendation

```mermaid
flowchart LR
    A[Session Data] --> B[Content Features]
    B --> C[User Preferences]
    C --> D[Collaborative Filtering]
    D --> E[Recommendation Engine]
    
    B --> B1[Article Views]
    B --> B2[Category Preferences]
    B --> B3[Engagement Patterns]
    
    style A fill:#e1f5ff
    style D fill:#e8f5e9
    style E fill:#f3e5f5
```

### 3. Subscription Conversion

```mermaid
flowchart TB
    A[Session Events] --> B[Conversion Features]
    B --> C[Subscription Model]
    C --> D[Conversion Probability]
    
    B --> B1[Engagement Score]
    B --> B2[Content Consumption]
    B --> B3[Newsletter Signups]
    B --> B4[Ad Clicks]
    
    style A fill:#e1f5ff
    style C fill:#e8f5e9
    style D fill:#f3e5f5
```

### 4. User Segmentation

```mermaid
graph TB
    A[User Features] --> B[Clustering Features]
    B --> C[K-Means Clustering]
    C --> D[User Segments]
    
    B --> B1[Engagement Level]
    B --> B2[Content Preferences]
    B --> B3[Device Usage]
    B --> B4[Geographic Patterns]
    B --> B5[Time Patterns]
    
    D --> D1[Power Users]
    D --> D2[Casual Users]
    D --> D3[New Visitors]
    D --> D4[At-Risk Users]
    
    style A fill:#e1f5ff
    style C fill:#e8f5e9
    style D fill:#f3e5f5
```

## Feature Store Structure

```mermaid
graph TB
    A[Feature Store] --> B[User Features]
    A --> C[Session Features]
    A --> D[Content Features]
    A --> E[Engagement Features]
    
    B --> B1[Real-time Features]
    B --> B2[Batch Features]
    
    C --> C1[Real-time Features]
    C --> C2[Batch Features]
    
    D --> D1[Real-time Features]
    D --> D2[Batch Features]
    
    E --> E1[Real-time Features]
    E --> E2[Batch Features]
    
    style A fill:#e1f5ff
```

## Real-Time Feature Extraction

```mermaid
sequenceDiagram
    participant User as User Session
    participant Kafka as Kafka Stream
    participant Spark as Spark Streaming
    participant FS as Feature Store
    participant ML as ML Model
    
    User->>Kafka: Generate Events
    Kafka->>Spark: Stream Events
    Spark->>Spark: Calculate Features
    Spark->>FS: Update Feature Store
    ML->>FS: Fetch Features
    FS->>ML: Return Feature Vector
    ML->>ML: Real-time Prediction
    
    Note over FS: Features updated in real-time
    Note over ML: Predictions made instantly
```

## Data Access Patterns for ML

### Pattern 1: Batch Training

```mermaid
flowchart LR
    A[ClickHouse Query] --> B[Feature Extraction]
    B --> C[DataFrame]
    C --> D[Model Training]
    
    style A fill:#e0f2f1
    style C fill:#e8f5e9
    style D fill:#f3e5f5
```

### Pattern 2: Real-Time Inference

```mermaid
flowchart LR
    A[Spark Streaming] --> B[Feature Calculation]
    B --> C[Feature Store]
    C --> D[ML Model]
    D --> E[Prediction]
    
    style A fill:#fff4e1
    style C fill:#e8f5e9
    style D fill:#f3e5f5
```

### Pattern 3: Historical Analysis

```mermaid
flowchart LR
    A[MinIO Delta Lake] --> B[Time Travel Query]
    B --> C[Historical Features]
    C --> D[Model Evaluation]
    
    style A fill:#f3e5f5
    style C fill:#e8f5e9
    style D fill:#fff4e1
```

## Quick Start for ML Engineers

### 1. Get User Features

```python
from clickhouse_driver import Client

client = Client(host='localhost', port=9002, 
                database='analytics', user='default', 
                password='clickhouse')

# Get user features for ML
query = """
SELECT 
    user_id,
    count() as total_sessions,
    avg(session_duration_sec) as avg_duration,
    avg(total_events) as avg_events,
    avg(article_views) as avg_article_views,
    subscription_tier,
    country,
    device_type
FROM session_metrics
GROUP BY user_id, subscription_tier, country, device_type
"""

features = client.execute(query)
```

### 2. Get Session Features

```python
# Get session features for ML
query = """
SELECT 
    session_id,
    user_id,
    session_duration_sec,
    total_events,
    article_views,
    article_clicks,
    video_plays,
    pages_visited_count,
    unique_categories_count,
    brand,
    country,
    device_type,
    subscription_tier
FROM session_metrics
WHERE session_start >= now() - INTERVAL 30 DAY
"""

sessions = client.execute(query)
```

### 3. Get Engagement Features

```python
# Calculate engagement score
query = """
SELECT 
    session_id,
    user_id,
    (
        (article_views * 2) +
        (article_clicks * 3) +
        (video_plays * 5) +
        (newsletter_signups * 20)
    ) as engagement_score,
    session_duration_sec,
    total_events
FROM session_metrics
ORDER BY session_start DESC
LIMIT 10000
"""

engagement = client.execute(query)
```

## Model Training Data Pipeline

```mermaid
flowchart TD
    A[Raw Data Sources] --> B[Feature Engineering]
    B --> C[Feature Validation]
    C --> D[Feature Store]
    D --> E[Training Dataset]
    E --> F[Model Training]
    F --> G[Model Evaluation]
    G --> H[Model Deployment]
    
    A --> A1[ClickHouse]
    A --> A2[MinIO Delta Lake]
    
    B --> B1[User Features]
    B --> B2[Session Features]
    B --> B3[Content Features]
    B --> B4[Engagement Features]
    
    style A fill:#e1f5ff
    style D fill:#e8f5e9
    style F fill:#f3e5f5
    style H fill:#fff4e1
```

## Feature Importance

```mermaid
graph LR
    A[ML Features] --> B[High Importance]
    A --> C[Medium Importance]
    A --> D[Low Importance]
    
    B --> B1[Engagement Score]
    B --> B2[Newsletter Signups]
    B --> B3[Subscription Tier]
    
    C --> C1[Session Duration]
    C --> C2[Article Views]
    C --> C3[Content Diversity]
    
    D --> D1[Device Type]
    D --> D2[Browser]
    D --> D3[Timezone]
    
    style B fill:#e8f5e9
    style C fill:#fff4e1
    style D fill:#f3e5f5
```

## Best Practices

### 1. Feature Engineering

- **Normalize features**: Use min-max or z-score normalization
- **Handle missing values**: Use median/mode for categorical, mean for numerical
- **Feature selection**: Remove highly correlated features
- **Time-based features**: Extract hour, day, week, month

### 2. Data Quality

- **Validate features**: Check for nulls, outliers, inconsistencies
- **Monitor data drift**: Track feature distributions over time
- **Feature versioning**: Track feature definitions and transformations

### 3. Performance

- **Pre-compute features**: Use materialized views in ClickHouse
- **Cache frequently used queries**: Store results in feature store
- **Incremental updates**: Update features incrementally, not from scratch

## Example ML Workflow

```mermaid
flowchart TD
    A[Start ML Project] --> B[Define Problem]
    B --> C[Identify Features]
    C --> D[Extract Features]
    D --> E[Feature Engineering]
    E --> F[Train Model]
    F --> G{Model Good?}
    G -->|No| C
    G -->|Yes| H[Deploy Model]
    H --> I[Monitor Performance]
    I --> J{Drift Detected?}
    J -->|Yes| C
    J -->|No| I
    
    style A fill:#e1f5ff
    style F fill:#e8f5e9
    style H fill:#f3e5f5
```

