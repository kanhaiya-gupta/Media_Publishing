# API Reference

## Event Schema

### UserEvent Structure

```mermaid
graph TB
    A[UserEvent] --> B[Core Fields]
    A --> C[Content Fields]
    A --> D[Device Fields]
    A --> E[Geographic Fields]
    A --> F[Engagement Fields]
    A --> G[Metadata Fields]
    
    B --> B1[event_id: UUID]
    B --> B2[user_id: Integer]
    B --> B3[session_id: UUID]
    B --> B4[event_type: String]
    B --> B5[timestamp: Long]
    
    C --> C1[brand: String]
    C --> C2[category: String]
    C --> C3[article_id: String]
    C --> C4[article_title: String]
    C --> C5[article_type: String]
    C --> C6[page_url: String]
    
    D --> D1[device_type: String]
    D --> D2[device_os: String]
    D --> D3[browser: String]
    
    E --> E1[country: String]
    E --> E2[city: String]
    E --> E3[timezone: String]
    
    F --> F1[engagement_metrics: Map]
    F --> F2[scroll_depth: Integer]
    F --> F3[time_on_page: Integer]
    F --> F4[read_progress: Integer]
    
    G --> G1[metadata: Map]
    G --> G2[session_start_time: Long]
    G --> G3[session_duration: Integer]
    G --> G4[articles_in_session: Integer]
    
    style A fill:#e1f5ff
```

### JSON Example

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": 123,
  "session_id": "26223370-4461-4c99-8dcc-766e517c8df6",
  "event_type": "article_view",
  "timestamp": 1699228800,
  "brand": "welt",
  "category": "politics",
  "article_id": "welt_politics_1234_1699228800",
  "article_title": "Breaking: Major Policy Shift Announced",
  "article_type": "news",
  "page_url": "https://welt.com/politics/welt_politics_1234_1699228800",
  "referrer": "google",
  "device_type": "desktop",
  "device_os": "Windows",
  "browser": "Chrome",
  "country": "DE",
  "city": "Berlin",
  "timezone": "Europe/Berlin",
  "subscription_tier": "premium",
  "user_segment": "engaged",
  "engagement_metrics": {
    "scroll_depth": 75,
    "time_on_page": 120,
    "read_progress": 60,
    "is_article_complete": false
  },
  "metadata": {
    "session_start_time": 1699228700,
    "session_duration": 100,
    "articles_in_session": 3,
    "is_new_session": false,
    "screen_resolution": "1920x1080",
    "language": "de-DE"
  }
}
```

## ClickHouse Schema

### session_metrics Table

```mermaid
erDiagram
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
        string referrer
        string timezone
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
        datetime created_at
        uint64 batch_id
    }
```

### Materialized Views

```mermaid
graph TB
    A[session_metrics] --> B[brand_analytics]
    A --> C[device_analytics]
    A --> D[geographic_analytics]
    A --> E[subscription_analytics]
    A --> F[page_analytics]
    A --> G[session_summary]
    
    style A fill:#e0f2f1
    style B fill:#e8f5e9
    style C fill:#e8f5e9
    style D fill:#e8f5e9
    style E fill:#e8f5e9
    style F fill:#e8f5e9
    style G fill:#e8f5e9
```

## Query Examples

### ClickHouse Queries

```sql
-- Total sessions by brand
SELECT brand, count() as total_sessions
FROM session_metrics
GROUP BY brand
ORDER BY total_sessions DESC;

-- Average session duration by device type
SELECT device_type, avg(session_duration_sec) as avg_duration
FROM session_metrics
GROUP BY device_type;

-- Top countries by sessions
SELECT country, count() as sessions
FROM session_metrics
GROUP BY country
ORDER BY sessions DESC
LIMIT 10;

-- Subscription tier analysis
SELECT subscription_tier, 
       count() as sessions,
       avg(session_duration_sec) as avg_duration,
       avg(total_events) as avg_events
FROM session_metrics
GROUP BY subscription_tier;
```

### MinIO/Delta Lake Queries

```python
# Read all batches
df = spark.read.format("delta").load("s3a://session-metrics/sessions/")

# Read specific batch
df = spark.read.format("delta").load("s3a://session-metrics/sessions/batch_1")

# Time travel query
df = spark.read.format("delta").option("versionAsOf", 0).load("s3a://session-metrics/sessions/")
```

## Configuration Reference

### Kafka Producer Configuration

```python
KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # Wait for all replicas
    retries=3,
    max_in_flight_requests_per_connection=1,
    enable_idempotence=True,
    compression_type='snappy'
)
```

### Spark Streaming Configuration

```python
SparkSession.builder \
    .appName("AxelSpringerRealTimeAnalytics") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

### ClickHouse Connection

```python
Client(
    host="localhost",
    port=9002,
    database="analytics",
    user="default",
    password="clickhouse"
)
```

### MinIO Connection

```python
config = {
    "endpoint": "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "path_style": True,
    "secure": False
}
```

## Event Types Reference

```mermaid
graph TB
    A[Event Types] --> B[article_view]
    A --> C[article_click]
    A --> D[video_play]
    A --> E[newsletter_signup]
    A --> F[subscription_prompt]
    A --> G[ad_click]
    A --> H[search]
    A --> I[navigation]
    A --> J[session_end]
    
    B --> B1[60% weight]
    C --> C1[15% weight]
    D --> D1[5% weight]
    E --> E1[2% weight]
    F --> F1[3% weight]
    G --> G1[5% weight]
    H --> H1[5% weight]
    I --> I1[5% weight]
    
    style A fill:#e1f5ff
```

## Data Types

### Spark Schema Types

```mermaid
graph LR
    A[Spark Types] --> B[StringType]
    A --> C[IntegerType]
    A --> D[LongType]
    A --> E[MapType]
    A --> F[ArrayType]
    A --> G[StructType]
    
    style A fill:#e8f5e9
```

### ClickHouse Types

- **String**: Variable-length strings
- **UInt32**: 32-bit unsigned integers
- **UInt16**: 16-bit unsigned integers
- **UInt64**: 64-bit unsigned integers
- **DateTime**: Date and time values
- **Float32**: 32-bit floating point

