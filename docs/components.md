# Component Details

## Kafka Producer

### Architecture

```mermaid
graph TB
    A[UserSession Class] --> B[Generate Session]
    B --> C[Generate Events]
    C --> D[UserEvent Dataclass]
    D --> E[Serialize to JSON]
    E --> F[Kafka Producer]
    F --> G[Snappy Compression]
    G --> H[Kafka Topic]
    
    subgraph "Session Configuration"
        I[Brand Selection]
        J[Device Type]
        K[Geographic Location]
        L[Subscription Tier]
    end
    
    B --> I
    B --> J
    B --> K
    B --> L
    
    style A fill:#e1f5ff
    style F fill:#e8f5e9
    style H fill:#fff4e1
```

### Configuration

- **Topic**: `web_clicks`
- **Compression**: Snappy
- **Acks**: `all` (wait for all replicas)
- **Idempotence**: Enabled
- **Retries**: 3

### Event Types

```mermaid
pie title Event Type Distribution
    "article_view" : 60
    "article_click" : 15
    "video_play" : 5
    "newsletter_signup" : 2
    "subscription_prompt" : 3
    "ad_click" : 5
    "search" : 5
    "navigation" : 5
```

## Spark Streaming

### Processing Pipeline

```mermaid
flowchart TD
    A[Kafka Source] --> B[Parse JSON]
    B --> C[Schema Validation]
    C --> D[Filter session_end]
    D --> E[Group by session_id]
    
    E --> F[Aggregate Metrics]
    F --> G[Session Metadata]
    F --> H[Temporal Metrics]
    F --> I[Event Counts]
    F --> J[Page Data]
    
    G --> K[Transform Data]
    H --> K
    I --> K
    J --> K
    
    K --> L[Write Batch]
    L --> M[MinIO Delta Lake]
    L --> N[ClickHouse]
    
    style A fill:#fff4e1
    style E fill:#e8f5e9
    style K fill:#f3e5f5
```

### Aggregation Logic

```mermaid
graph LR
    A[Events] --> B[Group by session_id]
    B --> C[First Event Values]
    B --> D[Min/Max Timestamps]
    B --> E[Count by Type]
    B --> F[Collect Lists]
    
    C --> G[Brand, Country, Device]
    D --> H[Duration Calculation]
    E --> I[Engagement Metrics]
    F --> J[Pages, Categories]
    
    G --> K[Final DataFrame]
    H --> K
    I --> K
    J --> K
    
    style B fill:#e8f5e9
    style K fill:#f3e5f5
```

### Configuration

- **App Name**: `AxelSpringerRealTimeAnalytics`
- **Checkpoint Location**: `/tmp/spark-checkpoints`
- **Trigger Interval**: 10 seconds
- **Output Mode**: `complete`
- **JARs**: Kafka, Hadoop-AWS, Delta Lake

## MinIO (Data Lake)

### Storage Structure

```mermaid
graph TB
    A[session-metrics bucket] --> B[sessions/]
    B --> C[batch_1/]
    B --> D[batch_2/]
    B --> E[batch_N/]
    
    C --> F[Delta Lake Files]
    F --> G[_delta_log/]
    F --> H[Parquet Files]
    
    style A fill:#f3e5f5
    style F fill:#e8f5e9
```

### Delta Lake Features

```mermaid
graph LR
    A[Delta Lake] --> B[ACID Transactions]
    A --> C[Time Travel]
    A --> D[Schema Evolution]
    A --> E[Upsert Support]
    A --> F[Versioning]
    
    style A fill:#f3e5f5
```

## ClickHouse

### Table Structure

```mermaid
graph TB
    A[session_metrics] --> B[Primary Table]
    B --> C[Materialized Views]
    
    C --> D[brand_analytics]
    C --> E[device_analytics]
    C --> F[geographic_analytics]
    C --> G[subscription_analytics]
    C --> H[page_analytics]
    C --> I[session_summary]
    
    style A fill:#e0f2f1
    style C fill:#e8f5e9
```

### Schema Design

```mermaid
erDiagram
    session_metrics ||--o{ brand_analytics : "aggregates"
    session_metrics ||--o{ device_analytics : "aggregates"
    session_metrics ||--o{ geographic_analytics : "aggregates"
    session_metrics ||--o{ subscription_analytics : "aggregates"
    session_metrics ||--o{ page_analytics : "aggregates"
    
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
        datetime created_at
        uint64 batch_id
    }
```

## Data Flow Between Components

```mermaid
sequenceDiagram
    participant P as Producer
    participant K as Kafka
    participant S as Spark
    participant M as MinIO
    participant C as ClickHouse
    
    P->>K: Event (JSON, Snappy)
    K->>S: Stream (batch)
    S->>S: Parse & Aggregate
    S->>M: Delta Lake Write
    S->>C: INSERT Query
    S->>S: Checkpoint
    Note over S: Next batch (10s)
```

## Performance Characteristics

### Throughput

```mermaid
graph LR
    A[Producer] -->|~10 events/sec| B[Kafka]
    B -->|~10 events/sec| C[Spark]
    C -->|~7 sessions/batch| D[MinIO]
    C -->|~7 sessions/batch| E[ClickHouse]
    
    style A fill:#e1f5ff
    style B fill:#fff4e1
    style C fill:#e8f5e9
```

### Latency

- **Producer → Kafka**: < 10ms
- **Kafka → Spark**: 10s (batch interval)
- **Spark Processing**: 1-2s
- **Storage Write**: 500ms-1s
- **Total End-to-End**: ~12-13s

## Scaling Considerations

```mermaid
graph TB
    A[Current Setup] --> B[Single Instance]
    
    B --> C[Horizontal Scaling]
    C --> D[Multiple Producers]
    C --> E[Kafka Cluster]
    C --> F[Spark Cluster]
    C --> G[MinIO Cluster]
    C --> H[ClickHouse Cluster]
    
    style A fill:#e1f5ff
    style C fill:#e8f5e9
```

