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

#### Current Setup (Development)

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

#### Production Scale (Terabyte-Level)

```mermaid
graph TB
    A[Production Throughput] --> B[Kafka: 100K-1M events/sec]
    B --> C[Spark: 50K-500K events/sec]
    C --> D[MinIO: 10K-100K sessions/batch]
    C --> E[ClickHouse: 10K-100K rows/sec]
    
    B --> F[Kafka Cluster: 3-5 brokers]
    C --> G[Spark Cluster: 10-50 workers]
    D --> H[MinIO Cluster: 3-5 nodes]
    E --> I[ClickHouse Cluster: 3-5 shards]
    
    style A fill:#e1f5ff
    style B fill:#fff4e1
    style C fill:#e8f5e9
    style D fill:#f3e5f5
    style E fill:#e0f2f1
```

### Performance Benchmarks

#### Throughput Metrics

| Component | Development | Production (Single) | Production (Clustered) |
|-----------|-------------|---------------------|------------------------|
| **Kafka Producer** | 10 events/sec | 10K events/sec | 100K-1M events/sec |
| **Kafka Topic** | 10 events/sec | 10K events/sec | 100K-1M events/sec |
| **Spark Streaming** | 10 events/sec | 5K events/sec | 50K-500K events/sec |
| **Session Aggregation** | 7 sessions/batch | 1K sessions/batch | 10K-100K sessions/batch |
| **MinIO Write** | 7 sessions/batch | 1K sessions/batch | 10K-100K sessions/batch |
| **ClickHouse Write** | 7 rows/batch | 1K rows/batch | 10K-100K rows/sec |
| **ClickHouse Query** | < 100ms | < 500ms | < 2s (complex queries) |

#### Latency Metrics

| Stage | Development | Production (P50) | Production (P99) |
|-------|-------------|-------------------|------------------|
| **Producer → Kafka** | < 10ms | < 5ms | < 20ms |
| **Kafka → Spark** | 10s (batch) | 10s (batch) | 10s (batch) |
| **Spark Processing** | 1-2s | 2-5s | 10-30s |
| **MinIO Write** | 500ms-1s | 1-3s | 5-10s |
| **ClickHouse Write** | 200-500ms | 500ms-2s | 3-8s |
| **Total End-to-End** | ~12-13s | ~15-20s | ~30-50s |

#### Data Volume Benchmarks

| Metric | Development | Production (Daily) | Production (Monthly) |
|--------|-------------|-------------------|----------------------|
| **Events Processed** | ~864K/day | 1B-10B/day | 30B-300B/month |
| **Sessions Created** | ~60K/day | 10M-100M/day | 300M-3B/month |
| **Data Stored (MinIO)** | ~100MB/day | 10-100GB/day | 300GB-3TB/month |
| **Data Stored (ClickHouse)** | ~50MB/day | 5-50GB/day | 150GB-1.5TB/month |
| **Query Volume** | < 100/day | 10K-100K/day | 300K-3M/month |

#### Resource Utilization

| Component | CPU | Memory | Storage | Network |
|-----------|-----|--------|---------|---------|
| **Kafka Producer** | 5-10% | 100-200MB | - | 1-10 Mbps |
| **Kafka Broker** | 10-20% | 1-2GB | 10-100GB | 10-100 Mbps |
| **Spark Streaming** | 20-40% | 2-4GB | - | 10-100 Mbps |
| **MinIO** | 5-15% | 500MB-1GB | 100GB-10TB | 10-100 Mbps |
| **ClickHouse** | 15-30% | 2-8GB | 50GB-5TB | 10-100 Mbps |

#### Production Scale (Terabyte-Level)

**Configuration:**
- **Kafka Cluster**: 3-5 brokers, 10-20 partitions per topic
- **Spark Cluster**: 10-50 workers, 4-8 cores, 8-16GB RAM per worker
- **MinIO Cluster**: 3-5 nodes, distributed mode
- **ClickHouse Cluster**: 3-5 shards, 2-3 replicas

**Performance Targets:**
- **Throughput**: 100K-1M events/second
- **Latency**: < 20s end-to-end (P99)
- **Storage**: 1-10TB per month
- **Query Performance**: < 2s for complex analytics queries
- **Availability**: 99.9% uptime

#### Cost Optimization

| Strategy | Impact | Cost Savings |
|----------|--------|--------------|
| **Data Retention** | Reduce storage by 50% | 50% storage cost |
| **Compression** | Reduce storage by 70% | 70% storage cost |
| **Partitioning** | Improve query performance | 30% compute cost |
| **Caching** | Reduce query load | 40% compute cost |
| **Archival** | Move old data to cold storage | 80% storage cost |

### Latency Breakdown

#### End-to-End Latency Analysis

```mermaid
graph LR
    A[Event Generated] -->|0-5ms| B[Kafka Producer]
    B -->|5-10ms| C[Kafka Topic]
    C -->|10s| D[Spark Reads Batch]
    D -->|2-5s| E[Spark Processing]
    E -->|1-3s| F[MinIO Write]
    E -->|0.5-2s| G[ClickHouse Write]
    F -->|Total: 13-20s| H[Data Available]
    G -->|Total: 13-20s| H
    
    style A fill:#e1f5ff
    style C fill:#fff4e1
    style E fill:#e8f5e9
    style H fill:#e0f2f1
```

#### Latency Optimization

1. **Kafka Optimization**
   - Batch size: 32KB-64KB
   - Compression: Snappy (balanced)
   - Producer acks: `all` (reliability)

2. **Spark Optimization**
   - Batch interval: 10s (balance latency vs throughput)
   - Parallelism: Match partition count
   - Checkpointing: Every 5 batches

3. **Storage Optimization**
   - MinIO: Parallel writes
   - ClickHouse: Batch inserts (1000+ rows)
   - Connection pooling

### Scalability Metrics

#### Horizontal Scaling Impact

| Component | Scale Factor | Throughput Increase | Latency Impact |
|-----------|--------------|---------------------|----------------|
| **Kafka Brokers** | 1 → 3 | 2.5x | Minimal |
| **Spark Workers** | 1 → 10 | 8x | Minimal |
| **MinIO Nodes** | 1 → 3 | 2.5x | Minimal |
| **ClickHouse Shards** | 1 → 3 | 2.5x | Minimal |

#### Vertical Scaling Impact

| Component | Resource Increase | Throughput Increase | Cost Impact |
|-----------|-------------------|---------------------|-------------|
| **CPU** | 2x → 4x cores | 1.8x | 2x cost |
| **Memory** | 4GB → 8GB | 1.2x | 2x cost |
| **Storage** | 100GB → 500GB | N/A | 5x cost |

### Performance Monitoring

#### Key Performance Indicators (KPIs)

1. **Throughput KPIs**
   - Events processed per second
   - Sessions created per batch
   - Data written per second
   - Query throughput

2. **Latency KPIs**
   - End-to-end latency (P50, P95, P99)
   - Processing latency
   - Query latency
   - Storage write latency

3. **Resource KPIs**
   - CPU utilization
   - Memory utilization
   - Storage utilization
   - Network bandwidth

4. **Quality KPIs**
   - Error rate
   - Data quality score
   - Availability
   - Recovery time

### Benchmarking Methodology

#### Load Testing

1. **Baseline Test**
   - Single producer, single consumer
   - Measure baseline performance

2. **Scale Test**
   - Multiple producers, multiple consumers
   - Measure scaling impact

3. **Stress Test**
   - Peak load simulation
   - Measure system limits

4. **Endurance Test**
   - Long-running test (24+ hours)
   - Measure stability and memory leaks

#### Performance Tuning

1. **Identify Bottlenecks**
   - Monitor resource utilization
   - Analyze latency breakdown
   - Profile code execution

2. **Optimize Components**
   - Tune Kafka configurations
   - Optimize Spark jobs
   - Optimize storage writes

3. **Validate Improvements**
   - Re-run benchmarks
   - Compare before/after metrics
   - Document improvements

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

