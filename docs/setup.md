# Setup Guide

## Prerequisites

```mermaid
graph TB
    A[System Requirements] --> B[Docker & Docker Compose]
    A --> C[Python 3.8+]
    A --> D[Java 11+]
    A --> E[8GB+ RAM]
    A --> F[Internet Connection]
    
    B --> G[Install Docker]
    C --> H[Install Python]
    D --> I[Install Java]
    
    style A fill:#e1f5ff
    style B fill:#e8f5e9
    style C fill:#e8f5e9
    style D fill:#e8f5e9
```

## Installation Steps

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd backend/kafka_check
```

### Step 2: Install Python Dependencies

```mermaid
flowchart LR
    A[requirements.txt] --> B[pip install]
    B --> C[kafka-python]
    B --> D[pyspark]
    B --> E[clickhouse-driver]
    B --> F[minio]
    B --> G[python-snappy]
    
    style A fill:#e1f5ff
    style B fill:#e8f5e9
```

```bash
pip install -r requirements.txt
```

### Step 3: Start Infrastructure

```mermaid
sequenceDiagram
    participant User
    participant Docker
    participant ZK as Zookeeper
    participant K as Kafka
    participant M as MinIO
    participant C as ClickHouse
    
    User->>Docker: docker-compose up -d
    Docker->>ZK: Start Zookeeper
    ZK->>ZK: Initialize (healthy)
    Docker->>K: Start Kafka
    K->>ZK: Connect to Zookeeper
    K->>K: Initialize (healthy)
    Docker->>M: Start MinIO
    M->>M: Initialize (healthy)
    Docker->>C: Start ClickHouse
    C->>C: Initialize
    Docker->>User: All services running
```

```bash
docker-compose up -d
```

### Step 4: Initialize ClickHouse

```mermaid
flowchart TD
    A[ClickHouse Container] --> B[Load init.sql]
    B --> C[Create analytics database]
    C --> D[Create session_metrics table]
    D --> E[Create materialized views]
    E --> F[brand_analytics]
    E --> G[device_analytics]
    E --> H[geographic_analytics]
    E --> I[subscription_analytics]
    E --> J[page_analytics]
    
    style A fill:#e0f2f1
    style D fill:#e8f5e9
```

```bash
docker exec -i clickhouse clickhouse-client --password clickhouse < clickhouse/init.sql
```

### Step 5: Create MinIO Bucket

```mermaid
flowchart LR
    A[create_minio_bucket.py] --> B[Connect to MinIO]
    B --> C[Create session-metrics bucket]
    C --> D[Verify Bucket]
    
    style A fill:#f3e5f5
    style C fill:#e8f5e9
```

```bash
python create_minio_bucket.py
```

### Step 6: Verify Setup

```mermaid
graph TB
    A[Verify Services] --> B[Check Docker Containers]
    A --> C[Check Kafka Topic]
    A --> D[Check ClickHouse Tables]
    A --> E[Check MinIO Bucket]
    
    B --> F[docker-compose ps]
    C --> G[kafka-topics --list]
    D --> H[SHOW TABLES]
    E --> I[List Buckets]
    
    style A fill:#e1f5ff
```

```bash
# Check containers
docker-compose ps

# Check Kafka topic
docker exec -it kafka_check-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Check ClickHouse tables
docker exec -it clickhouse clickhouse-client --password clickhouse --query "SHOW TABLES FROM analytics"
```

## Running the Pipeline

### Start Kafka Producer

```mermaid
flowchart TD
    A[kafka_producer.py] --> B[Check Kafka Connection]
    B --> C{Connected?}
    C -->|No| D[Error Message]
    C -->|Yes| E[Initialize Producer]
    E --> F[Start Session Loop]
    F --> G[Generate Events]
    G --> H[Send to Kafka]
    H --> I[Session End]
    I --> F
    
    style A fill:#e1f5ff
    style E fill:#e8f5e9
```

```bash
python kafka_producer.py
```

### Start Spark Streaming

```mermaid
flowchart TD
    A[spark_streaming.py] --> B[Create SparkSession]
    B --> C[Load JARs]
    C --> D[Read from Kafka]
    D --> E[Parse Events]
    E --> F[Aggregate Sessions]
    F --> G[Write to MinIO]
    F --> H[Write to ClickHouse]
    G --> I[Wait 10s]
    H --> I
    I --> D
    
    style A fill:#e8f5e9
    style F fill:#f3e5f5
```

```bash
python spark_streaming.py
```

## Verification

### Check Data in MinIO

```bash
python check_minio_data.py
```

### Check Data in ClickHouse

```bash
python check_clickhouse_data.py
```

## Troubleshooting

```mermaid
graph TB
    A[Issue] --> B{Type?}
    
    B -->|Connection Error| C[Check Docker Status]
    B -->|Schema Error| D[Check ClickHouse Schema]
    B -->|Data Loss| E[Check Kafka Offsets]
    B -->|Storage Error| F[Check MinIO Bucket]
    
    C --> G[docker-compose ps]
    D --> H[Verify init.sql]
    E --> I[Clear Checkpoints]
    F --> J[Verify Bucket Exists]
    
    style A fill:#ffebee
    style C fill:#fff3e0
    style D fill:#fff3e0
    style E fill:#fff3e0
    style F fill:#fff3e0
```

### Common Issues

1. **Kafka Connection Failed**
   - Ensure Docker containers are running
   - Check port 9092 is available

2. **ClickHouse Authentication Error**
   - Verify password in docker-compose.yml
   - Restart ClickHouse container

3. **MinIO Bucket Not Found**
   - Run `python create_minio_bucket.py`
   - Check MinIO console at http://localhost:9001

4. **Spark Checkpoint Error**
   - Clear checkpoint: `rm -rf /tmp/spark-checkpoints`
   - Set `failOnDataLoss: false` in spark_streaming.py

## Next Steps

After setup is complete, refer to:
- [Architecture Documentation](./architecture.md)
- [Data Flow Documentation](./data-flow.md)
- [Component Details](./components.md)

