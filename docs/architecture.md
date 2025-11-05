# System Architecture

## Overview

This document describes the architecture of the real-time analytics pipeline for Media Publishing's digital media properties.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Data Generation Layer"
        A[Kafka Producer<br/>Python]
        A1[User Session<br/>Simulator]
    end
    
    subgraph "Streaming Layer"
        B[Kafka Broker<br/>Topic: web_clicks]
        B1[Zookeeper<br/>Coordination]
    end
    
    subgraph "Processing Layer"
        C[Spark Streaming<br/>PySpark]
        C1[Session Aggregation]
        C2[Data Transformation]
    end
    
    subgraph "Storage Layer"
        D[MinIO<br/>S3-Compatible<br/>Data Lake]
        D1[Delta Lake<br/>ACID Transactions]
        E[ClickHouse<br/>OLAP Database]
        E1[Analytics Tables]
    end
    
    subgraph "Analytics Layer"
        F[Business Intelligence]
        G[ML Features]
        H[Real-time Dashboards]
    end
    
    A --> A1
    A1 -->|Snappy Compression| B
    B1 --> B
    B -->|Stream Events| C
    C --> C1
    C1 --> C2
    C2 -->|Delta Format| D
    C2 -->|Analytics| E
    D --> D1
    E --> E1
    D1 --> F
    E1 --> G
    E1 --> H
    
    style A fill:#e1f5ff
    style B fill:#fff4e1
    style C fill:#e8f5e9
    style D fill:#f3e5f5
    style E fill:#e0f2f1
```

## Component Architecture

### Kafka Producer

```mermaid
graph LR
    A[User Session] -->|Generate Events| B[Event Schema]
    B -->|Serialize JSON| C[Kafka Producer]
    C -->|Snappy Compression| D[Kafka Topic]
    
    subgraph "Event Types"
        E[article_view]
        F[article_click]
        G[video_play]
        H[newsletter_signup]
        I[subscription_prompt]
        J[ad_click]
        K[search]
        L[navigation]
    end
    
    B --> E
    B --> F
    B --> G
    B --> H
    B --> I
    B --> J
    B --> K
    B --> L
    
    style C fill:#e1f5ff
    style D fill:#fff4e1
```

### Spark Streaming Pipeline

```mermaid
graph TB
    A[Kafka Source] -->|Read Stream| B[Parse JSON]
    B -->|Schema Validation| C[Filter Events]
    C -->|Group by Session| D[Aggregate Metrics]
    D -->|Transform Data| E[Enrich with Metadata]
    E -->|Write Batch| F[MinIO Delta Lake]
    E -->|Write Batch| G[ClickHouse Analytics]
    
    subgraph "Aggregations"
        D1[Session Duration]
        D2[Event Counts]
        D3[Page Views]
        D4[Engagement Metrics]
    end
    
    D --> D1
    D --> D2
    D --> D3
    D --> D4
    
    style A fill:#fff4e1
    style E fill:#e8f5e9
    style F fill:#f3e5f5
    style G fill:#e0f2f1
```

### Data Storage Architecture

```mermaid
graph TB
    subgraph "MinIO - Data Lake"
        A[Delta Lake Format]
        A1[Batch Partitions]
        A2[ACID Transactions]
        A3[Time Travel]
    end
    
    subgraph "ClickHouse - Analytics"
        B[session_metrics]
        B1[brand_analytics]
        B2[device_analytics]
        B3[geographic_analytics]
        B4[subscription_analytics]
        B5[page_analytics]
    end
    
    A --> A1
    A1 --> A2
    A2 --> A3
    
    B --> B1
    B --> B2
    B --> B3
    B --> B4
    B --> B5
    
    style A fill:#f3e5f5
    style B fill:#e0f2f1
```

## Data Flow Architecture

### Event Processing Flow

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant K as Kafka
    participant S as Spark
    participant M as MinIO
    participant C as ClickHouse
    
    P->>K: Publish Event (Snappy)
    K->>S: Stream Batch
    S->>S: Parse & Validate
    S->>S: Aggregate by Session
    S->>S: Transform & Enrich
    S->>M: Write Delta Lake
    S->>C: Write Analytics
    S->>S: Commit Offset
    Note over S: Next Batch (10s)
```

### Session Lifecycle

```mermaid
stateDiagram-v2
    [*] --> SessionStart
    SessionStart --> EventGeneration
    EventGeneration --> EventProcessing
    EventProcessing --> EventGeneration : More Events
    EventProcessing --> SessionEnd : Timeout/Close
    SessionEnd --> Aggregation
    Aggregation --> Storage
    Storage --> [*]
    
    note right of SessionStart
        Generate Session ID
        Initialize Metadata
    end note
    
    note right of EventProcessing
        Track Engagement
        Record Timestamps
    end note
    
    note right of Aggregation
        Calculate Metrics
        Enrich Data
    end note
```

## Scalability Architecture

```mermaid
graph TB
    subgraph "Horizontal Scaling"
        A[Multiple Producers] -->|Load Balance| B[Kafka Cluster]
        B -->|Partition| C[Multiple Spark Jobs]
        C -->|Parallel Write| D[MinIO Cluster]
        C -->|Parallel Write| E[ClickHouse Cluster]
    end
    
    subgraph "Data Partitioning"
        F[By Brand]
        G[By Date]
        H[By Session ID]
    end
    
    B --> F
    B --> G
    B --> H
    
    style B fill:#fff4e1
    style C fill:#e8f5e9
```

## Failure Handling

```mermaid
graph TB
    A[Event Processing] -->|Success| B[Write to Storage]
    A -->|Failure| C[Retry Logic]
    C -->|Retry Success| B
    C -->|Retry Failed| D[Dead Letter Queue]
    D -->|Manual Review| E[Error Handling]
    
    B -->|Checkpoint| F[Offset Management]
    F -->|Failure| G[Recovery from Checkpoint]
    G --> A
    
    style C fill:#ffebee
    style D fill:#fff3e0
    style F fill:#e8f5e9
```

## Security Architecture

```mermaid
graph TB
    A[Producer] -->|Encrypted| B[Kafka]
    B -->|TLS| C[Spark Streaming]
    C -->|IAM| D[MinIO]
    C -->|Authentication| E[ClickHouse]
    
    subgraph "Security Layers"
        F[Network Security]
        G[Data Encryption]
        H[Access Control]
    end
    
    A --> F
    B --> G
    C --> H
    
    style F fill:#e3f2fd
    style G fill:#f3e5f5
    style H fill:#e8f5e9
```

