# Kafka Producers and Consumers - OwnLens

## Overview

Kafka producers and consumers for event streaming across all OwnLens domains.

## Structure

```
kafka/
├── __init__.py
├── base/
│   ├── __init__.py
│   ├── producer.py          # Base Kafka producer
│   └── consumer.py          # Base Kafka consumer
│
├── producers/                # Event producers
│   ├── __init__.py
│   ├── customer/
│   │   ├── user_event_producer.py
│   │   └── session_producer.py
│   ├── editorial/
│   │   ├── content_event_producer.py
│   │   └── article_producer.py
│   ├── company/
│   │   └── internal_content_producer.py
│   ├── security/
│   │   └── security_event_producer.py
│   ├── compliance/
│   │   └── compliance_event_producer.py
│   ├── audit/
│   │   └── audit_log_producer.py
│   ├── data_quality/
│   │   └── quality_metric_producer.py
│   └── ml_models/
│       └── model_prediction_producer.py
│
└── consumers/                # Event consumers
    ├── __init__.py
    ├── customer/
    │   ├── user_event_consumer.py
    │   └── session_consumer.py
    ├── editorial/
    │   ├── content_event_consumer.py
    │   └── article_consumer.py
    ├── company/
    │   └── internal_content_consumer.py
    ├── security/
    │   └── security_event_consumer.py
    ├── compliance/
    │   └── compliance_event_consumer.py
    ├── audit/
    │   └── audit_log_consumer.py
    ├── data_quality/
    │   └── quality_metric_consumer.py
    └── ml_models/
        └── model_prediction_consumer.py
```

## Purpose

### Producers
- **Purpose**: Publish events to Kafka topics
- **Use Cases**: 
  - User events from web applications
  - Content events from editorial systems
  - Security events from authentication systems
  - Compliance events from data privacy systems
  - Audit logs from all systems
  - Quality metrics from data quality checks
  - Model predictions from ML models

### Consumers
- **Purpose**: Consume events from Kafka topics
- **Use Cases**:
  - Real-time event processing
  - Event validation
  - Event routing
  - Event enrichment
  - Event aggregation

## Integration with ETL

- **ETL Pipelines**: Use Kafka as a source/destination via `KafkaExtractor` and `KafkaLoader`
- **Kafka Producers/Consumers**: Dedicated components for publishing/consuming events
- **Workflow**: Producers → Kafka → Consumers → ETL Pipelines → Storage

## Features

- ✅ **Pydantic Validation**: All events validated against Pydantic models
- ✅ **Error Handling**: Comprehensive error handling and retry logic
- ✅ **Compression**: Snappy compression for efficient message transfer
- ✅ **Idempotence**: Idempotent producers for exactly-once semantics
- ✅ **Schema Registry**: Schema validation and versioning
- ✅ **Monitoring**: Metrics and logging for observability

