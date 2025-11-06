# Data Governance

## Overview

This document outlines the data governance framework for the Media Publishing real-time analytics pipeline, ensuring compliance, privacy, security, and data quality at scale.

## Governance Framework

```mermaid
graph TB
    A[Data Governance] --> B[Privacy & Compliance]
    A --> C[Data Quality]
    A --> D[Access Control]
    A --> E[Data Lifecycle]
    A --> F[Audit & Monitoring]
    
    B --> B1[GDPR Compliance]
    B --> B2[Data Retention]
    B --> B3[User Consent]
    
    C --> C1[Data Validation]
    C --> C2[Quality Checks]
    C --> C3[Data Lineage]
    
    D --> D1[Role-Based Access]
    D --> D2[Data Classification]
    D --> D3[Encryption]
    
    E --> E1[Data Retention]
    E --> E2[Data Archival]
    E --> E3[Data Deletion]
    
    F --> F1[Audit Logs]
    F --> F2[Access Logs]
    F --> F3[Compliance Reports]
    
    style A fill:#e1f5ff
    style B fill:#ffebee
    style C fill:#e8f5e9
    style D fill:#fff3e0
    style E fill:#f3e5f5
    style F fill:#e0f2f1
```

## Privacy & Compliance

### GDPR Compliance

```mermaid
sequenceDiagram
    participant User
    participant Producer
    participant Kafka
    participant Spark
    participant Storage
    participant Compliance
    
    User->>Producer: User Event (with consent)
    Producer->>Producer: Validate Consent
    Producer->>Kafka: Publish Event
    Kafka->>Spark: Process Event
    Spark->>Storage: Store Data
    Storage->>Compliance: Log Data Access
    User->>Compliance: Right to Access
    Compliance->>Storage: Retrieve User Data
    Compliance->>User: Return Data
    User->>Compliance: Right to Deletion
    Compliance->>Storage: Delete User Data
    Storage->>Compliance: Confirm Deletion
```

#### Key Principles

1. **Lawful Basis for Processing**
   - User consent for analytics
   - Legitimate interest for service improvement
   - Contractual necessity for subscription services

2. **Data Minimization**
   - Collect only necessary data
   - Anonymize where possible
   - Aggregate for analytics

3. **Purpose Limitation**
   - Data used only for stated purposes
   - No secondary use without consent
   - Clear documentation of use cases

4. **Storage Limitation**
   - Data retention policies enforced
   - Automatic deletion after retention period
   - Archival for compliance requirements

### Data Retention Policies

```mermaid
graph TB
    A[Data Type] --> B[Retention Period]
    
    B --> C[Raw Events: 90 days]
    B --> D[Session Metrics: 2 years]
    B --> E[ML Features: 1 year]
    B --> F[Model Artifacts: 5 years]
    B --> G[Audit Logs: 7 years]
    
    C --> H[Auto-Delete]
    D --> I[Archive After 1 Year]
    E --> J[Archive After 6 Months]
    F --> K[Long-term Storage]
    G --> L[Compliance Archive]
    
    style C fill:#ffebee
    style D fill:#fff3e0
    style E fill:#e8f5e9
    style F fill:#e1f5ff
    style G fill:#f3e5f5
```

#### Retention Schedule

| Data Type | Retention Period | Archival | Deletion |
|-----------|----------------|----------|----------|
| Raw Kafka Events | 90 days | No | Automatic |
| Session Metrics (ClickHouse) | 2 years | After 1 year | After 2 years |
| ML Features | 1 year | After 6 months | After 1 year |
| Model Artifacts | 5 years | After 2 years | After 5 years |
| Audit Logs | 7 years | After 1 year | After 7 years |
| Delta Lake Data | 2 years | After 1 year | After 2 years |

### User Rights (GDPR)

#### Right to Access

```sql
-- Query user data from ClickHouse
SELECT *
FROM session_metrics
WHERE user_id = ?
ORDER BY session_start DESC
LIMIT 100;
```

#### Right to Deletion

```python
# Pseudo-code for data deletion
def delete_user_data(user_id: int):
    # 1. Delete from ClickHouse
    clickhouse.execute(f"DELETE FROM session_metrics WHERE user_id = {user_id}")
    
    # 2. Delete from MinIO (Delta Lake)
    spark.sql(f"DELETE FROM delta.`s3a://session-metrics/sessions/` WHERE user_id = {user_id}")
    
    # 3. Delete from Kafka (if still in retention)
    # Kafka retention handles this automatically
    
    # 4. Log deletion
    audit_log.log_deletion(user_id, datetime.now())
```

#### Right to Rectification

- Data correction procedures
- Update mechanisms for user data
- Validation of corrected data

#### Right to Portability

- Export user data in machine-readable format (JSON, CSV)
- API endpoints for data export
- Secure data transfer

## Data Quality

### Quality Framework

```mermaid
graph TB
    A[Data Quality] --> B[Completeness]
    A --> C[Accuracy]
    A --> D[Consistency]
    A --> E[Timeliness]
    A --> F[Validity]
    
    B --> B1[Required Fields Present]
    B --> B2[No Missing Values]
    
    C --> C1[Data Validation]
    C --> C2[Business Rules]
    
    D --> D1[Cross-System Consistency]
    D --> D2[Referential Integrity]
    
    E --> E1[Data Freshness]
    E --> E2[Processing Latency]
    
    F --> V1[Schema Validation]
    V --> V2[Data Type Checks]
    
    style A fill:#e8f5e9
```

### Quality Checks

#### Automated Quality Checks

```python
# Daily data quality checks (from data-quality.yml workflow)
def check_data_quality():
    checks = {
        'freshness': check_data_freshness(),
        'completeness': check_data_completeness(),
        'accuracy': check_data_accuracy(),
        'volume': check_data_volume(),
        'anomalies': check_data_anomalies()
    }
    return checks
```

#### Quality Metrics

| Metric | Target | Alert Threshold |
|--------|--------|----------------|
| Data Freshness | < 1 hour | > 2 hours |
| Completeness | > 99% | < 95% |
| Accuracy | > 99.5% | < 99% |
| Processing Latency | < 15s | > 30s |
| Error Rate | < 0.1% | > 1% |

### Data Lineage

```mermaid
graph LR
    A[Kafka Events] --> B[Spark Streaming]
    B --> C[Session Aggregation]
    C --> D[MinIO Delta Lake]
    C --> E[ClickHouse Analytics]
    E --> F[ML Features]
    F --> G[ML Models]
    G --> H[Predictions]
    
    style A fill:#e1f5ff
    style D fill:#f3e5f5
    style E fill:#e0f2f1
    style G fill:#fff3e0
```

#### Lineage Tracking

- **Source**: Kafka topic `web_clicks`
- **Transformation**: Spark Streaming aggregation
- **Destination**: MinIO (Delta Lake) and ClickHouse
- **Derived Data**: ML features, model predictions
- **Metadata**: Timestamps, batch IDs, processing times

## Access Control

### Role-Based Access Control (RBAC)

```mermaid
graph TB
    A[User Roles] --> B[Data Engineer]
    A --> C[Data Scientist]
    A --> D[Analyst]
    A --> E[Admin]
    A --> F[Auditor]
    
    B --> B1[Read/Write: All Data]
    C --> C1[Read: Analytics + ML Features]
    D --> D2[Read: Analytics Only]
    E --> E1[Full Access]
    F --> F1[Read: Audit Logs Only]
    
    style B fill:#e8f5e9
    style C fill:#e1f5ff
    style D fill:#fff3e0
    style E fill:#ffebee
    style F fill:#f3e5f5
```

### Access Levels

| Role | MinIO | ClickHouse | ML Models | Audit Logs |
|------|-------|------------|-----------|------------|
| Data Engineer | Read/Write | Read/Write | Read/Write | Read |
| Data Scientist | Read | Read/Write | Read/Write | Read |
| Analyst | Read | Read | Read | - |
| Admin | Full | Full | Full | Full |
| Auditor | - | - | - | Read |

### Data Classification

```mermaid
graph LR
    A[Data Classification] --> B[Public]
    A --> C[Internal]
    A --> D[Confidential]
    A --> E[Restricted]
    
    B --> B1[No Restrictions]
    C --> C1[Internal Use Only]
    D --> D2[Encrypted, Limited Access]
    E --> E3[Highest Security, Audit Required]
    
    style B fill:#e8f5e9
    style C fill:#e1f5ff
    style D fill:#fff3e0
    style E fill:#ffebee
```

#### Classification Levels

- **Public**: Aggregated, anonymized statistics
- **Internal**: Session metrics, analytics (de-identified)
- **Confidential**: User-level data, ML features
- **Restricted**: Personal identifiers, sensitive attributes

## Data Lifecycle Management

### Lifecycle Stages

```mermaid
graph LR
    A[Ingestion] --> B[Processing]
    B --> C[Storage]
    C --> D[Analytics]
    D --> E[Archival]
    E --> F[Deletion]
    
    C --> G[Active: 0-1 Year]
    E --> H[Archive: 1-2 Years]
    F --> I[Deleted: After Retention]
    
    style A fill:#e1f5ff
    style C fill:#e8f5e9
    style E fill:#fff3e0
    style F fill:#ffebee
```

### Lifecycle Policies

#### Active Data (0-1 Year)
- **Location**: ClickHouse (hot storage)
- **Access**: Fast queries, real-time analytics
- **Cost**: Higher storage cost
- **Use Case**: Current analytics, ML training

#### Archived Data (1-2 Years)
- **Location**: MinIO (cold storage)
- **Access**: Slower queries, batch processing
- **Cost**: Lower storage cost
- **Use Case**: Historical analysis, compliance

#### Deleted Data (After Retention)
- **Process**: Automated deletion
- **Verification**: Audit log confirmation
- **Backup**: Compliance archives (if required)

## Audit & Monitoring

### Audit Logging

```mermaid
graph TB
    A[Audit Events] --> B[Data Access]
    A --> C[Data Modification]
    A --> D[Data Deletion]
    A --> E[Configuration Changes]
    A --> F[Security Events]
    
    B --> G[Who: User ID]
    B --> H[What: Data Accessed]
    B --> I[When: Timestamp]
    B --> J[Where: Source IP]
    
    style A fill:#e0f2f1
```

### Audit Log Schema

```sql
CREATE TABLE audit_logs (
    log_id String,
    timestamp DateTime,
    user_id String,
    action String,  -- 'READ', 'WRITE', 'DELETE', 'EXPORT'
    resource_type String,  -- 'session_metrics', 'ml_features', etc.
    resource_id String,
    ip_address String,
    user_agent String,
    success Boolean,
    error_message String
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id);
```

### Monitoring & Alerts

#### Key Metrics

- **Access Patterns**: Unusual access patterns
- **Data Volume**: Unexpected data volume changes
- **Error Rates**: Increased error rates
- **Compliance**: Retention policy violations
- **Security**: Failed authentication attempts

#### Alert Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| Failed Logins | > 5/hour | > 20/hour |
| Unusual Access | > 3 std dev | > 5 std dev |
| Data Volume Drop | > 20% | > 50% |
| Retention Violation | Any | Multiple |

## Compliance Reporting

### Regular Reports

1. **Monthly Compliance Report**
   - Data retention compliance
   - Access control audit
   - Data quality metrics
   - Security incidents

2. **Quarterly Privacy Report**
   - User data requests (access, deletion)
   - Consent management
   - Data breach incidents (if any)
   - Privacy impact assessments

3. **Annual Audit Report**
   - Full system audit
   - Compliance certification
   - Policy review and updates
   - Training and awareness

### Compliance Checklist

- [ ] GDPR compliance verified
- [ ] Data retention policies enforced
- [ ] Access controls implemented
- [ ] Audit logging active
- [ ] Data quality checks automated
- [ ] Privacy policies documented
- [ ] User rights procedures defined
- [ ] Security measures in place
- [ ] Regular compliance reviews scheduled
- [ ] Staff training completed

## Implementation

### Data Governance Tools

1. **Data Catalog**: Track data assets, lineage, metadata
2. **Access Management**: RBAC implementation
3. **Audit Logging**: Comprehensive audit trail
4. **Data Quality**: Automated quality checks
5. **Compliance Monitoring**: Automated compliance checks

### Best Practices

1. **Privacy by Design**: Build privacy into architecture
2. **Data Minimization**: Collect only necessary data
3. **Transparency**: Clear documentation of data usage
4. **Accountability**: Regular audits and reviews
5. **Continuous Improvement**: Regular policy updates

## References

- [GDPR Compliance Guide](https://gdpr.eu/)
- [Data Governance Framework](https://www.dama.org/)
- [Privacy by Design Principles](https://www.ipc.on.ca/privacy/privacy-by-design/)

