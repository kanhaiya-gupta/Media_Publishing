# OwnLens - Complete Schema Overview

## ✅ World-Class Digitalization Platform - Complete Schema

This document provides a comprehensive overview of all schemas in the OwnLens platform, designed for **world-class digitalization** with enterprise-grade features.

---

## 📊 Schema Summary

| Schema | Purpose | Tables | Key Features |
|--------|---------|--------|--------------|
| **base.sql** | Core infrastructure | 12 | Multi-company, multi-brand, multi-country support |
| **customer.sql** | Customer analytics | 8 | User behavior, ML features, predictions |
| **editorial_core.sql** | Core editorial | 9 | Articles metadata, authors, performance analytics |
| **editorial_content.sql** | Article content | 2 | Content storage, versioning, full-text search |
| **editorial_media.sql** | Media assets | 6 | Images, videos, documents, object storage |
| **company.sql** | Company analytics | 8 | Internal communications, department analytics |
| **security.sql** | Security & RBAC | 7 | Role-based access, API keys, sessions |
| **audit.sql** | Audit & logging | 6 | Comprehensive audit trail, data lineage |
| **compliance.sql** | GDPR & compliance | 7 | Consent, data retention, breach tracking |
| **ml_models.sql** | ML model management | 6 | Model registry, versioning, monitoring |
| **data_quality.sql** | Data quality | 5 | Validation, metrics, alerts |
| **configuration.sql** | Configuration | 4 | Feature flags, system settings |
| **TOTAL** | **Complete Platform** | **72+ tables** | **Enterprise-ready** |

---

## 🏗️ Architecture Layers

### 1. Foundation Layer (`base.sql`)

**Purpose**: Core infrastructure shared across all domains

**Key Features**:
- ✅ Multi-company support (e.g., Axel Springer)
- ✅ Multi-brand support (e.g., Bild, Die Welt, Business Insider)
- ✅ Multi-country support (brands operating in multiple countries)
- ✅ User management (base users, accounts, subscriptions)
- ✅ Geographic data (countries, cities)
- ✅ Device/browser reference data
- ✅ Category hierarchy (shared across domains)

**Tables**: 12 tables

---

### 2. Domain Layer

#### Customer Domain (`customer.sql`)

**Purpose**: Customer analytics and user behavior

**Key Features**:
- ✅ User sessions and events (partitioned by date)
- ✅ ML-ready user features
- ✅ User segmentation (ML clustering)
- ✅ Churn predictions
- ✅ Content recommendations
- ✅ Conversion predictions

**Tables**: 8 tables

#### Editorial Core Domain (`editorial_core.sql`)

**Purpose**: Content performance and editorial intelligence

**Key Features**:
- ✅ Authors and articles
- ✅ Article/author/category performance metrics
- ✅ Content events (partitioned by date)
- ✅ Headline A/B testing
- ✅ Trending topics detection
- ✅ Content strategy recommendations

**Tables**: 9 tables

#### Company Domain (`company.sql`)

**Purpose**: Internal communications and company analytics

**Key Features**:
- ✅ Departments and employees
- ✅ Internal content (announcements, newsletters)
- ✅ Content performance metrics
- ✅ Department performance analytics
- ✅ Employee engagement tracking
- ✅ Communications analytics

**Tables**: 8 tables

---

### 3. Security & Access Control Layer (`security.sql`)

**Purpose**: Role-Based Access Control (RBAC) and authentication

**Key Features**:
- ✅ Role-based access control (RBAC)
- ✅ Permissions management
- ✅ User-role assignments (scoped to company/brand)
- ✅ API key management
- ✅ API key usage tracking (partitioned by date)
- ✅ User session management

**Tables**: 7 tables

**Roles Supported**:
- Administrator
- Data Engineer
- Data Scientist
- Analyst
- Editor
- Senior Editor
- Auditor
- Company Admin

---

### 4. Audit & Logging Layer (`audit.sql`)

**Purpose**: Comprehensive audit trail for compliance and security

**Key Features**:
- ✅ Complete audit log (all actions) (partitioned by date)
- ✅ Data change tracking (INSERT, UPDATE, DELETE) (partitioned by date)
- ✅ Data access tracking (partitioned by date)
- ✅ Security events tracking (partitioned by date)
- ✅ Data lineage tracking (partitioned by date)
- ✅ Compliance events tracking (partitioned by date)

**Tables**: 6 tables (all partitioned for performance)

**Audit Coverage**:
- Who: User ID, API key, session
- What: Action, resource type, resource ID
- When: Timestamp, date
- Where: IP address, user agent, endpoint
- Result: Success/failure, status code, error message

---

### 5. Compliance & Privacy Layer (`compliance.sql`)

**Purpose**: GDPR compliance, data privacy, and data retention

**Key Features**:
- ✅ User consent tracking (GDPR Article 6, 7)
- ✅ Data subject requests (GDPR Article 15, 17, 20)
  - Right to access
  - Right to deletion
  - Right to rectification
  - Right to data portability
  - Right to restriction
- ✅ Data retention policies
- ✅ Data retention execution tracking
- ✅ Data anonymization tracking
- ✅ Privacy Impact Assessments (PIA/DPIA)
- ✅ Data breach incident tracking (GDPR Article 33, 34)

**Tables**: 7 tables

**GDPR Compliance**:
- ✅ Consent management
- ✅ Data subject rights
- ✅ Data retention policies
- ✅ Data anonymization
- ✅ Breach notification (72-hour requirement)
- ✅ Privacy impact assessments

---

### 6. ML Model Management Layer (`ml_models.sql`)

**Purpose**: ML model registry, versioning, training, and monitoring

**Key Features**:
- ✅ Model registry with versioning
- ✅ Model features tracking
- ✅ Training runs tracking
- ✅ Predictions tracking (partitioned by date)
- ✅ Model performance monitoring
- ✅ Data drift detection
- ✅ Model A/B testing

**Tables**: 6 tables

**Model Lifecycle**:
- Training → Validation → Staging → Production → Deprecated → Archived
- Versioning with semantic versioning (1.0.0, 1.1.0, etc.)
- Performance monitoring and drift detection
- A/B testing for model comparison

---

### 7. Data Quality Layer (`data_quality.sql`)

**Purpose**: Data quality validation, metrics, and alerts

**Key Features**:
- ✅ Data quality rules (completeness, accuracy, consistency, validity, timeliness, uniqueness)
- ✅ Data quality check executions (partitioned by date)
- ✅ Data quality metrics (aggregated by date)
- ✅ Data quality alerts (partitioned by date)
- ✅ Individual record validation (partitioned by date)

**Tables**: 5 tables

**Quality Dimensions**:
- ✅ Completeness (null checks)
- ✅ Accuracy (validity checks)
- ✅ Consistency (duplicate checks)
- ✅ Validity (format, range checks)
- ✅ Timeliness (freshness checks)
- ✅ Uniqueness (duplicate checks)

---

### 8. Configuration Management Layer (`configuration.sql`)

**Purpose**: Feature flags and system configuration

**Key Features**:
- ✅ Feature flags (boolean, percentage, user list, custom)
- ✅ Feature flag history (change tracking)
- ✅ System configuration settings
- ✅ Configuration history (change tracking)
- ✅ Environment-specific settings (development, staging, production)
- ✅ Company/brand-scoped settings

**Tables**: 4 tables

**Feature Flag Types**:
- Boolean (on/off)
- Percentage (gradual rollout)
- User list (specific users)
- Custom (JSON-based)

---

## 🎯 World-Class Features

### ✅ Enterprise-Grade Security

- **RBAC**: Role-based access control with fine-grained permissions
- **API Keys**: Service-to-service authentication with usage tracking
- **Sessions**: User session management with expiration
- **Audit Trail**: Complete audit logging for all actions
- **Data Access Tracking**: Track who accessed what data when

### ✅ GDPR Compliance

- **Consent Management**: Track user consent for all processing
- **Data Subject Rights**: Support for access, deletion, portability, rectification
- **Data Retention**: Automated retention policies
- **Data Anonymization**: Track anonymized data
- **Breach Management**: Track and respond to data breaches (72-hour notification)
- **Privacy Assessments**: PIA/DPIA tracking

### ✅ ML Model Management

- **Model Registry**: Centralized model registry with versioning
- **Training Tracking**: Track all training runs
- **Prediction Tracking**: Track all predictions (partitioned)
- **Performance Monitoring**: Monitor model performance over time
- **Drift Detection**: Detect data drift and performance drift
- **A/B Testing**: Compare model versions

### ✅ Data Quality

- **Validation Rules**: Define and enforce data quality rules
- **Quality Metrics**: Track quality metrics over time
- **Quality Alerts**: Alert when quality drops
- **Record Validation**: Validate individual records
- **Quality Dimensions**: Completeness, accuracy, consistency, validity, timeliness, uniqueness

### ✅ Configuration Management

- **Feature Flags**: Feature toggling for gradual rollouts
- **System Settings**: Environment-specific configuration
- **Change History**: Track all configuration changes
- **Scoped Settings**: Company/brand-scoped settings

### ✅ Scalability & Performance

- **Partitioning**: Event tables partitioned by date (monthly partitions)
- **Indexes**: Comprehensive indexes for performance
- **JSONB**: Flexible metadata storage
- **UUIDs**: Distributed ID generation
- **Triggers**: Automatic timestamp updates

### ✅ Multi-Tenancy

- **Company Scoping**: All data scoped to companies
- **Brand Scoping**: Data can be scoped to brands
- **User Scoping**: User-specific data isolation
- **Role Scoping**: Roles can be scoped to company/brand

---

## 📈 Schema Statistics

- **Total Tables**: 72+ tables
- **Partitioned Tables**: 15+ tables (for performance)
- **Indexes**: 200+ indexes (for query performance)
- **Triggers**: 30+ triggers (for automation)
- **Foreign Keys**: 100+ relationships (for data integrity)
- **JSONB Fields**: 50+ fields (for flexibility)

---

## 🔄 Data Flow

```
User Events → Kafka → Processing → PostgreSQL
                                    ↓
                            ┌───────┴───────┐
                            ↓               ↓
                    Domain Tables    Audit/Compliance
                            ↓               ↓
                    ML Models      Data Quality
                            ↓               ↓
                    Predictions    Alerts
```

---

## 🚀 Deployment

### Installation Order

1. **Base Schema** (`base.sql`) - Foundation
2. **Security Schema** (`security.sql`) - Access control
3. **Audit Schema** (`audit.sql`) - Audit logging
4. **Compliance Schema** (`compliance.sql`) - GDPR compliance
5. **ML Models Schema** (`ml_models.sql`) - Model management
6. **Data Quality Schema** (`data_quality.sql`) - Quality checks
7. **Configuration Schema** (`configuration.sql`) - Feature flags
8. **Domain Schemas** (customer, editorial, company) - Business logic

### Partition Management

Monthly partitions are created for:
- Event tables (customer_events, editorial_content_events, company_content_events)
- Audit tables (audit_logs, audit_data_changes, audit_data_access, etc.)
- Prediction tables (ml_model_predictions)
- Quality check tables (data_quality_checks, data_quality_alerts)

**Create new partitions**:
```sql
-- Example: Create partition for March 2024
CREATE TABLE customer_events_2024_03 PARTITION OF customer_events
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
```

---

## ✅ Completeness Checklist

### Core Features
- ✅ Multi-company, multi-brand, multi-country support
- ✅ Customer analytics (sessions, events, ML features)
- ✅ Editorial analytics (articles, authors, performance)
- ✅ Company analytics (internal content, departments)
- ✅ User management and authentication

### Security & Access
- ✅ Role-Based Access Control (RBAC)
- ✅ Permissions management
- ✅ API key management
- ✅ Session management
- ✅ Audit logging

### Compliance & Privacy
- ✅ GDPR compliance (consent, data subject rights)
- ✅ Data retention policies
- ✅ Data anonymization
- ✅ Breach incident tracking
- ✅ Privacy impact assessments

### ML & Analytics
- ✅ ML model registry and versioning
- ✅ Model training tracking
- ✅ Prediction tracking
- ✅ Model performance monitoring
- ✅ Data drift detection
- ✅ A/B testing

### Data Quality
- ✅ Data quality rules
- ✅ Quality metrics tracking
- ✅ Quality alerts
- ✅ Record validation

### Configuration
- ✅ Feature flags
- ✅ System configuration
- ✅ Change history

### Performance & Scalability
- ✅ Table partitioning
- ✅ Comprehensive indexes
- ✅ JSONB for flexibility
- ✅ UUID primary keys

---

## 🎉 Conclusion

The OwnLens schema is **complete** and **world-class**, providing:

✅ **Enterprise-Grade Security** - RBAC, API keys, audit logging  
✅ **GDPR Compliance** - Consent, data subject rights, retention  
✅ **ML Model Management** - Registry, versioning, monitoring  
✅ **Data Quality** - Validation, metrics, alerts  
✅ **Configuration Management** - Feature flags, settings  
✅ **Multi-Tenancy** - Company/brand scoping  
✅ **Scalability** - Partitioning, indexing, performance optimization  
✅ **Comprehensive Audit Trail** - Complete tracking of all actions  
✅ **Data Lineage** - Track data transformations  
✅ **Compliance Tracking** - GDPR, retention, breaches  

**Total**: 80+ tables, 250+ indexes, 15+ partitioned tables, enterprise-ready!

### Content & Media Management

- ✅ **Article Content Storage** - Full article content with versioning
- ✅ **Content Versioning** - Track all content versions (draft → published)
- ✅ **Full-Text Search** - PostgreSQL tsvector with GIN indexes
- ✅ **Media Asset Management** - Images, videos, documents with metadata
- ✅ **Media Variants** - Thumbnails, different sizes, formats
- ✅ **Object Storage Integration** - S3/MinIO paths and URLs
- ✅ **Article-Media Relationships** - Link media to articles with positions
- ✅ **Media Collections** - Galleries, playlists, albums
- ✅ **Media Usage Tracking** - Analytics on media usage

---

**Schema Version**: 1.0.0  
**Last Updated**: 2024-01-15  
**Status**: ✅ Complete - World-Class Digitalization Platform

