# OwnLens PostgreSQL Schema

## Overview

This directory contains the complete PostgreSQL schema for OwnLens, organized by domain following Clean Architecture principles.

## Schema Structure

```
database/schema/
├── base.sql                      # Base schema (companies, brands, countries, users, etc.)
├── customer.sql                  # Customer domain schema
├── editorial_core.sql              # Core editorial (articles metadata, authors, performance)
├── editorial_content.sql          # Editorial content (article content, versioning, full-text search)
├── editorial_media.sql            # Editorial media assets (images, videos, documents, relationships)
├── company.sql                   # Company domain schema
├── security.sql                  # Security & Access Control (RBAC, API keys, sessions)
├── audit.sql                     # Audit & Logging (comprehensive audit trail)
├── compliance.sql                 # Compliance & Data Privacy (GDPR, retention, consent)
├── ml_models.sql                 # ML Model Management (registry, versioning, monitoring)
├── data_quality.sql              # Data Quality (validation, metrics, alerts)
├── configuration.sql             # Configuration Management (feature flags, settings)
├── object_storage_structure.md    # Object storage (S3/MinIO) structure documentation
├── editorial_content_data_flow.md # Content data flow documentation
└── README.md                     # This file
```

## Installation

### 1. Create Database

```sql
CREATE DATABASE ownlens;
\c ownlens;
```

### 2. Run Schema Files in Order

```bash
# Base schema first (required for all domains)
psql -U postgres -d ownlens -f database/schema/base.sql

# Security & infrastructure schemas
psql -U postgres -d ownlens -f database/schema/security.sql
psql -U postgres -d ownlens -f database/schema/audit.sql
psql -U postgres -d ownlens -f database/schema/compliance.sql
psql -U postgres -d ownlens -f database/schema/ml_models.sql
psql -U postgres -d ownlens -f database/schema/data_quality.sql
psql -U postgres -d ownlens -f database/schema/configuration.sql

# Domain schemas (order matters for editorial)
psql -U postgres -d ownlens -f database/schema/customer.sql
psql -U postgres -d ownlens -f database/schema/editorial_core.sql
psql -U postgres -d ownlens -f database/schema/editorial_content.sql
psql -U postgres -d ownlens -f database/schema/editorial_media.sql
psql -U postgres -d ownlens -f database/schema/company.sql
```

### 3. Or Run All at Once

```bash
psql -U postgres -d ownlens -f database/schema/base.sql \
     -f database/schema/security.sql \
     -f database/schema/audit.sql \
     -f database/schema/compliance.sql \
     -f database/schema/ml_models.sql \
     -f database/schema/data_quality.sql \
     -f database/schema/configuration.sql \
     -f database/schema/customer.sql \
     -f database/schema/editorial_core.sql \
     -f database/schema/editorial_content.sql \
     -f database/schema/editorial_media.sql \
     -f database/schema/company.sql
```

## Schema Details

### Base Schema (`base.sql`)

**Purpose**: Common infrastructure shared across all domains

**Tables**:
- `companies` - Companies (e.g., Axel Springer)
- `brands` - Brands owned by companies (e.g., Bild, Die Welt, Business Insider)
- `brand_countries` - Countries where each brand operates
- `countries` - Country reference data
- `cities` - City reference data
- `categories` - Content categories hierarchy
- `users` - Base users table
- `user_accounts` - User accounts with subscription info
- `device_types` - Device type reference
- `operating_systems` - OS reference
- `browsers` - Browser reference

### Customer Schema (`customer.sql`)

**Purpose**: Customer analytics and user behavior

**Tables**:
- `customer_sessions` - Aggregated session metrics
- `customer_events` - Raw user events (partitioned by date)
- `customer_user_features` - ML-ready user features
- `customer_user_segments` - User segments from ML clustering
- `customer_user_segment_assignments` - User segment assignments
- `customer_churn_predictions` - Churn predictions from ML models
- `customer_recommendations` - Content recommendations for users
- `customer_conversion_predictions` - Subscription conversion predictions

### Editorial Core Schema (`editorial_core.sql`)

**Purpose**: Core editorial analytics (articles metadata, authors, performance metrics)

**Tables**:
- `editorial_authors` - Authors/journalists
- `editorial_articles` - Articles and content pieces (metadata)
- `editorial_article_performance` - Daily article performance metrics
- `editorial_author_performance` - Daily author performance metrics
- `editorial_category_performance` - Daily category performance metrics
- `editorial_content_events` - Raw content events (partitioned by date)
- `editorial_headline_tests` - Headline A/B testing
- `editorial_trending_topics` - Trending topics detection
- `editorial_content_recommendations` - Content strategy recommendations

### Editorial Content Schema (`editorial_content.sql`)

**Purpose**: Article content storage, versioning, and full-text search

**Tables**:
- `editorial_article_content` - Full article content with versioning
- `editorial_content_versions` - Content version history and change tracking

**Features**:
- ✅ Full-text search (tsvector, GIN indexes)
- ✅ Content versioning (draft → published → archived)
- ✅ Multiple content formats (HTML, Markdown, JSON)
- ✅ Content blocks and sections (rich content)
- ✅ Automatic search vector generation
- ✅ Keyword extraction
- ✅ Word count and reading time calculation

### Editorial Media Schema (`editorial_media.sql`)

**Purpose**: Media asset management (images, videos, documents) with object storage

**Tables**:
- `editorial_media_assets` - Media assets (images, videos, documents) with object storage paths
- `editorial_media_variants` - Media variants (thumbnails, different sizes, formats)
- `editorial_content_media` - Article-media relationships (which media belongs to which article)
- `editorial_media_collections` - Media collections (galleries, playlists, albums)
- `editorial_media_collection_items` - Media items in collections
- `editorial_media_usage` - Media usage tracking across articles (for analytics)

**Features**:
- ✅ Object storage integration (S3/MinIO paths)
- ✅ Media variants (thumbnails, small, medium, large)
- ✅ Media metadata (dimensions, duration, format, etc.)
- ✅ Article-media relationships with positions
- ✅ Media collections (galleries, playlists)
- ✅ Usage tracking and analytics
- ✅ Helper functions for media URLs

### Company Schema (`company.sql`)

**Purpose**: Internal communications and company content analytics

**Tables**:
- `company_departments` - Departments within companies
- `company_employees` - Employees linked to users
- `company_internal_content` - Internal company content
- `company_content_performance` - Internal content performance metrics
- `company_department_performance` - Department performance metrics
- `company_content_events` - Raw company content events (partitioned by date)
- `company_employee_engagement` - Employee engagement metrics
- `company_communications_analytics` - Overall internal communications analytics

### Security Schema (`security.sql`)

**Purpose**: Role-Based Access Control (RBAC), authentication, and security

**Tables**:
- `security_roles` - RBAC roles
- `security_permissions` - Permissions for resources and actions
- `security_role_permissions` - Role-permission mappings
- `security_user_roles` - User-role assignments (scoped to company/brand)
- `security_api_keys` - API keys for service-to-service authentication
- `security_api_key_usage` - API key usage tracking (partitioned by date)
- `security_user_sessions` - User authentication sessions

### Audit Schema (`audit.sql`)

**Purpose**: Comprehensive audit logging for compliance, security, and data lineage

**Tables**:
- `audit_logs` - Main audit log (all actions) (partitioned by date)
- `audit_data_changes` - Track all data changes (INSERT, UPDATE, DELETE) (partitioned by date)
- `audit_data_access` - Track data access for compliance (partitioned by date)
- `audit_security_events` - Track security-related events (partitioned by date)
- `audit_data_lineage` - Track data lineage and transformations (partitioned by date)
- `audit_compliance_events` - Track compliance events (GDPR, data retention, etc.) (partitioned by date)

### Compliance Schema (`compliance.sql`)

**Purpose**: GDPR compliance, data privacy, consent management, data retention

**Tables**:
- `compliance_user_consent` - User consent tracking for GDPR compliance
- `compliance_data_subject_requests` - GDPR data subject requests (access, deletion, etc.)
- `compliance_retention_policies` - Data retention policies and rules
- `compliance_retention_executions` - Data retention policy execution log
- `compliance_anonymized_data` - Track anonymized data for compliance
- `compliance_privacy_assessments` - Privacy Impact Assessments (PIA/DPIA)
- `compliance_breach_incidents` - Data breach incident tracking and response

### ML Models Schema (`ml_models.sql`)

**Purpose**: ML model registry, versioning, training, deployment, and monitoring

**Tables**:
- `ml_model_registry` - ML model registry with versioning and metadata
- `ml_model_features` - Input features for each ML model
- `ml_model_training_runs` - Model training runs and execution tracking
- `ml_model_predictions` - Model predictions tracking (partitioned by date)
- `ml_model_monitoring` - Model performance monitoring and drift detection
- `ml_model_ab_tests` - Model A/B testing for comparing model versions

### Data Quality Schema (`data_quality.sql`)

**Purpose**: Data quality checks, validation rules, data quality metrics

**Tables**:
- `data_quality_rules` - Data quality validation rules and thresholds
- `data_quality_checks` - Data quality check executions (partitioned by date)
- `data_quality_metrics` - Data quality metrics aggregated by date
- `data_quality_alerts` - Data quality alerts when quality drops (partitioned by date)
- `data_validation_results` - Individual record validation results (partitioned by date)

### Configuration Schema (`configuration.sql`)

**Purpose**: Feature flags, system configuration, settings management

**Tables**:
- `configuration_feature_flags` - Feature flags for feature toggling
- `configuration_feature_flag_history` - Feature flag change history
- `configuration_system_settings` - System configuration settings
- `configuration_system_settings_history` - System configuration change history

## Additional Documentation

### Object Storage Structure (`object_storage_structure.md`)

Complete documentation for S3/MinIO bucket structure:
- Media file organization (images, videos, documents)
- Path structure and naming conventions
- URL generation (MinIO, S3, CDN)
- Storage policies and lifecycle rules
- Backup strategy
- CDN integration

### Content Data Flow (`editorial_content_data_flow.md`)

Complete data flow documentation:
- Article creation flow
- Media upload flow
- Content publishing flow
- Article reading flow
- Full-text search flow
- Performance optimizations
- Error handling

## Key Features

### Multi-Company, Multi-Brand Support

- Companies can have multiple brands
- Brands can operate in multiple countries
- All analytics are scoped to brand/company level

### Partitioning

- Event tables are partitioned by date for performance
- Monthly partitions for `customer_events`, `editorial_content_events`, `company_content_events`

### Indexes

- Comprehensive indexes for all foreign keys
- Performance indexes for common queries
- Full-text search indexes where applicable

### Triggers

- Automatic `updated_at` timestamp updates
- Data validation triggers (can be added)

### Extensions

- `uuid-ossp` - UUID generation
- `pg_trgm` - Text search (trigram matching)

## Data Model Relationships

```
Companies
  └── Brands (1:N)
      └── Brand Countries (N:M)
      └── Categories (1:N)
      └── Customer Sessions (1:N)
      └── Editorial Articles (1:N)
      └── Company Internal Content (1:N)

Users
  └── User Accounts (1:N)
  └── Customer Sessions (1:N)
  └── Company Employees (1:1)

Brands
  └── Editorial Authors (1:N)
      └── Editorial Articles (1:N)
          └── Editorial Article Performance (1:N)
          └── Editorial Content Events (1:N)
```

## Usage Examples

### Insert Company and Brand

```sql
-- Insert company
INSERT INTO companies (company_name, company_code, headquarters_country_code)
VALUES ('Axel Springer', 'axel_springer', 'DE')
RETURNING company_id;

-- Insert brand
INSERT INTO brands (company_id, brand_name, brand_code, primary_country_code)
VALUES (
    (SELECT company_id FROM companies WHERE company_code = 'axel_springer'),
    'Bild',
    'bild',
    'DE'
)
RETURNING brand_id;
```

### Query Customer Analytics

```sql
-- Top users by engagement
SELECT 
    u.user_id,
    u.email,
    cuf.avg_engagement_score,
    cuf.total_sessions
FROM customer_user_features cuf
JOIN users u ON cuf.user_id = u.user_id
WHERE cuf.brand_id = (SELECT brand_id FROM brands WHERE brand_code = 'bild')
ORDER BY cuf.avg_engagement_score DESC
LIMIT 10;
```

### Query Editorial Analytics

```sql
-- Top articles by views
SELECT 
    a.article_id,
    a.title,
    ap.total_views,
    ap.engagement_score
FROM editorial_article_performance ap
JOIN editorial_articles a ON ap.article_id = a.article_id
WHERE ap.brand_id = (SELECT brand_id FROM brands WHERE brand_code = 'bild')
  AND ap.performance_date = CURRENT_DATE - INTERVAL '1 day'
ORDER BY ap.total_views DESC
LIMIT 10;
```

### Query Company Analytics

```sql
-- Department performance
SELECT 
    d.department_name,
    dp.avg_engagement_score,
    dp.content_views,
    dp.employee_engagement_rate
FROM company_department_performance dp
JOIN company_departments d ON dp.department_id = d.department_id
WHERE dp.company_id = (SELECT company_id FROM companies WHERE company_code = 'axel_springer')
  AND dp.performance_date = CURRENT_DATE - INTERVAL '1 day'
ORDER BY dp.avg_engagement_score DESC;
```

## Maintenance

### Creating New Partitions

For partitioned tables, create new monthly partitions:

```sql
-- Example: Create partition for March 2024
CREATE TABLE customer_events_2024_03 PARTITION OF customer_events
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
```

### Vacuum and Analyze

Regular maintenance:

```sql
VACUUM ANALYZE;
```

## Notes

- All timestamps use `TIMESTAMP WITH TIME ZONE`
- UUIDs are used for primary keys
- JSONB is used for flexible metadata storage
- Foreign keys have appropriate `ON DELETE` actions
- Indexes are created for performance optimization
- Triggers maintain `updated_at` timestamps automatically

## Version

**Schema Version**: 1.0.0  
**Last Updated**: 2024-01-15

