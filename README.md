# Media Publishing - Real-Time Analytics Pipeline

Enterprise-grade real-time analytics pipeline for processing user events across digital media properties for a world-class publishing company.

## 🏗️ Architecture Overview

```mermaid
graph TB
    A[Kafka Producer] -->|Events| B[Kafka Topic<br/>web_clicks]
    B -->|Stream| C[Spark Streaming]
    C -->|Delta Lake| D[MinIO<br/>Data Lake]
    C -->|Analytics| E[ClickHouse<br/>OLAP Database]
    D -->|Query| F[Data Analytics]
    E -->|Query| G[ML Features]
    E -->|Query| H[Business Intelligence]
    
    style A fill:#e1f5ff
    style B fill:#fff4e1
    style C fill:#e8f5e9
    style D fill:#f3e5f5
    style E fill:#e0f2f1
```

## 📊 Data Flow

```mermaid
sequenceDiagram
    participant User as User Session
    participant Producer as Kafka Producer
    participant Kafka as Kafka Topic
    participant Spark as Spark Streaming
    participant MinIO as MinIO (Delta Lake)
    participant CH as ClickHouse
    
    User->>Producer: Generate Events
    Producer->>Kafka: Publish Events (Snappy)
    Kafka->>Spark: Stream Events
    Spark->>Spark: Aggregate Sessions
    Spark->>MinIO: Write Delta Lake
    Spark->>CH: Write Analytics
    Spark->>Spark: Next Batch (10s)
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- Java 11+ (for Spark)

### Installation

```bash
# Install Python dependencies
pip install -r requirements.txt

# Start infrastructure
docker-compose up -d

# Initialize ClickHouse schema
docker exec -i clickhouse clickhouse-client --password clickhouse < clickhouse/init.sql

# Create MinIO bucket
python create_minio_bucket.py
```

### Running the Pipeline

```bash
# Terminal 1: Start Kafka Producer
python kafka_producer.py

# Terminal 2: Start Spark Streaming
python spark_streaming.py
```

## 🔄 CI/CD Pipeline

The project includes a comprehensive CI/CD pipeline with automated testing, code quality checks, and deployment workflows.

### CI/CD Features

- ✅ **Automated Testing**: Unit tests, integration tests, and ML pipeline validation
- ✅ **Code Quality**: Linting (Flake8, Pylint), formatting (Black, isort), security scanning (Bandit)
- ✅ **Code Metrics**: Cyclomatic complexity, maintainability index, code coverage
- ✅ **Docker Validation**: Automated Docker image builds and validation
- ✅ **Data Quality Checks**: Daily data quality validation
- ✅ **ML Model Training**: Automated weekly model retraining
- ✅ **Deployment**: Automated deployment to staging and production

### Quick CI/CD Commands

```bash
# Setup environment
make setup

# Run tests
make test

# Run linting
make lint

# Format code
make format

# Run CI checks locally
make ci-test
make ci-lint
make ci-format-check

# Run ML pipeline
make ml-pipeline
# Or directly:
python run_ml.py

# Health checks
make health-check

# Validate workflows locally
make validate-workflows
```

### CI/CD Documentation

- **[CI/CD Guide](./docs/cicd.md)** - Complete CI/CD pipeline documentation
- **GitHub Actions**: `.github/workflows/` directory
  - `ci.yml` - Main CI pipeline
  - `data-quality.yml` - Data quality checks
  - `ml-training.yml` - ML model training

## 📁 Documentation

- **[Architecture](./docs/architecture.md)** - System architecture and design
- **[Data Flow](./docs/data-flow.md)** - Detailed data flow diagrams
- **[Setup Guide](./docs/setup.md)** - Step-by-step setup instructions
- **[Components](./docs/components.md)** - Component details, configurations, and performance benchmarks
- **[API Reference](./docs/api-reference.md)** - Schema and API documentation
- **[CI/CD Guide](./docs/cicd.md)** - CI/CD pipeline and workflows
- **[Data Model](./docs/data-model.md)** - ML-ready data model and feature extraction
- **[ML Models](./docs/ml-models.md)** - ML model recommendations and architectures
- **[Data Governance](./docs/data-governance.md)** - Privacy, compliance, and data governance framework

## 🏢 Brands Supported

- **Bild** - German tabloid (politics, sports, entertainment, lifestyle, crime)
- **Die Welt** - German newspaper (politics, business, technology, culture)
- **Business Insider** - Business news (business, tech, finance, careers, markets)
- **Politico** - Political news (politics, policy, elections, congress)
- **Sport Bild** - Sports magazine (football, basketball, tennis, olympics)

## 📈 Metrics Tracked

- **Session Metrics**: Duration, events, pages visited
- **Engagement Metrics**: Article views, clicks, video plays, newsletter signups
- **Geographic Analytics**: Country, city, timezone
- **Device Analytics**: Device type, OS, browser
- **Subscription Analytics**: Tier, user segment, conversion rates

## 🛠️ Technology Stack

- **Kafka** - Event streaming platform
- **Spark Streaming** - Real-time data processing
- **MinIO** - S3-compatible object storage (Data Lake)
- **Delta Lake** - ACID transactions for data lakes
- **ClickHouse** - Columnar OLAP database

## 📊 Data Storage

- **MinIO (Delta Lake)**: Raw session data with time travel capabilities
- **ClickHouse**: Aggregated analytics for fast queries and ML features

## 🔍 Monitoring

```bash
# Check MinIO data
python check_minio_data.py

# Check ClickHouse data
python check_clickhouse_data.py

# Verify services
docker-compose ps
```

## 📝 License

Internal use - Media Publishing

## 👥 Authors

Data Engineering Team - Media Publishing
