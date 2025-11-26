# OwnLens ETL Module

## Overview

ETL (Extract, Transform, Load) pipelines for data processing using Apache Spark.

## Structure

```
etl/
├── base/                      # Base ETL classes
│   ├── extractor.py          # Base extractor
│   ├── transformer.py        # Base transformer
│   ├── loader.py              # Base loader
│   └── pipeline.py           # Pipeline orchestrator
│
├── extractors/                # Data extractors
│   ├── postgresql_extractor.py
│   ├── kafka_extractor.py
│   ├── api_extractor.py
│   ├── s3_extractor.py
│   └── customer/             # Customer domain extractors
│
├── transformers/              # Data transformers
│   ├── base_transformer.py
│   └── customer/             # Customer domain transformers
│
├── loaders/                   # Data loaders
│   ├── postgresql_loader.py
│   ├── clickhouse_loader.py
│   ├── s3_loader.py
│   └── kafka_loader.py
│
├── orchestration/             # ETL orchestration
│   └── etl_pipeline.py       # Main ETL pipeline orchestration
│
└── utils/                     # ETL utilities
    ├── spark_session.py      # Spark session management
    ├── config.py              # ETL configuration
    └── validators.py          # Data validators
```

## Usage

### ETL Pipeline Orchestration

The main way to run ETL pipelines is through the orchestration module:

```python
from src.ownlens.processing.etl.orchestration import run_etl_pipeline

# Run complete ETL pipeline: Extract → Transform → Load
results = run_etl_pipeline(
    source=None,  # None = all sources (PostgreSQL, Kafka, S3)
    load=True,
    load_destinations=["s3", "postgresql", "clickhouse"]
)
```

Or use the command-line script:

```bash
python scripts/run_etl_extraction_transformation_load.py --load
```

### Custom Pipeline

```python
from pyspark.sql import SparkSession
from src.ownlens.processing.etl.base.pipeline import ETLPipeline
from src.ownlens.processing.etl.extractors import PostgreSQLExtractor
from src.ownlens.processing.etl.transformers import BaseDataTransformer
from src.ownlens.processing.etl.loaders import PostgreSQLLoader
from src.ownlens.processing.etl.utils import get_spark_session, get_etl_config

# Create pipeline
spark = get_spark_session()
config = get_etl_config()

extractor = PostgreSQLExtractor(spark, config)
transformer = BaseDataTransformer(spark)
loader = PostgreSQLLoader(spark, config)

pipeline = ETLPipeline(
    spark=spark,
    extractor=extractor,
    transformer=transformer,
    loader=loader,
    name="CustomPipeline"
)

# Run pipeline
pipeline.run(
    extract_kwargs={"table": "customer_events"},
    load_kwargs={"destination": "customer_events_processed", "mode": "append"}
)
```

## Configuration

ETL configuration is managed through environment variables or `ETLConfig` class:

```python
from src.ownlens.processing.etl.utils.config import ETLConfig

config = ETLConfig(
    postgresql_host="localhost",
    postgresql_database="ownlens",
    kafka_bootstrap_servers="localhost:9092",
    s3_endpoint="http://localhost:9000",
    s3_bucket="ownlens"
)
```

## Features

- **Extractors**: PostgreSQL, Kafka, API, S3/MinIO
- **Transformers**: Data cleaning, aggregation, feature engineering
- **Loaders**: PostgreSQL, ClickHouse, S3/MinIO, Kafka
- **Orchestration**: Complete ETL pipeline orchestration (Extract → Transform → Load)
- **Utilities**: Spark session management, configuration, data validation, table dependencies

## Next Steps

- ✅ Integrated with Airflow for orchestration
- Add DBT models for SQL transformations
- Add Kafka producers for event publishing





