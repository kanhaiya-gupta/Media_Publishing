"""
ETL Loaders
===========

Data loaders for various destinations.
"""

from .postgresql_loader import PostgreSQLLoader
from .clickhouse_loader import ClickHouseLoader
from .s3_loader import S3Loader
from .kafka_loader import KafkaLoader

__all__ = [
    "PostgreSQLLoader",
    "ClickHouseLoader",
    "S3Loader",
    "KafkaLoader",
]

