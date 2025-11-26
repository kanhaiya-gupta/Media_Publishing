"""
ETL Extractors
==============

Data extractors for various sources (PostgreSQL, Kafka, API, S3).
"""

from .postgresql_extractor import PostgreSQLExtractor
from .kafka_extractor import KafkaExtractor
from .api_extractor import APIExtractor
from .s3_extractor import S3Extractor

__all__ = [
    "PostgreSQLExtractor",
    "KafkaExtractor",
    "APIExtractor",
    "S3Extractor",
]

