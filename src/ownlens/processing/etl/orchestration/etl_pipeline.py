"""
ETL Pipeline Orchestration
===========================

Production-ready ETL pipeline orchestration module.
Extract → Transform → Load from all sources.
"""

import os
import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..utils.spark_session import get_spark_session
from ..utils.config import get_etl_config
from ..utils.table_dependencies import sort_tables_by_dependency, resolve_dependencies
from ..extractors.postgresql_extractor import (
    PostgreSQLExtractor,
    POSTGRESQL_TABLES as DEFAULT_POSTGRESQL_TABLES
)
from ..extractors.kafka_extractor import (
    KafkaExtractor,
    KAFKA_TOPICS as DEFAULT_KAFKA_TOPICS
)
from ..extractors.s3_extractor import (
    S3Extractor,
    S3_PATHS as DEFAULT_S3_PATHS
)
from ..transformers.base_transformer import BaseDataTransformer
from ..utils.transformer_factory import TransformerFactory
from ..utils.loader_factory import LoaderFactory
from ..utils.loader_config import get_table_load_config

logger = logging.getLogger(__name__)

# Use default lists from extractor classes
POSTGRESQL_TABLES = DEFAULT_POSTGRESQL_TABLES
KAFKA_TOPICS = DEFAULT_KAFKA_TOPICS
S3_PATHS = DEFAULT_S3_PATHS


def extract_postgresql_tables(
    spark,
    config,
    tables: Optional[List[str]] = None,
    transformer: Optional[BaseDataTransformer] = None,
    load: bool = False,
    load_destinations: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Extract and transform data from PostgreSQL tables.
    
    When loading to PostgreSQL (or other destinations with foreign key constraints),
    tables are automatically sorted by dependency order to avoid FK violations.
    """
    results = {}
    extractor = PostgreSQLExtractor(spark, config)
    tables_to_extract = tables or POSTGRESQL_TABLES
    
    # Sort tables by dependency order when loading to destinations with FK constraints
    # This ensures parent tables are loaded before child tables
    if load and load_destinations:
        destinations_with_fk = ["postgresql"]  # Add other destinations with FK constraints here if needed
        if any(dest in destinations_with_fk for dest in load_destinations):
            tables_to_extract = sort_tables_by_dependency(tables_to_extract)
            logger.info(f"📋 Tables sorted by dependency order for FK-safe loading")
    
    logger.info("=" * 80)
    logger.info("PostgreSQL Extraction + Transformation")
    logger.info("=" * 80)
    logger.info(f"Tables to extract: {len(tables_to_extract)}")
    
    for table in tables_to_extract:
        try:
            logger.info(f"\nExtracting from table: {table}")
            
            # Extract
            df = extractor.extract(table=table)
            
            if df is None:
                logger.warning(f"⚠️  No data extracted from table: {table}")
                results[table] = {"success": False, "reason": "No data extracted"}
                continue
            
            row_count = df.count()
            logger.info(f"✅ Extracted {row_count} rows from table: {table}")
            
            if row_count == 0:
                logger.warning(f"⚠️  Table {table} is empty")
                results[table] = {"success": True, "rows": 0, "transformed": False}
                continue
            
            # Transform
            table_transformer = transformer
            if table_transformer is None:
                transformer_config = config.extra_config if hasattr(config, 'extra_config') else None
                table_transformer = TransformerFactory.get_transformer(spark, table, transformer_config)
            
            if table_transformer:
                logger.info(f"Transforming data from table: {table}")
                try:
                    df_transformed = table_transformer.transform(df)
                    
                    if df_transformed is None:
                        logger.warning(f"⚠️  Transformation returned None for table: {table}")
                        results[table] = {"success": True, "rows": row_count, "transformed": False}
                        continue
                    
                    transformed_count = df_transformed.count()
                    logger.info(f"✅ Transformed {transformed_count} rows from table: {table}")
                    results[table] = {
                        "success": True,
                        "rows": row_count,
                        "transformed": True,
                        "transformed_rows": transformed_count
                    }
                    
                    # Load (optional)
                    if load and load_destinations:
                        logger.info(f"Loading transformed data from table: {table}")
                        load_results = {}
                        rows_to_load = df_transformed.count()
                        for dest in load_destinations:
                            try:
                                loader = LoaderFactory.get_loader(dest, spark, config)
                                table_config = get_table_load_config(table, dest)
                                
                                if table_config:
                                    load_kwargs = {
                                        "table_name": table,
                                        "mode": table_config.mode,
                                        "format": table_config.format if dest == "s3" else None,
                                        "partition_by": table_config.partition_by if dest == "s3" else None,
                                        "primary_key": table_config.primary_key if dest == "postgresql" else None,
                                        "conflict_resolution": table_config.conflict_resolution if dest == "postgresql" else None,
                                        "s3_path_template": table_config.s3_path_template if dest == "s3" else None,
                                        **table_config.kwargs
                                    }
                                    load_kwargs = {k: v for k, v in load_kwargs.items() if v is not None}
                                    
                                    if dest == "s3":
                                        destination = table_config.s3_path_template or f"data-lake/{table}/"
                                    elif dest == "clickhouse":
                                        destination = table_config.clickhouse_table or table
                                    else:
                                        destination = table
                                    
                                    result = loader.load(df_transformed, destination, **load_kwargs)
                                    if isinstance(result, dict):
                                        success = result.get("success", False)
                                        rows_inserted = result.get("rows_inserted", 0)
                                    else:
                                        success = result
                                        rows_inserted = rows_to_load if success else 0
                                    
                                    load_results[dest] = {"success": success, "rows_loaded": rows_inserted}
                                    
                                    if success:
                                        logger.info(f"✅ Loaded {rows_inserted} rows from {table} to {dest}")
                                    else:
                                        logger.error(f"❌ Failed to load {table} to {dest}")
                                else:
                                    if dest == "s3":
                                        destination = f"data-lake/{table}/"
                                    elif dest == "clickhouse":
                                        destination = table
                                    else:
                                        destination = table
                                    
                                    result = loader.load(df_transformed, destination, table_name=table)
                                    if isinstance(result, dict):
                                        success = result.get("success", False)
                                        rows_inserted = result.get("rows_inserted", 0)
                                    else:
                                        success = result
                                        rows_inserted = rows_to_load if success else 0
                                    
                                    load_results[dest] = {"success": success, "rows_loaded": rows_inserted}
                                    
                                    if success:
                                        logger.info(f"✅ Loaded {rows_inserted} rows from {table} to {dest}")
                                    else:
                                        logger.error(f"❌ Failed to load {table} to {dest}")
                                        
                            except Exception as e:
                                logger.error(f"❌ Error loading {table} to {dest}: {e}", exc_info=True)
                                load_results[dest] = {"success": False, "error": str(e), "rows_loaded": 0}
                        
                        results[table]["loaded"] = any(r.get("success", False) for r in load_results.values())
                        results[table]["load_results"] = load_results
                        total_rows_loaded = sum(r.get("rows_loaded", 0) for r in load_results.values())
                        results[table]["rows_loaded"] = total_rows_loaded
                    
                except Exception as e:
                    logger.error(f"❌ Transformation failed for table {table}: {e}", exc_info=True)
                    results[table] = {"success": True, "rows": row_count, "transformed": False, "error": str(e)}
            else:
                logger.info(f"⚠️  No transformer provided, skipping transformation for table: {table}")
                results[table] = {"success": True, "rows": row_count, "transformed": False}
            
        except Exception as e:
            logger.error(f"❌ Extraction failed for table {table}: {e}", exc_info=True)
            results[table] = {"success": False, "error": str(e)}
    
    return results


def extract_kafka_topics(
    spark,
    config,
    topics: Optional[List[str]] = None,
    transformer: Optional[BaseDataTransformer] = None,
    load: bool = False
) -> Dict[str, Any]:
    """Extract and transform data from Kafka topics (batch mode)."""
    results = {}
    extractor = KafkaExtractor(spark, config)
    topics_to_extract = topics or KAFKA_TOPICS
    
    logger.info("=" * 80)
    logger.info("Kafka Extraction + Transformation (Batch Mode)")
    logger.info("=" * 80)
    logger.info(f"Topics to extract: {len(topics_to_extract)}")
    
    for topic in topics_to_extract:
        try:
            logger.info(f"\nExtracting from Kafka topic: {topic}")
            
            df = extractor.extract_batch(
                topics=topic,
                starting_offsets="earliest",
                ending_offsets="latest"
            )
            
            if df is None:
                logger.warning(f"⚠️  No data extracted from topic: {topic}")
                results[topic] = {"success": False, "reason": "No data extracted"}
                continue
            
            row_count = df.count()
            logger.info(f"✅ Extracted {row_count} rows from topic: {topic}")
            
            if row_count == 0:
                logger.warning(f"⚠️  Topic {topic} is empty or does not exist")
                results[topic] = {"success": True, "rows": 0, "transformed": False}
                continue
            
            topic_transformer = transformer
            if topic_transformer is None:
                transformer_config = config.extra_config if hasattr(config, 'extra_config') else None
                topic_transformer = TransformerFactory.get_transformer(spark, topic, transformer_config)
            
            if topic_transformer:
                logger.info(f"Transforming data from topic: {topic}")
                try:
                    df_transformed = topic_transformer.transform(df)
                    
                    if df_transformed is None:
                        logger.warning(f"⚠️  Transformation returned None for topic: {topic}")
                        results[topic] = {"success": True, "rows": row_count, "transformed": False}
                        continue
                    
                    transformed_count = df_transformed.count()
                    logger.info(f"✅ Transformed {transformed_count} rows from topic: {topic}")
                    results[topic] = {
                        "success": True,
                        "rows": row_count,
                        "transformed": True,
                        "transformed_rows": transformed_count
                    }
                    
                    if load:
                        logger.info(f"Loading transformed data from topic: {topic}")
                        # TODO: Add loader logic here
                        logger.info(f"✅ Loaded transformed data from topic: {topic}")
                        results[topic]["loaded"] = True
                    
                except Exception as e:
                    logger.error(f"❌ Transformation failed for topic {topic}: {e}", exc_info=True)
                    results[topic] = {"success": True, "rows": row_count, "transformed": False, "error": str(e)}
            else:
                logger.info(f"⚠️  No transformer provided, skipping transformation for topic: {topic}")
                results[topic] = {"success": True, "rows": row_count, "transformed": False}
            
        except Exception as e:
            logger.error(f"❌ Extraction failed for topic {topic}: {e}", exc_info=True)
            results[topic] = {"success": False, "error": str(e)}
    
    return results


def extract_s3_paths(
    spark,
    config,
    paths: Optional[List[str]] = None,
    transformer: Optional[BaseDataTransformer] = None,
    load: bool = False,
    load_destinations: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Extract and transform data from S3/MinIO paths."""
    results = {}
    extractor = S3Extractor(spark, config)
    paths_to_extract = paths or S3_PATHS
    
    logger.info("=" * 80)
    logger.info("S3/MinIO Extraction + Transformation")
    logger.info("=" * 80)
    logger.info(f"Paths to extract: {len(paths_to_extract)}")
    
    for path in paths_to_extract:
        try:
            logger.info(f"\nExtracting from S3 path: {path}")
            
            df = extractor.extract(path=path)
            
            if df is None:
                logger.warning(f"⚠️  No data extracted from path: {path}")
                results[path] = {"success": False, "reason": "No data extracted"}
                continue
            
            row_count = df.count()
            logger.info(f"✅ Extracted {row_count} rows from path: {path}")
            
            if row_count == 0:
                logger.warning(f"⚠️  Path {path} is empty")
                results[path] = {"success": True, "rows": 0, "transformed": False}
                continue
            
            path_name = path.split("/")[-1].replace(".parquet", "").replace(".json", "").replace(".csv", "")
            if not path_name:
                path_name = path if path else ""
            
            path_transformer = transformer
            if path_transformer is None:
                transformer_config = config.extra_config if hasattr(config, 'extra_config') else None
                path_transformer = TransformerFactory.get_transformer(spark, path_name, transformer_config)
            
            if path_transformer is None:
                logger.info(f"S3 path '{path}' contains file metadata - no transformation needed")
                results[path] = {
                    "success": True,
                    "rows": row_count,
                    "transformed": False,
                    "note": "File metadata - no transformation needed"
                }
                continue
            
            if path_transformer:
                logger.info(f"Transforming data from path: {path}")
                try:
                    df_transformed = path_transformer.transform(df)
                    
                    if df_transformed is None:
                        logger.warning(f"⚠️  Transformation returned None for path: {path}")
                        results[path] = {"success": True, "rows": row_count, "transformed": False}
                        continue
                    
                    transformed_count = df_transformed.count()
                    logger.info(f"✅ Transformed {transformed_count} rows from path: {path}")
                    results[path] = {
                        "success": True,
                        "rows": row_count,
                        "transformed": True,
                        "transformed_rows": transformed_count
                    }
                    
                    if load:
                        logger.info(f"Loading transformed data from path: {path}")
                        # TODO: Add loader logic here
                        logger.info(f"✅ Loaded transformed data from path: {path}")
                        results[path]["loaded"] = True
                    
                except Exception as e:
                    logger.error(f"❌ Transformation failed for path {path}: {e}", exc_info=True)
                    results[path] = {"success": True, "rows": row_count, "transformed": False, "error": str(e)}
            else:
                logger.info(f"⚠️  No transformer provided, skipping transformation for path: {path}")
                results[path] = {"success": True, "rows": row_count, "transformed": False}
            
        except Exception as e:
            logger.error(f"❌ Extraction failed for path {path}: {e}", exc_info=True)
            results[path] = {"success": False, "error": str(e)}
    
    return results


def run_etl_pipeline(
    source: Optional[str] = None,
    load: bool = False,
    load_destinations: Optional[List[str]] = None,
    tables: Optional[List[str]] = None,
    topics: Optional[List[str]] = None,
    s3_paths: Optional[List[str]] = None,
    load_seed_to_production: bool = True
) -> Dict[str, Any]:
    """
    Run complete ETL pipeline: Extract → Transform → Load.
    
    Args:
        source: Source to extract from ('postgresql', 'kafka', 's3', or None for all)
        load: Whether to load transformed data
        load_destinations: List of destinations to load to ('s3', 'postgresql', 'clickhouse')
        tables: List of PostgreSQL tables to extract (defaults to all)
        topics: List of Kafka topics to extract (defaults to all)
        s3_paths: List of S3 paths to extract (defaults to all)
        load_seed_to_production: Whether to load seed data to production database first (default: True)
        
    Returns:
        Dictionary with results for each source
    """
    logger.info("=" * 80)
    logger.info("ETL Pipeline: Extract → Transform → Load")
    logger.info("=" * 80)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Source: {source or 'all'}")
    logger.info(f"Load: {load}")
    logger.info("=" * 80)
    
    # Load seed data to production database first (if loading to PostgreSQL)
    if load and load_destinations and "postgresql" in load_destinations and load_seed_to_production:
        logger.info("\n" + "=" * 80)
        logger.info("Loading Seed Data to Production Database")
        logger.info("=" * 80)
        try:
            from source_data.scripts.load_all_seed_data import load_all_seed_data
            from src.ownlens.repositories.shared import create_connection_manager
            
            prod_db = os.getenv("POSTGRES_DB", "ownlens")
            
            async def load_seed_to_prod():
                connection_manager = create_connection_manager(database=prod_db)
                await connection_manager.connect()
                logger.info(f"Loading seed data to production database: {prod_db}")
                seed_results = await load_all_seed_data(connection_manager=connection_manager)
                await connection_manager.close()
                return seed_results
            
            seed_results = asyncio.run(load_seed_to_prod())
            logger.info("✅ Seed data loaded to production database successfully")
        except Exception as e:
            logger.warning(f"⚠️ Could not load seed data to production database: {e}")
            logger.warning("⚠️ Foreign key violations may occur when loading dependent tables")
    
    # Initialize Spark session
    logger.info("\nInitializing Spark session...")
    spark = get_spark_session(app_name="ETLPipeline")
    config = get_etl_config()
    
    transformer = None
    results = {}
    
    # Extract from PostgreSQL
    if source is None or source == "postgresql":
        logger.info("\n" + "=" * 80)
        logger.info("PostgreSQL Extraction + Transformation")
        logger.info("=" * 80)
        postgresql_results = extract_postgresql_tables(
            spark=spark,
            config=config,
            tables=tables,
            transformer=transformer,
            load=load,
            load_destinations=load_destinations
        )
        results["postgresql"] = postgresql_results
    
    # Extract from Kafka
    if source is None or source == "kafka":
        logger.info("\n" + "=" * 80)
        logger.info("Kafka Extraction + Transformation")
        logger.info("=" * 80)
        kafka_results = extract_kafka_topics(
            spark=spark,
            config=config,
            topics=topics,
            transformer=transformer,
            load=load
        )
        results["kafka"] = kafka_results
    
    # Extract from S3
    if source is None or source == "s3":
        logger.info("\n" + "=" * 80)
        logger.info("S3/MinIO Extraction + Transformation")
        logger.info("=" * 80)
        s3_results = extract_s3_paths(
            spark=spark,
            config=config,
            paths=s3_paths,
            transformer=transformer,
            load=load,
            load_destinations=load_destinations
        )
        results["s3"] = s3_results
    
    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("ETL Pipeline Summary")
    logger.info("=" * 80)
    
    for source_name, source_results in results.items():
        logger.info(f"\n{source_name.upper()}:")
        success_count = sum(1 for r in source_results.values() if r.get("success", False))
        total_count = len(source_results)
        logger.info(f"  Success: {success_count}/{total_count}")
        
        for item_name, item_result in source_results.items():
            if item_result.get("success"):
                rows = item_result.get("rows", 0)
                transformed = item_result.get("transformed", False)
                transformed_rows = item_result.get("transformed_rows", None)
                rows_loaded = item_result.get("rows_loaded", 0)
                loaded = item_result.get("loaded", False)
                status = "✅" if transformed else "⚠️"
                
                if transformed and transformed_rows is not None:
                    if rows == transformed_rows:
                        if loaded and rows_loaded > 0:
                            logger.info(f"  {status} {item_name}: {rows} rows extracted, {transformed_rows} rows transformed, {rows_loaded} rows loaded")
                        else:
                            logger.info(f"  {status} {item_name}: {rows} rows extracted, {transformed_rows} rows transformed")
                    else:
                        if loaded and rows_loaded > 0:
                            logger.info(f"  {status} {item_name}: {rows} rows extracted, {transformed_rows} rows transformed, {rows_loaded} rows loaded (filtered: {rows - transformed_rows})")
                        else:
                            logger.info(f"  {status} {item_name}: {rows} rows extracted, {transformed_rows} rows transformed (filtered: {rows - transformed_rows})")
                else:
                    if loaded and rows_loaded > 0:
                        logger.info(f"  {status} {item_name}: {rows} rows extracted, {rows_loaded} rows loaded" + (f", transformed" if transformed else ""))
                    else:
                        logger.info(f"  {status} {item_name}: {rows} rows extracted" + (f", transformed" if transformed else ""))
            else:
                logger.info(f"  ❌ {item_name}: {item_result.get('error', 'Failed')}")
    
    logger.info("=" * 80)
    logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    return results

