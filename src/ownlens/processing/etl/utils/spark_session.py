"""
Spark Session Management
========================

Manages Spark session creation and configuration for ETL pipelines.
"""

from typing import Optional, Dict, Any
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """
    Manages Spark session lifecycle and configuration.
    
    Provides singleton pattern for Spark session management with
    support for different configurations (batch, streaming, local, cluster).
    """
    
    _sessions: Dict[str, SparkSession] = {}
    
    @classmethod
    def create_session(
        cls,
        app_name: str,
        master: str = "local[*]",
        config: Optional[Dict[str, Any]] = None,
        enable_hive: bool = False,
        session_type: str = "default"
    ) -> SparkSession:
        """
        Create or get existing Spark session.
        
        Args:
            app_name: Application name
            master: Spark master URL (default: "local[*]")
            config: Additional Spark configuration
            enable_hive: Enable Hive support
            session_type: Session type identifier (for multiple sessions)
            
        Returns:
            SparkSession instance
        """
        session_key = f"{app_name}_{session_type}"
        
        if session_key in cls._sessions:
            logger.info(f"Reusing existing Spark session: {session_key}")
            return cls._sessions[session_key]
        
        logger.info(f"Creating new Spark session: {session_key}")
        
        builder = SparkSession.builder.appName(app_name).master(master)
        
        if enable_hive:
            builder = builder.enableHiveSupport()
        
        # Suppress KAFKA-1894 warning early via Spark configuration
        # This needs to be set before creating the session
        builder = builder.config("spark.executor.extraJavaOptions", "-Dlog4j.logger.org.apache.spark.sql.kafka010.KafkaDataConsumer=ERROR")
        builder = builder.config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache.spark.sql.kafka010.KafkaDataConsumer=ERROR")
        
        # Required JAR packages (downloaded from Maven on first run)
        # Note: The first run will download JARs from Maven (requires internet)
        jars_packages = (
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"  # Kafka connector
            "org.apache.hadoop:hadoop-aws:3.3.4,"  # S3/MinIO support
            "io.delta:delta-core_2.12:2.4.0,"  # Delta Lake
            "org.postgresql:postgresql:42.7.1,"  # PostgreSQL JDBC driver
            "com.clickhouse:clickhouse-jdbc:0.6.0,"  # ClickHouse JDBC driver
            "org.apache.httpcomponents.client5:httpclient5:5.2.1"  # HTTP client for ClickHouse JDBC
        )
        builder = builder.config("spark.jars.packages", jars_packages)
        
        # Delta Lake extensions
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        
        # Default configurations
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            # Memory configurations - increased since sessions are cleaned up after each task
            # For local mode, driver and executor share the same JVM
            # Using 8GB for maximum performance with large datasets and complex transformations
            "spark.driver.memory": "8g",  # Driver memory (maximum performance for large datasets)
            "spark.driver.maxResultSize": "4g",  # Max result size for collect() (allows very large result sets)
            "spark.executor.memory": "8g",  # Executor memory (for local[*] this is the same JVM)
            "spark.memory.fraction": "0.6",  # Use 60% of heap for execution and storage
            "spark.memory.storageFraction": "0.3",  # 30% of memory fraction for storage
            # Timeout configurations for long-running jobs
            "spark.network.timeout": "600s",  # Network timeout (default: 120s) - increased for long operations
            "spark.executor.heartbeatInterval": "30s",  # Heartbeat interval (default: 10s) - less frequent heartbeats
            "spark.sql.broadcastTimeout": "600",  # Broadcast timeout in seconds (default: 300s)
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",  # Arrow batch size for better performance
        }
        
        # S3/MinIO configuration (can be overridden by config parameter)
        # Default values (can be overridden via config parameter or ETL config)
        s3_config = {
            "spark.hadoop.fs.s3a.endpoint": "http://localhost:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        }
        
        if config:
            default_config.update(config)
            # Allow S3 config to be overridden
            for key in s3_config:
                if key not in config:
                    default_config[key] = s3_config[key]
        else:
            default_config.update(s3_config)
        
        for key, value in default_config.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        cls._sessions[session_key] = spark
        
        # Suppress KAFKA-1894 warning (KafkaDataConsumer thread interruption warning)
        # This is a known Spark/Kafka issue that doesn't affect functionality
        # Only suppress the specific KafkaDataConsumer logger, not all warnings
        try:
            spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger("org.apache.spark.sql.kafka010.KafkaDataConsumer").setLevel(
                spark.sparkContext._jvm.org.apache.log4j.Level.ERROR
            )
        except Exception:
            # If JVM access fails, continue without suppression
            pass
        
        logger.info(f"Spark session created successfully: {session_key}")
        return spark
    
    @classmethod
    def create_streaming_session(
        cls,
        app_name: str,
        master: str = "local[*]",
        config: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        """
        Create Spark session optimized for streaming.
        
        Args:
            app_name: Application name
            master: Spark master URL
            config: Additional Spark configuration
            
        Returns:
            SparkSession configured for streaming
        """
        streaming_config = {
            "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoint",
            "spark.sql.streaming.schemaInference": "true",
        }
        
        if config:
            streaming_config.update(config)
        
        return cls.create_session(
            app_name=app_name,
            master=master,
            config=streaming_config,
            session_type="streaming"
        )
    
    @classmethod
    def stop_session(cls, session_type: str = "default", app_name: Optional[str] = None):
        """
        Stop Spark session.
        
        Args:
            session_type: Session type identifier
            app_name: Application name (optional, for specific session)
        """
        if app_name:
            session_key = f"{app_name}_{session_type}"
            if session_key in cls._sessions:
                logger.info(f"Stopping Spark session: {session_key}")
                cls._sessions[session_key].stop()
                del cls._sessions[session_key]
        else:
            # Stop all sessions
            for session_key, session in list(cls._sessions.items()):
                logger.info(f"Stopping Spark session: {session_key}")
                session.stop()
                del cls._sessions[session_key]
    
    @classmethod
    def get_session(cls, app_name: str, session_type: str = "default") -> Optional[SparkSession]:
        """
        Get existing Spark session.
        
        Args:
            app_name: Application name
            session_type: Session type identifier
            
        Returns:
            SparkSession instance or None if not found
        """
        session_key = f"{app_name}_{session_type}"
        return cls._sessions.get(session_key)


def get_spark_session(
    app_name: str = "OwnLensETL",
    master: str = "local[*]",
    config: Optional[Dict[str, Any]] = None,
    streaming: bool = False
) -> SparkSession:
    """
    Convenience function to get or create Spark session.
    
    Args:
        app_name: Application name
        master: Spark master URL
        config: Additional Spark configuration
        streaming: Whether to create streaming-optimized session
        
    Returns:
        SparkSession instance
    """
    if streaming:
        return SparkSessionManager.create_streaming_session(
            app_name=app_name,
            master=master,
            config=config
        )
    else:
        return SparkSessionManager.create_session(
            app_name=app_name,
            master=master,
            config=config
        )

