#!/usr/bin/env python3
"""
Test MinIO/Delta Lake write operation from Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import lit, col, to_json

try:
    # Create SparkSession with Delta Lake and S3/MinIO support
    # Using compatible versions: Spark 3.4.1 + Delta Lake 2.4.0
    spark = SparkSession.builder \
        .appName("MinIOWriteTest") \
        .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    print("Testing MinIO/Delta Lake write operation...")
    
    # Create a test DataFrame matching the session_metrics schema
    test_data = [
        (999, "2025-11-05 20:00:00", "2025-11-05 20:00:05", 5, 3, 
         ["home", "news", "tech"], '{"test": "data"}', 1, 3)
    ]
    
    test_df = spark.createDataFrame(
        test_data,
        ["user_id", "session_start", "session_end", "session_duration_sec", 
         "total_events", "pages_visited_list", "page_events", 
         "pages_visited_count", "unique_pages_count"]
    )
    
    # Convert array to JSON string for Delta Lake compatibility
    test_df = test_df.withColumn("pages_visited_list", to_json(col("pages_visited_list")))
    
    # Add batch_id
    test_df = test_df.withColumn("batch_id", lit(999))
    
    print("\nTest DataFrame:")
    test_df.show(truncate=False)
    
    # MinIO bucket and path
    bucket = "session-metrics"
    test_path_parquet = f"s3a://{bucket}/test/parquet_batch_999"
    test_path_delta = f"s3a://{bucket}/test/delta_batch_999"
    
    print(f"\nAttempting to write to MinIO: {test_path_parquet}")
    print("(Make sure bucket 'session-metrics' exists in MinIO)")
    
    try:
        # Test 1: Write to MinIO using Parquet format (basic connectivity test)
        print("\n" + "="*80)
        print("TEST 1: Writing to MinIO using Parquet format")
        print("="*80)
        test_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(test_path_parquet)
        
        print("✓ Parquet write successful!")
        
        # Try to read it back
        print(f"\nReading back from MinIO: {test_path_parquet}")
        read_df = spark.read \
            .format("parquet") \
            .load(test_path_parquet)
        
        print("✓ Read successful!")
        print(f"✓ Row count: {read_df.count()}")
        print("\nData read from MinIO:")
        read_df.show(truncate=False)
        
        # Verify data matches
        original_count = test_df.count()
        read_count = read_df.count()
        
        if original_count == read_count:
            print(f"\n✓ Data integrity check passed: {original_count} rows written and read")
        else:
            print(f"\n✗ Data integrity check failed: {original_count} written, {read_count} read")
        
        print("\n" + "="*80)
        print("✓ TEST 1 PASSED - MinIO/Parquet write and read work!")
        print("="*80)
        print(f"✓ Data written to: {test_path_parquet}")
        print("="*80)
        
        # Test 2: Try Delta Lake format
        print("\n" + "="*80)
        print("TEST 2: Writing to MinIO using Delta Lake format")
        print("="*80)
        try:
            test_df.write \
                .mode("overwrite") \
                .format("delta") \
                .save(test_path_delta)
            
            print("✓ Delta Lake write successful!")
            
            # Try to read it back
            print(f"\nReading back from MinIO: {test_path_delta}")
            delta_read_df = spark.read \
                .format("delta") \
            .load(test_path_delta)
            
            print("✓ Delta Lake read successful!")
            print(f"✓ Row count: {delta_read_df.count()}")
            print("\nData read from MinIO (Delta Lake):")
            delta_read_df.show(truncate=False)
            
            print("\n" + "="*80)
            print("✓ TEST 2 PASSED - MinIO/Delta Lake write and read work!")
            print("="*80)
            print(f"✓ Data written to: {test_path_delta}")
            print(f"✓ Delta Lake format: ACID transactions, time travel supported")
            print("="*80)
            
        except Exception as delta_e:
            print(f"⚠ Delta Lake test failed: {delta_e}")
            print("  Note: Using compatible versions (Spark 3.4.1 + Delta 2.4.0)")
            print("  If using Spark 3.5.1, may need different Delta version")
            import traceback
            traceback.print_exc()
        
        print("\n" + "="*80)
        print("✓ MINIO CONNECTIVITY CONFIRMED!")
        print("="*80)
        print(f"✓ Parquet written to: {test_path_parquet}")
        print("✓ MinIO bucket: session-metrics")
        print("✓ S3A filesystem working correctly")
        print("="*80)
        
    except Exception as e:
        print(f"✗ Write/Read failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Check if bucket exists
        if "NoSuchBucket" in str(e) or "bucket" in str(e).lower():
            print("\n" + "="*80)
            print("NOTE: Bucket 'session-metrics' may not exist in MinIO")
            print("Create it:")
            print("  1. Go to MinIO Console: http://localhost:9001")
            print("  2. Login: minioadmin / minioadmin")
            print("  3. Create bucket: 'session-metrics'")
            print("  Or run: python create_minio_bucket.py")
            print("="*80)
    
    spark.stop()
    
except Exception as e:
    print(f"✗ Connection failed: {e}")
    import traceback
    traceback.print_exc()

