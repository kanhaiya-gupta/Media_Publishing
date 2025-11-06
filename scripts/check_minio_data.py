#!/usr/bin/env python3
"""
Check data stored in MinIO (Delta Lake format)
"""

from pyspark.sql import SparkSession

# Create SparkSession with Delta Lake and S3/MinIO support
spark = SparkSession.builder \
    .appName("CheckMinIOData") \
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

MINIO_BUCKET = "s3a://session-metrics"

print("="*80)
print("Checking MinIO Data (Delta Lake format)")
print("="*80)

try:
    sessions_path = f"{MINIO_BUCKET}/sessions"
    
    # List all batch directories first
    print("\n1. Listing all batch directories in MinIO:")
    print("-" * 80)
    
    try:
        # List files using S3A filesystem
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(sessions_path),
            spark.sparkContext._jsc.hadoopConfiguration()
        )
        statuses = fs.listStatus(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(sessions_path))
        
        batch_dirs = []
        for status in statuses:
            path = status.getPath()
            dir_name = path.getName()
            if dir_name.startswith("batch_"):
                batch_num = dir_name.replace("batch_", "")
                try:
                    batch_dirs.append((int(batch_num), dir_name))
                except ValueError:
                    pass
        
        batch_dirs.sort(reverse=True)  # Most recent first
        print(f"Found {len(batch_dirs)} batch directories")
        print(f"Latest batches: {[d[1] for d in batch_dirs[:10]]}")
        
        if not batch_dirs:
            print("⚠ No batch directories found. Make sure data has been written to MinIO.")
            spark.stop()
            exit(0)
        
        # Limit to latest 20 batches for faster execution (can be changed)
        MAX_BATCHES = 20
        batches_to_process = batch_dirs[:MAX_BATCHES] if len(batch_dirs) > MAX_BATCHES else batch_dirs
        
        if len(batch_dirs) > MAX_BATCHES:
            print(f"\nNote: Processing latest {MAX_BATCHES} batches (out of {len(batch_dirs)} total)")
            print(f"      To process all batches, increase MAX_BATCHES in the script")
        
        # Read data from selected batches and union them
        print(f"\n2. Reading data from {len(batches_to_process)} batches...")
        print("-" * 80)
        
        dataframes = []
        successful_loads = 0
        for batch_num, batch_dir in batches_to_process:
            batch_path = f"{sessions_path}/{batch_dir}"
            try:
                df_batch = spark.read.format("delta").load(batch_path)
                count = df_batch.count()
                dataframes.append(df_batch)
                successful_loads += 1
                print(f"  ✓ Loaded {count:,} records from {batch_dir}")
            except Exception as e:
                # Skip if batch doesn't exist (might be in progress or empty)
                if "doesn't exist" not in str(e):
                    print(f"  ⚠ Could not load {batch_dir}: {e}")
        
        if not dataframes:
            print("⚠ No valid Delta tables found in batch directories.")
            spark.stop()
            exit(0)
        
        print(f"\n✓ Successfully loaded {successful_loads} out of {len(batches_to_process)} batches")
        
        # Union all dataframes
        from functools import reduce
        from pyspark.sql import DataFrame
        df = reduce(DataFrame.unionByName, dataframes)
        
        total_count = df.count()
        print(f"✓ Total records across processed batches: {total_count:,}")
        
        print(f"\n3. Sample data (first 20 rows):")
        print("-" * 80)
        df.show(20, truncate=False)
        
        print(f"\n4. Data summary:")
        print("-" * 80)
        df.describe().show()
        
        print(f"\n5. Session statistics (sample):")
        print("-" * 80)
        df.select(
            "user_id",
            "session_start",
            "session_end",
            "session_duration_sec",
            "total_events",
            "pages_visited_count",
            "unique_pages_count",
            "batch_id"
        ).show(20, truncate=False)
        
        # Aggregate statistics
        print(f"\n6. Aggregate statistics:")
        print("-" * 80)
        from pyspark.sql.functions import avg, max as spark_max, min as spark_min, count
        
        stats = df.agg(
            count("*").alias("total_sessions"),
            avg("session_duration_sec").alias("avg_duration_sec"),
            avg("total_events").alias("avg_events_per_session"),
            avg("pages_visited_count").alias("avg_pages_visited"),
            avg("unique_pages_count").alias("avg_unique_pages"),
            spark_max("session_duration_sec").alias("max_duration_sec"),
            spark_min("session_duration_sec").alias("min_duration_sec"),
            spark_max("batch_id").alias("latest_batch_id")
        )
        stats.show(truncate=False)
        
        # Statistics by batch
        print(f"\n7. Statistics by batch (latest 10 batches):")
        print("-" * 80)
        from pyspark.sql.functions import avg as spark_avg
        
        batch_stats = df.groupBy("batch_id").agg(
            count("*").alias("session_count"),
            spark_avg("session_duration_sec").alias("avg_duration"),
            spark_avg("total_events").alias("avg_events"),
            spark_max("session_start").alias("latest_session"),
            spark_min("session_start").alias("earliest_session")
        ).orderBy("batch_id", ascending=False).limit(10)
        
        batch_stats.show(truncate=False)
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        print("\nNote: Make sure MinIO bucket 'session-metrics' exists and contains data.")
        print("      You can check via MinIO Console: http://localhost:9001")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()

print("\n" + "="*80)
print("MinIO Data Check Complete")
print("="*80)
print("\nTip: You can also check MinIO data via:")
print("  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)")
print("  - Browse to bucket 'session-metrics' → sessions/")
print("="*80)

