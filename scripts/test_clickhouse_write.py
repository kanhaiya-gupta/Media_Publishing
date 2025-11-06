#!/usr/bin/env python3
"""
Test ClickHouse write operation using clickhouse-driver (native Python client)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, to_json

try:
    # Create SparkSession (no need for ClickHouse JDBC JARs)
    spark = SparkSession.builder \
        .appName("ClickHouseWriteTest") \
        .getOrCreate()
    
    # ClickHouse connection settings
    host = "localhost"
    port = 9002  # Native protocol port (mapped from container's 9000 to host's 9002)
    database = "analytics"
    user = "default"
    password = "clickhouse"
    
    print("Testing ClickHouse write operation using clickhouse-driver...")
    
    # Create a test DataFrame
    test_data = [
        (999, "2025-11-05 20:00:00", "2025-11-05 20:00:05", 5, 3, ["home", "news", "tech"], '{"test": "data"}', 1, 3)
    ]
    
    test_df = spark.createDataFrame(
        test_data,
        ["user_id", "session_start", "session_end", "session_duration_sec", 
         "total_events", "pages_visited_list", "page_events", "pages_visited_count", "unique_pages_count"]
    )
    
    # Convert array to JSON string for ClickHouse compatibility
    test_df = test_df.withColumn("pages_visited_list", to_json(col("pages_visited_list")))
    
    # Add batch_id
    test_df = test_df.withColumn("batch_id", lit(999))
    
    print("\nTest DataFrame:")
    test_df.show(truncate=False)
    
    # Write to ClickHouse using clickhouse-driver (native Python client)
    print("\nAttempting to write to ClickHouse using clickhouse-driver...")
    try:
        # Use foreachPartition to write each partition directly via clickhouse-driver
        def write_partition_to_clickhouse(partition):
            """Write partition to ClickHouse using clickhouse-driver"""
            from clickhouse_driver import Client
            
            # Connect to ClickHouse using native protocol
            client = Client(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            
            try:
                # Prepare data for batch insert
                from datetime import datetime
                
                data_to_insert = []
                for row in partition:
                    # Convert string timestamps to datetime objects for ClickHouse
                    session_start_dt = datetime.strptime(str(row.session_start), "%Y-%m-%d %H:%M:%S")
                    session_end_dt = datetime.strptime(str(row.session_end), "%Y-%m-%d %H:%M:%S")
                    
                    data_to_insert.append((
                        int(row.user_id),
                        session_start_dt,  # datetime object
                        session_end_dt,    # datetime object
                        int(row.session_duration_sec),
                        int(row.total_events),
                        str(row.pages_visited_list),
                        str(row.page_events),
                        int(row.pages_visited_count),
                        int(row.unique_pages_count),
                        int(row.batch_id)
                    ))
                
                if data_to_insert:
                    # Execute INSERT statement
                    insert_query = """
                    INSERT INTO session_metrics 
                    (user_id, session_start, session_end, session_duration_sec, 
                     total_events, pages_visited_list, page_events, 
                     pages_visited_count, unique_pages_count, batch_id)
                    VALUES
                    """
                    client.execute(insert_query, data_to_insert)
                    
            except Exception as e:
                print(f"Error in ClickHouse write: {e}")
                import traceback
                traceback.print_exc()
                raise
            finally:
                client.disconnect()
        
        # Use foreachPartition to write
        test_df.foreachPartition(write_partition_to_clickhouse)
        
        print("✓ Write successful!")
        
        # Now try to read it back using clickhouse-driver
        print("\nReading back the data...")
        from clickhouse_driver import Client
        
        read_client = Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        try:
            # Read data
            result = read_client.execute(
                "SELECT * FROM session_metrics WHERE user_id = 999"
            )
            
            print("✓ Read successful!")
            print(f"✓ Row count: {len(result)}")
            
            if result:
                # Convert to DataFrame for display
                from pyspark.sql.types import StructType, StructField, StringType, IntegerType
                from pyspark.sql import Row
                
                # Create DataFrame from results
                rows = [Row(*row) for row in result]
                read_df = spark.createDataFrame(rows)
                read_df.show(truncate=False)
            
        finally:
            read_client.disconnect()
        
        print("\n" + "="*80)
        print("✓ ALL TESTS PASSED - ClickHouse write and read work!")
        print("="*80)
        
    except Exception as e:
        print(f"✗ Write/Read failed: {e}")
        print("\nNote: This approach uses clickhouse-driver for native Python connection.")
        print("If clickhouse-driver is not installed, install it with: pip install clickhouse-driver")
        import traceback
        traceback.print_exc()
    
    spark.stop()
    
except Exception as e:
    print(f"✗ Connection failed: {e}")
    import traceback
    traceback.print_exc()
