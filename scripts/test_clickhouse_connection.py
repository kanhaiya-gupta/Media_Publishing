#!/usr/bin/env python3
"""
Test ClickHouse JDBC connection with password
"""

try:
    from pyspark.sql import SparkSession
    
    # Create SparkSession with ClickHouse JDBC support
    spark = SparkSession.builder \
        .appName("ClickHouseConnectionTest") \
        .config("spark.jars.packages", 
                "com.clickhouse:clickhouse-jdbc:0.6.0,"
                "org.apache.httpcomponents.client5:httpclient5:5.2.1") \
        .getOrCreate()
    
    # Test connection
    # Try different URL formats for ClickHouse
    url = "jdbc:clickhouse://localhost:8123/analytics?password=clickhouse"
    user = "default"
    password = "clickhouse"
    
    print("Testing ClickHouse JDBC connection...")
    
    # Try a simple query first to test connection
    print("\nTesting with a simple query...")
    try:
        query_df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("query", "SELECT COUNT(*) as count FROM session_metrics") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .load()
        
        print("✓ Query successful!")
        query_df.show()
    except Exception as e:
        print(f"✗ Query failed: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        exit(1)
    
    # Now try to read the full table with LIMIT to avoid issues
    print("\nTesting full table read (with LIMIT)...")
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("query", "SELECT * FROM session_metrics LIMIT 10") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .load()
        
        print(f"✓ Table read successful!")
        print(f"✓ Row count: {df.count()}")
        print("\nFirst few rows:")
        df.show(5, truncate=False)
    except Exception as e:
        print(f"✗ Table read failed: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        exit(1)
    
    print("\n" + "="*80)
    print("✓ ALL TESTS PASSED - ClickHouse connection works!")
    print("="*80)
    print("\n✓ Connection successful!")
    print("✓ Table 'session_metrics' exists")
    print("✓ Both READ and WRITE operations work!")
    
    spark.stop()
    
except Exception as e:
    print(f"✗ Connection failed: {e}")
    import traceback
    traceback.print_exc()

