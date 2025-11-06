#!/usr/bin/env python3
"""
Check data stored in ClickHouse
"""

from clickhouse_driver import Client

# ClickHouse connection settings
host = "localhost"
port = 9002  # Native protocol port
database = "analytics"
user = "default"
password = "clickhouse"

print("="*80)
print("Checking ClickHouse Data")
print("="*80)

try:
    # Connect to ClickHouse
    client = Client(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    
    print(f"\n✓ Connected to ClickHouse: {host}:{port}/{database}\n")
    
    # 1. Check total record count
    print("1. Total Sessions:")
    print("-" * 80)
    total_count = client.execute("SELECT COUNT(*) FROM session_metrics")[0][0]
    print(f"   Total sessions: {total_count:,}")
    
    # 2. Sample data
    print(f"\n2. Sample Data (first 10 rows):")
    print("-" * 80)
    sample_data = client.execute("""
        SELECT 
            user_id,
            session_start,
            session_end,
            session_duration_sec,
            total_events,
            pages_visited_count,
            unique_pages_count,
            batch_id
        FROM session_metrics
        ORDER BY session_start DESC
        LIMIT 10
    """)
    
    print(f"{'User ID':<10} {'Session Start':<20} {'Session End':<20} {'Duration':<10} {'Events':<8} {'Pages':<8} {'Unique':<8} {'Batch':<8}")
    print("-" * 120)
    for row in sample_data:
        user_id, start, end, duration, events, pages, unique_pages, batch = row
        print(f"{user_id:<10} {str(start):<20} {str(end):<20} {duration:<10} {events:<8} {pages:<8} {unique_pages:<8} {batch:<8}")
    
    # 3. Statistics
    print(f"\n3. Aggregate Statistics:")
    print("-" * 80)
    stats = client.execute("""
        SELECT 
            COUNT(*) as total_sessions,
            AVG(session_duration_sec) as avg_duration_sec,
            AVG(total_events) as avg_events_per_session,
            AVG(pages_visited_count) as avg_pages_visited,
            AVG(unique_pages_count) as avg_unique_pages,
            MAX(session_duration_sec) as max_duration_sec,
            MIN(session_duration_sec) as min_duration_sec,
            MAX(batch_id) as latest_batch_id
        FROM session_metrics
    """)[0]
    
    print(f"   Total Sessions:       {stats[0]:,}")
    print(f"   Avg Duration (sec):    {stats[1]:.2f}")
    print(f"   Avg Events/Session:    {stats[2]:.2f}")
    print(f"   Avg Pages Visited:    {stats[3]:.2f}")
    print(f"   Avg Unique Pages:     {stats[4]:.2f}")
    print(f"   Max Duration (sec):    {stats[5]}")
    print(f"   Min Duration (sec):    {stats[6]}")
    print(f"   Latest Batch ID:      {stats[7]}")
    
    # 4. Sessions by batch
    print(f"\n4. Sessions by Batch ID:")
    print("-" * 80)
    batch_stats = client.execute("""
        SELECT 
            batch_id,
            COUNT(*) as session_count,
            AVG(session_duration_sec) as avg_duration,
            MIN(session_start) as first_session,
            MAX(session_end) as last_session
        FROM session_metrics
        GROUP BY batch_id
        ORDER BY batch_id DESC
        LIMIT 10
    """)
    
    print(f"{'Batch ID':<10} {'Sessions':<12} {'Avg Duration':<15} {'First Session':<20} {'Last Session':<20}")
    print("-" * 100)
    for row in batch_stats:
        batch_id, count, avg_dur, first, last = row
        print(f"{batch_id:<10} {count:<12} {avg_dur:<15.2f} {str(first):<20} {str(last):<20}")
    
    # 5. Recent sessions
    print(f"\n5. Most Recent Sessions:")
    print("-" * 80)
    recent = client.execute("""
        SELECT 
            user_id,
            session_start,
            session_end,
            session_duration_sec,
            total_events,
            pages_visited_list,
            batch_id
        FROM session_metrics
        ORDER BY session_start DESC
        LIMIT 5
    """)
    
    for i, row in enumerate(recent, 1):
        user_id, start, end, duration, events, pages_list, batch = row
        print(f"\n   Session {i}:")
        print(f"   - User ID: {user_id}")
        print(f"   - Start: {start}")
        print(f"   - End: {end}")
        print(f"   - Duration: {duration} seconds")
        print(f"   - Events: {events}")
        print(f"   - Pages: {pages_list[:100]}..." if len(pages_list) > 100 else f"   - Pages: {pages_list}")
        print(f"   - Batch: {batch}")
    
    # 6. Top users by event count
    print(f"\n6. Top 10 Users by Total Events:")
    print("-" * 80)
    top_users = client.execute("""
        SELECT 
            user_id,
            COUNT(*) as session_count,
            SUM(total_events) as total_events,
            AVG(session_duration_sec) as avg_duration
        FROM session_metrics
        GROUP BY user_id
        ORDER BY total_events DESC
        LIMIT 10
    """)
    
    print(f"{'User ID':<10} {'Sessions':<12} {'Total Events':<15} {'Avg Duration':<15}")
    print("-" * 60)
    for row in top_users:
        user_id, sessions, events, avg_dur = row
        print(f"{user_id:<10} {sessions:<12} {events:<15} {avg_dur:<15.2f}")
    
    client.disconnect()
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("ClickHouse Data Check Complete")
print("="*80)
print("\nTip: You can also query ClickHouse directly using:")
print("  docker exec -it clickhouse clickhouse-client --password clickhouse")
print("  Then run: SELECT * FROM analytics.session_metrics LIMIT 10;")
print("="*80)

