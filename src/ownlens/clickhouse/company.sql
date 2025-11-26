-- ============================================================================
-- OwnLens - Company Domain Schema (ClickHouse)
-- ============================================================================
-- Company analytics: internal communications, company content, department analytics
-- ============================================================================

USE ownlens_analytics;

-- Company departments
CREATE TABLE IF NOT EXISTS company_departments (
    department_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    department_name String,
    department_code String,  -- Unique identifier within company
    parent_department_id String,  -- UUID as String (for hierarchy)
    description String,
    primary_country_code String,  -- ISO 3166-1 alpha-2
    primary_city_id String,  -- UUID as String
    office_address String,
    employee_count UInt32 DEFAULT 0,
    budget Float64,
    is_active UInt8 DEFAULT 1,  -- Boolean
    established_date Date,
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (company_id, department_code)
SETTINGS index_granularity = 8192;

-- Company employees (links to users table)
CREATE TABLE IF NOT EXISTS company_employees (
    employee_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    department_id String,  -- UUID as String
    employee_number String,  -- Employee ID number
    job_title String,
    job_level String,  -- 'junior', 'mid', 'senior', 'lead', 'manager', 'director', 'executive'
    employment_type String,  -- 'full_time', 'part_time', 'contract', 'intern'
    hire_date Date,
    termination_date Date,
    is_active UInt8 DEFAULT 1,  -- Boolean
    manager_id String,  -- UUID as String
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id, company_id)
SETTINGS index_granularity = 8192;

-- Internal company content (announcements, newsletters, internal articles)
CREATE TABLE IF NOT EXISTS company_internal_content (
    content_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    department_id String,  -- UUID as String
    title String,
    content_type String,  -- 'announcement', 'newsletter', 'internal_article', 'policy', 'update', 'event'
    content_body String,  -- TEXT as String
    author_id String,  -- UUID as String
    publish_date Date,
    expiry_date Date,
    is_published UInt8 DEFAULT 0,  -- Boolean
    is_active UInt8 DEFAULT 1,  -- Boolean
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (content_id)
SETTINGS index_granularity = 8192;

-- Company content events (internal content events from Kafka)
CREATE TABLE IF NOT EXISTS company_content_events (
    event_id String,  -- UUID as String
    content_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    employee_id String,  -- UUID as String
    user_id String,  -- UUID as String
    
    -- Event information
    event_type String,  -- 'content_view', 'content_click', 'share', etc.
    event_timestamp DateTime,
    
    -- Employee information
    department_id String,  -- UUID as String
    job_level String,
    
    -- Geographic information
    country_code String,
    city_id String,  -- UUID as String
    
    -- Device information
    device_type_id String,  -- UUID as String
    os_id String,  -- UUID as String
    browser_id String,  -- UUID as String
    
    -- Engagement metrics
    engagement_metrics String CODEC(ZSTD(3)),  -- JSON
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now(),
    
    -- Partitioning by date (materialized column for automatic population)
    event_date Date MATERIALIZED toDate(event_timestamp)
) ENGINE = MergeTree()
ORDER BY (event_timestamp, company_id, brand_id, content_id, event_type, event_id)
PARTITION BY toYYYYMM(event_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

-- Company content performance (aggregated by date)
CREATE TABLE IF NOT EXISTS company_content_performance (
    performance_id String,  -- UUID as String
    content_id String,  -- UUID as String
    company_id String,  -- UUID as String
    performance_date Date,
    
    -- View metrics
    total_views UInt32 DEFAULT 0,
    unique_views UInt32 DEFAULT 0,
    unique_employees UInt32 DEFAULT 0,
    
    -- Engagement metrics
    total_clicks UInt32 DEFAULT 0,
    total_shares UInt32 DEFAULT 0,
    total_comments UInt32 DEFAULT 0,
    total_likes UInt32 DEFAULT 0,
    total_bookmarks UInt32 DEFAULT 0,
    total_downloads UInt32 DEFAULT 0,
    
    -- Reading metrics
    avg_time_on_page_sec UInt32,
    avg_scroll_depth UInt8,  -- Percentage
    completion_rate Float32,
    
    -- Department breakdown (JSON)
    views_by_department String,  -- JSON
    engagement_by_department String,  -- JSON
    
    -- Geographic breakdown (JSON)
    views_by_country String,  -- JSON
    views_by_city String,  -- JSON
    
    -- Employee level breakdown (JSON)
    views_by_level String,  -- JSON
    
    -- Engagement score
    engagement_score Float32,
    reach_percentage Float32,
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (performance_date, content_id, company_id)
PARTITION BY toYYYYMM(performance_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Department performance (aggregated by date)
CREATE TABLE IF NOT EXISTS company_department_performance (
    performance_id String,  -- UUID as String
    department_id String,  -- UUID as String
    company_id String,  -- UUID as String
    performance_date Date,
    
    -- Content metrics
    content_published UInt16 DEFAULT 0,
    content_views UInt32 DEFAULT 0,
    content_engagement UInt32 DEFAULT 0,
    avg_engagement_score Float32,
    
    -- Employee engagement
    active_employees UInt32 DEFAULT 0,
    employee_engagement_rate Float32,
    avg_views_per_employee Float32,
    
    -- Top content
    top_content_id String,  -- UUID as String
    top_content_views UInt32,
    top_content_engagement Float32,
    
    -- Geographic metrics (JSON)
    views_by_country String,  -- JSON
    views_by_city String,  -- JSON
    
    -- Ranking
    department_rank UInt16,
    engagement_rank UInt16,
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (performance_date, department_id, company_id)
PARTITION BY toYYYYMM(performance_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Employee engagement (aggregated by date)
CREATE TABLE IF NOT EXISTS company_employee_engagement (
    engagement_id String,  -- UUID as String
    employee_id String,  -- UUID as String
    company_id String,  -- UUID as String
    department_id String,  -- UUID as String
    engagement_date Date,
    
    -- Engagement metrics
    content_views UInt32 DEFAULT 0,
    content_interactions UInt32 DEFAULT 0,
    avg_engagement_score Float32,
    time_spent_sec UInt32 DEFAULT 0,
    
    -- Content preferences
    preferred_content_types String,  -- JSON array
    preferred_categories String,  -- JSON array
    
    -- Activity metrics
    active_days UInt16 DEFAULT 0,
    first_interaction_time DateTime,
    last_interaction_time DateTime,
    
    -- Engagement level
    engagement_level String,  -- 'low', 'medium', 'high', 'very_high'
    engagement_score Float32,
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (engagement_date, employee_id, company_id)
PARTITION BY toYYYYMM(engagement_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Internal communications analytics (aggregated by date)
CREATE TABLE IF NOT EXISTS company_communications_analytics (
    analytics_id String,  -- UUID as String
    company_id String,  -- UUID as String
    analytics_date Date,
    
    -- Content metrics
    total_content_published UInt32 DEFAULT 0,
    total_content_views UInt32 DEFAULT 0,
    unique_employees_reached UInt32 DEFAULT 0,
    reach_percentage Float32,
    
    -- Engagement metrics
    total_engagement UInt32 DEFAULT 0,
    avg_engagement_score Float32,
    engagement_rate Float32,
    
    -- Department metrics
    active_departments UInt16 DEFAULT 0,
    top_department_id String,  -- UUID as String
    
    -- Content type breakdown (JSON)
    views_by_content_type String,  -- JSON
    engagement_by_content_type String,  -- JSON
    
    -- Geographic breakdown (JSON)
    views_by_country String,  -- JSON
    views_by_city String,  -- JSON
    
    -- Employee level breakdown (JSON)
    views_by_level String,  -- JSON
    engagement_by_level String,  -- JSON
    
    -- Trends
    views_growth_rate Float32,
    engagement_growth_rate Float32,
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (analytics_date, company_id)
PARTITION BY toYYYYMM(analytics_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

