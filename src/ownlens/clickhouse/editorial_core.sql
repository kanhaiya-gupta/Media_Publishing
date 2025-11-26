-- ============================================================================
-- OwnLens - Editorial Core Schema (ClickHouse)
-- ============================================================================
-- Core editorial: articles metadata, authors, performance analytics
-- ============================================================================

USE ownlens_analytics;

-- Authors reference (for analytics joins)
CREATE TABLE IF NOT EXISTS editorial_authors (
    author_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    author_name String,
    author_code String,
    email String,
    department String,
    role String,
    primary_country_code String,
    primary_city_id String,  -- UUID as String
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (author_id)
SETTINGS index_granularity = 8192;

-- Articles reference (for analytics joins)
CREATE TABLE IF NOT EXISTS editorial_articles (
    article_id String,  -- UUID as String
    external_article_id String,
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    title String,
    headline String,
    subtitle String,
    summary String,
    content_url String,
    article_type String,  -- 'news', 'opinion', 'analysis', etc.
    primary_author_id String,  -- UUID as String
    primary_category_id String,  -- UUID as String
    word_count UInt32,
    reading_time_minutes UInt16,
    image_count UInt16 DEFAULT 0,
    video_count UInt16 DEFAULT 0,
    publish_time DateTime,
    publish_date Date,
    status String,  -- 'draft', 'scheduled', 'published', etc.
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (article_id)
PARTITION BY toYYYYMM(publish_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Editorial content events (raw events from Kafka)
CREATE TABLE IF NOT EXISTS editorial_content_events (
    event_id String,  -- UUID as String
    article_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    user_id String,  -- UUID as String (nullable for anonymous)
    
    -- Event information
    event_type String,  -- 'article_view', 'article_click', 'share', etc.
    event_timestamp DateTime,
    
    -- Content information
    category_id String,  -- UUID as String
    author_id String,  -- UUID as String
    
    -- Geographic information
    country_code String,
    city_id String,  -- UUID as String
    
    -- Device information
    device_type_id String,  -- UUID as String
    os_id String,  -- UUID as String
    browser_id String,  -- UUID as String
    
    -- Referral information
    referrer String,
    referrer_type String,  -- 'direct', 'search', 'social', etc.
    
    -- Engagement metrics
    engagement_metrics String CODEC(ZSTD(3)),  -- JSON
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now(),
    
    -- Partitioning by date (materialized column for automatic population)
    event_date Date MATERIALIZED toDate(event_timestamp)
) ENGINE = MergeTree()
ORDER BY (event_timestamp, brand_id, article_id, event_type, event_id)
PARTITION BY toYYYYMM(event_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

-- Article performance (aggregated by date)
CREATE TABLE IF NOT EXISTS editorial_article_performance (
    performance_id String,  -- UUID as String
    article_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    performance_date Date,
    
    -- View metrics
    total_views UInt32 DEFAULT 0,
    unique_views UInt32 DEFAULT 0,
    unique_readers UInt32 DEFAULT 0,
    
    -- Engagement metrics
    total_clicks UInt32 DEFAULT 0,
    total_shares UInt32 DEFAULT 0,
    total_comments UInt32 DEFAULT 0,
    total_likes UInt32 DEFAULT 0,
    total_bookmarks UInt32 DEFAULT 0,
    
    -- Reading metrics
    avg_time_on_page_sec UInt32,
    avg_scroll_depth UInt8,  -- Percentage
    completion_rate Float32,
    bounce_rate Float32,
    
    -- Video metrics
    video_plays UInt32 DEFAULT 0,
    video_completions UInt32 DEFAULT 0,
    avg_video_watch_time_sec UInt32,
    
    -- Referral metrics
    direct_views UInt32 DEFAULT 0,
    search_views UInt32 DEFAULT 0,
    social_views UInt32 DEFAULT 0,
    newsletter_views UInt32 DEFAULT 0,
    internal_views UInt32 DEFAULT 0,
    
    -- Geographic metrics (JSON)
    top_countries String CODEC(ZSTD(3)),  -- JSON
    top_cities String CODEC(ZSTD(3)),  -- JSON
    
    -- Device metrics
    desktop_views UInt32 DEFAULT 0,
    mobile_views UInt32 DEFAULT 0,
    tablet_views UInt32 DEFAULT 0,
    
    -- Engagement score
    engagement_score Float32,
    quality_score Float32,
    
    -- Revenue metrics
    ad_revenue Float32 DEFAULT 0,
    subscription_conversions UInt16 DEFAULT 0,
    newsletter_signups UInt16 DEFAULT 0,
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (performance_date, article_id, brand_id)
PARTITION BY toYYYYMM(performance_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Author performance (aggregated by date)
CREATE TABLE IF NOT EXISTS editorial_author_performance (
    performance_id String,  -- UUID as String
    author_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    performance_date Date,
    
    -- Publishing metrics
    articles_published UInt16 DEFAULT 0,
    articles_scheduled UInt16 DEFAULT 0,
    articles_draft UInt16 DEFAULT 0,
    
    -- View metrics
    total_views UInt32 DEFAULT 0,
    unique_readers UInt32 DEFAULT 0,
    avg_views_per_article Float32,
    
    -- Engagement metrics
    total_engagement UInt32 DEFAULT 0,
    avg_engagement_score Float32,
    total_shares UInt32 DEFAULT 0,
    total_comments UInt32 DEFAULT 0,
    
    -- Best performing article
    best_article_id String,  -- UUID as String
    best_article_views UInt32,
    best_article_engagement Float32,
    
    -- Category performance
    top_category_id String,  -- UUID as String
    top_category_views UInt32,
    
    -- Ranking
    author_rank UInt16,
    engagement_rank UInt16,
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (performance_date, author_id, brand_id)
PARTITION BY toYYYYMM(performance_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Category performance (aggregated by date)
CREATE TABLE IF NOT EXISTS editorial_category_performance (
    performance_id String,  -- UUID as String
    category_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    performance_date Date,
    
    -- Publishing metrics
    articles_published UInt16 DEFAULT 0,
    articles_scheduled UInt16 DEFAULT 0,
    
    -- View metrics
    total_views UInt32 DEFAULT 0,
    unique_readers UInt32 DEFAULT 0,
    avg_views_per_article Float32,
    
    -- Engagement metrics
    total_engagement UInt32 DEFAULT 0,
    avg_engagement_score Float32,
    engagement_growth_rate Float32,
    
    -- Trending status
    is_trending UInt8 DEFAULT 0,  -- Boolean
    trending_score Float32,
    trending_rank UInt16,
    
    -- Peak engagement times
    peak_hour UInt8,  -- 0-23
    peak_day_of_week UInt8,  -- 0-6 (Sunday=0)
    
    -- Best performing article
    best_article_id String,  -- UUID as String
    best_article_views UInt32,
    
    -- Top authors
    top_author_ids String CODEC(ZSTD(3)),  -- JSON array of UUIDs
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (performance_date, category_id, brand_id)
PARTITION BY toYYYYMM(performance_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Headline A/B tests
CREATE TABLE IF NOT EXISTS editorial_headline_tests (
    test_id String,  -- UUID as String
    article_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Headline variants
    headline_variant_a String,
    headline_variant_b String,
    headline_variant_c String,
    
    -- Test configuration
    test_start_time DateTime,
    test_end_time DateTime,
    traffic_split String,  -- JSON
    
    -- Results
    variant_a_views UInt32 DEFAULT 0,
    variant_b_views UInt32 DEFAULT 0,
    variant_c_views UInt32 DEFAULT 0,
    variant_a_clicks UInt32 DEFAULT 0,
    variant_b_clicks UInt32 DEFAULT 0,
    variant_c_clicks UInt32 DEFAULT 0,
    variant_a_ctr Float32,
    variant_b_ctr Float32,
    variant_c_ctr Float32,
    
    -- Winner
    winning_variant String,  -- 'a', 'b', or 'c'
    statistical_significance Float32,
    
    -- Model predictions
    predicted_ctr_a Float32,
    predicted_ctr_b Float32,
    predicted_ctr_c Float32,
    model_version String,
    
    -- Status
    test_status String,  -- 'active', 'completed', 'cancelled'
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (test_id)
SETTINGS index_granularity = 8192;

-- Trending topics
CREATE TABLE IF NOT EXISTS editorial_trending_topics (
    topic_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    topic_name String,
    topic_keywords String,  -- JSON array
    
    -- Trending metrics
    trending_score Float32,
    engagement_score Float32,
    growth_rate Float32,
    articles_count UInt32 DEFAULT 0,
    
    -- Time period
    period_start DateTime,
    period_end DateTime,
    period_type String,  -- 'hour', 'day', 'week', 'month'
    
    -- Related categories
    related_category_ids String,  -- JSON array
    
    -- Top articles
    top_article_ids String,  -- JSON array
    
    -- Ranking
    trend_rank UInt16,
    
    -- Metadata
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (topic_id, period_start)
PARTITION BY toYYYYMM(period_start)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Content recommendations (from ML models)
CREATE TABLE IF NOT EXISTS editorial_content_recommendations (
    recommendation_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    recommendation_type String,  -- 'topic', 'category', 'author', 'publish_time'
    
    -- Recommendation details
    recommended_topic String,
    recommended_category_id String,  -- UUID as String
    recommended_author_id String,  -- UUID as String
    recommended_publish_time DateTime,
    
    -- Recommendation metrics
    recommendation_score Float32,
    estimated_engagement Float32,
    estimated_views UInt32,
    
    -- Reasoning
    reasoning String,
    similar_successful_content String,  -- JSON
    
    -- Model information
    model_version String,
    algorithm String,
    
    -- Status
    status String,  -- 'pending', 'accepted', 'rejected', 'implemented'
    implemented_at DateTime,
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    expires_at DateTime
) ENGINE = MergeTree()
ORDER BY (brand_id, recommendation_score, created_at)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 6 MONTH
SETTINGS index_granularity = 8192;

-- Daily article performance summary
CREATE TABLE IF NOT EXISTS editorial_article_performance_summary_daily (
    date Date,
    brand_id String,  -- UUID as String
    category_id String,  -- UUID as String
    
    total_articles UInt32,
    total_views UInt64,
    total_engagement UInt64,
    avg_engagement_score Float32,
    top_article_id String,  -- UUID as String
    top_article_views UInt32,
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (date, brand_id, category_id)
PARTITION BY toYYYYMM(date)
TTL created_at + INTERVAL 1 YEAR;

