-- ============================================================================
-- OwnLens - Customer Domain Schema (ClickHouse)
-- ============================================================================
-- Customer analytics: user behavior, sessions, events, engagement, ML features
-- ============================================================================

USE ownlens_analytics;

-- Customer sessions (aggregated session-level metrics)
CREATE TABLE IF NOT EXISTS customer_sessions (
    session_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    account_id String,  -- UUID as String
    
    -- Geographic information
    country_code String,
    city_id String,  -- UUID as String
    timezone String,
    
    -- Device information
    device_type_id String,  -- UUID as String
    os_id String,  -- UUID as String
    browser_id String,  -- UUID as String
    
    -- Session metadata
    referrer String,
    user_segment String,  -- 'power_user', 'engaged', 'casual', 'new_visitor', 'returning', 'subscriber'
    subscription_tier String,  -- 'free', 'premium', 'pro', 'enterprise'
    
    -- Temporal metrics
    session_start DateTime,
    session_end DateTime,
    session_duration_sec UInt32,
    
    -- Event counts
    total_events UInt16 DEFAULT 0,
    article_views UInt16 DEFAULT 0,
    article_clicks UInt16 DEFAULT 0,
    video_plays UInt16 DEFAULT 0,
    newsletter_signups UInt16 DEFAULT 0,
    ad_clicks UInt16 DEFAULT 0,
    searches UInt16 DEFAULT 0,
    
    -- Page metrics
    pages_visited_count UInt16 DEFAULT 0,
    unique_pages_count UInt16 DEFAULT 0,
    unique_categories_count UInt16 DEFAULT 0,
    
    -- JSON fields for detailed data
    categories_visited String CODEC(ZSTD(3)),  -- JSON array
    article_ids_visited String CODEC(ZSTD(3)),  -- JSON array
    article_titles_visited String CODEC(ZSTD(3)),  -- JSON array
    page_events String CODEC(ZSTD(3)),  -- JSON array
    
    -- Engagement metrics
    engagement_score Float32,
    scroll_depth_avg UInt8,  -- Percentage
    time_on_page_avg UInt32,  -- Seconds
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    batch_id String  -- UUID as String
) ENGINE = MergeTree()
ORDER BY (session_start, company_id, brand_id, user_id, session_id)
PARTITION BY toYYYYMM(session_start)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Customer events (raw events from Kafka)
CREATE TABLE IF NOT EXISTS customer_events (
    event_id String,  -- UUID as String
    session_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Event information
    event_type String,  -- 'article_view', 'article_click', 'video_play', etc.
    event_timestamp DateTime,
    
    -- Content information
    article_id String,
    article_title String,
    article_type String,
    category_id String,  -- UUID as String
    page_url String,
    
    -- Geographic information
    country_code String,
    city_id String,  -- UUID as String
    timezone String,
    
    -- Device information
    device_type_id String,  -- UUID as String
    os_id String,  -- UUID as String
    browser_id String,  -- UUID as String
    
    -- Engagement metrics
    engagement_metrics String CODEC(ZSTD(3)),  -- JSON
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now(),
    
    -- Partitioning by date
    event_date Date MATERIALIZED toDate(event_timestamp)
) ENGINE = MergeTree()
ORDER BY (event_timestamp, company_id, brand_id, event_type, event_id)
PARTITION BY toYYYYMM(event_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

-- Customer user features (ML-ready features)
CREATE TABLE IF NOT EXISTS customer_user_features (
    user_feature_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    feature_date Date,
    
    -- Behavioral features
    total_sessions UInt32 DEFAULT 0,
    avg_session_duration_sec Float32,
    min_session_duration_sec UInt32,
    max_session_duration_sec UInt32,
    std_session_duration_sec Float32,
    avg_events_per_session Float32,
    avg_pages_per_session Float32,
    avg_categories_diversity Float32,
    
    -- Engagement features
    avg_article_views Float32,
    avg_article_clicks Float32,
    avg_video_plays Float32,
    avg_searches Float32,
    total_newsletter_signups UInt16 DEFAULT 0,
    total_ad_clicks UInt16 DEFAULT 0,
    
    -- Engagement score
    avg_engagement_score Float32,
    max_engagement_score Float32,
    min_engagement_score Float32,
    
    -- Conversion features
    has_newsletter UInt8 DEFAULT 0,  -- Boolean
    newsletter_signup_rate Float32,
    
    -- Content features
    preferred_brand_id String,  -- UUID as String
    preferred_category_id String,  -- UUID as String
    preferred_device_type_id String,  -- UUID as String
    preferred_os_id String,  -- UUID as String
    preferred_browser_id String,  -- UUID as String
    
    -- Geographic features
    primary_country_code String,
    primary_city_id String,  -- UUID as String
    
    -- Subscription features
    current_subscription_tier String,
    current_user_segment String,
    
    -- Temporal features
    first_session_date Date,
    last_session_date Date,
    days_active UInt16,
    
    -- Recency features
    days_since_last_session UInt16,
    active_today UInt8 DEFAULT 0,  -- Boolean
    active_last_7_days UInt8 DEFAULT 0,  -- Boolean
    active_last_30_days UInt8 DEFAULT 0,  -- Boolean
    
    -- Churn prediction features
    churn_risk_score Float32,
    churn_probability Float32,
    
    -- Additional ML features
    ml_features String CODEC(ZSTD(3)),  -- JSON
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (feature_date, company_id, brand_id, user_id)
PARTITION BY toYYYYMM(feature_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- User segments (from ML clustering)
CREATE TABLE IF NOT EXISTS customer_user_segments (
    segment_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    segment_name String,
    segment_code String,
    cluster_number UInt16,
    description String,
    user_count UInt32 DEFAULT 0,
    avg_engagement_score Float32,
    avg_sessions Float32,
    avg_duration_sec Float32,
    newsletter_signup_rate Float32,
    model_version String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (segment_id)
SETTINGS index_granularity = 8192;

-- User segment assignments
CREATE TABLE IF NOT EXISTS customer_user_segment_assignments (
    assignment_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    segment_id String,  -- UUID as String
    assignment_date Date,
    confidence_score Float32,
    model_version String,
    is_current UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (company_id, brand_id, user_id, is_current, assignment_date)
PARTITION BY toYYYYMM(assignment_date)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Churn predictions
CREATE TABLE IF NOT EXISTS customer_churn_predictions (
    prediction_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    prediction_date Date,
    
    -- Prediction results
    churn_probability Float32,
    churn_risk_level String,  -- 'low', 'medium', 'high', 'critical'
    is_churned UInt8 DEFAULT 0,  -- Boolean
    churned_date Date,
    
    -- Model information
    model_version String,
    model_confidence Float32,
    feature_importance String CODEC(ZSTD(3)),  -- JSON
    
    -- Key features used
    days_since_last_session UInt16,
    total_sessions UInt32,
    avg_engagement_score Float32,
    subscription_tier String,
    
    -- Metadata
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (prediction_date, company_id, brand_id, user_id)
PARTITION BY toYYYYMM(prediction_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Content recommendations
CREATE TABLE IF NOT EXISTS customer_recommendations (
    recommendation_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    article_id String,
    category_id String,  -- UUID as String
    
    -- Recommendation details
    recommendation_type String,  -- 'collaborative', 'content_based', 'hybrid'
    recommendation_score Float32,
    rank_position UInt16,
    
    -- Model information
    model_version String,
    algorithm String,  -- 'NMF', 'content_based', 'hybrid'
    
    -- User interaction
    was_shown UInt8 DEFAULT 0,  -- Boolean
    was_clicked UInt8 DEFAULT 0,  -- Boolean
    was_viewed UInt8 DEFAULT 0,  -- Boolean
    clicked_at DateTime,
    viewed_at DateTime,
    
    -- Metadata
    created_at DateTime DEFAULT now(),
    expires_at DateTime
) ENGINE = MergeTree()
ORDER BY (company_id, brand_id, user_id, recommendation_score, created_at)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 6 MONTH
SETTINGS index_granularity = 8192;

-- Conversion predictions
CREATE TABLE IF NOT EXISTS customer_conversion_predictions (
    prediction_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    prediction_date Date,
    
    -- Prediction results
    conversion_probability Float32,
    conversion_tier String,
    conversion_value Float32,
    
    -- Model information
    model_version String,
    model_confidence Float32,
    feature_importance String CODEC(ZSTD(3)),  -- JSON
    
    -- Actual conversion (for validation)
    did_convert UInt8 DEFAULT 0,  -- Boolean
    converted_at DateTime,
    actual_tier String,
    
    -- Metadata
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (prediction_date, company_id, brand_id, user_id)
PARTITION BY toYYYYMM(prediction_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Daily session summary (materialized view)
CREATE TABLE IF NOT EXISTS customer_session_summary_daily (
    date Date,
    brand_id String,  -- UUID as String
    country_code String,
    device_type_id String,  -- UUID as String
    subscription_tier String,
    user_segment String,
    
    total_sessions UInt32,
    total_users UInt32,
    avg_session_duration_sec Float32,
    avg_events_per_session Float32,
    total_article_views UInt64,
    total_video_plays UInt64,
    total_newsletter_signups UInt64,
    total_ad_clicks UInt64,
    
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (date, brand_id, country_code, device_type_id, subscription_tier, user_segment)
PARTITION BY toYYYYMM(date)
TTL created_at + INTERVAL 1 YEAR;

