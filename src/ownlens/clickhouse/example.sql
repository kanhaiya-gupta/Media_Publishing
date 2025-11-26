-- ClickHouse schema for Media Publishing session analytics
-- This will be executed when ClickHouse starts

CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

-- Session metrics table for ML Analytics with comprehensive event tracking
CREATE TABLE IF NOT EXISTS session_metrics (
    session_id String,  -- UUID for session tracking
    user_id UInt32,
    brand String,  -- bild, welt, business_insider, politico, sport_bild
    country String,
    city String,
    device_type String,  -- desktop, mobile, tablet, smart_tv, app
    device_os String,  -- iOS, Android, Windows, macOS, Linux
    browser String,  -- Chrome, Safari, Firefox, Edge, Opera
    subscription_tier String,  -- free, premium, pro, enterprise
    user_segment String,  -- power_user, engaged, casual, new_visitor, returning, subscriber
    referrer String,  -- direct, google, facebook, twitter, newsletter, internal, etc.
    timezone String,
    session_start DateTime,
    session_end DateTime,
    session_duration_sec UInt32,
    total_events UInt16,
    -- Engagement metrics
    article_views UInt16,
    article_clicks UInt16,
    video_plays UInt16,
    newsletter_signups UInt16,
    ad_clicks UInt16,
    searches UInt16,
    -- Page metrics
    pages_visited_count UInt16,
    unique_pages_count UInt16,
    unique_categories_count UInt16,
    -- JSON fields for detailed data
    categories_visited String,  -- JSON array of categories
    article_ids_visited String,  -- JSON array of article IDs
    article_titles_visited String,  -- JSON array of article titles
    page_events String,  -- JSON array of all page events with metadata
    -- Metadata
    created_at DateTime DEFAULT now(),
    batch_id UInt64
) ENGINE = MergeTree()
ORDER BY (session_id, session_start)
PARTITION BY toYYYYMM(session_start)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Materialized view for brand-level analytics
CREATE TABLE IF NOT EXISTS brand_analytics (
    date Date,
    brand String,
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
ORDER BY (date, brand)
PARTITION BY toYYYYMM(date)
TTL created_at + INTERVAL 1 YEAR;

-- Materialized view for geographic analytics
CREATE TABLE IF NOT EXISTS geographic_analytics (
    date Date,
    country String,
    city String,
    total_sessions UInt32,
    total_users UInt32,
    avg_session_duration_sec Float32,
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (date, country, city)
PARTITION BY toYYYYMM(date)
TTL created_at + INTERVAL 1 YEAR;

-- Materialized view for device analytics
CREATE TABLE IF NOT EXISTS device_analytics (
    date Date,
    device_type String,
    device_os String,
    browser String,
    total_sessions UInt32,
    total_users UInt32,
    avg_session_duration_sec Float32,
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (date, device_type, device_os, browser)
PARTITION BY toYYYYMM(date)
TTL created_at + INTERVAL 1 YEAR;

-- Materialized view for subscription analytics
CREATE TABLE IF NOT EXISTS subscription_analytics (
    date Date,
    subscription_tier String,
    user_segment String,
    total_sessions UInt32,
    total_users UInt32,
    avg_session_duration_sec Float32,
    avg_events_per_session Float32,
    conversion_rate Float32,  -- newsletter signups / total sessions
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (date, subscription_tier, user_segment)
PARTITION BY toYYYYMM(date)
TTL created_at + INTERVAL 1 YEAR;

-- Materialized view for page-level analytics (for ML features)
CREATE TABLE IF NOT EXISTS page_analytics (
    user_id UInt32,
    session_id String,
    article_id String,
    article_title String,
    category String,
    visit_count UInt16,
    event_type String,  -- article_view, article_click, etc.
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, session_id, article_id, created_at)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 1 YEAR;

-- Summary table for quick analytics queries
CREATE TABLE IF NOT EXISTS session_summary (
    date Date,
    total_sessions UInt32,
    total_users UInt32,
    avg_session_duration_sec Float32,
    avg_events_per_session Float32,
    total_article_views UInt64,
    total_video_plays UInt64,
    total_newsletter_signups UInt64,
    created_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (date)
PARTITION BY toYYYYMM(date)
TTL created_at + INTERVAL 1 YEAR;
