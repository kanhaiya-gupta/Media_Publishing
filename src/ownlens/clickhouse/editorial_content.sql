-- ============================================================================
-- OwnLens - Editorial Content Schema (ClickHouse)
-- ============================================================================
-- Article content analytics and versioning metrics
-- Note: Full content is stored in PostgreSQL, this is for analytics only
-- ============================================================================

USE ownlens_analytics;

-- Article content analytics (aggregated metrics per content version)
CREATE TABLE IF NOT EXISTS editorial_article_content (
    content_id String,  -- UUID as String
    article_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    content_version UInt16,
    
    -- Content details
    content_format String,  -- 'html', 'markdown', 'json', 'plain_text'
    content_status String,  -- 'draft', 'review', 'approved', 'published', 'archived'
    is_current_version UInt8 DEFAULT 0,  -- Boolean
    is_published_version UInt8 DEFAULT 0,  -- Boolean
    
    -- Content metrics
    word_count UInt32,
    character_count UInt32,
    reading_time_minutes UInt16,
    content_language_code String,
    
    -- Versioning
    previous_version_id String,  -- UUID as String
    version_created_by String,  -- UUID as String
    
    -- Timestamps
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    published_at DateTime,
    
    -- Metadata
    metadata String  -- JSON
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (content_id, content_version)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Content version history (analytics)
CREATE TABLE IF NOT EXISTS editorial_content_versions (
    version_id String,  -- UUID as String
    article_id String,  -- UUID as String
    content_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Version details
    version_number UInt16,
    version_type String,  -- 'draft', 'revision', 'correction', 'update', 'republish'
    version_status String,  -- 'draft', 'review', 'approved', 'published', 'rejected'
    
    -- Version metadata
    version_name String,
    version_description String,
    change_summary String,
    
    -- Changes tracking
    changed_fields String,  -- JSON array
    changes_json String,  -- JSON
    
    -- Who and when
    created_by String,  -- UUID as String
    reviewed_by String,  -- UUID as String
    approved_by String,  -- UUID as String
    published_by String,  -- UUID as String
    
    -- Timestamps
    created_at DateTime DEFAULT now(),
    reviewed_at DateTime,
    approved_at DateTime,
    published_at DateTime,
    
    -- Version comparison
    previous_version_id String,  -- UUID as String
    diff_summary String,
    
    -- Metadata
    metadata String  -- JSON
) ENGINE = MergeTree()
ORDER BY (version_id, created_at)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

