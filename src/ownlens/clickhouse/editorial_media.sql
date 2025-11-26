-- ============================================================================
-- OwnLens - Editorial Media Assets Schema (ClickHouse)
-- ============================================================================
-- Media asset analytics and usage tracking
-- Note: Media files are stored in object storage, this is for analytics only
-- ============================================================================

USE ownlens_analytics;

-- Media assets reference (for analytics joins)
CREATE TABLE IF NOT EXISTS editorial_media_assets (
    media_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    media_name String,
    media_code String,
    external_media_id String,
    
    -- Media type
    media_type String,  -- 'image', 'video', 'document', 'audio', 'other'
    media_category String,
    
    -- File information
    original_filename String,
    file_extension String,
    mime_type String,
    file_size_bytes UInt64,
    file_hash String,
    
    -- Object storage paths
    storage_type String,  -- 's3', 'minio', 'local', 'cdn'
    storage_bucket String,
    storage_path String,
    storage_url String,
    
    -- Image-specific metadata
    image_width UInt16,
    image_height UInt16,
    image_aspect_ratio Float32,
    image_color_space String,
    image_dpi UInt16,
    image_orientation String,
    
    -- Video-specific metadata
    video_duration_sec UInt32,
    video_duration_formatted String,
    video_width UInt16,
    video_height UInt16,
    video_frame_rate Float32,
    video_codec String,
    video_bitrate UInt32,
    video_has_audio UInt8 DEFAULT 0,  -- Boolean
    video_audio_codec String,
    
    -- Document-specific metadata
    document_page_count UInt16,
    document_word_count UInt32,
    document_language_code String,
    
    -- Media content
    title String,
    description String,
    alt_text String,
    caption String,
    credit String,
    copyright String,
    license_type String,
    
    -- SEO and metadata
    keywords String,  -- JSON array
    tags String,  -- JSON array
    metadata String,  -- JSON
    
    -- Relationships
    uploaded_by String,  -- UUID as String
    author_id String,  -- UUID as String
    category_id String,  -- UUID as String
    
    -- Status
    is_active UInt8 DEFAULT 1,  -- Boolean
    is_public UInt8 DEFAULT 1,  -- Boolean
    is_featured UInt8 DEFAULT 0,  -- Boolean
    
    -- Timestamps
    uploaded_at DateTime DEFAULT now(),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    last_used_at DateTime,
    
    -- Usage tracking
    usage_count UInt32 DEFAULT 0,
    view_count UInt32 DEFAULT 0,
    download_count UInt32 DEFAULT 0
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (media_id)
SETTINGS index_granularity = 8192;

-- Media usage tracking (aggregated by date)
CREATE TABLE IF NOT EXISTS editorial_media_usage (
    usage_id String,  -- UUID as String
    media_id String,  -- UUID as String
    article_id String,  -- UUID as String
    usage_date Date,
    
    -- Usage metrics
    view_count UInt32 DEFAULT 0,
    click_count UInt32 DEFAULT 0,
    download_count UInt32 DEFAULT 0,
    share_count UInt32 DEFAULT 0,
    
    -- Engagement metrics
    avg_view_duration_sec UInt32,
    completion_rate Float32,
    
    -- Context
    brand_id String,  -- UUID as String
    
    -- Metadata
    metadata String,  -- JSON
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
ORDER BY (media_id, article_id, usage_date)
PARTITION BY toYYYYMM(usage_date)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Media variants (thumbnails, different sizes, formats)
CREATE TABLE IF NOT EXISTS editorial_media_variants (
    variant_id String,  -- UUID as String
    media_id String,  -- UUID as String
    variant_type String,  -- 'thumbnail', 'small', 'medium', 'large', 'original', 'webp', 'avif', 'preview'
    variant_size String,  -- '100x100', '500x500', '1920x1080', etc.
    variant_format String,  -- 'jpg', 'png', 'webp', 'avif', 'mp4', etc.
    file_size_bytes UInt64,
    file_hash String,
    storage_path String,
    storage_url String,
    width UInt16,
    height UInt16,
    aspect_ratio Float32,
    quality_level String,  -- 'low', 'medium', 'high', 'original'
    compression_ratio Float32,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (media_id, variant_type)
SETTINGS index_granularity = 8192;

-- Media collections (galleries, playlists, etc.)
CREATE TABLE IF NOT EXISTS editorial_media_collections (
    collection_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    collection_name String,
    collection_type String,  -- 'gallery', 'playlist', 'album', 'folder'
    description String,
    media_count UInt32 DEFAULT 0,
    cover_media_id String,  -- UUID as String
    is_active UInt8 DEFAULT 1,  -- Boolean
    is_public UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    created_by String  -- UUID as String
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (collection_id)
SETTINGS index_granularity = 8192;

-- Media collection items
CREATE TABLE IF NOT EXISTS editorial_media_collection_items (
    item_id String,  -- UUID as String
    collection_id String,  -- UUID as String
    media_id String,  -- UUID as String
    position_in_collection UInt16,
    item_title String,
    item_description String,
    added_at DateTime DEFAULT now(),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (collection_id, position_in_collection)
SETTINGS index_granularity = 8192;

-- Content-media relationships (for analytics)
CREATE TABLE IF NOT EXISTS editorial_content_media (
    relationship_id String,  -- UUID as String
    article_id String,  -- UUID as String
    media_id String,  -- UUID as String
    content_id String,  -- UUID as String
    
    -- Relationship details
    relationship_type String,  -- 'featured_image', 'inline_image', 'gallery_image', etc.
    position_in_content UInt16,
    position_in_section String,
    
    -- Media usage in content
    media_context String,
    media_caption String,
    media_alt_text String,
    is_featured UInt8 DEFAULT 0,  -- Boolean
    is_required UInt8 DEFAULT 0,  -- Boolean
    
    -- Display settings
    display_size String,
    alignment String,
    wrap_text UInt8 DEFAULT 0,  -- Boolean
    
    -- Timestamps
    linked_at DateTime DEFAULT now(),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    
    -- Metadata
    metadata String  -- JSON
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (relationship_id)
SETTINGS index_granularity = 8192;

