-- ============================================================================
-- OwnLens - Editorial Media Assets Schema
-- ============================================================================
-- Media asset management (images, videos, documents) with object storage
-- ============================================================================

-- ============================================================================
-- MEDIA ASSETS
-- ============================================================================

-- Media assets table (images, videos, documents)
CREATE TABLE IF NOT EXISTS editorial_media_assets (
    media_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Media identification
    media_name VARCHAR(255) NOT NULL,
    media_code VARCHAR(100) UNIQUE, -- Unique identifier for media
    external_media_id VARCHAR(255), -- External system media ID
    
    -- Media type
    media_type VARCHAR(50) NOT NULL, -- 'image', 'video', 'document', 'audio', 'other'
    media_category VARCHAR(100), -- 'photo', 'illustration', 'infographic', 'video_news', 'video_feature', 'pdf', 'doc', etc.
    
    -- File information
    original_filename VARCHAR(500) NOT NULL,
    file_extension VARCHAR(20), -- 'jpg', 'png', 'mp4', 'pdf', etc.
    mime_type VARCHAR(100), -- 'image/jpeg', 'video/mp4', 'application/pdf', etc.
    file_size_bytes BIGINT, -- File size in bytes
    file_hash VARCHAR(255), -- SHA-256 hash for deduplication
    
    -- Object storage paths
    storage_type VARCHAR(50) DEFAULT 's3', -- 's3', 'minio', 'local', 'cdn'
    storage_bucket VARCHAR(255) DEFAULT 'ownlens-media', -- S3/MinIO bucket name
    storage_path VARCHAR(1000) NOT NULL, -- Full path in object storage
    storage_url VARCHAR(1000), -- Full URL to media file
    
    -- Image-specific metadata
    image_width INTEGER, -- Width in pixels
    image_height INTEGER, -- Height in pixels
    image_aspect_ratio DECIMAL(5, 2), -- Width/Height ratio
    image_color_space VARCHAR(50), -- 'RGB', 'CMYK', 'Grayscale', etc.
    image_dpi INTEGER, -- Dots per inch
    image_orientation VARCHAR(20), -- 'landscape', 'portrait', 'square'
    
    -- Video-specific metadata
    video_duration_sec INTEGER, -- Duration in seconds
    video_duration_formatted VARCHAR(20), -- 'HH:MM:SS' format
    video_width INTEGER, -- Width in pixels
    video_height INTEGER, -- Height in pixels
    video_frame_rate DECIMAL(5, 2), -- Frames per second
    video_codec VARCHAR(50), -- 'h264', 'h265', 'vp9', etc.
    video_bitrate INTEGER, -- Bitrate in bps
    video_has_audio BOOLEAN DEFAULT FALSE,
    video_audio_codec VARCHAR(50), -- 'aac', 'mp3', etc.
    
    -- Document-specific metadata
    document_page_count INTEGER, -- Number of pages (for PDFs)
    document_word_count INTEGER, -- Word count (for documents)
    document_language_code CHAR(5), -- ISO 639-1
    
    -- Media content
    title VARCHAR(500), -- Media title
    description TEXT, -- Media description
    alt_text TEXT, -- Alt text for images (accessibility)
    caption TEXT, -- Caption for media
    credit VARCHAR(255), -- Photo/video credit
    copyright VARCHAR(255), -- Copyright information
    license_type VARCHAR(100), -- 'royalty_free', 'rights_managed', 'editorial', 'creative_commons', etc.
    
    -- SEO and metadata
    keywords VARCHAR(255)[], -- Array of keywords
    tags VARCHAR(100)[], -- Array of tags
    metadata JSONB DEFAULT '{}'::jsonb, -- Additional metadata
    
    -- Relationships
    uploaded_by UUID REFERENCES users(user_id),
    author_id UUID REFERENCES editorial_authors(author_id), -- If media created by author
    category_id UUID REFERENCES categories(category_id), -- Media category
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_public BOOLEAN DEFAULT TRUE, -- Is media publicly accessible?
    is_featured BOOLEAN DEFAULT FALSE, -- Is this featured media?
    
    -- Timestamps
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP WITH TIME ZONE, -- Last time media was used in an article
    
    -- Usage tracking
    usage_count INTEGER DEFAULT 0, -- Number of times used in articles
    view_count INTEGER DEFAULT 0, -- Number of times viewed
    download_count INTEGER DEFAULT 0 -- Number of times downloaded
);

-- ============================================================================
-- MEDIA VARIANTS (Different sizes/formats of same media)
-- ============================================================================

-- Media variants (thumbnails, different sizes, formats)
CREATE TABLE IF NOT EXISTS editorial_media_variants (
    variant_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    media_id UUID NOT NULL REFERENCES editorial_media_assets(media_id) ON DELETE CASCADE,
    
    -- Variant details
    variant_type VARCHAR(50) NOT NULL, -- 'thumbnail', 'small', 'medium', 'large', 'original', 'webp', 'avif', 'preview'
    variant_size VARCHAR(50), -- '100x100', '500x500', '1920x1080', etc.
    variant_format VARCHAR(20), -- 'jpg', 'png', 'webp', 'avif', 'mp4', etc.
    
    -- File information
    file_size_bytes BIGINT, -- File size in bytes
    file_hash VARCHAR(255), -- SHA-256 hash
    
    -- Object storage paths
    storage_path VARCHAR(1000) NOT NULL, -- Path in object storage
    storage_url VARCHAR(1000), -- Full URL to variant
    
    -- Dimensions (for images/videos)
    width INTEGER,
    height INTEGER,
    aspect_ratio DECIMAL(5, 2),
    
    -- Quality settings
    quality_level VARCHAR(20), -- 'low', 'medium', 'high', 'original'
    compression_ratio DECIMAL(5, 2), -- Compression ratio (0-1)
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_media_variant UNIQUE (media_id, variant_type)
);

-- ============================================================================
-- CONTENT-MEDIA RELATIONSHIPS
-- ============================================================================

-- Article-media relationships (which media belongs to which article)
CREATE TABLE IF NOT EXISTS editorial_content_media (
    relationship_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES editorial_articles(article_id) ON DELETE CASCADE,
    media_id UUID NOT NULL REFERENCES editorial_media_assets(media_id) ON DELETE CASCADE,
    content_id UUID REFERENCES editorial_article_content(content_id) ON DELETE SET NULL, -- Specific content version
    
    -- Relationship details
    relationship_type VARCHAR(50) NOT NULL, -- 'featured_image', 'inline_image', 'gallery_image', 'video', 'document', 'thumbnail'
    position_in_content INTEGER, -- Position in article (1, 2, 3, ...)
    position_in_section VARCHAR(100), -- Section name or identifier
    
    -- Media usage in content
    media_context TEXT, -- Context where media is used (surrounding text)
    media_caption TEXT, -- Caption shown with media
    media_alt_text TEXT, -- Alt text for accessibility
    is_featured BOOLEAN DEFAULT FALSE, -- Is this featured media for the article?
    is_required BOOLEAN DEFAULT FALSE, -- Is this media required for the article?
    
    -- Display settings
    display_size VARCHAR(50), -- 'small', 'medium', 'large', 'full_width'
    alignment VARCHAR(20), -- 'left', 'center', 'right', 'full_width'
    wrap_text BOOLEAN DEFAULT FALSE, -- Wrap text around media
    
    -- Timestamps
    linked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    
    CONSTRAINT unique_article_media_position UNIQUE (article_id, media_id, position_in_content)
);

-- ============================================================================
-- MEDIA COLLECTIONS
-- ============================================================================

-- Media collections (galleries, playlists, etc.)
CREATE TABLE IF NOT EXISTS editorial_media_collections (
    collection_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Collection details
    collection_name VARCHAR(255) NOT NULL,
    collection_type VARCHAR(50) NOT NULL, -- 'gallery', 'playlist', 'album', 'folder'
    description TEXT,
    
    -- Collection metadata
    media_count INTEGER DEFAULT 0, -- Number of media in collection
    cover_media_id UUID REFERENCES editorial_media_assets(media_id), -- Cover image/video
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_public BOOLEAN DEFAULT TRUE,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id)
);

-- Media collection items
CREATE TABLE IF NOT EXISTS editorial_media_collection_items (
    item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    collection_id UUID NOT NULL REFERENCES editorial_media_collections(collection_id) ON DELETE CASCADE,
    media_id UUID NOT NULL REFERENCES editorial_media_assets(media_id) ON DELETE CASCADE,
    
    -- Item details
    position_in_collection INTEGER, -- Order in collection (1, 2, 3, ...)
    item_title VARCHAR(255), -- Title for this item in collection
    item_description TEXT, -- Description for this item
    
    -- Timestamps
    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_collection_media UNIQUE (collection_id, media_id)
);

-- ============================================================================
-- MEDIA USAGE TRACKING
-- ============================================================================

-- Track media usage across articles (for analytics)
CREATE TABLE IF NOT EXISTS editorial_media_usage (
    usage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    media_id UUID NOT NULL REFERENCES editorial_media_assets(media_id) ON DELETE CASCADE,
    article_id UUID REFERENCES editorial_articles(article_id) ON DELETE SET NULL,
    usage_date DATE NOT NULL,
    
    -- Usage metrics
    view_count INTEGER DEFAULT 0, -- Times media was viewed
    click_count INTEGER DEFAULT 0, -- Times media was clicked
    download_count INTEGER DEFAULT 0, -- Times media was downloaded
    share_count INTEGER DEFAULT 0, -- Times media was shared
    
    -- Engagement metrics
    avg_view_duration_sec INTEGER, -- Average view duration (for videos)
    completion_rate DECIMAL(5, 4), -- Completion rate (for videos)
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_media_usage_date UNIQUE (media_id, article_id, usage_date)
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Media assets indexes
CREATE INDEX IF NOT EXISTS idx_media_assets_brand_id ON editorial_media_assets(brand_id);
CREATE INDEX IF NOT EXISTS idx_media_assets_type ON editorial_media_assets(media_type);
CREATE INDEX IF NOT EXISTS idx_media_assets_category ON editorial_media_assets(media_category);
CREATE INDEX IF NOT EXISTS idx_media_assets_code ON editorial_media_assets(media_code) WHERE media_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_media_assets_hash ON editorial_media_assets(file_hash) WHERE file_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_media_assets_active ON editorial_media_assets(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_media_assets_uploaded_at ON editorial_media_assets(uploaded_at);
CREATE INDEX IF NOT EXISTS idx_media_assets_last_used ON editorial_media_assets(last_used_at) WHERE last_used_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_media_assets_usage_count ON editorial_media_assets(usage_count DESC);
CREATE INDEX IF NOT EXISTS idx_media_assets_tags ON editorial_media_assets USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_media_assets_keywords ON editorial_media_assets USING gin(keywords);
CREATE INDEX IF NOT EXISTS idx_media_assets_metadata ON editorial_media_assets USING gin(metadata);

-- Media variants indexes
CREATE INDEX IF NOT EXISTS idx_media_variants_media_id ON editorial_media_variants(media_id);
CREATE INDEX IF NOT EXISTS idx_media_variants_type ON editorial_media_variants(media_id, variant_type);
CREATE INDEX IF NOT EXISTS idx_media_variants_active ON editorial_media_variants(is_active) WHERE is_active = TRUE;

-- Content-media relationships indexes
CREATE INDEX IF NOT EXISTS idx_content_media_article_id ON editorial_content_media(article_id);
CREATE INDEX IF NOT EXISTS idx_content_media_media_id ON editorial_content_media(media_id);
CREATE INDEX IF NOT EXISTS idx_content_media_content_id ON editorial_content_media(content_id) WHERE content_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_content_media_type ON editorial_content_media(relationship_type);
CREATE INDEX IF NOT EXISTS idx_content_media_featured ON editorial_content_media(article_id, is_featured) WHERE is_featured = TRUE;
CREATE INDEX IF NOT EXISTS idx_content_media_position ON editorial_content_media(article_id, position_in_content) WHERE position_in_content IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_content_media_unique ON editorial_content_media(article_id, media_id);

-- Media collections indexes
CREATE INDEX IF NOT EXISTS idx_media_collections_brand_id ON editorial_media_collections(brand_id);
CREATE INDEX IF NOT EXISTS idx_media_collections_type ON editorial_media_collections(collection_type);
CREATE INDEX IF NOT EXISTS idx_media_collections_active ON editorial_media_collections(is_active) WHERE is_active = TRUE;

-- Media collection items indexes
CREATE INDEX IF NOT EXISTS idx_collection_items_collection_id ON editorial_media_collection_items(collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_items_media_id ON editorial_media_collection_items(media_id);
CREATE INDEX IF NOT EXISTS idx_collection_items_position ON editorial_media_collection_items(collection_id, position_in_collection) WHERE position_in_collection IS NOT NULL;

-- Media usage indexes
CREATE INDEX IF NOT EXISTS idx_media_usage_media_id ON editorial_media_usage(media_id);
CREATE INDEX IF NOT EXISTS idx_media_usage_article_id ON editorial_media_usage(article_id) WHERE article_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_media_usage_date ON editorial_media_usage(usage_date);
CREATE INDEX IF NOT EXISTS idx_media_usage_media_date ON editorial_media_usage(media_id, usage_date);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Function to update media usage count
CREATE OR REPLACE FUNCTION update_media_usage_count()
RETURNS TRIGGER AS $$
BEGIN
    -- Update usage count when media is linked to article
    IF TG_OP = 'INSERT' THEN
        UPDATE editorial_media_assets
        SET usage_count = usage_count + 1,
            last_used_at = CURRENT_TIMESTAMP
        WHERE media_id = NEW.media_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE editorial_media_assets
        SET usage_count = GREATEST(0, usage_count - 1)
        WHERE media_id = OLD.media_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update usage count
CREATE TRIGGER update_media_usage_trigger
    AFTER INSERT OR DELETE ON editorial_content_media
    FOR EACH ROW
    EXECUTE FUNCTION update_media_usage_count();

-- Function to update collection media count
CREATE OR REPLACE FUNCTION update_collection_media_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE editorial_media_collections
        SET media_count = media_count + 1
        WHERE collection_id = NEW.collection_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE editorial_media_collections
        SET media_count = GREATEST(0, media_count - 1)
        WHERE collection_id = OLD.collection_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update collection count
CREATE TRIGGER update_collection_count_trigger
    AFTER INSERT OR DELETE ON editorial_media_collection_items
    FOR EACH ROW
    EXECUTE FUNCTION update_collection_media_count();

-- Update triggers
CREATE TRIGGER update_media_assets_updated_at BEFORE UPDATE ON editorial_media_assets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_media_variants_updated_at BEFORE UPDATE ON editorial_media_variants
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_content_media_updated_at BEFORE UPDATE ON editorial_content_media
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_media_collections_updated_at BEFORE UPDATE ON editorial_media_collections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_media_usage_updated_at BEFORE UPDATE ON editorial_media_usage
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to get media URL (with variant support)
CREATE OR REPLACE FUNCTION get_media_url(
    p_media_id UUID,
    p_variant_type VARCHAR(50) DEFAULT 'original'
)
RETURNS VARCHAR(1000) AS $$
DECLARE
    v_url VARCHAR(1000);
BEGIN
    -- Try to get variant URL first
    SELECT storage_url INTO v_url
    FROM editorial_media_variants
    WHERE media_id = p_media_id
      AND variant_type = p_variant_type
      AND is_active = TRUE
    LIMIT 1;
    
    -- If variant not found, get original media URL
    IF v_url IS NULL THEN
        SELECT storage_url INTO v_url
        FROM editorial_media_assets
        WHERE media_id = p_media_id
          AND is_active = TRUE;
    END IF;
    
    RETURN v_url;
END;
$$ LANGUAGE plpgsql;

-- Function to get article media (with positions)
CREATE OR REPLACE FUNCTION get_article_media(
    p_article_id UUID,
    p_media_type VARCHAR(50) DEFAULT NULL
)
RETURNS TABLE (
    media_id UUID,
    media_name VARCHAR(255),
    media_type VARCHAR(50),
    relationship_type VARCHAR(50),
    position_in_content INTEGER,
    storage_url VARCHAR(1000),
    thumbnail_url VARCHAR(1000),
    caption TEXT,
    alt_text TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.media_id,
        m.media_name,
        m.media_type,
        cm.relationship_type,
        cm.position_in_content,
        m.storage_url,
        get_media_url(m.media_id, 'thumbnail') AS thumbnail_url,
        cm.media_caption,
        COALESCE(cm.media_alt_text, m.alt_text) AS alt_text
    FROM editorial_content_media cm
    INNER JOIN editorial_media_assets m ON cm.media_id = m.media_id
    WHERE cm.article_id = p_article_id
      AND m.is_active = TRUE
      AND (p_media_type IS NULL OR m.media_type = p_media_type)
    ORDER BY cm.position_in_content ASC NULLS LAST;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE editorial_media_assets IS 'Media assets (images, videos, documents) with object storage paths';
COMMENT ON TABLE editorial_media_variants IS 'Media variants (thumbnails, different sizes, formats)';
COMMENT ON TABLE editorial_content_media IS 'Article-media relationships (which media belongs to which article)';
COMMENT ON TABLE editorial_media_collections IS 'Media collections (galleries, playlists, albums)';
COMMENT ON TABLE editorial_media_collection_items IS 'Media items in collections';
COMMENT ON TABLE editorial_media_usage IS 'Media usage tracking across articles (for analytics)';
COMMENT ON COLUMN editorial_media_assets.storage_path IS 'Full path in object storage (e.g., images/brand_id/media_id/original.jpg)';
COMMENT ON COLUMN editorial_media_assets.storage_url IS 'Full URL to media file (e.g., https://s3.amazonaws.com/bucket/path)';
COMMENT ON FUNCTION get_media_url IS 'Get media URL with variant support (thumbnail, small, medium, large, original)';
COMMENT ON FUNCTION get_article_media IS 'Get all media for an article with positions and relationships';

