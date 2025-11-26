-- ============================================================================
-- OwnLens - Editorial Content Schema
-- ============================================================================
-- Article content storage, versioning, and full-text search
-- ============================================================================

-- Enable full-text search extensions
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- Trigram for fuzzy search
-- Note: tsvector is built-in, no extension needed

-- ============================================================================
-- ARTICLE CONTENT
-- ============================================================================

-- Article content table (full article body with versioning)
CREATE TABLE IF NOT EXISTS editorial_article_content (
    content_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES editorial_articles(article_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Content details
    content_version INTEGER NOT NULL DEFAULT 1, -- Version number (1, 2, 3, ...)
    content_format VARCHAR(50) NOT NULL DEFAULT 'html', -- 'html', 'markdown', 'json', 'plain_text'
    content_body TEXT NOT NULL, -- Full article content (HTML, Markdown, or JSON)
    content_body_json JSONB, -- Structured content (for JSON format - blocks, sections)
    
    -- Content structure (for rich content)
    content_blocks JSONB DEFAULT '[]'::jsonb, -- Array of content blocks (headings, paragraphs, images, videos, etc.)
    content_sections JSONB DEFAULT '[]'::jsonb, -- Array of sections (introduction, body, conclusion, etc.)
    
    -- Content metadata
    word_count INTEGER, -- Word count of content
    character_count INTEGER, -- Character count
    reading_time_minutes INTEGER, -- Estimated reading time
    content_language_code CHAR(5), -- ISO 639-1 (e.g., 'en', 'de')
    
    -- Full-text search
    search_vector tsvector, -- Full-text search vector (auto-generated)
    search_keywords TEXT[], -- Extracted keywords for search
    
    -- Content status
    content_status VARCHAR(50) DEFAULT 'draft', -- 'draft', 'review', 'approved', 'published', 'archived'
    is_current_version BOOLEAN DEFAULT TRUE, -- Is this the current version?
    is_published_version BOOLEAN DEFAULT FALSE, -- Is this the published version?
    
    -- Versioning
    previous_version_id UUID REFERENCES editorial_article_content(content_id) ON DELETE SET NULL,
    version_notes TEXT, -- Notes about this version (what changed, why)
    version_created_by UUID REFERENCES users(user_id),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP WITH TIME ZONE, -- When this version was published
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    
    CONSTRAINT unique_article_version UNIQUE (article_id, content_version)
);

-- ============================================================================
-- CONTENT VERSIONS
-- ============================================================================

-- Content version history (track all versions)
CREATE TABLE IF NOT EXISTS editorial_content_versions (
    version_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES editorial_articles(article_id) ON DELETE CASCADE,
    content_id UUID NOT NULL REFERENCES editorial_article_content(content_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Version details
    version_number INTEGER NOT NULL,
    version_type VARCHAR(50) NOT NULL, -- 'draft', 'revision', 'correction', 'update', 'republish'
    version_status VARCHAR(50) DEFAULT 'draft', -- 'draft', 'review', 'approved', 'published', 'rejected'
    
    -- Version metadata
    version_name VARCHAR(255), -- Human-readable version name (e.g., "Initial Draft", "Post-Review Update")
    version_description TEXT, -- Description of changes in this version
    change_summary TEXT, -- Summary of changes from previous version
    
    -- Changes tracking
    changed_fields TEXT[], -- Array of field names that changed
    changes_json JSONB DEFAULT '{}'::jsonb, -- Detailed changes (old vs new values)
    
    -- Who and when
    created_by UUID REFERENCES users(user_id),
    reviewed_by UUID REFERENCES users(user_id),
    approved_by UUID REFERENCES users(user_id),
    published_by UUID REFERENCES users(user_id),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP WITH TIME ZONE,
    approved_at TIMESTAMP WITH TIME ZONE,
    published_at TIMESTAMP WITH TIME ZONE,
    
    -- Version comparison
    previous_version_id UUID REFERENCES editorial_content_versions(version_id) ON DELETE SET NULL,
    diff_summary TEXT, -- Text diff summary
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    
    CONSTRAINT unique_article_version_number UNIQUE (article_id, version_number)
);

-- ============================================================================
-- FULL-TEXT SEARCH INDEXES
-- ============================================================================

-- Create GIN index for full-text search
CREATE INDEX IF NOT EXISTS idx_article_content_search_vector 
    ON editorial_article_content USING gin(search_vector);

-- Create index for current versions (most common query)
CREATE INDEX IF NOT EXISTS idx_article_content_current 
    ON editorial_article_content(article_id, is_current_version) 
    WHERE is_current_version = TRUE;

-- Create index for published versions
CREATE INDEX IF NOT EXISTS idx_article_content_published 
    ON editorial_article_content(article_id, is_published_version) 
    WHERE is_published_version = TRUE;

-- Create index for content format
CREATE INDEX IF NOT EXISTS idx_article_content_format 
    ON editorial_article_content(content_format);

-- Create index for content status
CREATE INDEX IF NOT EXISTS idx_article_content_status 
    ON editorial_article_content(content_status);

-- Create index for version
CREATE INDEX IF NOT EXISTS idx_article_content_version 
    ON editorial_article_content(article_id, content_version);

-- Create index for JSONB content blocks (for JSON queries)
CREATE INDEX IF NOT EXISTS idx_article_content_blocks 
    ON editorial_article_content USING gin(content_blocks);

-- Create index for JSONB content sections
CREATE INDEX IF NOT EXISTS idx_article_content_sections 
    ON editorial_article_content USING gin(content_sections);

-- Create index for search keywords
CREATE INDEX IF NOT EXISTS idx_article_content_keywords 
    ON editorial_article_content USING gin(search_keywords);

-- ============================================================================
-- TRIGGERS FOR FULL-TEXT SEARCH
-- ============================================================================

-- Function to generate search vector from content
CREATE OR REPLACE FUNCTION generate_article_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    -- Generate search vector from title, headline, summary, and content
    NEW.search_vector := 
        setweight(to_tsvector('english', COALESCE((SELECT title FROM editorial_articles WHERE article_id = NEW.article_id), '')), 'A') ||
        setweight(to_tsvector('english', COALESCE((SELECT headline FROM editorial_articles WHERE article_id = NEW.article_id), '')), 'A') ||
        setweight(to_tsvector('english', COALESCE((SELECT summary FROM editorial_articles WHERE article_id = NEW.article_id), '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(NEW.content_body, '')), 'C');
    
    -- Extract keywords (simple extraction - can be enhanced)
    NEW.search_keywords := string_to_array(
        lower(regexp_replace(NEW.content_body, '[^a-zA-Z0-9\s]', '', 'g')),
        ' '
    );
    
    -- Update word count
    NEW.word_count := array_length(string_to_array(NEW.content_body, ' '), 1);
    
    -- Update character count
    NEW.character_count := length(NEW.content_body);
    
    -- Estimate reading time (average 200 words per minute)
    NEW.reading_time_minutes := GREATEST(1, (NEW.word_count / 200)::INTEGER);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-generate search vector
CREATE TRIGGER generate_search_vector_trigger
    BEFORE INSERT OR UPDATE ON editorial_article_content
    FOR EACH ROW
    EXECUTE FUNCTION generate_article_search_vector();

-- ============================================================================
-- TRIGGERS FOR VERSIONING
-- ============================================================================

-- Function to manage version numbers
CREATE OR REPLACE FUNCTION manage_content_versioning()
RETURNS TRIGGER AS $$
DECLARE
    max_version INTEGER;
BEGIN
    -- If new content, get next version number
    IF TG_OP = 'INSERT' THEN
        SELECT COALESCE(MAX(content_version), 0) + 1
        INTO max_version
        FROM editorial_article_content
        WHERE article_id = NEW.article_id;
        
        NEW.content_version := max_version;
        
        -- Mark as current version if it's the first version
        IF max_version = 1 THEN
            NEW.is_current_version := TRUE;
        ELSE
            -- Unset previous current version
            UPDATE editorial_article_content
            SET is_current_version = FALSE
            WHERE article_id = NEW.article_id
              AND is_current_version = TRUE;
            
            NEW.is_current_version := TRUE;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for version management
CREATE TRIGGER manage_versioning_trigger
    BEFORE INSERT ON editorial_article_content
    FOR EACH ROW
    EXECUTE FUNCTION manage_content_versioning();

-- ============================================================================
-- TRIGGERS FOR UPDATED_AT
-- ============================================================================

CREATE TRIGGER update_article_content_updated_at BEFORE UPDATE ON editorial_article_content
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_content_versions_updated_at BEFORE UPDATE ON editorial_content_versions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Article content indexes
CREATE INDEX IF NOT EXISTS idx_article_content_article_id ON editorial_article_content(article_id);
CREATE INDEX IF NOT EXISTS idx_article_content_brand_id ON editorial_article_content(brand_id);
CREATE INDEX IF NOT EXISTS idx_article_content_version ON editorial_article_content(article_id, content_version);
CREATE INDEX IF NOT EXISTS idx_article_content_status ON editorial_article_content(content_status);
CREATE INDEX IF NOT EXISTS idx_article_content_current_version ON editorial_article_content(article_id, is_current_version) WHERE is_current_version = TRUE;
CREATE INDEX IF NOT EXISTS idx_article_content_published_version ON editorial_article_content(article_id, is_published_version) WHERE is_published_version = TRUE;
CREATE INDEX IF NOT EXISTS idx_article_content_created_at ON editorial_article_content(created_at);

-- Content versions indexes
CREATE INDEX IF NOT EXISTS idx_content_versions_article_id ON editorial_content_versions(article_id);
CREATE INDEX IF NOT EXISTS idx_content_versions_content_id ON editorial_content_versions(content_id);
CREATE INDEX IF NOT EXISTS idx_content_versions_brand_id ON editorial_content_versions(brand_id);
CREATE INDEX IF NOT EXISTS idx_content_versions_version_number ON editorial_content_versions(article_id, version_number);
CREATE INDEX IF NOT EXISTS idx_content_versions_status ON editorial_content_versions(version_status);
CREATE INDEX IF NOT EXISTS idx_content_versions_type ON editorial_content_versions(version_type);
CREATE INDEX IF NOT EXISTS idx_content_versions_created_at ON editorial_content_versions(created_at);

-- ============================================================================
-- FULL-TEXT SEARCH FUNCTIONS
-- ============================================================================

-- Function to search articles by content
CREATE OR REPLACE FUNCTION search_articles_by_content(
    search_query TEXT,
    brand_id_filter UUID DEFAULT NULL,
    category_id_filter UUID DEFAULT NULL,
    limit_results INTEGER DEFAULT 20
)
RETURNS TABLE (
    article_id UUID,
    title VARCHAR(500),
    headline VARCHAR(500),
    summary TEXT,
    content_body TEXT,
    relevance REAL,
    word_count INTEGER,
    reading_time_minutes INTEGER,
    publish_time TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.article_id,
        a.title,
        a.headline,
        a.summary,
        c.content_body,
        ts_rank(c.search_vector, plainto_tsquery('english', search_query)) AS relevance,
        c.word_count,
        c.reading_time_minutes,
        a.publish_time
    FROM editorial_articles a
    INNER JOIN editorial_article_content c ON a.article_id = c.article_id
    WHERE 
        c.is_current_version = TRUE
        AND c.is_published_version = TRUE
        AND c.search_vector @@ plainto_tsquery('english', search_query)
        AND (brand_id_filter IS NULL OR a.brand_id = brand_id_filter)
        AND (category_id_filter IS NULL OR a.primary_category_id = category_id_filter)
    ORDER BY relevance DESC, a.publish_time DESC
    LIMIT limit_results;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE editorial_article_content IS 'Full article content with versioning and full-text search';
COMMENT ON TABLE editorial_content_versions IS 'Content version history and change tracking';
COMMENT ON COLUMN editorial_article_content.content_body IS 'Full article content (HTML, Markdown, or plain text)';
COMMENT ON COLUMN editorial_article_content.content_body_json IS 'Structured content in JSON format (for rich content editors)';
COMMENT ON COLUMN editorial_article_content.content_blocks IS 'Array of content blocks (headings, paragraphs, images, videos, etc.)';
COMMENT ON COLUMN editorial_article_content.content_sections IS 'Array of content sections (introduction, body, conclusion, etc.)';
COMMENT ON COLUMN editorial_article_content.search_vector IS 'Full-text search vector (auto-generated from content)';
COMMENT ON FUNCTION search_articles_by_content IS 'Search articles by content using full-text search';

