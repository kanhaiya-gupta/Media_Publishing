-- ============================================================================
-- OwnLens - Editorial Core Schema
-- ============================================================================
-- Core editorial: articles metadata, authors, performance analytics, category trends
-- Note: For article content, see editorial_content.sql
-- Note: For media assets, see editorial_media.sql
-- ============================================================================

-- ============================================================================
-- AUTHORS
-- ============================================================================

-- Authors table (journalists, content creators)
CREATE TABLE IF NOT EXISTS editorial_authors (
    author_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL, -- Link to user if author has account
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Author information
    author_name VARCHAR(255) NOT NULL,
    author_code VARCHAR(100) NOT NULL, -- Unique identifier within brand
    email VARCHAR(255),
    bio TEXT,
    profile_image_url VARCHAR(500),
    department VARCHAR(100), -- 'politics', 'sports', 'business', 'technology', etc.
    role VARCHAR(100), -- 'journalist', 'editor', 'correspondent', 'columnist', etc.
    expertise_areas VARCHAR(255)[], -- Array of expertise areas
    
    -- Geographic information
    primary_country_code CHAR(2) REFERENCES countries(country_code),
    primary_city_id UUID REFERENCES cities(city_id),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    hire_date DATE,
    last_article_date DATE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_brand_author_code UNIQUE (brand_id, author_code)
);

-- ============================================================================
-- ARTICLES
-- ============================================================================

-- Articles table (content pieces)
CREATE TABLE IF NOT EXISTS editorial_articles (
    article_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    external_article_id VARCHAR(255), -- External system article ID
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Article information
    title VARCHAR(500) NOT NULL,
    headline VARCHAR(500), -- Different from title (for A/B testing)
    subtitle TEXT,
    summary TEXT,
    content_url VARCHAR(1000) NOT NULL,
    article_type VARCHAR(50), -- 'news', 'opinion', 'analysis', 'feature', 'gallery', 'video', 'listicle'
    
    -- Author information
    primary_author_id UUID REFERENCES editorial_authors(author_id) ON DELETE SET NULL,
    co_author_ids UUID[], -- Array of co-author UUIDs
    
    -- Category information
    primary_category_id UUID REFERENCES categories(category_id),
    secondary_category_ids UUID[], -- Array of secondary category UUIDs
    tags VARCHAR(100)[], -- Array of tags
    
    -- Content metrics
    word_count INTEGER,
    reading_time_minutes INTEGER, -- Estimated reading time
    image_count INTEGER DEFAULT 0,
    video_count INTEGER DEFAULT 0,
    
    -- Publishing information
    publish_time TIMESTAMP WITH TIME ZONE NOT NULL,
    publish_date DATE NOT NULL,
    last_updated_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'draft', -- 'draft', 'scheduled', 'published', 'archived', 'deleted'
    
    -- SEO information
    meta_description TEXT,
    meta_keywords VARCHAR(500)[],
    canonical_url VARCHAR(1000),
    
    -- Geographic relevance
    relevant_countries CHAR(2)[], -- Array of country codes
    relevant_cities UUID[], -- Array of city IDs
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- ARTICLE PERFORMANCE
-- ============================================================================

-- Article performance metrics (aggregated by date)
CREATE TABLE IF NOT EXISTS editorial_article_performance (
    performance_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES editorial_articles(article_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    performance_date DATE NOT NULL,
    
    -- View metrics
    total_views INTEGER DEFAULT 0,
    unique_views INTEGER DEFAULT 0,
    unique_readers INTEGER DEFAULT 0,
    
    -- Engagement metrics
    total_clicks INTEGER DEFAULT 0,
    total_shares INTEGER DEFAULT 0,
    total_comments INTEGER DEFAULT 0,
    total_likes INTEGER DEFAULT 0,
    total_bookmarks INTEGER DEFAULT 0,
    
    -- Reading metrics
    avg_time_on_page_sec INTEGER,
    avg_scroll_depth INTEGER, -- Percentage
    completion_rate DECIMAL(5, 4), -- Percentage of readers who finished
    bounce_rate DECIMAL(5, 4), -- Percentage of single-page sessions
    
    -- Video metrics (if applicable)
    video_plays INTEGER DEFAULT 0,
    video_completions INTEGER DEFAULT 0,
    avg_video_watch_time_sec INTEGER,
    
    -- Referral metrics
    direct_views INTEGER DEFAULT 0,
    search_views INTEGER DEFAULT 0,
    social_views INTEGER DEFAULT 0,
    newsletter_views INTEGER DEFAULT 0,
    internal_views INTEGER DEFAULT 0,
    
    -- Geographic metrics
    top_countries JSONB DEFAULT '{}'::jsonb, -- {country_code: view_count}
    top_cities JSONB DEFAULT '{}'::jsonb, -- {city_id: view_count}
    
    -- Device metrics
    desktop_views INTEGER DEFAULT 0,
    mobile_views INTEGER DEFAULT 0,
    tablet_views INTEGER DEFAULT 0,
    
    -- Engagement score
    engagement_score DECIMAL(10, 2), -- Calculated engagement score
    quality_score DECIMAL(10, 2), -- ML-predicted quality score
    
    -- Revenue metrics
    ad_revenue DECIMAL(10, 2) DEFAULT 0,
    subscription_conversions INTEGER DEFAULT 0,
    newsletter_signups INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_article_date UNIQUE (article_id, performance_date)
);

-- ============================================================================
-- AUTHOR PERFORMANCE
-- ============================================================================

-- Author performance metrics (aggregated by date)
CREATE TABLE IF NOT EXISTS editorial_author_performance (
    performance_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    author_id UUID NOT NULL REFERENCES editorial_authors(author_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    performance_date DATE NOT NULL,
    
    -- Publishing metrics
    articles_published INTEGER DEFAULT 0,
    articles_scheduled INTEGER DEFAULT 0,
    articles_draft INTEGER DEFAULT 0,
    
    -- View metrics
    total_views INTEGER DEFAULT 0,
    unique_readers INTEGER DEFAULT 0,
    avg_views_per_article DECIMAL(10, 2),
    
    -- Engagement metrics
    total_engagement INTEGER DEFAULT 0,
    avg_engagement_score DECIMAL(10, 2),
    total_shares INTEGER DEFAULT 0,
    total_comments INTEGER DEFAULT 0,
    
    -- Best performing article
    best_article_id UUID REFERENCES editorial_articles(article_id),
    best_article_views INTEGER,
    best_article_engagement DECIMAL(10, 2),
    
    -- Category performance
    top_category_id UUID REFERENCES categories(category_id),
    top_category_views INTEGER,
    
    -- Ranking
    author_rank INTEGER, -- Rank among authors for this date
    engagement_rank INTEGER, -- Rank by engagement
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_author_date UNIQUE (author_id, performance_date)
);

-- ============================================================================
-- CATEGORY PERFORMANCE
-- ============================================================================

-- Category performance metrics (aggregated by date)
CREATE TABLE IF NOT EXISTS editorial_category_performance (
    performance_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    category_id UUID NOT NULL REFERENCES categories(category_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    performance_date DATE NOT NULL,
    
    -- Publishing metrics
    articles_published INTEGER DEFAULT 0,
    articles_scheduled INTEGER DEFAULT 0,
    
    -- View metrics
    total_views INTEGER DEFAULT 0,
    unique_readers INTEGER DEFAULT 0,
    avg_views_per_article DECIMAL(10, 2),
    
    -- Engagement metrics
    total_engagement INTEGER DEFAULT 0,
    avg_engagement_score DECIMAL(10, 2),
    engagement_growth_rate DECIMAL(5, 4), -- Percentage change from previous period
    
    -- Trending status
    is_trending BOOLEAN DEFAULT FALSE,
    trending_score DECIMAL(10, 2), -- Calculated trending score
    trending_rank INTEGER, -- Rank among categories
    
    -- Peak engagement times
    peak_hour INTEGER, -- Hour of day (0-23) with highest engagement
    peak_day_of_week INTEGER, -- Day of week (0-6, Sunday=0) with highest engagement
    
    -- Best performing article
    best_article_id UUID REFERENCES editorial_articles(article_id),
    best_article_views INTEGER,
    
    -- Top authors in category
    top_author_ids UUID[], -- Array of top author UUIDs
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_category_date UNIQUE (category_id, performance_date)
);

-- ============================================================================
-- CONTENT EVENTS (Editorial-specific events)
-- ============================================================================

-- Content events from Kafka (editorial domain)
CREATE TABLE IF NOT EXISTS editorial_content_events (
    event_id UUID DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES editorial_articles(article_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL, -- May be NULL for anonymous users
    
    -- Event information
    event_type VARCHAR(50) NOT NULL, -- 'article_view', 'article_click', 'share', 'comment', 'like', 'bookmark'
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Content information
    category_id UUID REFERENCES categories(category_id),
    author_id UUID REFERENCES editorial_authors(author_id),
    
    -- Geographic information
    country_code CHAR(2) REFERENCES countries(country_code),
    city_id UUID REFERENCES cities(city_id),
    
    -- Device information
    device_type_id UUID REFERENCES device_types(device_type_id),
    os_id UUID REFERENCES operating_systems(os_id),
    browser_id UUID REFERENCES browsers(browser_id),
    
    -- Referral information
    referrer VARCHAR(255),
    referrer_type VARCHAR(50), -- 'direct', 'search', 'social', 'newsletter', 'internal'
    
    -- Engagement metrics
    engagement_metrics JSONB DEFAULT '{}'::jsonb, -- Event-specific metrics
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Partitioning by date
    event_date DATE NOT NULL,
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

-- Create monthly partitions for editorial_content_events (2024-2025)
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_01 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_02 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_03 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_04 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_05 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_06 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_07 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_08 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_09 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_10 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_11 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2024_12 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_01 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_02 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_03 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_04 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_05 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_06 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_07 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_08 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_09 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_10 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_11 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS editorial_content_events_2025_12 PARTITION OF editorial_content_events
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- ============================================================================
-- HEADLINE OPTIMIZATION
-- ============================================================================

-- Headline A/B testing and optimization
CREATE TABLE IF NOT EXISTS editorial_headline_tests (
    test_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    article_id UUID NOT NULL REFERENCES editorial_articles(article_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Headline variants
    headline_variant_a VARCHAR(500) NOT NULL,
    headline_variant_b VARCHAR(500),
    headline_variant_c VARCHAR(500),
    
    -- Test configuration
    test_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    test_end_time TIMESTAMP WITH TIME ZONE,
    traffic_split JSONB DEFAULT '{"a": 0.5, "b": 0.5}'::jsonb, -- Traffic distribution
    
    -- Results
    variant_a_views INTEGER DEFAULT 0,
    variant_b_views INTEGER DEFAULT 0,
    variant_c_views INTEGER DEFAULT 0,
    variant_a_clicks INTEGER DEFAULT 0,
    variant_b_clicks INTEGER DEFAULT 0,
    variant_c_clicks INTEGER DEFAULT 0,
    variant_a_ctr DECIMAL(5, 4), -- Click-through rate
    variant_b_ctr DECIMAL(5, 4),
    variant_c_ctr DECIMAL(5, 4),
    
    -- Winner
    winning_variant VARCHAR(1), -- 'a', 'b', or 'c'
    statistical_significance DECIMAL(5, 4), -- P-value or confidence level
    
    -- Model predictions
    predicted_ctr_a DECIMAL(5, 4),
    predicted_ctr_b DECIMAL(5, 4),
    predicted_ctr_c DECIMAL(5, 4),
    model_version VARCHAR(50),
    
    -- Status
    test_status VARCHAR(50) DEFAULT 'active', -- 'active', 'completed', 'cancelled'
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- TRENDING TOPICS
-- ============================================================================

-- Trending topics detection
CREATE TABLE IF NOT EXISTS editorial_trending_topics (
    topic_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    topic_name VARCHAR(255) NOT NULL,
    topic_keywords VARCHAR(255)[], -- Array of keywords
    
    -- Trending metrics
    trending_score DECIMAL(10, 2) NOT NULL,
    engagement_score DECIMAL(10, 2),
    growth_rate DECIMAL(5, 4), -- Percentage growth
    articles_count INTEGER DEFAULT 0,
    
    -- Time period
    period_start TIMESTAMP WITH TIME ZONE NOT NULL,
    period_end TIMESTAMP WITH TIME ZONE NOT NULL,
    period_type VARCHAR(50), -- 'hour', 'day', 'week', 'month'
    
    -- Related categories
    related_category_ids UUID[],
    
    -- Top articles
    top_article_ids UUID[], -- Array of top article UUIDs
    
    -- Ranking
    trend_rank INTEGER, -- Rank among trending topics
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- CONTENT STRATEGY RECOMMENDATIONS
-- ============================================================================

-- Content strategy recommendations from ML models
CREATE TABLE IF NOT EXISTS editorial_content_recommendations (
    recommendation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    recommendation_type VARCHAR(50) NOT NULL, -- 'topic', 'category', 'author', 'publish_time'
    
    -- Recommendation details
    recommended_topic VARCHAR(255),
    recommended_category_id UUID REFERENCES categories(category_id),
    recommended_author_id UUID REFERENCES editorial_authors(author_id),
    recommended_publish_time TIMESTAMP WITH TIME ZONE,
    
    -- Recommendation metrics
    recommendation_score DECIMAL(10, 6) NOT NULL, -- Strength of recommendation (0-1)
    estimated_engagement DECIMAL(10, 2), -- Predicted engagement
    estimated_views INTEGER, -- Predicted views
    
    -- Reasoning
    reasoning TEXT, -- Why this recommendation
    similar_successful_content JSONB DEFAULT '[]'::jsonb, -- Examples of similar successful content
    
    -- Model information
    model_version VARCHAR(50),
    algorithm VARCHAR(100),
    
    -- Status
    status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'accepted', 'rejected', 'implemented'
    implemented_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Authors indexes
CREATE INDEX IF NOT EXISTS idx_editorial_authors_brand_id ON editorial_authors(brand_id);
CREATE INDEX IF NOT EXISTS idx_editorial_authors_user_id ON editorial_authors(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_editorial_authors_code ON editorial_authors(brand_id, author_code);
CREATE INDEX IF NOT EXISTS idx_editorial_authors_active ON editorial_authors(brand_id, is_active);
CREATE INDEX IF NOT EXISTS idx_editorial_authors_department ON editorial_authors(department);

-- Articles indexes
CREATE INDEX IF NOT EXISTS idx_editorial_articles_brand_id ON editorial_articles(brand_id);
CREATE INDEX IF NOT EXISTS idx_editorial_articles_external_id ON editorial_articles(external_article_id) WHERE external_article_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_editorial_articles_author_id ON editorial_articles(primary_author_id);
CREATE INDEX IF NOT EXISTS idx_editorial_articles_category_id ON editorial_articles(primary_category_id);
CREATE INDEX IF NOT EXISTS idx_editorial_articles_publish_time ON editorial_articles(publish_time);
CREATE INDEX IF NOT EXISTS idx_editorial_articles_publish_date ON editorial_articles(publish_date);
CREATE INDEX IF NOT EXISTS idx_editorial_articles_status ON editorial_articles(status);
CREATE INDEX IF NOT EXISTS idx_editorial_articles_brand_status ON editorial_articles(brand_id, status);
CREATE INDEX IF NOT EXISTS idx_editorial_articles_title_search ON editorial_articles USING gin(to_tsvector('english', title));

-- Article performance indexes
CREATE INDEX IF NOT EXISTS idx_article_performance_article_id ON editorial_article_performance(article_id);
CREATE INDEX IF NOT EXISTS idx_article_performance_brand_id ON editorial_article_performance(brand_id);
CREATE INDEX IF NOT EXISTS idx_article_performance_date ON editorial_article_performance(performance_date);
CREATE INDEX IF NOT EXISTS idx_article_performance_views ON editorial_article_performance(total_views DESC);
CREATE INDEX IF NOT EXISTS idx_article_performance_engagement ON editorial_article_performance(engagement_score DESC);
CREATE INDEX IF NOT EXISTS idx_article_performance_article_date ON editorial_article_performance(article_id, performance_date);

-- Author performance indexes
CREATE INDEX IF NOT EXISTS idx_author_performance_author_id ON editorial_author_performance(author_id);
CREATE INDEX IF NOT EXISTS idx_author_performance_brand_id ON editorial_author_performance(brand_id);
CREATE INDEX IF NOT EXISTS idx_author_performance_date ON editorial_author_performance(performance_date);
CREATE INDEX IF NOT EXISTS idx_author_performance_views ON editorial_author_performance(total_views DESC);
CREATE INDEX IF NOT EXISTS idx_author_performance_engagement ON editorial_author_performance(avg_engagement_score DESC);
CREATE INDEX IF NOT EXISTS idx_author_performance_rank ON editorial_author_performance(performance_date, author_rank);

-- Category performance indexes
CREATE INDEX IF NOT EXISTS idx_category_performance_category_id ON editorial_category_performance(category_id);
CREATE INDEX IF NOT EXISTS idx_category_performance_brand_id ON editorial_category_performance(brand_id);
CREATE INDEX IF NOT EXISTS idx_category_performance_date ON editorial_category_performance(performance_date);
CREATE INDEX IF NOT EXISTS idx_category_performance_trending ON editorial_category_performance(is_trending, trending_score DESC);
CREATE INDEX IF NOT EXISTS idx_category_performance_category_date ON editorial_category_performance(category_id, performance_date);

-- Content events indexes
CREATE INDEX IF NOT EXISTS idx_content_events_article_id ON editorial_content_events(article_id);
CREATE INDEX IF NOT EXISTS idx_content_events_brand_id ON editorial_content_events(brand_id);
CREATE INDEX IF NOT EXISTS idx_content_events_user_id ON editorial_content_events(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_content_events_event_timestamp ON editorial_content_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_content_events_event_type ON editorial_content_events(event_type);
CREATE INDEX IF NOT EXISTS idx_content_events_author_id ON editorial_content_events(author_id) WHERE author_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_content_events_category_id ON editorial_content_events(category_id) WHERE category_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_content_events_date ON editorial_content_events(event_date);

-- Headline tests indexes
CREATE INDEX IF NOT EXISTS idx_headline_tests_article_id ON editorial_headline_tests(article_id);
CREATE INDEX IF NOT EXISTS idx_headline_tests_brand_id ON editorial_headline_tests(brand_id);
CREATE INDEX IF NOT EXISTS idx_headline_tests_status ON editorial_headline_tests(test_status);
CREATE INDEX IF NOT EXISTS idx_headline_tests_active ON editorial_headline_tests(brand_id, test_status) WHERE test_status = 'active';

-- Trending topics indexes
CREATE INDEX IF NOT EXISTS idx_trending_topics_brand_id ON editorial_trending_topics(brand_id);
CREATE INDEX IF NOT EXISTS idx_trending_topics_score ON editorial_trending_topics(trending_score DESC);
CREATE INDEX IF NOT EXISTS idx_trending_topics_period ON editorial_trending_topics(period_start, period_end);
CREATE INDEX IF NOT EXISTS idx_trending_topics_rank ON editorial_trending_topics(period_start, trend_rank);

-- Content recommendations indexes
CREATE INDEX IF NOT EXISTS idx_content_recommendations_brand_id ON editorial_content_recommendations(brand_id);
CREATE INDEX IF NOT EXISTS idx_content_recommendations_type ON editorial_content_recommendations(recommendation_type);
CREATE INDEX IF NOT EXISTS idx_content_recommendations_score ON editorial_content_recommendations(recommendation_score DESC);
CREATE INDEX IF NOT EXISTS idx_content_recommendations_status ON editorial_content_recommendations(status);
CREATE INDEX IF NOT EXISTS idx_content_recommendations_active ON editorial_content_recommendations(brand_id, status) WHERE status IN ('pending', 'accepted');

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Triggers to populate date columns from timestamp columns
CREATE OR REPLACE FUNCTION set_editorial_date_from_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_TABLE_NAME = 'editorial_articles' THEN
        NEW.publish_date = DATE(NEW.publish_time);
    ELSIF TG_TABLE_NAME = 'editorial_content_events' THEN
        NEW.event_date = DATE(NEW.event_timestamp);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_editorial_articles_publish_date 
    BEFORE INSERT OR UPDATE ON editorial_articles
    FOR EACH ROW 
    WHEN (NEW.publish_date IS NULL)
    EXECUTE FUNCTION set_editorial_date_from_timestamp();

CREATE TRIGGER set_editorial_content_events_date 
    BEFORE INSERT OR UPDATE ON editorial_content_events
    FOR EACH ROW 
    WHEN (NEW.event_date IS NULL)
    EXECUTE FUNCTION set_editorial_date_from_timestamp();

CREATE TRIGGER update_editorial_authors_updated_at BEFORE UPDATE ON editorial_authors
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_editorial_articles_updated_at BEFORE UPDATE ON editorial_articles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_article_performance_updated_at BEFORE UPDATE ON editorial_article_performance
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_author_performance_updated_at BEFORE UPDATE ON editorial_author_performance
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_category_performance_updated_at BEFORE UPDATE ON editorial_category_performance
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_headline_tests_updated_at BEFORE UPDATE ON editorial_headline_tests
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE editorial_authors IS 'Authors/journalists who create content';
COMMENT ON TABLE editorial_articles IS 'Articles and content pieces';
COMMENT ON TABLE editorial_article_performance IS 'Daily aggregated article performance metrics';
COMMENT ON TABLE editorial_author_performance IS 'Daily aggregated author performance metrics';
COMMENT ON TABLE editorial_category_performance IS 'Daily aggregated category performance metrics';
COMMENT ON TABLE editorial_content_events IS 'Raw content events from Kafka (partitioned by date)';
COMMENT ON TABLE editorial_headline_tests IS 'Headline A/B testing and optimization';
COMMENT ON TABLE editorial_trending_topics IS 'Trending topics detection';
COMMENT ON TABLE editorial_content_recommendations IS 'Content strategy recommendations from ML models';

