-- ============================================================================
-- OwnLens - Customer Domain Schema
-- ============================================================================
-- Customer analytics: user behavior, sessions, events, engagement, ML features
-- ============================================================================

-- ============================================================================
-- USER SESSIONS
-- ============================================================================

-- User sessions table (aggregated session-level metrics)
CREATE TABLE IF NOT EXISTS customer_sessions (
    session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    account_id UUID REFERENCES user_accounts(account_id) ON DELETE SET NULL,
    
    -- Geographic information
    country_code CHAR(2) REFERENCES countries(country_code),
    city_id UUID REFERENCES cities(city_id),
    timezone VARCHAR(50),
    
    -- Device information
    device_type_id UUID REFERENCES device_types(device_type_id),
    os_id UUID REFERENCES operating_systems(os_id),
    browser_id UUID REFERENCES browsers(browser_id),
    
    -- Session metadata
    referrer VARCHAR(255), -- 'direct', 'google', 'facebook', 'twitter', 'newsletter', 'internal'
    user_segment VARCHAR(50), -- 'power_user', 'engaged', 'casual', 'new_visitor', 'returning', 'subscriber'
    subscription_tier VARCHAR(50), -- 'free', 'premium', 'pro', 'enterprise'
    
    -- Temporal metrics
    session_start TIMESTAMP WITH TIME ZONE NOT NULL,
    session_end TIMESTAMP WITH TIME ZONE,
    session_duration_sec INTEGER,
    
    -- Event counts
    total_events INTEGER DEFAULT 0,
    article_views INTEGER DEFAULT 0,
    article_clicks INTEGER DEFAULT 0,
    video_plays INTEGER DEFAULT 0,
    newsletter_signups INTEGER DEFAULT 0,
    ad_clicks INTEGER DEFAULT 0,
    searches INTEGER DEFAULT 0,
    
    -- Page metrics
    pages_visited_count INTEGER DEFAULT 0,
    unique_pages_count INTEGER DEFAULT 0,
    unique_categories_count INTEGER DEFAULT 0,
    
    -- JSON fields for detailed data
    categories_visited JSONB DEFAULT '[]'::jsonb, -- Array of category codes
    article_ids_visited JSONB DEFAULT '[]'::jsonb, -- Array of article IDs
    article_titles_visited JSONB DEFAULT '[]'::jsonb, -- Array of article titles
    page_events JSONB DEFAULT '[]'::jsonb, -- Array of page events with metadata
    
    -- Engagement metrics
    engagement_score DECIMAL(10, 2), -- Calculated engagement score
    scroll_depth_avg INTEGER, -- Average scroll depth percentage
    time_on_page_avg INTEGER, -- Average time on page in seconds
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    batch_id UUID -- For batch processing tracking
);

-- ============================================================================
-- USER EVENTS (Raw events)
-- ============================================================================

-- User events table (individual events from Kafka)
CREATE TABLE IF NOT EXISTS customer_events (
    event_id UUID DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES customer_sessions(session_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Event information
    event_type VARCHAR(50) NOT NULL, -- 'article_view', 'article_click', 'video_play', 'newsletter_signup', etc.
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Content information
    article_id VARCHAR(255),
    article_title VARCHAR(500),
    article_type VARCHAR(50), -- 'news', 'opinion', 'gallery', 'video', 'analysis'
    category_id UUID REFERENCES categories(category_id),
    page_url VARCHAR(1000),
    
    -- Geographic information
    country_code CHAR(2) REFERENCES countries(country_code),
    city_id UUID REFERENCES cities(city_id),
    timezone VARCHAR(50),
    
    -- Device information
    device_type_id UUID REFERENCES device_types(device_type_id),
    os_id UUID REFERENCES operating_systems(os_id),
    browser_id UUID REFERENCES browsers(browser_id),
    
    -- Engagement metrics (event-specific)
    engagement_metrics JSONB DEFAULT '{}'::jsonb, -- Event-specific metrics (scroll_depth, time_on_page, etc.)
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Partitioning by date for performance
    event_date DATE NOT NULL,
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

-- Create monthly partitions for customer_events (2024-2025)
CREATE TABLE IF NOT EXISTS customer_events_2024_01 PARTITION OF customer_events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_02 PARTITION OF customer_events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_03 PARTITION OF customer_events
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_04 PARTITION OF customer_events
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_05 PARTITION OF customer_events
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_06 PARTITION OF customer_events
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_07 PARTITION OF customer_events
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_08 PARTITION OF customer_events
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_09 PARTITION OF customer_events
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_10 PARTITION OF customer_events
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_11 PARTITION OF customer_events
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE IF NOT EXISTS customer_events_2024_12 PARTITION OF customer_events
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_01 PARTITION OF customer_events
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_02 PARTITION OF customer_events
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_03 PARTITION OF customer_events
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_04 PARTITION OF customer_events
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_05 PARTITION OF customer_events
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_06 PARTITION OF customer_events
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_07 PARTITION OF customer_events
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_08 PARTITION OF customer_events
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_09 PARTITION OF customer_events
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_10 PARTITION OF customer_events
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_11 PARTITION OF customer_events
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS customer_events_2025_12 PARTITION OF customer_events
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- ============================================================================
-- USER FEATURES (ML-ready features)
-- ============================================================================

-- User-level aggregated features for ML models
CREATE TABLE IF NOT EXISTS customer_user_features (
    user_feature_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    feature_date DATE NOT NULL, -- Date when features were calculated
    
    -- Behavioral features
    total_sessions INTEGER DEFAULT 0,
    avg_session_duration_sec DECIMAL(10, 2),
    min_session_duration_sec INTEGER,
    max_session_duration_sec INTEGER,
    std_session_duration_sec DECIMAL(10, 2),
    avg_events_per_session DECIMAL(10, 2),
    avg_pages_per_session DECIMAL(10, 2),
    avg_categories_diversity DECIMAL(10, 2),
    
    -- Engagement features
    avg_article_views DECIMAL(10, 2),
    avg_article_clicks DECIMAL(10, 2),
    avg_video_plays DECIMAL(10, 2),
    avg_searches DECIMAL(10, 2),
    total_newsletter_signups INTEGER DEFAULT 0,
    total_ad_clicks INTEGER DEFAULT 0,
    
    -- Engagement score
    avg_engagement_score DECIMAL(10, 2),
    max_engagement_score DECIMAL(10, 2),
    min_engagement_score DECIMAL(10, 2),
    
    -- Conversion features
    has_newsletter BOOLEAN DEFAULT FALSE,
    newsletter_signup_rate DECIMAL(5, 4), -- Rate between 0 and 1
    
    -- Content features
    preferred_brand_id UUID REFERENCES brands(brand_id),
    preferred_category_id UUID REFERENCES categories(category_id),
    preferred_device_type_id UUID REFERENCES device_types(device_type_id),
    preferred_os_id UUID REFERENCES operating_systems(os_id),
    preferred_browser_id UUID REFERENCES browsers(browser_id),
    
    -- Geographic features
    primary_country_code CHAR(2) REFERENCES countries(country_code),
    primary_city_id UUID REFERENCES cities(city_id),
    
    -- Subscription features
    current_subscription_tier VARCHAR(50),
    current_user_segment VARCHAR(50),
    
    -- Temporal features
    first_session_date DATE,
    last_session_date DATE,
    days_active INTEGER,
    
    -- Recency features
    days_since_last_session INTEGER,
    active_today BOOLEAN DEFAULT FALSE,
    active_last_7_days BOOLEAN DEFAULT FALSE,
    active_last_30_days BOOLEAN DEFAULT FALSE,
    
    -- Churn prediction features
    churn_risk_score DECIMAL(5, 4), -- Between 0 and 1
    churn_probability DECIMAL(5, 4), -- ML model prediction
    
    -- Additional ML features (JSON for flexibility)
    ml_features JSONB DEFAULT '{}'::jsonb,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_user_brand_feature_date UNIQUE (user_id, brand_id, feature_date)
);

-- ============================================================================
-- USER SEGMENTATION
-- ============================================================================

-- User segments (from ML clustering)
CREATE TABLE IF NOT EXISTS customer_user_segments (
    segment_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    segment_name VARCHAR(100) NOT NULL, -- 'high_engagement', 'casual', 'power_user', etc.
    segment_code VARCHAR(50) NOT NULL,
    cluster_number INTEGER, -- K-means cluster number
    description TEXT,
    user_count INTEGER DEFAULT 0,
    avg_engagement_score DECIMAL(10, 2),
    avg_sessions DECIMAL(10, 2),
    avg_duration_sec DECIMAL(10, 2),
    newsletter_signup_rate DECIMAL(5, 4),
    model_version VARCHAR(50), -- ML model version used
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_brand_segment UNIQUE (brand_id, segment_code)
);

-- User segment assignments
CREATE TABLE IF NOT EXISTS customer_user_segment_assignments (
    assignment_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    segment_id UUID NOT NULL REFERENCES customer_user_segments(segment_id) ON DELETE CASCADE,
    assignment_date DATE NOT NULL,
    confidence_score DECIMAL(5, 4), -- Model confidence (0-1)
    model_version VARCHAR(50),
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_user_brand_current UNIQUE (user_id, brand_id, is_current)
);

-- ============================================================================
-- CHURN PREDICTIONS
-- ============================================================================

-- Churn predictions from ML models
CREATE TABLE IF NOT EXISTS customer_churn_predictions (
    prediction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    prediction_date DATE NOT NULL,
    
    -- Prediction results
    churn_probability DECIMAL(5, 4) NOT NULL, -- Between 0 and 1
    churn_risk_level VARCHAR(20), -- 'low', 'medium', 'high', 'critical'
    is_churned BOOLEAN DEFAULT FALSE, -- Actual churn status (for validation)
    churned_date DATE, -- Actual churn date (if churned)
    
    -- Model information
    model_version VARCHAR(50) NOT NULL,
    model_confidence DECIMAL(5, 4), -- Model confidence score
    feature_importance JSONB DEFAULT '{}'::jsonb, -- Top contributing features
    
    -- Key features used
    days_since_last_session INTEGER,
    total_sessions INTEGER,
    avg_engagement_score DECIMAL(10, 2),
    subscription_tier VARCHAR(50),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_user_brand_churn_prediction_date UNIQUE (user_id, brand_id, prediction_date)
);

-- ============================================================================
-- RECOMMENDATIONS
-- ============================================================================

-- Content recommendations for users
CREATE TABLE IF NOT EXISTS customer_recommendations (
    recommendation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    article_id VARCHAR(255) NOT NULL, -- Recommended article ID
    category_id UUID REFERENCES categories(category_id),
    
    -- Recommendation details
    recommendation_type VARCHAR(50), -- 'collaborative', 'content_based', 'hybrid'
    recommendation_score DECIMAL(10, 6) NOT NULL, -- Recommendation strength (0-1)
    rank_position INTEGER, -- Position in recommendation list (1, 2, 3, ...)
    
    -- Model information
    model_version VARCHAR(50),
    algorithm VARCHAR(100), -- 'NMF', 'content_based', 'hybrid'
    
    -- User interaction
    was_shown BOOLEAN DEFAULT FALSE,
    was_clicked BOOLEAN DEFAULT FALSE,
    was_viewed BOOLEAN DEFAULT FALSE,
    clicked_at TIMESTAMP WITH TIME ZONE,
    viewed_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE -- Recommendation expiration
);

-- ============================================================================
-- CONVERSION PREDICTIONS
-- ============================================================================

-- Subscription conversion predictions
CREATE TABLE IF NOT EXISTS customer_conversion_predictions (
    prediction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    prediction_date DATE NOT NULL,
    
    -- Prediction results
    conversion_probability DECIMAL(5, 4) NOT NULL, -- Between 0 and 1
    conversion_tier VARCHAR(50), -- Predicted subscription tier
    conversion_value DECIMAL(10, 2), -- Predicted lifetime value
    
    -- Model information
    model_version VARCHAR(50) NOT NULL,
    model_confidence DECIMAL(5, 4),
    feature_importance JSONB DEFAULT '{}'::jsonb,
    
    -- Actual conversion (for validation)
    did_convert BOOLEAN DEFAULT FALSE,
    converted_at TIMESTAMP WITH TIME ZONE,
    actual_tier VARCHAR(50),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_user_brand_conversion_prediction_date UNIQUE (user_id, brand_id, prediction_date)
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Customer sessions indexes
CREATE INDEX IF NOT EXISTS idx_customer_sessions_user_id ON customer_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_customer_sessions_brand_id ON customer_sessions(brand_id);
CREATE INDEX IF NOT EXISTS idx_customer_sessions_session_start ON customer_sessions(session_start);
CREATE INDEX IF NOT EXISTS idx_customer_sessions_user_brand ON customer_sessions(user_id, brand_id);
-- Note: Cannot index DATE(session_start) directly as DATE() is not immutable
-- Use session_start timestamp index instead, or create a date column with trigger
CREATE INDEX IF NOT EXISTS idx_customer_sessions_country ON customer_sessions(country_code);
CREATE INDEX IF NOT EXISTS idx_customer_sessions_device ON customer_sessions(device_type_id);
CREATE INDEX IF NOT EXISTS idx_customer_sessions_segment ON customer_sessions(user_segment);

-- Customer events indexes
CREATE INDEX IF NOT EXISTS idx_customer_events_session_id ON customer_events(session_id);
CREATE INDEX IF NOT EXISTS idx_customer_events_user_id ON customer_events(user_id);
CREATE INDEX IF NOT EXISTS idx_customer_events_brand_id ON customer_events(brand_id);
CREATE INDEX IF NOT EXISTS idx_customer_events_event_timestamp ON customer_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_customer_events_event_type ON customer_events(event_type);
CREATE INDEX IF NOT EXISTS idx_customer_events_article_id ON customer_events(article_id) WHERE article_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_customer_events_category ON customer_events(category_id) WHERE category_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_customer_events_date ON customer_events(event_date);

-- User features indexes
CREATE INDEX IF NOT EXISTS idx_user_features_user_id ON customer_user_features(user_id);
CREATE INDEX IF NOT EXISTS idx_user_features_brand_id ON customer_user_features(brand_id);
CREATE INDEX IF NOT EXISTS idx_user_features_date ON customer_user_features(feature_date);
CREATE INDEX IF NOT EXISTS idx_user_features_user_brand_date ON customer_user_features(user_id, brand_id, feature_date);
CREATE INDEX IF NOT EXISTS idx_user_features_churn_risk ON customer_user_features(churn_risk_score) WHERE churn_risk_score IS NOT NULL;

-- User segments indexes
CREATE INDEX IF NOT EXISTS idx_user_segments_brand_id ON customer_user_segments(brand_id);
CREATE INDEX IF NOT EXISTS idx_user_segments_active ON customer_user_segments(brand_id, is_active);

-- User segment assignments indexes
CREATE INDEX IF NOT EXISTS idx_segment_assignments_user_id ON customer_user_segment_assignments(user_id);
CREATE INDEX IF NOT EXISTS idx_segment_assignments_brand_id ON customer_user_segment_assignments(brand_id);
CREATE INDEX IF NOT EXISTS idx_segment_assignments_segment_id ON customer_user_segment_assignments(segment_id);
CREATE INDEX IF NOT EXISTS idx_segment_assignments_current ON customer_user_segment_assignments(user_id, brand_id, is_current) WHERE is_current = TRUE;

-- Churn predictions indexes
CREATE INDEX IF NOT EXISTS idx_churn_predictions_user_id ON customer_churn_predictions(user_id);
CREATE INDEX IF NOT EXISTS idx_churn_predictions_brand_id ON customer_churn_predictions(brand_id);
CREATE INDEX IF NOT EXISTS idx_churn_predictions_date ON customer_churn_predictions(prediction_date);
CREATE INDEX IF NOT EXISTS idx_churn_predictions_risk ON customer_churn_predictions(churn_risk_level);
CREATE INDEX IF NOT EXISTS idx_churn_predictions_probability ON customer_churn_predictions(churn_probability DESC);

-- Recommendations indexes
CREATE INDEX IF NOT EXISTS idx_recommendations_user_id ON customer_recommendations(user_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_brand_id ON customer_recommendations(brand_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_article_id ON customer_recommendations(article_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_score ON customer_recommendations(recommendation_score DESC);
CREATE INDEX IF NOT EXISTS idx_recommendations_user_brand ON customer_recommendations(user_id, brand_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_recommendations_active ON customer_recommendations(user_id, brand_id) WHERE expires_at IS NULL;

-- Conversion predictions indexes
CREATE INDEX IF NOT EXISTS idx_conversion_predictions_user_id ON customer_conversion_predictions(user_id);
CREATE INDEX IF NOT EXISTS idx_conversion_predictions_brand_id ON customer_conversion_predictions(brand_id);
CREATE INDEX IF NOT EXISTS idx_conversion_predictions_date ON customer_conversion_predictions(prediction_date);
CREATE INDEX IF NOT EXISTS idx_conversion_predictions_probability ON customer_conversion_predictions(conversion_probability DESC);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Trigger to populate event_date from event_timestamp
CREATE OR REPLACE FUNCTION set_customer_event_date()
RETURNS TRIGGER AS $$
BEGIN
    NEW.event_date = DATE(NEW.event_timestamp);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_customer_events_date 
    BEFORE INSERT OR UPDATE ON customer_events
    FOR EACH ROW 
    WHEN (NEW.event_date IS NULL)
    EXECUTE FUNCTION set_customer_event_date();

CREATE TRIGGER update_customer_sessions_updated_at BEFORE UPDATE ON customer_sessions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_features_updated_at BEFORE UPDATE ON customer_user_features
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_segments_updated_at BEFORE UPDATE ON customer_user_segments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE customer_sessions IS 'Aggregated session-level metrics for customer analytics';
COMMENT ON TABLE customer_events IS 'Raw user events from Kafka (partitioned by date)';
COMMENT ON TABLE customer_user_features IS 'ML-ready user-level features for model training';
COMMENT ON TABLE customer_user_segments IS 'User segments from ML clustering models';
COMMENT ON TABLE customer_user_segment_assignments IS 'User assignments to segments';
COMMENT ON TABLE customer_churn_predictions IS 'Churn predictions from ML models';
COMMENT ON TABLE customer_recommendations IS 'Content recommendations for users';
COMMENT ON TABLE customer_conversion_predictions IS 'Subscription conversion predictions';

