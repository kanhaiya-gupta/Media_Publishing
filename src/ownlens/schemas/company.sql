-- ============================================================================
-- OwnLens - Company Domain Schema
-- ============================================================================
-- Company analytics: internal communications, company content, department analytics
-- ============================================================================

-- ============================================================================
-- DEPARTMENTS
-- ============================================================================

-- Departments within companies
CREATE TABLE IF NOT EXISTS company_departments (
    department_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Department information
    department_name VARCHAR(255) NOT NULL,
    department_code VARCHAR(100) NOT NULL, -- Unique identifier within company
    parent_department_id UUID REFERENCES company_departments(department_id) ON DELETE SET NULL, -- For hierarchy
    description TEXT,
    
    -- Geographic information
    primary_country_code CHAR(2) REFERENCES countries(country_code),
    primary_city_id UUID REFERENCES cities(city_id),
    office_address TEXT,
    
    -- Department metrics
    employee_count INTEGER DEFAULT 0,
    budget DECIMAL(15, 2),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    established_date DATE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_company_department_code UNIQUE (company_id, department_code)
);

-- ============================================================================
-- EMPLOYEES (Company users)
-- ============================================================================

-- Employees table (links to users table)
CREATE TABLE IF NOT EXISTS company_employees (
    employee_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    department_id UUID REFERENCES company_departments(department_id) ON DELETE SET NULL,
    
    -- Employee information
    employee_number VARCHAR(100) UNIQUE, -- Employee ID number
    job_title VARCHAR(255),
    job_level VARCHAR(50), -- 'junior', 'mid', 'senior', 'lead', 'manager', 'director', 'executive'
    employment_type VARCHAR(50), -- 'full_time', 'part_time', 'contract', 'intern'
    
    -- Employment dates
    hire_date DATE NOT NULL,
    termination_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Manager relationship
    manager_id UUID REFERENCES company_employees(employee_id) ON DELETE SET NULL,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_user_company UNIQUE (user_id, company_id)
);

-- ============================================================================
-- INTERNAL CONTENT
-- ============================================================================

-- Internal company content (announcements, newsletters, internal articles)
CREATE TABLE IF NOT EXISTS company_internal_content (
    content_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    department_id UUID REFERENCES company_departments(department_id) ON DELETE SET NULL,
    
    -- Content information
    title VARCHAR(500) NOT NULL,
    content_type VARCHAR(50) NOT NULL, -- 'announcement', 'newsletter', 'internal_article', 'policy', 'update', 'event'
    summary TEXT,
    content_url VARCHAR(1000),
    content_body TEXT, -- Full content (if stored)
    
    -- Author information
    author_employee_id UUID REFERENCES company_employees(employee_id) ON DELETE SET NULL,
    author_department_id UUID REFERENCES company_departments(department_id),
    
    -- Category information
    category_id UUID REFERENCES categories(category_id),
    tags VARCHAR(100)[],
    
    -- Publishing information
    publish_time TIMESTAMP WITH TIME ZONE NOT NULL,
    publish_date DATE NOT NULL,
    expiry_date DATE, -- Content expiration date
    status VARCHAR(50) DEFAULT 'draft', -- 'draft', 'scheduled', 'published', 'archived', 'deleted'
    
    -- Target audience
    target_departments UUID[], -- Array of department IDs
    target_employee_levels VARCHAR(50)[], -- Array of job levels
    target_countries CHAR(2)[], -- Array of country codes
    is_company_wide BOOLEAN DEFAULT FALSE, -- Available to all employees
    
    -- Priority
    priority_level VARCHAR(20) DEFAULT 'normal', -- 'low', 'normal', 'high', 'urgent'
    is_important BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INTERNAL CONTENT PERFORMANCE
-- ============================================================================

-- Internal content performance metrics (aggregated by date)
CREATE TABLE IF NOT EXISTS company_content_performance (
    performance_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content_id UUID NOT NULL REFERENCES company_internal_content(content_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    performance_date DATE NOT NULL,
    
    -- View metrics
    total_views INTEGER DEFAULT 0,
    unique_views INTEGER DEFAULT 0,
    unique_employees INTEGER DEFAULT 0, -- Unique employees who viewed
    
    -- Engagement metrics
    total_clicks INTEGER DEFAULT 0,
    total_shares INTEGER DEFAULT 0,
    total_comments INTEGER DEFAULT 0,
    total_likes INTEGER DEFAULT 0,
    total_bookmarks INTEGER DEFAULT 0,
    total_downloads INTEGER DEFAULT 0,
    
    -- Reading metrics
    avg_time_on_page_sec INTEGER,
    avg_scroll_depth INTEGER, -- Percentage
    completion_rate DECIMAL(5, 4), -- Percentage who finished reading
    
    -- Department breakdown
    views_by_department JSONB DEFAULT '{}'::jsonb, -- {department_id: view_count}
    engagement_by_department JSONB DEFAULT '{}'::jsonb, -- {department_id: engagement_score}
    
    -- Geographic breakdown
    views_by_country JSONB DEFAULT '{}'::jsonb, -- {country_code: view_count}
    views_by_city JSONB DEFAULT '{}'::jsonb, -- {city_id: view_count}
    
    -- Employee level breakdown
    views_by_level JSONB DEFAULT '{}'::jsonb, -- {job_level: view_count}
    
    -- Engagement score
    engagement_score DECIMAL(10, 2), -- Calculated engagement score
    reach_percentage DECIMAL(5, 4), -- Percentage of target audience reached
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_content_date UNIQUE (content_id, performance_date)
);

-- ============================================================================
-- DEPARTMENT PERFORMANCE
-- ============================================================================

-- Department performance metrics (aggregated by date)
CREATE TABLE IF NOT EXISTS company_department_performance (
    performance_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    department_id UUID NOT NULL REFERENCES company_departments(department_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    performance_date DATE NOT NULL,
    
    -- Content metrics
    content_published INTEGER DEFAULT 0,
    content_views INTEGER DEFAULT 0,
    content_engagement INTEGER DEFAULT 0,
    avg_engagement_score DECIMAL(10, 2),
    
    -- Employee engagement
    active_employees INTEGER DEFAULT 0, -- Employees who engaged with content
    employee_engagement_rate DECIMAL(5, 4), -- Percentage of employees engaged
    avg_views_per_employee DECIMAL(10, 2),
    
    -- Top content
    top_content_id UUID REFERENCES company_internal_content(content_id),
    top_content_views INTEGER,
    top_content_engagement DECIMAL(10, 2),
    
    -- Geographic metrics
    views_by_country JSONB DEFAULT '{}'::jsonb,
    views_by_city JSONB DEFAULT '{}'::jsonb,
    
    -- Ranking
    department_rank INTEGER, -- Rank among departments
    engagement_rank INTEGER, -- Rank by engagement
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_department_date UNIQUE (department_id, performance_date)
);

-- ============================================================================
-- COMPANY EVENTS (Internal content events)
-- ============================================================================

-- Company content events from Kafka
CREATE TABLE IF NOT EXISTS company_content_events (
    event_id UUID DEFAULT uuid_generate_v4(),
    content_id UUID NOT NULL REFERENCES company_internal_content(content_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    employee_id UUID REFERENCES company_employees(employee_id) ON DELETE SET NULL,
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    
    -- Event information
    event_type VARCHAR(50) NOT NULL, -- 'content_view', 'content_click', 'share', 'comment', 'like', 'download'
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Employee information
    department_id UUID REFERENCES company_departments(department_id),
    job_level VARCHAR(50),
    
    -- Geographic information
    country_code CHAR(2) REFERENCES countries(country_code),
    city_id UUID REFERENCES cities(city_id),
    
    -- Device information
    device_type_id UUID REFERENCES device_types(device_type_id),
    os_id UUID REFERENCES operating_systems(os_id),
    browser_id UUID REFERENCES browsers(browser_id),
    
    -- Engagement metrics
    engagement_metrics JSONB DEFAULT '{}'::jsonb, -- Event-specific metrics
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Partitioning by date
    event_date DATE NOT NULL,
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

-- Create monthly partitions for company_content_events (2024-2025)
CREATE TABLE IF NOT EXISTS company_content_events_2024_01 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_02 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_03 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_04 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_05 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_06 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_07 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_08 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_09 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_10 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_11 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE IF NOT EXISTS company_content_events_2024_12 PARTITION OF company_content_events
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_01 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_02 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_03 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_04 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_05 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_06 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_07 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_08 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_09 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_10 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_11 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS company_content_events_2025_12 PARTITION OF company_content_events
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- ============================================================================
-- EMPLOYEE ENGAGEMENT
-- ============================================================================

-- Employee engagement metrics (aggregated by date)
CREATE TABLE IF NOT EXISTS company_employee_engagement (
    engagement_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    employee_id UUID NOT NULL REFERENCES company_employees(employee_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    department_id UUID REFERENCES company_departments(department_id),
    engagement_date DATE NOT NULL,
    
    -- Engagement metrics
    content_views INTEGER DEFAULT 0,
    content_interactions INTEGER DEFAULT 0, -- Clicks, shares, comments, etc.
    avg_engagement_score DECIMAL(10, 2),
    time_spent_sec INTEGER DEFAULT 0, -- Total time spent on content
    
    -- Content preferences
    preferred_content_types VARCHAR(50)[], -- Array of content types
    preferred_categories UUID[], -- Array of category IDs
    
    -- Activity metrics
    active_days INTEGER DEFAULT 0, -- Days with activity
    first_interaction_time TIMESTAMP WITH TIME ZONE,
    last_interaction_time TIMESTAMP WITH TIME ZONE,
    
    -- Engagement level
    engagement_level VARCHAR(20), -- 'low', 'medium', 'high', 'very_high'
    engagement_score DECIMAL(10, 2), -- Overall engagement score
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_employee_date UNIQUE (employee_id, engagement_date)
);

-- ============================================================================
-- INTERNAL COMMUNICATIONS ANALYTICS
-- ============================================================================

-- Overall internal communications analytics (aggregated by date)
CREATE TABLE IF NOT EXISTS company_communications_analytics (
    analytics_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    analytics_date DATE NOT NULL,
    
    -- Content metrics
    total_content_published INTEGER DEFAULT 0,
    total_content_views INTEGER DEFAULT 0,
    unique_employees_reached INTEGER DEFAULT 0,
    reach_percentage DECIMAL(5, 4), -- Percentage of total employees reached
    
    -- Engagement metrics
    total_engagement INTEGER DEFAULT 0,
    avg_engagement_score DECIMAL(10, 2),
    engagement_rate DECIMAL(5, 4), -- Engagement per view
    
    -- Department metrics
    active_departments INTEGER DEFAULT 0, -- Departments with activity
    top_department_id UUID REFERENCES company_departments(department_id),
    
    -- Content type breakdown
    views_by_content_type JSONB DEFAULT '{}'::jsonb, -- {content_type: view_count}
    engagement_by_content_type JSONB DEFAULT '{}'::jsonb, -- {content_type: engagement_score}
    
    -- Geographic breakdown
    views_by_country JSONB DEFAULT '{}'::jsonb,
    views_by_city JSONB DEFAULT '{}'::jsonb,
    
    -- Employee level breakdown
    views_by_level JSONB DEFAULT '{}'::jsonb,
    engagement_by_level JSONB DEFAULT '{}'::jsonb,
    
    -- Trends
    views_growth_rate DECIMAL(5, 4), -- Percentage change from previous period
    engagement_growth_rate DECIMAL(5, 4),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_company_date UNIQUE (company_id, analytics_date)
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Departments indexes
CREATE INDEX IF NOT EXISTS idx_company_departments_company_id ON company_departments(company_id);
CREATE INDEX IF NOT EXISTS idx_company_departments_parent_id ON company_departments(parent_department_id) WHERE parent_department_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_company_departments_code ON company_departments(company_id, department_code);
CREATE INDEX IF NOT EXISTS idx_company_departments_active ON company_departments(company_id, is_active);

-- Employees indexes
CREATE INDEX IF NOT EXISTS idx_company_employees_user_id ON company_employees(user_id);
CREATE INDEX IF NOT EXISTS idx_company_employees_company_id ON company_employees(company_id);
CREATE INDEX IF NOT EXISTS idx_company_employees_department_id ON company_employees(department_id);
CREATE INDEX IF NOT EXISTS idx_company_employees_manager_id ON company_employees(manager_id) WHERE manager_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_company_employees_active ON company_employees(company_id, is_active);
CREATE INDEX IF NOT EXISTS idx_company_employees_employee_number ON company_employees(employee_number) WHERE employee_number IS NOT NULL;

-- Internal content indexes
CREATE INDEX IF NOT EXISTS idx_internal_content_company_id ON company_internal_content(company_id);
CREATE INDEX IF NOT EXISTS idx_internal_content_department_id ON company_internal_content(department_id);
CREATE INDEX IF NOT EXISTS idx_internal_content_author_id ON company_internal_content(author_employee_id);
CREATE INDEX IF NOT EXISTS idx_internal_content_publish_time ON company_internal_content(publish_time);
CREATE INDEX IF NOT EXISTS idx_internal_content_publish_date ON company_internal_content(publish_date);
CREATE INDEX IF NOT EXISTS idx_internal_content_status ON company_internal_content(status);
CREATE INDEX IF NOT EXISTS idx_internal_content_type ON company_internal_content(content_type);
CREATE INDEX IF NOT EXISTS idx_internal_content_company_status ON company_internal_content(company_id, status);
CREATE INDEX IF NOT EXISTS idx_internal_content_important ON company_internal_content(company_id, is_important) WHERE is_important = TRUE;

-- Content performance indexes
CREATE INDEX IF NOT EXISTS idx_content_performance_content_id ON company_content_performance(content_id);
CREATE INDEX IF NOT EXISTS idx_content_performance_company_id ON company_content_performance(company_id);
CREATE INDEX IF NOT EXISTS idx_content_performance_date ON company_content_performance(performance_date);
CREATE INDEX IF NOT EXISTS idx_content_performance_views ON company_content_performance(total_views DESC);
CREATE INDEX IF NOT EXISTS idx_content_performance_engagement ON company_content_performance(engagement_score DESC);
CREATE INDEX IF NOT EXISTS idx_content_performance_content_date ON company_content_performance(content_id, performance_date);

-- Department performance indexes
CREATE INDEX IF NOT EXISTS idx_department_performance_department_id ON company_department_performance(department_id);
CREATE INDEX IF NOT EXISTS idx_department_performance_company_id ON company_department_performance(company_id);
CREATE INDEX IF NOT EXISTS idx_department_performance_date ON company_department_performance(performance_date);
CREATE INDEX IF NOT EXISTS idx_department_performance_engagement ON company_department_performance(avg_engagement_score DESC);
CREATE INDEX IF NOT EXISTS idx_department_performance_rank ON company_department_performance(performance_date, department_rank);

-- Company events indexes
CREATE INDEX IF NOT EXISTS idx_company_events_content_id ON company_content_events(content_id);
CREATE INDEX IF NOT EXISTS idx_company_events_company_id ON company_content_events(company_id);
CREATE INDEX IF NOT EXISTS idx_company_events_employee_id ON company_content_events(employee_id) WHERE employee_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_company_events_user_id ON company_content_events(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_company_events_event_timestamp ON company_content_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_company_events_event_type ON company_content_events(event_type);
CREATE INDEX IF NOT EXISTS idx_company_events_department_id ON company_content_events(department_id) WHERE department_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_company_events_date ON company_content_events(event_date);

-- Employee engagement indexes
CREATE INDEX IF NOT EXISTS idx_employee_engagement_employee_id ON company_employee_engagement(employee_id);
CREATE INDEX IF NOT EXISTS idx_employee_engagement_company_id ON company_employee_engagement(company_id);
CREATE INDEX IF NOT EXISTS idx_employee_engagement_department_id ON company_employee_engagement(department_id);
CREATE INDEX IF NOT EXISTS idx_employee_engagement_date ON company_employee_engagement(engagement_date);
CREATE INDEX IF NOT EXISTS idx_employee_engagement_score ON company_employee_engagement(engagement_score DESC);
CREATE INDEX IF NOT EXISTS idx_employee_engagement_level ON company_employee_engagement(engagement_level);
CREATE INDEX IF NOT EXISTS idx_employee_engagement_employee_date ON company_employee_engagement(employee_id, engagement_date);

-- Communications analytics indexes
CREATE INDEX IF NOT EXISTS idx_communications_analytics_company_id ON company_communications_analytics(company_id);
CREATE INDEX IF NOT EXISTS idx_communications_analytics_date ON company_communications_analytics(analytics_date);
CREATE INDEX IF NOT EXISTS idx_communications_analytics_company_date ON company_communications_analytics(company_id, analytics_date);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Triggers to populate date columns from timestamp columns
CREATE OR REPLACE FUNCTION set_company_date_from_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_TABLE_NAME = 'company_internal_content' THEN
        NEW.publish_date = DATE(NEW.publish_time);
    ELSIF TG_TABLE_NAME = 'company_content_events' THEN
        NEW.event_date = DATE(NEW.event_timestamp);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_company_internal_content_publish_date 
    BEFORE INSERT OR UPDATE ON company_internal_content
    FOR EACH ROW 
    WHEN (NEW.publish_date IS NULL)
    EXECUTE FUNCTION set_company_date_from_timestamp();

CREATE TRIGGER set_company_content_events_date 
    BEFORE INSERT OR UPDATE ON company_content_events
    FOR EACH ROW 
    WHEN (NEW.event_date IS NULL)
    EXECUTE FUNCTION set_company_date_from_timestamp();

CREATE TRIGGER update_company_departments_updated_at BEFORE UPDATE ON company_departments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_company_employees_updated_at BEFORE UPDATE ON company_employees
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_internal_content_updated_at BEFORE UPDATE ON company_internal_content
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_content_performance_updated_at BEFORE UPDATE ON company_content_performance
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_department_performance_updated_at BEFORE UPDATE ON company_department_performance
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_employee_engagement_updated_at BEFORE UPDATE ON company_employee_engagement
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_communications_analytics_updated_at BEFORE UPDATE ON company_communications_analytics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE company_departments IS 'Departments within companies';
COMMENT ON TABLE company_employees IS 'Employees linked to users (for company domain)';
COMMENT ON TABLE company_internal_content IS 'Internal company content (announcements, newsletters, etc.)';
COMMENT ON TABLE company_content_performance IS 'Daily aggregated internal content performance metrics';
COMMENT ON TABLE company_department_performance IS 'Daily aggregated department performance metrics';
COMMENT ON TABLE company_content_events IS 'Raw company content events from Kafka (partitioned by date)';
COMMENT ON TABLE company_employee_engagement IS 'Daily aggregated employee engagement metrics';
COMMENT ON TABLE company_communications_analytics IS 'Overall internal communications analytics by date';

