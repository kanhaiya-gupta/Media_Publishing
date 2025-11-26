-- ============================================================================
-- OwnLens - Base Schema
-- ============================================================================
-- Common tables for companies, brands, countries, and shared infrastructure
-- Supports multi-company, multi-brand, multi-country architecture
-- ============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- For text search

-- ============================================================================
-- COMPANIES & BRANDS
-- ============================================================================

-- Companies table (e.g., Axel Springer, etc.)
CREATE TABLE IF NOT EXISTS companies (
    company_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_name VARCHAR(255) NOT NULL UNIQUE,
    company_code VARCHAR(50) NOT NULL UNIQUE, -- e.g., 'axel_springer'
    legal_name VARCHAR(255),
    headquarters_country_code CHAR(2), -- ISO 3166-1 alpha-2
    headquarters_city VARCHAR(100),
    website_url VARCHAR(500),
    industry VARCHAR(100),
    founded_year INTEGER,
    employee_count INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}'::jsonb, -- Additional company metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- Brands table (e.g., Bild, Die Welt, Business Insider, Politico, Sport Bild)
CREATE TABLE IF NOT EXISTS brands (
    brand_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_name VARCHAR(255) NOT NULL,
    brand_code VARCHAR(50) NOT NULL, -- e.g., 'bild', 'welt', 'business_insider'
    brand_type VARCHAR(50), -- 'newspaper', 'magazine', 'digital', 'tv', 'radio'
    description TEXT,
    website_url VARCHAR(500),
    logo_url VARCHAR(500),
    primary_language_code CHAR(5), -- ISO 639-1 (e.g., 'de', 'en')
    primary_country_code CHAR(2), -- ISO 3166-1 alpha-2
    launch_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}'::jsonb, -- Brand-specific metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100),
    CONSTRAINT unique_brand_code UNIQUE (company_id, brand_code)
);

-- Brand countries (which countries each brand operates in)
CREATE TABLE IF NOT EXISTS brand_countries (
    brand_country_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    country_code CHAR(2) NOT NULL, -- ISO 3166-1 alpha-2
    is_primary BOOLEAN DEFAULT FALSE,
    launch_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_brand_country UNIQUE (brand_id, country_code)
);

-- ============================================================================
-- COUNTRIES & GEOGRAPHY
-- ============================================================================

-- Countries reference table
CREATE TABLE IF NOT EXISTS countries (
    country_code CHAR(2) PRIMARY KEY, -- ISO 3166-1 alpha-2
    country_name VARCHAR(255) NOT NULL,
    country_name_native VARCHAR(255),
    continent_code CHAR(2), -- e.g., 'EU', 'NA', 'AS'
    region VARCHAR(100),
    timezone VARCHAR(50), -- Primary timezone
    currency_code CHAR(3), -- ISO 4217
    language_codes CHAR(5)[], -- Array of ISO 639-1 codes
    population INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Cities reference table
CREATE TABLE IF NOT EXISTS cities (
    city_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    city_name VARCHAR(255) NOT NULL,
    country_code CHAR(2) NOT NULL REFERENCES countries(country_code),
    state_province VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    population INTEGER,
    timezone VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_city_country UNIQUE (city_name, country_code)
);

-- ============================================================================
-- CATEGORIES (Shared across domains)
-- ============================================================================

-- Category hierarchy (for both editorial and company content)
CREATE TABLE IF NOT EXISTS categories (
    category_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    brand_id UUID REFERENCES brands(brand_id) ON DELETE CASCADE,
    category_name VARCHAR(255) NOT NULL,
    category_code VARCHAR(100) NOT NULL, -- e.g., 'politics', 'sports', 'business'
    parent_category_id UUID REFERENCES categories(category_id) ON DELETE SET NULL,
    category_type VARCHAR(50) NOT NULL, -- 'editorial', 'company', 'both'
    description TEXT,
    display_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_brand_category UNIQUE (brand_id, category_code)
);

-- ============================================================================
-- USERS & AUTHENTICATION (Base user structure)
-- ============================================================================

-- Base users table (shared across customer and company domains)
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_external_id VARCHAR(255), -- External system user ID
    email VARCHAR(255) UNIQUE,
    username VARCHAR(100) UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    display_name VARCHAR(255),
    phone_number VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(20),
    profile_image_url VARCHAR(500),
    primary_country_code CHAR(2) REFERENCES countries(country_code),
    primary_language_code CHAR(5),
    timezone VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    is_verified BOOLEAN DEFAULT FALSE,
    last_login_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- User accounts (for customer domain - subscriptions, etc.)
CREATE TABLE IF NOT EXISTS user_accounts (
    account_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    account_type VARCHAR(50) NOT NULL, -- 'customer', 'editorial', 'company'
    subscription_tier VARCHAR(50) DEFAULT 'free', -- 'free', 'premium', 'pro', 'enterprise'
    subscription_status VARCHAR(50) DEFAULT 'active', -- 'active', 'cancelled', 'expired', 'trial'
    subscription_start_date DATE,
    subscription_end_date DATE,
    trial_end_date DATE,
    payment_method VARCHAR(50),
    billing_country_code CHAR(2) REFERENCES countries(country_code),
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DEVICES & BROWSERS (Shared reference data)
-- ============================================================================

-- Device types reference
CREATE TABLE IF NOT EXISTS device_types (
    device_type_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    device_type_code VARCHAR(50) UNIQUE NOT NULL, -- 'desktop', 'mobile', 'tablet', 'smart_tv', 'app'
    device_type_name VARCHAR(100) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Operating systems reference
CREATE TABLE IF NOT EXISTS operating_systems (
    os_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    os_code VARCHAR(50) UNIQUE NOT NULL, -- 'iOS', 'Android', 'Windows', 'macOS', 'Linux'
    os_name VARCHAR(100) NOT NULL,
    os_family VARCHAR(50), -- 'mobile', 'desktop', 'server'
    version VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Browsers reference
CREATE TABLE IF NOT EXISTS browsers (
    browser_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    browser_code VARCHAR(50) UNIQUE NOT NULL, -- 'Chrome', 'Safari', 'Firefox', 'Edge', 'Opera'
    browser_name VARCHAR(100) NOT NULL,
    vendor VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Companies indexes
CREATE INDEX IF NOT EXISTS idx_companies_company_code ON companies(company_code);
CREATE INDEX IF NOT EXISTS idx_companies_is_active ON companies(is_active);

-- Brands indexes
CREATE INDEX IF NOT EXISTS idx_brands_company_id ON brands(company_id);
CREATE INDEX IF NOT EXISTS idx_brands_brand_code ON brands(brand_code);
CREATE INDEX IF NOT EXISTS idx_brands_is_active ON brands(is_active);
CREATE INDEX IF NOT EXISTS idx_brands_company_active ON brands(company_id, is_active);

-- Brand countries indexes
CREATE INDEX IF NOT EXISTS idx_brand_countries_brand_id ON brand_countries(brand_id);
CREATE INDEX IF NOT EXISTS idx_brand_countries_country_code ON brand_countries(country_code);
CREATE INDEX IF NOT EXISTS idx_brand_countries_active ON brand_countries(brand_id, is_active);

-- Countries indexes
CREATE INDEX IF NOT EXISTS idx_countries_continent ON countries(continent_code);
CREATE INDEX IF NOT EXISTS idx_countries_is_active ON countries(is_active);

-- Cities indexes
CREATE INDEX IF NOT EXISTS idx_cities_country_code ON cities(country_code);
CREATE INDEX IF NOT EXISTS idx_cities_country_active ON cities(country_code, is_active);
CREATE INDEX IF NOT EXISTS idx_cities_location ON cities(latitude, longitude) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Categories indexes
CREATE INDEX IF NOT EXISTS idx_categories_brand_id ON categories(brand_id);
CREATE INDEX IF NOT EXISTS idx_categories_parent_id ON categories(parent_category_id);
CREATE INDEX IF NOT EXISTS idx_categories_type ON categories(category_type);
CREATE INDEX IF NOT EXISTS idx_categories_brand_type ON categories(brand_id, category_type, is_active);

-- Users indexes
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_external_id ON users(user_external_id) WHERE user_external_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active);
CREATE INDEX IF NOT EXISTS idx_users_country ON users(primary_country_code);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);

-- User accounts indexes
CREATE INDEX IF NOT EXISTS idx_user_accounts_user_id ON user_accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_user_accounts_brand_id ON user_accounts(brand_id);
CREATE INDEX IF NOT EXISTS idx_user_accounts_type ON user_accounts(account_type);
CREATE INDEX IF NOT EXISTS idx_user_accounts_subscription ON user_accounts(subscription_tier, subscription_status);
CREATE INDEX IF NOT EXISTS idx_user_accounts_user_brand ON user_accounts(user_id, brand_id);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply updated_at trigger to relevant tables
CREATE TRIGGER update_companies_updated_at BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_brands_updated_at BEFORE UPDATE ON brands
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_countries_updated_at BEFORE UPDATE ON countries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_categories_updated_at BEFORE UPDATE ON categories
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_accounts_updated_at BEFORE UPDATE ON user_accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INITIAL DATA (Seed data)
-- ============================================================================

-- Insert default device types
INSERT INTO device_types (device_type_code, device_type_name, description) VALUES
    ('desktop', 'Desktop', 'Desktop computers'),
    ('mobile', 'Mobile', 'Mobile phones'),
    ('tablet', 'Tablet', 'Tablet devices'),
    ('smart_tv', 'Smart TV', 'Smart television devices'),
    ('app', 'Mobile App', 'Mobile application')
ON CONFLICT (device_type_code) DO NOTHING;

-- Insert default operating systems
INSERT INTO operating_systems (os_code, os_name, os_family) VALUES
    ('iOS', 'iOS', 'mobile'),
    ('Android', 'Android', 'mobile'),
    ('Windows', 'Windows', 'desktop'),
    ('macOS', 'macOS', 'desktop'),
    ('Linux', 'Linux', 'desktop')
ON CONFLICT (os_code) DO NOTHING;

-- Insert default browsers
INSERT INTO browsers (browser_code, browser_name, vendor) VALUES
    ('Chrome', 'Google Chrome', 'Google'),
    ('Safari', 'Safari', 'Apple'),
    ('Firefox', 'Mozilla Firefox', 'Mozilla'),
    ('Edge', 'Microsoft Edge', 'Microsoft'),
    ('Opera', 'Opera', 'Opera Software')
ON CONFLICT (browser_code) DO NOTHING;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE companies IS 'Companies that own multiple brands (e.g., Axel Springer)';
COMMENT ON TABLE brands IS 'Brands owned by companies (e.g., Bild, Die Welt, Business Insider)';
COMMENT ON TABLE brand_countries IS 'Countries where each brand operates';
COMMENT ON TABLE countries IS 'Reference table for countries (ISO 3166-1 alpha-2)';
COMMENT ON TABLE cities IS 'Reference table for cities with geographic data';
COMMENT ON TABLE categories IS 'Content categories hierarchy (shared across domains)';
COMMENT ON TABLE users IS 'Base users table (shared across customer and company domains)';
COMMENT ON TABLE user_accounts IS 'User accounts with subscription information';
COMMENT ON TABLE device_types IS 'Reference table for device types';
COMMENT ON TABLE operating_systems IS 'Reference table for operating systems';
COMMENT ON TABLE browsers IS 'Reference table for web browsers';

