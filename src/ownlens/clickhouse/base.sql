-- ============================================================================
-- OwnLens - Base Schema (ClickHouse)
-- ============================================================================
-- Reference data and base tables for analytics
-- Note: ClickHouse is optimized for analytics, so reference data is typically
-- stored in PostgreSQL and joined during queries or replicated to ClickHouse
-- ============================================================================

-- Create database
CREATE DATABASE IF NOT EXISTS ownlens_analytics;

USE ownlens_analytics;

-- Reference tables for analytics (denormalized for performance)
-- These are typically populated from PostgreSQL reference tables

-- Companies reference (for analytics joins)
CREATE TABLE IF NOT EXISTS companies (
    company_id String,  -- UUID as String
    company_name String,
    company_code String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (company_id)
SETTINGS index_granularity = 8192;

-- Brands reference (for analytics joins)
CREATE TABLE IF NOT EXISTS brands (
    brand_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_name String,
    brand_code String,
    brand_type String,
    primary_language_code String,
    primary_country_code String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (brand_id)
SETTINGS index_granularity = 8192;

-- Countries reference
CREATE TABLE IF NOT EXISTS countries (
    country_code String,  -- ISO 3166-1 alpha-2
    country_name String,
    continent_code String,
    region String,
    timezone String,
    currency_code String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (country_code)
SETTINGS index_granularity = 8192;

-- Cities reference
CREATE TABLE IF NOT EXISTS cities (
    city_id String,  -- UUID as String
    city_name String,
    country_code String,
    state_province String,
    latitude Float64,
    longitude Float64,
    population UInt32,
    timezone String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (city_id)
SETTINGS index_granularity = 8192;

-- Categories reference
CREATE TABLE IF NOT EXISTS categories (
    category_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    category_name String,
    category_code String,
    parent_category_id String,  -- UUID as String
    category_type String,  -- 'editorial', 'company', 'both'
    display_order UInt16 DEFAULT 0,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (category_id)
SETTINGS index_granularity = 8192;

-- Device types reference
CREATE TABLE IF NOT EXISTS device_types (
    device_type_id String,  -- UUID as String
    device_type_code String,  -- 'desktop', 'mobile', 'tablet', etc.
    device_type_name String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (device_type_id)
SETTINGS index_granularity = 8192;

-- Operating systems reference
CREATE TABLE IF NOT EXISTS operating_systems (
    os_id String,  -- UUID as String
    os_code String,  -- 'iOS', 'Android', 'Windows', etc.
    os_name String,
    os_family String,  -- 'mobile', 'desktop', 'server'
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (os_id)
SETTINGS index_granularity = 8192;

-- Browsers reference
CREATE TABLE IF NOT EXISTS browsers (
    browser_id String,  -- UUID as String
    browser_code String,  -- 'Chrome', 'Safari', 'Firefox', etc.
    browser_name String,
    vendor String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (browser_id)
SETTINGS index_granularity = 8192;

-- Brand countries reference
CREATE TABLE IF NOT EXISTS brand_countries (
    brand_country_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    country_code String,  -- ISO 3166-1 alpha-2
    is_primary UInt8 DEFAULT 0,  -- Boolean
    launch_date Date DEFAULT toDate(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (brand_id, country_code)
SETTINGS index_granularity = 8192;

-- Users reference (for analytics joins)
CREATE TABLE IF NOT EXISTS users (
    user_id String,  -- UUID as String
    user_external_id String,
    email String,
    username String,
    first_name String,
    last_name String,
    display_name String,
    phone_number String,
    date_of_birth Date DEFAULT toDate(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    gender String,
    profile_image_url String,
    primary_country_code String,  -- ISO 3166-1 alpha-2
    primary_language_code String,
    timezone String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    is_verified UInt8 DEFAULT 0,  -- Boolean
    last_login_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    deleted_at DateTime DEFAULT toDateTime(0)  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id)
SETTINGS index_granularity = 8192;

-- User accounts (for customer domain - subscriptions, etc.)
CREATE TABLE IF NOT EXISTS user_accounts (
    account_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    account_type String,  -- 'customer', 'editorial', 'company'
    subscription_tier String DEFAULT 'free',  -- 'free', 'premium', 'pro', 'enterprise'
    subscription_status String DEFAULT 'active',  -- 'active', 'cancelled', 'expired', 'trial'
    subscription_start_date Date,
    subscription_end_date Date,
    trial_end_date Date,
    payment_method String,
    billing_country_code String,  -- ISO 3166-1 alpha-2
    is_active UInt8 DEFAULT 1,  -- Boolean
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id)
SETTINGS index_granularity = 8192;

