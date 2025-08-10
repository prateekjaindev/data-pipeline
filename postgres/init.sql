-- ICO Analytics Database Schema
-- Create database extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS processed_data;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw Events Tables (Partitioned by date)
-- Clickstream Events
CREATE TABLE raw_data.clickstream_events (
    event_id UUID PRIMARY KEY,
    session_id UUID NOT NULL,
    user_id VARCHAR(100),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    event_date DATE GENERATED ALWAYS AS (timestamp::date) STORED,
    event_type VARCHAR(50) NOT NULL,
    page_type VARCHAR(50) NOT NULL,
    page_url TEXT NOT NULL,
    referrer TEXT,
    user_agent TEXT,
    ip_address INET,
    country VARCHAR(100),
    city VARCHAR(100),
    device_type VARCHAR(20),
    browser VARCHAR(50),
    session_duration INTEGER,
    page_load_time FLOAT,
    ico_website_id INTEGER NOT NULL,
    ico_website_name VARCHAR(100) NOT NULL,
    button_clicked VARCHAR(100),
    form_filled VARCHAR(100),
    scroll_depth FLOAT,
    bounce BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (event_date);

-- Create partitions for clickstream events (current month and next 6 months)
CREATE TABLE raw_data.clickstream_events_2024_01 PARTITION OF raw_data.clickstream_events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE raw_data.clickstream_events_2024_02 PARTITION OF raw_data.clickstream_events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE raw_data.clickstream_events_2024_03 PARTITION OF raw_data.clickstream_events
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE raw_data.clickstream_events_2024_04 PARTITION OF raw_data.clickstream_events
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE raw_data.clickstream_events_2024_05 PARTITION OF raw_data.clickstream_events
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE raw_data.clickstream_events_2024_06 PARTITION OF raw_data.clickstream_events
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE raw_data.clickstream_events_2024_07 PARTITION OF raw_data.clickstream_events
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE raw_data.clickstream_events_2024_08 PARTITION OF raw_data.clickstream_events
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

-- User Registrations
CREATE TABLE raw_data.user_registrations (
    registration_id UUID PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL UNIQUE,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    wallet_address VARCHAR(100),
    social_handles JSONB,
    consent_marketing BOOLEAN DEFAULT FALSE,
    consent_data BOOLEAN DEFAULT TRUE,
    country VARCHAR(100),
    ip_address INET,
    ico_website_id INTEGER NOT NULL,
    referral_source VARCHAR(100),
    kyc_completed BOOLEAN DEFAULT FALSE,
    verification_tier VARCHAR(20) DEFAULT 'basic',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Conversion Events
CREATE TABLE raw_data.conversions (
    conversion_id UUID PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    session_id UUID NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    conversion_type VARCHAR(50) NOT NULL,
    ico_website_id INTEGER NOT NULL,
    amount_usd DECIMAL(15,2),
    token_amount DECIMAL(20,6),
    payment_method VARCHAR(20),
    funnel_stage VARCHAR(50),
    time_to_convert INTEGER,
    previous_visits INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ICO Websites Reference Table
CREATE TABLE processed_data.ico_websites (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    domain VARCHAR(255) NOT NULL UNIQUE,
    token_symbol VARCHAR(10),
    category VARCHAR(50),
    target_raise BIGINT,
    launch_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- User Profiles (Processed)
CREATE TABLE processed_data.user_profiles (
    user_id VARCHAR(100) PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    wallet_address VARCHAR(100),
    country VARCHAR(100),
    registration_date TIMESTAMP WITH TIME ZONE,
    first_seen TIMESTAMP WITH TIME ZONE,
    last_seen TIMESTAMP WITH TIME ZONE,
    total_sessions INTEGER DEFAULT 0,
    total_page_views INTEGER DEFAULT 0,
    total_time_spent INTEGER DEFAULT 0,
    conversion_count INTEGER DEFAULT 0,
    total_spent_usd DECIMAL(15,2) DEFAULT 0,
    preferred_ico_websites INTEGER[],
    device_types VARCHAR(20)[],
    marketing_consent BOOLEAN DEFAULT FALSE,
    kyc_status VARCHAR(20) DEFAULT 'pending',
    risk_score FLOAT DEFAULT 0.5,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Real-time Analytics Tables
CREATE TABLE analytics.realtime_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value FLOAT NOT NULL,
    dimensions JSONB,
    ico_website_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Aggregated Metrics (5-minute windows)
CREATE TABLE analytics.metrics_5min (
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    ico_website_id INTEGER NOT NULL,
    country VARCHAR(100),
    device_type VARCHAR(20),
    total_events BIGINT DEFAULT 0,
    unique_sessions BIGINT DEFAULT 0,
    unique_users BIGINT DEFAULT 0,
    page_views BIGINT DEFAULT 0,
    registrations BIGINT DEFAULT 0,
    conversions BIGINT DEFAULT 0,
    total_revenue_usd DECIMAL(15,2) DEFAULT 0,
    avg_session_duration FLOAT DEFAULT 0,
    avg_page_load_time FLOAT DEFAULT 0,
    bounce_rate FLOAT DEFAULT 0,
    conversion_rate FLOAT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, ico_website_id, COALESCE(country, ''), COALESCE(device_type, ''))
);

-- Aggregated Metrics (15-minute windows)
CREATE TABLE analytics.metrics_15min (
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    ico_website_id INTEGER NOT NULL,
    country VARCHAR(100),
    device_type VARCHAR(20),
    total_events BIGINT DEFAULT 0,
    unique_sessions BIGINT DEFAULT 0,
    unique_users BIGINT DEFAULT 0,
    page_views BIGINT DEFAULT 0,
    registrations BIGINT DEFAULT 0,
    conversions BIGINT DEFAULT 0,
    total_revenue_usd DECIMAL(15,2) DEFAULT 0,
    avg_session_duration FLOAT DEFAULT 0,
    avg_page_load_time FLOAT DEFAULT 0,
    bounce_rate FLOAT DEFAULT 0,
    conversion_rate FLOAT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, ico_website_id, COALESCE(country, ''), COALESCE(device_type, ''))
);

-- Hourly Aggregated Metrics
CREATE TABLE analytics.metrics_hourly (
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    ico_website_id INTEGER NOT NULL,
    country VARCHAR(100),
    device_type VARCHAR(20),
    total_events BIGINT DEFAULT 0,
    unique_sessions BIGINT DEFAULT 0,
    unique_users BIGINT DEFAULT 0,
    page_views BIGINT DEFAULT 0,
    registrations BIGINT DEFAULT 0,
    conversions BIGINT DEFAULT 0,
    total_revenue_usd DECIMAL(15,2) DEFAULT 0,
    avg_session_duration FLOAT DEFAULT 0,
    avg_page_load_time FLOAT DEFAULT 0,
    bounce_rate FLOAT DEFAULT 0,
    conversion_rate FLOAT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, ico_website_id, COALESCE(country, ''), COALESCE(device_type, ''))
);

-- Daily Aggregated Metrics
CREATE TABLE analytics.metrics_daily (
    date DATE NOT NULL,
    ico_website_id INTEGER NOT NULL,
    country VARCHAR(100),
    device_type VARCHAR(20),
    total_events BIGINT DEFAULT 0,
    unique_sessions BIGINT DEFAULT 0,
    unique_users BIGINT DEFAULT 0,
    page_views BIGINT DEFAULT 0,
    registrations BIGINT DEFAULT 0,
    conversions BIGINT DEFAULT 0,
    total_revenue_usd DECIMAL(15,2) DEFAULT 0,
    avg_session_duration FLOAT DEFAULT 0,
    avg_page_load_time FLOAT DEFAULT 0,
    bounce_rate FLOAT DEFAULT 0,
    conversion_rate FLOAT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ico_website_id, COALESCE(country, ''), COALESCE(device_type, ''))
);

-- Indexes for performance
-- Clickstream events indexes
CREATE INDEX idx_clickstream_timestamp ON raw_data.clickstream_events (timestamp);
CREATE INDEX idx_clickstream_session_id ON raw_data.clickstream_events (session_id);
CREATE INDEX idx_clickstream_user_id ON raw_data.clickstream_events (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_clickstream_ico_website ON raw_data.clickstream_events (ico_website_id);
CREATE INDEX idx_clickstream_country ON raw_data.clickstream_events (country);
CREATE INDEX idx_clickstream_event_type ON raw_data.clickstream_events (event_type);

-- User registrations indexes
CREATE INDEX idx_registrations_timestamp ON raw_data.user_registrations (timestamp);
CREATE INDEX idx_registrations_ico_website ON raw_data.user_registrations (ico_website_id);
CREATE INDEX idx_registrations_country ON raw_data.user_registrations (country);

-- Conversions indexes
CREATE INDEX idx_conversions_timestamp ON raw_data.conversions (timestamp);
CREATE INDEX idx_conversions_user_id ON raw_data.conversions (user_id);
CREATE INDEX idx_conversions_ico_website ON raw_data.conversions (ico_website_id);
CREATE INDEX idx_conversions_type ON raw_data.conversions (conversion_type);

-- Analytics indexes
CREATE INDEX idx_realtime_timestamp ON analytics.realtime_metrics (timestamp);
CREATE INDEX idx_realtime_metric_name ON analytics.realtime_metrics (metric_name);
CREATE INDEX idx_realtime_ico_website ON analytics.realtime_metrics (ico_website_id);

CREATE INDEX idx_metrics_5min_window ON analytics.metrics_5min (window_start);
CREATE INDEX idx_metrics_15min_window ON analytics.metrics_15min (window_start);
CREATE INDEX idx_metrics_hourly_window ON analytics.metrics_hourly (window_start);
CREATE INDEX idx_metrics_daily_date ON analytics.metrics_daily (date);

-- Materialized Views for common queries
CREATE MATERIALIZED VIEW analytics.current_active_sessions AS
SELECT 
    COUNT(DISTINCT session_id) as active_sessions,
    ico_website_id,
    country,
    device_type,
    CURRENT_TIMESTAMP as last_updated
FROM raw_data.clickstream_events 
WHERE timestamp >= NOW() - INTERVAL '30 minutes'
GROUP BY ico_website_id, country, device_type;

CREATE MATERIALIZED VIEW analytics.top_pages_today AS
SELECT 
    page_type,
    ico_website_id,
    ico_website_name,
    COUNT(*) as page_views,
    COUNT(DISTINCT session_id) as unique_sessions,
    AVG(page_load_time) as avg_load_time,
    CURRENT_TIMESTAMP as last_updated
FROM raw_data.clickstream_events 
WHERE event_date = CURRENT_DATE
  AND event_type = 'page_view'
GROUP BY page_type, ico_website_id, ico_website_name
ORDER BY page_views DESC;

CREATE MATERIALIZED VIEW analytics.conversion_funnel AS
SELECT 
    ico_website_id,
    ico_website_name,
    COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN session_id END) as visitors,
    COUNT(DISTINCT CASE WHEN page_type = 'register' THEN session_id END) as register_views,
    COUNT(DISTINCT r.user_id) as registrations,
    COUNT(DISTINCT c.user_id) as conversions,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN session_id END) > 0 
        THEN COUNT(DISTINCT r.user_id)::float / COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN session_id END) * 100 
        ELSE 0 
    END as registration_rate,
    CASE 
        WHEN COUNT(DISTINCT r.user_id) > 0 
        THEN COUNT(DISTINCT c.user_id)::float / COUNT(DISTINCT r.user_id) * 100 
        ELSE 0 
    END as conversion_rate,
    CURRENT_TIMESTAMP as last_updated
FROM raw_data.clickstream_events ce
LEFT JOIN raw_data.user_registrations r ON ce.ico_website_id = r.ico_website_id
LEFT JOIN raw_data.conversions c ON r.user_id = c.user_id 
WHERE ce.event_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY ico_website_id, ico_website_name;

-- Functions for data maintenance
CREATE OR REPLACE FUNCTION create_clickstream_partition(partition_date DATE)
RETURNS void AS $$
DECLARE
    partition_name text;
    start_date date;
    end_date date;
BEGIN
    partition_name := 'clickstream_events_' || to_char(partition_date, 'YYYY_MM');
    start_date := date_trunc('month', partition_date)::date;
    end_date := (date_trunc('month', partition_date) + interval '1 month')::date;
    
    EXECUTE format('CREATE TABLE IF NOT EXISTS raw_data.%I PARTITION OF raw_data.clickstream_events
                    FOR VALUES FROM (%L) TO (%L)', 
                    partition_name, start_date, end_date);
END;
$$ LANGUAGE plpgsql;

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW analytics.current_active_sessions;
    REFRESH MATERIALIZED VIEW analytics.top_pages_today;
    REFRESH MATERIALIZED VIEW analytics.conversion_funnel;
END;
$$ LANGUAGE plpgsql;

-- Insert sample ICO websites data
INSERT INTO processed_data.ico_websites (id, name, domain, token_symbol, category, target_raise, launch_date) VALUES
(1, 'CryptoCoin', 'cryptocoin.io', 'CRC', 'DeFi', 50000000, '2024-03-15'),
(2, 'BlockVault', 'blockvault.com', 'BVT', 'Security', 25000000, '2024-04-01'),
(3, 'MetaChain', 'metacchain.org', 'META', 'Gaming', 75000000, '2024-02-20'),
(4, 'EcoToken', 'ecotoken.green', 'ECO', 'Sustainability', 30000000, '2024-05-10'),
(5, 'AIChain', 'aichain.tech', 'AIC', 'AI', 100000000, '2024-01-30'),
(6, 'HealthLedger', 'healthledger.med', 'HLT', 'Healthcare', 40000000, '2024-06-15'),
(7, 'RealEstateDAO', 'realestate-dao.com', 'RED', 'Real Estate', 80000000, '2024-03-01'),
(8, 'SupplyChain+', 'supplychain-plus.biz', 'SCP', 'Logistics', 35000000, '2024-04-20'),
(9, 'SocialFi', 'socialfi.social', 'SOF', 'Social Media', 45000000, '2024-02-10'),
(10, 'QuantumNet', 'quantumnet.quantum', 'QNT', 'Infrastructure', 120000000, '2024-07-01'),
(11, 'LearningChain', 'learningchain.edu', 'LRN', 'Education', 20000000, '2024-05-25'),
(12, 'MusicDAO', 'musicdao.music', 'MUS', 'Entertainment', 60000000, '2024-03-30'),
(13, 'TravelCoin', 'travelcoin.travel', 'TRV', 'Travel', 30000000, '2024-04-15'),
(14, 'FoodTrace', 'foodtrace.food', 'FTR', 'Food Safety', 25000000, '2024-06-01'),
(15, 'EnergyGrid', 'energygrid.energy', 'EGR', 'Energy', 90000000, '2024-01-15'),
(16, 'SportsFan', 'sportsfan.sports', 'SPF', 'Sports', 40000000, '2024-05-05'),
(17, 'InsuranceDAO', 'insurance-dao.insure', 'IND', 'Insurance', 55000000, '2024-07-10'),
(18, 'CloudCompute', 'cloudcompute.cloud', 'CLC', 'Cloud Computing', 70000000, '2024-02-28'),
(19, 'IoTChain', 'iotchain.iot', 'IOT', 'Internet of Things', 45000000, '2024-06-20'),
(20, 'CharityChain', 'charitychain.charity', 'CCH', 'Charity', 15000000, '2024-08-01');

-- Create database users and permissions
CREATE USER flink_user WITH PASSWORD 'flink_password';
CREATE USER grafana_user WITH PASSWORD 'grafana_password';
CREATE USER analytics_user WITH PASSWORD 'analytics_password';

-- Grant permissions
GRANT USAGE ON SCHEMA raw_data TO flink_user;
GRANT USAGE ON SCHEMA processed_data TO flink_user;
GRANT USAGE ON SCHEMA analytics TO flink_user;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO flink_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA processed_data TO flink_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO flink_user;

GRANT SELECT ON ALL TABLES IN SCHEMA raw_data TO grafana_user;
GRANT SELECT ON ALL TABLES IN SCHEMA processed_data TO grafana_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO grafana_user;

GRANT SELECT ON ALL TABLES IN SCHEMA raw_data TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA processed_data TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_user;