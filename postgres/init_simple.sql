-- Simple ICO Analytics Database Schema
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS processed_data;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw Events Tables
-- Clickstream Events (without partitioning for simplicity)
CREATE TABLE IF NOT EXISTS raw_data.clickstream_events (
    event_id UUID PRIMARY KEY,
    session_id UUID NOT NULL,
    user_id VARCHAR(100),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
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
);

-- User Registrations
CREATE TABLE IF NOT EXISTS raw_data.user_registrations (
    registration_id UUID PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL UNIQUE,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    email VARCHAR(255) NOT NULL,
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
CREATE TABLE IF NOT EXISTS raw_data.conversions (
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
CREATE TABLE IF NOT EXISTS processed_data.ico_websites (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    domain VARCHAR(255) NOT NULL UNIQUE,
    token_symbol VARCHAR(10),
    category VARCHAR(50),
    target_raise BIGINT,
    launch_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_clickstream_timestamp ON raw_data.clickstream_events (timestamp);
CREATE INDEX IF NOT EXISTS idx_clickstream_session_id ON raw_data.clickstream_events (session_id);
CREATE INDEX IF NOT EXISTS idx_clickstream_ico_website ON raw_data.clickstream_events (ico_website_id);
CREATE INDEX IF NOT EXISTS idx_registrations_timestamp ON raw_data.user_registrations (timestamp);
CREATE INDEX IF NOT EXISTS idx_conversions_timestamp ON raw_data.conversions (timestamp);

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
(20, 'CharityChain', 'charitychain.charity', 'CCH', 'Charity', 15000000, '2024-08-01')
ON CONFLICT (id) DO NOTHING;

-- Create database users and permissions
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_user') THEN
        CREATE USER grafana_user WITH PASSWORD 'grafana_password';
    END IF;
END
$$;

-- Grant permissions
GRANT USAGE ON SCHEMA raw_data TO grafana_user;
GRANT USAGE ON SCHEMA processed_data TO grafana_user;
GRANT USAGE ON SCHEMA analytics TO grafana_user;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_data TO grafana_user;
GRANT SELECT ON ALL TABLES IN SCHEMA processed_data TO grafana_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO grafana_user;