-- Hierarchy Management System Database Schema
-- PostgreSQL Database Schema for Telecommunications Hierarchy

-- Create database (run this separately if needed)
-- CREATE DATABASE hierarchy_db;

-- Connect to the database and create tables

-- Regions table (highest level)
CREATE TABLE IF NOT EXISTS regions (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Markets table (belongs to regions)
CREATE TABLE IF NOT EXISTS markets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    region_id INTEGER NOT NULL REFERENCES regions(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, region_id)
);

-- GNBs table (belongs to markets)
CREATE TABLE IF NOT EXISTS gnbs (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    market_id INTEGER NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, market_id)
);

-- DUs table (belongs to GNBs)
CREATE TABLE IF NOT EXISTS dus (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gnb_id INTEGER NOT NULL REFERENCES gnbs(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, gnb_id)
);

-- Sectors table (belongs to DUs or directly to GNBs)
CREATE TABLE IF NOT EXISTS sectors (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    du_id INTEGER REFERENCES dus(id) ON DELETE CASCADE,
    gnb_id INTEGER REFERENCES gnbs(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_sector_parent CHECK (
        (du_id IS NOT NULL AND gnb_id IS NULL) OR 
        (du_id IS NULL AND gnb_id IS NOT NULL)
    ),
    UNIQUE(name, du_id, gnb_id)
);

-- Carriers table (belongs to sectors)
CREATE TABLE IF NOT EXISTS carriers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sector_id INTEGER NOT NULL REFERENCES sectors(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, sector_id)
);

-- Audit Log table to track CSV file processing
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(500) NOT NULL UNIQUE,
    file_path VARCHAR(1000) NOT NULL,
    file_hash VARCHAR(64) NOT NULL, -- SHA-256 hash to prevent duplicate processing
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_added JSONB, -- Store count of new records for each entity type
    status VARCHAR(50) DEFAULT 'SUCCESS', -- SUCCESS, ERROR, PARTIAL
    error_message TEXT,
    processing_time_seconds DECIMAL(10,3)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_markets_region_id ON markets(region_id);
CREATE INDEX IF NOT EXISTS idx_gnbs_market_id ON gnbs(market_id);
CREATE INDEX IF NOT EXISTS idx_dus_gnb_id ON dus(gnb_id);
CREATE INDEX IF NOT EXISTS idx_sectors_du_id ON sectors(du_id);
CREATE INDEX IF NOT EXISTS idx_sectors_gnb_id ON sectors(gnb_id);
CREATE INDEX IF NOT EXISTS idx_carriers_sector_id ON carriers(sector_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_file_hash ON audit_log(file_hash);
CREATE INDEX IF NOT EXISTS idx_audit_log_processed_at ON audit_log(processed_at);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at columns
CREATE TRIGGER update_regions_updated_at BEFORE UPDATE ON regions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_markets_updated_at BEFORE UPDATE ON markets FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_gnbs_updated_at BEFORE UPDATE ON gnbs FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_dus_updated_at BEFORE UPDATE ON dus FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_sectors_updated_at BEFORE UPDATE ON sectors FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_carriers_updated_at BEFORE UPDATE ON carriers FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
