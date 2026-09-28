-- 01_schema.sql: Core schema for Online Auction Project

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(64) PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Item table
CREATE TABLE IF NOT EXISTS items (
    id VARCHAR(64) PRIMARY KEY DEFAULT gen_random_uuid()::text,
    description TEXT NOT NULL,
    images JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Auctions table
CREATE TABLE IF NOT EXISTS auctions (
    id VARCHAR(64) PRIMARY KEY DEFAULT gen_random_uuid()::text,
    item_id VARCHAR(64) NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    creator_id VARCHAR(64) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    start_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP WITH TIME ZONE NOT NULL,
    base_price NUMERIC(12, 2) NOT NULL CHECK (base_price >= 0),
    max_price NUMERIC(12, 2) NOT NULL CHECK (max_price >= base_price),
    winning_bid_id VARCHAR(64),
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'CLOSED', 'CANCELLED')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Bids table
CREATE TABLE IF NOT EXISTS bids (
    id VARCHAR(64) PRIMARY KEY DEFAULT gen_random_uuid()::text,
    auction_id VARCHAR(64) NOT NULL REFERENCES auctions(id) ON DELETE CASCADE,
    placer_id VARCHAR(64) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    price NUMERIC(12, 2) NOT NULL CHECK (price > 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add foreign key constraint for winning_bid_id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_auctions_winning_bid'
    ) THEN
        ALTER TABLE auctions 
        ADD CONSTRAINT fk_auctions_winning_bid 
        FOREIGN KEY (winning_bid_id) REFERENCES bids(id) ON DELETE SET NULL;
    END IF;
END $$;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_auctions_status_end_date ON auctions(status, end_date);
CREATE INDEX IF NOT EXISTS idx_bids_auction_created ON bids(auction_id, created_at DESC);
