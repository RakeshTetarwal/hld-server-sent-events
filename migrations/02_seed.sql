-- 02_seed.sql: Initial seed data for quick testing

-- Insert test users
INSERT INTO users (id, name, email) VALUES
    ('user-1', 'Alice Seller', 'alice@example.com'),
    ('user-2', 'Bob Bidder', 'bob@example.com'),
    ('user-3', 'Charlie Bidder', 'charlie@example.com')
ON CONFLICT (id) DO NOTHING;

-- Insert a test item
INSERT INTO items (id, description, images) VALUES
    ('item-1', 'Vintage 1968 Mechanical Chronograph Watch', '["https://example.com/watch1.jpg", "https://example.com/watch2.jpg"]'::jsonb)
ON CONFLICT (id) DO NOTHING;

-- Insert an active test auction (duration 1 day)
INSERT INTO auctions (id, item_id, creator_id, start_date, end_date, base_price, max_price, status) VALUES
    ('auction-1', 'item-1', 'user-1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '1 day', 100.00, 100.00, 'ACTIVE')
ON CONFLICT (id) DO NOTHING;
