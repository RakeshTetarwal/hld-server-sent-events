CREATE_AUCTION_WITH_ITEM = """
    WITH new_item AS (
        INSERT INTO items (description, images)
        VALUES ($1, $2)
        RETURNING id, description, images
    )
    INSERT INTO auctions (
        item_id, creator_id, start_date, end_date, 
        base_price, max_price, status
    )
    SELECT id, $3, $4, $5, $6, $6, $7
    FROM new_item
    RETURNING 
        auctions.*, 
        (SELECT description FROM new_item) AS item_description,
        (SELECT images FROM new_item) AS item_images;
"""

GET_AUCTION = """
    SELECT 
        a.id, a.item_id, a.creator_id, a.start_date, a.end_date,
        a.base_price, a.max_price, a.winning_bid_id, a.status, a.created_at,
        i.description AS item_description, i.images AS item_images
    FROM auctions a
    JOIN items i ON a.item_id = i.id
    WHERE a.id = $1;
"""

GET_AUCTION_BIDS = """
    SELECT id, auction_id, placer_id, price, created_at
    FROM bids
    WHERE auction_id = $1
    ORDER BY created_at DESC
    LIMIT $2;
"""

GET_AUCTION_FOR_BID = """
    SELECT max_price, status, end_date 
    FROM auctions 
    WHERE id = $1;
"""

INSERT_BID = """
    INSERT INTO bids (auction_id, placer_id, price)
    VALUES ($1, $2, $3)
    RETURNING id, auction_id, placer_id, price, created_at;
"""

UPDATE_AUCTION_OPTIMISTIC = """
    UPDATE auctions
    SET max_price = $1, winning_bid_id = $2
    WHERE id = $3 AND max_price < $1 AND status = $4
    RETURNING *;
"""

CLOSE_AUCTION = """
    UPDATE auctions
    SET status = $1
    WHERE id = $2 AND status = $3
    RETURNING *;
"""
