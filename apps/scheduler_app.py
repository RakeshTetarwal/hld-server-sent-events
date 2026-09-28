import os
import time
import asyncio
import redis.asyncio as aioredis

from src.infrastructure.db import get_db_pool, close_db_pool
from src.infrastructure.postgres_repo import PostgresAuctionRepository, PostgresBidRepository
from src.infrastructure.redis_publisher import RedisNotificationPublisher
from src.services.auction_service import AuctionManagerService

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6381"))
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1.0"))


async def main():
    print(f"Starting Auction Expiration Scheduler polling Redis at {REDIS_HOST}:{REDIS_PORT}...")
    await get_db_pool()
    redis_client = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)

    # Dependency Injection
    auction_repo = PostgresAuctionRepository()
    bid_repo = PostgresBidRepository()
    notifier = RedisNotificationPublisher(redis_client)
    auction_manager = AuctionManagerService(auction_repo, bid_repo, redis_client, notifier)

    try:
        while True:
            try:
                now_ts = time.time()
                expired_ids = await redis_client.zrangebyscore("auction_expirations", 0, now_ts)
                for auction_id in expired_ids:
                    removed = await redis_client.zrem("auction_expirations", auction_id)
                    if removed:
                        closed = await auction_manager.close_expired_auction(auction_id)
                        if closed:
                            print(f"[Scheduler] Closed auction {closed.id}. Winner: {closed.winning_bid_id}, Price: {closed.max_price}")
            except Exception as e:
                print(f"[Scheduler] Error: {e}")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        await redis_client.aclose()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
