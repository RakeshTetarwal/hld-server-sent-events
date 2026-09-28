import os
import json
import asyncio
from aiokafka import AIOKafkaConsumer
import redis.asyncio as aioredis

from src.infrastructure.db import get_db_pool, close_db_pool
from src.infrastructure.postgres_repo import PostgresAuctionRepository
from src.infrastructure.redis_publisher import RedisNotificationPublisher
from src.services.bidding_service import BiddingEngineService

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_BID_TOPIC = os.getenv("KAFKA_BID_TOPIC", "auction.bids.incoming")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "auction-bid-workers")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6381"))


class KafkaBidWorker:
    """Worker daemon orchestrating bid consumption and bidding engine execution."""

    def __init__(self, bidding_engine: BiddingEngineService):
        self.bidding_engine = bidding_engine

    async def handle_message(self, raw_value: bytes):
        payload = json.loads(raw_value.decode("utf-8"))
        await self.bidding_engine.process_bid(
            tracking_id=payload["tracking_id"],
            auction_id=payload["auction_id"],
            placer_id=payload["placer_id"],
            price=float(payload["price"]),
        )


async def main():
    print(f"Starting Kafka Worker connecting to {KAFKA_BOOTSTRAP_SERVERS}...")
    await get_db_pool()
    redis_client = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)

    # Dependency Injection
    auction_repo = PostgresAuctionRepository()
    notifier = RedisNotificationPublisher(redis_client)
    bidding_engine = BiddingEngineService(auction_repo, notifier)
    worker = KafkaBidWorker(bidding_engine)

    consumer = AIOKafkaConsumer(
        KAFKA_BID_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    for attempt in range(15):
        try:
            await consumer.start()
            break
        except Exception as e:
            if attempt == 14:
                raise
            print(f"Worker waiting for Kafka to be ready ({e})... attempt {attempt+1}/15")
            await asyncio.sleep(2)

    print(f"Worker subscribed to topic '{KAFKA_BID_TOPIC}' (group: '{KAFKA_GROUP_ID}'). Waiting for bids...")

    try:
        async for msg in consumer:
            try:
                await worker.handle_message(msg.value)
            except Exception as e:
                print(f"Error processing bid: {e}")
    finally:
        await consumer.stop()
        await redis_client.aclose()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
