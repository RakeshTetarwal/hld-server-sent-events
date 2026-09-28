import json
from typing import Optional
import redis.asyncio as aioredis
from src.interfaces.publishers import NotificationPublisher
from src.domain.value_objects import BidResult


class RedisNotificationPublisher(NotificationPublisher):
    """Redis Pub/Sub implementation of NotificationPublisher."""

    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def publish_bid_outcome(self, result: BidResult) -> None:
        channel = f"auction:{result.auction_id}"
        if result.success:
            payload = {
                "type": "bid_accepted",
                "tracking_id": result.tracking_id,
                "auction_id": result.auction_id,
                "placer_id": result.placer_id,
                "price": result.price,
                "max_price": result.updated_auction.max_price if result.updated_auction else result.price,
                "winning_bid_id": result.bid_id,
            }
        else:
            payload = {
                "type": "bid_rejected",
                "tracking_id": result.tracking_id,
                "auction_id": result.auction_id,
                "placer_id": result.placer_id,
                "price": result.price,
                "status": result.status,
            }
        await self.redis.publish(channel, json.dumps(payload))

    async def publish_auction_closed(
        self, auction_id: str, winning_bid_id: Optional[str], final_price: float
    ) -> None:
        channel = f"auction:{auction_id}"
        payload = {
            "type": "auction_closed",
            "auction_id": auction_id,
            "winning_bid_id": winning_bid_id,
            "final_price": final_price,
        }
        await self.redis.publish(channel, json.dumps(payload))
