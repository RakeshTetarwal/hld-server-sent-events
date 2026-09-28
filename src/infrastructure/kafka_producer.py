import json
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from src.interfaces.producers import IBidQueueProducer


class KafkaBidQueueProducer(IBidQueueProducer):
    """Kafka implementation of IBidQueueProducer, partitioned strictly by auction_id."""

    def __init__(self, producer: AIOKafkaProducer, topic: str):
        self.producer = producer
        self.topic = topic

    async def enqueue_bid(
        self, tracking_id: str, auction_id: str, placer_id: str, price: float
    ) -> None:
        payload = {
            "tracking_id": tracking_id,
            "auction_id": auction_id,
            "placer_id": placer_id,
            "price": price,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await self.producer.send_and_wait(
            topic=self.topic,
            key=auction_id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
        )
