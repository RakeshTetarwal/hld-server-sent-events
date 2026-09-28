import os
import json
import uuid
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import redis.asyncio as aioredis

from src.infrastructure.db import get_db_pool, close_db_pool
from src.infrastructure.postgres_repo import PostgresAuctionRepository, PostgresBidRepository
from src.infrastructure.kafka_producer import KafkaBidQueueProducer
from src.interfaces.producers import BidQueueProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_BID_TOPIC = os.getenv("KAFKA_BID_TOPIC", "auction.bids.incoming")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6381"))


class PlaceBidRequest(BaseModel):
    placer_id: str
    price: float


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_db_pool()

    # Resilient Kafka producer connection with retry
    kafka_prod = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    for attempt in range(15):
        try:
            await kafka_prod.start()
            break
        except Exception as e:
            if attempt == 14:
                raise
            print(f"Waiting for Kafka to be ready ({e})... attempt {attempt+1}/15")
            await asyncio.sleep(2)

    app.state.bid_producer = KafkaBidQueueProducer(kafka_prod, KAFKA_BID_TOPIC)
    app.state.kafka_raw_producer = kafka_prod

    redis_client = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
    app.state.redis = redis_client
    app.state.auction_repo = PostgresAuctionRepository()
    app.state.bid_repo = PostgresBidRepository()

    yield

    await kafka_prod.stop()
    await redis_client.aclose()
    await close_db_pool()


app = FastAPI(title="Bidding Producer & Stream Service", lifespan=lifespan)


@app.post("/auctions/{auction_id}/bids", status_code=status.HTTP_202_ACCEPTED)
async def place_bid_endpoint(auction_id: str, req: PlaceBidRequest):
    if req.price <= 0:
        raise HTTPException(status_code=400, detail="Bid price must be greater than zero")

    tracking_id = str(uuid.uuid4())
    producer: BidQueueProducer = app.state.bid_producer
    await producer.enqueue_bid(
        tracking_id=tracking_id,
        auction_id=auction_id,
        placer_id=req.placer_id,
        price=req.price,
    )

    return {
        "tracking_id": tracking_id,
        "auction_id": auction_id,
        "status": "IN_PROCESS",
        "message": "Bid submitted for processing",
    }


@app.get("/auctions/{auction_id}/bids")
async def get_auction_bids(auction_id: str):
    bid_repo: PostgresBidRepository = app.state.bid_repo
    return await bid_repo.get_bids_by_auction(auction_id)


@app.get("/auctions/{auction_id}/stream")
async def auction_sse_stream(request: Request, auction_id: str):
    async def event_generator():
        redis_client: aioredis.Redis = app.state.redis
        pubsub = redis_client.pubsub()
        channel = f"auction:{auction_id}"
        await pubsub.subscribe(channel)

        try:
            # Send initial snapshot if auction exists
            auction_repo: PostgresAuctionRepository = app.state.auction_repo
            auction = await auction_repo.get_by_id(auction_id)
            if auction:
                initial_data = json.dumps({
                    "type": "snapshot",
                    "auction_id": auction.id,
                    "max_price": auction.max_price,
                    "status": auction.status,
                    "winning_bid_id": auction.winning_bid_id,
                })
                yield f"data: {initial_data}\n\n"

            # Stream live updates from Redis Pub/Sub
            async for message in pubsub.listen():
                if await request.is_disconnected():
                    break
                if message["type"] == "message":
                    yield f"data: {message['data']}\n\n"
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
