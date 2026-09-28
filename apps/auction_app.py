import os
from datetime import datetime
from contextlib import asynccontextmanager
from typing import List, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import redis.asyncio as aioredis

from src.infrastructure.db import get_db_pool, close_db_pool
from src.infrastructure.postgres_repo import PostgresAuctionRepository, PostgresBidRepository
from src.services.auction_service import AuctionManagerService

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6381"))


class CreateAuctionRequest(BaseModel):
    creator_id: str
    description: str
    images: List[str] = Field(default_factory=list)
    base_price: float
    end_date: datetime
    start_date: Optional[datetime] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_db_pool()
    redis_client = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
    
    auction_repo = PostgresAuctionRepository()
    bid_repo = PostgresBidRepository()
    app.state.auction_service = AuctionManagerService(auction_repo, bid_repo, redis_client)
    app.state.redis = redis_client

    yield

    await redis_client.aclose()
    await close_db_pool()


app = FastAPI(title="Auction Management Service", lifespan=lifespan)


@app.post("/auctions", status_code=status.HTTP_201_CREATED)
async def create_auction_endpoint(req: CreateAuctionRequest):
    service: AuctionManagerService = app.state.auction_service
    auction = await service.create_auction(
        creator_id=req.creator_id,
        description=req.description,
        images=req.images,
        base_price=req.base_price,
        end_date=req.end_date,
        start_date=req.start_date,
    )
    return auction


@app.get("/auctions/{auction_id}")
async def get_auction_endpoint(auction_id: str):
    service: AuctionManagerService = app.state.auction_service
    auction = await service.get_auction(auction_id)
    if not auction:
        raise HTTPException(status_code=404, detail="Auction not found")
    return auction


@app.get("/auctions/{auction_id}/bids")
async def get_bids_endpoint(auction_id: str):
    service: AuctionManagerService = app.state.auction_service
    return await service.get_bid_history(auction_id)
