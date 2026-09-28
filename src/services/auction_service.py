from typing import Optional, List
import redis.asyncio as aioredis

from src.domain.entities import Auction, Bid
from src.interfaces.repositories import IAuctionRepository, IBidRepository
from src.interfaces.publishers import INotificationPublisher


class AuctionManagerService:
    """Application Service handling auction lifecycle and delayed scheduling."""

    def __init__(
        self,
        auction_repo: IAuctionRepository,
        bid_repo: IBidRepository,
        redis_client: aioredis.Redis,
        notifier: Optional[INotificationPublisher] = None,
    ):
        self.auction_repo = auction_repo
        self.bid_repo = bid_repo
        self.redis = redis_client
        self.notifier = notifier

    async def create_auction(
        self,
        creator_id: str,
        description: str,
        images: List[str],
        base_price: float,
        end_date,
        start_date=None,
    ) -> Auction:
        auction = await self.auction_repo.create_with_item(
            creator_id=creator_id,
            description=description,
            images=images,
            base_price=base_price,
            end_date=end_date,
            start_date=start_date,
        )
        # Schedule delayed expiration task in Redis Sorted Set (ZSET)
        await self.redis.zadd("auction_expirations", {auction.id: auction.end_date.timestamp()})
        return auction

    async def get_auction(self, auction_id: str) -> Optional[Auction]:
        return await self.auction_repo.get_by_id(auction_id)

    async def get_bid_history(self, auction_id: str, limit: int = 50) -> List[Bid]:
        return await self.bid_repo.get_bids_by_auction(auction_id, limit)

    async def close_expired_auction(self, auction_id: str) -> Optional[Auction]:
        closed = await self.auction_repo.close_auction(auction_id)
        if closed and self.notifier:
            await self.notifier.publish_auction_closed(
                auction_id=closed.id,
                winning_bid_id=closed.winning_bid_id,
                final_price=closed.max_price or closed.base_price,
            )
        return closed
