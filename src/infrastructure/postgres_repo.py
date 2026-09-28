from typing import Optional, List, Tuple
from datetime import datetime, timezone
import asyncpg

from src.domain.entities import Auction, Item, Bid
from src.domain.enums import BidStatus, AuctionStatus
from src.interfaces.repositories import IAuctionRepository, IBidRepository
from src.infrastructure import queries
from src.infrastructure.db import get_db_pool, transaction


class PostgresAuctionRepository(IAuctionRepository):
    """PostgreSQL implementation of IAuctionRepository with optimistic locking."""

    def _row_to_auction(self, row: Optional[asyncpg.Record]) -> Optional[Auction]:
        if not row:
            return None
        data = dict(row)
        item = Item(
            id=data.get("item_id"),
            description=data.get("item_description", ""),
            images=data.get("item_images") or [],
        )
        return Auction(
            id=data.get("id"),
            item_id=data.get("item_id"),
            creator_id=data.get("creator_id"),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            base_price=float(data.get("base_price")),
            max_price=float(data.get("max_price")),
            winning_bid_id=data.get("winning_bid_id"),
            status=AuctionStatus(data.get("status")),
            created_at=data.get("created_at"),
            item=item,
        )

    async def create_with_item(
        self,
        creator_id: str,
        description: str,
        images: List[str],
        base_price: float,
        end_date,
        start_date=None,
    ) -> Auction:
        if start_date is None:
            start_date = datetime.now(timezone.utc)

        db = await get_db_pool()
        row = await db.fetchrow(
            queries.CREATE_AUCTION_WITH_ITEM,
            description,
            images,
            creator_id,
            start_date,
            end_date,
            base_price,
            AuctionStatus.ACTIVE,
        )
        return self._row_to_auction(row)

    async def get_by_id(self, auction_id: str) -> Optional[Auction]:
        db = await get_db_pool()
        row = await db.fetchrow(queries.GET_AUCTION, auction_id)
        return self._row_to_auction(row)

    async def update_highest_bid_optimistic(
        self, auction_id: str, placer_id: str, price: float
    ) -> Tuple[bool, BidStatus, Optional[Auction], Optional[str]]:
        async with transaction() as (conn, tr):
            current = await conn.fetchrow(queries.GET_AUCTION_FOR_BID, auction_id)
            if not current:
                return False, BidStatus.AUCTION_NOT_FOUND, None, None

            if current["status"] != AuctionStatus.ACTIVE:
                return False, BidStatus.AUCTION_NOT_ACTIVE, None, None

            if datetime.now(timezone.utc) > current["end_date"]:
                await conn.execute(queries.CLOSE_AUCTION, AuctionStatus.CLOSED, auction_id, AuctionStatus.ACTIVE)
                return False, BidStatus.AUCTION_EXPIRED, None, None

            if price <= float(current["max_price"]):
                return False, BidStatus.BID_TOO_LOW, None, None

            bid_row = await conn.fetchrow(queries.INSERT_BID, auction_id, placer_id, price)
            bid_id = bid_row["id"]

            updated_row = await conn.fetchrow(
                queries.UPDATE_AUCTION_OPTIMISTIC,
                price,
                bid_id,
                auction_id,
                AuctionStatus.ACTIVE,
            )

            if updated_row is None:
                await tr.rollback()
                return False, BidStatus.OUTBID, None, None

            # Fetch joined row for complete entity mapping
            joined_row = await conn.fetchrow(queries.GET_AUCTION, auction_id)
            return True, BidStatus.ACCEPTED, self._row_to_auction(joined_row), bid_id

    async def close_auction(self, auction_id: str) -> Optional[Auction]:
        db = await get_db_pool()
        row = await db.fetchrow(queries.CLOSE_AUCTION, AuctionStatus.CLOSED, auction_id, AuctionStatus.ACTIVE)
        if not row:
            return None
        joined_row = await db.fetchrow(queries.GET_AUCTION, auction_id)
        return self._row_to_auction(joined_row)


class PostgresBidRepository(IBidRepository):
    """PostgreSQL implementation of IBidRepository."""

    async def get_bids_by_auction(self, auction_id: str, limit: int = 50) -> List[Bid]:
        db = await get_db_pool()
        rows = await db.fetch(queries.GET_AUCTION_BIDS, auction_id, limit)
        return [
            Bid(
                id=r["id"],
                auction_id=r["auction_id"],
                placer_id=r["placer_id"],
                price=float(r["price"]),
                created_at=r["created_at"],
            )
            for r in rows
        ]
