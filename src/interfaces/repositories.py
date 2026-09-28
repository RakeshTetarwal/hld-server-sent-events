from abc import ABC, abstractmethod
from typing import Optional, List, Tuple
from src.domain.entities import Auction, Bid
from src.domain.enums import BidStatus


class IAuctionRepository(ABC):
    @abstractmethod
    async def create_with_item(
        self,
        creator_id: str,
        description: str,
        images: List[str],
        base_price: float,
        end_date,
        start_date=None,
    ) -> Auction:
        pass

    @abstractmethod
    async def get_by_id(self, auction_id: str) -> Optional[Auction]:
        pass

    @abstractmethod
    async def update_highest_bid_optimistic(
        self, auction_id: str, placer_id: str, price: float
    ) -> Tuple[bool, BidStatus, Optional[Auction], Optional[str]]:
        pass

    @abstractmethod
    async def close_auction(self, auction_id: str) -> Optional[Auction]:
        pass


class IBidRepository(ABC):
    @abstractmethod
    async def get_bids_by_auction(self, auction_id: str, limit: int = 50) -> List[Bid]:
        pass
