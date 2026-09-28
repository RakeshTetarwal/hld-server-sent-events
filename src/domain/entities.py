from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List
from src.domain.enums import BidStatus, AuctionStatus


@dataclass
class Item:
    description: str
    images: List[str] = field(default_factory=list)
    id: Optional[str] = None
    created_at: Optional[datetime] = None


@dataclass
class Bid:
    auction_id: str
    placer_id: str
    price: float
    id: Optional[str] = None
    created_at: Optional[datetime] = None


@dataclass
class Auction:
    item_id: str
    creator_id: str
    base_price: float
    end_date: datetime
    start_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    max_price: Optional[float] = None
    winning_bid_id: Optional[str] = None
    status: AuctionStatus = AuctionStatus.ACTIVE
    id: Optional[str] = None
    created_at: Optional[datetime] = None
    item: Optional[Item] = None

    def __post_init__(self):
        if self.max_price is None:
            self.max_price = self.base_price

    def is_active(self) -> bool:
        return self.status == AuctionStatus.ACTIVE and not self.is_expired()

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        current_time = now or datetime.now(timezone.utc)
        return current_time > self.end_date

    def can_accept_bid(self, price: float) -> tuple[bool, BidStatus]:
        if self.status != AuctionStatus.ACTIVE:
            return False, BidStatus.AUCTION_NOT_ACTIVE
        if self.is_expired():
            return False, BidStatus.AUCTION_EXPIRED
        if price <= (self.max_price or self.base_price):
            return False, BidStatus.BID_TOO_LOW
        return True, BidStatus.ACCEPTED

    def close(self) -> None:
        self.status = AuctionStatus.CLOSED
