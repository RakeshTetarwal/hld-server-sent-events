from dataclasses import dataclass
from typing import Optional
from src.domain.entities import Auction
from src.domain.enums import BidStatus


@dataclass(frozen=True)
class BidResult:
    """Immutable Value Object representing the outcome of a bid placement."""
    tracking_id: str
    auction_id: str
    placer_id: str
    price: float
    success: bool
    status: BidStatus
    updated_auction: Optional[Auction] = None
    bid_id: Optional[str] = None
